"""Проект ETL. Ильнур Аскаров, когорта 49. Этапы 1–3."""
from datetime import datetime, timedelta
from pathlib import Path
import requests
from psycopg2 import sql
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from order_utils import COLUMNS, normalize_csv, normalize_report

SQL_DIR = Path(__file__).parent / 'sql'
PG_CONN = 'postgresql_de'

def api_settings():
    nickname = Variable.get('project3_nickname', default_var='').strip()
    if not nickname:
        raise ValueError('Задайте Airflow Variable project3_nickname: ваш X-Nickname из курса')
    connection = BaseHook.get_connection('http_conn_id')
    key = connection.extra_dejson.get('api_key')
    if not key:
        raise ValueError('В http_conn_id Extra отсутствует api_key')
    base = connection.host.rstrip('/')
    if not base.startswith(('http://', 'https://')):
        base = 'https://' + base
    headers = {'X-Nickname': nickname, 'X-Cohort': '49', 'X-Project': 'True', 'X-API-KEY': key}
    return base, headers, nickname

def api_request(method, url, headers, **kwargs):
    response = requests.request(method, url, headers=headers, timeout=(10,60), **kwargs)
    response.raise_for_status()
    return response.json()

def get_report_id():
    # Стабильный report_id для всех дней и повторных запусков.
    existing = Variable.get('project3_report_id', default_var='')
    if existing:
        return existing
    base, headers, _ = api_settings()
    task_id = api_request('POST', base+'/generate_report', headers)['task_id']
    return task_id

def report_ready(**context):
    from airflow.exceptions import AirflowException
    base, headers, _ = api_settings()
    report_id = Variable.get('project3_report_id', default_var='')
    if report_id:
        return report_id
    ti = context['ti']
    # Получение task_id выделено в отдельную задачу: повтор опроса не создаёт новый отчёт.
    task_id = ti.xcom_pull(task_ids='generate_report')
    result = api_request('GET', base+'/get_report', headers, params={'task_id':task_id})
    if result.get('status') != 'SUCCESS':
        raise AirflowException('Отчёт пока не готов: ' + str(result.get('status')))
    report_id = result['data']['report_id']
    Variable.set('project3_report_id', report_id)
    return report_id

def load_day(ds, **context):
    base, headers, nickname = api_settings()
    report_id = context['ti'].xcom_pull(task_ids='get_report')
    result = api_request('GET', base+'/get_increment', headers,
                         params={'report_id':report_id,'date':ds+'T00:00:00'})
    if result.get('status') != 'SUCCESS':
        raise ValueError('Инкремент не получен: ' + str(result.get('status')))
    increment_id = result['data']['increment_id']
    url = ('https://storage.yandexcloud.net/s3-sprint3/cohort_49/'
           + nickname + '/project/' + str(increment_id) + '/user_order_log_inc.csv')
    response = requests.get(url, timeout=(10,120))
    response.raise_for_status()
    rows = normalize_csv(response.content, ds)
    replace_days({ds: rows})


def replace_days(days, bootstrap_report_id=None):
    """Общая атомарная загрузка: основной отчёт и дневные инкременты."""
    hook = PostgresHook(postgres_conn_id=PG_CONN)
    conn = hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Один writer, в том числе для ручного вызова и scheduler.
                cur.execute("SELECT pg_advisory_xact_lock(490003)")
                cur.execute((SQL_DIR/'001_migration.sql').read_text())
                if bootstrap_report_id:
                    cur.execute("SELECT report_id FROM mart.project3_load_state WHERE report_id=%s",
                                (bootstrap_report_id,))
                    if cur.fetchone():
                        return
                for ds, rows in sorted(days.items()):
                    cur.execute('SELECT count(*) FROM mart.d_calendar WHERE date_actual::date=%s::date',(ds,))
                    if cur.fetchone()[0] != 1:
                        raise ValueError('В календаре должна быть ровно одна запись для '+ds)
                    cur.execute("DELETE FROM staging.user_order_log WHERE date_time >= %s::date AND date_time < %s::date + interval '1 day'",(ds,ds))
                    if rows:
                        query = sql.SQL('INSERT INTO staging.user_order_log ({}) VALUES %s').format(
                            sql.SQL(',').join(map(sql.Identifier,COLUMNS))).as_string(cur)
                        execute_values(cur,query,[tuple(row[c] for c in COLUMNS) for row in rows])
                    cur.execute((SQL_DIR/'002_sales.sql').read_text(),{'day':ds})
                # Только после загрузки всех дней — пересчёт затронутых недель.
                week_days = {}
                for ds in days:
                    date = datetime.fromisoformat(ds).date()
                    week_days[date - timedelta(days=date.weekday())] = ds
                for ds in week_days.values():
                    cur.execute((SQL_DIR/'003_retention.sql').read_text(),{'day':ds})
                if bootstrap_report_id:
                    cur.execute("INSERT INTO mart.project3_load_state(report_id,start_date,end_date) VALUES (%s,%s,%s)",
                                (bootstrap_report_id,min(days),max(days)))
    finally:
        conn.close()


def bootstrap_initial_report():
    """Первичная загрузка user_order_log.csv; после успеха повторно не затирает инкременты."""
    report_id = Variable.get('project3_report_id')
    hook = PostgresHook(postgres_conn_id=PG_CONN)
    conn = hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('mart.project3_load_state')")
                if cur.fetchone()[0]:
                    cur.execute("SELECT report_id FROM mart.project3_load_state WHERE report_id=%s",(report_id,))
                    if cur.fetchone():
                        print('Основной отчёт уже загружен')
                        return
    finally:
        conn.close()
    _, _, nickname = api_settings()
    url = ('https://storage.yandexcloud.net/s3-sprint3/cohort_49/'
           + nickname + '/project/' + report_id + '/user_order_log.csv')
    response = requests.get(url, timeout=(15,120))
    response.raise_for_status()
    days = normalize_report(response.content)
    print('Основной отчёт: дней',len(days),'уникальных записей',sum(map(len,days.values())))
    replace_days(days, bootstrap_report_id=report_id)
    print('Основной отчёт и витрины загружены:', min(days), '—', max(days))

with DAG('sales_mart_project3',start_date=datetime(2020,1,1),
         schedule_interval='@daily',catchup=False,max_active_runs=1,
         default_args={'owner':'Ильнур Аскаров','retries':3,'retry_delay':timedelta(seconds=30)},
         tags=['project3','cohort49']) as dag:
    generate = PythonOperator(task_id='generate_report',python_callable=get_report_id)
    report = PythonOperator(task_id='get_report',python_callable=report_ready,retries=20)
    bootstrap = PythonOperator(task_id='bootstrap_initial_report',python_callable=bootstrap_initial_report)
    load = PythonOperator(task_id='load_day_and_marts',python_callable=load_day)
    generate >> report >> bootstrap >> load
