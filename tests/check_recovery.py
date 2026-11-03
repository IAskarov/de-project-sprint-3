"""Интеграционная проверка восстановления неполного инкремента в учебном контейнере.
Временно заменяет один день неполной копией источника, затем восстанавливает через API.
Запуск при остановленном расписании DAG: python3 -u check_recovery.py 2026-11-01.
"""
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, '/lessons/dags')
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from sprint3 import api_settings, api_request, replace_days, load_day
from order_utils import normalize_csv

TABLES = ('staging.user_order_log', 'mart.f_sales', 'mart.f_customer_retention')

def snapshot():
    h = PostgresHook(postgres_conn_id='postgresql_de')
    result = {}
    for table in TABLES:
        rows = h.get_records("SELECT to_jsonb(t) - 'id' FROM " + table + " t")
        records = sorted(json.dumps(row[0], sort_keys=True, default=str) for row in rows)
        result[table] = {
            'rows': len(records),
            'sha256': hashlib.sha256('\n'.join(records).encode()).hexdigest()
        }
    return result


def main(day):
    if day != '2026-11-01':
        # Не ограничивает проект датой; защита от опечатки при текущей ручной проверке.
        raise ValueError('Этот сценарий подготовлен для воскресенья 2026-11-01')
    if PostgresHook(postgres_conn_id='postgresql_de').get_first(
        "SELECT count(*) FROM mart.f_sales f JOIN mart.d_calendar c USING(date_id) WHERE c.date_actual::date=%s::date", (day,)
    )[0] == 0:
        raise ValueError('Сначала успешно загрузите воскресный инкремент обычным DAG')
    report_id = Variable.get('project3_report_id')
    base, headers, nickname = api_settings()
    result = api_request('GET',base+'/get_increment',headers,
                         params={'report_id':report_id,'date':day+'T00:00:00'})
    if result.get('status') != 'SUCCESS':
        raise ValueError('API не вернул инкремент: '+str(result.get('status')))
    increment_id = result['data']['increment_id']
    url = ('https://storage.yandexcloud.net/s3-sprint3/cohort_49/' + nickname
           + '/project/' + str(increment_id) + '/user_order_log_inc.csv')
    response = requests.get(url,timeout=(15,120))
    response.raise_for_status()
    full_rows = normalize_csv(response.content,day)
    if len(full_rows)<2:
        raise ValueError('Для сценария нужны хотя бы две записи')
    backup = Path('/tmp/project3_recovery_'+day+'.csv')
    backup.write_bytes(response.content)
    print('Полный источник сохранён:', backup, 'записей:', len(full_rows))
    before = snapshot()
    # Данные уже должны соответствовать текущей версии источника.
    # Если источник обновился, проверку начинаем заново на его полной версии.
    replace_days({day:full_rows})
    baseline = snapshot()
    print('Текущий API совпал с ранее загруженными данными:', baseline == before)
    Path('/tmp/project3_recovery_before.json').write_text(json.dumps(baseline,indent=2))
    modified = False
    try:
        modified = True
        replace_days({day:full_rows[:-1]})
        partial = snapshot()
        if partial['mart.f_sales'] == baseline['mart.f_sales']:
            raise AssertionError('Неполный источник не изменил продажи')
        print('Смоделирована потеря одной записи; продажи изменились')
        # Та же функция, что исполняется PythonOperator: настоящий API и CSV.
        ti = SimpleNamespace(xcom_pull=lambda **kwargs: report_id)
        load_day(ds=day,ti=ti)
        after = snapshot()
        for table in TABLES:
            print(table, 'ВОССТАНОВЛЕНО' if baseline[table]==after[table] else 'РАСХОЖДЕНИЕ',
                  'строк:',after[table]['rows'])
        if baseline != after:
            raise AssertionError('Состояние отличается от полной версии источника; см. вывод')
        modified = False
        print('Восстановление через API без дубликатов: True')
        Path('/tmp/project3_recovery_result.json').write_text(
            json.dumps({'day':day,'success':True,'before':baseline,'after':after},indent=2))
    finally:
        if modified:
            print('Проверка не завершилась: восстанавливаю сохранённый полный источник')
            replace_days({day:full_rows})
            if snapshot() != baseline:
                raise RuntimeError('Проверьте восстановление по сохранённому CSV: '+str(backup))
            print('Полное исходное состояние восстановлено из резервной копии')

if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv)>1 else '2026-11-01')
