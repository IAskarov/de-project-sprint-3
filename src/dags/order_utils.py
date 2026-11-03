"""Нормализация CSV без зависимости от Airflow."""
import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

COLUMNS = ['date_time', 'city_id', 'city_name', 'customer_id', 'first_name',
           'last_name', 'item_id', 'item_name', 'quantity', 'payment_amount',
           'uniq_id', 'status', 'source_order_id']

def normalize_csv(content, business_date):
    reader = csv.DictReader(io.StringIO(content.decode('utf-8-sig')))
    required = set(COLUMNS) - {'status', 'source_order_id'}
    if not reader.fieldnames or not required.issubset(reader.fieldnames):
        raise ValueError('В CSV отсутствуют поля: ' + str(required - set(reader.fieldnames or [])))
    events = {}
    for raw in reader:
        row = {k: raw.get(k) for k in COLUMNS}
        row['status'] = raw.get('status') or 'shipped'
        if row['status'] not in ('shipped', 'refunded'):
            raise ValueError('Неизвестный статус: ' + row['status'])
        if datetime.fromisoformat(row['date_time']).date().isoformat() != business_date:
            raise ValueError('В инкременте обнаружена другая дата')
        if not row['uniq_id']:
            raise ValueError('Пустой uniq_id')
        # Если источник содержит order_id, используем его; иначе учебный uniq_id.
        row['source_order_id'] = raw.get('order_id') or row['uniq_id']
        for key in ('city_id', 'customer_id', 'item_id'):
            row[key] = int(row[key])
        for key in ('quantity', 'payment_amount'):
            try:
                value = Decimal(row[key])
                if not value.is_finite():
                    raise ValueError('Неконечное число')
                row[key] = abs(value)  # знак задаётся один раз в mart
            except (InvalidOperation, TypeError):
                raise ValueError('Некорректное число: ' + key)
        # Продажа и возврат с одним uniq_id остаются разными событиями.
        event = (row['uniq_id'], row['status'], row['item_id'])
        if event in events and events[event] != row:
            raise ValueError('Разные данные у одного ключа события: ' + str(event))
        events[event] = row
    return list(events.values())


def normalize_report(content):
    """Основной отчёт: группировка по дням и проверка каждого дня до записи в БД."""
    from datetime import timedelta
    reader = csv.DictReader(io.StringIO(content.decode('utf-8-sig')))
    fields = reader.fieldnames
    required = set(COLUMNS) - {'status', 'source_order_id'}
    if not fields or not required.issubset(fields):
        raise ValueError('Некорректные колонки основного отчёта')
    grouped = {}
    for row in reader:
        day = datetime.fromisoformat(row['date_time']).date().isoformat()
        grouped.setdefault(day, []).append(row)
    if not grouped:
        raise ValueError('Основной отчёт пуст — начальная загрузка отменена')
    start = datetime.fromisoformat(min(grouped)).date()
    end = datetime.fromisoformat(max(grouped)).date()
    result = {}
    day = start
    while day <= end:
        key = day.isoformat()
        stream = io.StringIO()
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(grouped.get(key, []))
        result[key] = normalize_csv(stream.getvalue().encode('utf-8'), key)
        day += timedelta(days=1)
    return result
