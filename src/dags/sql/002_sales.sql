-- Исправлено: полная замена дня; все запросы выполняются в одной транзакции.
INSERT INTO mart.d_city(city_id, city_name)
SELECT DISTINCT city_id, city_name FROM staging.user_order_log
WHERE date_time >= %(day)s::date AND date_time < %(day)s::date + interval '1 day'
ON CONFLICT (city_id) DO UPDATE SET city_name = EXCLUDED.city_name;

INSERT INTO mart.d_item(item_id, item_name)
SELECT DISTINCT item_id, item_name FROM staging.user_order_log
WHERE date_time >= %(day)s::date AND date_time < %(day)s::date + interval '1 day'
ON CONFLICT (item_id) DO UPDATE SET item_name = EXCLUDED.item_name;

INSERT INTO mart.d_customer(customer_id, first_name, last_name, city_id)
SELECT DISTINCT ON (customer_id) customer_id, first_name, last_name, city_id
FROM staging.user_order_log
WHERE date_time >= %(day)s::date AND date_time < %(day)s::date + interval '1 day'
ORDER BY customer_id, date_time DESC, uniq_id DESC
ON CONFLICT (customer_id) DO UPDATE SET first_name = EXCLUDED.first_name,
 last_name = EXCLUDED.last_name, city_id = EXCLUDED.city_id;

DELETE FROM mart.f_sales f USING mart.d_calendar c
WHERE f.date_id = c.date_id AND c.date_actual::date = %(day)s::date;
INSERT INTO mart.f_sales
(date_id,item_id,customer_id,city_id,quantity,payment_amount,status,source_order_id,source_event_id)
SELECT c.date_id,u.item_id,u.customer_id,u.city_id,
 CASE WHEN u.status='refunded' THEN -abs(u.quantity) ELSE abs(u.quantity) END,
 CASE WHEN u.status='refunded' THEN -abs(u.payment_amount) ELSE abs(u.payment_amount) END,
 u.status,coalesce(u.source_order_id,u.uniq_id::text),u.uniq_id::text
FROM staging.user_order_log u
JOIN mart.d_calendar c ON c.date_actual::date=u.date_time::date
WHERE u.date_time >= %(day)s::date AND u.date_time < %(day)s::date + interval '1 day';
