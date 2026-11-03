-- Выполнять после успешной загрузки; это read-only проверки.
-- Должно быть 0 строк:
SELECT period_name,period_id,item_id,count(*)
FROM mart.f_customer_retention GROUP BY 1,2,3 HAVING count(*)>1;
SELECT date_id,item_id,status,source_event_id,count(*)
FROM mart.f_sales WHERE source_event_id IS NOT NULL
GROUP BY 1,2,3,4 HAVING count(*)>1;
SELECT status,count(*) FROM mart.f_sales
WHERE status IS NULL OR status NOT IN ('shipped','refunded') GROUP BY status;
SELECT * FROM mart.f_sales
WHERE (status='refunded' AND payment_amount>0)
   OR (status='shipped' AND payment_amount<0);
-- Дневная выручка для сравнения до/после повторного запуска:
SELECT c.date_actual,count(*) AS rows_count,sum(f.payment_amount) AS revenue
FROM mart.f_sales f JOIN mart.d_calendar c USING(date_id)
GROUP BY c.date_actual ORDER BY c.date_actual;
-- Сохранить результат и сравнить после повторного запуска:
SELECT * FROM mart.f_customer_retention ORDER BY period_id,item_id;
