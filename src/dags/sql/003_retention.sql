-- Исправлено: пересчитываем всю календарную неделю изменённого дня.
DELETE FROM mart.f_customer_retention
WHERE period_name='weekly' AND period_id =
 extract(isoyear FROM %(day)s::date)::int * 100 + extract(week FROM %(day)s::date)::int;

INSERT INTO mart.f_customer_retention
(new_customers_count,returning_customers_count,refunded_customer_count,
 period_name,period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
WITH events AS (
 SELECT f.* FROM mart.f_sales f JOIN mart.d_calendar c USING(date_id)
 WHERE c.date_actual >= date_trunc('week', %(day)s::date)
   AND c.date_actual < date_trunc('week', %(day)s::date) + interval '7 days'
), customer_item AS (
 SELECT customer_id,item_id,
 count(DISTINCT coalesce(source_order_id,'legacy:'||id::text))
   FILTER (WHERE status='shipped') AS orders_count,
 sum(payment_amount) AS revenue,
 count(*) FILTER (WHERE status='refunded') AS refunds
 FROM events GROUP BY customer_id,item_id
)
SELECT count(*) FILTER (WHERE orders_count=1),
 count(*) FILTER (WHERE orders_count>1),
 count(*) FILTER (WHERE refunds>0),
 'weekly', extract(isoyear FROM %(day)s::date)::int*100 + extract(week FROM %(day)s::date)::int,
 item_id,
 coalesce(sum(revenue) FILTER (WHERE orders_count=1),0),
 coalesce(sum(revenue) FILTER (WHERE orders_count>1),0),sum(refunds)
FROM customer_item GROUP BY item_id;
