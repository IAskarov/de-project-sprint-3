-- Выполняется автоматически перед загрузкой. Исходные таблицы предоставляет контейнер.
ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS source_order_id text;
UPDATE staging.user_order_log SET status = 'shipped' WHERE status IS NULL;
ALTER TABLE staging.user_order_log ALTER COLUMN status SET DEFAULT 'shipped';
ALTER TABLE staging.user_order_log ALTER COLUMN status SET NOT NULL;

ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS source_order_id text;
ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS source_event_id text;
UPDATE mart.f_sales SET status = 'shipped' WHERE status IS NULL;
ALTER TABLE mart.f_sales ALTER COLUMN status SET DEFAULT 'shipped';
ALTER TABLE mart.f_sales ALTER COLUMN status SET NOT NULL;

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
 new_customers_count bigint NOT NULL,
 returning_customers_count bigint NOT NULL,
 refunded_customer_count bigint NOT NULL,
 period_name text NOT NULL,
 period_id integer NOT NULL,
 item_id integer NOT NULL,
 new_customers_revenue numeric NOT NULL,
 returning_customers_revenue numeric NOT NULL,
 customers_refunded bigint NOT NULL
);
-- period_id = ISO-год * 100 + ISO-неделя, например 202053.
CREATE UNIQUE INDEX IF NOT EXISTS ux_retention_period_item
 ON mart.f_customer_retention(period_name, period_id, item_id);

-- Маркер записывается в той же транзакции, что и основной отчёт.
CREATE TABLE IF NOT EXISTS mart.project3_load_state (
 report_id text PRIMARY KEY,
 start_date date NOT NULL,
 end_date date NOT NULL,
 loaded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
