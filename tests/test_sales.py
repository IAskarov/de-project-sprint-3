"""Проверка SQL загрузки продаж в DuckDB; параметр psycopg2 заменяется литералом."""
import unittest
from pathlib import Path
try:
 import duckdb
except ImportError:
 duckdb=None
@unittest.skipIf(duckdb is None,'Установите duckdb')
class SalesTests(unittest.TestCase):
 def setUp(self):
  self.db=duckdb.connect()
  self.db.execute('CREATE SCHEMA mart; CREATE SCHEMA staging')
  self.db.execute('CREATE TABLE mart.d_calendar(date_id int,date_actual date)')
  self.db.execute("INSERT INTO mart.d_calendar VALUES (1,'2020-01-22')")
  self.db.execute('CREATE TABLE mart.d_item(item_id int primary key,item_name varchar)')
  self.db.execute('CREATE TABLE mart.d_city(city_id int primary key,city_name varchar)')
  self.db.execute('CREATE TABLE mart.d_customer(customer_id int primary key,first_name varchar,last_name varchar,city_id int)')
  self.db.execute('CREATE TABLE mart.f_sales(date_id int,item_id int,customer_id int,city_id int,quantity numeric,payment_amount numeric,status varchar,source_order_id varchar,source_event_id varchar)')
  self.db.execute('CREATE TABLE staging.user_order_log(date_time timestamp,item_id int,item_name varchar,city_id int,city_name varchar,customer_id int,first_name varchar,last_name varchar,quantity numeric,payment_amount numeric,status varchar,source_order_id varchar,uniq_id varchar)')
  self.db.execute("""INSERT INTO staging.user_order_log VALUES
  ('2020-01-22 13:00',1,'Item',1,'City',10,'A','B',1,100,'shipped','o1','e1'),
  ('2020-01-22 14:00',1,'Item',1,'City',10,'A','B',1,100,'refunded','o1','e1')""")
 def tearDown(self): self.db.close()
 def load(self):
  query=(Path(__file__).resolve().parents[1]/'src/dags/sql/002_sales.sql').read_text()
  self.db.execute(query.replace('%(day)s',"'2020-01-22'"))
 def test_refund_offsets_sale(self):
  self.load();self.assertEqual(self.db.execute('SELECT count(*),sum(payment_amount),sum(quantity) FROM mart.f_sales').fetchone(),(2,0,0))
 def test_rerun_does_not_duplicate(self):
  self.load();self.load();self.assertEqual(self.db.execute('SELECT count(*) FROM mart.f_sales').fetchone()[0],2)
 def test_corrected_source_replaces_day(self):
  self.load();self.db.execute("DELETE FROM staging.user_order_log WHERE status='refunded'")
  self.load();self.assertEqual(self.db.execute('SELECT count(*),sum(payment_amount) FROM mart.f_sales').fetchone(),(1,100))
 def test_dimension_update(self):
  self.load();self.db.execute("UPDATE staging.user_order_log SET item_name='New name'")
  self.load();self.assertEqual(self.db.execute('SELECT item_name FROM mart.d_item').fetchone()[0],'New name')
if __name__=='__main__':unittest.main()
