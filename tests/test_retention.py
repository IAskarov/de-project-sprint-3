"""Проверка настоящего SQL агрегации на синтетических данных в DuckDB.
Это не замена интеграционной проверки PostgreSQL/Airflow.
Запуск: python -m pip install duckdb && python -m unittest discover -s tests -v
"""
from pathlib import Path
import unittest
try:
 import duckdb
except ImportError:
 duckdb=None

@unittest.skipIf(duckdb is None,'Для проверки SQL установите duckdb')
class RetentionTests(unittest.TestCase):
 def setUp(self):
  self.db=duckdb.connect()
  self.db.execute('CREATE SCHEMA mart')
  self.db.execute('CREATE TABLE mart.d_calendar(date_id integer,date_actual date)')
  self.db.execute("INSERT INTO mart.d_calendar VALUES (1,'2020-12-28'),(2,'2021-01-03'),(3,'2021-01-04')")
  self.db.execute('CREATE TABLE mart.f_sales(id integer,date_id integer,item_id integer,customer_id integer,payment_amount decimal(12,2),status varchar,source_order_id varchar)')
  self.db.execute('CREATE TABLE mart.f_customer_retention(new_customers_count bigint,returning_customers_count bigint,refunded_customer_count bigint,period_name varchar,period_id integer,item_id integer,new_customers_revenue decimal(12,2),returning_customers_revenue decimal(12,2),customers_refunded bigint)')
  self.db.execute("""INSERT INTO mart.f_sales VALUES
  (1,1,1,10,100,'shipped','a'),(2,2,1,10,-100,'refunded','a'),
  (3,1,1,20,200,'shipped','b'),(4,2,1,20,300,'shipped','c'),
  (5,1,2,30,-50,'refunded','d'),(6,3,1,40,999,'shipped','e')""")
 def tearDown(self): self.db.close()
 def recalc(self,day='2021-01-03'):
  statement=(Path(__file__).resolve().parents[1]/'src/dags/sql/003_retention.sql').read_text()
  # Единственная адаптация: синтаксис именованного параметра psycopg2.
  self.db.execute(statement.replace('%(day)s',"'"+day+"'"))
 def test_metrics_and_year_boundary(self):
  self.recalc()
  rows=self.db.execute('SELECT * FROM mart.f_customer_retention ORDER BY item_id').fetchall()
  self.assertEqual(rows[0],(1,1,1,'weekly',202053,1,0,500,1))
  self.assertEqual(rows[1],(0,0,1,'weekly',202053,2,0,0,1))
 def test_rerun_identical(self):
  self.recalc(); before=self.db.execute('SELECT * FROM mart.f_customer_retention ORDER BY item_id').fetchall()
  self.recalc(); self.assertEqual(before,self.db.execute('SELECT * FROM mart.f_customer_retention ORDER BY item_id').fetchall())
 def test_corrected_increment_reclassifies_customer(self):
  self.recalc()
  self.db.execute("INSERT INTO mart.f_sales VALUES (7,2,1,10,50,'shipped','f')")
  self.recalc()
  self.assertEqual(self.db.execute('SELECT new_customers_count,returning_customers_count,returning_customers_revenue FROM mart.f_customer_retention WHERE item_id=1').fetchone(),(0,2,550))
 def test_other_week_preserved(self):
  self.recalc(); self.recalc('2021-01-04')
  self.assertEqual(self.db.execute('SELECT count(*) FROM mart.f_customer_retention').fetchone()[0],3)
if __name__=='__main__':unittest.main()
