import csv
import io
import sys
import unittest
from pathlib import Path
from decimal import Decimal
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'/'dags'))
from order_utils import normalize_csv, normalize_report

BASE=dict(date_time='2020-01-22T13:45:00', city_id='1', city_name='Perm',
 customer_id='10', first_name='A',last_name='B',item_id='2',item_name='Item',
 quantity='1',payment_amount='100',uniq_id='event1')
def content(rows):
 out=io.StringIO(); fields=list(dict.fromkeys(k for r in rows for k in r))
 writer=csv.DictWriter(out,fieldnames=fields);writer.writeheader();writer.writerows(rows)
 return out.getvalue().encode()
class NormalizationTests(unittest.TestCase):
 def test_old_format(self):
  self.assertEqual(normalize_csv(content([BASE]),'2020-01-22')[0]['status'],'shipped')
 def test_sale_and_refund_survive(self):
  self.assertEqual(len(normalize_csv(content([dict(BASE,status='shipped'),dict(BASE,status='refunded')]),'2020-01-22')),2)
 def test_exact_duplicate(self):
  self.assertEqual(len(normalize_csv(content([BASE,BASE]),'2020-01-22')),1)
 def test_conflicting_duplicate(self):
  with self.assertRaises(ValueError): normalize_csv(content([BASE,dict(BASE,payment_amount='99')]),'2020-01-22')
 def test_wrong_date(self):
  with self.assertRaises(ValueError): normalize_csv(content([BASE]),'2020-01-23')
 def test_unknown_status(self):
  with self.assertRaises(ValueError): normalize_csv(content([dict(BASE,status='lost')]),'2020-01-22')
 def test_refund_amount_is_unsigned_in_staging(self):
  self.assertEqual(normalize_csv(content([dict(BASE,status='refunded',payment_amount='-100')]),'2020-01-22')[0]['payment_amount'],Decimal('100'))
 def test_real_order_key(self):
  self.assertEqual(normalize_csv(content([dict(BASE,order_id='order5')]),'2020-01-22')[0]['source_order_id'],'order5')
class ReportTests(unittest.TestCase):
 def test_repeated_id_is_ignored(self):
  days=normalize_report(content([dict(BASE,id='1'),dict(BASE,id='2')]))
  self.assertEqual(len(days['2020-01-22']),1)
 def test_missing_day_is_included(self):
  days=normalize_report(content([BASE,dict(BASE,uniq_id='event2',date_time='2020-01-24T00:00:00')]))
  self.assertEqual(list(days),['2020-01-22','2020-01-23','2020-01-24'])
  self.assertEqual(days['2020-01-23'],[])
 def test_conflict_fails_before_database(self):
  with self.assertRaises(ValueError):
   normalize_report(content([BASE,dict(BASE,payment_amount='999')]))
 def test_status_defaults_for_all_days(self):
  days=normalize_report(content([BASE,dict(BASE,uniq_id='event2',date_time='2020-01-23T00:00:00')]))
  self.assertTrue(all(row['status']=='shipped' for day in days.values() for row in day))
if __name__=='__main__': unittest.main()
