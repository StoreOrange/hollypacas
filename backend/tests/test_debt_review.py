"""Vendor/customer review and configurable alerts; isolated SQLite only."""
import unittest
from datetime import date,datetime
from decimal import Decimal
from unittest.mock import patch
from tests import test_mobile_collections as fixtures
from app.routers import web as w
from app.core import mobile_collections as service
from app.models.sales import CollectionReviewSetting

class DebtReviewTests(unittest.TestCase):
 def setUp(self):
  self.f=fixtures.MobileCollectionTests();self.f.setUp();self.db=self.f.db;self.client=self.f.client
 def tearDown(self):self.f.tearDown()
 def extra(self,date_value=datetime(2026,9,30)):
  self.db.add(w.VentaFactura(id=3,numero='EXTRA-3',vendedor_id=1,bodega_id=1,cliente_id=1,moneda='USD',total_usd=50,total_cs=1850,fecha=date_value,estado='ACTIVA',estado_cobranza='PENDIENTE'));self.db.commit()
 def configuration(self,**changes):
  data=dict(token=service.form_token(self.f.user,-1),overdue_days='30',recent_days='7',recent_color='green',aged_color='red');data.update(changes);return data
 def test_client_groups_multiple_invoices_and_owns_only_its_vendor(self):
  self.extra();self.assertEqual(self.f.post().status_code,303)
  response=self.client.get('/m/deudas?client=1&vendor=2')
  self.assertEqual(response.status_code,200);self.assertEqual(response.context['count'],2)
  self.assertEqual(response.context['stats']['due_usd'],Decimal('130'))
  self.assertEqual(response.context['stats']['received_usd'],Decimal('20'))
  self.assertIn('F-1',response.text);self.assertIn('EXTRA-3',response.text);self.assertNotIn('F-2',response.text)
  self.assertIn('Factura F-1',response.text)
  self.assertEqual(self.client.get('/m/deudas?client=999').status_code,404)
  self.assertEqual(self.client.get('/m/deudas?client=1&invoice=2').status_code,404)
 def test_admin_starts_with_vendors_and_drills_into_client(self):
  self.extra();self.f.user=self.f.admin
  response=self.client.get('/m/deudas')
  self.assertEqual(response.context['level'],'vendors');self.assertEqual(response.context['count'],2)
  self.assertIn('vendor=1',response.text)
  response=self.client.get('/m/deudas?vendor=1')
  self.assertEqual(response.context['level'],'clients');self.assertEqual(response.context['count'],1)
  response=self.client.get('/m/deudas?vendor=1&client=1')
  self.assertEqual(response.context['level'],'invoices');self.assertEqual(response.context['count'],2)
  self.assertNotIn('F-2',response.text)
 def test_recent_real_abonos_exclude_credit_adjustments_and_initial_tenders(self):
  self.assertEqual(self.f.post().status_code,303)
  self.db.add(w.CobranzaAbono(factura_id=1,branch_id=1,bodega_id=1,secuencia=20,numero='CIERRE-FAKE',fecha=date(2026,10,30),tipo_mov='NOTA_CREDITO',moneda='USD',monto_usd=5,monto_cs=185,observacion='Cierre administrativo de cartera'))
  self.db.add(w.CobranzaAbono(factura_id=1,branch_id=1,bodega_id=1,secuencia=21,numero='OLD-PAY',fecha=date(2026,10,1),tipo_mov='ABONO',moneda='USD',monto_usd=5,monto_cs=185));self.db.commit()
  response=self.client.get('/m/deudas?view=recent')
  self.assertEqual(response.context['count'],1);self.assertEqual(response.context['stats']['recent_usd'],20)
  self.assertNotIn('CIERRE-FAKE',response.text);self.assertNotIn('OLD-PAY',response.text)
  self.assertIn('Factura F-1',response.text);self.assertIn('is-recent',response.text)
 def test_aged_uses_issue_date_and_partial_payment_does_not_reset_age(self):
  self.extra();response=self.client.get('/m/deudas?view=aged')
  self.assertEqual(response.context['count'],1);self.assertIn('EXTRA-3',response.text)
  self.assertEqual(self.f.post().status_code,303)
  self.f.user=self.f.admin
  response=self.client.post('/m/deudas/configuracion',data=self.configuration(overdue_days='20'),follow_redirects=False)
  self.assertEqual(response.status_code,303)
  response=self.client.get('/m/deudas?vendor=1&view=aged')
  self.assertEqual(response.context['count'],2)
  self.assertTrue(all(row['aged'] for row in response.context['items']))
  self.assertIn('has-aged',response.text)
 def test_seller_cannot_change_settings_admin_can_change_without_altering_debt(self):
  before=service.balance(self.db.get(w.VentaFactura,1))['due']
  self.assertEqual(self.client.post('/m/deudas/configuracion',data=self.configuration()).status_code,403)
  self.f.user=self.f.admin
  response=self.client.post('/m/deudas/configuracion',data=self.configuration(overdue_days='15',recent_days='3',recent_color='blue',aged_color='amber'),follow_redirects=False)
  self.assertEqual(response.status_code,303)
  setting=self.db.get(CollectionReviewSetting,1)
  self.assertEqual((setting.overdue_days,setting.recent_days,setting.recent_color,setting.aged_color),(15,3,'blue','amber'))
  self.assertEqual(setting.updated_by,self.f.admin.id)
  self.assertEqual(service.balance(self.db.get(w.VentaFactura,1))['due'],before)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),0)
  self.f.user=self.f.seller;response=self.client.get('/m/deudas')
  self.assertEqual(response.context['config']['aged_color'],'amber');self.assertNotIn('action="/m/deudas/configuracion"',response.text)
 def test_configuration_rejects_invalid_days_colors_tokens_and_origin(self):
  self.f.user=self.f.admin
  for changes in [{'overdue_days':'0'},{'overdue_days':'3651'},{'recent_days':'abc'},{'recent_days':'-5'},{'recent_color':'red;display:none'},{'token':'bad'}]:
   with self.subTest(changes=changes):self.assertEqual(self.client.post('/m/deudas/configuracion',data=self.configuration(**changes)).status_code,400)
  self.assertEqual(self.db.query(CollectionReviewSetting).count(),0)
  self.assertEqual(self.client.post('/m/deudas/configuracion',data=self.configuration(),headers={'Origin':'https://other.invalid'}).status_code,403)
 def test_company_and_unlinked_access_stays_restricted(self):
  with patch.object(w,'_is_pacasholl_company',return_value=False):self.assertEqual(self.client.get('/m/deudas').status_code,404)
  self.f.user=w.User(id=50,email='no@qa.invalid',roles=[w.Role(name='vendedor')])
  self.assertEqual(self.client.get('/m/deudas').status_code,403)
