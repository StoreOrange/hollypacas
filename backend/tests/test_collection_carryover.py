"""Closed-day enrollment and independent remittance balances; no live database."""
import unittest
from datetime import date, datetime
from unittest.mock import patch
from tests import test_mobile_collections as fixtures
from app.core import collection_carryover as tracking, mobile_collections as general
from app.models.sales import CollectionCarryover, CollectionTrackingEpoch, CollectionPanelPayment
from app.routers import web as w

class CarryoverTests(unittest.TestCase):
 def setUp(self):
  self.f=fixtures.MobileCollectionTests();self.f.setUp();self.db=self.f.db
  self.db.query(CollectionCarryover).delete();self.db.query(CollectionTrackingEpoch).delete();self.db.query(w.CierreCaja).delete()
  for f in self.db.query(w.VentaFactura):f.created_at=datetime(2026,10,1,12)
  self.db.commit()
 def tearDown(self):self.f.tearDown()
 def close(self,id=10,warehouse=1,day=date(2026,10,29),at=datetime(2026,10,30,1)):
  close=w.CierreCaja(id=id,bodega_id=warehouse,branch_id=warehouse,fecha=day,created_at=at)
  self.db.add(close);self.db.flush();return close
 def start(self):
  result=tracking.initialize(self.db,self.f.admin,date(2026,10,30));self.db.commit();return result
 def test_no_close_no_debts_and_linked_seller_empty_panel_works(self):
  self.start()
  self.assertEqual(tracking.portfolio(self.db,self.f.seller),[])
  self.assertEqual(self.f.client.get('/m/deudas').status_code,200)
  self.assertEqual(self.f.client.get('/m/cartera/1').status_code,404)
 def test_same_day_close_waits_until_next_day_and_does_not_repeat(self):
  self.close(day=date(2026,10,30),at=datetime(2026,10,31,1));self.start()
  self.assertEqual(tracking.portfolio(self.db,self.f.seller),[])
  with patch.object(w,'local_today',return_value=date(2026,10,31)):
   self.assertEqual(len(tracking.portfolio(self.db,self.f.seller)),1)
  self.assertEqual(tracking.enroll_close(self.db,self.close(id=11,day=date(2026,10,31))),[])
  self.assertEqual(self.db.query(CollectionCarryover).count(),1)
  self.assertEqual(self.start()[1],[])
 def test_only_pending_at_close_in_its_warehouse_and_before_its_timestamp(self):
  f=self.db.get(w.VentaFactura,1);f.created_at=datetime(2026,10,30,2)
  self.close();self.start();self.assertEqual(self.db.query(CollectionCarryover).count(),0)
  f.created_at=datetime(2026,10,1);f.estado_cobranza='PAGADA';self.db.commit()
  self.assertEqual(tracking.enroll_close(self.db,self.close(id=11)),[])
  f.estado_cobranza='PENDIENTE';f.estado='ANULADA';self.db.flush()
  self.assertEqual(tracking.enroll_close(self.db,self.close(id=12)),[])
  f.estado='ACTIVA';self.db.flush()
  rows=tracking.enroll_close(self.db,self.close(id=13));self.db.commit()
  self.assertEqual([r.factura_id for r in rows],[1]);self.assertEqual(rows[0].eligible_on,date(2026,10,30))
 def test_new_close_hook_enrolls_only_after_activation(self):
  c=self.close();self.assertEqual(tracking.enroll_close(self.db,c),[])
  self.start();self.close(id=11,warehouse=2)
  self.assertEqual(len(tracking.enroll_close(self.db,self.db.get(w.CierreCaja,11))),1)
  self.db.commit();self.assertEqual(self.db.query(CollectionCarryover).count(),2)
 def test_opening_is_net_at_activation_old_payments_retained_but_not_imported(self):
  self.db.add(w.CobranzaAbono(factura_id=1,branch_id=1,bodega_id=1,secuencia=1,numero='OLD-10',fecha=date(2026,10,1),tipo_mov='ABONO',moneda='USD',monto_usd=10,monto_cs=370))
  self.close();self.start()
  record=self.db.query(CollectionCarryover).one()
  self.assertEqual(tracking.balance(record)['due'],90)
  self.assertEqual(tracking.balance(record)['paid'],0)
  self.assertEqual(self.db.query(CollectionPanelPayment).count(),0)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),1)
  self.assertNotIn('OLD-10',self.f.client.get('/m/cartera?view=history').text)
  record.factura.estado_cobranza='PAGADA';self.db.commit()
  self.assertEqual(self.f.post(values=self.f.values(amount='90')).status_code,303)
  self.assertEqual(self.f.panel_balance()['due'],0)
  self.assertEqual(general.balance(record.factura)['due'],90)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),1)
 def test_frozen_vendor_scope_and_item_details_in_both_views(self):
  self.close();self.start();f=self.db.get(w.VentaFactura,1);f.vendedor_id=2
  product=w.Producto(cod_producto='TEST-25',descripcion='Calcetas 25 libras')
  self.db.add(product);self.db.flush()
  self.db.add(w.VentaItem(factura_id=1,producto_id=product.id,cantidad=2,peso_lbs=25,precio_unitario_usd=50,subtotal_usd=100));self.db.commit()
  for url in ('/m/cartera/1','/m/deudas?client=1'):
   response=self.f.client.get(url);self.assertEqual(response.status_code,200)
   self.assertIn('TEST-25',response.text);self.assertIn('Calcetas 25 libras',response.text);self.assertIn('Cantidad:',response.text)
  self.f.user=self.f.other;self.assertEqual(self.f.client.get('/m/cartera/1').status_code,404)
  self.assertEqual(self.f.post().status_code,404)
  self.f.user=self.f.admin;self.assertEqual(self.f.client.get('/m/cartera/1').status_code,200)
