"""Financial and authorization regressions, isolated SQLite only."""
import io
import unittest
from datetime import date, datetime
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from PIL import Image
from app.database import Base
from app.core import mobile_collections as s
from app.routers import mobile_collections as route, web as w
from app.models.sales import MobileCollectionPayment

class MobileCollectionTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite://',connect_args={'check_same_thread':False},poolclass=StaticPool)
  Base.metadata.create_all(self.engine);self.db=Session(self.engine)
  self.db.add_all([w.Branch(id=1,code='central',name='Central'),w.Branch(id=2,code='esteli',name='Esteli'),w.Bodega(id=1,branch_id=1,code='central',name='Central'),w.Bodega(id=2,branch_id=2,code='esteli',name='Esteli'),w.Vendedor(id=1,nombre='Carlos'),w.Vendedor(id=2,nombre='Otra persona'),w.Cliente(id=1,nombre='Ana'),w.ExchangeRate(effective_date=date(2026,1,1),period='DIA',rate=37)])
  self.admin=w.User(id=1,email='admin@qa.invalid',hashed_password='qa',roles=[w.Role(name='administrador')])
  self.seller=w.User(id=2,email='seller@qa.invalid',hashed_password='qa',vendedor_id=1,roles=[w.Role(name='vendedor')])
  self.other=w.User(id=3,email='other@qa.invalid',hashed_password='qa',vendedor_id=2,roles=self.seller.roles)
  self.db.add_all([self.admin,self.seller,self.other]);self.db.flush()
  for id,vendor,warehouse in [(1,1,1),(2,2,2)]:
   self.db.add(w.VentaFactura(id=id,numero='F-'+str(id),vendedor_id=vendor,bodega_id=warehouse,cliente_id=1,moneda='USD',total_usd=100,total_cs=3700,fecha=datetime(2026,10,1),estado='ACTIVA',condicion_venta='CONTADO',estado_cobranza='PENDIENTE'))
  self.db.commit();self.user=self.seller
  self.patches=[patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'_allowed_branch_codes',return_value={'central','esteli'}),patch.object(w,'local_today',return_value=date(2026,10,30))]
  for p in self.patches:p.start()
  app=FastAPI();app.include_router(route.router);app.state.templates=Jinja2Templates(directory=str(__import__('pathlib').Path(__file__).resolve().parents[1]/'app/templates'))
  app.dependency_overrides[w.get_db]=lambda:self.db;app.dependency_overrides[w._require_user_web]=lambda:self.user
  self.client=TestClient(app)
 def tearDown(self):
  self.client.close()
  for p in reversed(self.patches):p.stop()
  self.db.close();self.engine.dispose()
 def png(self):
  b=io.BytesIO();Image.new('RGB',(40,30),'blue').save(b,'PNG');return b.getvalue()
 def values(self,id=1,**changes):
  data=dict(token=s.form_token(self.user,id),date='2026-10-30',currency='USD',amount='20.00',method='deposito',reference='REF123',bank='Banco QA',note='Pago parcial');data.update(changes);return data
 def post(self,id=1,values=None,photo=True):
  return self.client.post(f'/m/cartera/{id}/pagos',data=values or self.values(id),files={'support':('prueba.png',self.png(),'image/png')} if photo else None,follow_redirects=False)
 def test_scope_on_portfolio_detail_write_and_image(self):
  html=self.client.get('/m/cartera').text;self.assertIn('F-1',html);self.assertNotIn('F-2',html)
  self.assertEqual(self.client.get('/m/cartera/2').status_code,404);self.assertEqual(self.post(2).status_code,404)
  self.assertEqual(self.post().status_code,303);payment=self.db.query(MobileCollectionPayment).one();self.user=self.other
  self.assertEqual(self.client.get(f'/m/cartera/soportes/{payment.id}').status_code,404)
  self.user=self.admin;self.assertEqual(self.client.get(f'/m/cartera/soportes/{payment.id}').status_code,200)
  self.assertIn('F-2',self.client.get('/m/cartera').text)
 def test_abono_and_private_jpeg_support(self):
  self.assertEqual(self.post().status_code,303);abono=self.db.query(w.CobranzaAbono).one()
  self.assertEqual(abono.monto_usd,20);self.assertEqual(abono.monto_cs,740);self.assertFalse(abono.afecta_caja)
  self.assertEqual(s.balance(self.db.get(w.VentaFactura,1))['due'],80)
  payment=self.db.query(MobileCollectionPayment).one();response=self.client.get(f'/m/cartera/soportes/{payment.id}')
  self.assertIn('no-store',response.headers['cache-control']);self.assertEqual(Image.open(io.BytesIO(response.content)).format,'JPEG')
  self.assertIn('REF123',self.client.get('/m/cartera?view=history').text)
 def test_replay_is_idempotent_even_after_full_payment(self):
  values=self.values(amount='100.00');self.assertEqual(self.post(values=values).status_code,303);self.assertEqual(self.post(values=values).status_code,303)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),1);self.assertEqual(self.db.get(w.VentaFactura,1).estado_cobranza,'PAGADA')
  self.assertEqual(self.post().status_code,400)
 def test_duplicate_reference_partial_payment_is_rejected(self):
  self.assertEqual(self.post().status_code,303);self.assertEqual(self.post().status_code,400);self.assertEqual(self.db.query(w.CobranzaAbono).count(),1)
 def test_initial_payments_credit_and_debit_notes(self):
  self.db.add(w.VentaPago(factura_id=1,forma_pago_id=1,monto_usd=20,monto_cs=740))
  for i,(kind,value) in enumerate([('ABONO',10),('NOTA_CREDITO',5),('NOTA_DEBITO',2)],1):
   self.db.add(w.CobranzaAbono(factura_id=1,branch_id=1,bodega_id=1,secuencia=i,numero='AB'+str(i),fecha=date(2026,10,2),tipo_mov=kind,moneda='USD',monto_usd=value,monto_cs=value*37))
  self.db.commit();self.db.expire_all();f=self.db.get(w.VentaFactura,1);self.assertEqual(s.balance(f)['due'],67)
  self.assertEqual(self.post(values=self.values(amount='68')).status_code,400);self.assertEqual(self.post(values=self.values(amount='67')).status_code,303)
  self.assertEqual(f.estado_cobranza,'PAGADA')
 def test_bad_inputs_and_missing_support_make_no_payments(self):
  for changes in [{'amount':'NaN'},{'amount':'-1'},{'amount':'1.001'},{'amount':'101'},{'date':'2026-11-01'},{'date':'2026-09-30'},{'date':'bad'},{'currency':'XXX'},{'method':'fake'},{'reference':''}]:
   with self.subTest(changes=changes):self.assertEqual(self.post(values=self.values(**changes)).status_code,400)
  self.assertEqual(self.post(photo=False).status_code,400)
  response=self.client.post('/m/cartera/1/pagos',data=self.values(),files={'support':('x.png',b'<html>not a photo</html>','image/png')})
  self.assertEqual(response.status_code,400);self.assertEqual(self.db.query(w.CobranzaAbono).count(),0);self.assertEqual(self.db.query(MobileCollectionPayment).count(),0)
 def test_wrong_company_unlinked_user_and_csrf(self):
  with patch.object(w,'_is_pacasholl_company',return_value=False):self.assertEqual(self.client.get('/m/cartera').status_code,404)
  self.assertEqual(self.post(values=self.values(token='bad')).status_code,400)
  self.assertEqual(self.client.post('/m/cartera/1/pagos',data=self.values(),headers={'Origin':'https://other.invalid'}).status_code,403)
  self.user=w.User(id=9,email='unlinked@qa.invalid',roles=[w.Role(name='vendedor')]);self.assertEqual(self.client.get('/m/cartera').status_code,403)
 def test_cross_currency_limits_and_cash(self):
  self.assertEqual(self.post(values=self.values(currency='CS',amount='3700.01')).status_code,400)
  self.assertEqual(self.post(values=self.values(currency='CS',amount='3700')).status_code,303);self.assertEqual(s.balance(self.db.get(w.VentaFactura,1))['due'],0)
  self.user=self.other;self.assertEqual(self.post(2,values=self.values(2,method='efectivo',reference='',bank=''),photo=False).status_code,303)
 def test_annulled_invoice_and_missing_rate(self):
  f=self.db.get(w.VentaFactura,1);f.estado='ANULADA';self.db.commit();self.assertEqual(self.post().status_code,400)
  f.estado='ACTIVA';self.db.query(w.ExchangeRate).delete();self.db.commit();self.assertEqual(self.post().status_code,400)
 def test_failure_rolls_back_payment_and_invoice_state(self):
  with patch.object(self.db,'commit',side_effect=RuntimeError('disk failure')):
   with self.assertRaises(RuntimeError):self.post(values=self.values(amount='100.00'))
  self.db.rollback();self.assertEqual(self.db.query(w.CobranzaAbono).count(),0);self.assertEqual(self.db.get(w.VentaFactura,1).estado_cobranza,'PENDIENTE')

 def test_vendor_filters_and_reference_search(self):
  self.assertNotIn('F-2',self.client.get('/m/cartera?vendor=2').text)
  self.assertEqual(self.post().status_code,303)
  self.assertIn('REF123',self.client.get('/m/cartera?view=history&q=REF123').text)
  self.user=self.admin
  html=self.client.get('/m/cartera?vendor=2').text
  self.assertIn('F-2',html);self.assertNotIn('href="/m/cartera/1"',html)
 def test_cordoba_invoice_and_unassigned_admin_filter(self):
  f=self.db.get(w.VentaFactura,1);f.moneda='CS';self.db.commit()
  self.assertEqual(self.post(values=self.values(currency='CS',amount='500')).status_code,303)
  self.assertEqual(s.balance(f)['due'],3200)
  f.vendedor_id=None;self.db.commit();self.user=self.admin
  html=self.client.get('/m/cartera?vendor=0').text
  self.assertIn('F-1',html);self.assertNotIn('href="/m/cartera/2"',html)

 def test_legacy_delete_preserves_mobile_support_audit(self):
  from starlette.requests import Request
  self.assertEqual(self.post().status_code,303)
  payment=self.db.query(MobileCollectionPayment).one()
  with patch.object(w,'_resolve_branch_bodega',return_value=(None,self.db.get(w.Bodega,1))):
   response=w.sales_cobranza_abono_delete(venta_id=1,abono_id=payment.abono_id,request=Request({'type':'http','headers':[]}),db=self.db,user=self.admin)
  self.assertEqual(response.status_code,409)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),1)

 def test_mobile_menu_is_visible_to_sellers_without_broad_payment_permissions(self):
  import json,re
  from types import SimpleNamespace
  from jinja2 import Environment,FileSystemLoader
  from pathlib import Path
  env=Environment(loader=FileSystemLoader(str(Path(__file__).resolve().parents[1]/'app/templates')))
  menu=[item for item in w.SIDEBAR_MENU_ITEMS if item['id'] in {'mobile_collections','collection_review'}]
  request=SimpleNamespace(state=SimpleNamespace(has_permissions=True,permission_names=set(),branding={'company_key':'hollywood_pacas'},menu_links=menu))
  for user,expected in [(self.seller,True),(self.admin,True),(w.User(email='bodega@qa.invalid',roles=[w.Role(name='bodega')]),False),(w.User(email='unlinked@qa.invalid',roles=[w.Role(name='vendedor')]),False),(w.User(email='moto@qa.invalid',vendedor_id=1,roles=[w.Role(name='motorizado')]),False)]:
   html=env.get_template('partials/sidebar.html').render(request=request,user=user,version='test')
   data=json.loads(re.search(r'<script id="sidebar-menu-data" type="application/json">(.*?)</script>',html,re.S).group(1))
   self.assertEqual(bool(data),expected)
   if data:self.assertFalse(data[0]['mobile_only'])
  request.state.branding={'company_key':'bdzapatos'}
  html=env.get_template('partials/sidebar.html').render(request=request,user=self.admin,version='test')
  data=json.loads(re.search(r'<script id="sidebar-menu-data" type="application/json">(.*?)</script>',html,re.S).group(1))
  self.assertEqual(data,[])

 def test_new_collection_menu_is_near_preventas_in_existing_saved_order(self):
  old_order=[item['id'] for item in w.SIDEBAR_MENU_ITEMS if item['id']!='mobile_collections']
  result=w._normalize_sidebar_menu_order(old_order)
  self.assertEqual(result.index('mobile_collections'),result.index('sales_preventas_mobile')+1)
 def test_pending_cash_tender_is_not_a_received_payment_and_basis_survives_settlement(self):
  f=self.db.get(w.VentaFactura,1)
  initial=w.VentaPago(factura_id=1,forma_pago_id=3,moneda='USD',monto_usd=100,monto_cs=3700)
  self.db.add(initial);self.db.commit()
  self.assertEqual(s.balance(f)['due'],100)
  self.assertIn('F-1',self.client.get('/m/cartera').text)
  self.assertEqual(self.post().status_code,303)
  self.db.expire_all();self.assertEqual(s.balance(f)['due'],80)
  self.assertEqual(s.balance(f)['paid'],20)
  self.assertEqual(self.post(values=self.values(amount='80.00',reference='SECOND')).status_code,303)
  self.db.expire_all()
  self.assertEqual(f.estado_cobranza,'PAGADA')
  self.assertEqual(s.balance(f)['due'],0)
  self.assertEqual(s.balance(f)['paid'],100)
  self.assertEqual(initial.monto_usd,100)
  self.assertNotIn('Pago inicial de factura',self.client.get('/m/cartera/1').text)
 def test_paid_cash_invoice_is_not_reopened(self):
  f=self.db.get(w.VentaFactura,1);f.estado_cobranza='PAGADA'
  self.db.add(w.VentaPago(factura_id=1,forma_pago_id=3,moneda='USD',monto_usd=100,monto_cs=3700));self.db.commit()
  self.assertEqual(s.balance(f)['due'],0)
  self.assertEqual(self.post().status_code,400)
 def test_failed_pending_cash_collection_rolls_back_deferred_basis(self):
  from app.models.sales import MobileCollectionBasis
  self.db.add(w.VentaPago(factura_id=1,forma_pago_id=3,moneda='USD',monto_usd=100,monto_cs=3700));self.db.commit()
  with patch.object(self.db,'commit',side_effect=RuntimeError('disk failure')):
   with self.assertRaises(RuntimeError):self.post()
  self.db.rollback()
  self.assertEqual(self.db.query(MobileCollectionBasis).count(),0)
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),0)
  self.assertEqual(self.db.get(w.VentaFactura,1).estado_cobranza,'PENDIENTE')

 def test_new_pending_invoices_appear_before_older_debts(self):
  self.db.add(w.VentaFactura(id=3,numero='RECENT-3',vendedor_id=1,bodega_id=1,cliente_id=1,moneda='USD',total_usd=25,total_cs=925,fecha=datetime(2026,10,29),estado='ACTIVA',condicion_venta='CONTADO',estado_cobranza='PENDIENTE'))
  self.db.commit()
  html=self.client.get('/m/cartera').text
  self.assertLess(html.index('RECENT-3'),html.index('F-1'))

 def test_review_panel_respects_ownership_and_administrator_scope(self):
  page=self.client.get('/m/deudas?vendor=2')
  self.assertEqual(page.status_code,200)
  self.assertIn('Revisión de deudas',page.text)
  self.assertIn('F-1',page.text);self.assertNotIn('F-2',page.text)
  self.user=self.admin
  page=self.client.get('/m/deudas')
  self.assertIn('F-1',page.text);self.assertIn('F-2',page.text)
  self.assertIn('Cartera pendiente por vendedor',page.text)
 def test_owner_closure_preserves_exceptions_cash_and_sales(self):
  from app.core.collection_closure import close_pending
  self.db.add(w.VentaPago(factura_id=1,forma_pago_id=3,moneda='USD',monto_usd=100,monto_cs=3700))
  self.db.add(w.VentaFactura(id=3,numero='VOID-3',vendedor_id=1,bodega_id=1,moneda='USD',total_usd=200,total_cs=7400,fecha=datetime(2026,10,2),estado='ANULADA',estado_cobranza='PENDIENTE'))
  self.db.commit()
  result=close_pending(self.db,self.admin,{2},'TEST-CIERRE')
  self.db.commit()
  self.assertEqual(result['closed'],2);self.assertEqual(result['adjustments'],1)
  self.assertEqual(result['remaining'],[2])
  note=self.db.query(w.CobranzaAbono).one();self.assertEqual(note.tipo_mov,'NOTA_CREDITO');self.assertFalse(note.afecta_caja)
  self.assertEqual(self.db.get(w.VentaFactura,1).estado,'ACTIVA')
  self.assertEqual(self.db.get(w.VentaFactura,3).estado,'ANULADA')
  self.assertEqual(self.db.get(w.VentaFactura,2).estado_cobranza,'PENDIENTE')
  self.assertEqual(s.balance(self.db.get(w.VentaFactura,1))['due'],0)
  self.assertEqual(self.db.query(w.VentaPago).one().monto_usd,100)
  self.assertEqual(close_pending(self.db,self.admin,{2},'TEST-CIERRE')['closed'],0)
 def test_closure_rejects_seller_and_invalid_exceptions(self):
  from app.core.collection_closure import close_pending
  from fastapi import HTTPException
  with self.assertRaises(HTTPException):close_pending(self.db,self.seller,set(),'TEST')
  with self.assertRaises(ValueError):close_pending(self.db,self.admin,{999},'TEST')
  self.assertEqual(self.db.query(w.CobranzaAbono).count(),0)
