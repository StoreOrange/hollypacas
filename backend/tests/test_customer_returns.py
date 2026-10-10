import asyncio
import json
import unittest
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.datastructures import FormData
from app.database import Base
from app.routers import web
from app.models.user import Branch, User, Role
from app.models.sales import AccountingPolicySetting, Cliente, VentaFactura, VentaItem, VentaPago, FormaPago, Vendedor, AperturaCaja, ReciboCaja
from app.models.inventory import Bodega, Producto, ColorCatalog, ShoeProductVariant, ShoeVariantStock, IngresoTipo, IngresoInventario, IngresoItem, ExchangeRate
from app.models.returns import CustomerReturn
from app.core.customer_returns import create_return, consume_credits

class CustomerReturnTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
  self.now=datetime(2026,10,10,10,30)
  self.db.add_all([Branch(id=i,code=c,name=c.upper()) for i,c in [(1,'central'),(2,'kg'),(3,'kgf')]])
  self.db.add_all([Bodega(id=i,branch_id=i,code=str(i),name='Bodega '+str(i)) for i in [1,2,3]])
  self.db.add_all([Cliente(id=1,nombre='Ana Pérez'),Cliente(id=2,nombre='Otra persona'),Cliente(id=3,nombre='Consumidor final'),Vendedor(id=1,nombre='Vendedor'),FormaPago(id=1,nombre='Efectivo'),FormaPago(id=2,nombre='Anticipo')])
  self.db.add(Producto(id=1,cod_producto='SAND',descripcion='Sandalia elegante',precio_venta1=300,costo_producto=100,activo=True))
  self.db.add(ColorCatalog(id=1,nombre='Negro',abreviatura='NEG'))
  self.db.add(ShoeProductVariant(id=1,producto_id=1,color_id=1,talla='38',cod_variante='SAND-NEG-38',activo=True))
  self.db.add(ShoeVariantStock(variante_id=1,bodega_id=1,existencia=10))
  self.db.add(IngresoTipo(id=1,nombre='Apertura'));self.db.add(IngresoInventario(id=1,tipo_id=1,bodega_id=1,fecha=self.now.date(),moneda='CS'))
  self.db.add(IngresoItem(ingreso_id=1,producto_id=1,variante_id=1,cantidad=12))
  self.db.add(VentaFactura(id=1,numero='A-000001',secuencia=1,bodega_id=1,cliente_id=1,vendedor_id=1,fecha=self.now,moneda='CS',condicion_venta='CONTADO',estado='ACTIVA',estado_cobranza='PAGADA',total_cs=450,total_usd=Decimal('450')/37,descuento_global_porcentaje=10))
  self.db.add(VentaItem(id=1,factura_id=1,producto_id=1,variante_id=1,cantidad=2,precio_unitario_cs=250,subtotal_cs=500))
  self.db.add(AccountingPolicySetting(auto_entry_enabled=False));self.db.add(ExchangeRate(effective_date=self.now.date(),period='DIA',rate=37));self.db.add(AperturaCaja(branch_id=1,bodega_id=1,fecha=self.now.date(),monto_cs=0,usuario_registro="Caja prueba"));self.db.commit()
  self.user=User(full_name='Caja prueba',email='test',roles=[Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
  self.patches=[patch.object(web,'get_active_company_key',return_value='bdzapatos'),patch.object(web,'local_today',return_value=self.now.date()),patch.object(web,'local_now_naive',return_value=self.now)]
  for p in self.patches:p.start()
 def tearDown(self):
  for p in self.patches:p.stop()
  self.db.close();self.engine.dispose()
 def request(self,data=None):
  class Templates:
   def TemplateResponse(self,name,context):return context
  class State:templates=Templates()
  class App:state=State()
  req=Request({'type':'http','method':'POST','path':'/sales/devoluciones','headers':[],'query_string':b'','app':App()});req._form=FormData(data or {});return req
 def endpoint(self,path,method='GET'):return next(r.endpoint for r in web.router.routes if r.path==path and method in r.methods)
 def create(self,quantity='1',kind='CANJE',key='a'*32):
  row=create_return(self.db,item_id=1,quantity=quantity,kind=kind,reason='Cambio de talla',customer_name='Ana Pérez',operation_key=key,warehouse=self.db.get(Bodega,1),allowed_ids={1,2,3},actor='Caja prueba',now=self.now,rate=Decimal('37'));self.db.commit();return row
 def invoice(self,total=300,customer=1):
  inv=VentaFactura(numero='A-000002',secuencia=2,bodega_id=1,cliente_id=customer,fecha=self.now,moneda='CS',condicion_venta='CONTADO',estado='ACTIVA',total_cs=total);self.db.add(inv);self.db.flush();return inv
 def consume(self,inv,credits,amounts=None,extra=0):
  amounts=amounts or [r.monto_cs for r in credits];payments=[VentaPago(forma_pago_id=2,moneda='CS',monto_cs=a,monto_original=a) for a in amounts]
  if extra:payments.append(VentaPago(forma_pago_id=1,moneda='CS',monto_cs=extra,monto_original=extra))
  return consume_credits(self.db,payments=payments,credit_ids=[str(c.id) for c in credits],invoice=inv,allowed_ids={1,2,3},now=self.now,actor='Caja prueba')
 def test_discount_inventory_and_idempotency(self):
  row=self.create();self.assertEqual(row.monto_cs,225);self.assertEqual(row.estado,'ANTICIPO DISPONIBLE')
  self.assertEqual(self.db.query(ShoeVariantStock).one().existencia,11);self.assertEqual(web._balances_by_bodega(self.db,[1],[1])[(1,1)],11)
  self.assertEqual(self.create().id,row.id);self.assertEqual(self.db.query(ShoeVariantStock).one().existencia,11);self.assertEqual(self.db.query(CustomerReturn).count(),1);self.assertEqual(self.db.query(ReciboCaja).count(),0)
 def test_cash_return_egress_and_no_credit(self):
  row=self.create(kind='DINERO');self.assertEqual(row.estado,'DINERO ENTREGADO')
  receipt=self.db.query(ReciboCaja).one();self.assertEqual(receipt.tipo,'EGRESO');self.assertEqual(receipt.monto_cs,225);self.assertTrue(receipt.afecta_caja)
  with self.assertRaises(ValueError):self.consume(self.invoice(225),[row])
 def test_invalid_quantity_oversold_and_unpaid(self):
  for qty in ['0','-1','3','0.5','NaN','Infinity','bad']:
   with self.subTest(qty=qty),self.assertRaises(ValueError):self.create(quantity=qty)
   self.db.rollback()
  self.create();self.create(key='b'*32)
  with self.assertRaises(ValueError):self.create(key='c'*32)
  self.db.rollback();self.db.get(VentaFactura,1).estado_cobranza='PENDIENTE';self.db.commit()
  with self.assertRaises(ValueError):self.create(key='d'*32)
 def test_multiple_credits_full_redemption_and_no_reuse(self):
  a=self.create();b=self.create(key='b'*32);inv=self.invoice(500);self.assertEqual(len(self.consume(inv,[a,b],extra=50)),2);self.db.commit();self.assertEqual(a.factura_canje_id,inv.id)
  with self.assertRaises(ValueError):self.consume(inv,[a])
 def test_partial_duplicate_wrong_customer_lower_total_and_cashout(self):
  row=self.create();inv=self.invoice(225)
  for amounts,credits,extra in [([100],[row],125),([225,225],[row,row],0),([225],[row],1)]:
   with self.assertRaises(ValueError):self.consume(inv,credits,amounts,extra)
  inv.cliente_id=2
  with self.assertRaises(ValueError):self.consume(inv,[row])
  inv.cliente_id=1;inv.total_cs=224
  with self.assertRaises(ValueError):self.consume(inv,[row])
  inv.total_cs=225;inv.moneda='USD'
  with self.assertRaises(ValueError):self.consume(inv,[row])
  self.assertIsNone(row.factura_canje_id)
 def test_rounding_residue(self):
  item=self.db.get(VentaItem,1);item.cantidad=3;item.subtotal_cs=100;self.db.get(VentaFactura,1).total_cs=100;self.db.commit()
  values=[self.create(key=key*32).monto_cs for key in ['a','b','c']];self.assertEqual(values,[Decimal('33.33'),Decimal('33.33'),Decimal('33.34')]);self.assertEqual(sum(values),100)
 def test_search_exact_intelligent_and_store_scope(self):
  endpoint=self.endpoint('/sales/devoluciones/facturas')
  def search(**kw):
   args=dict(numero='',cliente='',fecha='',producto='');args.update(kw);return json.loads(endpoint(self.request(),db=self.db,user=self.user,**args).body)
  self.assertEqual(search(numero='A-00000')['items'],[]);self.assertEqual(len(search(numero='a-000001',producto='sandalia negro 38')['items']),1);self.assertEqual(search(numero='A-000001',producto='rojo')['items'],[]);self.assertEqual(len(search(numero='A-000001',producto='sandalia negró')['items']),1);self.assertEqual(len(search(numero='A-000001',producto='sandlia')['items']),1)
  self.create();self.assertEqual(search(numero='A-000001')['items'][0]['disponible'],1)
  self.user.roles=[Role(name='cajero')];self.user.default_branch_id=2;self.user.default_bodega_id=2;self.assertEqual(search(numero='A-000001')['items'],[])
 def test_checkout_atomic_and_cash_closing_excludes_credit(self):
  row=self.create();data=[('operation_key','c'*32),('cliente_id','1'),('vendedor_id','1'),('fecha',self.now.date().isoformat()),('moneda','CS'),('item_producto_id','1'),('item_variante_id','1'),('item_cantidad','1'),('item_precio','300'),('pago_forma_id','2'),('pago_moneda','CS'),('pago_monto','225'),('pago_banco_id',''),('pago_cuenta_id',''),('pago_devolucion_id',str(row.id)),('pago_forma_id','1'),('pago_moneda','CS'),('pago_monto','75'),('pago_banco_id',''),('pago_cuenta_id',''),('pago_devolucion_id','')]
  result=asyncio.run(web.sales_create_invoice(self.request(data),self.db,self.user));self.assertIn('print_id=',result.headers['location'],result.headers['location'])
  self.db.refresh(row);self.assertIsNotNone(row.factura_canje_id);self.assertEqual(self.db.query(VentaPago).count(),2)
  again=asyncio.run(web.sales_create_invoice(self.request(data),self.db,self.user));self.assertIn('ya+registrada',again.headers['location'])
  preview=web.sales_cierre(self.request(),self.db,self.user);self.assertAlmostEqual(preview['total_calculado_usd']*37,525,places=2);self.assertEqual(self.db.query(ShoeVariantStock).one().existencia,10)
 def test_cannot_reverse_return_or_redemption_invoice(self):
  row=self.create();inv=self.invoice(225);self.consume(inv,[row]);self.db.commit()
  for ident in [1,inv.id]:
   result=asyncio.run(web.sales_reversion_confirm(ident,self.request({'motivo':'Prueba'}),self.db,self.user));self.assertEqual(result.status_code,400);self.assertIn('devoluciones',json.loads(result.body)['message'])
 def test_money_return_affects_home_and_close(self):
  from app.core.shoe_home import snapshot
  self.create(kind='DINERO');data=snapshot(self.db,[self.db.get(Bodega,1)],self.now.date());self.assertEqual(data['totals']['sales'],'225.00');self.assertEqual(data['totals']['cash_cs'],'-225.00')
  preview=web.sales_cierre(self.request(),self.db,self.user);self.assertAlmostEqual(preview['total_calculado_usd']*37,225,places=2)
 def test_return_and_redemption_accounting(self):
  from app.core.init_db import _seed_cuentas_contables,_seed_accounting_voucher_types
  from app.models.sales import AccountingPolicySetting,AccountingEntry,CuentaContable
  _seed_cuentas_contables(self.db);_seed_accounting_voucher_types(self.db)
  self.db.query(AccountingPolicySetting).one().auto_entry_enabled=True;self.db.commit()
  row=self.create();web._post_return_accounting(self.db,row);self.db.commit()
  entry=self.db.query(AccountingEntry).filter_by(referencia='AUTO-DEV-1').one()
  liability=self.db.query(CuentaContable).filter_by(codigo='2198').one()
  self.assertEqual(entry.total_debe,325);self.assertEqual(entry.total_haber,325)
  self.assertEqual(next(line.haber for line in entry.lines if line.cuenta_id==liability.id),225)
  self.assertEqual(sum(line.debe for line in entry.lines),sum(line.haber for line in entry.lines))
  invoice=self.invoice(300)
  payments=[VentaPago(forma_pago_id=2,monto_cs=225),VentaPago(forma_pago_id=1,monto_cs=75)]
  entries=web._build_sale_accounting_entries(self.db,factura=invoice,branch_id=1,entry_date=self.now.date(),sale_amount_cs=Decimal('300'),cost_amount_cs=Decimal('100'),payments=payments)
  sale=next(e for e in entries if e.referencia.startswith('AUTO-VTA'))
  self.assertEqual(next(line.debe for line in sale.lines if line.cuenta_id==liability.id),225)
  self.assertEqual(sum(line.debe for line in sale.lines),300)
  web._post_return_accounting(self.db,row);self.db.commit();self.assertEqual(self.db.query(AccountingEntry).filter_by(referencia="AUTO-DEV-1").count(),1)

 def test_submit_routes_history_and_cash_only_no_credit(self):
  submit=self.endpoint('/sales/devoluciones','POST')
  data={'operation_key':'d'*32,'item_id':'1','cantidad':'1','tipo':'DINERO','motivo':'No le ajusta','cliente_nombre':'Ana Pérez'}
  response=asyncio.run(submit(self.request(data),self.db,self.user))
  self.assertEqual(response.status_code,200,response.body)
  result=json.loads(response.body);self.assertEqual(result['devolucion']['estado'],'DINERO ENTREGADO')
  history=self.endpoint('/sales/devoluciones/historial')
  rows=json.loads(history(self.request(),q='',estado='DINERO',desde='',hasta='',page=1,db=self.db,user=self.user).body)['items']
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['recibo'],'DEV-000001')
  credits=self.endpoint('/sales/devoluciones/anticipos')
  with patch.object(web,'_enforce_permission'):
   self.assertEqual(json.loads(credits(self.request(),q='Ana',cliente_id='',db=self.db,user=self.user).body)['items'],[])

 def test_redemption_checkout_failure_rolls_back_everything(self):
  row=self.create()
  data=[('operation_key','f'*32),('cliente_id','2'),('vendedor_id','1'),('fecha',self.now.date().isoformat()),('moneda','CS'),('item_producto_id','1'),('item_variante_id','1'),('item_cantidad','1'),('item_precio','300'),('pago_forma_id','2'),('pago_moneda','CS'),('pago_monto','225'),('pago_devolucion_id',str(row.id)),('pago_forma_id','1'),('pago_moneda','CS'),('pago_monto','75'),('pago_devolucion_id','')]
  result=asyncio.run(web.sales_create_invoice(self.request(data),self.db,self.user));self.assertIn('otro+cliente',result.headers['location'])
  self.assertEqual(self.db.query(VentaFactura).count(),1);self.assertEqual(self.db.query(ShoeVariantStock).one().existencia,11);self.assertIsNone(self.db.get(CustomerReturn,row.id).factura_canje_id)

 def test_generic_customer_requires_name_and_gets_own_credit(self):
  self.db.get(VentaFactura,1).cliente_id=3;self.db.commit()
  kwargs=dict(item_id=1,quantity=1,kind='CANJE',reason='Talla',operation_key='e'*32,warehouse=self.db.get(Bodega,1),allowed_ids={1},actor='Caja',now=self.now,rate=Decimal('37'))
  with self.assertRaises(ValueError):create_return(self.db,customer_name='',**kwargs)
  row=create_return(self.db,customer_name='María Gómez',**kwargs);self.db.commit()
  self.assertEqual(row.cliente.nombre,'María Gómez');self.assertNotEqual(row.cliente_id,3)

 def test_credit_cannot_be_converted_to_cash_by_returning_new_purchase(self):
  self.db.add(VentaPago(factura_id=1,forma_pago_id=2,moneda='CS',monto_cs=450,monto_original=450));self.db.commit()
  with self.assertRaises(ValueError):self.create(kind='DINERO')
  self.db.rollback();self.assertEqual(self.create(kind='CANJE').monto_cs,225)

 def test_anticipo_payment_requires_real_credit_reference(self):
  inv=self.invoice(225)
  payment=VentaPago(forma_pago_id=2,moneda='CS',monto_cs=225,monto_original=225)
  with self.assertRaises(ValueError):consume_credits(self.db,payments=[payment],credit_ids=[],invoice=inv,allowed_ids={1},now=self.now,actor='Caja')
 def test_invoice_search_omits_zeroes_but_keeps_whole_number(self):
  endpoint=self.endpoint('/sales/devoluciones/facturas')
  for number in ['1','0001','A-1','a1',' A - 01 ','A-000001']:
   data=json.loads(endpoint(self.request(),numero=number,cliente='',fecha='',producto='',db=self.db,user=self.user).body)
   self.assertEqual(len(data['items']),1,number)
  for number in ['11','B-1','A-10']:
   data=json.loads(endpoint(self.request(),numero=number,cliente='',fecha='',producto='',db=self.db,user=self.user).body)
   self.assertEqual(data['items'],[],number)

 def test_history_defaults_to_30_days_and_can_show_older_dates(self):
  from datetime import timedelta
  self.db.get(VentaItem,1).cantidad=3;self.db.commit()
  recent=self.create();boundary=self.create(key='b'*32);older=self.create(key='c'*32)
  boundary.created_at=self.now-timedelta(days=29);older.created_at=self.now-timedelta(days=30);self.db.commit()
  endpoint=self.endpoint('/sales/devoluciones/historial')
  def history(start='',end=''):
   return json.loads(endpoint(self.request(),q='',estado='',desde=start,hasta=end,page=1,db=self.db,user=self.user).body)
  self.assertEqual(history()['total'],2)
  self.assertEqual(history((self.now-timedelta(days=60)).date().isoformat(),self.now.date().isoformat())['total'],3)
  self.assertFalse(history('2026-10-11','2026-10-10')['ok'])

 def test_anticipo_modal_only_lists_uncanjed_exchange_returns_even_if_old(self):
  from datetime import timedelta
  used=self.create();pending=self.create(key='b'*32)
  pending.created_at=self.now-timedelta(days=60);self.db.commit()
  invoice=self.invoice(225);self.consume(invoice,[used]);self.db.commit()
  endpoint=self.endpoint('/sales/devoluciones/anticipos')
  result=json.loads(endpoint(self.request(),q='A-1',cliente_id='',db=self.db,user=self.user).body)
  self.assertEqual([r['id'] for r in result['items']],[pending.id])
