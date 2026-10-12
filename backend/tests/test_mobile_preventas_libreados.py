import asyncio,json,unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.datastructures import FormData
from app.database import Base
from app.routers import web
from app.models.user import Branch,Role,User
from app.models.sales import Cliente,Vendedor,Preventa,PreventaItem
from app.models.inventory import Bodega,Producto,IngresoInventario,IngresoTipo,IngresoItem,ExchangeRate

class LibreadoMobileTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
  self.db.add_all([Branch(id=1,code='central',name='Central'),Bodega(id=1,branch_id=1,code='central',name='Central'),Cliente(id=1,nombre='Cliente'),Vendedor(id=1,nombre='Ejecutivo'),Producto(id=1,cod_producto='LIB',descripcion='Ropa por libra',es_libreado=True,es_por_peso=True,precio_venta1_usd=2,precio_venta1=74),IngresoTipo(id=1,nombre='Compra'),IngresoInventario(id=1,tipo_id=1,bodega_id=1,fecha=date(2026,10,11),moneda='USD'),IngresoItem(ingreso_id=1,producto_id=1,cantidad=2),ExchangeRate(effective_date=date(2026,10,11),period='DIA',rate=37)])
  self.db.commit();self.user=User(email='test',full_name='Caja',roles=[Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
  self.patches=[patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_enforce_preventas_mobile_access'),patch.object(web,'local_today',return_value=date(2026,10,11))]
  for p in self.patches:p.start()
 def tearDown(self):
  for p in self.patches:p.stop()
  self.db.close();self.engine.dispose()
 def request(self,weight='12.5',qty='1'):
  req=Request({'type':'http','method':'POST','path':'/m/preventas','headers':[],'query_string':b''})
  req._form=FormData({'cliente_id':'1','vendedor_id':'1','moneda':'USD','fecha':'2026-10-11','item_producto_id':'1','item_cantidad':qty,'item_peso_lbs':weight,'item_precio_usd':'2','item_precio_cs':'74'})
  return req
 def test_weight_is_saved_and_billed_with_one_inventory_bulto(self):
  result=asyncio.run(web.mobile_preventas_create(self.request(),self.db,self.user))
  self.assertIn('result_preventa=',result.headers['location'])
  row=self.db.query(PreventaItem).one();self.assertEqual(row.cantidad,1);self.assertEqual(row.peso_lbs,Decimal('12.50'));self.assertEqual(row.subtotal_usd,25);self.assertEqual(row.subtotal_cs,925)
  pre=self.db.query(Preventa).one();self.assertEqual(pre.total_items,1);self.assertEqual(pre.total_usd,25)
 def test_invalid_or_missing_weight_cannot_create_preventa(self):
  for weight in ['', '0','-1','NaN','Infinity','invalid','0.001']:
   with self.subTest(weight=weight):
    result=asyncio.run(web.mobile_preventas_create(self.request(weight),self.db,self.user));self.assertIn('error=',result.headers['location']);self.assertEqual(self.db.query(Preventa).count(),0)
 def test_search_returns_libreado_flag_and_reserves_units(self):
  data=json.loads(web.mobile_preventas_products_search(self.request(),q='LIB',db=self.db,user=self.user).body)
  self.assertTrue(data['items'][0]['es_libreado']);self.assertEqual(data['items'][0]['existencia'],2)
  asyncio.run(web.mobile_preventas_create(self.request(),self.db,self.user));row=self.db.query(PreventaItem).one()
  self.assertEqual(web._preventa_required_qty_map([(row,self.db.get(Producto,1))])[1],1)
 def test_regular_products_still_bill_by_quantity(self):
  self.db.get(Producto,1).es_libreado=False;self.db.commit()
  asyncio.run(web.mobile_preventas_create(self.request(weight='100',qty='2'),self.db,self.user));row=self.db.query(PreventaItem).one()
  self.assertIsNone(row.peso_lbs);self.assertEqual(row.cantidad,2);self.assertEqual(row.subtotal_usd,4)

 def test_preventa_can_be_invoiced_with_its_weight(self):
  asyncio.run(web.mobile_preventas_create(self.request(),self.db,self.user))
  pre=self.db.query(Preventa).one()
  req=self.request();data=dict(req._form);data.update(operation_key='a'*32,preventa_id=str(pre.id),item_precio='2',condicion_venta='CREDITO')
  req._form=FormData(data)
  with patch.object(web,'_enforce_permission'),patch.object(web,'_is_shoes_mode',return_value=False):
   result=asyncio.run(web.sales_create_invoice(req,self.db,self.user))
  self.assertNotIn('error=',result.headers['location'],result.headers['location'])
  item=self.db.query(web.VentaItem).one();self.assertEqual(item.peso_lbs,Decimal('12.50'));self.assertEqual(item.cantidad,1);self.assertEqual(item.subtotal_usd,25)
  self.assertEqual(pre.estado,'FACTURADA')
