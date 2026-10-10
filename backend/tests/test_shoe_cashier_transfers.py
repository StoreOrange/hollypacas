import unittest
from unittest.mock import patch
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from fastapi import HTTPException
from app.database import Base
from app.routers import web
from app.models.user import User,Role,Branch
from app.models.inventory import Bodega
from app.models.sales import VentaFactura

class ShoeCashierTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
  self.db.add_all([Branch(id=1,code='central',name='Central'),Branch(id=2,code='kg',name='KG'),Branch(id=3,code='kgf',name='KGF')])
  self.db.add_all([Bodega(id=i,branch_id=i,code=code,name=code.upper()) for i,code in [(1,'central'),(2,'kg'),(3,'kgf')]])
  self.db.commit();self.user=User(email='test',roles=[Role(name='cajero')],default_branch_id=3,default_bodega_id=3)
 def tearDown(self):self.db.close();self.engine.dispose()
 def test_cashier_permission_only_shoes_and_only_transfers(self):
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   self.assertTrue(web._has_permission(self.user,'access.inventory.traslados'))
   self.assertTrue(web._has_permission(self.user,'menu.inventory.traslados'))
   self.assertFalse(web._has_permission(self.user,'access.inventory.egresos'))
  with patch.object(web,'get_active_company_key',return_value='hollywood_pacas'):
   self.assertFalse(web._has_permission(self.user,'access.inventory.traslados'))
 def test_defaults_and_sidebar(self):
  class Templates:
   def TemplateResponse(self,name,context):return context
  class State:templates=Templates()
  class App:state=State()
  request=Request({'type':'http','method':'GET','path':'/inventory/traslados-rapidos','headers':[],'query_string':b'','app':App()})
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   data=web.inventory_quick_transfers_page(request,self.db,self.user)
   self.assertEqual(data['default_origen_id'],3);self.assertEqual(data['default_destino_id'],1)
   self.assertEqual([b.id for b in data['origin_bodegas']],[3])
   link=next(i for i in web.get_sidebar_menu_layout(self.db) if i['id']=='inventory_traslados')
   self.assertEqual(link['perm'],'menu.inventory.traslados')
 def test_search_cannot_use_another_origin(self):
  request=Request({'type':'http','method':'GET','path':'/inventory/traslados-rapidos/search','headers':[],'query_string':b''})
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   with self.assertRaises(HTTPException) as error:web.inventory_quick_transfers_search(request,q='test',bodega_id=1,db=self.db,user=self.user)
   self.assertEqual(error.exception.status_code,403)
 def test_kgf_avoids_legacy_central_invoice_collision(self):
  self.db.add(VentaFactura(numero='C-000001',secuencia=1,bodega_id=1,fecha=datetime.now(),moneda='CS',estado='ACTIVA'))
  self.db.commit()
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   self.assertEqual(web._next_shoe_invoice_number(self.db,'kgf',3),(2,'C-000002'))
   self.assertEqual(web._next_shoe_invoice_number(self.db,'central',1),(2,'A-000002'))

 def test_admin_utilities_include_all_company_stores(self):
  admin=User(email='admin-test',roles=[Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
  admin.branches=[self.db.get(Branch,1)]
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   self.assertTrue(web._shoe_admin_all_stores(admin))
   self.assertEqual(web._utility_branch_ids(self.db,admin),{1,2,3})
   self.assertFalse(web._shoe_admin_all_stores(self.user))
   self.assertEqual(web._utility_branch_ids(self.db,self.user),{3})
 def test_admin_cross_store_access_is_only_shoes(self):
  admin=User(email='admin-test',roles=[Role(name='administrador')],default_branch_id=1)
  with patch.object(web,'get_active_company_key',return_value='hollywood_pacas'):
   self.assertFalse(web._shoe_admin_all_stores(admin))

 def test_reversions_only_central_even_for_admin(self):
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   self.assertFalse(web._can_request_shoe_reversion(self.db,self.user))
   self.user.default_branch_id=1;self.user.default_bodega_id=1
   self.assertTrue(web._can_request_shoe_reversion(self.db,self.user))
   self.user.default_branch_id=3;self.user.default_bodega_id=3
   self.user.roles=[Role(name='administrador')]
   self.assertFalse(web._can_request_shoe_reversion(self.db,self.user))
   self.user.default_branch_id=1;self.user.default_bodega_id=1
   self.assertTrue(web._can_request_shoe_reversion(self.db,self.user))

 def test_shoe_notifications_release_database_while_waiting(self):
  import asyncio
  from unittest.mock import AsyncMock,Mock
  request=Mock(query_params={})
  request.is_disconnected=AsyncMock(return_value=False)
  self.user.roles=[Role(name='administrador')]
  async def verify():
   with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
    response=await web.sales_preventas_notifications_stream(request,self.db,self.user)
    self.assertFalse(self.db.in_transaction())
    ready=await response.body_iterator.__anext__()
    self.assertIn('event: ready',ready)
    ping=await response.body_iterator.__anext__()
    self.assertIn('event: ping',ping)
    self.assertFalse(self.db.in_transaction())
    await response.body_iterator.aclose()
  asyncio.run(verify())

 def test_shoe_start_page_is_sales_only_in_shoes(self):
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   self.assertEqual(web.root().headers['location'],'/sales')
  with patch.object(web,'get_active_company_key',return_value='hollywood_pacas'):
   self.assertEqual(web.root().headers['location'],'/home')
 def test_shoe_login_redirects_to_sales(self):
  from unittest.mock import Mock
  user=User(email='login-test',full_name='Login test',hashed_password='test',is_active=True)
  db=Mock();db.query.return_value.filter.return_value.first.return_value=user
  request=Request({'type':'http','method':'POST','path':'/login','headers':[],'query_string':b'','scheme':'http','server':('localhost',8001)})
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'),patch.object(web,'verify_password',return_value=True):
   result=web.login_action(request,username='login-test',password='test',remember=None,db=db)
   self.assertEqual(result.headers['location'],'/sales')
   self.assertIn('access_token=',result.headers['set-cookie'])

 def test_transfer_search_accepts_parent_product_code_without_picking_a_size(self):
  import json
  from app.models.inventory import Producto,ColorCatalog,ShoeProductVariant,ShoeVariantStock
  self.db.add(Producto(id=900,cod_producto='PECHA03',descripcion='Sandalia FOREVER',activo=True))
  self.db.add(ColorCatalog(id=900,nombre='BLACK',abreviatura='BLK'))
  for ident,size in [(900,'6'),(901,'7')]:
   self.db.add(ShoeProductVariant(id=ident,producto_id=900,color_id=900,talla=size,cod_variante='PECHA03-BLK-'+size,activo=True))
   self.db.add(ShoeVariantStock(variante_id=ident,bodega_id=1,existencia=2))
  self.db.commit()
  request=Request({'type':'http','method':'GET','path':'/inventory/traslados-rapidos/search','headers':[],'query_string':b''})
  with patch.object(web,'_is_shoes_mode',return_value=True),patch.object(web,'_enforce_quick_transfer'),patch.object(web,'_is_shoe_cashier',return_value=False):
   data=json.loads(web.inventory_quick_transfers_search(request,q='PECHA03',bodega_id='1',color=None,talla=None,limit=120,exact=True,db=self.db,user=self.user).body)
   self.assertEqual({item['variant_id'] for item in data['items']},{900,901})
   data=json.loads(web.inventory_quick_transfers_search(request,q='PECHA03-BLK-7',bodega_id='1',color=None,talla=None,limit=120,exact=True,db=self.db,user=self.user).body)
   self.assertEqual([item['variant_id'] for item in data['items']],[901])

 def test_central_cashier_can_choose_all_origins(self):
  from unittest.mock import Mock
  self.user.default_branch_id=1;self.user.default_bodega_id=1
  request=Request({'type':'http','method':'GET','path':'/inventory/traslados-rapidos','headers':[],'query_string':b'','app':Mock()})
  request.app.state.templates.TemplateResponse.side_effect=lambda name,context:context
  with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
   data=web.inventory_quick_transfers_page(request,self.db,self.user)
   self.assertEqual({b.id for b in data['origin_bodegas']},{1,2,3})
   for origin in ['1','2','3']:
    response=web.inventory_quick_transfers_search(request,q='test',bodega_id=origin,color=None,talla=None,limit=120,exact=True,db=self.db,user=self.user)
    self.assertEqual(response.status_code,200)

 def test_central_cannot_register_same_warehouse_transfer(self):
  import asyncio
  from unittest.mock import AsyncMock,Mock
  from starlette.datastructures import FormData
  from app.models.inventory import EgresoTipo,EgresoInventario
  self.user.default_branch_id=1;self.user.default_bodega_id=1
  self.db.add(EgresoTipo(id=901,nombre='Traslado entre bodegas'));self.db.commit()
  for origin in ['1','2','3']:
   request=Mock()
   request.form=AsyncMock(return_value=FormData({'tipo_id':'901','bodega_id':origin,'bodega_destino_id':origin,'fecha':'2026-10-10','moneda':'CS','item_producto_id':'900','redirect_to':'/inventory/traslados-rapidos'}))
   with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
    response=asyncio.run(web.inventory_create_egreso(request,self.db,self.user))
   self.assertIn('La+bodega+destino+debe+ser+distinta+al+origen',response.headers['location'])
  self.assertEqual(self.db.query(EgresoInventario).count(),0)
