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
