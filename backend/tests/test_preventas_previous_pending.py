import unittest
from datetime import datetime,date,timedelta
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web
from app.models.sales import Preventa,Cliente,Vendedor
from app.models.inventory import Bodega
from app.models.user import Branch,User,Role

class PreviousPreventaTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
  self.today=date(2026,10,11)
  self.db.add(Branch(id=1,code='central',name='Central'));self.db.add(Bodega(id=1,branch_id=1,code='central',name='Central'))
  self.db.add(Vendedor(id=1,nombre='Ejecutivo'));self.db.add(Cliente(id=1,nombre='Cliente'))
  for ident,days,state,linked in [(1,0,'PENDIENTE',None),(2,1,'PENDIENTE',None),(3,3,'REVISION',None),(4,2,'FACTURADA',1),(5,1,'ANULADA',None),(6,2,'PENDIENTE',1)]:
   self.db.add(Preventa(id=ident,numero='PV-'+str(ident),branch_id=1,bodega_id=1,vendedor_id=1,cliente_id=1,fecha=datetime.combine(self.today-timedelta(days=days),datetime.min.time()),estado=state,venta_factura_id=linked))
  self.db.commit();self.user=User(email='test',roles=[Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
 def tearDown(self):self.db.close();self.engine.dispose()
 def request(self,query=b''):
  class Templates:
   def TemplateResponse(self,name,context):return context
  class State:templates=Templates()
  class App:state=State()
  return Request({'type':'http','method':'GET','path':'/sales/preventas','headers':[],'query_string':query,'app':App()})
 def test_today_panel_includes_only_uninvoiced_previous_pending(self):
  with patch.object(web,'local_today',return_value=self.today),patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_repair_preventa_currency_if_needed',return_value=False):
   data=web.sales_preventas_panel(self.request(),self.db,self.user)
   self.assertEqual({r['id'] for r in data['rows']},{1,2,3})
   ages={r['id']:r['pending_age'] for r in data['rows']}
   self.assertEqual(ages[2],'Pendiente de ayer');self.assertEqual(ages[3],'Pendiente hace 3 días');self.assertEqual(ages[1],'')
   data=web.sales_preventas_panel(self.request(b'fecha=2026-10-10'),self.db,self.user)
   self.assertEqual({r['id'] for r in data['rows']},{2,5})
 def test_other_companies_keep_date_filter(self):
  with patch.object(web,'local_today',return_value=self.today),patch.object(web,'_is_pacasholl_company',return_value=False),patch.object(web,'_repair_preventa_currency_if_needed',return_value=False):
   data=web.sales_preventas_panel(self.request(),self.db,self.user)
   self.assertEqual({r['id'] for r in data['rows']},{1})
 def test_mobile_shows_previous_pending_before_today(self):
  with patch.object(web,'local_today',return_value=self.today),patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_enforce_preventas_mobile_access'):
   data=web.mobile_preventas_page(self.request(),self.db,self.user)
   self.assertEqual([r['id'] for r in data['recent_preventas']],[3,2,1])
   self.assertEqual(data['recent_preventas'][1]['pending_age'],'Pendiente de ayer')
