import unittest
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from fastapi import HTTPException
from app.database import Base
from app.routers import web
from app.models.sales import MenuLayoutSetting
from app.models.user import User,Role

class ShoePricePanelTests(unittest.TestCase):
 def test_other_companies_cannot_open_panel(self):
  req=Request({'type':'http','method':'GET','path':'/inventory/shoes/prices','headers':[]})
  with patch.object(web,'_is_shoes_mode',return_value=False),self.assertRaises(HTTPException) as result:
   web.inventory_shoes_prices_page(req,User(email='test'))
  self.assertEqual(result.exception.status_code,404)
 def test_prices_require_existing_product_permission(self):
  req=Request({'type':'http','method':'GET','path':'/inventory/shoes/prices','headers':[(b'accept',b'application/json')],'query_string':b''})
  user=User(email='test@example.invalid',roles=[Role(name='vendedor')])
  with patch.object(web,'_is_shoes_mode',return_value=True),patch.object(web,'_send_permission_sms_alert'),self.assertRaises(HTTPException) as result:
   web.inventory_shoes_prices_page(req,user)
  self.assertEqual(result.exception.status_code,403)
 def test_saved_menu_order_still_places_prices_immediately_after_sales(self):
  engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
  try:
   with Session(engine) as db:
    db.add(MenuLayoutSetting(company_key='bdzapatos',menu_order_json='["home","inventory","sales","reports","shoe_prices"]'));db.commit()
    with patch.object(web,'get_active_company_key',return_value='bdzapatos'):
     menu=web.get_sidebar_menu_layout(db)
    ids=[item['id'] for item in menu]
    self.assertEqual(ids[ids.index('sales')+1],'shoe_prices')
    entry=next(item for item in menu if item['id']=='shoe_prices')
    self.assertEqual(entry['perm'],'access.inventory.productos');self.assertTrue(entry['shoes_only'])
  finally:engine.dispose()
