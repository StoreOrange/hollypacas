"""Price-editor authorization must depend on administrator role, not catalog permission."""
import json,re,unittest
from types import SimpleNamespace
from unittest.mock import patch
from fastapi import HTTPException
from starlette.requests import Request
from jinja2 import Environment,FileSystemLoader
from app.routers import web
from app.models.user import User,Role,Permission

class ShoePriceAdminTests(unittest.TestCase):
 def user(self,roles):
  return User(email='qa@example.invalid',full_name='QA',roles=[Role(name=name,permissions=[Permission(id=i+1,name='access.inventory.productos')]) for i,name in enumerate(roles)])
 def request(self):
  class Templates:
   def TemplateResponse(self,name,context):return {'name':name,'user':context['user']}
  return Request({'type':'http','method':'GET','path':'/inventory/shoes/prices','headers':[(b'accept',b'application/json')],'query_string':b'','app':SimpleNamespace(state=SimpleNamespace(templates=Templates()))})
 def test_non_admin_roles_cannot_open_or_save_even_with_product_permission(self):
  for role in ('vendedor','cajero','gerente','bodega'):
   with self.subTest(role=role),patch.object(web,'_is_shoes_mode',return_value=True):
    user=self.user([role]);self.assertTrue(web._has_permission(user,'access.inventory.productos'))
    with self.assertRaises(HTTPException) as error:web.inventory_shoes_prices_page(self.request(),user)
    self.assertEqual(error.exception.status_code,403)
    with self.assertRaises(HTTPException) as error:
     web.inventory_shoes_prices_apply(self.request(),product_ids='0',db=None,user=user,**{'precio_venta'+str(i):0 for i in range(1,8)})
    self.assertEqual(error.exception.status_code,403)
 def test_admin_role_keeps_access_with_or_without_additional_roles(self):
  for roles in (['administrador'],['gerente','administrador']):
   with self.subTest(roles=roles),patch.object(web,'_is_shoes_mode',return_value=True):
    result=web.inventory_shoes_prices_page(self.request(),self.user(roles));self.assertEqual(result['name'],'shoe_prices.html')
 def test_sidebar_hides_prices_from_non_admin_with_product_permission(self):
  env=Environment(loader=FileSystemLoader('app/templates'))
  menu=[next(item for item in web.SIDEBAR_MENU_ITEMS if item['id']=='shoe_prices')]
  req=SimpleNamespace(state=SimpleNamespace(has_permissions=True,permission_names={'access.inventory.productos'},branding={'company_key':'bdzapatos'},menu_links=menu))
  for role,visible in [('administrador',True),('cajero',False),('vendedor',False),('gerente',False)]:
   with self.subTest(role=role):
    html=env.get_template('partials/sidebar.html').render(request=req,user=self.user([role]),version='test')
    payload=re.search(r'<script id="sidebar-menu-data" type="application/json">(.*?)</script>',html,re.S).group(1)
    self.assertEqual(any(item['id']=='shoe_prices' for item in json.loads(payload)),visible)
