"""QA Firefox de los roles con datos/contabilidad aislados en SQLite. Sin pagos reales."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from urllib.parse import parse_qs
from email.parser import BytesParser
from email.policy import default
from io import BytesIO
from PIL import Image
from playwright.sync_api import sync_playwright
from tests.test_motorizado import MotoTests
from tests import test_pos_automation as fixture
from app.main import app
from app.routers import motorizado as router
from app.core import motorizado as service

case=MotoTests();case.setUp()
try:
 from app.core import init_db
 from unittest.mock import patch
 with patch.object(init_db,'get_active_company_key',return_value='hollywood_pacas'):init_db._seed_role_permissions(case.db)
 pending=case.create();delivered=case.create();canceled=case.create()
 service.transition(case.db,case.user,delivered.id,'ENTREGADO');case.db.commit()
 service.transition(case.db,case.seller,canceled.id,'ANULADO','Cliente canceló la entrega',True);case.db.commit()
 def render(user,view):
  request=fixture.AutoPosTests.request(case);request.scope.update(path='/motorizado',query_string=('view='+view+'&history=1').encode(),app=app,router=app.router,server=('127.0.0.1',8018),scheme='http',headers=[(b'host',b'127.0.0.1:8018')])
  request.state.branding={'company_key':'hollywood_pacas'};request.state.menu_links=fixture.web.SIDEBAR_MENU_ITEMS
  fixture.web._set_request_permissions(request,user)
  return router.home(request,case.db,user).body.decode()
 rider_html=render(case.user,'viajes');seller_html=render(case.seller,'costos')
finally:case.tearDown()
with sync_playwright() as p:
 browser=p.firefox.launch(headless=True);page=browser.new_page(viewport={'width':390,'height':844});errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
 current=[rider_html];posts=[]
 def respond(route):
  if route.request.method=='POST':
   content_type=route.request.headers.get('content-type','')
   if 'multipart/form-data' in content_type:
    message=BytesParser(policy=default).parsebytes(('Content-Type: '+content_type+'\r\n\r\n').encode()+(route.request.post_data_buffer or b''));fields={}
    for part in message.iter_parts():
     name=part.get_param('name',header='content-disposition')
     if not part.get_filename():fields.setdefault(name,[]).append(part.get_payload(decode=True).decode())
    posts.append(fields)
   else:posts.append(parse_qs(route.request.post_data or ''))
   route.fulfill(status=200,content_type='text/html',body='<p>Registro simulado</p>');return
  route.fulfill(status=200,content_type='text/html',body=current[0])
 page.route('**/motorizado',respond);page.route('**/motorizado?*',respond);page.route('**/motorizado/*/estado',respond)
 page.goto('http://127.0.0.1:8018/motorizado')
 assert page.locator('.moto-trip').count()==3
 assert page.locator('#mobileSidebar').count()==1
 import json
 assert any(item['id']=='motorizado' for item in json.loads(page.locator('#sidebar-menu-data').inner_text()))
 assert page.locator('.moto-entregado').count()==1 and page.locator('.moto-pendiente').count()==1 and page.locator('.moto-anulado').count()==1
 assert page.locator('#moto-results button:has-text("Confirmar recepción")').count()==0
 assert page.locator('.moto-cancel').count()==0
 assert page.locator('#moto-summary .moto-stats').count()==1
 assert page.locator('#moto-seller-search').count()==0
 assert page.locator('[data-moto-seller]').count()==2
 assert 'Ver proceso' not in page.locator('#moto-results').inner_text()
 assert 'Contabilizado' not in page.locator('#moto-results').inner_text()
 assert page.locator('a:has-text("Historial de viajes y vendedores")').count()==1
 assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth')
 page.screenshot(path='/tmp/motorizado-rider-mobile.png',full_page=True)
 page.locator('#moto-new').click();page.locator('.select2-container--open .select2-search__field').fill('Vendedor');page.get_by_role('option',name='Vendedor',exact=True).click();page.locator('#moto-destination').select_option('OTRO');assert page.locator('#moto-other').is_visible()
 page.locator('#moto-other').fill('Nagarote, parque central');page.locator('#moto-amount').fill('1250.50');page.locator('#moto-comments').click();assert page.locator('#moto-amount').input_value()=='1,250.50'
 out=BytesIO();Image.new('RGB',(100,80),'green').save(out,format='PNG')
 page.locator('#moto-new-photos').set_input_files({'name':'recibo-bus.png','mimeType':'image/png','buffer':out.getvalue()})
 assert page.locator('#moto-form [data-photo-preview] img').count()==1
 page.screenshot(path='/tmp/motorizado-register-mobile.png',full_page=True)
 page.locator('#moto-form button').click();page.wait_for_function('document.body.textContent.includes("Registro simulado")');assert len(posts)==1 and posts[0]['amount']==['1,250.50'] and posts[0]['csrf']
 current[0]=seller_html;page.goto('http://127.0.0.1:8018/motorizado?view=costos')
 assert page.locator('#moto-new').count()==0 and page.locator('.moto-history-groups').count()==1 and page.locator('.moto-history-group').count()==1
 assert page.locator('button:has-text("Confirmar recepción")').count()==1
 assert page.locator('.moto-cancel').count()==1
 assert page.locator('.moto-evidence').count()==3
 assert page.locator('form[action$="/fotos"]').count()==3
 assert page.locator('.moto-history').count()==3
 page.screenshot(path='/tmp/motorizado-seller-mobile.png',full_page=True)
 page.set_viewport_size({'width':1440,'height':1000});page.screenshot(path='/tmp/motorizado-seller-desktop.png',full_page=True)
 assert not errors,errors;browser.close()
print('PASS Firefox: motorizado/vendedor, registro móvil, moneda, roles, estados, historial y diseño sin desbordamiento. APIs de pago simuladas; datos contables en SQLite aislada.')
