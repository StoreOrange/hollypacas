"""Cierre desde Firefox móvil con API/contabilidad en SQLite aislada, sin pagos reales."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from urllib.parse import urlparse,parse_qs
from datetime import timedelta
from starlette.datastructures import FormData
from playwright.sync_api import sync_playwright
from tests import test_motorizado_closures as fixture,test_pos_automation as common
from app.main import app
from app.routers import motorizado as router
from app.models.sales import AccountingEntry,ReciboCaja,CajaDiaria

case=fixture.MotoClosureTests();case.setUp();db=case.case.db
try:
 trip=case.confirmed();user=[case.case.seller];entry_count=db.query(AccountingEntry).count()
 def request(url):
  parsed=urlparse(url);req=common.AutoPosTests.request(case.case);req.scope.update(path=parsed.path,query_string=parsed.query.encode(),app=app,router=app.router,server=('127.0.0.1',8018),scheme='http',headers=[(b'host',b'127.0.0.1:8018')]);req.state.branding={'company_key':'hollywood_pacas'};req.state.menu_links=router.web.SIDEBAR_MENU_ITEMS;router.web._set_request_permissions(req,user[0]);return req
 with sync_playwright() as p:
  browser=p.firefox.launch(headless=True);page=browser.new_page(viewport={'width':390,'height':844});errors=[];page.on('pageerror',lambda e:errors.append(str(e)));page.on('dialog',lambda d:d.accept())
  def respond(route):
   req=request(route.request.url)
   if route.request.method=='POST':
    pairs=parse_qs(route.request.post_data or '',keep_blank_values=True);form=FormData([(key,value) for key,values in pairs.items() for value in values]);response=router.close_trips(req,form,db,user[0]);assert response.status_code==303 and 'ok=' in response.headers.get('location','');result=router.home(request('http://127.0.0.1:8018/motorizado'),db,user[0]);route.fulfill(status=200,content_type='text/html',body=result.body);return
   response=router.home(req,db,user[0]);route.fulfill(status=200,content_type='text/html',body=response.body)
  page.route('**/motorizado',respond);page.route('**/motorizado?*',respond);page.route('**/motorizado/cerrar',respond)
  page.goto('http://127.0.0.1:8018/motorizado')
  assert page.locator('.moto-trip').count()==1
  page.locator('.moto-pay-group summary').click();page.locator('.moto-pay-group input[name="paid"]').check()
  page.screenshot(path='/tmp/moto-payment-mobile.png',full_page=True)
  page.locator('.moto-pay-group button').click();page.wait_for_selector('.moto-empty');assert page.locator('h1').inner_text().strip()=='Motorizado'
  assert page.locator('.moto-trip').count()==0
  assert db.query(AccountingEntry).count()==entry_count and db.query(ReciboCaja).count()==0 and db.query(CajaDiaria).count()==0
  user[0]=case.case.user;page.goto('http://127.0.0.1:8018/motorizado');assert page.locator('.moto-trip').count()==0
  page.get_by_role('link',name='Reporte de cierres').click();page.wait_for_url('**view=cierres')
  assert 'Cierre #1' in page.locator('.moto-trip-id').inner_text().title()
  page.locator('.moto-history summary').click();assert 'Viaje #1' in page.locator('.moto-closure-lines').inner_text()
  assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth')
  page.screenshot(path='/tmp/moto-closures-mobile.png',full_page=True)
  closed_date=trip.closure.closure_date.isoformat();case.case.clock+=timedelta(days=2)
  page.goto('http://127.0.0.1:8018/motorizado?view=cierres');assert page.locator('.moto-trip').count()==0
  page.goto(f'http://127.0.0.1:8018/motorizado?view=cierres&from={closed_date}&to={closed_date}');assert page.locator('.moto-trip').count()==1
  page.goto('http://127.0.0.1:8018/motorizado?history=1');assert page.locator('.moto-cerrado').count()==1
  assert not errors,errors;browser.close()
finally:case.case.tearDown()
print('PASS Firefox móvil: vendedor confirma pago y cierra; desaparece de pendientes del motorizado, permanece en historial y reporte por fecha. Sin segundo gasto ni caja de ventas; SQLite aislada.')
