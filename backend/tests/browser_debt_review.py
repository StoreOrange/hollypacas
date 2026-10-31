"""Responsive one-click review with fake invoices/payments in isolated SQLite."""
import sys
from pathlib import Path
from datetime import datetime
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests import test_mobile_collections as fixtures
from app.routers import web as w
f=fixtures.MobileCollectionTests();f.setUp()
try:
 f.db.add(w.VentaFactura(id=3,numero='EXTRA-3',vendedor_id=1,bodega_id=1,cliente_id=1,moneda='USD',total_usd=50,total_cs=1850,fecha=datetime(2026,9,30),estado='ACTIVA',estado_cobranza='PENDIENTE'));f.db.commit()
 assert f.post().status_code==303
 f.user=f.admin
 with sync_playwright() as p:
  browser=p.firefox.launch(headless=True);ctx=browser.new_context(viewport={'width':390,'height':844});errors=[]
  def dispatch(route):
   req=route.request;url=urlsplit(req.url)
   if url.path.startswith('/static/'):
    file=Path(__file__).resolve().parents[1]/'app'/url.path.lstrip('/')
    if file.is_file():return route.fulfill(path=str(file))
   if url.path.startswith('/m/'):
    response=f.client.request(req.method,url.path+('?' + url.query if url.query else ''),content=req.post_data_buffer,headers={key:value for key,value in req.headers.items() if key in {'content-type','origin'}},follow_redirects=False)
    if response.status_code==303:response=f.client.get(response.headers['location'])
    return route.fulfill(status=response.status_code,headers=dict(response.headers),body=response.content)
   return route.fulfill(content_type='application/json',body='{}')
  ctx.route('**/*',dispatch);page=ctx.new_page();page.on('pageerror',lambda err:errors.append(str(err)))
  page.goto('https://testserver/m/deudas')
  assert page.locator('.dr-vendor').count()==2
  assert page.locator('.dr-vendor.has-aged').count()==1
  page.screenshot(path='/tmp/holl-review-simple-vendors.png',full_page=True)
  page.locator('.dr-vendor[href*="vendor=1"]').click();page.wait_for_load_state('load')
  assert page.locator('.dr-client').count()==1
  assert '20.00' in page.locator('.dr-client').inner_text()
  page.locator('.dr-client').click();page.wait_for_load_state('load')
  assert page.locator('.dr-invoice').count()==2
  assert 'F-2' not in page.locator('.dr-panel').inner_text()
  page.locator('#dr-invoice-1 > summary').click()
  assert page.locator('#dr-invoice-1 .dr-payment.is-recent').is_visible()
  assert page.locator('#dr-invoice-1 .dr-payment').evaluate('(e)=>getComputedStyle(e).color')=='rgb(21, 128, 61)'
  page.screenshot(path='/tmp/holl-review-simple-client.png',full_page=True)
  page.locator('#dr-invoice-1 [data-support]').click();page.locator('.mc-lightbox').wait_for(state='visible');page.keyboard.press('Escape')
  page.goto('https://testserver/m/deudas?view=recent')
  assert page.locator('.dr-payment.is-recent').count()==1
  assert 'Factura F-1' in page.locator('.dr-payment').inner_text()
  page.goto('https://testserver/m/deudas?view=aged')
  assert page.locator('.dr-aged-invoice.has-aged').count()==1
  assert page.locator('.dr-aged-invoice').evaluate('(e)=>getComputedStyle(e).borderLeftColor')=='rgb(185, 28, 28)'
  page.screenshot(path='/tmp/holl-review-simple-aged.png',full_page=True)
  page.goto('https://testserver/m/deudas')
  page.locator('.dr-settings > summary').click()
  page.locator('#dr-aged-days').fill('20');page.locator('#dr-recent-color').select_option('blue');page.locator('#dr-aged-color').select_option('amber')
  page.locator('.dr-settings [type=submit]').click();page.get_by_role('status').filter(has_text='Alertas y colores actualizados').wait_for()
  assert page.locator('.dr-panel').get_attribute('style').find('#1d4ed8')>=0
  f.user=f.seller;page.goto('https://testserver/m/deudas')
  assert page.locator('.dr-settings').count()==0
  assert page.locator('.dr-client').count()==1
  page.locator('.dr-client').click();page.wait_for_load_state('load')
  assert page.locator('.dr-invoice').count()==2
  for width in (320,390,430,768):
   page.set_viewport_size({'width':width,'height':844})
   assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
  assert not errors,errors
  browser.close()
 print('PASS vendor/client/invoices drill-down, recent green real payment, aged red debt, image support, admin-only configuration and mobile widths; SQLite only.')
finally:f.tearDown()
