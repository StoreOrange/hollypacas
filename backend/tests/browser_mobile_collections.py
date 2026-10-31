"""Mobile UX with real server rendering; every payment is isolated SQLite test data."""
import sys
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_mobile_collections import MobileCollectionTests
from app.models.sales import CollectionPanelPayment as MobileCollectionPayment
from app.routers import web as w

fixture=MobileCollectionTests();fixture.setUp()
try:
 with sync_playwright() as p:
  browser=p.firefox.launch(headless=True)
  ctx=browser.new_context(viewport={'width':390,'height':844})
  errors=[]
  def dispatch(route):
   request=route.request;url=urlsplit(request.url)
   if url.path.startswith('/static/'):
    file=Path(__file__).resolve().parents[1]/'app'/url.path.lstrip('/')
    if file.is_file():return route.fulfill(path=str(file))
   if url.path.startswith('/m/cartera'):
    headers={key:value for key,value in request.headers.items() if key.lower() in {'content-type','origin'}}
    response=fixture.client.request(request.method,url.path+('?' + url.query if url.query else ''),content=request.post_data_buffer,headers=headers,follow_redirects=False)
    # Playwright routes cover only the initial redirect request. Follow it
    # in the isolated test server; the HTTP integration tests assert 303.
    if response.status_code==303:response=fixture.client.get(response.headers['location'])
    return route.fulfill(status=response.status_code,headers=dict(response.headers),body=response.content)
   return route.fulfill(content_type='application/json',body='{}')
  ctx.route('**/*',dispatch)
  page=ctx.new_page();page.on('pageerror',lambda error:errors.append(str(error)))
  page.goto('https://testserver/m/cartera')
  assert page.locator('.mc-invoice').count()==1
  assert 'F-1' in page.locator('.mc-invoice').inner_text()
  assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
  page.screenshot(path='/tmp/holl-cartera-vendedor.png',full_page=True)
  page.locator('.mc-invoice').click()
  page.wait_for_load_state('load')
  page.locator('#mc-amount').fill('20.00');page.locator('#mc-bank').fill('Banco QA');page.locator('#mc-reference').fill('DEP-123')
  page.locator('#mc-support').set_input_files({'name':'deposito.png','mimeType':'image/png','buffer':fixture.png()})
  page.locator('#mc-preview').wait_for(state='visible')
  assert '80' in page.locator('#mc-feedback').inner_text()
  page.screenshot(path='/tmp/holl-cartera-abono.png',full_page=True)
  page.locator('#mc-payment [type=submit]').click()
  page.get_by_role('status').filter(has_text='Abono registrado correctamente').wait_for()
  assert fixture.db.query(MobileCollectionPayment).count()==1
  assert fixture.db.query(w.CobranzaAbono).count()==0
  assert '80.00' in page.locator('.mc-detail-balances .due').inner_text()
  page.locator('[data-support]').first.click();assert page.locator('.mc-lightbox').is_visible()
  page.wait_for_function("document.querySelector('.mc-lightbox img').complete && document.querySelector('.mc-lightbox img').naturalWidth>0")
  page.keyboard.press('Escape')
  page.locator('#mc-settle').click();assert page.locator('#mc-amount').input_value()=='80.00'
  page.locator('#mc-bank').fill('Banco QA');page.locator('#mc-reference').fill('DEP-124')
  page.locator('#mc-support').set_input_files({'name':'deposito.png','mimeType':'image/png','buffer':fixture.png()})
  page.locator('#mc-payment [type=submit]').click()
  page.get_by_text('Esta deuda está saldada en este panel.').wait_for()
  assert fixture.db.get(w.VentaFactura,1).estado_cobranza=='PENDIENTE'
  fixture.user=fixture.admin
  page.goto('https://testserver/m/cartera')
  assert page.locator('#mc-vendor').count()==1
  assert page.locator('.mc-vendor-row').count()==1
  assert 'Otra persona' in page.locator('.mc-vendor-row').inner_text()
  page.screenshot(path='/tmp/holl-cartera-admin.png',full_page=True)
  page.goto('https://testserver/m/cartera?view=history&vendor=1')
  assert page.locator('[data-support]').count()==2
  page.screenshot(path='/tmp/holl-cartera-historial.png',full_page=True)
  page.set_viewport_size({'width':1440,'height':1000})
  assert page.locator('.mc-app').bounding_box()['width']<=800
  assert not errors,errors
  browser.close()
 print('PASS mobile seller/admin, partial/full payment, upload preview, private image, history, responsive layout; SQLite only.')
finally:
 if 'errors' in globals() and errors:print('Browser errors:',errors)
 fixture.tearDown()
