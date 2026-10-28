"""Local Miss UI; synthetic searches and intercepted writes; no real price changes."""
import sys,json,time
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.main import app
from app.config import get_active_database_url,get_active_company_key
from app.database import get_engine
from app.core.security import create_access_token
from sqlalchemy import text
from sqlalchemy.engine import make_url
from playwright.sync_api import sync_playwright

url=make_url(get_active_database_url());assert get_active_company_key()=='bdzapatos' and url.host in ('127.0.0.1','localhost') and url.database=='bdzapatos'
engine=get_engine();engine.echo=False
with engine.connect() as c:
 c.execute(text('SET TRANSACTION READ ONLY'))
 email=c.execute(text("select u.email from users u join user_roles ur on ur.user_id=u.id join roles r on r.id=ur.role_id where u.is_active=true and r.name='administrador' order by u.id limit 1")).scalar_one()
 sample=c.execute(text('select id,precio_venta1,precio_venta2,precio_venta7 from productos order by id limit 1')).one();sample_id=sample[0];before=tuple(sample[1:])
token=create_access_token({'sub':email})
items=[]
for variant,pid,code,color,size,stock in [(1,11,'EQUAL02','BLACK PU','6',3),(2,11,'EQUAL02','BLACK PU','6.5',2),(3,12,'CELEST','IVORY','7',7)]:
 item={'variant_id':variant,'producto_id':pid,'cod_producto':code,'cod_variante':'V'+str(variant).zfill(6),'descripcion':'Calzado '+code,'color':color,'talla':size,'existencia_global':stock}
 item.update({'precio_venta'+str(t):1000-(t-1)*10 for t in range(1,8)});items.append(item)
posts=[];errors=[];save_failure=[False]
with sync_playwright() as p:
 browser=p.firefox.launch(headless=True)
 ctx=browser.new_context(viewport={'width':1440,'height':1050})
 ctx.add_cookies([{'name':'access_token','value':token,'url':'http://127.0.0.1:8001','httpOnly':True,'sameSite':'Lax'}])
 def route(r):
  path=urlsplit(r.request.url).path
  if path=='/sales/shoes/variants/search':
   q=parse_qs(urlsplit(r.request.url).query).get('q',[''])[0]
   if q=='FAIL':return r.fulfill(status=500,json={'ok':False,'message':'Error de prueba'})
   return r.fulfill(json={'ok':True,'items':[] if q=='EMPTY' else items})
  if r.request.method not in ('GET','HEAD'):
   if path=='/inventory/shoes/prices/apply':
    raw=r.request.post_data;posts.append(raw);time.sleep(.1)
    if save_failure[0]:return r.fulfill(status=400,json={'ok':False,'message':'No se pudo guardar: prueba de error'})
    return r.fulfill(json={'ok':True,'message':'Se actualizaron 1 producto(s)','updated_count':1,'updated_products':[{'producto_id':11,'cod_producto':'EQUAL02','descripcion':'Calzado EQUAL02','variantes':[{'cod_variante':'V000001','color':'BLACK PU','talla':'6'},{'cod_variante':'V000002','color':'BLACK PU','talla':'6.5'},{'cod_variante':'V000004','color':'IVORY','talla':'8'}]}]})
   return r.abort()
  return r.continue_()
 ctx.route('**/*',route)
 page=ctx.new_page();page.set_default_timeout(15000);page.on('pageerror',lambda error:errors.append(str(error)))
 assert page.goto('http://127.0.0.1:8001/inventory/shoes/prices').status==200
 page.locator('.vue-side-nav a[href="/inventory/shoes/prices"]').first.wait_for()
 nav=page.locator('.vue-side .vue-side-nav a').evaluate_all('(nodes)=>nodes.map(n=>n.getAttribute("href"))')
 assert nav[nav.index('/sales')+1]=='/inventory/shoes/prices',nav
 assert page.locator('.vue-side a[href="/inventory/shoes/prices"]').get_attribute('aria-current')=='page'
 assert page.locator('#spReview').is_disabled()
 page.locator('#spQuery').fill('EQUAL02');page.locator('#spQuery').press('Enter')
 page.locator('#spRows tr[data-product-id="11"]').wait_for()
 assert page.locator('#spRows tr[data-product-id]').count()==2
 assert '0 productos seleccionados' in page.locator('#spSelectionCount').inner_text()
 page.locator('tr[data-product-id="11"] .sp-variant-button').click()
 assert page.locator('#spVariants11 .sp-variant-chip').count()==2
 page.locator('tr[data-product-id="11"] input[type=checkbox]').check()
 assert page.locator('#spCheckAll').evaluate('(el)=>el.indeterminate')
 page.locator('.sp-generator summary').click();page.locator('#spBase').fill('900');page.locator('#spGenerate').click()
 assert page.locator('#spPrice7').input_value()=='840.00'
 page.locator('#spPrice7').fill('');page.locator('#spReview').click()
 assert not page.locator('#spConfirmModal').is_visible() and len(posts)==0
 page.locator('#spPrice7').fill('840.00')
 assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
 page.screenshot(path='/tmp/miss-price-panel-desktop.png',full_page=True)
 page.set_viewport_size({'width':390,'height':844})
 assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
 page.screenshot(path='/tmp/miss-price-panel-mobile.png',full_page=True)
 page.locator('#spReview').click();page.locator('#spConfirmModal.show').wait_for()
 assert '1 producto.' in page.locator('#spConfirmSummary').inner_text()
 assert page.locator('#spConfirmProducts').inner_text()=='EQUAL02'
 assert page.locator('#spConfirmPrices tr').count()==7
 page.wait_for_function("!document.getElementById('spSave').disabled")
 page.screenshot(path='/tmp/miss-price-panel-confirm.png')
 page.evaluate("document.getElementById('spSave').click();document.getElementById('spSave').click()")
 page.locator('#spConfirmModal').wait_for(state='hidden')
 page.locator('#spSuccessModal.show').wait_for()
 page.wait_for_function("getComputedStyle(document.getElementById('spSuccessModal')).opacity==='1'")
 assert page.locator('#spSuccessTitle').inner_text()=='¡Precios actualizados!'
 receipt=page.locator('#spSuccessProducts').inner_text()
 for value in ('EQUAL02','BLACK PU','Talla 6.5','IVORY','Talla 8'):assert value in receipt,receipt
 assert page.locator('#spSuccessProducts .sp-success-product').count()==1
 assert '1 producto actualizado' in page.locator('#spSuccessCount').inner_text()
 assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
 page.screenshot(path='/tmp/miss-price-success-mobile.png')
 page.set_viewport_size({'width':1440,'height':1000});page.screenshot(path='/tmp/miss-price-success-desktop.png')
 page.locator('#spSuccessContinue').click();page.locator('#spSuccessModal').wait_for(state='hidden')
 assert len(posts)==1,posts
 raw=posts[0];assert 'name="product_ids"\r\n\r\n11\r\n' in raw,raw
 for tier,value in [(1,'900.00'),(7,'840.00')]:assert f'name="precio_venta{tier}"\r\n\r\n{value}\r\n' in raw,raw
 assert '0 productos seleccionados' in page.locator('#spSelectionCount').inner_text()
 assert '900.00' in page.locator('tr[data-product-id="11"]').inner_text()
 # A rejected save must keep the error in review and never show success.
 save_failure[0]=True
 page.locator('tr[data-product-id="11"] input[type=checkbox]').check();page.locator('#spReview').click()
 page.wait_for_function("!document.getElementById('spSave').disabled")
 page.locator('#spSave').click();page.locator('#spConfirmError').wait_for(state='visible')
 assert 'No se pudo guardar' in page.locator('#spConfirmError').inner_text()
 assert page.locator('#spSuccessModal').is_hidden() and len(posts)==2
 page.locator('#spConfirmModal').get_by_role('button',name='Volver a revisar',exact=True).click();page.locator('#spConfirmModal').wait_for(state='hidden')
 page.locator('#spQuery').fill('EMPTY');page.locator('#spSearch').click();page.get_by_text('Sin resultados',exact=True).wait_for();assert page.locator('#spCheckAll').is_disabled()
 page.locator('#spQuery').fill('FAIL');page.locator('#spSearch').click();page.get_by_text('No se pudo buscar',exact=True).wait_for()
 assert 'Error de prueba' in page.locator('#spStatus').inner_text()
 assert not errors,errors
 browser.close()
with engine.connect() as c:
 c.execute(text('SET TRANSACTION READ ONLY'))
 after=tuple(c.execute(text('select precio_venta1,precio_venta2,precio_venta7 from productos where id=:pid'),{'pid':sample_id}).one())
assert before==after
print('PASS local embedded panel, sidebar order/active state, product selection, color/size details, scale generation, empty validation, confirmation, one intercepted save, errors and desktop/mobile layout; real prices unchanged.')
