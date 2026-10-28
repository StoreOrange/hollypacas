"""Manual suggestions and scanner behavior, real local UI with read-only mocked searches."""
import asyncio,json,sys
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.main import app
from app.config import get_active_company_key,get_active_database_url
from app.database import get_engine
from app.core.security import create_access_token
from sqlalchemy import text
from sqlalchemy.engine import make_url
from playwright.async_api import async_playwright

async def main():
 url=make_url(get_active_database_url());assert get_active_company_key()=='bdzapatos' and url.host in ('127.0.0.1','localhost')
 engine=get_engine();engine.echo=False
 with engine.connect() as c:
  c.execute(text('SET TRANSACTION READ ONLY'))
  email=c.execute(text("select u.email from users u join user_roles ur on u.id=ur.user_id join roles r on r.id=ur.role_id where r.name='administrador' and u.is_active order by u.id limit 1")).scalar_one()
 token=create_access_token({'sub':email})
 def item(variant,color,size,stocks,code='EQUAL02'):
  return dict(variant_id=variant,producto_id=900+variant,cod_producto=code,cod_variante=code+'-'+color+'-'+size,scan_code='V'+str(variant).zfill(6),descripcion='Zapato de prueba',color=color,talla=size,stock_bodegas=stocks,existencia_global=sum(stocks.values()),**{'precio_venta'+str(i):1200-(i-1)*10 for i in range(1,8)})
 options=[item(1,'BLACK PU','6.5',{'central':2,'kg':0,'kgf':5}),item(2,'IVORY','8',{'central':0,'kg':3,'kgf':1})]
 requests=[];writes=[];errors=[]
 async with async_playwright() as p:
  browser=await p.firefox.launch(headless=True);ctx=await browser.new_context(viewport={'width':1440,'height':1050})
  await ctx.add_cookies([{'name':'access_token','value':token,'url':'http://127.0.0.1:8001','httpOnly':True,'sameSite':'Lax'}])
  async def route(r):
   if r.request.method not in ('GET','HEAD'):writes.append(r.request.url);return await r.abort()
   url=urlsplit(r.request.url)
   if url.path=='/sales/shoes/variants/search':
    params=parse_qs(url.query);q=params.get('q',[''])[0];requests.append((q,params.get('limit',[''])[0]))
    if q=='FAIL':return await r.fulfill(status=500,json={'ok':False})
    if q=='EMPTY':return await r.fulfill(json={'ok':True,'items':[]})
    if q in ('V000001','V000002'):
     rows=[row for row in options if row['scan_code']==q]
    elif q=='OLD':
     await asyncio.sleep(.65);rows=[item(3,'OLD COLOR','9',{'central':99,'kg':99,'kgf':99},'OLD RESULT')]
    elif q=='NEW':rows=[item(4,'NEW COLOR','10',{'central':4,'kg':5,'kgf':6},'NEW RESULT')]
    else:rows=options
    try:return await r.fulfill(json={'ok':True,'items':rows})
    except Exception:return
   return await r.continue_()
  await ctx.route('**/*',route);page=await ctx.new_page();page.set_default_timeout(15000);page.on('pageerror',lambda e:errors.append(str(e)))
  await page.goto('http://127.0.0.1:8001/inventory/shoes/prices')
  query=page.locator('#spQuery');await query.fill('EQUAL')
  await page.locator('#spSuggestionsList [role=option]').first.wait_for()
  assert await page.locator('#spSuggestionsList [role=option]').count()==2
  first=page.locator('#spSuggestion0');assert 'BLACK PU' in await first.inner_text() and '6.5' in await first.inner_text()
  assert await first.locator('.sp-suggestion-stock strong').all_text_contents()==['2','0','5']
  assert 'Central: 2, KG: 0, KGF: 5' in await first.get_attribute('aria-label')
  assert await page.locator('#spRows tr[data-product-id]').count()==0
  await page.screenshot(path='/tmp/miss-price-suggestions-desktop.png',full_page=True)
  await query.press('ArrowDown');assert await query.get_attribute('aria-activedescendant')=='spSuggestion0'
  await query.press('ArrowDown');await query.press('Enter')
  await page.locator('tr[data-product-id="902"]').wait_for()
  assert await query.input_value()=='V000002'
  assert await page.locator('#spColor').input_value()=='' and await page.locator('#spSize').input_value()==''
  assert await page.locator('#spSuggestions').is_hidden()
  assert await page.locator('#spReview').is_disabled()
  await page.locator('#spColor').fill('');await page.locator('#spSize').fill('');await query.fill('EQUAL')
  await page.locator('#spSuggestion0').wait_for();await page.locator('#spSuggestion0').click();await page.locator('tr[data-product-id="901"]').wait_for()
  assert await query.input_value()=='V000001'
  # Scanner Enter must not choose a previous manual suggestion or make a preview request.
  await query.fill('EQUAL');await page.locator('#spSuggestion0').wait_for()
  requests.clear();await query.fill('V000002');await query.press('Enter');await page.locator('tr[data-product-id="902"]').wait_for()
  await page.wait_for_timeout(350)
  assert requests==[('V000002','120')],requests
  # Late responses cannot replace the current options.
  await page.locator('#spColor').fill('');await page.locator('#spSize').fill('');await query.fill('OLD')
  await page.wait_for_function("document.getElementById('spSuggestionStatus').textContent.includes('Buscando')")
  await query.fill('NEW');await page.locator('#spSuggestion0').filter(has_text='NEW RESULT').wait_for();await page.wait_for_timeout(700)
  assert 'OLD RESULT' not in await page.locator('#spSuggestions').inner_text()
  await query.press('Escape');assert await page.locator('#spSuggestions').is_hidden()
  # Compact mobile layout retains all three per-warehouse counts.
  await page.set_viewport_size({'width':390,'height':844});await query.fill('EQUAL');await page.locator('#spSuggestion0').wait_for()
  for label in ('Central','KG','KGF'):assert await page.locator('#spSuggestion0 .sp-suggestion-stock small').get_by_text(label,exact=True).is_visible()
  assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
  await page.screenshot(path='/tmp/miss-price-suggestions-mobile.png',full_page=True)
  await query.fill('EMPTY');await page.wait_for_function("document.getElementById('spSuggestionStatus').textContent.includes('Sin coincidencias')")
  await query.fill('FAIL');await page.wait_for_function("document.getElementById('spSuggestionStatus').textContent.includes('No se pudieron')")
  assert not writes,writes
  assert not errors,errors
  await browser.close()
 print('PASS manual suggestions: code/color/size and Central/KG/KGF stocks; keyboard and click; scanner Enter; stale response protection; no results/errors; 390px; read-only, no price writes.')

if __name__=='__main__':asyncio.run(main())
