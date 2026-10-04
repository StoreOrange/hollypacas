"""Isolated browser checks; no real inventory or transfers are written."""
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from playwright.sync_api import sync_playwright
source=(Path(__file__).parents[1]/'app/templates/inventory_transfers_quick.html').read_text()
script=source[source.index('(() => {'):source.index('  {% if print_id %}')]+"window.scansDone=()=>scansPending===0;})();"
with sync_playwright() as p:
 b=p.chromium.launch(executable_path='/Applications/Brave Browser.app/Contents/MacOS/Brave Browser',headless=True)
 page=b.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
 page.route('http://transfer.test/',lambda r:r.fulfill(body='''<form id="quick-transfer-form"><input id="scan-input"><button type="button" id="scan-add">Agregar</button><button type="button" id="clear-transfer">Limpiar</button><select id="bodega-origen"><option value="1">A</option><option value="2">B</option></select><select id="bodega-destino"><option value="2">B</option></select><input id="fecha"><div id="scan-status"></div><input id="manual-search-input"><div id="manual-search-status"></div><table><tbody id="manual-search-body"></tbody></table><table><tbody id="transfer-items-body"><tr id="empty-transfer-row"></tr></tbody></table></form>''',content_type='text/html'))
 variant=dict(variant_id=1,producto_id=1,cod_variante='SHOE-38',scan_code='V000001',descripcion='Zapato',cod_producto='SHOE',color='Negro',talla='38',existencia=2,costo_cs=100)
 def search(route):
  params=parse_qs(urlsplit(route.request.url).query);code=params['q'][0]
  if params.get('exact')!=['true']:
   route.fulfill(json={'ok':True,'items':[]});return
  if code=='FAIL':route.abort();return
  rows=[variant] # even a bad server response must never cause a partial match
  if code=='ZERO': rows=[dict(variant,cod_variante='ZERO',existencia=0)]
  if code=='AMBIG':rows=[dict(variant,cod_variante='AMBIG'),dict(variant,cod_variante='AMBIG',variant_id=2)]
  route.fulfill(json={'ok':True,'items':rows,'bodega_id':int(params['bodega_id'][0])})
 page.route('**/search?*',search);page.goto('http://transfer.test/');page.add_script_tag(content=script)
 def scan(code):
  page.locator('#scan-input').fill(code);page.locator('#scan-input').press('Enter');page.wait_for_function('scansDone()')
  assert page.locator('#scan-input').evaluate('e=>e===document.activeElement')
 for code in ['UNKNOWN','SHOE','SHOE-3','AMBIG','ZERO','FAIL']:
  scan(code);assert page.locator('[data-item-row]').count()==0,code
 scan('V000001');scan('SHOE-38')
 assert page.locator('[data-item-row]').count()==1 and page.locator('.row-qty').input_value()=='2'
 scan('V000001');assert page.locator('.row-qty').input_value()=='2'
 assert 'Saldo insuficiente' in page.locator('#scan-status').inner_text()
 page.locator('#clear-transfer').click()
 page.evaluate("""()=>{for(let i=0;i<3;i++){const e=document.querySelector('#scan-input');e.value='V000001';e.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}));}}""")
 page.wait_for_function('scansDone()');assert page.locator('[data-item-row]').count()==1 and page.locator('.row-qty').input_value()=='2'
 page.locator('#clear-transfer').click()
 page.evaluate("""()=>{const s=document.querySelector('#scan-input');s.value='V000001';s.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}));const b=document.querySelector('#bodega-origen');b.value='2';b.dispatchEvent(new Event('change'));}""")
 page.wait_for_function('scansDone()');assert page.locator('[data-item-row]').count()==0
 assert not errors,errors
 print('PASS: exact short/full codes, unknown/partial/ambiguous rejected, zero stock, repeats, queued scans, focus and origin changes.')
 b.close()
