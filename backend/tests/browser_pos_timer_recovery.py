"""QA local Firefox: red simulada, contador y recuperación; no factura ni imprime."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from datetime import timedelta
from playwright.sync_api import sync_playwright
from app.database import get_engine,get_session_local
from app.routers import web
get_engine().echo=False
with get_session_local()() as db:
 user=db.query(web.User).join(web.User.roles).filter(web.Role.name=='administrador',web.User.is_active.is_(True)).first()
 if not user:raise RuntimeError('Se requiere un usuario administrador local para el QA de lectura')
 token=web.create_access_token({'sub':user.email},expires_delta=timedelta(minutes=3))
import json
with sync_playwright() as p:
 browser=p.firefox.launch(headless=True);context=browser.new_context(viewport={'width':1440,'height':1000});context.add_cookies([dict(name='access_token',value=token,url='http://127.0.0.1:8000')]);page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
 page.add_init_script("""(()=>{const real=window.fetch.bind(window);let first=true;window.fetch=(url,options)=>{if(first&&String(url).endsWith('/sales/automatico-pos/station')){first=false;return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>{window.__timeoutObserved=true;reject(new DOMException('Tiempo agotado','AbortError'));},{once:true}));}return real(url,options);};})();""")
 state={'ok':True,'enabled':False,'tasks':[]};boot=[0];ticks=[0];off=[False]
 def mock(route):
  path=route.request.url.split('/sales/automatico-pos/',1)[1];payload=json.loads(route.request.post_data or '{}')
  if path=='station':
   boot[0]+=1
   state['station_id']=payload['station_id']
  if path.endswith('/enable'):
   state['enabled']=payload['enabled'];state['tasks']=[{'id':101,'preventa_id':123,'status':'WAITING','preventa':'PV-PRUEBA','remaining':40,'message':'','job_id':None,'print_status':None}]
  if path.endswith('/tick'):
   ticks[0]+=1
   if off[0]:route.abort();return
  if path.endswith('/claim'):route.fulfill(status=200,content_type='application/json',body='{"ok":true,"job":null}');return
  route.fulfill(status=200,content_type='application/json',body=json.dumps(state))
 page.route('**/sales/automatico-pos/**',mock)
 page.goto('http://127.0.0.1:8000/sales');page.wait_for_function('!document.querySelector("#native-pos-enabled").disabled');assert boot[0]>=1 and page.evaluate('window.__timeoutObserved'),boot
 page.locator('#native-pos-enabled').check();page.wait_for_timeout(5400)
 left=int(page.locator('#native-pos-time').inner_text().split()[0]);assert 31<=left<=36,left;assert ticks[0]>=3,ticks
 off[0]=True;page.wait_for_function('document.querySelector("#native-pos-status").textContent.includes("Reconectando")')
 off[0]=False;page.wait_for_function('document.querySelector("#native-pos-status").textContent.includes("PV-PRUEBA")')
 later=int(page.locator('#native-pos-time').inner_text().split()[0]);assert later<=left,(later,left)
 assert not errors,errors;browser.close()
print('PASS Firefox: primera conexión fallida se recupera sola, contador no reinicia al consultar, corte de red se reconecta sin perder el plazo. Sin ventas ni impresión reales.')
