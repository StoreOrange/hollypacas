"""QA de la ventana persistente con APIs simuladas: sin ventas ni impresión real."""
import json
import time
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright

def run():
    env=Environment(loader=FileSystemLoader('app/templates'))
    station='a'*32
    context=dict(user={'id':1},pacasholl_libreado_enabled=True,pos_auto_bodega_id=1,pos_station_id=station)
    main=env.get_template('partials/pos_browser_switch.html').render(**context)
    console=env.get_template('pos_console.html').render(**context,pos_worker=True)
    script=Path('app/static/js/pos-browser.js').read_text()
    state={'ok':True,'enabled':False,'tasks':[],'delay_seconds':15}
    claimed=[];acked=[];lost=[False];next_job=[1]
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        ctx=browser.new_context()
        def route(r):
            path=r.request.url.split('https://pos.test',1)[1]
            body=json.loads(r.request.post_data or '{}')
            if path=='/sales':return r.fulfill(content_type='text/html',body=main)
            if path.endswith('/console'):return r.fulfill(content_type='text/html',body=console)
            if path.startswith('/static/js/'):return r.fulfill(content_type='text/javascript',body=script)
            if path.startswith('/static/'):return r.fulfill(body='')
            if path=='/other':return r.fulfill(content_type='text/html',body='<h1>Inventario</h1>')
            if '/ticket/' in path:
                job=int(path.rsplit('/',1)[1])
                return r.fulfill(content_type='text/html',body=f"<script>setTimeout(()=>parent.postMessage({{type:'holl-native-print',job_id:{job},claim_id:'claim{job}',status:'SENT'}},location.origin),100)</script>")
            if path.endswith('/enable'):state['enabled']=body['enabled']
            if path.endswith('/tick') and state['enabled'] and not state['tasks'] and next_job[0]<=3:
                n=next_job[0];next_job[0]+=1
                state['tasks']=[dict(id=n,preventa_id=n,invoice_id=n,invoice_number='F'+str(n),total='925',status='FACTURADA',print_status='QUEUED',job_id=n)]
            if path.endswith('/claim'):
                task=state['tasks'][0];n=task['job_id']
                assert task['print_status']=='QUEUED'
                claimed.append(n);task['print_status']='DISPATCHED'
                return r.fulfill(json={'job':dict(id=n,claim_id='claim'+str(n),url='/ticket/'+str(n))})
            if path.endswith('/ack'):
                if not lost[0]:lost[0]=True;return r.abort()
                acked.append(body['job_id']);state['tasks']=[]
            r.fulfill(json=state)
        ctx.route('**/*',route)
        page=ctx.new_page();errors=[]
        ctx.on('page',lambda tab:tab.on('pageerror',lambda e:errors.append(str(e))))
        page.goto('https://pos.test/sales');page.locator('#native-pos-enabled').wait_for(state='attached')
        page.wait_for_function('!document.querySelector("#native-pos-enabled").disabled')
        with page.expect_popup() as opened:page.locator('#native-pos-enabled').check()
        worker=opened.value
        worker.wait_for_function('document.querySelector("#native-pos-enabled").checked')
        page.goto('https://pos.test/other')
        duplicate=ctx.new_page();duplicate.goto('https://pos.test/sales/automatico-pos/'+station+'/console')
        duplicate.wait_for_function('document.querySelector("#native-pos-status").textContent.includes("otra ventana")')
        deadline=time.monotonic()+20
        while len(acked)<3 and time.monotonic()<deadline:worker.wait_for_timeout(100)
        assert claimed==[1,2,3],claimed
        assert acked==[1,2,3],acked
        assert not errors,errors
        browser.close()
    print('PASS: navegar fuera de Ventas, tres tickets FIFO, ACK perdido sin duplicar y segunda ventana excluida.')

if __name__=='__main__':run()
