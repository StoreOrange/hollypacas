"""QA de la ventana persistente con APIs simuladas: sin ventas ni impresión real."""
import json
import re
import sys
from types import SimpleNamespace
import time
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def ticket_html():
    # Ticket real renderizado por la ruta, usando solo una base SQLite aislada.
    from tests.test_pos_browser import NativePosTests
    from app.routers import pos_browser as native
    from starlette.templating import Jinja2Templates
    fixture=NativePosTests()
    fixture.setUp()
    try:
        _,job=fixture.invoice()
        claim=native.claim(fixture.station.id,fixture.request(),fixture.db,fixture.user)['job']
        request=fixture.request()
        request.scope['query_string']=('claim='+claim['claim_id']).encode()
        request.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
        return native.ticket(fixture.station.id,job.id,request,fixture.db,fixture.user).body.decode()
    finally:
        fixture.tearDown()


def run(mode="persistent"):
    env=Environment(loader=FileSystemLoader('app/templates'))
    station='a'*32
    context=dict(user={'id':1},pacasholl_libreado_enabled=True,pos_auto_bodega_id=1,pos_station_id=station)
    main=env.get_template('partials/pos_browser_switch.html').render(**context)
    console=env.get_template('pos_console.html').render(**context,pos_worker=True)
    script=Path('app/static/js/pos-browser.js').read_text()
    state={'ok':True,'enabled':mode=='already_enabled','tasks':[],'delay_seconds':15}
    snapshots=[]
    claimed=[];acked=[];printed=[];lost=[False];next_job=[1];allow_jobs=[mode!='closed_popup']
    ticket=ticket_html()
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        ctx=browser.new_context()
        ctx.expose_function('recordPrint',lambda path,label,text:(printed.append((int(path.rsplit('/',1)[1]),label)),snapshots.append(text)))
        ctx.add_init_script("window.print=()=>{window.recordPrint(location.pathname,document.getElementById('pos-copy-label')?.textContent,document.querySelector('.ticket').innerText);setTimeout(()=>dispatchEvent(new Event('afterprint')),0)}")
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
                html=re.sub(r'const job=\{.*?\};', 'const job='+json.dumps({'id':job,'claim_id':'claim'+str(job)})+';',ticket)
                return r.fulfill(content_type='text/html',body=html)
            if path.endswith('/enable'):state['enabled']=body['enabled']
            if path.endswith('/tick') and allow_jobs[0] and state['enabled'] and not state['tasks'] and next_job[0]<=3:
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
        if mode=='blocked_popup':page.add_init_script('window.open=()=>null')
        page.goto('https://pos.test/sales');page.locator('#native-pos-enabled').wait_for(state='attached')
        page.wait_for_function('!document.querySelector("#native-pos-enabled").disabled')
        worker=page
        if mode in ('persistent','closed_popup'):
            with page.expect_popup() as opened:page.locator('#native-pos-enabled').check()
            worker=opened.value
            worker.wait_for_function('document.querySelector("#native-pos-enabled").checked')
            if mode=='persistent':
                page.goto('https://pos.test/other')
                duplicate=ctx.new_page();duplicate.goto('https://pos.test/sales/automatico-pos/'+station+'/console')
                duplicate.wait_for_function('document.querySelector("#native-pos-status").textContent.includes("otra ventana")')
            else:
                worker.close();worker=page;allow_jobs[0]=True
        elif mode=='blocked_popup':
            page.locator('#native-pos-enabled').check()
        # already_enabled reproduce la caja real: activada pero sin ventana.
        deadline=time.monotonic()+20
        while len(acked)<3 and time.monotonic()<deadline:worker.wait_for_timeout(100)
        assert claimed==[1,2,3],claimed
        assert acked==[1,2,3],acked
        assert printed==[(job,label) for job in (1,2,3) for label in ('ORIGINAL','COPIA','EXCLUSIVA DE BODEGA')],printed
        for index,text in enumerate(snapshots):
            if index % 3 == 2:
                assert 'TOTAL BULTOS' in text and 'Precio:' not in text and 'Pagos aplicados' not in text and 'C$' not in text,text
            else:
                assert 'Precio:' in text and 'Pagos aplicados' in text,text
        assert not errors,errors
        browser.close()
    print('PASS '+mode+': tres facturas FIFO con tres ejemplares cada una y ACK perdido sin duplicar.')

if __name__=='__main__':
    for mode in ('persistent','already_enabled','blocked_popup','closed_popup'):run(mode)
