"""Firefox queue failure and bulk backup with real receipt rendering, isolated SQLite."""
import asyncio,json
from pathlib import Path
from types import SimpleNamespace
from datetime import timedelta
from playwright.sync_api import sync_playwright
from tests.test_pos_browser import NativePosTests
from app.routers import pos_browser as native
from app.models.pos_automation import PosPrintJob
from app.routers import web
from starlette.templating import Jinja2Templates

def complete(coroutine):
    # Request JSON is preloaded, so this endpoint has no asynchronous I/O in the fixture.
    try:coroutine.send(None)
    except StopIteration as result:return result.value
    raise AssertionError('Unexpected asynchronous fixture operation')


def run(mode):
    f=NativePosTests();f.setUp()
    try:
        _,first=f.invoice();first.status='SENT';f.db.commit()
        f.sale(2);asyncio.run(native.tick(f.station.id,f.request({'busy':False}),f.db,f.user));f.clock+=timedelta(seconds=15)
        asyncio.run(native.tick(f.station.id,f.request({'busy':False}),f.db,f.user))
        jobs=f.db.query(PosPrintJob).order_by(PosPrintJob.id).all();assert len(jobs)==2
        for job in jobs:job.status='QUEUED'
        f.db.commit()
        templates=Jinja2Templates(directory='app/templates')
        context=dict(user={'id':1},pacasholl_libreado_enabled=True,pos_auto_bodega_id=1,pos_station_id=f.station.id)
        main='<html><head><meta charset="utf-8"><link rel="stylesheet" href="/static/vendor/bootstrap.min.css"></head><body><div id="layout"><main>'+templates.get_template('partials/pos_browser_switch.html').render(**context)+'</main></div></body></html>'
        script=Path('app/static/js/pos-browser.js').read_text()
        claims=[];batches=[];calls=[];lost=[False];errors=[]
        def request(token=None):
            req=f.request();req.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=templates))
            if token:req.scope['query_string']=('claim='+token).encode()
            return req
        with sync_playwright() as p:
            browser=p.firefox.launch(headless=True);ctx=browser.new_context(viewport={'width':1280,'height':900})
            ctx.expose_function('recordPrint',lambda path,labels:calls.append((path,labels)))
            trigger="throw Error('printer unavailable')" if mode=='throw' else 'return'
            ctx.add_init_script("window.print=()=>{window.recordPrint(location.pathname,[...document.querySelectorAll('.ticket')].map(r=>r.querySelector('[data-pos-copy-label]')?.textContent));if(location.pathname.includes('/backup/'))setTimeout(()=>dispatchEvent(new Event('afterprint')),0);else {"+trigger+";}}")
            def route(r):
                path=r.request.url.split('https://pos.test',1)[1];payload=json.loads(r.request.post_data or '{}')
                if path=='/sales':return r.fulfill(content_type='text/html',body=main)
                if path.startswith('/static/js/'):return r.fulfill(content_type='text/javascript',body=script)
                if path.startswith('/static/'):
                    asset=Path('app'+path.split('?')[0])
                    return r.fulfill(content_type='text/css' if path.split('?')[0].endswith('.css') else 'application/octet-stream',body=asset.read_bytes() if asset.is_file() else b'')
                if path.endswith('/backup/claim'):
                    result=native.backup_claim(f.station.id,request(),payload,f.db,f.user);batches.append(result)
                    return r.fulfill(json=result)
                if '/backup/ticket?' in path:
                    response=native.backup_ticket(f.station.id,request(path.split('claim=')[1]),f.db,f.user)
                    return r.fulfill(content_type='text/html',body=response.body.decode())
                if path.endswith('/backup/ack'):
                    result=native.backup_ack(f.station.id,request(),payload,f.db,f.user)
                    if not lost[0]:lost[0]=True;return r.abort()
                    return r.fulfill(json=result)
                if path.endswith('/claim'):
                    result=native.claim(f.station.id,request(),f.db,f.user)
                    if result['job']:claims.append(result['job']['id'])
                    return r.fulfill(json=result)
                if '/jobs/' in path and '/ticket?' in path:
                    job=int(path.split('/jobs/')[1].split('/')[0]);token=path.split('claim=')[1]
                    html=native.ticket(f.station.id,job,request(token),f.db,f.user).body.decode().replace(',35000);',',300);')
                    return r.fulfill(content_type='text/html',body=html)
                if path.endswith('/ack'):
                    job=int(path.split('/jobs/')[1].split('/')[0]);result=complete(native.ack(f.station.id,job,f.request(payload),f.db,f.user))
                    return r.fulfill(json=result)
                return r.fulfill(json=native.state(f.db,f.station))
            ctx.route('**/*',route);ctx.on('page',lambda page:page.on('pageerror',lambda e:errors.append(str(e))))
            page=ctx.new_page();page.on('dialog',lambda dialog:dialog.accept());page.goto('https://pos.test/sales')
            page.locator('.pos-recovery').wait_for(state='visible')
            page.wait_for_function('!document.querySelector("[data-backup]").disabled')
            assert page.locator('[data-backup-job]').count()==2
            page.locator('[data-backup-job]').last.uncheck();page.wait_for_timeout(1200)
            assert not page.locator('[data-backup-job]').last.is_checked()
            page.locator('[data-backup-job]').last.check()
            page.screenshot(path=f'/tmp/pos-recovery-{mode}.png',full_page=True)
            with page.expect_popup() as popup:page.locator('[data-backup]').click()
            popup.value.wait_for_load_state()
            page.wait_for_function('document.querySelector(".pos-recovery").hidden',timeout=15000)
            page.wait_for_timeout(2500)
            assert len(batches)==1 and claims==[jobs[0].id],(batches,claims)
            batch_calls=[entry for entry in calls if '/backup/' in entry[0]]
            assert len(batch_calls)==1 and batch_calls[0][1]==['ORIGINAL','COPIA','EXCLUSIVA DE BODEGA']*2,batch_calls
            assert f.db.query(web.VentaFactura).count()==2
            assert all(job.status=='SENT' for job in jobs)
            assert not errors,errors
            browser.close()
            print('PASS',mode,': stalled/failed print shows central backup; 2 invoices/6 receipts in one call; lost ACK retried without rebilling or reprinting.')
    finally:f.tearDown()

if __name__=='__main__':
    for mode in ('missing_afterprint','throw'):run(mode)
