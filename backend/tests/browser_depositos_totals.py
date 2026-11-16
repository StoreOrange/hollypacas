"""Currency-separated deposit autosums and real refresh, isolated fixture only."""
import socket,threading,time
from pathlib import Path
from decimal import Decimal
from types import SimpleNamespace as N
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment,FileSystemLoader
from playwright.sync_api import sync_playwright
import uvicorn
root=Path(__file__).resolve().parents[1]
app=FastAPI();app.mount('/static',StaticFiles(directory=str(root/'app/static')),name='static')
env=Environment(loader=FileSystemLoader(root/'app/templates'))
records=[('CS','4000'),('CS','5290'),('CS','11840'),('CS','410'),('USD','200'),('USD','300'),('USD','200')]
seen=[]
@app.get('/sales/depositos',response_class=HTMLResponse)
def page(request:Request):
    seen.append(dict(request.query_params))
    request.state.branding={'company_key':'hollywood_pacas'};request.state.has_permissions=False;request.state.permission_names=[];request.state.menu_links=[]
    user=N(full_name='QA',email='qa',roles=[N(name='administrador')],default_branch=N(code='central',name='Central'),vendedor_id=None)
    deposits=[N(id=i+1,fecha='2026-11-16',vendedor_id=1,banco_id=1,cuenta_id=None,moneda=currency,monto_cs=Decimal(amount) if currency=='CS' else Decimal(amount)*37,monto_usd=Decimal(amount) if currency=='USD' else Decimal(amount)/37,metodo='DEPOSITO_BANCARIO',observacion='',vendedor=N(nombre='QA'),banco=N(nombre='BAC')) for i,(currency,amount) in enumerate(records)]
    html=env.get_template('sales_depositos.html').render(request=request,user=user,version='qa',branch=N(name='Central'),bodega=N(name='Central'),vendedores=[],bancos=[],cuentas=[],bac_bank_id='',depositos=deposits,summary_rows=[],summary_grouped_rows=[],total_cs=sum(d.monto_cs for d in deposits if d.moneda=='CS'),total_usd=sum(d.monto_usd for d in deposits if d.moneda=='USD'),today='2026-11-16',start_date='2026-11-16',end_date='2026-11-16',vendedor_q='',banco_q='',moneda_q='',error=None,success=None)
    return HTMLResponse(html,headers={'Cache-Control':'private, no-store, max-age=0'})
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
server=uvicorn.Server(uvicorn.Config(app,log_level='error',lifespan='off'));thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
try:
    for _ in range(100):
        if server.started:break
        time.sleep(.02)
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True);tab=browser.new_page(viewport={'width':1280,'height':900});errors=[];tab.on('pageerror',lambda e:errors.append(str(e)))
        response=tab.goto(f'http://127.0.0.1:{port}/sales/depositos?start_date=2026-11-16&end_date=2026-11-16&banco_id=1')
        assert 'no-store' in response.headers['cache-control']
        assert tab.locator('#depositos-footer-cs').inner_text()=='C$ 21,540.00'
        assert tab.locator('#depositos-footer-usd').inner_text()=='US$ 700.00'
        assert tab.locator('#depositos-list thead th').count()==9
        assert tab.locator('#depositos-list tbody tr').first.locator('td').count()==9
        records.append(('CS','1000'))
        tab.locator('#depositos-refresh').click();tab.wait_for_url('**/*_refresh=*')
        tab.wait_for_function("document.querySelector('#depositos-footer-cs').textContent==='C$ 22,540.00'")
        assert tab.locator('#depositos-kpi-cs').inner_text()=='C$ 22,540.00'
        assert seen[-1]['banco_id']=='1' and seen[-1]['start_date']=='2026-11-16'
        records.extend([('CS','0.10'),('CS','0.20')]);tab.reload()
        tab.wait_for_function("document.querySelector('#depositos-footer-cs').textContent==='C$ 22,540.30'")
        assert not errors,errors
        tab.locator('#depositos-list').screenshot(path='/tmp/depositos-autosuma.png')
        browser.close()
        print('PASS separate CS/USD totals, aligned footer, exact cents, server refresh updates total and preserves filters. No production writes.')
finally:server.should_exit=True;thread.join(5);sock.close()
