"""Mobile/desktop UI served by a local fixture app; no real email or inventory."""
import sys
import socket
import threading
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tests.test_inventory_requests import RequisitionTests
from playwright.sync_api import sync_playwright
from fastapi.staticfiles import StaticFiles
import uvicorn

case=RequisitionTests();case.setUp()
root=Path(__file__).resolve().parents[1]
case.app.mount('/static',StaticFiles(directory=str(root/'app/static')),name='static')
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
server=uvicorn.Server(uvicorn.Config(case.app,log_level='error',lifespan='off'))
thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
try:
    for _ in range(200):
        if server.started:break
        time.sleep(.02)
    assert server.started
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        page=browser.new_page(viewport={'width':390,'height':844})
        errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(f'http://127.0.0.1:{port}/sales/requisas?new=1')
        page.locator('#rq-query').fill('ropa domestica')
        page.locator('.rq-search-result').first.wait_for()
        assert 'Central: 10' in page.locator('.rq-search-result').first.inner_text()
        page.locator('.rq-search-result').first.click()
        page.locator('#rq-qty-1').fill('2')
        page.screenshot(path='/tmp/holl-requisas-mobile-create.png',full_page=True)
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Mobile horizontal overflow'
        page.locator('#rq-submit').click();page.wait_for_url('**/sales/requisas/1')
        page.get_by_role('heading',name='En espera de autorización').wait_for()
        from app.routers.web import EgresoInventario
        assert case.db.query(EgresoInventario).count()==0
        page.screenshot(path='/tmp/holl-requisas-mobile-pending.png',full_page=True)
        case.user=case.admin
        page.reload();page.get_by_role('button',name='Autorizar y realizar traslado').wait_for()
        page.screenshot(path='/tmp/holl-requisas-mobile-review.png',full_page=True)
        page.get_by_role('button',name='Autorizar y realizar traslado').click()
        page.get_by_role('heading',name='Traslado realizado').wait_for()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Mobile detail horizontal overflow'
        page.screenshot(path='/tmp/holl-requisas-mobile-approved.png',full_page=True)
        page.set_viewport_size({'width':1440,'height':1000});page.goto(f'http://127.0.0.1:{port}/sales/requisas?status=active')
        page.get_by_text('REQ-000001',exact=True).wait_for()
        page.screenshot(path='/tmp/holl-requisas-desktop.png',full_page=True)
        page.goto(f'http://127.0.0.1:{port}/sales/requisas/1/recibo')
        page.get_by_role('button',name='Imprimir POS').wait_for()
        page.screenshot(path='/tmp/holl-requisas-pos.png',full_page=True)
        assert not errors,errors
        browser.close()
    print('PASS mobile create, search stock, cashier pending, admin approve, desktop panel and POS receipt. Isolated data only.')
finally:
    server.should_exit=True;thread.join(timeout=5);sock.close();case.tearDown()
