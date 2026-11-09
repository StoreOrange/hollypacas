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
        assert not page.locator('#rq-preferences').evaluate('(e)=>e.open')
        page.locator('#rq-preferences summary').click()
        page.locator('#rq-seller').select_option('3')
        assert page.locator('#rq-seller-summary').inner_text()=='José Estelí'
        page.locator('#rq-seller').select_option('2')
        page.locator('#rq-preferences summary').click()
        page.locator('#rq-query').fill('ropa domestica')
        page.locator('.rq-search-result').first.wait_for()
        assert 'Central puede enviar' in page.locator('.rq-search-result').first.inner_text()
        assert '10 disponibles' in page.locator('.rq-search-result').first.inner_text()
        page.locator('.rq-search-result').first.scroll_into_view_if_needed()
        page.screenshot(path='/tmp/holl-requisas-mobile-search.png',full_page=False)
        page.locator('.rq-search-result').first.click()
        page.get_by_role('button',name='Agregar una unidad de DL20',exact=True).click()
        assert page.locator('#rq-qty-1').input_value()=='2'
        assert page.locator('#rq-unit-count').inner_text()=='2 unidades'
        page.get_by_role('button',name='Quitar una unidad de DL20',exact=True).click()
        assert page.locator('#rq-qty-1').input_value()=='1'
        page.locator('#rq-qty-1').fill('11')
        assert page.locator('.rq-stock-warning').is_visible()
        page.locator('#rq-qty-1').fill('2')
        assert not page.locator('.rq-stock-warning').is_visible()
        for width in (320,360,430,390):
            page.set_viewport_size({'width':width,'height':844})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'), f'Overflow at {width}px'
        page.locator('#rq-qty-1').scroll_into_view_if_needed()
        rect=page.locator('#rq-qty-1').bounding_box();footer=page.locator('.rq-compose-footer').bounding_box()
        assert rect['y']+rect['height']<=footer['y'], 'Quantity covered by action bar'
        page.locator('#rq-qty-1').blur()
        page.evaluate('window.scrollTo(0,0)')
        page.screenshot(path='/tmp/holl-requisas-mobile-create.png',full_page=False)
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Mobile horizontal overflow'
        page.locator('#rq-submit').click()
        page.locator('#rq-review').wait_for(state='visible')
        assert case.db.query(__import__('app.routers.web',fromlist=['EgresoInventario']).EgresoInventario).count()==0
        page.screenshot(path='/tmp/holl-requisas-mobile-confirm.png',full_page=False)
        page.get_by_role('button',name='Seguir editando',exact=True).click()
        assert page.locator('#rq-qty-1').input_value()=='2'
        page.locator('#rq-submit').click()
        page.get_by_role('button',name='Confirmar y solicitar',exact=True).click()
        page.wait_for_url('**/sales/requisas/1')
        page.get_by_role('heading',name='En espera de autorización').wait_for()
        from app.routers.web import EgresoInventario
        assert case.db.query(EgresoInventario).count()==0
        page.screenshot(path='/tmp/holl-requisas-mobile-pending.png',full_page=True)
        case.user=case.admin
        page.reload();page.get_by_role('button',name='Autorizar y realizar traslado').wait_for()
        page.set_viewport_size({'width':320,'height':844})
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'), 'Detail overflow on small phone'
        page.set_viewport_size({'width':390,'height':844})
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
