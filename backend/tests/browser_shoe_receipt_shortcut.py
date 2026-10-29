"""Atajos reales a ingresos individuales; sin guardar movimientos ni existencias."""
import sys
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_shoe_receipt_shortcut import ReceiptShortcutTests
from app.routers import web as w, shoe_unit_receipts as unit
from app.main import app
fixture=ReceiptShortcutTests();fixture.setUp()
try:
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        ctx=browser.new_context(viewport={'width':1440,'height':1000})
        errors=[];posts=[]
        ctx.on('page',lambda page:page.on('pageerror',lambda error:errors.append(str(error))))
        def route(r):
            url=urlsplit(r.request.url)
            if r.request.method=='POST':posts.append(url.path);return r.abort()
            request=fixture.request(url.query)
            request.scope.update(app=app,router=app.router,path=url.path,scheme='https',server=('miss.test',443))
            request.state.branding={'company_key':'bdzapatos'}
            if url.path=='/inventory/traslados-rapidos':
                response=w.inventory_quick_transfers_page(request,fixture.db,fixture.admin)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path==unit.PATH:
                response=unit.page(request,fixture.db,fixture.admin)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        ctx.route('**/*',route)
        page=ctx.new_page();page.goto('https://miss.test/inventory/traslados-rapidos')
        page.locator('#bodega-origen').select_option('3')
        link=page.locator('[data-receipt-origin-selector="#bodega-origen"]')
        assert 'bodega_id=3' in link.get_attribute('href')
        link.click()
        receipt=page.frame_locator("#shoe-receipt-modal iframe")
        receipt.locator('#unit-warehouse').wait_for()
        assert receipt.locator('#unit-warehouse').input_value()=='3'
        assert page.locator('#bodega-origen').input_value()=='3'
        assert len(ctx.pages)==1
        assert receipt.locator('[data-bs-sidebar]').count()==0
        page.locator('#shoe-receipt-modal [aria-label="Cerrar ingreso rápido"]').click()
        page.locator('#shoe-receipt-modal.show').wait_for(state='hidden')
        page.get_by_role('button',name='Buscar variante manual').click()
        page.locator('#variant-search-modal.show').wait_for()
        page.locator('#search-bodega-origen').select_option('1')
        modal_link=page.locator('#variant-search-modal [data-receipt-origin-selector]')
        assert 'bodega_id=1' in modal_link.get_attribute('href')
        page.locator('#variant-search-modal .modal-content').screenshot(path='/tmp/miss-transfer-receipt-shortcut.png',animations='disabled')
        page.locator('#variant-q').fill('EQUAL02')
        modal_link.click()
        second=page.frame_locator('#shoe-receipt-modal iframe');second.locator('#unit-warehouse').wait_for()
        assert second.locator('#unit-warehouse').input_value()=='1'
        assert second.locator('#unit-qty').input_value()=='1'
        assert second.locator('#unit-code').input_value()=='EQUAL02'
        assert not page.locator('#variant-search-modal').is_visible()
        page.locator('#shoe-receipt-modal .modal-content').screenshot(path='/tmp/miss-guided-receipt-desktop.png')
        page.locator('#shoe-receipt-modal [aria-label="Cerrar ingreso rápido"]').click()
        page.locator('#variant-search-modal.show').wait_for()
        assert page.locator('#variant-q').input_value()=='EQUAL02'
        assert len(ctx.pages)==1
        page.set_viewport_size({'width':390,'height':844})
        modal_link.click(); second.locator('#unit-warehouse').wait_for()
        page.locator('#shoe-receipt-modal .modal-content').screenshot(path='/tmp/miss-guided-receipt-mobile.png')
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        assert not posts,posts
        assert fixture.db.query(w.IngresoInventario).count()==0
        assert not errors,errors
        browser.close()
    print('PASS: botón de interfaz y pie del buscador; origen KGF/Central se conserva en ingreso individual; modal integrado, retorno al buscador, búsqueda heredada, vista móvil y cero registros automáticos.')
finally:
    fixture.tearDown()
