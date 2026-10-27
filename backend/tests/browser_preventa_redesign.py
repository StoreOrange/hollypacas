"""Holl mobile redesign: real templates/search data, isolated DB and intercepted submissions."""
import sys,json
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_mobile_discount_promotions import MobileDiscountTests
from app.routers import web as w
from app.main import app

fixture=MobileDiscountTests();fixture.setUp()
try:
    fixture.create();saved=fixture.db.query(w.Preventa).one()
    fixture.create(fixture.request(qty='1',item_role='',item_combo_group='',freeze_preventa='1'));frozen=fixture.db.query(w.Preventa).order_by(w.Preventa.id.desc()).first()
    def request(query=''):
        req=fixture.request();req.scope.update(app=app,router=app.router,scheme='https',server=('pv.test',443),path='/m/preventas',query_string=query.encode())
        req.state.branding={'company_key':'hollywood_pacas','trade_name':'Holl Pacas'};w._set_request_permissions(req,fixture.user);return req
    normal=w.mobile_preventas_page(request(),fixture.db,fixture.user).body
    result=w.mobile_preventas_page(request('result_preventa='+str(saved.id)),fixture.db,fixture.user).body
    frozen_html=w.mobile_preventas_page(request('result_preventa='+str(frozen.id)),fixture.db,fixture.user).body
    products=w.mobile_preventas_products_search(request(),q='LIB',db=fixture.db,user=fixture.user).body
    clients=w.mobile_preventas_clientes_search(request(),q='Cliente',db=fixture.db,user=fixture.user).body
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True);page=browser.new_page(viewport={'width':390,'height':844},has_touch=True)
        page.set_default_timeout(8000);errors=[];sent=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        def route(r):
            url=urlsplit(r.request.url)
            if url.path=='/m/preventas' and r.request.method=='GET':return r.fulfill(content_type='text/html',body=(frozen_html if url.query=='result_preventa='+str(frozen.id) else result) if 'result_preventa' in url.query else normal)
            if url.path=='/m/preventas' and r.request.method=='POST':
                sent.append(parse_qs(r.request.post_data or ''));return r.fulfill(content_type='text/html',body='<h1>Solicitud recibida</h1>')
            if url.path=='/m/preventas/productos/search':return r.fulfill(content_type='application/json',body=products)
            if url.path=='/m/preventas/clientes/search':return r.fulfill(content_type='application/json',body=clients)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        page.route('**/*',route);page.goto('https://pv.test/m/preventas')
        page.locator('.holl-preventa-active').first.wait_for()
        assert page.locator('.pv-step-nav').is_visible()
        assert page.locator('.pv-activity').evaluate('(el)=>el.compareDocumentPosition(document.getElementById("preventaForm")) & Node.DOCUMENT_POSITION_PRECEDING')
        assert 'Deberias cambiar de trabajo' not in page.locator('.holl-preventa').inner_text()
        assert page.locator('#noItemsRow').is_visible()
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.screenshot(path='/tmp/holl-preventa-mobile-redesign.png',full_page=True)
        page.locator('#submitPreventaBtn').click();page.locator('.swal2-confirm').click();assert not sent
        page.locator('[data-pv-step="preventaClientCard"]').click()
        page.locator('#clienteSearch').fill('Cliente');page.locator('#btnClienteSearch').click()
        page.locator('#clienteSearchResults button').first.click()
        assert 'Cliente' in page.locator('#clientePicked').inner_text()
        page.locator('[data-pv-step="preventaProductsCard"]').click()
        assert page.locator('[data-pv-step="preventaProductsCard"]').get_attribute('aria-current')=='step'
        page.locator('#productSearch').fill('LIB');page.locator('#btnProductSearch').click()
        page.locator('#productSearchResults button').first.click()
        row=page.locator('#itemsBody tr[data-item-row]');row.wait_for()
        row.locator('.item-qty').fill('3');row.locator('.item-qty').dispatch_event('change')
        assert page.locator('#draftBarTotalUsd').inner_text()=='6.00'
        assert row.locator('[name="item_cantidad"]').input_value()=='3'
        page.locator('[data-pv-step="preventaItemsCard"]').click()
        page.screenshot(path='/tmp/holl-preventa-cart-redesign.png',full_page=False)
        page.locator('#btnOpenCombo').click();page.locator('#combo-modal.show').wait_for()
        assert page.locator('#comboParentSearch').is_visible()
        page.locator('#combo-modal .btn-close').click();page.locator('#combo-modal').wait_for(state='hidden')
        page.emulate_media(reduced_motion='reduce')
        assert row.evaluate('(el)=>getComputedStyle(el).animationName')=='none'
        for width in (320,375,430,768):
            page.set_viewport_size({'width':width,'height':844});assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
        page.set_viewport_size({'width':390,'height':844})
        page.locator('#freezePreventaBtn').click();page.locator('.swal2-confirm').click()
        page.get_by_role('heading',name='Solicitud recibida').wait_for()
        assert len(sent)==1 and sent[0]['freeze_preventa']==['1']
        assert sent[0]['item_cantidad']==['3'] and sent[0]['item_precio_usd']==['2']
        page.goto('https://pv.test/m/preventas?result_preventa='+str(saved.id))
        page.get_by_role('heading',name='Preventa enviada correctamente').wait_for()
        assert page.locator('#preventaForm').count()==0
        assert page.locator('#newPreventaBtn').is_visible()
        page.screenshot(path='/tmp/holl-preventa-result-redesign.png',full_page=True)
        page.goto('https://pv.test/m/preventas?result_preventa='+str(frozen.id))
        page.get_by_role('heading',name='Preventa guardada como congelada').wait_for()
        assert 'panel de preventas' in page.locator('.result-hero').inner_text()
        assert not errors,errors;browser.close()
    print('PASS: nueva preventa guiada, historial debajo, cliente/búsqueda/carrito/total correctos, combos, bloqueo vacío, congelado, confirmación, 320–768px y movimiento reducido; solo base aislada.')
finally:fixture.tearDown()
