"""QA móvil con pantalla real, base aislada y envío interceptado; sin facturas reales."""
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_mobile_discount_promotions import MobileDiscountTests
from app.routers import web as w
from app.main import app

fixture=MobileDiscountTests();fixture.setUp()
try:
    request=fixture.request()
    request.scope.update(app=app,router=app.router,scheme='https',server=('pos.test',443),path='/m/preventas')
    request.state.branding={'company_key':'hollywood_pacas'}
    html=w.mobile_preventas_page(request,fixture.db,fixture.user).body.decode()
    promotions=w.mobile_discount_promotions(request,fixture.db,fixture.user).body
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        context=browser.new_context(viewport={'width':390,'height':844},has_touch=True)
        sent=[];errors=[]
        def route(r):
            path=urlsplit(r.request.url).path
            if path=='/m/preventas' and r.request.method=='GET':return r.fulfill(content_type='text/html',body=html)
            if path=='/m/preventas' and r.request.method=='POST':
                sent.append(parse_qs(r.request.post_data or ''))
                return r.fulfill(content_type='text/html',body='<h1>Preventa recibida</h1>')
            if path=='/m/preventas/discount-promotions':return r.fulfill(content_type='application/json',body=promotions)
            if path.startswith('/static/'):
                file=Path('app'+path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        context.route('**/*',route)
        page=context.new_page();page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto('https://pos.test/m/preventas')
        page.locator('#mobileDiscountOpen').click()
        page.get_by_role('button',name='Agregar paquete de 2 unidades').wait_for()
        page.locator('#mobileDiscountModal .modal-content').screenshot(path='/tmp/mobile-discount-modal.png',animations='disabled')
        page.get_by_role('button',name='Agregar paquete de 2 unidades').click()
        row=page.locator('tr[data-combo-group^="discount2-"]')
        assert row.count()==1
        assert row.locator('.item-qty').is_disabled()
        assert row.locator('input[name=item_cantidad]').input_value()=='2'
        assert row.locator('input[name=item_precio_usd]').input_value()=='1.5'
        # El formulario normal lleva la promoción, sin endpoint de facturación paralelo.
        values=page.locator('#preventaForm').evaluate('(form)=>Object.fromEntries(new FormData(form))')
        assert values['item_role']=='discount'
        assert values['item_combo_group'].startswith('discount2-1-')
        assert values['item_cantidad']=='2'
        page.locator('#mobileDiscountModal').wait_for(state='hidden')
        row.scroll_into_view_if_needed()
        page.screenshot(path='/tmp/mobile-discount-promotion.png',full_page=False,animations='disabled')
        page.locator('#submitPreventaBtn').click()
        page.locator('.swal2-confirm').click()
        page.get_by_role('heading',name='Preventa recibida').wait_for()
        assert len(sent)==1,sent
        assert sent[0]['item_cantidad']==['2']
        assert sent[0]['item_role']==['discount']
        assert sent[0]['freeze_preventa']==['0']
        assert not errors,errors
        browser.close()
    print('PASS móvil 390px: promoción visible, precio correcto, cantidad 2 bloqueada y metadatos en el formulario de preventa.')
finally:
    fixture.tearDown()
