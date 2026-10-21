"""Browser checks against isolated fixture HTML/API; real inventory stays unchanged."""
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import test_shoe_kardex as fixture
from playwright.sync_api import sync_playwright
from app.routers import web as w
case=fixture.KardexTests();case.setUp()
try:
    html=w.report_kardex(case.request(),case.db,case.admin).body.decode()
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        page=browser.new_page(viewport={'width':390,'height':844});errors=[];selected=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        def report(route):
            if 'variante_id=1' in route.request.url:selected.append(parse_qs(urlsplit(route.request.url).query))
            route.fulfill(body=html,content_type='text/html')
        page.route('**/reports/kardex?*',report)
        def search(route):
            q=parse_qs(urlsplit(route.request.url).query)['q'][0]
            data=json.loads(w.report_kardex_items(case.request(),q,case.db,case.admin).body)
            route.fulfill(json=data)
        page.route('**/reports/kardex/items?*',search)
        page.goto('http://127.0.0.1:8001/reports/kardex?start_date=2026-10-19&end_date=2026-10-19')
        assert page.locator('select[name=bodega_id] option').count()==4
        page.get_by_role('button',name='Buscar y seleccionar artículo').click()
        search_input=page.locator('#kardex-item-search');search_input.wait_for(state='visible')
        page.wait_for_function("document.activeElement.id==='kardex-item-search'")
        search_input.fill('ZAP-1');page.locator('#kardex-item-results button').first.wait_for()
        search_input.press('ArrowDown')
        assert page.locator('#kardex-item-results button[aria-selected=true]').count()==1
        search_input.fill('Negro 38');page.wait_for_function("document.querySelectorAll('#kardex-item-results button').length===1")
        page.screenshot(path='/tmp/miss-kardex-modal-mobile.png',full_page=False)
        search_input.press('Enter');page.wait_for_url(lambda url: 'variante_id=1' in url)
        page.locator('#kardex-filters').wait_for()
        assert selected and selected[0]['variante_id']==['1'] and selected[0]['producto_id']==['1']
        assert selected[0]['start_date']==['2026-10-19']
        assert not errors,errors
        browser.close()
    print('PASS Firefox móvil: bodegas, modal, foco, búsqueda, flechas, Enter y filtros conservados. Sin modificar inventario real.')
finally:case.tearDown()
