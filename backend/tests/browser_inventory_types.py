"""Pantallas reales con DB de prueba; valida filtros y modal directo del laboratorio."""
import sys
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_inventory_type_report import InventoryTypeReportTests
from app.routers import web as w
fixture=InventoryTypeReportTests();fixture.setUp()
try:
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        ctx=browser.new_context(viewport={'width':1440,'height':1000})
        errors=[]
        ctx.on('page',lambda page:page.on('pageerror',lambda error:errors.append(str(error))))
        def route(r):
            url=urlsplit(r.request.url)
            request=fixture.request(url.query)
            request.scope.update(scheme='https',server=('report.test',443),path=url.path)
            if url.path=='/reports/movimientos-tipo':
                with patch.object(w,'_utility_branch_ids',return_value={1,2}):response=w.report_inventory_types(request,fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path=='/inventory/laboratorios-produccion':
                response=w.inventory_production_laboratories_page(request,fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            r.fulfill(body='')
        ctx.route('**/*',route)
        page=ctx.new_page()
        page.goto('https://report.test/reports/movimientos-tipo?start_date=2026-10-20&end_date=2026-10-20')
        page.get_by_role('heading',name='Ingresos y egresos por tipo',exact=True).wait_for()
        assert page.locator('#movement-warehouse').input_value()=='1'
        page.screenshot(path='/tmp/inventory-types-report.png',full_page=True)
        with page.expect_popup() as opened:
            page.get_by_role('link',name='LAB-TEST').first.click()
        lab=opened.value
        lab.locator('#labDetail1.show').wait_for(state='visible')
        assert 'focus_laboratorio=1' in lab.url
        assert lab.locator('#labDetail1 .modal-title').inner_text().strip()=='LAB-TEST'
        lab.screenshot(path='/tmp/inventory-types-lab.png',full_page=False)
        page.locator('#movement-branch').select_option('2')
        assert page.locator('#movement-warehouse').input_value()=='2'
        page.get_by_role('button',name='Consultar').click()
        page.wait_for_url('**/reports/movimientos-tipo?**branch_id=2**')
        assert page.get_by_text('Sin laboratorio',exact=True).count()==1
        assert page.get_by_role('link',name='LAB-TEST').count()==0
        assert not errors,errors
        browser.close()
    print('PASS: resumen y detalle por tipo, cambio de sucursal/bodega y enlace que abre el modal real de laboratorio.')
finally:
    fixture.tearDown()
