"""Pantalla contable real con corrección en SQLite aislada, sin datos de producción."""
import sys
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_accounting_registry import RegistryTests
from app.routers import web as w,accounting_registry as registry
fixture=RegistryTests();fixture.setUp()
try:
    original=fixture.entry('POSTEADO');original.referencia='FAC-ALQ-778';original.descripcion='Pago registrado';fixture.db.commit()
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        ctx=browser.new_context(viewport={'width':1440,'height':1000})
        errors=[];redirects=[]
        ctx.on('page',lambda tab:tab.on('pageerror',lambda error:errors.append(str(error))))
        def route(r):
            url=urlsplit(r.request.url);request=fixture.req(url.query)
            request.scope.update(path=url.path,scheme='https',server=('ledger.test',443))
            if url.path=='/accounting/comprobantes':
                response=registry.page(request,fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.endswith('/correct') and r.request.method=='POST':
                form=parse_qs(r.request.post_data)
                response=w.accounting_entry_correct(request,original.id,form['motivo'][0],form['fecha_reversa'][0],fixture.db,fixture.user)
                redirects.append(response.headers['location'])
                return r.fulfill(content_type='text/html',body='<h1>Correccion preparada</h1>')
            if url.path=='/accounting/entries':
                response=w.accounting_entries_page(request,fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        ctx.route('**/*',route)
        page=ctx.new_page();page.set_default_timeout(7000);page.add_init_script('window.__qaErrors=[];window.addEventListener("error",e=>window.__qaErrors.push(e.message))');page.goto('https://ledger.test/accounting/comprobantes?q=ALQ-778&amount=100')
        page.get_by_text('1 comprobantes encontrados',exact=True).wait_for()
        page.get_by_role('button',name='Detalle',exact=True).click()
        page.locator('#registryEntry'+str(original.id)+'.show').wait_for()
        assert '1101' in page.locator('#registryEntry'+str(original.id)).inner_text()
        page.locator('#registryEntry'+str(original.id)+' .btn-close').click()
        page.locator('#registryEntry'+str(original.id)).wait_for(state='hidden')
        page.locator('.modal-backdrop').wait_for(state='detached')
        page.screenshot(path='/tmp/holl-accounting-registry.png',full_page=True)
        page.get_by_role('button',name='Corregir',exact=True).click()
        modal=page.locator('#registryCorrect'+str(original.id))
        modal.locator('textarea').fill('Cuenta incorrecta en comprobante de prueba')
        page.locator('#registryCorrect'+str(original.id)+' .modal-content').screenshot(path='/tmp/holl-accounting-correction.png',animations='disabled')
        modal.get_by_role('button',name='Revertir y abrir borrador').click()
        page.get_by_role('heading',name='Correccion preparada').wait_for()
        assert len(redirects)==1 and 'edit_entry_id=' in redirects[0],redirects
        # Una nueva navegación permite interceptar el destino del 303 sin DNS real.
        page.goto('https://ledger.test'+redirects[0],wait_until='domcontentloaded')
        page.locator('.line-debit').first.wait_for(timeout=5000)
        assert page.locator('#entryReference').input_value()=='FAC-ALQ-778'
        assert page.locator('.line-debit').first.input_value()=='100.00'
        assert page.locator('.line-account').first.is_editable()
        assert fixture.db.get(w.AccountingEntry,original.id).estado=='REVERSADO'
        assert fixture.db.query(w.AccountingEntry).filter_by(estado='BORRADOR').count()==1
        assert not errors,errors
        browser.close()
    print('PASS: búsqueda por referencia y monto, modal de cuentas, confirmación de corrección y apertura del borrador editable con el original conservado.')
finally:
    fixture.tearDown()
