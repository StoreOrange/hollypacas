"""Keyboard workflow, real SQLite posting and automatic PDF modal; no production writes."""
import sys
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_accounting_registry import RegistryTests
from app.routers import web as w

fixture=RegistryTests();fixture.setUp()
try:
    fixture.db.add_all([
        w.CuentaContable(codigo='1112',nombre='Inventario de producción en proceso',tipo='BALANCE',naturaleza='DEBE',activo=True),
        w.CuentaContable(codigo='2102',nombre='Proveedores',tipo='BALANCE',naturaleza='HABER',activo=True),
        w.AccountingJournal(code='COM',name='Compras',kind='PURCHASE',currency='CS',voucher_type_id=1,active=True),
    ]);fixture.db.commit()
    redirects=[];errors=[];pdf_responses=[]
    with patch.object(w,'get_active_company_key',return_value='hollywood_pacas'),sync_playwright() as p:
        browser=p.firefox.launch(headless=True);page=browser.new_page(viewport={'width':1440,'height':1100})
        page.on('pageerror',lambda error:errors.append(str(error)));page.set_default_timeout(7000)
        def route(r):
            url=urlsplit(r.request.url);req=fixture.req(url.query)
            req.scope.update(path=url.path,scheme='https',server=('ledger.test',443))
            if url.path=='/accounting/entries' and r.request.method=='POST':
                form=parse_qs(r.request.post_data,keep_blank_values=True)
                kwargs={key:form.get(key,[''])[0] for key in ['fecha','tipo_id','branch_id','referencia','descripcion','moneda','tasa_sistema','tasa_cambio','estado_comprobante','edit_entry_id','journal_id','cliente_id_contable','proveedor_id_contable','vencimiento_contable','manual_operation_key']}
                kwargs['tipo_id']=int(kwargs['tipo_id'])
                for key in ['line_cuenta_id','line_descripcion','line_debe','line_haber']:kwargs[key]=form.get(key,[])
                result=w.accounting_entries_create(req,db=fixture.db,user=fixture.user,**kwargs)
                redirects.append(result.headers['location'])
                return r.fulfill(content_type='text/html',body='<h1>Guardado</h1>')
            if url.path=='/accounting/entries':
                response=w.accounting_entries_page(req,fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.endswith('/pdf'):
                entry=fixture.db.get(w.AccountingEntry,int(url.path.split('/')[-2]))
                body=w._build_accounting_entry_pdf_bytes(entry,{'legal_name':'Holl Pacas','logo_url':'/static/logo_hollywood.png'})
                pdf_responses.append(body)
                return r.fulfill(content_type='application/pdf',body=body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        page.route('**/*',route);page.goto('https://ledger.test/accounting/entries')
        page.locator('.line-account').first.wait_for()
        page.locator('#entryDate').fill('2026-10-12');page.locator('#entryDate').press('Enter')
        assert page.locator('#entryReference').evaluate('(el)=>el===document.activeElement')
        page.locator('#entryReference').fill('FAC-GUIADA-27');page.locator('#entryReference').press('Enter')
        assert page.locator('#entryDescription').evaluate('(el)=>el===document.activeElement')
        page.locator('#entryDescription').fill('Compra de pacas para producción en proceso')
        for current,next_id in [('entryDescription','entrySupplier'),('entrySupplier','entryBranch'),('entryBranch','entryCurrency'),('entryCurrency','schoolOperation'),('schoolOperation','schoolAmount')]:
            page.locator('#'+current).press('Enter');assert page.locator('#'+next_id).evaluate('(el)=>el===document.activeElement'),next_id
        page.locator('#schoolAmount').fill('1000');page.locator('#schoolAmount').press('Enter')
        page.locator('#schoolPayment').select_option('credit');page.locator('#schoolPayment').press('Enter')
        assert page.locator('#schoolCredit').evaluate('(el)=>el===document.activeElement')
        page.locator('#schoolCredit').press('Shift+Enter')
        assert page.locator('#schoolPayment').evaluate('(el)=>el===document.activeElement')
        page.locator('#schoolPayment').press('Enter');page.locator('#schoolCredit').press('Enter')
        page.locator('#schoolSource').select_option('manual');page.locator('#schoolSource').press('Enter')
        assert page.locator('#schoolPreview').evaluate('(el)=>el===document.activeElement')
        page.locator('#schoolPreview').press('Enter')
        assert page.locator('#schoolApply').evaluate('(el)=>el===document.activeElement')
        page.locator('#schoolApply').press('Enter')
        assert page.locator('#balanceStatus').inner_text()=='Cuadrado · listo para publicar'
        page.locator('.line-credit').nth(1).fill('900')
        assert page.locator('#publishEntry').is_disabled()
        page.locator('.line-credit').nth(1).fill('1000')
        page.locator('.line-credit').nth(1).press('Control+Enter')
        assert page.locator('#publishEntry').evaluate('(el)=>el===document.activeElement')
        page.screenshot(path='/tmp/holl-guided-accounting.png',full_page=True)
        page.locator('#publishEntry').press('Enter');page.get_by_role('heading',name='Guardado',exact=True).wait_for()
        assert len(redirects)==1 and 'pdf_entry_id=' in redirects[0],redirects
        entry=fixture.db.query(w.AccountingEntry).one()
        assert entry.estado=='POSTEADO' and entry.referencia=='FAC-GUIADA-27'
        assert str(entry.fecha)=='2026-10-12'
        page.goto('https://ledger.test'+redirects[0],wait_until='domcontentloaded')
        page.locator('#voucherPreviewModal.show').wait_for()
        page.locator('#voucherPdfPages canvas').wait_for()
        page.wait_for_function('!document.getElementById("voucherPdfPrint").disabled')
        assert page.locator('#voucherPdfPages canvas').count()>=1
        assert page.locator('#voucherPdfOpen').get_attribute('href')==f'/accounting/entries/{entry.id}/pdf'
        assert page.locator('#voucherPdfPrint').is_enabled()
        assert pdf_responses and pdf_responses[-1].startswith(b'%PDF')
        page.screenshot(path='/tmp/holl-voucher-pdf-modal.png',full_page=True)
        page.evaluate("""() => {
            window.__printedPages=0;
            new MutationObserver(()=>{
                const frame=document.getElementById('voucherPrintFrame');
                if(frame&&!frame.dataset.printStub){
                    frame.dataset.printStub='1';
                    frame.contentWindow.print=()=>window.__printedPages=frame.contentDocument.images.length;
                }
            }).observe(document.body,{childList:true});
        }""")
        page.locator('#voucherPdfPrint').click()
        page.wait_for_function('window.__printedPages>0')
        assert page.evaluate('window.__printedPages')==page.locator('#voucherPdfPages canvas').count()
        page.locator('#voucherNext').click();page.locator('#voucherPreviewModal').wait_for(state='hidden')
        page.wait_for_function('!location.search.includes("pdf_entry_id")')
        assert page.locator('#entryDate').evaluate('(el)=>el===document.activeElement')
        page.set_viewport_size({'width':390,'height':844})
        assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth')
        page.screenshot(path='/tmp/holl-guided-accounting-mobile.png',full_page=True)
        assert not errors,errors
        browser.close()
    print('PASS: fecha/referencia/concepto únicos, Enter/Shift+Enter, cuenta separada, cuadre protegido, publicación real aislada y modal PDF automático con impresión y siguiente registro.')
finally:fixture.tearDown()
