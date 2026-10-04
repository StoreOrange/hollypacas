"""Run with Playwright available; uses isolated DOM and mocked catalog, no real sales."""
from pathlib import Path
from playwright.sync_api import sync_playwright

source = (Path(__file__).parents[1] / 'app/templates/sales_zapatos.html').read_text()
def block(start, end):
    return source[source.index(start):source.index(end, source.index(start))]

setup = '''
const productSearch=document.querySelector('#search'),itemsTable=document.querySelector('tbody');
const emptyItems=null,layout=null,preventaIdInput=null,monedaField={value:'USD'},rateToday=36;
const scannerState=document.querySelector('#state'),scannerLastCode=null,paymentModal=null,comboModal=null;
let searchTimer,productNavMode='search',activeProductRows=[];
const updateTotals=()=>{},refreshVisibleProductBalances=()=>{},refreshVariantBalances=()=>{},updateRowSubtotalLabels=()=>{};
const formatMoney=(n)=>Number(n).toFixed(2);
const buildVariantSearchUrl=q=>'http://scanner.test/variants?q='+q;
const fetchProducts=async()=>{activeProductRows=[{dataset:{code:'PARTIAL-MATCH'}}];};
const refreshActiveRows=()=>{},setActiveProduct=()=>{},addActiveProduct=()=>{throw Error('Wrong product selected');};
window.alerts=[];const Swal={fire:config=>{alerts.push(config);return Promise.resolve();}};
'''
script = setup + block('  const addItemRow =', '  const isCreditSale =') + block('  const findInvoiceRow =', '  const parseReservedDetails =') + block('  const addVariantToInvoice =', '  const renderVariantSearchRows =') + block('  const SCAN_MIN_LEN =', '  {% if error %}')
script += '\nwindow.done=()=>!barcodeQueueRunning;window.addVariant=addVariantToInvoice;window.scan=queueBarcode;window.keepFocus=focusScannerInput;'

with sync_playwright() as p:
    browser=p.chromium.launch(executable_path='/Applications/Brave Browser.app/Contents/MacOS/Brave Browser',headless=True)
    page=browser.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
    page.set_content('<input id="search"><input id="payment"><span id="state"></span><table><tbody></tbody></table>')
    def catalog(route):
        code=route.request.url.split('q=')[-1]
        if code=='ERROR':
            route.abort();return
        items=[] if code=='UNKNOWN' else [dict(producto_id=1,variant_id=2 if code=='SHOE002' else 1,scan_code=code,
                cod_variante=code,cod_producto='SHOE',descripcion='Zapato',color='Negro',talla='38',
                existencia=0 if code=='ZERO' else 3,selected_price_usd=10,selected_price_cs=360)]
        route.fulfill(json={'ok':True,'items':items})
    page.route('http://scanner.test/**',catalog)
    page.add_script_tag(content=script)
    page.locator('#search').focus()
    def scan(code):
        page.keyboard.type(code,delay=5);page.keyboard.press('Enter');page.wait_for_function('done()')
        page.wait_for_timeout(30)
        assert page.locator('#search').evaluate('e=>e===document.activeElement')
    scan('SHOE001')
    assert page.locator('tr').count()==1
    scan('SHOE001')
    assert page.locator('tr').count()==1 and page.locator('.row-qty').input_value()=='2'
    scan('SHOE001');scan('SHOE001')
    assert page.locator('.row-qty').input_value()=='3'
    assert page.evaluate('alerts.at(-1).title')=='Saldo insuficiente'
    scan('SHOE002');assert page.locator('tr').count()==2
    scan('ZERO');assert page.locator('tr').count()==2
    assert page.evaluate('alerts.at(-1).title')=='Sin existencia'
    scan('UNKNOWN');assert page.locator('tr').count()==2
    scan('ERROR');assert page.locator('tr').count()==2
    page.evaluate("itemsTable.innerHTML='';scan('SHOE001');scan('SHOE001');scan('SHOE001');scan('SHOE001')")
    page.wait_for_function('done()');assert page.locator('tr').count()==1
    assert page.locator('.row-qty').input_value()=='3'
    # Editing payment must not trigger a scan or lose focus.
    page.locator('#payment').focus();page.keyboard.type('12345',delay=5);page.keyboard.press('Enter')
    page.evaluate('keepFocus()');assert page.locator('#payment').evaluate('e=>e===document.activeElement')
    assert page.locator('tr').count()==1
    # A first manual addition exceeding stock must also be refused.
    assert not page.evaluate("addVariant({producto_id:9,variant_id:9,existencia:1},2)")
    assert not errors,errors
    print('PASS: focus after scans/errors; duplicate variants merge; distinct variants remain separate; stock cap; rapid queue; no partial matches; manual editing respected.')
    browser.close()
