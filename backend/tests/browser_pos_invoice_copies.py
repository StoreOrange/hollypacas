"""Normal POS: render real ticket, simulate printing and verify the three labels."""
from pathlib import Path
import sys
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fitz
from playwright.sync_api import sync_playwright
from starlette.templating import Jinja2Templates
from tests.test_pos_browser import NativePosTests
from app.routers import web

fixture = NativePosTests(); fixture.setUp()
try:
    _, job = fixture.invoice()
    request = fixture.request()
    request.scope['query_string'] = b'copies=2&return_to=/sales'
    request.scope['app'] = SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
    html = web.sales_ticket_print(request, job.invoice_id, fixture.db, fixture.user).body.decode()
    invoice = fixture.db.get(web.VentaFactura, job.invoice_id)
    pdf, _ = web._build_pos_print_payload(invoice, None, 2)
    Path('/tmp/holl-pos-three-receipts.pdf').write_bytes(pdf)
    doc = fitz.open(stream=pdf, filetype='pdf')
    doc[2].get_pixmap(matrix=fitz.Matrix(2, 2)).save('/tmp/holl-pos-bodega-receipt.png')
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        context = browser.new_context()
        printed = []
        navigations = []
        context.expose_function('recordPrint', lambda snapshot: printed.append(snapshot))
        context.add_init_script('window.print=()=>{window.recordPrint({label:document.getElementById("pos-copy-label").textContent,text:document.querySelector(".ticket").innerText,invoiceSize:parseFloat(getComputedStyle(document.querySelector(".warehouse-invoice")).fontSize)});setTimeout(()=>dispatchEvent(new Event("afterprint")),10)}')
        def route(r):
            if r.request.is_navigation_request(): navigations.append(r.request.url)
            if '/ticket/print' in r.request.url: r.fulfill(content_type='text/html', body=html)
            else: r.fulfill(content_type='text/html', body='<h1>Ventas</h1>')
        context.route('**/*', route)
        page = context.new_page()
        page.goto('https://pos.test/sales/1/ticket/print?copies=2')
        page.wait_for_url(f'**/sales/{invoice.id}/ticket')
        assert not any(url.endswith('/sales') or '/sales?scan_ready=' in url for url in navigations), navigations
        assert [entry['label'] for entry in printed] == ['ORIGINAL', 'COPIA', 'EXCLUSIVA DE BODEGA'], printed
        for entry in printed[:2]:
            assert 'Precio:' in entry['text'] and 'Pagos aplicados' in entry['text'], entry
        third = printed[2]
        assert 'TOTAL BULTOS: 1' in third['text'] and invoice.numero in third['text'], third
        assert third['invoiceSize'] >= 24
        for forbidden in ('C$', '$', 'Precio:', 'Subtotal', 'Descuentos', 'Pagos aplicados', 'Efectivo', 'Vuelto', 'Saldo'):
            assert forbidden not in third['text'], (forbidden, third)
        for allow_close in (False, True):
            ctx = browser.new_context()
            receipts = []
            requests = []
            ctx.expose_function('recordPrint', lambda label: receipts.append(label))
            ctx.add_init_script("window.print=()=>{window.recordPrint(document.getElementById('pos-copy-label').textContent);setTimeout(()=>dispatchEvent(new Event('afterprint')),10)}")
            if not allow_close:
                ctx.add_init_script('window.close=()=>{}')
            def popup_route(r):
                if r.request.is_navigation_request(): requests.append(r.request.url)
                if '/ticket/print' in r.request.url:
                    r.fulfill(content_type='text/html', body=html)
                elif r.request.url.endswith('/existing-sales-form'):
                    r.fulfill(content_type='text/html', body="<input id='draft'><button id='receipt' onclick=\"window.open('/sales/1/ticket/print?copies=2','receipt')\">Comprobante</button>")
                else:
                    r.fulfill(content_type='text/html', body='<h1>PDF de factura</h1>')
            ctx.route('**/*', popup_route)
            main = ctx.new_page();main.goto('https://pos.test/existing-sales-form')
            main.locator('#draft').fill('BORRADOR SIN CAMBIOS')
            with main.expect_popup() as opened:
                main.locator('#receipt').click()
            popup = opened.value
            if allow_close:
                if not popup.is_closed(): popup.wait_for_event('close')
            else:
                popup.wait_for_url(f'**/sales/{invoice.id}/ticket')
            assert receipts == ['ORIGINAL', 'COPIA', 'EXCLUSIVA DE BODEGA'], receipts
            assert main.url == 'https://pos.test/existing-sales-form'
            assert main.locator('#draft').input_value() == 'BORRADOR SIN CAMBIOS'
            assert requests.count('https://pos.test/existing-sales-form') == 1, requests
            assert not any(url.endswith('/sales') or '/sales?scan_ready=' in url for url in requests), requests
            ctx.close()
        browser.close()
    print('PASS: normal POS prints original, copy and warehouse receipt once each, even with legacy copies=2 link; popup and blocked-close fallback preserve the existing form; fallback opens only PDF; no physical printing.')
finally:
    fixture.tearDown()
