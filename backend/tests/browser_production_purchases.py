"""Verify guided monetary purchases in an isolated SQLite fixture."""
import sys
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_accounting_registry import RegistryTests
from app.routers import web as w
fixture=RegistryTests();fixture.setUp()
try:
    account=w.CuentaContable(codigo='1112',nombre='Inventario de producción en proceso',tipo='BALANCE',naturaleza='DEBE',activo=True)
    fixture.db.add_all([account,w.CuentaContable(codigo='1104',nombre='Inventarios',tipo='BALANCE',naturaleza='DEBE',activo=True),w.CuentaContable(codigo='2102',nombre='Proveedores',tipo='BALANCE',naturaleza='HABER',activo=True)])
    fixture.db.add(w.AccountingJournal(code='COM',name='Compras',kind='PURCHASE',currency='CS',voucher_type_id=1,active=True))
    fixture.db.commit()
    with patch.object(w,'get_active_company_key',return_value='hollywood_pacas'),sync_playwright() as p:
        browser=p.firefox.launch(headless=True);page=browser.new_page(viewport={'width':1360,'height':1000});errors=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        def route(r):
            url=urlsplit(r.request.url)
            if url.path=='/accounting/entries':
                response=w.accounting_entries_page(fixture.req(),fixture.db,fixture.user)
                return r.fulfill(content_type='text/html',body=response.body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        page.route('**/*',route);page.goto('https://ledger.test/accounting/entries')
        page.locator('.line-account').first.wait_for()
        assert page.locator('#schoolOperation').input_value()=='production_inventory'
        assert page.locator('#schoolDebit').is_disabled()
        assert page.locator('#schoolDebit').input_value()==str(account.id)
        page.locator('#schoolAmount').fill('1000')
        page.locator('#entryCurrency').select_option('USD')
        page.locator('#schoolPayment').select_option('partial')
        page.locator('#schoolPaid').fill('300')
        page.locator('#schoolSource').select_option('manual')
        page.locator('#entryReference').fill('COMPRA-QA-001')
        page.locator('#schoolPreview').click()
        assert page.locator('#schoolApply').is_enabled(),page.locator('#schoolMessage').inner_text()
        page.locator('#schoolApply').click()
        assert '1112' in page.locator('.line-account').first.input_value()
        assert float(page.locator('.line-debit').first.input_value().replace(',',''))==1000
        assert page.locator('#entryCurrency').input_value()=='USD'
        assert page.locator('#entryJournal option:checked').inner_text()=='Compras · COM'
        assert float(page.locator('.line-credit').nth(1).input_value().replace(',',''))==300
        assert float(page.locator('.line-credit').nth(2).input_value().replace(',',''))==700
        assert not fixture.db.query(w.AccountingEntry).count()
        assert not errors,errors
        page.screenshot(path='/tmp/holl-production-purchase.png',full_page=True)
        browser.close()
    print('PASS: cuenta exclusiva, compra parcial 1000 USD = 300 + 700, moneda conservada, sin crear asientos ni stock durante la preparación.')
finally:fixture.tearDown()
