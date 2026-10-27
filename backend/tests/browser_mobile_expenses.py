"""Real isolated mobile expense, assisted categorization, review, receipt and retry."""
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.async_api import async_playwright
from tests.test_mobile_expenses import MobileExpenseTests
from app.routers import mobile_expenses as routes,web as w
from app.models.sales import MobileExpense


async def main():
    fixture=MobileExpenseTests();fixture.setUp()
    try:
        errors=[];redirects=[];submissions=[]
        async with async_playwright() as p:
            browser=await p.firefox.launch(headless=True)
            page=await browser.new_page(viewport={'width':390,'height':844},device_scale_factor=1)
            page.set_default_timeout(12000);page.on('pageerror',lambda error:errors.append(str(error)))
            async def route(r):
                url=urlsplit(r.request.url)
                if url.path=='/m/gastos' and r.request.method=='POST':
                    data={key:values[-1] for key,values in parse_qs(r.request.post_data,keep_blank_values=True).items()}
                    submissions.append(data)
                    response=await routes.create(fixture.req(data=data),fixture.db,fixture.user)
                    if response.status_code==303:
                        redirects.append(response.headers['location'])
                        return await r.fulfill(content_type='text/html',body='<h1>Guardado</h1>')
                    return await r.fulfill(status=response.status_code,content_type='text/html',body=response.body)
                if url.path=='/m/gastos':
                    return await r.fulfill(content_type='text/html',body=routes.page(fixture.req(url.query),fixture.db,fixture.user).body)
                if url.path.endswith('/pdf'):
                    response=routes.pdf(fixture.req(),int(url.path.split('/')[-2]),fixture.db,fixture.user)
                    body=b''.join([chunk async for chunk in response.body_iterator])
                    return await r.fulfill(content_type='application/pdf',body=body)
                if url.path.startswith('/static/'):
                    file=Path('app'+url.path)
                    if file.is_file():return await r.fulfill(path=str(file))
                return await r.fulfill(content_type='application/json',body='{}')
            await page.route('**/*',route);await page.goto('https://expense.test/m/gastos')
            await page.locator('#expenseDescription').fill('Reparación urgente de la puerta de la bodega')
            await page.locator('#suggestExpenseType').click()
            assert await page.locator('#expenseCategory').input_value()=='repair'
            assert '6105' in await page.locator('#expenseAccountHint').text_content()
            await page.locator('#expenseCategory').select_option('extraordinary')
            await page.locator('#expenseAmount').fill('125.50')
            await page.locator('#expensePayment').select_option('cash:1')
            assert await page.locator('#expenseWarehouse').is_visible()
            assert await page.locator('#expensePaidBy').is_disabled()
            await page.locator('#expenseBeneficiary').fill('Servicio de prueba')
            await page.locator('#expenseReference').fill('FACT-EXT-27')
            assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            await page.screenshot(path='/tmp/holl-mobile-expense-form.png',full_page=True)
            await page.locator('#expenseSave').click()
            assert await page.locator('#expenseConfirmation').is_visible()
            assert fixture.db.query(MobileExpense).count()==0
            assert '125.50' in await page.locator('#expenseConfirmation').inner_text()
            await page.locator('#expenseSave').click();await page.get_by_role('heading',name='Guardado',exact=True).wait_for()
            assert len(redirects)==1 and 'saved=' in redirects[0],redirects
            expense=fixture.db.query(MobileExpense).one()
            assert expense.entry.lines[0].cuenta.codigo=='6901'
            assert fixture.db.query(w.CajaDiaria).one().saldo_cs==874.50
            retry=await routes.create(fixture.req(data=submissions[0]),fixture.db,fixture.user)
            assert retry.headers['location']==redirects[0]
            assert fixture.db.query(MobileExpense).count()==1
            assert fixture.db.query(w.CajaDiaria).one().saldo_cs==874.50
            await page.goto('https://expense.test'+redirects[0])
            await page.locator('#voucherPreviewModal.show').wait_for()
            await page.locator('#voucherPdfPages canvas').wait_for()
            await page.wait_for_function('!document.getElementById("voucherPdfPrint").disabled')
            assert await page.locator('#voucherPdfOpen').get_attribute('href')==f'/m/gastos/{expense.id}/pdf'
            await page.screenshot(path='/tmp/holl-mobile-expense-pdf-modal.png',full_page=True)
            await page.evaluate("""() => {
                window.__printedPages=0;
                new MutationObserver(()=>{const frame=document.getElementById('voucherPrintFrame');if(frame&&!frame.dataset.stub){frame.dataset.stub='1';frame.contentWindow.print=()=>window.__printedPages=frame.contentDocument.images.length;}}).observe(document.body,{childList:true});
            }""")
            await page.locator('#voucherPdfPrint').click();await page.wait_for_function('window.__printedPages>0')
            await page.locator('#voucherNext').click();await page.wait_for_function('!location.search.includes("saved")')
            await page.locator('#expensePayment').select_option('personal')
            assert await page.locator('#expensePaidBy').is_visible()
            assert await page.locator('#expenseWarehouse').is_disabled()
            assert 'No se descuenta' in await page.locator('#expensePaymentHint').inner_text()
            assert not errors,errors
            await browser.close()
        print('PASS: móvil 390px sin desborde, sugerencia por motivo, categoría confirmada, revisión, gasto extraordinario y caja una sola vez, PDF visible/imprimible e interfaz de reembolso.')
    finally:fixture.tearDown()


if __name__=='__main__':asyncio.run(main())
