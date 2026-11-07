"""Review and confirm employment correction in mobile/desktop isolated fixtures."""
import sys,threading,socket,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tests.test_payroll_start_date import StartDateTests
from fastapi.staticfiles import StaticFiles
from playwright.sync_api import sync_playwright
import uvicorn

case=StartDateTests();case.setUp()
# TestClient and live fixture server use the same fixture app.
app=case.client.app
app.mount('/static',StaticFiles(directory=str(Path(__file__).resolve().parents[1]/'app/static')),name='static')
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
server=uvicorn.Server(uvicorn.Config(app,log_level='error',lifespan='off'))
thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
try:
    for _ in range(200):
        if server.started:break
        time.sleep(.02)
    assert server.started
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        page=browser.new_page(viewport={'width':390,'height':844});errors=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(f'http://127.0.0.1:{port}{case.path}')
        page.locator('#hire-date').fill('2026-08-01')
        page.locator('#hire-date').press('Tab')
        page.get_by_role('button',name='Comparar y revisar cálculos').click()
        try:page.get_by_role('heading',name='2. Comparación de prestaciones',exact=True).wait_for(timeout=5000)
        except Exception:
            print('BODY',page.url,page.locator('body').inner_text(),flush=True);raise
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.screenshot(path='/tmp/holl-hire-date-mobile.png',full_page=True)
        assert case.profile.contract_start.isoformat()=='2026-09-01'
        page.set_viewport_size({'width':1440,'height':1100})
        page.screenshot(path='/tmp/holl-hire-date-desktop.png',full_page=True)
        page.locator('#hire-reason').fill('Fecha verificada contra el contrato del trabajador')
        page.locator('#hire-reviewed').check()
        page.get_by_role('button',name='Guardar fecha y recalcular').click()
        page.get_by_text('Fecha corregida y cálculos actualizados.',exact=False).wait_for()
        assert case.profile.contract_start.isoformat()=='2026-08-01'
        assert page.get_by_text('Historial de correcciones',exact=True).count()==1
        assert not errors,errors
        browser.close()
    print('PASS mobile/desktop comparison and reviewed atomic confirmation on isolated employee fixture')
finally:
    server.should_exit=True;thread.join(timeout=5);sock.close();case.tearDown()
