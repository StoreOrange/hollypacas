"""Installation guide in local WebKit; no accounts or inventory."""
import socket,threading,time
from pathlib import Path
from unittest.mock import patch
from fastapi import FastAPI,Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from playwright.sync_api import sync_playwright
import uvicorn
from app.routers import web as w
root=Path(__file__).resolve().parents[1]
app=FastAPI();app.state.templates=Jinja2Templates(directory=str(root/'app/templates'))
app.mount('/static',StaticFiles(directory=str(root/'app/static')),name='static')
@app.get('/instalar')
def guide(request:Request):
    with patch.object(w,'get_active_company_key',return_value='hollywood_pacas'):return w.hollywood_install_page(request)
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
server=uvicorn.Server(uvicorn.Config(app,log_level='error',lifespan='off'))
thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
try:
    for _ in range(100):
        if server.started:break
        time.sleep(.02)
    with sync_playwright() as p:
        browser=p.webkit.launch(headless=True)
        iphone='Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 Version/18.0 Mobile/15E148 Safari/604.1'
        context=browser.new_context(user_agent=iphone,viewport={'width':390,'height':844},is_mobile=True,has_touch=True)
        page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(f'http://127.0.0.1:{port}/instalar')
        assert page.locator('[data-panel="0"]').is_visible()
        assert page.locator('#browser-help').is_hidden()
        for width in (320,375,390,430):
            page.set_viewport_size({'width':width,'height':844});assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
        page.set_viewport_size({'width':390,'height':844});page.screenshot(path='/tmp/hollywood-install-iphone.png',full_page=True)
        page.locator('#next').click();assert page.locator('[data-panel="1"]').is_visible()
        page.locator('.help summary').click();assert page.locator('.help p').is_visible()
        page.locator('#next').click();assert page.locator('[data-panel="2"]').is_visible()
        page.locator('#next').click();assert 'no instala' in page.locator('#guide-status').inner_text()
        page.locator('#previous').click();assert page.locator('[data-panel="1"]').is_visible()
        page.locator('[data-step="0"]').click();assert page.locator('[data-panel="0"]').is_visible()
        page.evaluate("Object.defineProperty(navigator,'clipboard',{value:{writeText:async(value)=>window.copied=value},configurable:true})")
        page.locator('footer [data-copy]').click();assert page.evaluate('window.copied').endswith('/instalar')
        page.evaluate("Object.defineProperty(navigator,'share',{value:async(data)=>{window.shared=data},configurable:true})")
        page.locator('#share-guide').click();assert page.evaluate('window.shared.url').endswith('/instalar')
        page.evaluate("Object.defineProperty(navigator,'clipboard',{value:null,configurable:true})")
        page.locator('footer [data-copy]').click();assert page.locator('#copy-fallback').is_visible()
        page.locator('[data-device="android"]').click();assert page.locator('#android-guide').is_visible()
        page.locator('[data-device="iphone"]').click();assert page.locator('#iphone-guide').is_visible()
        assert not errors,errors
        context.close()
        for ua,standalone,help_expected,android_expected in [(iphone+' WhatsApp',False,True,False),('Mozilla/5.0 Android',False,False,True),(iphone,True,False,False)]:
            context=browser.new_context(user_agent=ua,viewport={'width':390,'height':844},is_mobile=True)
            if standalone:context.add_init_script("Object.defineProperty(navigator,'standalone',{value:true})")
            page=context.new_page();page.goto(f'http://127.0.0.1:{port}/instalar')
            assert page.locator('#standalone-panel').is_visible()==standalone
            assert page.locator('#setup-panel').is_hidden()==standalone
            assert page.locator('#browser-help').is_visible()==help_expected
            if android_expected:assert page.locator('#android-guide').is_visible()
            context.close()
        browser.close()
        print('PASS WebKit: iPhone steps, 320–430px, help, copy/share, fallback, WhatsApp, Android and standalone mode. No database writes.')
finally:
    server.should_exit=True;thread.join(5);sock.close()
