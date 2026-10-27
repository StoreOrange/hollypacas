"""Local visual/interaction QA: no live database, Git or deployment operations."""
import sys
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from tests.test_mobile_expenses import MobileExpenseTests
from app.routers import web as w,mobile_expenses as expenses
from app.main import app

fixture=MobileExpenseTests();fixture.setUp()
try:
    def home(company='hollywood_pacas',permissions=None):
        req=fixture.req();req.scope['path']='/home';req.state.branding={'company_key':company,'trade_name':'Holl Pacas','legal_name':'Hollywood Pacas'}
        if permissions is not None:req.state.permission_names=set(permissions)
        return app.state.templates.TemplateResponse('home.html',{'request':req,'user':fixture.user,'version':'local',
            'home_preventas':[],'shoe_home':None,'home_laboratory_notifications':[],'can_view_laboratory_notifications':False,
            'sales_interface_code':'','comestibles_theme_enabled':False})
    with sync_playwright() as p:
        browser=p.firefox.launch(headless=True)
        page=browser.new_page(viewport={'width':390,'height':844});errors=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        def route(r):
            url=urlsplit(r.request.url)
            if url.path=='/home':return r.fulfill(content_type='text/html',body=home().body)
            if url.path=='/limited':return r.fulfill(content_type='text/html',body=home(permissions={'menu.sales.preventas.mobile','access.sales.preventas.mobile'}).body)
            if url.path=='/other-company':return r.fulfill(content_type='text/html',body=home(company='bdzapatos').body)
            if url.path=='/m/gastos':return r.fulfill(content_type='text/html',body=expenses.page(fixture.req(),fixture.db,fixture.user).body)
            if url.path.startswith('/static/'):
                file=Path('app'+url.path)
                if file.is_file():return r.fulfill(path=str(file))
            return r.fulfill(content_type='application/json',body='{}')
        page.route('**/*',route);page.goto('https://local.test/home');page.locator('#hollHomeResult').wait_for()
        assert page.locator('.home-mobile-hero').count()==0
        assert page.locator('.holl-home').is_visible()
        assert page.locator('.odoo-header').is_hidden()
        assert page.locator('.holl-home a[href="/m/gastos"]').is_visible()
        assert page.locator('.holl-home a[href="/reports"]').count()==0
        assert page.locator('.holl-home a[href="/accounting"]').count()==0
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.screenshot(path='/tmp/holl-home-mobile-local.png',full_page=True)
        page.locator('#hollHomeSearch').fill('quiero registrar un gasto')
        visible=page.locator('.holl-home-action:visible');assert visible.count()==1
        assert visible.first.get_attribute('href')=='/m/gastos'
        page.screenshot(path='/tmp/holl-home-search-local.png',full_page=True)
        page.locator('#hollHomeSearch').press('Enter');page.locator('#mobileExpenseForm').wait_for()
        page.goto('https://local.test/home')
        page.locator('[data-home-filter="view"]').click()
        assert page.locator('[data-home-kind="register"]:visible').count()==0
        assert page.locator('[data-home-kind="view"]:visible').count()>0
        page.locator('#hollHomeSearch').fill('noexisteunmodulo')
        assert page.locator('#hollHomeEmpty').is_visible()
        page.locator('#hollHomeReset').click()
        assert page.locator('[data-home-filter="all"]').get_attribute('aria-pressed')=='true'
        page.locator('.holl-home-menu').click();page.locator('#mobileSidebar.show').wait_for()
        page.locator('#mobileSidebar .btn-close').click();page.locator('#mobileSidebar').wait_for(state='hidden')
        page.emulate_media(reduced_motion='reduce')
        assert page.locator('.holl-home-action').first.evaluate('(el)=>getComputedStyle(el).animationName')=='none'
        for width in (320,375,430,768):
            page.set_viewport_size({'width':width,'height':844})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
        page.set_viewport_size({'width':1440,'height':1000})
        assert page.locator('.holl-home').is_hidden()
        assert page.locator('.odoo-header').is_visible()
        page.screenshot(path='/tmp/holl-home-desktop-local.png',full_page=True)
        page.set_viewport_size({'width':390,'height':844});page.goto('https://local.test/limited')
        assert page.locator('.holl-home a[href="/m/gastos"]').count()==0
        assert page.locator('.holl-home-action').count()==1
        page.goto('https://local.test/other-company')
        assert page.locator('.holl-home').count()==0
        assert page.locator('.home-mobile-hero').is_visible()
        assert not errors,errors
        browser.close()
    print('PASS: home móvil limpio sin recuadro azul, búsqueda asistida, enlace a gastos, filtros, menú, permisos, 320–768px sin desborde, movimiento reducido, escritorio y otras empresas conservados. Solo datos locales sintéticos.')
finally:fixture.tearDown()
