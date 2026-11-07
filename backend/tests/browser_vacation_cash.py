"""Isolated UI verification: cash defaults, readable warehouses and draft consent."""
import sys,threading,socket,time
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tests.test_payroll_start_date import StartDateTests
from app.routers import payroll_vacations as route,web as w,payroll
from app.models.payroll import PayrollSettlement
from app.core.payroll_benefits import balance
from fastapi.staticfiles import StaticFiles
from playwright.sync_api import sync_playwright
import uvicorn
case=StartDateTests();case.setUp()
mock=patch.object(route,'_browser_admin',return_value=case.user);mock.start()
try:
    db=case.db;profile=case.profile
    available=balance(db,profile,case.today)
    bonus=payroll._benefit_preview(profile,profile.employee,case.today)['bonus_amount']
    db.add(PayrollSettlement(employee_id=profile.employee_id,branch_id=case.fixture.branch.id,termination_date=case.today,reason_code='RENUNCIA',reason_detail='Borrador QA',salary_snapshot=profile.monthly_salary,vacation_days=available['days'],vacation_amount=available['amount'],bonus_amount=bonus,gross_amount=available['amount']+bonus,net_amount=available['amount']+bonus,status='DRAFT'))
    warehouse=w.Bodega(code='central',name='Central',branch_id=case.fixture.branch.id,activo=True);db.add(warehouse)
    voucher=db.query(w.AccountingVoucherType).filter_by(code='EGRESO').one()
    journal=w.AccountingJournal(code='CAJA',name='Caja',kind='CASH',voucher_type_id=voucher.id,main_account_id=case.fixture.accounts[6].id,active=True);db.add(journal);db.commit()
    app=case.client.app;app.include_router(route.router)
    app.mount('/static',StaticFiles(directory=str(Path(__file__).resolve().parents[1]/'app/static')),name='static')
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(app,log_level='error',lifespan='off'))
    thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
    try:
        for _ in range(200):
            if server.started:break
            time.sleep(.02)
        with sync_playwright() as p:
            browser=p.firefox.launch(headless=True)
            page=browser.new_page(viewport={'width':390,'height':844});errors=[]
            page.on('pageerror',lambda e:errors.append(str(e)))
            page.goto(f'http://127.0.0.1:{port}/payroll/vacations')
            form=page.locator('#vacationForm')
            form.locator('[name=employee_id]').select_option(str(profile.employee_id))
            form.locator('[name=kind]').select_option('CASH')
            form.locator('[name=days]').fill('1')
            form.locator('[name=reason]').fill('Pago de vacaciones autorizado')
            assert form.locator('[name=journal_id]').input_value()==str(journal.id)
            assert form.locator('[name=cash_bodega_id]').input_value()==str(warehouse.id)
            assert 'Central' in form.locator('[name=cash_bodega_id] option:checked').inner_text()
            assert page.locator('#vacationDraftAck').is_visible()
            assert not form.evaluate('(f)=>f.checkValidity()')
            page.locator('#vacationDraftAck').check()
            assert form.evaluate('(f)=>f.checkValidity()')
            page.screenshot(path='/tmp/holl-vacation-cash-mobile.png',full_page=True)
            page.set_viewport_size({'width':1440,'height':1000})
            page.screenshot(path='/tmp/holl-vacation-cash-desktop.png',full_page=True)
            assert not errors,errors
            browser.close()
        print('PASS cash form: warehouse names/defaults and explicit draft adjustment consent. No payment submitted.')
    finally:server.should_exit=True;thread.join(timeout=5);sock.close()
finally:mock.stop();case.tearDown()
