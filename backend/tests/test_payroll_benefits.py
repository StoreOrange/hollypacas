import unittest
from datetime import date
from decimal import Decimal
from app.core.payroll_benefits import month_days, money


class BenefitCalendarTests(unittest.TestCase):
    def test_complete_months(self):
        for year in (2024,2025,2026):
            self.assertEqual(month_days(date(year,1,1),date(year,6,30)),Decimal(15))
            self.assertEqual(month_days(date(year,1,1),date(year,12,31)),Decimal(30))

    def test_february_leap_year(self):
        self.assertEqual(month_days(date(2024,2,1),date(2024,2,29)),Decimal('2.5'))
        self.assertEqual(month_days(date(2025,2,1),date(2025,2,28)),Decimal('2.5'))

    def test_partial_month_and_inclusive_dates(self):
        self.assertEqual(month_days(date(2026,4,1),date(2026,4,15)),Decimal('1.25'))
        self.assertEqual(month_days(date(2026,4,30),date(2026,5,1)),Decimal('2.5')/30+Decimal('2.5')/31)

    def test_no_employment(self):
        self.assertEqual(month_days(None,date(2026,1,1)),0)
        self.assertEqual(month_days(date(2026,2,1),date(2026,1,1)),0)

    def test_pay_rounds_half_up(self):
        self.assertEqual(money(Decimal('1000.005')),Decimal('1000.01'))


class BenefitLedgerTests(unittest.TestCase):
    """Base aislada en memoria: los servicios publican asientos reales, sin tocar empresas."""
    def setUp(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from app.database import Base
        from app.routers import web
        from app.models.payroll import PayrollEmployeeProfile,PayrollBenefitSetting,PayrollAccountingSetting
        from app.models.attendance import HREmployee
        self.web=web
        self.engine=create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db=Session(self.engine,autoflush=False)
        self.today=date(2026,10,14)
        from unittest.mock import patch
        self.clock=patch.object(web,'local_today',return_value=self.today);self.clock.start()
        branch=web.Branch(code='QA',name='Sucursal prueba',activo=True);self.db.add(branch);self.db.flush();self.branch=branch
        self.accounts=[]
        for code,nature,kind in [('6208','DEBE','RESULTADO'),('6209','DEBE','RESULTADO'),('2113','HABER','BALANCE'),('21131','HABER','BALANCE'),('21132','HABER','BALANCE'),('21133','HABER','BALANCE'),('1102','DEBE','BALANCE')]:
            row=web.CuentaContable(codigo=code,nombre=code,tipo=kind,naturaleza=nature,activo=True);self.db.add(row);self.db.flush();self.accounts.append(row)
        for code in ('DIARIO','EGRESO'):
            self.db.add(web.AccountingVoucherType(code=code,nombre=code,prefijo=code,activo=True))
        self.db.flush()
        voucher=self.db.query(web.AccountingVoucherType).filter_by(code='EGRESO').one()
        self.bank=web.AccountingJournal(code='BANK',name='Banco prueba',kind='BANK',voucher_type_id=voucher.id,main_account_id=self.accounts[6].id,active=True);self.db.add(self.bank)
        employee=HREmployee(employee_code='QA',full_name='Trabajador prueba',hire_date=date(2026,9,1),branch_id=branch.id,status='ACTIVE');self.db.add(employee);self.db.flush()
        self.profile=PayrollEmployeeProfile(employee_id=employee.id,monthly_salary=30000,currency='NIO',contract_start=employee.hire_date,active=True);self.db.add(self.profile)
        self.db.add(PayrollBenefitSetting(id=1,enabled=True,start_date=self.today,expense_id=self.accounts[1].id,opening_debit_id=self.accounts[1].id,vacation_payable_id=self.accounts[3].id,bonus_payable_id=self.accounts[4].id,seniority_payable_id=self.accounts[5].id))
        self.db.add(PayrollAccountingSetting(id=1,salary_expense_id=self.accounts[0].id,salary_payable_id=self.accounts[2].id));self.db.flush()

    def tearDown(self):
        self.db.close();self.engine.dispose();self.clock.stop()

    def test_idempotent_daily_and_salary_revaluation(self):
        from app.core import payroll_benefits as b
        snapshot=b.sync_profile(self.db,self.profile,self.today,'QA')
        initial=snapshot.vacation_amount;count=self.db.query(self.web.AccountingEntry).count()
        b.sync_profile(self.db,self.profile,self.today,'QA')
        self.assertEqual(self.db.query(self.web.AccountingEntry).count(),count)
        self.profile.monthly_salary=60000
        b.sync_profile(self.db,self.profile,self.today,'QA')
        self.assertEqual(snapshot.vacation_amount,initial*2)
        self.assertEqual(self.db.query(self.web.AccountingEntry).count(),count+1)

    def test_bank_payment_idempotent_and_refund(self):
        from app.core import payroll_benefits as b
        token='a'*32;initial=b.balance(self.db,self.profile,self.today)
        move=b.record(self.db,self.profile,'CASH',self.today,Decimal(1),None,None,'Autorización prueba',token,'QA','TRANSFERENCIA',self.bank.id)
        self.assertIsNone(move.cash_receipt_id)
        self.assertEqual(b.record(self.db,self.profile,'CASH',self.today,1,None,None,'Autorización prueba',token,'QA','TRANSFERENCIA',self.bank.id).id,move.id)
        self.assertEqual(b.balance(self.db,self.profile,self.today)['days'],initial['days']-1)
        reversal=b.void_movement(self.db,move,'Pago bancario cancelado','QA')
        self.assertIsNotNone(reversal.accounting_entry_id)
        self.assertEqual(b.balance(self.db,self.profile,self.today)['days'],initial['days'])

    def test_no_negative_balance(self):
        from app.core import payroll_benefits as b
        with self.assertRaisesRegex(ValueError,'suficientes'):
            b.record(self.db,self.profile,'CASH',self.today,100,None,None,'Autorización prueba','a'*32,'QA','TRANSFERENCIA',self.bank.id)
        self.assertEqual(self.db.query(self.web.AccountingEntry).count(),0)

    def test_rest_overlap_and_future_cancellation(self):
        from app.core import payroll_benefits as b
        from datetime import timedelta
        start=self.today+timedelta(days=1)
        move=b.record(self.db,self.profile,'REST',self.today,0,start,start,'Descanso autorizado','b'*32,'QA')
        with self.assertRaisesRegex(ValueError,'superpone'):
            b.record(self.db,self.profile,'REST',self.today,0,start,start,'Descanso repetido','c'*32,'QA')
        b.void_movement(self.db,move,'Cancelación antes de iniciar','QA')
        self.assertEqual(b.rest_dates(self.db,self.profile.employee_id,start,start),set())

    def test_rest_applies_provision_without_duplicate_salary_expense(self):
        from app.core import payroll_benefits as b,accounting_payroll as ledger
        from app.models.payroll import PayrollPeriod,PayrollCalculation,PayrollVacationRelease
        move=b.record(self.db,self.profile,'REST',self.today,0,self.today,self.today,'Descanso autorizado','d'*32,'QA')
        snapshot=b.sync_profile(self.db,self.profile,self.today,'QA');before=snapshot.vacation_amount
        period=PayrollPeriod(branch_id=self.branch.id,code='P-QA',date_from=self.today,date_to=self.today,pay_date=self.today,status='DRAFT');self.db.add(period);self.db.flush()
        calc=PayrollCalculation(period_id=period.id,employee_id=self.profile.employee_id,monthly_salary=30000,base_pay=1000,gross_pay=1000,total_deductions=0,late_deduction=0,net_pay=1000);self.db.add(calc);self.db.flush()
        ledger.accrue(self.db,period,'QA')
        release=self.db.query(PayrollVacationRelease).one()
        entry=self.db.get(self.web.AccountingEntry,release.accounting_entry_id)
        self.assertEqual(entry.lines[0].cuenta_id,self.accounts[3].id)
        self.assertEqual(entry.lines[1].cuenta_id,self.accounts[0].id)
        self.assertEqual(snapshot.vacation_amount,before-1000)
        ledger.accrue(self.db,period,'QA')
        self.assertEqual(self.db.query(PayrollVacationRelease).count(),1)
        with self.assertRaisesRegex(ValueError,'comenzó'):
            b.void_movement(self.db,move,'Corrección no permitida','QA')

    def test_paid_through_history_excluded(self):
        from app.core import payroll_benefits as b
        self.profile.vacation_paid_through=date(2026,9,30)
        self.assertEqual(b.balance(self.db,self.profile,self.today)['earned'],Decimal(14)*Decimal('2.5')/31)
