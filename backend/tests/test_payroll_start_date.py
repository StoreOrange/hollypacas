"""Employment-date corrections: no retroactive payments, balanced deltas, atomic drafts."""
import json
import unittest
from datetime import date,datetime
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.templating import Jinja2Templates
from pathlib import Path
from tests import test_payroll_benefits as fixtures
from app.routers import web as w,payroll, payroll_start_date as route
from app.core import payroll_start_date as s,payroll_benefits as benefits
from app.models.payroll import (PayrollStartDateChange,PayrollBenefitSetting,PayrollBenefitAccrual,PayrollVacationMovement,PayrollPeriod,PayrollCalculation,PayrollSettlement,PayrollPayment)
from app.models.attendance import AttendancePolicySetting,HREmployee


class StartDateTests(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine
        from sqlalchemy.pool import StaticPool
        engine=create_engine('sqlite://',connect_args={'check_same_thread':False},poolclass=StaticPool)
        self.fixture=fixtures.BenefitLedgerTests()
        with patch('sqlalchemy.create_engine',return_value=engine):self.fixture.setUp()
        self.db=self.fixture.db;self.profile=self.fixture.profile;self.today=self.fixture.today
        self.user=w.User(id=1,full_name='Admin QA',email='admin@qa.invalid',hashed_password='qa',roles=[w.Role(name='administrador')])
        self.db.add(self.user);self.db.commit()
        self.patches=[patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'_user_scoped_branch_ids',return_value={self.fixture.branch.id}),patch.object(w,'local_now_naive',return_value=datetime(2026,10,14,10,15)),patch.object(route,'_browser_admin',side_effect=lambda request,db:self.user)]
        for p in self.patches:p.start()
        app=FastAPI();app.include_router(route.router)
        app.state.templates=Jinja2Templates(directory=str(Path(__file__).resolve().parents[1]/'app/templates'))
        app.dependency_overrides[route.get_db]=lambda:self.db
        self.client=TestClient(app)

    def tearDown(self):
        self.client.close()
        for p in reversed(self.patches):p.stop()
        self.fixture.tearDown()

    @property
    def path(self):return f'/payroll/employees/{self.profile.employee_id}/start-date'

    def reviewed(self,new=date(2026,8,1)):
        view=s.preview(self.db,self.profile,new,self.today)
        nonce=uuid4().hex
        return dict(new_date=new.isoformat(),nonce=nonce,fingerprint=view['fingerprint'],signature=s.sign(self.user,self.profile.employee_id,new,view['fingerprint'],nonce),reason='Fecha verificada con el contrato laboral',reviewed='on')

    def apply(self,data=None):
        return self.client.post(self.path,data=data or self.reviewed(),follow_redirects=False)

    def accrue(self):
        row=benefits.sync_profile(self.db,self.profile,self.today,'QA');self.db.commit();return row

    def test_preview_is_read_only_and_shows_before_after(self):
        self.accrue();count=self.db.query(w.AccountingEntry).count()
        response=self.client.get(self.path+'?new_date=2026-08-01')
        self.assertEqual(response.status_code,200);self.assertIn('Comparación de prestaciones',response.text)
        self.assertEqual(self.profile.contract_start,date(2026,9,1));self.assertEqual(self.profile.employee.hire_date,date(2026,9,1))
        self.assertEqual(self.db.query(w.AccountingEntry).count(),count);self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)

    def test_earlier_date_recalculates_rights_and_posts_only_delta(self):
        old=self.accrue();old_amount=Decimal(old.vacation_amount);old_entry=self.db.get(w.AccountingEntry,old.accounting_entry_id)
        original_lines=[(x.debe,x.haber) for x in old_entry.lines]
        response=self.apply();self.assertEqual(response.status_code,303)
        self.db.refresh(self.profile)
        self.assertEqual(self.profile.contract_start,date(2026,8,1));self.assertEqual(self.profile.employee.hire_date,date(2026,8,1))
        self.db.refresh(old);self.assertEqual(Decimal(old.vacation_amount)-old_amount,2500)
        audit=self.db.query(PayrollStartDateChange).one();self.assertEqual(audit.changed_by_id,1)
        adjustment=self.db.get(w.AccountingEntry,audit.adjustment_entry_id)
        self.assertEqual(adjustment.fecha,self.today);self.assertEqual(adjustment.total_debe,adjustment.total_haber)
        self.assertEqual([(x.debe,x.haber) for x in old_entry.lines],original_lines)
        self.assertEqual(old_entry.estado,'POSTEADO');self.assertEqual(self.db.query(PayrollPayment).count(),0)
        self.assertEqual(self.db.query(PayrollVacationMovement).count(),0)

    def test_later_date_reduces_provision_with_balanced_entry(self):
        previous=self.accrue();before=previous.vacation_amount
        self.assertEqual(self.apply(self.reviewed(date(2026,10,1))).status_code,303)
        self.db.refresh(previous);self.assertLess(previous.vacation_amount,before)
        audit=self.db.query(PayrollStartDateChange).one();entry=self.db.get(w.AccountingEntry,audit.adjustment_entry_id)
        self.assertEqual(entry.total_debe,entry.total_haber)
        self.assertGreater(sum(x.debe for x in entry.lines if x.cuenta_id==self.fixture.accounts[3].id),0)

    def test_replay_does_not_recalculate_or_post_twice(self):
        self.accrue();data=self.reviewed()
        self.assertEqual(self.apply(data).status_code,303);count=self.db.query(w.AccountingEntry).count()
        self.assertEqual(self.apply(data).status_code,303)
        self.assertEqual(self.db.query(PayrollStartDateChange).count(),1);self.assertEqual(self.db.query(w.AccountingEntry).count(),count)

    def test_stale_review_and_tampered_date_do_not_save(self):
        data=self.reviewed();self.profile.monthly_salary=31000;self.db.commit()
        self.assertEqual(self.apply(data).status_code,400)
        self.db.refresh(self.profile);self.assertEqual(self.profile.contract_start,date(2026,9,1))
        data=self.reviewed();data['new_date']='2026-07-01'
        self.assertEqual(self.apply(data).status_code,400);self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)

    def test_future_dates_and_missing_reason_are_rejected(self):
        self.assertIn('no posterior a hoy',self.client.get(self.path+'?new_date=2026-12-01').text)
        data=self.reviewed();data['reason']='x';self.assertEqual(self.apply(data).status_code,400)
        self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)

    def test_preserves_paid_through_dates(self):
        self.profile.vacation_paid_through=self.profile.bonus_paid_through=self.profile.seniority_paid_through=date(2026,9,30);self.db.commit()
        before=s.figures(self.db,self.profile,self.today)
        self.assertEqual(self.apply().status_code,303)
        after=s.figures(self.db,self.profile,self.today)
        self.assertEqual(before['vacation_amount'],after['vacation_amount']);self.assertEqual(before['bonus_amount'],after['bonus_amount'])
        self.assertEqual(self.profile.vacation_paid_through,date(2026,9,30))

    def test_used_vacation_is_not_repaid_or_deleted(self):
        move=benefits.record(self.db,self.profile,'CASH',self.today,1,None,None,'Pago ya autorizado','a'*32,'QA','TRANSFERENCIA',self.fixture.bank.id);self.db.commit()
        amount=move.amount;entry=move.accounting_entry_id
        self.assertEqual(self.apply().status_code,303);self.db.refresh(move)
        self.assertEqual(move.amount,amount);self.assertEqual(move.accounting_entry_id,entry)
        self.assertEqual(self.db.query(PayrollVacationMovement).count(),1)
        self.assertEqual(benefits.balance(self.db,self.profile,self.today)['used'],1)

    def test_rejects_new_date_that_invalidates_previously_used_days(self):
        self.db.add(PayrollVacationMovement(operation_key='c'*32,employee_id=self.profile.employee_id,kind='CASH',status='ACTIVE',effective_date=date(2026,9,30),days=2,salary=30000,amount=2000,reason='Vacaciones históricas',created_by='QA'));self.db.commit()
        with self.assertRaisesRegex(ValueError,'utilizadas'):
            s.preview(self.db,self.profile,date(2026,9,15),self.today)
        self.assertEqual(self.profile.contract_start,date(2026,9,1))

    def test_disabled_provision_updates_rights_without_accounting(self):
        self.db.get(PayrollBenefitSetting,1).enabled=False;self.db.commit()
        self.assertEqual(self.apply().status_code,303)
        self.assertEqual(self.db.query(w.AccountingEntry).count(),0)
        self.assertEqual(self.profile.contract_start,date(2026,8,1))

    def test_accounting_failure_rolls_back_dates_audit_and_all_calculations(self):
        self.accrue();before_count=self.db.query(w.AccountingEntry).count()
        data=self.reviewed()
        with patch.object(benefits,'sync_profile',side_effect=RuntimeError('posting failure')):
            with self.assertRaises(RuntimeError):self.apply(data)
        self.db.expire_all()
        self.assertEqual(self.profile.contract_start,date(2026,9,1));self.assertEqual(self.profile.employee.hire_date,date(2026,9,1))
        self.assertEqual(self.db.query(w.AccountingEntry).count(),before_count);self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)

    def test_scope_role_origin_and_company(self):
        with patch.object(w,'_user_scoped_branch_ids',return_value={999}):self.assertEqual(self.client.get(self.path).status_code,403)
        with patch.object(w,'_is_pacasholl_company',return_value=False):self.assertEqual(self.client.get(self.path).status_code,404)
        self.assertEqual(self.client.post(self.path,data=self.reviewed(),headers={'Origin':'https://other.invalid'}).status_code,403)
        self.assertEqual(self.client.post(self.path,data={'new_date':'2026-08-01'}).status_code,403)

    def test_closed_payroll_unchanged_and_only_this_employee_draft_recalculated(self):
        from app.models.payroll import PayrollEmployeeProfile
        self.db.add(AttendancePolicySetting())
        other=HREmployee(employee_code='OTHER',full_name='Otro trabajador',hire_date=date(2026,9,1),branch_id=self.fixture.branch.id,status='ACTIVE');self.db.add(other);self.db.flush()
        self.db.add(PayrollEmployeeProfile(employee_id=other.id,monthly_salary=30000,contract_start=date(2026,9,1)))
        closed=PayrollPeriod(branch_id=self.fixture.branch.id,code='CLOSED',date_from=date(2026,9,1),date_to=date(2026,9,15),pay_date=date(2026,9,15),status='CLOSED')
        draft=PayrollPeriod(branch_id=self.fixture.branch.id,code='DRAFT',date_from=date(2026,10,1),date_to=date(2026,10,15),pay_date=date(2026,10,15),status='DRAFT')
        self.db.add_all([closed,draft]);self.db.flush()
        values=dict(monthly_salary=30000,base_pay=999,gross_pay=999,net_pay=999,days_worked=1)
        old=PayrollCalculation(period_id=closed.id,employee_id=self.profile.employee_id,**values)
        mine=PayrollCalculation(period_id=draft.id,employee_id=self.profile.employee_id,**values)
        theirs=PayrollCalculation(period_id=draft.id,employee_id=other.id,**values)
        self.db.add_all([old,mine,theirs]);self.db.commit()
        self.assertEqual(self.apply(self.reviewed(date(2026,10,10))).status_code,303)
        self.db.refresh(old);self.db.refresh(mine);self.db.refresh(theirs)
        self.assertEqual(old.net_pay,999);self.assertEqual(theirs.net_pay,999)
        self.assertEqual(mine.days_worked,5);self.assertEqual(mine.base_pay,5000)
        self.assertEqual(json.loads(self.db.query(PayrollStartDateChange).one().draft_period_ids),[draft.id])

    def test_draft_settlement_updates_but_paid_snapshot_is_preserved(self):
        def row(status):return PayrollSettlement(employee_id=self.profile.employee_id,branch_id=self.fixture.branch.id,termination_date=self.today,reason_code='RENUNCIA',reason_detail='Documento QA',salary_snapshot=30000,gross_amount=100,net_amount=100,apply_seniority=False,status=status)
        draft=row('DRAFT');paid=row('PAID');self.db.add_all([draft,paid]);self.db.commit()
        self.assertEqual(self.apply().status_code,303)
        self.db.refresh(draft);self.db.refresh(paid)
        self.assertGreater(draft.gross_amount,100);self.assertEqual(paid.gross_amount,100)
        self.assertEqual(json.loads(self.db.query(PayrollStartDateChange).one().draft_settlement_ids),[draft.id])

    def test_regular_profile_save_requires_reviewed_date_correction(self):
        from starlette.requests import Request
        request=Request({'type':'http','method':'POST','path':'/payroll','headers':[],'query_string':b''})
        with patch.object(payroll,'_browser_admin',return_value=self.user):
            response=payroll.save_employee_profile(self.profile.employee_id,request,monthly_salary=Decimal(30000),contract_start='2026-08-01',device_user_link_id='',branch_id='',db=self.db)
        self.assertIn('/start-date',response.headers['location'])
        self.assertEqual(self.profile.contract_start,date(2026,9,1))

    def test_failure_after_draft_calculation_does_not_commit_any_change(self):
        self.db.add(AttendancePolicySetting())
        period=PayrollPeriod(branch_id=self.fixture.branch.id,code='ATOMIC',date_from=date(2026,10,1),date_to=date(2026,10,15),pay_date=date(2026,10,15),status='DRAFT')
        self.db.add(period);self.db.flush()
        calculation=PayrollCalculation(period_id=period.id,employee_id=self.profile.employee_id,monthly_salary=30000,base_pay=777,gross_pay=777,net_pay=777)
        self.db.add(calculation);self.db.commit()
        real=payroll._calculate_period_records
        def fail(*args,**kwargs):
            real(*args,**kwargs)
            raise RuntimeError('Failure after draft calculation')
        with patch.object(payroll,'_calculate_period_records',side_effect=fail):
            with self.assertRaises(RuntimeError):self.apply(self.reviewed(date(2026,10,10)))
        self.db.refresh(calculation);self.db.refresh(self.profile)
        self.assertEqual(calculation.base_pay,777);self.assertEqual(self.profile.contract_start,date(2026,9,1))
        self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)

    def test_accounting_closed_date_prevents_correction_without_mutation(self):
        from fastapi import HTTPException
        self.accrue()
        with patch.object(w,'_assert_accounting_date_open',side_effect=HTTPException(400,'Período contable cerrado')):
            with self.assertRaises(HTTPException):self.reviewed()
        self.assertEqual(self.profile.contract_start,date(2026,9,1))

    def test_date_correction_cannot_leave_negative_draft_payroll(self):
        from app.models.payroll import PayrollAdjustment
        self.db.add(AttendancePolicySetting())
        period=PayrollPeriod(branch_id=self.fixture.branch.id,code='NET',date_from=date(2026,10,1),date_to=date(2026,10,15),pay_date=date(2026,10,15),status='DRAFT')
        self.db.add(period);self.db.flush()
        calc=PayrollCalculation(period_id=period.id,employee_id=self.profile.employee_id,monthly_salary=30000,base_pay=14000,gross_pay=14000,net_pay=9000)
        self.db.add_all([calc,PayrollAdjustment(period_id=period.id,employee_id=self.profile.employee_id,adjustment_type='DEDUCTION',description='Deducción QA',amount=5000)])
        self.db.commit()
        self.assertEqual(self.apply(self.reviewed(date(2026,10,14))).status_code,400)
        self.db.refresh(self.profile);self.db.refresh(calc)
        self.assertEqual(self.profile.contract_start,date(2026,9,1));self.assertEqual(calc.net_pay,9000)
        self.assertEqual(self.db.query(PayrollStartDateChange).count(),0)
