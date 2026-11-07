"""Cash vacations with an unpaid settlement: debit once and reduce the draft once."""
import unittest
from decimal import Decimal
from datetime import timedelta
from tests import test_payroll_benefits as fixtures
from app.core import payroll_benefits as benefits
from app.routers import web as w,payroll
from app.models.payroll import PayrollSettlement,PayrollVacationMovement,PayrollVacationDraftAdjustment


class VacationCashDraftTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.BenefitLedgerTests();self.case.setUp();self.db=self.case.db;self.profile=self.case.profile;self.today=self.case.today
        self.warehouse=w.Bodega(code='central',name='Central',branch_id=self.case.branch.id,activo=True)
        self.db.add(self.warehouse)
        voucher=self.db.query(w.AccountingVoucherType).filter_by(code='EGRESO').one()
        self.journal=w.AccountingJournal(code='CAJA-QA',name='Caja QA',kind='CASH',branch_id=self.case.branch.id,voucher_type_id=voucher.id,main_account_id=self.case.accounts[6].id,active=True)
        self.db.add(self.journal);self.db.commit()

    def tearDown(self):self.case.tearDown()

    def draft(self,cutoff=None,deduction=0):
        cutoff=cutoff or self.today
        balance=benefits.balance(self.db,self.profile,cutoff)
        bonus=payroll._benefit_preview(self.profile,self.profile.employee,cutoff)['bonus_amount']
        row=PayrollSettlement(employee_id=self.profile.employee_id,branch_id=self.case.branch.id,termination_date=cutoff,reason_code='RENUNCIA',reason_detail='Borrador de prueba',salary_snapshot=self.profile.monthly_salary,vacation_days=balance['days'],vacation_amount=balance['amount'],bonus_amount=bonus,seniority_amount=0,other_additions=0,deductions_amount=deduction,gross_amount=balance['amount']+bonus,net_amount=balance['amount']+bonus-Decimal(deduction),apply_seniority=False,status='DRAFT')
        self.db.add(row);self.db.commit();return row

    def pay(self,update=True,days=Decimal(1)):
        return benefits.record(self.db,self.profile,'CASH',self.today,days,None,None,'Pago de vacaciones autorizado','1'*32,'QA','EFECTIVO',self.journal.id,self.warehouse,update_settlement=update)

    def test_cash_payment_reduces_draft_once_and_creates_receipt(self):
        draft=self.draft();before=(draft.vacation_amount,draft.gross_amount,draft.bonus_amount)
        move=self.pay();self.db.commit()
        self.assertIsNotNone(move.cash_receipt_id)
        receipt=self.db.get(w.ReciboCaja,move.cash_receipt_id)
        self.assertEqual(receipt.tipo,'EGRESO');self.assertEqual(receipt.monto_cs,1000)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,-1000)
        self.assertEqual(draft.vacation_amount,before[0]-1000);self.assertEqual(draft.gross_amount,before[1]-1000)
        self.assertEqual(draft.bonus_amount,before[2]);self.assertEqual(draft.status,'DRAFT')
        self.assertEqual(self.db.query(PayrollVacationDraftAdjustment).count(),1)
        self.assertEqual(self.pay().id,move.id);self.db.commit()
        self.assertEqual(self.db.query(w.ReciboCaja).count(),1);self.assertEqual(self.db.query(PayrollVacationMovement).count(),1)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,-1000)
        self.assertEqual(self.db.query(PayrollVacationDraftAdjustment).count(),1)

    def test_pending_draft_requires_explicit_review(self):
        self.draft()
        with self.assertRaisesRegex(ValueError,'Confirma'):self.pay(update=False)
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0)

    def test_older_termination_is_not_silently_changed(self):
        draft=self.draft(self.today-timedelta(days=1));original=draft.termination_date
        with self.assertRaisesRegex(ValueError,'anterior a hoy'):self.pay()
        self.assertEqual(draft.termination_date,original);self.assertEqual(self.db.query(w.ReciboCaja).count(),0)

    def test_insufficient_net_rolls_back_receipt_balance_and_draft(self):
        balance=benefits.balance(self.db,self.profile,self.today)
        draft=self.draft(deduction=balance['amount']);before=draft.vacation_amount
        with self.assertRaisesRegex(ValueError,'descuentos excesivos'):
            with self.db.begin_nested():self.pay()
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0);self.assertEqual(self.db.query(PayrollVacationMovement).count(),0)
        self.assertEqual(self.db.query(PayrollVacationDraftAdjustment).count(),0)
        self.db.refresh(draft);self.assertEqual(draft.vacation_amount,before)

    def test_closed_cash_is_preserved_and_no_partial_payment(self):
        self.draft();self.db.add(w.CierreCaja(branch_id=self.case.branch.id,bodega_id=self.warehouse.id,fecha=self.today));self.db.commit()
        with self.assertRaisesRegex(ValueError,'cerrada'):
            with self.db.begin_nested():self.pay()
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0);self.assertEqual(self.db.query(PayrollVacationMovement).count(),0)
        self.assertEqual(self.db.query(PayrollVacationDraftAdjustment).count(),0)

    def test_final_settlement_does_not_pay_the_same_vacation_twice(self):
        draft=self.draft();original=draft.gross_amount
        self.pay();self.db.commit()
        benefits.pay_settlement(self.db,draft,self.profile,'QA','EFECTIVO',self.journal.id,self.warehouse)
        self.db.commit()
        paid=sum((r.monto_cs for r in self.db.query(w.ReciboCaja).all()),Decimal(0))
        self.assertEqual(paid,original)
        self.assertEqual(self.db.query(PayrollVacationMovement).count(),2)

    def test_full_displayed_balance_can_be_paid_without_unpayable_rounding_residue(self):
        draft=self.draft();original=draft.vacation_amount
        days=benefits.balance(self.db,self.profile,self.today)['days'].quantize(Decimal('.0001'))
        move=self.pay(days=days);self.db.commit()
        self.assertEqual(move.amount,original)
        self.assertEqual(benefits.balance(self.db,self.profile,self.today)['amount'],0)
        self.assertEqual(draft.vacation_amount,0)

    def test_full_rounded_up_day_balance_is_not_rejected_or_overpaid(self):
        from datetime import date
        self.profile.contract_start=date(2026,9,2);self.db.commit()
        draft=self.draft();original=draft.vacation_amount
        available=benefits.balance(self.db,self.profile,self.today)['days']
        days=available.quantize(Decimal('.0001'))
        self.assertGreater(days,available)
        move=self.pay(days=days);self.db.commit()
        self.assertEqual(move.amount,original)
        self.assertEqual(benefits.balance(self.db,self.profile,self.today)['days'],0)
        self.assertEqual(draft.vacation_amount,0)
