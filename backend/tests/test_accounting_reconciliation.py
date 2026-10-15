import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web as w
from app.routers import accounting_workspace as a
from app.core.accounting_workflow import reconcile_lines, reconciliation_used, seed_journals
from app.models.sales import AccountingJournal, AccountingBankTransaction, AccountingBankMatch


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.db.add_all([w.Branch(id=1, code='central', name='Central'), w.Branch(id=2, code='esteli', name='Esteli'),
            w.Cliente(id=1, nombre='Cliente A'), w.Cliente(id=2, nombre='Cliente B'),
            w.CuentaContable(id=1, codigo='1103', nombre='Clientes por cobrar', tipo='BALANCE', naturaleza='DEBE', activo=True),
            w.CuentaContable(id=2, codigo='1102', nombre='Banco', tipo='BALANCE', naturaleza='DEBE', activo=True),
            w.CuentaContable(id=3, codigo='4101', nombre='Ventas', tipo='RESULTADO', naturaleza='HABER', activo=True),
            w.AccountingVoucherType(id=1, code='DIARIO', nombre='Diario', prefijo='CD', activo=True),
            w.AccountingVoucherType(id=2, code='INGRESO', nombre='Ingreso', prefijo='CI', activo=True),
            w.AccountingVoucherType(id=3, code='EGRESO', nombre='Egreso', prefijo='CE', activo=True)])
        self.db.commit()
        self.user = w.User(email='admin@test', roles=[w.Role(name='administrador')])
        self.request = Request({'type':'http','method':'POST','path':'/accounting','headers':[],'query_string':b''})
        self.patches = [patch.object(w,'_enforce_permission'), patch.object(w,'_user_scoped_branch_ids',return_value=[1])]
        for p in self.patches:p.start()
        self.debit = self.line(1, 1, 100, 0)
        self.credit = self.line(2, 1, 0, 60)

    def tearDown(self):
        for p in self.patches:p.stop()
        self.db.close();self.engine.dispose()

    def line(self, ident, account, debit, credit, client=1, branch=1):
        entry = w.AccountingEntry(id=ident, tipo_id=1, branch_id=branch, numero=f'CD-{ident}', fecha=date(2026,10,12), periodo='2026-10', secuencia=ident,
            descripcion='Prueba', estado='POSTEADO', total_debe=debit or credit, total_haber=debit or credit,
            lines=[w.AccountingEntryLine(cuenta_id=account, cliente_id=client, debe=debit, haber=credit),
                   w.AccountingEntryLine(cuenta_id=3, cliente_id=client, debe=credit, haber=debit)])
        self.db.add(entry);self.db.commit();return entry.lines[0]

    def test_partial_reconciliation_and_overapplication(self):
        reconcile_lines(self.db,self.debit,self.credit,Decimal('40'),'Abono parcial','admin')
        self.assertEqual(reconciliation_used(self.db,self.debit.id),40)
        self.assertEqual(self.debit.debe-reconciliation_used(self.db,self.debit.id),60)
        with self.assertRaises(ValueError):reconcile_lines(self.db,self.debit,self.credit,Decimal('30'),'Sobreaplicación','admin')
        reconcile_lines(self.db,self.debit,self.credit,Decimal('20'),'Saldo del abono','admin')
        self.assertEqual(reconciliation_used(self.db,self.credit.id),60)

    def test_incompatible_contact_or_branch_or_account(self):
        for credit in [self.line(3,1,0,100,client=2),self.line(4,1,0,100,branch=2),self.line(5,2,0,100)]:
            with self.assertRaises(ValueError):reconcile_lines(self.db,self.debit,credit,Decimal('10'),'No coincide','admin')

    def test_cancel_restores_residual_with_audit(self):
        row=reconcile_lines(self.db,self.debit,self.credit,Decimal('40'),'Abono parcial','admin');self.db.commit()
        result=a.reconciliation_undo(self.request,row.id,self.db,self.user)
        self.assertNotIn('error=',result.headers['location']);self.assertEqual(reconciliation_used(self.db,self.debit.id),0)
        self.assertEqual(row.canceled_by,self.user.email)

    def test_reconciled_entry_must_be_unmatched_before_reversal(self):
        reconcile_lines(self.db,self.debit,self.credit,Decimal('40'),'Abono parcial','admin');self.db.commit()
        result=w.accounting_entry_reverse(self.request,self.debit.entry_id,'Corrección del asiento','2026-10-12',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(self.debit.entry.estado,'POSTEADO')

    def setup_bank(self):
        journal=AccountingJournal(code='BNK',name='Banco central',kind='BANK',currency='CS',branch_id=1,voucher_type_id=2,main_account_id=2,active=True)
        self.db.add(journal);self.db.commit()
        result=a.bank_transaction(self.request,journal.id,'2026-10-12','DEP-123','100','1',self.db,self.user)
        self.assertNotIn('error=',result.headers['location'])
        return journal,self.db.query(AccountingBankTransaction).one(),self.line(3,2,100,0)

    def test_bank_match_does_not_duplicate_ledger_entry(self):
        journal,transaction,line=self.setup_bank();before=self.db.query(w.AccountingEntry).count()
        result=a.bank_match(self.request,transaction.id,line.id,'100',self.db,self.user)
        self.assertNotIn('error=',result.headers['location']);self.assertEqual(a.bank_used(self.db,transaction_id=transaction.id),100)
        self.assertEqual(self.db.query(w.AccountingEntry).count(),before)
        again=a.bank_match(self.request,transaction.id,line.id,'1',self.db,self.user)
        self.assertIn('error=',again.headers['location']);self.assertEqual(self.db.query(AccountingBankMatch).count(),1)

    def test_duplicate_bank_transaction_rejected(self):
        journal,transaction,line=self.setup_bank()
        result=a.bank_transaction(self.request,journal.id,'2026-10-12','DEP-123','100','1',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(self.db.query(AccountingBankTransaction).count(),1)

    def test_bank_match_checks_sign_and_access(self):
        journal,transaction,line=self.setup_bank();wrong=self.line(4,2,0,100)
        result=a.bank_match(self.request,transaction.id,wrong.id,'100',self.db,self.user)
        self.assertIn('error=',result.headers['location'])
        other=self.line(5,2,100,0,branch=2)
        result=a.bank_match(self.request,transaction.id,other.id,'100',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(self.db.query(AccountingBankMatch).count(),0)

    def test_history_adapter_keeps_amounts_and_numbers(self):
        self.debit.entry.referencia='AUTO-VTA-100';self.db.commit()
        before=[(r.id,r.numero,r.total_debe,r.total_haber,r.estado) for r in self.db.query(w.AccountingEntry).order_by(w.AccountingEntry.id)]
        seed_journals(self.db);self.db.expire_all()
        after=[(r.id,r.numero,r.total_debe,r.total_haber,r.estado) for r in self.db.query(w.AccountingEntry).order_by(w.AccountingEntry.id)]
        self.assertEqual(before,after);self.assertEqual(self.debit.entry.journal.kind,'SALES');self.assertEqual(self.credit.entry.journal.kind,'GENERAL')
        seed_journals(self.db);self.assertEqual(self.db.query(AccountingJournal).count(),5)

    def test_bank_counterpart_is_atomic_and_cannot_duplicate_after_unmatch(self):
        journal,transaction,line=self.setup_bank()
        result=a.bank_counterpart(self.request,transaction.id,3,'Ingreso no registrado',self.db,self.user)
        self.assertNotIn('error=',result.headers['location'])
        self.assertEqual(a.bank_used(self.db,transaction_id=transaction.id),100)
        entry=self.db.query(w.AccountingEntry).filter_by(referencia=f'EXTRACTO-{transaction.id}').one()
        self.assertEqual(entry.total_debe,100);self.assertEqual(entry.total_haber,100)
        match=self.db.query(AccountingBankMatch).one()
        a.bank_undo(self.request,match.id,self.db,self.user)
        again=a.bank_counterpart(self.request,transaction.id,3,'Duplicación de ingreso',self.db,self.user)
        self.assertIn('error=',again.headers['location'])
        self.assertEqual(self.db.query(w.AccountingEntry).filter_by(referencia=f'EXTRACTO-{transaction.id}').count(),1)

    def test_closed_period_rejects_bank_counterpart(self):
        journal,transaction,line=self.setup_bank()
        self.db.add(w.AccountingLockSetting(id=1,closed_through=date(2026,10,12),reason='Cierre revisado',updated_by='admin'));self.db.commit()
        result=a.bank_counterpart(self.request,transaction.id,3,'Ingreso no registrado',self.db,self.user)
        self.assertIn('error=',result.headers['location'])
        self.assertEqual(self.db.query(AccountingBankMatch).count(),0)
