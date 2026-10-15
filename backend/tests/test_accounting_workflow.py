import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web as w
from app.core.accounting_workflow import validate_postable_entry


class AccountingWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.day = date(2026, 10, 12)
        self.db.add_all([w.Branch(id=1, code='central', name='Central'), w.Branch(id=2, code='esteli', name='Esteli'),
            w.CuentaContable(id=1, codigo='1101', nombre='Caja', tipo='BALANCE', naturaleza='DEBE', activo=True),
            w.CuentaContable(id=2, codigo='4101', nombre='Ventas', tipo='RESULTADO', naturaleza='HABER', activo=True),
            w.AccountingVoucherType(id=1, code='DIARIO', nombre='Diario', prefijo='CD', activo=True)])
        self.db.commit()
        self.user = w.User(email='contador@test', roles=[w.Role(name='administrador')])
        self.request = Request({'type': 'http', 'method': 'POST', 'path': '/accounting', 'headers': [], 'query_string': b''})
        self.patches = [patch.object(w, '_enforce_permission'), patch.object(w, '_user_scoped_branch_ids', return_value=[1]),
                        patch.object(w, '_get_accounting_policy', return_value={'strict_mode': False})]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.db.close()
        self.engine.dispose()

    def entry(self, state='BORRADOR', branch=1, seq=1):
        row = w.AccountingEntry(tipo_id=1, branch_id=branch, fecha=self.day, periodo='2026-10', secuencia=seq,
            numero=f'CD-202610-{seq:05d}', descripcion='Venta', estado=state, moneda='CS', total_debe=100, total_haber=100,
            lines=[w.AccountingEntryLine(cuenta_id=1, debe=100, haber=0), w.AccountingEntryLine(cuenta_id=2, debe=0, haber=100)])
        self.db.add(row)
        self.db.commit()
        return row

    def test_draft_excluded_from_books_and_publish_included(self):
        row = self.entry()
        self.assertEqual(w._scope_accounting_entries_query(self.db, [1]).count(), 0)
        result = w.accounting_entry_post(self.request, row.id, self.db, self.user)
        self.assertNotIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'POSTEADO')
        self.assertEqual(w._scope_accounting_entries_query(self.db, [1]).count(), 1)

    def test_unbalanced_draft_cannot_publish(self):
        row = self.entry()
        row.lines[0].debe = 99
        self.db.commit()
        result = w.accounting_entry_post(self.request, row.id, self.db, self.user)
        self.assertIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'BORRADOR')

    def test_inactive_accounts_cannot_publish(self):
        row = self.entry()
        row.lines[0].cuenta.activo = False
        self.db.commit()
        with self.assertRaises(ValueError):
            validate_postable_entry(row)

    def test_reversal_preserves_original_and_balances_ledger(self):
        row = self.entry('POSTEADO')
        result = w.accounting_entry_reverse(self.request, row.id, 'Corrección de registro', '2026-10-12', self.db, self.user)
        self.assertNotIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'REVERSADO')
        reversal = self.db.query(w.AccountingEntry).filter_by(reversa_de_id=row.id).one()
        self.assertEqual(row.lines[0].debe, 100)
        self.assertEqual(reversal.lines[0].haber, 100)
        rows = w._scope_accounting_entries_query(self.db, [1]).all()
        self.assertEqual(len(rows), 2)
        balance = sum((line.debe - line.haber for r in rows for line in r.lines if line.cuenta_id == 1), Decimal('0'))
        self.assertEqual(balance, 0)
        again = w.accounting_entry_reverse(self.request, row.id, 'Segunda reversión', '2026-10-12', self.db, self.user)
        self.assertIn('error=', again.headers['location'])
        self.assertEqual(self.db.query(w.AccountingEntry).count(), 2)

    def test_reversal_cannot_predate_original(self):
        row = self.entry('POSTEADO')
        result = w.accounting_entry_reverse(self.request, row.id, 'Corrección de registro', '2026-10-11', self.db, self.user)
        self.assertIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'POSTEADO')

    def test_other_branch_cannot_publish(self):
        row = self.entry(branch=2)
        result = w.accounting_entry_post(self.request, row.id, self.db, self.user)
        self.assertIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'BORRADOR')

    def test_sequence_is_unique_across_branches(self):
        self.entry('POSTEADO', branch=1, seq=9)
        self.assertEqual(w._next_accounting_sequence(self.db, 1, 2, '2026-10'), 10)

    def test_legacy_annul_cannot_remove_posted_entry(self):
        row = self.entry('POSTEADO')
        result = w.accounting_entry_annul(self.request, row.id, 'Error', self.db, self.user)
        self.assertIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'POSTEADO')

    def test_closure_rejects_pending_drafts(self):
        self.entry()
        with patch.object(w, 'local_today', return_value=date(2026, 10, 13)):
            with self.assertRaises(w.HTTPException) as exc:
                w.accounting_lock_date(self.request, '2026-10-12', 'Cierre mensual revisado', self.db, self.user)
        self.assertEqual(exc.exception.status_code, 409)
        self.assertEqual(self.db.query(w.AccountingLockSetting).count(), 0)

    def test_closed_period_blocks_posting_and_allows_later_reversal(self):
        row = self.entry('POSTEADO')
        with patch.object(w, 'local_today', return_value=date(2026, 10, 13)):
            w.accounting_lock_date(self.request, '2026-10-12', 'Cierre mensual revisado', self.db, self.user)
        with self.assertRaises(w.HTTPException):
            w._assert_accounting_date_open(self.db, self.day)
        result = w.accounting_entry_reverse(self.request, row.id, 'Corrección después del cierre', '2026-10-13', self.db, self.user)
        self.assertNotIn('error=', result.headers['location'])
        self.assertEqual(row.estado, 'REVERSADO')

    def test_non_administrator_cannot_close(self):
        self.user.roles = [w.Role(name='contador')]
        with self.assertRaises(w.HTTPException) as exc:
            w.accounting_lock_date(self.request, '2026-10-11', 'Cierre mensual revisado', self.db, self.user)
        self.assertEqual(exc.exception.status_code, 403)

    def test_automatic_entries_also_respect_closure(self):
        self.db.add(w.AccountingLockSetting(id=1, closed_through=self.day, reason='Cierre revisado', updated_by='admin'))
        self.db.commit()
        with patch.object(w, '_get_accounting_policy', return_value={'auto_entry_enabled': True}):
            with self.assertRaises(w.HTTPException):
                w._build_auto_accounting_entry(self.db, event_code='INV_IN', branch_id=1, entry_date=self.day,
                    amount=Decimal('100'), reference='AUTO-TEST', description='Prueba')
            with self.assertRaises(w.HTTPException):
                w._build_sale_accounting_entries(self.db, factura=w.VentaFactura(id=1), branch_id=1,
                    entry_date=self.day, sale_amount_cs=Decimal('100'), cost_amount_cs=Decimal('50'))
        self.assertEqual(self.db.query(w.AccountingEntry).count(), 0)

    def manual_create(self, state='BORRADOR', debit='100', credit='0', key=''):
        return w.accounting_entries_create(self.request, fecha='2026-10-12', tipo_id=1, branch_id='1',
            referencia='MANUAL-QA', descripcion='Registro manual', moneda='CS', tasa_sistema='0', tasa_cambio='0',
            estado_comprobante=state, edit_entry_id='', journal_id='', manual_operation_key=key, cliente_id_contable='', proveedor_id_contable='', vencimiento_contable='',
            line_cuenta_id=['1'] if credit=='0' else ['1','2'], line_descripcion=['Cargo','Abono'],
            line_debe=[debit,'0'], line_haber=['0',credit], db=self.db, user=self.user)

    def test_manual_unbalanced_draft_can_be_saved_but_not_published(self):
        with patch.object(w,'_is_pacasholl_company',return_value=True):
            result=self.manual_create()
        self.assertNotIn('error=',result.headers['location'])
        row=self.db.query(w.AccountingEntry).one();self.assertEqual(row.estado,'BORRADOR');self.assertEqual(row.total_haber,0)
        posted=w.accounting_entry_post(self.request,row.id,self.db,self.user)
        self.assertIn('error=',posted.headers['location']);self.assertEqual(row.estado,'BORRADOR')

    def test_manual_balanced_entry_publishes_without_intelligent_rules(self):
        with patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'_validate_accounting_entry_policy',side_effect=AssertionError('Heuristics must not run')):
            result=self.manual_create(state='POSTEADO',credit='100')
        self.assertNotIn('error=',result.headers['location'])
        row=self.db.query(w.AccountingEntry).one();self.assertEqual(row.estado,'POSTEADO');self.assertEqual(row.publicado_por,self.user.email)

    def test_repeated_manual_submission_does_not_duplicate_entry(self):
        with patch.object(w,'_is_pacasholl_company',return_value=True):
            first=self.manual_create(key='a'*32)
            second=self.manual_create(key='a'*32)
        self.assertNotIn('error=',first.headers['location']);self.assertNotIn('error=',second.headers['location'])
        self.assertEqual(self.db.query(w.AccountingEntry).count(),1)

    def test_explicit_voucher_type_can_differ_from_journal_default(self):
        self.db.add(w.AccountingVoucherType(id=2,code='INGRESO',nombre='Ingreso',prefijo='CI',activo=True))
        journal=w.AccountingJournal(code='TEST',name='Diario configurado',kind='GENERAL',currency='CS',voucher_type_id=2,active=True)
        self.db.add(journal);self.db.commit()
        with patch.object(w,'_is_pacasholl_company',return_value=True):
            result=w.accounting_entries_create(self.request,fecha='2026-10-12',tipo_id=1,branch_id='1',referencia='TIPO-QA',
                descripcion='Tipo elegido manualmente',moneda='CS',tasa_sistema='0',tasa_cambio='0',estado_comprobante='POSTEADO',
                edit_entry_id='',journal_id=str(journal.id),manual_operation_key='',cliente_id_contable='',proveedor_id_contable='',vencimiento_contable='',
                line_cuenta_id=['1','2'],line_descripcion=['Cargo','Abono'],line_debe=['100','0'],line_haber=['0','100'],db=self.db,user=self.user)
        self.assertNotIn('error=',result.headers['location'])
        row=self.db.query(w.AccountingEntry).one();self.assertEqual(row.tipo_id,1);self.assertEqual(row.journal_id,journal.id)
