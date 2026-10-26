"""Búsqueda y corrección auditada en SQLite aislada, sin tocar comprobantes reales."""
import unittest
from unittest.mock import patch
from datetime import date,datetime
from decimal import Decimal
from urllib.parse import parse_qs,urlsplit
from starlette.requests import Request
from fastapi import HTTPException
from tests import test_accounting_workflow as fixture
from app.routers import web as w,accounting_registry as registry
from app.main import app

class RegistryTests(unittest.TestCase):
    entry=fixture.AccountingWorkflowTests.entry
    def setUp(self):
        fixture.AccountingWorkflowTests.setUp(self)
        self.company=patch.object(w,'_is_pacasholl_company',return_value=True);self.company.start()
        self.today=patch.object(w,'local_today',return_value=self.day);self.today.start()
    def tearDown(self):
        self.today.stop();self.company.stop();fixture.AccountingWorkflowTests.tearDown(self)
    def req(self,query=''):
        r=Request(dict(type='http',method='GET',path='/accounting/comprobantes',scheme='http',server=('test',80),headers=[],query_string=query.encode(),app=app,router=app.router));r.state.branding={'company_key':'hollywood_pacas'};return r
    def test_search_reference_lines_amount_dates_and_account(self):
        row=self.entry('POSTEADO');row.referencia='Factura-GASTO-321';row.lines[0].descripcion='Soporte alquiler SEP';row.fecha=date(2025,9,15);row.periodo='2025-09';self.db.commit()
        for query in ('q=GASTO-321','q=alquiler','amount=100','month=2025-09','account_id=1','amount_min=99&amount_max=101'):
            with self.subTest(query=query):self.assertEqual([r.id for r in registry.registry_data(self.req(query),self.db,self.user)['entries']],[row.id])
        for query in ('q=noexiste','amount=0','month=2026-10','start=2026-01-01'):
            self.assertEqual(registry.registry_data(self.req(query),self.db,self.user)['count'],0)
    def test_invoice_number_search_and_scope(self):
        self.db.add(w.Bodega(id=1,branch_id=1,code='A',name='Central'))
        self.db.add(w.VentaFactura(id=11,numero='FACT-908',bodega_id=1,fecha=datetime(2026,10,12),moneda='CS'))
        row=self.entry('POSTEADO');row.referencia='AUTO-VTA-11';self.db.commit()
        self.assertEqual(registry.registry_data(self.req('q=FACT-908'),self.db,self.user)['count'],1)
        self.entry('POSTEADO',branch=2,seq=2)
        self.assertEqual(registry.registry_data(self.req(),self.db,self.user)['count'],1)
        with self.assertRaises(HTTPException):registry.registry_data(self.req('branch_id=2'),self.db,self.user)
    def test_capture_currency_and_exact_line_payment(self):
        row=self.entry('POSTEADO');row.moneda='USD';row.tasa_cambio=37;row.total_debe=row.total_haber=3700
        row.lines[0].debe=3700;row.lines[1].haber=3700;self.db.commit()
        self.assertEqual(registry.registry_data(self.req('amount=100&amount_currency=origen'),self.db,self.user)['count'],1)
        self.assertEqual(registry.registry_data(self.req('amount=100'),self.db,self.user)['count'],0)
        row.lines[0].debe=3600;self.db.commit()
        self.assertEqual(registry.registry_data(self.req('amount=3600'),self.db,self.user)['count'],1)
    def test_bad_filters_and_pagination(self):
        for query in ('amount=NaN','amount=-1','amount=1.234','month=2026-13','start=bad','amount_min=10&amount_max=1','start=2026-12-01&end=2026-01-01','estado=bad'):
            with self.subTest(query=query),self.assertRaises(HTTPException):registry.registry_data(self.req(query),self.db,self.user)
        for i in range(1,53):self.entry(seq=i)
        first=registry.registry_data(self.req(),self.db,self.user)
        second=registry.registry_data(self.req('page=2'),self.db,self.user)
        self.assertEqual(first['count'],52);self.assertEqual(len(first['entries']),50);self.assertEqual(len(second['entries']),2)
    def test_correction_is_atomic_audited_and_idempotent(self):
        original=self.entry('POSTEADO');original.referencia='DOC-888';self.db.commit()
        result=w.accounting_entry_correct(self.request,original.id,'Cuenta incorrecta en gasto','2026-10-12',self.db,self.user)
        self.assertNotIn('error=',result.headers['location'])
        draft_id=int(parse_qs(urlsplit(result.headers['location']).query)['edit_entry_id'][0])
        draft=self.db.get(w.AccountingEntry,draft_id)
        self.assertEqual(original.estado,'REVERSADO');self.assertEqual(original.lines[0].debe,100)
        self.assertEqual(draft.estado,'BORRADOR');self.assertEqual(draft.referencia,'DOC-888')
        self.assertEqual(draft.total_debe,100);self.assertIn(draft.numero,original.anulado_motivo)
        self.assertEqual(self.db.query(w.AccountingEntry).filter_by(reversa_de_id=original.id).count(),1)
        again=w.accounting_entry_correct(self.request,original.id,'Reintento de corrección','2026-10-12',self.db,self.user)
        self.assertIn('edit_entry_id='+str(draft_id),again.headers['location'])
        self.assertEqual(self.db.query(w.AccountingEntry).count(),3)
        posted=w._scope_accounting_entries_query(self.db,[1]).all()
        self.assertEqual(sum((line.debe-line.haber for e in posted for line in e.lines if line.cuenta_id==1),Decimal(0)),0)
    def test_correction_blocks_automatic_reconciled_closed_and_other_branch(self):
        row=self.entry('POSTEADO');row.referencia='AUTO-VTA-10';self.db.commit()
        result=w.accounting_entry_correct(self.request,row.id,'Monto incorrecto','2026-10-12',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(row.estado,'POSTEADO')
        row.referencia='Manual';self.db.commit()
        with patch.object(w,'_assert_accounting_date_open',side_effect=HTTPException(409,'Cerrado')):
            result=w.accounting_entry_correct(self.request,row.id,'Monto incorrecto','2026-10-12',self.db,self.user)
            self.assertIn('error=',result.headers['location'])
        other=self.entry('POSTEADO',branch=2,seq=2)
        self.assertIn('error=',w.accounting_entry_correct(self.request,other.id,'Cuenta incorrecta','2026-10-12',self.db,self.user).headers['location'])
        self.assertEqual(self.db.query(w.AccountingEntry).count(),2)
    def test_render_registry_has_filters_details_and_correction(self):
        self.entry('POSTEADO')
        html=registry.page(self.req(),self.db,self.user).body.decode()
        for text in ('Buscador de comprobantes','Moneda del filtro','registryCorrect','Revertir y abrir borrador','Cuenta contable'):self.assertIn(text,html)
    def test_correction_blocks_cash_receipts_and_reconciliation(self):
        row=self.entry('POSTEADO')
        self.db.add(w.ReciboCaja(id=1,numero='ROC-123',branch_id=1,bodega_id=1,rubro_id=1,motivo_id=1,tipo='EGRESO',fecha=self.day,moneda='CS',descripcion='Pago de soporte',accounting_entry_id=row.id))
        self.db.commit()
        self.assertEqual(registry.registry_data(self.req('q=ROC-123'),self.db,self.user)['count'],1)
        result=w.accounting_entry_correct(self.request,row.id,'Cuenta equivocada','2026-10-12',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(row.estado,'POSTEADO')
        self.db.query(w.ReciboCaja).delete();self.db.commit()
        self.db.add(w.AccountingReconciliation(debit_line_id=row.lines[0].id,credit_line_id=row.lines[1].id,amount_cs=10,reason='Test match',created_by='test'));self.db.commit()
        result=w.accounting_entry_correct(self.request,row.id,'Cuenta equivocada','2026-10-12',self.db,self.user)
        self.assertIn('error=',result.headers['location']);self.assertEqual(row.estado,'POSTEADO')
    def test_failed_correction_rolls_back_original_and_reversal(self):
        row=self.entry('POSTEADO')
        real_flush=self.db.flush
        def fail_when_draft(*args,**kwargs):
            if any(isinstance(e,w.AccountingEntry) and e.estado=='BORRADOR' for e in self.db.new):raise RuntimeError('Simulated database failure')
            return real_flush(*args,**kwargs)
        with patch.object(self.db,'flush',side_effect=fail_when_draft):
            with self.assertRaises(RuntimeError):w.accounting_entry_correct(self.request,row.id,'Corregir monto','2026-10-12',self.db,self.user)
        self.db.rollback();self.db.refresh(row)
        self.assertEqual(row.estado,'POSTEADO');self.assertEqual(self.db.query(w.AccountingEntry).count(),1)
    def test_corrected_draft_can_change_amount_and_account_before_publishing(self):
        original=self.entry('POSTEADO')
        response=w.accounting_entry_correct(self.request,original.id,'Monto y cuenta incorrectos','2026-10-12',self.db,self.user)
        draft=self.db.get(w.AccountingEntry,int(parse_qs(urlsplit(response.headers['location']).query)['edit_entry_id'][0]))
        account=w.CuentaContable(codigo='4102',nombre='Otra venta',tipo='RESULTADO',naturaleza='HABER',activo=True)
        self.db.add(account);self.db.flush()
        draft.lines[1].cuenta=account;draft.lines[0].debe=125;draft.lines[1].haber=125;draft.total_debe=draft.total_haber=125
        self.db.commit()
        result=w.accounting_entry_post(self.request,draft.id,self.db,self.user)
        self.assertNotIn('error=',result.headers['location'])
        self.assertEqual(draft.estado,'POSTEADO');self.assertEqual(draft.total_debe,125)
        self.assertEqual(original.total_debe,100);self.assertEqual(original.lines[1].cuenta_id,2)
