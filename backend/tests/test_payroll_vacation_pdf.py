"""Vacation PDFs are scoped, read-only and use payment snapshots."""
import unittest
try:
    import fitz
except ImportError:
    fitz=None
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from fastapi import HTTPException
from starlette.requests import Request
from fastapi.templating import Jinja2Templates
from tests import test_vacation_cash_draft as fixtures
from app.routers import payroll_vacations as route,web as w
from app.models.payroll import PayrollVacationMovement,PayrollVacationReversal


class VacationPdfTests(unittest.TestCase):
    def setUp(self):
        self.case=fixtures.VacationCashDraftTests();self.case.setUp();self.db=self.case.db
        self.case.draft();self.move=self.case.pay();self.db.commit()
        self.user=SimpleNamespace(id=1,email='admin@qa.invalid')
        self.patches=[patch.object(route,'_browser_admin',return_value=self.user),patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'_user_scoped_branch_ids',return_value={self.case.case.branch.id})]
        for p in self.patches:p.start()
        app=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory=str(Path(__file__).resolve().parents[1]/'app/templates'))))
        self.request=Request({'type':'http','method':'GET','path':'/payroll/vacations','headers':[],'query_string':b'','app':app})

    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.case.tearDown()

    def text(self,data):
        if not fitz:self.skipTest('Text/visual PDF QA uses optional PyMuPDF')
        with fitz.open(stream=data,filetype='pdf') as doc:return '\n'.join(page.get_text() for page in doc)

    def test_individual_uses_original_payment_snapshot_and_does_not_write(self):
        self.case.profile.monthly_salary=60000;self.db.commit()
        counts=(self.db.query(PayrollVacationMovement).count(),self.db.query(w.ReciboCaja).count(),self.db.query(w.AccountingEntry).count())
        response=route.movement_pdf(self.move.id,self.request,self.db)
        self.assertEqual(response.media_type,'application/pdf');self.assertTrue(response.body.startswith(b'%PDF'))
        self.assertIn('inline',response.headers['content-disposition']);self.assertIn('no-store',response.headers['cache-control'])
        text=self.text(response.body)
        for value in ['Pago de vacaciones','Trabajador prueba','30,000.00','1,000.00','Efectivo','Liquidación en borrador #','ROC-P-']:self.assertIn(value,text)
        self.assertNotIn('60,000.00',text)
        self.assertEqual(counts,(self.db.query(PayrollVacationMovement).count(),self.db.query(w.ReciboCaja).count(),self.db.query(w.AccountingEntry).count()))

    def test_history_and_employee_filter_and_buttons(self):
        filtered=Request(dict(self.request.scope,query_string=f'employee_id={self.move.employee_id}'.encode()))
        html=route.home(filtered,employee_id=str(self.move.employee_id),db=self.db).body.decode()
        self.assertIn(f'/payroll/vacations/{self.move.id}/pdf',html);self.assertIn('Reporte PDF del historial',html)
        self.assertIn(f'history.pdf?employee_id={self.move.employee_id}',html)
        response=route.history_pdf(self.request,employee_id=str(self.move.employee_id),db=self.db)
        self.assertIn('1 movimientos',self.text(response.body))
        empty=route.history_pdf(self.request,employee_id='999',db=self.db)
        self.assertIn('Sin movimientos',self.text(empty.body))

    def test_outside_branch_is_denied_and_history_hides_employee(self):
        with patch.object(w,'_user_scoped_branch_ids',return_value={999}):
            with self.assertRaises(HTTPException) as denied:route.movement_pdf(self.move.id,self.request,self.db)
            self.assertEqual(denied.exception.status_code,403)
            self.assertNotIn('Trabajador prueba',self.text(route.history_pdf(self.request,db=self.db).body))

    def test_void_pdf_clearly_shows_annulled_status(self):
        self.move.status='VOID'
        self.db.add(PayrollVacationReversal(movement_id=self.move.id,reason='Anulación documentada de prueba',created_by='QA'));self.db.commit()
        text=self.text(route.movement_pdf(self.move.id,self.request,self.db).body)
        self.assertIn('REGISTRO ANULADO',text);self.assertIn('Anulación documentada',text)

    def test_missing_record_and_wrong_company(self):
        with self.assertRaises(HTTPException) as missing:route.movement_pdf(999,self.request,self.db)
        self.assertEqual(missing.exception.status_code,404)
        with patch.object(w,'_is_pacasholl_company',return_value=False):
            with self.assertRaises(HTTPException) as other:route.movement_pdf(self.move.id,self.request,self.db)
            self.assertEqual(other.exception.status_code,404)

    def test_history_pages_keep_headers_and_footer(self):
        if not fitz:self.skipTest('Multi-page PDF QA uses optional PyMuPDF')
        from app.core.payroll_vacation_pdf import build
        from datetime import datetime
        data=build([self.move]*90,{'trade_name':'PACAS HOLL'},datetime(2026,11,7,16),{},{},{},{})
        with fitz.open(stream=data,filetype='pdf') as doc:
            self.assertGreater(len(doc),1)
            for page in doc:
                text=page.get_text()
                self.assertIn('Fecha / folio',text);self.assertIn('Nicaragua',text)

    def test_http_pdf_bytes_headers_and_counts_without_optional_renderer(self):
        before=(self.db.query(PayrollVacationMovement).count(),self.db.query(w.ReciboCaja).count(),self.db.query(w.AccountingEntry).count())
        response=route.movement_pdf(self.move.id,self.request,self.db)
        self.assertTrue(response.body.startswith(b'%PDF'));self.assertEqual(response.media_type,'application/pdf')
        self.assertIn('inline',response.headers['content-disposition']);self.assertIn('no-store',response.headers['cache-control'])
        after=(self.db.query(PayrollVacationMovement).count(),self.db.query(w.ReciboCaja).count(),self.db.query(w.AccountingEntry).count())
        self.assertEqual(before,after)
