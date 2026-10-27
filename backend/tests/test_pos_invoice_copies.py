"""Three distinct Holl POS receipts, using isolated SQLite and no physical printer."""
import unittest
from unittest.mock import patch
import fitz
from app.routers import web
from tests.test_pos_browser import NativePosTests


class PosInvoiceCopiesTests(unittest.TestCase):
    setUp = NativePosTests.setUp
    tearDown = NativePosTests.tearDown
    request = NativePosTests.request
    sale = NativePosTests.sale
    activate = NativePosTests.activate
    due = NativePosTests.due
    invoice = NativePosTests.invoice

    def test_queue_and_pdf_have_exactly_three_distinct_receipts(self):
        _, job = self.invoice()
        self.assertEqual(job.copies, 3)
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        pdf, copies = web._build_pos_print_payload(invoice, None, 5)
        self.assertEqual(copies, 1)  # The document already contains all three.
        doc = fitz.open(stream=pdf, filetype='pdf')
        self.assertEqual(len(doc), 3)
        for page, label in zip(doc, ('ORIGINAL', 'COPIA', 'EXCLUSIVA DE BODEGA')):
            text = page.get_text()
            self.assertIn(label, text)
            self.assertIn(invoice.numero, text)
            self.assertLess(text.index('Sucursal:'), text.index(label))
            self.assertLess(text.index(label), text.index('REFERENCIA / FACTURA' if label == 'EXCLUSIVA DE BODEGA' else 'Factura:'))
            spans = [s for b in page.get_text('dict')['blocks'] if 'lines' in b for l in b['lines'] for s in l['spans']]
            self.assertTrue(any(s['text'] == label and 'Bold' in s['font'] for s in spans))
        self.assertNotIn('EXCLUSIVA DE BODEGA', doc[0].get_text())
        self.assertNotIn('EXCLUSIVA DE BODEGA', doc[1].get_text())

    def test_other_companies_keep_configured_copies_and_existing_receipt(self):
        _, job = self.invoice()
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        with patch.object(web, '_is_pacasholl_company', return_value=False):
            pdf, copies = web._build_pos_print_payload(invoice, None, 2)
            self.assertEqual(copies, 2)
            doc = fitz.open(stream=pdf, filetype='pdf')
            self.assertEqual(len(doc), len(fitz.open(stream=web._build_pos_ticket_pdf_bytes(invoice, None), filetype="pdf")))
            self.assertNotIn('EXCLUSIVA DE BODEGA', doc[0].get_text())

    def test_central_warehouse_receipt_has_no_money_and_large_reference(self):
        _, job = self.invoice()
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        pdf, _ = web._build_pos_print_payload(invoice, None, 3)
        doc = fitz.open(stream=pdf, filetype='pdf')
        for page in doc[:2]:
            self.assertIn('Precio:', page.get_text())
            self.assertIn('Pagos aplicados', page.get_text())
        page = doc[2]
        text = page.get_text()
        for forbidden in ('C$', '$', 'Precio:', 'Subtotal:', 'Descuentos:', 'Equivalente', 'Pagos aplicados', 'Efectivo', 'Vuelto:', 'Saldo:'):
            self.assertNotIn(forbidden, text)
        self.assertIn('TOTAL BULTOS: 1', text)
        self.assertIn('Vendedor', text)
        spans = [s for b in page.get_text('dict')['blocks'] if 'lines' in b for l in b['lines'] for s in l['spans']]
        self.assertTrue(any(s['text'] == invoice.numero and s['size'] >= 20 and 'Bold' in s['font'] for s in spans))

    def test_esteli_warehouse_receipt_keeps_money(self):
        _, job = self.invoice()
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        with patch.object(invoice.bodega.branch, 'code', 'esteli'):
            self.assertFalse(web._warehouse_dispatch_only(invoice))
            pdf, _ = web._build_pos_print_payload(invoice, None, 3)
            third = fitz.open(stream=pdf, filetype='pdf')[2]
            self.assertIn('EXCLUSIVA DE BODEGA', third.get_text())
            self.assertIn('Pagos aplicados', third.get_text())
            self.assertIn('Precio:', third.get_text())

    def test_warehouse_total_includes_physical_gifts(self):
        _, job = self.invoice()
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        invoice.items.append(web.VentaItem(producto=invoice.items[0].producto, cantidad=2, combo_role='gift'))
        pdf, _ = web._build_pos_print_payload(invoice, None, 3)
        doc = fitz.open(stream=pdf, filetype='pdf')
        self.assertIn('TOTAL BULTOS: 3', doc[2].get_text())
        self.assertIn('Total unds: 1', doc[0].get_text())

    def test_server_print_sends_bundle_once(self):
        _, job = self.invoice()
        invoice = self.db.get(web.VentaFactura, job.invoice_id)
        with patch.object(web, '_get_sumatra_path', return_value='SumatraPDF.exe'), patch.object(web.subprocess, 'run') as run:
            web._print_pos_ticket(invoice, 'QA only', 3)
            self.assertEqual(run.call_count, 1)
