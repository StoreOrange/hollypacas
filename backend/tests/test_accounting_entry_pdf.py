"""Letter PDF preserves complete lines, saved currency, branding and draft status."""
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace as Obj
try:
    import fitz
except ImportError:
    fitz = None
from app.core.accounting_entry_pdf import build_accounting_entry_pdf


def example_entry(count=2, state='POSTEADO'):
    lines=[]
    for i in range(count):
        lines.append(Obj(cuenta=Obj(codigo=f'1112.{i:02}',nombre='Inventario de producción en proceso con nombre completo sin recortes'),
                         descripcion=f'Línea {i+1}: inversión por compra con detalle completo y verificación FINAL-{i+1}',
                         proveedor=Obj(nombre='Proveedor de prueba & Asociados'),cliente=None,vencimiento=date(2026,11,15),
                         debe=Decimal('3700') if i%2==0 else Decimal('0'),haber=Decimal('3700') if i%2 else Decimal('0')))
    return Obj(numero='CE-202610-00027',estado=state,fecha=date(2026,10,27),periodo='2026-10',
               branch=Obj(name='Central'),tipo=Obj(nombre='Egreso'),journal=Obj(name='Compras'),referencia='FACT-PROV-273',
               descripcion='Compra para producción en proceso. Concepto con caracteres < > & y detalles completos. '*4,
               moneda='USD',tasa_cambio=Decimal('37'),total_debe=Decimal('3700')*(count//2),total_haber=Decimal('3700')*(count//2),
               lines=lines,reversa_de=None,anulado_motivo=None,created_at=datetime(2026,10,27,16,30),creado_por='contador@ejemplo.com',
               publicado_por='supervisor@ejemplo.com',publicado_at=datetime(2026,10,27,10,35))

PROFILE={'legal_name':'Pacas Hollywood','ruc':'J0000000000000','phone':'8900-0300','address':'Sucursal Central, Nicaragua','email':'contabilidad@ejemplo.com'}
LOGO=Path(__file__).resolve().parents[1]/'app/static/logo_hollywood.png'


@unittest.skipUnless(fitz, 'Visual PDF QA requires optional PyMuPDF')
class AccountingPdfTests(unittest.TestCase):
    def test_letter_logo_complete_details_currency_and_audit(self):
        pdf=fitz.open(stream=build_accounting_entry_pdf(example_entry(),PROFILE,LOGO),filetype='pdf')
        self.assertEqual(len(pdf),1)
        self.assertEqual(tuple(pdf[0].rect),(0,0,612,792))
        text=pdf[0].get_text()
        for value in ('COMPROBANTE CONTABLE','CE-202610-00027','PUBLICADO','FACT-PROV-273','FINAL-2',
                      '37.0000','USD 100.00','CORRECTO','contador@ejemplo.com','supervisor@ejemplo.com','Autorizado por'):
            self.assertIn(value,text)
        self.assertTrue(pdf[0].get_images())
    def test_multipage_repeats_header_preserves_last_detail_and_totals(self):
        pdf=fitz.open(stream=build_accounting_entry_pdf(example_entry(60),PROFILE,LOGO),filetype='pdf')
        self.assertGreater(len(pdf),2)
        all_text=''.join(page.get_text() for page in pdf)
        for i in range(60):self.assertIn('FINAL-'+str(i+1),all_text)
        for page in pdf:
            text=page.get_text()
            self.assertIn('CE-202610-00027',text)
            self.assertIn('COMPROBANTE CONTABLE',text)
            self.assertTrue(page.get_images())
            self.assertIn('Página '+str(page.number+1),text)
            for word in page.get_text('words'):
                self.assertGreaterEqual(word[0],35)
                self.assertLessEqual(word[2],577)
                self.assertGreaterEqual(word[1],15)
                self.assertLessEqual(word[3],780)
        self.assertIn('111,000.00',all_text)
    def test_draft_unbalanced_has_no_posted_claim(self):
        entry=example_entry(state='BORRADOR');entry.total_haber=Decimal('3600');entry.publicado_por=None;entry.publicado_at=None
        pdf=fitz.open(stream=build_accounting_entry_pdf(entry,PROFILE,LOGO),filetype='pdf')
        text=''.join(page.get_text() for page in pdf)
        self.assertIn('SIN EFECTO EN LIBROS',text)
        self.assertIn('PENDIENTE',text)
        self.assertIn('100.00',text)
        self.assertNotIn('CUADRE: CORRECTO',text)

    def test_registration_utc_and_publication_local_show_same_timezone(self):
        entry=example_entry()
        entry.created_at=datetime(2026,10,27,3,30)
        entry.publicado_at=datetime(2026,10,26,21,31)
        pdf=fitz.open(stream=build_accounting_entry_pdf(entry,PROFILE,LOGO),filetype='pdf')
        text=''.join(page.get_text() for page in pdf)
        self.assertIn('Fecha de registro: 26/10/2026 21:30',text)
        self.assertIn('Fecha de publicación: 26/10/2026 21:31',text)
        self.assertIn('hora de Nicaragua',text)
