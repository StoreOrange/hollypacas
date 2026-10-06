import asyncio
import json
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.datastructures import FormData
from app.database import Base
from app.routers import web
from app.models.user import User, Branch
from app.models.inventory import Bodega
from app.models.sales import AperturaCaja, CierreCaja, VentaFactura


class ShoeCashOpeningTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.day = date(2026, 10, 6)
        for i, code in [(1, 'central'), (2, 'kg'), (3, 'kgf')]:
            self.db.add(Branch(id=i, code=code, name=code.upper()))
            self.db.add(Bodega(id=i, branch_id=i, code=code, name=code.upper(), activo=True))
        self.db.add(web.ExchangeRate(effective_date=self.day, period="DIA", rate=37))
        self.db.commit()
        self.user = User(email='cash-test', full_name='Caja prueba', default_branch_id=2, default_bodega_id=2)
        self.patches = [patch.object(web, '_is_shoes_mode', return_value=True),
                        patch.object(web, '_enforce_permission'),
                        patch.object(web, 'local_today', return_value=self.day)]
        for p in self.patches: p.start()

    def tearDown(self):
        for p in self.patches: p.stop()
        self.db.close()
        self.engine.dispose()

    def request(self, data=None):
        class Templates:
            def TemplateResponse(self, name, context): return context
        class State: templates = Templates()
        class App: state = State()
        req = Request({'type': 'http', 'method': 'POST', 'path': '/sales',
                       'headers': [], 'query_string': b'', 'app': App()})
        req._form = FormData(data or {})
        return req

    def open(self, amount='500.00'):
        return asyncio.run(web.sales_cash_opening(self.request({'monto_cs': amount}), self.db, self.user))

    def test_validates_amount_and_opening_is_once_per_store_and_day(self):
        for amount in ['-1', 'NaN', 'Infinity', '0.001', '1000000000000', '']:
            self.assertIn('invalido', self.open(amount).headers['location'])
        self.assertTrue(web.sales_page(self.request(), self.db, self.user)['cash_opening_required'])
        self.open()
        page = web.sales_page(self.request(), self.db, self.user)
        self.assertFalse(page['cash_opening_required'])
        self.assertFalse(page['cash_is_closed'])
        self.open('900')
        self.assertEqual(self.db.query(AperturaCaja).count(), 1)
        self.assertEqual(self.db.query(AperturaCaja).one().monto_cs, Decimal('500.00'))
        self.assertIsNone(web._shoe_cash_opening(self.db, 1, self.day))
        self.assertIsNone(web._shoe_cash_opening(self.db, 2, date(2026, 10, 7)))
        self.assertEqual(web._shoe_cash_opening(self.db, 2, self.day).usuario_registro, 'Caja prueba')

    def test_sales_require_opening_and_closed_cash_cannot_sell(self):
        data = {'operation_key': 'a' * 32, 'vendedor_id': '1', 'fecha': self.day.isoformat(),
                'moneda': 'CS', 'item_producto_id': '1', 'item_cantidad': '1', 'item_precio': '200'}
        result = asyncio.run(web.sales_create_invoice(self.request(data), self.db, self.user))
        self.assertIn('apertura', result.headers['location'])
        self.open()
        asyncio.run(web.sales_cierre_create(self.request({'cs_100': '5'}), self.db, self.user))
        result = asyncio.run(web.sales_create_invoice(self.request(data), self.db, self.user))
        self.assertIn('apertura', result.headers['location'])
        with patch.object(web, 'local_today', return_value=date(2026, 10, 7)):
            self.open('600')
        self.assertEqual(self.db.query(AperturaCaja).count(), 2)

    def test_zero_is_valid_and_other_companies_are_unchanged(self):
        self.open('0')
        self.assertEqual(self.db.query(AperturaCaja).one().monto_cs, 0)
        with patch.object(web, '_is_shoes_mode', return_value=False):
            self.assertIsNone(web._shoe_cash_opening(self.db, 2, self.day))

    def test_opening_added_once_to_expected_cash_and_close_is_idempotent(self):
        self.open()
        self.db.add(VentaFactura(numero='B-TEST', secuencia=1, bodega_id=2,
                                 fecha=datetime(2026, 10, 6, 12), moneda='CS',
                                 estado='ACTIVA', estado_cobranza='PAGADA', total_cs=200,
                                 total_usd=Decimal('200') / 37))
        self.db.commit()
        preview = web.sales_cierre(self.request(), self.db, self.user)
        self.assertEqual(preview['apertura_cs'], 500)
        self.assertAlmostEqual(preview['total_calculado_usd'] * 37, 700, places=2)
        result = asyncio.run(web.sales_cierre_create(self.request({'cs_100': '7'}), self.db, self.user))
        self.assertEqual(result.status_code, 303)
        close = self.db.query(CierreCaja).one()
        self.assertEqual(close.total_efectivo_cs, 700)
        self.assertEqual(close.diferencia_usd, 0)
        opening = self.db.query(AperturaCaja).one()
        self.assertEqual(opening.cierre_id, close.id)
        asyncio.run(web.sales_cierre_create(self.request({'cs_100': '7'}), self.db, self.user))
        self.assertEqual(self.db.query(CierreCaja).count(), 1)
        self.assertIn('cerrada', self.open().headers['location'])
        self.assertEqual(self.db.query(AperturaCaja).one().monto_cs, 500)
        response = web.sales_cierre_pdf(self.request(), close.id, self.db, self.user)
        async def read():
            return b''.join([chunk async for chunk in response.body_iterator])
        pdf = asyncio.run(read())
        self.assertTrue(pdf.startswith(b'%PDF'))
