"""Miss sales scope regression; data is isolated SQLite."""
import unittest
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web
from app.models.user import Branch, User, Role, Permission
from app.models.inventory import Bodega, Producto
from app.models.sales import VentaFactura, VentaItem

class DetailedSalesScopeTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        branches = [Branch(id=i, code=code, name=code, activo=i != 5)
                    for i, code in enumerate(('central', 'kg', 'kgf', 'esteli', 'closed'), 1)]
        self.db.add_all(branches)
        self.db.add_all([Bodega(id=b.id, branch_id=b.id, code=b.code, name=b.name) for b in branches])
        self.db.add(Producto(id=1, cod_producto='QA', descripcion='QA'))
        self.db.flush()
        self.admin = User(email='admin@qa.invalid', default_branch_id=1, default_bodega_id=1,
                          branches=[branches[0]], roles=[Role(name='administrador')])
        self.cashier = User(email='cashier@qa.invalid', default_branch_id=2, default_bodega_id=2,
                            branches=[branches[1]], roles=[Role(name='cajero', permissions=[Permission(name='access.reports')])])
        for i, (branch, amount, when, state) in enumerate([
            (1, 100, datetime(2026, 10, 24), 'ACTIVA'),
            (2, 200, datetime(2026, 10, 24, 12), 'ACTIVA'),
            (3, 300, datetime(2026, 10, 24, 23, 59, 59), 'ACTIVA'),
            (3, 400, datetime(2026, 10, 25), 'ACTIVA'),
            (2, 999, datetime(2026, 10, 24), 'ANULADA'),
            (4, 800, datetime(2026, 10, 24), 'ACTIVA'),
            (5, 900, datetime(2026, 10, 24), 'ACTIVA'),
        ], 1):
            self.db.add(VentaFactura(id=i, numero=str(i), bodega_id=branch, fecha=when,
                                    moneda='CS', tasa_cambio=37, total_cs=amount, estado=state))
            self.db.add(VentaItem(factura_id=i, producto_id=1, cantidad=1, subtotal_cs=amount, precio_unitario_cs=amount))
        self.db.commit()
        self.mode = patch.object(web, '_is_shoes_mode', return_value=True)
        self.codes = patch.object(web, '_allowed_branch_codes', return_value={'central', 'kg', 'kgf', 'closed'})
        self.mode.start(); self.codes.start()

    def tearDown(self):
        self.codes.stop(); self.mode.stop(); self.db.close(); self.engine.dispose()

    def report(self, user, branch='all'):
        return web._build_sales_report_rows(self.db, user, date(2026, 10, 24), date(2026, 10, 24), branch, None, '')

    def test_admin_all_stores_and_individual_filters(self):
        result = self.report(self.admin)
        self.assertEqual(result[2:5], (Decimal('600'), 3, 3.0))
        self.assertEqual(len(result[0]), 4)
        self.assertEqual(self.report(self.admin, '2')[2], Decimal('200'))
        self.assertEqual(self.report(self.admin, '3')[2], Decimal('300'))
        self.assertEqual(web._user_scoped_branch_ids(self.db, self.admin), {1})

    def test_cashier_cannot_report_another_store(self):
        self.assertEqual(self.report(self.cashier)[2], Decimal('200'))
        self.assertEqual(self.report(self.cashier, '3')[0], [])

    def test_other_companies_keep_existing_admin_scope(self):
        with patch.object(web, '_is_shoes_mode', return_value=False):
            self.assertEqual(self.report(self.admin)[2], Decimal('100'))

    def test_page_choices_match_query_scope(self):
        class Templates:
            def TemplateResponse(self, name, context): return context
        request = Request({'type': 'http', 'method': 'GET', 'path': '/reports/ventas', 'headers': [],
                           'query_string': b'start_date=2026-10-24&end_date=2026-10-24&branch_id=all',
                           'app': SimpleNamespace(state=SimpleNamespace(templates=Templates()))})
        for user, expected in ((self.admin, {1, 2, 3}), (self.cashier, {2})):
            context = web.report_sales_detailed(request, self.db, user)
            self.assertEqual({b.id for b in context['branches']}, expected)
