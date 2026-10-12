import unittest
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.database import Base
from app.routers import web as w


class KardexConsolidatedTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.today = date(2026, 10, 12)
        self.db.add_all([
            w.Branch(id=1, code='central', name='Central'),
            w.Bodega(id=1, branch_id=1, code='central', name='Central'),
            w.Bodega(id=2, branch_id=1, code='custodio', name='Custodio'),
            w.Producto(id=1, cod_producto='HL-SHORTCAMS', descripcion='Short'),
            w.IngresoTipo(id=1, nombre='Produccion'),
        ])
        for ident, warehouse in [(1, 1), (2, 2)]:
            self.db.add(w.IngresoInventario(id=ident, tipo_id=1, bodega_id=warehouse, fecha=self.today, moneda='USD'))
            self.db.add(w.IngresoItem(ingreso_id=ident, producto_id=1, cantidad=2))
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.engine.dispose()

    def report(self, warehouse='all', day=None):
        return w._build_kardex_movements(self.db, day or self.today, day or self.today, warehouse, 'HL-SHORTCAMS')

    def test_two_laboratories_in_different_warehouses_total_four(self):
        rows, summary = self.report()
        self.assertEqual([r['saldo'] for r in rows], [Decimal('2'), Decimal('2')])
        self.assertEqual(summary['total_ingresos'], 4)
        self.assertEqual(summary['saldo_final'], 4)
        self.assertEqual(self.report('1')[1]['saldo_final'], 2)

    def test_sale_reduces_consolidated_balance_to_three(self):
        self.db.add(w.VentaFactura(id=1, numero='TEST', fecha=datetime(2026, 10, 12, 8, 35), bodega_id=1, moneda='USD', estado='ACTIVA'))
        self.db.add(w.VentaItem(factura_id=1, producto_id=1, cantidad=1))
        self.db.commit()
        summary = self.report()[1]
        self.assertEqual(summary['total_ingresos'], 4)
        self.assertEqual(summary['total_ventas'], 1)
        self.assertEqual(summary['saldo_final'], 3)

    def test_opening_stock_counts_even_without_current_movements(self):
        rows, summary = self.report(day=date(2026, 10, 13))
        self.assertEqual(rows, [])
        self.assertEqual(summary['saldo_inicial'], 4)
        self.assertEqual(summary['saldo_final'], 4)
