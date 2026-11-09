import unittest
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.database import Base
from app.routers import web as w
from starlette.requests import Request
from fastapi import HTTPException


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
        self.assertEqual([r['inicial'] for r in summary['saldos_por_bodega']], [2, 2])

    def test_backdated_receipt_keeps_document_day_and_opening_balance(self):
        receipt = self.db.get(w.IngresoInventario, 1)
        receipt.fecha = date(2026, 10, 11)
        receipt.created_at = datetime(2026, 10, 13, 18)
        self.db.get(w.IngresoInventario, 2).created_at = datetime(2026, 10, 12, 14)
        self.db.add(w.VentaFactura(id=1, numero='BACKDATE', bodega_id=1,
            fecha=datetime(2026, 10, 12, 9), created_at=datetime(2026, 10, 12, 15),
            moneda='CS', estado='ACTIVA'))
        self.db.add(w.VentaItem(factura_id=1, producto_id=1, cantidad=1))
        self.db.commit()
        rows, summary = w._build_kardex_movements(self.db, date(2026, 10, 11),
            self.today, 'all', 'HL-SHORTCAMS')
        self.assertEqual(rows[0]['source_id'], 1)
        self.assertEqual(rows[0]['source_kind'], 'ingreso')
        self.assertTrue(all(r['saldo'] >= 0 for r in rows))
        daily, daily_summary = self.report('1')
        self.assertEqual(daily_summary['saldo_inicial'], 2)
        self.assertEqual(daily[0]['saldo_anterior'], 2)
        self.assertEqual(daily[0]['saldo'], 1)
        self.assertEqual(summary['saldo_final'], 3)

    def test_registration_time_precedes_type_and_document_id(self):
        self.db.get(w.IngresoInventario, 1).created_at = datetime(2026, 10, 12, 21, 0, 0, 20)
        self.db.get(w.IngresoInventario, 2).created_at = datetime(2026, 10, 12, 14)
        self.db.add(w.EgresoTipo(id=1, nombre='Salida'))
        self.db.add(w.EgresoInventario(id=8, tipo_id=1, bodega_id=1, fecha=self.today,
            moneda='CS', created_at=datetime(2026, 10, 12, 21, 0, 0, 10)))
        self.db.add(w.EgresoItem(egreso_id=8, producto_id=1, cantidad=1))
        self.db.commit()
        rows, summary = self.report()
        self.assertEqual([(r['source_kind'], r['source_id']) for r in rows],
            [('ingreso', 2), ('egreso', 8), ('ingreso', 1)])
        self.assertEqual(rows[0]['registrado_en'], datetime(2026, 10, 12, 8))
        self.assertEqual([r['saldo'] for r in rows], [2, -1, 1])
        self.assertEqual(summary['saldo_final'], 3)

    def test_range_does_not_change_same_movement_balance(self):
        receipt = self.db.get(w.IngresoInventario, 1)
        receipt.fecha = date(2026, 10, 10)
        self.db.add(w.VentaFactura(id=1, numero='RANGE', bodega_id=1,
            fecha=datetime(2026, 10, 12, 9), moneda='CS', estado='ACTIVA'))
        self.db.add(w.VentaItem(factura_id=1, producto_id=1, cantidad=1))
        self.db.commit()
        full, _ = w._build_kardex_movements(self.db, date(2026, 10, 1), self.today, '1', '')
        short, summary = self.report('1')
        expected = next(r for r in full if r['source_kind'] == 'venta')
        self.assertEqual(short[0]['saldo_anterior'], expected['saldo_anterior'])
        self.assertEqual(short[0]['saldo'], expected['saldo'])
        self.assertEqual(summary['saldo_inicial'], 2)

    def test_local_pos_sale_follows_utc_inventory_receipt_without_false_negative(self):
        receipt = self.db.get(w.IngresoInventario, 1)
        receipt.created_at = datetime(2026, 10, 12, 22, 10, 50)
        self.db.add(w.VentaFactura(id=1, numero='TZ', bodega_id=1,
            fecha=datetime(2026, 10, 12, 16, 38),
            created_at=datetime(2026, 10, 12, 16, 38, 53), moneda='CS', estado='ACTIVA'))
        self.db.add(w.VentaItem(factura_id=1, producto_id=1, cantidad=1))
        self.db.commit()
        rows, summary = self.report('1')
        self.assertEqual([r['source_kind'] for r in rows], ['ingreso', 'venta'])
        self.assertEqual(rows[0]['registrado_en'], datetime(2026, 10, 12, 16, 10, 50))
        self.assertEqual(rows[1]['registrado_en'], datetime(2026, 10, 12, 16, 38, 53))
        self.assertEqual([r['saldo'] for r in rows], [2, 1])
        self.assertEqual(summary['movimientos_negativos'], 0)

    def test_invalid_ranges_are_rejected_instead_of_silently_using_today(self):
        for query in ('start_date=incorrecta', 'start_date=2026-10-20&end_date=2026-10-01'):
            with self.assertRaises(HTTPException) as exc:
                w._kardex_report_filters(Request({'type': 'http', 'query_string': query.encode()}))
            self.assertEqual(exc.exception.status_code, 400)
        start, end, _, _ = w._kardex_report_filters(Request({'type': 'http', 'query_string': b'end_date=2026-08-29'}))
        self.assertEqual(start, date(2026, 8, 29))
        self.assertEqual(start, end)

    def test_transfer_route_and_direct_pdf_for_both_warehouses(self):
        self.db.add(w.EgresoTipo(id=1, nombre='Traslado entre bodegas'))
        self.db.add(w.IngresoTipo(id=2, nombre='Traslado entre bodegas'))
        self.db.add(w.EgresoInventario(id=10, tipo_id=1, bodega_id=1, bodega_destino_id=2, fecha=self.today, moneda='CS'))
        self.db.add(w.EgresoItem(egreso_id=10, producto_id=1, cantidad=2))
        self.db.add(w.IngresoInventario(id=10, tipo_id=2, bodega_id=2, fecha=self.today, moneda='CS', observacion='Traslado desde Central hacia Custodio. Egreso #10'))
        self.db.add(w.IngresoItem(ingreso_id=10, producto_id=1, cantidad=2))
        self.db.commit()
        rows, summary = self.report()
        transfers = [r for r in rows if r['source_id'] == 10]
        self.assertEqual(len(transfers), 2)
        for row in transfers:
            self.assertEqual(row['transfer_route'], 'Central → Custodio')
            self.assertIn('Central → Custodio', row['concepto'])
            self.assertEqual(row['pdf_url'], f"/inventory/{'ingresos' if row['source_kind'] == 'ingreso' else 'egresos'}/10/pdf")
        self.assertEqual(summary['saldo_final'], 4)
