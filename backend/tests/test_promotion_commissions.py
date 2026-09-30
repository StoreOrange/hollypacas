import asyncio
import unittest
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.datastructures import FormData

from app.core.commission_rates import commission_rate, parse_commission_amount
from app.database import Base
from app.routers import web
from app.models.inventory import Producto, Bodega
from app.models.user import Branch
from app.models.sales import (Vendedor, VentaFactura, VentaItem, ProductoComision,
                              VentaComisionAsignacion, VentaComisionFinal)


def request_form(values):
    class Request:
        async def form(self):
            return FormData(values)
    return Request()


class PromotionCommissionsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.mode = patch.object(web, '_is_pacasholl_company', return_value=True)
        self.mode.start()
        self.permission = patch.object(web, '_enforce_permission')
        self.permission.start()
        self.day = date(2026, 9, 30)
        self.user = SimpleNamespace(full_name='Test')
        self.db.add_all([Branch(id=1, code='A', name='Sucursal'),
                         Bodega(id=1, code='B', name='Bodega', branch_id=1),
                         Vendedor(id=1, nombre='Ana'), Vendedor(id=2, nombre='Bea'),
                         Producto(id=1, cod_producto='PACA', descripcion='Paca prueba')])
        self.invoice = VentaFactura(id=1, numero='TEST-1', moneda='USD',
                                    fecha=datetime(2026, 9, 30, 10), vendedor_id=1, bodega_id=1)
        self.db.add(self.invoice)
        self.normal = VentaItem(id=1, factura_id=1, producto_id=1, cantidad=3,
                                precio_unitario_usd=100, subtotal_usd=300)
        self.promo = VentaItem(id=2, factura_id=1, producto_id=1, cantidad=4,
                               precio_unitario_usd=60, subtotal_usd=240,
                               combo_role='discount', combo_group='discount2-1-test')
        self.config = ProductoComision(producto_id=1, comision_usd=10, comision_promocion_usd=2)
        self.db.add_all([self.normal, self.promo, self.config]); self.db.commit()
        web._ensure_commission_temp_snapshot(self.db, self.day, 'all')

    def tearDown(self):
        self.db.close(); self.engine.dispose(); self.mode.stop(); self.permission.stop()

    def assignments(self):
        return web._build_commission_assignment_rows(self.db, self.day, self.day, 'all', '', '', '')

    def finalize(self):
        return asyncio.run(web.sales_comisiones_finalize_day(
            request_form({'fecha': str(self.day), 'branch_id': 'all'}), self.db, self.user))

    def test_same_product_normal_and_promotion(self):
        rows, *rest = self.assignments()
        by_item = {r['venta_item_id']: r for r in rows}
        self.assertEqual(by_item[1]['comision_unit_usd'], 10)
        self.assertEqual(by_item[1]['comision_total_usd'], 30)
        self.assertFalse(by_item[1]['comision_promocion'])
        self.assertEqual(by_item[2]['comision_unit_usd'], 2)
        self.assertEqual(by_item[2]['comision_total_usd'], 8) # 4 units, not 2 packages
        self.assertTrue(by_item[2]['comision_promocion'])
        self.assertEqual(rest[4], 38)
        report = web._build_commission_reports_data(self.db, self.day, self.day, 'all', '')
        self.assertEqual(report['total_comision_usd'], 38)
        self.assertEqual(sum(r['comision_promocion'] for r in report['detail_rows']), 1)

    def test_split_between_vendors_keeps_promotion_rate(self):
        original = self.db.query(VentaComisionAsignacion).filter_by(venta_item_id=2).one()
        original.cantidad = 1; original.subtotal_usd = 60
        self.db.add(VentaComisionAsignacion(venta_item_id=2, factura_id=1, branch_id=1,
             producto_id=1, bodega_id=1, fecha=self.day, vendedor_origen_id=1,
             vendedor_asignado_id=2, cantidad=3, precio_unitario_usd=60, subtotal_usd=180))
        self.db.commit()
        rows = [r for r in self.assignments()[0] if r['venta_item_id']==2]
        self.assertEqual(sorted(r['comision_total_usd'] for r in rows), [2, 6])
        self.assertTrue(all(r['comision_promocion'] for r in rows))
        self.finalize()
        rows = self.db.query(VentaComisionFinal).filter_by(venta_item_id=2).all()
        self.assertEqual(sorted(r.comision_total_usd for r in rows), [2, 6])

    def test_closing_stores_rates_and_later_price_edit_preserves_final(self):
        self.finalize()
        rows = {r.venta_item_id:r for r in self.db.query(VentaComisionFinal).all()}
        self.assertEqual(rows[1].comision_total_usd, 30)
        self.assertEqual(rows[2].comision_total_usd, 8)
        self.config.comision_promocion_usd=1; self.config.comision_usd=12; self.db.commit()
        self.assertEqual(rows[1].comision_unit_usd, 10)
        self.assertEqual(rows[2].comision_unit_usd, 2)

    def test_unset_promotion_is_pending_and_cannot_replace_final(self):
        self.finalize()
        self.config.comision_promocion_usd=None; self.db.commit()
        rows = self.assignments()[0]
        self.assertTrue(next(r for r in rows if r['venta_item_id']==2)['comision_pendiente'])
        response = self.finalize()
        error = parse_qs(urlsplit(response.headers['location']).query)['error'][0]
        self.assertIn('promocion', error)
        self.assertEqual(self.db.query(VentaComisionFinal).count(), 2)
        self.assertEqual(self.db.query(VentaComisionFinal).filter_by(venta_item_id=2).one().comision_total_usd, 8)
        missing=web._commission_missing_prices(self.db,self.day,self.day,'all')
        self.assertEqual(len(missing),1); self.assertIn('2 por precio especial',missing[0]['descripcion'])

    def test_explicit_zero_is_configured(self):
        self.config.comision_promocion_usd=0; self.db.commit()
        self.assertFalse(web._commission_missing_prices(self.db,self.day,self.day,'all'))
        self.finalize()
        self.assertEqual(self.db.query(VentaComisionFinal).filter_by(venta_item_id=2).one().comision_total_usd,0)

    def test_save_both_prices_and_partial_legacy_form(self):
        asyncio.run(web.sales_comisiones_save_prices(request_form({'comision_1':'11.25','comision_promocion_1':'1.50'}),self.db,self.user))
        self.assertEqual(self.config.comision_usd,Decimal('11.25'))
        self.assertEqual(self.config.comision_promocion_usd,Decimal('1.50'))
        asyncio.run(web.sales_comisiones_save_prices(request_form({'comision_1':'12'}),self.db,self.user))
        self.assertEqual(self.config.comision_promocion_usd,Decimal('1.50'))

    def test_invalid_price_does_not_partially_save(self):
        for invalid in ('-1','NaN','Infinity','abc','1.234'):
            with self.subTest(invalid=invalid):
                response=asyncio.run(web.sales_comisiones_save_prices(request_form({'comision_1':'13','comision_promocion_1':invalid}),self.db,self.user))
                self.assertIn('error=',response.headers['location'])
                self.assertEqual(self.config.comision_usd,10)
                self.assertEqual(self.config.comision_promocion_usd,2)

    def test_other_promotions_and_company_keep_normal(self):
        for role,group in [('gift','promotion-1'),('parent','promotion-1'),('parent','combo-1'),(None,None),('discount','other')]:
            self.assertEqual(commission_rate(self.config,SimpleNamespace(combo_role=role,combo_group=group)),10)
        self.assertEqual(commission_rate(self.config,self.promo,enabled=False),10)
        with patch.object(web,'_is_pacasholl_company',return_value=False):
            self.assertTrue(all(r['comision_unit_usd']==10 for r in self.assignments()[0]))

    def test_mobile_temporary_and_final_commissions_agree(self):
        data = web._build_mobile_assigned_commission_data(self.db, branch=None, bodega=None,
                    vendedor_id=1, start_date=self.day, end_date=self.day)
        self.assertEqual(Decimal(data["summary"]["total_comision_usd"]),38)
        self.assertEqual({d["tipo_comision"] for d in data["product_rows"][0]["details"]},
                         {"Normal", "2 por precio especial"})
        self.finalize()
        self.config.comision_promocion_usd=1; self.db.commit()
        data = web._build_mobile_assigned_commission_data(self.db, branch=None, bodega=None,
                    vendedor_id=1, start_date=self.day, end_date=self.day)
        self.assertEqual(Decimal(data["summary"]["total_comision_usd"]),38)

    def test_xlsx_export_preserves_type_and_marks_pending(self):
        import io
        from openpyxl import load_workbook
        from starlette.requests import Request
        self.config.comision_promocion_usd=None; self.db.commit()
        request=Request({"type":"http", "query_string":b"rep_start_date=2026-09-30&rep_end_date=2026-09-30"})
        with patch.object(web,"_scoped_branches_query",side_effect=lambda db:db.query(Branch)):
            response=web.sales_comisiones_reports_xlsx(request,self.db,self.user)
        async def read():
            return b"".join([chunk async for chunk in response.body_iterator])
        book=load_workbook(io.BytesIO(asyncio.run(read())))
        self.assertIn("TOTAL PARCIAL",book["Sabana"]["A3"].value)
        rows=list(book["Detalle"].values)
        promo=next(row for row in rows[1:] if row[-2]=="2 por precio especial")
        self.assertIsNone(promo[8]);self.assertIsNone(promo[9])
        self.assertEqual(promo[-1],"Sin configurar")

    def grid_update(self, promo, include=True):
        from app.models.inventory import ExchangeRate
        if not self.db.query(ExchangeRate).first():
            self.db.add(ExchangeRate(effective_date=date(2020,1,1),period="dia",rate=36.6))
            self.db.commit()
        row={"id":1,"costo":"50","precio":"100","comision":"10"}
        if include:
            row["comision_promocion"]=promo
        class Request:
            async def json(self): return {"rows":[row]}
        with patch.object(web,"_is_hollpacas_mode",return_value=True):
            return asyncio.run(web.inventory_grid_bulk_update(Request(),self.db,self.user))

    def test_inventory_grid_feeds_commissions_and_keeps_normal(self):
        response=self.grid_update("1.25")
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.config.comision_promocion_usd,Decimal("1.25"))
        rows={r["venta_item_id"]:r for r in self.assignments()[0]}
        self.assertEqual(rows[1]["comision_total_usd"],30)
        self.assertEqual(rows[2]["comision_total_usd"],5)
        self.grid_update(None,include=False)
        self.assertEqual(self.config.comision_promocion_usd,Decimal("1.25"))
        self.grid_update("0")
        self.assertEqual(self.config.comision_promocion_usd,0)
        self.grid_update("")
        self.assertIsNone(self.config.comision_promocion_usd)

    def test_inventory_rejects_invalid_promotion_before_updates(self):
        for value in ("-1","NaN","Infinity","bad","1.234"):
            with self.subTest(value=value):
                self.assertEqual(self.grid_update(value).status_code,400)
                self.assertEqual(self.config.comision_promocion_usd,2)
                self.assertEqual(self.config.comision_usd,10)

    def auto_payload(self, item=2):
        from app.core.commission_assignment_state import assignment_revision
        rows=self.db.query(VentaComisionAsignacion).filter_by(venta_item_id=item).order_by(VentaComisionAsignacion.id).all()
        return {"item_id":item,"revision":assignment_revision(rows),"rows":[{"temp_id":r.id,"client_id":str(r.id),"vendedor_id":r.vendedor_asignado_id,"cantidad":str(r.cantidad)} for r in rows]}

    def auto_save(self,payload,allowed=None):
        import json
        class Request:
            async def json(self):return payload
        with patch.object(web,"_user_scoped_branch_ids",return_value={1} if allowed is None else allowed):
            response=asyncio.run(web.sales_comisiones_autosave(Request(),self.db,self.user))
        return response.status_code,json.loads(response.body)

    def test_autosave_reassignment_split_delete_and_revision(self):
        payload=self.auto_payload()
        payload["rows"][0]["cantidad"]="1"
        payload["rows"].append({"temp_id":0,"client_id":"new","vendedor_id":2,"cantidad":"3"})
        code,result=self.auto_save(payload)
        self.assertEqual(code,200,result)
        self.assertEqual([r["comision_total_usd"] for r in result["rows"]],["2.00","6.00"])
        stale_code,_=self.auto_save(payload)
        self.assertEqual(stale_code,409)
        self.assertEqual(self.db.query(VentaComisionAsignacion).filter_by(venta_item_id=2).count(),2)
        payload=self.auto_payload()
        payload["rows"]=payload["rows"][:1]
        payload["rows"][0]["cantidad"]="4"
        payload["rows"][0]["vendedor_id"]=2
        code,result=self.auto_save(payload)
        self.assertEqual(code,200,result)
        self.assertEqual(result["rows"][0]["comision_total_usd"],"8.00")
        self.assertEqual(self.db.query(VentaComisionAsignacion).filter_by(venta_item_id=2).count(),1)
        next_payload=self.auto_payload()
        self.assertEqual(next_payload["revision"],result["revision"])

    def test_autosave_invalid_quantities_are_atomic(self):
        for quantity in ("-4","4.9","NaN","Infinity","", "5", "3"):
            with self.subTest(quantity=quantity):
                payload=self.auto_payload()
                payload["rows"][0]["vendedor_id"]=2
                payload["rows"][0]["cantidad"]=quantity
                code,_=self.auto_save(payload)
                self.assertEqual(code,400)
                row=self.db.query(VentaComisionAsignacion).filter_by(venta_item_id=2).one()
                self.assertEqual(row.cantidad,4);self.assertEqual(row.vendedor_asignado_id,1)

    def test_autosave_denies_foreign_row_vendor_and_branch(self):
        payload=self.auto_payload();payload["rows"][0]["temp_id"]=self.auto_payload(1)["rows"][0]["temp_id"]
        self.assertEqual(self.auto_save(payload)[0],400)
        payload=self.auto_payload();payload["rows"][0]["vendedor_id"]=99999
        self.assertEqual(self.auto_save(payload)[0],400)
        self.assertEqual(self.auto_save(self.auto_payload(),allowed={2})[0],403)
        payload=self.auto_payload();payload["rows"].append(dict(payload["rows"][0]))
        self.assertEqual(self.auto_save(payload)[0],400)

    def test_autosave_rejects_annulled_sale_and_preserves_closed_amounts(self):
        self.finalize()
        payload=self.auto_payload();payload["rows"][0]["vendedor_id"]=2
        self.assertEqual(self.auto_save(payload)[0],200)
        final=self.db.query(VentaComisionFinal).filter_by(venta_item_id=2).one()
        self.assertEqual(final.vendedor_asignado_id,1);self.assertEqual(final.comision_total_usd,8)
        self.invoice.estado="ANULADA";self.db.commit()
        self.assertEqual(self.auto_save(self.auto_payload())[0],409)

    def test_vendor_filter_does_not_duplicate_other_assignments(self):
        before=self.db.query(VentaComisionAsignacion).count()
        web._build_commission_assignment_rows(self.db,self.day,self.day,'all','','2','')
        self.assertEqual(self.db.query(VentaComisionAsignacion).count(),before)

    def test_rounding_matches_detail_summary_and_final(self):
        self.config.comision_usd=Decimal("0.15");self.config.comision_promocion_usd=Decimal("0.07");self.db.commit()
        expected=Decimal("0.73") # 3*.15 + 4*.07
        rows=self.assignments()[0]
        self.assertEqual(sum(Decimal(str(r["comision_total_usd"])) for r in rows),expected)
        report=web._build_commission_reports_data(self.db,self.day,self.day,'all','')
        self.assertEqual(Decimal(str(report["total_comision_usd"])),expected)
        self.finalize()
        self.assertEqual(sum(r.comision_total_usd for r in self.db.query(VentaComisionFinal).all()),expected)
        self.assertEqual(web._commission_amount(Decimal("0.15"),Decimal("1.5")),Decimal("0.23"))

    def test_report_excludes_annulled_sale_even_before_snapshot_refresh(self):
        self.invoice.estado="ANULADA";self.db.commit()
        report=web._build_commission_reports_data(self.db,self.day,self.day,'all','',normalize=False)
        self.assertEqual(report["detail_rows"],[])
        self.assertEqual(report["total_comision_usd"],0)

    def test_annulled_sale_removed_from_assignments(self):
        self.invoice.estado='ANULADA';self.db.commit()
        web._ensure_commission_temp_snapshot(self.db,self.day,'all')
        self.assertEqual(self.assignments()[0],[])


if __name__=='__main__':
    unittest.main()
