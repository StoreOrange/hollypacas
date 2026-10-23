"""Promociones móviles: precios autoritativos y paquete indivisible hasta facturar."""
import asyncio
import json
import unittest
from decimal import Decimal
from unittest.mock import patch
from starlette.datastructures import FormData
from tests import test_mobile_preventas_libreados as libreado
from app.routers import web as w

class MobileDiscountTests(unittest.TestCase):
    tearDown = libreado.LibreadoMobileTests.tearDown
    def setUp(self):
        libreado.LibreadoMobileTests.setUp(self)
        self.db.get(w.Producto,1).es_libreado=False
        self.db.get(w.IngresoItem,1).cantidad=20
        self.db.add(w.PromocionDescuentoProducto(id=1,producto_id=1,cantidad_paquete=2,precio_paquete_usd=3,cupo_unidades=10))
        self.db.add(w.PromocionDescuentoSucursal(promocion_id=1,branch_id=1,precio_paquete_usd=3,cupo_unidades=10,activo=True))
        self.db.commit()
    def request(self, qty='2', **changes):
        req=libreado.LibreadoMobileTests.request(self,weight='0',qty=qty)
        values=dict(req._form)
        values.update(item_role='discount',item_combo_group='discount2-1-mobiletest',item_precio_usd='0.01',item_precio_cs='0.01')
        values.update(changes);req._form=FormData(values)
        return req
    def create(self, req=None):
        return asyncio.run(w.mobile_preventas_create(req or self.request(),self.db,self.user))
    def invoice(self, pre, qty='2', group='discount2-1-mobiletest', role='discount'):
        req=self.request(qty,operation_key='b'*32,preventa_id=str(pre.id),condicion_venta='CREDITO',item_precio='0.01',item_combo_group=group,item_role=role)
        with patch.object(w,'_enforce_permission'),patch.object(w,'_is_shoes_mode',return_value=False):
            return asyncio.run(w.sales_create_invoice(req,self.db,self.user))
    def test_price_and_quantity_survive_preventa_and_invoice(self):
        self.assertIn('result_preventa=',self.create().headers['location'])
        item=self.db.query(w.PreventaItem).one()
        self.assertEqual(item.cantidad,2);self.assertEqual(item.precio_unitario_usd,Decimal('1.50'));self.assertEqual(item.subtotal_usd,3)
        pre=self.db.query(w.Preventa).one();result=self.invoice(pre)
        self.assertNotIn('error=',result.headers['location'],result.headers['location'])
        sold=self.db.query(w.VentaItem).one()
        self.assertEqual(sold.cantidad,2);self.assertEqual(sold.subtotal_usd,3);self.assertEqual(sold.combo_role,'discount');self.assertEqual(pre.estado,'FACTURADA')
        self.assertEqual(w._discount_promotion_used_units(self.db,1,1),2)
        self.assertEqual(w._balances_by_bodega(self.db,[1],[1])[(1,1)],18)
    def test_tampered_quantities_do_not_create_preventa(self):
        for qty in ('1','3','4','2.1','NaN','Infinity','-2','0'):
            with self.subTest(qty=qty):
                self.assertIn('error=',self.create(self.request(qty)).headers['location']);self.assertEqual(self.db.query(w.Preventa).count(),0)
    def test_invoice_cannot_change_or_strip_saved_package(self):
        self.create();pre=self.db.query(w.Preventa).one()
        for qty,group,role in [('4','discount2-1-mobiletest','discount'),('1','discount2-1-mobiletest','discount'),('2','','')]:
            with self.subTest(qty=qty,role=role):
                result=self.invoice(pre,qty,group,role)
                self.assertIn('error=',result.headers['location']);self.assertEqual(self.db.query(w.VentaFactura).count(),0)
    def test_inactive_or_foreign_promotion_cannot_be_used(self):
        allocation=self.db.query(w.PromocionDescuentoSucursal).one();allocation.activo=False;self.db.commit()
        self.assertIn('error=',self.create().headers['location'])
        allocation.activo=True;allocation.branch_id=999;self.db.commit()
        self.assertIn('error=',self.create().headers['location']);self.assertEqual(self.db.query(w.Preventa).count(),0)
    def test_pending_packages_reserve_quota_and_listing(self):
        allocation=self.db.query(w.PromocionDescuentoSucursal).one();allocation.cupo_unidades=2;self.db.commit()
        self.create();payload=json.loads(w.mobile_discount_promotions(self.request(),self.db,self.user).body)
        self.assertEqual(payload['items'][0]['available_packages'],0)
        self.assertIn('error=',self.create(self.request(item_combo_group='discount2-1-second')).headers['location']);self.assertEqual(self.db.query(w.Preventa).count(),1)
    def test_stock_check_and_plain_product_price(self):
        self.db.get(w.IngresoItem,1).cantidad=1;self.db.commit();self.assertIn('error=',self.create().headers['location'])
        self.db.get(w.IngresoItem,1).cantidad=20;self.db.commit();self.create(self.request(item_role='',item_combo_group=''))
        self.assertEqual(self.db.query(w.PreventaItem).one().precio_unitario_usd,2)

    def test_own_pending_package_reserves_physical_stock(self):
        self.db.get(w.IngresoItem,1).cantidad=2;self.db.commit()
        self.assertIn('result_preventa=',self.create().headers['location'])
        self.assertIn('error=',self.create(self.request(item_combo_group='discount2-1-second')).headers['location'])
        self.assertEqual(self.db.query(w.Preventa).count(),1)

    def test_two_packages_share_quota_and_duplicate_group_is_rejected(self):
        allocation=self.db.query(w.PromocionDescuentoSucursal).one()
        allocation.cupo_unidades=2;self.db.commit()
        req=self.request()
        data=list(req._form.multi_items())
        second=[('item_producto_id','1'),('item_cantidad','2'),('item_role','discount'),('item_combo_group','discount2-1-second')]
        req._form=FormData(data+second)
        self.assertIn('error=',self.create(req).headers['location'])
        self.assertEqual(self.db.query(w.Preventa).count(),0)
        allocation.cupo_unidades=4;self.db.commit()
        self.assertIn('result_preventa=',self.create(req).headers['location'])
        self.assertEqual(self.db.query(w.Preventa).one().total_items,4)
        self.assertEqual(self.db.query(w.Preventa).one().total_usd,6)
