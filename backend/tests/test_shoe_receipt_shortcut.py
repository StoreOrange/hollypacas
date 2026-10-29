"""Atajo al ingreso individual: preselección autorizada, sin crear inventario."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from fastapi import HTTPException
from app.database import Base
from app.routers import web as w, shoe_unit_receipts as unit

class ReceiptShortcutTests(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
        self.db.add_all([w.Branch(id=1,code='central',name='Central'),w.Branch(id=3,code='kgf',name='KGF'),w.Bodega(id=1,branch_id=1,code='central',name='Central'),w.Bodega(id=3,branch_id=3,code='kgf',name='KGF'),w.Bodega(id=4,branch_id=3,code='inactive',name='Inactiva',activo=False)])
        self.db.commit()
        self.admin=w.User(email='admin',roles=[w.Role(name='administrador')],default_branch_id=1,default_bodega_id=1,branches=[self.db.get(w.Branch,1)])
        self.staff=w.User(email='staff',roles=[w.Role(name='contador')],default_branch_id=1,default_bodega_id=1,branches=[self.db.get(w.Branch,1)])
        self.company=patch.object(w,'get_active_company_key',return_value='bdzapatos');self.company.start()
        self.scope=patch.object(w,'_multi_branch_enabled_mode',return_value=True);self.scope.start()
    def tearDown(self):
        self.scope.stop();self.company.stop();self.db.close();self.engine.dispose()
    def request(self,query=''):
        class Templates:
            def TemplateResponse(self,name,context):return context
        return Request(dict(type='http',method='GET',path=unit.PATH,headers=[],query_string=query.encode(),app=SimpleNamespace(state=SimpleNamespace(templates=Templates()))))
    def test_admin_origin_preselection_and_scope_match_transfer_utilities(self):
        result=unit.page(self.request('bodega_id=3'),self.db,self.admin)
        self.assertEqual(result['selected_warehouse_id'],3)
        self.assertEqual({b.id for b in result['warehouses']},{1,3})
        self.assertEqual(self.db.query(w.IngresoInventario).count(),0)
    def test_default_preserved_without_origin_parameter(self):
        result=unit.page(self.request(),self.db,self.admin)
        self.assertEqual(result['selected_warehouse_id'],1)
    def test_invalid_unknown_or_inactive_origin_is_rejected(self):
        for value,status in [('bogus',400),('999',403),('4',403)]:
            with self.subTest(value=value),self.assertRaises(HTTPException) as error:unit.page(self.request('bodega_id='+value),self.db,self.admin)
            self.assertEqual(error.exception.status_code,status)
    def test_staff_does_not_gain_cross_store_access(self):
        with patch.object(w,'_enforce_permission'):
            self.assertEqual([b.id for b in unit.allowed_warehouses(self.db,self.staff)],[1])
            with self.assertRaises(HTTPException) as error:unit.page(self.request('bodega_id=3'),self.db,self.staff)
            self.assertEqual(error.exception.status_code,403)
    def test_cashier_does_not_gain_receipt_permission(self):
        cashier=w.User(email='cashier',roles=[w.Role(name='cajero')],default_branch_id=1,default_bodega_id=1)
        with self.assertRaises(HTTPException) as error:unit.page(self.request('bodega_id=1'),self.db,cashier)
        self.assertEqual(error.exception.status_code,303)

    def test_embedded_page_preserves_scope_and_initial_search(self):
        result=unit.page(self.request('bodega_id=3&embed=1&q=EQUAL02'),self.db,self.admin)
        self.assertTrue(result['embedded'])
        self.assertEqual(result['initial_query'],'EQUAL02')
        self.assertEqual(result['selected_warehouse_id'],3)
        self.assertEqual(self.db.query(w.IngresoInventario).count(),0)
