"""Costos y laboratorio en una sola bodega; SQLite aislada."""
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from fastapi import HTTPException
from app.database import Base
from app.routers import web as w
from app.core.inventory_type_report import inventory_type_rows
from app.main import app

class InventoryTypeReportTests(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
        self.day=date(2026,10,20)
        self.db.add_all([w.Branch(id=1,code='central',name='Central'),w.Branch(id=2,code='other',name='Otra'),w.Bodega(id=1,branch_id=1,code='central',name='Central'),w.Bodega(id=2,branch_id=2,code='other',name='Otra'),w.Producto(id=1,cod_producto='A',descripcion='Paca A'),w.Producto(id=2,cod_producto='B',descripcion='Paca B'),w.IngresoTipo(id=1,nombre='Producción'),w.IngresoTipo(id=2,nombre='Traslado'),w.EgresoTipo(id=1,nombre='Producción'),w.EgresoTipo(id=2,nombre='Traslado')])
        for ident,warehouse,kind,day in [(1,1,1,self.day),(2,1,2,self.day),(3,2,1,self.day),(4,1,1,date(2026,9,1))]:
            self.db.add(w.IngresoInventario(id=ident,bodega_id=warehouse,tipo_id=kind,fecha=day,moneda='CS',tasa_cambio=40))
            self.db.add(w.IngresoItem(ingreso_id=ident,producto_id=1,cantidad=2,subtotal_cs=100,subtotal_usd=Decimal('2.5')))
        self.db.add(w.IngresoItem(ingreso_id=1,producto_id=2,cantidad=1,costo_unitario_cs=50,costo_unitario_usd=Decimal('1.25')))
        self.db.add(w.EgresoInventario(id=1,bodega_id=1,tipo_id=1,fecha=self.day,moneda='CS'))
        self.db.add(w.EgresoItem(egreso_id=1,producto_id=1,cantidad=1,subtotal_cs=80,subtotal_usd=2))
        self.db.add(w.ProductionLaboratory(id=1,numero='LAB-TEST',fecha=date(2026,9,1),bodega_origen_id=1,bodega_destino_id=1))
        self.db.add(w.ProductionLaboratoryMovement(laboratorio_id=1,egreso_id=1,ingreso_id=1,clase='AJUSTE'))
        self.db.commit()
        self.user=w.User(email='audit',roles=[w.Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
        self.patches=[patch.object(w,'_enforce_permission'),patch.object(w,'_is_hollpacas_mode',return_value=True),patch.object(w,'_utility_branch_ids',return_value={1}),patch.object(w,'local_today',return_value=self.day)]
        for p in self.patches:p.start()
    def tearDown(self):
        for p in self.patches:p.stop()
        self.db.close();self.engine.dispose()
    def request(self,query=''):
        request=Request(dict(type='http',method='GET',path='/reports/movimientos-tipo',scheme='http',server=('test',80),headers=[],query_string=query.encode(),app=app,router=app.router))
        request.state.branding={'company_key':'hollywood_pacas'}
        return request
    def test_types_and_costs_are_not_duplicated_by_items_or_lab(self):
        groups,totals=inventory_type_rows(self.db,self.day,self.day,1)
        self.assertEqual(len(groups),3)
        self.assertEqual(totals['Ingreso']['documents'],2)
        self.assertEqual(totals['Ingreso']['cost_cs'],250)
        self.assertEqual(totals['Ingreso']['quantity'],5)
        self.assertEqual(totals['Egreso']['cost_cs'],80)
        row=next(r for g in groups for r in g['rows'] if r['direction']=='Ingreso' and r['id']==1)
        self.assertEqual(row['cost_cs'],150);self.assertEqual(row['lab_number'],'LAB-TEST')
        self.assertEqual(row['lab_date'],date(2026,9,1))
    def test_direction_type_date_and_warehouse_filters(self):
        groups,totals=inventory_type_rows(self.db,self.day,self.day,1,'ingreso',income_type=2)
        self.assertEqual(len(groups),1);self.assertEqual(totals['Ingreso']['cost_cs'],100)
        self.assertEqual(totals['Egreso']['documents'],0)
        groups,totals=inventory_type_rows(self.db,self.day,self.day,2)
        self.assertEqual(totals['Ingreso']['cost_cs'],100)
        groups,totals=inventory_type_rows(self.db,date(2025,1,1),date(2025,1,2),1)
        self.assertEqual(groups,[]);self.assertEqual(totals['Ingreso']['cost_cs'],0)
    def test_conversion_uses_each_document_rate_and_missing_values(self):
        self.db.add(w.IngresoItem(ingreso_id=1,producto_id=1,cantidad=1,subtotal_usd=2))
        self.db.commit()
        groups,totals=inventory_type_rows(self.db,self.day,self.day,1,'ingreso',income_type=1)
        self.assertEqual(totals['Ingreso']['cost_cs'],230)
        self.assertEqual(totals['Ingreso']['cost_usd'],Decimal('5.75'))
    def test_report_keeps_single_warehouse_and_links_exact_lab_date(self):
        response=w.report_inventory_types(self.request('start_date=2026-10-20&end_date=2026-10-20&bodega_id=1'),self.db,self.user)
        html=response.body.decode()
        self.assertIn('focus_laboratorio=1#labDetail1',html)
        self.assertIn('start_date=2026-09-01&amp;end_date=2026-09-01',html)
        self.assertIn('Sin laboratorio',html)
        self.assertEqual(response.context['warehouse'].id,1)
        self.assertEqual(len(response.context['warehouses']),1)
    def test_forbidden_scope_and_bad_filters(self):
        for query,status in [('bodega_id=2',403),('branch_id=2',403),('start_date=no',400),('direction=sale',400),('ingreso_tipo_id=999',400)]:
            with self.subTest(query=query),self.assertRaises(HTTPException) as error:w.report_inventory_types(self.request(query),self.db,self.user)
            self.assertEqual(error.exception.status_code,status)
        with patch.object(w,'_utility_branch_ids',return_value={1,2}):
            with self.assertRaises(HTTPException):w.report_inventory_types(self.request('branch_id=1&bodega_id=2'),self.db,self.user)
    def test_lab_link_does_not_grant_permission(self):
        with patch.object(w,'_has_permission',return_value=False):
            response=w.report_inventory_types(self.request('start_date=2026-10-20&end_date=2026-10-20'),self.db,self.user)
            self.assertNotIn('focus_laboratorio=',response.body.decode())
            self.assertIn('LAB-TEST',response.body.decode())
    def test_company_permission_and_empty_warehouse_scope(self):
        with patch.object(w,'_is_hollpacas_mode',return_value=False):
            with self.assertRaises(HTTPException) as error:w.report_inventory_types(self.request(),self.db,self.user)
            self.assertEqual(error.exception.status_code,404)
        with patch.object(w,'_enforce_permission',side_effect=HTTPException(403)):
            with self.assertRaises(HTTPException) as error:w.report_inventory_types(self.request(),self.db,self.user)
            self.assertEqual(error.exception.status_code,403)
        with patch.object(w,'_utility_branch_ids',return_value=set()):
            with self.assertRaises(HTTPException) as error:w.report_inventory_types(self.request(),self.db,self.user)
            self.assertEqual(error.exception.status_code,403)
    def test_reversed_dates_and_note_do_not_invent_lab_relation(self):
        self.db.get(w.IngresoInventario,2).observacion='LAB-TEST producción'
        self.db.commit()
        response=w.report_inventory_types(self.request('start_date=2026-10-21&end_date=2026-10-19'),self.db,self.user)
        self.assertEqual(response.context['start_date'],'2026-10-19')
        self.assertEqual(response.context['end_date'],'2026-10-21')
        row=next(r for g in response.context['groups'] for r in g['rows'] if r['direction']=='Ingreso' and r['id']==2)
        self.assertIsNone(row['lab_id'])
