"""Requisition authorization and ledger invariants; isolated databases only."""
import io
import json
import unittest
from datetime import date, datetime
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.templating import Jinja2Templates
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from PIL import Image
from app.database import Base
from app.routers import web as w, inventory_requests as route
from app.core import inventory_requests as s
from app.models.inventory_requests import InventoryRequest, InventoryRequestItem, InventoryRequestEvent


class RequisitionTests(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite://',connect_args={'check_same_thread':False},poolclass=StaticPool)
        Base.metadata.create_all(self.engine);self.db=Session(self.engine)
        self.db.add_all([w.Branch(id=1,code='central',name='Central'),w.Branch(id=2,code='esteli',name='Estelí'),w.Bodega(id=1,code='central',name='Central',branch_id=1),w.Bodega(id=2,code='esteli',name='Estelí',branch_id=2),w.Producto(id=1,cod_producto='DL20',descripcion='Ropa de casa doméstica',costo_producto=100),w.Producto(id=2,cod_producto='HL9',descripcion='Ropa surtida',costo_producto=80),w.Vendedor(id=1,nombre='Vendedor Central'),w.Vendedor(id=2,nombre='Ana Estelí'),w.Vendedor(id=3,nombre='José Estelí')])
        self.db.add_all([w.VendedorBodega(vendedor_id=1,bodega_id=1),w.VendedorBodega(vendedor_id=2,bodega_id=2),w.VendedorBodega(vendedor_id=3,bodega_id=2)])
        self.admin=w.User(id=1,email='admin@qa.invalid',full_name='Admin QA',hashed_password='qa',roles=[w.Role(name='administrador')],default_branch_id=1,default_bodega_id=1)
        self.cashier=w.User(id=2,email='esteli@qa.invalid',full_name='Caja Estelí',hashed_password='qa',roles=[w.Role(name='cajero')],default_branch_id=2,default_bodega_id=2)
        self.central=w.User(id=3,email='central@qa.invalid',full_name='Caja Central',hashed_password='qa',roles=self.cashier.roles,default_branch_id=1,default_bodega_id=1)
        self.db.add_all([self.admin,self.cashier,self.central,w.IngresoTipo(id=1,nombre='Compra')]);self.db.flush()
        self.db.add(w.IngresoInventario(id=1,tipo_id=1,bodega_id=1,fecha=date(2026,11,1),moneda='CS'))
        self.db.add_all([w.IngresoItem(ingreso_id=1,producto_id=1,cantidad=10),w.IngresoItem(ingreso_id=1,producto_id=2,cantidad=2)])
        self.db.add(w.SaldoProducto(producto_id=1,existencia=10));self.db.commit();self.user=self.cashier
        self.real_notify=s.notify
        self.patches=[patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'_allowed_branch_codes',return_value={'central','esteli'}),patch.object(w,'local_today',return_value=date(2026,11,6)),patch.object(w,'local_now_naive',return_value=datetime(2026,11,6,14,30)),patch.object(s,'notify')]
        for p in self.patches:p.start()
        self.app=FastAPI();self.app.include_router(route.router)
        self.app.state.templates=Jinja2Templates(directory=str(Path(__file__).resolve().parents[1]/'app/templates'))
        self.app.dependency_overrides[w.get_db]=lambda:self.db
        self.app.dependency_overrides[route.user_web]=lambda:self.user
        self.client=TestClient(self.app)

    def tearDown(self):
        self.client.close()
        for p in reversed(self.patches):p.stop()
        self.db.close();self.engine.dispose()

    def data(self,**changes):
        values=dict(token=route.token(self.user,'create'),destination_id='2',seller_id='2',items=json.dumps([{'id':1,'quantity':2}]),observation='Preparar para envío')
        values.update(changes);return values

    def submit(self,values=None):
        return self.client.post(s.PATH,data=values or self.data(),follow_redirects=False)

    def record(self):
        self.assertEqual(self.submit().status_code,303)
        return self.db.query(InventoryRequest).one()

    def approve(self,ident,**changes):
        values={'token':route.token(self.user,f'approve:{ident}')};values.update(changes)
        return self.client.post(f'{s.PATH}/{ident}/autorizar',data=values,follow_redirects=False)

    def stock(self):
        self.db.expire_all()
        return w._balances_by_bodega(self.db,[1,2],[1,2])

    def test_request_records_actual_user_seller_date_without_moving_inventory(self):
        before=self.stock();record=self.record()
        self.assertEqual((record.requested_by,record.requester_name,record.seller_name),(2,'Caja Estelí','Ana Estelí'))
        self.assertEqual((record.fecha,record.created_at),(date(2026,11,6),datetime(2026,11,6,14,30)))
        self.assertEqual((record.source_id,record.destination_id,record.status),(1,2,'pending'))
        self.assertEqual(self.stock(),before)
        self.assertEqual(self.db.query(w.EgresoInventario).count(),0)
        self.assertEqual(self.db.query(InventoryRequestEvent).count(),1)
        s.notify.assert_called_once_with(record.id)

    def test_search_shows_both_warehouse_balances_and_accented_names(self):
        data=self.client.get(s.PATH+'/productos?q=ropa%20domestica').json()['items']
        self.assertEqual(len(data),1);self.assertEqual((data[0]['local'],data[0]['source']),(0,10))
        html=self.client.get(s.PATH).text
        self.assertIn('Ana Estelí',html);self.assertIn('José Estelí',html);self.assertNotIn('Vendedor Central</option>',html)

    def test_submit_replay_is_idempotent(self):
        values=self.data()
        self.assertEqual(self.submit(values).status_code,303);self.assertEqual(self.submit(values).status_code,303)
        self.assertEqual(self.db.query(InventoryRequest).count(),1);self.assertEqual(self.db.query(InventoryRequestItem).count(),1)
        s.notify.assert_called_once()

    def test_invalid_quantities_duplicate_products_and_seller_make_no_request(self):
        for qty in [0,-1,1.5,'2.0','NaN',True,1000001]:
            with self.subTest(qty=qty):
                response=self.submit(self.data(items=json.dumps([{'id':1,'quantity':qty}])))
                self.assertEqual(response.status_code,303);self.assertIn('error=',response.headers['location'])
        for changes in [dict(items='[]'),dict(items=json.dumps([{'id':1,'quantity':1},{'id':1,'quantity':1}])),dict(seller_id='1'),dict(items=json.dumps([{'id':999,'quantity':1}])),dict(observation='x'*1001)]:
            self.assertIn('error=',self.submit(self.data(**changes)).headers['location'])
        self.assertEqual(self.db.query(InventoryRequest).count(),0)

    def test_scope_cross_warehouse_and_admin_only_decisions(self):
        self.assertEqual(self.submit(self.data(destination_id='1')).status_code,403)
        record=self.record()
        self.assertEqual(self.approve(record.id).status_code,403)
        self.user=self.central
        self.assertEqual(self.client.get(f'{s.PATH}/{record.id}').status_code,404)
        self.user=w.User(id=99,roles=[w.Role(name='motorizado')])
        self.assertEqual(self.client.get(s.PATH).status_code,403)
        self.user=self.admin
        with patch.object(w,'_is_pacasholl_company',return_value=False):self.assertEqual(self.client.get(s.PATH).status_code,404)

    def test_approval_atomic_paired_transfer_and_replay(self):
        record=self.record();self.user=self.admin
        self.assertEqual(self.approve(record.id).status_code,303)
        self.db.refresh(record)
        self.assertEqual(record.status,'active');self.assertEqual(record.decided_by,1)
        self.assertEqual(self.stock()[(1,1)],8);self.assertEqual(self.stock()[(1,2)],2)
        self.assertEqual(self.db.query(w.SaldoProducto).filter_by(producto_id=1).one().existencia,10)
        out=self.db.get(w.EgresoInventario,record.egreso_id);receipt=self.db.get(w.IngresoInventario,record.ingreso_id)
        self.assertIn(f'Egreso #{out.id}',receipt.observacion);self.assertEqual(out.total_cs,200)
        self.assertEqual((out.fecha,receipt.fecha),(date(2026,11,6),date(2026,11,6)))
        self.assertEqual(self.approve(record.id).status_code,303)
        self.assertEqual(self.db.query(w.EgresoInventario).count(),1);self.assertEqual(self.db.query(w.IngresoInventario).count(),2)
        self.assertEqual(self.db.query(InventoryRequestEvent).count(),2)

    def test_insufficient_one_line_cancels_all_lines_without_stock_movements(self):
        self.assertEqual(self.submit(self.data(items=json.dumps([{'id':1,'quantity':2},{'id':2,'quantity':3}]))).status_code,303)
        record=self.db.query(InventoryRequest).one();self.user=self.admin;before=self.stock()
        self.assertEqual(self.approve(record.id).status_code,303)
        self.db.refresh(record);self.assertEqual(record.status,'cancelled');self.assertIn('HL9',record.decision_reason)
        self.assertEqual(self.stock(),before);self.assertEqual(self.db.query(w.EgresoInventario).count(),0)
        self.assertEqual(self.approve(record.id).status_code,303);self.assertEqual(self.db.query(InventoryRequestEvent).count(),2)

    def test_stock_is_rechecked_after_intervening_sale(self):
        record=self.record()
        self.db.add(w.VentaFactura(id=1,numero='QA-SALE',bodega_id=1,fecha=datetime(2026,11,6),moneda='CS',estado='ACTIVA'))
        self.db.add(w.VentaItem(factura_id=1,producto_id=1,cantidad=9));self.db.commit()
        self.user=self.admin;self.assertEqual(self.approve(record.id).status_code,303)
        self.db.refresh(record);self.assertEqual(record.status,'cancelled')
        self.assertEqual(self.stock()[(1,1)],1);self.assertEqual(self.db.query(w.EgresoInventario).count(),0)

    def test_failure_rolls_back_every_document_and_state(self):
        record=self.record();self.user=self.admin
        with patch.object(s,'event',side_effect=RuntimeError('write failure')):
            with self.assertRaises(RuntimeError):self.approve(record.id)
        self.db.expire_all();self.assertEqual(self.db.get(InventoryRequest,record.id).status,'pending')
        self.assertEqual(self.db.query(w.EgresoInventario).count(),0);self.assertEqual(self.db.query(w.IngresoInventario).count(),1)
        self.assertEqual(self.stock()[(1,1)],10)

    def test_csrf_origin_and_action_scope(self):
        self.assertEqual(self.submit(self.data(token='bad')).status_code,403)
        self.assertEqual(self.client.post(s.PATH,data=self.data(),headers={'Origin':'https://other.invalid'}).status_code,403)
        record=self.record();self.user=self.admin
        self.assertEqual(self.approve(record.id,token=route.token(self.user,f'cancel:{record.id}')).status_code,403)
        self.assertEqual(self.approve(record.id,token=route.token(self.cashier,f'approve:{record.id}')).status_code,403)

    def test_explicit_cancel_keeps_inventory_and_blocks_approval(self):
        record=self.record()
        response=self.client.post(f'{s.PATH}/{record.id}/anular',data={'token':route.token(self.user,f'cancel:{record.id}'),'reason':'Ya no se necesita'},follow_redirects=False)
        self.assertEqual(response.status_code,303);self.db.refresh(record);self.assertEqual(record.status,'cancelled')
        self.user=self.admin;self.approve(record.id);self.assertEqual(self.db.query(w.EgresoInventario).count(),0)

    def test_pending_detail_and_receipts_after_approval(self):
        record=self.record()
        response=self.client.get(f'{s.PATH}/{record.id}')
        self.assertEqual(response.status_code,200);self.assertIn('En espera de autorización',response.text)
        self.assertEqual(self.client.get(f'{s.PATH}/{record.id}/recibo').status_code,409)
        self.user=self.admin;self.assertIn('Autorizar y realizar traslado',self.client.get(f'{s.PATH}/{record.id}').text)
        self.approve(record.id)
        self.assertIn('Imprimir POS',self.client.get(f'{s.PATH}/{record.id}/recibo').text)
        image=self.client.get(f'{s.PATH}/{record.id}/recibo.jpg')
        self.assertEqual(image.status_code,200);self.assertEqual(image.headers['content-type'],'image/jpeg')
        self.assertEqual(Image.open(io.BytesIO(image.content)).format,'JPEG')

    def test_reverse_route_is_supported(self):
        self.user=self.central
        self.assertEqual(self.submit(self.data(destination_id=1,seller_id=1)).status_code,303)
        record=self.db.query(InventoryRequest).one()
        self.assertEqual((record.source_id,record.destination_id),(2,1))

    def test_mail_contains_authenticated_detail_link_and_escaped_observations(self):
        record=self.record();record.observation='<script>alert(1)</script>';self.db.commit()
        html=s.notification_html(record)
        self.assertIn(f'http://testserver{s.PATH}/{record.id}',html)
        self.assertIn('Revisar y autorizar',html);self.assertIn('DL20',html);self.assertIn('&lt;script&gt;',html)
        self.assertNotIn('/autorizar',record.review_url)

    def test_email_failure_is_visible_and_retry_sends_once(self):
        from sqlalchemy.orm import sessionmaker
        record=self.record()
        self.db.add(w.EmailConfig(sender_email='sender@qa.invalid',active=True))
        self.db.add(w.NotificationRecipient(email='receiver@qa.invalid',active=True));self.db.commit()
        with patch('app.database.get_session_local',return_value=sessionmaker(bind=self.engine)),patch.object(w,'_send_html_email',return_value='Error SMTP: timeout') as sender:
            self.real_notify(record.id)
            self.db.refresh(record);self.assertEqual(record.email_state,'failed');self.assertIn('timeout',record.email_error)
            sender.return_value=None
            self.real_notify(record.id);self.real_notify(record.id)
            self.db.refresh(record);self.assertEqual(record.email_state,'sent');self.assertIsNotNone(record.notified_at)
            self.assertEqual(sender.call_count,2)
            self.assertEqual(sender.call_args.kwargs['recipients'],['receiver@qa.invalid'])
            self.assertIn(f'/sales/requisas/{record.id}',sender.call_args.kwargs['html_body'])
        self.assertEqual(self.db.query(w.EgresoInventario).count(),0)

    def test_inactive_product_cancels_without_movement(self):
        record=self.record();self.db.get(w.Producto,1).activo=False;self.db.commit()
        self.user=self.admin;self.approve(record.id);self.db.refresh(record)
        self.assertEqual(record.status,'cancelled');self.assertEqual(self.db.query(w.EgresoInventario).count(),0)

    def test_email_login_return_is_local_and_only_requisition_paths(self):
        from starlette.requests import Request
        request=Request({'type':'http','method':'POST','path':'/login','headers':[],'query_string':b'','scheme':'http','server':('localhost',8001)})
        with patch.object(w,'verify_password',return_value=True),patch.object(w,'_is_shoes_mode',return_value=False):
            response=w.login_action(request,username=self.admin.email,password='qa',remember=None,requisition_next='/sales/requisas/12',db=self.db)
            self.assertEqual(response.headers['location'],'/sales/requisas/12')
            response=w.login_action(request,username=self.admin.email,password='qa',remember=None,requisition_next='//evil.invalid',db=self.db)
            self.assertEqual(response.headers['location'],'/home')

    def test_pending_shortage_detail_blocks_authorize_and_offers_cancel(self):
        self.assertEqual(self.submit(self.data(items=json.dumps([{'id':1,'quantity':11}]))).status_code,303)
        record=self.db.query(InventoryRequest).one();self.user=self.admin
        html=self.client.get(f'{s.PATH}/{record.id}').text
        self.assertIn('Anular por falta de saldo',html)
        self.assertNotIn('action="/sales/requisas/1/autorizar"',html)
