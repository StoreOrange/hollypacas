import asyncio,hashlib,json,unittest
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from fastapi import HTTPException
from app.database import Base
from app.routers import web,pos_automation as routes
from app.core import pos_automation as auto
from app.models.pos_automation import PosAutoStation,PosAutoTask,PosPrintJob
from app.models.user import User,Role,Branch
from app.models.inventory import Bodega,Producto,IngresoTipo,IngresoInventario,IngresoItem,ExchangeRate
from app.models.sales import Cliente,Vendedor,Preventa,PreventaItem,FormaPago,VentaFactura,VentaPago,AccountingEntry


class AutoPosTests(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine,autoflush=False)
        self.now=datetime(2026,10,15,12,0);self.clock=self.now
        self.db.add_all([Branch(id=1,code='central',name='Central'),Bodega(id=1,branch_id=1,code='central',name='Central'),Cliente(id=1,nombre='Cliente'),Vendedor(id=1,nombre='Vendedor'),FormaPago(id=1,nombre='Efectivo'),Producto(id=1,cod_producto='LIB',descripcion='Ropa por libra',es_libreado=True,es_por_peso=True,precio_venta1_usd=2,precio_venta1=74,costo_producto=30),IngresoTipo(id=1,nombre='Compra'),IngresoInventario(id=1,tipo_id=1,bodega_id=1,fecha=self.now.date(),moneda='CS'),IngresoItem(ingreso_id=1,producto_id=1,cantidad=4),ExchangeRate(effective_date=self.now.date(),period='DIA',rate=37)])
        self.user=User(id=1,email='qa@example.invalid',full_name='Caja prueba',hashed_password='not-used',is_active=True,roles=[Role(name='administrador')],default_branch_id=1,default_bodega_id=1);self.db.add(self.user);self.db.commit()
        from app.core.init_db import _seed_cuentas_contables,_seed_accounting_voucher_types
        _seed_cuentas_contables(self.db);_seed_accounting_voucher_types(self.db)
        self.station=PosAutoStation(id='a'*32,user_id=1,branch_id=1,bodega_id=1,token_hash=hashlib.sha256(b'test').hexdigest(),enabled=False,agent_seen=self.now,agent_ready=True,printer_name='EPSON TM-T20');self.db.add(self.station);self.db.commit()
        self.patches=[patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_is_shoes_mode',return_value=False),patch.object(web,'local_today',side_effect=lambda:self.clock.date()),patch.object(web,'local_now_naive',side_effect=lambda:self.clock),patch.object(web,'local_now',side_effect=lambda:self.clock.replace(tzinfo=timezone.utc)),patch.object(web,'_send_mobile_preventa_push_notifications')]
        for item in self.patches:item.start()

    def tearDown(self):
        for item in reversed(self.patches):item.stop()
        self.db.close();self.engine.dispose()

    def request(self,payload=None,token=None):
        headers=[(b'host',b'test.local')]
        if token:headers.append((b'authorization',('Bearer '+token).encode()))
        req=Request({'type':'http','method':'POST','scheme':'https','server':('test.local',443),'path':'/sales/auto-preventas','headers':headers,'query_string':b''});req._json=payload or {};return req

    def sale(self,ident=1,**kwargs):
        sale=Preventa(id=ident,numero='PV-'+str(ident),branch_id=1,bodega_id=1,vendedor_id=1,cliente_id=1,fecha=kwargs.pop('fecha',self.now),estado=kwargs.pop('estado','PENDIENTE'),total_cs=925,total_usd=25,total_items=1,**kwargs)
        sale.items=[PreventaItem(producto_id=1,cantidad=1,peso_lbs=Decimal('12.5'),precio_unitario_cs=74,precio_unitario_usd=2,subtotal_cs=925,subtotal_usd=25)]
        self.db.add(sale);self.db.commit();return sale

    def activate(self):auto.set_enabled(self.db,self.station,True,self.clock);self.db.commit()

    def due(self):
        self.activate();sale=self.sale();tasks=auto.prepare(self.db,self.station,self.clock);self.db.commit()
        self.clock+=timedelta(seconds=15);self.station.agent_seen=self.clock;self.db.commit();return sale,tasks[0]

    def test_old_pending_excluded_and_timer_is_durable(self):
        self.sale();self.activate();self.sale(2);self.sale(3,is_frozen=True);self.sale(4,fecha=self.now-timedelta(days=1))
        tasks=auto.prepare(self.db,self.station,self.clock);self.assertEqual([t.preventa_id for t in tasks],[2]);self.assertEqual(tasks[0].due_at,self.now+timedelta(seconds=15));self.db.commit()
        self.clock+=timedelta(seconds=5);same=auto.prepare(self.db,self.station,self.clock);self.assertEqual(same[0].due_at,self.now+timedelta(seconds=15))

    def test_invoice_cash_cordobas_weight_and_accounting_once(self):
        sale,task=self.due()
        with patch.object(web,'_print_pos_ticket') as server_print:
            asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock))
            server_print.assert_not_called()
        invoice=self.db.query(VentaFactura).one();payment=self.db.query(VentaPago).one();job=self.db.query(PosPrintJob).one()
        self.assertEqual(invoice.total_cs,925);self.assertEqual(invoice.moneda,'CS');self.assertEqual(payment.moneda,'CS');self.assertEqual(payment.monto_cs,925);self.assertEqual(payment.forma_pago_id,1)
        self.assertEqual(invoice.items[0].peso_lbs,Decimal('12.50'));self.assertEqual(invoice.items[0].cantidad,1)
        self.assertEqual(sale.estado,'FACTURADA');self.assertEqual(task.status,'FACTURADA');self.assertEqual(job.invoice_id,invoice.id)
        self.assertGreater(self.db.query(AccountingEntry).count(),0)
        count=self.db.query(AccountingEntry).count()
        asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock))
        self.assertEqual(self.db.query(VentaFactura).count(),1);self.assertEqual(self.db.query(PosPrintJob).count(),1);self.assertEqual(self.db.query(AccountingEntry).count(),count)

    def test_early_offline_and_busy_never_invoice(self):
        self.activate();self.sale();task=auto.prepare(self.db,self.station,self.clock)[0];self.db.commit()
        with self.assertRaisesRegex(ValueError,'15 segundos'):asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock))
        self.clock+=timedelta(seconds=21)  # Plazo vencido y agente sin latido por más de 20 s.
        asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock));self.assertEqual(self.db.query(VentaFactura).count(),0)
        self.station.agent_seen=self.clock;self.db.commit()
        asyncio.run(routes.tick(self.station.id,self.request({'busy':True}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),0)

    def test_frozen_and_cancelled_not_billed(self):
        sale,task=self.due();sale.is_frozen=True;self.db.commit()
        asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock));self.assertEqual(task.status,'CANCELLED');self.assertEqual(self.db.query(VentaFactura).count(),0)

    def test_unique_owner_and_deactivation(self):
        self.activate();second=PosAutoStation(id='b'*32,user_id=1,branch_id=1,bodega_id=1,enabled=False,agent_ready=True,agent_seen=self.now);self.db.add(second);self.db.commit()
        with self.assertRaisesRegex(ValueError,'Otra caja'):auto.set_enabled(self.db,second,True,self.clock)
        self.sale();auto.prepare(self.db,self.station,self.clock);self.db.commit();auto.set_enabled(self.db,self.station,False,self.clock)
        self.assertEqual(self.db.query(PosAutoTask).one().status,'CANCELLED')

    def test_company_and_agent_scope(self):
        with patch.object(web,'_is_pacasholl_company',return_value=False):
            with self.assertRaises(HTTPException) as exc:routes.agent(self.request(token='test'),self.db,self.station.id)
            self.assertEqual(exc.exception.status_code,404)
        with self.assertRaises(HTTPException):routes.agent(self.request(token='wrong'),self.db,self.station.id)
        self.assertEqual(routes.agent(self.request(token='test'),self.db,self.station.id).id,self.station.id)

    def test_dispatch_ack_and_reprint_keep_same_invoice(self):
        sale,task=self.due();asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock))
        payload={'ready':True,'printer':'EPSON TM-T20','message':'Lista'}
        result=asyncio.run(routes.next_job(self.station.id,self.request(payload,'test'),self.db));job=result['job'];self.assertIsNotNone(job);self.assertTrue(job['pdf'].startswith('JVBER'))
        second=asyncio.run(routes.next_job(self.station.id,self.request(payload,'test'),self.db));self.assertIsNone(second['job'])
        with self.assertRaises(HTTPException):asyncio.run(routes.ack(self.station.id,job['id'],self.request({'claim_id':'wrong','status':'SENT'},'test'),self.db))
        asyncio.run(routes.ack(self.station.id,job['id'],self.request({'claim_id':job['claim_id'],'status':'UNKNOWN','message':'Se interrumpió'},'test'),self.db))
        asyncio.run(routes.retry(self.station.id,job['id'],self.request({'confirmed':True}),self.db,self.user))
        replacement=asyncio.run(routes.next_job(self.station.id,self.request(payload,'test'),self.db))['job'];self.assertNotEqual(replacement['claim_id'],job['claim_id']);self.assertEqual(replacement['id'],job['id']);self.assertEqual(self.db.query(VentaFactura).count(),1)
        asyncio.run(routes.ack(self.station.id,job['id'],self.request({'claim_id':replacement['claim_id'],'status':'SENT','message':'Enviado'},'test'),self.db))
        with self.assertRaises(HTTPException):asyncio.run(routes.retry(self.station.id,job['id'],self.request({'confirmed':True}),self.db,self.user))

    def test_stock_failure_leaves_no_invoice_or_print_job(self):
        sale,task=self.due();self.db.query(IngresoItem).delete();self.db.commit()
        payload=asyncio.run(routes.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
        self.assertEqual(self.db.query(VentaFactura).count(),0);self.assertEqual(self.db.query(PosPrintJob).count(),0)
        self.assertEqual(self.db.get(PosAutoTask,task.id).status,'ERROR');self.assertEqual(self.db.get(Preventa,sale.id).estado,'PENDIENTE')
        self.assertIn('Stock insuficiente',payload['tasks'][0]['message'])
        asyncio.run(routes.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),0)

    def test_closed_cash_does_not_generate_sale(self):
        sale,task=self.due();self.db.add(web.CierreCaja(branch_id=1,bodega_id=1,fecha=self.clock.date()));self.db.commit()
        with self.assertRaisesRegex(ValueError,'cerrada'):asyncio.run(auto.invoice_due(self.db,self.station,task,self.request(),self.user,self.clock))
        self.assertEqual(self.db.query(VentaFactura).count(),0)

    def test_revoke_stops_agent_and_pending_timer(self):
        self.activate();self.sale();auto.prepare(self.db,self.station,self.clock);self.db.commit()
        routes.revoke(self.station.id,self.request(),self.db,self.user)
        self.assertFalse(self.station.enabled);self.assertIsNone(self.station.token_hash);self.assertEqual(self.db.query(PosAutoTask).one().status,'CANCELLED')
        with self.assertRaises(HTTPException):routes.agent(self.request(token='test'),self.db,self.station.id)

    def test_installer_is_scoped_zip_and_rotates_secret(self):
        import zipfile,io
        response=routes.installer(self.station.id,self.request(),self.db,self.user)
        self.assertEqual(response.headers['cache-control'],'no-store')
        with zipfile.ZipFile(io.BytesIO(response.body)) as package:
            self.assertEqual(set(package.namelist()),{'Instalar.ps1','hollpacas_pos_agent.py','LEEME.txt','hollpacas-pos.json'})
            config=json.loads(package.read('hollpacas-pos.json'));self.assertEqual(config['station_id'],self.station.id);self.assertEqual(config['api_url'],'https://test.local')
            self.assertEqual(hashlib.sha256(config['token'].encode()).hexdigest(),self.station.token_hash)
        with self.assertRaises(HTTPException):routes.agent(self.request(token='test'),self.db,self.station.id)
        self.assertFalse(self.station.agent_ready)
