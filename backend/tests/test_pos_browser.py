import asyncio,unittest
from datetime import timedelta
from types import SimpleNamespace
from starlette.templating import Jinja2Templates
from app.routers import pos_browser as native
from app.models.pos_automation import PosPrintJob,PosAutoTask
from app.models.sales import VentaFactura
from tests import test_pos_automation as fixture


class NativePosTests(unittest.TestCase):
    setUp=fixture.AutoPosTests.setUp
    tearDown=fixture.AutoPosTests.tearDown
    request=fixture.AutoPosTests.request
    sale=fixture.AutoPosTests.sale

    def activate(self):
        self.station.agent_ready=False;self.station.agent_seen=None;self.db.commit()
        result=asyncio.run(native.enable(self.station.id,self.request({'enabled':True}),self.db,self.user))
        self.assertTrue(result['enabled']);self.assertNotIn('agent_message',result)

    def due(self):
        self.activate();sale=self.sale()
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
        self.clock+=timedelta(seconds=15)
        return sale

    def invoice(self):
        sale=self.due();asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));return sale,self.db.query(PosPrintJob).one()

    def test_checkbox_works_without_agent_and_only_after_fifteen_seconds(self):
        sale=self.due();self.assertEqual(self.db.query(VentaFactura).count(),0)
        self.clock-=timedelta(seconds=1);asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),0)
        self.clock+=timedelta(seconds=1);asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).one().total_cs,925)
        self.assertEqual(self.db.query(PosPrintJob).one().status,'QUEUED')

    def test_print_claim_is_unique_and_ack_not_a_new_sale(self):
        sale,job=self.invoice();result=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        self.assertIn('/sales/automatico-pos/',result['url']);self.assertIsNone(native.claim(self.station.id,self.request(),self.db,self.user)['job'])
        asyncio.run(native.ack(self.station.id,job.id,self.request({'claim_id':result['claim_id'],'status':'SENT'}),self.db,self.user))
        self.assertEqual(job.status,'SENT');self.assertEqual(self.db.query(VentaFactura).count(),1)
        asyncio.run(native.retry(self.station.id,job.id,self.request({'confirmed':True}),self.db,self.user));replacement=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        self.assertNotEqual(result['claim_id'],replacement['claim_id']);self.assertEqual(self.db.query(VentaFactura).count(),1)

    def test_missing_print_confirmation_stops_new_invoices(self):
        sale,job=self.invoice();self.sale(2)
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.clock+=timedelta(seconds=15)
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),1)
        result=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        asyncio.run(native.ack(self.station.id,job.id,self.request({'claim_id':result['claim_id'],'status':'UNKNOWN'}),self.db,self.user))
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),1)

    def test_busy_cart_and_off_switch_do_not_bill(self):
        self.due();asyncio.run(native.tick(self.station.id,self.request({'busy':True}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),0)
        asyncio.run(native.enable(self.station.id,self.request({'enabled':False}),self.db,self.user));asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.assertEqual(self.db.query(VentaFactura).count(),0);self.assertEqual(self.db.query(PosAutoTask).one().status,'CANCELLED')

    def test_ticket_uses_browser_callback_and_existing_pos_layout(self):
        sale,job=self.invoice();claim=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        request=self.request();request.scope['query_string']=('claim='+claim['claim_id']).encode();request.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
        result=native.ticket(self.station.id,job.id,request,self.db,self.user)
        html=result.body.decode();self.assertIn("type:'holl-native-print'",html);self.assertIn('Ropa por libra',html);self.assertIn('window.print()',html);self.assertEqual(result.headers['cache-control'],'no-store')

    def test_old_agent_routes_stay_absent(self):
        from app.main import app
        self.assertFalse(any(route.path.startswith(('/api/pos-agent','/sales/auto-preventas')) for route in app.routes))
        self.assertTrue(any(route.path.startswith('/sales/automatico-pos') for route in app.routes))

    def test_completed_print_is_removed_from_live_queue_not_history(self):
        sale,job=self.invoice();claim=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        result=asyncio.run(native.ack(self.station.id,job.id,self.request({'claim_id':claim['claim_id'],'status':'SENT'}),self.db,self.user))
        self.assertEqual(result['tasks'],[]);self.assertIn(sale.id,result['completed_preventa_ids'])
        self.assertEqual(native.state(self.db,self.station)['tasks'],[])
        self.assertEqual(self.db.query(VentaFactura).count(),1);self.assertEqual(self.db.query(PosAutoTask).one().status,'FACTURADA');self.assertEqual(self.db.query(PosPrintJob).one().status,'SENT')

    def test_notification_checks_whether_preventa_is_still_pending(self):
        from app.routers import web
        sale=self.due();request=self.request();request.scope['query_string']=f'last_id={sale.id}&current_id={sale.id}'.encode()
        before=web.sales_preventas_notifications_poll(request,self.db,self.user)
        self.assertTrue(before['current_pending'])
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
        after=web.sales_preventas_notifications_poll(request,self.db,self.user)
        self.assertFalse(after['current_pending']);self.assertEqual(after['items'],[])

    def test_empty_print_queue_releases_transaction_lock(self):
        from unittest.mock import patch
        with patch.object(self.db,'commit',wraps=self.db.commit) as commit:
            result=native.claim(self.station.id,self.request(),self.db,self.user)
            self.assertIsNone(result['job']);self.assertEqual(commit.call_count,1)

    def test_already_dispatched_claim_releases_transaction_lock(self):
        from unittest.mock import patch
        self.invoice();native.claim(self.station.id,self.request(),self.db,self.user)
        with patch.object(self.db,'commit',wraps=self.db.commit) as commit:
            result=native.claim(self.station.id,self.request(),self.db,self.user)
            self.assertIsNone(result['job']);self.assertEqual(commit.call_count,1)

    def test_native_database_routes_use_worker_threads(self):
        import inspect
        from app.main import app
        routes=[route for route in app.routes if route.path.startswith('/sales/automatico-pos')]
        self.assertGreater(len(routes),5)
        self.assertTrue(all(not inspect.iscoroutinefunction(route.endpoint) for route in routes))

    def test_idle_poll_and_claim_do_not_take_financial_or_inventory_lock(self):
        from unittest.mock import patch
        with patch.object(native.auto,'lock_scope',side_effect=AssertionError('No debe bloquear la bodega')), patch.object(fixture.web,'_lock_accounting_transaction',side_effect=AssertionError('No debe bloquear contabilidad')):
            asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
            self.assertIsNone(native.claim(self.station.id,self.request(),self.db,self.user)['job'])

    def test_completed_history_state_uses_four_queries_without_product_details(self):
        from sqlalchemy import event
        sale,job=self.invoice();job.status='SENT';self.db.commit()
        queries=[]
        def record(conn,cursor,statement,parameters,context,executemany):queries.append(statement)
        self.db.refresh(self.station)
        event.listen(self.engine,'before_cursor_execute',record)
        try:result=native.state(self.db,self.station)
        finally:event.remove(self.engine,'before_cursor_execute',record)
        self.assertEqual(result['tasks'],[]);self.assertIn(sale.id,result['completed_preventa_ids'])
        self.assertEqual(len(queries),4,queries)
        self.assertFalse(any('ventas_factura_items' in q or 'productos.' in q for q in queries))

    def test_notification_database_wait_does_not_block_async_loop(self):
        import time,threading
        from unittest.mock import MagicMock,patch,AsyncMock
        db=MagicMock();query=MagicMock()
        db.query.return_value=query
        for method in ('join','outerjoin','filter','order_by','limit'):getattr(query,method).return_value=query
        threads=[]
        def slow_query():
            threads.append(threading.get_ident());time.sleep(.15);return []
        query.all.side_effect=slow_query
        request=self.request();request.is_disconnected=AsyncMock(return_value=False)
        async def verify():
            with patch.object(fixture.web,'_resolve_branch_bodega',return_value=(None,None)),patch.object(fixture.web,'_enforce_permission'):
                response=await fixture.web.sales_preventas_notifications_stream(request,db,self.user)
                iterator=response.body_iterator
                await iterator.__anext__()
                pending=asyncio.create_task(iterator.__anext__())
                started=time.monotonic();await asyncio.sleep(.03)
                self.assertLess(time.monotonic()-started,.12)
                self.assertFalse(pending.done())
                self.assertIn('event: ping',await pending)
                await iterator.aclose()
        asyncio.run(verify())
        self.assertTrue(threads);self.assertNotEqual(threads[0],threading.get_ident());db.close.assert_called()

    def test_multiple_preventas_are_invoiced_and_printed_fifo(self):
        self.activate()
        sales=[self.sale(i) for i in (1,2,3)]
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
        self.clock+=timedelta(seconds=15)
        for sale in sales:
            asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
            claim=native.claim(self.station.id,self.request(),self.db,self.user)['job']
            job=self.db.get(PosPrintJob,claim['id'])
            self.db.refresh(sale)
            self.assertEqual(job.invoice_id,sale.venta_factura_id)
            self.assertIsNone(native.claim(self.station.id,self.request(),self.db,self.user)['job'])
            asyncio.run(native.ack(self.station.id,job.id,self.request({'claim_id':claim['claim_id'],'status':'SENT'}),self.db,self.user))
        self.assertEqual(self.db.query(VentaFactura).count(),3)
        self.assertEqual(native.state(self.db,self.station)['tasks'],[])

    def test_unknown_oldest_job_cannot_be_overtaken(self):
        sale,job=self.invoice()
        job.status='UNKNOWN'
        self.db.add(PosPrintJob(station_id=self.station.id,invoice_id=999,copies=1,status='QUEUED'))
        self.db.commit()
        self.assertIsNone(native.claim(self.station.id,self.request(),self.db,self.user)['job'])

    def test_old_pending_print_is_visible_beyond_history_limit(self):
        sale,job=self.invoice()
        for i in range(20):
            self.db.add(PosAutoTask(station_id=self.station.id,preventa_id=100+i,operation_key='history'+str(i),due_at=self.clock,status='CANCELLED'))
        self.db.commit()
        self.assertEqual([row['job_id'] for row in native.state(self.db,self.station)['tasks']],[job.id])

    def test_console_keeps_station_and_does_not_require_sales_page(self):
        request=self.request()
        request.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
        html=native.console(self.station.id,request,self.db,self.user).body.decode()
        self.assertIn('data-worker="1"',html)
        self.assertIn('data-station="'+self.station.id+'"',html)
        self.assertIn('pos-browser.js?v=9',html)

    def test_dispatch_timeout_becomes_review_without_rebilling(self):
        sale,job=self.invoice();native.claim(self.station.id,self.request(),self.db,self.user)
        self.clock+=timedelta(seconds=41)
        result=asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user))
        self.assertEqual(job.status,'UNKNOWN');self.assertTrue(result['tasks'][0]['retry_allowed'])
        self.assertEqual(self.db.query(VentaFactura).count(),1)
        self.assertIsNone(native.claim(self.station.id,self.request(),self.db,self.user)['job'])

    def test_backup_uses_existing_invoice_and_all_three_copies(self):
        sale,job=self.invoice();job.status='UNKNOWN';self.db.commit()
        result=native.backup_claim(self.station.id,self.request(),{'confirmed':True,'job_ids':[job.id]},self.db,self.user)
        self.assertEqual(result['job_ids'],[job.id]);self.assertEqual(job.status,'DISPATCHED')
        request=self.request();request.scope['query_string']=('claim='+result['claim_id']).encode()
        request.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
        html=native.backup_ticket(self.station.id,request,self.db,self.user).body.decode()
        self.assertEqual(html.count('data-pos-copy-label="true"'),3)
        self.assertIn('warehouse-dispatch',html);self.assertIn('EXCLUSIVA DE BODEGA',html)
        self.assertIn('window.print()',html)
        result=native.backup_ack(self.station.id,self.request(),{'claim_id':result['claim_id'],'job_ids':[job.id],'status':'SENT'},self.db,self.user)
        self.assertEqual(job.status,'SENT');self.assertEqual(result['tasks'],[])
        self.assertEqual(self.db.query(VentaFactura).count(),1)
        self.assertEqual(sale.venta_factura_id,job.invoice_id)

    def test_backup_rejects_active_sent_foreign_and_unconfirmed_jobs(self):
        from fastapi import HTTPException
        sale,job=self.invoice();native.claim(self.station.id,self.request(),self.db,self.user)
        for status,payload in [('DISPATCHED',{'confirmed':True,'job_ids':[job.id]}),('SENT',{'confirmed':True,'job_ids':[job.id]}),('UNKNOWN',{'confirmed':False,'job_ids':[job.id]}),('UNKNOWN',{'confirmed':True,'job_ids':[999999]})]:
            job.status=status;self.db.commit()
            with self.assertRaises(HTTPException):native.backup_claim(self.station.id,self.request(),payload,self.db,self.user)
        self.assertEqual(self.db.query(VentaFactura).count(),1)

    def test_backup_old_ack_cannot_clear_new_print_attempt(self):
        from fastapi import HTTPException
        sale,job=self.invoice();job.status='UNKNOWN';self.db.commit()
        native.backup_claim(self.station.id,self.request(),{'confirmed':True,'job_ids':[job.id]},self.db,self.user)
        with self.assertRaises(HTTPException):native.backup_ack(self.station.id,self.request(),{'job_ids':[job.id],'claim_id':'old-claim','status':'SENT'},self.db,self.user)
        self.assertEqual(job.status,'DISPATCHED')

    def test_warehouse_backup_contains_no_financial_nodes_even_without_css(self):
        from app.core.pos_browser_receipts import receipt_copies
        sale,job=self.invoice();claim=native.claim(self.station.id,self.request(),self.db,self.user)['job']
        request=self.request();request.scope['query_string']=('claim='+claim['claim_id']).encode()
        request.scope['app']=SimpleNamespace(state=SimpleNamespace(templates=Jinja2Templates(directory='app/templates')))
        response=native.ticket(self.station.id,job.id,request,self.db,self.user)
        receipts,_=receipt_copies(response.body.decode(),('ORIGINAL','COPIA','EXCLUSIVA DE BODEGA'),3)
        for receipt in receipts[:2]:
            self.assertIn('Precio:',receipt)
            self.assertIn('Pagos aplicados',receipt)
        for forbidden in ('pos-money','pos-invoice-details','pos-commercial-footer','Precio:','Subtotal','Descuentos','Pagos aplicados','C$','Vuelto'):
            self.assertNotIn(forbidden,receipts[2])
        for expected in ('EXCLUSIVA DE BODEGA','REFERENCIA / FACTURA','VENDEDOR','TOTAL BULTOS','Ropa por libra'):
            self.assertIn(expected,receipts[2])
