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
        self.clock+=timedelta(seconds=40)
        return sale

    def invoice(self):
        sale=self.due();asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));return sale,self.db.query(PosPrintJob).one()

    def test_checkbox_works_without_agent_and_only_after_forty_seconds(self):
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
        asyncio.run(native.tick(self.station.id,self.request({'busy':False}),self.db,self.user));self.clock+=timedelta(seconds=40)
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
