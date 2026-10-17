import unittest
from datetime import timedelta
from uuid import uuid4
from fastapi import HTTPException
from app.core import motorizado as service
from app.models.motorizado import MotoTripClosure,MotoTripEvent
from app.models.sales import AccountingEntry,ReciboCaja,CajaDiaria,CierreCaja,AccountingPeriodClosure
from app.main import app
from app.routers import motorizado as router
from tests import test_motorizado as fixture,test_pos_automation as common


class MotoClosureTests(unittest.TestCase):
    def setUp(self):
        self.case=fixture.MotoTests();self.case.setUp();self.addCleanup(self.case.tearDown)
    def confirmed(self):
        trip=self.case.create();service.transition(self.case.db,self.case.user,trip.id,'ENTREGADO');service.transition(self.case.db,self.case.seller,trip.id,'RECIBIDO');self.case.db.commit();return trip
    def request(self,query):
        request=common.AutoPosTests.request(self.case);request.scope.update(path='/motorizado',query_string=query.encode(),app=app,router=app.router);request.state.branding={'company_key':'hollywood_pacas'};return request

    def test_payment_closes_batch_without_second_expense_or_cash_changes(self):
        trips=[self.confirmed(),self.confirmed()];before=self.case.db.query(AccountingEntry).count()
        closure=service.close_trips(self.case.db,self.case.seller,[t.id for t in trips],uuid4().hex,True,'Pago confirmado');self.case.db.commit()
        self.assertEqual(closure.trip_count,2);self.assertEqual(closure.amount_cs,2501)
        self.assertTrue(all(t.closure_id==closure.id for t in trips))
        self.assertEqual(self.case.db.query(AccountingEntry).count(),before);self.assertEqual(self.case.db.query(ReciboCaja).count(),0);self.assertEqual(self.case.db.query(CajaDiaria).count(),0)
        self.assertEqual(self.case.db.query(MotoTripEvent).filter_by(action='CERRADO').count(),2)

    def test_idempotency_and_closed_trips_cannot_be_paid_twice(self):
        trip=self.confirmed();token=uuid4().hex
        first=service.close_trips(self.case.db,self.case.seller,[trip.id],token,True);self.case.db.commit()
        second=service.close_trips(self.case.db,self.case.seller,[trip.id],token,True);self.case.db.commit();self.assertEqual(first.id,second.id)
        with self.assertRaises(ValueError):service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,True)
        self.case.db.rollback();self.assertEqual(self.case.db.query(MotoTripClosure).count(),1)
        with self.assertRaises(ValueError):service.transition(self.case.db,self.case.seller,trip.id,'ANULADO','No se hizo',True)

    def test_only_assigned_seller_can_close_and_payment_must_be_confirmed(self):
        trip=self.confirmed()
        for user in [self.case.user,self.case.other]:
            with self.assertRaises(HTTPException):service.close_trips(self.case.db,user,[trip.id],uuid4().hex,True)
        with self.assertRaises(ValueError):service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,False)
        self.assertEqual(self.case.db.query(MotoTripClosure).count(),0)

    def test_pending_and_unconfirmed_delivery_cannot_be_closed(self):
        trip=self.case.create()
        with self.assertRaises(ValueError):service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,True)
        service.transition(self.case.db,self.case.user,trip.id,'ENTREGADO');self.case.db.commit()
        with self.assertRaises(ValueError):service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,True)

    def test_closed_trip_disappears_from_active_view_stays_in_history_and_reports(self):
        trip=self.confirmed();trip.trip_date=self.case.clock.date()-timedelta(days=3);self.case.db.commit()
        before=router.home(self.request(''),self.case.db,self.case.user);self.assertEqual(before.context['summary']['count'],1)
        closure=service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,True);self.case.db.commit()
        active=router.home(self.request(''),self.case.db,self.case.user);self.assertEqual(active.context['summary']['count'],0)
        history=router.home(self.request('history=1'),self.case.db,self.case.user);self.assertEqual(history.context['summary']['count'],1)
        report=router.home(self.request('view=cierres'),self.case.db,self.case.user);self.assertEqual(report.context['summary']['count'],1);self.assertEqual(report.context['rows'][0].id,closure.id)
        other=router.home(self.request('view=cierres&seller=1'),self.case.db,self.case.other);self.assertEqual(other.context['summary']['count'],0)
        self.case.clock+=timedelta(days=2)
        today=router.home(self.request('view=cierres'),self.case.db,self.case.user);self.assertEqual(today.context['summary']['count'],0)
        date=closure.closure_date.isoformat();past=router.home(self.request(f'view=cierres&from={date}&to={date}'),self.case.db,self.case.user);self.assertEqual(past.context['summary']['total'],trip.amount_cs)

    def test_payment_closure_does_not_change_sales_or_accounting_closures(self):
        trip=self.confirmed()
        self.case.db.add(CierreCaja(branch_id=1,bodega_id=1,fecha=self.case.clock.date(),total_efectivo_cs=700))
        self.case.db.add(AccountingPeriodClosure(kind='MONTH',period='2026-10',start_date=self.case.clock.date(),end_date=self.case.clock.date(),closed=True,reason='QA',updated_by='QA'));self.case.db.commit()
        service.close_trips(self.case.db,self.case.seller,[trip.id],uuid4().hex,True);self.case.db.commit()
        self.assertEqual(self.case.db.query(CierreCaja).one().total_efectivo_cs,700);self.assertTrue(self.case.db.query(AccountingPeriodClosure).one().closed)
