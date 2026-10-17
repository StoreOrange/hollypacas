import unittest
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4
from sqlalchemy import func
from fastapi import HTTPException
from app.core import motorizado as service,motorizado_legacy as legacy
from app.models.motorizado import MotoTrip,MotoTripEvent
from app.models.user import User,Role
from app.models.sales import AccountingJournal,AccountingVoucherType,CuentaContable,AccountingEntry,ReciboCaja,CajaDiaria,CierreCaja,Vendedor,ReciboRubro,ReciboMotivo
from tests import test_pos_automation as fixture


class MotoTests(unittest.TestCase):
    setUpBase=fixture.AutoPosTests.setUp
    tearDown=fixture.AutoPosTests.tearDown

    def setUp(self):
        self.setUpBase()
        self.user.roles=[Role(name='motorizado')]
        self.seller=User(id=2,email='seller@example.invalid',full_name='Vendedora',hashed_password='unused',is_active=True,vendedor_id=1,default_branch_id=1,default_bodega_id=1,roles=[Role(name='vendedor')])
        self.other=User(id=3,email='other@example.invalid',full_name='Otro vendedor',hashed_password='unused',is_active=True,vendedor_id=2,default_branch_id=1,default_bodega_id=1,roles=self.seller.roles)
        self.db.add_all([self.seller,self.other,Vendedor(id=2,nombre='Otro vendedor')]);self.db.flush()
        voucher=self.db.query(AccountingVoucherType).filter_by(code='EGRESO').one();cash=self.db.query(CuentaContable).filter_by(codigo='1101').one()
        self.db.add(AccountingJournal(code='MOTO-QA',name='Caja',kind='CASH',branch_id=1,voucher_type_id=voucher.id,main_account_id=cash.id,active=True))
        bank=self.db.query(CuentaContable).filter_by(codigo='1102').one()
        self.db.add(AccountingJournal(code='BANCO',name='Banco',kind='BANK',branch_id=1,voucher_type_id=voucher.id,main_account_id=bank.id,currency='CS',active=True));self.db.commit()

    def payload(self,**extra):return dict(operation_key=uuid4().hex,seller_id='1',destination='Tipitapa',amount='1,250.50',comments='Paquete de prueba',**extra)
    def create(self,payload=None):
        trip=service.create(self.db,self.user,payload or self.payload());self.db.commit();return trip

    def test_trip_posts_transport_expense_to_bank_without_touching_sales_cash(self):
        trip=self.create();entry=trip.accounting_entry
        self.assertEqual(trip.rider_id,self.user.id);self.assertEqual(trip.amount_cs,Decimal('1250.50'))
        self.assertIsNone(trip.receipt_id);self.assertEqual(self.db.query(ReciboCaja).count(),0);self.assertEqual(self.db.query(CajaDiaria).count(),0)
        self.assertEqual(entry.estado,'POSTEADO');self.assertEqual(entry.total_debe,entry.total_haber)
        self.assertEqual(next(line for line in entry.lines if line.debe>0).cuenta.codigo,'6104')
        self.assertEqual(next(line for line in entry.lines if line.haber>0).cuenta.codigo,'1102')
        self.assertEqual(entry.journal.kind,'BANK');self.assertEqual(self.db.query(MotoTripEvent).one().action,'REGISTRADO')

    def test_duplicate_submission_does_not_duplicate_cash_or_accounting(self):
        payload=self.payload();first=self.create(payload);second=self.create(payload)
        self.assertEqual(first.id,second.id);self.assertEqual(self.db.query(ReciboCaja).count(),0);self.assertEqual(self.db.query(AccountingEntry).count(),1)

    def test_only_assigned_rider_marks_delivery_only_assigned_seller_receives(self):
        trip=self.create()
        with self.assertRaises(HTTPException):service.transition(self.db,self.seller,trip.id,'ENTREGADO')
        with self.assertRaises(HTTPException):service.transition(self.db,self.user,trip.id,'RECIBIDO')
        with self.assertRaises(ValueError):service.transition(self.db,self.seller,trip.id,'RECIBIDO')
        service.transition(self.db,self.user,trip.id,'ENTREGADO');self.db.commit()
        service.transition(self.db,self.user,trip.id,'ENTREGADO');self.db.commit()
        with self.assertRaises(HTTPException):service.transition(self.db,self.other,trip.id,'RECIBIDO')
        service.transition(self.db,self.seller,trip.id,'RECIBIDO');self.db.commit()
        self.assertEqual(trip.status,'RECIBIDO');self.assertEqual(self.db.query(MotoTripEvent).count(),3);self.assertEqual(self.db.query(ReciboCaja).count(),0)

    def test_only_seller_voids_and_cash_refund_reverses_once(self):
        trip=self.create()
        with self.assertRaises(HTTPException):service.transition(self.db,self.user,trip.id,'ANULADO','No se realizó',True)
        with self.assertRaises(ValueError):service.transition(self.db,self.seller,trip.id,'ANULADO','No se realizó',False)
        service.transition(self.db,self.seller,trip.id,'ANULADO','No se realizó',True);self.db.commit()
        self.assertEqual(trip.status,'ANULADO');self.assertIsNotNone(trip.reversal_entry_id);self.assertEqual(self.db.query(CajaDiaria).count(),0)
        self.assertEqual(trip.accounting_entry.estado,'REVERSADO');self.assertEqual(trip.reversal_entry.reversa_de_id,trip.accounting_entry_id)
        self.assertEqual(self.db.query(AccountingEntryLine).with_entities(func.sum(AccountingEntryLine.debe)).scalar(),Decimal('2501.00'))
        service.transition(self.db,self.seller,trip.id,'ANULADO','No se realizó',True);self.db.commit()
        self.assertEqual(self.db.query(ReciboCaja).count(),0);self.assertEqual(self.db.query(MotoTripEvent).count(),2)
        self.assertEqual(service.scope(self.db,self.user,self.db.query(MotoTrip)).one().status,'ANULADO')

    def test_delivered_trip_cannot_be_canceled(self):
        trip=self.create();service.transition(self.db,self.user,trip.id,'ENTREGADO');self.db.commit()
        with self.assertRaises(ValueError):service.transition(self.db,self.seller,trip.id,'ANULADO','No se realizó',True)
        self.db.rollback();self.assertEqual(self.db.query(ReciboCaja).count(),0)

    def test_seller_isolation_and_admin_cannot_override_seller_actions(self):
        trip=self.create();self.assertEqual(service.scope(self.db,self.other,self.db.query(MotoTrip)).count(),0)
        self.assertEqual(service.scope(self.db,self.seller,self.db.query(MotoTrip)).count(),1)
        with self.assertRaises(HTTPException):service.allowed_trip(self.db,self.other,trip)
        admin=User(id=9,default_branch_id=1,default_bodega_id=1,roles=[Role(name='administrador')])
        with self.assertRaises(HTTPException):service.transition(self.db,admin,trip.id,'ANULADO','No se realizó',True)

    def test_invalid_amounts_and_destination_never_post(self):
        for value in ('0','-2','NaN','Infinity','5.555','1,2','abc'):
            payload=self.payload();payload['amount']=value
            with self.assertRaises((ValueError,ArithmeticError)):service.create(self.db,self.user,payload)
            self.db.rollback()
        payload=self.payload();payload['destination']='OTRO';payload['other_destination']=''
        with self.assertRaises(ValueError):service.create(self.db,self.user,payload)
        self.db.rollback();self.assertEqual(self.db.query(ReciboCaja).count(),0)
        payload['other_destination']='Nagarote, parque central';trip=self.create(payload);self.assertEqual(trip.destination,payload['other_destination'])

    def test_failure_rolls_back_receipt_trip_cash_and_entry(self):
        with patch.object(service,'posted_entry',side_effect=ValueError('Configuración incompleta')):
            with self.assertRaises(ValueError):service.create(self.db,self.user,self.payload())
            self.db.rollback()
        self.assertEqual(self.db.query(ReciboCaja).count(),0);self.assertEqual(self.db.query(MotoTrip).count(),0);self.assertEqual(self.db.query(AccountingEntry).count(),0);self.assertEqual(self.db.query(CajaDiaria).count(),0)

    def test_missing_transport_account_prevents_posting(self):
        self.db.query(CuentaContable).filter_by(codigo='6104').update({'activo':False});self.db.commit()
        with self.assertRaises(ValueError):service.create(self.db,self.user,self.payload())
        self.db.rollback();self.assertEqual(self.db.query(ReciboCaja).count(),0)

    def test_closed_sales_cash_never_blocks_or_changes_trip_but_accounting_closure_is_respected(self):
        from app.models.sales import AccountingPeriodClosure
        from datetime import timedelta
        for day in [self.clock.date(),self.clock.date()-timedelta(days=1)]:
            self.db.add(CierreCaja(branch_id=1,bodega_id=1,fecha=day,total_calculado_usd=300,total_efectivo_cs=1000))
            self.db.add(CajaDiaria(branch_id=1,bodega_id=1,fecha=day,saldo_cs=1000,saldo_usd=300))
        self.db.commit()
        trip=self.create();service.transition(self.db,self.seller,trip.id,'ANULADO','Viaje cancelado',True);self.db.commit()
        self.assertEqual(self.db.query(ReciboCaja).count(),0)
        for box in self.db.query(CajaDiaria).all():self.assertEqual(box.saldo_cs,1000);self.assertEqual(box.saldo_usd,300)
        for closing in self.db.query(CierreCaja).all():self.assertEqual(closing.total_calculado_usd,300);self.assertEqual(closing.total_efectivo_cs,1000)
        self.db.add(AccountingPeriodClosure(kind='MONTH',period='2026-10',start_date=self.clock.date(),end_date=self.clock.date(),closed=True,reason='Cierre QA',updated_by='QA'));self.db.commit()
        with self.assertRaises(HTTPException):service.create(self.db,self.user,self.payload())
        self.db.rollback();self.assertEqual(self.db.query(MotoTrip).count(),1)

    def test_routes_reject_other_companies_and_unsupported_roles(self):
        from app.routers import motorizado as router
        request=fixture.AutoPosTests.request(self)
        with patch.object(fixture.web,'_is_pacasholl_company',return_value=False):
            with self.assertRaises(HTTPException) as error:router.access(request,self.db)
            self.assertEqual(error.exception.status_code,404)
        outsider=User(roles=[Role(name='cajero')])
        with patch.object(fixture.web,'_require_user_web',return_value=outsider):
            with self.assertRaises(HTTPException) as error:router.access(request,self.db)
            self.assertEqual(error.exception.status_code,403)

    def test_forms_reject_missing_or_wrong_csrf(self):
        from app.routers import motorizado as router
        request=fixture.AutoPosTests.request(self)
        with self.assertRaises(HTTPException):router.verify(request,self.user,{})
        router.verify(request,self.user,{'csrf':router.csrf(request,self.user)})

    def test_reports_cannot_show_other_sellers_and_keep_cancellations_visible(self):
        from app.routers import motorizado as router
        from app.main import app
        trip=self.create()
        request=fixture.AutoPosTests.request(self);request.scope.update(path='/motorizado',query_string=b'view=costos',app=app,router=app.router)
        request.state.branding={'company_key':'hollywood_pacas'}
        result=router.home(request,self.db,self.seller)
        self.assertEqual(result.context['summary']['count'],1);self.assertEqual(result.context['summary']['total'],Decimal('1250.50'))
        result=router.home(request,self.db,self.other);self.assertEqual(result.context['summary']['count'],0)
        request.scope['query_string']=b'view=costos&seller=1'
        result=router.home(request,self.db,self.other);self.assertEqual(result.context['summary']['count'],0)
        service.transition(self.db,self.seller,trip.id,'ANULADO','Entrega cancelada',True);self.db.commit()
        request=request.__class__({**request.scope,'query_string':b'view=costos&history=1'})
        result=router.home(request,self.db,self.seller);self.assertEqual(result.context['summary']['count'],1);self.assertEqual(result.context['summary']['total'],0);self.assertEqual(result.context['summary']['canceled'],Decimal('1250.50'))
        self.assertEqual(result.context['trips'][0].status,'ANULADO')
        from datetime import timedelta
        trip.trip_date=self.clock.date()-timedelta(days=5);self.db.commit()
        request=request.__class__({**request.scope,'query_string':b'view=costos&history=1'})
        result=router.home(request,self.db,self.seller)
        self.assertEqual(result.context['summary']['count'],1);self.assertTrue(result.context['history']);self.assertEqual(result.context['start'],trip.trip_date)
        result=router.home(request,self.db,self.other);self.assertEqual(result.context['summary']['count'],0)


    def test_history_groups_all_filtered_trips_by_rider_and_seller(self):
        from app.routers import motorizado as router
        from app.main import app
        self.create();canceled=self.create();self.create(self.payload() | {'seller_id':'2','amount':'75.25'})
        service.transition(self.db,self.seller,canceled.id,'ANULADO','Entrega cancelada',True);self.db.commit()
        request=fixture.AutoPosTests.request(self)
        request.scope.update(path='/motorizado',query_string=b'history=1',app=app,router=app.router)
        request.state.branding={'company_key':'hollywood_pacas'}
        result=router.home(request,self.db,self.user)
        groups={row.seller_id:row for row in result.context['history_groups']}
        self.assertEqual(len(groups),2)
        self.assertEqual(groups[1].count,2)
        self.assertEqual(groups[1].registered_total,Decimal('2501.00'))
        self.assertEqual(groups[1].canceled,Decimal('1250.50'))
        self.assertEqual(groups[2].registered_total,Decimal('75.25'))
        self.assertEqual(result.context['summary']['registered_total'],Decimal('2576.25'))
        self.assertIn('Historial agrupado',result.body.decode())
        self.assertIn('TOTAL GENERAL',result.body.decode())
        self.assertIn('Toca para ver sus viajes',result.body.decode())
        self.assertIn('#moto-trip-details',result.body.decode())
        request=request.__class__({**request.scope,'query_string':b'history=1&seller=1'})
        groups=router.home(request,self.db,self.user).context['history_groups']
        self.assertEqual(len(groups),1);self.assertEqual(groups[0].count,2)
        request=request.__class__({**request.scope,'query_string':b'view=costos&history=1&seller=1'})
        self.assertEqual(router.home(request,self.db,self.other).context['history_groups'],[])

    def test_role_seed_links_moto_menu_without_granting_sales_or_accounting(self):
        from app.core import init_db
        with patch.object(init_db,'get_active_company_key',return_value='hollywood_pacas'):
            init_db._seed_roles(self.db);init_db._seed_role_permissions(self.db)
        role=self.db.query(Role).filter_by(name='motorizado').one()
        self.assertEqual({p.name for p in role.permissions},{'menu.motorizado'})
        self.assertIn('menu.motorizado',{p.name for p in self.seller.roles[0].permissions})

    def test_cancel_uses_original_accounts_even_if_catalog_was_renamed(self):
        trip=self.create();trip.accounting_entry.lines[0].cuenta.nombre='Transporte renombrado'
        self.db.query(CuentaContable).filter_by(codigo='6104').update({'activo':False});self.db.commit()
        service.transition(self.db,self.seller,trip.id,'ANULADO','Entrega cancelada',True);self.db.commit()
        self.assertEqual(trip.status,'ANULADO');self.assertEqual(self.db.query(CajaDiaria).count(),0)

    def test_bank_must_not_point_to_sales_cash(self):
        bank=self.db.query(AccountingJournal).filter_by(code='BANCO').one()
        bank.main_account_id=self.db.query(CuentaContable).filter_by(codigo='1101').one().id;self.db.commit()
        with self.assertRaises(ValueError):self.create()
        self.db.rollback();self.assertEqual(self.db.query(AccountingEntry).count(),0)

    def test_legacy_cash_payment_is_reclassified_once_with_original_audit_kept(self):
        from app.core.accounting_cash_receipts import build_receipt_entry
        rubro=ReciboRubro(nombre='Legacy Moto',tipo='EGRESO',cuenta_id=self.db.query(CuentaContable).filter_by(codigo='6104').one().id,activo=True)
        motive=ReciboMotivo(nombre='Legacy viaje',tipo='EGRESO',activo=True);self.db.add_all([rubro,motive]);self.db.flush()
        receipt=ReciboCaja(numero='ROC-M-LEGACY',secuencia=1,branch_id=1,bodega_id=1,tipo='EGRESO',rubro_id=rubro.id,motivo_id=motive.id,fecha=self.clock.date(),moneda='CS',monto_cs=100,monto_usd=0,afecta_caja=True)
        self.db.add(receipt);self.db.flush();receipt.accounting_entry_id=build_receipt_entry(self.db,receipt,rubro,None,'QA').id
        self.db.add(CajaDiaria(branch_id=1,bodega_id=1,fecha=self.clock.date(),saldo_cs=900,saldo_usd=0))
        trip=MotoTrip(operation_key=uuid4().hex,rider_id=self.user.id,seller_id=1,branch_id=1,bodega_id=1,trip_date=self.clock.date(),destination='Tipitapa',amount_cs=100,comments='',status='PENDIENTE',receipt_id=receipt.id,created_at=self.clock)
        self.db.add(trip);self.db.commit()
        legacy.isolate_legacy(self.db,trip,self.user);self.db.commit()
        self.assertFalse(receipt.afecta_caja);self.assertEqual(self.db.query(CajaDiaria).one().saldo_cs,1000)
        self.assertEqual(trip.accounting_entry.lines[1].cuenta.codigo,'1102')
        self.assertEqual(receipt.accounting_entry.estado,'REVERSADO')
        entries=self.db.query(AccountingEntry).count();legacy.isolate_legacy(self.db,trip,self.user);self.db.commit();self.assertEqual(self.db.query(AccountingEntry).count(),entries)

from app.models.sales import AccountingEntryLine
