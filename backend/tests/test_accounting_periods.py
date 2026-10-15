import unittest
from datetime import date
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web as w
from app.routers import accounting_workspace as a
from app.models.sales import AccountingPeriodClosure as Closure, AccountingPeriodEvent as Event
from app.core.accounting_periods import close_period, reopen_period, reclose_period, period_dates, adapt_legacy_lock


class AccountingPeriodTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.db = Session(self.engine)
        self.today = date(2026,10,12)
        self.user = w.User(email='admin@test', roles=[w.Role(name='administrador')])
        self.request = Request({'type':'http','method':'POST','path':'/accounting/periods','headers':[],'query_string':b''})
        self.patches = [patch.object(w,'_enforce_permission'),patch.object(w,'local_today',return_value=self.today)]
        for p in self.patches:p.start()

    def tearDown(self):
        for p in self.patches:p.stop()
        self.db.close();self.engine.dispose()

    def test_calendar_month_year_and_leap_day(self):
        self.assertEqual(period_dates('MONTH','2024-02'),(date(2024,2,1),date(2024,2,29)))
        self.assertEqual(period_dates('YEAR','2025'),(date(2025,1,1),date(2025,12,31)))
        for kind,period in [('MONTH','2026-13'),('YEAR','oops'),('INVALID','2025')]:
            with self.assertRaises(ValueError):period_dates(kind,period)

    def test_month_blocks_only_its_dates_and_reopening_allows_corrections(self):
        row=close_period(self.db,'MONTH','2026-09','Cuadre mensual completado',self.user.email,self.today);self.db.commit()
        with self.assertRaises(w.HTTPException):w._assert_accounting_date_open(self.db,date(2026,9,30))
        w._assert_accounting_date_open(self.db,date(2026,8,31));w._assert_accounting_date_open(self.db,date(2026,10,1))
        reopen_period(self.db,row.id,'Corregir gasto duplicado',self.user.email);self.db.commit()
        w._assert_accounting_date_open(self.db,date(2026,9,30))
        self.assertEqual([e.action for e in self.db.query(Event).order_by(Event.id)],['CIERRE','REAPERTURA'])

    def test_annual_and_monthly_overlap_require_both_reopened(self):
        month=close_period(self.db,'MONTH','2025-09','Cuadre del mes revisado',self.user.email,self.today)
        year=close_period(self.db,'YEAR','2025','Revisión anual completada',self.user.email,self.today);self.db.commit()
        row,blockers=reopen_period(self.db,month.id,'Corrección del mes de septiembre',self.user.email);self.db.commit()
        self.assertIn(year.id,[r.id for r in blockers])
        with self.assertRaises(w.HTTPException):w._assert_accounting_date_open(self.db,date(2025,9,10))
        reopen_period(self.db,year.id,'Habilitar corrección anual',self.user.email);self.db.commit()
        w._assert_accounting_date_open(self.db,date(2025,9,10))

    def test_annual_reopening_keeps_independent_monthly_locks(self):
        month=close_period(self.db,'MONTH','2025-09','Cuadre mensual revisado',self.user.email,self.today)
        year=close_period(self.db,'YEAR','2025','Cuadre anual revisado',self.user.email,self.today);self.db.commit()
        reopen_period(self.db,year.id,'Revisión de otras fechas',self.user.email);self.db.commit()
        with self.assertRaises(w.HTTPException):w._assert_accounting_date_open(self.db,date(2025,9,10))
        w._assert_accounting_date_open(self.db,date(2025,10,10))

    def test_reclose_preserves_all_events_and_rejects_duplicate_close(self):
        row=close_period(self.db,'MONTH','2026-09','Cierre mensual inicial',self.user.email,self.today);self.db.commit()
        with self.assertRaises(ValueError):close_period(self.db,'MONTH','2026-09','Cierre duplicado',self.user.email,self.today)
        reopen_period(self.db,row.id,'Corregir registro duplicado',self.user.email);self.db.commit()
        reclose_period(self.db,row.id,'Correcciones revisadas y cuadradas',self.user.email,self.today);self.db.commit()
        self.assertTrue(row.closed);self.assertEqual(self.db.query(Closure).count(),1)
        self.assertEqual([e.action for e in self.db.query(Event).order_by(Event.id)],['CIERRE','REAPERTURA','CIERRE'])

    def test_cannot_close_unfinished_month_or_year(self):
        for kind,period in [('MONTH','2026-10'),('MONTH','2026-11'),('YEAR','2026')]:
            with self.assertRaises(ValueError):close_period(self.db,kind,period,'Revisión completada',self.user.email,self.today)
        self.assertEqual(self.db.query(Closure).count(),0)

    def test_close_rejects_drafts_and_reopen_requires_reason(self):
        self.db.add(w.AccountingVoucherType(id=1,code='DIARIO',nombre='Diario',prefijo='CD'))
        self.db.add(w.AccountingEntry(tipo_id=1,numero='DRAFT',fecha=date(2026,9,2),periodo='2026-09',estado='BORRADOR'));self.db.commit()
        with self.assertRaises(ValueError):close_period(self.db,'MONTH','2026-09','Revisión completada',self.user.email,self.today)
        self.assertEqual(self.db.query(Closure).count(),0)
        self.db.query(w.AccountingEntry).delete();self.db.commit()
        row=close_period(self.db,'MONTH','2026-09','Revisión completada',self.user.email,self.today);self.db.commit()
        with self.assertRaises(ValueError):reopen_period(self.db,row.id,'',self.user.email)
        self.assertTrue(row.closed)

    def test_non_admin_cannot_close_or_reopen(self):
        self.user.roles=[w.Role(name='contador')]
        for action in [lambda:a.period_close(self.request,'MONTH','2026-09','Cierre mensual',self.db,self.user),
                       lambda:a.period_reopen(self.request,1,'Corrección pendiente',self.db,self.user)]:
            with self.assertRaises(w.HTTPException) as exc:action()
            self.assertEqual(exc.exception.status_code,403)

    def test_legacy_cutoff_can_be_reopened_without_reactivating_at_startup(self):
        self.db.add(w.AccountingLockSetting(id=1,closed_through=date(2026,9,30),reason='Cierre histórico',updated_by='admin'));self.db.commit()
        row=adapt_legacy_lock(self.db);self.db.commit()
        with self.assertRaises(w.HTTPException):w._assert_accounting_date_open(self.db,date(2026,8,1))
        reopen_period(self.db,row.id,'Corregir historial registrado',self.user.email);self.db.commit()
        adapt_legacy_lock(self.db);self.db.commit()
        w._assert_accounting_date_open(self.db,date(2026,8,1))
        self.assertEqual(self.db.query(Closure).count(),1)
