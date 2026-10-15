"""Explicit month/year locks; reopening keeps the audit trail intact."""
import calendar
from .utils import local_now_naive
from datetime import date
from ..models.sales import AccountingPeriodClosure as Closure, AccountingPeriodEvent as Event, AccountingEntry, AccountingLockSetting


def period_dates(kind, period):
    try:
        if kind == 'MONTH' and len(period) == 7:
            start = date.fromisoformat(period + '-01')
            return start, date(start.year, start.month, calendar.monthrange(start.year, start.month)[1])
        if kind == 'YEAR' and len(period) == 4 and period.isdigit():
            year = int(period)
            return date(year, 1, 1), date(year, 12, 31)
    except ValueError:
        pass
    raise ValueError('Selecciona un mes o año válido.')


def adapt_legacy_lock(db):
    lock = db.get(AccountingLockSetting, 1)
    existing = db.query(Closure).filter_by(kind='LEGACY', period='HISTORICO').first()
    if not lock or existing:
        return existing
    row = Closure(kind='LEGACY', period='HISTORICO', start_date=date.min, end_date=lock.closed_through,
                  closed=True, reason=lock.reason, updated_by=lock.updated_by)
    db.add(row); db.flush()
    db.add(Event(closure_id=row.id, action='IMPORTADO', created_at=local_now_naive(), reason='Adaptación del cierre por fecha existente: ' + lock.reason[:180], actor=lock.updated_by))
    return row


def close_period(db, kind, period, reason, actor, today):
    start, end = period_dates(kind, period)
    reason = reason.strip()
    if len(reason) < 5:
        raise ValueError('Detalla el motivo del cierre (al menos 5 caracteres).')
    if end >= today:
        raise ValueError('Solo puedes cerrar meses o años que ya terminaron.')
    if db.query(AccountingEntry.id).filter(AccountingEntry.estado == 'BORRADOR', AccountingEntry.fecha >= start, AccountingEntry.fecha <= end).first():
        raise ValueError('Hay borradores en el período. Complétalos y publícalos, o cancélalos antes de cerrar.')
    row = db.query(Closure).filter_by(kind=kind, period=period).with_for_update().first()
    if row and row.closed:
        raise ValueError('Este período ya está cerrado.')
    if not row:
        row = Closure(kind=kind, period=period, start_date=start, end_date=end)
        db.add(row)
    row.closed, row.reason, row.updated_by = True, reason[:260], actor
    db.flush()
    db.add(Event(closure_id=row.id, action='CIERRE', created_at=local_now_naive(), reason=reason[:260], actor=actor))
    return row


def reopen_period(db, closure_id, reason, actor):
    reason = reason.strip()
    if len(reason) < 5:
        raise ValueError('Detalla el motivo de reapertura (al menos 5 caracteres).')
    row = db.query(Closure).filter_by(id=closure_id).with_for_update().first()
    if not row or not row.closed:
        raise ValueError('El período no existe o ya está abierto.')
    row.closed, row.reason, row.updated_by = False, reason[:260], actor
    db.add(Event(closure_id=row.id, action='REAPERTURA', created_at=local_now_naive(), reason=reason[:260], actor=actor))
    db.flush()
    blockers = db.query(Closure).filter(Closure.closed.is_(True), Closure.start_date <= row.end_date, Closure.end_date >= row.start_date).order_by(Closure.start_date).all()
    return row, blockers


def reclose_period(db, closure_id, reason, actor, today):
    row = db.query(Closure).filter_by(id=closure_id).with_for_update().first()
    if not row:
        raise ValueError('Período no encontrado.')
    if row.kind != 'LEGACY':
        return close_period(db, row.kind, row.period, reason, actor, today)
    if row.closed or len(reason.strip()) < 5:
        raise ValueError('El cierre ya está vigente o falta detallar el motivo.')
    if db.query(AccountingEntry.id).filter(AccountingEntry.estado == 'BORRADOR', AccountingEntry.fecha >= row.start_date, AccountingEntry.fecha <= row.end_date).first():
        raise ValueError('Resuelve los borradores de ese período antes de cerrarlo.')
    row.closed, row.reason, row.updated_by = True, reason.strip()[:260], actor
    db.add(Event(closure_id=row.id, action='CIERRE', created_at=local_now_naive(), reason=reason.strip()[:260], actor=actor))
    return row
