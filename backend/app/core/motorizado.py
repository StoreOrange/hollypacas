"""Gasto de transporte independiente de las cajas de ventas y sus cierres."""
import re
from types import SimpleNamespace
from fastapi import HTTPException
from sqlalchemy import or_
from ..models.motorizado import MotoTrip,MotoTripEvent,MotoTripClosure
from ..models.inventory import Bodega
from ..models.sales import (Vendedor,VendedorBodega,CuentaContable,AccountingEntryLine,AccountingBankMatch,AccountingReconciliation)
from .accounting_cash_receipts import receipt_amounts
from .accounting_payroll import posted_entry

DESTINATIONS=('Terminal Mercado Mayoreo (Rigoberto Cabezas)','Terminal Mercado Roberto Huembes','Terminal Mercado Israel Lewites','Terminal de microbuses UCA','Tipitapa','Masaya','Ciudad Sandino','Mateare','Ticuantepe','Nindirí','El Crucero','Granada')


def roles(user):
    from .motorizado_access import restricted
    return {'motorizado'} if restricted(user) else {r.name.lower() for r in user.roles}
def seller_can(user,trip):return 'vendedor' in roles(user) and user.vendedor_id==trip.seller_id

def allowed_trip(db,user,trip):
    from ..routers import web
    if not trip or trip.branch_id not in web._user_scoped_branch_ids(db,user):raise HTTPException(404,'Viaje no encontrado')
    names=roles(user)
    if 'administrador' not in names and not ('motorizado' in names and trip.rider_id==user.id) and not seller_can(user,trip):raise HTTPException(403,'No tienes permiso sobre este viaje')
    return trip


def scope(db,user,query):
    from ..routers import web
    query=query.filter(MotoTrip.branch_id.in_(web._user_scoped_branch_ids(db,user)))
    if 'administrador' not in roles(user):
        conditions=[]
        if 'motorizado' in roles(user):conditions.append(MotoTrip.rider_id==user.id)
        if 'vendedor' in roles(user) and user.vendedor_id:conditions.append(MotoTrip.seller_id==user.vendedor_id)
        query=query.filter(or_(*conditions) if conditions else MotoTrip.id<0)
    return query


def seed_bank(db):
    from ..models.sales import AccountingJournal
    bank=db.query(CuentaContable).filter_by(codigo='1102',activo=True,tipo='BALANCE',naturaleza='DEBE').first()
    journal=db.query(AccountingJournal).filter_by(code='BANCO',kind='BANK',branch_id=None,active=True).first()
    if bank and 'banc' in bank.nombre.lower() and journal and not journal.main_account_id:
        journal.main_account_id=bank.id;db.flush()


def bank_journals(db,branch_id):
    from ..models.sales import AccountingJournal
    rows=db.query(AccountingJournal).filter_by(kind='BANK',active=True,currency='CS').filter(or_(AccountingJournal.branch_id==branch_id,AccountingJournal.branch_id.is_(None))).order_by(AccountingJournal.name).all()
    return [row for row in rows if row.branch_id==branch_id] or [row for row in rows if row.branch_id is None]


def bank_account(db,branch_id,journal_id=None):
    choices=bank_journals(db,branch_id)
    journal=next((row for row in choices if str(row.id)==str(journal_id)),None) if journal_id else choices[0] if len(choices)==1 else None
    if not journal:raise ValueError('Selecciona un diario bancario en C$ configurado para la sucursal')
    account=journal.main_account
    current=account;seen=set();is_bank=False
    while current and current.id not in seen:
        seen.add(current.id)
        if current.codigo=='1102':is_bank=True;break
        current=current.parent
    if not account or not account.activo or account.tipo!='BALANCE' or account.naturaleza!='DEBE' or not is_bank:raise ValueError('Configura una cuenta de Bancos (1102 o sus subcuentas) en el diario bancario. No se permite una cuenta de caja de ventas')
    return journal,account


def transport_account(db):
    expense=db.query(CuentaContable).filter_by(codigo='6104',activo=True,tipo='RESULTADO',naturaleza='DEBE').first()
    if not expense:raise ValueError('Configura la cuenta 6104 Transporte y Fletes en Contabilidad')
    return expense


def expense_entry(db,branch_id,day,value,actor,reference,description,original=None,journal_id=None):
    from ..routers import web
    if original:
        if original.estado!='POSTEADO':raise ValueError('El comprobante original no está disponible para revertir')
        ids=[line.id for line in original.lines]
        if db.query(AccountingBankMatch.id).filter(AccountingBankMatch.canceled_at.is_(None),AccountingBankMatch.line_id.in_(ids)).first() or db.query(AccountingReconciliation.id).filter(AccountingReconciliation.canceled_at.is_(None),or_(AccountingReconciliation.debit_line_id.in_(ids),AccountingReconciliation.credit_line_id.in_(ids))).first():raise ValueError('Primero deshaz la conciliación del comprobante original')
        lines=[AccountingEntryLine(cuenta=line.cuenta,debe=line.haber,haber=line.debe,descripcion=description[:200]) for line in original.lines]
        journal=original.journal
    else:
        expense=transport_account(db);journal,bank=bank_account(db,branch_id,journal_id)
        lines=[AccountingEntryLine(cuenta=expense,debe=value,haber=0,descripcion=description[:200]),AccountingEntryLine(cuenta=bank,debe=0,haber=value,descripcion='Gasto de transporte con contrapartida Banco')]
    entry=posted_entry(db,SimpleNamespace(branch_id=branch_id),day,reference,description,lines,actor,'DIARIO',journal,original)
    if original:
        original.estado='REVERSADO';original.anulado_por=actor;original.anulado_motivo=description[:260];original.anulado_at=web.local_now_naive()
    return entry


def create(db,user,payload):
    from ..routers import web
    if 'motorizado' not in roles(user):raise HTTPException(403,'Solo el rol motorizado puede registrar sus viajes')
    token=str(payload.get('operation_key') or '')
    if not re.fullmatch('[a-f0-9]{32}',token):raise ValueError('Recarga el formulario y vuelve a intentar')
    branch,warehouse=web._resolve_branch_bodega(db,user)
    if not branch or not warehouse:raise ValueError('Asigna sucursal y bodega al usuario motorizado')
    day=web.local_today();web._assert_accounting_date_open(db,day)
    previous=db.query(MotoTrip).filter_by(operation_key=token).first()
    if previous:
        if previous.rider_id!=user.id:raise HTTPException(403,'Operación de otro usuario')
        return previous
    seller=db.get(Vendedor,int(payload.get('seller_id') or 0))
    if not seller or not seller.activo:raise ValueError('Selecciona un vendedor activo')
    assignments=db.query(Bodega.branch_id).join(VendedorBodega,VendedorBodega.bodega_id==Bodega.id).filter(VendedorBodega.vendedor_id==seller.id).all()
    if assignments and branch.id not in {r[0] for r in assignments}:raise ValueError('El vendedor no pertenece a esta sucursal')
    destination=str(payload.get('destination') or '').strip()
    if destination=='OTRO':destination=str(payload.get('other_destination') or '').strip()
    elif destination not in DESTINATIONS:raise ValueError('Selecciona un destino de la lista u Otro destino')
    if not 3<=len(destination)<=160:raise ValueError('Describe el destino, entre 3 y 160 caracteres')
    comments=str(payload.get('comments') or '').strip()
    if len(comments)>500:raise ValueError('El comentario admite hasta 500 caracteres')
    _,value=receipt_amounts(payload.get('amount'),'CS',0)
    entry=expense_entry(db,branch.id,day,value,user.full_name or user.email,f'AUTO-MOTO-{token}',f'Viaje · {seller.nombre} · {destination}',journal_id=payload.get('journal_id'))
    trip=MotoTrip(operation_key=token,rider_id=user.id,seller_id=seller.id,branch_id=branch.id,bodega_id=warehouse.id,trip_date=day,destination=destination,amount_cs=value,comments=comments,status='PENDIENTE',accounting_entry_id=entry.id,created_at=web.local_now_naive())
    db.add(trip);db.flush();db.add(MotoTripEvent(trip_id=trip.id,actor_id=user.id,action='REGISTRADO',comment=comments,created_at=web.local_now_naive()))
    return trip


def transition(db,user,trip_id,action,reason='',refund=False):
    from ..routers import web
    trip=allowed_trip(db,user,db.get(MotoTrip,trip_id))
    if action=='ENTREGADO':
        if 'motorizado' not in roles(user) or user.id!=trip.rider_id:raise HTTPException(403,'Solo el motorizado de este viaje puede marcar la entrega')
    elif action in {'RECIBIDO','ANULADO'}:
        if not seller_can(user,trip):raise HTTPException(403,'Solo el vendedor asignado puede confirmar o anular este viaje')
    else:raise ValueError('Acción no válida')
    if action=='ANULADO':
        if not 5<=len(reason.strip())<=500:raise ValueError('Escribe un motivo de anulación de 5 a 500 caracteres')
        if not refund:raise ValueError('Confirma la devolución o cancelación del pago bancario antes de anular')
        web._assert_accounting_date_open(db,web.local_today())
    trip=db.query(MotoTrip).filter_by(id=trip.id).populate_existing().with_for_update().one()
    if trip.closure_id:raise ValueError('El viaje ya está pagado y cerrado. Puedes consultarlo en el historial')
    if trip.status==action:return trip
    expected={'ENTREGADO':'PENDIENTE','RECIBIDO':'ENTREGADO','ANULADO':'PENDIENTE'}
    if trip.status!=expected[action]:raise ValueError('El viaje cambió de estado: solo se anulan viajes pendientes y solo se confirman viajes entregados')
    if action=='ANULADO' and not trip.accounting_entry_id:raise ValueError('Reclasifica primero este viaje anterior para separarlo de las cajas de ventas')
    if action=='ANULADO':trip.reversal_entry_id=expense_entry(db,trip.branch_id,web.local_today(),trip.amount_cs,user.full_name or user.email,f'AUTO-MOTO-VOID-{trip.id}',f'Anulación viaje #{trip.id}: {reason}',original=trip.accounting_entry).id
    trip.status=action
    db.add(MotoTripEvent(trip_id=trip.id,actor_id=user.id,action=action,comment=reason.strip(),created_at=web.local_now_naive()));db.flush()
    return trip



def closure_scope(db,user,query):
    from ..routers import web
    query=query.filter(MotoTripClosure.branch_id.in_(web._user_scoped_branch_ids(db,user)))
    if 'administrador' not in roles(user):
        clauses=[]
        if 'motorizado' in roles(user):clauses.append(MotoTripClosure.rider_id==user.id)
        if 'vendedor' in roles(user) and user.vendedor_id:clauses.append(MotoTripClosure.seller_id==user.vendedor_id)
        query=query.filter(or_(*clauses) if clauses else MotoTripClosure.id<0)
    return query


def close_trips(db,user,trip_ids,token,paid=False,comment=''):
    from decimal import Decimal
    from ..routers import web
    if 'vendedor' not in roles(user) or not user.vendedor_id:raise HTTPException(403,'Solo el vendedor asignado puede pagar y cerrar sus viajes')
    if not re.fullmatch('[a-f0-9]{32}',str(token)):raise ValueError('Recarga el formulario de cierre')
    if not paid:raise ValueError('Confirma que ya pagaste al motorizado antes de cerrar')
    if not trip_ids or len(trip_ids)>100 or len(set(trip_ids))!=len(trip_ids):raise ValueError('Selecciona entre 1 y 100 viajes sin repetir')
    if len(comment)>500:raise ValueError('El comentario admite hasta 500 caracteres')
    rows=scope(db,user,db.query(MotoTrip)).filter(MotoTrip.id.in_(trip_ids)).order_by(MotoTrip.id).all()
    if len(rows)!=len(trip_ids) or any(not seller_can(user,row) for row in rows):raise HTTPException(403,'Solo puedes cerrar viajes asignados a tu vendedor')
    if len({(row.rider_id,row.branch_id) for row in rows})!=1:raise ValueError('Cada cierre corresponde a un solo motorizado y sucursal')
    # Bloquear únicamente los viajes, en orden estable; no caja, contabilidad ni usuarios.
    rows=scope(db,user,db.query(MotoTrip)).filter(MotoTrip.id.in_(trip_ids)).order_by(MotoTrip.id).populate_existing().with_for_update().all()
    previous=db.query(MotoTripClosure).filter_by(operation_key=token).first()
    if previous:
        if previous.closed_by_id!=user.id or {row.id for row in previous.trips}!=set(trip_ids):raise HTTPException(409,'Esta operación ya corresponde a otro cierre')
        return previous
    if len(rows)!=len(trip_ids) or any(not seller_can(user,row) for row in rows):raise HTTPException(403,'El vendedor asignado cambió; revisa los viajes')
    if any(row.closure_id or row.status!='RECIBIDO' for row in rows):raise ValueError('Solo puedes cerrar viajes con recepción confirmada que aún estén pendientes de pago')
    total=sum((row.amount_cs for row in rows),Decimal(0))
    closure=MotoTripClosure(operation_key=token,rider_id=rows[0].rider_id,seller_id=user.vendedor_id,branch_id=rows[0].branch_id,closed_by_id=user.id,closure_date=web.local_today(),closed_at=web.local_now_naive(),amount_cs=total,trip_count=len(rows),comment=comment.strip())
    db.add(closure);db.flush()
    for row in rows:
        row.closure_id=closure.id
        db.add(MotoTripEvent(trip_id=row.id,actor_id=user.id,action='CERRADO',comment=(f'Cierre #{closure.id}. Pago confirmado por el vendedor.'+(' '+comment.strip() if comment.strip() else ''))[:500],created_at=closure.closed_at))
    db.flush();return closure
