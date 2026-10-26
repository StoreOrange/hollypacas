"""Localizador contable de Hollpacas. Consulta paginada, sin modificar comprobantes."""
import calendar
from datetime import date
from decimal import Decimal,InvalidOperation
from urllib.parse import urlencode
from fastapi import APIRouter,Depends,HTTPException,Request
from sqlalchemy import or_,and_,func,case,cast,String,literal
from sqlalchemy.orm import selectinload,joinedload
from . import web as w

router=APIRouter(prefix='/accounting')


def registry_data(request,db,user):
    w._enforce_permission(request,user,'access.accounting.entries')
    if not w._is_pacasholl_company():raise HTTPException(404)
    E,L=w.AccountingEntry,w.AccountingEntryLine
    scope=w._user_scoped_branch_ids(db,user)
    query=db.query(E).filter(or_(E.branch_id.is_(None),E.branch_id.in_(scope)))
    filters={key:request.query_params.get(key,'').strip() for key in ('q','start','end','month','branch_id','estado','journal_id','tipo_id','account_id','amount','amount_min','amount_max','amount_currency')}
    def number(key):
        value=filters[key]
        if value and not value.isdigit():raise HTTPException(400,'Filtro inválido: '+key)
        return int(value) if value else None
    if filters['branch_id']=='general':query=query.filter(E.branch_id.is_(None))
    elif filters['branch_id']:
        branch=number('branch_id')
        if branch not in scope:raise HTTPException(403,'Sucursal no autorizada')
        query=query.filter(E.branch_id==branch)
    for key,column in [('journal_id',E.journal_id),('tipo_id',E.tipo_id)]:
        value=number(key)
        if value:query=query.filter(column==value)
    if filters['estado']:
        if filters['estado'] not in ('BORRADOR','POSTEADO','REVERSADO','ANULADO'):raise HTTPException(400,'Estado inválido')
        query=query.filter(E.estado==filters['estado'])
    account=number('account_id')
    if account:query=query.filter(E.id.in_(db.query(L.entry_id).filter(L.cuenta_id==account)))
    try:
        start=date.fromisoformat(filters['start']) if filters['start'] else None
        end=date.fromisoformat(filters['end']) if filters['end'] else None
        if filters['month']:
            month=date.fromisoformat(filters['month']+'-01')
            start=start or month;end=end or month.replace(day=calendar.monthrange(month.year,month.month)[1])
        if start and end and start>end:raise ValueError()
    except ValueError:raise HTTPException(400,'Revisa el rango de fechas o mes')
    if start:query=query.filter(E.fecha>=start)
    if end:query=query.filter(E.fecha<=end)
    if filters['q']:
        like='%'+filters['q'].lower()+'%'
        invoices=db.query(w.VentaFactura.id).join(w.Bodega).filter(w.Bodega.branch_id.in_(scope),func.lower(w.VentaFactura.numero).like(like))
        receipts=db.query(w.ReciboCaja.accounting_entry_id).filter(w.ReciboCaja.branch_id.in_(scope),or_(func.lower(w.ReciboCaja.numero).like(like),func.lower(w.ReciboCaja.descripcion).like(like)))
        query=query.filter(or_(func.lower(E.numero).like(like),func.lower(func.coalesce(E.referencia,'')).like(like),func.lower(E.descripcion).like(like),E.id.in_(db.query(L.entry_id).filter(func.lower(func.coalesce(L.descripcion,'')).like(like))),E.id.in_(receipts),E.referencia.in_(invoices.with_entities(literal('AUTO-VTA-')+cast(w.VentaFactura.id,String))),E.referencia.in_(invoices.with_entities(literal('AUTO-COSTO-VTA-')+cast(w.VentaFactura.id,String)))))
    if filters['amount_currency'] not in ('','CS','origen'):raise HTTPException(400,'Moneda de búsqueda inválida')
    factor=case((E.moneda=='USD',func.nullif(E.tasa_cambio,0)),else_=1) if filters['amount_currency']=='origen' else literal(1)
    def money(key):
        if not filters[key]:return None
        try:
            value=Decimal(filters[key])
            if not value.is_finite() or value<0 or value>Decimal('999999999999.99') or value!=value.quantize(Decimal('.01')):raise ValueError()
            return value
        except (ValueError,InvalidOperation):raise HTTPException(400,'Monto inválido; usa un número positivo con hasta 2 decimales')
    exact,low,high=money('amount'),money('amount_min'),money('amount_max')
    if low is not None and high is not None and low>high:raise HTTPException(400,'El monto desde no puede superar el monto hasta')
    if exact is not None:
        line_matches=db.query(L.id).filter(L.entry_id==E.id,or_(and_(L.debe>0,func.round(L.debe/factor,2)==exact),and_(L.haber>0,func.round(L.haber/factor,2)==exact))).correlate(E).exists()
        query=query.filter(or_(func.round(E.total_debe/factor,2)==exact,func.round(E.total_haber/factor,2)==exact,line_matches))
    if low is not None:query=query.filter(E.total_debe/factor>=low)
    if high is not None:query=query.filter(E.total_debe/factor<=high)
    try:page=max(1,int(request.query_params.get('page','1')))
    except ValueError:raise HTTPException(400,'Página inválida')
    count=query.count();pages=max(1,(count+49)//50);page=min(page,pages)
    entries=query.options(joinedload(E.tipo),joinedload(E.journal),joinedload(E.branch),selectinload(E.lines).joinedload(L.cuenta),selectinload(E.lines).joinedload(L.cliente),selectinload(E.lines).joinedload(L.proveedor)).order_by(E.fecha.desc(),E.id.desc()).offset((page-1)*50).limit(50).all()
    sources={}
    entry_ids=[entry.id for entry in entries]
    for receipt in db.query(w.ReciboCaja).filter(w.ReciboCaja.accounting_entry_id.in_(entry_ids),w.ReciboCaja.branch_id.in_(scope)).all():
        sources[receipt.accounting_entry_id]=dict(label=receipt.numero,url=f'/sales/roc/{receipt.id}/pdf')
    sale_ids={}
    for entry in entries:
        ref=entry.referencia or ''
        for prefix in ('AUTO-VTA-','AUTO-COSTO-VTA-'):
            if ref.startswith(prefix) and ref[len(prefix):].isdigit():sale_ids[entry.id]=int(ref[len(prefix):])
    sales={invoice.id:invoice for invoice in db.query(w.VentaFactura).join(w.Bodega).filter(w.VentaFactura.id.in_(sale_ids.values()),w.Bodega.branch_id.in_(scope)).all()}
    for entry_id,sale_id in sale_ids.items():
        if sale_id in sales:sources[entry_id]=dict(label=sales[sale_id].numero,url=f'/sales/{sale_id}/pdf')
    def page_url(value):return '/accounting/comprobantes?'+urlencode({**filters,'page':value})
    return dict(entries=entries,sources=sources,filters=filters,count=count,page=page,pages=pages,previous=page_url(page-1) if page>1 else '',following=page_url(page+1) if page<pages else '',branches=db.query(w.Branch).filter(w.Branch.id.in_(scope)).order_by(w.Branch.name).all(),journals=db.query(w.AccountingJournal).filter(or_(w.AccountingJournal.branch_id.is_(None),w.AccountingJournal.branch_id.in_(scope))).order_by(w.AccountingJournal.name).all(),types=db.query(w.AccountingVoucherType).order_by(w.AccountingVoucherType.nombre).all(),accounts=db.query(w.CuentaContable).order_by(w.CuentaContable.codigo).all())


@router.get('/comprobantes')
def page(request:Request,db=Depends(w.get_db),user=Depends(w._require_admin_web)):
    data=registry_data(request,db,user)
    return request.app.state.templates.TemplateResponse('accounting_registry.html',dict(request=request,user=user,version=w.settings.UI_VERSION,today=w.local_today().isoformat(),**data))
