"""Read-only operational snapshot for the Miss Zapatos home."""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from ..models.sales import VentaFactura, VentaPago, FormaPago, CierreCaja, DepositoCliente
from ..models.inventory import Producto, ShoeProductVariant, ShoeVariantStock, ColorCatalog


def money(value):
    return f'{Decimal(str(value or 0)).quantize(Decimal(".01"), rounding=ROUND_HALF_UP):,.2f}'


def snapshot(db, stores, day, *, sales=True, cash=True):
    ids=[s.id for s in stores]
    rows={s.id:dict(id=s.id,name=s.name,sales=Decimal(0),invoices=0,cash_cs=Decimal(0),cash_usd=Decimal(0),
                   counted_cs=Decimal(0),counted_usd=Decimal(0),closed=False,movements=[]) for s in stores}
    start=datetime.combine(day,datetime.min.time());end=start+timedelta(days=1)
    invoices=db.query(VentaFactura).options(joinedload(VentaFactura.pagos).joinedload(VentaPago.forma_pago)).filter(
        VentaFactura.bodega_id.in_(ids),VentaFactura.fecha>=start,VentaFactura.fecha<end,
        VentaFactura.estado!='ANULADA',or_(VentaFactura.setato_excluida.is_(False),VentaFactura.setato_excluida.is_(None))).all() if sales or cash else []
    for invoice in invoices:
        row=rows[invoice.bodega_id]
        row['sales']+=Decimal(str(invoice.total_cs or 0));row['invoices']+=1
        if not cash:continue
        for payment in invoice.pagos:
            method=(payment.forma_pago.nombre if payment.forma_pago else 'Otro').strip()
            currency='USD' if payment.moneda=='USD' else 'CS'
            amount=Decimal(str(payment.monto_original or 0))
            if method.casefold()=='efectivo':row['cash_'+currency.lower()]+=amount
            else:
                row['movements'].append(dict(kind='Cobro · '+method,reference=invoice.numero,currency=currency,amount=money(amount)))
    if cash:
        # Latest count per warehouse: repeated closures are snapshots, not additive.
        closes=db.query(CierreCaja).filter(CierreCaja.bodega_id.in_(ids),CierreCaja.fecha==day).order_by(CierreCaja.created_at.desc(),CierreCaja.id.desc()).all()
        for close in closes:
            row=rows[close.bodega_id]
            if row['closed']:continue
            row.update(closed=True,counted_cs=Decimal(str(close.total_efectivo_cs or 0)),counted_usd=Decimal(str(close.total_efectivo_usd or 0)))
        deposits=db.query(DepositoCliente).filter(DepositoCliente.bodega_id.in_(ids),DepositoCliente.fecha==day).order_by(DepositoCliente.id.desc()).all()
        for deposit in deposits:
            currency='USD' if deposit.moneda=='USD' else 'CS'
            amount=deposit.monto_usd if currency=='USD' else deposit.monto_cs
            rows[deposit.bodega_id]['movements'].append(dict(kind='Registro · '+(deposit.metodo or 'Depósito').replace('_',' ').title(),reference=f'#{deposit.id}',currency=currency,amount=money(amount)))
    totals={key:money(sum((r[key] for r in rows.values()),Decimal(0))) for key in ['sales','cash_cs','cash_usd','counted_cs','counted_usd']}
    for row in rows.values():
        for key in totals:row[key]=money(row[key])
        if not sales:row['sales']=None;row['invoices']=None
    if not sales:totals['sales']=None
    return dict(date=day.strftime('%d/%m/%Y'),stores=list(rows.values()),totals=totals,can_sales=sales,can_cash=cash)


def inventory(db, stores, code):
    query=(code or '').strip()
    if not query:return []
    # Manual filtering only; never substitute or auto-select a product.
    code_filter=or_(Producto.cod_producto.ilike('%'+query.replace('%',r'\%').replace('_',r'\_')+'%',escape='\\'),
                    ShoeProductVariant.cod_variante.ilike('%'+query.replace('%',r'\%').replace('_',r'\_')+'%',escape='\\'))
    if query.upper().startswith('V') and query[1:].isdigit() and query.upper()==f'V{int(query[1:]):06d}':
        code_filter=ShoeProductVariant.id==int(query[1:])
    variants=db.query(ShoeProductVariant,Producto,ColorCatalog).join(Producto,Producto.id==ShoeProductVariant.producto_id).join(ColorCatalog,ColorCatalog.id==ShoeProductVariant.color_id).filter(
        Producto.activo.is_(True),ShoeProductVariant.activo.is_(True),code_filter).order_by(Producto.cod_producto,ShoeProductVariant.cod_variante).limit(41).all()
    stocks=db.query(ShoeVariantStock).filter(ShoeVariantStock.bodega_id.in_([s.id for s in stores]),ShoeVariantStock.variante_id.in_([v.id for v,_,_ in variants])).all()
    balances={(s.variante_id,s.bodega_id):s.existencia for s in stocks}
    return [dict(code=v.cod_variante,model=p.cod_producto,description=p.descripcion,color=c.nombre,size=v.talla,
                 stores=[dict(name=s.name,qty=f'{Decimal(str(balances.get((v.id,s.id),0))):g}') for s in stores]) for v,p,c in variants]
