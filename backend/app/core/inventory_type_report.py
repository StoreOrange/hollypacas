"""Movimientos por tipo, en una bodega, sin duplicar costos por vínculos de laboratorio."""
from decimal import Decimal
from sqlalchemy import func, case
from ..models.inventory import (IngresoInventario, IngresoItem, IngresoTipo,
    EgresoInventario, EgresoItem, EgresoTipo, ProductionLaboratoryMovement, ProductionLaboratory)


def inventory_type_rows(db, start, end, warehouse_id, direction='ambos', income_type=None, expense_type=None):
    rows=[]
    for label, doc, item, kind, foreign_key, lab_key, selected in (
        ('Ingreso',IngresoInventario,IngresoItem,IngresoTipo,IngresoItem.ingreso_id,ProductionLaboratoryMovement.ingreso_id,income_type),
        ('Egreso',EgresoInventario,EgresoItem,EgresoTipo,EgresoItem.egreso_id,ProductionLaboratoryMovement.egreso_id,expense_type),
    ):
        if direction not in ('ambos',label.lower()):
            continue
        cs=func.coalesce(func.nullif(item.subtotal_cs,0),item.cantidad*item.costo_unitario_cs,0)
        usd=func.coalesce(func.nullif(item.subtotal_usd,0),item.cantidad*item.costo_unitario_usd,0)
        historical_cs=case(((cs==0)&(doc.tasa_cambio>0),usd*doc.tasa_cambio),else_=cs)
        historical_usd=case(((usd==0)&(doc.tasa_cambio>0),cs/func.nullif(doc.tasa_cambio,0)),else_=usd)
        amounts=(db.query(foreign_key.label('document_id'),func.sum(item.cantidad).label('quantity'),func.sum(historical_cs).label('cost_cs'),func.sum(historical_usd).label('cost_usd'))
            .join(doc,doc.id==foreign_key).filter(doc.bodega_id==warehouse_id,doc.fecha>=start,doc.fecha<=end)
            .group_by(foreign_key).subquery())
        query=(db.query(doc,kind.nombre,amounts.c.quantity,amounts.c.cost_cs,amounts.c.cost_usd,ProductionLaboratory,ProductionLaboratoryMovement.clase)
            .outerjoin(kind,kind.id==doc.tipo_id).outerjoin(amounts,amounts.c.document_id==doc.id)
            .outerjoin(ProductionLaboratoryMovement,lab_key==doc.id)
            .outerjoin(ProductionLaboratory,ProductionLaboratory.id==ProductionLaboratoryMovement.laboratorio_id)
            .filter(doc.bodega_id==warehouse_id,doc.fecha>=start,doc.fecha<=end))
        if selected:query=query.filter(doc.tipo_id==selected)
        for record,type_name,quantity,cost_cs,cost_usd,lab,lab_class in query.all():
            cs_value=Decimal(str(cost_cs or 0));usd_value=Decimal(str(cost_usd or 0))
            rows.append(dict(direction=label,id=record.id,date=record.fecha,type_id=record.tipo_id,type=type_name or 'Sin tipo',quantity=Decimal(str(quantity or 0)),cost_cs=cs_value.quantize(Decimal('.01')),cost_usd=usd_value.quantize(Decimal('.01')),note=record.observacion or '',user=record.usuario_registro or '-',lab_id=lab.id if lab else None,lab_number=lab.numero if lab else '',lab_date=lab.fecha if lab else None,lab_class=lab_class or '',document_url=f'/inventory/{"ingresos" if label=="Ingreso" else "egresos"}?start_date={record.fecha}&end_date={record.fecha}&focus_{"ingreso" if label=="Ingreso" else "egreso"}_id={record.id}'))
    rows.sort(key=lambda r:(r['direction'],r['type'].casefold(),r['date'],r['id']))
    groups={}
    totals={label:dict(documents=0,quantity=Decimal(0),cost_cs=Decimal(0),cost_usd=Decimal(0)) for label in ('Ingreso','Egreso')}
    for row in rows:
        key=(row['direction'],row['type_id'])
        bucket=groups.setdefault(key,dict(direction=row['direction'],type=row['type'],rows=[],documents=0,quantity=Decimal(0),cost_cs=Decimal(0),cost_usd=Decimal(0)))
        bucket['rows'].append(row)
        for target in (bucket,totals[row['direction']]):
            target['documents']+=1
            for field in ('quantity','cost_cs','cost_usd'):target[field]+=row[field]
    return list(groups.values()),totals
