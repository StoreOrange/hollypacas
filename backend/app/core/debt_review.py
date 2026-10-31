"""Read-only vendor → customer → invoice review, separating receipts from adjustments."""
from datetime import timedelta
from decimal import Decimal
from ..models.sales import CollectionReviewSetting

PALETTE = {
    'green': {'label':'Verde', 'text':'#15803d', 'background':'#edfdf3', 'border':'#b7e9cb'},
    'red': {'label':'Rojo', 'text':'#b91c1c', 'background':'#fff1f2', 'border':'#fecdd3'},
    'amber': {'label':'Ámbar', 'text':'#a16207', 'background':'#fffbeb', 'border':'#f5dea0'},
    'blue': {'label':'Azul', 'text':'#1d4ed8', 'background':'#eff6ff', 'border':'#bfdbfe'},
}


def settings(db):
    row = db.get(CollectionReviewSetting,1)
    return {'overdue_days':row.overdue_days if row else 30, 'recent_days':row.recent_days if row else 7,
            'recent_color':row.recent_color if row and row.recent_color in PALETTE else 'green',
            'aged_color':row.aged_color if row and row.aged_color in PALETTE else 'red'}


def blank():
    return dict(due_cs=Decimal(0),due_usd=Decimal(0),received_cs=Decimal(0),received_usd=Decimal(0),
                recent_cs=Decimal(0),recent_usd=Decimal(0),recent_count=0,pending_count=0,aged_count=0,
                invoice_count=0,adjustment_cs=Decimal(0),adjustment_usd=Decimal(0))


def build(rows,today,config):
    vendors={};clients={};receipts=[];invoices=[]
    first_recent=today-timedelta(days=config['recent_days']-1)
    for row in rows:
        f=row['invoice'];vendor=f.vendedor_id or 0;client=f.cliente_id or 0
        vendor_name=f.vendedor.nombre if f.vendedor else 'Sin vendedor asignado'
        client_name=f.cliente.nombre if f.cliente else 'Consumidor final / sin cliente'
        if vendor not in vendors:vendors[vendor]=dict(blank(),id=vendor,name=vendor_name,client_keys=set())
        key=(vendor,client)
        if key not in clients:clients[key]=dict(blank(),vendor=vendor,client=client,name=client_name,vendor_name=vendor_name,rows=[],receipts=[])
        v,g=vendors[vendor],clients[key];v['client_keys'].add(key)
        age=max((today-f.fecha.date()).days,0) if f.fecha else 0
        aged=bool(row['due'] and age>=config['overdue_days'])
        cash_cs=Decimal(0);cash_usd=Decimal(0);adj_cs=Decimal(0);adj_usd=Decimal(0)
        events=[];adjustments=[]
        for initial in f.pagos:
            if initial.id in row['deferred_ids']:continue
            cs=Decimal(str(initial.monto_cs or 0));usd=Decimal(str(initial.monto_usd or 0))
            cash_cs+=cs;cash_usd+=usd
            events.append(dict(kind='initial',number='Pago al facturar',date=f.fecha.date() if f.fecha else None,
                cs=cs,usd=usd,currency=f.moneda or 'CS',amount=usd if f.moneda=='USD' else cs,
                registered_by=f.usuario_registro or 'Registro de factura',recent=False,abono_id=None,
                invoice=f,vendor=vendor,client=client,client_name=client_name,vendor_name=vendor_name))
        for a in f.abonos:
            cs=Decimal(str(a.monto_cs or 0));usd=Decimal(str(a.monto_usd or 0))
            if (a.tipo_mov or 'ABONO').strip().upper()!='ABONO':
                factor=Decimal(-1) if a.tipo_mov=='NOTA_DEBITO' else Decimal(1)
                adj_cs+=cs*factor;adj_usd+=usd*factor;adjustments.append(a);continue
            cash_cs+=cs;cash_usd+=usd
            recent=bool(a.fecha and first_recent<=a.fecha<=today)
            event=dict(kind='abono',number=a.numero,date=a.fecha,cs=cs,usd=usd,currency=a.moneda or 'CS',
                amount=usd if a.moneda=='USD' else cs,registered_by=a.usuario_registro or 'Sin usuario indicado',
                recent=recent,abono_id=a.id,invoice=f,vendor=vendor,client=client,client_name=client_name,vendor_name=vendor_name)
            events.append(event);receipts.append(event)
        events.sort(key=lambda e:(e['date'] or today,e['abono_id'] or 0),reverse=True)
        item=dict(row,age=age,aged=aged,received_cs=cash_cs,received_usd=cash_usd,
            received_native=cash_usd if f.moneda=='USD' else cash_cs,
            adjustment_native=adj_usd if f.moneda=='USD' else adj_cs,events=events,adjustments=adjustments,
            vendor=vendor,client=client,client_name=client_name,vendor_name=vendor_name)
        invoices.append(item);g['rows'].append(item);g['receipts'].extend(events)
        recent_events=[e for e in events if e['recent']]
        for target in (v,g):
            target['invoice_count']+=1;target['pending_count']+=int(bool(row['due']));target['aged_count']+=int(aged)
            target['due_cs']+=row['due_cs'];target['due_usd']+=row['due_usd']
            target['received_cs']+=cash_cs;target['received_usd']+=cash_usd
            target['adjustment_cs']+=adj_cs;target['adjustment_usd']+=adj_usd
            target['recent_count']+=len(recent_events)
            target['recent_cs']+=sum((e['cs'] for e in recent_events),Decimal(0))
            target['recent_usd']+=sum((e['usd'] for e in recent_events),Decimal(0))
    for v in vendors.values():v['client_count']=len(v['client_keys'])
    vendor_list=sorted(vendors.values(),key=lambda v:(-v['aged_count'],-v['due_cs'],-v['recent_count'],v['name'].casefold()))
    client_list=sorted(clients.values(),key=lambda g:(-g['aged_count'],-g['due_cs'],-g['recent_count'],g['name'].casefold()))
    receipts.sort(key=lambda e:(e['date'] or today,e['abono_id'] or 0),reverse=True)
    return dict(vendors=vendor_list,clients=client_list,receipts=receipts,invoices=invoices,first_recent=first_recent)
