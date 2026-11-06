"""Standalone shareable, legible mobile JPG receipt, generated from approved rows."""
import io
import textwrap
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


def jpeg(record):
    candidates=['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf','/System/Library/Fonts/Supplemental/Arial.ttf']
    path=next((p for p in candidates if Path(p).exists()),None)
    font=ImageFont.truetype(path,25) if path else ImageFont.load_default()
    bold=ImageFont.truetype(path,34) if path else font
    lines=[]
    def add(text):lines.extend(textwrap.wrap(str(text),width=43) or [''])
    add('PACAS HOLL');add('COMPROBANTE DE TRASLADO');add(record.numero);add('')
    add(f'De: {record.source.name}');add(f'Para: {record.destination.name}')
    add(f'Solicitud: {record.fecha:%d/%m/%Y}');add(f'Autorizado: {record.decided_at:%d/%m/%Y %H:%M} · Nicaragua')
    add(f'Solicitó: {record.requester_name}');add(f'Vendedor: {record.seller_name}');add(f'Autorizó: {record.decider_name}');add('')
    for item in record.items:
        add(f'{item.quantity} unid. | {item.code}');add(item.description);add('')
    add(f'Total: {sum(i.quantity for i in record.items)} unidades')
    add(f'Egreso #{record.egreso_id} / Ingreso #{record.ingreso_id}')
    if record.observation:add('Observaciones: '+record.observation)
    add('Traslado realizado · Activa');add('Conservar para control de entrega y recepción.')
    image=Image.new('RGB',(760,100+len(lines)*37),'white');draw=ImageDraw.Draw(image)
    for idx,line in enumerate(lines):draw.text((40,35+idx*37),line,fill='#132f4c',font=bold if idx==0 else font)
    buf=io.BytesIO();image.save(buf,'JPEG',quality=92,optimize=True)
    return buf.getvalue()
