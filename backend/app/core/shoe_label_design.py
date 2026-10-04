"""Label layout shared by the editor, PDF preview and print jobs."""
import copy
import io
import json
import math
import os
import tempfile
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth, getAscentDescent
from reportlab.graphics.barcode.code128 import Code128

PATH = Path(__file__).resolve().parents[2] / 'settings' / 'miss_zapatos_labels.json'
FONTS = {'Helvetica': ('Helvetica', 'Helvetica-Bold'), 'Times': ('Times-Roman', 'Times-Bold'), 'Courier': ('Courier', 'Courier-Bold')}
DEFAULT = {'width': 50, 'height': 20, 'margin': 2, 'font': 'Helvetica', 'offset_x': 0, 'offset_y': 0,
           'rotation': 0, 'barcode_max_width': 46, 'barcode_height': 5, 'barcode_top': 7.3,
           'barcode_width': 0.25, 'barcode_x': None, 'fields': {}}
for key, label, size, y, bold in [('name','Producto',5,3.4,True),('detail','Color / talla',4.5,5.9,False),('code','Código',4.5,14.3,True),('reference','Referencia',4.5,17,False),('price','Precio',5,17,True)]:
    DEFAULT['fields'][key] = {'label': label, 'size': size, 'top': y, 'bold': bold, 'visible': True, 'align': 'center', 'x': None}

DEFAULT['fields']['reference'].update(align='left', top=17)
DEFAULT['fields']['price'].update(align='right', top=17)


def validate(data):
    if not isinstance(data, dict): raise ValueError('Configuración inválida')
    result = copy.deepcopy(DEFAULT)
    for key, low, high in [('margin',2,10),('width',30,150),('height',20,150),('offset_x',-10,10),('offset_y',-10,10),('barcode_height',5,100),('barcode_top',0,140),('barcode_width',0.25,0.6),('barcode_max_width',20,140)]:
        value = data.get(key, result[key])
        if isinstance(value, bool): raise ValueError(f'Valor inválido: {key}')
        value = float(value)
        if not math.isfinite(value) or not low <= value <= high: raise ValueError(f'{key}: debe estar entre {low} y {high}')
        result[key] = value
    rotation = data.get('rotation', 0)
    if type(rotation) is not int or rotation not in (0, 180): raise ValueError('Seleccione giro normal o 180°')
    result['rotation'] = rotation
    result['font'] = data.get('font', result['font'])
    if result['font'] not in FONTS: raise ValueError('Tipografía no disponible')
    fields = data.get('fields', result['fields'])
    if not isinstance(fields, dict): raise ValueError('Campos inválidos')
    for key, field in result['fields'].items():
        incoming = fields.get(key, field)
        if not isinstance(incoming, dict): raise ValueError('Campo inválido')
        for prop, low, high in [('size',4,32),('top',0,result['height'])]:
            value = float(incoming.get(prop, field[prop]))
            if not math.isfinite(value) or not low <= value <= high: raise ValueError(f'{field["label"]}: {prop} fuera de rango')
            field[prop] = value
        for prop in ('bold', 'visible'):
            value = incoming.get(prop,field[prop])
            if type(value) is not bool: raise ValueError('Use las casillas de visibilidad y negrita')
            field[prop] = value
        field['align'] = incoming.get('align', 'center')
        if field['align'] not in ('left','center','right'): raise ValueError('Alineación inválida')
    margin = result['margin']
    if result['width'] - 2*(margin+abs(result['offset_x'])) < 20:
        raise ValueError('Los márgenes y desplazamientos dejan muy poco ancho útil')
    for key, field in result['fields'].items():
        incoming = fields.get(key, {})
        field['x'] = incoming.get('x')
        if field['x'] is not None:
            field['x'] = float(field['x'])
            if not math.isfinite(field['x']) or not 0 <= field['x'] <= result['width']:
                raise ValueError(f'{field["label"]}: posición horizontal inválida')
    result['barcode_x'] = data.get('barcode_x')
    if result['barcode_x'] is not None:
        result['barcode_x'] = float(result['barcode_x'])
        if not math.isfinite(result['barcode_x']) or not 0 <= result['barcode_x'] <= result['width']:
            raise ValueError('Posición horizontal del código de barras inválida')
    return result


def load():
    return validate(json.loads(PATH.read_text())) if PATH.exists() else copy.deepcopy(DEFAULT)


def save(data):
    value = validate(data)
    PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=PATH.parent, delete=False, encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        name = f.name
    os.replace(name, PATH)
    return value


def build_barcode(code, d):
    # Never stretch the symbol or remove its quiet zones to fit the label.
    available = min(d['barcode_max_width'], d['width']-2*d.get('margin',3)-2*abs(d['offset_x']))*mm
    desired = d['barcode_width']
    candidates = [desired] + [v for v in (0.375, 0.25, 0.21) if v < desired]
    for bar_mm in candidates:
        barcode = Code128(code, barHeight=d['barcode_height']*mm, barWidth=bar_mm*mm,
                          quiet=True, lquiet=10*bar_mm*mm, rquiet=10*bar_mm*mm)
        if barcode.width <= available:
            return barcode
    raise ValueError(f'El código {code} no cabe sin adelgazar demasiado las barras. Aumente el ancho máximo del código o el ancho de etiqueta.')


def anchor_x(d, field):
    if field.get('x') is not None: return field['x']
    return {'left': d['margin'], 'center': d['width']/2, 'right': d['width']-d['margin']}[field['align']]


def layout(row, design, show_price=True):
    """Millimetre geometry used by both the drag editor and PDF rendering."""
    d = validate(design)
    nodes, errors = [], []
    for key, field in d['fields'].items():
        if not field['visible'] or (key == 'price' and not show_price): continue
        font = FONTS[d['font']][int(field['bold'])]
        x = anchor_x(d, field) + d['offset_x']
        baseline = field['top'] + d['offset_y']
        available = {'left': d['width']-d['margin']-x,
                     'right': x-d['margin'],
                     'center': 2*min(x-d['margin'],d['width']-d['margin']-x)}[field['align']]
        text = str(row.get(key,''))
        size = field['size']
        width = stringWidth(text,font,size)/mm
        if width > available and width > 0:
            size = max(4, size*max(0,available)/width)
            width = stringWidth(text,font,size)/mm
        ascent, descent = getAscentDescent(font,size)
        left = x - {'left':0, 'center':width/2, 'right':width}[field['align']]
        nodes.append({'id':key,'label':field['label'],'text':text,'x':x,'y':baseline,
                      'left':left,'top':baseline-ascent/mm,'width':width,'height':(ascent-descent)/mm,
                      'font':font,'size':size,'bold':field['bold'],'align':field['align']})
    rects=[]
    try:
        barcode = build_barcode(str(row['code']),d)
        bw = barcode.width/mm
        class Collector:
            def rect(self,x,y,w,h,**kwargs): rects.append([x/mm, y/mm, w/mm, h/mm])
        barcode.canv=Collector()
        barcode.draw()
    except ValueError as exc:
        errors.append(str(exc))
        bw = min(d['barcode_max_width'], d['width']-2*d['margin'])
    bx = d['width']/2 if d.get('barcode_x') is None else d['barcode_x']
    bx += d['offset_x']
    by = d['barcode_top']+d['offset_y']
    nodes.append({'id':'barcode','label':'Código de barras','x':bx,'y':by,'left':bx-bw/2,
                  'top':by,'width':bw,'height':d['barcode_height'],'bars':rects})
    for node in nodes:
        if (node['left'] < d['margin']-0.001 or node['top'] < d['margin']-0.001 or
            node['left']+node['width'] > d['width']-d['margin']+0.001 or
            node['top']+node['height'] > d['height']-d['margin']+0.001):
            errors.append(f"{node['label']}: queda fuera de los márgenes; mueva el elemento o reduzca su tamaño.")
    for i,a in enumerate(nodes):
        for b in nodes[i+1:]:
            if (min(a['left']+a['width'],b['left']+b['width']) > max(a['left'],b['left'])-0.5 and
                min(a['top']+a['height'],b['top']+b['height']) > max(a['top'],b['top'])-0.5):
                errors.append(f"{a['label']} y {b['label']}: deje al menos 0.5 mm de separación.")
    return {'width':d['width'],'height':d['height'],'margin':d['margin'],'rotation':d['rotation'],
            'nodes':nodes,'errors':errors}


def render(rows, design, show_price=True):
    d = validate(design)
    buffer = io.BytesIO()
    width, height = d['width']*mm, d['height']*mm
    pdf = canvas.Canvas(buffer, pagesize=(width,height))
    count = 0
    for row in rows:
        drawing = layout(row,d,show_price)
        if drawing['errors']: raise ValueError(' '.join(drawing['errors']))
        for _ in range(row.get('quantity',1)):
            if count: pdf.showPage()
            count += 1
            pdf.saveState()
            if d['rotation'] == 180:
                pdf.translate(width,height)
                pdf.rotate(180)
            for node in drawing['nodes']:
                if node['id']=='barcode':
                    for x,y,w,h in node['bars']:
                        pdf.rect((node['left']+x)*mm, height-(node['top']+h-y)*mm,w*mm,h*mm,stroke=0,fill=1)
                else:
                    pdf.setFont(node['font'],node['size'])
                    pdf.drawString(node['left']*mm,height-node['y']*mm,node['text'])
            pdf.restoreState()
    pdf.save()
    return buffer.getvalue(), count
