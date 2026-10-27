"""Letter-sized vouchers using saved accounting values and company branding."""
import io
from decimal import Decimal
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, KeepTogether


def build_accounting_entry_pdf(entry, profile, logo_path=None):
    buffer = io.BytesIO()
    ink = colors.HexColor('#243449')
    muted = colors.HexColor('#596777')
    border = colors.HexColor('#d5dde5')
    base = ParagraphStyle('body', fontName='Helvetica', fontSize=8, leading=11, textColor=ink, splitLongWords=True)
    small = ParagraphStyle('small', parent=base, fontSize=7, leading=10, textColor=muted)
    bold = ParagraphStyle('bold', parent=base, fontName='Helvetica-Bold')
    right = ParagraphStyle('right', parent=base, alignment=TA_RIGHT, fontName='Helvetica-Bold')
    def p(value, style=base):
        return Paragraph(escape(str(value if value is not None else '-')).replace('\n', '<br/>'), style)
    def money(value):
        return f'{Decimal(str(value or 0)):,.2f}'
    def labelled(label, value):
        return Paragraph(f'<font color="#596777">{escape(label)}</font><br/><b>{escape(str(value or "-"))}</b>', base)
    number = entry.numero or '-'
    status = {'POSTEADO': 'PUBLICADO', 'BORRADOR': 'BORRADOR - SIN EFECTO EN LIBROS', 'REVERSADO': 'REVERSADO', 'ANULADO': 'ANULADO'}.get(entry.estado, entry.estado or '-')
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=36, leftMargin=36, topMargin=146,
                            bottomMargin=44, title=f'Comprobante contable {number}', author=profile.get('legal_name') or '')
    def page_frame(canvas, document):
        canvas.saveState()
        canvas.setFillColor(ink)
        if logo_path and Path(logo_path).is_file():
            canvas.drawImage(str(logo_path), 36, 706, width=68, height=50, preserveAspectRatio=True, anchor='c', mask='auto')
        company = p(profile.get('legal_name') or profile.get('trade_name') or 'Empresa', bold)
        _, h = company.wrap(225, 42)
        company.drawOn(canvas, 116, 752-h)
        contact = ' | '.join(label+str(profile[key]) for key,label in (('ruc','RUC: '),('phone','Tel.: ')) if profile.get(key))
        info = p(contact, small); _, h2 = info.wrap(225, 30); info.drawOn(canvas,116,747-h-h2)
        canvas.setFont('Helvetica-Bold', 12)
        canvas.drawRightString(576, 748, 'COMPROBANTE CONTABLE')
        label = p(number, right); _, nh = label.wrap(216, 40); label.drawOn(canvas,360,733-nh)
        state = p(status, ParagraphStyle('state',parent=right,fontSize=7,leading=10)); _, sh = state.wrap(216,40);state.drawOn(canvas,360,729-nh-sh)
        address = p(' | '.join(str(profile.get(key)) for key in ('address','email','website') if profile.get(key)), small)
        _, ah = address.wrap(540, 40); address.drawOn(canvas,36,688-ah)
        canvas.setStrokeColor(border); canvas.line(36,655,576,655)
        canvas.line(36,34,576,34)
        canvas.setFillColor(muted);canvas.setFont('Helvetica',7)
        canvas.drawString(36,23,f'{number} | Valores contables en C$')
        canvas.drawRightString(576,23,f'Página {document.page}')
        canvas.restoreState()
    meta = Table([
        [labelled('FECHA CONTABLE',entry.fecha.strftime('%d/%m/%Y') if entry.fecha else '-'),labelled('PERÍODO',entry.periodo),labelled('SUCURSAL',entry.branch.name if entry.branch else 'General')],
        [labelled('TIPO',entry.tipo.nombre if entry.tipo else '-'),labelled('DIARIO',entry.journal.name if entry.journal else '-'),labelled('REFERENCIA / DOCUMENTO',entry.referencia)],
    ], colWidths=[180]*3, hAlign='LEFT')
    meta.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,-1),colors.HexColor('#f3f6f9')),('BOX',(0,0),(-1,-1),.5,border),('LEFTPADDING',(0,0),(-1,-1),10),('RIGHTPADDING',(0,0),(-1,-1),10),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8)]))
    story=[meta,Spacer(1,12),p('CONCEPTO',bold),Spacer(1,4),p(entry.descripcion or '-'),Spacer(1,10)]
    currency=entry.moneda or 'CS'
    rate=Decimal(str(entry.tasa_cambio or 0))
    currency_text=f'Moneda de captura: {"USD" if currency=="USD" else "C$"}. Moneda de los libros y de las líneas siguientes: C$.'
    if currency=='USD':
        currency_text+=f' Tipo de cambio registrado: C$ {rate:,.4f} por USD.'
    story.extend([p(currency_text,small),Spacer(1,10)])
    rows=[[p('CÓDIGO',bold),p('CUENTA CONTABLE',bold),p('DETALLE / TERCERO',bold),p('DEBE C$',right),p('HABER C$',right)]]
    for line in entry.lines:
        details=[line.descripcion or '']
        if line.proveedor: details.append('Proveedor: '+line.proveedor.nombre)
        if line.cliente: details.append('Cliente: '+line.cliente.nombre)
        if line.vencimiento: details.append('Vence: '+line.vencimiento.strftime('%d/%m/%Y'))
        rows.append([p(line.cuenta.codigo if line.cuenta else '-'),p(line.cuenta.nombre if line.cuenta else '-'),p('\n'.join(filter(None,details)) or '-'),p(money(line.debe),right),p(money(line.haber),right)])
    table=Table(rows,colWidths=[55,140,145,100,100],repeatRows=1,hAlign='LEFT')
    table.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e8eef4')),('LINEBELOW',(0,0),(-1,0),.8,border),('LINEBELOW',(0,1),(-1,-1),.35,border),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#fafbfd')]),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8)]))
    story.append(table)
    debit=Decimal(str(entry.total_debe or 0));credit=Decimal(str(entry.total_haber or 0))
    totals=Table([[p('TOTALES C$',bold),p(money(debit),right),p(money(credit),right)]],colWidths=[340,100,100])
    totals.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),colors.HexColor('#e8eef4')),('TOPPADDING',(0,0),(-1,-1),10),('BOTTOMPADDING',(0,0),(-1,-1),10),('RIGHTPADDING',(0,0),(-1,-1),7)]))
    summary=[Spacer(1,8),totals,Spacer(1,6),p('CUADRE: '+('CORRECTO - Debe = Haber' if debit==credit and debit>0 else 'PENDIENTE')+f' | Diferencia C$ {money(abs(debit-credit))}',bold)]
    if currency=='USD' and rate>0:
        summary.extend([Spacer(1,4),p(f'Equivalente en moneda de captura: Debe USD {money(debit/rate)} | Haber USD {money(credit/rate)}',small)])
    if entry.reversa_de:
        summary.extend([Spacer(1,6),p('Reversión del comprobante: '+entry.reversa_de.numero,small)])
    if entry.anulado_motivo:
        summary.extend([Spacer(1,6),p('Motivo / trazabilidad: '+entry.anulado_motivo,small)])
    story.append(KeepTogether(summary))
    created=entry.created_at.strftime('%d/%m/%Y %H:%M') if entry.created_at else '-'
    posted=entry.publicado_at.strftime('%d/%m/%Y %H:%M') if entry.publicado_at else '-'
    story.extend([Spacer(1,14),p(f'Registrado por: {entry.creado_por or "-"} | Fecha de registro: {created}',small),p(f'Publicado por: {entry.publicado_por or "-"} | Fecha de publicación: {posted}',small),Spacer(1,32)])
    signatures=Table([[p('Elaborado por',small),p('Revisado por',small),p('Autorizado por',small)]],colWidths=[180]*3)
    signatures.setStyle(TableStyle([('LINEABOVE',(0,0),(-1,0),.5,border),('TOPPADDING',(0,0),(-1,-1),7)]))
    story.append(signatures)
    doc.build(story,onFirstPage=page_frame,onLaterPages=page_frame)
    return buffer.getvalue()
