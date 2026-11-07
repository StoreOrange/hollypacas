"""Read-only PDF reports from stored vacation movements, never revalued salaries."""
import io
import json
from datetime import timezone
from zoneinfo import ZoneInfo
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle,getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,KeepTogether

TYPES={'CASH':'Pago de vacaciones','REST':'Descanso remunerado','SETTLEMENT':'Vacaciones en liquidación final'}


def local_stamp(value):
    if not value:return '-'
    # Movement/reversal created_at defaults to datetime.utcnow in the payroll model.
    aware=value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return aware.astimezone(ZoneInfo('America/Managua')).strftime('%d/%m/%Y %H:%M:%S')


def build(moves,company,generated,entries,receipts,adjustments,reversals,individual=False):
    buffer=io.BytesIO()
    title=TYPES.get(moves[0].kind,'Movimiento de vacaciones') if individual else 'Historial de vacaciones'
    doc=SimpleDocTemplate(buffer,pagesize=letter,leftMargin=16*mm,rightMargin=16*mm,topMargin=16*mm,bottomMargin=18*mm,title=title,author=company.get('trade_name') or 'Pacas Holl')
    styles=getSampleStyleSheet();blue=colors.HexColor('#153B64');gray=colors.HexColor('#60758B')
    normal=ParagraphStyle('VacationBody',parent=styles['Normal'],fontName='Helvetica',fontSize=9,leading=13,textColor=colors.HexColor('#22374C'))
    small=ParagraphStyle('VacationSmall',parent=normal,fontSize=8,leading=11,textColor=gray)
    heading=ParagraphStyle('VacationHeading',parent=normal,fontName='Helvetica-Bold',fontSize=12,leading=16,spaceBefore=12,spaceAfter=8,textColor=blue)
    brand=ParagraphStyle('VacationBrand',parent=heading,fontSize=19,leading=23,spaceBefore=0,spaceAfter=4)
    header=ParagraphStyle('VacationHeader',parent=normal,fontName='Helvetica-Bold',fontSize=8,leading=11,textColor=colors.white)
    def para(value,style=normal):return Paragraph(escape(str(value or '-')).replace('\n','<br/>'),style)
    def table(data,widths,head=False):
        result=Table(data,colWidths=widths,repeatRows=1 if head else 0,hAlign='LEFT')
        rules=[('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),9),('RIGHTPADDING',(0,0),(-1,-1),9),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),('LINEBELOW',(0,0),(-1,-1),.4,colors.HexColor('#DCE5EE'))]
        if head:rules += [('BACKGROUND',(0,0),(-1,0),blue),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#F4F7FB')])]
        else:rules += [('BACKGROUND',(0,0),(-1,-1),colors.HexColor('#F4F7FB'))]
        result.setStyle(TableStyle(rules));return result
    story=[para(company.get('trade_name') or 'PACAS HOLL',brand),para(title,heading)]
    company_line=' | '.join(str(company.get(k) or '') for k in ('legal_name','ruc','phone') if company.get(k))
    if company_line:story.append(para(company_line,small))
    story += [Spacer(1,6*mm)]
    if individual:
        move=moves[0];employee=move.employee;entry=entries.get(move.accounting_entry_id);receipt=receipts.get(move.cash_receipt_id)
        branch=receipt.branch if receipt else (entry.branch if entry and entry.branch else employee.branch)
        status='ANULADO' if move.status!='ACTIVE' else 'VIGENTE'
        story.append(para(f'VAC-{move.id:06d} | {status}',heading))
        if move.status!='ACTIVE':story.append(Paragraph('<b>REGISTRO ANULADO - CONSERVAR COMO REFERENCIA HISTÓRICA</b>',ParagraphStyle('VacationVoid',parent=normal,textColor=colors.HexColor('#B42335'))))
        identity=[
            [para('Empleado\n'+employee.full_name),para('Código / identificación\n'+employee.employee_code+' / '+(employee.identification or 'Sin identificación'))],
            [para('Sucursal\n'+(branch.name if branch else 'Sin sucursal')),para('Fecha del movimiento\n'+move.effective_date.strftime('%d/%m/%Y'))],
        ]
        story.append(table(identity,[doc.width/2]*2));story.append(Spacer(1,5*mm))
        amount_label='MONTO PAGADO C$' if move.kind=='CASH' else 'VALOR DE VACACIONES C$'
        summary=[[para('DÍAS',header),para('SALARIO MENSUAL C$',header),para(amount_label,header)],
                 [para(f'{move.days:,.4f}'),para(f'{move.salary:,.2f}'),para(f'{move.amount:,.2f}')]]
        story.append(table(summary,[doc.width/3]*3,True))
        if move.date_from:story.append(para(f'Descanso: {move.date_from:%d/%m/%Y} al {move.date_to:%d/%m/%Y}',heading))
        if move.kind=='REST':story.append(para('Descanso remunerado. Su salario se paga en la planilla habitual.',small))
        elif move.kind=='SETTLEMENT':story.append(para('El importe corresponde únicamente a vacaciones incluidas en la liquidación final.',small))
        method='Efectivo' if receipt else ('Pago bancario' if entry and entry.journal and entry.journal.kind=='BANK' else 'Según comprobante contable')
        docs=[]
        if move.kind!='REST':docs.append([para('Forma de registro'),para(method)])
        if receipt:docs.append([para('Recibo de caja'),para(receipt.numero+' | '+(receipt.bodega.name if receipt.bodega else ''))])
        if entry:docs.append([para('Comprobante contable'),para(entry.numero+' | '+entry.estado)])
        if entry and entry.journal:docs.append([para('Diario'),para(entry.journal.name)])
        docs += [[para('Registrado por'),para(move.created_by or '-')],[para('Fecha y hora de registro'),para(local_stamp(move.created_at)+' | Nicaragua')]]
        story += [para('Datos del registro',heading),table(docs,[doc.width*.34,doc.width*.66]),para('Motivo / autorización',heading),para(move.reason)]
        for adjustment in adjustments.get(move.id,[]):
            before=json.loads(adjustment.before_json);after=json.loads(adjustment.after_json)
            detail=f"Liquidación en borrador #{adjustment.settlement_id}: vacaciones actualizadas de C$ {float(before['vacation_amount']):,.2f} a C$ {float(after['vacation_amount']):,.2f}."
            story.extend([para('Control de liquidación pendiente',heading),para(detail)])
        reversal=reversals.get(move.id)
        if reversal:story.extend([para('Anulación / reintegro',heading),para(reversal.reason),para('Registró: '+reversal.created_by+' | '+local_stamp(reversal.created_at)+' | Nicaragua',small)])
        if move.status=='ACTIVE':
            signatures=table([[para('________________________\nAutorizó / entregó'),para('________________________\nTrabajador / recibió')]], [doc.width/2]*2)
            story.extend([Spacer(1,12*mm),KeepTogether([signatures,Spacer(1,3*mm),para('Conservar junto con la autorización y el soporte de entrega.',small)])])
    else:
        active=[m for m in moves if m.status=='ACTIVE' and m.kind=='CASH']
        total=sum((m.amount for m in active),0)
        story.append(para(f'{len(moves)} movimientos | Pagos directos vigentes: C$ {total:,.2f}',heading))
        story.append(para('El total considera pagos directos vigentes. Los descansos y las vacaciones incluidas en liquidaciones se identifican por separado en el detalle.',small))
        data=[[para(label,header) for label in ['Fecha / folio','Trabajador','Movimiento / estado','Días','Valor C$']]]
        for move in moves:
            data.append([para(f'{move.effective_date:%d/%m/%Y}\nVAC-{move.id:06d}',small),para(move.employee.full_name),para(TYPES.get(move.kind,move.kind)+'\n'+('Vigente' if move.status=='ACTIVE' else 'Anulado')),para(f'{move.days:,.4f}'),para(f'{move.amount:,.2f}')])
        if not moves:data.append([para('Sin movimientos para el filtro seleccionado'),para('-'),para('-'),para('-'),para('-')])
        story.extend([Spacer(1,4*mm),table(data,[doc.width*.18,doc.width*.27,doc.width*.28,doc.width*.11,doc.width*.16],True)])
    def footer(canvas,document):
        canvas.saveState();canvas.setStrokeColor(colors.HexColor('#DCE5EE'));canvas.line(doc.leftMargin,13*mm,letter[0]-doc.rightMargin,13*mm)
        canvas.setFont('Helvetica',7);canvas.setFillColor(gray)
        canvas.drawString(doc.leftMargin,9*mm,'Generado '+generated.strftime('%d/%m/%Y %H:%M')+' | Hora de Nicaragua')
        canvas.drawRightString(letter[0]-doc.rightMargin,9*mm,f'Página {document.page}')
        canvas.restoreState()
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    return buffer.getvalue()
