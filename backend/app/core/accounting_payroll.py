"""Devengo y pago se contabilizan por separado, sin recalcular salarios."""
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import func, or_, text
from fastapi import HTTPException
from ..models.payroll import PayrollAccountingSetting, PayrollDeductionType, PayrollCalculation, PayrollPayment
from ..models.sales import (CuentaContable, AccountingJournal, AccountingEntry, AccountingEntryLine,
    AccountingBankMatch, AccountingReconciliation, ReciboCaja, ReciboRubro, ReciboMotivo, CajaDiaria, CierreCaja)


def amount(value):
    number = Decimal(str(value or 0))
    if not number.is_finite() or number < 0 or number != number.quantize(Decimal('.01')):
        raise ValueError('La planilla contiene un importe inválido o con más de dos decimales')
    return number.quantize(Decimal('.01'))


def lock(db):
    from ..routers import web
    web._lock_accounting_transaction(db)
    if db.get_bind().dialect.name == 'postgresql':
        db.execute(text('SELECT pg_advisory_xact_lock(83742021)'))


def account(db, ident):
    result = db.get(CuentaContable, ident) if ident else None
    if not result or not result.activo:
        raise ValueError('Configura una cuenta activa para salarios, obligación o deducción en Vinculación contable de planilla')
    return result


def settings(db):
    row = db.get(PayrollAccountingSetting, 1)
    if not row:
        expense = db.query(CuentaContable).filter_by(codigo='6208', activo=True, tipo='RESULTADO', naturaleza='DEBE').first()
        payable = db.query(CuentaContable).filter_by(codigo='2113', activo=True, tipo='BALANCE', naturaleza='HABER').first()
        if not expense or not payable:
            raise ValueError('Configura las cuentas de salarios y salarios por pagar')
        row = PayrollAccountingSetting(id=1, salary_expense_id=expense.id, salary_payable_id=payable.id)
        db.add(row);db.flush()
        receivable = db.query(CuentaContable).filter_by(codigo='1108', activo=True, tipo='BALANCE', naturaleza='DEBE').first()
        withheld = db.query(CuentaContable).filter_by(codigo='2110', activo=True, tipo='BALANCE', naturaleza='HABER').first()
        for dtype in db.query(PayrollDeductionType).all():
            if not dtype.accounting_account_id:
                mapped = receivable if dtype.category in {'LOAN','ADVANCE'} else withheld if dtype.category == 'LEGAL' else expense if dtype.category == 'ABSENCE' else None
                if mapped: dtype.accounting_account_id = mapped.id
        db.flush()
    return row


def decomposition(calculation):
    gross, deductions, net, late = map(amount, [calculation.gross_pay, calculation.total_deductions, calculation.net_pay, calculation.late_deduction])
    planned = sum((amount(line.amount) for line in calculation.deduction_lines), Decimal(0))
    manual = deductions - planned - late
    if net != gross - deductions or manual < 0:
        raise ValueError('La planilla no cuadra entre bruto, deducciones y neto. Revisa el cálculo antes de cerrar')
    return gross, net, late, manual


def posted_entry(db, period, entry_date, reference, description, lines, actor, voucher_code='DIARIO', journal=None, reversal_of=None):
    from ..routers import web
    web._assert_accounting_date_open(db, entry_date)
    voucher = web._find_voucher_type_for_code(db, voucher_code)
    if not voucher or not voucher.activo:
        raise ValueError('Habilita el tipo de comprobante '+voucher_code)
    total_debit = sum((line.debe or Decimal(0) for line in lines), Decimal(0))
    total_credit = sum((line.haber or Decimal(0) for line in lines), Decimal(0))
    sequence = web._next_accounting_sequence(db, voucher.id, period.branch_id, web._accounting_period(entry_date))
    result = AccountingEntry(tipo_id=voucher.id, journal_id=journal.id if journal else None,
        branch_id=period.branch_id, fecha=entry_date, periodo=web._accounting_period(entry_date), secuencia=sequence,
        numero=web._build_accounting_entry_number(voucher,web._accounting_period(entry_date),sequence),
        referencia=reference, descripcion=description[:400], estado='POSTEADO', total_debe=total_debit,total_haber=total_credit,
        creado_por=actor,publicado_por=actor,publicado_at=web.local_now_naive(),reversa_de_id=reversal_of.id if reversal_of else None,lines=lines)
    web.validate_postable_entry(result,allow_inactive=bool(reversal_of));db.add(result);db.flush();return result


def accrue(db, period, actor):
    if period.accounting_entry_id:
        return db.get(AccountingEntry,period.accounting_entry_id)
    if db.query(PayrollPayment.id).filter_by(period_id=period.id,status='PAID').first():
        raise ValueError('Planilla histórica pagada sin asiento. Revisa su regularización; no se reconoce otra vez automáticamente')
    if not period.branch_id: raise ValueError('La planilla debe pertenecer a una sucursal')
    config=settings(db);expense=account(db,config.salary_expense_id);payable=account(db,config.salary_payable_id)
    if expense.tipo!='RESULTADO' or expense.naturaleza!='DEBE' or payable.tipo!='BALANCE' or payable.naturaleza!='HABER' or expense.id==payable.id:
        raise ValueError('Revisa gasto salarial y obligación: deben ser cuentas distintas y de tipos correctos')
    calculations=db.query(PayrollCalculation).filter_by(period_id=period.id).order_by(PayrollCalculation.employee_id).all()
    if not calculations:raise ValueError('Calcula la planilla antes de cerrarla')
    lines=[]
    def add(acct,debit,credit,detail):
        if debit or credit:lines.append(AccountingEntryLine(cuenta=acct,debe=debit,haber=credit,descripcion=detail[:200]))
    for calc in calculations:
        gross,net,late,manual=decomposition(calc);name=calc.employee.full_name
        add(expense,gross,0,f'{name} · salario bruto')
        add(payable,0,net,f'{name} · neto por pagar')
        add(expense,0,late,f'{name} · tiempo no remunerado')
        if manual:add(account(db,config.manual_deduction_id),0,manual,f'{name} · deducciones manuales')
        for line in calc.deduction_lines:
            if amount(line.amount):
                dtype=line.employee_deduction.deduction_type
                if not dtype.accounting_account_id:raise ValueError('Configura la cuenta de deducción '+dtype.name)
                add(account(db,dtype.accounting_account_id),0,amount(line.amount),f'{name} · {dtype.name}')
    if not lines:return None
    journal=db.query(AccountingJournal).filter_by(code='VAR',active=True).first()
    entry=posted_entry(db,period,period.date_to,f'AUTO-PLAN-DEV-{period.id}',f'Devengo de planilla {period.code}',lines,actor,journal=journal)
    period.accounting_entry_id=entry.id;period.accounting_payable_id=payable.id;db.flush()
    from .payroll_benefits import release_rest
    release_rest(db,period,actor)
    return entry


def payment_journal(db, period, method, ident):
    kind='CASH' if method=='EFECTIVO' else 'BANK'
    available=db.query(AccountingJournal).filter(AccountingJournal.kind==kind,AccountingJournal.active.is_(True),or_(AccountingJournal.branch_id==period.branch_id,AccountingJournal.branch_id.is_(None))).all()
    chosen=next((item for item in available if item.id==int(ident)),None) if ident else None
    if not chosen:
        preferred=[item for item in available if item.branch_id==period.branch_id] or available
        if ident or len(preferred)!=1:raise ValueError('Selecciona el diario de caja o banco desde donde se pagará')
        chosen=preferred[0]
    source=account(db,chosen.main_account_id)
    if source.tipo!='BALANCE' or source.naturaleza!='DEBE':raise ValueError('El diario requiere una cuenta de caja o banco válida')
    return chosen,source


def cash_movement(db, payment, entry, warehouse, entry_date, kind, source_account_id, actor):
    from ..routers import web
    if not warehouse or warehouse.branch_id!=payment.branch_id:raise ValueError('Selecciona la bodega de caja de la misma sucursal de la planilla')
    if db.get_bind().dialect.name=='postgresql':db.execute(text('SELECT pg_advisory_xact_lock(83742020)'))
    if db.query(CierreCaja.id).filter_by(bodega_id=warehouse.id,fecha=entry_date).first():raise ValueError('La caja de esa fecha está cerrada; no se puede mover el pago')
    payable=account(db,payment.period.accounting_payable_id)
    name='Pago de planilla · '+payable.codigo
    rubro=db.query(ReciboRubro).filter_by(nombre=name).first()
    if not rubro:rubro=ReciboRubro(nombre=name,tipo='AMBOS',cuenta_id=payable.id,activo=True);db.add(rubro);db.flush()
    motive_name='Pago de planilla integrado' if kind=='EGRESO' else 'Reintegro de planilla anulado'
    motive=db.query(ReciboMotivo).filter_by(nombre=motive_name,tipo=kind,activo=True).first()
    if not motive:motive=ReciboMotivo(nombre=motive_name,tipo=kind,activo=True);db.add(motive);db.flush()
    sequence=int(db.query(func.max(ReciboCaja.secuencia)).filter(ReciboCaja.numero.like('ROC-P-%')).scalar() or 0)+1
    rate=db.query(web.ExchangeRate).filter(web.ExchangeRate.effective_date<=web.local_today()).order_by(web.ExchangeRate.effective_date.desc()).first()
    exchange=Decimal(str(rate.rate)) if rate else Decimal(0)
    cs=amount(payment.amount);usd=(cs/exchange).quantize(Decimal('.01'),rounding=ROUND_HALF_UP) if exchange>0 else Decimal(0)
    receipt=ReciboCaja(numero=f'ROC-P-{sequence:07d}',secuencia=sequence,branch_id=payment.branch_id,bodega_id=warehouse.id,
        tipo=kind,rubro_id=rubro.id,motivo_id=motive.id,descripcion=f'{payment.period.code} · '+('Pago de planilla' if kind=='EGRESO' else 'Reintegro por anulación de pago'),
        fecha=entry_date,moneda='CS',monto_cs=cs,monto_usd=usd,afecta_caja=True,usuario_registro=actor,accounting_entry_id=entry.id)
    db.add(receipt);db.flush()
    box=db.query(CajaDiaria).filter_by(branch_id=payment.branch_id,bodega_id=warehouse.id,fecha=entry_date).with_for_update().first()
    if not box:box=CajaDiaria(branch_id=payment.branch_id,bodega_id=warehouse.id,fecha=entry_date,saldo_cs=0,saldo_usd=0);db.add(box)
    sign=1 if kind=='INGRESO' else -1
    box.saldo_cs=Decimal(str(box.saldo_cs or 0))+sign*cs;box.saldo_usd=Decimal(str(box.saldo_usd or 0))+sign*usd
    return receipt


def pay(db, payment, actor, journal_id, warehouse=None):
    if payment.accounting_entry_id:
        existing=db.get(AccountingEntry,payment.accounting_entry_id)
        if existing and existing.estado=='POSTEADO':return existing
        raise ValueError('Este pago ya fue contabilizado y revertido; utiliza un nuevo pago si corresponde')
    period=payment.period
    if not period.accounting_entry_id:
        previous=db.query(PayrollPayment.id).filter(PayrollPayment.period_id==period.id,PayrollPayment.status=='PAID',PayrollPayment.id!=payment.id).first()
        if previous:raise ValueError('La planilla ya tiene otro pago activo')
        old_status=payment.status;payment.status='PENDING';db.flush()
        accrue(db,period,actor)
        payment.status=old_status;db.flush()
    if not period.accounting_entry_id:raise ValueError('Esta planilla no tiene importe devengado para pagar')
    payable=account(db,period.accounting_payable_id)
    journal,source=payment_journal(db,period,payment.payment_method,journal_id)
    if source.id==payable.id:raise ValueError('Caja/banco y salarios por pagar deben ser distintos')
    value=amount(payment.amount)
    if not value:raise ValueError('El neto de la planilla debe ser mayor que cero para registrar un pago')
    entry=posted_entry(db,period,payment.payment_date,f'AUTO-PLAN-PAY-{payment.id}',f'Pago de planilla {period.code}',[
        AccountingEntryLine(cuenta=payable,debe=value,haber=0,descripcion='Cancela neto por pagar'),
        AccountingEntryLine(cuenta=source,debe=0,haber=value,descripcion='Pago de planilla')],actor,'EGRESO',journal)
    payment.accounting_entry_id=entry.id
    if payment.payment_method=='EFECTIVO':payment.cash_receipt_id=cash_movement(db,payment,entry,warehouse,payment.payment_date,'EGRESO',source.id,actor).id
    return entry


def void(db,payment,actor,reason,entry_date):
    from ..routers import web
    entry=db.get(AccountingEntry,payment.accounting_entry_id) if payment.accounting_entry_id else None
    if not entry:raise ValueError('Pago histórico sin asiento vinculado. Revisa su regularización contable antes de anularlo')
    if payment.reversal_entry_id or entry.estado!='POSTEADO':raise ValueError('Este pago ya tiene reversión o no está publicado')
    if entry_date<entry.fecha:raise ValueError('La anulación no puede ser anterior al pago')
    ids=[line.id for line in entry.lines]
    if db.query(AccountingBankMatch.id).filter(AccountingBankMatch.canceled_at.is_(None),AccountingBankMatch.line_id.in_(ids)).first() or db.query(AccountingReconciliation.id).filter(AccountingReconciliation.canceled_at.is_(None),or_(AccountingReconciliation.debit_line_id.in_(ids),AccountingReconciliation.credit_line_id.in_(ids))).first():raise ValueError('Deshaz la conciliación de ese pago antes de anularlo')
    lines=[AccountingEntryLine(cuenta=line.cuenta,debe=line.haber,haber=line.debe,descripcion=('Reversión · '+reason)[:200]) for line in entry.lines]
    journal=db.get(AccountingJournal,entry.journal_id)
    reversal=posted_entry(db,payment.period,entry_date,f'AUTO-PLAN-VOID-{payment.id}',f'Anulación de pago {payment.period.code}: {reason}',lines,actor,'DIARIO',journal,entry)
    payment.reversal_entry_id=reversal.id;entry.estado='REVERSADO'
    if payment.cash_receipt_id:
        original=db.get(ReciboCaja,payment.cash_receipt_id)
        payment.refund_receipt_id=cash_movement(db,payment,reversal,original.bodega,entry_date,'INGRESO',entry.lines[1].cuenta_id,actor).id
    return reversal
