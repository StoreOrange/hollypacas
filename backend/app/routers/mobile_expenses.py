"""Simple owner/manager expenses, restricted to Holl Pacas and explicit permission."""
import io
import uuid
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from ..routers import web as w
from ..models.sales import MobileExpense
from ..core.mobile_expenses import CATEGORIES, CATEGORY_MAP, PERMISSION, payment_choices, expense_account, register_expense
from ..core.accounting_entry_pdf import build_accounting_entry_pdf

router=APIRouter()


def enforce(request,user):
    if not w._is_pacasholl_company():raise HTTPException(404,'No disponible')
    w._enforce_permission(request,user,PERMISSION)


def expense_query(db,user):
    return db.query(MobileExpense).filter(MobileExpense.branch_id.in_(w._user_scoped_branch_ids(db,user)))


def render(request,db,user,values=None,error='',status=200):
    scopes=w._user_scoped_branch_ids(db,user)
    branches=db.query(w.Branch).filter(w.Branch.id.in_(scopes)).order_by(w.Branch.name).all()
    warehouses=db.query(w.Bodega).filter(w.Bodega.branch_id.in_(scopes),w.Bodega.activo.is_(True)).order_by(w.Bodega.name).all()
    branch,warehouse=w._resolve_branch_bodega(db,user)
    form={'operation_key':uuid.uuid4().hex,'fecha':w.local_today().isoformat(),'branch_id':str(branch.id) if branch and branch.id in scopes else '',
          'bodega_id':str(warehouse.id) if warehouse and warehouse.branch_id in scopes else '', 'moneda':'CS','paid_by':user.full_name or user.email}
    form.update(dict(values or {}))
    categories=[]
    for key,label,code,hint in CATEGORIES:
        try:
            account=expense_account(db,key)
            categories.append({'value':key,'label':label,'hint':hint,'account':account.codigo+' · '+account.nombre})
        except ValueError:pass
    recent=expense_query(db,user).options(joinedload(MobileExpense.entry),joinedload(MobileExpense.branch)).order_by(MobileExpense.id.desc()).limit(20).all()
    saved=None
    raw=request.query_params.get('saved','')
    if raw.isdigit():saved=expense_query(db,user).filter(MobileExpense.id==int(raw)).first()
    return request.app.state.templates.TemplateResponse('mobile_expenses.html',{
        'request':request,'user':user,'form':form,'error':error,'categories':categories,'payments':payment_choices(db,scopes),
        'branches':branches,'warehouses':warehouses,'recent_expenses':recent,'saved_expense':saved,'today':w.local_today().isoformat(),
        'voucher_modal_description':'Recibo PDF del gasto registrado. Puedes imprimirlo o abrir el archivo.',
        'voucher_modal_title':'Recibo de gasto · tamaño carta','version':w.settings.UI_VERSION,
    },status_code=status)


@router.get('/m/gastos')
def page(request:Request,db=Depends(w.get_db),user=Depends(w._require_user_web)):
    enforce(request,user)
    return render(request,db,user)


@router.post('/m/gastos')
async def create(request:Request,db=Depends(w.get_db),user=Depends(w._require_user_web)):
    enforce(request,user)
    values=dict(await request.form())
    try:
        expense=register_expense(db,user,values)
        db.commit()
    except (ValueError,HTTPException) as exc:
        db.rollback()
        return render(request,db,user,values,str(exc.detail if isinstance(exc,HTTPException) else exc),400)
    except IntegrityError:
        db.rollback()
        existing=expense_query(db,user).filter(MobileExpense.operation_key==values.get('operation_key'),MobileExpense.user_id==user.id).first()
        if existing:return RedirectResponse('/m/gastos?saved='+str(existing.id),status_code=303)
        return render(request,db,user,values,'No se pudo guardar. Recarga y revisa si el gasto ya aparece registrado.',409)
    return RedirectResponse('/m/gastos?saved='+str(expense.id),status_code=303)


@router.get('/m/gastos/{expense_id}/pdf')
def pdf(request:Request,expense_id:int,db=Depends(w.get_db),user=Depends(w._require_user_web)):
    enforce(request,user)
    expense=expense_query(db,user).filter(MobileExpense.id==expense_id).first()
    if not expense:raise HTTPException(404,'Gasto no encontrado')
    profile=w._company_profile_payload(db)
    zone=db.execute(w.sql_text('SHOW TIMEZONE')).scalar_one() if db.get_bind().dialect.name=='postgresql' else 'UTC'
    details=[('Beneficiario',expense.beneficiary),('Monto pagado',f'{"USD" if expense.currency=="USD" else "C$"} {expense.amount:,.2f}'),('Tipo de gasto',CATEGORY_MAP.get(expense.category,('',expense.category))[1]),
             ('Forma de pago',{'cash':'Caja del negocio','bank':'Banco del negocio','personal':'Pagado personalmente; pendiente de reembolso'}[expense.payment]),
             ('Factura / soporte',expense.support_reference or 'Sin factura / soporte pendiente'),('Comprobante contable',expense.entry.numero)]
    if expense.paid_by:details.append(('Pagado por',expense.paid_by))
    if expense.cash_receipt:details.append(('Recibo de caja',expense.cash_receipt.numero))
    if expense.bodega:details.append(('Caja / bodega',expense.bodega.name))
    details.append(('Documento','Recibo interno de gasto. Conservar el soporte original.'))
    body=build_accounting_entry_pdf(expense.entry,profile,w._resolve_logo_path(profile.get('logo_url','')),zone,
        document_title='RECIBO DE GASTO',document_number=expense.numero,receipt_details=details)
    return StreamingResponse(io.BytesIO(body),media_type='application/pdf',headers={'Content-Disposition':f'inline; filename={expense.numero}.pdf'})
