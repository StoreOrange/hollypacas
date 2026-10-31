"""Recover a pre-cutover mobile payment only when its invoice crossed an executed close."""
import argparse,json,os
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.main import app
from app.database import get_engine
from app.core.collection_carryover import recover_legacy_payment,balance
from app.routers import web as w


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payment-id',type=int,required=True)
    parser.add_argument('--audit-file',required=True)
    args=parser.parse_args();os.umask(0o077)
    target=Path(args.audit_file)
    if target.exists():raise RuntimeError('Ya existe esa auditoría')
    target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    engine=get_engine();engine.echo=False
    with engine.connect().execution_options(isolation_level='REPEATABLE READ') as c:
        with Session(bind=c) as db:
            def state():
                return {table:dict(db.execute(text("SELECT count(*) AS count,md5(coalesce(string_agg(to_jsonb(t)::text,'' ORDER BY id),'')) AS digest FROM "+table+" t")).mappings().one()) for table in ('ventas_facturas','ventas_pagos','cobranza_abonos','mobile_collection_payments')}
            before=state()
            admin_id=db.execute(text("SELECT u.id FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id WHERE u.is_active AND r.name='administrador' ORDER BY u.id LIMIT 1")).scalar_one()
            payment=recover_legacy_payment(db,db.get(w.User,admin_id),args.payment_id)
            db.flush();db.expire(payment.carryover,['payments'])
            result=dict(source_payment=args.payment_id,panel_payment=payment.id,invoice=payment.factura.numero,
                paid_cs=payment.monto_cs,paid_usd=payment.monto_usd,due=balance(payment.carryover)['due'],
                cierre_id=payment.carryover.cierre_id,has_support=bool(payment.support),financial_before=before,financial_after=state())
            if result['financial_before']!=result['financial_after']:raise RuntimeError('El registro financiero cambió; operación revertida')
            db.commit()
    target.write_text(json.dumps(result,default=str,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result.items() if not k.startswith('financial')},default=str))


if __name__=='__main__':main()
