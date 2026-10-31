"""Start the independent Holl panel once; preserve and audit the financial ledger.
Run from backend with PYTHONPATH=. after taking a database backup.
"""
import argparse
import json
import os
from datetime import date
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.main import app
from app.database import get_engine
from app.config import get_active_company_key
from app.core import collection_carryover as tracking
from app.models.sales import CollectionTrackingEpoch, CollectionCarryover, CollectionPanelPayment
from app.routers import web as w


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date',type=date.fromisoformat,required=True)
    parser.add_argument('--audit-file',required=True)
    args=parser.parse_args()
    os.umask(0o077)
    if get_active_company_key() not in {'hollywood_pacas','pacasholl','hollpacas'}:
        raise RuntimeError('Esta operación solo corresponde a Holl Pacas')
    if args.date!=w.local_today():raise RuntimeError('El inicio debe corresponder a hoy')
    target=Path(args.audit_file)
    if target.exists():raise RuntimeError('El archivo de auditoría ya existe')
    target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    engine=get_engine();engine.echo=False
    for model in (CollectionTrackingEpoch,CollectionCarryover,CollectionPanelPayment):
        model.__table__.create(engine,checkfirst=True)
    with engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
        with Session(bind=connection) as db:
            def financial_state():
                return {table:dict(db.execute(text(
                    "SELECT count(*) AS count, md5(coalesce(string_agg(to_jsonb(t)::text, '' ORDER BY id),'')) AS digest FROM "+table+" t"
                )).mappings().one()) for table in ('ventas_facturas','ventas_pagos','cobranza_abonos','mobile_collection_payments')}
            before=financial_state()
            admin_id=db.execute(text("SELECT u.id FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id WHERE u.is_active AND r.name='administrador' ORDER BY u.id LIMIT 1")).scalar_one()
            epoch,created=tracking.initialize(db,db.get(w.User,admin_id),args.date)
            db.flush()
            after=financial_state()
            if before!=after:raise RuntimeError('El registro financiero cambió; operación revertida')
            result=dict(start_date=epoch.start_date,created=[dict(invoice=r.factura.numero,opening_usd=r.opening_usd,opening_cs=r.opening_cs,eligible_on=r.eligible_on,cierre_id=r.cierre_id) for r in created],financial_before=before,financial_after=after,panel_payments=db.query(CollectionPanelPayment).count())
            db.commit()
    target.write_text(json.dumps(result,default=str,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result.items() if not k.startswith('financial')},default=str,ensure_ascii=False))


if __name__=='__main__':main()
