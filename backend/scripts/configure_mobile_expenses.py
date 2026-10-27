"""Create only the assisted expense metadata table and permission in Holl Pacas.

Preview by default. --apply requires an exclusive JSON audit file.
No chart changes, financial entries, cash balances or inventory movements.
"""
import argparse
import json
import sys
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine,inspect,text
from sqlalchemy.engine import make_url
from app.config import get_active_company_key,get_active_database_url
from app.models.sales import MobileExpense
from app.models.user import Permission
from app.core.mobile_expenses import PERMISSION
from app.core.mobile_expense_schema import ensure_mobile_expense_columns


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true');parser.add_argument('--audit-file',type=Path)
    parser.add_argument('--local-only',action='store_true',help='Reject a non-local database host')
    args=parser.parse_args()
    if args.apply and not args.audit_file:parser.error('--apply requiere --audit-file')
    if get_active_company_key()!='hollywood_pacas':raise SystemExit('Solo hollywood_pacas')
    url=get_active_database_url()
    if args.local_only and make_url(url).host not in {'localhost','127.0.0.1'}:raise SystemExit('Se requiere una base local')
    with create_engine(url).begin() as conn:
        conn.execute(text('SELECT pg_advisory_xact_lock(83742027)'))
        exists=inspect(conn).has_table(MobileExpense.__tablename__)
        permission=conn.execute(text('SELECT id FROM permissions WHERE name=:name'),{'name':PERMISSION}).scalar()
        missing_columns=[]
        if exists:
            columns={c['name'] for c in inspect(conn).get_columns(MobileExpense.__tablename__)}
            missing_columns=sorted(set(MobileExpense.__table__.columns.keys())-columns)
            if set(missing_columns)-{'amount','currency','exchange_rate'}:raise SystemExit('Tabla existente incompatible; no se modificó')
        before={'table_exists':exists,'permission_exists':bool(permission),'missing_columns':missing_columns}
        if args.apply:
            with args.audit_file.open('x',encoding='utf-8') as audit:
                json.dump({'company':'hollywood_pacas','at':datetime.now(timezone.utc).isoformat(),'before':before,
                           'action':'create_mobile_expenses_table_and_permission_only'},audit,indent=2)
            MobileExpense.__table__.create(conn,checkfirst=True)
            ensure_mobile_expense_columns(conn)
            if not permission:conn.execute(Permission.__table__.insert().values(name=PERMISSION))
        print(json.dumps({'status':'applied' if args.apply else 'preview','before':before,'table':MobileExpense.__tablename__}))


if __name__=='__main__':main()
