"""Create evidence table only. Does not pay vacations or change draft settlements."""
from app.config import get_active_company_key
from app.database import get_engine
from app.routers import web
from app.models.payroll import PayrollVacationDraftAdjustment
if get_active_company_key()!='hollywood_pacas':raise SystemExit('Pacas Holl only')
with get_engine().begin() as connection:PayrollVacationDraftAdjustment.__table__.create(bind=connection,checkfirst=True)
print('Vacation draft-adjustment audit ready; no payments executed.')
