"""Add only the date-correction audit table; employee data stays unchanged."""
from app.config import get_active_company_key
from app.database import get_engine
from app.routers import web
from app.models.payroll import PayrollStartDateChange
if get_active_company_key()!='hollywood_pacas':raise SystemExit('Run only in Pacas Holl')
with get_engine().begin() as conn:PayrollStartDateChange.__table__.create(bind=conn,checkfirst=True)
print('Employment-date audit table ready; no employee dates changed.')
