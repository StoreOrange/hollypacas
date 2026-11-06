"""Create only the new requisition tables; never create inventory movements."""
from app.database import get_engine
from app.config import get_active_company_key
from app.models.inventory_requests import InventoryRequest, InventoryRequestItem, InventoryRequestEvent
from app.routers import web
if get_active_company_key() != 'hollywood_pacas':
    raise SystemExit('Run only in the Pacas Holl instance')
with get_engine().begin() as conn:
    for model in (InventoryRequest,InventoryRequestItem,InventoryRequestEvent):
        model.__table__.create(bind=conn,checkfirst=True)
print('Requisition tables ready. No inventory changes.')
