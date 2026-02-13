from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from .core.init_db import init_db
from .database import get_session_local
from .models.sales import CompanyProfileSetting
from .routers import auth, inventory, web


app = FastAPI(title="ERP System Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(inventory.router)
app.include_router(web.router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.state.templates = Jinja2Templates(directory="app/templates")


def _default_branding() -> dict[str, str]:
    return {
        "legal_name": "Hollywood Pacas",
        "trade_name": "Hollywood Pacas",
        "app_title": "ERP Hollywood Pacas",
        "sidebar_subtitle": "ERP Central",
        "website": "http://hollywoodpacas.com.ni",
        "phone": "8900-0300",
        "address": "",
        "email": "admin@hollywoodpacas.com",
        "logo_url": "/static/logo_hollywood.png",
        "pos_logo_url": "/static/logo_hollywood.png",
        "favicon_url": "/static/favicon.ico",
        "inventory_cs_only": False,
    }


@app.middleware("http")
async def attach_branding(request, call_next):
    branding = _default_branding()
    db = None
    try:
        db = get_session_local()()
        row = db.query(CompanyProfileSetting).order_by(CompanyProfileSetting.id.asc()).first()
        if row:
            branding.update(
                {
                    "legal_name": row.legal_name or branding["legal_name"],
                    "trade_name": row.trade_name or branding["trade_name"],
                    "app_title": row.app_title or branding["app_title"],
                    "sidebar_subtitle": row.sidebar_subtitle or branding["sidebar_subtitle"],
                    "website": row.website or branding["website"],
                    "phone": row.phone or branding["phone"],
                    "address": row.address or branding["address"],
                    "email": row.email or branding["email"],
                    "logo_url": row.logo_url or branding["logo_url"],
                    "pos_logo_url": row.pos_logo_url or branding["pos_logo_url"],
                    "favicon_url": row.favicon_url or branding["favicon_url"],
                    "inventory_cs_only": bool(row.inventory_cs_only),
                }
            )
    except SQLAlchemyError:
        pass
    finally:
        if db is not None:
            db.close()

    request.state.branding = branding
    return await call_next(request)


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/")
def root():
    return {"message": "API ERP lista"}
