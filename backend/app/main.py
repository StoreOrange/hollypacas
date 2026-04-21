import os
import re
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from .config import get_active_company_key
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
_DEFAULT_LOGO_URL = "/static/logo_hollywood.png"


def _default_branding() -> dict[str, str]:
    active_company = ""
    db_name = ""
    try:
        active_company = (get_active_company_key() or "").strip().lower()
    except Exception:
        active_company = (os.getenv("ACTIVE_COMPANY", "") or "").strip().lower()
    try:
        db_name = (urlparse(os.getenv("DATABASE_URL", "")).path or "").rsplit("/", 1)[-1].strip().lower()
    except Exception:
        db_name = ""
    shoes_mode = db_name == "bdzapatos" or "zapato" in db_name
    restaurant_mode = active_company == "barrera" or "barrera" in db_name
    if shoes_mode:
        return {
            "legal_name": "Miss Zapatos",
            "trade_name": "Miss Zapatos",
            "app_title": "ERP Miss Zapatos",
            "sidebar_subtitle": "ERP Zapateria",
            "website": "",
            "phone": "",
            "address": "",
            "email": "",
            "logo_url": "/static/logo_hollywood.png",
            "pos_logo_url": "/static/logo_hollywood.png",
            "favicon_url": "/static/favicon.ico",
            "inventory_cs_only": False,
            "theme_code": "default",
        }
    if restaurant_mode:
        return {
            "legal_name": "La Barrera Restaurante",
            "trade_name": "La Barrera",
            "app_title": "ERP La Barrera",
            "sidebar_subtitle": "Restaurante & Bar",
            "website": "",
            "phone": "",
            "address": "Sucursal principal",
            "email": "",
            "logo_url": "/static/logo_hollywood.png",
            "pos_logo_url": "/static/logo_hollywood.png",
            "favicon_url": "/static/favicon.ico",
            "inventory_cs_only": False,
            "theme_code": "default",
        }
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
        "theme_code": "default",
    }


def _apply_company_logo_fallback(branding: dict[str, str]) -> dict[str, str]:
    logo_url = (branding.get("logo_url") or "").strip()
    pos_logo_url = (branding.get("pos_logo_url") or "").strip()
    needs_logo_fallback = (not logo_url) or (logo_url == _DEFAULT_LOGO_URL)
    needs_pos_fallback = (not pos_logo_url) or (pos_logo_url == _DEFAULT_LOGO_URL)
    if not (needs_logo_fallback or needs_pos_fallback):
        return branding

    try:
        active_company = (get_active_company_key() or "").strip().lower() or "default"
    except Exception:
        active_company = (os.getenv("ACTIVE_COMPANY", "") or "").strip().lower() or "default"

    safe_company = re.sub(r"[^a-z0-9_-]+", "", active_company) or "default"
    static_dir = Path(__file__).resolve().parent / "static"
    company_dir = static_dir / "company_assets" / safe_company
    if not company_dir.exists():
        return branding

    logo_candidates = sorted(company_dir.glob("logo_*.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    pos_logo_candidates = sorted(company_dir.glob("pos_logo_*.*"), key=lambda p: p.stat().st_mtime, reverse=True)

    if needs_logo_fallback and logo_candidates:
        relative_logo = logo_candidates[0].relative_to(static_dir).as_posix()
        branding["logo_url"] = f"/static/{relative_logo}"
    if needs_pos_fallback:
        if pos_logo_candidates:
            relative_pos_logo = pos_logo_candidates[0].relative_to(static_dir).as_posix()
            branding["pos_logo_url"] = f"/static/{relative_pos_logo}"
        elif branding.get("logo_url"):
            branding["pos_logo_url"] = branding["logo_url"]
    return branding


@app.middleware("http")
async def attach_branding(request, call_next):
    branding = _default_branding()
    menu_links = web.SIDEBAR_MENU_ITEMS
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
                    "theme_code": (getattr(row, "theme_code", "") or branding.get("theme_code") or "default"),
                }
            )
        menu_links = web.get_sidebar_menu_layout(db)
    except SQLAlchemyError:
        pass
    except Exception:
        menu_links = web.SIDEBAR_MENU_ITEMS
    finally:
        if db is not None:
            db.close()

    branding = _apply_company_logo_fallback(branding)
    request.state.branding = branding
    request.state.menu_links = menu_links
    return await call_next(request)


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/")
def root():
    return {"message": "API ERP lista"}
