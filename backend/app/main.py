from fastapi import FastAPI
from .routers import auth
from .database import Base, engine
from .database import SessionLocal
from .models.user import User
from .core.security import hash_password
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="ERP System Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir peticiones desde cualquier origen
    allow_credentials=True,
    allow_methods=["*"],  # Aceptar TODOS los métodos, incluyendo OPTIONS
    allow_headers=["*"],  # Aceptar todos los headers (Content-Type, Authorization, etc.)
)

Base.metadata.create_all(bind=engine)

app.include_router(auth.router)

@app.get("/")
def root():
    return {"message": "API ERP lista 🔥"}


def create_initial_admin():
    db = SessionLocal()
    admin_email = "admin@erp.com"

    existing = db.query(User).filter(User.email == admin_email).first()

    if not existing:
        admin = User(
            full_name="Administrador",
            email=admin_email,
            hashed_password=hash_password("1234"),
            is_active=True
        )
        db.add(admin)
        db.commit()
        db.refresh(admin)
        print(">>> Usuario admin creado (admin@erp.com / 1234)")
    else:
        print(">>> Usuario admin ya existe")

create_initial_admin()