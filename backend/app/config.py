import os
from pathlib import Path

from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
FALLBACK_ENV_PATHS = [
    ENV_PATH,
    Path.cwd() / ".env",
    Path.cwd() / "backend" / ".env",
]
for env_path in FALLBACK_ENV_PATHS:
    load_dotenv(env_path, override=True)


class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "ERP System")
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://user:1234@localhost:5432/hollpacas",
    )
    ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "admin")
    ADMIN_EMAIL: str = os.getenv("ADMIN_EMAIL", "admin@hollywoodpacas.com")
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "020416")
    ADMIN_FULL_NAME: str = os.getenv("ADMIN_FULL_NAME", "Administrador")
    UI_VERSION: str = os.getenv("UI_VERSION", "02.009.2026")
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.zoho.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMS_WEBHOOK_URL: str = os.getenv("SMS_WEBHOOK_URL", "")
    SMS_WEBHOOK_TOKEN: str = os.getenv("SMS_WEBHOOK_TOKEN", "")
    SMS_ALERT_RECIPIENTS: str = os.getenv("SMS_ALERT_RECIPIENTS", "")


settings = Settings()
