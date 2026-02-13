import os
import re
from pathlib import Path
from typing import Any

from dotenv import dotenv_values, load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
COMPANIES_ENV_PATH = Path(__file__).resolve().parents[1] / ".env.companies"
FALLBACK_ENV_PATHS = [
    ENV_PATH,
    Path.cwd() / ".env",
    Path.cwd() / "backend" / ".env",
]
for env_path in FALLBACK_ENV_PATHS:
    load_dotenv(env_path, override=True)


def _env_get(values: dict[str, Any], key: str) -> str:
    return str(values.get(key) or values.get(f"\ufeff{key}") or "").strip()


def _normalize_company_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", (value or "").strip().lower()).strip("_")
    return normalized


def _to_company_env_key(value: str) -> str:
    return _normalize_company_key(value).upper()


def _split_company_keys(raw: str) -> list[str]:
    keys: list[str] = []
    for item in (raw or "").split(","):
        key = _normalize_company_key(item)
        if key and key not in keys:
            keys.append(key)
    return keys


def _update_env_file(path: Path, updates: dict[str, str]) -> None:
    updates_norm = {str(k).strip(): str(v).strip() for k, v in updates.items() if str(k).strip()}
    if not updates_norm:
        return

    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    pending = dict(updates_norm)
    output: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            output.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in pending:
            output.append(f"{key}={pending.pop(key)}")
        else:
            output.append(line)

    if output and output[-1].strip():
        output.append("")

    for key, value in pending.items():
        output.append(f"{key}={value}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")


def _bootstrap_companies_env_if_missing() -> None:
    if COMPANIES_ENV_PATH.exists():
        return
    initial_key = _normalize_company_key(os.getenv("ACTIVE_COMPANY", "hollywood_pacas")) or "hollywood_pacas"
    initial_name = os.getenv("PROJECT_NAME", "Hollywood Pacas ERP").strip() or "Hollywood Pacas ERP"
    initial_db_url = os.getenv("DATABASE_URL", "postgresql://user:1234@localhost:5432/hollpacas").strip()
    env_key = _to_company_env_key(initial_key)
    content = [
        "# Perfiles de empresa / base de datos",
        "# Agrega una clave por empresa y su DATABASE_URL correspondiente.",
        f"ACTIVE_COMPANY={initial_key}",
        f"COMPANY_KEYS={initial_key}",
        f"COMPANY_{env_key}_NAME={initial_name}",
        f"COMPANY_{env_key}_DATABASE_URL={initial_db_url}",
        "",
    ]
    COMPANIES_ENV_PATH.write_text("\n".join(content), encoding="utf-8")


def get_company_profiles() -> list[dict[str, str]]:
    _bootstrap_companies_env_if_missing()
    values = dotenv_values(COMPANIES_ENV_PATH)
    keys = _split_company_keys(_env_get(values, "COMPANY_KEYS"))
    active_key = _normalize_company_key(_env_get(values, "ACTIVE_COMPANY") or os.getenv("ACTIVE_COMPANY", ""))

    profiles: list[dict[str, str]] = []
    for key in keys:
        env_key = _to_company_env_key(key)
        name = _env_get(values, f"COMPANY_{env_key}_NAME") or key
        database_url = _env_get(values, f"COMPANY_{env_key}_DATABASE_URL")
        if not database_url:
            continue
        profiles.append({"key": key, "name": name, "database_url": database_url})

    # Fallback: si el archivo no estaba poblado, usar DATABASE_URL actual.
    if not profiles:
        fallback_key = active_key or "hollywood_pacas"
        profiles.append(
            {
                "key": fallback_key,
                "name": os.getenv("PROJECT_NAME", "ERP System"),
                "database_url": os.getenv(
                    "DATABASE_URL",
                    "postgresql://user:1234@localhost:5432/hollpacas",
                ),
            }
        )

    return profiles


def get_active_company_key() -> str:
    values = dotenv_values(COMPANIES_ENV_PATH) if COMPANIES_ENV_PATH.exists() else {}
    key = _normalize_company_key(_env_get(values, "ACTIVE_COMPANY") or os.getenv("ACTIVE_COMPANY", ""))
    if key:
        return key
    profiles = get_company_profiles()
    return profiles[0]["key"] if profiles else "hollywood_pacas"


def get_active_company_profile() -> dict[str, str]:
    profiles = get_company_profiles()
    active_key = get_active_company_key()
    for profile in profiles:
        if profile["key"] == active_key:
            return profile
    return profiles[0] if profiles else {"key": "hollywood_pacas", "name": "ERP System", "database_url": ""}


def get_active_database_url() -> str:
    profile = get_active_company_profile()
    db_url = (profile.get("database_url") or "").strip()
    if db_url:
        return db_url
    return os.getenv("DATABASE_URL", "postgresql://user:1234@localhost:5432/hollpacas")


def upsert_company_profile(*, key: str, name: str, database_url: str, activate: bool = False) -> dict[str, str]:
    normalized_key = _normalize_company_key(key)
    if not normalized_key:
        raise ValueError("Clave de empresa invalida")

    profile_name = (name or "").strip() or normalized_key
    profile_db_url = (database_url or "").strip()
    if not profile_db_url:
        raise ValueError("DATABASE_URL requerida")

    current = dotenv_values(COMPANIES_ENV_PATH) if COMPANIES_ENV_PATH.exists() else {}
    keys = _split_company_keys(_env_get(current, "COMPANY_KEYS"))
    if normalized_key not in keys:
        keys.append(normalized_key)

    env_key = _to_company_env_key(normalized_key)
    updates = {
        "COMPANY_KEYS": ",".join(keys),
        f"COMPANY_{env_key}_NAME": profile_name,
        f"COMPANY_{env_key}_DATABASE_URL": profile_db_url,
    }
    if activate:
        updates["ACTIVE_COMPANY"] = normalized_key
    _update_env_file(COMPANIES_ENV_PATH, updates)

    if activate:
        set_active_company(normalized_key)

    return {"key": normalized_key, "name": profile_name, "database_url": profile_db_url}


def set_active_company(company_key: str) -> dict[str, str]:
    normalized_key = _normalize_company_key(company_key)
    if not normalized_key:
        raise ValueError("Empresa invalida")

    profiles = get_company_profiles()
    profile = next((p for p in profiles if p["key"] == normalized_key), None)
    if not profile:
        raise ValueError("Empresa no registrada")

    _update_env_file(COMPANIES_ENV_PATH, {"ACTIVE_COMPANY": normalized_key})
    _update_env_file(
        ENV_PATH,
        {
            "ACTIVE_COMPANY": normalized_key,
            "DATABASE_URL": profile["database_url"],
        },
    )
    os.environ["ACTIVE_COMPANY"] = normalized_key
    os.environ["DATABASE_URL"] = profile["database_url"]
    return profile


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
