from threading import Lock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from .config import get_active_database_url

Base = declarative_base()

_engine_lock = Lock()
_engine = None
_session_local = None
_current_database_url = ""


def _build_engine(database_url: str):
    return create_engine(database_url, echo=True, pool_pre_ping=True)


def refresh_engine(force: bool = False):
    global _engine, _session_local, _current_database_url

    target_database_url = get_active_database_url()
    with _engine_lock:
        if not force and _engine is not None and target_database_url == _current_database_url:
            return _engine

        if _engine is not None:
            _engine.dispose()

        _engine = _build_engine(target_database_url)
        _session_local = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
        _current_database_url = target_database_url
        return _engine


def get_engine():
    return refresh_engine(force=False)


def get_session_local():
    refresh_engine(force=False)
    return _session_local


def get_current_database_url() -> str:
    refresh_engine(force=False)
    return _current_database_url


refresh_engine(force=True)
