"""Database engine and session configuration."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)


def _ensure_mysql_charset(database_url: str) -> str:
    if not database_url.startswith("mysql"):
        return database_url

    parts = urlsplit(database_url)
    query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
    if (query_items.get("charset") or "").strip().lower() != "utf8mb4":
        query_items["charset"] = "utf8mb4"
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query_items),
            parts.fragment,
        )
    )


def _resolve_database_url() -> tuple[str, str]:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if database_url:
        backend = "mysql" if database_url.startswith("mysql") else "sqlite"
        return _ensure_mysql_charset(database_url), backend

    db_host = (os.getenv("DB_HOST") or "").strip()
    db_port = (os.getenv("DB_PORT") or "3306").strip()
    db_user = (os.getenv("DB_USER") or "").strip()
    db_password = (os.getenv("DB_PASSWORD") or "").strip()
    db_name = (os.getenv("DB_NAME") or "").strip()

    if all([db_host, db_user, db_password, db_name]):
        url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
        return url, "mysql"

    db_path = (os.getenv("LOCAL_DB_PATH") or "data/customers.db").strip()
    if not Path(db_path).is_absolute():
        db_path = str((Path(__file__).resolve().parent.parent / db_path).resolve())
    return f"sqlite:///{db_path}", "sqlite"


SQLALCHEMY_DATABASE_URL, DB_BACKEND = _resolve_database_url()

engine_kwargs: dict[str, object] = {
    "pool_pre_ping": True,
    "future": True,
}

if DB_BACKEND == "sqlite":
    engine_kwargs["connect_args"] = {"check_same_thread": False}
elif DB_BACKEND == "mysql":
    engine_kwargs["connect_args"] = {
        "charset": "utf8mb4",
        "init_command": "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci",
    }

if (os.getenv("SQL_ECHO") or "").lower() == "true":
    engine_kwargs["echo"] = True

engine = create_engine(SQLALCHEMY_DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
