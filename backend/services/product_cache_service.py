"""Database-backed product cache service."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from backend.database import Base, SessionLocal, engine
from backend.db_models import ProductCacheEntry

logger = logging.getLogger(__name__)

_TABLES_READY = False


def _ensure_tables() -> None:
    global _TABLES_READY
    if _TABLES_READY:
        return
    Base.metadata.create_all(bind=engine, tables=[ProductCacheEntry.__table__], checkfirst=True)
    _TABLES_READY = True

def get_cache_map() -> dict[str, str]:
    _ensure_tables()
    with SessionLocal() as db:
        rows = db.execute(select(ProductCacheEntry)).scalars().all()
        result: dict[str, str] = {"enterprise_credit": "", "enterprise_mortgage": "", "personal": "", "lastUpdated": ""}
        last_updated = ""
        for row in rows:
            result[row.cache_key] = row.content or ""
            if row.last_updated and row.last_updated > last_updated:
                last_updated = row.last_updated
        result["enterprise"] = result.get("enterprise_credit") or ""
        result["lastUpdated"] = last_updated
        return result


def get_cache_content(cache_key: str) -> str:
    cache_map = get_cache_map()
    if cache_key == "enterprise":
        cache_key = "enterprise_credit"
    return cache_map.get(cache_key) or ""


def save_cache_map(enterprise: str, personal: str, enterprise_mortgage: str | None = None) -> str:
    _ensure_tables()
    last_updated = datetime.now(tz=timezone.utc).isoformat()
    payload = {
        "enterprise_credit": enterprise or "",
        "personal": personal or "",
        "enterprise_mortgage": enterprise_mortgage or "",
    }
    with SessionLocal() as db:
        for cache_key, content in payload.items():
            row = db.execute(select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_key)).scalar_one_or_none()
            if row:
                row.content = content
                row.last_updated = last_updated
                row.source = "wiki_refresh"
            elif content:
                db.add(
                    ProductCacheEntry(
                        cache_key=cache_key,
                        content=content,
                        last_updated=last_updated,
                        source="wiki_refresh",
                    )
                )
        db.commit()
    return last_updated


def save_cache_entry(cache_key: str, content: str, source: str = "wiki_refresh") -> str:
    _ensure_tables()
    last_updated = datetime.now(tz=timezone.utc).isoformat()
    if cache_key == "enterprise":
        cache_key = "enterprise_credit"
    with SessionLocal() as db:
        row = db.execute(select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_key)).scalar_one_or_none()
        if row:
            row.content = content or ""
            row.last_updated = last_updated
            row.source = source
        else:
            db.add(
                ProductCacheEntry(
                    cache_key=cache_key,
                    content=content or "",
                    last_updated=last_updated,
                    source=source,
                )
            )
        db.commit()
    return last_updated
