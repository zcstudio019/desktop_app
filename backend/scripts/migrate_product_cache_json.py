"""One-off migration script for legacy product_cache.json -> product_cache_entries."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from backend.database import Base, SessionLocal, engine
from backend.db_models import ProductCacheEntry

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
PRODUCT_CACHE_FILE = DATA_DIR / "product_cache.json"


def main() -> None:
    Base.metadata.create_all(bind=engine, tables=[ProductCacheEntry.__table__], checkfirst=True)
    if not PRODUCT_CACHE_FILE.exists():
        print("legacy product_cache.json not found, skipped")
        return

    with open(PRODUCT_CACHE_FILE, encoding="utf-8") as f:
        data = json.load(f)

    last_updated = data.get("lastUpdated") or datetime.now(tz=timezone.utc).isoformat()
    payload = {
        "enterprise_credit": data.get("enterprise_credit") or data.get("enterprise") or "",
        "enterprise_mortgage": data.get("enterprise_mortgage") or "",
        "personal": data.get("personal") or "",
    }
    migrated = 0

    with SessionLocal() as db:
        for cache_key, content in payload.items():
            if not content:
                continue
            existing = db.query(ProductCacheEntry).filter(ProductCacheEntry.cache_key == cache_key).first()
            if existing:
                continue
            db.add(
                ProductCacheEntry(
                    cache_key=cache_key,
                    content=content,
                    last_updated=last_updated,
                    source="legacy_json_import",
                )
            )
            migrated += 1
        db.commit()

    print(f"migrated product cache entries: {migrated}")


if __name__ == "__main__":
    main()
