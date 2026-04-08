"""One-off migration script for legacy activity_log.json -> SQLAlchemy tables."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from backend.database import Base, SessionLocal, engine
from backend.db_models import ActivityLogEntry, CustomerActivityState

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
ACTIVITY_FILE = DATA_DIR / "activity_log.json"


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def main() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[ActivityLogEntry.__table__, CustomerActivityState.__table__],
        checkfirst=True,
    )
    if not ACTIVITY_FILE.exists():
        print("legacy activity_log.json not found, skipped")
        return

    with open(ACTIVITY_FILE, encoding="utf-8") as f:
        data = json.load(f)

    activities = data.get("activities") or []
    customers = data.get("customers") or {}
    migrated_activities = 0
    migrated_customers = 0

    with SessionLocal() as db:
        for activity in activities:
            if not isinstance(activity, dict):
                continue
            activity_id = activity.get("id") or str(uuid.uuid4())[:8]
            existing = db.query(ActivityLogEntry).filter(ActivityLogEntry.activity_id == activity_id).first()
            if existing:
                continue
            db.add(
                ActivityLogEntry(
                    activity_id=activity_id,
                    activity_type=activity.get("type") or "",
                    activity_time=activity.get("time") or _now_iso(),
                    status=activity.get("status") or "completed",
                    title=activity.get("title") or "",
                    description=activity.get("description") or "",
                    customer_name=activity.get("customer") or "",
                    customer_id=activity.get("customerId") or "",
                    username=activity.get("username") or "",
                    file_name=activity.get("filename") or "",
                    file_type=activity.get("documentType") or "",
                    metadata_json=json.dumps(activity.get("metadata") or {}, ensure_ascii=False),
                )
            )
            migrated_activities += 1

        if isinstance(customers, dict):
            for customer_name, state in customers.items():
                if not isinstance(state, dict) or not customer_name:
                    continue
                existing = db.query(CustomerActivityState).filter(CustomerActivityState.customer_name == customer_name).first()
                if existing:
                    continue
                db.add(
                    CustomerActivityState(
                        customer_name=customer_name,
                        has_application=1 if state.get("hasApplication") else 0,
                        has_matching=1 if state.get("hasMatching") else 0,
                        last_update=state.get("lastUpdate") or _now_iso(),
                    )
                )
                migrated_customers += 1
        db.commit()

    print(f"migrated activities: {migrated_activities}")
    print(f"migrated customer states: {migrated_customers}")


if __name__ == "__main__":
    main()
