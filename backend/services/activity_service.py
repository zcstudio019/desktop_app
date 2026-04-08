"""Persistent activity logging backed by SQLAlchemy."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc

from backend.database import SessionLocal
from backend.db_models import ActivityLogEntry, CustomerActivityState

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _loads_metadata(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        import json

        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def add_activity(
    activity_type: str,
    filename: str | None = None,
    customer: str | None = None,
    status: str = "completed",
    document_type: str | None = None,
    *,
    title: str | None = None,
    description: str | None = None,
    customer_id: str | None = None,
    username: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    """Append one activity entry to persistent SQL storage."""
    activity_id = str(uuid.uuid4())[:8]
    activity_time = _now_iso()

    try:
        import json

        with SessionLocal() as db:
            db.add(
                ActivityLogEntry(
                    activity_id=activity_id,
                    activity_type=activity_type,
                    activity_time=activity_time,
                    status=status,
                    title=title or "",
                    description=description or "",
                    customer_name=customer or "",
                    customer_id=customer_id or "",
                    username=username or "",
                    file_name=filename or "",
                    file_type=document_type or "",
                    metadata_json=json.dumps(metadata or {}, ensure_ascii=False),
                )
            )
            db.commit()
        logger.info(
            "Added activity type=%s customer=%s customer_id=%s status=%s",
            activity_type,
            customer or "",
            customer_id or "",
            status,
        )
        return activity_id
    except Exception as exc:
        logger.error("Failed to persist activity log entry: %s", exc)
        return None


def update_customer_status(
    customer: str,
    has_application: bool | None = None,
    has_matching: bool | None = None,
) -> bool:
    """Upsert customer activity summary state."""
    if not customer:
        return False

    try:
        with SessionLocal() as db:
            state = db.query(CustomerActivityState).filter(CustomerActivityState.customer_name == customer).first()
            if state is None:
                state = CustomerActivityState(
                    customer_name=customer,
                    has_application=1 if has_application else 0,
                    has_matching=1 if has_matching else 0,
                    last_update=_now_iso(),
                )
                db.add(state)
            else:
                if has_application is not None:
                    state.has_application = 1 if has_application else 0
                if has_matching is not None:
                    state.has_matching = 1 if has_matching else 0
                state.last_update = _now_iso()
            db.commit()
        logger.info("Updated customer status for %s", customer)
        return True
    except Exception as exc:
        logger.error("Failed to update customer status for %s: %s", customer, exc)
        return False


def get_dashboard_stats(feishu_records: list[dict] | None = None) -> dict[str, int]:
    """Compute dashboard stats from SQL activity storage."""
    try:
        with SessionLocal() as db:
            states = db.query(CustomerActivityState).all()
            pending = 0
            completed = 0
            for state in states:
                has_app = bool(state.has_application)
                has_match = bool(state.has_matching)
                if has_app and has_match:
                    completed += 1
                elif has_app or has_match:
                    pending += 1

            today = datetime.now(tz=timezone.utc).date().isoformat()
            today_uploads = (
                db.query(ActivityLogEntry)
                .filter(ActivityLogEntry.activity_type == "upload")
                .all()
            )
            today_upload_count = sum(1 for item in today_uploads if str(item.activity_time or "").startswith(today))

            total_customers = len(feishu_records) if feishu_records is not None else len(states)
            return {
                "todayUploads": today_upload_count,
                "pending": pending,
                "completed": completed,
                "totalCustomers": total_customers,
            }
    except Exception as exc:
        logger.error("Failed to compute dashboard stats from activity log: %s", exc)
        return {
            "todayUploads": 0,
            "pending": 0,
            "completed": 0,
            "totalCustomers": len(feishu_records) if feishu_records is not None else 0,
        }


def get_recent_activities(limit: int = 10) -> list[dict[str, Any]]:
    """Return recent activities formatted for dashboard consumption."""
    try:
        with SessionLocal() as db:
            rows = (
                db.query(ActivityLogEntry)
                .order_by(desc(ActivityLogEntry.activity_time), desc(ActivityLogEntry.id))
                .limit(limit)
                .all()
            )

        activities: list[dict[str, Any]] = []
        for row in rows:
            activities.append(
                {
                    "id": row.activity_id,
                    "type": row.activity_type or "",
                    "time": _format_relative_time(row.activity_time or ""),
                    "createdAt": row.activity_time or "",
                    "status": row.status or "completed",
                    "title": row.title or "",
                    "description": row.description or "",
                    "customerName": row.customer_name or "",
                    "customerId": row.customer_id or "",
                    "username": row.username or "",
                    "fileName": row.file_name or "",
                    "fileType": row.file_type or "",
                    "metadata": _loads_metadata(row.metadata_json),
                }
            )
        return activities
    except Exception as exc:
        logger.error("Failed to load recent activities from SQL storage: %s", exc)
        return []


def _format_relative_time(iso_time: str) -> str:
    if not iso_time:
        return "未知时间"

    try:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        now = datetime.now(tz=timezone.utc)
        diff = now - dt
        seconds = diff.total_seconds()

        if seconds < 60:
            return "刚刚"
        if seconds < 3600:
            return f"{int(seconds / 60)} 分钟前"
        if seconds < 86400:
            return f"{int(seconds / 3600)} 小时前"
        if seconds < 604800:
            return f"{int(seconds / 86400)} 天前"
        return dt.astimezone().strftime("%Y-%m-%d")
    except Exception:
        return "未知时间"
