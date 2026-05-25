from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from backend.database import Base, SessionLocal, engine
from backend.db_models import IncomeConfirmationOverride

logger = logging.getLogger(__name__)


def _json_loads(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
        return [str(item) for item in parsed] if isinstance(parsed, list) else []
    except Exception:
        return []


def _record_to_dict(record: IncomeConfirmationOverride) -> dict[str, Any]:
    return {
        "id": record.id,
        "customer_id": record.customer_id,
        "document_id": record.document_id,
        "source_type": record.source_type,
        "income_type": record.income_type,
        "target_type": record.target_type,
        "counterparty_name": record.counterparty_name,
        "amount": float(record.amount or 0),
        "months": _json_loads(record.months_json),
        "transaction_ids": _json_loads(record.transaction_ids_json),
        "manual_status": record.manual_status,
        "reason": record.reason or "",
        "confirmed_by": record.confirmed_by or "",
        "confirmed_at": record.confirmed_at.isoformat() if record.confirmed_at else "",
        "created_at": record.created_at.isoformat() if record.created_at else "",
        "updated_at": record.updated_at.isoformat() if record.updated_at else "",
    }


def list_income_confirmations(customer_id: str) -> list[dict[str, Any]]:
    Base.metadata.create_all(bind=engine, tables=[IncomeConfirmationOverride.__table__], checkfirst=True)
    with SessionLocal() as db:
        rows = db.execute(
            select(IncomeConfirmationOverride)
            .where(IncomeConfirmationOverride.customer_id == customer_id)
            .where(IncomeConfirmationOverride.source_type == "personal_flow")
        ).scalars().all()
    return [_record_to_dict(row) for row in rows]


def save_income_confirmation(
    customer_id: str,
    document_id: str,
    payload: dict[str, Any],
    *,
    confirmed_by: str,
) -> dict[str, Any]:
    Base.metadata.create_all(bind=engine, tables=[IncomeConfirmationOverride.__table__], checkfirst=True)
    income_type = str(payload.get("income_type") or "suspected_salary")
    target_type = str(payload.get("target_type") or "confirmed_salary")
    counterparty_name = str(payload.get("counterparty_name") or "").strip()
    manual_status = str(payload.get("manual_status") or "pending")
    if income_type != "suspected_salary" or target_type != "confirmed_salary":
        raise ValueError("仅支持疑似工资转明确工资的人工确认")
    if manual_status not in {"confirmed", "rejected", "pending"}:
        raise ValueError("无效的人工确认状态")
    if not counterparty_name:
        raise ValueError("付款方名称不能为空")
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        record = db.execute(
            select(IncomeConfirmationOverride)
            .where(IncomeConfirmationOverride.document_id == document_id)
            .where(IncomeConfirmationOverride.counterparty_name == counterparty_name)
            .where(IncomeConfirmationOverride.income_type == income_type)
        ).scalar_one_or_none()
        if record is None:
            record = IncomeConfirmationOverride(
                customer_id=customer_id,
                document_id=document_id,
                source_type="personal_flow",
                income_type=income_type,
                counterparty_name=counterparty_name,
            )
            db.add(record)
        record.customer_id = customer_id
        record.target_type = target_type
        record.amount = float(payload.get("amount") or 0)
        record.months_json = json.dumps(list(payload.get("months") or []), ensure_ascii=False)
        record.transaction_ids_json = json.dumps(list(payload.get("transaction_ids") or []), ensure_ascii=False)
        record.manual_status = manual_status
        record.reason = str(payload.get("reason") or "")
        record.confirmed_by = confirmed_by
        record.confirmed_at = now
        db.commit()
        db.refresh(record)
        result = _record_to_dict(record)
    logger.info(
        "[PersonalFlow][INCOME_CONFIRMATION] customer_id=%s document_id=%s counterparty=%s status=%s confirmed_by=%s",
        customer_id,
        document_id,
        counterparty_name,
        manual_status,
        confirmed_by,
    )
    return result
