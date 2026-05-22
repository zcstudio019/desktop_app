from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from backend.database import Base, SessionLocal, engine
from backend.db_models import CustomerFlowRule

logger = logging.getLogger(__name__)

DEFAULT_INTERNAL_KEYWORDS = ["内部转账", "账户互转", "资金归集", "往来款", "本系统转帐", "备用金"]
RULE_LIST_FIELDS = (
    "related_company_names",
    "self_account_numbers",
    "internal_transfer_keywords",
    "operating_counterparty_whitelist",
    "internal_counterparty_blacklist",
    "personal_counterparty_names",
)


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _dumps(value: Any, default: Any) -> str:
    if value is None:
        value = default
    return json.dumps(value, ensure_ascii=False, default=str)


def _normalize_rules(data: dict[str, Any] | None, customer_id: str) -> dict[str, Any]:
    data = data or {}
    rules = {
        "customer_id": customer_id,
        "related_company_names": list(data.get("related_company_names") or []),
        "self_account_numbers": list(data.get("self_account_numbers") or []),
        "internal_transfer_keywords": list(data.get("internal_transfer_keywords") or DEFAULT_INTERNAL_KEYWORDS),
        "operating_counterparty_whitelist": list(data.get("operating_counterparty_whitelist") or []),
        "internal_counterparty_blacklist": list(data.get("internal_counterparty_blacklist") or []),
        "personal_counterparty_names": list(data.get("personal_counterparty_names") or []),
        "manual_overrides": dict(data.get("manual_overrides") or {}),
        "updated_by": data.get("updated_by") or "",
    }
    return rules


def get_enterprise_flow_rules(customer_id: str) -> dict[str, Any]:
    Base.metadata.create_all(bind=engine, tables=[CustomerFlowRule.__table__], checkfirst=True)
    with SessionLocal() as db:
        record = db.execute(select(CustomerFlowRule).where(CustomerFlowRule.customer_id == customer_id)).scalar_one_or_none()
        if record is None:
            rules = _normalize_rules({}, customer_id)
            logger.info("[EnterpriseFlowRules] load customer_id=%s related_companies=0 self_accounts=0", customer_id)
            return rules
        rules = _normalize_rules(
            {
                "related_company_names": _loads(record.related_company_names_json, []),
                "self_account_numbers": _loads(record.self_account_numbers_json, []),
                "internal_transfer_keywords": _loads(record.internal_transfer_keywords_json, DEFAULT_INTERNAL_KEYWORDS),
                "operating_counterparty_whitelist": _loads(record.operating_counterparty_whitelist_json, []),
                "internal_counterparty_blacklist": _loads(record.internal_counterparty_blacklist_json, []),
                "personal_counterparty_names": _loads(record.personal_counterparty_names_json, []),
                "manual_overrides": _loads(record.manual_overrides_json, {}),
                "updated_by": record.updated_by or "",
            },
            customer_id,
        )
        logger.info(
            "[EnterpriseFlowRules] load customer_id=%s related_companies=%s self_accounts=%s",
            customer_id,
            len(rules["related_company_names"]),
            len(rules["self_account_numbers"]),
        )
        return rules


def save_enterprise_flow_rules(customer_id: str, payload: dict[str, Any], updated_by: str = "") -> dict[str, Any]:
    Base.metadata.create_all(bind=engine, tables=[CustomerFlowRule.__table__], checkfirst=True)
    rules = _normalize_rules(payload, customer_id)
    rules["updated_by"] = updated_by or rules.get("updated_by") or ""
    with SessionLocal() as db:
        record = db.execute(select(CustomerFlowRule).where(CustomerFlowRule.customer_id == customer_id)).scalar_one_or_none()
        if record is None:
            record = CustomerFlowRule(customer_id=customer_id)
            db.add(record)
        record.related_company_names_json = _dumps(rules["related_company_names"], [])
        record.self_account_numbers_json = _dumps(rules["self_account_numbers"], [])
        record.internal_transfer_keywords_json = _dumps(rules["internal_transfer_keywords"], DEFAULT_INTERNAL_KEYWORDS)
        record.operating_counterparty_whitelist_json = _dumps(rules["operating_counterparty_whitelist"], [])
        record.internal_counterparty_blacklist_json = _dumps(rules["internal_counterparty_blacklist"], [])
        record.personal_counterparty_names_json = _dumps(rules["personal_counterparty_names"], [])
        record.manual_overrides_json = _dumps(rules["manual_overrides"], {})
        record.updated_by = rules["updated_by"]
        db.commit()
    logger.info("[EnterpriseFlowRules] save customer_id=%s updated_by=%s", customer_id, updated_by)
    return get_enterprise_flow_rules(customer_id)


def update_transaction_review(
    customer_id: str,
    transaction_id: str,
    review: dict[str, Any],
    reviewed_by: str = "",
) -> dict[str, Any]:
    rules = get_enterprise_flow_rules(customer_id)
    overrides = dict(rules.get("manual_overrides") or {})
    before = overrides.get(transaction_id)
    overrides[transaction_id] = {
        "nature": review.get("nature") or "unknown",
        "exclude_from_operating": bool(review.get("exclude_from_operating")),
        "manual_reason": review.get("manual_reason") or review.get("reason") or "",
        "reviewed_by": reviewed_by or review.get("reviewed_by") or "",
        "reviewed_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    rules["manual_overrides"] = overrides
    saved = save_enterprise_flow_rules(customer_id, rules, reviewed_by)
    logger.info(
        "[EnterpriseFlowReview] transaction_id=%s old_nature=%s new_nature=%s actor=%s",
        transaction_id,
        (before or {}).get("nature"),
        overrides[transaction_id].get("nature"),
        reviewed_by,
    )
    return saved
