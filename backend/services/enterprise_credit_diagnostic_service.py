from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from backend.document_types import normalize_document_type_code


ENTERPRISE_CREDIT_TYPES = ("enterprise_credit_report", "enterprise_credit")
ABNORMAL_CLASSIFICATIONS = ("关注", "次级", "可疑", "损失")


def _empty_result() -> dict[str, Any]:
    return {
        "has_enterprise_credit_report": False,
        "credit_status": "unknown",
        "debt_summary": {
            "total_unsettled_balance": None,
            "short_term_loan_balance": None,
            "long_term_loan_balance": None,
            "credit_limit_total": None,
            "used_credit_total": None,
            "credit_usage_rate": None,
        },
        "loan_summary": {
            "active_loan_count": 0,
            "upcoming_due_loans": [],
            "overdue_loans": [],
            "abnormal_classification_loans": [],
        },
        "guarantee_summary": {
            "has_external_guarantee": False,
            "external_guarantee_balance": None,
            "guarantee_risks": [],
        },
        "key_risks": ["尚未上传企业征信报告，无法判断企业当前银行负债、授信和逾期情况"],
        "positive_signals": [],
        "recommended_actions": ["请补充企业征信报告，用于判断企业银行负债、授信使用率、逾期和担保情况"],
        "summary": "尚未上传企业征信报告，当前融资诊断无法覆盖企业银行负债、授信、逾期和担保情况。",
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean_list(items: list[Any]) -> list[str]:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = _first(value.get("amount"), value.get("value"), value.get("number"))
    text = str(value).strip()
    if not text or text in {"-", "未识别", "暂无", "null", "None"}:
        return None
    text = text.replace(",", "").replace("万元", "").replace("元", "").replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def _sum_numbers(values: list[Any]) -> float | None:
    total = 0.0
    seen = False
    for value in values:
        number = _number(value)
        if number is None:
            continue
        total += number
        seen = True
    return round(total, 2) if seen else None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt != "%Y%m%d" else text[:8], fmt).date()
        except ValueError:
            continue
    return None


def _dedupe_dicts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for item in items:
        key = (
            str(item.get("institution") or ""),
            str(item.get("account_number") or ""),
            str(item.get("balance") or ""),
            str(item.get("due_date") or ""),
            str(item.get("classification") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _payload_from_extracted_data(extracted_data: dict[str, Any]) -> dict[str, Any]:
    for key in ("structured_data", "structured_json", "parsed_json", "extracted_json", "data"):
        value = extracted_data.get(key)
        if isinstance(value, dict):
            return value
    return extracted_data


def _latest_sort_key(extraction: dict[str, Any]) -> str:
    return str(extraction.get("uploaded_at") or extraction.get("created_at") or extraction.get("updated_at") or "")


def _is_enterprise_credit_extraction(extraction: dict[str, Any]) -> bool:
    extraction_type = normalize_document_type_code(str(extraction.get("extraction_type") or ""))
    if extraction_type in {"enterprise_credit_report", "enterprise_credit"}:
        return True
    data = _as_dict(extraction.get("extracted_data"))
    schema_version = str(data.get("schema_version") or "")
    return schema_version.startswith("enterprise_credit")


def _extract_latest_payload_from_extractions(extractions: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [item for item in extractions if isinstance(item, dict) and _is_enterprise_credit_extraction(item)]
    candidates.sort(key=_latest_sort_key, reverse=True)
    for item in candidates:
        data = _as_dict(item.get("extracted_data"))
        payload = _payload_from_extracted_data(data)
        if payload:
            return payload
    return None


async def load_latest_enterprise_credit_payload(storage: Any, customer_id: str) -> dict[str, Any] | None:
    if storage is None or not customer_id:
        return None
    if hasattr(storage, "get_latest_extraction_by_types"):
        try:
            latest = await storage.get_latest_extraction_by_types(str(customer_id), list(ENTERPRISE_CREDIT_TYPES))
            data = _as_dict((latest or {}).get("extracted_data"))
            payload = _payload_from_extracted_data(data)
            if payload:
                return payload
        except Exception:
            pass
    try:
        extractions = await storage.get_extractions_by_customer(str(customer_id))
    except Exception:
        extractions = []
    if not isinstance(extractions, list):
        return None
    return _extract_latest_payload_from_extractions(extractions)


def _agent_result(payload: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(payload.get("agent_result"))


def _credit_summary(payload: dict[str, Any]) -> dict[str, Any]:
    agent_summary = _as_dict(_agent_result(payload).get("credit_summary"))
    summary = _as_dict(payload.get("credit_summary"))
    merged = dict(agent_summary)
    merged.update({key: value for key, value in summary.items() if value not in (None, "", [], {})})
    return merged


def _credit_lines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    agent = _agent_result(payload)
    lines = (
        _as_list(payload.get("credit_facilities"))
        or _as_list(payload.get("credit_lines"))
        or _as_list(agent.get("credit_lines"))
    )
    return [item for item in lines if isinstance(item, dict)]


def _raw_loans(payload: dict[str, Any]) -> list[dict[str, Any]]:
    agent = _agent_result(payload)
    lists = [
        _as_list(payload.get("active_loans")),
        _as_list(payload.get("short_loans_final")),
        _as_list(payload.get("short_loans")),
        _as_list(payload.get("medium_loans_final")),
        _as_list(payload.get("medium_loans")),
        _as_list(payload.get("long_term_loans")),
        _as_list(payload.get("revolving_loans")),
        _as_list(payload.get("revolving_overdrafts")),
        _as_list(agent.get("short_term_loans")),
        _as_list(agent.get("medium_long_term_loans")),
        _as_list(agent.get("revolving_overdrafts")),
    ]
    loans: list[dict[str, Any]] = []
    for items in lists:
        loans.extend(item for item in items if isinstance(item, dict))
    return loans


def _normalize_loan(loan: dict[str, Any]) -> dict[str, Any]:
    institution = _first(loan.get("institution"), loan.get("institution_name"), loan.get("bank"), loan.get("lender"))
    balance = _number(_first(loan.get("balance"), loan.get("outstanding_balance"), loan.get("current_balance"), loan.get("loan_balance")))
    overdue_months = int(_number(_first(loan.get("overdue_months"), loan.get("overdue_month_count"))) or 0)
    classification = str(_first(loan.get("five_classification"), loan.get("five_category"), loan.get("classification"), loan.get("status")) or "").strip()
    due_date = _first(loan.get("due_date"), loan.get("end_date"), loan.get("maturity_date"), loan.get("expiry_date"))
    return {
        "institution": str(institution or ""),
        "account_number": str(_first(loan.get("account_no"), loan.get("account_number"), loan.get("account_id")) or ""),
        "balance": balance,
        "due_date": str(due_date or ""),
        "overdue_months": overdue_months,
        "classification": classification,
        "term_type": str(_first(loan.get("term_type"), loan.get("section_type"), loan.get("loan_term_type")) or ""),
    }


def _loan_public_item(loan: dict[str, Any]) -> dict[str, Any]:
    return {
        "institution": loan.get("institution") or "",
        "account_number": loan.get("account_number") or "",
        "balance": loan.get("balance"),
        "due_date": loan.get("due_date") or "",
        "overdue_months": loan.get("overdue_months") or 0,
        "classification": loan.get("classification") or "",
    }


def _is_overdue(loan: dict[str, Any]) -> bool:
    text = f"{loan.get('classification') or ''} {loan.get('status') or ''}"
    return int(loan.get("overdue_months") or 0) > 0 or "逾期" in text


def _is_abnormal(loan: dict[str, Any]) -> bool:
    classification = str(loan.get("classification") or "")
    return any(keyword in classification for keyword in ABNORMAL_CLASSIFICATIONS)


def _is_short_term(loan: dict[str, Any]) -> bool:
    term_type = str(loan.get("term_type") or "").lower()
    return "short" in term_type or "短" in term_type


def _is_long_term(loan: dict[str, Any]) -> bool:
    term_type = str(loan.get("term_type") or "").lower()
    return "medium" in term_type or "long" in term_type or "中" in term_type or "长期" in term_type


def _guarantees(payload: dict[str, Any]) -> list[dict[str, Any]]:
    agent = _agent_result(payload)
    items = (
        _as_list(payload.get("guarantees"))
        + _as_list(payload.get("bank_guarantee_other_business"))
        + _as_list(payload.get("external_guarantees"))
        + _as_list(agent.get("guarantees"))
    )
    return [item for item in items if isinstance(item, dict)]


def build_enterprise_credit_diagnostic_from_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return _empty_result()

    summary = _credit_summary(payload)
    loans = _dedupe_dicts([_normalize_loan(item) for item in _raw_loans(payload)])
    credit_lines = _credit_lines(payload)
    guarantees = _guarantees(payload)

    credit_limit_total = _sum_numbers([
        _first(item.get("credit_amount"), item.get("total_limit"), item.get("credit_limit_total"))
        for item in credit_lines
    ])
    used_credit_total = _sum_numbers([
        _first(item.get("used_amount"), item.get("used_limit"), item.get("used_credit_total"))
        for item in credit_lines
    ])
    credit_usage_rate = None
    if credit_limit_total and used_credit_total is not None:
        credit_usage_rate = round(used_credit_total / credit_limit_total, 4)

    short_balance = _number(_first(summary.get("short_term_loan_balance"), summary.get("active_short_term_debt_total")))
    long_balance = _number(_first(summary.get("medium_long_term_loan_balance"), summary.get("long_term_loan_balance"), summary.get("active_long_term_debt_total")))
    if short_balance is None:
        short_balance = _sum_numbers([loan.get("balance") for loan in loans if _is_short_term(loan)])
    if long_balance is None:
        long_balance = _sum_numbers([loan.get("balance") for loan in loans if _is_long_term(loan)])
    total_balance = _number(_first(summary.get("active_borrowing_balance"), summary.get("unsettled_credit_balance"), summary.get("total_unsettled_balance")))
    if total_balance is None:
        total_balance = _sum_numbers([loan.get("balance") for loan in loans])

    external_guarantee_balance = _number(_first(summary.get("guarantee_balance"), summary.get("external_guarantee_balance")))
    if external_guarantee_balance is None:
        external_guarantee_balance = _sum_numbers([
            _first(item.get("balance"), item.get("guarantee_balance"), item.get("amount"))
            for item in guarantees
        ])

    today = date.today()
    upcoming_due_loans = []
    overdue_loans = []
    abnormal_loans = []
    for loan in loans:
        due = _parse_date(loan.get("due_date"))
        if due and today <= due <= today + timedelta(days=90):
            upcoming_due_loans.append(_loan_public_item(loan))
        if _is_overdue(loan):
            overdue_loans.append(_loan_public_item(loan))
        if _is_abnormal(loan):
            abnormal_loans.append(_loan_public_item(loan))

    key_risks: list[str] = []
    positive_signals: list[str] = []
    recommended_actions: list[str] = []
    guarantee_risks: list[str] = []

    if overdue_loans:
        key_risks.append("企业征信存在逾期记录，可能影响新增授信审批")
    if abnormal_loans:
        key_risks.append("五级分类存在非正常状态，银行审批通过难度较高")
    has_external_guarantee = bool((external_guarantee_balance and external_guarantee_balance > 0) or guarantees)
    if has_external_guarantee:
        risk = "企业存在对外担保，可能占用授信空间并增加或有负债风险"
        key_risks.append(risk)
        guarantee_risks.append(risk)
    if credit_usage_rate is not None:
        if credit_usage_rate >= 0.8:
            key_risks.append("授信使用率较高，新增授信空间可能受限")
        elif credit_usage_rate < 0.5:
            positive_signals.append("授信使用率相对可控")
    if upcoming_due_loans:
        recommended_actions.append("建议提前规划即将到期贷款的续贷或置换方案")

    serious_overdue = any(int(item.get("overdue_months") or 0) >= 3 for item in overdue_loans)
    if abnormal_loans or serious_overdue:
        credit_status = "risky"
    elif overdue_loans or has_external_guarantee or (credit_usage_rate is not None and credit_usage_rate >= 0.8):
        credit_status = "attention"
    else:
        credit_status = "normal"

    if not key_risks:
        positive_signals.append("企业征信暂未识别明显逾期、非正常五级分类或高授信占用风险")
    if not recommended_actions:
        recommended_actions.append("可继续结合流水、财报和经营资料进行综合授信测算")

    active_loan_count = len(loans)
    summary_text = (
        f"已读取企业征信报告，当前未结清贷款 {active_loan_count} 笔"
        f"，未结清余额{total_balance if total_balance is not None else '未识别'}。"
    )
    if key_risks:
        summary_text += "需关注：" + "；".join(key_risks[:3]) + "。"
    else:
        summary_text += "暂未识别明显企业征信风险。"

    return {
        "has_enterprise_credit_report": True,
        "credit_status": credit_status,
        "debt_summary": {
            "total_unsettled_balance": total_balance,
            "short_term_loan_balance": short_balance,
            "long_term_loan_balance": long_balance,
            "credit_limit_total": credit_limit_total,
            "used_credit_total": used_credit_total,
            "credit_usage_rate": credit_usage_rate,
        },
        "loan_summary": {
            "active_loan_count": active_loan_count,
            "upcoming_due_loans": upcoming_due_loans,
            "overdue_loans": overdue_loans,
            "abnormal_classification_loans": abnormal_loans,
        },
        "guarantee_summary": {
            "has_external_guarantee": has_external_guarantee,
            "external_guarantee_balance": external_guarantee_balance,
            "guarantee_risks": guarantee_risks,
        },
        "key_risks": _clean_list(key_risks),
        "positive_signals": _clean_list(positive_signals),
        "recommended_actions": _clean_list(recommended_actions),
        "summary": summary_text,
    }


async def build_enterprise_credit_diagnostic(storage: Any, customer_id: str) -> dict[str, Any]:
    payload = await load_latest_enterprise_credit_payload(storage, customer_id)
    return build_enterprise_credit_diagnostic_from_payload(payload)
