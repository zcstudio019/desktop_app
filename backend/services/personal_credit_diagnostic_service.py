from __future__ import annotations

from typing import Any

from backend.document_types import normalize_document_type_code


PERSONAL_CREDIT_TYPES = ("personal_credit_report", "personal_credit")
SERIOUS_NEGATIVE_KEYWORDS = ("呆账", "资产处置", "保证人代偿", "强制执行", "当前逾期未结清")
ABNORMAL_CLASSIFICATIONS = ("次级", "可疑", "损失")


def _empty_result() -> dict[str, Any]:
    return {
        "has_personal_credit_report": False,
        "credit_status": "unknown",
        "debt_summary": {
            "loan_balance": None,
            "credit_card_used_amount": None,
            "external_guarantee_balance": None,
        },
        "overdue_summary": {
            "has_loan_overdue": False,
            "has_credit_card_overdue": False,
            "overdue_records": [],
        },
        "query_summary": {
            "last_3_months_query_count": None,
            "last_6_months_query_count": None,
            "query_risk_level": "unknown",
        },
        "serious_negative_summary": {
            "has_serious_negative": False,
            "items": [],
        },
        "key_risks": ["尚未上传个人征信报告，无法判断法人或实际控制人个人负债、逾期和查询情况"],
        "positive_signals": [],
        "recommended_actions": ["请补充法人/实际控制人个人征信报告，用于判断个人负债、信用卡使用、逾期和查询情况"],
        "summary": "尚未上传个人征信报告，当前融资诊断无法覆盖法人或实际控制人个人负债、逾期和查询情况。",
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


def _payload_from_extracted_data(extracted_data: dict[str, Any]) -> dict[str, Any]:
    for key in ("structured_data", "structured_json", "parsed_json", "extracted_json", "data", "report_json"):
        value = extracted_data.get(key)
        if isinstance(value, dict):
            return value
    return extracted_data


def _latest_sort_key(extraction: dict[str, Any]) -> str:
    return str(extraction.get("uploaded_at") or extraction.get("created_at") or extraction.get("updated_at") or "")


def _is_personal_credit_extraction(extraction: dict[str, Any]) -> bool:
    extraction_type = normalize_document_type_code(str(extraction.get("extraction_type") or ""))
    if extraction_type in {"personal_credit_report", "personal_credit"}:
        return True
    data = _as_dict(extraction.get("extracted_data"))
    schema_version = str(data.get("schema_version") or "")
    nested = _payload_from_extracted_data(data)
    nested_schema = str(nested.get("schema_version") or "") if isinstance(nested, dict) else ""
    return schema_version.startswith("personal_credit_report") or nested_schema.startswith("personal_credit_report")


def _extract_latest_payload_from_extractions(extractions: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [item for item in extractions if isinstance(item, dict) and _is_personal_credit_extraction(item)]
    candidates.sort(key=_latest_sort_key, reverse=True)
    for item in candidates:
        data = _as_dict(item.get("extracted_data"))
        payload = _payload_from_extracted_data(data)
        if payload:
            return payload
    return None


async def load_latest_personal_credit_payload(storage: Any, customer_id: str) -> dict[str, Any] | None:
    if storage is None or not customer_id:
        return None
    if hasattr(storage, "get_latest_extraction_by_types"):
        try:
            latest = await storage.get_latest_extraction_by_types(str(customer_id), list(PERSONAL_CREDIT_TYPES))
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


def _records(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    agent = _agent_result(payload)
    items = _as_list(payload.get(key)) or _as_list(agent.get(key))
    return [item for item in items if isinstance(item, dict)]


def _summary(payload: dict[str, Any]) -> dict[str, Any]:
    agent_summary = _as_dict(_agent_result(payload).get("credit_summary"))
    summary = _as_dict(payload.get("credit_summary"))
    merged = dict(agent_summary)
    merged.update({key: value for key, value in summary.items() if value not in (None, "", [], {})})
    return merged


def _query_statistics(payload: dict[str, Any]) -> dict[str, Any]:
    agent_stats = _as_dict(_agent_result(payload).get("query_statistics"))
    stats = _as_dict(payload.get("query_statistics"))
    merged = dict(agent_stats)
    merged.update({key: value for key, value in stats.items() if value not in (None, "", [], {})})
    return merged


def _query_count(stats: dict[str, Any], period: str) -> int | None:
    direct = _number(_first(stats.get(f"{period}_query_count"), stats.get(period)))
    if direct is not None:
        return int(direct)
    institution = _as_dict(stats.get("institution_query"))
    personal = _as_dict(stats.get("personal_query"))
    values = [
        _number(institution.get(period)),
        _number(personal.get(period)),
    ]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return int(sum(values))


def _query_risk_level(last_3: int | None, last_6: int | None) -> str:
    if last_3 is None and last_6 is None:
        return "unknown"
    if (last_3 is not None and last_3 >= 6) or (last_6 is not None and last_6 >= 10):
        return "high"
    if (last_3 is not None and last_3 >= 3) or (last_6 is not None and last_6 >= 6):
        return "medium"
    return "low"


def _combined_text(record: dict[str, Any]) -> str:
    return " ".join(str(value or "") for value in record.values())


def _is_overdue(record: dict[str, Any]) -> bool:
    amount = _number(record.get("overdue_amount"))
    months = _number(_first(record.get("overdue_months"), record.get("months")))
    text = _combined_text(record)
    no_overdue = any(phrase in text for phrase in ("当前无逾期", "无逾期", "未发生逾期"))
    return bool((amount and amount > 0) or (months and months > 0) or ("逾期" in text and not no_overdue))


def _overdue_item(record: dict[str, Any], record_type: str) -> dict[str, Any]:
    return {
        "record_type": record_type,
        "institution": str(_first(record.get("institution"), record.get("issuer"), record.get("query_institution")) or ""),
        "amount": _number(_first(record.get("overdue_amount"), record.get("amount"))),
        "months": int(_number(_first(record.get("overdue_months"), record.get("months"))) or 0),
        "account_number": str(_first(record.get("account_no"), record.get("account_number"), record.get("card_tail_no")) or ""),
        "status": str(_first(record.get("overdue_status"), record.get("account_status"), record.get("status"), record.get("five_category")) or ""),
    }


def _is_serious_negative(record: dict[str, Any]) -> bool:
    text = _combined_text(record)
    if any(keyword in text for keyword in SERIOUS_NEGATIVE_KEYWORDS):
        return True
    classification = str(_first(record.get("five_category"), record.get("five_classification"), record.get("classification")) or "")
    return any(keyword in classification for keyword in ABNORMAL_CLASSIFICATIONS)


def _serious_item(record: dict[str, Any], record_type: str) -> dict[str, Any]:
    return {
        "record_type": record_type,
        "institution": str(_first(record.get("institution"), record.get("issuer"), record.get("authority")) or ""),
        "amount": _number(_first(record.get("amount"), record.get("overdue_amount"), record.get("balance"))),
        "status": str(_first(record.get("status"), record.get("account_status"), record.get("five_category"), record.get("record_type")) or ""),
        "description": _combined_text(record)[:160],
    }


def build_personal_credit_diagnostic_from_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return _empty_result()

    summary = _summary(payload)
    loan_accounts = _records(payload, "loan_accounts")
    credit_cards = _records(payload, "credit_card_accounts")
    overdue_records = _records(payload, "overdue_records")
    public_records = _records(payload, "public_records")
    guarantees = _records(payload, "guarantees")
    related_responsibilities = _records(payload, "related_repayment_responsibilities")

    loan_balance = _number(_first(summary.get("loan_balance"), summary.get("active_loan_balance"), summary.get("outstanding_loan_balance")))
    if loan_balance is None:
        loan_balance = _sum_numbers([
            _first(item.get("balance"), item.get("loan_balance"), item.get("outstanding_balance"))
            for item in loan_accounts
        ])
    credit_card_used = _number(_first(summary.get("credit_card_used_amount"), summary.get("credit_card_used_limit"), summary.get("used_credit_card_amount")))
    if credit_card_used is None:
        credit_card_used = _sum_numbers([
            _first(item.get("used_amount"), item.get("used_limit"), item.get("balance"), item.get("special_installment_balance"))
            for item in credit_cards
        ])
    external_guarantee_balance = _number(_first(summary.get("external_guarantee_balance"), summary.get("guarantee_balance")))
    if external_guarantee_balance is None:
        external_guarantee_balance = _sum_numbers([
            _first(item.get("guarantee_balance"), item.get("guarantee_amount"), item.get("loan_balance"), item.get("balance"))
            for item in guarantees + related_responsibilities
        ])

    loan_overdues = [_overdue_item(item, "loan") for item in loan_accounts if _is_overdue(item)]
    card_overdues = [_overdue_item(item, "credit_card") for item in credit_cards if _is_overdue(item)]
    explicit_overdues = [_overdue_item(item, str(item.get("record_type") or "overdue")) for item in overdue_records if _is_overdue(item) or item]
    all_overdues = loan_overdues + card_overdues + explicit_overdues

    serious_items = []
    for record_type, items in (
        ("loan", loan_accounts),
        ("credit_card", credit_cards),
        ("overdue", overdue_records),
        ("public_record", public_records),
    ):
        serious_items.extend(_serious_item(item, record_type) for item in items if _is_serious_negative(item))

    stats = _query_statistics(payload)
    last_3 = _query_count(stats, "last_3_months")
    last_6 = _query_count(stats, "last_6_months")
    query_risk = _query_risk_level(last_3, last_6)

    key_risks: list[str] = []
    positive_signals: list[str] = []
    recommended_actions: list[str] = []

    if loan_overdues or any(item.get("record_type") == "loan" for item in explicit_overdues):
        key_risks.append("个人征信存在贷款逾期记录，可能影响企业融资审批")
    if card_overdues or any("信用卡" in str(item.get("record_type") or "") or "贷记卡" in str(item.get("record_type") or "") for item in explicit_overdues):
        key_risks.append("个人征信存在信用卡逾期记录，银行审批会重点关注")
    if serious_items:
        key_risks.append("个人征信存在严重负面记录，新增融资审批难度较高")
    if query_risk == "high":
        key_risks.append("近3个月或近6个月征信查询次数较多，可能被银行视为多头申请")
    elif query_risk == "medium":
        key_risks.append("近期征信查询次数偏多，建议核实是否存在多头申请")
    if credit_card_used is not None and credit_card_used >= 50000:
        key_risks.append("信用卡使用额度较高，可能影响个人负债率判断")
    if external_guarantee_balance is not None and external_guarantee_balance > 0:
        key_risks.append("个人征信存在对外担保，可能形成或有负债")

    if query_risk == "low":
        positive_signals.append("近期征信查询次数相对可控")
    if not all_overdues and not serious_items:
        positive_signals.append("个人征信暂未识别明显逾期或严重负面记录")
    if key_risks:
        recommended_actions.append("建议核实个人征信风险事项，并结合收入流水判断个人负债承压情况")
    else:
        recommended_actions.append("可继续结合个人流水、企业流水和财报进行综合融资测算")

    has_serious_negative = bool(serious_items)
    has_attention_risk = bool(all_overdues or query_risk in {"medium", "high"} or (credit_card_used is not None and credit_card_used >= 50000) or (external_guarantee_balance is not None and external_guarantee_balance > 0))
    if has_serious_negative:
        credit_status = "risky"
    elif has_attention_risk:
        credit_status = "attention"
    else:
        credit_status = "normal"

    summary_text = (
        f"已读取个人征信报告，贷款余额{loan_balance if loan_balance is not None else '未识别'}，"
        f"信用卡已用额度{credit_card_used if credit_card_used is not None else '未识别'}。"
    )
    if key_risks:
        summary_text += "需关注：" + "；".join(key_risks[:3]) + "。"
    else:
        summary_text += "暂未识别明显个人征信风险。"

    return {
        "has_personal_credit_report": True,
        "credit_status": credit_status,
        "debt_summary": {
            "loan_balance": loan_balance,
            "credit_card_used_amount": credit_card_used,
            "external_guarantee_balance": external_guarantee_balance,
        },
        "overdue_summary": {
            "has_loan_overdue": bool(loan_overdues or any(item.get("record_type") == "loan" for item in explicit_overdues)),
            "has_credit_card_overdue": bool(card_overdues or any("信用卡" in str(item.get("record_type") or "") or "贷记卡" in str(item.get("record_type") or "") for item in explicit_overdues)),
            "overdue_records": all_overdues,
        },
        "query_summary": {
            "last_3_months_query_count": last_3,
            "last_6_months_query_count": last_6,
            "query_risk_level": query_risk,
        },
        "serious_negative_summary": {
            "has_serious_negative": has_serious_negative,
            "items": serious_items,
        },
        "key_risks": _clean_list(key_risks),
        "positive_signals": _clean_list(positive_signals),
        "recommended_actions": _clean_list(recommended_actions),
        "summary": summary_text,
    }


async def build_personal_credit_diagnostic(storage: Any, customer_id: str) -> dict[str, Any]:
    payload = await load_latest_personal_credit_payload(storage, customer_id)
    return build_personal_credit_diagnostic_from_payload(payload)
