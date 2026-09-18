"""Read-only customer material access and credit one-page report generation.

This module deliberately sits above the existing extraction/agent pipeline.  It only
reads persisted customer, profile, document and extraction records; it never runs
OCR, reparses a source document, or writes a derived report record.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

from services.ai_service import AIService
from backend.services.credit_report_markdown_renderer import render_credit_one_page_report
from backend.services.credit_report_rules_v1 import evaluate_credit_report_rules

logger = logging.getLogger(__name__)

CREDIT_REPORT_INTENT = "credit_one_page_report"
MAX_REPORT_CONTEXT_CHARS = 48_000
MAX_SINGLE_MATERIAL_CHARS = 14_000
ABNORMAL_FIELD_TEXT = "资料异常，需核验"

_DISALLOWED_SOURCE_KEYS = {
    "raw_text", "ocr_text", "full_text", "raw_markdown", "raw_html", "html",
    "page_text", "pages_text", "source_text", "markdown", "markdown_summary",
    "report_markdown", "summary_markdown", "content_markdown",
}
_ABNORMAL_TEXT_PATTERNS = (
    re.compile(r"<br\s*/?>", re.IGNORECASE),
    re.compile(r"个人信用报告|企业信用报告|信息概要|信贷记录概要"),
    re.compile(r"第\s*\d+\s*页\s*(?:/|共)\s*\d+\s*页"),
    re.compile(r"(?:报告编号|查询请求时间|报告时间).{0,80}(?:信息概要|信贷记录)", re.DOTALL),
)

_REPORT_PATTERNS = (
    re.compile(r"(?:生成|出|做|整理|分析).{0,12}(?:征信一页纸|征信速览|征信分析报告|标准征信报告|征信报告)"),
    re.compile(r"^(?:征信一页纸|征信速览|征信分析报告|标准征信报告)[，,。！!]*(?:生成|分析|报告)?$"),
    re.compile(r"分析.{0,12}(?:客户|申请人|主体).{0,5}征信(?:情况|信息)?$"),
)

_EXPLICIT_NAME_PATTERNS = (
    re.compile(r"(?:根据|用|读取|分析)\s*[“\"']?([\u4e00-\u9fffA-Za-z0-9·（）()\-]{2,40})[”\"']?\s*的(?:已有)?资料"),
    re.compile(r"(?:为|给)\s*[“\"']?([\u4e00-\u9fffA-Za-z0-9·（）()\-]{2,40})[”\"']?\s*(?:生成|做|出)"),
)

_TYPE_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("个人征信", ("personal_credit", "个人征信", "个人信用报告")),
    ("企业征信", ("enterprise_credit", "企业征信", "企业信用报告")),
    (
        "KYC及主体关系",
        (
            "id_card", "身份证", "business_license", "营业执照", "company_articles", "公司章程",
            "shareholder_id_card", "股东身份证",
        ),
    ),
)

_REQUIRED_HEADINGS = (
    "# 征信速览报告",
    "## 🚨 紧急关注",
    "# 一、主体基本信息",
    "## 核心指标",
    "# 二、未结清贷款",
    "## 企业贷款",
    "## 个人贷款",
    "# 三、信用卡",
    "# 四、担保及相关还款责任",
    "## 4.1 企业对外担保",
    "## 4.2 法人相关还款责任",
    "# 五、逾期与公共记录",
    "# 六、征信查询记录",
    "# 七、历史信贷特征",
    "# 八、征信指标检查",
    "# 九、优化路线图",
    "## 🔴 紧急（1周内）",
    "## 🟡 中期（1个月内）",
    "## 🟢 长期（3-6个月）",
    "# 十、综合说明",
    "## ✅ 征信优势",
    "## ⚠️ 征信风险",
    "## 综合说明",
    "## 一句话结论",
)


def is_credit_report_request(message: str) -> bool:
    """Deterministically recognize the narrow report intent without affecting chat."""
    text = re.sub(r"\s+", "", message or "")
    return bool(text and any(pattern.search(text) for pattern in _REPORT_PATTERNS))


def extract_requested_customer_name(message: str) -> str | None:
    """Extract an explicitly named customer only from report-specific phrasing."""
    text = (message or "").strip()
    for pattern in _EXPLICIT_NAME_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        name = match.group(1).strip(" ，,。；;：:的")
        # Avoid treating generic demonstratives as a customer name.
        if name not in {"这个客户", "该客户", "当前客户", "客户", "这个", "当前"}:
            return name
    return None


def _mask_identifier(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    if not text:
        return "暂未获取"
    if len(text) <= 6:
        return f"{text[:1]}***{text[-1:]}"
    return f"{text[:4]}{'*' * max(4, len(text) - 8)}{text[-4:]}"


def _mask_identifiers_in_text(text: str) -> str:
    # Credit identifiers are commonly 18 characters. Mask before the LLM sees them.
    pattern = re.compile(r"(?<![A-Za-z0-9])[0-9A-Z]{17}[0-9X](?![A-Za-z0-9])", re.IGNORECASE)
    return pattern.sub(lambda match: _mask_identifier(match.group(0)), text)


def _safe_json(value: Any, limit: int = MAX_SINGLE_MATERIAL_CHARS) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        rendered = str(value)
    return _mask_identifiers_in_text(rendered[:limit])


def _material_group(extraction_type: str) -> str:
    normalized = str(extraction_type or "").lower()
    for group, aliases in _TYPE_GROUPS:
        if any(alias.lower() in normalized for alias in aliases):
            return group
    return "其他资料"


def _drop_disallowed_source_fields(value: Any) -> Any:
    """Remove raw document bodies before data enters the report-generation layer."""
    if isinstance(value, dict):
        return {
            key: _drop_disallowed_source_fields(child)
            for key, child in value.items()
            if str(key).lower() not in _DISALLOWED_SOURCE_KEYS
        }
    if isinstance(value, list):
        return [_drop_disallowed_source_fields(child) for child in value]
    return value


def _safe_business_scalar(value: Any, field_name: str = "field", max_length: int = 300) -> Any:
    """Reject page-like/OCR-like text instead of truncating and accidentally exposing it."""
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, (dict, list, tuple, set)):
        logger.warning("credit report rejected non-scalar field=%s type=%s", field_name, type(value).__name__)
        return ABNORMAL_FIELD_TEXT
    text = re.sub(r"[\t\r]+", " ", str(value)).strip()
    abnormal = (
        len(text) > max_length
        or text.count("\n") > 3
        or len(re.findall(r"(?:^|\n)\s*(?:#{1,6}|[一二三四五六七八九十]+、)", text)) >= 2
        or any(pattern.search(text) for pattern in _ABNORMAL_TEXT_PATTERNS)
    )
    if abnormal:
        logger.warning("credit report rejected abnormal text field=%s length=%s", field_name, len(text))
        return ABNORMAL_FIELD_TEXT
    return _mask_identifiers_in_text(re.sub(r"\s*\n\s*", "；", text))


async def get_customer_materials(storage_service: Any, customer_id: str) -> dict[str, Any]:
    """High-level read-only Tool: load an existing customer's persisted materials."""
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        return {"status": "customer_not_found", "customer": None, "materials": [], "processing": False}

    extractions, documents = await asyncio.gather(
        storage_service.get_extractions_by_customer(customer_id),
        storage_service.list_documents(customer_id),
    )
    extractions = [item for item in (extractions or []) if isinstance(item, dict)]
    documents = [item for item in (documents or []) if isinstance(item, dict) and item.get("is_active", True)]
    processing = any(
        str(item.get("extraction_status") or "").lower() in {"pending", "running", "processing", "queued"}
        for item in extractions
    )
    processing = processing or bool(documents and not extractions)
    successful = [
        item for item in extractions
        if str(item.get("extraction_status") or "success").lower()
        in {"success", "completed", "partial", "partial_success", "partial_failed"}
    ]

    materials: list[dict[str, Any]] = []
    for item in successful:
        category = _material_group(str(item.get("extraction_type") or ""))
        if category == "其他资料":
            continue
        materials.append(
            {
                "category": category,
                "type": str(item.get("extraction_type") or "其他资料"),
                "status": str(item.get("extraction_status") or "success"),
                "structured_data": _drop_disallowed_source_fields(
                    item.get("confirmed_data") or item.get("extracted_data") or {}
                ),
                "created_at": str(item.get("created_at") or ""),
            }
        )

    return {
        "status": "ok",
        "customer": {
            "name": customer.get("name") or "暂未获取",
            "customer_type": customer.get("customer_type") or "暂未获取",
            "id_card_masked": _mask_identifier(customer.get("id_card")),
            "phone_masked": _mask_identifier(customer.get("phone")),
        },
        "materials": materials,
        "document_count": len(documents),
        "successful_material_count": len(successful),
        "processing": processing,
    }


def build_report_context(material_result: dict[str, Any]) -> str:
    """Serialize only the normalized report model; never pass Markdown or OCR text."""
    customer = material_result.get("customer") or {}
    subjects = material_result.get("subjects") or {}
    base = {
        "customer_subject": subjects.get("customer_subject") or customer.get("name") or "暂未获取",
        "enterprise_credit_subject": subjects.get("enterprise_credit_subject") or "暂未获取",
        "personal_credit_subject": subjects.get("personal_credit_subject") or "暂未获取",
        "personal_credit_subject_role": subjects.get("personal_credit_subject_role") or "需人工核实",
        "customer_type": customer.get("customer_type") or "暂未获取",
    }
    model = material_result.get("report_model") if isinstance(material_result.get("report_model"), dict) else {}
    context = {
        "customer_basic": base,
        "enterprise_credit": model.get("enterprise_credit") or {"provided": False},
        "personal_credit": model.get("personal_credit") or {"provided": False},
        "metrics": model.get("metrics") or {},
        "rule_checks": model.get("rule_checks") or [],
        "materials_processing": bool(material_result.get("processing")),
    }
    return _safe_json(context, MAX_REPORT_CONTEXT_CHARS)


async def resolve_report_customer(
    storage_service: Any,
    message: str,
    selected_customer_id: str | None,
) -> dict[str, Any]:
    """Resolve explicit name first, otherwise use the currently selected customer."""
    explicit_name = extract_requested_customer_name(message)
    if not explicit_name:
        if not selected_customer_id:
            return {"status": "missing_context"}
        customer = await storage_service.get_customer(selected_customer_id)
        return {"status": "ok", "customer": customer} if customer else {"status": "customer_not_found"}

    customers = await storage_service.list_customers()
    matches = [item for item in customers or [] if str(item.get("name") or "").strip() == explicit_name]
    if not matches:
        return {"status": "customer_not_found", "requested_name": explicit_name}
    if len(matches) > 1:
        choices = [
            {
                "name": item.get("name") or explicit_name,
                "customer_type": item.get("customer_type") or "类型未记录",
                "phone_masked": _mask_identifier(item.get("phone")),
                "updated_at": str(item.get("updated_at") or "更新时间未记录")[:19],
            }
            for item in matches
        ]
        return {"status": "ambiguous", "requested_name": explicit_name, "choices": choices}
    return {"status": "ok", "customer": matches[0]}


def _strip_internal_output(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:markdown)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = _mask_identifiers_in_text(cleaned)
    # Internal identifiers must never be displayed in the assistant response.
    cleaned = re.sub(r"(?im)^.*\b(?:customer_id|evidence_id|extraction_id|doc_id)\b.*$\n?", "", cleaned)
    internal_statuses = {
        "needs_review": "待核验",
        "missing": "资料不足",
        "pending": "待处理",
        "unknown": "资料不足",
        "normalized": "已标准化",
    }
    for internal, business_text in internal_statuses.items():
        cleaned = re.sub(rf"(?i)\b{re.escape(internal)}\b", business_text, cleaned)
    cleaned = re.sub(r"(?i)\b(?:raw_text|ocr_text|internal_status)\b", "", cleaned)
    return cleaned.strip()


def _has_complete_structure(report: str) -> bool:
    return all(heading in report for heading in _REQUIRED_HEADINGS)


def _has_unsafe_report_text(report: str) -> bool:
    return bool(
        re.search(r"<br\s*/?>", report, re.IGNORECASE)
        or re.search(r"个人信用报告\s*(?:信息概要|信贷记录概要)", report)
        or re.search(r"第\s*\d+\s*页\s*(?:/|共)\s*\d+\s*页", report)
        or re.search(r"(?i)\b(?:needs_review|raw_text|ocr_text|normalized|internal_status)\b", report)
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, dict) and "value" in value:
        value = value.get("value")
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("，", "")
    text = re.sub(r"(?:人民币)?元|万元|%|次|笔|户", "", text).strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _sum_known(values: list[Any]) -> float | None:
    parsed = [number for value in values if (number := _number(value)) is not None]
    return round(sum(parsed), 2) if parsed else None


def _integer_if_integral(value: int | float) -> int | float:
    if isinstance(value, int):
        return value
    return int(value) if value == int(value) else value


def _normalize_money_unit(value: Any) -> str | None:
    text = str(value or "").strip().upper().replace(" ", "")
    if text in {"万元", "人民币万元", "CNY万元", "RMB万元"}:
        return "万元"
    if text in {"元", "人民币", "人民币元", "CNY", "RMB", "CNY元", "RMB元"}:
        return "元"
    return None


def _declared_unit(record: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if unit := _normalize_money_unit(record.get(key)):
            return unit
    return None


def _explicit_money_unit(
    record: dict[str, Any],
    value: Any,
    field_name: str,
    *,
    section_unit: str | None = None,
    document_unit: str | None = None,
) -> str | None:
    text = str(value or "")
    if "万元" in text:
        return "万元"
    if re.search(r"(?:人民币)?元", text):
        return "元"
    unit = _declared_unit(record, (f"{field_name}_unit", "amount_unit", "currency_unit", "unit"))
    if unit:
        return unit
    if account_unit := _declared_unit(record, ("currency", "currency_name", "currency_code")):
        return account_unit
    if normalized_section_unit := _normalize_money_unit(section_unit):
        return normalized_section_unit
    if normalized_document_unit := _normalize_money_unit(document_unit):
        return normalized_document_unit
    # Evidence is never copied into context/output. Only an exact currency-unit
    # token may be extracted, then the source text is discarded.
    evidence = str(record.get("evidence") or "")
    if evidence and len(evidence) <= 500:
        if "万元" in evidence:
            return "万元"
        if "人民币元" in evidence or re.search(r"(?:金额|余额|本金)[^。；\n]{0,40}元", evidence):
            return "元"
    return None


def _money(
    record: dict[str, Any],
    *keys: str,
    section_unit: str | None = None,
    document_unit: str | None = None,
) -> dict[str, Any] | None:
    """Return a value/unit object; missing units are explicitly reviewable."""
    selected_key = next((key for key in keys if record.get(key) not in (None, "")), None)
    if not selected_key:
        return None
    raw = record.get(selected_key)
    number = _number(raw)
    if number is None:
        safe = _safe_business_scalar(raw, selected_key)
        if safe == ABNORMAL_FIELD_TEXT:
            return {"value": None, "unit": None, "message": ABNORMAL_FIELD_TEXT}
        return {"value": safe, "unit": None}
    unit = _explicit_money_unit(record, raw, selected_key, section_unit=section_unit, document_unit=document_unit)
    value: int | float = _integer_if_integral(number)
    return {"value": value, "unit": unit}


def _money_from_value(
    value: Any,
    record: dict[str, Any],
    field_name: str,
    *,
    section_unit: str | None = None,
    document_unit: str | None = None,
) -> dict[str, Any] | None:
    proxy = dict(record)
    proxy[field_name] = value
    return _money(proxy, field_name, section_unit=section_unit, document_unit=document_unit)


def _money_sum(records: list[dict[str, Any]], *keys: str) -> dict[str, Any] | None:
    monies = [_money(record, *keys) for record in records]
    monies = [money for money in monies if money and _number(money) is not None]
    if not monies:
        return None
    units = {money.get("unit") for money in monies}
    total = round(sum((_number(money) or 0.0 for money in monies), 0.0), 2)
    unit = next(iter(units)) if len(units) == 1 else None
    return {"value": _integer_if_integral(total), "unit": unit}


def _money_sum_objects(values: list[Any]) -> dict[str, Any] | None:
    monies = [value for value in values if isinstance(value, dict) and _number(value) is not None]
    if not monies:
        return None
    units = {money.get("unit") for money in monies}
    total = round(sum((_number(money) or 0.0 for money in monies), 0.0), 2)
    unit = next(iter(units)) if len(units) == 1 else None
    return {"value": _integer_if_integral(total), "unit": unit}


def _unwrap_credit_payload(data: Any, personal: bool) -> dict[str, Any]:
    queue = [_as_dict(data)]
    seen: set[int] = set()
    markers = {"basic_info", "loan_accounts", "credit_card_accounts"} if personal else {"report_meta", "report_basic", "credit_summary", "short_term_loans", "active_loans"}
    while queue:
        current = queue.pop(0)
        if not current or id(current) in seen:
            continue
        seen.add(id(current))
        if markers.intersection(current):
            return current
        for key in ("report_json", "extracted_json", "structured_data", "structured_json", "parsed_json", "data", "agent_result"):
            child = current.get(key)
            if isinstance(child, dict):
                queue.append(child)
    return {}


def _latest_credit_payload(materials: list[dict[str, Any]], category: str) -> dict[str, Any]:
    candidates = [item for item in materials if item.get("category") == category]
    candidates.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    for item in candidates:
        payload = _unwrap_credit_payload(item.get("structured_data"), personal=category == "个人征信")
        if payload:
            return payload
    return {}


def _dedupe_records(items: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for item in items:
        fingerprint = tuple(str(item.get(key) or "") for key in keys)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        result.append(item)
    return result


def _safe_field(record: dict[str, Any], *keys: str, max_length: int = 300) -> Any:
    key = next((candidate for candidate in keys if record.get(candidate) not in (None, "")), None)
    if not key:
        return None
    return _safe_business_scalar(record.get(key), key, max_length)


def _enterprise_credit_model(payload: dict[str, Any], generated_at: str | None = None) -> dict[str, Any]:
    agent = _as_dict(payload.get("agent_result"))
    meta = {**_as_dict(agent.get("report_meta")), **_as_dict(payload.get("report_meta")), **_as_dict(payload.get("report_basic"))}
    summary = {**_as_dict(agent.get("credit_summary")), **_as_dict(payload.get("credit_summary"))}
    document_unit = _declared_unit(payload, ("normalized_unit", "document_unit", "amount_unit", "currency_unit", "unit")) or _declared_unit(
        agent, ("normalized_unit", "document_unit", "amount_unit", "currency_unit", "unit")
    )
    loan_section_unit = _declared_unit(payload, ("loan_currency", "loan_amount_unit", "loan_unit"))
    guarantee_section_unit = _declared_unit(payload, ("guarantee_currency", "guarantee_amount_unit", "guarantee_unit"))
    raw_loans: list[dict[str, Any]] = []
    for source in (payload, agent):
        for key in ("active_loans", "short_loans_final", "short_loans", "medium_loans_final", "medium_loans", "long_term_loans", "revolving_loans", "short_term_loans", "medium_long_term_loans", "revolving_overdrafts"):
            raw_loans.extend(_as_list(source.get(key)))
    raw_loans = _dedupe_records(raw_loans, ("institution", "institution_name", "balance", "start_date", "end_date"))
    loans: list[dict[str, Any]] = []
    for index, item in enumerate(raw_loans, start=1):
        loans.append({
            "index": index,
            "institution": _safe_field(item, "institution", "institution_name", "bank"),
            "institution_type": _safe_field(item, "institution_type", "organization_type"),
            "loan_type": _safe_field(item, "business_type", "loan_type", "term_type"),
            "contract_amount": _money(item, "loan_amount", "amount", "credit_amount", section_unit=loan_section_unit, document_unit=document_unit),
            "balance": _money(item, "balance", "outstanding_balance", "current_balance", section_unit=loan_section_unit, document_unit=document_unit),
            "start_date": _safe_field(item, "start_date", "open_date"),
            "due_date": _safe_field(item, "end_date", "due_date", "maturity_date"),
            "due_date_assessment": _due_date_assessment(_safe_field(item, "end_date", "due_date", "maturity_date"), generated_at),
            "status": _safe_field(item, "status", "five_category", "five_classification"),
        })
    raw_external: list[dict[str, Any]] = []
    for source in (payload, agent):
        raw_external.extend(_as_list(source.get("external_guarantees")))
    external: list[dict[str, Any]] = []
    for item in _dedupe_records(raw_external, ("guaranteed_subject", "institution_name", "balance", "guarantee_amount")):
        external.append({
            "guaranteed_subject": _safe_field(item, "guaranteed_subject", "guarantee_for", "customer_name"),
            "institution": _safe_field(item, "institution", "institution_name"),
            "guarantee_amount": _money(item, "guarantee_amount", "amount", section_unit=guarantee_section_unit, document_unit=document_unit),
            "balance": _money(item, "guarantee_balance", "balance", section_unit=guarantee_section_unit, document_unit=document_unit),
            "guarantee_date": _safe_field(item, "guarantee_date", "start_date"),
            "status": _safe_field(item, "status", "five_category"),
        })
    overdue_records = []
    for item in raw_loans:
        overdue_months = _number(item.get("overdue_months"))
        status = str(_first(item.get("status"), item.get("five_category"), item.get("five_classification")) or "")
        if (overdue_months is not None and overdue_months > 0) or "逾期" in status:
            overdue_records.append({
                "institution": _safe_field(item, "institution", "institution_name"),
                "account_type": _safe_field(item, "business_type", "loan_type"),
                "current_status": _safe_field(item, "status", "five_category", "five_classification"),
                "overdue_months": int(overdue_months) if overdue_months is not None else None,
            })
    guarantee_key_present = "external_guarantee_balance" in summary or "guarantee_balance" in summary
    guarantee_balance = _first(summary.get("external_guarantee_balance"), summary.get("guarantee_balance"))
    return {
        "provided": bool(payload),
        "report_meta": {
            "customer_name": _safe_field(meta, "customer_name", "company_name", "enterprise_name"),
            "unified_social_credit_code": _safe_field(meta, "unified_social_credit_code"),
            "report_time": _safe_field(meta, "report_time", "report_date"),
        },
        "summary": {
            "unsettled_credit_balance": _money_from_value(
                _first(summary.get("unsettled_credit_balance"), summary.get("total_unsettled_balance")),
                summary,
                "unsettled_credit_balance",
                document_unit=document_unit,
            ) if _first(summary.get("unsettled_credit_balance"), summary.get("total_unsettled_balance")) is not None else None,
            "unsettled_credit_institution_count": _safe_business_scalar(
                summary.get("unsettled_credit_institution_count"), "unsettled_credit_institution_count"
            ),
        },
        "loans": loans,
        "external_guarantees": external,
        "external_guarantee_explicit": guarantee_key_present or bool(external),
        "external_guarantee_balance": (
            _money_from_value(guarantee_balance, summary, "external_guarantee_balance", section_unit=guarantee_section_unit, document_unit=document_unit)
            if guarantee_balance is not None
            else _money_sum(raw_external, "guarantee_balance", "balance")
        ),
        "overdue_records": overdue_records,
    }


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    match = re.search(r"((?:19|20)\d{2})[-./年](\d{1,2})[-./月](\d{1,2})", text)
    if not match:
        return None
    try:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _due_date_assessment(due_date: Any, generated_at: Any) -> str | None:
    due = _parse_date(due_date)
    generated = _parse_date(generated_at)
    if due is None or generated is None:
        return None
    due_text = due.strftime("%Y-%m-%d")
    if due.date() > generated.date():
        return f"到期日为{due_text}，截至本报告生成日尚未到期。"
    if due.date() == generated.date():
        return f"到期日为{due_text}，与本报告生成日为同一日。"
    return f"原到期日为{due_text}，早于本报告生成日；当前是否已结清、续贷或展期资料不足，需核实。"


def _query_matrix(records: list[dict[str, Any]], report_time: Any) -> tuple[list[dict[str, Any]], int | None, str]:
    windows = (("近1月", 1), ("近2月", 2), ("近3月", 3), ("近6月", 6), ("近9月", 9), ("近1年", 12), ("近2年", 24))
    if not records:
        return ([{"window": label, "loan_approval": "资料不足", "credit_card_approval": "资料不足", "guarantee_review": "资料不足", "legal_person_review": "资料不足"} for label, _ in windows], None, "资料不足")
    reference = _parse_date(report_time) or max((_parse_date(item.get("query_date")) for item in records), default=None)
    if reference is None:
        return ([{"window": label, "loan_approval": "资料不足", "credit_card_approval": "资料不足", "guarantee_review": "资料不足", "legal_person_review": "资料不足"} for label, _ in windows], None, "查询日期不足，无法按窗口统计")
    rows: list[dict[str, Any]] = []
    recent_loan: list[str] = []
    institution_query_6m = 0
    for label, months in windows:
        counts = {"loan_approval": 0, "credit_card_approval": 0, "guarantee_review": 0, "legal_person_review": 0}
        for item in records:
            query_date = _parse_date(item.get("query_date"))
            if not query_date:
                continue
            diff = (reference.year - query_date.year) * 12 + reference.month - query_date.month
            if diff < 0 or diff >= months:
                continue
            reason = str(_first(item.get("query_reason"), item.get("query_type")) or "")
            key = None
            if "贷款审批" in reason:
                key = "loan_approval"
            elif "信用卡审批" in reason:
                key = "credit_card_approval"
            elif "担保资格" in reason:
                key = "guarantee_review"
            elif "法人资信" in reason:
                key = "legal_person_review"
            if key:
                counts[key] += 1
            if months == 6 and key:
                institution_query_6m += 1
            if months == 6 and key == "loan_approval":
                date_value = _safe_field(item, "query_date") or "日期未识别"
                institution = _safe_field(item, "query_institution", "institution") or "机构未识别"
                recent_loan.append(f"{date_value} / {institution}")
        rows.append({"window": label, **counts})
    return rows, institution_query_6m, "；".join(dict.fromkeys(recent_loan)) if recent_loan else "未识别到近6个月贷款审批查询明细"


def _personal_credit_model(payload: dict[str, Any], responsible_subject: str, generated_at: str | None = None) -> dict[str, Any]:
    basic = _as_dict(payload.get("basic_info"))
    summary = _as_dict(payload.get("credit_summary"))
    raw_loans = _as_list(payload.get("loan_accounts"))
    raw_cards = _as_list(payload.get("credit_card_accounts"))
    raw_related = _as_list(payload.get("related_repayment_responsibilities"))
    document_unit = _declared_unit(payload, ("normalized_unit", "document_unit", "amount_unit", "currency_unit", "unit"))
    loan_section_unit = _declared_unit(payload, ("loan_currency", "loan_amount_unit", "loan_unit"))
    card_section_unit = _declared_unit(payload, ("credit_card_currency", "credit_card_amount_unit", "credit_card_unit"))
    related_section_unit = _declared_unit(payload, ("related_repayment_currency", "related_repayment_amount_unit", "related_repayment_unit"))
    loans = []
    for index, item in enumerate(raw_loans, start=1):
        loans.append({
            "index": index,
            "institution": _safe_field(item, "institution", "institution_name"),
            "institution_type": _safe_field(item, "institution_type"),
            "loan_type": _safe_field(item, "loan_type", "business_type"),
            "contract_amount": _money(item, "loan_amount", "amount", "issued_amount", section_unit=loan_section_unit, document_unit=document_unit),
            "balance": _money(item, "balance", "loan_balance", section_unit=loan_section_unit, document_unit=document_unit),
            "start_date": _safe_field(item, "start_date", "open_date"),
            "due_date": _safe_field(item, "due_date"),
            "due_date_assessment": _due_date_assessment(_safe_field(item, "due_date"), generated_at),
            "status": _safe_field(item, "account_status", "overdue_status", "five_category"),
        })
    cards = []
    for item in raw_cards:
        limit = _first(item.get("credit_limit"), item.get("limit"))
        used = _first(item.get("used_amount"), item.get("used_limit"), item.get("balance"))
        rate = None
        if (limit_num := _number(limit)) and (used_num := _number(used)) is not None:
            rate = f"{used_num / limit_num:.2%}"
        cards.append({
            "issuer": _safe_field(item, "issuer", "institution"),
            "currency": _safe_field(item, "currency"),
            "credit_limit": _money(item, "credit_limit", "limit", section_unit=card_section_unit, document_unit=document_unit),
            "used_amount": _money(item, "used_amount", "used_limit", "balance", section_unit=card_section_unit, document_unit=document_unit),
            "usage_rate": rate or "资料不足",
            "overdue": _safe_field(item, "overdue_description", "overdue_status", "overdue_amount"),
            "remark": _safe_field(item, "account_status"),
        })
    related = []
    for item in raw_related:
        related.append({
            "responsible_subject": _safe_business_scalar(responsible_subject or "暂未获取", "responsible_subject"),
            "related_party": _safe_field(item, "related_party"),
            "institution": _safe_field(item, "institution"),
            "responsibility_amount": _money(item, "responsibility_amount", section_unit=related_section_unit, document_unit=document_unit),
            "balance": _money(item, "loan_balance", "balance", section_unit=related_section_unit, document_unit=document_unit),
            "responsibility_type": _safe_field(item, "responsibility_type"),
            "business_type": _safe_field(item, "business_type"),
            "as_of_date": _safe_field(item, "as_of_date"),
        })
    query_records = [
        {
            "query_date": _safe_field(item, "query_date"),
            "query_institution": _safe_field(item, "query_institution", "institution"),
            "query_reason": _safe_field(item, "query_reason", "query_type"),
        }
        for item in _as_list(payload.get("query_records"))
    ]
    matrix, institution_query_6m, recent_loan = _query_matrix(query_records, basic.get("report_time"))
    overdue_records = []
    for item in _as_list(payload.get("overdue_records")):
        normalized_overdue = {
            "account_type": _safe_field(item, "account_type", "record_type", "business_type"),
            "institution": _safe_field(item, "institution", "issuer"),
            "current_status": _safe_field(item, "current_status", "overdue_status", "status"),
            "overdue_amount": _money(item, "overdue_amount", document_unit=document_unit),
            "overdue_months": _safe_field(item, "overdue_months"),
        }
        if normalized_overdue["institution"] and normalized_overdue["account_type"] and _record_has_current_overdue(item):
            overdue_records.append(normalized_overdue)
    loan_overdue_count = _number(summary.get("loan_overdue_account_count"))
    card_overdue_count = _number(summary.get("credit_card_overdue_account_count"))
    ninety_values = [
        _number(summary.get(key))
        for key in ("overdue_90_plus_account_count", "loan_90d_overdue_account_count", "credit_card_90d_overdue_account_count")
        if summary.get(key) is not None
    ]
    if loan_overdue_count == 0 and card_overdue_count == 0 and ninety_values and all(value == 0 for value in ninety_values):
        overdue_records = []
    public_records = []
    for item in _as_list(payload.get("public_records")):
        public_records.append({
            "record_type": _safe_field(item, "record_type", "type"),
            "status": _safe_field(item, "status"),
            "date": _safe_field(item, "date", "record_date"),
            "amount": _money(item, "amount", document_unit=document_unit),
            "content": _safe_field(item, "content", "description"),
        })
    non_credit_records = []
    for item in _as_list(payload.get("non_credit_transactions")):
        non_credit_records.append({
            "record_type": _safe_field(item, "record_type", "type"),
            "status": _safe_field(item, "status"),
            "date": _safe_field(item, "date", "record_date"),
            "amount": _money(item, "amount", document_unit=document_unit),
            "content": _safe_field(item, "content", "description"),
        })
    return {
        "provided": bool(payload),
        "basic_info": {
            key: _safe_business_scalar(value, key)
            for key, value in basic.items()
            if key in {"name", "id_type", "id_number", "report_number", "report_time", "marital_status"}
        },
        "summary": {
            key: _safe_business_scalar(value, key) for key, value in summary.items()
            if key in {
                "credit_card_account_count", "active_credit_card_account_count", "loan_account_count",
                "outstanding_loan_account_count", "credit_card_overdue_account_count",
                "credit_card_90d_overdue_account_count", "loan_overdue_account_count",
                "loan_90d_overdue_account_count", "personal_related_repayment_responsibility_account_count",
                "overdue_90_plus_account_count",
                "enterprise_related_repayment_responsibility_account_count",
            }
        },
        "loans": loans,
        "credit_cards": cards,
        "related_repayment_responsibilities": related,
        "overdue_records": overdue_records,
        "overdue_summary": {
            "loan_overdue_account_count": _safe_business_scalar(summary.get("loan_overdue_account_count"), "loan_overdue_account_count"),
            "credit_card_overdue_account_count": _safe_business_scalar(summary.get("credit_card_overdue_account_count"), "credit_card_overdue_account_count"),
            "loan_90d_overdue_account_count": _safe_business_scalar(summary.get("loan_90d_overdue_account_count"), "loan_90d_overdue_account_count"),
            "credit_card_90d_overdue_account_count": _safe_business_scalar(summary.get("credit_card_90d_overdue_account_count"), "credit_card_90d_overdue_account_count"),
            "overdue_90_plus_account_count": _safe_business_scalar(summary.get("overdue_90_plus_account_count"), "overdue_90_plus_account_count"),
        },
        "public_records": public_records,
        "non_credit_transactions": non_credit_records,
        "query_records": query_records,
        "query_matrix": matrix,
        "institution_query_6m_count": institution_query_6m,
        "recent_loan_approval_queries": recent_loan,
    }


def _walk_key_values(value: Any) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            pairs.append((str(key), child))
            pairs.extend(_walk_key_values(child))
    elif isinstance(value, list):
        for child in value:
            pairs.extend(_walk_key_values(child))
    return pairs


def _find_identifier(materials: list[dict[str, Any]], keys: tuple[str, ...]) -> Any:
    for material in materials:
        for key, value in _walk_key_values(material.get("structured_data")):
            if key.lower() in keys and value not in (None, "", [], {}):
                return value
    return None


def _relation_names(value: Any) -> set[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        return {cleaned} if cleaned else set()
    if isinstance(value, dict):
        names: set[str] = set()
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", str(key).lower())
            if normalized in {"name", "fullname", "personname", "姓名", "名称"}:
                names.update(_relation_names(child))
        return names
    if isinstance(value, list):
        names: set[str] = set()
        for child in value:
            names.update(_relation_names(child))
        return names
    return set()


def _derive_subjects(customer: dict[str, Any], materials: list[dict[str, Any]], enterprise_payload: dict[str, Any], personal_payload: dict[str, Any]) -> dict[str, Any]:
    enterprise_meta = {**_as_dict(_as_dict(enterprise_payload.get("agent_result")).get("report_meta")), **_as_dict(enterprise_payload.get("report_meta")), **_as_dict(enterprise_payload.get("report_basic"))}
    personal_basic = _as_dict(personal_payload.get("basic_info"))
    enterprise_subject = _first(enterprise_meta.get("customer_name"), enterprise_meta.get("company_name"), enterprise_meta.get("enterprise_name"))
    personal_subject = personal_basic.get("name")
    relation_sources = [item for item in materials if item.get("category") in {"企业征信", "KYC及主体关系"}]
    role_names: dict[str, set[str]] = {"法定代表人": set(), "实际控制人": set()}
    if personal_subject:
        for material in relation_sources:
            structured = material.get("structured_data")
            for key, value in _walk_key_values(structured):
                normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", key.lower())
                names = _relation_names(value)
                if not names and isinstance(value, str) and value.strip():
                    names = {value.strip()}
                if normalized in {"legalrepresentative", "legalperson", "legalrepresentativename", "legalpersonname", "法人", "法人姓名", "法定代表人", "法定代表人姓名"}:
                    role_names["法定代表人"].update(names)
                if normalized in {"actualcontroller", "actualcontrollername", "controller", "实际控制人", "实际控制人姓名"}:
                    role_names["实际控制人"].update(names)
    subject_name = str(personal_subject or "").strip()
    relation_conflict = any(names and names != {subject_name} for names in role_names.values())
    relation_complete = all(role_names[role] for role in ("法定代表人", "实际控制人"))
    roles = [role for role in ("法定代表人", "实际控制人") if role_names[role] == {subject_name}]
    enterprise_identifier = _first(
        enterprise_meta.get("unified_social_credit_code"),
        _find_identifier(materials, ("unified_social_credit_code", "social_credit_code", "credit_code")),
    )
    personal_identifier = _first(personal_basic.get("id_number"), customer.get("id_card"))
    return {
        "customer_subject": _safe_business_scalar(customer.get("name") or "暂未获取", "customer_subject"),
        "enterprise_credit_subject": _safe_business_scalar(enterprise_subject or "暂未获取", "enterprise_credit_subject"),
        "personal_credit_subject": _safe_business_scalar(personal_subject or "暂未获取", "personal_credit_subject"),
        "personal_credit_subject_role": " / ".join(roles) if relation_complete and not relation_conflict else "需人工核实",
        "enterprise_identifier_masked": _mask_identifier(enterprise_identifier),
        "personal_identifier_masked": _mask_identifier(personal_identifier),
    }


def _record_has_current_overdue(record: dict[str, Any]) -> bool:
    amount = _number(record.get("overdue_amount"))
    if amount is not None and amount > 0:
        return True
    statuses = {
        re.sub(r"\s+", "", str(record.get(key) or ""))
        for key in ("current_status", "overdue_status", "account_status", "status")
    }
    return bool(statuses.intersection({"逾期", "当前逾期", "当前存在逾期"}))


def _overdue_conflicts(personal_payload: dict[str, Any], summary: dict[str, Any]) -> list[str]:
    conflicts: list[str] = []
    loan_summary = _number(summary.get("loan_overdue_account_count"))
    card_summary = _number(summary.get("credit_card_overdue_account_count"))
    loan_current = any(_record_has_current_overdue(item) for item in _as_list(personal_payload.get("loan_accounts")))
    card_current = any(_record_has_current_overdue(item) for item in _as_list(personal_payload.get("credit_card_accounts")))
    if loan_summary == 0 and loan_current:
        conflicts.append("贷款逾期账户数=0，但贷款明细存在当前逾期状态")
    if card_summary == 0 and card_current:
        conflicts.append("信用卡逾期账户数=0，但信用卡明细存在当前逾期状态")
    return conflicts


def build_credit_report_model(material_result: dict[str, Any], generated_at: str | None = None) -> dict[str, Any]:
    materials = material_result.get("materials") if isinstance(material_result.get("materials"), list) else []
    customer = material_result.get("customer") or {}
    enterprise_payload = _latest_credit_payload(materials, "企业征信")
    personal_payload = _latest_credit_payload(materials, "个人征信")
    subjects = _derive_subjects(customer, materials, enterprise_payload, personal_payload)
    enterprise = _enterprise_credit_model(enterprise_payload, generated_at)
    personal = _personal_credit_model(personal_payload, subjects.get("personal_credit_subject") or "", generated_at)
    es = enterprise.get("summary") or {}
    ps = personal.get("summary") or {}
    personal_loan_balance = _money_sum_objects([item.get("balance") for item in personal.get("loans") or []])
    card_limit = _money_sum_objects([item.get("credit_limit") for item in personal.get("credit_cards") or []])
    card_used = _money_sum_objects([item.get("used_amount") for item in personal.get("credit_cards") or []])
    loan_overdue = ps.get("loan_overdue_account_count")
    card_overdue = ps.get("credit_card_overdue_account_count")
    explicit_90 = ps.get("overdue_90_plus_account_count")
    loan_90 = ps.get("loan_90d_overdue_account_count")
    card_90 = ps.get("credit_card_90d_overdue_account_count")
    overdue_90 = _number(explicit_90)
    if overdue_90 is None:
        overdue_90 = None if loan_90 is None and card_90 is None else (_number(loan_90) or 0) + (_number(card_90) or 0)
    enterprise_balance_value = _first(es.get("unsettled_credit_balance"), es.get("total_unsettled_balance"))
    enterprise_balance = (
        enterprise_balance_value
        if isinstance(enterprise_balance_value, dict)
        else _money_from_value(enterprise_balance_value, es, "unsettled_credit_balance")
        if enterprise_balance_value is not None
        else _money_sum_objects([item.get("balance") for item in enterprise.get("loans") or []])
    )
    card_limit_number = _number(card_limit)
    card_used_number = _number(card_used)
    overdue_conflicts = _overdue_conflicts(personal_payload, ps)
    card_summary_count = ps.get("credit_card_account_count")
    card_detail_count = len(personal.get("credit_cards") or [])
    card_count_mismatch = (
        _number(card_summary_count) is not None
        and int(_number(card_summary_count) or 0) != card_detail_count
    )
    metrics = {
        "enterprise_unsettled_loan_balance": enterprise_balance,
        "enterprise_unsettled_loan_institution_count": es.get("unsettled_credit_institution_count"),
        "personal_unsettled_loan_balance": personal_loan_balance,
        "personal_unsettled_loan_account_count": _first(ps.get("outstanding_loan_account_count"), len(personal.get("loans") or []) or None),
        "personal_credit_card_limit": card_limit,
        "personal_credit_card_used": card_used,
        "credit_card_usage_rate": f"{card_used_number / card_limit_number:.2%}" if card_limit_number and card_used_number is not None else None,
        "loan_overdue_account_count": loan_overdue,
        "credit_card_overdue_account_count": card_overdue,
        "overdue_90d_account_count": int(overdue_90) if overdue_90 is not None else None,
        "enterprise_external_guarantee_balance": enterprise.get("external_guarantee_balance"),
        "enterprise_external_guarantee_explicit": bool(enterprise.get("external_guarantee_explicit")),
        "personal_related_repayment_balance": _money_sum_objects([item.get("balance") for item in personal.get("related_repayment_responsibilities") or []]),
        "institution_query_6m_count": personal.get("institution_query_6m_count"),
        "dti": None,
        "online_loan_count": None,
        "large_revolving_due_concentration": None,
        "credit_card_account_count": ps.get("credit_card_account_count"),
        "credit_card_detail_count": card_detail_count,
        "credit_card_count_mismatch": card_count_mismatch,
        "overdue_data_conflicts": overdue_conflicts,
    }
    model = {
        "subjects": subjects,
        "dates": {
            "generated_at": generated_at,
            "enterprise_source_report_date": (enterprise.get("report_meta") or {}).get("report_time"),
            "personal_source_report_date": (personal.get("basic_info") or {}).get("report_time"),
        },
        "enterprise_credit": enterprise,
        "personal_credit": personal,
        "metrics": metrics,
    }
    model["rule_checks"] = evaluate_credit_report_rules(model)
    material_result["subjects"] = subjects
    material_result["report_model"] = model
    return model


NARRATIVE_SYSTEM_PROMPT = """你是一名融资资料分析助手。只根据传入的 credit_one_page_report_v1 结构化事实生成简短叙述 JSON。
禁止生成 Markdown 表格。禁止修改、补充或重新判断 rule_checks 的状态。禁止自行创造查询、DTI、网贷、信用卡使用率等阈值。
禁止引用财务报表、资产负债率、营业收入、应收账款、利润、经营现金流、企业流水、个人流水、房产。
企业贷款与个人贷款必须分开；个人信用卡不得描述为企业信用卡；法人相关还款责任不得描述为企业对外担保。
必须区分 source_report_date 与 generated_at。源征信查询窗口只相对于源征信报告日期。禁止把早于 generated_at 的到期日描述为“将于到期”“即将到期”或“距报告时间较近”。
禁止引用不可靠、要素缺失、疑似或关键词匹配得到的逾期条目；逾期结论只能使用传入的稳定结构化概要和可靠明细。
资料不足时写“资料不足”，可信字段矛盾时写“待核验”。不得输出审批概率、额度、利率或放款承诺。
只返回 JSON 对象，键必须为：emergency_attention, query_frequency_analysis, historical_credit_features, optimization_urgent, optimization_medium, optimization_long, advantages, risks, comprehensive_summary, one_sentence_conclusion。
除四个说明字段外，其余字段均为字符串数组。"""


def _default_narrative(report_model: dict[str, Any]) -> dict[str, Any]:
    rules = report_model.get("rule_checks") or []
    risks = [f"{item['item']}：{item['current']}" for item in rules if item.get("status") in {"风险", "关注", "超标"}]
    strengths = [f"{item['item']}：{item['current']}" for item in rules if item.get("status") == "达标"]
    urgent = [item.get("direction") for item in rules if item.get("status") in {"风险", "待核验"} and item.get("direction")]
    medium = [item.get("direction") for item in rules if item.get("status") in {"关注", "待评估"} and item.get("direction")]
    long_term = [item.get("direction") for item in rules if item.get("status") == "资料不足" and item.get("direction")]
    personal = report_model.get("personal_credit") or {}
    enterprise = report_model.get("enterprise_credit") or {}
    if personal.get("related_repayment_responsibilities"):
        risks.append("法人个人征信存在相关还款责任，需与企业对外担保分口径核验。")
        urgent.append("核验法人相关还款责任的责任类型、当前余额及关联主体。")
    if enterprise.get("external_guarantee_explicit") and _number(enterprise.get("external_guarantee_balance")) == 0:
        strengths.append("企业征信明确记录企业对外担保余额为0。")
    features = [
        f"企业未结清贷款记录：{len(enterprise.get('loans') or [])}条" if enterprise.get("provided") else "企业征信资料不足",
        f"个人未结清贷款记录：{len(personal.get('loans') or [])}条" if personal.get("provided") else "个人征信资料不足",
        f"个人信用卡记录：{len(personal.get('credit_cards') or [])}条" if personal.get("provided") else "个人信用卡资料不足",
    ]
    return {
        "emergency_attention": risks or ["暂无需要立即处理的明确风险事项。"],
        "query_frequency_analysis": "当前值仅作事实展示；未配置统一银行准入阈值的项目均为待评估。",
        "historical_credit_features": features,
        "optimization_urgent": list(dict.fromkeys(item for item in urgent if item)),
        "optimization_medium": list(dict.fromkeys(item for item in medium if item)),
        "optimization_long": list(dict.fromkeys(item for item in long_term if item))[:4],
        "advantages": strengths,
        "risks": risks,
        "comprehensive_summary": "本报告仅基于已保存的企业征信、个人征信及必要主体关系资料，缺失项目未作推断。",
        "one_sentence_conclusion": "需结合明确征信事实及具体银行正式准入规则进一步人工评估。",
    }


def _narrative_has_unsafe_value(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_narrative_has_unsafe_value(child) for child in value.values())
    if isinstance(value, list):
        return any(_narrative_has_unsafe_value(child) for child in value)
    if not isinstance(value, str):
        return False
    return (
        len(value) > 300
        or value.count("\n") > 3
        or any(pattern.search(value) for pattern in _ABNORMAL_TEXT_PATTERNS)
    )


def _merge_required_narrative(parsed: dict[str, Any], report_model: dict[str, Any]) -> dict[str, Any]:
    default = _default_narrative(report_model)
    for key in default:
        if key not in parsed or not isinstance(parsed[key], type(default[key])):
            parsed[key] = default[key]
    for key in ("optimization_urgent", "optimization_medium", "optimization_long", "risks", "advantages"):
        parsed[key] = list(dict.fromkeys([*default[key], *parsed[key]]))
    return parsed


def _parse_narrative(raw: str, report_model: dict[str, Any]) -> dict[str, Any]:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", str(raw or "").strip(), flags=re.IGNORECASE)
    try:
        parsed = json.loads(text)
    except Exception:
        return _default_narrative(report_model)
    if not isinstance(parsed, dict):
        return _default_narrative(report_model)
    forbidden = re.compile(
        r"资产负债率|营业收入|收入下滑|应收账款|经营现金流|财务报表|企业流水|个人流水|房产|审批概率|大概率通过|<br\s*/?>"
        r"|将于.{0,30}到期|即将到期|临近到期|距.{0,30}(?:到期|报告时间).{0,20}(?:较近|临近)"
        r"|要素缺失.{0,20}逾期|逾期.{0,20}要素缺失|疑似逾期|逾期条目"
        r"|\b(?:needs_review|raw_text|ocr_text|normalized|internal_status)\b",
        re.IGNORECASE,
    )
    rendered = json.dumps(parsed, ensure_ascii=False)
    if forbidden.search(rendered):
        return _default_narrative(report_model)
    source_numbers = set(re.findall(r"\d+(?:\.\d+)?", json.dumps(report_model, ensure_ascii=False, default=str)))
    output_numbers = set(re.findall(r"\d+(?:\.\d+)?", rendered))
    if not output_numbers.issubset(source_numbers):
        return _default_narrative(report_model)
    if _narrative_has_unsafe_value(parsed):
        return _default_narrative(report_model)
    return _merge_required_narrative(parsed, report_model)


async def generate_credit_one_page_report(
    storage_service: Any,
    ai_service: AIService,
    message: str,
    selected_customer_id: str | None = None,
) -> dict[str, Any]:
    """Complete report flow with business-safe failures and no writes."""
    try:
        resolved = await resolve_report_customer(storage_service, message, selected_customer_id)
    except Exception as exc:
        logger.exception("report customer resolution failed: %s", exc)
        return {"message": "读取客户信息失败，请稍后重试。", "data": {"reportStatus": "tool_failed"}}
    status = resolved.get("status")
    if status == "missing_context":
        return {
            "message": "请先选择需要生成报告的客户，或输入“根据客户名称的资料生成征信一页纸”。",
            "data": {"reportStatus": "waiting_for_input"},
        }
    if status == "customer_not_found":
        requested = resolved.get("requested_name")
        suffix = f"「{requested}」" if requested else "该客户"
        return {"message": f"未找到{suffix}的客户资料，请核对客户名称或先选择客户。", "data": {"reportStatus": "customer_not_found"}}
    if status == "ambiguous":
        choice_lines = []
        for index, item in enumerate(resolved.get("choices") or [], start=1):
            choice_lines.append(
                f"{index}. {item['name']}（{item['customer_type']}，联系方式 {item['phone_masked']}，更新 {item['updated_at']}）"
            )
        return {
            "message": "找到多个同名客户，请选择需要生成报告的客户。\n\n" + "\n".join(choice_lines),
            "data": {"reportStatus": "waiting_for_input", "reason": "duplicate_customer_name"},
        }

    customer = resolved.get("customer") or {}
    customer_id = str(customer.get("customer_id") or "")
    try:
        materials = await get_customer_materials(storage_service, customer_id)
    except Exception as exc:
        logger.exception("get_customer_materials failed: %s", exc)
        return {"message": "读取客户资料失败，请稍后重试。", "data": {"reportStatus": "tool_failed"}}

    if materials.get("status") != "ok":
        return {"message": "未找到该客户的已保存资料，请核对客户后重试。", "data": {"reportStatus": "customer_not_found"}}
    if not materials.get("materials"):
        if materials.get("processing"):
            message_text = "该客户资料仍在解析中，暂时没有可用于生成报告的已保存结果，请稍后再试。"
            report_status = "materials_processing"
        else:
            message_text = "该客户暂无已成功解析并保存的资料，暂时无法生成征信速览报告。"
            report_status = "no_materials"
        return {"message": message_text, "data": {"reportStatus": report_status}}

    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    report_model = build_credit_report_model(materials, generated_at)
    context = build_report_context(materials)
    user_prompt = f"""以下内容已经由程序按企业征信、个人征信和主体关系分离。请只生成叙述 JSON，不要生成表格或重新分类责任口径。

<subject_context>
{context}
</subject_context>

<structured_credit_facts_and_rule_results>
{_safe_json(report_model, MAX_REPORT_CONTEXT_CHARS)}
</structured_credit_facts_and_rule_results>
"""
    try:
        raw_narrative = await asyncio.to_thread(
            ai_service.extract,
            NARRATIVE_SYSTEM_PROMPT,
            user_prompt,
            "deepseek-chat",
            120,
            4096,
        )
    except Exception as exc:
        logger.exception("credit one-page report model call failed: %s", exc)
        return {"message": "征信速览报告生成失败，请稍后重试。", "data": {"reportStatus": "model_failed"}}

    narrative = _parse_narrative(raw_narrative, report_model)
    report = _strip_internal_output(render_credit_one_page_report(report_model, narrative, generated_at))
    if not report or not _has_complete_structure(report) or _has_unsafe_report_text(report):
        logger.warning("credit one-page report failed structure validation")
        return {
            "message": "报告生成结果不完整，系统未向您展示可能缺失关键信息的内容，请稍后重试。",
            "data": {"reportStatus": "model_failed", "reason": "invalid_structure"},
        }
    return {
        "message": report,
        "data": {
            "reportStatus": "completed",
            "reportType": CREDIT_REPORT_INTENT,
            "templateVersion": "credit_one_page_report_v1",
            "materialsProcessing": bool(materials.get("processing")),
        },
    }
