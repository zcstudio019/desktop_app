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


def _extract_material_text(extraction: dict[str, Any]) -> str:
    data = extraction.get("confirmed_data") or extraction.get("extracted_data") or {}
    if not isinstance(data, dict):
        return _safe_json(data)
    for key in ("markdown", "markdown_summary", "report_markdown", "summary_markdown", "content_markdown"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return _mask_identifiers_in_text(value.strip()[:MAX_SINGLE_MATERIAL_CHARS])
    return _safe_json(data)


async def get_customer_materials(storage_service: Any, customer_id: str) -> dict[str, Any]:
    """High-level read-only Tool: load an existing customer's persisted materials."""
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        return {"status": "customer_not_found", "customer": None, "materials": [], "processing": False}

    profile, extractions, documents = await asyncio.gather(
        storage_service.get_customer_profile(customer_id),
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
                "content": _extract_material_text(item),
                "structured_data": item.get("confirmed_data") or item.get("extracted_data") or {},
                "created_at": str(item.get("created_at") or ""),
            }
        )

    profile_markdown = ""
    if isinstance(profile, dict):
        profile_markdown = _mask_identifiers_in_text(str(profile.get("markdown_content") or "").strip())

    return {
        "status": "ok",
        "customer": {
            "name": customer.get("name") or "暂未获取",
            "customer_type": customer.get("customer_type") or "暂未获取",
            "id_card_masked": _mask_identifier(customer.get("id_card")),
            "phone_masked": _mask_identifier(customer.get("phone")),
        },
        "profile_markdown": profile_markdown,
        "materials": materials,
        "document_count": len(documents),
        "successful_material_count": len(successful),
        "processing": processing,
    }


def _split_markdown_sections(markdown: str) -> list[str]:
    if not markdown.strip():
        return []
    chunks = re.split(r"(?=^#{1,4}\s+)", markdown, flags=re.MULTILINE)
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def _section_category(section: str) -> str:
    heading = section.splitlines()[0].lower() if section else ""
    if "个人征信" in heading or "个人信用" in heading:
        return "个人征信"
    if "企业征信" in heading or "企业信用" in heading:
        return "企业征信"
    if any(word in heading for word in ("客户基础", "身份证", "营业执照", "公司章程", "关联企业", "主体关系")):
        return "KYC及主体关系"
    return "其他资料"


def build_report_context(material_result: dict[str, Any]) -> str:
    """Build the narrow credit-report context; finance/flow/property are excluded."""
    customer = material_result.get("customer") or {}
    subjects = material_result.get("subjects") or {}
    base = {
        "customer_subject": subjects.get("customer_subject") or customer.get("name") or "暂未获取",
        "enterprise_credit_subject": subjects.get("enterprise_credit_subject") or "暂未获取",
        "personal_credit_subject": subjects.get("personal_credit_subject") or "暂未获取",
        "personal_credit_subject_role": subjects.get("personal_credit_subject_role") or "需人工核实",
        "customer_type": customer.get("customer_type") or "暂未获取",
    }
    buckets: dict[str, list[str]] = {name: [] for name, _ in _TYPE_GROUPS}

    # Profile Markdown may contain finance, flow and property sections. Only copy
    # explicitly allowed headings; never pass the whole profile to this report.
    for section in _split_markdown_sections(str(material_result.get("profile_markdown") or "")):
        category = _section_category(section)
        if category != "其他资料":
            buckets[category].append(section)
    for material in material_result.get("materials") or []:
        if not isinstance(material, dict):
            continue
        category = str(material.get("category") or "其他资料")
        if category not in buckets:
            continue
        content = str(material.get("content") or "").strip()
        if content:
            buckets.setdefault(category, []).append(content)

    # Credit evidence receives most of the context budget. Other categories are
    # deliberately smaller to prevent large transaction tables from overflowing.
    category_limits = {
        "个人征信": 18_000,
        "企业征信": 16_000,
        "KYC及主体关系": 5_000,
    }
    parts = ["## 客户基本信息\n" + _safe_json(base, 3_000)]
    for category in ("企业征信", "个人征信", "KYC及主体关系"):
        unique: list[str] = []
        seen: set[str] = set()
        for raw in buckets.get(category) or []:
            compact = raw.strip()
            fingerprint = compact[:300]
            if not compact or fingerprint in seen:
                continue
            seen.add(fingerprint)
            unique.append(compact)
        joined = "\n\n".join(unique)
        if joined:
            parts.append(f"## {category}\n{joined[:category_limits[category]]}")
        else:
            parts.append(f"## {category}\n未提供或未解析")

    processing_note = "有资料仍在解析，当前报告仅基于已成功保存的结果。" if material_result.get("processing") else "未发现仍在解析的资料。"
    parts.append(f"## 资料状态\n{processing_note}")
    return _mask_identifiers_in_text("\n\n".join(parts))[:MAX_REPORT_CONTEXT_CHARS]


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
    return cleaned.strip()


def _has_complete_structure(report: str) -> bool:
    return all(heading in report for heading in _REQUIRED_HEADINGS)


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


def _display_number(value: Any) -> Any:
    if value is None or value == "":
        return "资料不足"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _display_amount(record: dict[str, Any], *keys: str) -> Any:
    """Preserve a source-provided currency unit without inventing one."""
    value = _first(*(record.get(key) for key in keys))
    displayed = _display_number(value)
    if displayed == "资料不足" or re.search(r"元|万元|币", str(displayed)):
        return displayed
    evidence = str(record.get("evidence") or record.get("source_text") or "")
    if "万元" in evidence:
        return f"{displayed}万元"
    if "人民币元" in evidence or re.search(r"(?:金额|余额|本金)[^。；\n]{0,30}元", evidence):
        return f"{displayed}元"
    return displayed


def _display_amount_sum(records: list[dict[str, Any]], *keys: str) -> Any:
    total = _sum_known([_first(*(record.get(key) for key in keys)) for record in records])
    displayed = _display_number(total)
    if displayed == "资料不足":
        return displayed
    rendered = [_display_amount(record, *keys) for record in records]
    if rendered and all(str(value).endswith("万元") for value in rendered):
        return f"{displayed}万元"
    if rendered and all(str(value).endswith("元") and not str(value).endswith("万元") for value in rendered):
        return f"{displayed}元"
    return displayed


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


def _enterprise_credit_model(payload: dict[str, Any]) -> dict[str, Any]:
    agent = _as_dict(payload.get("agent_result"))
    meta = {**_as_dict(agent.get("report_meta")), **_as_dict(payload.get("report_meta")), **_as_dict(payload.get("report_basic"))}
    summary = {**_as_dict(agent.get("credit_summary")), **_as_dict(payload.get("credit_summary"))}
    raw_loans: list[dict[str, Any]] = []
    for source in (payload, agent):
        for key in ("active_loans", "short_loans_final", "short_loans", "medium_loans_final", "medium_loans", "long_term_loans", "revolving_loans", "short_term_loans", "medium_long_term_loans", "revolving_overdrafts"):
            raw_loans.extend(_as_list(source.get(key)))
    raw_loans = _dedupe_records(raw_loans, ("institution", "institution_name", "balance", "start_date", "end_date"))
    loans: list[dict[str, Any]] = []
    for index, item in enumerate(raw_loans, start=1):
        loans.append({
            "index": index,
            "institution": _first(item.get("institution"), item.get("institution_name"), item.get("bank")),
            "institution_type": _first(item.get("institution_type"), item.get("organization_type")),
            "loan_type": _first(item.get("business_type"), item.get("loan_type"), item.get("term_type")),
            "contract_amount": _display_amount(item, "loan_amount", "amount", "credit_amount"),
            "balance": _display_amount(item, "balance", "outstanding_balance", "current_balance"),
            "start_date": _first(item.get("start_date"), item.get("open_date")),
            "due_date": _first(item.get("end_date"), item.get("due_date"), item.get("maturity_date")),
            "status": _first(item.get("status"), item.get("five_category"), item.get("five_classification")),
        })
    raw_external: list[dict[str, Any]] = []
    for source in (payload, agent):
        raw_external.extend(_as_list(source.get("external_guarantees")))
    external: list[dict[str, Any]] = []
    for item in _dedupe_records(raw_external, ("guaranteed_subject", "institution_name", "balance", "guarantee_amount")):
        external.append({
            "guaranteed_subject": _first(item.get("guaranteed_subject"), item.get("guarantee_for"), item.get("customer_name")),
            "institution": _first(item.get("institution"), item.get("institution_name")),
            "guarantee_amount": _display_amount(item, "guarantee_amount", "amount"),
            "balance": _display_amount(item, "guarantee_balance", "balance"),
            "guarantee_date": _first(item.get("guarantee_date"), item.get("start_date")),
            "status": _first(item.get("status"), item.get("five_category")),
        })
    abnormal = _as_list(payload.get("abnormal_records")) or _as_list(agent.get("abnormal_records"))
    overdue_records = []
    for item in raw_loans:
        overdue_months = _number(item.get("overdue_months"))
        status = str(_first(item.get("status"), item.get("five_category"), item.get("five_classification")) or "")
        if (overdue_months is not None and overdue_months > 0) or "逾期" in status:
            overdue_records.append({"description": f"{_first(item.get('institution'), item.get('institution_name')) or '机构未识别'}：{status or f'逾期月数{int(overdue_months or 0)}'}"})
    guarantee_key_present = "external_guarantee_balance" in summary or "guarantee_balance" in summary
    guarantee_balance = _first(summary.get("external_guarantee_balance"), summary.get("guarantee_balance"))
    return {
        "provided": bool(payload),
        "report_meta": meta,
        "summary": summary,
        "loans": loans,
        "external_guarantees": external,
        "external_guarantee_explicit": guarantee_key_present or bool(external),
        "external_guarantee_balance": guarantee_balance if guarantee_balance is not None else _sum_known([item.get("balance") for item in raw_external]),
        "overdue_records": overdue_records,
        "abnormal_records": abnormal,
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


def _query_matrix(records: list[dict[str, Any]], report_time: Any) -> tuple[list[dict[str, Any]], int | None, str]:
    windows = (("近1月", 1), ("近2月", 2), ("近3月", 3), ("近6月", 6), ("近9月", 9), ("近1年", 12), ("近2年", 24))
    if not records:
        return ([{"window": label, "loan_approval": "资料不足", "credit_card_approval": "资料不足", "guarantee_review": "资料不足", "legal_person_review": "资料不足"} for label, _ in windows], None, "资料不足")
    reference = _parse_date(report_time) or max((_parse_date(item.get("query_date")) for item in records), default=None)
    if reference is None:
        return ([{"window": label, "loan_approval": "资料不足", "credit_card_approval": "资料不足", "guarantee_review": "资料不足", "legal_person_review": "资料不足"} for label, _ in windows], None, "查询日期不足，无法按窗口统计")
    rows: list[dict[str, Any]] = []
    recent_loan: list[str] = []
    hard_6m = 0
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
                hard_6m += 1
            if months == 6 and key == "loan_approval":
                recent_loan.append(f"{item.get('query_date') or '日期未识别'} / {item.get('query_institution') or '机构未识别'}")
        rows.append({"window": label, **counts})
    return rows, hard_6m, "；".join(dict.fromkeys(recent_loan)) if recent_loan else "未识别到近6个月贷款审批查询明细"


def _personal_credit_model(payload: dict[str, Any], responsible_subject: str) -> dict[str, Any]:
    basic = _as_dict(payload.get("basic_info"))
    summary = _as_dict(payload.get("credit_summary"))
    raw_loans = _as_list(payload.get("loan_accounts"))
    raw_cards = _as_list(payload.get("credit_card_accounts"))
    raw_related = _as_list(payload.get("related_repayment_responsibilities"))
    loans = []
    for index, item in enumerate(raw_loans, start=1):
        loans.append({
            "index": index,
            "institution": _first(item.get("institution"), item.get("institution_name")),
            "institution_type": item.get("institution_type"),
            "loan_type": _first(item.get("loan_type"), item.get("business_type")),
            "contract_amount": _display_amount(item, "loan_amount", "amount", "issued_amount"),
            "balance": _display_amount(item, "balance", "loan_balance"),
            "start_date": _first(item.get("start_date"), item.get("open_date")),
            "due_date": item.get("due_date"),
            "status": _first(item.get("account_status"), item.get("overdue_status"), item.get("five_category")),
        })
    cards = []
    for item in raw_cards:
        limit = _first(item.get("credit_limit"), item.get("limit"))
        used = _first(item.get("used_amount"), item.get("used_limit"), item.get("balance"))
        rate = None
        if (limit_num := _number(limit)) and (used_num := _number(used)) is not None:
            rate = f"{used_num / limit_num:.2%}"
        cards.append({
            "issuer": _first(item.get("issuer"), item.get("institution")),
            "currency": item.get("currency"),
            "credit_limit": _display_number(limit),
            "used_amount": _display_number(used),
            "usage_rate": rate or "资料不足",
            "overdue": _first(item.get("overdue_description"), item.get("overdue_status"), item.get("overdue_amount")),
            "remark": item.get("account_status"),
        })
    related = []
    for item in raw_related:
        related.append({
            "responsible_subject": responsible_subject or "暂未获取",
            "related_party": item.get("related_party"),
            "institution": item.get("institution"),
            "responsibility_amount": _display_amount(item, "responsibility_amount"),
            "balance": _display_amount(item, "loan_balance", "balance"),
            "responsibility_type": item.get("responsibility_type"),
            "business_type": item.get("business_type"),
            "as_of_date": item.get("as_of_date"),
        })
    query_records = _as_list(payload.get("query_records"))
    matrix, hard_6m, recent_loan = _query_matrix(query_records, basic.get("report_time"))
    return {
        "provided": bool(payload),
        "basic_info": basic,
        "summary": summary,
        "loans": loans,
        "credit_cards": cards,
        "related_repayment_responsibilities": related,
        "guarantees": _as_list(payload.get("guarantees")),
        "overdue_records": [
            {"description": "；".join(str(value) for value in item.values() if value not in (None, ""))}
            for item in _as_list(payload.get("overdue_records"))
        ],
        "public_records": _as_list(payload.get("public_records")),
        "non_credit_transactions": _as_list(payload.get("non_credit_transactions")),
        "query_records": query_records,
        "query_matrix": matrix,
        "hard_query_6m_count": hard_6m,
        "recent_loan_approval_queries": recent_loan,
        "indicators": _as_dict(payload.get("personal_credit_indicators")),
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


def _derive_subjects(customer: dict[str, Any], materials: list[dict[str, Any]], enterprise_payload: dict[str, Any], personal_payload: dict[str, Any]) -> dict[str, Any]:
    enterprise_meta = {**_as_dict(_as_dict(enterprise_payload.get("agent_result")).get("report_meta")), **_as_dict(enterprise_payload.get("report_meta")), **_as_dict(enterprise_payload.get("report_basic"))}
    personal_basic = _as_dict(personal_payload.get("basic_info"))
    enterprise_subject = _first(enterprise_meta.get("customer_name"), enterprise_meta.get("company_name"), enterprise_meta.get("enterprise_name"))
    personal_subject = personal_basic.get("name")
    relation_sources = [item for item in materials if item.get("category") in {"企业征信", "KYC及主体关系"}]
    roles: list[str] = []
    if personal_subject:
        for material in relation_sources:
            structured = material.get("structured_data")
            for key, value in _walk_key_values(structured):
                normalized = key.lower()
                if str(value).strip() != str(personal_subject).strip():
                    continue
                if normalized in {"legal_representative", "legal_person", "legal_representative_name", "法人", "法定代表人"} and "法定代表人" not in roles:
                    roles.append("法定代表人")
                if normalized in {"actual_controller", "actual_controller_name", "controller", "实际控制人"} and "实际控制人" not in roles:
                    roles.append("实际控制人")
            text = str(material.get("content") or "")
            escaped = re.escape(str(personal_subject))
            if re.search(rf"(?:法定代表人|法人)[：:\s]*{escaped}", text) and "法定代表人" not in roles:
                roles.append("法定代表人")
            if re.search(rf"实际控制人[：:\s]*{escaped}", text) and "实际控制人" not in roles:
                roles.append("实际控制人")
    enterprise_identifier = _first(
        enterprise_meta.get("unified_social_credit_code"),
        _find_identifier(materials, ("unified_social_credit_code", "social_credit_code", "credit_code")),
    )
    personal_identifier = _first(personal_basic.get("id_number"), customer.get("id_card"))
    return {
        "customer_subject": customer.get("name") or "暂未获取",
        "enterprise_credit_subject": enterprise_subject or "暂未获取",
        "personal_credit_subject": personal_subject or "暂未获取",
        "personal_credit_subject_role": " / ".join(roles) if roles else "需人工核实",
        "enterprise_identifier_masked": _mask_identifier(enterprise_identifier),
        "personal_identifier_masked": _mask_identifier(personal_identifier),
    }


def build_credit_report_model(material_result: dict[str, Any]) -> dict[str, Any]:
    materials = material_result.get("materials") if isinstance(material_result.get("materials"), list) else []
    customer = material_result.get("customer") or {}
    enterprise_payload = _latest_credit_payload(materials, "企业征信")
    personal_payload = _latest_credit_payload(materials, "个人征信")
    subjects = _derive_subjects(customer, materials, enterprise_payload, personal_payload)
    enterprise = _enterprise_credit_model(enterprise_payload)
    personal = _personal_credit_model(personal_payload, subjects.get("personal_credit_subject") or "")
    es = enterprise.get("summary") or {}
    ps = personal.get("summary") or {}
    personal_loan_balance = _sum_known([item.get("balance") for item in _as_list(personal_payload.get("loan_accounts"))])
    card_limit = _sum_known([_first(item.get("credit_limit"), item.get("limit")) for item in _as_list(personal_payload.get("credit_card_accounts"))])
    card_used = _sum_known([_first(item.get("used_amount"), item.get("used_limit"), item.get("balance")) for item in _as_list(personal_payload.get("credit_card_accounts"))])
    related_records = _as_list(personal_payload.get("related_repayment_responsibilities"))
    loan_overdue = ps.get("loan_overdue_account_count")
    card_overdue = ps.get("credit_card_overdue_account_count")
    loan_90 = ps.get("loan_90d_overdue_account_count")
    card_90 = ps.get("credit_card_90d_overdue_account_count")
    overdue_90 = None if loan_90 is None and card_90 is None else (_number(loan_90) or 0) + (_number(card_90) or 0)
    metrics = {
        "enterprise_unsettled_loan_balance": _display_number(_first(es.get("unsettled_credit_balance"), es.get("total_unsettled_balance"), _sum_known([_number(item.get("balance")) for item in enterprise.get("loans") or []]))),
        "enterprise_unsettled_loan_count": _first(es.get("unsettled_credit_institution_count"), len(enterprise.get("loans") or []) or None),
        "personal_unsettled_loan_balance": _display_number(personal_loan_balance),
        "personal_unsettled_loan_account_count": _first(ps.get("outstanding_loan_account_count"), len(personal.get("loans") or []) or None),
        "personal_credit_card_limit": _display_number(card_limit),
        "personal_credit_card_used": _display_number(card_used),
        "credit_card_usage_rate": f"{card_used / card_limit:.2%}" if card_limit and card_used is not None else None,
        "loan_overdue_account_count": loan_overdue,
        "credit_card_overdue_account_count": card_overdue,
        "overdue_90d_account_count": int(overdue_90) if overdue_90 is not None else None,
        "enterprise_external_guarantee_balance": _display_number(enterprise.get("external_guarantee_balance")),
        "enterprise_external_guarantee_explicit": bool(enterprise.get("external_guarantee_explicit")),
        "personal_related_repayment_balance": _display_amount_sum(related_records, "loan_balance", "balance"),
        "hard_query_6m_count": personal.get("hard_query_6m_count"),
        "dti": None,
        "online_loan_count": None,
        "large_revolving_due_concentration": None,
        "credit_card_account_count": ps.get("credit_card_account_count"),
    }
    model = {"subjects": subjects, "enterprise_credit": enterprise, "personal_credit": personal, "metrics": metrics}
    model["rule_checks"] = evaluate_credit_report_rules(model)
    material_result["subjects"] = subjects
    return model


NARRATIVE_SYSTEM_PROMPT = """你是一名融资资料分析助手。只根据传入的 credit_one_page_report_v1 结构化事实生成简短叙述 JSON。
禁止生成 Markdown 表格。禁止修改、补充或重新判断 rule_checks 的状态。禁止自行创造查询、DTI、网贷、信用卡使用率等阈值。
禁止引用财务报表、资产负债率、营业收入、应收账款、利润、经营现金流、企业流水、个人流水、房产。
企业贷款与个人贷款必须分开；个人信用卡不得描述为企业信用卡；法人相关还款责任不得描述为企业对外担保。
资料不足时写“资料不足”，矛盾时写“需人工核实”。不得输出审批概率、额度、利率或放款承诺。
只返回 JSON 对象，键必须为：emergency_attention, query_frequency_analysis, historical_credit_features, optimization_urgent, optimization_medium, optimization_long, advantages, risks, comprehensive_summary, one_sentence_conclusion。
除四个说明字段外，其余字段均为字符串数组。"""


def _default_narrative(report_model: dict[str, Any]) -> dict[str, Any]:
    rules = report_model.get("rule_checks") or []
    risks = [f"{item['item']}：{item['current']}" for item in rules if item.get("status") in {"风险", "关注", "超标"}]
    strengths = [f"{item['item']}：{item['current']}" for item in rules if item.get("status") == "达标"]
    urgent = [item.get("direction") for item in rules if item.get("status") == "风险" and item.get("direction")]
    personal = report_model.get("personal_credit") or {}
    enterprise = report_model.get("enterprise_credit") or {}
    if personal.get("related_repayment_responsibilities"):
        risks.append("法人个人征信存在相关还款责任，需与企业对外担保分口径核验。")
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
        "optimization_urgent": urgent,
        "optimization_medium": [item.get("direction") for item in rules if item.get("status") == "关注" and item.get("direction")],
        "optimization_long": [],
        "advantages": strengths,
        "risks": risks,
        "comprehensive_summary": "本报告仅基于已保存的企业征信、个人征信及必要主体关系资料，缺失项目未作推断。",
        "one_sentence_conclusion": "需结合明确征信事实及具体银行正式准入规则进一步人工评估。",
    }


def _parse_narrative(raw: str, report_model: dict[str, Any]) -> dict[str, Any]:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", str(raw or "").strip(), flags=re.IGNORECASE)
    try:
        parsed = json.loads(text)
    except Exception:
        return _default_narrative(report_model)
    if not isinstance(parsed, dict):
        return _default_narrative(report_model)
    forbidden = re.compile(r"资产负债率|营业收入|收入下滑|应收账款|经营现金流|财务报表|企业流水|个人流水|房产|审批概率|大概率通过")
    rendered = json.dumps(parsed, ensure_ascii=False)
    if forbidden.search(rendered):
        return _default_narrative(report_model)
    source_numbers = set(re.findall(r"\d+(?:\.\d+)?", json.dumps(report_model, ensure_ascii=False, default=str)))
    output_numbers = set(re.findall(r"\d+(?:\.\d+)?", rendered))
    if not output_numbers.issubset(source_numbers):
        return _default_narrative(report_model)
    default = _default_narrative(report_model)
    for key in default:
        if key not in parsed or not isinstance(parsed[key], type(default[key])):
            parsed[key] = default[key]
    return parsed


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
    report_model = build_credit_report_model(materials)
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
    if not report or not _has_complete_structure(report):
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
