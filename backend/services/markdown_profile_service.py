"""Customer markdown profile generation and persistence helpers."""

from __future__ import annotations

import json
import logging
from typing import Any

from .local_storage_service import DEFAULT_RAG_SOURCE_PRIORITY

logger = logging.getLogger(__name__)

RISK_REPORT_SCHEMA_TEMPLATE: dict[str, Any] = {
    "customer_summary": {
        "customer_id": "",
        "customer_name": "",
        "customer_type": "",
        "industry": "",
        "financing_need": "",
        "data_completeness": {"status": "", "score": 0, "missing_items": []},
    },
    "overall_assessment": {
        "total_score": 0,
        "risk_level": "high",
        "conclusion": "",
        "immediate_application_recommended": False,
        "basis": [],
    },
    "risk_dimensions": [],
    "matched_schemes": {"has_match": False, "items": []},
    "no_match_analysis": {"has_no_match_issue": False, "reasons": [], "core_shortboards": [], "basis": []},
    "optimization_suggestions": {
        "short_term": [],
        "mid_term": [],
        "document_supplement": [],
        "credit_optimization": [],
        "debt_optimization": [],
    },
    "financing_plan": {"current_stage": "", "one_to_three_months": [], "three_to_six_months": [], "alternative_paths": []},
    "final_recommendation": {"action": "", "priority_product_types": [], "next_steps": [], "basis": []},
}


def get_risk_report_schema_template() -> dict[str, Any]:
    return json.loads(json.dumps(RISK_REPORT_SCHEMA_TEMPLATE, ensure_ascii=False))


def get_rag_source_priority() -> list[str]:
    return list(DEFAULT_RAG_SOURCE_PRIORITY)


def _format_customer_type(customer_type: Any) -> str:
    value = str(customer_type or "").strip().lower()
    if value == "personal":
        return "个人"
    return "企业"


def _markdown_section(title: str, lines: list[str]) -> str:
    body = "\n".join(line for line in lines if line.strip()) or "- 暂无数据"
    return f"## {title}\n{body}"


def _format_value(value: Any) -> str:
    if value is None or value == "":
        return "暂无"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


STRUCTURED_FIELD_LABELS: dict[str, str] = {
    "account_name": "账户名称",
    "account_number": "账号",
    "bank_name": "银行名称",
    "bank_branch": "开户支行",
    "license_number": "核准号",
    "account_type": "账户性质",
    "open_date": "开户日期",
    "company_name": "公司名称",
    "credit_code": "统一社会信用代码",
    "legal_person": "法定代表人",
    "registered_capital": "注册资本",
    "establish_date": "成立日期",
    "business_scope": "经营范围",
    "address": "地址",
    "company_type": "类型",
    "document_type_code": "资料类型编码",
    "document_type_name": "资料类型名称",
    "storage_label": "资料归类",
    "currency": "币种",
    "start_date": "开始日期",
    "end_date": "结束日期",
    "opening_balance": "期初余额",
    "closing_balance": "期末余额",
    "total_income": "总收入",
    "total_expense": "总支出",
    "transaction_count": "交易笔数",
    "monthly_avg_income": "月均收入",
    "monthly_avg_expense": "月均支出",
    "top_inflows": "大额流入",
    "top_outflows": "大额流出",
    "top_transactions": "大额交易",
    "frequent_counterparties": "高频对手方",
    "abnormal_summary": "异常摘要",
    "summary": "摘要",
    "shareholders": "股东信息",
    "management_structure": "治理结构",
    "source_type": "来源类型",
}


def _format_field_label(key: str) -> str:
    normalized = str(key or "").strip()
    if not normalized:
        return "未命名字段"
    return STRUCTURED_FIELD_LABELS.get(
        normalized,
        normalized.replace("_", " ").strip(),
    )


async def _build_document_sections(storage_service: Any, customer_id: str) -> tuple[list[str], list[dict[str, Any]]]:
    extractions = await storage_service.get_extractions_by_customer(customer_id)
    sections: list[str] = []
    source_documents: list[dict[str, Any]] = []
    for extraction in extractions:
        extraction_type = extraction.get("extraction_type") or "未命名资料"
        extracted_data = extraction.get("extracted_data") or {}
        source_documents.append(
            {
                "source_type": extraction_type,
                "extraction_id": extraction.get("extraction_id"),
                "doc_id": extraction.get("doc_id"),
            }
        )
        lines = [f"- 来源类型：{extraction_type}"]
        if isinstance(extracted_data, dict):
            for key, value in extracted_data.items():
                lines.append(f"- {_format_field_label(key)}：{_format_value(value)}")
        sections.append(_markdown_section(extraction_type, lines))
    return sections, source_documents


async def _build_application_section(storage_service: Any, customer_id: str) -> tuple[str, dict[str, Any]]:
    applications = await storage_service.list_saved_applications(customer_id=customer_id)
    active = [item for item in applications if not item.get("stale")]

    if not active:
        if applications:
            latest_stale = applications[0]
            lines = [
                "- 当前已保存申请表因资料更新而失效",
                f"- 失效原因：{latest_stale.get('stale_reason') or '客户资料已更新，请重新生成申请表'}",
                f"- 失效时间：{latest_stale.get('stale_at') or '暂无记录'}",
            ]
            return _markdown_section("申请表摘要", lines), {"count": len(applications), "stale": True}
        return _markdown_section("申请表摘要", ["- 暂无已保存申请表"]), {"count": 0}

    latest = active[0]
    lines = [
        f"- 贷款类型：{latest.get('loanType') or '暂无'}",
        f"- 保存时间：{latest.get('savedAt') or '暂无'}",
        f"- 结构化数据：{_format_value(latest.get('applicationData') or {})}",
    ]
    return _markdown_section("申请表摘要", lines), {"count": len(active), "latest_saved_at": latest.get("savedAt")}


def _build_scheme_section(snapshot: dict[str, Any] | None) -> tuple[str, dict[str, Any]]:
    if not snapshot:
        return _markdown_section("方案匹配摘要", ["- 当前暂无已保存匹配方案"]), {"matched": False}

    lines = [
        f"- 来源：{snapshot.get('source') or 'manual'}",
        f"- 更新时间：{snapshot.get('updated_at') or snapshot.get('created_at') or '暂无'}",
        f"- 内容摘要：{snapshot.get('summary_markdown') or snapshot.get('raw_result') or '暂无'}",
    ]
    return _markdown_section("方案匹配摘要", lines), {"matched": True}


async def build_auto_profile_payload(storage_service: Any, customer_id: str) -> dict[str, Any]:
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise ValueError("customer not found")

    customer_name = customer.get("name") or ""
    doc_sections, source_documents = await _build_document_sections(storage_service, customer_id)
    application_section, application_snapshot = await _build_application_section(storage_service, customer_id)
    scheme_snapshot = await storage_service.get_latest_scheme_snapshot(customer_id)
    scheme_section, scheme_meta = _build_scheme_section(scheme_snapshot)

    overview_lines = [
        f"- 客户名称：{customer_name or '暂无'}",
        f"- 客户类型：{_format_customer_type(customer.get('customer_type'))}",
        f"- 上传账号：{customer.get('uploader') or '暂无'}",
        f"- 最近上传时间：{customer.get('upload_time') or customer.get('updated_at') or '暂无'}",
    ]

    markdown_parts = [
        "# 资料汇总",
        _markdown_section("客户基础信息", overview_lines),
        _markdown_section(
            "使用说明",
            [
                "- 该内容可由系统自动整理，也可手动补充修订。",
                "- 手动保存后会作为当前使用版本。",
                "- RAG 检索优先级：资料汇总 > 已解析资料文本 > 方案匹配摘要 > 申请表摘要。",
            ],
        ),
        _markdown_section("已解析资料索引", [f"- 共 {len(source_documents)} 份资料"] if source_documents else ["- 暂无已解析资料"]),
        *doc_sections,
        application_section,
        scheme_section,
    ]

    return {
        "customer_id": customer_id,
        "customer_name": customer_name,
        "title": f"{customer_name or customer_id}资料汇总",
        "markdown_content": "\n\n".join(markdown_parts).strip(),
        "source_mode": "auto",
        "source_snapshot": {
            "customer_name": customer_name,
            "source_documents": source_documents,
            "application_summary": application_snapshot,
            "scheme_summary": scheme_meta,
        },
        "rag_source_priority": get_rag_source_priority(),
        "risk_report_schema": get_risk_report_schema_template(),
    }


async def get_or_create_customer_profile(storage_service: Any, customer_id: str) -> tuple[dict[str, Any], bool]:
    existing = await storage_service.get_customer_profile(customer_id)
    if existing:
        return existing, False

    generated = await build_auto_profile_payload(storage_service, customer_id)
    saved = await storage_service.upsert_customer_profile(generated)
    return saved, True


async def regenerate_customer_profile(storage_service: Any, customer_id: str) -> dict[str, Any]:
    generated = await build_auto_profile_payload(storage_service, customer_id)
    return await storage_service.upsert_customer_profile(generated)
