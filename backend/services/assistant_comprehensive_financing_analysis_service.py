"""Chat orchestration for structured comprehensive financing analysis."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from datetime import datetime
from typing import Any

from backend.services.assistant_credit_report_service import resolve_report_customer
from backend.services.comprehensive_financing_analysis_service import (
    analyze_comprehensive_financing_report,
    build_analysis_input_debug_summary,
)
from backend.services.comprehensive_financing_report_context_service import (
    build_comprehensive_financing_report_context,
)
from backend.services.comprehensive_financing_report_markdown_renderer import (
    render_comprehensive_financing_report,
)
from backend.services.comprehensive_financing_report_export_service import freeze_comprehensive_financing_report


logger = logging.getLogger(__name__)
COMPREHENSIVE_FINANCING_ANALYSIS_INTENT = "comprehensive_financing_analysis_report"

_REQUEST_PATTERNS = (
    re.compile(r"(?:生成|做|出|整理).{0,12}(?:客户)?综合融资分析(?:报告)?"),
    re.compile(r"分析.{0,12}(?:客户|企业).{0,8}(?:整体融资|综合融资)(?:情况|能力)?"),
    re.compile(r"综合分析.{0,18}(?:征信|流水|财务)"),
    re.compile(r"完整融资分析"),
)


def is_comprehensive_financing_analysis_request(message: str) -> bool:
    text = re.sub(r"\s+", "", message or "")
    return bool(text and any(pattern.search(text) for pattern in _REQUEST_PATTERNS))


async def generate_comprehensive_financing_analysis(
    storage_service: Any,
    message: str,
    selected_customer_id: str | None = None,
    llm: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    """Resolve one customer, build Step 2 facts, and return the Step 3 object."""
    resolved = await resolve_report_customer(storage_service, message, selected_customer_id)
    status = resolved.get("status")
    if status == "missing_context":
        return {"message": "请先选择客户或在请求中写明企业名称。", "data": {"analysisStatus": "missing_customer"}}
    if status == "customer_not_found":
        return {"message": "未找到对应客户资料，请核对企业名称。", "data": {"analysisStatus": "customer_not_found"}}
    if status == "ambiguous":
        return {"message": "找到多个同名客户，请先选择具体客户。", "data": {
            "analysisStatus": "ambiguous_customer", "choices": resolved.get("choices") or [],
        }}
    customer = resolved.get("customer") or {}
    customer_id = str(customer.get("customer_id") or customer.get("id") or selected_customer_id or "")
    if not customer_id:
        return {"message": "客户记录缺少有效标识，请核对客户资料。", "data": {"analysisStatus": "customer_not_found"}}

    model = await build_comprehensive_financing_report_context(storage_service, customer_id)
    debug_summary = build_analysis_input_debug_summary(model)
    logger.info("comprehensive financing LLM input summary=%s", json.dumps(debug_summary, ensure_ascii=False))
    analysis = await analyze_comprehensive_financing_report(model, llm=llm)
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    markdown = render_comprehensive_financing_report(model, analysis, generated_at)
    export_data: dict[str, Any] = {}
    try:
        export_data = await freeze_comprehensive_financing_report(
            storage_service, customer_id, model, analysis, generated_at, markdown,
        )
    except Exception:
        logger.exception("comprehensive report snapshot could not be saved")
        export_data = {"exportMessage": "报告已生成，预览和 PDF 暂不可用，请稍后重试。"}
    return {
        "message": markdown,
        "data": {
            "analysisStatus": "completed",
            "reportType": model.report_type,
            "templateVersion": model.template_version,
            "reportStatus": "completed" if export_data.get("reportId") else "export_unavailable",
            **export_data,
            "generatedAt": generated_at,
            "analysis": analysis.model_dump(),
            "analysisValidationFallbackUsed": analysis.validation_fallback_used,
            "contextDebugSummary": debug_summary,
        },
        "reasoning": None,
    }
