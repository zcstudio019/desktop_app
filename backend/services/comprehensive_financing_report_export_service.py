"""Frozen comprehensive report snapshots and export; no data refresh or LLM calls."""

from __future__ import annotations

import copy
import re
import uuid
from typing import Any
from urllib.parse import quote

from backend.services.comprehensive_financing_analysis_service import ComprehensiveFinancingAnalysisResult
from backend.services.comprehensive_financing_report_html_renderer import render_comprehensive_financing_report_html
from backend.services.comprehensive_financing_report_model import ComprehensiveFinancingReportModel, TEMPLATE_VERSION
from backend.services.credit_report_export_service import render_html_pdf, CreditReportExportUnavailable


# The shared snapshot table stores report_version in VARCHAR(32). Keep the
# public template version in the frozen payload and use a compact DB selector.
STORAGE_REPORT_VERSION = "comprehensive_financing_v1"


async def freeze_comprehensive_financing_report(
    storage: Any,
    customer_id: str,
    model: ComprehensiveFinancingReportModel,
    analysis: ComprehensiveFinancingAnalysisResult,
    generated_at: str,
    markdown: str,
) -> dict[str, str]:
    creator = getattr(storage, "create_financing_diagnostic_report_snapshot", None)
    if not callable(creator):
        raise RuntimeError("当前存储后端暂不支持综合报告快照")
    report_id = uuid.uuid4().hex
    payload = copy.deepcopy({
        "report_type": model.report_type,
        "template_version": TEMPLATE_VERSION,
        "report_model": model.model_dump(mode="json"),
        "analysis_result": analysis.model_dump(mode="json"),
        "markdown": markdown,
        "generated_at": generated_at,
    })
    saved = await creator({
        "report_id": report_id,
        "customer_id": customer_id,
        "report_version": STORAGE_REPORT_VERSION,
        "report_status": "completed",
        "report_json": payload,
        "report_markdown": markdown,
        "source_summary": {"report_type": model.report_type, "template_version": TEMPLATE_VERSION},
        "generated_at": generated_at,
    })
    saved_id = str(saved.get("report_id") or saved.get("id") or report_id)
    base = f"/api/customers/{quote(customer_id, safe='')}/comprehensive-financing-report/snapshots/{quote(saved_id, safe='')}"
    return {"reportId": saved_id, "previewUrl": base + "/preview", "pdfUrl": base + "/export/pdf"}


def _frozen(snapshot: dict[str, Any]) -> tuple[ComprehensiveFinancingReportModel, ComprehensiveFinancingAnalysisResult, str]:
    payload = snapshot.get("report_json") or {}
    if (snapshot.get("report_version") != STORAGE_REPORT_VERSION
            or payload.get("report_type") != "comprehensive_financing_analysis_report"
            or payload.get("template_version") != TEMPLATE_VERSION):
        raise ValueError("未找到该综合融资报告版本")
    model = ComprehensiveFinancingReportModel.model_validate(payload["report_model"])
    analysis = ComprehensiveFinancingAnalysisResult.model_validate(payload["analysis_result"])
    return model, analysis, str(payload["generated_at"])


def snapshot_html(snapshot: dict[str, Any]) -> str:
    model, analysis, generated_at = _frozen(snapshot)
    return render_comprehensive_financing_report_html(model, analysis, generated_at)


def pdf_filename(snapshot: dict[str, Any]) -> str:
    model, _, generated_at = _frozen(snapshot)
    name = str(model.subject_profile.get("enterprise_name") or model.customer.get("name") or "未命名客户")
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f\x7f]', "_", name).strip(" .")[:80] or "未命名客户"
    date = re.sub(r"\D", "", generated_at[:10]) or "日期未记录"
    return f"客户综合融资分析报告_{name}_{date}.pdf"


__all__ = ["freeze_comprehensive_financing_report", "snapshot_html", "pdf_filename", "render_html_pdf", "CreditReportExportUnavailable"]
