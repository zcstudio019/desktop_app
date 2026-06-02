from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report
from backend.services.enterprise_bank_flow_diagnostic_service import build_enterprise_bank_flow_diagnostic
from backend.services.enterprise_credit_diagnostic_service import build_enterprise_credit_diagnostic
from backend.services.financial_statement_diagnostic_service import build_financial_statement_diagnostic
from backend.services.financing_kyc_diagnostic_service import build_financing_kyc_diagnostic
from backend.services.kyc_completeness_service import evaluate_kyc_completeness
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile
from backend.services.personal_credit_diagnostic_service import build_personal_credit_diagnostic


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def can_save_financing_diagnostic_report_snapshot(role: str | None) -> bool:
    return str(role or "").lower() in {"admin", "operator"}


def _next_report_version(existing: list[dict[str, Any]]) -> str:
    max_no = 0
    for item in existing:
        version = str(item.get("report_version") or "")
        match = re.search(r"(\d+)$", version)
        if not match:
            continue
        max_no = max(max_no, int(match.group(1)))
    return f"v{max_no + 1}"


def build_financing_diagnostic_source_summary(report: dict[str, Any]) -> dict[str, Any]:
    kyc_profile = _as_dict(report.get("kyc_diagnostic"))
    enterprise_credit = _as_dict(report.get("enterprise_credit_diagnostic"))
    personal_credit = _as_dict(report.get("personal_credit_diagnostic"))
    enterprise_flow = _as_dict(report.get("enterprise_bank_flow_diagnostic"))
    financial = _as_dict(report.get("financial_statement_diagnostic"))
    advice = _as_dict(report.get("comprehensive_financing_advice"))
    return {
        "has_kyc_profile": bool(kyc_profile),
        "has_enterprise_credit_report": bool(enterprise_credit.get("has_enterprise_credit_report")),
        "has_personal_credit_report": bool(personal_credit.get("has_personal_credit_report")),
        "has_enterprise_bank_flow": bool(enterprise_flow.get("has_enterprise_bank_flow")),
        "has_financial_statement": bool(financial.get("has_financial_statement")),
        "overall_status": str(advice.get("overall_status") or ""),
        "financing_readiness_score": int(advice.get("financing_readiness_score") or 0),
    }


async def build_realtime_financing_diagnostic_report(
    storage_service: Any,
    customer_id: str,
    customer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    customer_payload = customer if isinstance(customer, dict) else {}
    profile = await build_customer_kyc_profile(storage_service, customer_id)
    completeness = evaluate_kyc_completeness(profile)
    diagnostic = build_financing_kyc_diagnostic(profile, completeness)
    enterprise_credit = await build_enterprise_credit_diagnostic(storage_service, customer_id)
    personal_credit = await build_personal_credit_diagnostic(storage_service, customer_id)
    enterprise_flow = await build_enterprise_bank_flow_diagnostic(storage_service, customer_id, profile)
    financial_statement = await build_financial_statement_diagnostic(storage_service, customer_id)
    return build_customer_financing_diagnostic_report(
        customer_id,
        customer_payload,
        profile,
        completeness,
        diagnostic,
        enterprise_credit,
        personal_credit,
        enterprise_flow,
        financial_statement,
    )


class FinancingDiagnosticReportSnapshotService:
    def __init__(self, storage_service: Any) -> None:
        self.storage_service = storage_service

    async def save_current_report_snapshot(
        self,
        customer_id: str,
        *,
        customer: dict[str, Any] | None = None,
        generated_by: str = "",
    ) -> dict[str, Any]:
        report = await build_realtime_financing_diagnostic_report(self.storage_service, customer_id, customer)
        existing = await self.list_snapshots(customer_id, limit=1000, include_detail=False)
        report_version = _next_report_version(existing)
        generated_at = datetime.now(timezone.utc).isoformat()
        source_summary = build_financing_diagnostic_source_summary(report)
        creator = getattr(self.storage_service, "create_financing_diagnostic_report_snapshot", None)
        if not callable(creator):
            raise RuntimeError("当前存储后端不支持融资诊断报告快照")
        snapshot = await creator(
            {
                "report_id": uuid.uuid4().hex,
                "customer_id": customer_id,
                "report_version": report_version,
                "report_status": report.get("report_status") or "draft",
                "report_json": report,
                "report_markdown": report.get("report_markdown") or "",
                "source_summary": source_summary,
                "generated_by": generated_by,
                "generated_at": generated_at,
            }
        )
        return {
            "success": True,
            "report_id": snapshot.get("report_id") or snapshot.get("id") or "",
            "report_version": snapshot.get("report_version") or report_version,
            "generated_at": snapshot.get("generated_at") or generated_at,
            "message": "融资诊断报告快照已保存",
        }

    async def list_snapshots(
        self,
        customer_id: str,
        *,
        limit: int = 20,
        include_detail: bool = False,
    ) -> list[dict[str, Any]]:
        lister = getattr(self.storage_service, "list_financing_diagnostic_report_snapshots", None)
        if not callable(lister):
            return []
        rows = await lister(customer_id, limit=limit)
        if include_detail:
            return rows
        summaries: list[dict[str, Any]] = []
        for row in rows:
            summaries.append(
                {
                    "id": row.get("report_id") or row.get("id") or "",
                    "report_version": row.get("report_version") or "",
                    "report_status": row.get("report_status") or "draft",
                    "generated_by": row.get("generated_by") or "",
                    "generated_at": row.get("generated_at") or "",
                    "source_summary": row.get("source_summary") if isinstance(row.get("source_summary"), dict) else {},
                    "summary": row.get("summary") or "",
                }
            )
        return summaries

    async def get_snapshot(self, customer_id: str, report_id: str) -> dict[str, Any] | None:
        getter = getattr(self.storage_service, "get_financing_diagnostic_report_snapshot", None)
        if not callable(getter):
            return None
        row = await getter(customer_id, report_id)
        if not row:
            return None
        return {
            "id": row.get("report_id") or row.get("id") or "",
            "customer_id": row.get("customer_id") or customer_id,
            "report_version": row.get("report_version") or "",
            "report_status": row.get("report_status") or "draft",
            "report_json": row.get("report_json") if isinstance(row.get("report_json"), dict) else {},
            "report_markdown": row.get("report_markdown") or "",
            "source_summary": row.get("source_summary") if isinstance(row.get("source_summary"), dict) else {},
            "generated_by": row.get("generated_by") or "",
            "generated_at": row.get("generated_at") or "",
        }
