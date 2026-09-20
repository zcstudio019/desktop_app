"""Serializable, narrative-free facts for the comprehensive financing report."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


MaterialStatus = Literal["confirmed", "available", "partial", "missing", "needs_review"]
REPORT_TYPE = "comprehensive_financing_analysis_report"
TEMPLATE_VERSION = "comprehensive_financing_analysis_report_v1"


class MaterialRecord(BaseModel):
    type: str
    status: MaterialStatus = "missing"
    latest_date: str | None = None
    usable: bool = False
    notes: str = ""


class ComprehensiveFinancingReportModel(BaseModel):
    """The sole facts layer. Sections contain whitelisted business fields only."""

    report_type: Literal["comprehensive_financing_analysis_report"] = REPORT_TYPE
    template_version: Literal["comprehensive_financing_analysis_report_v1"] = TEMPLATE_VERSION
    customer: dict[str, Any] = Field(default_factory=dict)
    data_scope: dict[str, Any] = Field(default_factory=dict)
    subject_profile: dict[str, Any] = Field(default_factory=dict)
    financing_requirement: dict[str, Any] = Field(default_factory=dict)
    enterprise_credit: dict[str, Any] = Field(default_factory=dict)
    personal_credit: dict[str, Any] = Field(default_factory=dict)
    enterprise_cashflow: dict[str, Any] = Field(default_factory=dict)
    personal_cashflow: dict[str, Any] = Field(default_factory=dict)
    financials: dict[str, Any] = Field(default_factory=dict)
    assets: dict[str, Any] = Field(default_factory=dict)
    risk_context: dict[str, Any] = Field(default_factory=dict)
    existing_financing_plan: dict[str, Any] = Field(default_factory=dict)
    derived_metrics: dict[str, Any] = Field(default_factory=dict)
    conflicts: list[dict[str, Any]] = Field(default_factory=list)
    data_quality: dict[str, Any] = Field(default_factory=dict)
    source_dates: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def reject_unstructured_source_content(self) -> "ComprehensiveFinancingReportModel":
        forbidden = {
            "raw_text", "ocr_text", "full_text", "evidence", "evidence_text",
            "markdown", "report_markdown", "html", "raw_html", "base64",
            "pdf_bytes", "prompt", "tool_call", "llm_response", "source_text",
        }

        def check(value: Any) -> None:
            if isinstance(value, dict):
                for key, item in value.items():
                    if str(key).lower() in forbidden:
                        raise ValueError(f"Unstructured source field is forbidden: {key}")
                    check(item)
            elif isinstance(value, list):
                for item in value:
                    check(item)

        check(self.model_dump())
        return self
