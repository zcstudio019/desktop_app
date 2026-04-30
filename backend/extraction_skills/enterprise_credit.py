from __future__ import annotations

import re
from typing import Any

from backend.document_types import get_document_display_name, get_document_storage_label

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult

_DATE_PATTERN = re.compile(r"((?:19|20)\d{2}[年./-]\d{1,2}[月./-]\d{1,2}日?)")
_NUMBER_PATTERN = re.compile(r"(\d+)")


def _clean_text(value: str | None) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip("：:;；，,。 ")


def _customer_name_from_customer_id(customer_id: str) -> str:
    raw = str(customer_id or "").strip()
    if raw.startswith("enterprise_"):
        return raw.split("enterprise_", 1)[1].strip()
    if raw.startswith("personal_"):
        return raw.split("personal_", 1)[1].strip()
    return ""


def _extract_by_patterns(text: str, patterns: tuple[str, ...]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _clean_text(match.group(1))
    return ""


def _extract_count(text: str, labels: tuple[str, ...]) -> int | None:
    for label in labels:
        match = re.search(rf"{label}[：:\s]*([0-9]+)", text)
        if match:
            return int(match.group(1))
    return None


def _extract_compact_preview(text: str, limit: int = 3000) -> str:
    return str(text or "").strip()[:limit]


def _build_risk_signals(text: str) -> list[dict[str, Any]]:
    normalized = str(text or "")
    rules = [
        ("逾期", "high", ("逾期",)),
        ("欠息", "high", ("欠息",)),
        ("关注", "medium", ("关注", "五级分类")),
        ("不良", "high", ("不良",)),
        ("被执行", "high", ("被执行", "执行案件")),
        ("失信", "high", ("失信", "失信被执行")),
        ("行政处罚", "medium", ("行政处罚",)),
    ]
    signals: list[dict[str, Any]] = []
    seen_types: set[str] = set()
    for signal_type, level, keywords in rules:
        evidence = next((keyword for keyword in keywords if keyword in normalized), "")
        if not evidence or signal_type in seen_types:
            continue
        seen_types.add(signal_type)
        signals.append(
            {
                "type": signal_type,
                "level": level,
                "text": f"报告中出现“{evidence}”相关内容，建议人工复核企业信用风险。",
                "evidence": evidence,
            }
        )
    return signals


class EnterpriseCreditSkill(BaseExtractionSkill):
    document_type = "enterprise_credit"
    supported_extensions = {".pdf", ".png", ".jpg", ".jpeg"}

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        raw_text = str(input_data.raw_text or "")
        raw_pages = input_data.metadata.get("raw_pages") or []

        company_name = (
            _clean_text(input_data.metadata.get("customer_name"))
            or _customer_name_from_customer_id(input_data.customer_id)
            or _extract_by_patterns(
                raw_text,
                (
                    r"企业名称[：:\s]*([^\n\r]{2,120})",
                    r"被查询者名称[：:\s]*([^\n\r]{2,120})",
                    r"报告主体[：:\s]*([^\n\r]{2,120})",
                    r"名称[：:\s]*([^\n\r]{2,120})",
                ),
            )
        )
        credit_code = _extract_by_patterns(
            raw_text,
            (
                r"(?:统一社会信用代码|信用代码)[：:\s]*([0-9A-Z]{18})",
                r"(?:组织机构代码|注册号)[：:\s]*([0-9A-Z\-]{8,32})",
            ),
        )
        report_no = _extract_by_patterns(
            raw_text,
            (
                r"(?:报告编号|信用报告编号|编号)[：:\s]*([A-Za-z0-9\-_/（）()]+)",
            ),
        )
        report_date = _extract_by_patterns(
            raw_text,
            (
                r"(?:报告时间|报告日期|查询日期)[：:\s]*(" + _DATE_PATTERN.pattern + r")",
            ),
        )
        legal_representative = _extract_by_patterns(
            raw_text,
            (
                r"(?:法定代表人|法人代表)[：:\s]*([^\n\r]{2,40})",
            ),
        )
        registered_capital = _extract_by_patterns(
            raw_text,
            (
                r"(?:注册资本|注册资金)[：:\s]*([^\n\r]{1,60})",
            ),
        )
        established_date = _extract_by_patterns(
            raw_text,
            (
                r"(?:成立日期|成立时间|注册日期)[：:\s]*(" + _DATE_PATTERN.pattern + r")",
            ),
        )
        business_status = _extract_by_patterns(
            raw_text,
            (
                r"(?:经营状态|登记状态|企业状态)[：:\s]*([^\n\r]{1,60})",
            ),
        )
        address = _extract_by_patterns(
            raw_text,
            (
                r"(?:住所|注册地址|地址)[：:\s]*([^\n\r]{4,160})",
            ),
        )
        business_scope = _extract_by_patterns(
            raw_text,
            (
                r"(?:经营范围)[：:\s]*([^\n\r]{4,400})",
            ),
        )

        extracted_json: dict[str, Any] = {
            "report_basic": {
                "company_name": company_name,
                "credit_code": credit_code,
                "report_no": report_no,
                "report_date": report_date,
            },
            "registration_info": {
                "legal_representative": legal_representative,
                "registered_capital": registered_capital,
                "established_date": established_date,
                "business_status": business_status,
                "address": address,
                "business_scope": business_scope,
            },
            "credit_summary": {
                "loan_account_count": _extract_count(raw_text, ("贷款账户数", "贷款账户", "信贷账户数")),
                "outstanding_loan_count": _extract_count(raw_text, ("未结清贷款数", "未结清贷款", "未结清账户数")),
                "overdue_account_count": _extract_count(raw_text, ("逾期账户数", "逾期账户", "逾期笔数")),
                "guarantee_count": _extract_count(raw_text, ("担保笔数", "担保账户数", "对外担保数")),
                "query_count": _extract_count(raw_text, ("查询次数", "查询记录数", "被查询次数")),
            },
            "loan_records": [],
            "guarantee_records": [],
            "public_records": [],
            "queries": [],
            "risk_signals": _build_risk_signals(raw_text),
            "source_pages": [item.get("page") for item in raw_pages if isinstance(item, dict) and item.get("page") is not None],
            "raw_text_preview": _extract_compact_preview(raw_text),
        }

        summary_lines = [
            "## 企业征信摘要",
            "",
            "### 报告基础信息",
            f"- 企业名称：{company_name or '未识别'}",
            f"- 统一社会信用代码：{credit_code or '未识别'}",
            f"- 报告编号：{report_no or '未识别'}",
            f"- 报告时间：{report_date or '未识别'}",
            "",
            "### 登记信息",
            f"- 法定代表人：{legal_representative or '未识别'}",
            f"- 注册资本：{registered_capital or '未识别'}",
            f"- 成立日期：{established_date or '未识别'}",
            f"- 经营状态：{business_status or '未识别'}",
            f"- 地址：{address or '未识别'}",
            "",
            "### 信贷概要",
            f"- 贷款账户数：{extracted_json['credit_summary']['loan_account_count'] if extracted_json['credit_summary']['loan_account_count'] is not None else '未识别'}",
            f"- 未结清贷款数：{extracted_json['credit_summary']['outstanding_loan_count'] if extracted_json['credit_summary']['outstanding_loan_count'] is not None else '未识别'}",
            f"- 逾期账户数：{extracted_json['credit_summary']['overdue_account_count'] if extracted_json['credit_summary']['overdue_account_count'] is not None else '未识别'}",
            f"- 对外担保数：{extracted_json['credit_summary']['guarantee_count'] if extracted_json['credit_summary']['guarantee_count'] is not None else '未识别'}",
            f"- 查询次数：{extracted_json['credit_summary']['query_count'] if extracted_json['credit_summary']['query_count'] is not None else '未识别'}",
            "",
            "### 风险提示",
        ]
        risk_signals = extracted_json["risk_signals"]
        if risk_signals:
            for signal in risk_signals:
                summary_lines.append(f"- [{signal.get('level')}] {signal.get('text')}")
        else:
            summary_lines.append("- 暂未识别到明确风险关键词。")

        warnings: list[str] = []
        if not company_name:
            warnings.append("未稳定识别企业名称，建议核对报告首页。")

        return ExtractionResult(
            document_type=self.document_type,
            schema_version="enterprise_credit.v1",
            extracted_json=extracted_json,
            markdown_summary="\n".join(summary_lines).strip(),
            confidence=0.75,
            warnings=warnings,
            errors=[],
            skill_name="enterprise_credit",
            skill_version="v1",
        )


def build_enterprise_credit_content(
    *,
    text: str,
    customer_id: str = "",
    customer_name: str = "",
    file_name: str = "",
    file_path: str = "",
    document_id: str = "",
    raw_pages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    skill = EnterpriseCreditSkill()
    result = skill.extract(
        ExtractionInput(
            customer_id=customer_id,
            document_id=document_id,
            document_type=skill.document_type,
            file_name=file_name,
            file_path=file_path,
            raw_text=text,
            metadata={
                "customer_name": customer_name,
                "raw_pages": raw_pages or [],
            },
        )
    )
    report_basic = result.extracted_json.get("report_basic") or {}
    return {
        "document_type_code": "enterprise_credit",
        "document_type_name": get_document_display_name("enterprise_credit"),
        "storage_label": get_document_storage_label("enterprise_credit"),
        "skill_name": result.skill_name,
        "skill_version": result.skill_version,
        "schema_version": result.schema_version,
        "extraction_status": "success" if not result.errors else "failed",
        "confidence": result.confidence,
        "warnings": result.warnings,
        "errors": result.errors,
        "markdown_summary": result.markdown_summary,
        "extracted_json": result.extracted_json,
        "company_name": report_basic.get("company_name") or customer_name,
        "customer_name": report_basic.get("company_name") or customer_name,
        "credit_code": report_basic.get("credit_code") or "",
        "report_no": report_basic.get("report_no") or "",
        "report_date": report_basic.get("report_date") or "",
        "risk_signals": result.extracted_json.get("risk_signals") or [],
        "raw_text_preview": result.extracted_json.get("raw_text_preview") or "",
    }
