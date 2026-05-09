from __future__ import annotations

from typing import Any

from .schemas import AgentResult


def _fmt_amount(value: float | None) -> str:
    if value is None:
        return "未识别"
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return text or "0"


def normalize_agent_result(result: AgentResult) -> AgentResult:
    short_sum = round(sum(float(x.balance or 0) for x in result.short_term_loans), 2)
    medium_sum = round(sum(float(x.balance or 0) for x in result.medium_long_term_loans), 2)
    if result.credit_summary.short_term_loan_balance is None and result.short_term_loans:
        result.credit_summary.short_term_loan_balance = short_sum
    if result.credit_summary.medium_long_term_loan_balance is None and result.medium_long_term_loans:
        result.credit_summary.medium_long_term_loan_balance = medium_sum
    if result.credit_summary.unsettled_credit_balance is None and (short_sum or medium_sum):
        result.credit_summary.unsettled_credit_balance = round(short_sum + medium_sum, 2)
    if result.credit_summary.credit_line_count is None and result.credit_lines:
        result.credit_summary.credit_line_count = len(result.credit_lines)
    return result


def build_markdown(result: dict[str, Any]) -> str:
    meta = result.get("report_meta") or {}
    summary = result.get("credit_summary") or {}
    short_loans = result.get("short_term_loans") or []
    medium_loans = result.get("medium_long_term_loans") or []
    credit_lines = result.get("credit_lines") or []
    bills = result.get("bills") or []
    lcs = result.get("letters_of_credit") or []
    guarantees = result.get("guarantees") or []
    validation = result.get("validation") or {}

    lines: list[str] = []
    lines.extend([
        "## 企业征信摘要",
        "",
        "### 报告基础信息",
        f"- 企业名称：{meta.get('customer_name') or '未识别'}",
        f"- 统一社会信用代码：{meta.get('unified_social_credit_code') or '未识别'}",
        f"- 查询机构：{meta.get('query_org') or '未识别'}",
        f"- 报告时间：{meta.get('report_time') or '未识别'}",
        "",
        "### 信贷概要",
        f"- 当前未结清借贷余额：{_fmt_amount(summary.get('unsettled_credit_balance'))}",
        f"- 当前未结清信贷机构数：{summary.get('unsettled_credit_institution_count') if summary.get('unsettled_credit_institution_count') is not None else '未识别'}",
        f"- 短期借款余额：{_fmt_amount(summary.get('short_term_loan_balance'))}",
    ])
    if short_loans:
        lines.extend(_loan_markdown_lines(short_loans))
    else:
        lines.append("  - 暂未识别到短期借款明细")
    lines.append(f"- 中长期借款余额：{_fmt_amount(summary.get('medium_long_term_loan_balance'))}")
    if medium_loans:
        lines.extend(_loan_markdown_lines(medium_loans))
    else:
        lines.append("  - 暂未识别到中长期借款明细")
    lines.append(f"- 对外担保余额：{_fmt_amount(summary.get('external_guarantee_balance'))}")

    lines.extend(["", "### 授信信息"])
    if credit_lines:
        for item in credit_lines:
            lines.extend([
                f"- 授信机构：{item.get('institution_name') or '未识别'}",
                f"  授信额度类型：{item.get('credit_type') or '未识别'}",
                f"  额度循环标志：{'是' if item.get('credit_revolving') else '否' if item.get('credit_revolving') is False else '未识别'}",
                f"  授信额度：{_fmt_amount(item.get('credit_amount'))} 万元",
                f"  已用额度：{_fmt_amount(item.get('used_amount'))} 万元",
                f"  生效日期：{item.get('effective_date') or '未识别'}",
                f"  到期日：{item.get('expiry_date') or '未识别'}",
            ])
    else:
        lines.append("- 本报告未展示逐笔授信信息明细")

    if bills or lcs:
        lines.extend(["", "### 银行承兑汇票和信用证"])
        for item in [*bills, *lcs]:
            lines.extend(_business_markdown_lines(item))
    if guarantees:
        lines.extend(["", "### 银行保函及其他业务"])
        for item in guarantees:
            lines.extend(_business_markdown_lines(item))

    warnings = validation.get("warnings") or []
    errors = validation.get("errors") or []
    if warnings or errors:
        lines.extend(["", "### 校验与异常"])
        for item in errors:
            lines.append(f"- 错误：{item}")
        for item in warnings:
            lines.append(f"- 警告：{item}")
    return "\n".join(lines).strip()


def agent_result_to_legacy_extraction(agent_result: dict[str, Any]) -> tuple[dict[str, Any], str]:
    meta = agent_result.get("report_meta") or {}
    summary = agent_result.get("credit_summary") or {}
    short_loans = agent_result.get("short_term_loans") or []
    medium_loans = agent_result.get("medium_long_term_loans") or []
    credit_lines = agent_result.get("credit_lines") or []
    extracted_json = {
        "schema_version": "enterprise_credit.agent.v1",
        "report_basic": {
            "company_name": meta.get("customer_name"),
            "credit_code": meta.get("unified_social_credit_code"),
            "query_institution": meta.get("query_org"),
            "report_date": meta.get("report_time"),
        },
        "identity_info": {
            "unified_social_credit_code": meta.get("unified_social_credit_code"),
        },
        "credit_summary": {
            "active_borrowing_balance": summary.get("unsettled_credit_balance"),
            "current_active_credit_institution_count": summary.get("unsettled_credit_institution_count"),
            "short_term_loan_balance": summary.get("short_term_loan_balance"),
            "medium_long_term_loan_balance": summary.get("medium_long_term_loan_balance"),
            "guarantee_balance": summary.get("external_guarantee_balance"),
        },
        "short_loans": [_legacy_from_agent_loan(x, "short") for x in short_loans],
        "short_loans_final": [_legacy_from_agent_loan(x, "short") for x in short_loans],
        "medium_loans": [_legacy_from_agent_loan(x, "medium_long") for x in medium_loans],
        "medium_loans_final": [_legacy_from_agent_loan(x, "medium_long") for x in medium_loans],
        "long_term_loans": [_legacy_from_agent_loan(x, "medium_long") for x in medium_loans],
        "active_loans": [_legacy_from_agent_loan(x, "short") for x in short_loans] + [_legacy_from_agent_loan(x, "medium_long") for x in medium_loans],
        "credit_facilities": [_legacy_from_agent_credit_line(x) for x in credit_lines],
        "bills": agent_result.get("bills") or [],
        "letters_of_credit": agent_result.get("letters_of_credit") or [],
        "bank_guarantee_other_business": agent_result.get("guarantees") or [],
        "validation": agent_result.get("validation") or {},
        "confidence": agent_result.get("confidence") or {},
        "raw_evidence_map": agent_result.get("raw_evidence_map") or {},
        "agent_result": agent_result,
    }
    return extracted_json, build_markdown(agent_result)


def _loan_markdown_lines(loans: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for loan in loans:
        lines.extend([
            f"  - 机构：{loan.get('institution_name') or '未识别'}",
            f"    业务：{loan.get('business_type') or '未识别'}",
            f"    担保方式：{loan.get('guarantee_type') or '未识别'}",
            f"    借款金额：{_fmt_amount(loan.get('loan_amount'))} 万元",
            f"    余额：{_fmt_amount(loan.get('balance'))} 万元",
            f"    开立日期：{loan.get('start_date') or '未识别'}",
            f"    到期日：{loan.get('end_date') or '未识别'}",
            f"    五级分类：{loan.get('five_category') or '未识别'}",
            f"    逾期月数：{loan.get('overdue_months') if loan.get('overdue_months') is not None else 0}",
        ])
    return lines


def _business_markdown_lines(item: dict[str, Any]) -> list[str]:
    return [
        f"- 授信机构：{item.get('institution_name') or '未识别'}",
        f"  业务种类：{item.get('business_type') or '未识别'}",
        f"  五级分类：{item.get('five_category') or '未识别'}",
        f"  账户数：{item.get('account_count') if item.get('account_count') is not None else '未识别'}",
        f"  余额：{_fmt_amount(item.get('balance'))} 万元",
    ]


def _legacy_from_agent_loan(loan: dict[str, Any], term_type: str) -> dict[str, Any]:
    return {
        "bank": loan.get("institution_name"),
        "institution": loan.get("institution_name"),
        "biz_type": loan.get("business_type"),
        "loan_type": loan.get("business_type"),
        "guarantee": loan.get("guarantee_type"),
        "guarantee_type": loan.get("guarantee_type"),
        "loan_amount": _fmt_amount(loan.get("loan_amount")) if loan.get("loan_amount") is not None else "",
        "balance": _fmt_amount(loan.get("balance")) if loan.get("balance") is not None else "",
        "open_date": loan.get("start_date"),
        "start_date": loan.get("start_date"),
        "due_date": loan.get("end_date"),
        "end_date": loan.get("end_date"),
        "five_classification": loan.get("five_category"),
        "overdue_months": str(loan.get("overdue_months") or 0),
        "term_type": term_type,
        "evidence_text": loan.get("evidence_text"),
    }


def _legacy_from_agent_credit_line(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "institution": item.get("institution_name"),
        "bank": item.get("institution_name"),
        "credit_type": item.get("credit_type"),
        "is_revolving": "是" if item.get("credit_revolving") else "否" if item.get("credit_revolving") is False else "",
        "credit_amount": _fmt_amount(item.get("credit_amount")) if item.get("credit_amount") is not None else "",
        "used_amount": _fmt_amount(item.get("used_amount")) if item.get("used_amount") is not None else "",
        "effective_date": item.get("effective_date"),
        "due_date": item.get("expiry_date"),
        "evidence_text": item.get("evidence_text"),
    }
