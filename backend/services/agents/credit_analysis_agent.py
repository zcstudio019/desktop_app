from __future__ import annotations

from typing import Any

from .base import BaseAgent


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in (None, "", "未识别"):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


class CreditAnalysisAgent(BaseAgent):
    agent_name = "CreditAnalysisAgent"

    async def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        credit = context.get("enterprise_credit") or {}
        summary = credit.get("credit_summary") or {}
        agent_summary = (credit.get("agent_result") or {}).get("credit_summary") or {}
        credit_lines = credit.get("credit_facilities") or credit.get("credit_lines") or (credit.get("agent_result") or {}).get("credit_lines") or []
        short_loans = credit.get("short_loans") or credit.get("short_loans_final") or (credit.get("agent_result") or {}).get("short_term_loans") or []
        medium_loans = credit.get("medium_loans") or credit.get("medium_loans_final") or (credit.get("agent_result") or {}).get("medium_long_term_loans") or []
        active_loans = credit.get("active_loans") or [*short_loans, *medium_loans]

        credit_line_total = sum(_first_number(x.get("credit_amount"), x.get("total_limit")) or 0 for x in credit_lines if isinstance(x, dict))
        used_credit_line = sum(_first_number(x.get("used_amount"), x.get("used_limit")) or 0 for x in credit_lines if isinstance(x, dict))
        institutions = {
            x.get("bank") or x.get("institution") or x.get("institution_name") or ""
            for x in active_loans
            if isinstance(x, dict) and (x.get("bank") or x.get("institution") or x.get("institution_name"))
        }
        profile = {
            "total_unsettled_balance": _first_number(summary.get("active_borrowing_balance"), summary.get("unsettled_credit_balance"), agent_summary.get("unsettled_credit_balance")),
            "short_term_loan_balance": _first_number(summary.get("short_term_loan_balance"), summary.get("active_short_term_debt_total"), agent_summary.get("short_term_loan_balance")),
            "medium_long_term_loan_balance": _first_number(summary.get("medium_long_term_loan_balance"), summary.get("active_long_term_debt_total"), agent_summary.get("medium_long_term_loan_balance")),
            "credit_line_total": round(credit_line_total, 2) if credit_line_total else None,
            "used_credit_line": round(used_credit_line, 2) if used_credit_line else None,
            "external_guarantee_balance": _first_number(summary.get("guarantee_balance"), agent_summary.get("external_guarantee_balance")),
            "institution_count": summary.get("current_active_credit_institution_count") or summary.get("unsettled_credit_institution_count") or len(institutions) or None,
            "short_term_loan_count": len(short_loans),
            "medium_long_term_loan_count": len(medium_loans),
            "credit_line_count": len(credit_lines),
        }
        analysis = []
        if profile["short_term_loan_balance"]:
            analysis.append(f"短期借款余额约 {profile['short_term_loan_balance']} 万元")
        if profile["medium_long_term_loan_balance"]:
            analysis.append(f"中长期借款余额约 {profile['medium_long_term_loan_balance']} 万元")
        if profile["credit_line_total"] is not None:
            analysis.append(f"授信总额约 {profile['credit_line_total']} 万元，已用约 {profile['used_credit_line'] or 0} 万元")
        return {
            "agent_name": self.agent_name,
            "status": "success",
            "credit_profile": profile,
            "analysis": analysis,
            "warnings": [] if credit else ["未找到企业征信结构化结果"],
        }
