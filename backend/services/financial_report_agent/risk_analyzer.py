from __future__ import annotations

from typing import Any

from .skills.analyze_bank_credit_risk_skill import analyze_bank_credit_risk


def analyze_financial_credit_risk(data: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    analysis = analyze_bank_credit_risk(data, history)
    if hasattr(analysis, "model_dump"):
        return analysis.model_dump()
    return analysis.dict()
