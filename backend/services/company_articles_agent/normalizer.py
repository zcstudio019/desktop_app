from __future__ import annotations

from typing import Any

from .schema import CompanyArticlesResult, Shareholder


def _missing(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _text(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "未识别"


def normalize_company_articles(data: dict[str, Any], *, filename: str = "", raw_text: str = "") -> CompanyArticlesResult:
    shareholders = data.get("shareholders") if isinstance(data.get("shareholders"), list) else []
    normalized_shareholders: list[Shareholder] = []
    for item in shareholders:
        if isinstance(item, Shareholder):
            normalized_shareholders.append(item)
        elif isinstance(item, dict):
            normalized_shareholders.append(Shareholder(**{key: item.get(key) for key in Shareholder.__dataclass_fields__}))

    result = CompanyArticlesResult(
        title=_text(data.get("title")),
        company_name=_text(data.get("company_name")),
        company_address=_text(data.get("company_address")),
        business_scope=_text(data.get("business_scope")),
        registered_capital=_text(data.get("registered_capital")),
        registered_capital_amount=data.get("registered_capital_amount"),
        currency=_text(data.get("currency") or "人民币"),
        shareholders=normalized_shareholders,
        governance={key: _text(value) for key, value in (data.get("governance") or {}).items()},
        major_resolution_rules={key: _text(value) for key, value in (data.get("major_resolution_rules") or {}).items()},
        equity_transfer_summary=_text(data.get("equity_transfer_summary")),
        finance_and_profit_summary=_text(data.get("finance_and_profit_summary")),
        dissolution_and_liquidation_summary=_text(data.get("dissolution_and_liquidation_summary")),
        senior_management_obligations_summary=_text(data.get("senior_management_obligations_summary")),
        articles_effective_rule=_text(data.get("articles_effective_rule")),
        signature_info=data.get("signature_info") if isinstance(data.get("signature_info"), dict) else {},
        page_count=int(data.get("page_count") or 0),
        raw_text_preview=str(raw_text or "")[:500],
        metadata={"filename": filename},
    )
    if _missing(result.signature_info.get("signing_date")):
        result.signature_info["signing_date"] = "未填写/未识别"
    if _missing(result.signature_info.get("signature_page")):
        result.signature_info["signature_page"] = "未识别"
    if _missing(result.signature_info.get("has_signature_or_stamp")):
        result.signature_info["has_signature_or_stamp"] = "未识别"
    if _missing(result.signature_info.get("signature_detection_summary")):
        result.signature_info["signature_detection_summary"] = "未识别"
    return result
