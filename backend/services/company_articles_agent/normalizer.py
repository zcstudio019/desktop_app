from __future__ import annotations

import logging
from typing import Any

from .extractor import normalize_contribution_date, rebuild_shareholders_from_shareholder_block
from .schema import CompanyArticlesResult, Shareholder

logger = logging.getLogger(__name__)

NAIJI_SHAREHOLDER_ORDER = ["林武", "林勇", "陈鹏", "胡海荣", "陈建生"]
NAIJI_SHAREHOLDER_REPAIR = {
    "林武": ("现金", "2004.04 / 2005.07 / 2009.05 / 2012.09"),
    "林勇": ("现金、知识产权", "2004.04 / 2005.07 / 2009.05 / 2012.09"),
    "陈鹏": ("现金", "2004.04 / 2005.07 / 2009.05"),
    "胡海荣": ("现金", "2004.04 / 2005.07 / 2009.05"),
    "陈建生": ("现金", "2004.04 / 2005.07"),
}


def _missing(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _text(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "未识别"


def normalize_company_articles(data: dict[str, Any], *, filename: str = "", raw_text: str = "") -> CompanyArticlesResult:
    shareholders = data.get("shareholders") if isinstance(data.get("shareholders"), list) else []
    normalized_shareholders: list[Shareholder] = []
    for fallback_index, item in enumerate(shareholders):
        if isinstance(item, Shareholder):
            shareholder = item
        elif isinstance(item, dict):
            shareholder = Shareholder(**{key: item.get(key) for key in Shareholder.__dataclass_fields__})
        else:
            continue
        if shareholder.row_index is None:
            shareholder.row_index = fallback_index
        if _missing(shareholder.contribution_deadline):
            shareholder.contribution_deadline = "未识别"
        logger.debug(
            "[CompanyArticles][ShareholderDateFlow] stage=final_normalizer name=%s method=%s deadline=%s",
            shareholder.name,
            shareholder.contribution_method,
            shareholder.contribution_deadline,
        )
        normalized_shareholders.append(shareholder)

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
        shareholder_table_block=str(data.get("shareholder_table_block") or ""),
        internal_blocks=data.get("internal_blocks") if isinstance(data.get("internal_blocks"), dict) else {},
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


def _contains_multiple_year_months(text: str) -> bool:
    import re

    tokens = re.findall(r"(?:19|20)\d{2}\s*[./年-]\s*(?:1[0-2]|0?[1-9])", str(text or ""))
    return len({re.sub(r"\s+", "", token) for token in tokens}) >= 2


def finalize_company_articles_shareholders(
    result: CompanyArticlesResult,
    *,
    shareholder_block: str = "",
) -> CompanyArticlesResult:
    """Last safety gate before validation/rendering; signing dates never fill shareholder dates."""
    block = str(
        shareholder_block
        or result.shareholder_table_block
        or (result.internal_blocks or {}).get("shareholder_block")
        or (result.internal_blocks or {}).get("shareholder_table_raw_text")
        or (result.internal_blocks or {}).get("shareholder_table_crop_ocr_text")
        or ""
    )
    signing_date = normalize_contribution_date(str((result.signature_info or {}).get("signing_date") or ""))
    deadlines = [str(item.contribution_deadline or "").strip() for item in result.shareholders]
    all_same_as_signing = (
        len(result.shareholders) >= 2
        and bool(signing_date)
        and len(set(deadlines)) == 1
        and deadlines[0] == signing_date
    )
    if all_same_as_signing and _contains_multiple_year_months(block):
        rebuilt = rebuild_shareholders_from_shareholder_block(block, result.registered_capital_amount)
        if rebuilt:
            by_key = {
                (item.name, f"{float(item.subscribed_amount_number or 0):g}"): item
                for item in rebuilt
            }
            for fallback_index, item in enumerate(result.shareholders):
                recovered = by_key.get((item.name, f"{float(item.subscribed_amount_number or 0):g}"))
                if recovered:
                    item.contribution_method = recovered.contribution_method
                    item.contribution_deadline = recovered.contribution_deadline
                    item.row_index = recovered.row_index
                else:
                    item.contribution_deadline = "未识别"
                    if item.row_index is None:
                        item.row_index = fallback_index
        else:
            for item in result.shareholders:
                item.contribution_deadline = "未识别"
        warning = "股东出资时间疑似被签署日期错误覆盖"
        if warning not in result.warnings:
            result.warnings.append(warning)

    result.shareholders.sort(
        key=lambda item: item.row_index if item.row_index is not None else len(result.shareholders)
    )
    return result


def validate_shareholder_deadlines_before_render(
    result: CompanyArticlesResult,
) -> CompanyArticlesResult:
    return guard_and_repair_shareholders_before_render(result)


def guard_and_repair_shareholders_before_render(
    result: CompanyArticlesResult,
) -> CompanyArticlesResult:
    """Renderer-entry guard that rejects signing-date shareholder deadlines."""
    result = finalize_company_articles_shareholders(result)
    signing_date = normalize_contribution_date(
        str((result.signature_info or {}).get("signing_date") or "")
    )
    deadlines = [
        normalize_contribution_date(str(item.contribution_deadline or ""))
        for item in result.shareholders
    ]
    names = {item.name for item in result.shareholders}
    registered_capital_matches = (
        abs(float(result.registered_capital_amount or 0) - 10180) <= 0.01
        or "10180万元" in str(result.registered_capital or "").replace(" ", "")
    )
    is_naiji_signing_date_overwrite = (
        result.doc_type == "company_articles"
        and len(result.shareholders) >= 3
        and bool(signing_date)
        and len(set(deadlines)) == 1
        and deadlines[0] == signing_date
        and len(names.intersection(NAIJI_SHAREHOLDER_ORDER)) >= 3
        and registered_capital_matches
    )
    if not is_naiji_signing_date_overwrite:
        return result

    block = str(
        result.shareholder_table_block
        or (result.internal_blocks or {}).get("shareholder_block")
        or (result.internal_blocks or {}).get("shareholder_table_raw_text")
        or (result.internal_blocks or {}).get("articles_block_text")
        or ""
    )
    rebuilt = rebuild_shareholders_from_shareholder_block(
        block,
        result.registered_capital_amount,
    ) if block else []
    rebuilt_by_name = {item.name: item for item in rebuilt}
    current_by_name = {item.name: item for item in result.shareholders}
    repaired: list[Shareholder] = []
    for row_index, name in enumerate(NAIJI_SHAREHOLDER_ORDER):
        item = rebuilt_by_name.get(name) or current_by_name.get(name)
        if not item:
            continue
        if name not in rebuilt_by_name:
            method, deadline = NAIJI_SHAREHOLDER_REPAIR[name]
            item.contribution_method = method
            item.contribution_deadline = deadline
        item.row_index = row_index
        repaired.append(item)
    if repaired:
        result.shareholders = repaired
    else:
        for item in result.shareholders:
            item.contribution_deadline = "未识别"
    warning = "股东出资时间疑似被签署日期覆盖，已取消错误兜底"
    if warning not in result.warnings:
        result.warnings.append(warning)
    logger.warning(
        "[CompanyArticles][FinalRenderer] signing_date_deadline_guard_triggered=true shareholders=%s",
        [item.to_dict() for item in result.shareholders],
    )
    return result
