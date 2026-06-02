from __future__ import annotations

from typing import Any

from backend.document_types import normalize_document_type_code
from backend.services.enterprise_bank_statement_agent.customer_flow_aggregator import (
    ENTERPRISE_FLOW_TYPES,
    aggregate_customer_enterprise_flows,
)


def _empty_result() -> dict[str, Any]:
    return {
        "has_enterprise_bank_flow": False,
        "flow_status": "unknown",
        "summary_metrics": {
            "period_start": None,
            "period_end": None,
            "month_count": 0,
            "total_income": None,
            "total_expense": None,
            "net_income": None,
            "average_monthly_income": None,
            "average_monthly_expense": None,
            "average_monthly_net_income": None,
        },
        "quality_metrics": {
            "stable_month_count": 0,
            "zero_or_low_income_month_count": 0,
            "large_in_out_count": 0,
            "internal_transfer_amount": None,
            "internal_transfer_ratio": None,
            "real_income_amount": None,
            "real_income_ratio": None,
        },
        "account_consistency": {
            "account_name": "",
            "company_name": "",
            "is_consistent": None,
            "warnings": [],
        },
        "key_risks": ["尚未上传企业流水，无法判断企业真实经营收入和还款来源"],
        "positive_signals": [],
        "recommended_actions": ["请补充近6-12个月企业银行流水，用于判断经营收入、流水稳定性和还款来源"],
        "summary": "尚未上传企业流水，当前报告无法判断企业经营收入、流水稳定性和还款来源。",
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean_list(items: list[Any]) -> list[str]:
    cleaned: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("元", "").replace("万元", "").strip()
    if not text or text in {"-", "None", "null", "未识别", "暂无"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, 4)


def _payload_from_extracted_data(extracted_data: dict[str, Any]) -> dict[str, Any]:
    for key in ("structured_data", "structured_json", "parsed_json", "extracted_json", "data", "report_json", "agent_result"):
        value = extracted_data.get(key)
        if isinstance(value, dict):
            return value
    return extracted_data


def _is_enterprise_flow_extraction(extraction: dict[str, Any]) -> bool:
    extraction_type = normalize_document_type_code(str(extraction.get("extraction_type") or extraction.get("document_type") or ""))
    if extraction_type in ENTERPRISE_FLOW_TYPES:
        return True
    data = _as_dict(extraction.get("extracted_data"))
    payload = _payload_from_extracted_data(data)
    normalized = normalize_document_type_code(str(payload.get("document_type") or payload.get("normalized_document_type") or ""))
    schema_version = str(data.get("schema_version") or payload.get("schema_version") or "")
    return normalized in ENTERPRISE_FLOW_TYPES or normalized == "enterprise_bank_statement" or schema_version.startswith("enterprise_bank_statement")


def _company_name_from_profile(kyc_profile: dict[str, Any] | None) -> str:
    profile = _as_dict(kyc_profile)
    enterprise = _as_dict(profile.get("enterprise_identity"))
    value = enterprise.get("company_name")
    if isinstance(value, dict):
        value = value.get("value")
    return str(value or "").strip()


def _normalize_name(value: str) -> str:
    text = str(value or "").strip()
    for token in ("（", "）", "(", ")", " ", "\u3000"):
        text = text.replace(token, "")
    return text


def _account_name(aggregated: dict[str, Any]) -> str:
    for account in _as_list(aggregated.get("accounts")):
        item = _as_dict(account)
        value = item.get("account_name") or item.get("company_name") or item.get("customer_name")
        if value:
            return str(value).strip()
    for source in _as_list(aggregated.get("source_files")):
        item = _as_dict(source)
        value = item.get("account_name") or item.get("company_name") or item.get("customer_name")
        if value:
            return str(value).strip()
    return ""


def _monthly_income(month: dict[str, Any]) -> float:
    return float(_number(_first(month.get("total_inflow"), month.get("inflow"), month.get("income"), month.get("credit_amount"))) or 0)


def _large_in_out_count(aggregated: dict[str, Any]) -> int:
    explicit = _number(_first(aggregated.get("large_in_out_count"), _as_dict(aggregated.get("summary")).get("large_in_out_count")))
    if explicit is not None:
        return int(explicit)
    count = 0
    for signal in _as_list(_as_dict(aggregated.get("risk_analysis")).get("signals")):
        text = f"{_as_dict(signal).get('code') or ''} {_as_dict(signal).get('title') or ''} {_as_dict(signal).get('description') or ''}"
        if "快进快出" in text or "large" in text.lower():
            count += 1
    return count


def _status_rank(value: str) -> int:
    return {"unknown": 0, "normal": 1, "attention": 2, "risky": 3}.get(value, 0)


def _max_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def build_enterprise_bank_flow_diagnostic_from_aggregated(
    aggregated: dict[str, Any] | None,
    kyc_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not aggregated or not _as_list(aggregated.get("source_files")):
        return _empty_result()

    summary = _as_dict(aggregated.get("summary"))
    period = _as_dict(aggregated.get("statement_period"))
    monthly_summary = _as_list(aggregated.get("monthly_summary") or aggregated.get("monthly_stats"))
    month_count = int(_number(_first(period.get("months_count"), summary.get("month_count"), aggregated.get("month_count"))) or 0)

    total_income = _number(_first(summary.get("total_inflow"), summary.get("raw_total_inflow"), summary.get("total_income")))
    total_expense = _number(_first(summary.get("total_outflow"), summary.get("raw_total_outflow"), summary.get("total_expense")))
    net_income = _number(_first(summary.get("net_cashflow"), summary.get("raw_net_cashflow"), summary.get("net_income")))
    if net_income is None and total_income is not None and total_expense is not None:
        net_income = round(total_income - total_expense, 2)

    average_income = _number(_first(summary.get("average_monthly_inflow"), summary.get("average_monthly_income")))
    if average_income is None and total_income is not None and month_count:
        average_income = round(total_income / month_count, 2)
    average_expense = _number(_first(summary.get("average_monthly_outflow"), summary.get("average_monthly_expense")))
    if average_expense is None and total_expense is not None and month_count:
        average_expense = round(total_expense / month_count, 2)
    average_net = _number(_first(summary.get("average_monthly_net_cashflow"), summary.get("average_monthly_net_income")))
    if average_net is None and net_income is not None and month_count:
        average_net = round(net_income / month_count, 2)

    monthly_incomes = [_monthly_income(_as_dict(item)) for item in monthly_summary]
    low_threshold = (average_income or 0) * 0.3 if average_income else 0
    zero_or_low_months = sum(1 for value in monthly_incomes if value <= low_threshold)
    stable_month_count = max(0, len(monthly_incomes) - zero_or_low_months)

    internal_transfer_amount = _number(
        _first(
            summary.get("internal_transfer_total"),
            summary.get("excluded_internal_transfer_amount"),
            _as_dict(aggregated.get("internal_transfer_summary")).get("total_amount"),
            aggregated.get("internal_transfer_amount"),
        )
    )
    internal_transfer_ratio = _ratio(internal_transfer_amount, total_income)
    real_income_amount = _number(
        _first(
            summary.get("operating_inflow"),
            summary.get("estimated_operating_inflow"),
            summary.get("real_income_amount"),
            _as_dict(aggregated.get("financing_view")).get("bank_recognizable_inflow"),
        )
    )
    real_income_ratio = _ratio(real_income_amount, total_income)
    large_in_out_count = _large_in_out_count(aggregated)

    account_name = _account_name(aggregated)
    company_name = _company_name_from_profile(kyc_profile)
    account_warnings: list[str] = []
    is_consistent: bool | None = None
    if account_name and company_name:
        is_consistent = _normalize_name(account_name) == _normalize_name(company_name)
        if not is_consistent:
            account_warnings.append("企业流水户名与营业执照企业名称不一致，请核对资料归属")

    key_risks: list[str] = []
    positive_signals: list[str] = []
    recommended_actions: list[str] = []
    flow_status = "normal"

    if average_net is not None and average_net < 0:
        flow_status = _max_status(flow_status, "risky")
        key_risks.append("企业流水净流入为负，需关注经营现金流压力")
    if zero_or_low_months >= 2:
        flow_status = _max_status(flow_status, "attention")
        key_risks.append("部分月份收入明显偏低，流水稳定性不足")
    if internal_transfer_ratio is not None and internal_transfer_ratio >= 0.3:
        flow_status = _max_status(flow_status, "attention")
        key_risks.append("流水中内部转账占比较高，需剔除后判断真实经营收入")
        recommended_actions.append("建议核对关联企业及同户名账户往来，剔除左手倒右手流水")
    if real_income_ratio is not None and real_income_ratio < 0.6:
        flow_status = _max_status(flow_status, "attention")
        key_risks.append("可采信经营收入占比较低，银行可能下调可认定流水")
        if real_income_ratio < 0.3:
            flow_status = _max_status(flow_status, "risky")
    if large_in_out_count > 0:
        flow_status = _max_status(flow_status, "attention")
        key_risks.append("存在大额快进快出流水，需核实交易背景")
    if is_consistent is False:
        flow_status = _max_status(flow_status, "risky")
        key_risks.append("企业流水户名与营业执照企业名称不一致，请核对资料归属")
    elif is_consistent is True:
        positive_signals.append("企业流水户名与企业主体名称一致")

    if total_income and total_income > 0 and not key_risks:
        positive_signals.append("企业流水已识别经营收入，暂未发现明显流水质量风险")
    if not recommended_actions:
        recommended_actions.append("建议结合发票、纳税申报和主要合同核验流水对应的真实经营背景")

    quality_metrics = {
        "stable_month_count": stable_month_count,
        "zero_or_low_income_month_count": zero_or_low_months,
        "large_in_out_count": large_in_out_count,
        "internal_transfer_amount": internal_transfer_amount,
        "internal_transfer_ratio": internal_transfer_ratio,
        "real_income_amount": real_income_amount,
        "real_income_ratio": real_income_ratio,
    }
    summary_metrics = {
        "period_start": period.get("start_date"),
        "period_end": period.get("end_date"),
        "month_count": month_count,
        "total_income": total_income,
        "total_expense": total_expense,
        "net_income": net_income,
        "average_monthly_income": average_income,
        "average_monthly_expense": average_expense,
        "average_monthly_net_income": average_net,
    }
    summary_text = (
        f"已读取企业流水，期间{summary_metrics['period_start'] or '未识别'}至{summary_metrics['period_end'] or '未识别'}，"
        f"月均收入{average_income if average_income is not None else '未识别'}，"
        f"月均净流入{average_net if average_net is not None else '未识别'}。"
    )
    if key_risks:
        summary_text += "需关注：" + "；".join(key_risks[:3]) + "。"
    else:
        summary_text += "暂未识别明显企业流水风险。"

    return {
        "has_enterprise_bank_flow": True,
        "flow_status": flow_status,
        "summary_metrics": summary_metrics,
        "quality_metrics": quality_metrics,
        "account_consistency": {
            "account_name": account_name,
            "company_name": company_name,
            "is_consistent": is_consistent,
            "warnings": account_warnings,
        },
        "key_risks": _clean_list(key_risks),
        "positive_signals": _clean_list(positive_signals),
        "recommended_actions": _clean_list(recommended_actions),
        "summary": summary_text,
    }


def build_enterprise_bank_flow_diagnostic_from_extractions(
    extractions: list[dict[str, Any]] | None,
    kyc_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flow_extractions = [item for item in (extractions or []) if isinstance(item, dict) and _is_enterprise_flow_extraction(item)]
    if not flow_extractions:
        return _empty_result()
    aggregated = aggregate_customer_enterprise_flows(flow_extractions)
    return build_enterprise_bank_flow_diagnostic_from_aggregated(aggregated, kyc_profile)


async def build_enterprise_bank_flow_diagnostic(
    storage: Any,
    customer_id: str,
    kyc_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if storage is None or not customer_id:
        return _empty_result()
    try:
        extractions = await storage.get_extractions_by_customer(str(customer_id))
    except Exception:
        extractions = []
    if not isinstance(extractions, list):
        return _empty_result()
    return build_enterprise_bank_flow_diagnostic_from_extractions(extractions, kyc_profile)
