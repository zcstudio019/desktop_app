from __future__ import annotations

from typing import Any

from backend.document_types import normalize_document_type_code


FINANCIAL_REPORT_TYPES = {"financial_report", "financial_data"}


def _empty_result() -> dict[str, Any]:
    return {
        "has_financial_statement": False,
        "financial_status": "unknown",
        "period": {
            "latest_period": None,
            "statement_type": None,
        },
        "profitability": {
            "revenue": None,
            "operating_cost": None,
            "gross_profit": None,
            "net_profit": None,
            "net_profit_margin": None,
        },
        "debt_capacity": {
            "total_assets": None,
            "total_liabilities": None,
            "owner_equity": None,
            "asset_liability_ratio": None,
            "short_term_borrowing": None,
            "long_term_borrowing": None,
        },
        "liquidity": {
            "current_assets": None,
            "current_liabilities": None,
            "current_ratio": None,
            "cash_balance": None,
        },
        "cash_flow": {
            "operating_cash_flow_net": None,
        },
        "key_risks": ["尚未上传财务报表，无法判断企业盈利能力、资产负债结构和偿债能力"],
        "positive_signals": [],
        "recommended_actions": ["请补充最近一年或最近一期财务报表，用于判断收入、利润、资产负债率和偿债能力"],
        "summary": "尚未上传财务报表，当前报告无法判断企业盈利能力、资产负债结构和偿债能力。",
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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
    if isinstance(value, dict):
        value = _first(
            value.get("normalized_value"),
            value.get("value"),
            value.get("amount"),
            value.get("current_value"),
            value.get("ending_balance"),
        )
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
            if key == "extracted_json" and isinstance(value.get("structured_json"), dict):
                return value["structured_json"]
            return value
    return extracted_data


def _is_financial_report_extraction(extraction: dict[str, Any]) -> bool:
    extraction_type = normalize_document_type_code(str(extraction.get("extraction_type") or extraction.get("document_type") or ""))
    if extraction_type in FINANCIAL_REPORT_TYPES:
        return True
    data = _as_dict(extraction.get("extracted_data"))
    payload = _payload_from_extracted_data(data)
    doc_type = normalize_document_type_code(str(payload.get("document_type") or payload.get("normalized_document_type") or ""))
    schema_version = str(data.get("schema_version") or payload.get("schema_version") or "")
    return doc_type == "financial_report" or schema_version.startswith("financial_report")


def _period_key(report: dict[str, Any]) -> str:
    info = _as_dict(report.get("company_info"))
    return str(
        _first(
            info.get("report_period_end"),
            info.get("report_date"),
            info.get("balance_sheet_date"),
            report.get("report_period_end"),
            report.get("report_date"),
            report.get("created_at"),
        )
        or ""
    )


def _latest_report(extractions: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    reports: list[dict[str, Any]] = []
    for extraction in extractions or []:
        if not isinstance(extraction, dict) or not _is_financial_report_extraction(extraction):
            continue
        data = _as_dict(extraction.get("extracted_data"))
        report = _payload_from_extracted_data(data)
        if isinstance(report, dict) and (report.get("balance_sheet") or report.get("income_statement") or report.get("cash_flow_statement")):
            if not report.get("created_at"):
                report = {**report, "created_at": extraction.get("created_at") or extraction.get("uploaded_at") or ""}
            reports.append(report)
    reports.sort(key=_period_key)
    return reports[-1] if reports else None


def _section(report: dict[str, Any], name: str) -> dict[str, Any]:
    return _as_dict(report.get(name))


def _pick_number(report: dict[str, Any], paths: list[tuple[str, str]]) -> float | None:
    for section_name, field_name in paths:
        section = _section(report, section_name) if section_name else report
        value = section.get(field_name)
        number = _number(value)
        if number is not None:
            return number
    return None


def _status_rank(value: str) -> int:
    return {"unknown": 0, "normal": 1, "attention": 2, "risky": 3}.get(value, 0)


def _max_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def build_financial_statement_diagnostic_from_report(report: dict[str, Any] | None) -> dict[str, Any]:
    if not report:
        return _empty_result()

    info = _as_dict(report.get("company_info"))
    revenue = _pick_number(report, [("income_statement", "revenue"), ("income_statement", "operating_revenue"), ("", "revenue"), ("", "营业收入"), ("", "主营业务收入")])
    operating_cost = _pick_number(report, [("income_statement", "operating_cost"), ("income_statement", "cost_of_sales"), ("", "operating_cost"), ("", "营业成本"), ("", "业务成本")])
    net_profit = _pick_number(report, [("income_statement", "net_profit"), ("", "net_profit"), ("", "净利润")])
    gross_profit = _pick_number(report, [("income_statement", "gross_profit"), ("", "gross_profit")])
    if gross_profit is None and revenue is not None and operating_cost is not None:
        gross_profit = round(revenue - operating_cost, 2)
    net_profit_margin = _ratio(net_profit, revenue)

    total_assets = _pick_number(report, [("balance_sheet", "total_assets"), ("", "total_assets"), ("", "资产总计")])
    total_liabilities = _pick_number(report, [("balance_sheet", "total_liabilities"), ("", "total_liabilities"), ("", "负债合计")])
    owner_equity = _pick_number(report, [("balance_sheet", "total_equity"), ("balance_sheet", "owner_equity"), ("", "owner_equity"), ("", "所有者权益合计")])
    asset_liability_ratio = _ratio(total_liabilities, total_assets)
    short_term_borrowing = _pick_number(report, [("balance_sheet", "short_term_loans"), ("balance_sheet", "short_term_borrowing"), ("", "short_term_borrowing"), ("", "短期借款")])
    long_term_borrowing = _pick_number(report, [("balance_sheet", "long_term_loans"), ("balance_sheet", "long_term_borrowing"), ("", "long_term_borrowing"), ("", "长期借款")])

    current_assets = _pick_number(report, [("balance_sheet", "current_assets_total"), ("balance_sheet", "total_current_assets"), ("", "current_assets"), ("", "流动资产合计")])
    current_liabilities = _pick_number(report, [("balance_sheet", "current_liabilities_total"), ("balance_sheet", "total_current_liabilities"), ("", "current_liabilities"), ("", "流动负债合计")])
    current_ratio = _ratio(current_assets, current_liabilities)
    cash_balance = _pick_number(report, [("balance_sheet", "cash_and_equivalents"), ("balance_sheet", "cash_balance"), ("", "cash_balance"), ("", "货币资金")])
    operating_cash_flow_net = _pick_number(report, [("cash_flow_statement", "net_operating_cash_flow"), ("cash_flow_statement", "operating_cash_flow_net"), ("", "operating_cash_flow_net"), ("", "经营活动现金流量净额")])

    key_risks: list[str] = []
    positive_signals: list[str] = []
    recommended_actions: list[str] = []
    financial_status = "normal"

    if net_profit is not None and net_profit < 0:
        financial_status = _max_status(financial_status, "attention")
        key_risks.append("企业净利润为负，盈利能力需关注")
        if revenue is not None and revenue > 0 and abs(net_profit) / revenue >= 0.2:
            financial_status = _max_status(financial_status, "risky")
            key_risks.append("企业亏损幅度较大，新增融资审批需重点关注")
    if revenue is not None and revenue > 0 and net_profit_margin is not None and net_profit_margin > 0:
        positive_signals.append("企业有营业收入和正向利润表现")

    if asset_liability_ratio is not None:
        if asset_liability_ratio >= 1:
            financial_status = _max_status(financial_status, "risky")
            key_risks.append("负债总额超过资产总额，存在资不抵债风险")
        elif asset_liability_ratio >= 0.8:
            financial_status = _max_status(financial_status, "attention")
            key_risks.append("资产负债率较高，新增融资空间可能受限")
        elif asset_liability_ratio < 0.6:
            positive_signals.append("资产负债率相对可控")

    if current_ratio is not None:
        if current_ratio < 1:
            financial_status = _max_status(financial_status, "attention")
            key_risks.append("流动比率低于1，短期偿债能力需关注")
        elif current_ratio >= 1.5:
            positive_signals.append("流动比率较好，短期偿债能力相对稳定")

    if operating_cash_flow_net is not None:
        if operating_cash_flow_net < 0:
            financial_status = _max_status(financial_status, "attention")
            key_risks.append("经营活动现金流量净额为负，需关注经营现金回款能力")
        elif operating_cash_flow_net > 0:
            positive_signals.append("经营活动现金流为正，具备一定经营造血能力")

    if short_term_borrowing is not None and short_term_borrowing > 0:
        if total_assets and short_term_borrowing / total_assets >= 0.3:
            financial_status = _max_status(financial_status, "attention")
            key_risks.append("短期借款金额较高，需关注短期偿债压力")

    if not recommended_actions:
        recommended_actions.append("建议结合财报附注、纳税申报和企业流水核验收入利润真实性")
    if not key_risks and not positive_signals:
        positive_signals.append("财务数据暂未识别明显异常风险")

    latest_period = str(_first(info.get("report_period_end"), info.get("report_date"), report.get("report_period_end"), report.get("report_date")) or "")
    statement_type = str(_first(info.get("report_type"), report.get("statement_type"), report.get("report_type")) or "")
    summary_text = (
        f"已读取财务数据，最近期间{latest_period or '未识别'}，"
        f"营业收入{revenue if revenue is not None else '未识别'}，"
        f"净利润{net_profit if net_profit is not None else '未识别'}。"
    )
    if key_risks:
        summary_text += "需关注：" + "；".join(key_risks[:3]) + "。"
    else:
        summary_text += "暂未识别明显财务数据风险。"

    return {
        "has_financial_statement": True,
        "financial_status": financial_status,
        "period": {
            "latest_period": latest_period or None,
            "statement_type": statement_type or None,
        },
        "profitability": {
            "revenue": revenue,
            "operating_cost": operating_cost,
            "gross_profit": gross_profit,
            "net_profit": net_profit,
            "net_profit_margin": net_profit_margin,
        },
        "debt_capacity": {
            "total_assets": total_assets,
            "total_liabilities": total_liabilities,
            "owner_equity": owner_equity,
            "asset_liability_ratio": asset_liability_ratio,
            "short_term_borrowing": short_term_borrowing,
            "long_term_borrowing": long_term_borrowing,
        },
        "liquidity": {
            "current_assets": current_assets,
            "current_liabilities": current_liabilities,
            "current_ratio": current_ratio,
            "cash_balance": cash_balance,
        },
        "cash_flow": {
            "operating_cash_flow_net": operating_cash_flow_net,
        },
        "key_risks": _clean_list(key_risks),
        "positive_signals": _clean_list(positive_signals),
        "recommended_actions": _clean_list(recommended_actions),
        "summary": summary_text,
    }


def build_financial_statement_diagnostic_from_extractions(extractions: list[dict[str, Any]] | None) -> dict[str, Any]:
    report = _latest_report(extractions)
    return build_financial_statement_diagnostic_from_report(report)


async def build_financial_statement_diagnostic(storage: Any, customer_id: str) -> dict[str, Any]:
    if storage is None or not customer_id:
        return _empty_result()
    try:
        extractions = await storage.get_extractions_by_customer(str(customer_id))
    except Exception:
        extractions = []
    if not isinstance(extractions, list):
        return _empty_result()
    return build_financial_statement_diagnostic_from_extractions(extractions)
