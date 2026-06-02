from backend.services.comprehensive_financing_advice_service import build_comprehensive_financing_advice
from backend.services.customer_financing_diagnostic_report_service import build_customer_financing_diagnostic_report


def _kyc(readiness_level="ready", usable=True, score=90):
    return {
        "usable_for_financing": usable,
        "readiness_level": readiness_level,
        "score": score,
        "summary": "KYC资料可用于初步融资评估",
    }


def _enterprise_credit(status="normal", has_report=True, credit_usage_rate=0.3, upcoming_due_loans=None):
    return {
        "has_enterprise_credit_report": has_report,
        "credit_status": status,
        "debt_summary": {
            "total_unsettled_balance": 100,
            "credit_limit_total": 500,
            "used_credit_total": 150,
            "credit_usage_rate": credit_usage_rate,
        },
        "loan_summary": {
            "active_loan_count": 1,
            "upcoming_due_loans": upcoming_due_loans or [],
            "overdue_loans": [],
            "abnormal_classification_loans": [],
        },
        "guarantee_summary": {
            "has_external_guarantee": False,
            "external_guarantee_balance": None,
            "guarantee_risks": [],
        },
        "key_risks": ["企业征信存在风险"] if status == "risky" else ([] if status == "normal" else ["企业征信存在需关注事项"]),
        "positive_signals": ["企业征信整体正常"] if status == "normal" else [],
        "recommended_actions": [],
        "summary": "企业征信诊断完成",
    }


def _personal_credit(status="normal", has_report=True):
    return {
        "has_personal_credit_report": has_report,
        "credit_status": status,
        "debt_summary": {
            "loan_balance": 50,
            "credit_card_used_amount": 10,
            "external_guarantee_balance": 0,
        },
        "overdue_summary": {
            "has_loan_overdue": False,
            "has_credit_card_overdue": False,
            "overdue_records": [],
        },
        "query_summary": {
            "last_3_months_query_count": 1,
            "last_6_months_query_count": 2,
            "query_risk_level": "low",
        },
        "serious_negative_summary": {
            "has_serious_negative": False,
            "items": [],
        },
        "key_risks": ["个人征信存在风险"] if status == "risky" else ([] if status == "normal" else ["个人征信存在需关注事项"]),
        "positive_signals": ["个人征信整体正常"] if status == "normal" else [],
        "recommended_actions": [],
        "summary": "个人征信诊断完成",
    }


def _flow(status="normal", has_flow=True, total_income=1200, net_income=300, real_ratio=0.8):
    return {
        "has_enterprise_bank_flow": has_flow,
        "flow_status": status,
        "summary_metrics": {
            "month_count": 6,
            "total_income": total_income,
            "total_expense": total_income - net_income,
            "net_income": net_income,
            "average_monthly_income": total_income / 6,
            "average_monthly_expense": (total_income - net_income) / 6,
            "average_monthly_net_income": net_income / 6,
        },
        "quality_metrics": {
            "stable_month_count": 6,
            "zero_or_low_income_month_count": 0,
            "large_in_out_count": 0,
            "internal_transfer_amount": 0,
            "internal_transfer_ratio": 0,
            "real_income_amount": total_income * real_ratio,
            "real_income_ratio": real_ratio,
        },
        "account_consistency": {
            "account_name": "示例公司",
            "company_name": "示例公司",
            "is_consistent": True,
            "warnings": [],
        },
        "key_risks": ["企业流水存在风险"] if status == "risky" else ([] if status == "normal" else ["企业流水存在需关注事项"]),
        "positive_signals": ["企业流水收入稳定"] if status == "normal" else [],
        "recommended_actions": [],
        "summary": "企业流水诊断完成",
    }


def _financial(status="normal", has_statement=True, revenue=1000, net_profit=100):
    return {
        "has_financial_statement": has_statement,
        "financial_status": status,
        "period": {"latest_period": "2025", "statement_type": "annual"},
        "profitability": {
            "revenue": revenue,
            "operating_cost": 600,
            "gross_profit": 400,
            "net_profit": net_profit,
            "net_profit_margin": net_profit / revenue if revenue else None,
        },
        "debt_capacity": {
            "total_assets": 2000,
            "total_liabilities": 800,
            "owner_equity": 1200,
            "asset_liability_ratio": 0.4,
        },
        "liquidity": {"current_ratio": 1.5},
        "cash_flow": {"operating_cash_flow_net": 100},
        "key_risks": ["财务数据存在风险"] if status == "risky" else ([] if status == "normal" else ["财务数据存在需关注事项"]),
        "positive_signals": ["财务盈利能力正常"] if status == "normal" else [],
        "recommended_actions": [],
        "summary": "财务数据诊断完成",
    }


def _report(
    *,
    kyc=None,
    enterprise_credit=None,
    personal_credit=None,
    flow=None,
    financial=None,
    risk_highlights=None,
):
    return {
        "financing_readiness": kyc or _kyc(),
        "enterprise_credit_diagnostic": enterprise_credit if enterprise_credit is not None else _enterprise_credit(),
        "personal_credit_diagnostic": personal_credit if personal_credit is not None else _personal_credit(),
        "enterprise_bank_flow_diagnostic": flow if flow is not None else _flow(),
        "financial_statement_diagnostic": financial if financial is not None else _financial(),
        "material_checklist": {"required_missing": [], "recommended_supplements": []},
        "risk_highlights": risk_highlights or [],
        "next_actions": [],
    }


def _profile(with_property=True):
    return {
        "assets": {
            "properties": [{"owner": "张三", "property_address": "上海市示例路1号"}] if with_property else [],
            "vehicles": [],
        }
    }


def _product(advice, product_type):
    return next(item for item in advice["recommended_product_directions"] if item["product_type"] == product_type)


def test_kyc_not_ready_makes_overall_not_ready():
    advice = build_comprehensive_financing_advice(_report(kyc=_kyc("not_ready", False, 40)), _profile())

    assert advice["overall_status"] == "not_ready"


def test_enterprise_credit_risky_makes_overall_not_ready():
    advice = build_comprehensive_financing_advice(_report(enterprise_credit=_enterprise_credit("risky")), _profile())

    assert advice["overall_status"] == "not_ready"


def test_personal_credit_risky_makes_overall_not_ready():
    advice = build_comprehensive_financing_advice(_report(personal_credit=_personal_credit("risky")), _profile())

    assert advice["overall_status"] == "not_ready"


def test_enterprise_flow_risky_makes_overall_not_ready():
    advice = build_comprehensive_financing_advice(_report(flow=_flow("risky")), _profile())

    assert advice["overall_status"] == "not_ready"


def test_financial_data_risky_makes_overall_not_ready():
    advice = build_comprehensive_financing_advice(_report(financial=_financial("risky")), _profile())

    assert advice["overall_status"] == "not_ready"


def test_attention_without_risky_makes_cautious():
    advice = build_comprehensive_financing_advice(_report(enterprise_credit=_enterprise_credit("attention")), _profile())

    assert advice["overall_status"] == "cautious"


def test_all_normal_makes_recommendable_or_high_quality():
    advice = build_comprehensive_financing_advice(_report(), _profile())

    assert advice["overall_status"] in {"recommendable", "high_quality"}
    assert advice["financing_readiness_score"] >= 90


def test_property_asset_makes_mortgage_loan_high_fit():
    advice = build_comprehensive_financing_advice(_report(), _profile(with_property=True))

    assert _product(advice, "mortgage_loan")["fit_level"] == "high"


def test_no_enterprise_flow_makes_bank_flow_loan_low_fit():
    advice = build_comprehensive_financing_advice(_report(flow=_flow(has_flow=False, status="unknown")), _profile())

    assert _product(advice, "bank_flow_loan")["fit_level"] == "low"


def test_comprehensive_advice_enters_financing_report():
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "示例客户"},
        _profile(),
        {"required_missing": []},
        {
            "material_completeness_score": 90,
            "usable_for_financing": True,
            "readiness_level": "ready",
            "summary": "KYC资料可用于初步融资评估",
        },
        _enterprise_credit(),
        _personal_credit(),
        _flow(),
        _financial(),
    )

    assert report["comprehensive_financing_advice"]["overall_status"] in {"recommendable", "high_quality"}


def test_report_markdown_contains_comprehensive_financing_advice_section():
    report = build_customer_financing_diagnostic_report(
        "customer-1",
        {"name": "示例客户"},
        _profile(),
        {"required_missing": []},
        {
            "material_completeness_score": 90,
            "usable_for_financing": True,
            "readiness_level": "ready",
            "summary": "KYC资料可用于初步融资评估",
        },
        _enterprise_credit(),
        _personal_credit(),
        _flow(),
        _financial(),
    )

    assert "综合融资建议" in report["report_markdown"]
