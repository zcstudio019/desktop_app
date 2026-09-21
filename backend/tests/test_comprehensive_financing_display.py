"""User-visible wording stays Chinese without changing report facts."""

from __future__ import annotations

import copy
import re

import pytest

from backend.services.comprehensive_financing_analysis_service import ComprehensiveFinancingAnalysisResult
from backend.services.comprehensive_financing_report_display import assert_no_internal_english_terms, localize_report_text
from backend.services.comprehensive_financing_report_html_renderer import render_comprehensive_financing_report_html
from backend.services.comprehensive_financing_report_markdown_renderer import render_comprehensive_financing_report
from backend.tests.test_comprehensive_financing_report_markdown_renderer import analysis_payload, report_model


@pytest.fixture
def localized_reports(report_model, analysis_payload):
    payload = copy.deepcopy(analysis_payload)
    report_model.financials["periods"][0]["debt_asset_ratio"] = 0.75
    payload["executive_summary"]["overall_observation"] = (
        "KYC主体资料；企业征信逾期概要 count 为 0；企业流水为 partial 状态；"
        "企业流水数据质量status为partial；"
        "资产负债率 0.9953；上期资产负债率0.75；经营入账 19493700.0 元；主体科技企业标签为空。"
    )
    payload["business_analysis"]["summary"] = (
        "subject_profile、derived_metrics、data_quality 均可引用；available、confirmed、"
        "missing、needs_review、potential、conditional、insufficient_data 不应显示。"
    )
    analysis = ComprehensiveFinancingAnalysisResult.model_validate(payload)
    original_model = report_model.model_dump()
    original_analysis = analysis.model_dump()
    markdown = render_comprehensive_financing_report(report_model, analysis, "2026-09-20")
    html = render_comprehensive_financing_report_html(report_model, analysis, "2026-09-20")
    assert report_model.model_dump() == original_model
    assert analysis.model_dump() == original_analysis
    return markdown, html


def test_comprehensive_report_has_no_kyc_term(localized_reports):
    for report in localized_reports:
        assert "主体及身份资料" in report
        assert "KYC" not in report


def test_comprehensive_report_has_no_count_term(localized_reports):
    for report in localized_reports:
        assert "企业征信逾期记录数为0" in report
        visible = re.sub(r"<style>.*?</style>", "", report, flags=re.DOTALL)
        assert "count" not in visible


def test_comprehensive_report_has_no_partial_term(localized_reports):
    for report in localized_reports:
        assert "企业流水可用于初步分析，但部分交易分类仍需进一步核验" in report
        assert "partial" not in report


def test_comprehensive_report_has_no_status_term(localized_reports):
    for report in localized_reports:
        assert "status" not in report.lower()
        assert "企业流水可用于初步分析，但部分交易分类及关联关系仍需复核" in report


def test_comprehensive_report_has_no_internal_status_enum(localized_reports):
    markdown, html = localized_reports
    assert_no_internal_english_terms(markdown)
    assert_no_internal_english_terms(html, html=True)
    for report in localized_reports:
        for term in ("available", "missing", "confirmed", "needs_review", "potential",
                     "conditional", "insufficient_data", "source_sections", "derived_metrics", "data_quality"):
            assert term not in report


def test_comprehensive_report_uses_chinese_source_labels(localized_reports):
    for report in localized_reports:
        assert "主体及身份资料" in report
        assert "程序计算指标" in report
        assert "资料完整度" in report
        assert "当前未获取可核验的科技企业资质或相关认定资料" in report


def test_comprehensive_report_formats_debt_ratio_as_percent(localized_reports):
    for report in localized_reports:
        assert "资产负债率 99.53%" in report
        assert "上期资产负债率75.00%" in report
        assert "资产负债率 0.9953" not in report
        assert "资产负债率0.75" not in report


def test_comprehensive_report_formats_money_for_display(localized_reports):
    for report in localized_reports:
        assert "经营入账 19,493,700.00元" in report
        assert "19493700.0 元" not in report
        assert "1,856.5万元" in report


def test_comprehensive_report_keeps_dates_and_business_numbers(localized_reports):
    for report in localized_reports:
        assert "2026-09-20" in report
        assert "99.53%" in report


def test_display_check_rejects_internal_english_term():
    with pytest.raises(ValueError, match="内部词"):
        assert_no_internal_english_terms("企业流水为 partial 状态")


def test_yuan_amounts_use_exact_decimal_display_format():
    text = localize_report_text("短期借款1800000.0 元；相关责任18739532.0元；个人贷款3314569.0 元")
    assert text == "短期借款1,800,000.00元；相关责任18,739,532.00元；个人贷款3,314,569.00元"
