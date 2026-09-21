"""User-visible wording stays Chinese without changing report facts."""

from __future__ import annotations

import copy
import re

import pytest

from backend.services.comprehensive_financing_analysis_service import ComprehensiveFinancingAnalysisResult
from backend.services.comprehensive_financing_report_display import assert_no_internal_english_terms, display_path_missing_conditions, localize_report_text
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


def test_partial_enterprise_cashflow_is_not_a_definitive_coverage_conclusion():
    rendered = localize_report_text(
        "企业流水净流入为负且经营流出高于经营流入。"
        "企业流水净流入为负，现金流覆盖能力不足。"
    )
    assert "企业流水收支结构仍需进一步核验" in rendered
    assert "当前不能据此直接判断企业真实经营现金流覆盖能力" in rendered
    assert "现金流覆盖能力不足" not in rendered
    assert "现金流覆盖能力不足" not in localize_report_text("按已保存分类初步统计的现金流覆盖能力不足")
    assert "企业流水净流入为负且经营流出高于经营流入" not in rendered


def test_missing_technology_and_asset_data_use_materials_language():
    rendered = localize_report_text(
        "科技企业标签为空；房产、车辆、设备、知识产权、股权、存款及其他抵押物清单均为空；"
        "资产资料缺失，未获取稳定结构化资产资料，各类资产清单均为空。"
        "当前缺少可核验科技企业资质或标签资料；科技标签。"
    )
    assert "当前未获取可核验的科技企业资质、认定或相关证明资料" in rendered
    assert "当前未获取可用于本次分析的稳定结构化资产资料" in rendered
    assert "标签为空" not in rendered and "清单均为空" not in rendered
    assert "标签资料" not in rendered and "科技标签" not in rendered


def test_comprehensive_report_section_numbers_match_in_markdown_and_html(localized_reports):
    markdown, html = localized_reports
    titles = [
        "五、资产与增信条件", "六、融资优势", "七、融资障碍", "八、当前核心问题",
        "九、融资路径方向", "十、行动计划", "十一、资料缺口与分析限制", "十二、综合结论",
    ]
    for report in (markdown, html):
        positions = [report.index(title) for title in titles]
        assert positions == sorted(positions)
        assert all(report.count(title) == 1 for title in titles)
        assert "五、融资优势" not in report


def test_partial_cashflow_remains_preliminary_in_both_reports(localized_reports):
    for report in localized_reports:
        assert "初步统计" in report
        assert "不代表最终核定真实经营收入" in report
        assert all(term not in report for term in (
            "现金流覆盖能力不足", "经营现金流恶化", "企业真实经营现金流为负", "偿债能力不足",
        ))


def test_credit_query_counts_have_no_unconfigured_threshold_judgment():
    rendered = localize_report_text(
        "近1年担保审查14次、近2年22次，反映个人层面存在较多担保相关审查记录。"
    )
    assert "近1年担保资格审查14次，近2年22次；当前仅作事实展示，具体影响需结合拟申请机构正式准入规则评估" in rendered
    assert all(word not in rendered for word in ("较多", "频繁", "偏高", "异常"))


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
        assert "当前未获取可核验的科技企业资质、认定或相关证明资料" in report


def test_regressed_cashflow_judgment_is_business_safe():
    source = (
        "企业流水净流入为负且内部互转规模较大。"
        "企业流水净流入为负，削弱流水对经营偿债能力的支撑。"
        "流水对经营偿债能力的支撑不足；真实经营现金流为负；现金流覆盖能力不足。"
    )
    rendered = localize_report_text(source)
    assert "企业流水收支结构仍需进一步核验" in rendered
    assert "按当前已保存分类口径，流出高于流入；但内部互转、关联关系及未识别交易尚未完全核验" in rendered
    assert "当前流水分类尚未完全稳定，需要剔除内部互转并核验关联方交易后" in rendered
    assert all(term not in rendered for term in (
        "净流入为负", "偿债能力的支撑", "真实经营现金流为负", "现金流覆盖能力不足",
    ))


def test_technology_path_gap_is_presentational_only():
    conditions = ["当前缺少可核验科技企业资质或标签资料"]
    assert display_path_missing_conditions("科技企业专项融资", conditions) == [
        "需补充可核验的科技企业资质、认定或相关证明资料，并明确融资需求"
    ]
    assert display_path_missing_conditions("信用融资", conditions) == conditions
    assert conditions == ["当前缺少可核验科技企业资质或标签资料"]
    assert display_path_missing_conditions("抵押融资", [
        "资产清单及权属证明", "资产评估或价值依据", "融资需求待确认"
    ]) == ["可核验的资产权属及可抵押状态资料", "可核验的资产估值依据", "融资需求待确认"]


def test_asset_list_and_action_use_materials_language():
    rendered = localize_report_text(
        "未获取资产清单；资产清单为空。补充稳定结构化资产资料，包括权属、估值及可抵押状态。"
    )
    assert "当前未获取可用于本次分析的稳定结构化资产资料" in rendered
    assert "补充可核验的资产权属、估值及可抵押状态资料" in rendered
    assert "资产清单" not in rendered


def test_debt_ratio_threshold_uses_percent_not_internal_decimal():
    rendered = localize_report_text(
        "资产负债率长期处于0.995以上。",
        debt_asset_ratio=0.9953,
        debt_asset_ratios=[0.9951, 0.9959, 0.9953],
    )
    assert "近三个已保存财务期间的资产负债率均在99%以上" in rendered
    assert "0.995" not in rendered


def test_unruled_intensity_words_use_facts_or_review_language():
    rendered = localize_report_text(
        "资产负债率极高；相关还款责任规模显著；潜在代偿压力较大。",
        debt_asset_ratio=0.9953,
    )
    assert "资产负债率为99.53%" in rendered
    assert "相关还款责任需结合被担保主体、余额构成及到期安排进一步评估" in rendered
    assert "相关责任需结合被担保主体、余额构成及到期安排进一步评估" in rendered
    assert all(term not in rendered for term in ("极高", "规模显著", "代偿压力较大"))


def test_frozen_report_asset_and_summary_regressions_use_business_language():
    rendered = localize_report_text(
        "资产资料未获取，未获取稳定结构化资产资料。"
        "补充个人流水与资产清单（含权属与估值依据）。"
        "资产清单、权属证明及估值依据。"
        "资产负债率99.53%、净资产252084.70元，财务杠杆处于极高水平。"
        "个人相关还款责任余额18739532元，规模显著。"
        "财务杠杆极高且净资产缓冲极薄。"
        "流水净流出、个人相关还款责任规模显著及融资需求缺失。"
    )
    assert "当前未获取可用于本次分析的稳定结构化资产资料" in rendered
    assert "补充个人流水，并补充可核验的资产权属、估值及可抵押状态资料" in rendered
    assert "可核验的资产权属、估值及可抵押状态资料" in rendered
    assert "18,739,532.00元" in rendered
    assert all(term not in rendered for term in (
        "资产清单", "规模显著", "极高", "流水净流出", "潜在代偿压力较大",
    ))
    assert "未获取当前未获取" not in localize_report_text("未获取科技企业资格标签")


def test_frozen_one_sentence_conclusion_remains_readable():
    source = (
        "逾期概要为零与连续企业流水构成有限信用基础，但高杠杆、流水净流出、"
        "个人相关还款责任规模显著及融资需求、个人流水、资产资料缺失，"
        "使融资推进需先补足关键材料并作有条件评估。"
    )
    rendered = localize_report_text(source)
    assert "企业及个人征信逾期概要为零，连续企业流水可供核验" in rendered
    assert "融资需求、个人流水与稳定结构化资产资料待补充后，再作有条件评估" in rendered
    assert all(term not in rendered for term in ("高杠杆", "流水净流出", "规模显著", "资产资料缺失"))


def test_regressed_wording_is_removed_from_markdown_and_html(report_model, analysis_payload):
    payload = copy.deepcopy(analysis_payload)
    payload["cashflow_analysis"]["summary"] = "企业流水净流入为负，削弱流水对经营偿债能力的支撑。"
    payload["financial_analysis"]["summary"] = "资产负债率长期处于0.995以上。"
    payload["core_issues"][0]["issue"] = "企业流水净流入为负且内部互转规模较大"
    payload["core_issues"][0]["financing_impact"] = "流水对经营偿债能力的支撑不足"
    payload["business_analysis"]["summary"] = "科技企业资格标签为空。"
    payload["asset_and_enhancement_analysis"]["summary"] = "未获取资产清单。"
    payload["action_plan"]["short_term"][0]["action"] = "补充稳定结构化资产资料，包括权属、估值及可抵押状态"
    payload["financing_paths"].append({
        "path": "科技企业专项融资", "status": "insufficient_data", "basis": [],
        "missing_conditions": ["当前缺少可核验科技企业资质或标签资料"],
        "source_sections": ["subject_profile", "financing_requirement"],
    })
    report_model.financials["periods"][0]["debt_asset_ratio"] = 0.9951
    report_model.financials["periods"][1]["debt_asset_ratio"] = 0.9959
    report_model.financials["periods"][2]["debt_asset_ratio"] = 0.9953
    analysis = ComprehensiveFinancingAnalysisResult.model_validate(payload)
    for report in (
        render_comprehensive_financing_report(report_model, analysis, "2026-09-20"),
        render_comprehensive_financing_report_html(report_model, analysis, "2026-09-20"),
    ):
        assert "企业流水收支结构仍需进一步核验" in report
        assert "需补充可核验的科技企业资质、认定或相关证明资料，并明确融资需求" in report
        assert "当前未获取可用于本次分析的稳定结构化资产资料" in report
        assert "近三个已保存财务期间的资产负债率均在99%以上" in report
        assert all(term not in report for term in (
            "净流入为负", "现金流覆盖能力不足", "偿债能力的支撑", "科技企业资格标签",
            "科技企业标签", "标签为空", "标签资料", "未获取资产清单", "0.995",
        ))


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
