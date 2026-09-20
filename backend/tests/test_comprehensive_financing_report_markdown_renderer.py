"""Step 4 deterministic Markdown rendering tests."""

import asyncio
import json

import pytest

from backend.services import assistant_comprehensive_financing_analysis_service as assistant_service
from backend.services.comprehensive_financing_analysis_service import (
    ComprehensiveFinancingAnalysisResult,
)
from backend.services.comprehensive_financing_report_markdown_renderer import (
    render_comprehensive_financing_report,
)
from backend.services.comprehensive_financing_report_model import (
    ComprehensiveFinancingReportModel,
)


@pytest.fixture
def report_model():
    statuses = {
        "enterprise_kyc": ("partial", "2026-04-10"),
        "enterprise_credit": ("available", "2026-04-15"),
        "personal_credit": ("available", "2026-04-01"),
        "enterprise_cashflow": ("partial", "2026-03-31"),
        "personal_cashflow": ("missing", None),
        "financial_statements": ("available", "2026-03-31"),
        "assets": ("missing", None),
        "financing_requirement": ("missing", None),
        "risk_assessment": ("missing", None),
        "financing_plan": ("missing", None),
    }
    periods = [
        {"period": "2024-12-31", "period_type": "annual", "unit": "元", "revenue": 10000000,
         "net_profit": 500000, "total_assets": 40000000, "total_liabilities": 30000000, "net_assets": 10000000},
        {"period": "2025-12-31", "period_type": "annual", "unit": "元", "revenue": 15000000,
         "net_profit": 300000, "total_assets": 50000000, "total_liabilities": 47000000, "net_assets": 3000000},
        {"period": "2026-03-31", "period_type": "monthly", "unit": "元", "revenue": 422018.35,
         "operating_cost": 900000, "net_profit": -647521.89, "total_assets": 53789185.41,
         "total_liabilities": 53537100.71, "net_assets": 252084.70, "accounts_receivable": 12000000,
         "inventory": 500000, "short_term_borrowings": 12000000, "long_term_borrowings": 3000000,
         "operating_cashflow": 242143.13, "debt_asset_ratio": 0.9953},
    ]
    return ComprehensiveFinancingReportModel(
        customer={"name": "上海意川建筑科技有限公司", "id": "internal-135"},
        data_scope={"materials": [
            {"type": kind, "status": status, "latest_date": latest, "usable": status != "missing", "notes": ""}
            for kind, (status, latest) in statuses.items()
        ]},
        subject_profile={"status": "partial", "enterprise_name": "上海意川建筑科技有限公司",
                         "unified_social_credit_code": "91310000TEST", "legal_representative": "黎云",
                         "actual_controller": "黎云", "shareholders": [{"name": "黎云"}],
                         "registered_capital": "1000万元", "established_date": "2018-01-01",
                         "registered_address": "上海市", "business_scope": "建筑科技服务"},
        financing_requirement={"status": "missing"},
        enterprise_credit={"status": "available", "outstanding_loan_balance": 1856.5, "unit": "万元",
                           "outstanding_loan_institution_count": 4, "overdue_summary": {"count": 0},
                           "nonperforming_summary": {"count": 0}, "guarantee_balance": 100,
                           "loan_record_count": 7, "query_summary": {"count": 3},
                           "source_report_date": "2026-04-15"},
        personal_credit={"status": "available", "unit": "万元", "people": [{"name": "黎云",
                         "roles": ["法定代表人", "实际控制人"], "loan_balance": 300,
                         "credit_card_limit": 50, "credit_card_used": 10, "overdue_summary": {"count": 0},
                         "query_summary": {"last_3_months": 2, "last_6_months": 5},
                         "related_repayment_balance": 1856.5, "source_report_date": "2026-04-01"}]},
        enterprise_cashflow={"status": "partial", "unit": "元", "account_count": 2,
                             "statement_period": {"start": "2025-04-01", "end": "2026-03-31", "months": 12},
                             "total_inflow": 42499565.67, "operating_inflow": 19493700.00,
                             "internal_transfer_inflow": 7351500.00, "related_party_inflow": None,
                             "non_operating_inflow": 15654365.67, "monthly_average_operating_inflow": 1624475.00},
        personal_cashflow={"status": "missing"},
        financials={"status": "available", "periods": periods, "latest": periods[-1],
                    "latest_period": "2026-03-31", "period_count": 3},
        assets={"status": "missing"}, risk_context={"status": "missing"},
        existing_financing_plan={"status": "missing"},
        derived_metrics={"total_enterprise_credit_balance": 1856.5, "total_personal_credit_balance": 300,
                         "total_related_repayment_balance": 1856.5, "enterprise_total_inflow": 42499565.67,
                         "enterprise_operating_inflow": 19493700.00,
                         "monthly_average_operating_inflow": 1624475.00, "debt_asset_ratio": 0.9953},
        data_quality={"status": "partial"},
        source_dates={"enterprise_credit": "2026-04-15", "personal_credit": "2026-04-01",
                      "enterprise_cashflow_start": "2025-04-01", "enterprise_cashflow_end": "2026-03-31",
                      "financial_latest_period": "2026-03-31"},
    )


@pytest.fixture
def analysis_payload():
    section = lambda summary, sources: {"summary": summary, "source_sections": sources}
    return {
        "executive_summary": {"overall_observation": "现有资料可用于初步综合分析。",
            "current_financing_readiness": "needs_data_completion", "main_strengths": ["企业征信及经营流水可用"],
            "main_constraints": ["净资产基础较薄"], "key_missing_information": ["个人流水、资产资料和融资需求待补充"]},
        "business_analysis": section("主体和经营资料可供初步核验。", ["subject_profile", "data_quality"]),
        "cashflow_analysis": section("经营入账按已保存分类初步统计，关联方流入尚不能可靠量化，需核验。", ["enterprise_cashflow"]),
        "financial_analysis": section("最新财务为月度口径，与十二个月流水覆盖周期不同，不能直接进行收入比例比较。", ["financials", "source_dates"]),
        "credit_analysis": section("企业征信与个人征信均可用于摘要分析。", ["enterprise_credit", "personal_credit"]),
        "enterprise_person_linkage": section("企业贷款与法人相关还款责任可能存在同一债务关系重叠，不能简单加总。", ["enterprise_credit", "personal_credit"]),
        "asset_and_enhancement_analysis": section("当前资料中未找到可用于本次分析的稳定结构化资产资料。", ["assets"]),
        "financing_strengths": [{"title": "征信资料可用", "fact": "企业与个人征信均已获取",
                                  "impact": "可进入后续事实核验", "source_sections": ["enterprise_credit", "personal_credit"]}],
        "financing_constraints": [{"title": "资产资料待补充", "fact": "稳定结构化资产资料尚未获取",
                                     "impact": "抵押增信能力暂无法判断", "required_action": "补充资产权属和估值资料",
                                     "source_sections": ["assets"]}],
        "core_issues": [{"issue": "资料仍需补充", "facts": ["个人流水和融资需求尚未明确"],
                         "financing_impact": "影响融资路径进一步筛选", "next_action": "补充个人流水并确认融资需求",
                         "source_sections": ["personal_cashflow", "financing_requirement"]}],
        "financing_paths": [
            {"path": "信用融资", "status": "conditional", "basis": ["企业征信及经营流水可用"],
             "missing_conditions": ["融资需求待确认"], "source_sections": ["enterprise_credit", "enterprise_cashflow"]},
            {"path": "抵押融资", "status": "insufficient_data", "basis": [],
             "missing_conditions": ["资产资料待补充"], "source_sections": ["assets"]},
        ],
        "action_plan": {"immediate": [{"action": "确认融资需求", "basis": "融资需求尚未明确", "source_sections": ["financing_requirement"]}],
                        "short_term": [{"action": "补充个人流水", "basis": "可采信个人收入无法核验", "source_sections": ["personal_cashflow"]}],
                        "medium_term": []},
        "data_limitations": [
            {"material_type": "personal_cashflow", "limitation": "当前缺少可用个人流水", "impact": "个人收入无法核验", "required_data": "补充个人流水"},
            {"material_type": "assets", "limitation": "稳定结构化资产资料尚未获取", "impact": "增信能力无法判断", "required_data": "补充资产资料"},
            {"material_type": "financing_requirement", "limitation": "融资金额、用途和期限尚未确认", "impact": "路径筛选受限", "required_data": "确认融资需求"},
            {"material_type": "enterprise_cashflow_classification", "limitation": "关联方分类仍需核验", "impact": "经营入账为初步统计", "required_data": "核验关联关系"},
            {"material_type": "financial_cashflow_period_mismatch", "limitation": "月度财务与十二个月流水期间不同", "impact": "不宜直接比较", "required_data": "补充同期间资料"},
        ],
        "conclusion": {"overall": "企业具备进一步核验基础，但关键资料仍需补充。",
                       "financing_direction": "先补齐资料，再评估信用融资方向。",
                       "prerequisites": ["明确融资需求", "补充个人流水和资产资料"],
                       "one_sentence": "补齐关键资料并核验口径后，再进入下一步融资评估。"},
    }


@pytest.fixture
def analysis_result(analysis_payload):
    return ComprehensiveFinancingAnalysisResult.model_validate(analysis_payload)


@pytest.fixture
def markdown(report_model, analysis_result):
    return render_comprehensive_financing_report(report_model, analysis_result, "2026-09-20 10:00:00")


def test_comprehensive_markdown_has_title(markdown):
    assert markdown.startswith("# 客户综合融资分析报告")


@pytest.mark.parametrize("heading", [
    "## 数据范围", "## 一、客户融资画像", "## 二、企业经营与流水分析", "## 三、财务分析",
    "## 四、征信与负债分析", "## 六、融资优势", "## 七、融资障碍", "## 八、当前核心问题",
    "## 九、融资路径方向", "## 十、行动计划", "## 十一、资料缺口与分析限制", "## 十二、综合结论",
])
def test_comprehensive_markdown_has_required_sections(markdown, heading):
    assert heading in markdown


def test_comprehensive_markdown_has_data_scope(markdown):
    assert "| 企业征信 | 已获取 | 2026-04-15 |" in markdown
    assert "| 企业流水 | 部分资料 | 2025-04-01 至 2026-03-31 |" in markdown


def test_system_analysis_status_not_rendered_as_customer_data_scope(markdown):
    data_scope = markdown.split("## 一、客户融资画像", 1)[0]
    assert "风险评估" not in data_scope
    assert "已有融资方案" not in data_scope


def test_system_analysis_source_refs_not_rendered(report_model, analysis_payload):
    payload = json.loads(json.dumps(analysis_payload, ensure_ascii=False))
    payload["business_analysis"]["source_sections"].extend(["risk_context", "existing_financing_plan"])
    rendered = render_comprehensive_financing_report(
        report_model, ComprehensiveFinancingAnalysisResult.model_validate(payload), "2026-09-20 10:00:00"
    )
    assert "风险评估" not in rendered
    assert "已有融资方案" not in rendered


def test_comprehensive_markdown_has_subject_profile(markdown):
    assert "上海意川建筑科技有限公司" in markdown and "黎云" in markdown


def test_comprehensive_markdown_has_cashflow_section(markdown):
    assert "42,499,565.67元" in markdown
    assert "19,493,700.00元" in markdown
    assert "7,351,500.00元" in markdown
    assert "1,624,475.00元" in markdown


def test_comprehensive_markdown_has_financial_section(markdown):
    assert "99.53%" in markdown and "252,084.70元" in markdown
    assert "不能直接进行收入比例比较" in markdown


def test_comprehensive_markdown_has_credit_section(markdown):
    assert "1,856.5万元" in markdown and "企业与个人负债联动" in markdown


def test_enterprise_credit_uses_unambiguous_loan_record_count(markdown):
    assert "| 企业贷款记录数 | 7 |" in markdown
    assert "到期/待核验记录数" not in markdown


def test_comprehensive_markdown_has_financing_strengths(markdown):
    assert "### 征信资料可用" in markdown


def test_comprehensive_markdown_has_financing_constraints(markdown):
    assert "### 资产资料待补充" in markdown


def test_comprehensive_markdown_has_core_issues(markdown):
    assert "### 1. 资料仍需补充" in markdown


def test_comprehensive_markdown_has_financing_paths(markdown):
    assert "| 信用融资 | 有条件 |" in markdown
    assert "| 抵押融资 | 资料不足 |" in markdown


def test_comprehensive_markdown_has_action_plan(markdown):
    assert "### 🔴 立即处理" in markdown and "### 🟡 短期处理" in markdown


def test_comprehensive_markdown_has_data_limitations(markdown):
    assert "当前缺少可用个人流水" in markdown and "关联方分类仍需核验" in markdown


def test_system_analysis_missing_not_rendered_as_customer_data_limitation(report_model, analysis_payload):
    payload = json.loads(json.dumps(analysis_payload, ensure_ascii=False))
    payload["data_limitations"].extend([
        {"material_type": "risk_assessment", "limitation": "风险评估资料不足", "impact": "", "required_data": ""},
        {"material_type": "financing_plan", "limitation": "已有融资方案资料不足", "impact": "", "required_data": ""},
    ])
    rendered = render_comprehensive_financing_report(
        report_model, ComprehensiveFinancingAnalysisResult.model_validate(payload), "2026-09-20 10:00:00"
    )
    assert "风险评估资料不足" not in rendered
    assert "已有融资方案资料不足" not in rendered


def test_comprehensive_markdown_has_conclusion(markdown):
    assert "### 一句话结论" in markdown and "补齐关键资料并核验口径后" in markdown


def test_markdown_missing_assets_does_not_say_no_assets(markdown):
    assert all(term not in markdown for term in ("无资产", "没有资产", "暂无资产"))


def test_markdown_missing_personal_cashflow_does_not_say_no_income(markdown):
    assert all(term not in markdown for term in ("个人无收入", "没有收入", "未见收入"))


def test_markdown_missing_financing_requirement_does_not_say_no_financing_need(markdown):
    assert all(term not in markdown for term in ("无融资需求", "没有融资需求", "不需要融资"))


def test_markdown_does_not_contain_raw_ocr(markdown):
    assert "raw_ocr" not in markdown and "ocr_text" not in markdown


def test_markdown_does_not_contain_evidence(markdown):
    assert "evidence" not in markdown


def test_markdown_does_not_contain_internal_ids(markdown):
    assert "document_id" not in markdown and "extraction_id" not in markdown and "internal-135" not in markdown


@pytest.mark.parametrize("enum_name", ["confirmed", "available", "partial", "missing", "needs_review",
                                        "potential", "conditional", "insufficient_data", "enterprise_cashflow", "data_quality"])
def test_markdown_does_not_contain_internal_enum_names(markdown, enum_name):
    assert enum_name not in markdown


def test_markdown_does_not_call_llm(report_model, analysis_result, monkeypatch):
    from services.ai_service import AIService
    monkeypatch.setattr(AIService, "extract", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("renderer called LLM")))
    assert render_comprehensive_financing_report(report_model, analysis_result, "2026-09-20").startswith("# 客户综合融资分析报告")


def test_assistant_message_is_complete_markdown(report_model, analysis_payload, monkeypatch):
    async def fake_builder(_storage, _customer_id):
        return report_model
    monkeypatch.setattr(assistant_service, "build_comprehensive_financing_report_context", fake_builder)
    class Storage:
        async def get_customer(self, customer_id):
            return {"customer_id": customer_id, "name": "上海意川建筑科技有限公司"}
    result = asyncio.run(assistant_service.generate_comprehensive_financing_analysis(
        Storage(), "生成客户综合融资分析报告", "135", llm=lambda *_: json.dumps(analysis_payload, ensure_ascii=False),
    ))
    assert result["message"].startswith("# 客户综合融资分析报告")
    assert "## 十二、综合结论" in result["message"]
