"""Step 3 analyzes the Step 2 facts model only; no storage or live LLM in tests."""

import asyncio
import copy
import json

import pytest

from backend.services.comprehensive_financing_report_model import ComprehensiveFinancingReportModel
from backend.services.comprehensive_financing_analysis_service import (
    ComprehensiveFinancingAnalysisResult,
    analyze_comprehensive_financing_report,
    build_conservative_analysis_fallback,
    build_safe_analysis_context,
    validate_analysis_result,
    build_analysis_input_debug_summary,
)
from backend.services import assistant_comprehensive_financing_analysis_service as assistant_analysis


@pytest.fixture
def facts():
    statuses = {"enterprise_kyc": "partial", "enterprise_credit": "available", "personal_credit": "available",
                "enterprise_cashflow": "partial", "personal_cashflow": "missing", "financial_statements": "available",
                "assets": "missing", "financing_requirement": "missing", "risk_assessment": "missing", "financing_plan": "missing"}
    return ComprehensiveFinancingReportModel(
        customer={"name": "测试企业", "id": "internal-123"},
        data_scope={"materials": [{"type": key, "status": value} for key, value in statuses.items()]},
        subject_profile={"status": "partial", "enterprise_name": "测试企业", "technology_enterprise_tags": []},
        financing_requirement={"status": "missing"},
        enterprise_credit={"status": "available", "outstanding_loan_balance": 100, "unit": "万元"},
        personal_credit={"status": "available", "people": [{"name": "张三", "roles": ["法定代表人"], "loan_balance": 40}]},
        enterprise_cashflow={"status": "partial", "unit": "元", "total_inflow": 2000000,
                             "operating_inflow": 800000, "internal_transfer_inflow": 500000,
                             "related_party_inflow": None, "non_operating_inflow": 700000,
                             "statement_period": {"start": "2025-01-01", "end": "2025-12-31", "months": 12}},
        personal_cashflow={"status": "missing"},
        financials={"status": "available", "latest": {"revenue": 10000, "debt_asset_ratio": 0.9,
                     "period": "2025-12-31", "period_type": "monthly", "unit": "元"},
                    "periods": [{"revenue": 10000, "period": "2025-12-31", "period_type": "monthly", "unit": "元"}]},
        assets={"status": "missing"}, risk_context={"status": "missing"}, existing_financing_plan={"status": "missing"},
        derived_metrics={"debt_asset_ratio": 0.9}, source_dates={"enterprise_cashflow_end": "2025-12-31"},
    )


@pytest.fixture
def valid_payload(facts):
    customer_material_types = {
        "enterprise_kyc", "enterprise_credit", "personal_credit", "enterprise_cashflow",
        "personal_cashflow", "financial_statements", "assets", "financing_requirement",
    }
    missing = [m["type"] for m in facts.data_scope["materials"]
               if m["type"] in customer_material_types and m["status"] == "missing"]
    missing.append("financial_cashflow_period_mismatch")
    section = lambda summary, sources: {"summary": summary, "source_sections": sources}
    return {
        "executive_summary": {"overall_observation": "经营和财务资料可供初步分析，仍需补齐关键资料。",
            "current_financing_readiness": "needs_data_completion", "main_strengths": ["有已保存经营流水"],
            "main_constraints": ["资料尚不完整"], "key_missing_information": missing},
        "business_analysis": section("经营信息可供初步分析。", ["subject_profile", "financials"]),
        "cashflow_analysis": section("流水按已保存分类初步统计，关联方流入尚不能可靠量化，需核验。", ["enterprise_cashflow"]),
        "financial_analysis": section("最新财务为月度，不能直接与全年流水比较。", ["financials", "enterprise_cashflow"]),
        "credit_analysis": section("企业与个人征信分别观察。", ["enterprise_credit", "personal_credit"]),
        "enterprise_person_linkage": section("企业融资与个人相关还款责任不能简单加总。", ["enterprise_credit", "personal_credit"]),
        "asset_and_enhancement_analysis": section("当前未获取稳定结构化资产资料，无法判断增信能力。", ["assets"]),
        "financing_strengths": [{"title": "资料基础", "fact": "有已保存企业流水", "impact": "可供初步核验",
                                 "source_sections": ["enterprise_cashflow"]}],
        "financing_constraints": [
            {"title": "个人收入待核验", "fact": "缺少可用个人流水", "impact": "无法核验稳定可采信个人收入",
             "required_action": "补充个人流水", "source_sections": ["personal_cashflow"]},
            {"title": "资本结构需核验", "fact": "资产负债率较高", "impact": "需核验偿债安全边际",
             "required_action": "补充完整财务", "source_sections": ["financials"]},
            {"title": "企业流水结构需核验", "fact": "经营入账为初步分类", "impact": "经营来源仍需核验",
             "required_action": "核验关联方及内部互转", "source_sections": ["enterprise_cashflow"]},
        ],
        "core_issues": [{"issue": "资料不完整", "facts": ["个人流水缺失"], "financing_impact": "影响下一步评估",
                         "next_action": "补齐个人流水", "source_sections": ["personal_cashflow"]}],
        "financing_paths": [
            {"path": "信用融资", "status": "conditional", "basis": ["有企业征信"], "missing_conditions": ["明确需求"], "source_sections": ["enterprise_credit", "financing_requirement"]},
            {"path": "抵押融资", "status": "insufficient_data", "basis": [], "missing_conditions": ["资产资料"], "source_sections": ["assets"]},
            {"path": "科技企业专项融资", "status": "insufficient_data", "basis": [], "missing_conditions": ["科技资格资料"], "source_sections": ["subject_profile"]},
            {"path": "保证/增信融资", "status": "conditional", "basis": ["已有征信资料"], "missing_conditions": ["核验保证责任"], "source_sections": ["enterprise_credit", "personal_credit"]},
        ],
        "action_plan": {"immediate": [{"action": "核验流水关联方", "basis": "分类尚未确认", "source_sections": ["enterprise_cashflow"]}],
                        "short_term": [{"action": "补充个人流水", "basis": "个人收入无法核验", "source_sections": ["personal_cashflow"]}],
                        "medium_term": []},
        "data_limitations": [{"material_type": item, "limitation": "当前资料中未找到可用结果", "impact": "对应分析受限", "required_data": "补充并核验资料"} for item in missing],
        "conclusion": {"overall": "可开展初步分析，正式路径筛选需补件。", "financing_direction": "优先核验信用融资条件。",
                       "prerequisites": ["明确融资需求", "补充缺失资料"], "one_sentence": "先补齐关键资料，再进入正式融资评估。"},
    }


def validate(payload, facts):
    return validate_analysis_result(ComprehensiveFinancingAnalysisResult.model_validate(payload), facts)


def test_analysis_uses_comprehensive_context_only(facts):
    context = build_safe_analysis_context(facts)
    assert set(context) == {"FACT", "DERIVED_METRIC", "STATUS", "CONFLICT", "MISSING_DATA"}
    assert "internal-123" not in json.dumps(context)
    assert context["FACT"]["enterprise_cashflow"]["operating_inflow"] == 800000
    assert "risk_assessment" not in context["MISSING_DATA"]
    assert "financing_plan" not in context["MISSING_DATA"]


def test_analysis_result_is_structured(facts, valid_payload):
    calls = []
    def fake(prompt, context):
        calls.append((prompt, context))
        return json.dumps(valid_payload, ensure_ascii=False)
    result = asyncio.run(analyze_comprehensive_financing_report(facts, fake))
    assert isinstance(result, ComprehensiveFinancingAnalysisResult)
    assert len(calls) == 1
    assert "FACT" in calls[0][1]


def test_analysis_does_not_return_markdown(facts, valid_payload):
    result = asyncio.run(analyze_comprehensive_financing_report(facts, lambda *_: json.dumps(valid_payload, ensure_ascii=False)))
    assert isinstance(result.model_dump(), dict)
    assert not validate(result.model_dump(), facts)


def test_missing_personal_cashflow_not_invented(facts, valid_payload):
    valid_payload["financing_constraints"][0]["fact"] = "没有个人收入"
    assert "将个人流水缺失写成无收入" in validate(valid_payload, facts)


def test_missing_assets_not_described_as_no_assets(facts, valid_payload):
    valid_payload["asset_and_enhancement_analysis"]["summary"] = "客户无资产。"
    assert "将资产资料缺失写成无资产" in validate(valid_payload, facts)


def test_missing_financing_requirement_not_invented(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "客户没有融资需求。"
    assert "将未确认需求写成无需求" in validate(valid_payload, facts)


def test_partial_cashflow_is_described_as_preliminary(facts, valid_payload):
    valid_payload["cashflow_analysis"]["summary"] = "流水收入已确认。"
    assert "部分流水未说明初步分类与关联方核验" in validate(valid_payload, facts)


def test_internal_transfer_not_treated_as_business_income(facts, valid_payload):
    valid_payload["cashflow_analysis"]["summary"] = "内部互转计入经营收入，按分类初步统计，关联方需核验。"
    assert "将内部互转当作经营收入" in validate(valid_payload, facts)


def test_internal_transfer_exclusion_is_not_false_positive(facts, valid_payload):
    valid_payload["cashflow_analysis"]["summary"] = "流水按分类初步统计，内部互转不应计入经营收入，关联方流入尚不能可靠量化，需核验。"
    assert "将内部互转当作经营收入" not in validate(valid_payload, facts)


def test_unknown_related_party_flow_not_guessed(facts, valid_payload):
    valid_payload["cashflow_analysis"]["summary"] = "流水按分类初步统计，关联方流入已确认。"
    assert "将未量化的关联方流入写成已确认" in validate(valid_payload, facts)


def test_monthly_financial_not_directly_compared_with_12m_cashflow(facts, valid_payload):
    valid_payload["financial_analysis"]["summary"] = "财务与流水收入可以直接比较。"
    assert "月度财务与全年流水被直接比较" in validate(valid_payload, facts)


def test_debt_asset_ratio_not_recalculated_by_llm(facts, valid_payload):
    valid_payload["financial_analysis"]["summary"] = "资产负债率约为85%，月度财务不能直接与全年流水比较。"
    assert any("Context 外比率" in error for error in validate(valid_payload, facts))


def test_enterprise_and_personal_debt_are_not_mixed(facts, valid_payload):
    assert build_safe_analysis_context(facts)["FACT"]["enterprise_credit"]["outstanding_loan_balance"] == 100
    assert build_safe_analysis_context(facts)["FACT"]["personal_credit"]["people"][0]["loan_balance"] == 40


def test_related_repayment_not_added_to_enterprise_debt(facts, valid_payload):
    valid_payload["enterprise_person_linkage"]["summary"] = "企业和个人负债可以加总。"
    assert "未提示企业债务与个人相关责任不可简单加总" in validate(valid_payload, facts)


def test_financing_paths_do_not_recommend_specific_bank(facts, valid_payload):
    valid_payload["financing_paths"][0]["basis"] = ["建议申请建设银行"]
    assert "包含具体银行推荐" in validate(valid_payload, facts)


def test_missing_asset_makes_mortgage_path_insufficient_data(facts, valid_payload):
    valid_payload["financing_paths"][1]["status"] = "potential"
    assert "缺资产时抵押融资须标记资料不足" in validate(valid_payload, facts)


def test_missing_tech_qualification_not_assumed(facts, valid_payload):
    valid_payload["financing_paths"][2]["status"] = "potential"
    assert "缺科技资格时专项融资须标记资料不足" in validate(valid_payload, facts)


def test_analysis_rejects_unknown_money_amount(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "建议申请1000万元。"
    assert any("Context 外金额" in error for error in validate(valid_payload, facts))


def test_analysis_rejects_unknown_money_without_yuan_suffix(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "建议申请1000万。"
    assert any("Context 外金额" in error for error in validate(valid_payload, facts))


def test_known_negative_profit_amount_is_allowed(facts, valid_payload):
    facts.financials["periods"][0]["net_profit"] = -100.0
    valid_payload["financial_analysis"]["summary"] = "净利润-100元；月度财务不能直接与全年流水比较。"
    assert not any("Context 外金额" in error for error in validate(valid_payload, facts))


def test_known_personal_credit_amount_is_allowed(facts, valid_payload):
    valid_payload["credit_analysis"]["summary"] = "个人贷款余额为40元。"
    assert not any("Context 外金额" in error for error in validate(valid_payload, facts))


def test_analysis_rejects_approval_probability(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "审批概率为80%。"
    assert "包含审批、额度、利率或抵押率承诺" in validate(valid_payload, facts)


def test_analysis_rejects_interest_rate_prediction(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "预计利率为3.5%。"
    assert validate(valid_payload, facts)


def test_analysis_rejects_credit_limit_prediction(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "预计额度为1000万元。"
    assert validate(valid_payload, facts)


@pytest.mark.parametrize("forbidden", ["raw_ocr", "evidence", "document_id", "<b>非法内容</b>", "# 报告"])
def test_analysis_has_no_raw_ocr_evidence_or_internal_ids(facts, valid_payload, forbidden):
    valid_payload["conclusion"]["overall"] = forbidden
    assert validate(valid_payload, facts)


def test_validation_failure_triggers_repair(facts, valid_payload):
    calls = []
    def fake(prompt, context):
        calls.append(context)
        if len(calls) == 1:
            return '{"invalid":true}'
        return json.dumps(valid_payload, ensure_ascii=False)
    asyncio.run(analyze_comprehensive_financing_report(facts, fake))
    assert len(calls) == 2
    assert json.loads(calls[1])["SAFE_CONTEXT"] == json.loads(calls[0])


def test_source_sections_allows_data_quality_or_prompt_never_outputs_it(valid_payload):
    valid_payload["business_analysis"]["source_sections"] = ["subject_profile", "data_quality"]
    parsed = ComprehensiveFinancingAnalysisResult.model_validate(valid_payload)
    assert "data_quality" in parsed.business_analysis.source_sections


def test_assets_missing_cannot_be_rendered_as_no_assets(facts, valid_payload):
    valid_payload["asset_and_enhancement_analysis"]["summary"] = "企业暂无资产。"
    assert "将资产资料缺失写成无资产" in validate(valid_payload, facts)


def test_personal_cashflow_missing_cannot_be_rendered_as_no_income(facts, valid_payload):
    valid_payload["financing_constraints"][0]["fact"] = "未见个人收入。"
    assert "将个人流水缺失写成无收入" in validate(valid_payload, facts)


def test_financing_requirement_missing_cannot_be_rendered_as_no_need(facts, valid_payload):
    valid_payload["conclusion"]["overall"] = "客户不需要融资。"
    assert "将未确认需求写成无需求" in validate(valid_payload, facts)


def test_repair_failure_uses_graceful_fallback(facts):
    calls = []
    def always_invalid(_prompt, payload):
        calls.append(payload)
        return '{"invalid":true}'
    result = asyncio.run(analyze_comprehensive_financing_report(facts, always_invalid))
    assert len(calls) == 2
    assert result.validation_fallback_used is True
    assert {item.path for item in result.financing_paths} == {"信用融资", "抵押融资", "保证/增信融资", "科技企业专项融资"}
    text = json.dumps(result.model_dump(), ensure_ascii=False)
    assert "无资产" not in text
    assert "无融资需求" not in text
    assert "个人无收入" not in text


def test_comprehensive_analysis_never_returns_500_for_repairable_output(facts, monkeypatch):
    async def fake_builder(_storage, _customer_id):
        return facts
    monkeypatch.setattr(assistant_analysis, "build_comprehensive_financing_report_context", fake_builder)
    class Storage:
        async def get_customer(self, customer_id):
            return {"customer_id": customer_id, "name": "上海意川建筑科技有限公司"}
    result = asyncio.run(assistant_analysis.generate_comprehensive_financing_analysis(
        Storage(), "生成客户综合融资分析报告", "customer-135", llm=lambda *_: '{"invalid":true}',
    ))
    assert result["data"]["analysisStatus"] == "completed"
    assert result["data"]["analysisValidationFallbackUsed"] is True


def test_real_context_not_replaced_by_file_metadata(facts, valid_payload, monkeypatch):
    facts.enterprise_cashflow.update({"total_inflow": 42499565.67, "operating_inflow": 19493700.0})
    facts.derived_metrics["monthly_average_operating_inflow"] = 1624475.0
    facts.financials["latest"] = {"debt_asset_ratio": 0.9953, "net_assets": 252084.7, "period_type": "monthly"}
    facts.financials["periods"][-1].update(facts.financials["latest"])
    captured = {}
    async def fake_builder(_storage, _customer_id):
        return facts
    def fake_llm(_prompt, context_json):
        captured.update(json.loads(context_json))
        return json.dumps(valid_payload, ensure_ascii=False)
    monkeypatch.setattr(assistant_analysis, "build_comprehensive_financing_report_context", fake_builder)
    class Storage:
        async def get_customer(self, customer_id):
            return {"customer_id": customer_id, "name": "上海意川建筑科技有限公司"}
        async def list_documents(self, _customer_id):
            raise AssertionError("综合分析不得读取文件列表")
    result = asyncio.run(assistant_analysis.generate_comprehensive_financing_analysis(
        Storage(), "生成客户综合融资分析报告", "customer-135", llm=fake_llm,
    ))
    assert result["data"]["analysisStatus"] == "completed"
    assert result["data"]["analysis"] == ComprehensiveFinancingAnalysisResult.model_validate(valid_payload).model_dump()
    assert captured["FACT"]["enterprise_cashflow"]["total_inflow"] == 42499565.67
    assert captured["FACT"]["financials"]["periods"][-1]["net_assets"] == 252084.7
    assert "documents" not in json.dumps(captured)


def test_analysis_input_debug_summary_contains_real_model_checks(facts):
    facts.enterprise_cashflow.update({"total_inflow": 42499565.67, "operating_inflow": 19493700.0})
    facts.derived_metrics["monthly_average_operating_inflow"] = 1624475.0
    facts.financials["latest"] = {"debt_asset_ratio": 0.9953, "net_assets": 252084.7}
    summary = build_analysis_input_debug_summary(facts)
    assert summary["sections"]["enterprise_credit"]["status"] == "available"
    assert summary["sections"]["personal_credit"]["status"] == "available"
    assert summary["sections"]["enterprise_cashflow"]["status"] == "partial"
    assert summary["sections"]["financials"]["status"] == "available"
    assert summary["sections"]["personal_cashflow"]["status"] == "missing"
    assert summary["sections"]["assets"]["status"] == "missing"
    assert summary["sections"]["financing_requirement"]["status"] == "missing"
    assert summary["checks"]["enterprise_cashflow.total_inflow"] == 42499565.67
    assert summary["checks"]["enterprise_cashflow.operating_inflow"] == 19493700.0
    assert summary["checks"]["derived_metrics.monthly_average_operating_inflow"] == 1624475.0
    assert summary["checks"]["financials.latest.debt_asset_ratio"] == 0.9953
    assert summary["checks"]["financials.latest.net_assets"] == 252084.7


def test_available_financials_not_reported_missing(facts, valid_payload):
    valid_payload["financial_analysis"]["summary"] = "财务报表尚未上传，不能分析；月度财务不能直接与全年流水比较。"
    assert "将可用或部分可用资料错误描述为未上传" in validate(valid_payload, facts)


def test_available_enterprise_credit_not_reported_missing(facts, valid_payload):
    valid_payload["credit_analysis"]["summary"] = "企业征信报告尚未上传。"
    assert "将可用或部分可用资料错误描述为未上传" in validate(valid_payload, facts)


def test_available_personal_credit_not_reported_missing(facts, valid_payload):
    valid_payload["credit_analysis"]["summary"] = "个人征信报告尚未上传。"
    assert "将可用或部分可用资料错误描述为未上传" in validate(valid_payload, facts)


def test_partial_cashflow_not_reported_missing(facts, valid_payload):
    valid_payload["cashflow_analysis"]["summary"] = "企业流水尚未上传。"
    errors = validate(valid_payload, facts)
    assert "将可用或部分可用资料错误描述为未上传" in errors


def test_contract_filename_not_used_to_infer_business_facts(facts, valid_payload):
    valid_payload["business_analysis"]["summary"] = "根据BIM咨询合同可判断企业具备项目承接能力。"
    assert "使用文件名或合同元数据推断经营事实" in validate(valid_payload, facts)
    assert "合同" not in json.dumps(build_safe_analysis_context(facts), ensure_ascii=False)


def test_analysis_result_is_not_markdown(facts, valid_payload):
    result = asyncio.run(analyze_comprehensive_financing_report(facts, lambda *_: json.dumps(valid_payload, ensure_ascii=False)))
    assert isinstance(result, ComprehensiveFinancingAnalysisResult)
    assert not isinstance(result, str)


def test_missing_assets_not_treated_as_no_assets(facts, valid_payload):
    valid_payload["asset_and_enhancement_analysis"]["summary"] = "该客户无资产。"
    assert "将资产资料缺失写成无资产" in validate(valid_payload, facts)


def test_comprehensive_request_uses_dedicated_service_path():
    assert assistant_analysis.is_comprehensive_financing_analysis_request(
        "根据上海意川建筑科技有限公司的全部资料生成客户综合融资分析报告"
    )
    assert assistant_analysis.COMPREHENSIVE_FINANCING_ANALYSIS_INTENT == "comprehensive_financing_analysis_report"


def test_high_debt_ratio_enters_financing_constraints(facts):
    result = build_conservative_analysis_fallback(facts)
    assert any("资产负债率" in item.fact or "资本结构" in item.title for item in result.financing_constraints)


def test_thin_net_assets_enters_financing_constraints(facts):
    facts.financials["latest"].update({"total_assets": 53789185.41, "net_assets": 252084.70, "unit": "元"})
    result = build_conservative_analysis_fallback(facts)
    assert any("净资产" in item.fact and "252,084.70元" in item.fact for item in result.financing_constraints)


def test_cashflow_structure_enters_financing_constraints(facts):
    result = build_conservative_analysis_fallback(facts)
    assert any("流水" in item.title and "内部互转" in item.fact for item in result.financing_constraints)


def test_system_missing_risk_report_not_treated_as_customer_financing_constraint(facts):
    result = build_conservative_analysis_fallback(facts)
    text = "\n".join(f"{item.title}{item.fact}" for item in result.financing_constraints)
    assert "风险评估" not in text and "风险报告" not in text


def test_system_analysis_missing_not_required_as_customer_data_limitation(facts, valid_payload):
    valid_payload["data_limitations"] = [
        item for item in valid_payload["data_limitations"]
        if item["material_type"] not in {"risk_assessment", "financing_plan"}
    ]
    errors = validate(valid_payload, facts)
    assert not any("未完整说明缺失资料" in error for error in errors)


def test_system_analysis_status_rejected_from_customer_data_limitations(facts, valid_payload):
    valid_payload["data_limitations"].append({
        "material_type": "risk_assessment", "limitation": "风险评估资料不足",
        "impact": "影响分析", "required_data": "生成风险评估",
    })
    assert "将系统分析状态写入客户资料缺口" in validate(valid_payload, facts)


def test_system_analysis_missing_not_described_as_customer_limitation_anywhere(facts, valid_payload):
    valid_payload["executive_summary"]["key_missing_information"].append("风险评估未生成")
    valid_payload["action_plan"]["short_term"].append({
        "action": "生成融资方案", "basis": "已有融资方案未生成", "source_sections": ["existing_financing_plan"]
    })
    assert "将系统分析状态写入客户报告" in validate(valid_payload, facts)


def test_missing_financing_plan_not_treated_as_customer_core_issue(facts):
    result = build_conservative_analysis_fallback(facts)
    text = "\n".join(f"{item.issue}{' '.join(item.facts)}" for item in result.core_issues)
    assert "融资方案" not in text and "方案匹配" not in text


def test_financing_strengths_not_empty_when_credit_and_cashflow_facts_support_them(facts):
    result = build_conservative_analysis_fallback(facts)
    assert result.financing_strengths
    assert any("流水" in item.title or "经营入账" in item.title for item in result.financing_strengths)


def test_financing_paths_are_generated_without_confirmed_financing_requirement(facts):
    result = build_conservative_analysis_fallback(facts)
    assert facts.financing_requirement["status"] == "missing"
    assert {item.path for item in result.financing_paths} >= {"信用融资", "抵押融资", "保证/增信融资", "科技企业专项融资"}


def test_mortgage_path_is_insufficient_data_when_assets_missing(facts):
    path = next(item for item in build_conservative_analysis_fallback(facts).financing_paths if item.path == "抵押融资")
    assert path.status == "insufficient_data"


def test_credit_path_can_be_conditional_without_product_recommendation(facts):
    path = next(item for item in build_conservative_analysis_fallback(facts).financing_paths if item.path == "信用融资")
    assert path.status == "conditional"
    assert not any("银行" in item for item in path.basis + path.missing_conditions)
