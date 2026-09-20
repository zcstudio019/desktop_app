"""Narrative analysis of the frozen comprehensive facts model; no storage access."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from backend.services.comprehensive_financing_report_model import ComprehensiveFinancingReportModel


READINESS = Literal["ready_for_further_evaluation", "conditionally_ready", "needs_data_completion", "needs_issue_resolution"]
PATH_STATUS = Literal["potential", "conditional", "insufficient_data"]
SourceSection = Literal[
    "subject_profile", "financing_requirement", "enterprise_credit", "personal_credit",
    "enterprise_cashflow", "personal_cashflow", "financials", "assets", "risk_context",
    "existing_financing_plan", "derived_metrics", "conflicts", "data_scope", "source_dates",
]
PATH_NAMES = {"信用融资", "抵押融资", "保证/增信融资", "科技企业专项融资", "存量融资置换/结构优化"}
SOURCE_SECTIONS = {"subject_profile", "financing_requirement", "enterprise_credit", "personal_credit",
                   "enterprise_cashflow", "personal_cashflow", "financials", "assets", "risk_context",
                   "existing_financing_plan", "derived_metrics", "conflicts", "data_scope", "source_dates"}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExecutiveSummary(StrictModel):
    overall_observation: str
    current_financing_readiness: READINESS
    main_strengths: list[str]
    main_constraints: list[str]
    key_missing_information: list[str]


class AnalysisSection(StrictModel):
    summary: str
    source_sections: list[SourceSection] = Field(min_length=1)


class Strength(StrictModel):
    title: str
    fact: str
    impact: str
    source_sections: list[SourceSection] = Field(min_length=1)


class Constraint(StrictModel):
    title: str
    fact: str
    impact: str
    required_action: str
    source_sections: list[SourceSection] = Field(min_length=1)


class CoreIssue(StrictModel):
    issue: str
    facts: list[str] = Field(min_length=1)
    financing_impact: str
    next_action: str
    source_sections: list[SourceSection] = Field(min_length=1)


class FinancingPath(StrictModel):
    path: str
    status: PATH_STATUS
    basis: list[str]
    missing_conditions: list[str]
    source_sections: list[SourceSection] = Field(min_length=1)


class Action(StrictModel):
    action: str
    basis: str
    source_sections: list[SourceSection] = Field(min_length=1)


class ActionPlan(StrictModel):
    immediate: list[Action]
    short_term: list[Action]
    medium_term: list[Action]


class DataLimitation(StrictModel):
    material_type: Literal[
        "enterprise_kyc", "enterprise_credit", "personal_credit", "enterprise_cashflow",
        "personal_cashflow", "financial_statements", "assets", "financing_requirement",
        "risk_assessment", "financing_plan", "financial_cashflow_period_mismatch",
        "enterprise_cashflow_classification", "source_date_comparability",
    ]
    limitation: str
    impact: str
    required_data: str


class Conclusion(StrictModel):
    overall: str
    financing_direction: str
    prerequisites: list[str]
    one_sentence: str


class ComprehensiveFinancingAnalysisResult(StrictModel):
    executive_summary: ExecutiveSummary
    business_analysis: AnalysisSection
    cashflow_analysis: AnalysisSection
    financial_analysis: AnalysisSection
    credit_analysis: AnalysisSection
    enterprise_person_linkage: AnalysisSection
    asset_and_enhancement_analysis: AnalysisSection
    financing_strengths: list[Strength]
    financing_constraints: list[Constraint]
    core_issues: list[CoreIssue] = Field(max_length=5)
    financing_paths: list[FinancingPath]
    action_plan: ActionPlan
    data_limitations: list[DataLimitation]
    conclusion: Conclusion


def _section(model: ComprehensiveFinancingReportModel, name: str, keys: tuple[str, ...]) -> dict[str, Any]:
    source = getattr(model, name)
    return {key: source.get(key) for key in keys if key in source}


def build_safe_analysis_context(model: ComprehensiveFinancingReportModel) -> dict[str, Any]:
    """Whitelisted facts only; excludes IDs, source documents, narrative and detail rows."""
    materials = [{"type": row.get("type"), "status": row.get("status"), "latest_date": row.get("latest_date")}
                 for row in model.data_scope.get("materials", [])]
    personal = [{key: person.get(key) for key in ("roles", "loan_balance", "loan_account_count", "credit_card_limit",
                                                 "credit_card_used", "credit_card_utilization", "overdue_summary", "query_summary",
                                                 "related_repayment_balance", "data_status", "source_report_date")}
                for person in model.personal_credit.get("people", [])]
    financial = [{key: period.get(key) for key in ("period", "period_type", "unit", "revenue", "operating_cost",
                                                  "gross_profit", "net_profit", "total_assets", "total_liabilities",
                                                  "net_assets", "accounts_receivable", "inventory", "short_term_borrowings",
                                                  "long_term_borrowings", "operating_cashflow", "debt_asset_ratio")}
                 for period in model.financials.get("periods", [])]
    assets = {kind: [{"type": kind, "market_value": asset.get("market_value"), "value_basis": asset.get("value_basis")}
                     for asset in model.assets.get(kind, [])]
              for kind in ("property", "vehicle", "equipment", "intellectual_property", "equity", "deposit", "other_collateral")}
    return {
        "FACT": {
            "subject_profile": _section(model, "subject_profile", ("enterprise_name", "established_date", "enterprise_type",
                "business_scope", "industry", "technology_enterprise_tags")),
            "financing_requirement": _section(model, "financing_requirement", ("financing_subject", "amount", "amount_unit",
                "currency", "purpose", "term", "expected_use_date", "guarantee_preference")),
            "enterprise_credit": _section(model, "enterprise_credit", ("outstanding_loan_balance", "unit",
                "outstanding_loan_institution_count", "overdue_summary", "nonperforming_summary", "guarantee_balance",
                "query_summary", "source_report_date")),
            "personal_credit": {"people": personal},
            "enterprise_cashflow": _section(model, "enterprise_cashflow", ("statement_period", "account_count", "unit",
                "total_inflow", "total_outflow", "net_inflow", "operating_inflow", "operating_outflow",
                "internal_transfer_inflow", "internal_transfer_outflow", "related_party_inflow", "related_party_outflow",
                "non_operating_inflow", "monthly_average_operating_inflow", "concentration_metrics", "data_quality")),
            "personal_cashflow": _section(model, "personal_cashflow", ("statement_period", "account_count",
                "confirmed_salary_income", "manually_confirmed_salary_income", "suspected_salary_income",
                "usable_salary_income", "other_income", "fixed_expense", "debt_repayment")),
            "financials": {"periods": financial, "latest_period": model.financials.get("latest_period"),
                           "trends": model.financials.get("trends"), "available_period_count": model.financials.get("available_period_count")},
            "assets": assets,
            "risk_context": _section(model, "risk_context", ("risk_level", "total_score", "generated_at", "stale")),
            "existing_financing_plan": _section(model, "existing_financing_plan", ("has_saved_result",)),
            "source_dates": model.source_dates,
        },
        "DERIVED_METRIC": {key: value for key, value in model.derived_metrics.items()
                           if key not in {"credit_card_utilization", "personal_query_counts"}},
        "STATUS": {row["type"]: row["status"] for row in materials},
        "CONFLICT": [{"type": item.get("type"), "message": item.get("message")} for item in model.conflicts],
        "MISSING_DATA": [row["type"] for row in materials if row["status"] == "missing"],
    }


SYSTEM_PROMPT = """你是融资顾问的综合分析助手。只根据用户提供的 JSON 中 FACT、DERIVED_METRIC、STATUS、CONFLICT、MISSING_DATA 写分析。
FACT 是已保存事实；DERIVED_METRIC 是程序计算值，不得重算；STATUS=partial 不能写成已确认；MISSING_DATA 不能写成客户不存在或金额为零。
只返回符合所给结构的 JSON 对象。每条判断给出 source_sections，字段名只能来自所给资料类别。禁止 Markdown、HTML、OCR/evidence、内部 ID。
不要猜金额、单位、日期、融资需求、资产、个人收入、关联企业、科技资格、审批概率、额度、利率或抵押率。
如引用金额或比率，只能逐字使用输入已有数值和单位；不要自行换算或取整。
总流入不是经营收入；内部互转不得计入经营收入；关联方未量化时明确说尚不能可靠量化。部分流水只能说按已保存分类初步统计。
月度财务与跨 12 个月流水不可直接勾稽。企业贷款、个人贷款、相关还款责任分开分析，不能简单加总。
缺少个人流水时说明无法核验稳定可采信个人收入；缺少资产时说未获取稳定结构化资产资料，不说无资产；缺少融资需求时说明金额、用途、期限及担保偏好未确认。
融资路径只用信用融资、抵押融资、保证/增信融资、科技企业专项融资、存量融资置换/结构优化，禁止具体银行或产品。缺资产时抵押融资 status=insufficient_data；缺科技资格时科技企业专项融资 status=insufficient_data。
融资优势、障碍和核心矛盾必须有事实依据；核心矛盾最多 5 项，按重要性排序。行动计划分 immediate、short_term、medium_term。"""


def _money_registry(model: ComprehensiveFinancingReportModel) -> set[tuple[float, str]]:
    result: set[tuple[float, str]] = set()
    def add(section: dict[str, Any], keys: tuple[str, ...], unit: str | None) -> None:
        if unit not in {"元", "万元", "亿元"}:
            return
        for key in keys:
            value = section.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                result.add((round(float(value), 2), unit))
    add(model.enterprise_credit, ("outstanding_loan_balance", "guarantee_balance"), model.enterprise_credit.get("unit"))
    add(model.enterprise_cashflow, ("total_inflow", "total_outflow", "net_inflow", "operating_inflow", "operating_outflow",
        "internal_transfer_inflow", "internal_transfer_outflow", "related_party_inflow", "related_party_outflow",
        "non_operating_inflow", "monthly_average_operating_inflow"), model.enterprise_cashflow.get("unit"))
    add(model.financing_requirement, ("amount",), model.financing_requirement.get("amount_unit"))
    for person in model.personal_credit.get("people", []):
        add(person, ("loan_balance", "credit_card_limit", "credit_card_used", "related_repayment_balance"),
            person.get("unit") or model.personal_credit.get("unit") or "元")
    for period in model.financials.get("periods", []):
        add(period, ("revenue", "operating_cost", "gross_profit", "net_profit", "total_assets", "total_liabilities",
            "net_assets", "accounts_receivable", "inventory", "short_term_borrowings", "long_term_borrowings", "operating_cashflow"), period.get("unit"))
    return result


def _all_text(result: ComprehensiveFinancingAnalysisResult) -> str:
    def walk(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            return [part for nested in value.values() for part in walk(nested)]
        if isinstance(value, list):
            return [part for nested in value for part in walk(nested)]
        return []
    return "\n".join(walk(result.model_dump()))


def validate_analysis_result(result: ComprehensiveFinancingAnalysisResult, model: ComprehensiveFinancingReportModel) -> list[str]:
    errors: list[str] = []
    payload = result.model_dump()
    text = _all_text(result)
    if re.search(r"(?m)^\s*(?:#{1,6}\s|\|)|<[^>]+>|(?:raw_ocr|ocr_text|raw_text|evidence|document_id|extraction_id|customer_id|source_id|report_id)", text, re.I):
        errors.append("包含 Markdown、HTML、原始证据或内部字段")
    if re.search(r"审批概率|通过率|通过可能性|获批可能性|大概率|一定能贷|保证获批|授信额度承诺|预计额度|预估额度|预计利率|执行利率|抵押率", text):
        errors.append("包含审批、额度、利率或抵押率承诺")
    if not model.subject_profile.get("technology_enterprise_tags") and re.search(r"(?:已是|属于|具备|拥有|认定为)[^。；\n]{0,12}(?:高新技术企业|科技型中小企业|专精特新)", text):
        errors.append("擅自认定科技企业资格")
    if re.search(r"(?:利率|年化|APR)[^。；\n]{0,20}\d+(?:\.\d+)?\s*%|\d+(?:\.\d+)?\s*%[^。；\n]{0,20}(?:利率|年化)", text, re.I):
        errors.append("包含预测利率")
    if re.search(r"(?:中国|建设|工商|农业|交通|招商|浦发|兴业|民生|中信|光大|平安|广发|华夏|邮储|上海|北京)银行", text):
        errors.append("包含具体银行推荐")
    for clause in re.findall(r"内部互转[^。；\n]{0,40}", text):
        if re.search(r"(?:计入|算作|作为)[^。；\n]{0,12}经营(?:收入|入账)", clause) and not re.search(r"不(?:得|应|能|可)?|排除|剔除|未计入|不能", clause):
            errors.append("将内部互转当作经营收入")
            break
    for match in re.finditer(r"(?<![\d.])([-−]?\d[\d,]*(?:\.\d+)?)\s*(亿元|万元|元|亿|万)(?![\d])", text):
        amount = round(float(match.group(1).replace(",", "").replace("−", "-")), 2)
        unit = {"亿": "亿元", "万": "万元"}.get(match.group(2), match.group(2))
        if (amount, unit) not in _money_registry(model):
            errors.append(f"出现 Context 外金额：{match.group(0)}")
    allowed_percentages = {round(float(value) * 100, 2) for value in model.derived_metrics.values()
                           if isinstance(value, (int, float)) and not isinstance(value, bool) and -1 <= value <= 1}
    for value in (model.financials.get("trends") or {}).values():
        if isinstance(value, (int, float)) and -1 <= value <= 1:
            allowed_percentages.add(round(float(value) * 100, 2))
    for match in re.finditer(r"(?<![\d.])(\d+(?:\.\d+)?)\s*%", text):
        value = float(match.group(1))
        if not any(abs(value - allowed) <= 0.05 for allowed in allowed_percentages):
            errors.append(f"出现 Context 外比率：{match.group(0)}")
    for item in result.financing_paths:
        if item.path not in PATH_NAMES:
            errors.append("出现未允许的融资路径")
        if item.path == "抵押融资" and model.assets.get("status") == "missing" and item.status != "insufficient_data":
            errors.append("缺资产时抵押融资须标记资料不足")
        if item.path == "科技企业专项融资" and not model.subject_profile.get("technology_enterprise_tags") and item.status != "insufficient_data":
            errors.append("缺科技资格时专项融资须标记资料不足")
    if model.assets.get("status") == "missing" and not any(item.path == "抵押融资" for item in result.financing_paths):
        errors.append("缺少抵押融资资料不足路径")
    if not model.subject_profile.get("technology_enterprise_tags") and not any(item.path == "科技企业专项融资" for item in result.financing_paths):
        errors.append("缺少科技专项资料不足路径")
    for item in result.financing_strengths + result.financing_constraints + result.core_issues + result.financing_paths:
        if any(section not in SOURCE_SECTIONS for section in item.source_sections):
            errors.append("事实引用包含未知 section")
    for item in (result.business_analysis, result.cashflow_analysis, result.financial_analysis,
                 result.credit_analysis, result.enterprise_person_linkage, result.asset_and_enhancement_analysis):
        if any(section not in SOURCE_SECTIONS for section in item.source_sections):
            errors.append("分析引用包含未知 section")
    for item in result.action_plan.immediate + result.action_plan.short_term + result.action_plan.medium_term:
        if any(section not in SOURCE_SECTIONS for section in item.source_sections):
            errors.append("行动计划引用包含未知 section")
    required_limits = {row["type"] for row in model.data_scope.get("materials", []) if row.get("status") == "missing"}
    found_limits = {item.material_type for item in result.data_limitations}
    known_limits = {row["type"] for row in model.data_scope.get("materials", [])} | {
        "financial_cashflow_period_mismatch", "enterprise_cashflow_classification", "source_date_comparability",
    }
    if not found_limits.issubset(known_limits):
        errors.append("资料限制包含未知资料类型")
    if not required_limits.issubset(found_limits):
        errors.append("未完整说明缺失资料")
    if model.enterprise_cashflow.get("status") == "partial":
        if "初步" not in result.cashflow_analysis.summary or "关联方" not in text or "核验" not in text:
            errors.append("部分流水未说明初步分类与关联方核验")
    if model.enterprise_cashflow.get("related_party_inflow") is None and model.enterprise_cashflow.get("status") != "missing":
        if "关联方" not in result.cashflow_analysis.summary or not any(word in result.cashflow_analysis.summary for word in ("无法", "尚不能", "未能", "待核验", "需核验")):
            errors.append("未说明关联方流入无法可靠量化")
        if re.search(r"关联方流入[^。；\n]{0,15}(?:已确认|已可靠量化|为零|为0)", text):
            errors.append("将未量化的关联方流入写成已确认")
    latest = model.financials.get("latest") or {}
    flow_period = model.enterprise_cashflow.get("statement_period") or {}
    if latest.get("period_type") == "monthly" and (flow_period.get("months") or 0) >= 10:
        if not any(word in result.financial_analysis.summary for word in ("不能直接", "不可直接")):
            errors.append("月度财务与全年流水被直接比较")
        if "financial_cashflow_period_mismatch" not in found_limits:
            errors.append("缺少财务与流水期间不可比限制")
    if model.personal_cashflow.get("status") == "missing" and re.search(r"无.{0,4}收入|没有.{0,4}收入|可采信个人收入为零", text):
        errors.append("将个人流水缺失写成无收入")
    if model.assets.get("status") == "missing" and re.search(r"无.{0,4}资产|没有.{0,4}资产|无抵押物", text):
        errors.append("将资产资料缺失写成无资产")
    if model.financing_requirement.get("status") == "missing" and re.search(r"无.{0,4}融资需求|没有.{0,4}融资需求", text):
        errors.append("将未确认需求写成无需求")
    if model.enterprise_credit.get("status") != "missing" and model.personal_credit.get("status") != "missing":
        if "不能简单加总" not in result.enterprise_person_linkage.summary:
            errors.append("未提示企业债务与个人相关责任不可简单加总")
    if len(result.core_issues) > 5:
        errors.append("核心问题超过五项")
    return list(dict.fromkeys(errors))


def _parse_json(raw: str) -> dict[str, Any]:
    raw = raw.strip()
    if raw.startswith("```json"):
        raw = raw[7:]
    if raw.startswith("```"):
        raw = raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    parsed = json.loads(raw.strip())
    if not isinstance(parsed, dict):
        raise ValueError("LLM 返回值不是 JSON object")
    return parsed


def _repair_instructions(errors: list[str]) -> str:
    instructions: list[str] = []
    joined = "；".join(errors)
    if "source_sections" in joined or "引用包含未知 section" in joined:
        instructions.append("所有 source_sections 只能使用 JSON Schema 枚举中的原始 section 名，不得使用点路径、中文名或自造名称。")
    if "将未确认需求写成无需求" in joined:
        instructions.append("将所有‘无融资需求/没有融资需求’改成‘当前尚未确认融资金额、用途、期限及担保偏好’。")
    if "未提示企业债务与个人相关责任不可简单加总" in joined:
        instructions.append("enterprise_person_linkage.summary 必须逐字包含‘企业融资与法人相关还款责任存在债务关系重叠，不能简单加总。’")
    if "部分流水未说明" in joined or "关联方" in joined:
        instructions.append("cashflow_analysis.summary 必须说明按已保存分类初步统计，且关联方流入尚不能可靠量化、仍需核验。")
    if "资料限制" in joined:
        instructions.append("data_limitations.material_type 只能使用 JSON Schema 中的枚举值。")
    return "\n".join(instructions)


async def analyze_comprehensive_financing_report(
    model: ComprehensiveFinancingReportModel,
    llm: Callable[[str, str], str] | None = None,
) -> ComprehensiveFinancingAnalysisResult:
    """One analysis request, with at most one repair against the same safe context."""
    context = build_safe_analysis_context(model)
    context_text = json.dumps(context, ensure_ascii=False, separators=(",", ":"))
    schema_text = json.dumps(ComprehensiveFinancingAnalysisResult.model_json_schema(), ensure_ascii=False)
    prompt = SYSTEM_PROMPT + "\n严格遵循这个 JSON Schema：\n" + schema_text
    if llm is None:
        from services.ai_service import AIService
        ai = AIService()
        def call(system: str, payload: str) -> str:
            return ai.extract(system, payload, "deepseek-chat", 120, 8192)
        llm = call
    previous_errors: list[str] = []
    for attempt in range(2):
        current_prompt = prompt if attempt == 0 else (
            prompt + "\n上次输出无效，请只修复下列问题，所有事实仍只来自同一 Context："
            + "；".join(previous_errors[:12]) + "\n精确修复要求：\n" + _repair_instructions(previous_errors)
        )
        raw = await asyncio.to_thread(llm, current_prompt, context_text)
        try:
            result = ComprehensiveFinancingAnalysisResult.model_validate(_parse_json(raw))
            errors = validate_analysis_result(result, model)
            if not errors:
                return result
            previous_errors = errors
        except (ValueError, ValidationError, json.JSONDecodeError) as exc:
            previous_errors = [str(exc)[:500]]
    raise ValueError("综合融资分析结果未通过校验：" + "；".join(previous_errors[:6]))
