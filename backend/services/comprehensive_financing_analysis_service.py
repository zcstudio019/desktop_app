"""Narrative analysis of the frozen comprehensive facts model; no storage access."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError

from backend.services.comprehensive_financing_report_model import ComprehensiveFinancingReportModel


READINESS = Literal["ready_for_further_evaluation", "conditionally_ready", "needs_data_completion", "needs_issue_resolution"]
PATH_STATUS = Literal["potential", "conditional", "insufficient_data"]
SourceSection = Literal[
    "subject_profile", "financing_requirement", "enterprise_credit", "personal_credit",
    "enterprise_cashflow", "personal_cashflow", "financials", "assets", "risk_context",
    "existing_financing_plan", "derived_metrics", "conflicts", "data_scope", "data_quality", "source_dates",
]
PATH_NAMES = {"信用融资", "抵押融资", "保证/增信融资", "科技企业专项融资", "存量融资置换/结构优化"}
SOURCE_SECTIONS = {"subject_profile", "financing_requirement", "enterprise_credit", "personal_credit",
                   "enterprise_cashflow", "personal_cashflow", "financials", "assets", "risk_context",
                   "existing_financing_plan", "derived_metrics", "conflicts", "data_scope", "data_quality", "source_dates"}

logger = logging.getLogger(__name__)

MISSING_COPY = {
    "personal_cashflow": "当前缺少可用个人流水，无法核验关键自然人的稳定可采信个人收入。",
    "assets": "当前资料中未找到可用于本次分析的稳定结构化资产资料，无法判断抵押或其他增信能力。",
    "financing_requirement": "当前尚未确认明确融资金额、用途、期限及担保偏好。",
    "risk_assessment": "当前未找到可用于本次分析的风险评估结果。",
    "financing_plan": "当前未找到已保存的融资方案或方案匹配结果。",
}


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
    _validation_fallback_used: bool = PrivateAttr(default=False)

    @property
    def validation_fallback_used(self) -> bool:
        return self._validation_fallback_used


def _section(model: ComprehensiveFinancingReportModel, name: str, keys: tuple[str, ...]) -> dict[str, Any]:
    source = getattr(model, name)
    return {key: source.get(key) for key in keys if key in source}


def build_safe_analysis_context(model: ComprehensiveFinancingReportModel) -> dict[str, Any]:
    """Whitelisted facts only; excludes IDs, source documents, narrative and detail rows."""
    materials = [{"type": row.get("type"), "status": row.get("status"), "latest_date": row.get("latest_date")}
                 for row in model.data_scope.get("materials", [])]
    personal = [{key: person.get(key) for key in ("name", "roles", "loan_balance", "loan_balance_money", "loan_account_count",
                                                 "credit_card_limit", "credit_card_limit_money", "credit_card_used",
                                                 "credit_card_used_money", "credit_card_utilization", "overdue_summary", "query_summary",
                                                 "related_repayment_balance", "related_repayment_balance_money", "data_status", "source_report_date")}
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
            "data_quality": model.data_quality,
            "source_dates": model.source_dates,
        },
        "DERIVED_METRIC": {key: value for key, value in model.derived_metrics.items()
                           if key not in {"credit_card_utilization", "personal_query_counts"}},
        "STATUS": {row["type"]: row["status"] for row in materials},
        "CONFLICT": [{"type": item.get("type"), "message": item.get("message")} for item in model.conflicts],
        "MISSING_DATA": [row["type"] for row in materials if row["status"] == "missing"],
    }


def build_analysis_input_debug_summary(model: ComprehensiveFinancingReportModel) -> dict[str, Any]:
    """Non-sensitive audit summary proving which fact sections enter the LLM."""
    section_names = (
        "subject_profile", "enterprise_credit", "personal_credit", "enterprise_cashflow",
        "personal_cashflow", "financials", "assets", "financing_requirement", "risk_context",
        "existing_financing_plan",
    )
    sections = {name: {"included": True, "status": getattr(model, name).get("status")} for name in section_names}
    sections.update({
        "derived_metrics": {"included": True}, "conflicts": {"included": True, "count": len(model.conflicts)},
        "data_quality": {"included": True, "status": model.data_quality.get("status")},
        "source_dates": {"included": True},
    })
    latest = model.financials.get("latest") or {}
    return {
        "sections": sections,
        "checks": {
            "enterprise_cashflow.total_inflow": model.enterprise_cashflow.get("total_inflow"),
            "enterprise_cashflow.operating_inflow": model.enterprise_cashflow.get("operating_inflow"),
            "derived_metrics.monthly_average_operating_inflow": model.derived_metrics.get("monthly_average_operating_inflow"),
            "financials.latest.debt_asset_ratio": latest.get("debt_asset_ratio"),
            "financials.latest.net_assets": latest.get("net_assets"),
        },
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

SYSTEM_PROMPT += """
融资障碍和核心问题按以下顺序选择：已有财务/征信/流水事实形成的约束、跨资料结构问题、影响判断的关键资料缺口。risk_context 和 existing_financing_plan 缺失仅是系统流程状态，禁止作为客户融资障碍或核心问题。
当资产负债率、净资产、净利润、短期借款、存量征信融资、流水分类或法人相关还款责任已有数据时，必须优先分析这些事实。月度净利润为负只能表述为最新月度口径为负，不得推断持续亏损。
若企业及个人征信逾期概要明确为零，可形成有限信用基础；若存在连续流水、初步经营入账或历史融资记录，应形成有事实依据的有限融资优势，不得仅输出资料缺口。
即使融资需求缺失，也必须评估信用融资、抵押融资、保证/增信融资、科技企业专项融资四条路径。信用融资和保证/增信融资只能给出有条件判断；缺资产时抵押融资为资料不足；缺科技资质时科技企业专项融资为资料不足。
个人征信查询只能引用 query_summary.windows 中稳定查询矩阵，不得自行汇总或改写窗口。"""


def _money_registry(model: ComprehensiveFinancingReportModel) -> set[tuple[float, str]]:
    result: set[tuple[float, str]] = set()
    def add(section: dict[str, Any], keys: tuple[str, ...], unit: str | None) -> None:
        if unit not in {"元", "万元", "亿元"}:
            return
        for key in keys:
            value = section.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                result.add((round(float(value), 2), unit))
    def add_money(value: Any) -> None:
        if not isinstance(value, dict):
            return
        number, unit = value.get("value"), value.get("unit")
        if isinstance(number, (int, float)) and not isinstance(number, bool) and unit in {"元", "万元", "亿元"}:
            result.add((round(float(number), 2), unit))
    add(model.enterprise_credit, ("outstanding_loan_balance", "guarantee_balance"), model.enterprise_credit.get("unit"))
    add(model.enterprise_cashflow, ("total_inflow", "total_outflow", "net_inflow", "operating_inflow", "operating_outflow",
        "internal_transfer_inflow", "internal_transfer_outflow", "related_party_inflow", "related_party_outflow",
        "non_operating_inflow", "monthly_average_operating_inflow"), model.enterprise_cashflow.get("unit"))
    add(model.financing_requirement, ("amount",), model.financing_requirement.get("amount_unit"))
    for person in model.personal_credit.get("people", []):
        add(person, ("loan_balance", "credit_card_limit", "credit_card_used", "related_repayment_balance"),
            person.get("unit") or model.personal_credit.get("unit") or "元")
        for key in ("loan_balance_money", "credit_card_limit_money", "credit_card_used_money", "related_repayment_balance_money"):
            add_money(person.get(key))
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
    metadata_inferences = (
        "合同001", "BIM咨询合同", "材料采购合同", "临空项目", "青浦项目", "多项目并行",
        "项目承接能力", "技术议价空间", "保函需求", "应收账款融资", "采购垫资",
    )
    if any(term in text for term in metadata_inferences):
        errors.append("使用文件名或合同元数据推断经营事实")
    availability_checks = (
        (model.financials.get("status") in {"available", "partial", "confirmed"}, r"财务(?:报表|资料|数据)[^。；\n]{0,10}(?:尚未上传|未上传|缺失)"),
        (model.enterprise_cashflow.get("status") in {"available", "partial", "confirmed"}, r"(?:银行|企业)流水[^。；\n]{0,10}(?:尚未上传|未上传|缺失)"),
        (model.enterprise_credit.get("status") in {"available", "partial", "confirmed"}, r"企业征信(?:报告)?[^。；\n]{0,10}(?:尚未上传|未上传|缺失)"),
        (model.personal_credit.get("status") in {"available", "partial", "confirmed"}, r"个人征信(?:报告)?[^。；\n]{0,10}(?:尚未上传|未上传|缺失)"),
    )
    if any(available and re.search(pattern, text) for available, pattern in availability_checks):
        errors.append("将可用或部分可用资料错误描述为未上传")
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
    path_names = {item.path for item in result.financing_paths}
    for required_path in ("信用融资", "抵押融资", "保证/增信融资", "科技企业专项融资"):
        if required_path not in path_names:
            errors.append(f"缺少必要融资路径：{required_path}")
    if model.assets.get("status") == "missing" and not any(item.path == "抵押融资" for item in result.financing_paths):
        errors.append("缺少抵押融资资料不足路径")
    if not model.subject_profile.get("technology_enterprise_tags") and not any(item.path == "科技企业专项融资" for item in result.financing_paths):
        errors.append("缺少科技专项资料不足路径")
    customer_issue_text = "\n".join(
        [f"{item.title} {item.fact} {item.impact}" for item in result.financing_constraints]
        + [f"{item.issue} {' '.join(item.facts)} {item.financing_impact}" for item in result.core_issues]
    )
    if re.search(r"(?:风险评估|风险报告).{0,12}(?:缺失|未生成|未找到)|(?:融资方案|方案匹配).{0,12}(?:缺失|未生成|未找到)", customer_issue_text):
        errors.append("将系统流程状态当作客户融资障碍或核心问题")
    latest = model.financials.get("latest") or {}
    debt_ratio = latest.get("debt_asset_ratio")
    if isinstance(debt_ratio, (int, float)) and debt_ratio >= 0.8 and not re.search(r"资产负债率|资本结构|负债水平", customer_issue_text):
        errors.append("高资产负债率未进入融资障碍")
    net_assets, total_assets = latest.get("net_assets"), latest.get("total_assets")
    if (isinstance(net_assets, (int, float)) and isinstance(total_assets, (int, float)) and total_assets > 0
            and net_assets / total_assets <= 0.1 and not re.search(r"净资产|资本基础|安全边际", customer_issue_text)):
        errors.append("净资产基础较薄未进入融资障碍")
    flow = model.enterprise_cashflow
    if (flow.get("status") == "partial" or any(isinstance(flow.get(key), (int, float)) and flow.get(key) > 0
            for key in ("internal_transfer_inflow", "non_operating_inflow"))) and not re.search(r"流水|经营入账|内部互转|未识别", customer_issue_text):
        errors.append("企业流水结构未进入融资障碍")
    if model.enterprise_credit.get("status") in {"available", "confirmed"} and flow.get("status") in {"available", "partial", "confirmed"}:
        if not result.financing_strengths:
            errors.append("征信与流水事实可用但融资优势为空")
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
    if model.personal_cashflow.get("status") == "missing" and re.search(
        r"(?:个人|关键自然人)?(?:无|没有|未见)(?:稳定|可采信|明确)?(?:个人)?收入|个人没有流水|可采信个人收入为零", text
    ):
        errors.append("将个人流水缺失写成无收入")
    if model.assets.get("status") == "missing" and re.search(
        r"(?:企业|客户)?(?:无|没有|暂无|未持有)(?:可用|有效|明确|任何)?资产|无抵押物", text
    ):
        errors.append("将资产资料缺失写成无资产")
    if model.financing_requirement.get("status") == "missing" and re.search(
        r"(?:无|没有|暂无|不需要)(?:明确)?融资需求|客户不需要融资", text
    ):
        errors.append("将未确认需求写成无需求")
    if model.risk_context.get("status") == "missing" and re.search(r"无风险|风险为零", text):
        errors.append("将风险结果缺失写成无风险")
    if model.existing_financing_plan.get("status") == "missing" and re.search(r"无方案需求|不需要融资方案", text):
        errors.append("将融资方案缺失写成不需要方案")
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
        instructions.append(f"融资需求缺失只能表述为：{MISSING_COPY['financing_requirement']}")
    if "将资产资料缺失写成无资产" in joined:
        instructions.append(f"资产资料缺失只能表述为：{MISSING_COPY['assets']}")
    if "将个人流水缺失写成无收入" in joined:
        instructions.append(f"个人流水缺失只能表述为：{MISSING_COPY['personal_cashflow']}")
    if "将风险结果缺失写成无风险" in joined:
        instructions.append(f"风险结果缺失只能表述为：{MISSING_COPY['risk_assessment']}")
    if "将融资方案缺失写成不需要方案" in joined:
        instructions.append(f"融资方案缺失只能表述为：{MISSING_COPY['financing_plan']}")
    if "未提示企业债务与个人相关责任不可简单加总" in joined:
        instructions.append("enterprise_person_linkage.summary 必须逐字包含‘企业融资与法人相关还款责任存在债务关系重叠，不能简单加总。’")
    if "部分流水未说明" in joined or "关联方" in joined:
        instructions.append("cashflow_analysis.summary 必须说明按已保存分类初步统计，且关联方流入尚不能可靠量化、仍需核验。")
    if "资料限制" in joined:
        instructions.append("data_limitations.material_type 只能使用 JSON Schema 中的枚举值。")
    if "必要融资路径" in joined:
        instructions.append("financing_paths 必须包含信用融资、抵押融资、保证/增信融资、科技企业专项融资；不得推荐具体银行或产品。")
    if "系统流程状态" in joined:
        instructions.append("从 financing_constraints 和 core_issues 中移除风险评估未生成、融资方案未生成；它们只保留在 data_limitations。")
    if any(term in joined for term in ("高资产负债率", "净资产基础", "企业流水结构")):
        instructions.append("融资障碍优先写已有财务和流水事实：资本结构、净资产基础、最新期间盈利、存量融资、流水分类；每项引用 Context 原值。")
    if "融资优势为空" in joined:
        instructions.append("基于明确的零逾期概要、连续流水、初步经营入账或历史融资记录生成有限优势，禁止使用‘征信优秀’或审批承诺。")
    return "\n".join(instructions)


async def repair_comprehensive_financing_analysis_result(
    model: ComprehensiveFinancingReportModel,
    safe_context_text: str,
    original_output: str,
    validator_errors: list[str],
    llm: Callable[[str, str], str],
) -> str:
    """Repair structure/copy once without changing or reloading the fact context."""
    schema_text = json.dumps(ComprehensiveFinancingAnalysisResult.model_json_schema(), ensure_ascii=False)
    repair_payload = json.dumps(
        {
            "SAFE_CONTEXT": json.loads(safe_context_text),
            "INVALID_OUTPUT": original_output,
            "VALIDATOR_ERRORS": validator_errors[:12],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    repair_prompt = SYSTEM_PROMPT + """
你正在修复一次无效输出。只能修正 JSON 结构、枚举值和违规表述，不得新增 Context 中没有的事实、金额、比率或结论。
不得将 missing 改写为“无/没有/未持有/不需要”；不得将 partial 改写为“已确认”。
资产缺失不得写成无资产；个人流水缺失不得写成无收入；融资需求缺失不得写成无融资需求。
完整返回修复后的 JSON object，不要解释修复过程。
""" + "\n针对本次错误的精确修复要求：\n" + _repair_instructions(validator_errors)
    repair_prompt += "\n严格遵循这个 JSON Schema：\n" + schema_text
    return await asyncio.to_thread(llm, repair_prompt, repair_payload)


def _status(model: ComprehensiveFinancingReportModel, name: str) -> str:
    return str(getattr(model, name).get("status") or "missing")


def _amount_text(value: Any, unit: Any = None) -> str:
    if isinstance(value, dict):
        unit = value.get("unit") or unit
        value = value.get("value")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "资料不足"
    suffix = str(unit or "").strip()
    number = f"{float(value):,.2f}" if suffix == "元" else f"{float(value):,.2f}".rstrip("0").rstrip(".")
    return f"{number}{suffix}" if suffix else f"{number}（单位待核验）"


def _personal_money(model: ComprehensiveFinancingReportModel, key: str, numeric_key: str) -> Any:
    value = model.derived_metrics.get(key)
    if isinstance(value, dict):
        return value
    return {"value": model.derived_metrics.get(numeric_key), "unit": model.personal_credit.get("unit")}


def build_conservative_analysis_fallback(
    model: ComprehensiveFinancingReportModel,
) -> ComprehensiveFinancingAnalysisResult:
    """Build a deterministic facts-only result when both model outputs fail validation."""
    missing = {row.get("type") for row in model.data_scope.get("materials", []) if row.get("status") == "missing"}
    limitations: list[DataLimitation] = []
    limitation_copy = {
        "personal_cashflow": (MISSING_COPY["personal_cashflow"], "无法核验关键自然人稳定可采信个人收入", "补充可用个人流水及人工确认结果"),
        "assets": (MISSING_COPY["assets"], "无法判断抵押或其他资产增信能力", "补充已结构化的资产权属与估值依据"),
        "financing_requirement": (MISSING_COPY["financing_requirement"], "无法确定融资路径的具体适配条件", "确认融资金额、用途、期限及担保偏好"),
        "risk_assessment": (MISSING_COPY["risk_assessment"], "本次仅依据当前事实资料作保守分析", "补充有效风险评估结果"),
        "financing_plan": (MISSING_COPY["financing_plan"], "本次不引用既有产品匹配结论", "如需产品匹配，另行生成并保存融资方案"),
    }
    for material_type in ("personal_cashflow", "assets", "financing_requirement", "risk_assessment", "financing_plan"):
        if material_type in missing:
            limitation, impact, required = limitation_copy[material_type]
            limitations.append(DataLimitation(material_type=material_type, limitation=limitation, impact=impact, required_data=required))
    if _status(model, "enterprise_cashflow") == "partial":
        limitations.append(DataLimitation(
            material_type="enterprise_cashflow_classification",
            limitation="企业流水可用于初步分析，但关联方分类仍未完全核验。",
            impact="当前经营入账仅代表按已保存分类的初步统计。",
            required_data="核验关联方与内部账户分类。",
        ))
    latest = model.financials.get("latest") or {}
    flow_period = model.enterprise_cashflow.get("statement_period") or {}
    if latest.get("period_type") == "monthly" and (flow_period.get("months") or 0) >= 10:
        limitations.append(DataLimitation(
            material_type="financial_cashflow_period_mismatch",
            limitation="最新财务为月度口径，与企业流水覆盖周期不完全可比。",
            impact="当前不能直接进行同期间收入勾稽。",
            required_data="补充与流水覆盖期一致的财务数据后再核验。",
        ))

    missing_information = [item.limitation for item in limitations if item.material_type in {
        "personal_cashflow", "assets", "financing_requirement", "enterprise_cashflow_classification",
        "financial_cashflow_period_mismatch",
    }]
    constraints: list[Constraint] = []
    debt_ratio = latest.get("debt_asset_ratio")
    net_assets, total_assets = latest.get("net_assets"), latest.get("total_assets")
    financial_unit = latest.get("unit")
    if isinstance(debt_ratio, (int, float)) and debt_ratio >= 0.8:
        facts = [f"最新财务资产负债率为{debt_ratio * 100:.2f}%"]
        if isinstance(net_assets, (int, float)):
            facts.append(f"净资产为{_amount_text(net_assets, financial_unit)}")
        constraints.append(Constraint(
            title="资本结构与净资产基础偏弱",
            fact="；".join(facts) + "。",
            impact="当前财务口径下负债水平较高、净资产基础较薄，可能影响部分授信产品对资本结构和偿债安全边际的判断。",
            required_action="结合最新完整年度财务核验资本结构，并明确可执行的资本补充或负债优化安排。",
            source_sections=["financials", "derived_metrics"],
        ))
    net_profit = latest.get("net_profit")
    if isinstance(net_profit, (int, float)) and net_profit < 0:
        period_label = "最新月度财务口径" if latest.get("period_type") == "monthly" else "最新财务期间"
        constraints.append(Constraint(
            title="最新期间盈利表现承压",
            fact=f"{period_label}净利润为{_amount_text(net_profit, financial_unit)}。",
            impact="当前期间盈利表现为负，需要结合完整年度数据判断盈利持续性和偿债来源。",
            required_action="补充并核验最新完整年度利润数据及利润变化原因。",
            source_sections=["financials"],
        ))
    enterprise_balance = model.enterprise_credit.get("outstanding_loan_balance_money") or {
        "value": model.enterprise_credit.get("outstanding_loan_balance"), "unit": model.enterprise_credit.get("unit")}
    short_borrowing = latest.get("short_term_borrowings")
    if (isinstance(enterprise_balance, dict) and isinstance(enterprise_balance.get("value"), (int, float))) or isinstance(short_borrowing, (int, float)):
        facts = []
        if isinstance(enterprise_balance, dict) and isinstance(enterprise_balance.get("value"), (int, float)):
            facts.append(f"企业征信未结清融资余额为{_amount_text(enterprise_balance)}")
        if isinstance(short_borrowing, (int, float)):
            facts.append(f"最新财务短期借款为{_amount_text(short_borrowing, financial_unit)}")
        constraints.append(Constraint(
            title="存量融资与短期债务结构需评估",
            fact="；".join(facts) + "。",
            impact="存量融资规模及短期债务结构会影响新增融资空间与期限安排。",
            required_action="核验存量融资到期分布、还款安排和新增融资用途，不预测具体审批额度。",
            source_sections=["enterprise_credit", "financials"],
        ))
    flow = model.enterprise_cashflow
    if flow.get("status") == "partial" or any(isinstance(flow.get(key), (int, float)) and flow.get(key) > 0
            for key in ("internal_transfer_inflow", "non_operating_inflow")):
        flow_unit = flow.get("unit")
        constraints.append(Constraint(
            title="企业流水经营入账结构仍需核验",
            fact=(f"总流入{_amount_text(flow.get('total_inflow'), flow_unit)}；当前分类下初步经营入账"
                  f"{_amount_text(flow.get('operating_inflow'), flow_unit)}；内部互转流入"
                  f"{_amount_text(flow.get('internal_transfer_inflow'), flow_unit)}；其他非经营或未识别流入"
                  f"{_amount_text(flow.get('non_operating_inflow'), flow_unit)}。"),
            impact="银行流水规模不等于真实经营收入，当前仍有内部互转及非经营或未识别流入，新增融资评估前需进一步核实经营来源。",
            required_action="核验关联方、内部账户和未识别交易分类，形成可复核的经营入账口径。",
            source_sections=["enterprise_cashflow", "derived_metrics"],
        ))
    related_money = _personal_money(model, "total_related_repayment_balance_money", "total_related_repayment_balance")
    if isinstance(related_money, dict) and isinstance(related_money.get("value"), (int, float)) and related_money.get("value") > 0:
        constraints.append(Constraint(
            title="企业与法人信用责任联动较强",
            fact=f"法人相关还款责任余额为{_amount_text(related_money)}，并需结合企业融资关系核验。",
            impact="企业融资与法人个人信用责任存在关联，会影响新增保证或增信安排的可用空间。",
            required_action="逐笔核验相关还款责任对应的企业债务，避免与企业融资余额重复计算。",
            source_sections=["enterprise_credit", "personal_credit"],
        ))

    strengths: list[Strength] = []
    enterprise_overdue = (model.enterprise_credit.get("overdue_summary") or {}).get("count")
    personal_overdues = [person.get("overdue_summary") or {} for person in model.personal_credit.get("people", [])]
    stable_no_overdue = enterprise_overdue == 0 and bool(personal_overdues) and all(
        overdue.get("loan_overdue_account_count") == 0
        and overdue.get("credit_card_overdue_account_count") == 0
        and overdue.get("overdue_90d_account_count") == 0
        for overdue in personal_overdues
    )
    if stable_no_overdue:
        strengths.append(Strength(
            title="稳定征信事实未见明确逾期账户",
            fact="当前稳定企业及个人征信概要中，贷款、信用卡及90天以上逾期账户数均为0。",
            impact="为后续授信评估提供一定信用基础，但不代表审批结论。",
            source_sections=["enterprise_credit", "personal_credit"],
        ))
    if (flow_period.get("months") or 0) >= 10 and flow.get("account_count"):
        strengths.append(Strength(
            title="具备连续经营流水核验基础",
            fact=f"企业流水覆盖{flow_period.get('start')}至{flow_period.get('end')}，共{flow.get('account_count')}个账户。",
            impact="具备进一步核验经营真实性和现金流情况的数据基础。",
            source_sections=["enterprise_cashflow", "source_dates"],
        ))
    if isinstance(flow.get("operating_inflow"), (int, float)) and flow.get("operating_inflow") > 0:
        strengths.append(Strength(
            title="已识别一定规模的初步经营入账",
            fact=f"按当前已保存分类初步统计，经营入账为{_amount_text(flow.get('operating_inflow'), flow.get('unit'))}。",
            impact="企业存在可用于进一步核验经营活动的银行流水基础。",
            source_sections=["enterprise_cashflow", "derived_metrics"],
        ))
    institution_count = model.enterprise_credit.get("outstanding_loan_institution_count")
    if isinstance(institution_count, (int, float)) and institution_count > 0:
        strengths.append(Strength(
            title="存在持续金融机构融资记录",
            fact=f"企业征信显示已有{int(institution_count)}家金融机构的存量融资记录。",
            impact="可用于观察历史融资结构与还款表现。",
            source_sections=["enterprise_credit"],
        ))

    issues = [
        CoreIssue(
            issue=item.title,
            facts=[item.fact],
            financing_impact=item.impact,
            next_action=item.required_action,
            source_sections=item.source_sections,
        )
        for item in constraints[:5]
    ]
    if model.financing_requirement.get("status") == "missing" and len(issues) < 5:
        issues.append(CoreIssue(
            issue="融资需求尚未明确",
            facts=[MISSING_COPY["financing_requirement"]],
            financing_impact="缺少金额、用途和期限会限制融资路径的进一步筛选。",
            next_action="确认融资主体、金额、用途、期限、用款时间及担保偏好。",
            source_sections=["financing_requirement"],
        ))
    immediate = [Action(action=item.required_action, basis=item.fact, source_sections=item.source_sections) for item in constraints[:3]]
    if model.financing_requirement.get("status") == "missing":
        immediate.append(Action(action="确认融资金额、用途、期限及担保偏好。", basis=MISSING_COPY["financing_requirement"], source_sections=["financing_requirement"]))
    credit_basis = []
    if model.enterprise_credit.get("status") in {"available", "confirmed"}:
        credit_basis.append("企业存在已保存的征信及历史融资记录")
    if (flow_period.get("months") or 0) >= 10:
        credit_basis.append("企业存在连续经营流水核验基础")
    credit_missing = ["明确融资需求", "进一步核验经营流水分类"]
    if isinstance(debt_ratio, (int, float)) and debt_ratio >= 0.8:
        credit_missing.append("核验较高资产负债率和净资产基础对授信条件的影响")
    financing_paths = [
        FinancingPath(path="信用融资", status="conditional", basis=credit_basis,
                      missing_conditions=credit_missing, source_sections=["enterprise_credit", "enterprise_cashflow", "financials", "financing_requirement"]),
        FinancingPath(path="抵押融资", status="insufficient_data" if model.assets.get("status") == "missing" else "conditional",
                      basis=[] if model.assets.get("status") == "missing" else ["已有结构化资产资料可供进一步核验"],
                      missing_conditions=["当前缺少稳定结构化资产资料，暂无法评估抵押融资条件"] if model.assets.get("status") == "missing" else ["核验权属、估值及可抵押状态"],
                      source_sections=["assets"]),
        FinancingPath(path="保证/增信融资", status="conditional",
                      basis=["法人相关还款责任及保证关系已有征信事实可供核验"],
                      missing_conditions=["评估法人已有责任规模及可用增信空间"],
                      source_sections=["personal_credit", "enterprise_credit"]),
        FinancingPath(path="科技企业专项融资", status="conditional" if model.subject_profile.get("technology_enterprise_tags") else "insufficient_data",
                      basis=list(model.subject_profile.get("technology_enterprise_tags") or []),
                      missing_conditions=[] if model.subject_profile.get("technology_enterprise_tags") else ["当前缺少可核验科技企业资质或标签资料"],
                      source_sections=["subject_profile"]),
    ]
    result = ComprehensiveFinancingAnalysisResult(
        executive_summary=ExecutiveSummary(
            overall_observation="企业具备征信、连续流水和财务分析基础；当前应优先核验资本结构、存量融资、流水分类及企业与法人信用责任联动。",
            current_financing_readiness="needs_issue_resolution" if constraints else "ready_for_further_evaluation",
            main_strengths=[item.title for item in strengths],
            main_constraints=[item.fact for item in constraints],
            key_missing_information=missing_information,
        ),
        business_analysis=AnalysisSection(
            summary="企业主体资料与已保存经营资料仅用于基础事实核对，仍需结合资料状态继续核验。",
            source_sections=["subject_profile", "data_quality"],
        ),
        cashflow_analysis=AnalysisSection(
            summary=("企业流水可用于初步分析；经营入账按当前已保存分类初步统计，关联方流入尚不能可靠量化，仍需核验。"
                     if _status(model, "enterprise_cashflow") == "partial" else "企业流水按当前结构化资料状态进行事实核对。"),
            source_sections=["enterprise_cashflow", "derived_metrics"],
        ),
        financial_analysis=AnalysisSection(
            summary=("最新财务为月度口径，与企业流水覆盖周期不同，当前不能直接进行同期间收入勾稽。"
                     if latest.get("period_type") == "monthly" and (flow_period.get("months") or 0) >= 10
                     else "财务分析仅引用已保存期间与程序计算指标。"),
            source_sections=["financials", "derived_metrics", "source_dates"],
        ),
        credit_analysis=AnalysisSection(
            summary="企业征信与个人征信均按稳定征信事实口径分别核对；逾期和查询结论使用已保存的标准化概要与查询窗口，不合并计算企业和个人债务。",
            source_sections=["enterprise_credit", "personal_credit"],
        ),
        enterprise_person_linkage=AnalysisSection(
            summary="企业融资与法人相关还款责任存在债务关系重叠，不能简单加总。",
            source_sections=["enterprise_credit", "personal_credit"],
        ),
        asset_and_enhancement_analysis=AnalysisSection(
            summary=MISSING_COPY["assets"] if _status(model, "assets") == "missing" else "资产与增信条件仅按当前结构化资料核对。",
            source_sections=["assets"],
        ),
        financing_strengths=strengths,
        financing_constraints=constraints,
        core_issues=issues,
        financing_paths=financing_paths,
        action_plan=ActionPlan(
            immediate=immediate,
            short_term=[Action(action="补充最新完整年度财务、个人流水及资产资料。", basis="用于核验盈利持续性、个人收入与可用增信条件。", source_sections=["financials", "personal_cashflow", "assets"])],
            medium_term=[Action(action="结合已核验资料优化存量融资期限与主体责任结构。", basis="存量融资、短期借款和法人相关责任需协同评估。", source_sections=["enterprise_credit", "personal_credit", "financials"])],
        ),
        data_limitations=limitations,
        conclusion=Conclusion(
            overall="企业具备进一步融资评估的数据基础，但资本结构、最新期间盈利、存量融资、流水分类及法人责任联动需要优先核验。",
            financing_direction="可有条件评估信用融资和保证/增信融资；抵押融资与科技专项融资需先补齐相应资格资料。",
            prerequisites=[item.required_action for item in constraints[:4]] + (["确认明确融资需求。"] if model.financing_requirement.get("status") == "missing" else []),
            one_sentence="企业具备进一步评估基础，但需先核验资本结构、存量融资、经营流水及法人责任联动并明确融资需求。",
        ),
    )
    result._validation_fallback_used = True
    return result


def _parse_and_validate(
    raw: str,
    model: ComprehensiveFinancingReportModel,
) -> tuple[ComprehensiveFinancingAnalysisResult | None, list[str]]:
    try:
        result = ComprehensiveFinancingAnalysisResult.model_validate(_parse_json(raw))
    except (ValueError, ValidationError, json.JSONDecodeError) as exc:
        return None, [str(exc)[:1000]]
    errors = validate_analysis_result(result, model)
    return (result if not errors else None), errors


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
    raw = ""
    errors: list[str] = []
    try:
        raw = await asyncio.to_thread(llm, prompt, context_text)
        result, errors = _parse_and_validate(raw, model)
        if result is not None:
            return result
        repaired_raw = await repair_comprehensive_financing_analysis_result(
            model, context_text, raw, errors, llm
        )
        repaired_result, repair_errors = _parse_and_validate(repaired_raw, model)
        if repaired_result is not None:
            return repaired_result
        errors = repair_errors
    except Exception as exc:
        errors = [f"LLM 分析或修复异常：{type(exc).__name__}: {str(exc)[:500]}"]
    logger.warning(
        "analysis_validation_fallback_used=True errors=%s",
        "；".join(errors[:6]),
    )
    return build_conservative_analysis_fallback(model)
