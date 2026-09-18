"""Deterministic rules for credit_one_page_report_v1.

Only rules with an objective, source-backed decision are evaluated.  Metrics that
need a bank/product-specific threshold are deliberately marked ``待评估``.
"""

from __future__ import annotations

from typing import Any

RULE_VERSION = "credit_report_rules_v1"


def _present(value: Any) -> bool:
    if isinstance(value, dict) and "value" in value:
        return value.get("value") not in (None, "")
    return value not in (None, "", [], {})


def _number(value: Any) -> float | None:
    if isinstance(value, dict) and "value" in value:
        value = value.get("value")
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("，", "").replace("%", "")
    for suffix in ("人民币元", "万元", "元", "次", "笔", "户"):
        text = text.replace(suffix, "")
    try:
        return float(text)
    except ValueError:
        return None


def _item(name: str, status: str, current: str, basis: str, direction: str) -> dict[str, str]:
    return {"item": name, "status": status, "current": current, "basis": basis, "direction": direction}


def evaluate_credit_report_rules(report_model: dict[str, Any]) -> list[dict[str, str]]:
    """Evaluate v1 rules without inventing any bank admission threshold."""
    metrics = report_model.get("metrics") if isinstance(report_model.get("metrics"), dict) else {}

    loan_overdue = _number(metrics.get("loan_overdue_account_count"))
    card_overdue = _number(metrics.get("credit_card_overdue_account_count"))
    overdue_values = [value for value in (loan_overdue, card_overdue) if value is not None]
    overdue_conflicts = metrics.get("overdue_data_conflicts") if isinstance(metrics.get("overdue_data_conflicts"), list) else []
    if overdue_conflicts:
        overdue = _item(
            "逾期记录",
            "待核验",
            "资料存在冲突",
            "；".join(str(item) for item in overdue_conflicts),
            "核对源征信报告",
        )
    elif any(value > 0 for value in overdue_values):
        overdue = _item("逾期记录", "风险", f"贷款逾期账户数：{_display(loan_overdue)}；信用卡逾期账户数：{_display(card_overdue)}", "源征信存在逾期账户", "核对逾期状态、金额及结清时间，并准备情况说明")
    elif len(overdue_values) == 2:
        overdue = _item("逾期记录", "达标", "贷款及信用卡逾期账户数均为0", "源征信明确记录为0", "持续保持按时还款")
    else:
        overdue = _item("逾期记录", "资料不足", "资料不足", "缺少可确认的逾期账户统计", "补充完整征信报告或人工核验")

    overdue_90 = _number(metrics.get("overdue_90d_account_count"))
    if overdue_90 is None:
        overdue_90_item = _item("90天以上逾期", "资料不足", "资料不足", "缺少90天以上逾期账户统计", "补充完整征信报告或人工核验")
    elif overdue_90 > 0:
        overdue_90_item = _item("90天以上逾期", "风险", f"90天以上逾期账户数：{_display(overdue_90)}", "源征信存在90天以上逾期账户", "优先核实逾期状态及处置记录")
    else:
        overdue_90_item = _item("90天以上逾期", "达标", "90天以上逾期账户数：0", "源征信明确记录为0", "持续保持按时还款")

    hard_queries = metrics.get("hard_query_6m_count")
    query_item = (
        _item("硬查询次数", "待评估", f"近6个月硬查询次数：{hard_queries}", "已提取当前值，但未配置统一银行准入阈值", "按拟申请银行或产品的正式规则人工评估")
        if _present(hard_queries)
        else _item("硬查询次数", "资料不足", "资料不足", "缺少可确认的近6个月分类统计", "补充查询记录或人工核验")
    )

    card_rate = metrics.get("credit_card_usage_rate")
    card_item = (
        _item("信用卡使用率", "待评估", str(card_rate), "已计算使用率，但未配置统一银行准入阈值", "按拟申请银行或产品的正式规则人工评估")
        if _present(card_rate)
        else _item("信用卡使用率", "资料不足", "资料不足", "缺少有效授信额度或已用额度", "补充信用卡账户明细")
    )

    dti = metrics.get("dti")
    dti_item = (
        _item("负债收入比 DTI", "待评估", str(dti), "已具备收入和月度债务口径，但未配置统一银行准入阈值", "按拟申请银行或产品的正式规则人工评估")
        if _present(dti)
        else _item("负债收入比 DTI", "资料不足", "资料不足", "缺少收入或月度债务偿还口径，未计算", "补充可核验收入及月还款数据")
    )

    online_count = metrics.get("online_loan_count")
    online_item = (
        _item("网贷笔数", "待评估", f"识别笔数：{online_count}", "已提取当前值，但未配置统一银行准入阈值", "按拟申请银行或产品的正式规则人工评估")
        if _present(online_count)
        else _item("网贷笔数", "资料不足", "资料不足", "源资料未提供可靠机构分类", "人工核验贷款机构类别")
    )

    concentration = metrics.get("large_revolving_due_concentration")
    concentration_item = (
        _item("大额循环授信到期集中度", "待评估", str(concentration), "已提取当前情况，但未配置统一阈值", "结合拟申请产品规则人工评估")
        if _present(concentration)
        else _item("大额循环授信到期集中度", "资料不足", "资料不足", "缺少完整循环授信金额及到期日", "补充授信及到期明细")
    )

    enterprise_guarantee = _number(metrics.get("enterprise_external_guarantee_balance"))
    enterprise_guarantee_known = bool(metrics.get("enterprise_external_guarantee_explicit"))
    if enterprise_guarantee is not None and enterprise_guarantee > 0:
        guarantee_item = _item("企业对外担保", "关注", f"余额：{_display(metrics.get('enterprise_external_guarantee_balance'))}", "企业征信明确存在对外担保余额", "核实被担保主体、期限及代偿风险")
    elif enterprise_guarantee == 0 and enterprise_guarantee_known:
        guarantee_item = _item("企业对外担保", "达标", f"余额：{_display(metrics.get('enterprise_external_guarantee_balance'))}", "企业征信明确记录为0", "持续关注新增对外担保")
    else:
        guarantee_item = _item("企业对外担保", "资料不足", "资料不足", "企业征信未提供可确认口径", "补充企业征信担保信息")

    card_count = metrics.get("credit_card_account_count")
    card_count_item = (
        _item("信用卡数量", "待评估", f"账户数：{card_count}", "已提取当前值，但未配置统一银行准入阈值", "按拟申请银行或产品的正式规则人工评估")
        if _present(card_count)
        else _item("信用卡数量", "资料不足", "资料不足", "缺少可确认的信用卡账户统计", "补充个人征信概要")
    )

    if metrics.get("credit_card_count_mismatch"):
        card_consistency_item = _item(
            "信用卡账户数量一致性",
            "待核验",
            f"征信概要账户数：{_display(card_count)}；可稳定提取明细数：{_display(metrics.get('credit_card_detail_count'))}",
            "征信概要账户数量与本节可稳定提取明细数量不一致",
            "核对源征信报告及未展示账户状态",
        )
    elif _present(card_count):
        card_consistency_item = _item(
            "信用卡账户数量一致性", "达标",
            f"征信概要账户数与可稳定提取明细数均为{_display(card_count)}",
            "概要统计与可稳定提取明细数量一致", "无需额外处理",
        )
    else:
        card_consistency_item = _item(
            "信用卡账户数量一致性", "资料不足", "资料不足",
            "缺少征信概要信用卡账户数", "补充个人征信概要并核对明细",
        )

    return [overdue, overdue_90_item, query_item, card_item, dti_item, online_item, concentration_item, guarantee_item, card_count_item, card_consistency_item]


def _display(value: Any) -> str:
    if value is None:
        return "资料不足"
    if isinstance(value, dict) and "value" in value:
        number = _number(value)
        if number is None:
            return "资料不足"
        rendered = str(int(number)) if number == int(number) else str(number)
        unit = value.get("unit")
        return f"{rendered}{unit}" if unit else f"{rendered}（单位待核验）"
    number = _number(value)
    if number is not None and number == int(number):
        return str(int(number))
    return str(value)
