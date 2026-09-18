"""Deterministic Markdown renderer for credit_one_page_report_v1."""

from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)
ABNORMAL_FIELD_TEXT = "资料异常，需核验"

TABLE_HEADERS: dict[str, tuple[str, ...]] = {
    "subject": ("主体信息", "内容"),
    "core_metrics": ("指标", "当前情况", "口径/来源"),
    "loan": ("序号", "贷款机构", "机构类别", "贷款类型", "合同金额", "当前余额", "发放日期", "到期日期", "状态/备注"),
    "credit_card": ("发卡行", "币种", "信用额度", "已用额度", "使用率", "逾期", "备注"),
    "enterprise_guarantee": ("被担保主体", "贷款机构", "担保金额", "当前余额", "担保日期", "状态"),
    "related_liability": ("责任主体", "被担保/关联主体", "贷款机构", "责任金额", "当前余额", "责任类型", "业务类型", "截至日期"),
    "query": ("时间范围", "贷款审批", "信用卡审批", "担保资格审查", "法人资信审查"),
    "rule_check": ("检查项", "状态", "当前情况", "判断依据", "优化方向"),
}


def format_number(value: Any) -> str:
    """Format report numbers without assuming a float implementation."""
    if value is None:
        return "资料不足"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return f"{int(value):,}"
        return format(value, "f").rstrip("0").rstrip(".")
    return str(value)


def render_money(value: Any, unit: str | None = None) -> str:
    text = format_number(value)
    if text == "资料不足":
        return text
    return f"{text}{unit}" if unit else f"{text}（单位待核验）"


def _format_money(value: dict[str, Any]) -> str:
    amount = value.get("value")
    if amount in (None, ""):
        return ABNORMAL_FIELD_TEXT if value.get("unit_status") == "abnormal" else "资料不足"
    return render_money(amount, value.get("unit"))


def _money_number(value: Any) -> float | None:
    if isinstance(value, dict):
        value = value.get("value")
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _cell(value: Any, empty: str = "资料不足") -> str:
    if isinstance(value, dict) and "value" in value:
        text = _format_money(value)
    elif value is None or value == "":
        text = empty
    elif isinstance(value, bool):
        text = "是" if value else "否"
    else:
        text = str(value)
    if (
        len(text) > 300
        or re.search(r"<br\s*/?>", text, re.IGNORECASE)
        or re.search(r"个人信用报告|企业信用报告|信息概要|信贷记录概要", text)
        or text.count("\n") > 3
    ):
        logger.warning("credit report renderer rejected abnormal cell length=%s", len(text))
        text = ABNORMAL_FIELD_TEXT
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", "；").strip() or empty


def _row_values(item: dict[str, Any], fields: Iterable[str]) -> list[Any]:
    return [item.get(field) for field in fields]


def render_markdown_table(headers: list[str], rows: list[list[Any]], empty_label: str = "资料不足") -> str:
    """Render a valid GFM table with identical column counts for every row."""
    width = len(headers)
    normalized_rows = rows or [[empty_label] * width]
    lines = [
        "| " + " | ".join(_cell(header, "-") for header in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in normalized_rows:
        padded = list(row[:width]) + [empty_label] * max(0, width - len(row))
        lines.append("| " + " | ".join(_cell(value, empty_label) for value in padded) + " |")
    return "\n".join(lines)


def render_core_metrics_table(metrics: dict[str, Any]) -> str:
    definitions = (
        ("企业未结清贷款余额", "enterprise_unsettled_loan_balance", "企业征信"),
        ("企业未结清贷款机构数", "enterprise_unsettled_loan_institution_count", "企业征信原始指标语义"),
        ("个人未结清贷款余额", "personal_unsettled_loan_balance", "个人征信"),
        ("个人未结清贷款账户数", "personal_unsettled_loan_account_count", "个人征信"),
        ("个人信用卡授信额度", "personal_credit_card_limit", "个人征信"),
        ("个人信用卡已用额度", "personal_credit_card_used", "个人征信"),
        ("个人信用卡使用率", "credit_card_usage_rate", "根据明确额度计算"),
        ("贷款逾期账户数", "loan_overdue_account_count", "个人征信原始指标语义"),
        ("信用卡逾期账户数", "credit_card_overdue_account_count", "个人征信原始指标语义"),
        ("90天以上逾期账户数", "overdue_90d_account_count", "个人征信原始指标语义"),
        ("企业对外担保余额", "enterprise_external_guarantee_balance", "仅企业征信口径"),
        ("法人相关还款责任余额", "personal_related_repayment_balance", "仅个人征信相关还款责任口径"),
        ("近6个月征信机构查询次数", "institution_query_6m_count", "个人征信查询记录分类合计；未定义硬查询口径"),
    )
    rows = [[label, metrics.get(key), source] for label, key, source in definitions]
    return render_markdown_table(list(TABLE_HEADERS["core_metrics"]), rows)


def render_loan_table(loans: list[dict[str, Any]]) -> str:
    fields = ("index", "institution", "institution_type", "loan_type", "contract_amount", "balance", "start_date", "due_date", "status")
    rows = [_row_values(item, fields) for item in loans]
    return render_markdown_table(
        list(TABLE_HEADERS["loan"]),
        rows,
    )


def render_credit_card_table(cards: list[dict[str, Any]]) -> str:
    fields = ("issuer", "currency", "credit_limit", "used_amount", "usage_rate", "overdue", "remark")
    return render_markdown_table(
        list(TABLE_HEADERS["credit_card"]),
        [_row_values(item, fields) for item in cards],
    )


def render_enterprise_guarantee_table(items: list[dict[str, Any]], explicit_zero: bool = False, zero_balance: Any = None) -> str:
    if not items and explicit_zero:
        return render_markdown_table(
            list(TABLE_HEADERS["enterprise_guarantee"]),
            [["企业征信明确记录为0", "-", None, zero_balance, "-", "无余额"]],
        )
    fields = ("guaranteed_subject", "institution", "guarantee_amount", "balance", "guarantee_date", "status")
    return render_markdown_table(
        list(TABLE_HEADERS["enterprise_guarantee"]),
        [_row_values(item, fields) for item in items],
    )


def render_related_liability_table(items: list[dict[str, Any]]) -> str:
    fields = ("responsible_subject", "related_party", "institution", "responsibility_amount", "balance", "responsibility_type", "business_type", "as_of_date")
    return render_markdown_table(
        list(TABLE_HEADERS["related_liability"]),
        [_row_values(item, fields) for item in items],
    )


def render_query_table(rows: list[dict[str, Any]]) -> str:
    fields = ("window", "loan_approval", "credit_card_approval", "guarantee_review", "legal_person_review")
    return render_markdown_table(
        list(TABLE_HEADERS["query"]),
        [_row_values(item, fields) for item in rows],
    )


def render_rule_check_table(items: list[dict[str, Any]]) -> str:
    fields = ("item", "status", "current", "basis", "direction")
    return render_markdown_table(
        list(TABLE_HEADERS["rule_check"]),
        [_row_values(item, fields) for item in items],
    )


def _bullets(items: Any, empty: str = "资料不足") -> str:
    values = items if isinstance(items, list) else []
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return "\n".join(f"- {item}" for item in cleaned) if cleaned else f"- {empty}"


def _record_lines(records: list[dict[str, Any]], labels: tuple[tuple[str, str], ...]) -> str:
    if not records:
        return "- 资料不足"
    lines: list[str] = []
    for label, key in labels:
        values = [_cell(item.get(key)) for item in records if item.get(key) not in (None, "")]
        lines.append(f"- {label}：{'；'.join(values) if values else '资料不足'}")
    return "\n".join(lines)


def _personal_overdue_section(personal: dict[str, Any], metrics: dict[str, Any]) -> str:
    summary = personal.get("overdue_summary") if isinstance(personal.get("overdue_summary"), dict) else {}
    loan = summary.get("loan_overdue_account_count")
    card = summary.get("credit_card_overdue_account_count")
    over_90 = metrics.get("overdue_90d_account_count")
    lines = [
        f"- 贷款逾期账户数：{_cell(loan)}",
        f"- 信用卡逾期账户数：{_cell(card)}",
        f"- 90天以上逾期账户数：{_cell(over_90)}",
    ]
    conflicts = metrics.get("overdue_data_conflicts") if isinstance(metrics.get("overdue_data_conflicts"), list) else []
    if conflicts:
        lines.append(f"- 状态：待核验（{'；'.join(_cell(item) for item in conflicts)}）")
        return "\n".join(lines)
    known_counts = [_money_number(value) for value in (loan, card, over_90)]
    if all(value == 0 for value in known_counts if value is not None) and any(value is not None for value in known_counts):
        return "\n".join(lines)
    if any(value is not None and value > 0 for value in known_counts):
        details = personal.get("overdue_records") if isinstance(personal.get("overdue_records"), list) else []
        lines.append("\n**稳定结构化逾期明细：**")
        lines.append(_record_lines(details, (("机构", "institution"), ("账户/业务类型", "account_type"), ("当前状态", "current_status"), ("逾期金额", "overdue_amount"), ("逾期月数", "overdue_months"))))
    return "\n".join(lines)


def render_credit_one_page_report(report_model: dict[str, Any], narrative: dict[str, Any], generated_at: str) -> str:
    subjects = report_model.get("subjects") or {}
    enterprise = report_model.get("enterprise_credit") or {}
    personal = report_model.get("personal_credit") or {}
    metrics = report_model.get("metrics") or {}
    rules = report_model.get("rule_checks") or []

    enterprise_loans = enterprise.get("loans") if isinstance(enterprise.get("loans"), list) else []
    personal_loans = personal.get("loans") if isinstance(personal.get("loans"), list) else []
    cards = personal.get("credit_cards") if isinstance(personal.get("credit_cards"), list) else []
    enterprise_guarantees = enterprise.get("external_guarantees") if isinstance(enterprise.get("external_guarantees"), list) else []
    related = personal.get("related_repayment_responsibilities") if isinstance(personal.get("related_repayment_responsibilities"), list) else []

    emergency = narrative.get("emergency_attention") if isinstance(narrative, dict) else []
    if not emergency:
        emergency = ["暂无需要立即处理的明确风险事项。"]

    basic_rows = [
        ["企业客户", subjects.get("customer_subject")],
        ["企业征信主体", subjects.get("enterprise_credit_subject")],
        ["个人征信主体", subjects.get("personal_credit_subject")],
        ["个人与企业关系", subjects.get("personal_credit_subject_role")],
        ["企业统一社会信用代码", subjects.get("enterprise_identifier_masked")],
        ["个人证件号码", subjects.get("personal_identifier_masked")],
        ["个人征信报告编号", (personal.get("basic_info") or {}).get("report_number")],
        ["个人征信报告时间", (personal.get("basic_info") or {}).get("report_time")],
        ["婚姻状况", (personal.get("basic_info") or {}).get("marital_status")],
    ]

    enterprise_overdue = enterprise.get("overdue_records") if isinstance(enterprise.get("overdue_records"), list) else []
    public_records = personal.get("public_records") if isinstance(personal.get("public_records"), list) else []
    non_credit = personal.get("non_credit_transactions") if isinstance(personal.get("non_credit_transactions"), list) else []

    return f"""# 征信速览报告

企业客户：{_cell(subjects.get('customer_subject'))}

企业征信主体：{_cell(subjects.get('enterprise_credit_subject'))}

个人征信主体：{_cell(subjects.get('personal_credit_subject'))}

个人与企业关系：{_cell(subjects.get('personal_credit_subject_role'), '需人工核实')}

报告时间：{generated_at}

---

## 🚨 紧急关注

{_bullets(emergency, '暂无需要立即处理的明确风险事项。')}

# 一、主体基本信息

{render_markdown_table(list(TABLE_HEADERS['subject']), basic_rows)}

## 核心指标

{render_core_metrics_table(metrics)}

# 二、未结清贷款

## 企业贷款

主体：{_cell(subjects.get('enterprise_credit_subject'))}

{render_loan_table(enterprise_loans)}

## 个人贷款

主体：{_cell(subjects.get('personal_credit_subject'))}

{render_loan_table(personal_loans)}

# 三、信用卡

以下仅为个人征信主体“{_cell(subjects.get('personal_credit_subject'))}”的信用卡信息，不属于企业信用卡。

{render_credit_card_table(cards)}

**账户数量核对：** {_cell(
    f"征信概要账户数为{metrics.get('credit_card_account_count')}，本节可稳定提取明细数为{metrics.get('credit_card_detail_count')}，两者不一致，需核验。"
    if metrics.get('credit_card_count_mismatch')
    else (
        f"征信概要账户数与本节可稳定提取明细数均为{metrics.get('credit_card_account_count')}。"
        if metrics.get('credit_card_account_count') is not None
        else "资料不足"
    )
)}

# 四、担保及相关还款责任

## 4.1 企业对外担保

本节只采用企业征信口径。

{render_enterprise_guarantee_table(
    enterprise_guarantees,
    bool(metrics.get('enterprise_external_guarantee_explicit') and _money_number(metrics.get('enterprise_external_guarantee_balance')) == 0),
    metrics.get('enterprise_external_guarantee_balance'),
)}

## 4.2 法人相关还款责任

本节只采用“{_cell(subjects.get('personal_credit_subject'))}”个人征信中的相关还款责任、保证人责任或共同借款责任，不等同于企业对外担保。

{render_related_liability_table(related)}

# 五、逾期与公共记录

## 企业征信逾期

{_record_lines(enterprise_overdue, (('机构', 'institution'), ('账户/业务类型', 'account_type'), ('当前状态', 'current_status'), ('逾期月数', 'overdue_months')))}

## 个人征信逾期

{_personal_overdue_section(personal, metrics)}

## 公共记录

{_record_lines(public_records, (('记录类型', 'record_type'), ('状态', 'status'), ('日期', 'date'), ('金额', 'amount'), ('内容', 'content')))}

## 非信贷交易记录

{_record_lines(non_credit, (('记录类型', 'record_type'), ('状态', 'status'), ('日期', 'date'), ('金额', 'amount'), ('内容', 'content')))}

# 六、征信查询记录

{render_query_table(personal.get('query_matrix') or [])}

**查询频率说明：** {_cell(narrative.get('query_frequency_analysis') if isinstance(narrative, dict) else None)}

**近期贷款审批查询明细：** {_cell(personal.get('recent_loan_approval_queries'))}

# 七、历史信贷特征

{_bullets(narrative.get('historical_credit_features') if isinstance(narrative, dict) else [])}

# 八、征信指标检查

规则版本：`credit_report_rules_v1`

{render_rule_check_table(rules)}

# 九、优化路线图

## 🔴 紧急（1周内）

{_bullets(narrative.get('optimization_urgent') if isinstance(narrative, dict) else [])}

## 🟡 中期（1个月内）

{_bullets(narrative.get('optimization_medium') if isinstance(narrative, dict) else [])}

## 🟢 长期（3-6个月）

{_bullets(narrative.get('optimization_long') if isinstance(narrative, dict) else [])}

# 十、综合说明

## ✅ 征信优势

{_bullets(narrative.get('advantages') if isinstance(narrative, dict) else [])}

## ⚠️ 征信风险

{_bullets(narrative.get('risks') if isinstance(narrative, dict) else [])}

## 综合说明

{_cell(narrative.get('comprehensive_summary') if isinstance(narrative, dict) else None)}

## 一句话结论

{_cell(narrative.get('one_sentence_conclusion') if isinstance(narrative, dict) else None)}
""".strip()
