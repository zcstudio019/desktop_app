"""Deterministic Markdown renderer for credit_one_page_report_v1."""

from __future__ import annotations

from typing import Any, Iterable


def _cell(value: Any, empty: str = "资料不足") -> str:
    if value is None or value == "":
        text = empty
    elif isinstance(value, bool):
        text = "是" if value else "否"
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\r", " ").replace("\n", "<br>").strip() or empty


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
        ("企业未结清贷款笔数", "enterprise_unsettled_loan_count", "企业征信"),
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
        ("近6个月硬查询次数", "hard_query_6m_count", "个人征信查询记录"),
    )
    rows = [[label, metrics.get(key), source] for label, key, source in definitions]
    return render_markdown_table(["指标", "当前情况", "口径/来源"], rows)


def render_loan_table(loans: list[dict[str, Any]]) -> str:
    fields = ("index", "institution", "institution_type", "loan_type", "contract_amount", "balance", "start_date", "due_date", "status")
    rows = [_row_values(item, fields) for item in loans]
    return render_markdown_table(
        ["序号", "贷款机构", "机构类别", "贷款类型", "合同金额", "当前余额", "发放日期", "到期日期", "状态/备注"],
        rows,
    )


def render_credit_card_table(cards: list[dict[str, Any]]) -> str:
    fields = ("issuer", "currency", "credit_limit", "used_amount", "usage_rate", "overdue", "remark")
    return render_markdown_table(
        ["发卡行", "币种", "信用额度", "已用额度", "使用率", "逾期", "备注"],
        [_row_values(item, fields) for item in cards],
    )


def render_enterprise_guarantee_table(items: list[dict[str, Any]], explicit_zero: bool = False) -> str:
    if not items and explicit_zero:
        return render_markdown_table(
            ["被担保主体", "贷款机构", "担保金额", "当前余额", "担保日期", "状态"],
            [["企业征信明确记录为0", "-", "0", "0", "-", "无余额"]],
        )
    fields = ("guaranteed_subject", "institution", "guarantee_amount", "balance", "guarantee_date", "status")
    return render_markdown_table(
        ["被担保主体", "贷款机构", "担保金额", "当前余额", "担保日期", "状态"],
        [_row_values(item, fields) for item in items],
    )


def render_related_liability_table(items: list[dict[str, Any]]) -> str:
    fields = ("responsible_subject", "related_party", "institution", "responsibility_amount", "balance", "responsibility_type", "business_type", "as_of_date")
    return render_markdown_table(
        ["责任主体", "被担保/关联主体", "贷款机构", "责任金额", "当前余额", "责任类型", "业务类型", "截至日期"],
        [_row_values(item, fields) for item in items],
    )


def render_query_table(rows: list[dict[str, Any]]) -> str:
    fields = ("window", "loan_approval", "credit_card_approval", "guarantee_review", "legal_person_review")
    return render_markdown_table(
        ["时间范围", "贷款审批", "信用卡审批", "担保资格审查", "法人资信审查"],
        [_row_values(item, fields) for item in rows],
    )


def render_rule_check_table(items: list[dict[str, Any]]) -> str:
    fields = ("item", "status", "current", "basis", "direction")
    return render_markdown_table(
        ["检查项", "状态", "当前情况", "判断依据", "优化方向"],
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
    personal_overdue = personal.get("overdue_records") if isinstance(personal.get("overdue_records"), list) else []
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

{render_markdown_table(['主体信息', '内容'], basic_rows)}

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

# 四、担保及相关还款责任

## 4.1 企业对外担保

本节只采用企业征信口径。

{render_enterprise_guarantee_table(enterprise_guarantees, bool(metrics.get('enterprise_external_guarantee_explicit') and metrics.get('enterprise_external_guarantee_balance') in (0, 0.0, '0')))}

## 4.2 法人相关还款责任

本节只采用“{_cell(subjects.get('personal_credit_subject'))}”个人征信中的相关还款责任、保证人责任或共同借款责任，不等同于企业对外担保。

{render_related_liability_table(related)}

# 五、逾期与公共记录

## 企业征信逾期

{_record_lines(enterprise_overdue, (('逾期记录', 'description'),))}

## 个人征信逾期

{_record_lines(personal_overdue, (('逾期记录', 'description'),))}

## 公共记录

{_record_lines(public_records, (('记录类型', 'record_type'), ('内容', 'content')))}

## 非信贷交易记录

{_record_lines(non_credit, (('记录类型', 'record_type'), ('内容', 'content')))}

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

