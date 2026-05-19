from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

RISK_LEVEL_LABELS = {
    "low": "低",
    "medium": "中",
    "high": "高",
}

MISSING_FIELD_LABELS = {
    "basic_info.report_number": "报告编号",
    "basic_info.name": "姓名",
    "basic_info.id_number": "证件号码",
}

WARNING_LABELS = {
    "loan_accounts_not_array": "贷款账户明细格式异常，已按空列表处理",
    "credit_card_accounts_not_array": "信用卡账户明细格式异常，已按空列表处理",
    "query_records_not_array": "查询记录格式异常，已按空列表处理",
}

SUMMARY_ROWS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("信用卡账户数", "credit_card_account_count", ()),
    ("当前有效信用卡账户数", "active_credit_card_account_count", ("credit_card_active_count",)),
    ("贷款账户数", "loan_account_count", ("housing_loan_account_count", "other_loan_account_count")),
    ("购房贷款账户数", "housing_loan_account_count", ()),
    ("其他贷款账户数", "other_loan_account_count", ()),
    ("未结清贷款账户数", "outstanding_loan_account_count", ("housing_loan_outstanding_count", "other_loan_outstanding_count")),
    ("未结清购房贷款账户数", "housing_loan_outstanding_count", ()),
    ("未结清其他贷款账户数", "other_loan_outstanding_count", ()),
    ("信用卡逾期账户数", "credit_card_overdue_account_count", ("credit_card_overdue_count",)),
    ("信用卡 90 天以上逾期账户数", "credit_card_90d_overdue_account_count", ("credit_card_90d_overdue_count",)),
    ("贷款逾期账户数", "loan_overdue_account_count", ("housing_loan_overdue_count", "other_loan_overdue_count")),
    ("贷款 90 天以上逾期账户数", "loan_90d_overdue_account_count", ()),
    ("为个人相关还款责任账户数", "personal_related_repayment_responsibility_account_count", ()),
    ("为企业相关还款责任账户数", "enterprise_related_repayment_responsibility_account_count", ()),
)


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == []


def _value(value: Any, empty: str = "未识别") -> str:
    if _is_empty(value):
        return empty
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<=[\u4e00-\u9fff])\n(?=[\u4e00-\u9fff])", "", text)
    text = re.sub(r"(?<=[A-Za-z0-9])\n(?=[A-Za-z0-9])", "", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"股份有\s*限公司", "股份有限公司", text)
    text = re.sub(r"有限公\s*司", "有限公司", text)
    text = re.sub(r"支\s*行", "支行", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _count(value: Any) -> str:
    return "未识别" if value is None or value == "" else str(value)


SUMMARY_ZERO_VALUES = {"--", "——", "-", "—", "未显示", "0 / 未显示", "0 / 未显示为有效"}


def _summary_count(value: Any) -> str:
    if value is None or value == "":
        return "0"
    text = str(value).strip()
    return "0" if text in SUMMARY_ZERO_VALUES else text


def _summary_number(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _summary_sum(summary: dict[str, Any], keys: tuple[str, ...]) -> str:
    numbers = [_summary_number(summary.get(key)) for key in keys]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def _summary_value(summary: dict[str, Any], key: str, legacy_keys: tuple[str, ...] = ()) -> str:
    value = summary.get(key)
    if value not in (None, ""):
        return _summary_count(value)
    return _summary_count(_summary_sum(summary, legacy_keys))


def _rate(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "未识别"
    return f"{value * 100:.2f}%"


def _yes_no(value: Any) -> str:
    return "是" if bool(value) else "否"


def _risk_level(value: Any) -> str:
    return RISK_LEVEL_LABELS.get(str(value or "").lower(), _value(value))


def _dedupe_lines(items: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        cleaned = str(item or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _append_field(lines: list[str], label: str, value: Any, empty: str = "未识别") -> None:
    lines.append(f"- {label}：{_value(value, empty)}")


def _append_account(lines: list[str], title: str, record: dict[str, Any], fields: tuple[tuple[str, str], ...]) -> None:
    lines.append(f"### {title}")
    for key, label in fields:
        _append_field(lines, label, record.get(key))


def _has_real_card_tail_no(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text) and text not in {"未识别", "--", "-", "无"}


def _append_credit_card_account(lines: list[str], title: str, record: dict[str, Any]) -> None:
    lines.append(f"### {title}")
    for key, label in (
        ("open_date", "发卡日期"),
        ("institution", "发卡机构"),
        ("card_type", "卡类型"),
        ("currency", "币种"),
    ):
        _append_field(lines, label, record.get(key))
    if _has_real_card_tail_no(record.get("card_tail_no")):
        _append_field(lines, "卡片尾号", record.get("card_tail_no"))
    for key, label in (
        ("credit_limit", "信用额度"),
        ("used_limit", "已使用额度"),
    ):
        _append_field(lines, label, record.get(key))
    if record.get("special_installment_balance"):
        _append_field(lines, "大额专项分期余额", record.get("special_installment_balance"))
    for key, label in (
        ("account_status", "账户状态"),
        ("report_cutoff", "截至日期"),
    ):
        _append_field(lines, label, record.get(key))


def _format_warning(item: Any) -> str:
    text = str(item or "").strip()
    if not text:
        return ""
    if text in WARNING_LABELS:
        return WARNING_LABELS[text]
    if text == "证件号码未识别或格式异常":
        return text
    if text.startswith("loan_account_count_mismatch"):
        return "贷款账户概要数量与明细数量差异较大"
    if text.startswith("credit_card_account_count_mismatch"):
        return "信用卡账户概要数量与明细数量差异较大"
    if text.startswith("query_date_parse_failed"):
        return "部分查询日期无法解析，查询次数统计可能不完整"
    if text.startswith("basic_info_contaminated"):
        return "报告基础信息疑似混入其他区块内容，请核验"
    if text.startswith("loan_account_contains_card_terms"):
        return "贷款账户明细疑似混入信用卡信息，请核验"
    if text.startswith("credit_card_account_count_unusually_large"):
        return "信用卡账户数异常偏大，请核验 OCR 或分段结果"
    if text.startswith("已过滤疑似相关还款责任/查询记录污染的贷款账户"):
        return text
    if text.startswith("已过滤疑似非本人贷款账户/相关还款责任污染记录"):
        return text
    if text.startswith("loan_account_pollution_suspected"):
        return "贷款账户明细疑似混入相关还款责任/查询记录内容，请核验"
    if text.startswith("closed_credit_card_account_still_present"):
        return "信用卡账户明细疑似仍包含销户账户，请核验"
    return text


def _pending_items(report: dict[str, Any], warnings: list[Any], missing: list[Any]) -> list[str]:
    items: list[str] = []
    for item in missing:
        label = MISSING_FIELD_LABELS.get(str(item), str(item))
        items.append(f"{label}未识别")
    for warning in warnings:
        formatted = _format_warning(warning)
        if formatted:
            items.append(formatted)
    indicators = report.get("personal_credit_indicators") or {}
    for warning in indicators.get("warnings") or []:
        formatted = _format_warning(warning)
        if formatted:
            items.append(formatted)
    return _dedupe_lines(items)


def render_personal_credit_markdown(report: dict[str, Any]) -> str:
    basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
    summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}
    loans = report.get("loan_accounts") if isinstance(report.get("loan_accounts"), list) else []
    cards = report.get("credit_card_accounts") if isinstance(report.get("credit_card_accounts"), list) else []
    related_repayments = report.get("related_repayment_responsibilities") if isinstance(report.get("related_repayment_responsibilities"), list) else []
    guarantees = report.get("guarantees") if isinstance(report.get("guarantees"), list) else []
    non_credit_transactions = report.get("non_credit_transactions") if isinstance(report.get("non_credit_transactions"), list) else []
    public_records = report.get("public_records") if isinstance(report.get("public_records"), list) else []
    queries = report.get("query_records") if isinstance(report.get("query_records"), list) else []
    query_statistics = report.get("query_statistics") if isinstance(report.get("query_statistics"), dict) else {}
    indicators = report.get("personal_credit_indicators") if isinstance(report.get("personal_credit_indicators"), dict) else {}
    risk_flags = report.get("risk_flags") if isinstance(report.get("risk_flags"), list) else []
    warnings = report.get("warnings") if isinstance(report.get("warnings"), list) else []
    missing = report.get("missing_fields") if isinstance(report.get("missing_fields"), list) else []
    pending = _pending_items(report, warnings, missing)
    logger.info(
        "[PersonalCredit][Summary][BEFORE_MARKDOWN] credit_card_90d=%s loan_90d=%s personal_related=%s enterprise_related=%s",
        summary.get("credit_card_90d_overdue_account_count"),
        summary.get("loan_90d_overdue_account_count"),
        summary.get("personal_related_repayment_responsibility_account_count"),
        summary.get("enterprise_related_repayment_responsibility_account_count"),
    )

    lines: list[str] = [
        "# 个人征信报告",
        "",
        "## 一、资料信息",
        "- 资料类型：个人征信报告",
        f"- 来源文件：{_value(basic.get('source_file'))}",
        "- 原件状态：可查看",
        "",
        "## 二、报告基础信息",
    ]
    _append_field(lines, "姓名", basic.get("name"))
    _append_field(lines, "证件类型", basic.get("id_type"))
    _append_field(lines, "证件号码", basic.get("id_number"))
    _append_field(lines, "婚姻状况", basic.get("marital_status"))
    _append_field(lines, "报告编号", basic.get("report_number"))
    _append_field(lines, "报告时间", basic.get("report_time"))

    lines.extend([
        "",
        "## 三、信贷记录概要",
        "| 项目 | 数量 / 状态 |",
        "|---|---:|",
    ])
    for label, key, legacy_keys in SUMMARY_ROWS:
        lines.append(f"| {label} | {_summary_value(summary, key, legacy_keys)} |")

    lines.extend([
        "",
        "## 四、贷款账户明细",
        "仅展示未结清、当前有效或存在异常的贷款账户；已结清贷款不展示。",
    ])
    if loans:
        for index, item in enumerate(loans, start=1):
            if isinstance(item, dict):
                _append_account(lines, f"贷款 {index}", item, (
                    ("start_date", "起始日期"),
                    ("institution", "机构"),
                    ("amount", "金额"),
                    ("loan_type", "类型"),
                    ("due_date", "到期日期"),
                    ("cutoff_date", "截止日期"),
                    ("balance", "余额"),
                    ("overdue_status", "逾期"),
                ))
    else:
        lines.append("- 暂无需要展示的未结清贷款账户。")

    lines.extend([
        "",
        "## 五、信用卡账户明细",
        "仅展示当前有效、未销户且为人民币账户的信用卡；销户账户、外币账户不展示。",
    ])
    if cards:
        for index, item in enumerate(cards, start=1):
            if isinstance(item, dict):
                _append_credit_card_account(lines, f"账户 {index}", item)
    else:
        lines.append("- 暂无需要展示的当前有效人民币信用卡账户。")

    lines.extend(["", "## 六、相关还款责任信息"])
    if related_repayments:
        for index, item in enumerate(related_repayments, start=1):
            if isinstance(item, dict):
                _append_account(lines, f"相关还款责任 {index}", item, (
                    ("start_date", "起始日期"),
                    ("related_party", "被担保/相关企业"),
                    ("responsibility_type", "责任人类型"),
                    ("institution", "办理机构"),
                    ("business_type", "业务类型"),
                    ("responsibility_amount", "相关还款责任金额"),
                    ("balance_type", "余额类型"),
                    ("loan_balance", "贷款/融资余额"),
                    ("contract_no", "合同编号"),
                    ("as_of_date", "截至日期"),
                ))
                if item.get("_duplicate_contract_no_warning") or item.get("duplicate_contract_no_warning"):
                    _append_field(lines, "核验提示", item.get("warning") or "合同编号与其他记录重复，但起始日期或贷款余额不同，已保留待核验")
    else:
        lines.append("- 暂无相关还款责任信息。")

    lines.extend(["", "## 七、担保信息"])
    if guarantees:
        for index, item in enumerate(guarantees, start=1):
            if isinstance(item, dict):
                _append_account(lines, f"记录 {index}", item, (
                    ("guarantee_for", "被担保人"),
                    ("guarantee_amount", "担保金额"),
                    ("guarantee_balance", "担保余额"),
                    ("guarantee_status", "状态"),
                ))
    else:
        lines.append("- 暂无")

    lines.extend(["", "## 八、非信贷交易记录"])
    if non_credit_transactions:
        for index, item in enumerate(non_credit_transactions, start=1):
            if isinstance(item, dict):
                _append_account(lines, f"非信贷交易记录 {index}", item, (
                    ("record_type", "记录类型"),
                    ("date", "日期"),
                    ("institution", "机构"),
                    ("amount", "金额"),
                    ("content", "内容"),
                ))
    else:
        lines.append("- 暂无")

    lines.extend(["", "## 九、公共记录"])
    if public_records:
        for index, item in enumerate(public_records, start=1):
            if isinstance(item, dict):
                _append_account(lines, f"记录 {index}", item, (
                    ("record_type", "记录类型"),
                    ("record_date", "日期"),
                    ("authority", "机构"),
                    ("amount", "金额"),
                    ("content", "内容"),
                ))
    else:
        lines.append("- 暂无")

    lines.extend(["", "## 十、查询记录"])
    institution_query = query_statistics.get("institution_query") if isinstance(query_statistics.get("institution_query"), dict) else {}
    personal_query = query_statistics.get("personal_query") if isinstance(query_statistics.get("personal_query"), dict) else {}
    lines.extend([
        "### 机构查询",
        f"- 近1个月查询次数：{_count(institution_query.get('last_1_month'))}",
        f"- 近3个月查询次数：{_count(institution_query.get('last_3_months'))}",
        f"- 近6个月查询次数：{_count(institution_query.get('last_6_months'))}",
        "",
        "### 个人查询",
        f"- 近1个月查询次数：{_count(personal_query.get('last_1_month'))}",
        f"- 近3个月查询次数：{_count(personal_query.get('last_3_months'))}",
        f"- 近6个月查询次数：{_count(personal_query.get('last_6_months'))}",
    ])

    risk_reasons = _dedupe_lines([*(str(x) for x in risk_flags), *(str(x) for x in indicators.get("risk_reasons") or [])])
    lines.extend([
        "",
        "## 十一、风险提示",
        f"- 综合风险等级：{_risk_level(indicators.get('risk_level'))}",
        f"- 当前逾期：{_yes_no(indicators.get('has_current_overdue'))}",
        f"- 90天以上逾期：{_yes_no(indicators.get('has_90d_overdue'))}",
        f"- 呆账/代偿/核销/强制执行：{_yes_no(indicators.get('has_bad_debt_or_compensation'))}",
        f"- 近 1 个月贷款审批查询次数：{_count(indicators.get('loan_approval_queries_1m'))}",
        f"- 近 3 个月贷款审批查询次数：{_count(indicators.get('loan_approval_queries_3m'))}",
        f"- 近 6 个月贷款审批查询次数：{_count(indicators.get('loan_approval_queries_6m'))}",
        f"- 近 3 个月信用卡审批查询次数：{_count(indicators.get('credit_card_approval_queries_3m'))}",
        f"- 信用卡使用率：{_rate(indicators.get('credit_card_usage_rate'))}",
        f"- 相关还款责任：{_yes_no(indicators.get('has_related_repayment_responsibility'))}",
        f"- 相关还款责任余额：{_value(indicators.get('related_repayment_total_balance'), '暂无')}",
    ])
    lines.append("- 风险原因：" + ("；".join(risk_reasons) if risk_reasons else "暂无"))

    lines.extend(["", "## 十二、待核验项"])
    if pending:
        lines.extend(f"- {item}" for item in pending)
    else:
        lines.append("- 暂无")
    return "\n".join(lines).strip()
