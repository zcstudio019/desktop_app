from __future__ import annotations

from typing import Any

from .display_mapper import to_display_json
from .field_labels import CASH_FLOW_LABELS


def _display_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "企业信息" in payload or "资产负债表" in payload:
        return payload
    if isinstance(payload.get("display_json"), dict):
        return payload["display_json"]
    structured = payload.get("structured_json") if isinstance(payload.get("structured_json"), dict) else payload
    reports = structured.get("reports") if isinstance(structured, dict) else None
    if isinstance(reports, dict):
        structured = {**structured, **reports}
    elif isinstance(reports, list) and reports and isinstance(reports[-1], dict):
        structured = {**structured, **reports[-1]}
    return to_display_json(structured if isinstance(structured, dict) else {})


def _field_value(item: dict[str, Any] | Any) -> Any:
    if isinstance(item, dict):
        return item.get("标准化数值") if "标准化数值" in item else item.get("normalized_value")
    return None


def _money(item: dict[str, Any] | Any) -> str:
    value = _field_value(item)
    return "-" if value is None else f"{float(value):,.2f}"


def _previous_money(item: dict[str, Any] | Any) -> str:
    if not isinstance(item, dict):
        return "-"
    value = item.get("对比列标准化数值") if "对比列标准化数值" in item else item.get("previous_normalized_value")
    return "-" if value is None else f"{float(value):,.2f}"


def _page(item: dict[str, Any] | Any) -> str:
    value = (item.get("来源页码") if "来源页码" in item else item.get("source_page")) if isinstance(item, dict) else None
    return "-" if value is None else str(value)


def _confidence(item: dict[str, Any] | Any) -> str:
    value = (item.get("置信度") if "置信度" in item else item.get("confidence")) if isinstance(item, dict) else None
    if _field_value(item) is None:
        return "-"
    return "-" if value is None else f"{float(value):.2f}"


def _ratio(value: Any) -> str:
    return "-" if value is None else f"{float(value):.2%}"


CASH_FLOW_KEY_BY_LABEL = {label: key for key, label in CASH_FLOW_LABELS.items()}
CASH_FLOW_ZERO_VALUE_KEEP_FIELDS = {
    "net_operating_cash_flow",
    "net_investing_cash_flow",
    "net_cash_increase",
    "beginning_cash_balance",
    "ending_cash_balance",
}


def _previous_field_value(item: dict[str, Any] | Any) -> Any:
    if not isinstance(item, dict):
        return None
    return item.get("对比列标准化数值") if "对比列标准化数值" in item else item.get("previous_normalized_value")


def _item_meta(item: dict[str, Any] | Any, english_key: str, chinese_key: str) -> Any:
    if not isinstance(item, dict):
        return None
    return item.get(chinese_key) if chinese_key in item else item.get(english_key)


def _is_missing(value: Any) -> bool:
    return value is None or str(value).strip() in {"", "-", "—"}


def _is_zero(value: Any) -> bool:
    if _is_missing(value):
        return False
    try:
        return float(str(value).replace(",", "").replace("元", "").strip()) == 0.0
    except (TypeError, ValueError):
        return False


def _has_non_zero_value(item: dict[str, Any] | Any) -> bool:
    current = _field_value(item)
    previous = _previous_field_value(item)
    return any(not _is_missing(value) and not _is_zero(value) for value in (current, previous))


def should_render_cash_flow_row(field_key: str, item: dict[str, Any] | Any) -> bool:
    current = _field_value(item)
    previous = _previous_field_value(item)
    original_present = _item_meta(item, "original_present", "原表存在")
    calculated = bool(_item_meta(item, "calculated", "系统计算"))
    source_text = _item_meta(item, "source_text", "来源文本")
    raw_value = _item_meta(item, "raw_value", "原始值")
    previous_raw_value = _item_meta(item, "previous_raw_value", "对比列原始值")
    if calculated and not original_present:
        return False
    if original_present is False:
        return False
    if original_present is None:
        if source_text == "由现金流量明细项计算得出":
            return False
        if not (source_text or raw_value or previous_raw_value):
            return False
    if _is_missing(current) and _is_missing(previous):
        return False
    current_is_empty_or_zero = _is_missing(current) or _is_zero(current)
    previous_is_empty_or_zero = _is_missing(previous) or _is_zero(previous)
    if current_is_empty_or_zero and previous_is_empty_or_zero:
        return field_key in CASH_FLOW_ZERO_VALUE_KEEP_FIELDS
    return True


def _statement_table(
    lines: list[str], title: str, section: dict[str, Any], labels: list[str],
    amount_header: str, previous_header: str, *, hide_double_zero_details: bool = False,
    keep_zero_labels: set[str] | None = None, cash_flow: bool = False,
    hide_missing: bool = False,
    non_zero_only_labels: set[str] | None = None,
) -> None:
    lines.extend([
        "",
        f"### {title}",
        f"| 项目 | {amount_header} | {previous_header} | 来源页码 | 置信度 |",
        "|---|---:|---:|---:|---:|",
    ])
    required_zero_labels = keep_zero_labels or set()
    non_zero_labels = non_zero_only_labels or set()
    for label in labels:
        item = section.get(label) or {}
        if hide_missing and _field_value(item) is None and _previous_field_value(item) is None:
            continue
        if label in non_zero_labels and not _has_non_zero_value(item):
            continue
        if cash_flow and not should_render_cash_flow_row(CASH_FLOW_KEY_BY_LABEL.get(label, ""), item):
            continue
        rendered_label = label
        if cash_flow:
            rendered_label = str(_item_meta(item, "display_label", "展示项目名") or label)
        current = _field_value(item)
        previous = _previous_field_value(item)
        if (
            hide_double_zero_details
            and label not in required_zero_labels
            and current is not None
            and previous is not None
            and float(current) == 0.0
            and float(previous) == 0.0
        ):
            continue
        lines.append(f"| {rendered_label} | {_money(item)} | {_previous_money(item)} | {_page(item)} | {_confidence(item)} |")


def render_financial_report_markdown(data: dict[str, Any]) -> str:
    display_json = _display_payload(data)
    info = display_json.get("企业信息") or {}
    balance = display_json.get("资产负债表") or {}
    income = display_json.get("利润表") or {}
    cashflow = display_json.get("现金流量表") or {}
    ratios = display_json.get("财务指标") or {}
    risk = display_json.get("银行授信分析") or {}
    trends = display_json.get("趋势指标") or []
    lines = [
        "## 财务报表",
        "",
        "- 资料类型：财务报表",
        f"- 来源文件：{display_json.get('来源文件') or '-'}",
        "- 原件状态：可查看",
        "- 报告标题：财务报表授信分析报告",
        f"- 文档类型：{display_json.get('文档类型') or '财务报表'}",
        "",
        "### 企业信息",
        "| 字段 | 内容 |",
        "|---|---|",
    ]
    for label in [
        "企业名称", "纳税人识别号", "会计准则", "报表类型", "所属期开始日期",
        "所属期结束日期", "报送日期", "币种", "金额单位",
    ]:
        lines.append(f"| {label} | {info.get(label) or '-'} |")
    _statement_table(
        lines, "资产负债表摘要", balance,
        [
            "货币资金", "短期投资", "应收账款", "预付款项", "其他应收款", "存货",
            "流动资产合计", "长期股权投资", "固定资产原价", "固定资产账面价值",
            "固定资产", "无形资产", "非流动资产合计", "资产总计",
            "短期借款", "应付账款", "其他应付款", "流动负债合计", "长期借款",
            "长期应付款", "非流动负债合计", "负债合计", "实收资本", "未分配利润",
            "所有者权益合计", "负债和所有者权益总计",
        ],
        "期末余额",
        "上年年末余额",
        hide_missing=True,
        non_zero_only_labels={"短期投资", "长期股权投资", "固定资产原价", "固定资产账面价值", "长期应付款"},
    )
    _statement_table(
        lines, "利润表摘要", income,
        ["营业收入", "营业成本", "税金及附加", "销售费用", "管理费用", "研发费用", "财务费用", "营业利润",
         "利润总额", "所得税费用", "净利润"],
        "本期金额",
        "上期金额",
    )
    _statement_table(
        lines, "现金流量表摘要", cashflow,
        [
            "销售商品、提供劳务收到的现金",
            "收到的税费返还",
            "收到其他与经营活动有关的现金",
            "经营活动现金流入小计",
            "购买商品、接受劳务支付的现金",
            "支付给职工以及为职工支付的现金",
            "支付的各项税费",
            "支付其他与经营活动有关的现金",
            "经营活动现金流出小计",
            "经营活动产生的现金流量净额",
            "收回投资收到的现金",
            "取得投资收益收到的现金",
            "处置固定资产、无形资产和其他长期资产收回的现金净额",
            "处置子公司及其他营业单位收到的现金净额",
            "收到其他与投资活动有关的现金",
            "投资活动现金流入小计",
            "购建固定资产、无形资产和其他长期资产支付的现金",
            "投资支付的现金",
            "取得子公司及其他营业单位支付的现金净额",
            "支付其他与投资活动有关的现金",
            "投资活动现金流出小计",
            "投资活动产生的现金流量净额",
            "吸收投资收到的现金",
            "取得借款收到的现金",
            "收到其他与筹资活动有关的现金",
            "筹资活动现金流入小计",
            "偿还债务支付的现金",
            "分配股利、利润或偿付利息支付的现金",
            "支付其他与筹资活动有关的现金",
            "筹资活动现金流出小计",
            "筹资活动产生的现金流量净额",
            "汇率变动对现金及现金等价物的影响",
            "现金及现金等价物净增加额",
            "期初现金及现金等价物余额",
            "期末现金及现金等价物余额",
        ],
        "本期金额",
        "上期金额",
        cash_flow=True,
    )
    lines.extend([
        "",
        "### 银行授信核心指标表",
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| 资产负债率 | {_ratio(ratios.get('资产负债率'))} |",
        f"| 流动比率 | {ratios.get('流动比率') if ratios.get('流动比率') is not None else '-'} |",
        f"| 速动比率 | {ratios.get('速动比率') if ratios.get('速动比率') is not None else '-'} |",
        f"| 毛利率 | {_ratio(ratios.get('毛利率'))} |",
        f"| 净利率 | {_ratio(ratios.get('净利率'))} |",
        f"| 经营现金流收入比 | {_ratio(ratios.get('经营现金流收入比'))} |",
        "",
        "### 经营趋势分析",
    ])
    if trends:
        lines.extend(["| 所属期间 | 营业收入（元） | 净利润（元） | 经营现金流（元） |", "|---|---:|---:|---:|"])
        for item in trends:
            lines.append(
                f"| {item.get('所属期间') or '-'} | {float(item.get('营业收入') or 0):,.2f} | "
                f"{float(item.get('净利润') or 0):,.2f} | {float(item.get('经营活动产生的现金流量净额') or 0):,.2f} |"
            )
    else:
        lines.append("- 当前仅解析本期报表，待补充历史期间后生成趋势比较。")
    lines.extend([
        "",
        "### 偿债能力分析",
        f"- 资产负债率：{_ratio(ratios.get('资产负债率'))}；现金比率：{_ratio(ratios.get('现金比率'))}。",
        "",
        "### 盈利能力分析",
        f"- 毛利率：{_ratio(ratios.get('毛利率'))}；净利率：{_ratio(ratios.get('净利率'))}。",
        "",
        "### 现金流质量分析",
        f"- 经营现金流收入比：{_ratio(ratios.get('经营现金流收入比'))}；筹资依赖度：{_ratio(ratios.get('筹资依赖度'))}。",
        "",
        "### 异常科目分析",
    ])
    findings = risk.get("风险发现") or []
    lines.extend([f"- [{item.get('风险等级')}] {item.get('风险标题')}：{item.get('风险说明')}" for item in findings] or ["- 未识别到需单列提示的异常财务项目。"])
    lines.extend(["", "### 银行贷款审核关注点"])
    lines.extend([f"- {item}" for item in risk.get("银行审核关注点") or []] or ["- 按常规贷前调查核实收入、负债与现金流真实性。"])
    lines.extend(["", "### 缺失材料清单"])
    lines.extend([f"- {item.get('材料名称')}：{item.get('补充原因')}" for item in risk.get("缺失材料") or []])
    lines.extend([
        "",
        "### 综合授信建议",
        f"- 风险等级：{risk.get('综合风险等级') or '未知'}",
        f"- 授信观点：{risk.get('授信观点') or '-'}",
        f"- 建议策略：{risk.get('建议授信策略') or '-'}",
    ])
    warnings = display_json.get("数据校验提示") or []
    if warnings:
        lines.extend(["", "### 数据校验提示"] + [f"- {warning}" for warning in warnings])
    return "\n".join(lines)
