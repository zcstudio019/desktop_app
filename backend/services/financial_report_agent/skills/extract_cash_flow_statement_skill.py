from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..evidence import build_evidence
from ..schema import AmountField, CashFlowStatement, EvidenceItem
from ._common import extract_amount_fields, normalize_label


CASH_FLOW_FIELDS = {
    "cash_received_from_sales": (
        "销售商品、提供劳务收到的现金",
        "销售商品提供劳务收到的现金",
        "销售产成品、商品、提供劳务收到的现金",
    ),
    "tax_refund_received": ("收到的税费返还",),
    "other_cash_received_related_to_operating": ("收到其他与经营活动有关的现金", "收到的其他与经营活动有关的现金"),
    "operating_cash_inflow_total": ("经营活动现金流入小计",),
    "cash_paid_for_goods_services": (
        "购买商品、接受劳务支付的现金",
        "购买商品接受劳务支付的现金",
        "购买原材料、商品、接受劳务支付的现金",
    ),
    "cash_paid_to_employees": ("支付给职工以及为职工支付的现金", "支付的职工薪酬"),
    "taxes_paid": ("支付的各项税费", "支付的税费"),
    "other_cash_paid_related_to_operating": ("支付其他与经营活动有关的现金", "支付的其他与经营活动有关的现金"),
    "operating_cash_outflow_total": ("经营活动现金流出小计",),
    "net_operating_cash_flow": ("经营活动产生的现金流量净额",),
    "cash_received_from_investment_recovery": (
        "收回投资收到的现金",
        "收回短期投资、长期债券投资和长期股权投资收到的现金",
    ),
    "investment_income_cash_received": ("取得投资收益收到的现金",),
    "cash_received_from_disposal_assets": (
        "处置固定资产、无形资产和其他长期资产收回的现金净额",
        "处置固定资产、无形资产和其他长期资产而收回的现金净额",
        "处置固定资产、无形资产和其他非流动资产收回的现金净额",
    ),
    "cash_received_from_disposal_subsidiaries": ("处置子公司及其他营业单位收到的现金净额",),
    "other_cash_received_related_to_investing": ("收到其他与投资活动有关的现金", "收到的其他与投资活动有关的现金"),
    "investing_cash_inflow_total": ("投资活动现金流入小计",),
    "cash_paid_for_fixed_intangible_assets": (
        "购建固定资产、无形资产和其他长期资产支付的现金",
        "购建固定资产、无形资产和其他长期资产所支付的现金",
        "购建固定资产、无形资产和其他非流动资产支付的现金",
    ),
    "cash_paid_for_investments": ("投资支付的现金", "短期投资、长期债券投资和长期股权投资支付的现金"),
    "cash_paid_for_acquisition_subsidiaries": ("取得子公司及其他营业单位支付的现金净额",),
    "other_cash_paid_related_to_investing": ("支付其他与投资活动有关的现金", "支付的其他与投资活动有关的现金"),
    "investing_cash_outflow_total": ("投资活动现金流出小计",),
    "net_investing_cash_flow": ("投资活动产生的现金流量净额",),
    "cash_received_from_investors": ("吸收投资收到的现金", "取得投资者投资收到的现金"),
    "cash_received_from_borrowings": ("取得借款收到的现金",),
    "other_cash_received_related_to_financing": ("收到其他与筹资活动有关的现金", "收到的其他与筹资活动有关的现金"),
    "financing_cash_inflow_total": ("筹资活动现金流入小计",),
    "cash_paid_for_debt_repayment": ("偿还债务支付的现金", "偿还借款本金支付的现金"),
    "cash_paid_for_dividends_profit_interest": (
        "分配股利、利润或偿付利息支付的现金",
        "分配股利、利润和偿付利息支付的现金",
        "偿还借款利息支付的现金",
        "分配利润支付的现金",
    ),
    "other_cash_paid_related_to_financing": ("支付其他与筹资活动有关的现金", "支付的其他与筹资活动有关的现金"),
    "financing_cash_outflow_total": ("筹资活动现金流出小计",),
    "net_financing_cash_flow": ("筹资活动产生的现金流量净额",),
    "effect_of_exchange_rate_changes": ("汇率变动对现金及现金等价物的影响",),
    "net_cash_increase": ("现金及现金等价物净增加额", "现金及现金等价物净增加额（减少以“-”号填列）", "现金净增加额"),
    "beginning_cash_balance": (
        "期初现金及现金等价物余额",
        "加：期初现金及现金等价物余额",
        "加:期初现金及现金等价物余额",
        "加：期初现金余额",
        "加:期初现金余额",
        "加 期初现金余额",
        "期初现金余额",
    ),
    "ending_cash_balance": (
        "期末现金及现金等价物余额",
        "六、期末现金及现金等价物余额",
        "五、期末现金余额",
        "期末现金余额",
    ),
}

SUBTOTAL_COMPONENTS = {
    "operating_cash_inflow_total": (
        "cash_received_from_sales",
        "tax_refund_received",
        "other_cash_received_related_to_operating",
    ),
    "operating_cash_outflow_total": (
        "cash_paid_for_goods_services",
        "cash_paid_to_employees",
        "taxes_paid",
        "other_cash_paid_related_to_operating",
    ),
    "investing_cash_inflow_total": (
        "cash_received_from_investment_recovery",
        "investment_income_cash_received",
        "cash_received_from_disposal_assets",
        "cash_received_from_disposal_subsidiaries",
        "other_cash_received_related_to_investing",
    ),
    "investing_cash_outflow_total": (
        "cash_paid_for_fixed_intangible_assets",
        "cash_paid_for_investments",
        "cash_paid_for_acquisition_subsidiaries",
        "other_cash_paid_related_to_investing",
    ),
    "financing_cash_inflow_total": (
        "cash_received_from_investors",
        "cash_received_from_borrowings",
        "other_cash_received_related_to_financing",
    ),
    "financing_cash_outflow_total": (
        "cash_paid_for_debt_repayment",
        "cash_paid_for_dividends_profit_interest",
        "other_cash_paid_related_to_financing",
    ),
}

SMALL_BUSINESS_MARKERS = (
    "小企业会计准则",
    "适用执行小企业会计准则的企业",
    "销售产成品、商品、提供劳务收到的现金",
    "购买原材料、商品、接受劳务支付的现金",
)

SMALL_BUSINESS_DISPLAY_LABELS = {
    "cash_received_from_disposal_assets": "处置固定资产、无形资产和其他非流动资产收回的现金净额",
    "cash_paid_for_fixed_intangible_assets": "购建固定资产、无形资产和其他非流动资产支付的现金",
    "cash_received_from_investors": "取得投资者投资收到的现金",
    "cash_paid_for_debt_repayment": "偿还借款本金支付的现金",
}
ORIGINAL_CASH_BALANCE_LABEL_FIELDS = {"net_cash_increase", "beginning_cash_balance", "ending_cash_balance"}


def _cash_flow_template(pages: list[dict[str, Any]]) -> str:
    source = normalize_label("\n".join(str(page.get("text") or "") for page in pages))
    if any(normalize_label(marker) in source for marker in SMALL_BUSINESS_MARKERS):
        return "small_business_cash_flow"
    return "enterprise_accounting_cash_flow"


def _decorate_original_rows(values: dict[str, AmountField], template_type: str) -> None:
    for field, item in values.items():
        item.template_type = template_type
        if item.normalized_value is None or not item.source_text:
            continue
        source_text = normalize_label(item.source_text)
        original_label = next(
            (
                label for label in sorted(CASH_FLOW_FIELDS[field], key=len, reverse=True)
                if normalize_label(label) in source_text
            ),
            CASH_FLOW_FIELDS[field][0],
        )
        item.original_label = original_label
        if template_type == "small_business_cash_flow" and field in ORIGINAL_CASH_BALANCE_LABEL_FIELDS:
            item.display_label = original_label.removeprefix("加：").removeprefix("加:")
        elif template_type == "small_business_cash_flow":
            item.display_label = SMALL_BUSINESS_DISPLAY_LABELS.get(field, CASH_FLOW_FIELDS[field][0])
        else:
            item.display_label = CASH_FLOW_FIELDS[field][0]
        item.original_present = True
        item.calculated = False


def _calculated_subtotal(
    values: dict[str, AmountField],
    components: tuple[str, ...],
    template_type: str,
) -> AmountField | None:
    items = [values.get(field) or AmountField() for field in components]
    current_values = [item.normalized_value for item in items if item.normalized_value is not None]
    previous_values = [item.previous_normalized_value for item in items if item.previous_normalized_value is not None]
    current = round(sum(current_values), 2) if current_values else None
    previous = round(sum(previous_values), 2) if previous_values else None
    if current in (None, 0) and previous in (None, 0):
        return None
    source_page = next((item.source_page for item in items if item.source_page is not None), None)
    return AmountField(
        raw_value=f"{current:,.2f}" if current is not None else "",
        normalized_value=current,
        previous_raw_value=f"{previous:,.2f}" if previous is not None else "",
        previous_normalized_value=previous,
        current_value=current,
        compare_value=previous,
        current_column_label="本期金额",
        previous_column_label="上期金额",
        source_page=source_page,
        source_text="由现金流量明细项计算得出",
        confidence=0.90,
        display_label="",
        original_present=False,
        calculated=True,
        template_type=template_type,
    )


def extract_cash_flow_statement(
    pages: list[dict[str, Any]], source_file: str, multiplier: Decimal
) -> tuple[CashFlowStatement, list[EvidenceItem]]:
    values, evidence = extract_amount_fields(
        pages=pages,
        mapping=CASH_FLOW_FIELDS,
        table_name="现金流量表",
        field_prefix="cash_flow_statement",
        source_file=source_file,
        multiplier=multiplier,
        current_column_label="本期金额",
        previous_column_label="上期金额",
        prefer_last_amounts=True,
    )
    template_type = _cash_flow_template(pages)
    _decorate_original_rows(values, template_type)
    for subtotal_field, components in SUBTOTAL_COMPONENTS.items():
        if values[subtotal_field].normalized_value is not None:
            continue
        calculated = _calculated_subtotal(values, components, template_type)
        if calculated is None:
            continue
        values[subtotal_field] = calculated
        evidence.append(
            build_evidence(
                field_path=f"cash_flow_statement.{subtotal_field}",
                source_file=source_file,
                source_page=calculated.source_page,
                table_name="现金流量表",
                row_label=CASH_FLOW_FIELDS[subtotal_field][0],
                column_label="本期金额 / 上期金额",
                raw_text=calculated.source_text,
                confidence=calculated.confidence,
            )
        )
    return CashFlowStatement(**values), evidence
