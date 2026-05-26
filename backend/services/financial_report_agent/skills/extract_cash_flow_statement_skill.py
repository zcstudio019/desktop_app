from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..schema import CashFlowStatement, EvidenceItem
from ._common import extract_amount_fields


CASH_FLOW_FIELDS = {
    "cash_received_from_sales": ("销售商品、提供劳务收到的现金", "销售商品提供劳务收到的现金"),
    "tax_refund_received": ("收到的税费返还",),
    "other_cash_received_related_to_operating": ("收到其他与经营活动有关的现金", "收到的其他与经营活动有关的现金"),
    "operating_cash_inflow_total": ("经营活动现金流入小计",),
    "cash_paid_for_goods_services": ("购买商品、接受劳务支付的现金", "购买商品接受劳务支付的现金"),
    "cash_paid_to_employees": ("支付给职工以及为职工支付的现金",),
    "taxes_paid": ("支付的各项税费",),
    "other_cash_paid_related_to_operating": ("支付其他与经营活动有关的现金", "支付的其他与经营活动有关的现金"),
    "operating_cash_outflow_total": ("经营活动现金流出小计",),
    "net_operating_cash_flow": ("经营活动产生的现金流量净额",),
    "cash_received_from_investment_recovery": ("收回投资收到的现金",),
    "investment_income_cash_received": ("取得投资收益收到的现金",),
    "cash_received_from_disposal_assets": (
        "处置固定资产、无形资产和其他长期资产收回的现金净额",
        "处置固定资产、无形资产和其他长期资产而收回的现金净额",
    ),
    "cash_received_from_disposal_subsidiaries": ("处置子公司及其他营业单位收到的现金净额",),
    "other_cash_received_related_to_investing": ("收到其他与投资活动有关的现金", "收到的其他与投资活动有关的现金"),
    "investing_cash_inflow_total": ("投资活动现金流入小计",),
    "cash_paid_for_fixed_intangible_assets": (
        "购建固定资产、无形资产和其他长期资产支付的现金",
        "购建固定资产、无形资产和其他长期资产所支付的现金",
    ),
    "cash_paid_for_investments": ("投资支付的现金",),
    "cash_paid_for_acquisition_subsidiaries": ("取得子公司及其他营业单位支付的现金净额",),
    "other_cash_paid_related_to_investing": ("支付其他与投资活动有关的现金", "支付的其他与投资活动有关的现金"),
    "investing_cash_outflow_total": ("投资活动现金流出小计",),
    "net_investing_cash_flow": ("投资活动产生的现金流量净额",),
    "cash_received_from_investors": ("吸收投资收到的现金",),
    "cash_received_from_borrowings": ("取得借款收到的现金",),
    "other_cash_received_related_to_financing": ("收到其他与筹资活动有关的现金", "收到的其他与筹资活动有关的现金"),
    "financing_cash_inflow_total": ("筹资活动现金流入小计",),
    "cash_paid_for_debt_repayment": ("偿还债务支付的现金",),
    "cash_paid_for_dividends_profit_interest": (
        "分配股利、利润或偿付利息支付的现金",
        "分配股利、利润和偿付利息支付的现金",
    ),
    "other_cash_paid_related_to_financing": ("支付其他与筹资活动有关的现金", "支付的其他与筹资活动有关的现金"),
    "financing_cash_outflow_total": ("筹资活动现金流出小计",),
    "net_financing_cash_flow": ("筹资活动产生的现金流量净额",),
    "effect_of_exchange_rate_changes": ("汇率变动对现金及现金等价物的影响",),
    "net_cash_increase": ("现金及现金等价物净增加额", "现金及现金等价物净增加额（减少以“-”号填列）"),
    "beginning_cash_balance": ("期初现金及现金等价物余额", "加：期初现金及现金等价物余额"),
    "ending_cash_balance": ("期末现金及现金等价物余额", "六、期末现金及现金等价物余额"),
}


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
    )
    return CashFlowStatement(**values), evidence
