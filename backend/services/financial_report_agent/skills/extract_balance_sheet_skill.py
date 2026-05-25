from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..schema import BalanceSheet, EvidenceItem
from ._common import extract_amount_fields


BALANCE_FIELDS = {
    "cash_and_equivalents": ("货币资金",),
    "trading_financial_assets": ("交易性金融资产",),
    "notes_receivable": ("应收票据",),
    "accounts_receivable": ("应收账款",),
    "receivables_financing": ("应收款项融资",),
    "prepayments": ("预付款项", "预付账款"),
    "other_receivables": ("其他应收款",),
    "inventory": ("存货",),
    "current_assets_total": ("流动资产合计", "流动资产总计"),
    "long_term_equity_investment": ("长期股权投资",),
    "fixed_assets": ("固定资产",),
    "construction_in_progress": ("在建工程",),
    "intangible_assets": ("无形资产",),
    "long_term_prepaid_expenses": ("长期待摊费用",),
    "non_current_assets_total": ("非流动资产合计", "非流动资产总计"),
    "total_assets": ("资产总计", "资产合计"),
    "short_term_loans": ("短期借款",),
    "notes_payable": ("应付票据",),
    "accounts_payable": ("应付账款",),
    "advance_receipts": ("预收款项", "预收账款"),
    "contract_liabilities": ("合同负债",),
    "employee_benefits_payable": ("应付职工薪酬",),
    "taxes_payable": ("应交税费",),
    "other_payables": ("其他应付款",),
    "current_liabilities_total": ("流动负债合计", "流动负债总计"),
    "non_current_liabilities_due_within_one_year": ("一年内到期的非流动负债",),
    "long_term_loans": ("长期借款",),
    "non_current_liabilities_total": ("非流动负债合计", "非流动负债总计"),
    "total_liabilities": ("负债合计", "负债总计"),
    "paid_in_capital": ("实收资本", "股本"),
    "capital_reserve": ("资本公积",),
    "surplus_reserve": ("盈余公积",),
    "undistributed_profit": ("未分配利润",),
    "total_equity": ("所有者权益合计", "股东权益合计"),
    "total_liabilities_and_equity": ("负债和所有者权益总计", "负债及所有者权益总计", "负债和股东权益总计"),
}


def extract_balance_sheet(
    pages: list[dict[str, Any]], source_file: str, multiplier: Decimal
) -> tuple[BalanceSheet, list[EvidenceItem]]:
    values, evidence = extract_amount_fields(
        pages=pages,
        mapping=BALANCE_FIELDS,
        table_name="资产负债表",
        field_prefix="balance_sheet",
        source_file=source_file,
        multiplier=multiplier,
    )
    return BalanceSheet(**values), evidence
