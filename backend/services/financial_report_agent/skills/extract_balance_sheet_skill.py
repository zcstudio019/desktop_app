from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..evidence import build_evidence
from ..normalizer import value_of
from ..schema import AmountField, BalanceSheet, EvidenceItem
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
    "total_equity": (
        "所有者权益合计",
        "所有者权益（或股东权益）合计",
        "所有者权益（或股东权益)合计",
        "所有者权益（或股东权益） 合计",
        "所有者权益（或股东权益) 合计",
        "所有者权益（或股东权 益）合计",
        "所有者权益（或股东权益\n）合计",
        "所有者权益（或\n股东权益）合计",
        "所有者权益（或\n股东权 益）合计",
        "股东权益合计",
        "权益合计",
    ),
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
    if value_of(values.get("total_equity") or {}) is None:
        assets = value_of(values.get("total_assets") or {})
        liabilities = value_of(values.get("total_liabilities") or {})
        if assets is not None and liabilities is not None:
            calculated = round(assets - liabilities, 2)
            source_page = values["total_assets"].source_page or values["total_liabilities"].source_page
            source_text = "由资产总计 - 负债合计计算得出"
            values["total_equity"] = AmountField(
                raw_value=f"{calculated:,.2f}",
                normalized_value=calculated,
                source_page=source_page,
                source_text=source_text,
                confidence=0.90,
            )
            evidence.append(
                build_evidence(
                    field_path="balance_sheet.total_equity",
                    source_file=source_file,
                    source_page=source_page,
                    table_name="资产负债表",
                    row_label="所有者权益合计",
                    column_label="计算值",
                    raw_text=source_text,
                    confidence=0.90,
                )
            )
    return BalanceSheet(**values), evidence
