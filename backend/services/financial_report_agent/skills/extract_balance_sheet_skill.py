from __future__ import annotations

from decimal import Decimal
import logging
from typing import Any

from ..evidence import build_evidence
from ..normalizer import value_of
from ..schema import AmountField, BalanceSheet, EvidenceItem
from ._common import extract_amount_fields

logger = logging.getLogger(__name__)


BALANCE_FIELDS = {
    "cash_and_equivalents": ("货币资金",),
    "short_term_investments": ("短期投资",),
    "trading_financial_assets": ("交易性金融资产",),
    "notes_receivable": ("应收票据",),
    "accounts_receivable": ("应收账款",),
    "receivables_financing": ("应收款项融资",),
    "prepayments": ("预付款项", "预付账款"),
    "dividends_receivable": ("应收股利",),
    "interest_receivable": ("应收利息",),
    "other_receivables": ("其他应收款",),
    "inventory": ("存货",),
    "raw_materials": ("原材料",),
    "work_in_process": ("在产品",),
    "finished_goods": ("库存商品",),
    "revolving_materials": ("周转材料",),
    "other_current_assets": ("其他流动资产",),
    "current_assets_total": ("流动资产合计", "流动资产总计"),
    "long_term_bond_investments": ("长期债券投资",),
    "long_term_equity_investment": ("长期股权投资",),
    "fixed_assets_original_cost": ("固定资产原价",),
    "accumulated_depreciation": ("累计折旧",),
    "fixed_assets_net_value": ("固定资产账面价值", "固定资产净值"),
    "fixed_assets": ("固定资产",),
    "construction_in_progress": ("在建工程",),
    "construction_materials": ("工程物资",),
    "fixed_asset_disposal": ("固定资产清理",),
    "productive_biological_assets": ("生产性生物资产",),
    "intangible_assets": ("无形资产",),
    "development_expenditure": ("开发支出",),
    "long_term_prepaid_expenses": ("长期待摊费用",),
    "other_non_current_assets": ("其他非流动资产",),
    "non_current_assets_total": ("非流动资产合计", "非流动资产总计"),
    "total_assets": ("资产总计", "资产合计"),
    "short_term_loans": ("短期借款",),
    "notes_payable": ("应付票据",),
    "accounts_payable": ("应付账款",),
    "advance_receipts": ("预收款项", "预收账款"),
    "contract_liabilities": ("合同负债",),
    "employee_benefits_payable": ("应付职工薪酬",),
    "taxes_payable": ("应交税费",),
    "interest_payable": ("应付利息",),
    "profits_payable": ("应付利润",),
    "other_payables": ("其他应付款",),
    "other_current_liabilities": ("其他流动负债",),
    "current_liabilities_total": ("流动负债合计", "流动负债总计"),
    "non_current_liabilities_due_within_one_year": ("一年内到期的非流动负债",),
    "long_term_loans": ("长期借款",),
    "long_term_payables": ("长期应付款",),
    "deferred_income": ("递延收益",),
    "other_non_current_liabilities": ("其他非流动负债",),
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

BALANCE_SHEET_SUMMARY_FIELDS = (
    "cash_and_equivalents",
    "accounts_receivable",
    "prepayments",
    "other_receivables",
    "inventory",
    "current_assets_total",
    "long_term_equity_investment",
    "fixed_assets_original_cost",
    "fixed_assets_net_value",
    "fixed_assets",
    "intangible_assets",
    "non_current_assets_total",
    "total_assets",
    "short_term_loans",
    "accounts_payable",
    "other_payables",
    "current_liabilities_total",
    "long_term_loans",
    "long_term_payables",
    "non_current_liabilities_total",
    "total_liabilities",
    "paid_in_capital",
    "undistributed_profit",
    "total_equity",
    "total_liabilities_and_equity",
)


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
        current_column_label="期末余额",
        previous_column_label="上年年末余额",
        require_previous_amounts=True,
    )
    if value_of(values.get("total_equity") or {}) is None:
        assets = value_of(values.get("total_assets") or {})
        liabilities = value_of(values.get("total_liabilities") or {})
        if assets is not None and liabilities is not None:
            calculated = round(assets - liabilities, 2)
            source_page = values["total_assets"].source_page or values["total_liabilities"].source_page
            source_text = "由资产总计 - 负债合计计算得出"
            previous_assets = values["total_assets"].previous_normalized_value
            previous_liabilities = values["total_liabilities"].previous_normalized_value
            previous_calculated = (
                round(previous_assets - previous_liabilities, 2)
                if previous_assets is not None and previous_liabilities is not None
                else None
            )
            values["total_equity"] = AmountField(
                raw_value=f"{calculated:,.2f}",
                normalized_value=calculated,
                previous_raw_value=f"{previous_calculated:,.2f}" if previous_calculated is not None else "",
                previous_normalized_value=previous_calculated,
                current_value=calculated,
                compare_value=previous_calculated,
                current_column_label="期末余额",
                previous_column_label="上年年末余额",
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
    for field_key in BALANCE_SHEET_SUMMARY_FIELDS:
        item = values.get(field_key) or AmountField()
        logger.info(
            "[DEBUG][balance_sheet_row] %s",
            {
                "field_key": field_key,
                "original_label": BALANCE_FIELDS[field_key][0],
                "cells": item.source_text.split() if item.source_text else [],
                "row_text": item.source_text,
                "normalized_value": item.normalized_value,
                "previous_normalized_value": item.previous_normalized_value,
                "previous_raw_value": item.previous_raw_value,
                "source_page": item.source_page,
            },
        )
    return BalanceSheet(**values), evidence
