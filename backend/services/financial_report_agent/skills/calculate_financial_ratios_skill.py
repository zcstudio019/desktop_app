from __future__ import annotations

from typing import Any

from ..normalizer import value_of
from ..schema import FinancialRatios


def _v(section: dict[str, Any], key: str) -> float | None:
    return value_of(section.get(key) or {})


def _divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return round(numerator / denominator, 6)


def _sum(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    return round(sum(present), 2) if present else None


def calculate_financial_ratios(
    balance_sheet: dict[str, Any], income_statement: dict[str, Any], cash_flow_statement: dict[str, Any],
    prior_period: dict[str, Any] | None = None,
) -> FinancialRatios:
    prior_period = prior_period or {}
    prior_balance = prior_period.get("balance_sheet") or {}
    liabilities = _v(balance_sheet, "total_liabilities")
    assets = _v(balance_sheet, "total_assets")
    equity = _v(balance_sheet, "total_equity")
    current_assets = _v(balance_sheet, "current_assets_total")
    current_liabilities = _v(balance_sheet, "current_liabilities_total")
    cash = _v(balance_sheet, "cash_and_equivalents")
    short_loans = _v(balance_sheet, "short_term_loans")
    long_loans = _v(balance_sheet, "long_term_loans")
    due_one_year = _v(balance_sheet, "non_current_liabilities_due_within_one_year")
    revenue = _v(income_statement, "revenue")
    cost = _v(income_statement, "operating_cost")
    operating_profit = _v(income_statement, "operating_profit")
    net_profit = _v(income_statement, "net_profit")
    operating_cash_flow = _v(cash_flow_statement, "net_operating_cash_flow")
    financing_cash_flow = _v(cash_flow_statement, "net_financing_cash_flow")
    gross_profit = (round(revenue - cost, 2) if revenue is not None and cost is not None else None)
    average_ar = _sum(_v(balance_sheet, "accounts_receivable"), _v(prior_balance, "accounts_receivable"))
    average_inventory = _sum(_v(balance_sheet, "inventory"), _v(prior_balance, "inventory"))
    average_assets = _sum(assets, _v(prior_balance, "total_assets"))
    if prior_balance:
        average_ar = average_ar / 2 if average_ar is not None else None
        average_inventory = average_inventory / 2 if average_inventory is not None else None
        average_assets = average_assets / 2 if average_assets is not None else None
    quick_assets = _sum(
        cash, _v(balance_sheet, "notes_receivable"), _v(balance_sheet, "accounts_receivable"),
        _v(balance_sheet, "trading_financial_assets"),
    )
    expenses = _sum(
        _v(income_statement, "selling_expenses"), _v(income_statement, "admin_expenses"),
        _v(income_statement, "rd_expenses"), _v(income_statement, "finance_expenses"),
    )
    interest_bearing_debt = _sum(short_loans, long_loans, due_one_year)
    return FinancialRatios(
        asset_liability_ratio=_divide(liabilities, assets),
        debt_to_equity_ratio=_divide(liabilities, equity),
        current_ratio=_divide(current_assets, current_liabilities),
        quick_ratio=_divide(quick_assets, current_liabilities),
        cash_ratio=_divide(cash, current_liabilities),
        interest_bearing_debt=interest_bearing_debt,
        short_debt_cash_coverage=_divide(cash, short_loans),
        gross_profit=gross_profit,
        gross_margin=_divide(gross_profit, revenue),
        operating_margin=_divide(operating_profit, revenue),
        net_margin=_divide(net_profit, revenue),
        expense_ratio=_divide(expenses, revenue),
        operating_cash_flow_to_revenue=_divide(operating_cash_flow, revenue),
        sales_cash_collection_ratio=_divide(_v(cash_flow_statement, "cash_received_from_sales"), revenue),
        operating_cash_flow_to_short_term_debt=_divide(operating_cash_flow, short_loans),
        financing_dependence=_divide(financing_cash_flow, abs(operating_cash_flow) if operating_cash_flow else None),
        ar_turnover=_divide(revenue, average_ar),
        inventory_turnover=_divide(cost, average_inventory),
        total_asset_turnover=_divide(revenue, average_assets),
    )
