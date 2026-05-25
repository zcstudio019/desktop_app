from __future__ import annotations

from typing import Any

from .normalizer import value_of


def _v(section: dict[str, Any], field: str) -> float | None:
    return value_of(section.get(field) or {})


def _check(warnings: list[str], label: str, actual: float | None, expected: float | None, tolerance: float = 1.0) -> None:
    if actual is None or expected is None:
        return
    difference = abs(actual - expected)
    if difference > tolerance:
        warnings.append(f"{label}校验未通过：差额 {difference:,.2f} 元，已保留原始提取值。")


def validate_financial_report_result(data: dict[str, Any], tolerance: float = 1.0) -> list[str]:
    warnings: list[str] = []
    balance = data.get("balance_sheet") or {}
    income = data.get("income_statement") or {}
    cashflow = data.get("cash_flow_statement") or {}
    assets = _v(balance, "total_assets")
    liabilities = _v(balance, "total_liabilities")
    equity = _v(balance, "total_equity")
    _check(warnings, "资产总计与负债和所有者权益总计", assets, _v(balance, "total_liabilities_and_equity"), tolerance)
    if liabilities is not None and equity is not None:
        _check(warnings, "负债合计加所有者权益合计与资产总计", assets, liabilities + equity, tolerance)
    current_assets_components = [
        _v(balance, key) for key in (
            "cash_and_equivalents", "trading_financial_assets", "notes_receivable",
            "accounts_receivable", "receivables_financing", "prepayments",
            "other_receivables", "inventory",
        )
    ]
    if all(item is not None for item in current_assets_components):
        _check(warnings, "流动资产合计与核心流动资产科目", _v(balance, "current_assets_total"), sum(current_assets_components), tolerance)
    current_liability_components = [
        _v(balance, key) for key in (
            "short_term_loans", "notes_payable", "accounts_payable", "advance_receipts",
            "contract_liabilities", "employee_benefits_payable", "taxes_payable", "other_payables",
        )
    ]
    if all(item is not None for item in current_liability_components):
        _check(warnings, "流动负债合计与核心流动负债科目", _v(balance, "current_liabilities_total"), sum(current_liability_components), tolerance)
    revenue, cost = _v(income, "revenue"), _v(income, "operating_cost")
    expenses = [_v(income, key) for key in ("taxes_and_surcharges", "selling_expenses", "admin_expenses", "rd_expenses", "finance_expenses")]
    if revenue is not None and cost is not None and all(value is not None for value in expenses):
        _check(warnings, "营业利润与已提取损益明细", _v(income, "operating_profit"), revenue - cost - sum(expenses), tolerance)
    operating_profit = _v(income, "operating_profit")
    non_operating_income, non_operating_expense = _v(income, "non_operating_income"), _v(income, "non_operating_expense")
    if operating_profit is not None and non_operating_income is not None and non_operating_expense is not None:
        _check(warnings, "利润总额与已提取损益明细", _v(income, "total_profit"), operating_profit + non_operating_income - non_operating_expense, tolerance)
    total_profit, income_tax = _v(income, "total_profit"), _v(income, "income_tax_expense")
    if total_profit is not None and income_tax is not None:
        _check(warnings, "净利润与利润总额减所得税费用", _v(income, "net_profit"), total_profit - income_tax, tolerance)
    beginning, increase = _v(cashflow, "beginning_cash_balance"), _v(cashflow, "net_cash_increase")
    if beginning is not None and increase is not None:
        _check(warnings, "期末现金余额与期初余额加现金净增加额", _v(cashflow, "ending_cash_balance"), beginning + increase, tolerance)
    return warnings
