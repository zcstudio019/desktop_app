from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..schema import EvidenceItem, IncomeStatement
from ._common import extract_amount_fields


INCOME_FIELDS = {
    "revenue": ("营业收入",),
    "operating_cost": ("营业成本",),
    "taxes_and_surcharges": ("税金及附加", "营业税金及附加"),
    "selling_expenses": ("销售费用",),
    "admin_expenses": ("管理费用",),
    "rd_expenses": ("研发费用",),
    "finance_expenses": ("财务费用",),
    "interest_expense": ("利息费用", "利息支出"),
    "interest_income": ("利息收入",),
    "other_income": ("其他收益",),
    "investment_income": ("投资收益",),
    "operating_profit": ("营业利润",),
    "non_operating_income": ("营业外收入",),
    "non_operating_expense": ("营业外支出",),
    "total_profit": ("利润总额",),
    "income_tax_expense": ("所得税费用",),
    "net_profit": ("净利润",),
    "comprehensive_income_total": ("综合收益总额",),
}


def extract_income_statement(
    pages: list[dict[str, Any]], source_file: str, multiplier: Decimal
) -> tuple[IncomeStatement, list[EvidenceItem]]:
    values, evidence = extract_amount_fields(
        pages=pages,
        mapping=INCOME_FIELDS,
        table_name="利润表",
        field_prefix="income_statement",
        source_file=source_file,
        multiplier=multiplier,
        current_column_label="本期金额",
        previous_column_label="上期金额",
    )
    return IncomeStatement(**values), evidence
