from __future__ import annotations

from decimal import Decimal
import re
from typing import Any

from ..schema import EvidenceItem, IncomeStatement
from ._common import extract_amount_fields


def normalize_income_statement_label(label: str) -> str:
    normalized = re.sub(r"\s+", "", str(label or "").strip()).replace("：", ":")
    normalized = re.sub(r"^(?:[一二三四五六七八九十]+、|\d+[.、])", "", normalized)
    normalized = re.sub(r"^(?:加|减|其中):?", "", normalized)
    normalized = re.sub(r"[（(][^）)]*(?:填列|列示)[^）)]*[）)]", "", normalized)
    return normalized


def _income_aliases(*labels: str) -> tuple[str, ...]:
    aliases = [*labels, *(normalize_income_statement_label(label) for label in labels)]
    return tuple(dict.fromkeys(alias for alias in aliases if alias))


INCOME_FIELDS = {
    "revenue": _income_aliases("营业收入"),
    "operating_cost": _income_aliases(
        "营业成本", "减营业成本", "减：营业成本", "减:营业成本", "减 ： 营业成本", "营业成本（减项）"
    ),
    "taxes_and_surcharges": _income_aliases("税金及附加", "营业税金及附加"),
    "selling_expenses": _income_aliases("销售费用"),
    "admin_expenses": _income_aliases("管理费用"),
    "rd_expenses": _income_aliases("研发费用"),
    "finance_expenses": _income_aliases("财务费用"),
    "interest_expense": _income_aliases("利息费用", "利息支出"),
    "interest_income": _income_aliases("利息收入"),
    "other_income": _income_aliases("其他收益"),
    "investment_income": _income_aliases("投资收益"),
    "operating_profit": _income_aliases("营业利润"),
    "non_operating_income": _income_aliases("营业外收入"),
    "non_operating_expense": _income_aliases("营业外支出"),
    "total_profit": _income_aliases("利润总额"),
    "income_tax_expense": _income_aliases(
        "所得税费用", "减所得税费用", "减：所得税费用", "减:所得税费用", "减 ： 所得税费用",
        "所得税", "企业所得税费用"
    ),
    "net_profit": _income_aliases("净利润"),
    "comprehensive_income_total": _income_aliases("综合收益总额"),
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
        allowed_preceding_chinese={"加", "减", "中"},
    )
    return IncomeStatement(**values), evidence
