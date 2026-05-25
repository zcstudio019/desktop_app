from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..schema import EquityChangeStatement, EvidenceItem
from ._common import extract_amount_fields


EQUITY_FIELDS = {
    "beginning_equity": ("本年年初余额", "上年年末余额"),
    "owner_contributions": ("所有者投入资本", "股东投入资本"),
    "profit_distribution": ("利润分配",),
    "comprehensive_income": ("综合收益总额",),
    "ending_equity": ("本年年末余额", "本期期末余额"),
}


def extract_equity_change_statement(
    pages: list[dict[str, Any]], source_file: str, multiplier: Decimal
) -> tuple[EquityChangeStatement, list[EvidenceItem]]:
    values, evidence = extract_amount_fields(
        pages=pages,
        mapping=EQUITY_FIELDS,
        table_name="所有者权益变动表",
        field_prefix="equity_change_statement",
        source_file=source_file,
        multiplier=multiplier,
    )
    return EquityChangeStatement(**values), evidence
