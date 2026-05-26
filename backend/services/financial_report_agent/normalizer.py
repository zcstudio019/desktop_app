from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any


AMOUNT_RE = re.compile(r"(?<!\d)(?:[-－—(（]?\s*)?\d[\d,，]*(?:\.\d+)?\s*[)）]?")


def detect_unit(text: str) -> tuple[str, Decimal]:
    source = re.sub(r"\s+", "", str(text or ""))
    if re.search(r"单位\s*[:：]?\s*(?:人民币)?\s*万元", source):
        return "万元", Decimal("10000")
    if re.search(r"单位\s*[:：]?\s*(?:人民币)?\s*千元", source):
        return "千元", Decimal("1000")
    return "元", Decimal("1")


def normalize_amount(value: Any, multiplier: Decimal | float = Decimal("1")) -> float | None:
    text = str(value or "").strip()
    if not text or text in {"-", "--", "—", "－"}:
        return None
    negative = text.startswith(("-", "－", "—", "(", "（")) or text.endswith((")", "）"))
    cleaned = re.sub(r"[,\s，()（）－—-]", "", text)
    try:
        amount = Decimal(cleaned) * Decimal(str(multiplier))
    except InvalidOperation:
        return None
    if negative:
        amount = -abs(amount)
    return float(amount.quantize(Decimal("0.01")))


def first_current_amount(line: str, multiplier: Decimal | float = Decimal("1")) -> tuple[str, float | None]:
    values = current_and_previous_amounts(line, multiplier)
    return values[0], values[1]


def current_and_previous_amounts(
    line: str, multiplier: Decimal | float = Decimal("1")
) -> tuple[str, float | None, str, float | None]:
    tokens = [item.group(0).strip() for item in AMOUNT_RE.finditer(str(line or ""))]
    values: list[tuple[str, float]] = []
    for token in tokens:
        normalized = normalize_amount(token, multiplier)
        if normalized is None:
            continue
        # Financial report table lines often contain a short row sequence before values.
        digits = re.sub(r"\D", "", token)
        if "." not in token and "," not in token and len(digits) <= 3:
            continue
        values.append((token, normalized))
        if len(values) == 2:
            break
    if not values:
        return ("", None, "", None)
    if len(values) == 1:
        return (values[0][0], values[0][1], "", None)
    return (values[0][0], values[0][1], values[1][0], values[1][1])


def value_of(item: Any) -> float | None:
    if isinstance(item, dict):
        value = item.get("normalized_value")
    else:
        value = getattr(item, "normalized_value", None)
    return float(value) if value is not None else None
