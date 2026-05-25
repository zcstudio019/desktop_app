from __future__ import annotations

from decimal import Decimal
import re
from typing import Any

from ..evidence import build_evidence
from ..normalizer import first_current_amount
from ..schema import AmountField, EvidenceItem


def extract_amount_fields(
    *,
    pages: list[dict[str, Any]],
    mapping: dict[str, tuple[str, ...]],
    table_name: str,
    field_prefix: str,
    source_file: str,
    multiplier: Decimal,
) -> tuple[dict[str, AmountField], list[EvidenceItem]]:
    values: dict[str, AmountField] = {}
    evidence: list[EvidenceItem] = []
    for field, labels in mapping.items():
        found = AmountField()
        for page in pages:
            for raw_line in str(page.get("text") or "").splitlines():
                line = raw_line.strip()
                matched_label = next(
                    (label for label in labels if re.search(rf"(?<![\u4e00-\u9fff]){re.escape(label)}", line)),
                    None,
                )
                if not matched_label:
                    continue
                match = re.search(rf"(?<![\u4e00-\u9fff]){re.escape(matched_label)}", line)
                remainder = line[match.end():] if match else ""
                raw_value, normalized = first_current_amount(remainder, multiplier)
                if normalized is None:
                    continue
                confidence = 0.96 if table_name in str(page.get("text") or "") else 0.88
                found = AmountField(
                    raw_value=raw_value,
                    normalized_value=normalized,
                    source_page=int(page.get("page") or 1),
                    source_text=line,
                    confidence=confidence,
                )
                evidence.append(
                    build_evidence(
                        field_path=f"{field_prefix}.{field}",
                        source_file=source_file,
                        source_page=found.source_page,
                        table_name=table_name,
                        row_label=matched_label,
                        column_label="期末余额/本期金额",
                        raw_text=line,
                        confidence=confidence,
                    )
                )
                break
            if found.normalized_value is not None:
                break
        values[field] = found
    return values, evidence
