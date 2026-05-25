from __future__ import annotations

from decimal import Decimal
import re
from typing import Any

from ..evidence import build_evidence
from ..normalizer import first_current_amount
from ..schema import AmountField, EvidenceItem


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _matched_label(line: str, labels: tuple[str, ...]) -> tuple[str, int] | None:
    for label in sorted(labels, key=len, reverse=True):
        pattern = r"\s*".join(re.escape(character) for character in label)
        match = re.search(pattern, str(line or ""))
        if not match:
            continue
        preceding_text = str(line or "")[:match.start()].rstrip()
        preceding = preceding_text[-1:] if preceding_text else ""
        if not (preceding and "\u4e00" <= preceding <= "\u9fff"):
            return label, match.end()
    return None


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
    all_labels = tuple(label for labels in mapping.values() for label in labels)
    for field, labels in mapping.items():
        found = AmountField()
        for page in pages:
            raw_lines = str(page.get("text") or "").splitlines()
            for line_index, raw_line in enumerate(raw_lines):
                line = raw_line.strip()
                match_info = _matched_label(line, labels)
                if not match_info:
                    continue
                matched_label, _ = match_info
                # PDF layout text may put the row label and its row number/value on adjacent lines.
                candidate_lines = [line]
                for adjacent in raw_lines[line_index + 1:line_index + 4]:
                    if any(_matched_label(adjacent, (other,)) for other in all_labels if other != matched_label):
                        break
                    candidate_lines.append(adjacent.strip())
                candidate = " ".join(candidate_lines)
                candidate_match = _matched_label(candidate, (matched_label,))
                label_end = candidate_match[1] if candidate_match else 0
                remainder = candidate[label_end:]
                raw_value, normalized = first_current_amount(remainder, multiplier)
                if normalized is None:
                    continue
                confidence = 0.96 if table_name in _compact(str(page.get("text") or "")) else 0.88
                found = AmountField(
                    raw_value=raw_value,
                    normalized_value=normalized,
                    source_page=int(page.get("page") or 1),
                    source_text=candidate,
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
                        raw_text=candidate,
                        confidence=confidence,
                    )
                )
                break
            if found.normalized_value is not None:
                break
        values[field] = found
    return values, evidence
