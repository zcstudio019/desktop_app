from __future__ import annotations

from decimal import Decimal
import re
from typing import Any

from ..evidence import build_evidence
from ..normalizer import current_and_previous_amounts, first_current_amount
from ..schema import AmountField, EvidenceItem


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _normalized_with_positions(value: str) -> tuple[str, list[int]]:
    normalized: list[str] = []
    positions: list[int] = []
    for index, character in enumerate(str(value or "")):
        if character.isspace() or character in ":：":
            continue
        normalized.append("(" if character == "（" else ")" if character == "）" else character)
        positions.append(index + 1)
    return "".join(normalized), positions


def normalize_label(value: str) -> str:
    normalized, _ = _normalized_with_positions(value)
    return normalized


def _matched_label(
    line: str,
    labels: tuple[str, ...],
    allowed_preceding_chinese: set[str] | None = None,
) -> tuple[str, int] | None:
    permitted_prefixes = allowed_preceding_chinese or set()
    for label in sorted(labels, key=len, reverse=True):
        normalized_line, positions = _normalized_with_positions(line)
        normalized_label = normalize_label(label)
        match = re.search(re.escape(normalized_label), normalized_line)
        if not match:
            continue
        original_start = positions[match.start() - 1] if match.start() else 0
        original_end = positions[match.end() - 1]
        preceding_text = str(line or "")[:original_start].rstrip()
        preceding = preceding_text[-1:] if preceding_text else ""
        if not (preceding and "\u4e00" <= preceding <= "\u9fff") or preceding in permitted_prefixes:
            return label, original_end
    return None


def extract_amount_fields(
    *,
    pages: list[dict[str, Any]],
    mapping: dict[str, tuple[str, ...]],
    table_name: str,
    field_prefix: str,
    source_file: str,
    multiplier: Decimal,
    current_column_label: str = "期末余额/本期金额",
    previous_column_label: str = "上年年末余额/上期金额",
    allowed_preceding_chinese: set[str] | None = None,
    prefer_last_amounts: bool = False,
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
                candidates = [line]
                # Long labels are sometimes split over multiple layout/OCR lines.
                candidates.extend(
                    " ".join(item.strip() for item in raw_lines[line_index:line_index + size])
                    for size in (2, 3)
                    if line_index + size <= len(raw_lines)
                )
                match_info = None
                candidate = line
                for possible in candidates:
                    if possible != line:
                        normalized_first_line = normalize_label(line)
                        if not any(normalize_label(label).startswith(normalized_first_line) for label in labels):
                            continue
                    match_info = _matched_label(possible, labels, allowed_preceding_chinese)
                    if match_info:
                        candidate = possible
                        break
                if not match_info:
                    continue
                matched_label, _ = match_info
                # PDF layout text may put the row label and its row number/value on adjacent lines.
                if first_current_amount(candidate, multiplier)[1] is None:
                    candidate_lines = [candidate]
                    for adjacent in raw_lines[line_index + len(candidate.splitlines()):line_index + 4]:
                        if any(
                            _matched_label(adjacent, (other,), allowed_preceding_chinese)
                            for other in all_labels
                            if other != matched_label
                        ):
                            break
                        candidate_lines.append(adjacent.strip())
                    candidate = " ".join(candidate_lines)
                candidate_match = _matched_label(candidate, (matched_label,), allowed_preceding_chinese)
                label_end = candidate_match[1] if candidate_match else 0
                remainder = candidate[label_end:]
                raw_value, normalized, previous_raw_value, previous_normalized = current_and_previous_amounts(
                    remainder,
                    multiplier,
                    prefer_last_amounts=prefer_last_amounts,
                )
                if normalized is None:
                    continue
                confidence = 0.96 if table_name in _compact(str(page.get("text") or "")) else 0.88
                found = AmountField(
                    raw_value=raw_value,
                    normalized_value=normalized,
                    previous_raw_value=previous_raw_value,
                    previous_normalized_value=previous_normalized,
                    current_value=normalized,
                    compare_value=previous_normalized,
                    current_column_label=current_column_label,
                    previous_column_label=previous_column_label,
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
                        column_label=f"{current_column_label} / {previous_column_label}",
                        raw_text=candidate,
                        confidence=confidence,
                    )
                )
                break
            if found.normalized_value is not None:
                break
        values[field] = found
    return values, evidence
