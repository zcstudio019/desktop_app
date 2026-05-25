from __future__ import annotations

from .schema import EvidenceItem


def build_evidence(
    *,
    field_path: str,
    source_file: str,
    source_page: int | None,
    table_name: str,
    row_label: str,
    column_label: str,
    raw_text: str,
    confidence: float,
) -> EvidenceItem:
    return EvidenceItem(
        field_path=field_path,
        source_file=source_file,
        source_page=source_page,
        table_name=table_name,
        row_label=row_label,
        column_label=column_label,
        raw_text=raw_text,
        confidence=confidence,
    )
