from __future__ import annotations

import re
from typing import Any


def compact_text(text: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", text or "").strip()


def raw_preview(text: str, limit: int = 240) -> str:
    text = compact_text(text).replace("\n", " ")
    return text[:limit]


def make_evidence(value: Any, evidence_text: str, confidence: float = 0.82, page: int | None = None) -> dict[str, Any]:
    return {
        "value": value or "",
        "evidence_text": compact_text(evidence_text),
        "page": page,
        "confidence": confidence,
    }


def field_confidence(value: Any, base: float = 0.82) -> float:
    return base if value not in (None, "") else 0.0


def first_match(text: str, patterns: list[str], group: int | str = 1) -> tuple[str, str]:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I | re.M)
        if match:
            value = match.group(group).strip()
            return value, match.group(0).strip()
    return "", ""


def line_after_keyword(text: str, keywords: list[str], max_len: int = 80) -> tuple[str, str]:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        for keyword in keywords:
            if keyword in line:
                value = line.split(keyword, 1)[-1].strip(" :：,，")
                if not value and index + 1 < len(lines):
                    value = lines[index + 1].strip()
                if value:
                    return value[:max_len], line
    return "", ""


def build_field_maps(text: str, extracted: dict[str, tuple[Any, str, float]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
    fields: dict[str, Any] = {}
    evidence: dict[str, Any] = {}
    confidences: dict[str, float] = {}
    for field, (value, evidence_text, confidence) in extracted.items():
        fields[field] = value
        confidences[field] = field_confidence(value, confidence)
        if value:
            evidence[field] = make_evidence(value, evidence_text or str(value), confidences[field])
    return fields, evidence, confidences
