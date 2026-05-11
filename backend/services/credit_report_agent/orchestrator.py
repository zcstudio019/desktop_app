from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .evidence import save_debug_artifacts
from .extractors import (
    extract_basic_info,
    extract_bills,
    extract_credit_lines,
    extract_credit_summary,
    extract_external_guarantees,
    extract_guarantees,
    extract_letters_of_credit,
    extract_medium_long_term_loans,
    extract_revolving_overdrafts,
    extract_short_term_loans,
)
from .normalizer import normalize_agent_result
from .schemas import AgentResult, Confidence
from .segmenter import segment_report
from .validators import validate_agent_result

logger = logging.getLogger(__name__)


def _read_text_from_file(file_path: str | None) -> str:
    if not file_path:
        return ""
    path = Path(file_path)
    if not path.exists():
        return ""
    if path.suffix.lower() == ".pdf":
        # The production upload pipeline normally passes raw_text in, so this is
        # a lightweight local-test helper. Prefer optional text-layer readers when
        # available and fail soft for scanned PDFs.
        try:
            import fitz  # type: ignore

            with fitz.open(str(path)) as doc:
                return "\n".join(page.get_text("text") for page in doc)
        except Exception:
            pass
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            pass
        try:
            from PyPDF2 import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            logger.warning("[CreditReportAgent] no pdf text reader available or pdf has no text layer")
            return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        logger.exception("[CreditReportAgent] failed to read text file")
        return ""


def _section_confidence(section: str, count: int) -> float:
    if not section:
        return 0.0
    if count:
        return 0.82
    return 0.35


def extract_enterprise_credit_report_agent(
    file_path: str | None = None,
    raw_text: str | None = None,
    customer_id: str | None = None,
) -> dict[str, Any]:
    text = raw_text or _read_text_from_file(file_path)
    sections = segment_report(text)
    expected_counts = sections.get("expected_counts") if isinstance(sections.get("expected_counts"), dict) else {}

    result = AgentResult()
    result.report_meta = extract_basic_info(sections)
    result.credit_summary = extract_credit_summary(sections)
    result.short_term_loans = extract_short_term_loans(sections)
    result.medium_long_term_loans = extract_medium_long_term_loans(sections)
    result.revolving_overdrafts = extract_revolving_overdrafts(sections)
    result.credit_lines = extract_credit_lines(sections)
    result.bills = extract_bills(sections)
    result.letters_of_credit = extract_letters_of_credit(sections)
    result.guarantees = extract_guarantees(sections)
    result.external_guarantees = extract_external_guarantees(sections)
    result = normalize_agent_result(result)
    result.raw_evidence_map = {
        key: str(value)[:3000]
        for key, value in sections.items()
        if key != "full_text" and isinstance(value, str) and value
    }
    result.validation = validate_agent_result(result, expected_counts)

    by_section = {
        "basic_info": _section_confidence(str(sections.get("basic_info") or ""), 1 if result.report_meta.customer_name else 0),
        "short_term_loans": _section_confidence(str(sections.get("short_term_loans") or ""), len(result.short_term_loans)),
        "medium_long_term_loans": _section_confidence(str(sections.get("medium_long_term_loans") or ""), len(result.medium_long_term_loans)),
        "revolving_overdrafts": _section_confidence(str(sections.get("revolving_overdrafts") or sections.get("revolving_overdraft") or ""), len(result.revolving_overdrafts)),
        "credit_lines": _section_confidence(str(sections.get("credit_lines") or ""), len(result.credit_lines)),
        "bills": _section_confidence(str(sections.get("bills") or ""), len(result.bills) + len(result.letters_of_credit)),
        "guarantees": _section_confidence(str(sections.get("guarantees") or ""), len(result.guarantees)),
    }
    overall = round(sum(by_section.values()) / max(1, len(by_section)), 2)
    result.confidence = Confidence(overall=overall, by_section=by_section)
    payload = {
        "sections": {
            key: str(value)[:5000]
            for key, value in sections.items()
            if isinstance(value, str)
        },
        "result": result.to_dict(),
    }
    debug_path = save_debug_artifacts(customer_id=customer_id, payload=payload)
    if debug_path:
        result.debug["debug_path"] = debug_path
    return result.to_dict()
