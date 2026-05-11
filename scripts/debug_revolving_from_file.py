from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.extraction_skills.enterprise_credit import (  # noqa: E402
    _extract_revolving_window_for_final,
    _parse_revolving_window_for_final,
    final_normalize_credit_result,
)


def _read_text_from_pdf(path: Path) -> str:
    try:
        import fitz  # type: ignore

        with fitz.open(str(path)) as doc:
            return "\n".join(page.get_text("text") for page in doc)
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            raise RuntimeError(f"failed to read PDF text layer: {exc}") from exc


def _read_input(args: argparse.Namespace) -> str:
    path = Path(args.file or args.text)
    if not path.exists():
        raise FileNotFoundError(path)
    if args.file:
        if path.suffix.lower() == ".pdf":
            return _read_text_from_pdf(path)
        return path.read_text(encoding="utf-8", errors="ignore")
    return path.read_text(encoding="utf-8", errors="ignore")


def _summary_result(raw_text: str) -> dict[str, Any]:
    section = _extract_revolving_window_for_final(raw_text)
    extracted = _parse_revolving_window_for_final(raw_text)
    normalized = final_normalize_credit_result(
        {
            "credit_summary": {"revolving_overdraft_balance": "454.68" if "454.68" in raw_text else None},
            "revolving_overdrafts": [],
            "revolving_loans": [],
            "validation": {"warnings": []},
        },
        raw_text=raw_text,
        parser_path="debug_revolving_from_file",
    )
    return {
        "raw_text_len": len(raw_text),
        "has_revolving_keyword": "循环透支" in raw_text or "寰" in raw_text,
        "revolving_keyword_count": raw_text.count("循环透支") + raw_text.count("寰幆閫忔敮"),
        "section_len": len(section),
        "section_preview": section[:1000],
        "extractor_output_count": len(extracted),
        "extractor_output": extracted,
        "final_revolving_count": len(normalized.get("revolving_overdrafts") or []),
        "final_revolving_overdrafts": normalized.get("revolving_overdrafts") or [],
        "warnings": (normalized.get("validation") or {}).get("warnings") or [],
        "credit_debug": normalized.get("credit_debug") or {},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug enterprise-credit revolving overdraft extraction from a file.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Path to report PDF or text file")
    group.add_argument("--text", help="Path to raw text file")
    args = parser.parse_args()

    raw_text = _read_input(args)
    result = _summary_result(raw_text)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
