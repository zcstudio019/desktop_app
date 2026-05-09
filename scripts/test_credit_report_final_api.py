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
    CREDIT_PARSER_VERSION,
    _build_markdown_summary_v2,
    final_normalize_credit_result,
)


FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "credit_report_cases"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate final enterprise-credit payload used by profile markdown.")
    parser.add_argument(
        "--case",
        default="case_finance_lease_and_webank_short_loan",
        help="Fixture case under tests/fixtures/credit_report_cases.",
    )
    args = parser.parse_args()

    case_dir = FIXTURE_ROOT / args.case
    raw_path = case_dir / "input_text.txt"
    if not raw_path.exists():
        print(f"Fixture not found: {raw_path}")
        return 2

    raw_text = raw_path.read_text(encoding="utf-8")
    stale_result = _build_stale_profile_result()
    normalized = final_normalize_credit_result(
        stale_result,
        raw_text=raw_text,
        parser_path="final_api_test_cached_result",
    )
    markdown = _build_markdown_summary_v2(normalized)

    out_path = ROOT / "data" / "debug" / "credit_report_final_api_result.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps({"extracted_json": normalized, "markdown_summary": markdown}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    failures = _assert_final_payload(normalized)
    print("Enterprise credit final API payload test")
    print(f"- parser_version: {normalized.get('credit_parser_version')}")
    print(f"- parser_debug: {normalized.get('credit_parser_debug')}")
    print(f"- short_term_count: {len(normalized.get('short_loans_final') or [])}")
    print(f"- medium_long_term_count: {len(normalized.get('medium_loans_final') or [])}")
    print(f"- result_json: {out_path}")

    if failures:
        print("Assertions: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Assertions: PASS")
    return 0


def _build_stale_profile_result() -> dict[str, Any]:
    return {
        "schema_version": "enterprise_credit.v2",
        "credit_summary": {
            "short_term_loan_balance": "1529",
            "medium_long_term_loan_balance": "327.50",
            "medium_long_loan_count": 1,
            "short_loan_count": 6,
        },
        "short_loans": [
            {
                "bank": "公司",
                "institution": "公司",
                "institution_name": "公司",
                "biz_type": "融资型租赁",
                "loan_type": "融资型租赁",
                "business_type": "融资型租赁",
                "loan_amount": "400",
                "balance": "327.50",
                "open_date": "2025-11-12",
                "start_date": "2025-11-12",
                "due_date": "2028-11-10",
                "end_date": "2028-11-10",
                "five_classification": "正常",
                "overdue_months": "0",
                "evidence_text": "公司 融资型租赁 2025-11-12 2028-11-10 人民币元 400 保证 327.50 正常 0 0 0",
            }
        ],
        "short_loans_final": [
            {
                "bank": "公司",
                "institution": "公司",
                "institution_name": "公司",
                "biz_type": "融资型租赁",
                "loan_type": "融资型租赁",
                "business_type": "融资型租赁",
                "loan_amount": "400",
                "balance": "327.50",
                "open_date": "2025-11-12",
                "start_date": "2025-11-12",
                "due_date": "2028-11-10",
                "end_date": "2028-11-10",
                "five_classification": "正常",
                "overdue_months": "0",
            }
        ],
        "medium_loans": [],
        "medium_loans_final": [],
        "active_loans": [],
    }


def _assert_final_payload(result: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    short_loans = result.get("short_loans_final") or result.get("short_loans") or []
    medium_loans = result.get("medium_loans_final") or result.get("medium_loans") or []

    if result.get("credit_parser_version") != CREDIT_PARSER_VERSION:
        failures.append("parser_version is not latest")
    if any("融资" in _biz(x) or "铻嶈祫" in _biz(x) for x in short_loans):
        failures.append("short_term_loans still contains finance lease")
    if any((_bank(x) or "").strip() == "公司" for x in [*short_loans, *medium_loans]):
        failures.append("invalid institution_name=公司 leaked")
    if not any(
        "融资型租赁" in _biz(x)
        and _same_number(x.get("loan_amount"), "400")
        and _same_number(x.get("balance"), "327.50")
        for x in medium_loans
    ):
        failures.append("medium_long_term_loans missing finance lease 400/327.50")
    if not any(
        _bank(x) == "浙江网商银行股份有限公司"
        and "流动资金贷款" in _biz(x)
        and _same_number(x.get("loan_amount"), "30")
        and _same_number(x.get("balance"), "5")
        and (x.get("guarantee") or x.get("guarantee_type")) == "保证"
        for x in short_loans
    ):
        failures.append("short_term_loans missing 浙江网商银行 30/5")
    return failures


def _bank(item: dict[str, Any]) -> str:
    return str(item.get("institution_name") or item.get("institution") or item.get("bank") or "")


def _biz(item: dict[str, Any]) -> str:
    return str(item.get("business_type") or item.get("biz_type") or item.get("loan_type") or "")


def _same_number(left: Any, right: Any) -> bool:
    try:
        return abs(float(str(left).replace(",", "")) - float(str(right).replace(",", ""))) < 0.01
    except Exception:
        return str(left) == str(right)


if __name__ == "__main__":
    raise SystemExit(main())
