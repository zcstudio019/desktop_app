from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.credit_report_agent.orchestrator import extract_enterprise_credit_report_agent


FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "credit_report_cases"


def main() -> int:
    parser = argparse.ArgumentParser(description="Test enterprise credit report Agent extraction.")
    parser.add_argument("path", nargs="?", help="Path to report PDF/text file.")
    parser.add_argument("--case", dest="case_name", default="", help="Run a fixture case from tests/fixtures/credit_report_cases.")
    args = parser.parse_args()

    expected: dict[str, Any] = {}
    if args.case_name:
        case_dir = FIXTURE_ROOT / args.case_name
        input_path = case_dir / "input_text.txt"
        expected_path = case_dir / "expected_assertions.json"
        if not input_path.exists():
            print(f"Fixture input not found: {input_path}")
            return 2
        raw_text = input_path.read_text(encoding="utf-8")
        expected = json.loads(expected_path.read_text(encoding="utf-8")) if expected_path.exists() else {}
        result = extract_enterprise_credit_report_agent(raw_text=raw_text, customer_id=args.case_name)
    else:
        if not args.path:
            print("Usage: python scripts/test_credit_report_agent.py path/to/report.pdf")
            print("   or: python scripts/test_credit_report_agent.py --case case_finance_lease_and_webank_short_loan")
            return 2
        path = Path(args.path).resolve()
        result = extract_enterprise_credit_report_agent(file_path=str(path), customer_id="debug")

    out_path = ROOT / "data" / "debug" / "credit_report_agent_result.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = result.get("credit_summary") or {}
    validation = result.get("validation") or {}
    short_loans = result.get("short_term_loans") or []
    medium_loans = result.get("medium_long_term_loans") or []
    revolving_overdrafts = result.get("revolving_overdrafts") or []
    credit_lines = result.get("credit_lines") or []
    finance_like = [x for x in medium_loans if "融资" in (x.get("business_type") or "")]

    print("Enterprise credit agent result")
    print(f"- customer: {(result.get('report_meta') or {}).get('customer_name') or '未识别'}")
    print(f"- short loans: {len(short_loans)}, balance={summary.get('short_term_loan_balance')}")
    print(f"- medium/long loans: {len(medium_loans)}, balance={summary.get('medium_long_term_loan_balance')}")
    print(f"- revolving overdrafts: {len(revolving_overdrafts)}, balance={summary.get('revolving_overdraft_balance')}")
    print(f"- finance leases: {len(finance_like)}")
    print(f"- credit lines: {len(result.get('credit_lines') or [])}")
    print(f"- bills: {len(result.get('bills') or [])}")
    print(f"- letters of credit: {len(result.get('letters_of_credit') or [])}")
    print(f"- guarantees: {len(result.get('guarantees') or [])}")
    print(f"- has 浙江网商银行: {_has_webank(short_loans)}")
    print(f"- short contains finance lease: {_short_contains_forbidden(short_loans, ['融资型租赁', '融资租赁'])}")
    print(f"- validation errors: {validation.get('errors') or []}")
    print(f"- validation warnings: {validation.get('warnings') or []}")
    print(f"- json: {out_path}")

    failures = _assert_expected(result, expected)
    if failures:
        print("Assertions: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1
    if expected:
        print("Assertions: PASS")
    return 0


def _assert_expected(result: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    if not expected:
        return []
    failures: list[str] = []
    short_loans = result.get("short_term_loans") or []
    medium_loans = result.get("medium_long_term_loans") or []
    revolving_overdrafts = result.get("revolving_overdrafts") or []
    credit_lines = result.get("credit_lines") or []
    summary = result.get("credit_summary") or {}
    validation = result.get("validation") or {}
    all_loans = [*short_loans, *medium_loans]

    for target in expected.get("short_term_must_include") or []:
        if not _loan_matches(short_loans, target):
            failures.append(f"short_term_must_include missing: {target}")
    for target in expected.get("short_term_must_not_include") or []:
        if _loan_matches(short_loans, target):
            failures.append(f"short_term_must_not_include leaked: {target}")

    for keyword in expected.get("short_term_must_not_include_business_keywords") or []:
        if any(keyword in (loan.get("business_type") or "") for loan in short_loans):
            failures.append(f"short term contains forbidden business keyword: {keyword}")

    for target in expected.get("medium_long_or_lease_must_include") or []:
        if not _loan_matches(medium_loans, target):
            failures.append(f"medium_long_or_lease_must_include missing: {target}")

    forbidden_names = set(expected.get("invalid_institution_names_forbidden") or [])
    for loan in all_loans:
        if (loan.get("institution_name") or "") in forbidden_names:
            failures.append(f"invalid institution leaked: {loan.get('institution_name')}")
    expected_company = expected.get("company_name_must_equal")
    if expected_company and (result.get("report_meta") or {}).get("customer_name") != expected_company:
        failures.append(f"company_name mismatch: {(result.get('report_meta') or {}).get('customer_name')!r}")
    expected_revolving = expected.get("revolving_balance_must_equal")
    if expected_revolving is not None:
        try:
            if abs(float(summary.get("revolving_overdraft_balance") or 0) - float(expected_revolving)) > 0.01:
                failures.append(f"revolving_balance mismatch: {summary.get('revolving_overdraft_balance')!r}")
        except Exception:
            failures.append(f"revolving_balance not numeric: {summary.get('revolving_overdraft_balance')!r}")
        if not revolving_overdrafts and "revolving_balance_without_details" not in (validation.get("warnings") or []):
            failures.append("revolving balance positive but no details/warning")
    for target in expected.get("revolving_overdrafts_must_include") or []:
        if not _loan_matches(revolving_overdrafts, target):
            failures.append(f"revolving_overdrafts_must_include missing: {target}")
    expected_credit_count = expected.get("credit_lines_count")
    if expected_credit_count is not None and len(credit_lines) != int(expected_credit_count):
        failures.append(f"credit_lines_count mismatch: {len(credit_lines)} != {expected_credit_count}")
    for target in expected.get("credit_lines_must_include") or []:
        if not _credit_line_matches(credit_lines, target):
            failures.append(f"credit_lines_must_include missing: {target}")
    for target in expected.get("credit_lines_must_not_enter_short_term") or []:
        if _loan_matches(short_loans, target):
            failures.append(f"credit line leaked into short_term_loans: {target}")
    for warning in expected.get("must_not_have_warnings") or []:
        if warning in (validation.get("warnings") or []):
            failures.append(f"unexpected validation warning: {warning}")
    return failures


def _credit_line_matches(items: list[dict[str, Any]], target: dict[str, Any]) -> bool:
    for item in items:
        ok = True
        for key, expected_value in target.items():
            if key == "expiry_date":
                value = item.get("expiry_date") or item.get("due_date") or ""
            else:
                value = item.get(key)
            if isinstance(expected_value, (int, float)):
                try:
                    ok = ok and abs(float(value) - float(expected_value)) < 0.001
                except Exception:
                    ok = False
            elif isinstance(expected_value, bool):
                ok = ok and bool(value) is expected_value
            else:
                ok = ok and str(expected_value) in str(value)
        if ok:
            return True
    return False


def _loan_matches(loans: list[dict[str, Any]], target: dict[str, Any]) -> bool:
    for loan in loans:
        ok = True
        for key, expected_value in target.items():
            value = loan.get(key)
            if isinstance(expected_value, (int, float)):
                try:
                    ok = ok and abs(float(value) - float(expected_value)) < 0.001
                except Exception:
                    ok = False
            else:
                ok = ok and str(expected_value) in str(value)
        if ok:
            return True
    return False


def _has_webank(short_loans: list[dict[str, Any]]) -> bool:
    return any(
        "浙江网商银行股份有限公司" in (loan.get("institution_name") or "")
        and loan.get("business_type") == "流动资金贷款"
        and float(loan.get("loan_amount") or 0) == 30
        and float(loan.get("balance") or 0) == 5
        for loan in short_loans
    )


def _short_contains_forbidden(short_loans: list[dict[str, Any]], keywords: list[str]) -> bool:
    return any(any(keyword in (loan.get("business_type") or "") for keyword in keywords) for loan in short_loans)


if __name__ == "__main__":
    raise SystemExit(main())
