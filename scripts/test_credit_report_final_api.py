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
    safe_parse_credit_limits_from_raw,
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
    expected_path = case_dir / "expected_assertions.json"
    expected = json.loads(expected_path.read_text(encoding="utf-8")) if expected_path.exists() else {}
    stale_result = _build_stale_profile_result(args.case)
    if (expected or {}).get("credit_lines_must_include"):
        _, _, credit_facilities = safe_parse_credit_limits_from_raw(raw_text)
        stale_result["credit_facilities"] = credit_facilities
        stale_result["credit_lines"] = credit_facilities
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

    failures = _assert_final_payload(normalized, expected)
    print("Enterprise credit final API payload test")
    print(f"- parser_version: {normalized.get('credit_parser_version')}")
    print(f"- parser_debug: {normalized.get('credit_parser_debug')}")
    print(f"- credit_debug: {normalized.get('credit_debug')}")
    print(f"- short_term_count: {len(normalized.get('short_loans_final') or [])}")
    print(f"- medium_long_term_count: {len(normalized.get('medium_loans_final') or [])}")
    print(f"- revolving_overdraft_count: {len(normalized.get('revolving_overdrafts') or normalized.get('revolving_loans') or [])}")
    print(f"- result_json: {out_path}")

    if failures:
        print("Assertions: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Assertions: PASS")
    return 0


def _build_stale_profile_result(case_name: str = "") -> dict[str, Any]:
    if case_name == "case_company_name_and_revolving_overdraft":
        return {
            "schema_version": "enterprise_credit.v2",
            "report_basic": {
                "company_name": "智富金融信息服务（上海",
            },
            "identity_info": {
                "company_name": "智富金融信息服务（上海",
            },
            "credit_summary": {
                "short_term_loan_balance": "0",
                "medium_long_term_loan_balance": "0",
                "revolving_overdraft_balance": "454.68",
                "revolving_overdraft_count": 1,
            },
            "short_loans": [],
            "short_loans_final": [],
            "medium_loans": [],
            "medium_loans_final": [],
            "active_loans": [],
            "revolving_loans": [],
            "revolving_overdrafts": [],
        }
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
                "bank": "华夏银行股份有限公司上海分行",
                "institution": "华夏银行股份有限公司上海分行",
                "institution_name": "华夏银行股份有限公司上海分行",
                "biz_type": "流动资金贷款",
                "loan_type": "流动资金贷款",
                "business_type": "流动资金贷款",
                "loan_amount": "292",
                "balance": "292",
                "open_date": "2024-04-15",
                "start_date": "2024-04-15",
                "due_date": "2027-04-08",
                "end_date": "2027-04-08",
                "guarantee": "组合",
                "guarantee_type": "组合",
                "five_classification": "正常",
                "overdue_months": "0",
            },
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
                "bank": "华夏银行股份有限公司上海分行",
                "institution": "华夏银行股份有限公司上海分行",
                "institution_name": "华夏银行股份有限公司上海分行",
                "biz_type": "流动资金贷款",
                "loan_type": "流动资金贷款",
                "business_type": "流动资金贷款",
                "loan_amount": "292",
                "balance": "292",
                "open_date": "2024-04-15",
                "start_date": "2024-04-15",
                "due_date": "2027-04-08",
                "end_date": "2027-04-08",
                "guarantee": "组合",
                "guarantee_type": "组合",
                "five_classification": "正常",
                "overdue_months": "0",
            },
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


def _assert_final_payload(result: dict[str, Any], expected: dict[str, Any] | None = None) -> list[str]:
    failures: list[str] = []
    short_loans = result.get("short_loans_final") or result.get("short_loans") or []
    medium_loans = result.get("medium_loans_final") or result.get("medium_loans") or []
    revolving_loans = result.get("revolving_overdrafts") or result.get("revolving_loans") or []
    credit_lines = result.get("credit_lines") or result.get("credit_facilities") or []
    validation = result.get("validation") or {}
    warnings = validation.get("warnings") or []
    summary = result.get("credit_summary") or {}
    company_name = ((result.get("report_basic") or {}).get("company_name") or (result.get("identity_info") or {}).get("company_name") or "")

    if result.get("credit_parser_version") != CREDIT_PARSER_VERSION:
        failures.append("parser_version is not latest")
    credit_debug = result.get("credit_debug") or {}
    if (expected or {}).get("revolving_overdrafts_must_include"):
        if credit_debug.get("parser_version") != "revolving-trace-v3":
            failures.append(f"credit_debug.parser_version mismatch: {credit_debug.get('parser_version')!r}")
        if int(credit_debug.get("revolving_extracted_count") or 0) < 1:
            failures.append(f"credit_debug.revolving_extracted_count < 1: {credit_debug}")
        if int(credit_debug.get("revolving_returned_count") or 0) != 1:
            failures.append(f"credit_debug.revolving_returned_count != 1: {credit_debug}")
        if len(revolving_loans) != 1:
            failures.append(f"revolving_overdrafts.length != 1: {len(revolving_loans)}")
    expected_company = (expected or {}).get("company_name_must_equal")
    if expected_company and company_name != expected_company:
        failures.append(f"company_name mismatch: {company_name!r} != {expected_company!r}")
    if company_name.endswith("（上海") or company_name in set((expected or {}).get("forbidden_company_name_values") or []):
        failures.append(f"company_name is truncated/forbidden: {company_name!r}")
    if company_name and not any(suffix in company_name for suffix in ["有限公司", "股份有限公司", "合伙企业", "分公司", "个体工商户", "鏈夐檺鍏徃"]):
        failures.append(f"company_name missing enterprise suffix: {company_name!r}")
    expected_revolving = (expected or {}).get("revolving_balance_must_equal")
    if expected_revolving is not None:
        if not _same_number(summary.get("revolving_overdraft_balance"), expected_revolving):
            failures.append(f"revolving_overdraft_balance mismatch: {summary.get('revolving_overdraft_balance')!r}")
        if not revolving_loans and "revolving_balance_without_details" not in warnings:
            failures.append("revolving balance positive but no details and no revolving_balance_without_details warning")
    if any("融资" in _biz(x) or "铻嶈祫" in _biz(x) for x in short_loans):
        failures.append("short_term_loans still contains finance lease")
    if any("华夏银行股份有限公司上海分行" in _bank(x) and (x.get("end_date") or x.get("due_date")) == "2027-04-08" for x in short_loans):
        failures.append("short_term_loans contains medium-term 华夏银行 2027-04-08")
    if any((_bank(x) or "").strip() == "公司" for x in [*short_loans, *medium_loans]):
        failures.append("invalid institution_name=公司 leaked")
    if (not expected or (expected or {}).get("medium_long_or_lease_must_include")) and not any(
        "融资型租赁" in _biz(x)
        and _same_number(x.get("loan_amount"), "400")
        and _same_number(x.get("balance"), "327.50")
        and _guarantee(x) in {"淇濊瘉", "保证"}
        for x in medium_loans
    ):
        failures.append("medium_long_term_loans missing finance lease 400/327.50 with guarantee")
    needs_webank_assert = any(
        "娴欐睙" in str(item) or "浙江" in str(item)
        for item in (expected or {}).get("short_term_must_include") or []
    )
    if (not expected or needs_webank_assert) and not any(
        _bank(x) == "浙江网商银行股份有限公司"
        and "流动资金贷款" in _biz(x)
        and _same_number(x.get("loan_amount"), "30")
        and _same_number(x.get("balance"), "5")
        and (x.get("guarantee") or x.get("guarantee_type")) == "保证"
        for x in short_loans
    ):
        failures.append("short_term_loans missing 浙江网商银行 30/5")
    for target in (expected or {}).get("short_term_must_include") or []:
        if not _loan_matches(short_loans, target):
            failures.append(f"short_term_must_include missing: {target}")
    for target in (expected or {}).get("short_term_must_not_include") or []:
        if _loan_matches(short_loans, target):
            failures.append(f"short_term_must_not_include leaked: {target}")
    for target in (expected or {}).get("medium_long_or_lease_must_include") or []:
        if not _loan_matches(medium_loans, target):
            failures.append(f"medium_long_or_lease_must_include missing: {target}")
    for target in (expected or {}).get("revolving_overdrafts_must_include") or []:
        if not _loan_matches(revolving_loans, target):
            failures.append(f"revolving_overdrafts_must_include missing: {target}")
    for keyword in (expected or {}).get("revolving_institution_forbidden_keywords") or []:
        if any(keyword in _bank(x) for x in revolving_loans):
            failures.append(f"revolving institution contains forbidden keyword: {keyword}")
    expected_credit_count = (expected or {}).get("credit_lines_count")
    if expected_credit_count is not None and len(credit_lines) != int(expected_credit_count):
        failures.append(f"credit_lines_count mismatch: {len(credit_lines)} != {expected_credit_count}")
    for target in (expected or {}).get("credit_lines_must_include") or []:
        if not _credit_line_matches(credit_lines, target):
            failures.append(f"credit_lines_must_include missing: {target}")
    for target in (expected or {}).get("credit_lines_must_not_enter_short_term") or []:
        if _loan_matches(short_loans, target):
            failures.append(f"credit line leaked into short_term_loans: {target}")
    for warning in (expected or {}).get("must_not_have_warnings") or []:
        if warning in warnings:
            failures.append(f"unexpected validation warning: {warning}")
    return failures


def _credit_line_matches(items: list[dict[str, Any]], target: dict[str, Any]) -> bool:
    for item in items:
        ok = True
        for key, expected_value in target.items():
            if key == "institution_name":
                value = item.get("institution_name") or item.get("institution") or item.get("bank") or ""
            elif key == "credit_revolving":
                value = item.get("credit_revolving")
                if value is None:
                    value = str(item.get("is_revolving") or "") == "是"
            elif key == "expiry_date":
                value = item.get("expiry_date") or item.get("due_date") or item.get("end_date") or ""
            else:
                value = item.get(key)
            if isinstance(expected_value, (int, float)):
                ok = ok and _same_number(value, expected_value)
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
            if key == "institution_name":
                value = _bank(loan)
            elif key == "business_type":
                value = _biz(loan)
            elif key == "guarantee_type":
                value = _guarantee(loan)
            elif key == "credit_amount":
                value = loan.get("credit_amount") or loan.get("loan_amount")
            elif key == "start_date":
                value = loan.get("start_date") or loan.get("open_date") or ""
            elif key == "end_date":
                value = loan.get("end_date") or loan.get("due_date") or ""
            else:
                value = loan.get(key)
            if isinstance(expected_value, (int, float)):
                ok = ok and _same_number(value, expected_value)
            else:
                ok = ok and str(expected_value) in str(value)
        if ok:
            return True
    return False


def _bank(item: dict[str, Any]) -> str:
    return str(item.get("institution_name") or item.get("institution") or item.get("bank") or "")


def _biz(item: dict[str, Any]) -> str:
    return str(item.get("business_type") or item.get("biz_type") or item.get("loan_type") or "")


def _guarantee(item: dict[str, Any]) -> str:
    return str(item.get("guarantee_type") or item.get("guarantee_method") or item.get("guarantee") or "")


def _same_number(left: Any, right: Any) -> bool:
    try:
        return abs(float(str(left).replace(",", "")) - float(str(right).replace(",", ""))) < 0.01
    except Exception:
        return str(left) == str(right)


if __name__ == "__main__":
    raise SystemExit(main())
