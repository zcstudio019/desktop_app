from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.credit_report_agent.orchestrator import extract_enterprise_credit_report_agent


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_credit_report_agent.py path/to/report.pdf")
        return 2
    path = Path(sys.argv[1]).resolve()
    result = extract_enterprise_credit_report_agent(file_path=str(path), customer_id="debug")
    out_path = ROOT / "data" / "debug" / "credit_report_agent_result.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = result.get("credit_summary") or {}
    validation = result.get("validation") or {}
    print("Enterprise credit agent result")
    print(f"- customer: {(result.get('report_meta') or {}).get('customer_name') or '未识别'}")
    print(f"- short loans: {len(result.get('short_term_loans') or [])}, balance={summary.get('short_term_loan_balance')}")
    print(f"- medium/long loans: {len(result.get('medium_long_term_loans') or [])}, balance={summary.get('medium_long_term_loan_balance')}")
    print(f"- credit lines: {len(result.get('credit_lines') or [])}")
    print(f"- bills: {len(result.get('bills') or [])}")
    print(f"- letters of credit: {len(result.get('letters_of_credit') or [])}")
    print(f"- guarantees: {len(result.get('guarantees') or [])}")
    print(f"- validation errors: {validation.get('errors') or []}")
    print(f"- validation warnings: {validation.get('warnings') or []}")
    print(f"- json: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
