from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.agents import run_financing_agent_workflow_from_context  # noqa: E402
from backend.services.agents.metrics import build_agent_regression_metrics, save_metrics  # noqa: E402
from backend.services.agents.report_builder import save_regression_reports  # noqa: E402
from backend.services.agents.snapshot_diff import build_agent_snapshot, diff_agent_snapshot  # noqa: E402
from backend.services.agents.versioning import build_agent_version_fingerprint  # noqa: E402


FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "agent_cases"
SNAPSHOT_ROOT = ROOT / "tests" / "snapshots" / "agent_cases"
DISCLAIMER = "以上为资料初判，不构成贷款承诺，最终以银行审批为准。"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _contains_any(text: str, values: list[str]) -> bool:
    return any(value in text for value in values)


def _risk_items(report: dict[str, Any]) -> list[dict[str, Any]]:
    return list((report.get("risk_agent") or {}).get("risks") or [])


def _missing_items(report: dict[str, Any]) -> list[dict[str, Any]]:
    return list((report.get("missing_material_agent") or {}).get("required_materials") or [])


def _judgement(report: dict[str, Any]) -> dict[str, Any]:
    return (report.get("financing_judgement_agent") or {}).get("judgement") or {}


def _step_output(report: dict[str, Any], agent_name: str) -> dict[str, Any]:
    for step in report.get("steps") or []:
        if step.get("agent_name") == agent_name:
            return step.get("output") or {}
    return report.get(agent_name) or {}


def _assert_case(result: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    report = result.get("report") or {}
    risk = report.get("risk_agent") or {}
    judgement_agent = report.get("financing_judgement_agent") or {}
    judgement = _judgement(report)
    all_text = _dump(report)

    allowed_levels = expected.get("risk_level_in") or []
    if allowed_levels and risk.get("risk_level") not in allowed_levels:
        failures.append(f"RiskAgent risk_level={risk.get('risk_level')} not in {allowed_levels}")

    risk_text = _dump(_risk_items(report))
    for item in expected.get("must_have_risk_types") or []:
        if item not in risk_text:
            failures.append(f"RiskAgent missing risk type/keyword: {item}")

    missing_text = _dump(_missing_items(report))
    for item in expected.get("must_have_missing_materials") or []:
        if item not in missing_text:
            failures.append(f"MissingMaterialAgent missing material: {item}")

    if expected.get("must_include_disclaimer") and DISCLAIMER not in _dump(judgement_agent):
        failures.append("FinancingJudgementAgent missing compliance disclaimer")

    for term in expected.get("forbidden_terms") or []:
        if term in all_text:
            failures.append(f"Compliance forbidden term leaked: {term}")

    amount_range = str(judgement.get("estimated_amount_range") or "")
    amount_terms = expected.get("estimated_amount_must_contain") or []
    if amount_terms and not _contains_any(amount_range, amount_terms):
        failures.append(f"estimated_amount_range lacks required qualifier: {amount_terms}; got={amount_range}")

    for agent_name in ["RiskAgent", "FinancingJudgementAgent"]:
        output = _step_output(report, agent_name)
        validation_errors = output.get("validation_errors") or []
        if validation_errors and not output.get("fallback_used"):
            failures.append(f"{agent_name} validation_errors without fallback: {validation_errors}")
        if output.get("fallback_used") and not output.get("fallback_reason") and output.get("llm_used") is False:
            if "ENABLE_AGENT_LLM=false" not in str(output):
                failures.append(f"{agent_name} fallback_used without fallback_reason")

    if not result.get("success"):
        failures.append(f"workflow failed: {result.get('error_message')}")
    return failures


def _snapshot_path(case_name: str) -> Path:
    return SNAPSHOT_ROOT / f"{case_name}.snapshot.json"


def _severity_rank(severity: str) -> int:
    return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(severity, 0)


def _diff_has_blocking_drift(diff: dict[str, Any]) -> bool:
    return any(_severity_rank(item.get("severity") or "") >= _severity_rank("high") for item in diff.get("changes") or [])


def _print_snapshot_diff(diff: dict[str, Any], missing_snapshot: bool = False) -> None:
    if missing_snapshot:
        print("Snapshot: MISSING (run with --update-snapshots first)")
        return
    if not diff.get("has_drift"):
        print("Snapshot: STABLE")
        return
    print("Snapshot: DRIFT")
    for severity in ["critical", "high", "medium", "low"]:
        grouped = [item for item in diff.get("changes") or [] if item.get("severity") == severity]
        if not grouped:
            continue
        print(f"{severity.capitalize()}:")
        for item in grouped:
            field = item.get("field")
            if "added" in item or "removed" in item:
                added = ", ".join(item.get("added") or []) or "-"
                removed = ", ".join(item.get("removed") or []) or "-"
                print(f"- {field}: added=[{added}] removed=[{removed}]")
            else:
                print(f"- {field}: {item.get('old')} -> {item.get('new')}")


async def _run_case(case_dir: Path, *, use_llm: bool, update_snapshots: bool, diff_only: bool) -> dict[str, Any]:
    case_name = case_dir.name
    context = _load_json(case_dir / "input_context.json")
    expected = _load_json(case_dir / "expected_assertions.json")
    result = await run_financing_agent_workflow_from_context(context, use_llm=use_llm)
    failures = _assert_case(result, expected)
    report = result.get("report") or {}
    snapshot = build_agent_snapshot(report, case_name)
    snapshot_path = _snapshot_path(case_name)
    missing_snapshot = False
    previous_snapshot = None
    drift = {"case_name": case_name, "has_drift": False, "changes": []}

    print(f"Case: {case_name}")
    print(f"Regression: {'FAIL' if failures else 'PASS'}")
    if failures:
        for failure in failures:
            print(f"- {failure}")
    else:
        risk = report.get("risk_agent") or {}
        missing = report.get("missing_material_agent") or {}
        print(f"- risk_level={risk.get('risk_level')} missing={len(missing.get('required_materials') or [])}")

    if snapshot_path.exists():
        previous_snapshot = _load_json(snapshot_path)
        drift = diff_agent_snapshot(previous_snapshot, snapshot)
        _print_snapshot_diff(drift)
    else:
        missing_snapshot = True
        _print_snapshot_diff(drift, missing_snapshot=True)

    if update_snapshots:
        if failures:
            print(f"Snapshot update skipped: regression failed for {case_name}")
        else:
            _write_json(snapshot_path, snapshot)
            print(f"Snapshot updated: {snapshot_path}")
            missing_snapshot = False

    risk_output = _step_output(report, "RiskAgent")
    judgement_output = _step_output(report, "FinancingJudgementAgent")
    print(
        "Run: "
        f"run_id={result.get('run_id')} "
        f"llm={use_llm} "
        f"risk_llm_used={risk_output.get('llm_used')} "
        f"judgement_llm_used={judgement_output.get('llm_used')} "
        f"risk_fallback={risk_output.get('fallback_used')} "
        f"judgement_fallback={judgement_output.get('fallback_used')} "
        f"output={result.get('output_path')}"
    )
    print("")
    return {
        "case_name": case_name,
        "failures": failures,
        "result": result,
        "snapshot": snapshot,
        "previous_snapshot": previous_snapshot,
        "drift": drift,
        "missing_snapshot": missing_snapshot,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run financing Agent regression fixtures.")
    parser.add_argument("--case", dest="case_name", default="", help="Run only one fixture case directory.")
    parser.add_argument("--use-llm", action="store_true", help="Enable controlled LLM mode for Risk/Judgement agents.")
    parser.add_argument("--update-snapshots", action="store_true", help="Write current stable summaries as snapshot baselines after assertions pass.")
    parser.add_argument("--diff-only", action="store_true", help="Only inspect snapshot diff; does not update snapshots.")
    parser.add_argument("--fail-on-drift", action="store_true", help="Exit with code 1 when high or critical snapshot drift is detected.")
    parser.add_argument("--report", action="store_true", help="Generate Markdown regression report.")
    parser.add_argument("--html-report", action="store_true", help="Generate HTML regression report.")
    parser.add_argument("--metrics", action="store_true", help="Generate JSON regression metrics.")
    parser.add_argument("--print-fingerprint", action="store_true", help="Print current Agent version fingerprint and exit.")
    args = parser.parse_args()

    if args.print_fingerprint:
        print(json.dumps(build_agent_version_fingerprint(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    os.environ["ENABLE_FINANCING_AGENT"] = "true"
    if args.use_llm:
        os.environ["ENABLE_AGENT_LLM"] = "true"
        os.environ.setdefault("AGENT_LLM_MAX_RETRIES", "1")
        os.environ.setdefault("AGENT_LLM_TIMEOUT", "20")
    else:
        os.environ["ENABLE_AGENT_LLM"] = "false"

    case_dirs = [FIXTURE_ROOT / args.case_name] if args.case_name else sorted(path for path in FIXTURE_ROOT.iterdir() if path.is_dir())
    case_results: list[dict[str, Any]] = []
    for case_dir in case_dirs:
        if not (case_dir / "input_context.json").exists():
            print(f"[SKIP] {case_dir.name}: missing input_context.json")
            continue
        case_results.append(await _run_case(case_dir, use_llm=args.use_llm, update_snapshots=args.update_snapshots, diff_only=args.diff_only))

    regression_results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "use_llm": args.use_llm,
        "cases": case_results,
        "version_fingerprint": build_agent_version_fingerprint(),
    }
    metrics = build_agent_regression_metrics(regression_results)
    regression_results["metrics"] = metrics

    total_failures = [f"{case['case_name']}: {failure}" for case in case_results for failure in case.get("failures") or []]
    blocking_drifts = [case["case_name"] for case in case_results if _diff_has_blocking_drift(case.get("drift") or {})]
    missing_snapshots = [case["case_name"] for case in case_results if case.get("missing_snapshot")]

    report_paths = {}
    if args.report or args.html_report:
        report_paths = save_regression_reports(regression_results, markdown=args.report, html_report=args.html_report)
        for kind, path in report_paths.items():
            print(f"{kind}_report={path}")
    if args.metrics:
        metrics_path = save_metrics(metrics)
        print(f"metrics={metrics_path}")

    print(f"cases={len(case_results)} failed={len(total_failures)} blocking_drift={len(blocking_drifts)} use_llm={args.use_llm}")
    if total_failures:
        return 1
    if args.diff_only and missing_snapshots:
        return 1
    if args.fail_on_drift and blocking_drifts:
        print(f"fail-on-drift triggered: {', '.join(blocking_drifts)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
