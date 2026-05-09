from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PR_COMMENT_MARKER = "<!-- agent-regression-report -->"


def build_regression_markdown_report(results: dict[str, Any]) -> str:
    metrics = results.get("metrics") or {}
    cases = list(results.get("cases") or [])
    fingerprint = results.get("version_fingerprint") or {}
    previous_fingerprint = _previous_fingerprint(results)
    mode = "LLM" if results.get("use_llm") else "Rule"
    score = _stability_score(cases)
    lines = [
        "# Agent Regression Report",
        "",
        f"- Generated At: {results.get('generated_at') or datetime.now(timezone.utc).isoformat()}",
        f"- Mode: {mode}",
        f"- Total Cases: {metrics.get('total_cases', len(cases))}",
        f"- Passed: {metrics.get('passed_cases', 0)}",
        f"- Failed: {metrics.get('failed_cases', 0)}",
        f"- Drift Cases: {metrics.get('drift_cases', 0)}",
        f"- Overall Stability Score: **{score}**",
        "",
        "## Version Fingerprint",
        "",
        *_fingerprint_table(fingerprint),
        "",
        "## Fingerprint Changes",
        "",
        *_fingerprint_changes_table(previous_fingerprint, fingerprint),
        "",
        "## Case List",
        "",
        "| Case | Regression | Snapshot | Critical | High | Medium | Low | Possible Causes |",
        "|------|------------|----------|----------|------|--------|-----|-----------------|",
    ]
    for case in cases:
        counts = _severity_counts(case)
        regression = "✅ PASS" if not case.get("failures") else "❌ FAIL"
        snapshot = "🟢 STABLE" if not (case.get("drift") or {}).get("has_drift") else "🟠 DRIFT"
        causes = ", ".join((case.get("drift") or {}).get("possible_causes") or []) or "-"
        lines.append(
            f"| {case.get('case_name')} | {regression} | {snapshot} | "
            f"{counts['critical']} | {counts['high']} | {counts['medium']} | {counts['low']} | {causes} |"
        )
    lines.extend(["", *_drift_section(cases, "critical", "## Critical Drift"), "", *_drift_section(cases, "high", "## High Drift")])
    lines.extend(["", "## Compliance Warnings", ""])
    compliance_rows = _collect_step_values(cases, "compliance_warnings")
    lines.extend([f"- {row}" for row in compliance_rows] or ["- None"])
    lines.extend(["", "## Validation Errors", ""])
    validation_rows = _collect_step_values(cases, "validation_errors")
    lines.extend([f"- {row}" for row in validation_rows] or ["- None"])
    lines.extend(["", "## LLM Usage Summary", "", "| Agent | LLM Used | Fallback Used | Retry Count |", "|-------|----------|---------------|-------------|"])
    for row in _llm_rows(cases):
        lines.append(f"| {row['agent']} | {row['llm_used']} | {row['fallback_used']} | {row['retry_count']} |")
    lines.extend(["", "## Top Risks", ""])
    lines.extend([f"- {risk}" for risk in _top_risks(cases)] or ["- None"])
    lines.extend(["", "## Fallback Summary", ""])
    fallback_rows = [row for row in _llm_rows(cases) if row["fallback_used"]]
    lines.extend([f"- {row['case']} / {row['agent']} fallback used" for row in fallback_rows] or ["- None"])
    return "\n".join(lines) + "\n"


def build_regression_html_report(results: dict[str, Any]) -> str:
    markdown = build_regression_markdown_report(results)
    cases = list(results.get("cases") or [])
    fingerprint = results.get("version_fingerprint") or {}
    previous_fingerprint = _previous_fingerprint(results)
    score = _stability_score(cases)
    rows = []
    for case in cases:
        counts = _severity_counts(case)
        drift = (case.get("drift") or {}).get("has_drift")
        cls = "bad" if counts["critical"] or counts["high"] else "warn" if drift else "ok"
        causes = ", ".join((case.get("drift") or {}).get("possible_causes") or []) or "-"
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(case.get('case_name')))}</td>"
            f"<td>{'PASS' if not case.get('failures') else 'FAIL'}</td>"
            f"<td class='{cls}'>{'DRIFT' if drift else 'STABLE'}</td>"
            f"<td>{counts['critical']}</td><td>{counts['high']}</td><td>{counts['medium']}</td><td>{counts['low']}</td>"
            f"<td>{html.escape(causes)}</td>"
            "</tr>"
        )
    fp_rows = "".join(f"<tr><th>{html.escape(k)}</th><td>{html.escape(str(v))}</td></tr>" for k, v in _fingerprint_pairs(fingerprint))
    fp_change_rows = "".join(
        "<tr>"
        f"<td>{html.escape(row['item'])}</td>"
        f"<td>{html.escape(row['previous'])}</td>"
        f"<td>{html.escape(row['current'])}</td>"
        f"<td>{html.escape(row['changed'])}</td>"
        "</tr>"
        for row in _fingerprint_change_rows(previous_fingerprint, fingerprint)
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<title>Agent Regression Report</title>
<style>
body {{ margin: 0; padding: 32px; background: #10141f; color: #e6edf7; font-family: 'Segoe UI', sans-serif; }}
.card {{ background: #171d2b; border: 1px solid #283248; border-radius: 16px; padding: 24px; margin-bottom: 20px; }}
h1, h2 {{ margin-top: 0; }}
.score {{ font-size: 42px; font-weight: 800; color: {'#64d68a' if score >= 85 else '#ffd166' if score >= 70 else '#ff6b6b'}; }}
table {{ width: 100%; border-collapse: collapse; overflow: hidden; border-radius: 12px; }}
th, td {{ padding: 12px; border-bottom: 1px solid #2a344d; text-align: left; }}
th {{ color: #9fb3d9; background: #1e2638; }}
.ok {{ color: #64d68a; font-weight: 700; }}
.warn {{ color: #ffd166; font-weight: 700; }}
.bad {{ color: #ff6b6b; font-weight: 700; }}
pre {{ white-space: pre-wrap; background: #0c1019; border-radius: 12px; padding: 16px; color: #d9e6ff; }}
</style>
</head>
<body>
<div class="card">
<h1>Agent Regression Report</h1>
<div>Generated At: {html.escape(str(results.get('generated_at') or ''))}</div>
<div>Mode: {'LLM' if results.get('use_llm') else 'Rule'}</div>
<div>Overall Stability Score</div>
<div class="score">{score}</div>
</div>
<div class="card">
<h2>Version Fingerprint</h2>
<table>{fp_rows}</table>
</div>
<div class="card">
<h2>Fingerprint Changes</h2>
<table>
<thead><tr><th>Item</th><th>Previous</th><th>Current</th><th>Changed</th></tr></thead>
<tbody>{fp_change_rows}</tbody>
</table>
</div>
<div class="card">
<h2>Cases</h2>
<table>
<thead><tr><th>Case</th><th>Regression</th><th>Snapshot</th><th>Critical</th><th>High</th><th>Medium</th><th>Low</th><th>Possible Causes</th></tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</div>
<div class="card">
<h2>Markdown Detail</h2>
<pre>{html.escape(markdown)}</pre>
</div>
</body>
</html>
"""


def build_pr_summary_markdown(results: dict[str, Any]) -> str:
    metrics = results.get("metrics") or {}
    cases = list(results.get("cases") or [])
    fingerprint = results.get("version_fingerprint") or {}
    previous_fingerprint = _previous_fingerprint(results)
    score = _stability_score(cases)
    critical = sum(_severity_counts(case)["critical"] for case in cases)
    high = sum(_severity_counts(case)["high"] for case in cases)
    has_failures = any(case.get("failures") for case in cases)
    has_drift = any((case.get("drift") or {}).get("has_drift") for case in cases)
    causes = sorted({
        cause
        for case in cases
        for cause in ((case.get("drift") or {}).get("possible_causes") or [])
    })
    lines = [
        PR_COMMENT_MARKER,
        "",
        "# 🤖 Agent Regression Summary",
        "",
        f"- Overall Stability Score: {score}/100",
        f"- Regression: {'FAIL' if has_failures else 'PASS'}",
        f"- Snapshot Drift: {'DRIFT' if has_drift else 'STABLE'}",
        f"- Critical Drift: {critical}",
        f"- High Drift: {high}",
        f"- Possible Causes: {', '.join(causes) if causes else 'none'}",
        f"- LLM Usage Rate: {_pct(metrics.get('llm_usage_rate'))}",
        f"- Fallback Rate: {_pct(metrics.get('fallback_rate'))}",
        "",
        "## Fingerprint Changes",
        "",
        *_fingerprint_changes_table(previous_fingerprint, fingerprint),
        "",
        "## Top Drift",
        "",
        *_top_drift_lines(cases, limit=5),
        "",
        "## Reports",
        "- Markdown report: artifact `reports/agent_regression/latest_report.md`",
        "- HTML report: artifact `reports/agent_regression/latest_report.html`",
        "- Metrics: artifact `reports/agent_metrics/latest_metrics.json`",
    ]
    return "\n".join(lines) + "\n"


def save_regression_reports(results: dict[str, Any], *, markdown: bool = False, html_report: bool = False) -> dict[str, str]:
    output_dir = Path("reports/agent_regression")
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    if markdown:
        path = output_dir / "latest_report.md"
        path.write_text(build_regression_markdown_report(results), encoding="utf-8")
        paths["markdown"] = str(path)
    if html_report:
        path = output_dir / "latest_report.html"
        path.write_text(build_regression_html_report(results), encoding="utf-8")
        paths["html"] = str(path)
    if markdown or html_report:
        path = output_dir / "pr_summary.md"
        path.write_text(build_pr_summary_markdown(results), encoding="utf-8")
        paths["pr_summary"] = str(path)
    return paths


def _fingerprint_table(fingerprint: dict[str, Any]) -> list[str]:
    lines = ["| Item | Value |", "|------|-------|"]
    for key, value in _fingerprint_pairs(fingerprint):
        lines.append(f"| {key} | {value} |")
    return lines


def _fingerprint_changes_table(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    lines = ["| Item | Previous | Current | Changed |", "|------|----------|---------|---------|"]
    for row in _fingerprint_change_rows(previous, current):
        lines.append(f"| {row['item']} | {row['previous']} | {row['current']} | {row['changed']} |")
    return lines


def _fingerprint_change_rows(previous: dict[str, Any], current: dict[str, Any]) -> list[dict[str, str]]:
    previous = previous or {}
    current = current or {}

    def get(fp: dict[str, Any], path: list[str]) -> str:
        value: Any = fp
        for part in path:
            if not isinstance(value, dict):
                return "unknown"
            value = value.get(part)
        return str(value if value not in (None, "") else "unknown")

    rows = [
        ("Model", ["model_config", "model"]),
        ("RiskAgent Prompt", ["prompt_hashes", "RiskAgent"]),
        ("FinancingJudgementAgent Prompt", ["prompt_hashes", "FinancingJudgementAgent"]),
        ("RiskAgent Schema", ["schema_hashes", "RiskAgent"]),
        ("FinancingJudgementAgent Schema", ["schema_hashes", "FinancingJudgementAgent"]),
        ("RiskAgent Rule", ["rule_hashes", "RiskAgent"]),
        ("Compliance Guard", ["compliance_guard_hash"]),
    ]
    result = []
    for item, path in rows:
        old = get(previous, path)
        new = get(current, path)
        changed = "—" if old == "unknown" else "✅" if old != new else "—"
        result.append({"item": item, "previous": old, "current": new, "changed": changed})
    return result


def _previous_fingerprint(results: dict[str, Any]) -> dict[str, Any]:
    for case in results.get("cases") or []:
        previous = case.get("previous_snapshot") or {}
        fingerprint = previous.get("version_fingerprint") or {}
        if fingerprint:
            return fingerprint
    return {}


def _fingerprint_pairs(fingerprint: dict[str, Any]) -> list[tuple[str, Any]]:
    prompt_hashes = fingerprint.get("prompt_hashes") or {}
    schema_hashes = fingerprint.get("schema_hashes") or {}
    rule_hashes = fingerprint.get("rule_hashes") or {}
    model_config = fingerprint.get("model_config") or {}
    return [
        ("Commit", fingerprint.get("git_commit") or "unknown"),
        ("Branch", fingerprint.get("git_branch") or "unknown"),
        ("App Version", fingerprint.get("app_version") or "unknown"),
        ("Model", model_config.get("model") or "unknown"),
        ("RiskAgent Prompt Hash", prompt_hashes.get("RiskAgent") or "unknown"),
        ("FinancingJudgementAgent Prompt Hash", prompt_hashes.get("FinancingJudgementAgent") or "unknown"),
        ("RiskAgent Schema Hash", schema_hashes.get("RiskAgent") or "unknown"),
        ("FinancingJudgementAgent Schema Hash", schema_hashes.get("FinancingJudgementAgent") or "unknown"),
        ("RiskAgent Rule Hash", rule_hashes.get("RiskAgent") or "unknown"),
        ("Compliance Guard Hash", fingerprint.get("compliance_guard_hash") or "unknown"),
        ("Snapshot Version", fingerprint.get("snapshot_version") or "v1"),
    ]


def _stability_score(cases: list[dict[str, Any]]) -> int:
    score = 100
    penalty = {"critical": 30, "high": 15, "medium": 5, "low": 1}
    for case in cases:
        for change in (case.get("drift") or {}).get("changes") or []:
            score -= penalty.get(change.get("severity"), 0)
    return max(0, score)


def _severity_counts(case: dict[str, Any]) -> dict[str, int]:
    counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for change in (case.get("drift") or {}).get("changes") or []:
        severity = change.get("severity")
        if severity in counts:
            counts[severity] += 1
    return counts


def _drift_section(cases: list[dict[str, Any]], severity: str, title: str) -> list[str]:
    lines = [title, ""]
    rows = []
    for case in cases:
        causes = ", ".join((case.get("drift") or {}).get("possible_causes") or []) or "-"
        for change in (case.get("drift") or {}).get("changes") or []:
            if change.get("severity") == severity:
                rows.append(f"- {case.get('case_name')}: {change.get('field')} ({change.get('old')} -> {change.get('new')}); causes={causes}")
    lines.extend(rows or ["- None"])
    return lines


def _collect_step_values(cases: list[dict[str, Any]], key: str) -> list[str]:
    rows = []
    for case in cases:
        report = (case.get("result") or {}).get("report") or {}
        for step in report.get("steps") or []:
            output = step.get("output") or {}
            values = output.get(key) or []
            for value in values:
                rows.append(f"{case.get('case_name')} / {step.get('agent_name')}: {value}")
    return rows


def _llm_rows(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for case in cases:
        report = (case.get("result") or {}).get("report") or {}
        for step in report.get("steps") or []:
            output = step.get("output") or {}
            if "llm_used" in output or "fallback_used" in output:
                rows.append({
                    "case": case.get("case_name"),
                    "agent": step.get("agent_name"),
                    "llm_used": bool(output.get("llm_used")),
                    "fallback_used": bool(output.get("fallback_used")),
                    "retry_count": output.get("retry_count") or 0,
                })
    return rows


def _top_risks(cases: list[dict[str, Any]]) -> list[str]:
    risks = []
    for case in cases:
        report = (case.get("result") or {}).get("report") or {}
        for item in (report.get("risk_agent") or {}).get("risks") or []:
            risk_type = item.get("risk_type") or item.get("description")
            if risk_type:
                risks.append(f"{case.get('case_name')}: {risk_type}")
    return risks[:20]


def _top_drift_lines(cases: list[dict[str, Any]], limit: int = 5) -> list[str]:
    rows = []
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    for case in cases:
        causes = ", ".join((case.get("drift") or {}).get("possible_causes") or []) or "-"
        for change in (case.get("drift") or {}).get("changes") or []:
            rows.append((severity_order.get(change.get("severity"), 9), case.get("case_name"), change, causes))
    rows.sort(key=lambda item: item[0])
    lines = []
    for _, case_name, change, causes in rows[:limit]:
        if "added" in change or "removed" in change:
            added = ", ".join(change.get("added") or []) or "-"
            removed = ", ".join(change.get("removed") or []) or "-"
            lines.append(f"- [{change.get('severity')}] {case_name}: {change.get('field')} added=[{added}] removed=[{removed}], causes={causes}")
        else:
            lines.append(f"- [{change.get('severity')}] {case_name}: {change.get('field')} {change.get('old')} -> {change.get('new')}, causes={causes}")
    return lines or ["- None"]


def _pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return "0.0%"
