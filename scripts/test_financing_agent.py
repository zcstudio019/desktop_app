from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.agents.orchestrator import run_financing_agent_workflow


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Run financing Agent workflow for a customer.")
    parser.add_argument("--customer-id", required=True)
    parser.add_argument("--task-type", default="full")
    parser.add_argument("--use-llm", action="store_true", help="Enable controlled LLM enhancement for Risk/Judgement agents.")
    args = parser.parse_args()

    os.environ.setdefault("ENABLE_FINANCING_AGENT", "true")
    os.environ["ENABLE_AGENT_LLM"] = "true" if args.use_llm else os.getenv("ENABLE_AGENT_LLM", "false")
    result = await run_financing_agent_workflow(args.customer_id, args.task_type)
    report = result.get("report") or {}
    risk = report.get("risk_agent") or {}
    missing = report.get("missing_material_agent") or {}
    judgement_agent = report.get("financing_judgement_agent") or {}
    judgement = judgement_agent.get("judgement") or {}

    print(f"use_llm: {os.getenv('ENABLE_AGENT_LLM')}")
    print(f"run_id: {result.get('run_id')}")
    print(f"status: {result.get('status')}")
    print("steps:")
    for step in result.get("steps") or []:
        output = step.get("output") or {}
        print(
            f"- {step.get('step_order')}. {step.get('agent_name')}: {step.get('status')}"
            f" llm_used={output.get('llm_used')} fallback={output.get('fallback_used')}"
            f" retries={output.get('retry_count')}"
        )
    print(f"risk_level: {risk.get('risk_level') or 'unknown'}")
    print(f"RiskAgent llm_used: {risk.get('llm_used')}")
    print(f"RiskAgent fallback_used: {risk.get('fallback_used')}")
    print(f"RiskAgent validation_errors: {risk.get('validation_errors') or []}")
    print(f"RiskAgent compliance_warnings: {risk.get('compliance_warnings') or []}")
    print(f"FinancingJudgementAgent llm_used: {judgement_agent.get('llm_used')}")
    print(f"FinancingJudgementAgent fallback_used: {judgement_agent.get('fallback_used')}")
    print(f"FinancingJudgementAgent validation_errors: {judgement_agent.get('validation_errors') or []}")
    print(f"FinancingJudgementAgent compliance_warnings: {judgement_agent.get('compliance_warnings') or []}")
    print(f"missing_material_count: {len(missing.get('required_materials') or [])}")
    print(f"financing_opinion: {judgement.get('overall_opinion') or ''}")
    print(f"output_path: {result.get('output_path') or ''}")
    if result.get("error_message"):
        print(f"error_message: {result.get('error_message')}")
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
