from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.document_agents import DocumentAgentResult, run_document_extraction_agent
from backend.services.enterprise_bank_statement_agent import run_enterprise_bank_statement_agent


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python backend/scripts/test_enterprise_bank_statement_agent.py "样例文件路径.xlsx"')
    file_path = Path(sys.argv[1]).resolve()
    result = run_enterprise_bank_statement_agent(
        file_path=str(file_path),
        filename=file_path.name,
        document_type="enterprise_flow",
        metadata={"file_path": str(file_path)},
    )
    extracted = result["extracted_json"]
    print(json.dumps(extracted, ensure_ascii=False, indent=2))
    print("\n" + result["markdown_summary"])
    assert extracted["accounts"], "至少识别 1 个账户"
    assert extracted["transactions"], "至少识别 1 条交易"
    assert extracted["summary"]["total_inflow"] > 0, "total_inflow > 0"
    assert extracted["summary"]["total_outflow"] > 0, "total_outflow > 0"
    assert extracted["monthly_summary"], "monthly_summary 不为空"
    assert extracted["counterparty_summary"]["top_inflow_counterparties"], "top_inflow_counterparties 不为空"
    assert "企业流水分析报告" in result["markdown_summary"]

    orchestrated = run_document_extraction_agent(
        document_type="enterprise_flow",
        raw_text="",
        filename=file_path.name,
        customer_id="script-smoke",
        metadata={"file_path": str(file_path), "document_type": "enterprise_flow"},
    )
    assert isinstance(orchestrated, DocumentAgentResult)
    legacy = orchestrated.to_legacy_content()
    for key in ("extracted_json", "markdown_summary", "data", "confidence", "title", "type"):
        assert key in legacy, f"missing {key}"
    print("\nenterprise bank statement script smoke passed")


if __name__ == "__main__":
    main()
