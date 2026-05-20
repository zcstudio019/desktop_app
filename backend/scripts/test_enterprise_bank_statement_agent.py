from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.document_agents import DocumentAgentResult, run_document_extraction_agent
from backend.services.enterprise_bank_statement_agent import run_enterprise_bank_statement_agent


EXPECTED_BANKS = {"民生银行", "平安银行", "泰隆银行", "浙江网商"}
COUNTERPARTY_BANKS = {"中国建设银行", "中国农业银行", "交通银行", "招商银行"}


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit('Usage: python backend/scripts/test_enterprise_bank_statement_agent.py "样例文件路径.xlsx"')
    file_path = Path(sys.argv[1]).resolve()
    result = run_enterprise_bank_statement_agent(
        file_path=str(file_path),
        filename=file_path.name,
        document_type="enterprise_flow",
        metadata={"file_path": str(file_path), "customer_name": "上海龙盼新材料科技有限公司"},
    )
    extracted = result["extracted_json"]

    print("\naccounts:")
    for account in extracted.get("accounts") or []:
        print(
            json.dumps(
                {
                    "bank_name": account.get("bank_name"),
                    "account_name": account.get("account_name"),
                    "account_number": account.get("account_number"),
                    "sheet_name": account.get("sheet_name"),
                    "total_inflow": account.get("total_inflow"),
                    "total_outflow": account.get("total_outflow"),
                    "transaction_count": account.get("transaction_count"),
                },
                ensure_ascii=False,
            )
        )
    print("\nsummary:")
    print(json.dumps(extracted.get("summary") or {}, ensure_ascii=False, indent=2))
    print("\nwarnings:")
    print(json.dumps(extracted.get("warnings") or [], ensure_ascii=False, indent=2))
    print("\n" + result["markdown_summary"])

    accounts = extracted["accounts"]
    summary = extracted["summary"]
    bank_names = {item.get("bank_name") for item in accounts}
    assert accounts, "至少识别 1 个账户"
    assert extracted["transactions"], "至少识别 1 条交易"
    assert summary["total_inflow"] > 0, "total_inflow > 0"
    assert summary["total_outflow"] > 0, "total_outflow > 0"
    assert extracted["monthly_summary"], "monthly_summary 不为空"
    assert extracted["counterparty_summary"]["top_inflow_counterparties"], "top_inflow_counterparties 不为空"
    assert "企业流水分析报告" in result["markdown_summary"]

    assert summary["account_count"] == 4, f"account_count should be 4, got {summary['account_count']}"
    assert EXPECTED_BANKS.issubset(bank_names), f"missing banks: {EXPECTED_BANKS - bank_names}; got {bank_names}"
    assert not (COUNTERPARTY_BANKS & bank_names), f"counterparty bank leaked into accounts: {COUNTERPARTY_BANKS & bank_names}"

    minsheng = next((item for item in accounts if item.get("bank_name") == "民生银行"), None)
    assert minsheng, "missing 民生银行 account"
    assert minsheng.get("total_inflow", 0) > 0, "民生银行 total_inflow > 0"
    assert minsheng.get("total_outflow", 0) > 0, "民生银行 total_outflow > 0"
    if not minsheng.get("transaction_count"):
        assert any("民生银行" in warning and "顶部累计发生额" in warning for warning in extracted.get("warnings") or []), "民生银行明细缺失时必须输出顶部汇总 warning"

    orchestrated = run_document_extraction_agent(
        document_type="enterprise_flow",
        raw_text="",
        filename=file_path.name,
        customer_id="script-smoke",
        metadata={"file_path": str(file_path), "document_type": "enterprise_flow", "customer_name": "上海龙盼新材料科技有限公司"},
    )
    assert isinstance(orchestrated, DocumentAgentResult)
    legacy = orchestrated.to_legacy_content()
    for key in ("extracted_json", "markdown_summary", "data", "confidence", "title", "type"):
        assert key in legacy, f"missing {key}"
    print("\nenterprise bank statement script smoke passed")


if __name__ == "__main__":
    main()
