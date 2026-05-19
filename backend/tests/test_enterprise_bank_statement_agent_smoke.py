from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.document_types import should_append_same_type_document
from backend.services.document_agents import DocumentAgentResult, run_document_extraction_agent
from backend.services.enterprise_bank_statement_agent import run_enterprise_bank_statement_agent


SAMPLE_TEXT = """
中国建设银行企业客户交易明细
客户名称: 上海样例科技有限公司
银行名称: 中国建设银行
开户行: 中国建设银行上海浦东支行
账号: 31001599887766554433
币种: 人民币
流水期间: 2026-01-01 至 2026-03-31
期初余额: 50,000.00
期末余额: 180,000.00
交易日期 摘要 用途 对方户名 借方发生额 贷方发生额 余额
2026-01-05 转入 销售回款 上海采购有限公司  300,000.00 350,000.00
2026-01-06 转出 货款 上海供应链有限公司 120,000.00  230,000.00
2026-02-12 扣息 贷款利息 中国建设银行 2,000.00  228,000.00
2026-03-20 转入 服务费 杭州客户有限公司 200,000.00 428,000.00
2026-03-21 转出 采购款 上海供应链有限公司 248,000.00 180,000.00
"""


def test_agent_smoke() -> None:
    result = run_enterprise_bank_statement_agent(text=SAMPLE_TEXT, document_type="enterprise_flow", metadata={})
    assert result["extracted_json"]
    assert result["markdown_summary"]
    extracted = result["extracted_json"]
    assert "accounts" in extracted
    assert "summary" in extracted
    assert "transactions" in extracted


def test_document_agent_orchestrator_smoke() -> None:
    result = run_document_extraction_agent(
        document_type="enterprise_flow",
        raw_text=SAMPLE_TEXT,
        filename="enterprise-bank-statement.txt",
        customer_id="customer-smoke",
        metadata={},
    )
    assert isinstance(result, DocumentAgentResult)
    assert result.document_type == "enterprise_flow"
    assert "summary" in result.extracted_json
    assert result.markdown_summary


def test_enterprise_bank_statement_append_save_policy() -> None:
    assert should_append_same_type_document("enterprise_bank_statement")
    assert should_append_same_type_document("enterprise_flow")
    assert should_append_same_type_document("bank_statement_enterprise")
    assert should_append_same_type_document("company_bank_statement")


if __name__ == "__main__":
    test_agent_smoke()
    test_document_agent_orchestrator_smoke()
    test_enterprise_bank_statement_append_save_policy()
    print("enterprise bank statement smoke tests passed")
