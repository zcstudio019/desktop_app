from __future__ import annotations

import asyncio

from backend.services.bank_reconciliation_detail_display import sanitize_bank_reconciliation_detail_display
from backend.services.markdown_profile_service import _build_single_document_section


def test_sanitize_bank_reconciliation_detail_display_removes_dirty_fields() -> None:
    clean_markdown = "\n".join(
        [
            "## 银行对账明细",
            "",
            "- 资料类型：银行对账明细",
            "- 来源文件：工商银行对账明细202504-202603.xlsx",
            "",
            "### 月度资金变化",
            "",
            "| 月份 | 入账金额 | 出账金额 | 净流入 | 交易笔数 |",
            "|---|---:|---:|---:|---:|",
            "| 2025-07 | 6,170,000.00 | 6,535,275.33 | -365,275.33",
            "",
            "### 风险提示",
            "",
            "- 存在贷款、利息、手续费等非经营性交易，不能直接作为销售回款。",
        ]
    )
    dirty_markdown = "\n".join(
        [
            clean_markdown,
            "",
            "data：{ \"display_markdown\": \"...\" }",
            "markdown：## 银行对账明细...",
            "display markdown：## 银行对账明细...",
            "report markdown：## 银行对账明细...",
            "structured data：{\"transactions\":[{\"is_fee\": true}]}",
            "transactions：[{\"transaction_id\":\"1\",\"is_loan_related\": false}]",
        ]
    )
    result = {
        "title": "银行对账明细",
        "type": "bank_reconciliation_detail",
        "document_type": "bank_reconciliation_detail",
        "doc_type": "bank_reconciliation_detail",
        "doc_type_name": "银行对账明细",
        "data": {"display_markdown": clean_markdown},
        "markdown": dirty_markdown,
        "display_markdown": dirty_markdown,
        "report_markdown": dirty_markdown,
        "structured_data": {
            "doc_type": "bank_reconciliation_detail",
            "monthly": {"2025-07": {"count": 17}},
            "transactions": [{"transaction_id": "1", "is_fee": True}],
        },
        "transactions": [{"transaction_id": "1"}],
    }

    cleaned = sanitize_bank_reconciliation_detail_display(result)
    markdown = cleaned["display_markdown"]

    assert set(cleaned.keys()) == {"doc_type", "doc_type_name", "display_markdown"}
    assert markdown.count("## 银行对账明细") == 1
    assert "| 2025-07 | 6,170,000.00 | 6,535,275.33 | -365,275.33 | 17 |" in markdown
    forbidden = [
        "data：",
        "structured data：",
        "structured_data",
        "transactions：",
        "transaction_id",
        "is_fee",
        "is_loan_related",
        "true",
        "false",
        "null",
        "{",
        "}",
        "[",
        "]",
    ]
    for item in forbidden:
        assert item not in markdown


def test_profile_markdown_formatter_short_circuits_bank_reconciliation_detail() -> None:
    class Storage:
        async def get_document(self, doc_id: str) -> dict:
            return {"doc_id": doc_id, "file_name": "工商银行对账明细202504-202603.xlsx", "file_path": ""}

    dirty_data = {
        "doc_type": "bank_reconciliation_detail",
        "display_markdown": "\n".join(
            [
                "## 银行对账明细",
                "",
                "- 资料类型：银行对账明细",
                "",
                "data：{\"display_markdown\":\"...\"}",
                "structured data：{\"transactions\":[{\"is_fee\": true}]}",
            ]
        ),
        "markdown": "markdown：## 银行对账明细...",
        "report_markdown": "report markdown：## 银行对账明细...",
        "structured_data": {
            "doc_type": "bank_reconciliation_detail",
            "transactions": [{"transaction_id": "1", "is_loan_related": False}],
        },
        "data": {"display_markdown": "## 银行对账明细\n\n- 资料类型：银行对账明细"},
    }

    section, _source = asyncio.run(
        _build_single_document_section(
            Storage(),
            "customer_1",
            {
                "extraction_type": "bank_reconciliation_detail",
                "doc_id": "doc_1",
                "extracted_data": dirty_data,
            },
        )
    )

    assert section.count("## 银行对账明细") == 1
    forbidden = [
        "data：",
        "display markdown",
        "report markdown",
        "structured data",
        "transactions",
        "transaction_id",
        "is_fee",
        "is_loan_related",
        "true",
        "false",
        "{",
        "}",
        "[",
    ]
    for item in forbidden:
        assert item not in section
