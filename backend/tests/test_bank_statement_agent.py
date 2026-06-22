from backend.document_types import normalize_document_type_code
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code


HEADER = ["凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号", "用途", "摘要", "备注", "回单个性化信息"]


def sample_pages():
    base = """中国工商银行账户明细清单
本方账号：1001068319100134987 本方账号户名：上海慧川建筑科技有限公司
币种：人民币 单位：元 本方账号开户行：工行张江科技秀沿路支行
记账时间范围：20250401 - 20250930"""
    rows = [
        HEADER,
        ["P1", "622201", "2025-04-02 10:00:00", "贷", "上海项目公司", "102", "工程款", "项目款", "", "指令编号:A1"],
        ["P2", "622202", "2025-05-03", "借", "中国工商银行", "", "手续费", "跨行汇款手续费", "", "费用名称:跨行汇款手续费 实收金额:200.00 应收金额:220.00"],
        ["P3", "622203", "2025-06-04", "贷", "中国工商银行", "", "融资", "贷款发放", "", "贷款账号:123 借据编号:J1"],
        ["P4", "622204", "2025-07-05", "借", "中国工商银行", "", "还款", "贷款归还", "", "贷款帐号:456"],
        ["P5", "622205", "2025-08-06", "借", "往来单位", "", "往来款", "普通汇兑", "", ""],
        ["P6", "622206", "2025-09-07", "借", "股东", "", "借款", "", "", ""],
    ]
    second = base.replace("20250401 - 20250930", "20251001 - 20260331")
    return [
        {"page": 1, "text": base, "table_rows": rows, "source": "pdf_layout"},
        {"page": 2, "text": second, "table_rows": [], "source": "pdf_layout"},
        *[{"page": page, "text": "中国工商银行账户明细清单", "table_rows": [], "source": "pdf_layout"} for page in range(3, 8)],
    ]


def test_bank_statement_type_priority_and_registry():
    text = "中国工商银行账户明细清单 银行流水明细"
    assert detect_document_type_code(text, filename="工商银行对账单202504-202603.pdf") == "bank_statement"
    assert detect_document_type_code(text, explicit_type="enterprise_flow", filename="sample.pdf") == "bank_statement"
    assert normalize_document_type_code("银行账户明细") == "bank_statement"
    result = run_document_extraction_agent("bank_statement", text, "sample.pdf", metadata={"raw_pages": sample_pages()})
    assert result.agent_name == "bank_statement_agent"
    assert result.debug["skill_name"] == "bank_statement_skill"


def test_bank_statement_extracts_core_fields_periods_and_safe_amounts():
    content = build_structured_extraction(
        "\n".join(page["text"] for page in sample_pages()),
        "bank_statement",
        raw_pages=sample_pages(),
        filename="工商银行对账单202504-202603.pdf",
    )
    data = content["extracted_json"]
    assert data["doc_type"] == "bank_statement"
    assert data["doc_type_name"] == "银行对账单"
    assert data["agent_type"] == "bank_statement_agent"
    assert data["bank_name"] == "中国工商银行"
    assert data["statement_title"] == "中国工商银行账户明细清单"
    assert data["account_no"] == "1001068319100134987"
    assert data["account_name"] == "上海慧川建筑科技有限公司"
    assert data["opening_bank"] == "工行张江科技秀沿路支行"
    assert data["currency"] == "人民币"
    assert data["unit"] == "元"
    assert data["period_start"] == "2025-04-01"
    assert data["period_end"] == "2026-03-31"
    assert data["page_count"] == 7
    assert data["amount_recognition_status"] == "部分识别"
    assert data["transactions"][0]["金额"] is None
    fee = next(tx for tx in data["transactions"] if tx["交易分类"] == "银行手续费")
    assert fee["金额"] == 200.0
    assert {tx["交易分类"] for tx in data["transactions"]} >= {"经营收入", "银行手续费", "贷款发放", "贷款归还", "往来款", "资金拆借"}


def test_bank_statement_markdown_is_chinese_and_not_json():
    content = build_structured_extraction("中国工商银行账户明细清单", "bank_statement", raw_pages=sample_pages(), filename="sample.pdf")
    markdown = content["display_markdown"]
    for heading in ("## 银行对账单", "### 账户信息", "### 汇总信息", "### 交易明细摘要", "### 月度汇总", "### 交易分类汇总", "### 主要交易对手", "### 交易明细", "### 风险提示", "### 需人工复核"):
        assert heading in markdown
    for forbidden in ('"doc_type"', "transaction_category", "raw_text", "undefined", "null", "None"):
        assert forbidden not in markdown
    assert "不生成完整收入、支出和净流入统计" in markdown


def native_icbc_pages():
    return [
        {
            "page": 1,
            "text": """中国工商银行账户明细清单
账号： 1001068319100134987
时间范围： 20250401 - 20250930
000000000 1001068310000011681 2025-04-27 15:33:17
处理种类:贷款发放 贷款账号:1001068310000011681 借据编号:J001
000000001 03005029359 2025-04-28 09:29:05
附言:往来款 支付交易序号:25990180 报文种类:大额客户""",
            "table_rows": [],
            "source": "pdf_layout",
        },
        {"page": 4, "text": "中国工商银行账户明细清单\n时间范围：20251001－20260331", "table_rows": [], "source": "pdf_layout"},
        {"page": 7, "text": "000000099 03005029999 2026-03-24 18:20:01\n费用名称:跨行汇款手续费 实收金额:40.00", "table_rows": [], "source": "pdf_layout"},
        *[{"page": page, "text": "", "table_rows": [], "source": "pdf_layout"} for page in (2, 3, 5, 6)],
    ]


def test_icbc_native_text_anchor_without_debit_credit_column():
    pages = native_icbc_pages()
    content = build_structured_extraction("\n".join(page["text"] for page in pages), "bank_statement", raw_pages=pages, filename="工商银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["account_no"] == "1001068319100134987"
    assert data["bank_name"] == "中国工商银行"
    assert data["currency"] == "人民币"
    assert data["unit"] == "元"
    assert data["period_text"] == "2025-04-01 至 2026-03-31"
    assert data["page_count"] == 7
    assert data["transaction_count"] == 3
    assert data["first_transaction_date"] == "2025-04-27"
    assert data["last_transaction_date"] == "2026-03-24"
    assert data["transactions"][0]["debit_credit_flag"] == "未识别"
    assert data["amount_recognition_status"] == "部分识别"
    assert "- 账号：1001068319100134987" in content["markdown_summary"]
    assert "- 交易明细总数：3" in content["markdown_summary"]


def test_profile_uses_saved_bank_statement_agent_markdown():
    import asyncio
    from backend.services.markdown_profile_service import _build_single_document_section

    markdown = "## 银行对账单\n\n### 账户信息\n- 账号：1001068319100134987\n\n### 交易明细\n| 序号 | 交易时间 |\n|---:|---|\n| 1 | 2025-04-27 |"

    class Storage:
        async def get_document(self, _doc_id):
            return {"file_name": "工商银行对账单202504-202603.pdf", "file_path": "D:/stored/sample.pdf"}

    extraction = {
        "extraction_type": "bank_statement",
        "doc_id": "doc-1",
        "extracted_data": {
            "extracted_json": {"doc_type": "bank_statement", "account_no": "1001068319100134987"},
            "markdown_summary": markdown,
        },
    }
    rendered, _source = asyncio.run(_build_single_document_section(Storage(), "customer-1", extraction))
    assert rendered == markdown
