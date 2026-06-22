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
        ["P2", "622202", "2025-05-03 11:00:00", "借", "中国工商银行", "", "手续费", "跨行汇款手续费", "", "费用名称:跨行汇款手续费 实收金额:200.00 应收金额:220.00"],
        ["P3", "622203", "2025-06-04 12:00:00", "贷", "中国工商银行", "", "融资", "贷款发放", "", "贷款账号:123 借据编号:J1"],
        ["P4", "622204", "2025-07-05 13:00:00", "借", "中国工商银行", "", "还款", "贷款归还", "", "贷款帐号:456"],
        ["P5", "622205", "2025-08-06 14:00:00", "借", "往来单位", "", "往来款", "普通汇兑", "", ""],
        ["P6", "622206", "2025-09-07 15:00:00", "借", "股东单位", "", "借款", "", "", ""],
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
    fee = next(tx for tx in data["transactions"] if tx["交易分类"] == "银行费用")
    assert fee["金额"] == 200.0
    assert {tx["交易分类"] for tx in data["transactions"]} >= {"经营入账", "银行费用", "贷款发放", "贷款归还", "往来出账", "资金拆借"}


def test_bank_statement_markdown_is_chinese_and_not_json():
    content = build_structured_extraction("中国工商银行账户明细清单", "bank_statement", raw_pages=sample_pages(), filename="sample.pdf")
    markdown = content["display_markdown"]
    for heading in ("## 银行对账单", "### 账户信息", "### 流水分析摘要", "### 有效经营入账方汇总", "### 有效经营入账明细", "### 有效经营出账方汇总", "### 有效经营出账明细", "### 剔除项汇总", "### 贷款及融资相关交易", "### 银行费用及利息", "### 重点交易明细", "### 风险提示", "### 需人工复核"):
        assert heading in markdown
    for forbidden in ('"doc_type"', "transaction_category", "raw_text", "undefined", "null", "None"):
        assert forbidden not in markdown
    assert "不进行完整收入、支出和净流入测算" in markdown


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
    assert "- 原始交易笔数：3" in content["markdown_summary"]


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


def test_effective_flow_excludes_self_transfers_garbage_and_invalid_dates():
    header = ["凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号", "用途", "摘要", "备注", "回单个性化信息"]
    rows = [
        header,
        ["100001", "600001", "2025-04-27 09:00:00", "贷", "上海 意川 建筑 科技 有限 公司", "", "材料款", "项目回款", "", "指令编号:HQP928010352934"],
        ["100002", "600002", "2025-05-02 10:00:00", "贷", "上海真实客户有限公司", "", "工程款", "项目款", "", "支付交易序号:100"],
        ["100003", "600003", "2025-05-03 11:00:00", "借", "上海真实供应商有限公司", "", "灯具款", "材料采购", "", "提交人:w1910013498700001.c.1001"],
        ["100004", "600004", "2025-05-04 12:00:00", "贷", ":HQP928010352934", "", "往来款", "", "", ""],
        ["100005", "600005", "2025-04-01", "贷", "日期伪交易", "", "项目款", "", "", ""],
        ["100006", "600006", "1910-01-34 12:00:00", "贷", "异常日期单位", "", "项目款", "", "", ""],
        ["100007", "600007", "2025-06-01 13:00:00", "借", "中国工商银行", "", "手续费", "跨行汇款手续费", "", "实收金额:40.00 报文种类:大额客户"],
        ["100008", "600008", "2025-07-01 14:00:00", "贷", "中国工商银行", "", "融资", "贷款发放", "", "借据编号:J001"],
    ]
    pages = [{
        "page": 1,
        "text": "中国工商银行账户明细清单\n账号：1001068319100134987\n本方账号户名：上海意川建筑科技有限公司 币种：人民币 单位：元\n时间范围：20250401 - 20260331",
        "table_rows": rows,
        "source": "pdf_layout",
    }]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="sample.pdf")
    data = content["extracted_json"]
    self_tx = next(tx for tx in data["transactions"] if tx["voucher_no"] == "100001")
    expense_tx = next(tx for tx in data["transactions"] if tx["voucher_no"] == "100003")
    assert self_tx["is_self_transfer"] is True
    assert self_tx["exclude_from_effective_flow"] is True
    assert "本方户名与对方单位一致" in self_tx["exclude_reason"]
    assert expense_tx["category"] == "经营出账"
    assert data["first_transaction_date"] == "2025-04-27"
    assert data["last_transaction_date"] == "2025-07-01"
    assert data["self_transfer_count"] == 1
    assert data["effective_transaction_count"] == 2
    names = {item["counterparty"] for item in data["counterparty_summary"]}
    assert names == {"上海真实客户有限公司", "上海真实供应商有限公司"}
    markdown = content["markdown_summary"]
    assert "### 剔除项汇总" in markdown
    assert "上海意川建筑科技有限公司" not in markdown.split("### 有效经营入账方汇总", 1)[1].split("### 有效经营出账方汇总", 1)[0]
    for forbidden in ("回单个性化信息", "指令编号", "支付交易序号", "报文种类", "提交人", "HQP928", "w191001", "1910-01-34"):
        assert forbidden not in markdown


def test_opening_bank_suffix_normalization_and_operating_detail_layers():
    from backend.extraction_skills.bank_statement import normalize_opening_bank_name

    assert normalize_opening_bank_name("工行 张江科技秀沿路", "中国工商银行") == "工行张江科技秀沿路支行"
    assert normalize_opening_bank_name("工行张江科技秀沿路支行", "中国工商银行") == "工行张江科技秀沿路支行"
    assert normalize_opening_bank_name("中国工商银行上海分行", "中国工商银行") == "中国工商银行上海分行"
    header = ["凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号", "用途", "摘要", "备注", "回单个性化信息"]
    dates = [
        "2025-09-26 11:50:10", "2025-11-04 14:36:25", "2025-11-28 13:15:40",
        "2025-12-25 11:50:30", "2026-02-11 13:29:20",
    ]
    purposes = ["张江A04C-01", "张江A04C-01", "张江创新药A04C-01", "张江创新药基地A04C-01工程款", "张江创新药基地A04C-01工程款"]
    rows = [header] + [
        [str(200000 + index), str(700000 + index), trade_time, "贷", "上海建工智慧营造有限公司", "", purpose, "工程款", "", ""]
        for index, (trade_time, purpose) in enumerate(zip(dates, purposes), start=1)
    ]
    pages = [{
        "page": 1,
        "text": "中国工商银行账户明细清单\n账号：1001068319100134987\n本方账号户名：上海意川建筑科技有限公司\n本方账号开户行：工行张江科技秀沿路\n时间范围：20250401 - 20260331",
        "table_rows": rows,
        "source": "pdf_layout",
    }]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="sample.pdf")
    data = content["extracted_json"]
    assert data["opening_bank"] == "工行张江科技秀沿路支行"
    assert data["effective_inflow_count"] == 5
    assert data["effective_operating_inflow_count"] == 5
    assert data["effective_operating_inflow_counterparty_count"] == 1
    assert data["effective_inflow_counterparties"][0]["count"] == 5
    assert len(data["effective_operating_inflow_transactions"]) == 5
    markdown = content["markdown_summary"]
    assert "- 有效经营入账方数量：1" in markdown
    assert "- 有效入账笔数：5" in markdown
    assert "上海建工智慧营造有限公司 | 5 | 未识别" in markdown
    assert markdown.count("上海建工智慧营造有限公司") >= 6


def test_shanghai_bank_adapter_uses_table_columns_and_filename_period():
    header = ["交易日期", "摘要", "借方发生额", "贷方发生额", "余额", "对方账号", "对方户名", "用途", "流水号"]
    rows = [
        header,
        ["2025-04-08", "工程款", "--", "1,234.56", "9,999.00", "880001", "上海客户甲有限公司", "项目回款", "S001"],
        ["2025/04/09", "材料款", "￥500.00", "", "9,499.00", "880002", "上海供应商乙有限公司", "材料采购", "S002"],
        ["2025.04.10", "同户划转", "", "200.00元", "9,699.00", "880003", "上海测试科技有限公司", "转账", "S003"],
        ["2025-04-11", "账户管理费", "10.00", "", "9,689.00", "", "上海银行", "费用", "S004"],
        ["2026-12-08", "OCR错误日期", "", "100.00", "9,789.00", "880005", "错误单位", "项目款", "S005"],
    ]
    pages = [{
        "page": 1,
        "text": "上海银行股份有限公司\n上海银行对账单\n账户号：03005029359\n客户名称：上海测试科技有限公司\n开户网点：上海银行张江支行\n2010-00-10 2026-12-08",
        "table_rows": rows,
        "source": "pdf_layout",
    }] + [{"page": page, "text": "上海银行交易明细", "table_rows": [], "source": "pdf_layout"} for page in range(2, 30)]
    content = build_structured_extraction("\n".join(page["text"] for page in pages), "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["bank_format"] == "shanghai_bank"
    assert data["bank_name"] == "上海银行"
    assert data["statement_title"] == "上海银行对账单"
    assert data["account_no"] == "03005029359"
    assert data["account_name"] == "上海测试科技有限公司"
    assert data["opening_bank"] == "上海银行张江支行"
    assert data["period_start"] == "2025-04-01"
    assert data["period_end"] == "2026-03-31"
    assert data["page_count"] == 29
    assert data["valid_transaction_count"] == 4
    assert data["raw_transaction_count"] == 5
    assert data["ocr_anomaly_count"] == 1
    assert data["candidate_transaction_rows"] == 5
    assert data["ocr_abnormal_rows"] == 1
    assert data["raw_text_blocks_count"] < 942
    assert data["amount_recognition_status"] == "完整识别"
    assert data["self_transfer_count"] == 1
    assert data["effective_operating_inflow_count"] == 1
    assert data["effective_operating_outflow_count"] == 1
    assert data["transactions"][0]["direction"] == "入账"
    assert data["transactions"][0]["amount"] == 1234.56
    assert "2010-00-10" not in content["markdown_summary"]
    assert "2026-12-08" not in content["markdown_summary"]


def test_shanghai_bank_failure_diagnostic_does_not_render_empty_tables():
    pages = [{"page": 1, "text": "上海银行对账单\n客户名称：测试公司", "table_rows": [], "source": "ocr"}]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    markdown = content["markdown_summary"]
    assert "### 解析诊断" in markdown
    assert "- 银行格式：上海银行" in markdown
    assert "### 有效经营入账方汇总" not in markdown


def test_shanghai_bank_coordinate_header_and_column_recovery():
    def box(x0, y0, x1, y1, text):
        return {"x0": x0, "y0": y0, "x1": x1, "y1": y1, "text": text, "confidence": 0.99}

    boxes = [
        box(10, 10, 220, 25, "上海银行对账单"),
        box(10, 35, 220, 50, "账户号：03005029359"),
        box(250, 35, 520, 50, "客户名称：上海测试科技有限公司"),
        box(10, 60, 280, 75, "开户网点：上海银行张江支行"),
        box(10, 100, 90, 118, "交易日期"), box(110, 100, 180, 118, "摘要"),
        box(220, 100, 300, 118, "借方发生额"), box(330, 100, 410, 118, "贷方发生额"),
        box(440, 100, 500, 118, "余额"), box(530, 100, 610, 118, "对方账号"),
        box(650, 100, 740, 118, "对方户名"),
        box(10, 130, 90, 148, "2025-04-08"), box(110, 130, 180, 148, "工程款"),
        box(220, 130, 300, 148, "--"), box(330, 130, 410, 148, "1,234.56"),
        box(440, 130, 500, 148, "9,999.00"), box(530, 130, 610, 148, "880001"),
        box(650, 130, 800, 148, "上海客户甲有限公司"),
        box(10, 160, 90, 178, "04-09"), box(110, 160, 180, 178, "材料款"),
        box(220, 160, 300, 178, "500.00"), box(330, 160, 410, 178, "--"),
        box(440, 160, 500, 178, "9,499.00"), box(530, 160, 610, 178, "880002"),
        box(650, 160, 810, 178, "上海供应商乙有限公司"),
    ]
    pages = [{
        "page": 1, "text": "上海银行对账单\n账户号：03005029359\n客户名称：上海测试科技有限公司\n开户网点：上海银行张江支行",
        "text_boxes": boxes, "page_width": 850, "page_height": 1000, "table_rows": [], "source": "ocr_with_locations",
    }]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["parse_diagnostics"]["parser_path"] == "coordinate_table"
    assert data["parse_diagnostics"]["page_lines_count"] >= 6
    assert data["candidate_transaction_rows"] == 2
    assert data["valid_transaction_count"] == 2
    assert data["ocr_abnormal_rows"] == 0
    assert data["account_no"] == "03005029359"
    assert data["account_name"] == "上海测试科技有限公司"
    assert data["opening_bank"] == "上海银行张江支行"
    assert [tx["direction"] for tx in data["transactions"]] == ["入账", "出账"]
    assert [tx["amount"] for tx in data["transactions"]] == [1234.56, 500.0]
    assert data["transactions"][1]["transaction_time"] == "2025-04-09"
