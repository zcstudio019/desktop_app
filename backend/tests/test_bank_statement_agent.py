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


def test_shanghai_bank_native_text_row_parser_uses_header_and_serial_rows():
    text = """账户明细查询
记账日期: 2025-04-01---2026-03-31
选择账号: 03005029359 开户行: 上海银行浦西支行营业部 币种: 人民币 上海意川建筑科技 有限公司
借方总金额: 110,247,648.48 总笔数: 684 借方总笔数: 565 贷方总笔数: 119 贷方总金额: 107,714,789.82
交易流水号 交易时间 记账日期 交易方向 交易金额 余额 对手账号 对手名称 摘要 交易用途 备注
FT25091864343793 2025-04-01 11:12:46 2025-04-01 出账(借方) 25,184.84 2,536,212.12 6230520710081831770 陆德斐 跨行转账 奔驰车分期款
FT25091690973952 2025-04-01 14:27:53 2025-04-01 出账(借方) 200,000.00 2,336,212.12 32050176624200001278 靖江市桐梧贸易有限公司 跨行转账 临空项目2024.12月A2区防火包裹材料款
FT25092611111111 2025-09-26 11:50:10 2025-09-26 入账(贷方) 300,000.00 2,636,212.12 31001515100050022023 上海建工智慧营造有限公司 跨行转账 张江A04C-01工程款
BEA25107140212966000
1 2025-04-17 14:02:12 2025-04-17 出账(借方) 100,000.00 2,236,212.12 31050178360000008649上海意川建筑科技有限 公司 跨行转账 本方划转
"""
    pages = [{"page": 1, "text": text, "table_rows": [], "source": "pdf_native"}]
    content = build_structured_extraction(text, "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["parse_diagnostics"]["parser_path"] == "native_text_row"
    assert data["account_name"] == "上海意川建筑科技有限公司"
    assert data["opening_bank"] == "上海银行浦西支行营业部"
    assert data["account_no"] == "03005029359"
    assert data["period_start"] == "2025-04-01"
    assert data["period_end"] == "2026-03-31"
    assert data["raw_transaction_count"] == 684
    assert data["transaction_count"] == 684
    assert data["debit_count"] == 565
    assert data["credit_count"] == 119
    assert data["debit_total_amount"] == 110247648.48
    assert data["credit_total_amount"] == 107714789.82
    assert data["amount_recognition_status"] == "完整识别"
    assert data["valid_transaction_count"] == 4
    assert data["effective_operating_inflow_count"] == 1
    assert data["effective_operating_outflow_count"] == 1
    assert data["self_transfer_count"] == 1
    assert any(tx["counterparty_name"] == "上海意川建筑科技有限公司" and tx["is_self_transfer"] for tx in data["transactions"])
    assert "企业网上银行" not in "\n".join(item["counterparty"] for item in data["effective_outflow_counterparties"])
    assert "交易用途 摘要 对手名称 对手账号 余额" not in content["markdown_summary"]


def test_shanghai_bank_native_source_overrides_column_block_raw_pages():
    raw_text = """账户明细查询
记账日期: 2025-04-01---2026-03-31
选择账号: 03005029359 开户行: 上海银行浦西支行营业部 币种: 人民币 上海意川建筑科技 有限公司
借方总金额: 110,247,648.48 总笔数: 684 借方总笔数: 565 贷方总笔数: 119 贷方总金额: 107,714,789.82
交易流水号 交易时间 记账日期 交易方向 交易金额 余额 对手账号 对手名称 摘要 交易用途 备注
V025121995999209 2025-12-21 10:01:02 2025-12-21 入账(贷方) 88,000.00 1,088,000.00 31001515100050022023上海建工智慧营造有限公司 跨行转账 张江项目工程款
G025122114005920 2025-12-21 14:00:00 2025-12-21 出账(借方) 66.00 1,087,934.00 企业网上银行跨行同城 转账 2025年12月企业网上银行跨行同城转账手续费
"""
    raw_pages = [{"page": 1, "text": "上海银行对账单\n交易用途\n报销\n摘要\n跨行转账\n对手名称\n企业网上银行\n余额", "table_rows": [], "source": "ocr_column_block"}]
    content = build_structured_extraction(raw_text, "bank_statement", raw_pages=raw_pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["parse_diagnostics"]["parser_path"] == "native_text_row"
    assert data["account_name"] == "上海意川建筑科技有限公司"
    assert data["opening_bank"] == "上海银行浦西支行营业部"
    assert data["account_no"] == "03005029359"
    assert data["raw_transaction_count"] == 684
    assert data["valid_transaction_count"] == 2
    assert data["amount_recognition_status"] == "完整识别"
    assert data["effective_operating_inflow_count"] == 1
    assert data["bank_fee_count"] == 1
    assert "上海银行对账单采用列块恢复方式解析" not in content["markdown_summary"]
    assert "币种:03005029359" not in content["markdown_summary"]


def test_shanghai_bank_header_cleaning_removes_polluted_opening_bank_fragments():
    from backend.extraction_skills.bank_statement import clean_opening_bank, clean_account_name

    account_name = clean_account_name("上海意川建筑科技 有限公司")
    polluted = "币种:03005029359上海银行浦西支行营业部人民币上海意川建筑科技有限公司"
    assert account_name == "上海意川建筑科技有限公司"
    assert clean_opening_bank(polluted, account_no="03005029359", account_name=account_name) == "上海银行浦西支行营业部"

    text = """账户明细查询
记账日期: 2025-04-01---2026-03-31
选择账号: 03005029359 开户行: 上海银行浦西支行营业部 币种: 人民币 上海意川建筑科技 有限公司
借方总金额: 110,247,648.48 总笔数: 684 借方总笔数: 565 贷方总笔数: 119 贷方总金额: 107,714,789.82
交易流水号 交易时间 记账日期 交易方向 交易金额 余额 对手账号 对手名称 摘要 交易用途 备注
FT25091864343793 2025-04-01 11:12:46 2025-04-01 出账(借方) 25,184.84 2,536,212.12 6230520710081831770 陆德斐 跨行转账 奔驰车分期款
"""
    content = build_structured_extraction(text, "bank_statement", raw_pages=[{"page": 1, "text": text, "table_rows": []}], filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["opening_bank"] == "上海银行浦西支行营业部"
    assert "币种" not in data["opening_bank"]
    assert "03005029359" not in data["opening_bank"]
    assert data["account_name"] not in data["opening_bank"]


def test_related_person_transfer_is_excluded_only_when_profile_matches():
    text = """账户明细查询
记账日期: 2025-04-01---2026-03-31
选择账号: 03005029359 开户行: 上海银行浦西支行营业部 币种: 人民币 上海意川建筑科技 有限公司
借方总金额: 110,247,648.48 总笔数: 684 借方总笔数: 565 贷方总笔数: 119 贷方总金额: 107,714,789.82
交易流水号 交易时间 记账日期 交易方向 交易金额 余额 对手账号 对手名称 摘要 交易用途 备注
FT25091864343793 2025-04-01 11:12:46 2025-04-01 出账(借方) 25,184.84 2,536,212.12 6230520710081831770 陆德斐 跨行转账 材料款
FT25091690973952 2025-04-01 14:27:53 2025-04-01 出账(借方) 20,000.00 2,516,212.12 6230520710081831771 黎云 跨行转账 材料款
FT25092611111111 2025-09-26 11:50:10 2025-09-26 入账(贷方) 300,000.00 2,816,212.12 31001515100050022023 上海建工智慧营造有限公司 跨行转账 张江项目工程款
"""
    result = run_document_extraction_agent(
        "bank_statement",
        text,
        "上海银行对账单202504-202603.pdf",
        metadata={
            "raw_pages": [{"page": 1, "text": text, "table_rows": [], "source": "pdf_native"}],
            "customer_profile": {"legal_representative_name": "陆德斐"},
        },
    )
    data = result.extracted_json
    related = next(tx for tx in data["transactions"] if tx["counterparty_name"] == "陆德斐")
    unrelated = next(tx for tx in data["transactions"] if tx["counterparty_name"] == "黎云")
    assert related["is_related_person_transfer"] is True
    assert related["related_person_role"] == "法定代表人"
    assert related["exclude_from_effective_flow"] is True
    assert "公司账户与法人/关联人之间转账" in related["exclude_reason"]
    assert unrelated.get("is_related_person_transfer") is False
    assert data["related_person_transfer_count"] == 1
    assert data["effective_operating_outflow_count"] == 1
    assert data["effective_outflow_counterparties"][0]["counterparty"] == "黎云"
    markdown = result.markdown_summary
    assert "### 关联人及内部往来" in markdown
    assert "| 关联人转账 | 2025-04-01 11:12:46 | 出账 | 陆德斐 | 法定代表人 | 跨行转账 | 材料款 | 25,184.84 |" in markdown
    assert "| 关联人转账 | 1 | 公司账户与法人/实控人/股东/高管等关联个人之间的转账 |" in markdown
    assert "存在公司账户与法人/关联人之间的资金往来" in markdown


def test_generic_bank_statement_rejects_counterparty_list_as_account_name():
    text = """银行交易明细
交易日期 对方户名 对方账号 摘要 交易金额 余额
2024-05-13 百威（中国）销售有限公司 622201 单位国内汇款手续费 100.00 900.00
2024-05-13 福建律动信息技术有限公司 622202 单位国内汇款手续费 200.00 700.00
2024-05-13 福建西优网络有限公司 622203 单位国内汇款手续费 300.00 400.00
2024-05-13 上海告趣信息科技有限公司 622204 单位国内汇款手续费 400.00 0.00
"""
    content = build_structured_extraction(
        text,
        "bank_statement",
        raw_pages=[{"page": 1, "text": text, "table_rows": [], "source": "ocr"}],
        filename="31006662901300306458920240513_1.pdf",
    )
    data = content["extracted_json"]
    assert data["bank_format"] == "generic_bank_statement"
    assert data["account_name"] == ""
    assert data["customer_name"] == ""
    assert "百威（中国）销售有限公司 福建律动信息技术有限公司" not in content["markdown_summary"]
    assert "## 银行流水文件" in content["markdown_summary"]
    assert "- 提取状态：部分成功" in content["markdown_summary"]
    assert "未形成标准账户流水明细" in content["markdown_summary"]
    assert data["account_no"] == ""
    assert data["bank_name"] == ""


def test_bocm_statement_is_not_misrouted_to_receipt_bundle_and_parses_header_transactions():
    header = ["序号", "会计日期", "交易日期", "交易名称", "凭证种类", "凭证号码", "借方发生额", "贷方发生额", "余额", "卡号", "交易地点", "对方账号", "对方户名", "对方行名", "摘要", "流水号"]
    rows = [
        header,
        ["1", "20240513", "20240513", "单位国内汇款手续费", "", "V001", "766.14", "", "145,209.67", "", "上海", "622200001", "上海顺衡物流有限公司", "中国工商银行股份有限公司上海市习勤路支行", "物流费", "L001"],
        ["2", "20240513", "20240513", "单位国内汇款", "", "V002", "", "206,360.00", "351,569.67", "", "上海", "622200002", "上海汇付支付有限公司", "中国建设银行股份有限公司上海第五支行", "货款", "L002"],
    ]
    text = """交通银行上海市分行明细对账单
开户机构：交通银行上海长宁支行
账号：310066629013003064589
户名：上海乐芙兰电子商务有限公司
年份：2024
月份：05
币种：人民币
序号 会计日期 交易日期 交易名称 凭证种类 凭证号码 借方发生额 贷方发生额 余额 卡号 交易地点 对方账号 对方户名 对方行名 摘要 流水号
1 20240513 20240513 单位国内汇款手续费 766.14 145,209.67 622200001 上海顺衡物流有限公司 中国工商银行股份有限公司上海市习勤路支行 物流费 L001
2 20240513 20240513 单位国内汇款 206,360.00 351,569.67 622200002 上海汇付支付有限公司 中国建设银行股份有限公司上海第五支行 货款 L002
本月累计借方发生额：10,871,650.78
"""
    assert detect_document_type_code(text, filename="31006662901300306458920240520_2.pdf") == "bank_statement"
    content = build_structured_extraction(
        text,
        "bank_statement",
        raw_pages=[{"page": 1, "text": text, "table_rows": rows, "source": "pdf_layout"}],
        filename="31006662901300306458920240520_2.pdf",
    )
    data = content["extracted_json"]
    assert data["doc_type"] == "bank_statement"
    assert data["bank_format"] == "bocm_statement"
    assert data["statement_subtype"] == "account_statement"
    assert data["agent_type"] == "bank_statement_agent"
    assert data["bank_name"] == "交通银行"
    assert data["statement_title"] == "交通银行上海市分行明细对账单"
    assert data["opening_bank"] == "交通银行上海长宁支行"
    assert data["account_no"] == "310066629013003064589"
    assert data["account_name"] == "上海乐芙兰电子商务有限公司"
    assert data["currency"] == "人民币"
    assert data["period_start"] == "2024-05-01"
    assert data["period_end"] == "2024-05-31"
    assert data["valid_transaction_count"] == 2
    assert data["transactions"][0]["收支方向"] == "出账"
    assert data["transactions"][0]["金额"] == 766.14
    assert data["transactions"][1]["收支方向"] == "入账"
    assert data["transactions"][1]["金额"] == 206360.0
    assert data["transactions"][0]["counterparty_bank"] == "中国工商银行股份有限公司上海市习勤路支行"
    assert data["bank_name"] != "中国工商银行"
    assert data["amount_recognition_status"] == "完整识别"
    markdown = content["display_markdown"]
    assert "## 银行对账单" in markdown
    assert "交通银行" in markdown
    assert '"doc_type"' not in markdown
    assert "raw_text" not in markdown


def test_bocm_statement_native_text_fallback_parses_without_table_rows():
    text = """交通银行上海市分行明细对账单
开户机构：交通银行上海长宁支行
账号：310066629013003064589
户名：上海乐芙兰电子商务有限公司
年份：2024
月份：05
币种：人民币
承前余额 145,975.81
序号 会计日期 交易日期 交易名称 凭证种类 凭证号码 借方发生额 贷方发生额 余额 卡号 交易地点 对方账号 对方户名 对方行名 摘要 流水号
1 20240513 20240513 单位国内汇款手续费 766.14 145,209.67 622200001 上海顺衡物流有限公司 中国工商银行股份有限公司上海市习勤路支行 物流费 L001
2 20240513 20240513 单位国内汇款 206,360.00 351,569.67 622200002 上海汇付支付有限公司 中国建设银行股份有限公司上海第五支行 货款 L002
本月累计借方发生额：10,871,650.78
"""
    content = build_structured_extraction(
        text,
        "bank_statement",
        raw_pages=[{"page": 1, "text": text, "table_rows": [], "source": "pdf_text"}],
        filename="31006662901300306458920240520_2.pdf",
    )
    data = content["extracted_json"]
    assert data["bank_format"] == "bocm_statement"
    assert data["bank_name"] == "交通银行"
    assert data["account_no"] == "310066629013003064589"
    assert data["account_name"] == "上海乐芙兰电子商务有限公司"
    assert data["valid_transaction_count"] == 2
    assert data["transactions"][0]["收支方向"] == "出账"
    assert data["transactions"][0]["对方单位"] == "上海顺衡物流有限公司"
    assert data["transactions"][0]["对方行号"] == "中国工商银行股份有限公司上海市习勤路支行"
    assert data["transactions"][0]["金额"] == 766.14
    assert data["transactions"][1]["收支方向"] == "入账"
    assert data["transactions"][1]["对方单位"] == "上海汇付支付有限公司"
    assert data["transactions"][1]["金额"] == 206360.0


def test_shanghai_bank_failure_diagnostic_does_not_render_empty_tables():
    pages = [{"page": 1, "text": "上海银行对账单\n客户名称：测试公司", "table_rows": [], "source": "ocr"}]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    markdown = content["markdown_summary"]
    assert "## 银行流水文件" in markdown
    assert "- 提取状态：部分成功" in markdown
    assert "未形成标准账户流水明细" in markdown
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


def test_shanghai_bank_date_anchor_fallback_for_whole_line_ocr_boxes():
    def box(y0, text):
        return {"x0": 10, "y0": y0, "x1": 830, "y1": y0 + 16, "text": text, "confidence": 0.9}

    boxes = [
        box(10, "上海银行对账单"), box(30, "账号"), box(50, "03005029359"),
        box(70, "客户名称"), box(90, "上海测试科技有限公司"),
        box(110, "开户网点"), box(130, "上海银行张江支行"),
        box(180, "交易日期 摘要 借方发生额 贷方发生额 余额 对方账号 对方户名"),
        box(210, "2025-04-08 工程款 0.00 1,234.56 9,999.00 880001001 上海客户甲有限公司"),
        box(240, "04/09 材料款 500.00 0.00 9,499.00 880002002 上海供应商乙有限公司"),
    ]
    pages = [{
        "page": 1, "text": "\n".join(item["text"] for item in boxes), "text_boxes": boxes,
        "page_width": 850, "page_height": 1000, "table_rows": [], "source": "ocr_with_locations",
    }]
    content = build_structured_extraction(pages[0]["text"], "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["account_no"] == "03005029359"
    assert data["account_name"] == "上海测试科技有限公司"
    assert data["opening_bank"] == "上海银行张江支行"
    assert data["candidate_transaction_rows"] == 2
    assert data["valid_transaction_count"] == 2
    assert [tx["direction"] for tx in data["transactions"]] == ["入账", "出账"]
    assert [tx["amount"] for tx in data["transactions"]] == [1234.56, 500.0]


def test_shanghai_bank_forces_date_anchor_fallback_when_data_page_has_no_header():
    def box(y0, text):
        return {"x0": 10, "y0": y0, "x1": 830, "y1": y0 + 16, "text": text, "confidence": 0.9}

    page1_boxes = [
        box(10, "上海银行对账单"), box(30, "账号 03005029359"),
        box(50, "客户名称 上海测试科技有限公司"), box(70, "开户网点 上海银行张江支行"),
        box(180, "交易日期 摘要 借方发生额 贷方发生额 余额 对方账号 对方户名"),
    ]
    page2_boxes = [
        box(100, "2025-04-08 工程款 0.00 1,234.56 9,999.00 880001001 上海客户甲有限公司"),
        box(130, "04-09 材料款 500.00 0.00 9,499.00 880002002 上海供应商乙有限公司"),
    ]
    pages = [
        {"page": 1, "text": "\n".join(item["text"] for item in page1_boxes), "text_boxes": page1_boxes, "page_width": 850, "page_height": 1000, "table_rows": [], "source": "ocr_with_locations"},
        {"page": 2, "text": "\n".join(item["text"] for item in page2_boxes), "text_boxes": page2_boxes, "page_width": 850, "page_height": 1000, "table_rows": [], "source": "ocr_with_locations"},
    ]
    content = build_structured_extraction("\n".join(page["text"] for page in pages), "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["parse_diagnostics"]["coordinate_parse_valid_count"] == 0
    assert data["parse_diagnostics"]["parser_path"] == "date_anchor_text_fallback"
    assert data["parse_diagnostics"]["fallback_date_anchor_candidates"] == 2
    assert data["parse_diagnostics"]["fallback_valid_transactions"] == 2
    assert data["candidate_transaction_rows"] == 2
    assert data["valid_transaction_count"] == 2
    assert [tx["transaction_time"] for tx in data["transactions"]] == ["2025-04-08", "2025-04-09"]


def test_shanghai_bank_column_blocks_are_zipped_into_individual_rows():
    text = """上海银行对账单
记账日期
2025-04-08
2025-04-09
2025-04-10
交易用途
项目回款
材料采购
同户划转
摘要
工程款
跨行转账
往来款
借方发生额
0.00
500.00
200.00
贷方发生额
1,234.56
0.00
0.00
余额
9,999.00
9,499.00
9,299.00
对手账号
880001001
31001515100050022023
880003003
对手名称
上海客户甲有限公司
31001515100050022023上海凤环实业有限公司
上海意川建筑科技有限 公司
"""
    pages = [{"page": 1, "text": text, "table_rows": [], "source": "pdf_layout"}]
    content = build_structured_extraction(text, "bank_statement", raw_pages=pages, filename="上海银行对账单202504-202603.pdf")
    data = content["extracted_json"]
    assert data["parse_diagnostics"]["parser_path"] == "column_block"
    assert data["candidate_transaction_rows"] == 3
    assert data["valid_transaction_count"] == 3
    assert [tx["summary"] for tx in data["transactions"]] == ["工程款", "跨行转账", "往来款"]
    assert data["transactions"][1]["counterparty_account"] == "31001515100050022023"
    assert data["transactions"][1]["counterparty_name"] == "上海凤环实业有限公司"
    assert all("交易用途" not in (tx.get("summary") or "") for tx in data["transactions"])


def test_shanghai_bank_rejects_date_fragments_channels_and_page_blocks():
    from backend.extraction_skills.bank_statement import _amount_tokens, _clean_and_mark_transactions

    assert _amount_tokens("5.15日租房 6.15 公寓 2025.6.14") == []
    result = {
        "bank_format": "shanghai_bank", "account_name": "上海意川建筑科技有限公司",
        "period_start": "2025-04-01", "period_end": "2026-03-31",
        "transactions": [
            {"交易时间": "2025-05-15", "对方单位": "企业网上银行", "摘要": "转账", "用途": "", "备注": ""},
            {"交易时间": "2025-05-16", "对方单位": "上海客户有限公司", "摘要": "交易用途 报销 摘要 跨行转账 对手名称 上海客户有限公司 对手账号 123 余额", "用途": "", "备注": ""},
        ],
    }
    _clean_and_mark_transactions(result)
    assert result["transactions"][0]["clean_counterparty_name"] == ""
    assert result["transactions"][0]["channel"] == "企业网上银行"
    assert result["transactions"][1]["is_page_block"] is True
    assert result["transactions"][1]["is_valid_transaction"] is False


def test_shanghai_bank_infers_high_frequency_bidirectional_own_name():
    from backend.extraction_skills.bank_statement import _infer_account_name_from_counterparties

    result = {
        "bank_format": "shanghai_bank", "account_name": "",
        "transactions": [
            {"对方单位": "上海意川建筑科技有限 公司", "收支方向": "入账"},
            {"对方单位": "上海 意川 建筑 科技 有限公司", "收支方向": "出账"},
            {"对方单位": "上海意川建筑科技有限公司", "收支方向": "入账"},
            {"对方单位": "上海凤环实业有限公司", "收支方向": "出账"},
        ],
    }
    _infer_account_name_from_counterparties(result)
    assert result["account_name"] == "上海意川建筑科技有限公司"
    assert result["account_name_source"] == "high_frequency_counterparty_fallback"
