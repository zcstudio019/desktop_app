from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from backend.extraction_skills.bank_reconciliation_detail import AccountInfo, parse_bank_reconciliation_files, parse_qilu_transactions_by_regex


def _save_shanghai_sample(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "testReport"
    ws.append([None, "账户明细查询"])
    ws.append([None, "记账日期:", "2025-04-01---2026-03-31"])
    ws.append([None, "选择账号:", "03005029359", "上海意川建筑科技有限公司", "开户行:", "上海银行浦西支行营业部", None, "币种:", "人民币"])
    ws.append([None, "总笔数:", 2, None, "借方总笔数:", 1, None, "借方总金额:", "200,000.00"])
    ws.append([None, None, None, None, "贷方总笔数:", 1, None, "贷方总金额:", "1,000,000.00"])
    ws.append([None, "交易流水号", "交易时间", "记账日期", "交易方向", "交易金额", "余额", "对手账号", "对手名称", "摘要", "交易用途", "备注"])
    ws.append([None, "S001", "2025-04-01 11:12:46", "2025-04-01", "出账", "200,000.00", "800,000.00", "6222", "靖江市桐梧贸易有限公司", "跨行转账", "临空项目材料款", ""])
    ws.append([None, "S002", "2025-04-07 09:00:00", "2025-04-07", "入账", "1,000,000.00", "1,800,000.00", "0300", "上海意川建筑科技有限公司", "往来款", "", ""])
    ws.append([None, "S003", "2025-04-08 09:00:00", "2025-04-08", "入账", "50,000.00", "1,850,000.00", "0301", "远东宏信普惠融资租赁（天津）有限公司", "放款", "融资租赁", ""])
    ws.append([None, "S004", "2025-04-09 09:00:00", "2025-04-09", "入账", "10,000.00", "1,860,000.00", "0302", "吴卫利", "转账", "", ""])
    ws.append([None, "S005", "2025-04-10 09:00:00", "2025-04-10", "出账", "3,000.00", "1,857,000.00", "0303", "代发专用账户", "代发", "工资", ""])
    ws.append([None, "S006", "2025-04-11 09:00:00", "2025-04-11", "入账", "12,000.00", "1,869,000.00", "0304", "", "转账", "", ""])
    wb.save(path)


def _save_icbc_sample(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet0"
    ws.append(["[HISTORYDETAIL]"])
    ws.append(["凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号", "转入金额", "转出金额", "用途", "摘要", "附言"])
    ws.append(["I001", "123", "2025-04-27 10:00:00", "贷", "上海某项目有限公司", "17", "1,000.00", "17", "工程款", "17", "17"])
    ws.append(["I002", "456", "2026-03-24 10:00:00", "借", "上海材料有限公司", "17", "17", "500.00", "17", "采购款", "17"])
    ws.append(["I003", "789", "2025-05-01 10:00:00", "贷", "上海意川建筑科技有限公司", "17", "2,000.00", "17", "工程款", "17", "17"])
    ws.append(["I004", "111", "2025-06-01 10:00:00", "贷", "张三", "17", "3,000.00", "17", "工程款", "17", "17"])
    ws.append(["I005", "222", "2025-06-02 10:00:00", "借", "李四", "17", "17", "4,000.00", "材料款", "17", "17"])
    wb.save(path)


def test_bank_reconciliation_detail_aggregates_and_renders_compact_markdown(tmp_path: Path) -> None:
    shanghai = tmp_path / "shanghai_bank_detail.xlsx"
    icbc = tmp_path / "icbc_bank_detail.xlsx"
    _save_shanghai_sample(shanghai)
    _save_icbc_sample(icbc)

    result = parse_bank_reconciliation_files(
        [
            {"file_path": str(shanghai), "file_name": "上海银行对账明细202504-202603.xlsx"},
            {"file_path": str(icbc), "file_name": "工商银行对账明细202504-202603.xlsx"},
        ],
        metadata={
            "customer_name": "上海意川建筑科技有限公司",
            "legal_representative": "张三",
            "shareholders": [{"name": "李四"}],
        },
    )

    summary = result["summary"]
    markdown = result["display_markdown"]

    assert result["doc_type"] == "bank_reconciliation_detail"
    assert summary["file_count"] == 2
    assert summary["raw_transaction_count"] == 11
    assert summary["deduped_transaction_count"] == 11
    assert summary["date_start"] == "2025-04-01"
    assert summary["date_end"] == "2026-03-31"
    shanghai_file = result["files"][0]
    shanghai_account = result["accounts"][0]
    assert shanghai_file["bank_name"] == "上海银行"
    assert shanghai_file["header_row_no"] == 6
    assert shanghai_file["header_col_start"] == 2
    assert shanghai_file["raw_summary"]["raw_transaction_count"] == 2
    assert shanghai_file["raw_summary"]["debit_count"] == 1
    assert shanghai_file["raw_summary"]["credit_count"] == 1
    assert shanghai_file["raw_summary"]["debit_amount"] == "200000.00"
    assert shanghai_file["raw_summary"]["credit_amount"] == "1000000.00"
    assert shanghai_account["account_no"] == "03005029359"
    assert shanghai_account["account_name"] == "上海意川建筑科技有限公司"
    assert shanghai_account["branch_name"] == "上海银行浦西支行营业部"
    icbc_account = result["accounts"][1]
    assert icbc_account["bank_name"] == "工商银行"
    assert icbc_account["account_name"] == "上海意川建筑科技有限公司"
    assert icbc_account["account_no"] == ""
    assert icbc_account["branch_name"] == ""
    assert "## 银行对账明细" in markdown
    assert "### 核心资金概览" in markdown
    assert "### 经营判断" in markdown
    assert "### 月度经营资金变化" in markdown
    assert "### 主要经营入账来源" in markdown
    assert "### 主要经营出账对象" in markdown
    assert "### 风险提示" in markdown
    assert "文件解析质量清单" not in markdown
    assert "交易明细样例" not in markdown
    assert "03005029359" in markdown
    assert "上海银行浦西支行营业部" in markdown
    assert "| 17 |" not in markdown
    assert "raw_result" not in markdown
    assert "normalized_data" not in markdown
    assert "transactions:" not in markdown
    self_transfer_txs = [
        tx for tx in result["transactions"]
        if tx.get("counterparty_name") == "上海意川建筑科技有限公司"
    ]
    assert len(self_transfer_txs) == 2
    assert all(tx["is_self_transfer"] for tx in self_transfer_txs)
    assert all(tx["category"] == "内部/关联方往来" for tx in self_transfer_txs)
    assert all(not tx["is_operating_inflow"] and not tx["is_operating_outflow"] for tx in self_transfer_txs)
    assert summary["self_transfer_in_amount"] == "1002000.00"
    assert summary["in_amount_excluding_self_transfer"] == "76000.00"
    legal_rep_txs = [tx for tx in result["transactions"] if tx.get("counterparty_name") == "张三"]
    shareholder_txs = [tx for tx in result["transactions"] if tx.get("counterparty_name") == "李四"]
    assert legal_rep_txs[0]["is_excluded_related_party"]
    assert legal_rep_txs[0]["is_related_party_transfer"]
    assert legal_rep_txs[0]["excluded_reason"] == "法定代表人往来"
    assert shareholder_txs[0]["is_excluded_related_party"]
    assert shareholder_txs[0]["is_related_party_transfer"]
    assert shareholder_txs[0]["excluded_reason"] == "股东往来"
    assert not legal_rep_txs[0]["is_operating_inflow"]
    assert not shareholder_txs[0]["is_operating_outflow"]
    assert summary["excluded_related_transaction_count"] == 4
    assert summary["excluded_related_in_amount"] == "1005000.00"
    assert summary["excluded_related_out_amount"] == "4000.00"
    assert summary["in_amount_excluding_excluded_related"] == "73000.00"
    assert summary["out_amount_excluding_excluded_related"] == "203500.00"
    assert summary["operating_in_amount"] == "1000.00"
    assert summary["operating_out_amount"] == "200500.00"
    top_in_names = [name for name, _ in result["top_in"]]
    top_out_names = [name for name, _ in result["top_out"]]
    assert "上海意川建筑科技有限公司" not in top_in_names
    assert "张三" not in top_in_names
    assert "李四" not in top_out_names
    assert "远东宏信普惠融资租赁（天津）有限公司" not in top_in_names
    assert "吴卫利" not in top_in_names
    assert "未识别" not in top_in_names
    assert "代发专用账户" not in top_out_names
    assert result["top_in"][0][0] == "上海某项目有限公司"
    assert result["top_out"][0][0] == "靖江市桐梧贸易有限公司"
    assert result["top_in"][0][1]["evidence"] == "工程款"
    assert "临空项目材料款" in result["top_out"][0][1]["evidence"]
    assert "上海意川建筑科技有限公司" in markdown
    assert "张三" not in markdown
    assert "李四" not in markdown
    assert "吴卫利" not in markdown
    assert "远东宏信普惠融资租赁（天津）有限公司" not in markdown
    assert "代发专用账户" not in markdown
    assert "### 非经营性及噪音剔除说明" in markdown
    assert "内部/关联方往来" in markdown
    assert "| 剔除类型 | 入账金额 | 出账金额 | 笔数 | 说明 |" in markdown
    assert "| 其他非经营往来 |" in markdown
    assert "剔除后入账" not in markdown
    assert "剔除后出账" not in markdown
    assert "### 主要经营入账来源" in markdown
    assert "### 主要经营出账对象" in markdown
    assert "| 排名 | 对方户名 | 入账金额 | 笔数 | 判断 | 经营依据 |" in markdown
    assert "| 排名 | 对方户名 | 出账金额 | 笔数 | 判断 | 经营依据 |" in markdown
    assert "有效经营入账占原始入账比例" in markdown
    assert "融资分析中不宜直接按原始流水总额判断还款能力" in markdown
    assert "### 月度经营资金变化" in markdown
    assert "| 2025-05 |" not in markdown
    assert "| 2025-06 |" not in markdown


def test_shanghai_bank_reconciliation_detail_detects_b_column_header_and_meta(tmp_path: Path) -> None:
    shanghai = tmp_path / "shanghai_bank_detail.xlsx"
    _save_shanghai_sample(shanghai)

    result = parse_bank_reconciliation_files(
        [{"file_path": str(shanghai), "file_name": "上海银行对账明细202504-202603.xlsx"}]
    )

    summary = result["summary"]
    markdown = result["display_markdown"]
    parsed_file = result["files"][0]
    account = result["accounts"][0]
    assert summary["file_count"] == 1
    assert summary["deduped_transaction_count"] == 6
    assert result["extraction_status"] == "success"
    assert parsed_file["bank_name"] == "上海银行"
    assert parsed_file["header_row_no"] == 6
    assert parsed_file["header_col_start"] == 2
    assert parsed_file["raw_summary"]["raw_transaction_count"] == 2
    assert parsed_file["raw_summary"]["debit_count"] == 1
    assert parsed_file["raw_summary"]["credit_count"] == 1
    assert parsed_file["raw_summary"]["debit_amount"] == "200000.00"
    assert parsed_file["raw_summary"]["credit_amount"] == "1000000.00"
    assert account["account_no"] == "03005029359"
    assert account["account_name"] == "上海意川建筑科技有限公司"
    assert account["branch_name"] == "上海银行浦西支行营业部"
    assert "- 来源文件：上海银行对账明细202504-202603.xlsx" in markdown
    assert "- 提取状态：成功" in markdown
    assert "- 银行名称：上海银行" in markdown
    assert "- 交易笔数：6 笔" in markdown


def test_bank_reconciliation_detail_empty_files_returns_actionable_failure() -> None:
    result = parse_bank_reconciliation_files([])

    markdown = result["display_markdown"]
    assert result["extraction_status"] == "failed"
    assert result["failure_reason"] == "未收到可解析的银行对账明细文件"
    assert "失败原因：未收到可解析的银行对账明细文件" in markdown
    assert "来源文件：0 份文件" not in markdown
    assert "交易笔数：0 笔" not in markdown


def test_bank_reconciliation_detail_pdf_raw_pages_parses_and_renders(tmp_path: Path) -> None:
    pdf = tmp_path / "上海银行对账明细202504-202603.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% bank reconciliation detail stub\n")
    page_text = "\n".join(
        [
            "上海银行 账户明细查询",
            "户名：上海意川建筑科技有限公司 账号：03005029359 开户行：上海银行浦西支行营业部 币种：人民币",
            "交易日期 交易方向 交易金额 余额 对方户名 摘要 用途",
            "2025-04-01 出账 200,000.00 800,000.00 靖江市桐梧贸易有限公司 跨行转账 临空项目材料款",
            "2025-04-02 入账 100,000.00 900,000.00 上海某项目有限公司 回款 工程款",
        ]
    )

    result = parse_bank_reconciliation_files(
        [{"file_path": str(pdf), "file_name": "上海银行对账明细202504-202603.pdf"}],
        metadata={"raw_pages": [{"page": 1, "text": page_text}]},
    )

    summary = result["summary"]
    parsed_file = result["files"][0]
    account = result["accounts"][0]
    markdown = result["display_markdown"]

    assert result["extraction_status"] == "success"
    assert summary["file_count"] == 1
    assert summary["deduped_transaction_count"] == 2
    assert parsed_file["source_file"] == "上海银行对账明细202504-202603.pdf"
    assert parsed_file["sheet_name"] == "PDF"
    assert parsed_file["transaction_count"] == 2
    assert parsed_file["header_row_no"] == 3
    assert account["bank_name"] == "上海银行"
    assert account["account_name"] == "上海意川建筑科技有限公司"
    assert account["account_no"] == "03005029359"
    assert account["branch_name"] == "上海银行浦西支行营业部"
    assert summary["operating_in_amount"] == "100000.00"
    assert summary["operating_out_amount"] == "200000.00"
    assert "## 银行对账明细" in markdown
    assert "- 来源文件：上海银行对账明细202504-202603.pdf" in markdown
    assert "### 核心资金概览" in markdown
    assert "### 主要经营入账来源" in markdown
    assert "### 主要经营出账对象" in markdown
    assert "JSON" not in markdown
    assert "transactions:" not in markdown


def test_qilu_bank_reconciliation_detail_pdf_table_rows_parse(tmp_path: Path) -> None:
    pdf = tmp_path / "202501-6齐鲁银行流水(1).pdf"
    pdf.write_bytes(b"%PDF-1.4\n% qilu bank reconciliation detail stub\n")
    page_text = "\n".join(
        [
            "单位活期存款账户交易明细",
            "齐鲁银行",
            "开户机构：齐鲁银行股份有限公司德州开发区支行",
            "账号：86617005101421011677",
            "账户名称：艾绿工程建设（上海）有限公司",
            "起止日期：2025/01/01-2025/06/30",
            "交易方向：全部",
            "币种：人民币",
            "收入金额合计：1,000,000.00",
            "支出金额合计：200,000.00",
            "第1/1页，共2条",
        ]
    )
    table_rows = [
        ["序号", "记账日期", "交易渠道", "收入", "支出", "账户余额", "摘要|备注"],
        ["1", "2025/06/30", "人民银行", "1,000,000.00", "0.00", "6,569,855.83", "汇款|杨庄河项目进度款"],
        ["交易对手信息：", "29410078801400001148", "德州天衢文化旅游发展有限公司", "上海浦东发展银行股份有限公司德州分行"],
        ["2", "2025/06/29", "网银", "0.00", "200,000.00", "5,569,855.83", "转账|项目材料款"],
        ["交易对手信息：", "12345678901234567890", "山东材料有限公司", "齐鲁银行股份有限公司德州分行"],
    ]

    result = parse_bank_reconciliation_files(
        [{"file_path": str(pdf), "file_name": "202501-6齐鲁银行流水(1).pdf"}],
        metadata={"raw_pages": [{"page": 1, "text": page_text, "table_rows": table_rows}]},
    )

    summary = result["summary"]
    parsed_file = result["files"][0]
    account = result["accounts"][0]
    markdown = result["display_markdown"]

    assert result["extraction_status"] == "success"
    assert summary["file_count"] == 1
    assert summary["deduped_transaction_count"] == 2
    assert summary["in_amount"] == "1000000.00"
    assert summary["out_amount"] == "200000.00"
    assert parsed_file["bank_name"] == "齐鲁银行"
    assert parsed_file["raw_summary"]["raw_transaction_count"] == 2
    assert parsed_file["raw_summary"]["income_total"] == "1000000.00"
    assert parsed_file["raw_summary"]["out_total"] == "200000.00"
    assert account["bank_name"] == "齐鲁银行"
    assert account["account_no"] == "86617005101421011677"
    assert account["account_name"] == "艾绿工程建设（上海）有限公司"
    assert account["branch_name"] == "齐鲁银行股份有限公司德州开发区支行"
    assert account["date_start"] == "2025-01-01"
    assert account["date_end"] == "2025-06-30"
    assert "## 银行对账明细" in markdown
    assert "- 来源文件：202501-6齐鲁银行流水(1).pdf" in markdown
    assert "- 银行名称：齐鲁银行" in markdown
    assert "- 户名：艾绿工程建设（上海）有限公司" in markdown
    assert "- 账号：86617005101421011677" in markdown
    assert "- 开户行：齐鲁银行股份有限公司德州开发区支行" in markdown
    assert "- 覆盖时间：2025-01-01 至 2025-06-30" in markdown
    assert "- 交易笔数：2 笔" in markdown
    assert "transactions:" not in markdown
    assert "data：" not in markdown


def test_qilu_bank_reconciliation_detail_pdf_fragmented_text_blocks_parse(tmp_path: Path) -> None:
    pdf = tmp_path / "202501-6齐鲁银行流水(1).pdf"
    pdf.write_bytes(b"%PDF-1.4\n% qilu fragmented text stub\n")
    page_text = "\n".join(
        [
            "单位活期存款账户交易明细",
            "齐鲁银行",
            "开户机构：齐鲁银行股份有限公司德州开发区支行",
            "账号：86617005101421011677",
            "账户名称：艾绿工程建设（上海）有限公司",
            "起止日期：2025/01/01-2025/06/30",
            "交易方向：全部",
            "币种：人民币",
            "收入金额合计：1,000,000.00",
            "支出金额合计：4.50",
            "第1/1页，共2条",
            "1,000,000.00 0.00",
            "29410078801400001148 德州天衢文化旅游发展有限公司 上海浦东发展银行股份有限公司德州分行",
            "6,569,855.83",
            "交易对手信息：",
            "1",
            "2025-06-30 人民银行 汇款|杨庄河项目进度款",
            "0.00 4.50",
            "99999999999999999999 其他国内结算业务收入 齐鲁银行股份有限公司德州开发区支行",
            "6,569,851.33",
            "交易对手信息：",
            "3",
            "2025-06-24 网上银行 支付手续费|支付手续费",
        ]
    )

    result = parse_bank_reconciliation_files(
        [{"file_path": str(pdf), "file_name": "202501-6齐鲁银行流水(1).pdf"}],
        metadata={"raw_pages": [{"page": 1, "text": page_text}]},
    )

    summary = result["summary"]
    transactions = result["transactions"]
    first = transactions[0]
    fee = transactions[1]

    assert result["extraction_status"] == "success"
    assert summary["deduped_transaction_count"] == 2
    assert summary["in_amount"] == "1000000.00"
    assert summary["out_amount"] == "4.50"
    assert first["accounting_date"] == "2025-06-30"
    assert first["direction"] == "in"
    assert first["amount"] == "1000000.00"
    assert first["balance"] == "6569855.83"
    assert first["counterparty_account"] == "29410078801400001148"
    assert first["counterparty_name"] == "德州天衢文化旅游发展有限公司"
    assert first["counterparty_bank_no"] == "上海浦东发展银行股份有限公司德州分行"
    assert first["summary"] == "汇款|杨庄河项目进度款"
    assert fee["accounting_date"] == "2025-06-24"
    assert fee["direction"] == "out"
    assert fee["amount"] == "4.50"
    assert fee["counterparty_name"] == "其他国内结算业务收入"
    assert "手续费" in fee["category"]


def test_parse_qilu_transactions_by_regex_minimal_block() -> None:
    text = "\n".join(
        [
            "1,000,000.00 0.00",
            "29410078801400001148 德州天衢文化旅游发展有限公司 上海浦东发展银行股份有限公司德州分行",
            "6,569,855.83",
            "交易对手信息：",
            "1",
            "2025-06-30 人民银行 汇款|杨庄河项目进度款",
        ]
    )
    account = AccountInfo(bank_name="齐鲁银行", account_name="艾绿工程建设（上海）有限公司", account_no="86617005101421011677")

    transactions = parse_qilu_transactions_by_regex(text, "202501-6齐鲁银行流水(1).pdf", account)

    assert len(transactions) == 1
    tx = transactions[0]
    assert str(tx["amount"]) == "1000000.00"
    assert tx["direction"] == "in"
    assert tx["accounting_date"] == "2025-06-30"
    assert tx["counterparty_account"] == "29410078801400001148"
    assert tx["counterparty_name"] == "德州天衢文化旅游发展有限公司"
    assert tx["counterparty_bank_no"] == "上海浦东发展银行股份有限公司德州分行"
    assert tx["summary"] == "汇款|杨庄河项目进度款"
