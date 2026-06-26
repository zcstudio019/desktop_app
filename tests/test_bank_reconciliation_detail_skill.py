from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from backend.extraction_skills.bank_reconciliation_detail import parse_bank_reconciliation_files


def _save_shanghai_sample(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "testReport"
    ws.append(["账户明细查询"])
    ws.append(["记账日期:", "2025-04-01---2026-03-31"])
    ws.append(["选择账号:", "03005029359", "户名:", "上海意川建筑科技有限公司", "开户行:", "上海银行浦西支行营业部", "币种:", "人民币"])
    ws.append(["总笔数", 2, "借方总笔数", 1, "借方总金额", "200,000.00"])
    ws.append(["贷方总笔数", 1, "贷方总金额", "1,000,000.00"])
    ws.append(["交易流水号", "交易时间", "记账日期", "交易方向", "交易金额", "余额", "对手账号", "对手名称", "摘要", "交易用途", "备注"])
    ws.append(["S001", "2025-04-01 11:12:46", "2025-04-01", "出账", "200,000.00", "800,000.00", "6222", "靖江市桐梧贸易有限公司", "跨行转账", "临空项目材料款", ""])
    ws.append(["S002", "2025-04-07 09:00:00", "2025-04-07", "入账", "1,000,000.00", "1,800,000.00", "0300", "上海意川建筑科技有限公司", "往来款", "", ""])
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
    assert summary["raw_transaction_count"] == 7
    assert summary["deduped_transaction_count"] == 7
    assert summary["date_start"] == "2025-04-01"
    assert summary["date_end"] == "2026-03-31"
    assert "## 银行对账明细" in markdown
    assert "### 核心资金概览" in markdown
    assert "### 经营判断" in markdown
    assert "### 月度资金变化" in markdown
    assert "### 主要入账来源" in markdown
    assert "### 主要出账对象" in markdown
    assert "### 风险提示" in markdown
    assert "文件解析质量清单" not in markdown
    assert "交易明细样例" not in markdown
    assert "03005029359" not in markdown
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
    assert summary["in_amount_excluding_self_transfer"] == "4000.00"
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
    assert summary["in_amount_excluding_excluded_related"] == "1000.00"
    assert summary["out_amount_excluding_excluded_related"] == "200500.00"
    top_in_names = [name for name, _ in result["top_in"]]
    top_out_names = [name for name, _ in result["top_out"]]
    assert "上海意川建筑科技有限公司" not in top_in_names
    assert "张三" not in top_in_names
    assert "李四" not in top_out_names
    assert "上海意川建筑科技有限公司" not in markdown
    assert "张三" not in markdown
    assert "李四" not in markdown
    assert "### 剔除说明" in markdown
    assert "已剔除内部/关联方入账" in markdown
