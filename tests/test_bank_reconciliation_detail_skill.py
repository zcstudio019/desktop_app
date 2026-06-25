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
    ws.append(["总笔数:", 2, "借方总笔数:", 1, "借方总金额:", "200,000.00"])
    ws.append(["贷方总笔数:", 1, "贷方总金额:", "1,000,000.00"])
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
    wb.save(path)


def test_bank_reconciliation_detail_aggregates_and_renders_markdown(tmp_path: Path) -> None:
    shanghai = tmp_path / "shanghai_bank_detail.xlsx"
    icbc = tmp_path / "icbc_bank_detail.xlsx"
    _save_shanghai_sample(shanghai)
    _save_icbc_sample(icbc)

    result = parse_bank_reconciliation_files(
        [
            {"file_path": str(shanghai), "file_name": "shanghai_bank_detail.xlsx"},
            {"file_path": str(icbc), "file_name": "icbc_bank_detail.xlsx"},
        ]
    )

    summary = result["summary"]
    markdown = result["display_markdown"]

    assert result["doc_type"] == "bank_reconciliation_detail"
    assert summary["file_count"] == 2
    assert summary["raw_transaction_count"] == 4
    assert summary["deduped_transaction_count"] == 4
    assert summary["date_start"] == "2025-04-01"
    assert summary["date_end"] == "2026-03-31"
    assert "上海意川建筑科技有限公司" in markdown
    assert "03005029359" in markdown
    assert "上海银行浦西支行营业部" in markdown
    assert "已清理占位值 17" in markdown
    assert "| 17 |" not in markdown
    assert "raw_result" not in markdown
    assert "normalized_data" not in markdown
    assert "transactions:" not in markdown
