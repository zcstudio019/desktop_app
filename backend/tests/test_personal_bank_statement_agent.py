from __future__ import annotations

import sys
from pathlib import Path

from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.document_types import normalize_document_type_code, should_append_same_type_document
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.personal_bank_statement_agent.customer_flow_aggregator import aggregate_customer_personal_flows
from backend.services.personal_bank_statement_agent.orchestrator import run_personal_bank_statement_agent


def _make_workbook(path: Path, rows: list[list[object]], second_sheet: bool = False) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "招商银行"
    for row in rows:
        ws.append(row)
    if second_sheet:
        ws2 = wb.create_sheet("建设银行")
        for row in rows:
            ws2.append(row)
    wb.save(path)


def _sample_rows() -> list[list[object]]:
    return [
        ["户名", "张三"],
        ["账号", "6222000011112222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2026-01-05", "工资", "上海样例科技有限公司", "1001", "", 20000, 30000],
        ["2026-01-08", "经营回款 服务费", "客户A", "2001", "", 50000, 80000],
        ["2026-01-10", "本人转账", "张三", "6222000011112222", 10000, "", 70000],
        ["2026-01-12", "消费贷放款", "某消费金融", "3001", "", 30000, 100000],
        ["2026-01-18", "信用卡还款", "招商银行信用卡中心", "4001", 8000, "", 92000],
        ["2026-02-05", "薪资", "上海样例科技有限公司", "1001", "", 21000, 113000],
        ["2026-02-09", "餐饮消费", "餐厅", "5001", 300, "", 112700],
    ]


def test_document_type_aliases_and_append_policy() -> None:
    for alias in ["personal_flow", "personal_bank_statement", "bank_statement_personal", "individual_bank_statement", "个人流水", "个人银行流水"]:
        assert normalize_document_type_code(alias) == "personal_flow"
        assert should_append_same_type_document(alias)


def test_single_file_single_account_personal_flow(tmp_path: Path) -> None:
    path = tmp_path / "personal_flow.xlsx"
    _make_workbook(path, _sample_rows())
    result = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)
    data = result["extracted_json"]
    summary = data["customer_level_summary"]
    assert data["doc_type"] == "personal_flow"
    assert summary["salary_income"] == 41000
    assert summary["operating_income"] == 50000
    assert summary["loan_inflow"] == 30000
    assert summary["internal_transfer_income"] == 0
    assert data["markdown_summary"] if "markdown_summary" in data else result["markdown_summary"]


def test_single_file_multi_sheet(tmp_path: Path) -> None:
    path = tmp_path / "personal_flow_multi.xlsx"
    _make_workbook(path, _sample_rows(), second_sheet=True)
    result = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)
    data = result["extracted_json"]
    assert len(data["accounts"]) == 2
    assert data["customer_level_summary"]["raw_total_income"] >= 242000


def test_document_agent_dispatches_personal_flow(tmp_path: Path) -> None:
    path = tmp_path / "dispatch.xlsx"
    _make_workbook(path, _sample_rows())
    result = run_document_extraction_agent(
        document_type="personal_bank_statement",
        raw_text="",
        filename=path.name,
        customer_id="customer-1",
        metadata={"file_path": str(path), "customer_name": "张三"},
    )
    assert result.document_type == "personal_flow"
    assert result.debug["selected_agent"] == "personal_bank_statement_agent"
    assert result.extracted_json["customer_level_summary"]["salary_income"] > 0


def test_customer_aggregate_multi_files_partial_failure_and_delete_refresh(tmp_path: Path) -> None:
    path = tmp_path / "aggregate.xlsx"
    _make_workbook(path, _sample_rows())
    parsed = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    failed = {"doc_id": "bad", "extraction_type": "personal_flow", "file_name": "bad.xlsx", "extracted_data": {"extracted_json": {"extraction_status": "failed", "warnings": ["bad file"]}}}
    one = {"doc_id": "doc1", "extraction_id": "ext1", "extraction_type": "personal_flow", "file_name": path.name, "extracted_data": {"extracted_json": parsed}}
    two = {"doc_id": "doc2", "extraction_id": "ext2", "extraction_type": "personal_flow", "file_name": path.name, "extracted_data": {"extracted_json": parsed}}
    aggregated = aggregate_customer_personal_flows([one, two, failed])
    assert aggregated["source_document_count"] == 2
    assert aggregated["customer_level_summary"]["salary_income"] == parsed["customer_level_summary"]["salary_income"] * 2
    assert aggregated["failed_sources"]
    refreshed = aggregate_customer_personal_flows([two, failed])
    assert refreshed["source_document_count"] == 1
    assert refreshed["customer_level_summary"]["salary_income"] == parsed["customer_level_summary"]["salary_income"]


def test_directional_counterparties_and_classification(tmp_path: Path) -> None:
    path = tmp_path / "classification.xlsx"
    _make_workbook(path, _sample_rows())
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    account = data["accounts"][0]
    income_names = {item["name"] for item in account["top_income_counterparties"]}
    expense_names = {item["name"] for item in account["top_expense_counterparties"]}
    assert "上海样例科技有限公司" in income_names
    assert "招商银行信用卡中心" in expense_names
    txs = account["transactions"]
    assert any(tx["is_salary"] for tx in txs)
    assert any(tx["is_operating_income"] for tx in txs)
    assert any(tx["is_internal_transfer"] for tx in txs)
    assert any(tx["is_loan_inflow"] for tx in txs)
    assert any(tx["is_credit_card_repayment"] and tx["direction"] == "expense" for tx in txs)
