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
from backend.services.personal_bank_statement_agent.deterministic_summary import build_deterministic_personal_flow_summary
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


def _repayment_account_rows() -> list[list[object]]:
    return [
        ["银行", "兴业银行"],
        ["户名", "王敬培"],
        ["账号", "622908****0319"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2024-10-10", "汇款汇入", "", "", "", 14792.00, 14792.00],
        ["2024-10-10", "个贷还款", "兴业银行", "loan-1", 14788.47, "", 3.53],
        ["2024-11-10", "汇款汇入", "", "", "", 16800.00, 16803.53],
        ["2024-11-11", "贷款回收", "兴业银行", "loan-1", 16795.00, "", 8.53],
        ["2024-12-10", "汇款汇入", "", "", "", 15000.00, 15008.53],
        ["2024-12-10", "快捷支付", "支付宝", "pay-1", 8625.86, "", 6382.67],
        ["2024-12-10", "个贷还款", "兴业银行", "loan-1", 6374.14, "", 8.53],
        ["2025-01-10", "汇款汇入", "", "", "", 17000.00, 17008.53],
        ["2025-01-10", "贷款扣款", "兴业银行", "loan-1", 16995.00, "", 13.53],
        ["2025-02-10", "汇款汇入", "", "", "", 18000.00, 18013.53],
        ["2025-02-11", "个贷还款", "兴业银行", "loan-1", 17990.00, "", 23.53],
        ["2025-03-20", "存款利息", "兴业银行", "", "", 31.31, 54.84],
        ["2025-09-11", "汇款汇入", "", "", "", 15000.00, 15054.84],
        ["2025-09-11", "快捷支付", "微信支付", "pay-2", 8625.86, "", 6428.98],
        ["2025-09-11", "个贷还款", "兴业银行", "loan-1", 6374.14, "", 54.84],
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
    two = {"doc_id": "doc2", "extraction_id": "ext2", "extraction_type": "personal_flow", "file_name": "aggregate-2.xlsx", "extracted_data": {"extracted_json": parsed}}
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


def test_repayment_account_flow_xingye_sample(tmp_path: Path) -> None:
    path = tmp_path / "xingye_repayment.xlsx"
    _make_workbook(path, _repayment_account_rows())
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    income = data["income_verification"]
    expense = data["expense_analysis"]
    risk_codes = {item["code"] for item in data["risk_signals"]}
    assert income["unknown_inflow"] > 0
    assert income["verified_income"] == 0
    assert income["stable_income"] == 0
    assert expense["loan_repayment_ratio"] >= 0.6
    assert data["flow_nature"]["primary_type"] == "repayment_account_flow"
    assert {"income_source_unclear", "repayment_account_flow", "high_loan_repayment_ratio", "weak_cash_retention"}.issubset(risk_codes)
    assert data["financing_judgement"]["recommended_usage"] in {"可作为还款账户流水", "仅供参考"}


def test_fast_in_fast_out_single_and_combination_matches(tmp_path: Path) -> None:
    path = tmp_path / "fast_in_fast_out.xlsx"
    _make_workbook(path, _repayment_account_rows())
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    fast = data["fast_in_fast_out_analysis"]
    assert fast["has_fast_in_fast_out"]
    assert fast["matched_count"] >= 2
    assert any(abs(item["income_amount"] - 14792.0) < 0.01 and abs(item["expense_amount"] - 14788.47) < 0.01 for item in fast["matches"])
    assert any(abs(item["income_amount"] - 15000.0) < 0.01 and abs(item["expense_amount"] - 15000.0) < 0.01 for item in fast["matches"])


def test_unknown_inflow_not_stable_income(tmp_path: Path) -> None:
    path = tmp_path / "unknown_inflow.xlsx"
    _make_workbook(path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-01", "汇款汇入", "", "", "", 10000, 10000],
    ])
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    assert data["income_verification"]["unknown_inflow"] == 10000
    assert data["income_verification"]["stable_income"] == 0


def test_verified_salary_and_operating_income_enhanced(tmp_path: Path) -> None:
    path = tmp_path / "verified_income.xlsx"
    _make_workbook(path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-01", "代发工资", "某公司", "1001", "", 20000, 20000],
        ["2025-01-05", "销售款", "客户A", "2001", "", 30000, 50000],
        ["2025-01-06", "快捷支付", "商户", "3001", 500, "", 49500],
    ])
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    income = data["income_verification"]
    assert income["verified_salary_income"] == 20000
    assert income["verified_operating_income"] == 30000
    assert income["stable_income"] == 50000
    assert data["top_income_counterparties"][0]["amount"] == 30000
    assert data["top_expense_counterparties"][0]["amount"] == 500


def test_confirmed_salary_keywords(tmp_path: Path) -> None:
    path = tmp_path / "confirmed_salary.xlsx"
    _make_workbook(path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-10", "代发工资", "上海样例科技有限公司", "1001", "", 12000, 12000],
        ["2025-02-10", "工资发放", "上海样例科技有限公司", "1001", "", 12500, 24500],
    ])
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    income = data["income_verification"]
    assert income["confirmed_salary_income"] == 24500
    assert income["verified_salary_income"] == 24500
    assert income["suspected_salary_income"] == 0
    assert all(tx["salary_detection"]["salary_type"] == "confirmed_salary" for tx in data["transactions"] if tx["direction"] == "income")


def test_suspected_salary_requires_pattern_and_counterparty(tmp_path: Path) -> None:
    path = tmp_path / "suspected_salary.xlsx"
    _make_workbook(path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-10", "批量代发", "上海样例科技有限公司", "1001", "", 10000, 10000],
        ["2025-02-11", "批量代发", "上海样例科技有限公司", "1001", "", 10200, 20200],
        ["2025-03-10", "批量代发", "上海样例科技有限公司", "1001", "", 10100, 30300],
    ])
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    income = data["income_verification"]
    assert income["confirmed_salary_income"] == 0
    assert income["verified_salary_income"] == 0
    assert income["suspected_salary_income"] == 30300
    assert income["salary_continuity_level"] in {"none", "weak"}
    assert all(tx["salary_detection"]["salary_type"] == "suspected_salary" for tx in data["transactions"] if tx["direction"] == "income")


def test_salary_exclusions_and_unknown_transfer_not_salary(tmp_path: Path) -> None:
    path = tmp_path / "salary_exclusions.xlsx"
    _make_workbook(path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-01", "汇款汇入", "", "", "", 10000, 10000],
        ["2025-01-02", "报销", "上海样例科技有限公司", "1001", "", 1000, 11000],
        ["2025-01-03", "借款", "李四", "2001", "", 5000, 16000],
        ["2025-01-04", "货款", "客户A", "3001", "", 8000, 24000],
        ["2025-01-05", "劳务费", "项目方", "4001", "", 3000, 27000],
    ])
    data = run_personal_bank_statement_agent(file_path=str(path), filename=path.name)["extracted_json"]
    income = data["income_verification"]
    by_summary = {tx["summary"]: tx for tx in data["transactions"] if tx["direction"] == "income"}
    assert income["confirmed_salary_income"] == 0
    assert income["suspected_salary_income"] == 0
    assert by_summary["汇款汇入"]["category"] == "unknown_inflow"
    assert by_summary["报销"]["category"] == "reimbursement_or_advance_income"
    assert by_summary["借款"]["category"] == "borrowing_or_transfer_income"
    assert by_summary["货款"]["category"] == "operating_income"
    assert by_summary["劳务费"]["category"] == "labor_income"


def test_salary_continuity_strong_and_unstable(tmp_path: Path) -> None:
    strong_path = tmp_path / "salary_strong.xlsx"
    _make_workbook(strong_path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-10", "代发工资", "上海样例科技有限公司", "1001", "", 10000, 10000],
        ["2025-02-10", "代发工资", "上海样例科技有限公司", "1001", "", 10100, 20100],
        ["2025-03-11", "代发工资", "上海样例科技有限公司", "1001", "", 9900, 30000],
        ["2025-04-10", "代发工资", "上海样例科技有限公司", "1001", "", 10050, 40050],
        ["2025-05-09", "代发工资", "上海样例科技有限公司", "1001", "", 10000, 50050],
        ["2025-06-10", "代发工资", "上海样例科技有限公司", "1001", "", 10200, 60250],
    ])
    strong = run_personal_bank_statement_agent(file_path=str(strong_path), filename=strong_path.name)["extracted_json"]
    assert strong["income_verification"]["salary_continuity_level"] == "strong"

    weak_path = tmp_path / "salary_weak.xlsx"
    _make_workbook(weak_path, [
        ["户名", "张三"],
        ["账号", "6222"],
        ["交易日期", "摘要", "对方户名", "对方账号", "支出", "收入", "余额"],
        ["2025-01-03", "代发工资", "上海样例科技有限公司", "1001", "", 5000, 5000],
        ["2025-03-20", "代发工资", "上海样例科技有限公司", "1001", "", 20000, 25000],
    ])
    weak = run_personal_bank_statement_agent(file_path=str(weak_path), filename=weak_path.name)["extracted_json"]
    assert weak["income_verification"]["salary_continuity_level"] in {"weak", "none"}


def test_china_merchants_daifa_kuanxiang_suspected_salary_rows_metadata() -> None:
    rows = [
        {"交易日期": "2024-06-21", "交易金额": "11543.87", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
        {"交易日期": "2024-07-10", "交易金额": "16131.77", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
        {"交易日期": "2024-08-09", "交易金额": "15986.08", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
        {"交易日期": "2024-09-10", "交易金额": "15774.82", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
    ]
    data = run_personal_bank_statement_agent(filename="招商银行个人流水.xlsx", metadata={"rows": rows})["extracted_json"]
    income = data["income_verification"]
    assert income["suspected_salary_income"] > 0
    assert income["suspected_salary_count"] == 4
    assert income["salary_months"] == 4
    assert any(item["counterparty_name"] == "上海中兴软件有限责任公司" for item in income["salary_sources"])
    assert {tx["salary_detection"]["salary_type"] for tx in data["transactions"]} == {"suspected_salary"}
    assert income["confirmed_salary_income"] == 0
    assert income["verified_salary_income"] == 0


def test_deterministic_summary_from_transaction_amount_rows_metadata() -> None:
    rows = [
        {"交易日期": "2024-06-21", "交易金额": "11543.87", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
        {"交易日期": "2024-07-10", "交易金额": "16131.77", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
        {"交易日期": "2024-07-15", "交易金额": "-1000.00", "交易摘要": "转账汇款", "对手信息": "李四"},
    ]
    data = run_personal_bank_statement_agent(filename="招商银行个人流水.xlsx", metadata={"rows": rows})["extracted_json"]
    summary = data["deterministic_summary"]
    assert summary["total_income"] == 27675.64
    assert summary["total_expense"] == 1000
    assert summary["income_count"] == 2
    assert summary["expense_count"] == 1
    assert summary["net_cash_flow"] == 26675.64
    assert summary["max_income_transaction"]["summary"] == "代发款项"
    assert summary["max_expense_transaction"]["summary"] == "转账汇款"


def test_deterministic_personal_flow_summary_is_repeatable() -> None:
    payload = {
        "收支规模汇总": {"总收入金额": 1, "总支出金额": 1},
        "交易明细列表": [
            {"交易日期": "2024-06-21", "交易金额": "11543.87", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
            {"交易日期": "2024-07-10", "交易金额": "16131.77", "交易摘要": "代发款项", "对手信息": "上海中兴软件有限责任公司"},
            {"交易日期": "2024-07-15", "交易金额": "-1000.00", "交易摘要": "转账汇款", "对手信息": "李四"},
        ],
    }
    first = build_deterministic_personal_flow_summary(payload)
    second = build_deterministic_personal_flow_summary(payload)
    assert first["deterministic_summary"] == second["deterministic_summary"]
    assert first["income_verification"]["suspected_salary_income"] == second["income_verification"]["suspected_salary_income"]
    assert first["income_verification"]["suspected_salary_income"] == 27675.64
    assert first["income_verification"]["salary_sources"][0]["counterparty_name"] == "上海中兴软件有限责任公司"


def test_aggregate_prefers_detail_summary_when_ai_summary_mismatch() -> None:
    payload = {
        "doc_type": "personal_flow",
        "customer_level_summary": {"raw_total_income": 1, "raw_total_expense": 1},
        "raw_summary": {"total_income": 1, "total_expense": 1},
        "transactions": [
            {"transaction_date": "2024-01-01", "transaction_amount": 100, "summary": "汇款汇入", "counterparty_name": ""},
            {"transaction_date": "2024-01-02", "transaction_amount": -40, "summary": "转账汇款", "counterparty_name": ""},
        ],
    }
    aggregated = aggregate_customer_personal_flows([
        {"doc_id": "doc1", "extraction_type": "personal_flow", "file_name": "a.xlsx", "extracted_data": {"extracted_json": payload}}
    ])
    assert aggregated["deterministic_summary"]["total_income"] == 100
    assert aggregated["deterministic_summary"]["total_expense"] == 40
    assert aggregated["customer_level_summary"]["raw_total_income"] == 100
    assert aggregated["summary_warnings"][0]["code"] == "summary_detail_mismatch"


def test_duplicate_same_file_hash_only_latest_participates_in_personal_flow_aggregate() -> None:
    old_payload = {
        "doc_type": "personal_flow",
        "transactions": [
            {"transaction_date": "2024-01-01", "transaction_amount": 100, "summary": "汇款汇入"},
        ],
    }
    latest_payload = {
        "doc_type": "personal_flow",
        "transactions": [
            {"transaction_date": "2024-01-01", "transaction_amount": 200, "summary": "汇款汇入"},
        ],
    }
    aggregated = aggregate_customer_personal_flows([
        {
            "doc_id": "old",
            "extraction_id": "ext-old",
            "extraction_type": "personal_flow",
            "file_name": "same.xlsx",
            "file_hash": "hash-1",
            "created_at": "2024-01-01T00:00:00",
            "extracted_data": {"extracted_json": old_payload},
        },
        {
            "doc_id": "new",
            "extraction_id": "ext-new",
            "extraction_type": "personal_flow",
            "file_name": "same.xlsx",
            "file_hash": "hash-1",
            "created_at": "2024-01-02T00:00:00",
            "extracted_data": {"extracted_json": latest_payload},
        },
    ])
    assert aggregated["source_document_count"] == 1
    assert aggregated["source_files"][0]["document_id"] == "new"
    assert aggregated["deterministic_summary"]["total_income"] == 200
