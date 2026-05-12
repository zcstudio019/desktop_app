from __future__ import annotations

from backend.extraction_skills.enterprise_credit import (
    final_normalize_credit_result,
    parse_revolving_overdue_line,
    parse_revolving_repayment_line,
    recover_revolving_overdraft_from_window,
)


def test_recover_revolving_overdraft_from_real_log_window() -> None:
    text = """
B10411000H
00013104707
15627312662中国建设银行
股份有限公司
上海五角场支
行流动资金贷款 2024-02-22 2025-08-21 人民币元 460 新增
第 4 页/共
6 页
公共记录明细
获得许可记录
抵押 454.68 正常 0 0 0 2025-03-06
78.06 正常还款 5 --B10411000H
00013104707
15627312662见附件 2025-03-31
"""

    record = recover_revolving_overdraft_from_window(text)

    assert record is not None
    assert record["institution_name"] == "中国建设银行股份有限公司上海五角场支行"
    assert record["business_type"] == "流动资金贷款"
    assert record["credit_amount"] == 460
    assert record["balance"] == 454.68
    assert record["guarantee_type"] == "抵押"
    assert record["start_date"] == "2024-02-22"
    assert record["end_date"] == "2025-08-21"
    assert record["five_category"] == "正常"
    assert record["overdue_months"] == 0
    assert record["last_repayment_date"] == "2025-03-06"
    assert record["last_repayment_amount"] == 78.06
    assert record["remaining_repayment_months"] == 5
    assert record["report_date"] == "2025-03-31"
    assert record["_recovered_by"] == "revolving_window_recovery_v2"


def test_recover_revolving_overdraft_strips_account_prefix_and_keeps_overdue_months() -> None:
    text = """
31A10311000H
0001EwzE20230506XS000004116中国农业发展银行总行营业部
流动资金贷款 2023-05-06 2026-02-08 人民币元 0 新增
信用/无担保 0 正常 0 0 0 2024-12-16
7.31 正常还款 0 --A10311000H
0001EwzE20230506XS000004116见附件 2025-03-31
"""

    record = recover_revolving_overdraft_from_window(text)

    assert record is not None
    assert record["institution_name"] == "中国农业发展银行总行营业部"
    assert "A10311000H" not in record["institution_name"]
    assert "EwzE" not in record["institution_name"]
    assert "XS000004116" not in record["institution_name"]
    assert record["account_no"].startswith("A10311000H")
    assert record["business_type"] == "流动资金贷款"
    assert record["credit_amount"] == 0
    assert record["balance"] == 0
    assert record["guarantee_type"] == "信用/无担保"
    assert record["start_date"] == "2023-05-06"
    assert record["end_date"] == "2026-02-08"
    assert record["five_category"] == "正常"
    assert record["overdue_total"] == 0
    assert record["overdue_principal"] == 0
    assert record["overdue_months"] == 0
    assert record["overdue_months"] != 4
    assert record["last_repayment_amount"] == 7.31
    assert record["remaining_repayment_months"] == 0
    assert record["report_date"] == "2025-03-31"
    assert record["credit_agreement_no"].startswith("A10311000H")
    assert record["_overdue_line"].replace(" ", "") == "信用/无担保0正常0002024-12-16"
    assert "正常还款0" in record["_repayment_line"].replace(" ", "")


def test_revolving_layer_parsers_do_not_cross_assign_months() -> None:
    overdue = parse_revolving_overdue_line("信用/无担保 0 正常 0 0 0 2024-12-16")
    repayment = parse_revolving_repayment_line(
        "7.31 正常还款 0 --A10311000H0001EwzE20230506XS000004116见附件 2025-03-31"
    )

    assert overdue["guarantee_type"] == "信用/无担保"
    assert overdue["balance"] == 0
    assert overdue["five_category"] == "正常"
    assert overdue["overdue_total"] == 0
    assert overdue["overdue_principal"] == 0
    assert overdue["overdue_months"] == 0
    assert overdue["last_repayment_date"] == "2024-12-16"
    assert "overdue_months" not in repayment
    assert repayment["last_repayment_amount"] == 7.31
    assert repayment["remaining_repayment_months"] == 0
    assert repayment["credit_agreement_no"].startswith("A10311000H")


def test_final_normalize_reprotects_revolving_overdue_months_from_overdue_line() -> None:
    raw_text = """
循环透支 共 1 笔
31A10311000H
0001EwzE20230506XS000004116中国农业发展银行总行营业部
流动资金贷款 2023-05-06 2026-02-08 人民币元 0 新增
信用/无担保 0 正常 0 0 0 2024-12-16
7.31 正常还款 0 --A10311000H
0001EwzE20230506XS000004116见附件 2025-03-31
"""
    result = final_normalize_credit_result(
        {
            "credit_summary": {"revolving_overdraft_balance": 0},
            "revolving_overdrafts": [
                {
                    "institution_name": "31A10311000H0001EwzE20230506XS000004116中国农业发展银行总行营业部",
                    "business_type": "流动资金贷款",
                    "credit_amount": 0,
                    "balance": 0,
                    "guarantee_type": "信用/无担保",
                    "start_date": "2023-05-06",
                    "end_date": "2026-02-08",
                    "five_category": "正常",
                    "overdue_months": 4,
                    "remaining_repayment_months": 0,
                    "account_no": "A10311000H0001EwzE20230506XS000004116",
                    "_overdue_line": "信用/无担保 0 正常 0 0 0 2024-12-16",
                    "_repayment_line": "7.31 正常还款 0 --A10311000H0001EwzE20230506XS000004116见附件 2025-03-31",
                }
            ],
        },
        raw_text=raw_text,
    )
    record = result["revolving_overdrafts"][0]

    assert record["institution_name"] == "中国农业发展银行总行营业部"
    assert "A10311000H" not in record["institution_name"]
    assert record["overdue_months"] == 0
    assert record["remaining_repayment_months"] == 0
