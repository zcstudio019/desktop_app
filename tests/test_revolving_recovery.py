from __future__ import annotations

from backend.extraction_skills.enterprise_credit import recover_revolving_overdraft_from_window


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
