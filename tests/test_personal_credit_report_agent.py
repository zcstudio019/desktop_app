from __future__ import annotations

import asyncio

from backend.services.document_extractor_service import build_structured_extraction
from backend.services.markdown_profile_service import build_auto_profile_payload
from backend.services.personal_credit_report_agent.extract_basic_info import extract_basic_info
from backend.services.personal_credit_report_agent.extract_credit_summary import extract_credit_summary
from backend.services.personal_credit_report_agent.extract_query_records import extract_query_records
from backend.services.personal_credit_report_agent.markdown_renderer import render_personal_credit_markdown
from backend.services.personal_credit_report_agent.orchestrator import run_personal_credit_report_agent
from backend.services.personal_credit_report_agent.risk_analyzer import analyze_personal_credit_risk
from backend.services.personal_credit_report_agent.segmenter import segment_report


SAMPLE_TEXT = """
个人信用报告
中国人民银行征信中心

报告基础信息
报告编号：P202605120001
报告时间：2026-05-12 10:20:30
个人基本信息
姓名：张三 证件类型：身份证 证件号码：110101199001011234
婚姻状况：已婚

信贷记录概要
信用卡账户数 2
当前有效信用卡账户数 1
贷款账户数 2
未结清贷款账户数 2
信用卡逾期账户数 1
信用卡 90 天以上逾期账户数 0
贷款逾期账户数 1
贷款 90 天以上逾期账户数 0
为个人相关还款责任账户数 0 / 未显示
为企业相关还款责任账户数 1

信贷交易信息明细
贷款账户明细
1. 中国建设银行北京分行 购房贷款 发放金额：1000000元 余额：800000元 账户状态：正常 五级分类：正常 最近一次还款日期：2026-04-12 信息报告日期：2026-05-12
2. 某消费金融公司 消费贷款 发放金额：50000元 余额：10000元 账户状态：逾期 五级分类：关注 当前逾期金额：1200元 逾期信息：逾期1个月

贷记卡账户明细
1. 招商银行信用卡 贷记卡 授信额度：50000元 已用额度：12000元 账户状态：正常 最近一次还款日期：2026-04-12 信息报告日期：2026-05-12

查询记录
机构查询记录明细
2026-04-01 招商银行 信用卡审批 机构查询
2026-03-01 本人查询 本人查询 本人查询
"""


def test_extract_basic_info() -> None:
    sections = segment_report(SAMPLE_TEXT)
    basic = extract_basic_info(sections, source_file="sample.txt")
    assert basic["report_number"] == "P202605120001"
    assert basic["name"] == "张三"
    assert basic["id_number"] == "110101199001011234"
    assert basic["source_file"] == "sample.txt"


def test_extract_credit_summary() -> None:
    summary = extract_credit_summary(segment_report(SAMPLE_TEXT))
    assert summary["credit_card_account_count"] == "2"
    assert summary["active_credit_card_account_count"] == "1"
    assert summary["loan_account_count"] == "2"
    assert summary["outstanding_loan_account_count"] == "2"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "1"


def test_extract_query_records() -> None:
    records = extract_query_records(segment_report(SAMPLE_TEXT))
    assert len(records) >= 2
    assert records[0]["query_date"] == "2026-04-01"
    assert records[0]["query_reason"] == "信用卡审批"


def test_agent_output_schema_stable() -> None:
    result = run_personal_credit_report_agent(SAMPLE_TEXT, source_file="sample.txt", debug=True)
    report = result["report_json"]
    assert result["report_type"] == "personal_credit_report"
    for key in (
        "basic_info",
        "credit_summary",
        "loan_accounts",
        "credit_card_accounts",
        "guarantees",
        "overdue_records",
        "public_records",
        "query_records",
        "risk_flags",
        "missing_fields",
        "warnings",
    ):
        assert key in report
    assert isinstance(report["loan_accounts"], list)
    assert isinstance(report["credit_card_accounts"], list)
    assert isinstance(report["query_records"], list)


def test_markdown_renderer_not_empty() -> None:
    report = run_personal_credit_report_agent(SAMPLE_TEXT)["report_json"]
    markdown = render_personal_credit_markdown(report)
    assert "# 个人征信报告" in markdown
    assert "## 八、查询记录" in markdown
    assert "## 十、待核验项" in markdown


def test_personal_credit_risk_analyzer() -> None:
    report = run_personal_credit_report_agent(SAMPLE_TEXT)["report_json"]
    indicators = analyze_personal_credit_risk(report)
    assert indicators["has_current_overdue"] is True
    assert indicators["risk_level"] == "high"
    assert "存在为企业相关还款责任账户" in indicators["risk_reasons"]


def test_markdown_no_duplicate_header() -> None:
    result = run_personal_credit_report_agent(SAMPLE_TEXT, source_file="sample.txt")
    markdown = result["report_markdown"]
    assert markdown.count("资料信息") == 1
    assert "## 个人征信" not in markdown
    for item in ("type:", "title:", "confidence:", "markdown:"):
        assert item not in markdown


def test_basic_info_id_number_cleanup() -> None:
    text = """
个人信用报告
报告基础信息
姓名：沃志方 证件类型：身份证 证件号码：310110198211172732 未婚
报告编号：2025031104013907986945 报告时间：2025-03-11 04:01:39
中征码：3201050001674346
在中国建设银行股份有限公司办理业务
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    basic = report["basic_info"]
    assert basic["name"] == "沃志方"
    assert basic["id_type"] == "身份证"
    assert basic["id_number"] == "310110198211172732"
    assert "3201050001674346" not in basic["id_number"]


def test_report_number_and_time_separate_lines() -> None:
    text = """
个人信用报告
报告基础信息
姓名：沃志方 证件类型：身份证 证件号码：310110198211172732 未婚
报告编号：2025031104013907986945 报告时间：2025-03-11 04:01:39
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    report_no_line = "- 报告编号：2025031104013907986945"
    report_time_line = "- 报告时间：2025-03-11 04:01:39"
    assert report_no_line in markdown
    assert report_time_line in markdown
    assert markdown.index(report_no_line) < markdown.index(report_time_line)


def test_markdown_show_full_id_number() -> None:
    text = """
个人信用报告
报告基础信息
姓名：沃志方
证件类型：身份证
证件号码：310110198211172732
报告编号：2025031104013907986945
报告时间：2025-03-11 04:01:39
婚姻状况：未婚
"""
    result = run_personal_credit_report_agent(text)
    markdown = result["report_markdown"]
    assert "证件号码：310110198211172732" in markdown
    assert "310110********2732" not in markdown
    assert result["report_json"]["basic_info"]["id_number"] == "310110198211172732"


def test_credit_summary_new_fields() -> None:
    text = """
个人信用报告
信贷记录概要
信用卡账户数 5
当前有效信用卡账户数 0 / 未显示为有效
贷款账户数 3
未结清贷款账户数 1
信用卡逾期账户数 0
信用卡 90 天以上逾期账户数 0
贷款逾期账户数 0
贷款 90 天以上逾期账户数 0
为个人相关还款责任账户数 0 / 未显示
为企业相关还款责任账户数 9
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0 / 未显示为有效"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0 / 未显示"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "9"


def test_skip_closed_credit_card_accounts() -> None:
    text = """
个人信用报告
贷记卡账户明细
1. 招商银行信用卡 贷记卡 授信额度：50000元 已用额度：0元 账户状态：销户
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []


def test_skip_settled_loan_accounts() -> None:
    text = """
个人信用报告
贷款账户明细
1. 中国银行 消费贷款 发放金额：10000元 余额：0元 账户状态：已结清 五级分类：正常
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_keep_abnormal_closed_or_settled_accounts() -> None:
    loan_text = """
个人信用报告
贷款账户明细
1. 中国银行 消费贷款 发放金额：10000元 余额：0元 账户状态：已结清 五级分类：关注 当前逾期金额：100元
"""
    card_text = """
个人信用报告
贷记卡账户明细
1. 招商银行信用卡 贷记卡 授信额度：50000元 已用额度：0元 账户状态：销户 当前逾期金额：200元
"""
    loan_report = run_personal_credit_report_agent(loan_text)["report_json"]
    card_report = run_personal_credit_report_agent(card_text)["report_json"]
    assert len(loan_report["loan_accounts"]) == 1
    assert len(card_report["credit_card_accounts"]) == 1


def test_credit_summary_markdown_table() -> None:
    markdown = run_personal_credit_report_agent(SAMPLE_TEXT)["report_markdown"]
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert "信用卡账户数" in markdown
    assert "当前有效信用卡账户数" in markdown
    assert "贷款账户数" in markdown
    assert "未结清贷款账户数" in markdown
    assert "购房贷款账户数" not in markdown
    assert "其他贷款账户数" not in markdown


def test_personal_credit_report_markdown_new_summary_table() -> None:
    result = run_personal_credit_report_agent(SAMPLE_TEXT)
    markdown = result["report_markdown"]
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert "当前有效信用卡账户数" in markdown
    assert "贷款账户数" in markdown
    assert "未结清贷款账户数" in markdown
    assert "为企业相关还款责任账户数" in markdown


def test_document_extractor_personal_credit_markdown_summary() -> None:
    content = build_structured_extraction(SAMPLE_TEXT, "personal_credit", filename="personal.pdf")
    markdown = content.get("markdown_summary") or ""
    extracted_json = content.get("extracted_json") or {}
    data = content.get("data") or {}
    assert content["document_type_code"] == "personal_credit_report"
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert "| 项目 | 数量 / 状态 |" in (content.get("report_markdown") or "")
    assert "| 项目 | 数量 / 状态 |" in (extracted_json.get("report_markdown") or "")
    assert "| 项目 | 数量 / 状态 |" in (data.get("markdown_summary") or "")
    assert "购房贷款账户数" not in markdown
    assert "其他贷款账户数" not in markdown
    assert "担保笔数" not in markdown


def test_profile_sync_keeps_enterprise_and_personal_credit_sections() -> None:
    personal_content = build_structured_extraction(SAMPLE_TEXT, "personal_credit", filename="personal.pdf")
    enterprise_content = {
        "document_type_code": "enterprise_credit",
        "markdown_summary": "## 企业征信\n- 企业征信摘要：已解析",
        "extraction_status": "success",
    }

    class FakeStorage:
        async def get_customer(self, customer_id: str) -> dict[str, str]:
            return {"id": customer_id, "name": "测试客户", "customer_type": "enterprise"}

        async def get_business_extractions_by_customer(self, customer_id: str) -> list[dict[str, object]]:
            return [
                {
                    "extraction_id": "e1",
                    "doc_id": "d-enterprise",
                    "customer_id": customer_id,
                    "extraction_type": "enterprise_credit",
                    "extracted_data": enterprise_content,
                    "extraction_status": "success",
                },
                {
                    "extraction_id": "p1",
                    "doc_id": "d-personal",
                    "customer_id": customer_id,
                    "extraction_type": "personal_credit_report",
                    "extracted_data": personal_content,
                    "extraction_status": "success",
                },
            ]

        async def get_document(self, doc_id: str) -> dict[str, object]:
            return {
                "doc_id": doc_id,
                "file_name": "enterprise.pdf" if doc_id == "d-enterprise" else "personal.pdf",
                "file_path": f"/tmp/{doc_id}.pdf",
            }

        async def list_saved_applications(self, customer_id: str) -> list[dict[str, object]]:
            return []

        async def get_latest_scheme_snapshot(self, customer_id: str) -> None:
            return None

    payload = asyncio.run(build_auto_profile_payload(FakeStorage(), "customer-1"))
    markdown = payload["markdown_content"]
    assert "## 企业征信" in markdown
    assert "## 个人征信报告" in markdown
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert "当前有效信用卡账户数" in markdown
    assert "购房贷款账户数" not in markdown
    assert "其他贷款账户数" not in markdown
    assert "担保笔数" not in markdown


def test_credit_summary_does_not_take_90_from_label() -> None:
    text = """
个人信用报告
信贷记录概要
信用卡 90 天以上逾期账户数 0
信用卡账户数 5
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["credit_card_account_count"] != "90"


def test_credit_summary_inline_values() -> None:
    text = """
个人信用报告
信贷记录概要
信用卡账户数 5
当前有效信用卡账户数 0 / 未显示为有效
贷款账户数 3
未结清贷款账户数 1
信用卡逾期账户数 0
信用卡 90 天以上逾期账户数 0
贷款逾期账户数 0
贷款 90 天以上逾期账户数 0
为个人相关还款责任账户数 0 / 未显示
为企业相关还款责任账户数 9
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0 / 未显示为有效"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0 / 未显示"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "9"


def test_credit_summary_pipe_table() -> None:
    text = """
个人信用报告
信贷记录概要
| 项目 | 数量 / 状态 |
| 信用卡账户数 | 5 |
| 当前有效信用卡账户数 | 0 / 未显示为有效 |
| 贷款账户数 | 3 |
| 未结清贷款账户数 | 1 |
| 信用卡 90 天以上逾期账户数 | 0 |
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0 / 未显示为有效"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_90d_overdue_account_count"] == "0"


def test_credit_summary_multiline_table() -> None:
    text = """
个人信用报告
信息概要
项目
数量 / 状态
信用卡账户数
5
当前有效信用卡账户数
0 / 未显示为有效
贷款账户数
3
未结清贷款账户数
1
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0 / 未显示为有效"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"


def test_credit_summary_markdown_pipe_table() -> None:
    text = """
个人信用报告
信贷概要
| 项目 | 数量 / 状态 |
| 信用卡账户数 | 5 |
| 当前有效信用卡账户数 | 0 / 未显示为有效 |
| 贷款账户数 | 3 |
| 未结清贷款账户数 | 1 |
| 信用卡 90 天以上逾期账户数 | 0 |
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0 / 未显示为有效"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_90d_overdue_account_count"] == "0"


def test_personal_credit_agent_summary_not_all_unknown() -> None:
    result = run_personal_credit_report_agent("""
个人信用报告
信贷记录概要
信用卡 账 户 数        5
当前 有效 信用卡 账户 数    0 / 未显示为有效
贷款账户数
3
未结清贷款账户数
1
信用卡 90 天 以上 逾期 账户 数    0
贷款 90 天 以上 逾期 账户 数    0
""")
    summary = result["report_json"]["credit_summary"]
    assert any(summary.get(key) for key in ("credit_card_account_count", "loan_account_count", "outstanding_loan_account_count"))
    assert summary["credit_card_account_count"] == "5"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    summary_markdown = result["report_markdown"].split("## 四、贷款账户明细", 1)[0]
    assert "| 信用卡账户数 | 5 |" in summary_markdown
    assert "| 贷款账户数 | 3 |" in summary_markdown


def test_skip_zero_balance_loan_without_abnormal() -> None:
    text = """
个人信用报告
贷款账户明细
1. 中国银行 消费贷款 发放金额：10000元 余额：0元 账户状态：已结清 五级分类：正常
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_skip_zero_balance_unknown_status_without_abnormal() -> None:
    text = """
个人信用报告
贷款账户明细
1. 中国银行 消费贷款 发放金额：10000元 余额：0元 账户状态：未识别 五级分类：正常
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_keep_abnormal_settled_loan() -> None:
    text = """
个人信用报告
贷款账户明细
1. 中国银行 消费贷款 发放金额：10000元 余额：0元 账户状态：已结清 五级分类：次级 当前逾期金额：100元
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert len(report["loan_accounts"]) == 1


def test_skip_closed_credit_card_without_abnormal() -> None:
    text = """
个人信用报告
贷记卡账户明细
1. 招商银行信用卡 贷记卡 授信额度：50000元 已用额度：0元 账户状态：销户
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []
