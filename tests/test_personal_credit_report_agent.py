from __future__ import annotations

import asyncio

from backend.document_types import normalize_document_type_code
from backend.services.document_extractor_service import build_structured_extraction
from backend.services import markdown_profile_service
from backend.services.markdown_profile_service import build_auto_profile_payload
from backend.services.personal_credit_report_agent.extract_basic_info import extract_basic_info
from backend.services.personal_credit_report_agent.extract_credit_summary import extract_credit_summary
from backend.services.personal_credit_report_agent.extract_credit_card_accounts import clean_credit_card_candidate_text, extract_credit_card_accounts, parse_credit_card_account_block, recover_rmb_active_credit_cards
from backend.services.personal_credit_report_agent.extract_loan_accounts import parse_personal_loan_sentence
from backend.services.personal_credit_report_agent.extract_non_credit_transactions import extract_non_credit_transactions
from backend.services.personal_credit_report_agent.extract_related_repayment_responsibilities import extract_related_repayment_responsibilities
from backend.services.personal_credit_report_agent.extract_query_records import build_query_statistics, extract_query_records, is_countable_query_reason
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
        "non_credit_transactions",
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
    assert isinstance(report["non_credit_transactions"], list)
    assert isinstance(report["query_records"], list)


def test_markdown_renderer_not_empty() -> None:
    report = run_personal_credit_report_agent(SAMPLE_TEXT)["report_json"]
    markdown = render_personal_credit_markdown(report)
    assert "# 个人征信报告" in markdown
    assert "## 十、查询记录" in markdown
    assert "## 十二、待核验项" in markdown


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
    assert summary["active_credit_card_account_count"] == "0"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
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
    assert "购房贷款账户数" in markdown
    assert "其他贷款账户数" in markdown


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
    assert "购房贷款账户数" in markdown
    assert "其他贷款账户数" in markdown
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
    assert "购房贷款账户数" in markdown
    assert "其他贷款账户数" in markdown
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
    assert summary["active_credit_card_account_count"] == "0"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
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
    assert summary["active_credit_card_account_count"] == "0"
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
    assert summary["active_credit_card_account_count"] == "0"
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
    assert summary["active_credit_card_account_count"] == "0"
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


def test_normalize_personal_credit_document_type() -> None:
    assert normalize_document_type_code("personal_credit") == "personal_credit_report"
    assert normalize_document_type_code("个人征信") == "personal_credit_report"
    assert normalize_document_type_code("个人征信报告") == "personal_credit_report"
    assert normalize_document_type_code("personal_credit_report") == "personal_credit_report"
    assert normalize_document_type_code("enterprise_credit") == "enterprise_credit_report"
    assert normalize_document_type_code("企业征信") == "enterprise_credit_report"


def test_personal_credit_uses_new_agent_markdown() -> None:
    content = build_structured_extraction(SAMPLE_TEXT, "personal_credit", filename="personal.pdf")
    markdown = content.get("markdown_summary") or ""
    extracted_json = content.get("extracted_json") or {}
    assert content["document_type_code"] == "personal_credit_report"
    assert extracted_json.get("report_type") == "personal_credit_report"
    assert "个人征信报告" in markdown
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert "购房贷款账户数" in markdown
    assert "其他贷款账户数" in markdown
    assert "担保笔数" not in markdown


def test_profile_sync_personal_credit_does_not_use_enterprise_renderer() -> None:
    personal_content = build_structured_extraction(SAMPLE_TEXT, "personal_credit", filename="personal.pdf")
    personal_content["markdown_summary"] = (personal_content.get("markdown_summary") or "") + "\n\n<!-- personal-credit-agent-markdown -->"
    enterprise_content = {
        "document_type_code": "enterprise_credit_report",
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
                    "extraction_type": "enterprise_credit_report",
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

    markdown_profile_service.DEPRECATED_PERSONAL_CREDIT_RENDERER_CALLED = False
    payload = asyncio.run(build_auto_profile_payload(FakeStorage(), "customer-legacy-isolation"))
    markdown = payload["markdown_content"]
    assert "## 企业征信" in markdown
    assert "## 个人征信报告" in markdown
    assert "<!-- personal-credit-agent-markdown -->" in markdown
    assert "| 项目 | 数量 / 状态 |" in markdown
    assert not markdown_profile_service.DEPRECATED_PERSONAL_CREDIT_RENDERER_CALLED


def test_legacy_personal_credit_renderer_not_called_for_new_upload() -> None:
    markdown_profile_service.DEPRECATED_PERSONAL_CREDIT_RENDERER_CALLED = False
    content = build_structured_extraction(SAMPLE_TEXT, "personal_credit", filename="personal.pdf")
    assert content["document_type_code"] == "personal_credit_report"
    assert content.get("markdown_summary")
    assert not markdown_profile_service.DEPRECATED_PERSONAL_CREDIT_RENDERER_CALLED


def _assert_full_new_summary(summary: dict[str, object]) -> None:
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "9"


def test_credit_summary_columnar_ocr_table() -> None:
    text = """
个人征信报告
信贷记录概要
项目 数量 / 状态
信用卡账户数 当前有效信用卡账户数 贷款账户数 未结清贷款账户数 信用卡逾期账户数 信用卡90天以上逾期账户数 贷款逾期账户数 贷款90天以上逾期账户数 为个人相关还款责任账户数 为企业相关还款责任账户数
5 0 / 未显示为有效 3 1 0 0 0 0 0 / 未显示 9
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    _assert_full_new_summary(summary)


def test_credit_summary_multiline_labels_then_values() -> None:
    text = """
个人征信报告
信贷记录概要
信用卡账户数
当前有效信用卡账户数
贷款账户数
未结清贷款账户数
信用卡逾期账户数
信用卡90天以上逾期账户数
贷款逾期账户数
贷款90天以上逾期账户数
为个人相关还款责任账户数
为企业相关还款责任账户数
5
0 / 未显示为有效
3
1
0
0
0
0
0 / 未显示
9
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    _assert_full_new_summary(summary)


def test_credit_summary_no_label_90_pollution_columnar() -> None:
    text = """
个人征信报告
信贷记录概要
信用卡账户数 信用卡90天以上逾期账户数 贷款账户数 贷款90天以上逾期账户数 当前有效信用卡账户数 未结清贷款账户数
5 0 3 0 0 / 未显示为有效 1
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] != "90"
    assert summary["loan_account_count"] != "90"
    assert summary["credit_card_account_count"] == "5"
    assert summary["credit_card_90d_overdue_account_count"] == "0"


def test_credit_summary_current_realistic_values_not_all_unknown() -> None:
    text = """
个人信用报告
中国人民银行征信中心
报告编号：2025031104013907986945
信贷记录概要
项目 数量 / 状态
信用卡账户数 当前有效信用卡账户数 贷款账户数 未结清贷款账户数
信用卡逾期账户数 信用卡 90 天以上逾期账户数 贷款逾期账户数
贷款 90 天以上逾期账户数 为个人相关还款责任账户数 为企业相关还款责任账户数
5 0 / 未显示为有效 3 1 0 0 0 0 0 / 未显示 9
查询记录
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    recognized = [value for value in summary.values() if value not in (None, "", "未识别")]
    assert len(recognized) >= 6
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "9"
    assert summary["credit_card_account_count"] == "5"


def test_credit_summary_real_ocr_matrix() -> None:
    text = """
个人信用报告
信贷记录
这部分包含您的信用卡、贷款和其他信贷记录。金额类数据均以人民币计算,精确到元。
信用卡贷款
其他业务 逾期记录可能影响对您的信用评价。
购房 其他
账户数 5 -- 3 -- 购房贷款,包括个人住房贷款、个人商用
房(包括商住两用)贷款和个人住房公积
金贷款。未结清/未销户账户数 -- -- 1 --
发生过逾期的账户数 -- -- -- -- 发生过逾期的信用卡账户,指曾经“未按
时还最低还款额”的贷记卡账户和“透支
超过60天”的准贷记卡账户。发生过90天以上逾期的账户数 -- -- -- --
为个人 为企业
相关还款责任账户数 -- 9
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "5"
    assert summary["active_credit_card_account_count"] == "0"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "9"


def test_credit_summary_dash_values_render_as_zero() -> None:
    text = """
信息概要
信用卡 贷款 其他业务
购房 其他
账户数 38 -- 8 --
未结清/未销户账户数 32 -- 1 --
发生过逾期的账户数 -- -- -- --
发生过90天以上逾期的账户数 -- -- -- --
为个人 为企业
相关还款责任账户数 -- 10
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_account_count"] == "38"
    assert summary["active_credit_card_account_count"] == "32"
    assert summary["loan_account_count"] == "8"
    assert summary["housing_loan_account_count"] == "0"
    assert summary["other_loan_account_count"] == "8"
    assert summary["outstanding_loan_account_count"] == "1"
    assert summary["housing_loan_outstanding_count"] == "0"
    assert summary["other_loan_outstanding_count"] == "1"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "10"


def test_credit_summary_does_not_extract_60_from_explanation() -> None:
    text = """
信息概要
信用卡 贷款 其他业务
购房 其他
账户数 38 -- 8 --
未结清/未销户账户数 32 -- 1 --
发生过逾期的账户数 -- -- -- -- 发生过逾期的信用卡账户，指曾经“未按时还最低还款额”的贷记卡账户和“透支超过60天”的准贷记卡账户。
发生过90天以上逾期的账户数 -- -- -- --
为个人 为企业
相关还款责任账户数 -- 10
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] != "60"
    assert summary["loan_90d_overdue_account_count"] != "60"


def test_related_repayment_personal_enterprise_column_mapping() -> None:
    text = """
信息概要
为个人 为企业
相关还款责任账户数 -- 10
"""
    summary = run_personal_credit_report_agent(text)["report_json"]["credit_summary"]
    assert summary["personal_related_repayment_responsibility_account_count"] == "0"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "10"


def test_credit_summary_markdown_no_unrecognized_or_unshown() -> None:
    text = """
信息概要
信用卡 贷款 其他业务
购房 其他
账户数 38 -- 8 --
未结清/未销户账户数 32 -- 1 --
发生过逾期的账户数 -- -- -- --
发生过90天以上逾期的账户数 -- -- -- --
为个人 为企业
相关还款责任账户数 -- 10
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    summary_markdown = markdown.split("## 三、信贷记录概要", 1)[1].split("## 四、贷款账户明细", 1)[0]
    assert "未显示" not in summary_markdown
    assert "未识别" not in summary_markdown
    assert "0 / 未显示" not in summary_markdown
    assert "| 购房贷款账户数 | 0 |" in summary_markdown
    assert "| 信用卡 90 天以上逾期账户数 | 0 |" in summary_markdown
    assert "| 为个人相关还款责任账户数 | 0 |" in summary_markdown
    assert "| 为企业相关还款责任账户数 | 10 |" in summary_markdown


def test_loan_accounts_skip_related_repayment_responsibility() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:D10023010H00012024052800000716)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []
    assert all(item.get("account_no") != "D10023010H00012024052800000716" for item in report["loan_accounts"])


def test_loan_accounts_skip_query_record_pollution() -> None:
    text = """
个人征信报告
查询记录明细
2025年02月20日 南京银行股份有限公司上海虹口支行 贷款审批
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_loan_accounts_skip_settled_loan() -> None:
    text = """
个人征信报告
贷款
2017年12月16日深圳前海微众银行股份有限公司发放的10,000元(人民币)其他个人消费贷款，2018年01月已结清。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_loan_accounts_skip_zero_balance_no_abnormal_real_sentence() -> None:
    text = """
个人征信报告
贷款
2023年01月15日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信，额度有效期至2027年02月15日，可循环使用。截至2025年02月，信用额度100元(人民币)，余额为0，当前无逾期。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert len(report["loan_accounts"]) == 1
    assert report["loan_accounts"][0]["account_status"] == "当前有效"


def test_loan_accounts_keep_active_balance_own_loan() -> None:
    text = """
个人征信报告
贷款
2024年01月15日某某银行股份有限公司发放的100,000元(人民币)其他个人消费贷款，截至2025年02月，余额为50,000，当前无逾期。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    loans = report["loan_accounts"]
    assert len(loans) == 1
    assert "某某银行" in loans[0]["institution"]
    assert loans[0]["balance"] == "50,000"
    assert "其他个人消费贷款" in loans[0]["business_type"]


def test_loan_accounts_keep_abnormal_loan_real_sentence() -> None:
    text = """
个人征信报告
贷款
2024年01月15日某某银行发放的100,000元其他个人消费贷款，截至2025年02月，余额为0，当前逾期金额1,000，五级分类关注。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    loans = report["loan_accounts"]
    assert len(loans) == 1
    assert "1,000" in loans[0]["overdue_amount"]
    assert "关注" in loans[0]["five_category"]


ACTIVE_PERSONAL_LOAN_TEXT = """
个人征信报告
贷款
2025年04月25日无锡锡商银行股份有限公司发放的1,000,000元（人民币）个人经营性贷款，2026年04月25日到期。截至2026年03月，余额1,000,000。
2025年06月19日浙江泰隆商业银行股份有限公司宁波分行发放的1,500,000元（人民币）个人经营性贷款，2026年06月10日到期。截至2026年03月，余额1,500,000。
2025年11月10日河南中原消费金融股份有限公司发放的49,180元（人民币）其他个人消费贷款，2026年11月10日到期。截至2026年03月，余额33,103。
2022年01月21日浙江泰隆商业银行股份有限公司宁波分行为个人经营性贷款授信，额度有效期至2026年12月31日，可循环使用。截至2026年03月，信用额度500,000元（人民币），余额为500,000，当前无逾期。
2023年03月23日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信，额度有效期至2029年03月23日，可循环使用。截至2026年03月，信用额度35,200元（人民币），余额为18,253，当前无逾期。
2023年09月27日江苏苏商银行股份有限公司为个人经营性贷款授信，额度有效期至2026年09月25日，可循环使用。截至2026年02月，信用额度180,000元（人民币），余额为0，当前无逾期。
2024年09月10日江苏苏商银行股份有限公司为其他贷款授信，额度有效期至2026年09月05日，可循环使用。截至2026年02月，信用额度100,000元（人民币），余额为47,009，当前无逾期。
2025年05月08日浙江网商银行股份有限公司为个人经营性贷款授信，额度有效期至2026年10月18日，可循环使用。截至2026年03月，信用额度234,000元（人民币），余额为161,667，当前无逾期。
2025年10月10日武汉众邦银行股份有限公司为其他个人消费贷款授信，额度有效期至2026年10月10日，可循环使用。截至2026年03月，信用额度57,300元（人民币），余额为33,848，当前无逾期。
2025年12月18日中信百信银行股份有限公司为其他个人消费贷款授信，额度有效期至2027年12月18日，可循环使用。截至2026年03月，信用额度116,100元（人民币），余额为20,689，当前无逾期。
2006年03月31日中国建设银行股份有限公司上海市分行发放的500,000元（人民币）个人住房商业贷款，2009年05月已结清。
"""


def test_personal_loan_parse_direct_loan() -> None:
    parsed = parse_personal_loan_sentence(
        "2025年04月25日无锡锡商银行股份有限公司发放的1,000,000元（人民币）个人经营性贷款，2026年04月25日到期。截至2026年03月，余额1,000,000。"
    )
    assert parsed["start_date"] == "2025-04-25"
    assert parsed["institution"] == "无锡锡商银行股份有限公司"
    assert parsed["amount"] == "1,000,000元"
    assert parsed["loan_type"] == "个人经营性贷款"
    assert parsed["due_date"] == "2026-04-25"
    assert parsed["cutoff_date"] == "2026-03"
    assert parsed["balance"] == "1,000,000元"
    assert parsed["overdue_status"] == "无"


def test_personal_loan_parse_revolving_credit() -> None:
    parsed = parse_personal_loan_sentence(
        "2023年03月23日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信，额度有效期至2029年03月23日，可循环使用。截至2026年03月，信用额度35,200元（人民币），余额为18,253，当前无逾期。"
    )
    assert parsed["start_date"] == "2023-03-23"
    assert parsed["institution"] == "重庆蚂蚁消费金融有限公司"
    assert parsed["amount"] == "35,200元"
    assert parsed["loan_type"] == "其他个人消费贷款授信"
    assert parsed["due_date"] == "2029-03-23"
    assert parsed["cutoff_date"] == "2026-03"
    assert parsed["balance"] == "18,253元"
    assert parsed["overdue_status"] == "当前无逾期"


def test_personal_loan_extract_10_active_records() -> None:
    report = run_personal_credit_report_agent(ACTIVE_PERSONAL_LOAN_TEXT)["report_json"]
    assert len(report["loan_accounts"]) == 10
    assert report["loan_accounts"][0]["institution"] == "无锡锡商银行股份有限公司"
    assert report["loan_accounts"][5]["balance"] == "0元"


def test_personal_loan_skip_settled_records() -> None:
    text = """
个人征信报告
贷款
2006年03月31日中国建设银行股份有限公司上海市分行发放的500,000元（人民币）个人住房商业贷款，2009年05月已结清。
2009年05月14日浙江省象山县农村信用合作联社发放的800,000元（人民币）个人住房商业贷款，2010年05月已结清。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"] == []


def test_personal_loan_markdown_compact_fields() -> None:
    markdown = run_personal_credit_report_agent(ACTIVE_PERSONAL_LOAN_TEXT)["report_markdown"]
    loan_section = markdown.split("## 四、贷款账户明细", 1)[1].split("## 五、信用卡账户明细", 1)[0]
    assert "起始日期：2025-04-25" in loan_section
    assert "机构：无锡锡商银行股份有限公司" in loan_section
    assert "金额：1,000,000元" in loan_section
    assert "类型：个人经营性贷款" in loan_section
    assert "到期日期：2026-04-25" in loan_section
    assert "截止日期：2026-03" in loan_section
    assert "余额：1,000,000元" in loan_section
    assert "逾期：无" in loan_section
    assert "账户编号" not in loan_section
    assert "五级分类" not in loan_section
    assert "最近还款日期" not in loan_section
    assert "历史表现" not in loan_section
    assert "信息报告日期" not in loan_section


def test_loan_section_not_truncated_by_page_noise() -> None:
    text = ACTIVE_PERSONAL_LOAN_TEXT.replace(
        "2025年11月10日河南中原消费金融股份有限公司",
        "报告编号：2026031104013907986945\n信贷记录\n信息概要\n信用卡贷款\n购房 其他\n账户数 11 3 38 --\n第 1 页,共 9 页\n3.\n2025年11月10日河南中原消费金融股份有限公司",
    )
    report = run_personal_credit_report_agent(text)["report_json"]
    assert len(report["loan_accounts"]) == 10


def test_direct_loan_2_parse_with_wrapped_due_date_and_noise() -> None:
    text = """
个人征信报告
贷款
2025年06月19日浙江泰隆商业银行股份有限公司宁波分行发放的1,500,000元(人民币)个人经营性贷款,2026年06月10日到
期。截至2026年03月,余额1,500,000。
报告编号:2026040116014183292476 报告时间:2026-04-01 16:01:41
信贷记录
账户数 11 3 38 --
"""
    account = run_personal_credit_report_agent(text)["report_json"]["loan_accounts"][0]
    assert account["start_date"] == "2025-06-19"
    assert account["institution"] == "浙江泰隆商业银行股份有限公司宁波分行"
    assert account["amount"] == "1,500,000元"
    assert account["loan_type"] == "个人经营性贷款"
    assert account["due_date"] == "2026-06-10"
    assert account["cutoff_date"] == "2026-03"
    assert account["balance"] == "1,500,000元"
    assert account["overdue_status"] == "无"


def test_revolving_loan_20220121_should_not_be_dropped() -> None:
    account = run_personal_credit_report_agent(
        "个人征信报告\n贷款\n2022年01月21日浙江泰隆商业银行股份有限公司宁波分行为个人经营性贷款授信,额度有效期至2026年12月31日,可循环使用。截至2026年03月,信用额度500,000元(人民币),余额为500,000,当前无逾期。"
    )["report_json"]["loan_accounts"][0]
    assert account["start_date"] == "2022-01-21"
    assert account["institution"] == "浙江泰隆商业银行股份有限公司宁波分行"
    assert account["amount"] == "500,000元"
    assert account["loan_type"] == "个人经营性贷款授信"
    assert account["due_date"] == "2026-12-31"
    assert account["cutoff_date"] == "2026-03"
    assert account["balance"] == "500,000元"
    assert account["overdue_status"] == "当前无逾期"


def test_loan_type_keep_credit_grant_suffix() -> None:
    samples = [
        ("2022年01月21日浙江泰隆商业银行股份有限公司宁波分行为个人经营性贷款授信,额度有效期至2026年12月31日,可循环使用。截至2026年03月,信用额度500,000元(人民币),余额为500,000,当前无逾期。", "个人经营性贷款授信"),
        ("2023年03月23日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信,额度有效期至2029年03月23日,可循环使用。截至2026年03月,信用额度35,200元(人民币),余额为18,253,当前无逾期。", "其他个人消费贷款授信"),
        ("2024年09月10日江苏苏商银行股份有限公司为其他贷款授信,额度有效期至2026年09月05日,可循环使用。截至2026年02月,信用额度100,000元(人民币),余额为47,009,当前无逾期。", "其他贷款授信"),
    ]
    for text, expected in samples:
        assert parse_personal_loan_sentence(text)["loan_type"] == expected


def test_loan_extract_10_records_from_sample() -> None:
    report = run_personal_credit_report_agent(ACTIVE_PERSONAL_LOAN_TEXT)["report_json"]
    loans = report["loan_accounts"]
    assert len(loans) == 10
    assert any(item["start_date"] == "2022-01-21" and item["institution"] == "浙江泰隆商业银行股份有限公司宁波分行" for item in loans)
    loan2 = loans[1]
    assert loan2["start_date"] == "2025-06-19"
    assert loan2["amount"] == "1,500,000元"
    assert loan2["loan_type"] == "个人经营性贷款"
    assert loan2["due_date"] == "2026-06-10"
    assert loan2["cutoff_date"] == "2026-03"
    assert loan2["balance"] == "1,500,000元"
    assert "购房贷款" not in loan2["loan_type"]
    assert "记录可能影响对您的信用评价" not in str(loans)


def test_loan_markdown_no_noise() -> None:
    markdown = run_personal_credit_report_agent(ACTIVE_PERSONAL_LOAN_TEXT)["report_markdown"]
    loan_section = markdown.split("## 四、贷款账户明细", 1)[1].split("## 五、信用卡账户明细", 1)[0]
    assert "报告编号" not in loan_section
    assert "信贷记录" not in loan_section
    assert "账户数 11 3 38" not in loan_section
    assert "记录可能影响对您的信用评价" not in loan_section


def test_direct_loan_overdue_status_from_section_title() -> None:
    text = """
个人征信报告
贷款
从未发生过逾期的账户明细如下:
2025年04月25日无锡锡商银行股份有限公司发放的1,000,000元(人民币)个人经营性贷款,2026年04月25日到期。截至2026年03月,余额1,000,000。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["loan_accounts"][0]["overdue_status"] == "无"


def test_revolving_loan_overdue_status_current_no_overdue() -> None:
    report = run_personal_credit_report_agent(
        "个人征信报告\n贷款\n2023年03月23日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信,额度有效期至2029年03月23日,可循环使用。截至2026年03月,信用额度35,200元(人民币),余额为18,253,当前无逾期。"
    )["report_json"]
    assert report["loan_accounts"][0]["overdue_status"] == "当前无逾期"


def test_credit_card_skip_closed_accounts_from_list() -> None:
    text = """
个人征信报告
信用卡
从未逾期过的贷记卡及透支未超过60天的准贷记卡账户明细如下：
1. 2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡（美元账户），2009年09月销户。
2. 2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡（人民币账户），2009年09月销户。
3. 2008年02月28日平安银行股份有限公司信用卡中心发放的贷记卡（美元账户），2018年10月销户。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []


def test_credit_card_parse_closed_status_correctly() -> None:
    text = "2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡（美元账户），2009年09月销户。"
    parsed = parse_credit_card_account_block(text)
    assert parsed["account_status"] == "销户"
    assert parsed["card_type"] == "贷记卡"
    assert parsed["currency"] == "美元"
    assert parsed["institution"] == "中国建设银行股份有限公司上海市分行"
    records = extract_credit_card_accounts({"credit_card_accounts": text, "credit_transaction_details": "", "full_text": text})
    assert records == []
    report = run_personal_credit_report_agent(f"个人征信报告\n信用卡\n{text}")["report_json"]
    assert report["credit_card_accounts"] == []


def test_credit_card_keep_active_account() -> None:
    text = """
个人征信报告
信用卡
2024年01月01日某某银行股份有限公司发放的贷记卡（人民币账户），授信额度50,000元，已用额度10,000元，账户状态正常，当前无逾期。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    cards = report["credit_card_accounts"]
    assert len(cards) == 1
    assert "某某银行" in cards[0]["institution"]
    assert cards[0]["card_type"] == "贷记卡"
    assert cards[0]["currency"] == "人民币"
    assert "50,000" in cards[0]["credit_limit"]
    assert "10,000" in cards[0]["used_limit"]
    assert "正常" in cards[0]["account_status"]


def test_credit_card_keep_closed_abnormal_account() -> None:
    text = """
个人征信报告
信用卡
2020年01月01日某某银行发放的贷记卡（人民币账户），2023年01月销户，当前逾期金额1,000元。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    cards = report["credit_card_accounts"]
    assert len(cards) == 1
    assert "销户" in cards[0]["account_status"]
    assert "1,000" in cards[0]["overdue_amount"]


def test_credit_card_not_take_loan_credit_limit() -> None:
    text = """
个人征信报告
信用卡
2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡（美元账户），2009年09月销户。

贷款
2023年01月15日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信，截至2025年02月，信用额度100元，余额为0，当前无逾期。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []


def test_credit_card_closed_list_all_filtered() -> None:
    text = """
个人征信报告
信用卡
从未逾期过的贷记卡及透支未超过60天的准贷记卡账户明细如下:
1. 2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡(美元账户),2009年09月销户。
2. 2006年08月25日中国建设银行股份有限公司上海市分行发放的贷记卡(人民币账户),2009年09月销户。
贷款
2023年01月15日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信,截至2025年02月,信用额度100元(人民币),余额为0,当前无逾期。
"""
    result = run_personal_credit_report_agent(text)
    report = result["report_json"]
    markdown = result["report_markdown"]
    assert report["credit_card_accounts"] == []
    assert "暂无需要展示的当前有效人民币信用卡账户" in markdown
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "中国建设银行股份有限公司上海市分行" not in card_section
    assert "授信额度：100元" not in card_section
    assert "账户状态：未销户" not in card_section


ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT = """
个人征信报告
信贷记录概要
当前有效信用卡账户数 5
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡（人民币账户，卡片尾号：6049）。截至2026年03月，信用额度2,000，已使用额度0。
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡（美元账户，卡片尾号：6049）。截至2026年03月，信用额度2,000，已使用额度0。
2012年10月24日中国光大银行股份有限公司信用卡中心发放的贷记卡（美元账户，卡片尾号：8186）。截至2026年03月，信用额度0，已使用额度0。
2012年10月24日中国光大银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：8186）。截至2026年03月，信用额度0，已使用额度0。
2024年12月02日兴业银行股份有限公司发放的贷记卡（人民币账户）。截至2026年03月，信用额度15,000，已使用额度0。
2008年01月29日上海银行股份有限公司信用卡中心发放的贷记卡（人民币账户），2021年01月销户。
2008年01月29日上海银行股份有限公司信用卡中心发放的贷记卡（美元账户），2021年01月销户。
2023年09月11日招商银行股份有限公司信用卡中心发放的贷记卡（人民币账户），2025年03月销户。
贷款
2023年01月15日重庆蚂蚁消费金融有限公司为其他个人消费贷款授信，截至2025年02月，信用额度100元(人民币)，余额为0，当前无逾期。
"""


def test_active_credit_cards_are_extracted() -> None:
    report = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)["report_json"]
    cards = report["credit_card_accounts"]
    assert len(cards) == 3
    assert all(card["account_status"] == "当前有效" for card in cards)
    assert all(card["currency"] == "人民币" for card in cards)
    assert all("销户" not in card["evidence"] for card in cards)
    assert all("美元账户" not in card["evidence_text"] for card in cards)
    assert cards[0]["open_date"] == "2006-10-27"
    assert cards[0]["institution"] == "中国建设银行股份有限公司上海宝钢宝山支行"
    assert cards[0]["card_type"] == "贷记卡"
    assert cards[0]["currency"] == "人民币"
    assert cards[0]["card_tail_no"] == "6049"
    assert cards[0]["credit_limit"] == "2,000"
    assert cards[0]["used_limit"] == "0"
    assert cards[0]["report_cutoff"] == "2026-03"


def test_closed_credit_cards_are_filtered() -> None:
    closed = "2008年01月29日上海银行股份有限公司信用卡中心发放的贷记卡（人民币账户），2021年01月销户。"
    parsed = parse_credit_card_account_block(closed)
    assert parsed["account_status"] == "销户"
    assert parsed["is_closed"] is True
    result = run_personal_credit_report_agent(f"个人征信报告\n信用卡\n{closed}")
    assert result["report_json"]["credit_card_accounts"] == []
    card_section = result["report_markdown"].split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "上海银行股份有限公司信用卡中心" not in card_section


def test_credit_card_summary_consistency() -> None:
    result = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)
    markdown = result["report_markdown"]
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "暂无需要展示的当前有效人民币信用卡账户" not in card_section
    assert card_section.count("### 账户 ") >= 3
    assert "授信额度：100元" not in card_section


def test_credit_card_filter_usd_accounts() -> None:
    text = """
个人征信报告
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡（美元账户，卡片尾号：6049）。截至2026年03月，信用额度2,000，已使用额度0。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []


def test_credit_card_keep_rmb_accounts() -> None:
    text = """
个人征信报告
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡（人民币账户，卡片尾号：6049）。截至2026年03月，信用额度2,000，已使用额度0。
"""
    cards = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"
    assert cards[0]["card_tail_no"] == "6049"
    assert cards[0]["credit_limit"] == "2,000"
    assert cards[0]["used_amount"] == "0"


def test_credit_card_keep_only_rmb_active_cards_from_sample() -> None:
    cards = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)["report_json"]["credit_card_accounts"]
    assert len(cards) == 3
    assert all(card["currency"] == "人民币" for card in cards)
    assert all(card["currency"] != "美元" for card in cards)
    assert all("美元账户" not in card["evidence_text"] for card in cards)
    assert all("销户" not in card["evidence_text"] for card in cards)


def test_credit_card_markdown_no_usd() -> None:
    markdown = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)["report_markdown"]
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "币种：美元" not in card_section
    assert "美元账户" not in card_section
    assert "币种：人民币" in card_section
    assert "中国建设银行股份有限公司上海宝钢宝山支行" in card_section
    assert "中国光大银行股份有限公司信用卡中心" in card_section
    assert "兴业银行股份有限公司" in card_section


def test_credit_card_keep_rmb_when_same_tail_usd_exists() -> None:
    text = """
个人征信报告
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(人民币账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(美元账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
"""
    cards = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"
    assert cards[0]["institution"] == "中国建设银行股份有限公司上海宝钢宝山支行"
    assert cards[0]["card_tail_no"] == "6049"
    assert cards[0]["credit_limit"] == "2,000"
    assert "美元账户" not in cards[0]["evidence_text"]


def test_credit_card_active_rmb_sample_should_show_3() -> None:
    cards = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)["report_json"]["credit_card_accounts"]
    institutions = {card["institution"] for card in cards}
    assert len(cards) == 3
    assert "中国建设银行股份有限公司上海宝钢宝山支行" in institutions
    assert "中国光大银行股份有限公司信用卡中心" in institutions
    assert "兴业银行股份有限公司" in institutions
    assert all(card["currency"] == "人民币" for card in cards)
    assert all("美元" not in card["currency"] for card in cards)
    assert all("销户" not in card["evidence_text"] for card in cards)


def test_credit_card_dedupe_key_includes_currency() -> None:
    text = """
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(人民币账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(美元账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
"""
    parsed = extract_credit_card_accounts({"full_text": text})
    assert len(parsed) == 2
    assert {item["currency"] for item in parsed} == {"人民币", "美元"}
    cards = run_personal_credit_report_agent(f"个人征信报告\n{text}")["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"


def test_credit_card_markdown_contains_ccb_6049() -> None:
    markdown = run_personal_credit_report_agent(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT)["report_markdown"]
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "中国建设银行股份有限公司上海宝钢宝山支行" in card_section
    assert "卡片尾号：6049" in card_section
    assert "信用额度：2,000" in card_section
    assert "币种：人民币" in card_section
    assert "币种：美元" not in card_section


def test_credit_card_parse_wrapped_cutoff_date() -> None:
    text = "2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(人民币账户,卡片尾号:6049)。截至2026年\n03月,信用额度2,000,已使用额度0。"
    parsed = parse_credit_card_account_block(text)
    assert parsed["issuer"] == "中国建设银行股份有限公司上海宝钢宝山支行"
    assert parsed["currency"] == "人民币"
    assert parsed["card_tail_no"] == "6049"
    assert parsed["credit_limit"] == "2,000"
    assert parsed["used_amount"] == "0"
    assert parsed["report_cutoff"] == "2026-03"


def test_credit_card_keep_rmb_same_tail_as_usd() -> None:
    text = """
个人征信报告
信用卡
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(人民币账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
2006年10月27日中国建设银行股份有限公司上海宝钢宝山支行发放的贷记卡(美元账户,卡片尾号:6049)。截至2026年03月,信用额度2,000,已使用额度0。
"""
    parsed = extract_credit_card_accounts({"full_text": text})
    assert len(parsed) == 2
    cards = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"
    assert cards[0]["card_tail_no"] == "6049"
    assert cards[0]["institution"] == "中国建设银行股份有限公司上海宝钢宝山支行"


def test_credit_card_recovery_append_missing_ccb_6049() -> None:
    existing = [
        {
            "open_date": "2012-10-24",
            "institution": "中国光大银行股份有限公司信用卡中心",
            "issuer": "中国光大银行股份有限公司信用卡中心",
            "card_type": "贷记卡",
            "currency": "人民币",
            "card_tail_no": "8186",
            "credit_limit": "0",
            "used_limit": "0",
            "used_amount": "0",
            "account_status": "当前有效",
            "report_cutoff": "2026-03",
        },
        {
            "open_date": "2024-12-02",
            "institution": "兴业银行股份有限公司",
            "issuer": "兴业银行股份有限公司",
            "card_type": "贷记卡",
            "currency": "人民币",
            "card_tail_no": "",
            "credit_limit": "15,000",
            "used_limit": "0",
            "used_amount": "0",
            "account_status": "当前有效",
            "report_cutoff": "2026-03",
        },
    ]
    recovered = recover_rmb_active_credit_cards(
        segment_report(ACTIVE_AND_CLOSED_CREDIT_CARD_TEXT),
        existing,
        {"active_credit_card_account_count": "5"},
    )
    assert len(recovered) == 3
    assert any(
        item.get("institution") == "中国建设银行股份有限公司上海宝钢宝山支行"
        and item.get("currency") == "人民币"
        and item.get("card_tail_no") == "6049"
        for item in recovered
    )


def test_credit_card_limit_not_use_used_amount() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
"""
    cards = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    card = cards[0]
    assert card["open_date"] == "2020-11-27"
    assert card["institution"] == "广发银行股份有限公司信用卡中心"
    assert card["card_type"] == "贷记卡"
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] == "1019"
    assert card["credit_limit"] == "38,000"
    assert card["used_amount"] == "440"
    assert card["account_status"] == "当前有效"
    assert card["report_cutoff"] == "2025-12"


def test_credit_card_shanghai_rural_commercial_bank_not_dropped() -> None:
    text = """
个人征信报告
信用卡
2021年09月10日上海农村商业银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度30,000，已使用额度0。
"""
    result = run_personal_credit_report_agent(text)
    cards = result["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    card = cards[0]
    assert card["institution"] == "上海农村商业银行股份有限公司"
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] == ""
    assert card["credit_limit"] == "30,000"
    assert card["used_amount"] == "0"
    assert card["account_status"] == "当前有效"
    assert "上海农村商业银行股份有限公司" in result["report_markdown"]
    assert "信用额度：30,000" in result["report_markdown"]


def test_credit_card_icbc_rmb_8222_limit_100000() -> None:
    text = """
个人征信报告
信用卡
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（日元账户，卡片尾号：8222）。截至2026年01月，信用额度74,383，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（美元账户，卡片尾号：8222）。截至2026年01月，信用额度102,006，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（新加坡元账户，卡片尾号：8222）。截至2026年01月，信用额度105,968，已使用额度0。
"""
    parsed = extract_credit_card_accounts({"full_text": text})
    assert len(parsed) == 4
    assert {item["currency"] for item in parsed} == {"日元", "美元", "人民币", "新加坡元"}
    cards = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    card = cards[0]
    assert card["institution"] == "中国工商银行股份有限公司上海市分行"
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] == "8222"
    assert card["credit_limit"] == "100,000"
    assert card["used_amount"] == "0"
    assert card["report_cutoff"] == "2026-01"
    assert card["credit_limit"] not in {"74,383", "102,006", "105,968"}


def test_credit_card_dedupe_keeps_same_tail_different_currency_until_filter() -> None:
    text = """
信用卡
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（美元账户，卡片尾号：8222）。截至2026年01月，信用额度102,006，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
"""
    parsed = extract_credit_card_accounts({"full_text": text})
    assert len(parsed) == 2
    assert {item["currency"] for item in parsed} == {"美元", "人民币"}
    cards = run_personal_credit_report_agent(f"个人征信报告\n{text}")["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"
    assert cards[0]["credit_limit"] == "100,000"


def test_credit_card_markdown_amounts_correct() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
2021年09月10日上海农村商业银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度30,000，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（日元账户，卡片尾号：8222）。截至2026年01月，信用额度74,383，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（美元账户，卡片尾号：8222）。截至2026年01月，信用额度102,006，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "发卡机构：广发银行股份有限公司信用卡中心" in card_section
    assert "信用额度：38,000" in card_section
    assert "已使用额度：440" in card_section
    assert "发卡机构：上海农村商业银行股份有限公司" in card_section
    assert "信用额度：30,000" in card_section
    assert "发卡机构：中国工商银行股份有限公司上海市分行" in card_section
    assert "卡片尾号：8222" in card_section
    assert "信用额度：100,000" in card_section
    assert "信用额度：440" not in card_section
    assert "信用额度：87,186" not in card_section
    assert "信用额度：102,006" not in card_section


def test_credit_card_recovery_updates_missing_limit_and_appends_missing_cards() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
2021年09月10日上海农村商业银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度30,000，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
"""
    existing = [{
        "open_date": "2020-11-27",
        "institution": "广发银行股份有限公司信用卡中心",
        "issuer": "广发银行股份有限公司信用卡中心",
        "card_type": "贷记卡",
        "currency": "人民币",
        "card_tail_no": "1019",
        "credit_limit": "",
        "used_limit": "440",
        "used_amount": "440",
        "account_status": "",
        "report_cutoff": "2025-12",
    }]
    recovered = recover_rmb_active_credit_cards(segment_report(text), existing, {"active_credit_card_account_count": "32"})
    by_issuer = {item["institution"]: item for item in recovered}
    assert len(recovered) == 3
    assert by_issuer["广发银行股份有限公司信用卡中心"]["credit_limit"] == "38,000"
    assert by_issuer["广发银行股份有限公司信用卡中心"]["used_amount"] == "440"
    assert by_issuer["广发银行股份有限公司信用卡中心"]["account_status"] == "当前有效"
    assert by_issuer["上海农村商业银行股份有限公司"]["credit_limit"] == "30,000"
    assert by_issuer["中国工商银行股份有限公司上海市分行"]["credit_limit"] == "100,000"


def test_credit_card_guangfa_limit_38000() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
"""
    card = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"][0]
    assert card["issuer"] == "广发银行股份有限公司信用卡中心"
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] == "1019"
    assert card["credit_limit"] == "38,000"
    assert card["used_amount"] == "440"
    assert card["account_status"] == "当前有效"
    assert card["report_cutoff"] == "2025-12"


def test_credit_card_no_tail_number_should_display() -> None:
    text = """
个人征信报告
信用卡
2021年09月10日上海农村商业银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度30,000，已使用额度0。
"""
    result = run_personal_credit_report_agent(text)
    card = result["report_json"]["credit_card_accounts"][0]
    assert card["issuer"] == "上海农村商业银行股份有限公司"
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] in {"", "未识别", None}
    assert card["credit_limit"] == "30,000"
    assert card["used_amount"] == "0"
    assert card["account_status"] == "当前有效"
    assert "上海农村商业银行股份有限公司" in result["report_markdown"]


def test_credit_card_icbc_rmb_8222_should_display_limit_100000() -> None:
    text = """
个人征信报告
信用卡
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（日元账户，卡片尾号：8222）。截至2026年01月，信用额度74,383，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（美元账户，卡片尾号：8222）。截至2026年01月，信用额度102,006，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（新加坡元账户，卡片尾号：8222）。截至2026年01月，信用额度105,968，已使用额度0。
"""
    result = run_personal_credit_report_agent(text)
    card = result["report_json"]["credit_card_accounts"][0]
    assert card["currency"] == "人民币"
    assert card["card_tail_no"] == "8222"
    assert card["credit_limit"] == "100,000"
    assert card["used_amount"] == "0"
    assert "币种：美元" not in result["report_markdown"]
    assert "授信额度：102,006" not in result["report_markdown"]


def test_credit_card_limit_not_unknown_when_used_amount_exists() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
"""
    card = run_personal_credit_report_agent(text)["report_json"]["credit_card_accounts"][0]
    assert card["credit_limit"] != ""
    assert card["credit_limit"] != "未识别"
    assert card["credit_limit"] != "440"
    assert card["credit_limit"] == "38,000"


def test_credit_card_markdown_contains_all_expected_rmb_cards() -> None:
    text = """
个人征信报告
信用卡
2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡（人民币账户，卡片尾号：1019）。截至2025年12月，信用额度38,000，已使用额度440。
2021年09月10日上海农村商业银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度30,000，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
2025年01月01日北京银行股份有限公司发放的贷记卡（人民币账户）。截至2026年01月，信用额度500,000，已使用额度0。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "广发银行股份有限公司信用卡中心" in markdown
    assert "卡片尾号：1019" in markdown
    assert "信用额度：38,000" in markdown
    assert "已使用额度：440" in markdown
    assert "上海农村商业银行股份有限公司" in markdown
    assert "信用额度：30,000" in markdown
    assert "中国工商银行股份有限公司上海市分行" in markdown
    assert "卡片尾号：8222" in markdown
    assert "信用额度：100,000" in markdown
    assert "北京银行股份有限公司" in markdown
    assert "信用额度：500,000" in markdown


def test_credit_card_clean_candidate_text_handles_ocr_splits() -> None:
    text = "截至2025年\n12月,信 用额 度38,000,已使用额 度440,尚未激 活,销 户"
    cleaned = clean_credit_card_candidate_text(text)
    assert "截至2025年12月" in cleaned
    assert "信用额度38,000" in cleaned
    assert "已使用额度440" in cleaned
    assert "尚未激活" in cleaned
    assert "销户" in cleaned


def test_credit_card_parse_credit_limit_with_ocr_split_xinyong_edu() -> None:
    text = "2020年11月27日广发银行股份有限公司信用卡中心发放的贷记卡(人民币账户,卡片尾号:1019)。截至2025年12月,信用额 度38,000,已使用额度440。"
    parsed = parse_credit_card_account_block(text)
    assert parsed["credit_limit"] == "38,000"
    assert parsed["used_amount"] == "440"
    assert parsed["account_status"] == "当前有效"


def test_credit_card_parse_used_amount_with_ocr_split_yishiyong_edu() -> None:
    text = "2021年09月10日上海农村商业银行股份有限公司发放的贷记卡(人民币账户)。截至2025年12月,信用额度30,000,已使用额 度0。"
    result = run_personal_credit_report_agent(f"个人征信报告\n信用卡\n{text}")
    card = result["report_json"]["credit_card_accounts"][0]
    assert card["credit_limit"] == "30,000"
    assert card["used_amount"] == "0"
    assert card["account_status"] == "当前有效"
    assert card["card_tail_no"] in {"", "未识别", None}
    assert "卡片尾号：" not in result["report_markdown"]


def test_credit_card_parse_icbc_rmb_credit_limit_with_split_xin_yong() -> None:
    text = "2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡(人民币账户,卡片尾号:8222)。截至2026年01月,信 用额度100,000,已使用额度0。"
    card = run_personal_credit_report_agent(f"个人征信报告\n信用卡\n{text}")["report_json"]["credit_card_accounts"][0]
    assert card["credit_limit"] == "100,000"
    assert card["used_amount"] == "0"
    assert card["currency"] == "人民币"
    assert card["account_status"] == "当前有效"


def test_credit_card_display_filters_foreign_currency() -> None:
    text = """
个人征信报告
信用卡
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（日元账户，卡片尾号：8222）。截至2026年01月，信用额度74,383，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（美元账户，卡片尾号：8222）。截至2026年01月，信用额度102,006，已使用额度0。
2024年03月26日中国工商银行股份有限公司上海市分行发放的贷记卡（人民币账户，卡片尾号：8222）。截至2026年01月，信用额度100,000，已使用额度0。
"""
    result = run_personal_credit_report_agent(text)
    cards = result["report_json"]["credit_card_accounts"]
    assert len(cards) == 1
    assert cards[0]["currency"] == "人民币"
    assert "币种：日元" not in result["report_markdown"]
    assert "币种：美元" not in result["report_markdown"]


def test_credit_card_not_activated_not_displayed() -> None:
    text = """
个人征信报告
信用卡
2025年01月01日某某银行股份有限公司发放的贷记卡（人民币账户）。截至2025年12月，信用额度10,000，已使用额度0，尚未激 活。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    assert report["credit_card_accounts"] == []


def test_credit_card_markdown_only_core_fields() -> None:
    markdown = render_personal_credit_markdown({
        "basic_info": {"source_file": "sample.txt"},
        "credit_summary": {},
        "credit_card_accounts": [{
            "open_date": "2006-10-27",
            "institution": "中国建设银行股份有限公司上海宝钢宝山支行",
            "issuer": "中国建设银行股份有限公司上海宝钢宝山支行",
            "card_type": "贷记卡",
            "currency": "人民币",
            "card_tail_no": "6049",
            "credit_limit": "2,000",
            "used_limit": "0",
            "used_amount": "0",
            "account_status": "当前有效",
            "report_cutoff": "2026-03",
            "current_overdue_amount": "",
            "overdue_amount": "",
            "overdue_months": "",
            "recent_repayment_date": "",
            "recent_repayment_amount": "",
            "latest_repayment_date": "",
            "latest_repayment_amount": "",
            "history_performance": "",
            "report_date": "",
            "information_report_date": "",
        }],
    })
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert "发卡日期：2006-10-27" in card_section
    assert "发卡机构：中国建设银行股份有限公司上海宝钢宝山支行" in card_section
    assert "卡类型：贷记卡" in card_section
    assert "币种：人民币" in card_section
    assert "卡片尾号：6049" in card_section
    assert "信用额度：2,000" in card_section
    assert "已使用额度：0" in card_section
    assert "账户状态：当前有效" in card_section
    assert "截至日期：2026-03" in card_section
    assert "当前逾期金额" not in card_section
    assert "逾期月数" not in card_section
    assert "最近还款日期" not in card_section
    assert "最近还款金额" not in card_section
    assert "历史表现" not in card_section
    assert "信息报告日期" not in card_section


def test_credit_card_markdown_matches_compact_style() -> None:
    markdown = render_personal_credit_markdown({
        "basic_info": {"source_file": "sample.txt"},
        "credit_summary": {},
        "credit_card_accounts": [
            {
                "open_date": "2006-10-27",
                "institution": "中国建设银行股份有限公司上海宝钢宝山支行",
                "card_type": "贷记卡",
                "currency": "人民币",
                "card_tail_no": "6049",
                "credit_limit": "2,000",
                "used_limit": "0",
                "account_status": "当前有效",
                "report_cutoff": "2026-03",
                "overdue_amount": "",
                "history_performance": "",
            },
            {
                "open_date": "2012-10-24",
                "institution": "中国光大银行股份有限公司信用卡中心",
                "card_type": "贷记卡",
                "currency": "人民币",
                "card_tail_no": "8186",
                "credit_limit": "0",
                "used_limit": "0",
                "account_status": "当前有效",
                "report_cutoff": "2026-03",
                "overdue_amount": "",
                "history_performance": "",
            },
        ],
    })
    card_section = markdown.split("## 五、信用卡账户明细", 1)[1].split("## 六、相关还款责任信息", 1)[0]
    assert card_section.count("### 账户 ") == 2
    assert card_section.count("- ") == 18
    assert "当前逾期金额" not in card_section
    assert "逾期月数" not in card_section
    assert "最近还款" not in card_section
    assert "历史表现" not in card_section
    assert "信息报告日期" not in card_section


def test_credit_card_markdown_uses_credit_limit_label() -> None:
    markdown = render_personal_credit_markdown({
        "basic_info": {},
        "credit_summary": {},
        "credit_card_accounts": [{
            "open_date": "2004-11-18",
            "institution": "招商银行股份有限公司信用卡中心",
            "issuer": "招商银行股份有限公司信用卡中心",
            "card_type": "贷记卡",
            "currency": "人民币",
            "card_tail_no": "",
            "credit_limit": "96,000",
            "used_limit": "0",
            "used_amount": "0",
            "account_status": "当前有效",
            "report_cutoff": "2025-12",
        }],
    })
    assert "信用额度：96,000" in markdown
    assert "授信额度：96,000" not in markdown


def test_credit_card_markdown_hides_missing_tail_no() -> None:
    for tail_no in ("", "未识别", "--", "-", "无", None):
        markdown = render_personal_credit_markdown({
            "basic_info": {},
            "credit_summary": {},
            "credit_card_accounts": [{
                "open_date": "2004-11-18",
                "institution": "招商银行股份有限公司信用卡中心",
                "card_type": "贷记卡",
                "currency": "人民币",
                "card_tail_no": tail_no,
                "credit_limit": "96,000",
                "used_limit": "0",
                "account_status": "当前有效",
                "report_cutoff": "2025-12",
            }],
        })
        assert "卡片尾号：未识别" not in markdown
        assert "卡片尾号：" not in markdown


def test_credit_card_markdown_shows_existing_tail_no() -> None:
    markdown = render_personal_credit_markdown({
        "basic_info": {},
        "credit_summary": {},
        "credit_card_accounts": [{
            "open_date": "2020-11-27",
            "institution": "广发银行股份有限公司信用卡中心",
            "issuer": "广发银行股份有限公司信用卡中心",
            "card_type": "贷记卡",
            "currency": "人民币",
            "card_tail_no": "1019",
            "credit_limit": "38,000",
            "used_limit": "440",
            "used_amount": "440",
            "account_status": "当前有效",
            "report_cutoff": "2025-12",
        }],
    })
    assert "卡片尾号：1019" in markdown
    assert "信用额度：38,000" in markdown
    assert "授信额度：38,000" not in markdown


RELATED_REPAYMENT_TEXT = """
个人征信报告
相关还款责任信息
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:D10023010H00012024052800000716)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
"""


def test_extract_related_repayment_responsibilities() -> None:
    sections = segment_report(RELATED_REPAYMENT_TEXT)
    records = extract_related_repayment_responsibilities(sections, RELATED_REPAYMENT_TEXT)
    assert len(records) == 1
    item = records[0]
    assert item["related_party"] == "上海乐芙兰电子商务有限公司"
    assert item["responsibility_type"] == "保证人"
    assert item["institution"] == "南京银行股份有限公司上海虹口支行"
    assert item["responsibility_amount"] == "5,000,000"
    assert item["loan_balance"] == "5,000,000"
    assert item["contract_no"] == "D10023010H00012024052800000716"
    assert item["as_of_date"] in {"2025-02-20", "2025年02月20日"}


def test_extract_multiple_related_repayment_responsibilities() -> None:
    text = """
个人征信报告
相关还款责任信息
2023年11月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在中国建设银行股份有限公司上海浦东分行办理的贷款承担相关还款责任，责任人类型为共同借款人，相关还款责任金额--。截至2025年02月28日，贷款余额5,000,000(人民币元)。
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:D10023010H00012024052800000716)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    records = report["related_repayment_responsibilities"]
    assert len(records) == 2
    assert {item["responsibility_type"] for item in records} == {"共同借款人", "保证人"}


def test_related_repayment_not_in_loan_accounts() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_TEXT)["report_json"]
    assert report["loan_accounts"] == []
    assert len(report["related_repayment_responsibilities"]) == 1


def test_markdown_related_repayment_section() -> None:
    markdown = run_personal_credit_report_agent(RELATED_REPAYMENT_TEXT)["report_markdown"]
    assert "## 六、相关还款责任信息" in markdown
    assert "被担保/相关企业" in markdown
    assert "责任人类型" in markdown
    assert "办理机构" in markdown
    assert "相关还款责任金额" in markdown
    assert "贷款/融资余额" in markdown
    assert "合同编号" in markdown
    assert "截至日期" in markdown


def test_related_repayment_risk_indicator() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_TEXT)["report_json"]
    indicators = report["personal_credit_indicators"]
    assert indicators["has_related_repayment_responsibility"] is True
    assert indicators["related_repayment_responsibility_count"] == 1


def test_related_repayment_clean_wrapped_institution() -> None:
    text = """
个人征信报告
相关还款责任信息
2023年11月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在中国建设银行股份有
限公司上海浦东分行办理的贷款承担相关还款责任，责任人类型为共同借款人，相关还款责任金额--。截至2025年02月28日，贷款余额5,000,000(人民币元)。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    item = report["related_repayment_responsibilities"][0]
    assert item["institution"] == "中国建设银行股份有限公司上海浦东分行"
    assert item["responsibility_type"] == "共同借款人"
    assert item["loan_balance"] == "5,000,000"
    assert item["as_of_date"] == "2025-02-28"


def test_related_repayment_contract_no_next_line() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:
D10023010H00012024032700000243)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    item = report["related_repayment_responsibilities"][0]
    assert item["contract_no"] == "D10023010H00012024032700000243"
    assert item["loan_balance"] == "5,000,000"
    assert item["as_of_date"] == "2025-02-20"


def test_related_repayment_multiple_records_with_page_noise() -> None:
    text = """
个人征信报告
相关还款责任信息
第 1 页,共 6 页
4.
2023年11月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在中国建设银行股份有
限公司上海浦东分行办理的贷款承担相关还款责任，责任人类型为共同借款人，相关还款责任金额--(保证合同编号：
B10811000H00011881567)。截至2025年02月28日，贷款余额5,000,000(人民币元)。
5.
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:
D10023010H00012024032700000243)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
查询记录
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    records = report["related_repayment_responsibilities"]
    assert len(records) == 2
    assert records[0]["institution"] == "中国建设银行股份有限公司上海浦东分行"
    assert records[0]["contract_no"] == "B10811000H00011881567"
    assert records[0]["loan_balance"] == "5,000,000"
    assert records[1]["contract_no"] == "D10023010H00012024032700000243"
    assert records[1]["loan_balance"] == "5,000,000"


def test_related_repayment_markdown_single_line_institution() -> None:
    text = """
个人征信报告
相关还款责任信息
2023年11月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在中国建设银行股份有
限公司上海浦东分行办理的贷款承担相关还款责任，责任人类型为共同借款人，相关还款责任金额--。截至2025年02月28日，贷款余额5,000,000(人民币元)。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "办理机构：中国建设银行股份有限公司上海浦东分行" in markdown
    assert "中国建设银行股份有\n限公司" not in markdown


RELATED_REPAYMENT_9_TEXT = """
个人征信报告
相关还款责任信息
2023年11月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在中国建设银行股份有限公司上海浦东分行办理的贷款承担相关还款责任，责任人类型为共同借款人，相关还款责任金额--(保证合同编号：B10811000H00011881567)。截至2025年02月28日，贷款余额5,000,000(人民币元)。
2024年02月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额2,920,000(保证合同编号：B10711000H000120602403002084)。截至2025年02月21日，贷款余额2,920,000(人民币元)。
2024年04月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:
D10023010H00012024032700000243)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
第 1 页,共 6 页
4.
2024年06月04日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,000,000(保证合同编号:D10023010H00012024052800000716)。截至2025年02月20日，贷款余额5,000,000(人民币元)。
5.
2024年08月22日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,450,000(保证合同编号:D10023010H00012024082200000111)。截至2025年02月20日，贷款余额1,450,000(人民币元)。
6.
2024年08月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,450,000(保证合同编号:D10023010H00012024082300000112)。截至2025年02月20日，贷款余额1,450,000(人民币元)。
7.
2024年09月26日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,450,000(保证合同编号:D10023010H00012024092600000113)。截至2025年02月20日，贷款余额1,450,000(人民币元)。
8.
2024年11月12日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在南京银行股份有限公司上海虹口支行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,450,000(保证合同编号:D10023010H00012024111200000114)。截至2025年02月20日，贷款余额1,450,000(人民币元)。
9.
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
查询记录
"""


def test_related_repayment_start_date_field() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_json"]
    first = report["related_repayment_responsibilities"][0]
    assert first["start_date"] == "2023-11-02"
    assert first["as_of_date"] == "2025-02-28"


def test_related_repayment_extract_9_records() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_json"]
    records = report["related_repayment_responsibilities"]
    assert len(records) == 9
    assert [item["start_date"] for item in records] == [
        "2023-11-02",
        "2024-02-23",
        "2024-04-01",
        "2024-06-04",
        "2024-08-22",
        "2024-08-23",
        "2024-09-26",
        "2024-11-12",
        "2025-02-20",
    ]


def test_related_repayment_keep_similar_contract_numbers() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_json"]
    contract_numbers = {item["contract_no"] for item in report["related_repayment_responsibilities"]}
    assert "B10811000H00011881567" in contract_numbers
    assert "B10811000H0001181567" in contract_numbers
    assert len(report["related_repayment_responsibilities"]) == 9


def test_related_repayment_last_record_huaxia() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_json"]
    last = report["related_repayment_responsibilities"][-1]
    assert last["start_date"] == "2025-02-20"
    assert last["institution"] == "华夏银行股份有限公司上海分行"
    assert last["responsibility_amount"] == "550,000"
    assert last["loan_balance"] == "1,370,000"
    assert last["contract_no"] == "B10811000H0001181567"
    assert last["as_of_date"] == "2025-02-21"


def test_related_repayment_markdown_contains_start_date() -> None:
    markdown = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_markdown"]
    assert "- 起始日期：2025-02-20" in markdown
    assert "被担保/相关企业" in markdown
    assert "责任人类型" in markdown
    assert "办理机构" in markdown
    assert "相关还款责任金额" in markdown
    assert "贷款/融资余额" in markdown
    assert "合同编号" in markdown
    assert "截至日期" in markdown


def test_related_repayment_extract_last_9th_record() -> None:
    text = """
个人征信报告
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    records = report["related_repayment_responsibilities"]
    assert len(records) == 1
    item = records[0]
    assert item["start_date"] == "2025-02-20"
    assert item["related_party"] == "上海乐芙兰电子商务有限公司"
    assert item["institution"] == "华夏银行股份有限公司上海分行"
    assert item["responsibility_type"] == "保证人"
    assert item["responsibility_amount"] == "550,000"
    assert item["contract_no"] == "B10811000H0001181567"
    assert item["as_of_date"] == "2025-02-21"
    assert item["loan_balance"] == "1,370,000"


def test_related_repayment_keep_huaxia_two_similar_records() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年02月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000（保证合同编号：B10811000H00011881567）。截至2025年02月21日，贷款余额2,920,000（人民币元）。
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    report = run_personal_credit_report_agent(text)["report_json"]
    records = report["related_repayment_responsibilities"]
    contract_numbers = {item["contract_no"] for item in records}
    assert len(records) == 2
    assert "B10811000H00011881567" in contract_numbers
    assert "B10811000H0001181567" in contract_numbers


def test_related_repayment_extract_9_records_full_sample() -> None:
    report = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_json"]
    records = report["related_repayment_responsibilities"]
    assert len(records) == 9
    assert records[-1]["start_date"] == "2025-02-20"
    assert records[-1]["loan_balance"] == "1,370,000"


def test_related_repayment_markdown_contains_9th() -> None:
    markdown = run_personal_credit_report_agent(RELATED_REPAYMENT_9_TEXT)["report_markdown"]
    assert "### 相关还款责任 9" in markdown
    assert "起始日期：2025-02-20" in markdown
    assert "办理机构：华夏银行股份有限公司上海分行" in markdown
    assert "相关还款责任金额：550,000" in markdown
    assert "贷款/融资余额：1,370,000" in markdown
    assert "合同编号：B10811000H0001181567" in markdown


def test_related_repayment_parse_2025_02_20_record() -> None:
    text = """
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    records = extract_related_repayment_responsibilities({}, text)
    assert len(records) == 1
    item = records[0]
    assert item["start_date"] == "2025-02-20"
    assert item["related_party"] == "上海乐芙兰电子商务有限公司"
    assert item["institution"] == "华夏银行股份有限公司上海分行"
    assert item["responsibility_type"] == "保证人"
    assert item["responsibility_amount"] == "550,000"
    assert item["contract_no"] == "B10811000H0001181567"
    assert item["as_of_date"] == "2025-02-21"
    assert item["loan_balance"] == "1,370,000"


def test_related_repayment_chinese_punctuation() -> None:
    text = """
2025年02月20日，为上海乐芙兰电子商务有限公司（证件类型：中征码，证件号码：3201050001674346）在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["contract_no"] == "B10811000H0001181567"
    assert item["loan_balance"] == "1,370,000"


def test_related_repayment_no_drop_if_partial_fields_missing() -> None:
    text = """
2025年02月20日，为上海乐芙兰电子商务有限公司在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    records = extract_related_repayment_responsibilities({}, text)
    assert len(records) == 1
    assert records[0]["start_date"] == "2025-02-20"
    assert records[0]["contract_no"] == ""


def test_related_repayment_keep_two_huaxia_records() -> None:
    text = """
2024年02月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000（保证合同编号：B10811000H00011881567）。截至2025年02月21日，贷款余额2,920,000（人民币元）。
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额550,000（保证合同编号：B10811000H0001181567）。截至2025年02月21日，贷款余额1,370,000（人民币元）。
"""
    records = extract_related_repayment_responsibilities({}, text)
    contract_numbers = {item["contract_no"] for item in records}
    assert len(records) == 2
    assert "B10811000H00011881567" in contract_numbers
    assert "B10811000H0001181567" in contract_numbers


def test_related_repayment_9_candidates_9_parsed() -> None:
    records = extract_related_repayment_responsibilities({}, RELATED_REPAYMENT_9_TEXT)
    assert len(records) == 9
    assert any(item["start_date"] == "2025-02-20" for item in records)


def test_parse_2025_02_20_related_repayment() -> None:
    test_related_repayment_parse_2025_02_20_record()


def test_keep_two_huaxia_records_not_deduped() -> None:
    test_related_repayment_keep_two_huaxia_records()


def test_related_repayment_emergency_append_9th() -> None:
    records = extract_related_repayment_responsibilities({}, RELATED_REPAYMENT_9_TEXT)
    assert any(
        item["start_date"] == "2025-02-20"
        and item["contract_no"] == "B10811000H0001181567"
        and item["loan_balance"] == "1,370,000"
        for item in records
    )


def test_related_repayment_candidate_count_equals_parsed_count_when_valid() -> None:
    records = extract_related_repayment_responsibilities({}, RELATED_REPAYMENT_9_TEXT)
    assert len(records) == 9


def test_related_repayment_keep_same_contract_different_start_date_and_balance() -> None:
    text = """
相关还款责任信息
2024年02月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000(保证合同编号:B10811000H00011881567)。截至2025年02月21日，贷款余额2,920,000(人民币元)。
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000(保证合同编号:B10811000H00011881567)。截至2025年02月21日，贷款余额1,370,000(人民币元)。
"""
    records = extract_related_repayment_responsibilities({}, text)
    assert len(records) == 2
    assert any(item["start_date"] == "2024-02-23" and item["loan_balance"] == "2,920,000" for item in records)
    assert any(item["start_date"] == "2025-02-20" and item["loan_balance"] == "1,370,000" for item in records)
    assert any(item.get("_duplicate_contract_no_warning") for item in records)


def test_related_repayment_true_duplicate_removed() -> None:
    record = "2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000(保证合同编号:B10811000H00011881567)。截至2025年02月21日，贷款余额1,370,000(人民币元)。"
    records = extract_related_repayment_responsibilities({}, f"相关还款责任信息\n{record}\n{record}")
    assert len(records) == 1


def test_related_repayment_markdown_contains_9th_even_same_contract() -> None:
    text = """
相关还款责任信息
2024年01月01日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000001)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月02日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000002)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月03日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000003)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月04日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000004)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月05日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000005)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月06日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000006)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年01月07日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在某某银行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额1,000(保证合同编号:X00000000007)。截至2025年02月21日，贷款余额1,000(人民币元)。
2024年02月23日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000(保证合同编号:B10811000H00011881567)。截至2025年02月21日，贷款余额2,920,000(人民币元)。
2025年02月20日，为上海乐芙兰电子商务有限公司(证件类型:中征码,证件号码:3201050001674346)在华夏银行股份有限公司上海分行办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额5,500,000(保证合同编号:B10811000H00011881567)。截至2025年02月21日，贷款余额1,370,000(人民币元)。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "### 相关还款责任 9" in markdown
    assert "起始日期：2025-02-20" in markdown
    assert "贷款/融资余额：1,370,000" in markdown
    assert "核验提示：合同编号与其他记录重复" in markdown


def test_related_repayment_finance_lease_institution_and_balance() -> None:
    text = """
个人征信报告
相关还款责任信息
2025年11月12日，为上海意川建筑科技有限公司（证件类型：中征码，证件号码：310118UE83L3F406）在远东宏信普惠融资租赁（天津）有限公司办理的融资租赁承担相关还款责任，责任人类型为保证人，相关还款责任金额4,000,000（保证合同编号：X1201010000462ydph201107）。截至2026年03月12日，融资租赁余额3,424,532（人民币元）。
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["start_date"] == "2025-11-12"
    assert item["related_party"] == "上海意川建筑科技有限公司"
    assert item["responsibility_type"] == "保证人"
    assert item["institution"] == "远东宏信普惠融资租赁（天津）有限公司"
    assert item["business_type"] == "融资租赁"
    assert item["responsibility_amount"] == "4,000,000"
    assert item["contract_no"] == "X1201010000462ydph201107"
    assert item["as_of_date"] == "2026-03-12"
    assert item["balance_type"] == "融资租赁余额"
    assert item["loan_balance"] == "3,424,532"
    assert item["balance"] == "3,424,532"


def test_related_repayment_normal_loan_still_works() -> None:
    text = """
个人征信报告
相关还款责任信息
2025年11月11日，为上海意川建筑科技有限公司（证件类型：中征码，证件号码：310118UE83L3F406）在温州银行股份有限公司办理的贷款承担相关还款责任，责任人类型为保证人，相关还款责任金额3,000,000（保证合同编号：B12345678901）。截至2026年03月20日，贷款余额3,000,000（人民币元）。
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["institution"] == "温州银行股份有限公司"
    assert item["business_type"] == "贷款"
    assert item["balance_type"] == "贷款余额"
    assert item["loan_balance"] == "3,000,000"


def test_related_repayment_markdown_finance_lease_balance() -> None:
    text = """
个人征信报告
相关还款责任信息
2025年11月12日，为上海意川建筑科技有限公司（证件类型：中征码，证件号码：310118UE83L3F406）在远东宏信普惠融资租赁（天津）有限公司办理的融资租赁承担相关还款责任，责任人类型为保证人，相关还款责任金额4,000,000（保证合同编号：X1201010000462ydph201107）。截至2026年03月12日，融资租赁余额3,424,532（人民币元）。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "办理机构：远东宏信普惠融资租赁（天津）有限公司" in markdown
    assert "余额类型：融资租赁余额" in markdown
    assert "贷款/融资余额：3,424,532" in markdown
    assert "办理机构：未识别" not in markdown
    assert "贷款余额：未识别" not in markdown


def test_related_repayment_related_party_with_chinese_parentheses() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年01月08日,为上海昭晟机电（江苏）有限公司（证件类型:中征码,证件号码:320681UMU76GD178)在远东国际融资租赁有限公司办理的融资租赁承担相关还款责任,责任人类型为保证人,相关还款责任金额30,000,000（保证合同编号: X3101010000173SH23DG1N2MU2-IFELC23DG1N2MU2-U-02).截至2026年01月08日,融资租赁余额0（人民币元）.
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["related_party"] == "上海昭晟机电（江苏）有限公司"
    assert item["institution"] == "远东国际融资租赁有限公司"
    assert item["business_type"] == "融资租赁"
    assert item["balance_type"] == "融资租赁余额"
    assert item["loan_balance"] == "0"
    assert item["contract_no"] == "X3101010000173SH23DG1N2MU2-IFELC23DG1N2MU2-U-02"


def test_related_repayment_related_party_with_half_parentheses() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年01月08日,为上海昭晟机电(江苏)有限公司(证件类型:中征码,证件号码:320681UMU76GD178)在远东国际融资租赁有限公司办理的融资租赁承担相关还款责任,责任人类型为保证人,相关还款责任金额30,000,000（保证合同编号:X3101010000173SH23DG1N2MU2）。截至2026年01月08日,融资租赁余额0（人民币元）.
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["related_party"] == "上海昭晟机电(江苏)有限公司"


def test_related_repayment_related_party_mixed_parentheses() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年01月08日,为上海昭晟机电（江苏）有限公司(证件类型:中征码,证件号码:320681UMU76GD178)在远东国际融资租赁有限公司办理的融资租赁承担相关还款责任,责任人类型为保证人,相关还款责任金额30,000,000（保证合同编号:X3101010000173SH23DG1N2MU2）。截至2026年01月08日,融资租赁余额0（人民币元）.
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["related_party"] == "上海昭晟机电（江苏）有限公司"


def test_related_repayment_related_party_without_certificate_fallback() -> None:
    text = """
个人征信报告
相关还款责任信息
2026年01月09日,为上海昭晟机电设备有限公司在北京银行股份有限公司上海奉贤支行办理的贷款承担相关还款责任,责任人类型为保证人,相关还款责任金额1,000,000（保证合同编号:B12345678901）。截至2026年01月09日,贷款余额1,000,000（人民币元）.
"""
    item = extract_related_repayment_responsibilities({}, text)[0]
    assert item["related_party"] == "上海昭晟机电设备有限公司"
    assert item["institution"] == "北京银行股份有限公司上海奉贤支行"


def test_related_repayment_markdown_no_unknown_related_party() -> None:
    text = """
个人征信报告
相关还款责任信息
2024年01月08日,为上海昭晟机电（江苏）有限公司（证件类型:中征码,证件号码:320681UMU76GD178)在远东国际融资租赁有限公司办理的融资租赁承担相关还款责任,责任人类型为保证人,相关还款责任金额30,000,000（保证合同编号: X3101010000173SH23DG1N2MU2-IFELC23DG1N2MU2-U-02).截至2026年01月08日,融资租赁余额0（人民币元）.
2025年11月12日，为上海意川建筑科技有限公司（证件类型：中征码，证件号码：310118UE83L3F406）在远东宏信普惠融资租赁（天津）有限公司办理的融资租赁承担相关还款责任，责任人类型为保证人，相关还款责任金额4,000,000（保证合同编号：X1201010000462ydph201107）。截至2026年03月12日，融资租赁余额3,424,532（人民币元）。
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    related_section = markdown.split("## 六、相关还款责任信息", 1)[1].split("## 七、担保信息", 1)[0]
    assert "被担保/相关企业：未识别" not in related_section
    assert "被担保/相关企业：上海昭晟机电（江苏）有限公司" in related_section
    assert "被担保/相关企业：上海意川建筑科技有限公司" in related_section


def test_extract_no_non_credit_transactions() -> None:
    text = """
非信贷交易记录
系统中没有您最近5年内的非信贷交易记录。
"""
    records = extract_non_credit_transactions(segment_report(text), text)
    assert len(records) == 1
    assert records[0]["record_type"] == "系统中没有您最近5年内的非信贷交易记录"


def test_extract_non_credit_transactions_inline_ocr() -> None:
    text = "非信贷交易记录系统中没有您最近5年内的非信贷交易记录。\n公共记录系统中没有您最近5年内的公共信息记录。"
    report = run_personal_credit_report_agent(text)["report_json"]
    assert any("系统中没有您最近5年内的非信贷交易记录" in item.get("record_type", "") for item in report["non_credit_transactions"])
    assert any("系统中没有您最近5年内的公共信息记录" in item.get("record_type", "") for item in report["public_records"])
    assert not any("非信贷交易记录" in str(item) for item in report["public_records"])


def test_markdown_non_credit_transaction_section() -> None:
    text = "非信贷交易记录\n系统中没有您最近5年内的非信贷交易记录。"
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "## 八、非信贷交易记录" in markdown
    assert "记录类型：系统中没有您最近5年内的非信贷交易记录" in markdown


def test_markdown_section_order() -> None:
    markdown = run_personal_credit_report_agent("非信贷交易记录\n系统中没有您最近5年内的非信贷交易记录。\n公共记录\n系统中没有您最近5年内的公共信息记录。")["report_markdown"]
    guarantee = markdown.index("## 七、担保信息")
    non_credit = markdown.index("## 八、非信贷交易记录")
    public = markdown.index("## 九、公共记录")
    query = markdown.index("## 十、查询记录")
    assert guarantee < non_credit < public < query


def test_query_statistics_from_report_time() -> None:
    query_records = [
        {"query_date": "2025年02月12日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2025年02月10日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2025年01月07日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2024年11月04日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2024年10月02日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2024年08月12日", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2025年02月15日", "query_reason": "贷后管理", "query_type": "机构查询"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["institution_query"]["last_1_month"] == 1
    assert stats["institution_query"]["last_3_months"] == 3
    assert stats["institution_query"]["last_6_months"] == 5


def test_query_statistics_exclude_post_loan_management() -> None:
    query_records = [
        {"query_date": "2025-03-01", "query_reason": "贷后管理", "query_type": "机构查询"},
        {"query_date": "2025/02/20", "query_reason": "贷后管理", "query_type": "机构查询"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["institution_query"]["last_1_month"] == 0
    assert stats["institution_query"]["last_3_months"] == 0
    assert stats["institution_query"]["last_6_months"] == 0


def test_query_statistics_include_allowed_reasons() -> None:
    query_records = [
        {"query_date": "2025.02.20", "query_reason": "法人代表、负责人、高管等", "query_type": "机构查询"},
        {"query_date": "2025-02-21", "query_reason": "担保资格审查", "query_type": "机构查询"},
        {"query_date": "2025/02/22", "query_reason": "贷款审批", "query_type": "机构查询"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["institution_query"]["last_1_month"] == 3
    assert stats["institution_query"]["last_3_months"] == 3
    assert stats["institution_query"]["last_6_months"] == 3


def test_query_statistics_personal_query() -> None:
    query_records = [
        {"query_date": "2025-02-20", "query_reason": "贷款审批", "query_type": "本人查询"},
        {"query_date": "2025-02-21", "query_reason": "担保资格审查", "query_type": "个人查询"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["personal_query"]["last_1_month"] == 2
    assert stats["institution_query"]["last_1_month"] == 0


def test_query_markdown_summary_only() -> None:
    report = {
        "basic_info": {},
        "credit_summary": {},
        "query_records": [
            {"query_date": "2025-02-20", "query_institution": "某银行", "query_reason": "贷款审批", "query_type": "机构查询"}
        ],
        "query_statistics": {
            "institution_query": {"last_1_month": 1, "last_3_months": 1, "last_6_months": 1},
            "personal_query": {"last_1_month": 0, "last_3_months": 0, "last_6_months": 0},
        },
    }
    markdown = render_personal_credit_markdown(report)
    assert "## 十、查询记录" in markdown
    assert "### 机构查询" in markdown
    assert "近1个月查询次数" in markdown
    assert "近3个月查询次数" in markdown
    assert "近6个月查询次数" in markdown
    assert "### 个人查询" in markdown
    assert "### 记录 1" not in markdown
    assert "查询日期：" not in markdown
    assert "查询机构：" not in markdown
    assert "查询原因：" not in markdown
    assert "查询类型：" not in markdown


def test_query_statistics_month_boundary_inclusive() -> None:
    query_records = [
        {"query_date": "2025年03月09日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审查"},
        {"query_date": "2025年02月25日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审查"},
        {"query_date": "2025年02月19日", "query_type": "机构查询", "query_reason": "担保资格审查"},
        {"query_date": "2025年02月12日", "query_type": "机构查询", "query_reason": "担保资格审查"},
        {"query_date": "2025年02月11日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审查"},
        {"query_date": "2025年02月10日", "query_type": "机构查询", "query_reason": "担保资格审查"},
        {"query_date": "2025年02月09日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审查"},
        {"query_date": "2025年01月21日", "query_type": "机构查询", "query_reason": "担保资格审查"},
        {"query_date": "2025年01月09日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审查"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["institution_query"]["last_1_month"] == 5
    assert stats["institution_query"]["last_3_months"] == 9
    assert stats["institution_query"]["last_6_months"] == 9


def test_query_statistics_exclude_before_boundary() -> None:
    stats = build_query_statistics(
        [{"query_date": "2025年02月10日", "query_type": "机构查询", "query_reason": "担保资格审查"}],
        "2025-03-11 04:01:39",
    )
    assert stats["institution_query"]["last_1_month"] == 0
    assert stats["institution_query"]["last_3_months"] == 1


def test_query_statistics_include_boundary_date() -> None:
    stats = build_query_statistics(
        [{"query_date": "2025年02月11日", "query_type": "机构查询", "query_reason": "担保资格审查"}],
        "2025-03-11 04:01:39",
    )
    assert stats["institution_query"]["last_1_month"] == 1


def test_query_reason_legal_representative_credit_review() -> None:
    assert is_countable_query_reason("法人代表、负责人、高管等资信审查") is True


def test_query_reason_ocr_wrapped_credit_review() -> None:
    query_records = [
        {"query_date": "2025年02月25日", "query_type": "机构查询", "query_reason": "法人代表、负责人、高管等资信审\n查"}
    ]
    assert is_countable_query_reason(query_records[0]["query_reason"]) is True
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["institution_query"]["last_1_month"] == 1


def test_query_markdown_summary_counts() -> None:
    markdown = render_personal_credit_markdown(
        {
            "basic_info": {},
            "credit_summary": {},
            "query_statistics": {
                "institution_query": {"last_1_month": 5, "last_3_months": 9, "last_6_months": 9},
                "personal_query": {"last_1_month": 0, "last_3_months": 0, "last_6_months": 0},
            },
        }
    )
    assert "近1个月查询次数：5" in markdown
    assert "近3个月查询次数：9" in markdown
    assert "近6个月查询次数：9" in markdown


def test_personal_query_statistics_ignore_reason() -> None:
    query_records = [
        {"query_date": "2025年03月01日", "query_type": "个人查询", "query_reason": "本人查询信用报告"},
        {"query_date": "2025年02月20日", "query_type": "本人查询", "query_reason": ""},
        {"query_date": "2025年01月15日", "query_type": "个人查询", "query_reason": "互联网查询"},
        {"query_date": "2024年10月01日", "query_type": "本人查询", "query_reason": "柜台查询"},
        {"query_date": "2024年08月01日", "query_type": "本人查询", "query_reason": "本人查询"},
    ]
    stats = build_query_statistics(query_records, "2025-03-11 04:01:39")
    assert stats["personal_query"]["last_1_month"] == 2
    assert stats["personal_query"]["last_3_months"] == 3
    assert stats["personal_query"]["last_6_months"] == 4
    assert stats["institution_query"]["last_1_month"] == 0


def test_personal_query_not_filtered_by_post_loan_management() -> None:
    stats = build_query_statistics(
        [{"query_date": "2025年03月01日", "query_type": "个人查询", "query_reason": "贷后管理"}],
        "2025-03-11 04:01:39",
    )
    assert stats["personal_query"]["last_1_month"] == 1


def test_institution_query_still_filters_reason() -> None:
    stats = build_query_statistics(
        [{"query_date": "2025年03月01日", "query_type": "机构查询", "query_reason": "贷后管理"}],
        "2025-03-11 04:01:39",
    )
    assert stats["institution_query"]["last_1_month"] == 0


def test_query_markdown_personal_counts() -> None:
    markdown = render_personal_credit_markdown(
        {
            "basic_info": {},
            "credit_summary": {},
            "query_statistics": {
                "institution_query": {"last_1_month": 0, "last_3_months": 0, "last_6_months": 0},
                "personal_query": {"last_1_month": 2, "last_3_months": 3, "last_6_months": 4},
            },
        }
    )
    personal_section = markdown.split("### 个人查询", 1)[1]
    assert "近1个月查询次数：2" in personal_section
    assert "近3个月查询次数：3" in personal_section
    assert "近6个月查询次数：4" in personal_section


def test_credit_summary_matrix_with_housing_and_other_loans() -> None:
    text = """
信息概要
信用卡 贷款 其他业务
购房 其他
账户数 11 3 38 --
未结清/未销户账户数 5 -- 10 --
发生过逾期的账户数 -- -- -- --
发生过90天以上逾期的账户数 -- -- -- --
为个人 为企业
相关还款责任账户数 -- 7
"""
    summary = extract_credit_summary(segment_report(text))
    assert summary["credit_card_account_count"] == "11"
    assert summary["active_credit_card_account_count"] == "5"
    assert summary["housing_loan_account_count"] == "3"
    assert summary["other_loan_account_count"] == "38"
    assert summary["loan_account_count"] == "41"
    assert summary["housing_loan_outstanding_count"] == "0"
    assert summary["other_loan_outstanding_count"] == "10"
    assert summary["outstanding_loan_account_count"] == "10"
    assert summary["credit_card_overdue_account_count"] == "0"
    assert summary["loan_overdue_account_count"] == "0"
    assert summary["credit_card_90d_overdue_account_count"] == "0"
    assert summary["loan_90d_overdue_account_count"] == "0"
    assert summary["enterprise_related_repayment_responsibility_account_count"] == "7"


def test_credit_summary_markdown_shows_housing_loan_count() -> None:
    text = """
信息概要
账户数 11 3 38 --
未结清/未销户账户数 5 -- 10 --
发生过逾期的账户数 -- -- -- --
发生过90天以上逾期的账户数 -- -- -- --
"""
    markdown = run_personal_credit_report_agent(text)["report_markdown"]
    assert "| 购房贷款账户数 | 3 |" in markdown
    assert "| 其他贷款账户数 | 38 |" in markdown
    assert "| 贷款账户数 | 41 |" in markdown
    assert "| 未结清其他贷款账户数 | 10 |" in markdown


def test_credit_summary_old_matrix_still_compatible() -> None:
    text = """
信息概要
账户数 5 -- 3 --
未结清/未销户账户数 -- -- 1 --
发生过逾期的账户数 -- -- -- --
发生过90天以上逾期的账户数 -- -- -- --
"""
    summary = extract_credit_summary(segment_report(text))
    assert summary["housing_loan_account_count"] == "0"
    assert summary["other_loan_account_count"] == "3"
    assert summary["loan_account_count"] == "3"
    assert summary["outstanding_loan_account_count"] == "1"
