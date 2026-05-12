from __future__ import annotations

from pathlib import Path

from backend.services.personal_credit_report_agent.extract_basic_info import extract_basic_info
from backend.services.personal_credit_report_agent.extract_credit_summary import extract_credit_summary
from backend.services.personal_credit_report_agent.extract_query_records import extract_query_records
from backend.services.personal_credit_report_agent.markdown_renderer import render_personal_credit_markdown
from backend.services.personal_credit_report_agent.orchestrator import run_personal_credit_report_agent
from backend.services.personal_credit_report_agent.risk_analyzer import analyze_personal_credit_risk
from backend.services.personal_credit_report_agent.segmenter import segment_report
from backend.services.markdown_profile_service import normalize_personal_credit_result, render_personal_credit_markdown as render_profile_personal_credit_markdown


SAMPLE_TEXT = """
个人信用报告
中国人民银行征信中心

报告基础信息
报告编号: P202605120001
报告时间: 2026年05月12日

个人基本信息
姓名: 张三
证件类型: 身份证
证件号码: 110101199001011234
婚姻状况: 已婚

信贷记录概要
贷记卡账户 2 个，未销户 1 个，发生过逾期 1 个，发生过90天以上逾期 0 个。
购房贷款账户 1 个，未结清 1 个，发生过逾期 0 个。
其他贷款账户 2 个，未结清 1 个，发生过逾期 1 个。
其他业务账户 0 个。担保 1 笔。

信贷交易信息明细
贷款账户明细
1. 中国建设银行北京分行 购房贷款 发放金额: 1000000元 余额: 800000元 账户状态: 正常 五级分类: 正常 最近还款: 2026年04月 信息报告日期: 2026年05月
2. 某消费金融公司 消费贷款 发放金额: 50000元 余额: 10000元 账户状态: 逾期 五级分类: 关注 逾期信息: 逾期1个月

贷记卡账户明细
1. 招商银行信用卡 贷记卡 授信额度: 50000元 已用额度: 12000元 账户状态: 正常 最近还款: 2026年04月 信息报告日期: 2026年05月

担保信息
1. 为李四担保 担保金额: 200000元 担保余额: 100000元 状态: 正常

公共信息
无公共记录

查询记录
机构查询记录明细
2026年04月01日 招商银行 信用卡审批 机构查询
2026年03月01日 本人查询 本人查询
"""

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "personal_credit_report"


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def test_extract_basic_info() -> None:
    sections = segment_report(SAMPLE_TEXT)
    basic = extract_basic_info(sections, source_file="sample.txt")
    assert basic["report_number"] == "P202605120001"
    assert basic["name"] == "张三"
    assert basic["id_number"] == "110101199001011234"
    assert basic["source_file"] == "sample.txt"


def test_extract_credit_summary() -> None:
    summary = extract_credit_summary(segment_report(SAMPLE_TEXT))
    assert summary["credit_card_account_count"] == 2
    assert summary["housing_loan_account_count"] == 1
    assert summary["other_loan_account_count"] == 2
    assert summary["guarantee_count"] == 1


def test_extract_query_records() -> None:
    records = extract_query_records(segment_report(SAMPLE_TEXT))
    assert len(records) == 2
    assert records[0]["query_date"] == "2026年04月01日"
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


def test_fixture_basic_personal_credit_report() -> None:
    result = run_personal_credit_report_agent(_fixture("sample_001_basic.txt"), source_file="sample_001_basic.txt")
    report = result["report_json"]
    assert result["report_type"] == "personal_credit_report"
    assert report["basic_info"]["name"] == "王小明"
    assert isinstance(report["loan_accounts"], list)
    assert isinstance(report["credit_card_accounts"], list)
    assert isinstance(report["query_records"], list)
    assert "personal_credit_indicators" in report


def test_fixture_credit_card_and_loan_details() -> None:
    result = run_personal_credit_report_agent(_fixture("sample_002_with_credit_card_and_loan.txt"))
    report = result["report_json"]
    assert report["basic_info"]["name"] == "李小红"
    assert len(report["loan_accounts"]) >= 1
    assert len(report["credit_card_accounts"]) >= 1
    assert report["loan_accounts"][0]["account_no"]
    assert report["credit_card_accounts"][0]["credit_limit"]
    assert report["personal_credit_indicators"]["total_loan_balance"]


def test_fixture_overdue_and_query_risk_flags() -> None:
    result = run_personal_credit_report_agent(_fixture("sample_003_with_overdue_and_queries.txt"))
    report = result["report_json"]
    indicators = report["personal_credit_indicators"]
    assert report["basic_info"]["name"] == "赵逾期"
    assert indicators["risk_level"] == "high"
    assert indicators["has_current_overdue"] is True
    assert indicators["has_90d_overdue"] is True
    assert indicators["high_frequency_query_flag"] is True


def test_personal_credit_risk_analyzer() -> None:
    report = run_personal_credit_report_agent(_fixture("sample_003_with_overdue_and_queries.txt"))["report_json"]
    indicators = analyze_personal_credit_risk(report)
    assert indicators["loan_approval_queries_3m"] >= 4
    assert indicators["loan_approval_queries_6m"] >= 6
    assert indicators["credit_card_approval_queries_3m"] == 1
    assert indicators["risk_level"] == "high"


def test_profile_markdown_personal_credit_filters_metadata_and_enterprise_fields() -> None:
    raw = {
        "type": "personal_credit_report",
        "title": "个人征信报告",
        "confidence": 0.75,
        "markdown": "姓名：沃志方 证件类型：身份证 证件号码：310110198211172732 未婚\n报告编号：2025031104013907986945 报告时间：2025-03-11 04:01:39",
        "raw_text": "证件类型：中征码，证件号码：320105123456789 开户行：中国银行\n姓名：沃志方 证件类型：身份证 证件号码：310110198211172732 未婚",
        "extracted_json": {
            "basic_info": {
                "name": "沃志方 证件类型：身份证 证件号码：310110198211172732 未婚",
                "id_type": "中征码",
                "id_number": "322105****** 开户行 中国银行",
            },
            "credit_summary": {
                "credit_card_account_count": 1,
            },
        },
    }
    normalized = normalize_personal_credit_result(raw)
    assert normalized["basic_info"]["name"] == "沃志方"
    assert normalized["basic_info"]["id_type"] == "身份证"
    assert normalized["basic_info"]["id_number"] == "310110198211172732"
    assert normalized["basic_info"]["marriage_status"] == "未婚"

    markdown = render_profile_personal_credit_markdown(raw, "personal_credit.pdf", "可查看")
    forbidden = ("type:", "confidence", "markdown:", "title:", "中征码", "开户行", "统一社会信用代码", "企业名称")
    for item in forbidden:
        assert item not in markdown
    assert "姓名：沃志方" in markdown
    assert "证件类型：身份证" in markdown
    assert "证件号码：310110198211172732" in markdown
    assert "婚姻状况：未婚" in markdown


def test_markdown_no_duplicate_header() -> None:
    result = run_personal_credit_report_agent(SAMPLE_TEXT, source_file="sample.txt")
    markdown = result["report_markdown"]
    assert markdown.count("资料信息") == 1
    assert "## 个人征信" not in markdown
    for item in ("type", "title", "confidence", "markdown"):
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
