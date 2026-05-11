from __future__ import annotations

from pathlib import Path

from backend.extraction_skills.enterprise_credit import final_normalize_credit_result
from backend.services.credit_report_agent.extractors import extract_revolving_overdrafts
from backend.services.credit_report_agent.normalizer import agent_result_to_legacy_extraction
from backend.services.credit_report_agent.orchestrator import extract_enterprise_credit_report_agent
from backend.services.credit_report_agent.segmenter import segment_report


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "credit_report_cases" / "case_company_name_and_revolving_overdraft"


def _fixture_text() -> str:
    return (FIXTURE_DIR / "input_text.txt").read_text(encoding="utf-8")


def test_revolving_section_not_stolen_by_public_records() -> None:
    sections = segment_report(_fixture_text())
    revolving_section = str(sections.get("revolving_overdrafts") or "")

    assert "中国建设银行股份有限公司上海五角场支行" in revolving_section
    assert "抵押 454.68 正常" in revolving_section
    assert "公共记录明细" not in revolving_section


def test_multiline_revolving_record_can_be_assembled() -> None:
    sections = segment_report(_fixture_text())
    records = extract_revolving_overdrafts(sections)

    assert len(records) == 1
    item = records[0]
    assert item.institution_name == "中国建设银行股份有限公司上海五角场支行"
    assert item.business_type == "流动资金贷款"
    assert item.credit_amount == 460
    assert item.balance == 454.68
    assert item.guarantee_type == "抵押"
    assert item.five_category == "正常"
    assert item.overdue_months == 0
    assert item.evidence_text


def test_agent_result_has_revolving_details_and_no_empty_warning() -> None:
    result = extract_enterprise_credit_report_agent(raw_text=_fixture_text(), customer_id="pytest")
    records = result.get("revolving_overdrafts") or []
    warnings = (result.get("validation") or {}).get("warnings") or []
    reconciliation = (result.get("validation") or {}).get("reconciliation") or {}

    assert len(records) == 1
    assert records[0]["institution_name"] == "中国建设银行股份有限公司上海五角场支行"
    assert records[0]["balance"] == 454.68
    assert "revolving_balance_without_details" not in warnings
    assert reconciliation.get("revolving_balance_match") is True


def test_summary_balance_fallback_detail_when_section_has_no_full_row() -> None:
    raw_text = """
信息概要
未结清信贷及授信信息概要
循环透支 1 454.68
信贷记录明细
未结清信贷
循环透支 共 1 笔
循环透支余额：454.68
授信信息 共 0 笔
"""

    result = extract_enterprise_credit_report_agent(raw_text=raw_text, customer_id="pytest-low-confidence")
    records = result.get("revolving_overdrafts") or []
    warnings = (result.get("validation") or {}).get("warnings") or []

    assert records
    assert records[0]["balance"] == 454.68
    assert records[0]["warning"] == "summary_balance_fallback_detail"
    assert "revolving_balance_without_details" not in warnings
    assert "revolving_detail_low_confidence" in warnings


def test_frontend_legacy_field_path_contains_revolving_details() -> None:
    result = extract_enterprise_credit_report_agent(raw_text=_fixture_text(), customer_id="pytest-legacy")
    extracted_json, markdown = agent_result_to_legacy_extraction(result)

    assert extracted_json["revolving_overdrafts"]
    assert extracted_json["revolving_loans"]
    assert "中国建设银行股份有限公司上海五角场支行" in markdown


def test_final_normalize_exposes_revolving_overdrafts_path() -> None:
    raw_text = _fixture_text()
    normalized = final_normalize_credit_result(
        {
            "credit_summary": {
                "revolving_overdraft_balance": "454.68",
                "revolving_overdraft_count": 1,
            },
            "revolving_overdrafts": [],
            "validation": {"warnings": ["revolving_balance_without_details"], "errors": [], "reconciliation": {}},
        },
        raw_text=raw_text,
        parser_path="pytest_final_api",
    )

    records = normalized.get("revolving_overdrafts") or []
    warnings = (normalized.get("validation") or {}).get("warnings") or []

    assert records
    assert records[0]["institution_name"] == "中国建设银行股份有限公司上海五角场支行"
    assert records[0]["guarantee_type"] == "抵押"
    assert "revolving_balance_without_details" not in warnings
    assert normalized["credit_debug"]["api_return_revolving_count"] == 1
