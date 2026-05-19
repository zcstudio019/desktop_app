from __future__ import annotations

from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_agents.registry import (
    DOCUMENT_AGENT_REGISTRY,
    EnterpriseCreditReportAgentAdapter,
    PersonalCreditReportAgentAdapter,
    get_document_agent,
)


PERSONAL_CREDIT_TEXT = """
涓汉淇＄敤鎶ュ憡
涓浗浜烘皯閾惰寰佷俊涓績

鎶ュ憡鍩虹淇℃伅
鎶ュ憡缂栧彿锛歅202605120001
鎶ュ憡鏃堕棿锛?026-05-12 10:20:30
涓汉鍩烘湰淇℃伅
濮撳悕锛氬紶涓?璇佷欢绫诲瀷锛氳韩浠借瘉 璇佷欢鍙风爜锛?10101199001011234

淇¤捶璁板綍姒傝
淇＄敤鍗¤处鎴锋暟 1
褰撳墠鏈夋晥淇＄敤鍗¤处鎴锋暟 1
璐锋璐︽埛鏁?1
鏈粨娓呰捶娆捐处鎴锋暟 1

鏌ヨ璁板綍
2026-04-01 鎷涘晢閾惰 淇＄敤鍗″鎵?鏈烘瀯鏌ヨ
"""


def test_registry_has_enterprise_and_personal_credit_agents() -> None:
    assert "enterprise_credit_report" in DOCUMENT_AGENT_REGISTRY
    assert "personal_credit_report" in DOCUMENT_AGENT_REGISTRY


def test_get_document_agent_aliases() -> None:
    assert isinstance(get_document_agent("personal_credit"), PersonalCreditReportAgentAdapter)
    assert isinstance(get_document_agent("personal_credit_report"), PersonalCreditReportAgentAdapter)
    assert isinstance(get_document_agent("enterprise_credit"), EnterpriseCreditReportAgentAdapter)
    assert isinstance(get_document_agent("enterprise_credit_report"), EnterpriseCreditReportAgentAdapter)


def test_run_document_extraction_agent_personal_credit() -> None:
    result = run_document_extraction_agent(
        document_type="personal_credit_report",
        raw_text=PERSONAL_CREDIT_TEXT,
        filename="personal-credit.txt",
    )

    assert result.document_type == "personal_credit_report"
    assert "personal_credit_report" in result.agent_name
    assert isinstance(result.extracted_json, dict)
    assert isinstance(result.markdown_summary, str)


def test_document_agent_personal_credit_uses_skill() -> None:
    result = run_document_extraction_agent(
        document_type="personal_credit_report",
        raw_text=PERSONAL_CREDIT_TEXT,
        filename="personal-credit.txt",
    )

    assert result.debug["selected_agent"] == "personal_credit_report_agent"
    assert result.debug["skill_name"] == "personal_credit_report"
    assert result.raw_agent_result["skill_name"] == "personal_credit_report"
    assert result.schema_version == "personal_credit_report.agent.v1"


def test_personal_credit_adapter_no_direct_agent_call_if_possible(monkeypatch) -> None:
    from backend.services.document_agents import registry

    calls: list[dict] = []

    def fake_build_personal_credit_report_content(**kwargs):
        calls.append(kwargs)
        return {
            "type": "personal_credit_report",
            "title": "个人征信报告",
            "skill_name": "personal_credit_report",
            "schema_version": "personal_credit_report.agent.v1",
            "confidence": 0.75,
            "warnings": [],
            "markdown_summary": "# 个人征信报告",
            "extracted_json": {"schema_version": "personal_credit_report.agent.v1"},
            "evidence": {},
            "debug": {},
        }

    def fail_direct_agent_call(*args, **kwargs):
        raise AssertionError("adapter should call the skill wrapper, not run_personal_credit_report_agent directly")

    monkeypatch.setattr(registry, "build_personal_credit_report_content", fake_build_personal_credit_report_content)
    monkeypatch.setattr(
        "backend.services.personal_credit_report_agent.orchestrator.run_personal_credit_report_agent",
        fail_direct_agent_call,
    )

    result = run_document_extraction_agent(
        document_type="personal_credit_report",
        raw_text=PERSONAL_CREDIT_TEXT,
        filename="personal-credit.txt",
    )

    assert calls
    assert result.debug["skill_name"] == "personal_credit_report"
    assert result.schema_version == "personal_credit_report.agent.v1"


def test_run_document_extraction_agent_unknown_type_fallback() -> None:
    result = run_document_extraction_agent(
        document_type="unknown_document_type",
        raw_text="plain text",
        filename="unknown.txt",
    )

    assert result.document_type == "unknown_document_type"
    assert result.agent_name == "document_agent_fallback"
    assert isinstance(result.extracted_json, dict)
