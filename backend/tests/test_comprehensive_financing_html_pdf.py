"""Frozen comprehensive report export, isolation, and offline HTML tests."""

from __future__ import annotations

import asyncio
import copy
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.comprehensive_financing_analysis_service import ComprehensiveFinancingAnalysisResult
from backend.services.comprehensive_financing_report_html_renderer import render_comprehensive_financing_report_html
from backend.services.comprehensive_financing_report_export_service import (
    freeze_comprehensive_financing_report, pdf_filename, snapshot_html,
)
from backend.tests.test_comprehensive_financing_report_markdown_renderer import analysis_payload, analysis_result, report_model


class SnapshotStorage:
    def __init__(self):
        self.rows = {}

    async def create_financing_diagnostic_report_snapshot(self, payload):
        frozen = copy.deepcopy(payload)
        self.rows[(payload["customer_id"], payload["report_id"])] = frozen
        return frozen

    async def get_comprehensive_financing_report_snapshot(self, customer_id, report_id):
        row = self.rows.get((customer_id, report_id))
        return copy.deepcopy(row) if row and row["report_version"] == "comprehensive_financing_v1" else None

    async def get_customer(self, customer_id):
        return {"customer_id": customer_id, "name": "测试企业"}


@pytest.fixture
def html(report_model, analysis_result):
    return render_comprehensive_financing_report_html(report_model, analysis_result, "2026-09-20 10:00:00")


@pytest.mark.parametrize("phrase", [
    "客户综合融资分析报告", "数据范围", "核心融资指标", "企业流水", "财务分析", "征信与负债分析",
    "融资优势", "融资障碍", "当前核心问题", "融资路径方向", "行动计划", "资料缺口与分析限制", "综合结论",
])
def test_comprehensive_html_contains_required_sections(html, phrase):
    assert phrase in html


def test_comprehensive_html_contains_frozen_facts(html):
    for value in ("42,499,565.67元", "19,493,700.00元", "7,351,500.00元", "1,624,475.00元",
                  "99.53%", "252,084.70元", "1,856.5万元"):
        assert value in html
    assert "企业贷款记录数" in html
    assert "到期/待核验记录数" not in html
    assert "path-amber" in html and "path-gray" in html


def test_comprehensive_html_has_offline_a4_page_css(html):
    assert "@page { size: A4; margin: 12mm;" in html
    assert "display:table-header-group" in html
    assert "break-inside:avoid" in html
    assert "http://" not in html and "https://" not in html
    assert "<script" not in html


def test_pdf_has_no_nearly_empty_asset_page(html):
    assert ".cover-page { break-after: page;" in html
    assert ".report-page { break-before: auto;" in html
    assert html.index("五、资产与增信条件") < html.index("融资优势、障碍与核心问题")
    assert "break-before: page;" not in html


def test_core_issue_section_can_span_pages(html):
    assert ".item-card {" in html and "break-inside:avoid;" in html
    assert ".report-page { break-before: auto;" in html
    assert "<h3>七、当前核心问题</h3>" in html


def test_core_issue_card_does_not_split(html):
    assert "<article class='item-card issue'>" in html
    assert re.search(r"\.item-card\s*\{[^}]*break-inside:avoid", html)
    assert not re.search(r"\.report-page\s*\{[^}]*break-inside:avoid", html)


def test_financing_paths_render_as_cards(html):
    assert "<div class='path-grid'>" in html
    assert html.count("<article class='path-card'>") == 2
    assert "<th>融资路径</th>" not in html
    assert "grid-template-columns:repeat(2,minmax(0,1fr))" in html


def test_person_credit_summary_not_rendered_as_overwide_table(html, report_model, analysis_result):
    assert "<article class='person-credit'>" in html
    assert "个人贷款余额" in html and "相关还款责任" in html
    assert "<th scope='col'>姓名</th>" not in html
    report_model.personal_credit["people"][0]["query_summary"]["windows"] = [
        {"window": "近1月", "loan_approval": 0, "credit_card_approval": 0,
         "guarantee_review": 0, "legal_person_review": 0}
    ]
    with_queries = render_comprehensive_financing_report_html(report_model, analysis_result, "2026-09-21")
    assert "<th scope='col'>时间窗口</th>" in with_queries
    assert "<th scope='col'>窗口</th>" not in with_queries


def test_source_notes_are_visually_secondary(html):
    assert "class='source-note'" in html
    assert re.search(r"\.source-note\s*\{[^}]*color:#8a949e;[^}]*font-size:9px", html)
    assert "数据来源：财务报表、资料时点" in html


def test_business_ready_status_label_is_updated(report_model, analysis_payload):
    payload = copy.deepcopy(analysis_payload)
    payload["executive_summary"]["current_financing_readiness"] = "needs_issue_resolution"
    rendered = render_comprehensive_financing_report_html(
        report_model, ComprehensiveFinancingAnalysisResult.model_validate(payload), "2026-09-21"
    )
    assert "当前状态：</b>具备进一步评估基础，但存在前置条件" in rendered
    assert "当前融资准备状态" not in rendered
    assert "需先解决关键问题" not in rendered


def test_pdf_contains_no_orphan_section_heading(html):
    for tag in ("h2", "h3", "h4"):
        assert re.search(rf"{tag}\s*\{{[^}}]*break-after:\s*avoid", html)
    assert "第 \" counter(page) \" 页" in html


def test_pdf_does_not_change_report_facts(html, report_model, analysis_result):
    from backend.services.comprehensive_financing_report_markdown_renderer import render_comprehensive_financing_report
    markdown = render_comprehensive_financing_report(report_model, analysis_result, "2026-09-21")
    for value in ("42,499,565.67元", "19,493,700.00元", "7,351,500.00元", "1,624,475.00元",
                  "99.53%", "252,084.70元", "1,856.5万元"):
        assert value in html and value in markdown
    assert analysis_result.conclusion.one_sentence in html and analysis_result.conclusion.one_sentence in markdown


def test_comprehensive_html_escapes_user_text(report_model, analysis_result):
    report_model.subject_profile["enterprise_name"] = '<img src=x onerror="alert(1)">'
    analysis_result.conclusion.one_sentence = "<script>unsafe()</script>"
    html = render_comprehensive_financing_report_html(report_model, analysis_result, "2026-09-20")
    assert "<img" not in html and "<script" not in html
    assert "&lt;img" in html and "&lt;script&gt;unsafe()&lt;/script&gt;" in html


def test_comprehensive_html_excludes_internal_data(report_model, analysis_result):
    analysis_result.data_limitations.append(type(analysis_result.data_limitations[0])(
        material_type="risk_assessment", limitation="风险评估资料不足", impact="", required_data="",
    ))
    html = render_comprehensive_financing_report_html(report_model, analysis_result, "2026-09-20")
    for forbidden in ("raw_ocr", "evidence", "document_id", "extraction_id", "internal-135", "risk_assessment",
                      "financing_plan", "insufficient_data", "fallback", "schema", "风险评估资料不足"):
        assert forbidden not in html


def test_comprehensive_snapshot_is_frozen(report_model, analysis_result):
    storage = SnapshotStorage()
    links = asyncio.run(freeze_comprehensive_financing_report(storage, "c1", report_model, analysis_result, "2026-09-20", "# report"))
    frozen = asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", links["reportId"]))
    report_model.enterprise_cashflow["total_inflow"] = 1
    analysis_result.conclusion.one_sentence = "改动后"
    assert frozen["report_json"]["report_model"]["enterprise_cashflow"]["total_inflow"] == 42499565.67
    assert "改动后" not in snapshot_html(frozen)
    assert frozen["report_json"]["markdown"] == "# report"


def test_comprehensive_snapshot_report_type_is_isolated(report_model, analysis_result):
    from backend.services.comprehensive_financing_report_export_service import STORAGE_REPORT_VERSION
    assert len(STORAGE_REPORT_VERSION) <= 32  # shared production VARCHAR(32)
    storage = SnapshotStorage()
    links = asyncio.run(freeze_comprehensive_financing_report(storage, "c1", report_model, analysis_result, "2026-09-20", "md"))
    snapshot = asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", links["reportId"]))
    assert snapshot["report_version"] == "comprehensive_financing_v1"
    assert snapshot["report_json"]["report_type"] == "comprehensive_financing_analysis_report"
    assert snapshot["report_json"]["template_version"] == "comprehensive_financing_analysis_report_v1"
    assert "/comprehensive-financing-report/" in links["previewUrl"]
    assert "/credit-report/" not in links["pdfUrl"]
    assert asyncio.run(storage.get_comprehensive_financing_report_snapshot("c2", links["reportId"])) is None
    invalid = copy.deepcopy(snapshot)
    invalid["report_version"] = "credit_one_page_report_v1"
    with pytest.raises(ValueError):
        snapshot_html(invalid)


def test_financing_diagnostic_history_excludes_comprehensive_report():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from backend.db_models import Base, CustomerFinancingDiagnosticReportSnapshot
    from backend.services.sqlalchemy_storage_service import SQLAlchemyStorageService
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[CustomerFinancingDiagnosticReportSnapshot.__table__])
    storage = SQLAlchemyStorageService.__new__(SQLAlchemyStorageService)
    storage._session_factory = sessionmaker(bind=engine)
    versions = {"diagnostic": "v1", "credit": "credit_one_page_report_v1",
                "comprehensive": "comprehensive_financing_v1"}
    for report_id, version in versions.items():
        asyncio.run(storage.create_financing_diagnostic_report_snapshot({
            "report_id": report_id, "customer_id": "c1", "report_version": version,
            "report_status": "completed", "report_json": {"report_type": version},
            "generated_at": "2026-09-20",
        }))
    rows = asyncio.run(storage.list_financing_diagnostic_report_snapshots("c1"))
    assert [row["report_id"] for row in rows] == ["diagnostic"]
    assert asyncio.run(storage.get_financing_diagnostic_report_snapshot("c1", "comprehensive")) is None
    assert asyncio.run(storage.get_credit_report_snapshot("c1", "comprehensive")) is None
    assert asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", "comprehensive"))["report_id"] == "comprehensive"
    assert asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", "credit")) is None


def test_comprehensive_pdf_filename_is_safe(report_model, analysis_result):
    report_model.subject_profile["enterprise_name"] = '上海/测试:企业*?"<>|'
    storage = SnapshotStorage()
    links = asyncio.run(freeze_comprehensive_financing_report(storage, "c1", report_model, analysis_result, "2026-09-20", "md"))
    snapshot = asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", links["reportId"]))
    filename = pdf_filename(snapshot)
    assert filename.startswith("客户综合融资分析报告_上海_") and filename.endswith("_20260920.pdf")
    assert not re.search(r'[\\/:*?"<>|]', filename)


def test_comprehensive_preview_pdf_routes_use_frozen_snapshot(report_model, analysis_result, monkeypatch):
    import backend.services as services
    import backend.services.sqlalchemy_storage_service as sql_storage
    storage = SnapshotStorage()
    monkeypatch.setattr(services, "get_storage_service", lambda: storage)
    monkeypatch.setattr(sql_storage, "SQLAlchemyStorageService", lambda: storage)
    from backend.routers import customer as routes
    from backend.middleware.auth import get_current_user
    import backend.services.comprehensive_financing_report_export_service as exports
    monkeypatch.setattr(routes, "storage_service", storage)
    async def allow(*_args):
        return None
    monkeypatch.setattr(routes, "_ensure_local_customer_access", allow)
    pdf_calls = []
    async def fake_pdf(html):
        pdf_calls.append(html)
        return b"%PDF-1.7\ncomprehensive report"
    monkeypatch.setattr(exports, "render_html_pdf", fake_pdf)
    links = asyncio.run(freeze_comprehensive_financing_report(storage, "c1", report_model, analysis_result, "2026-09-20", "md"))
    app = FastAPI()
    app.include_router(routes.router, prefix="/api")
    app.dependency_overrides[get_current_user] = lambda: {"username": "tester", "role": "viewer"}
    with TestClient(app) as client:
        preview = client.get(links["previewUrl"])
        pdf = client.get(links["pdfUrl"])
        assert preview.status_code == 200 and preview.headers["content-type"].startswith("text/html")
        assert pdf.status_code == 200 and pdf.content.startswith(b"%PDF-")
        assert pdf.headers["content-type"] == "application/pdf"
        assert "filename*=UTF-8" in pdf.headers["content-disposition"]
        assert len(pdf_calls) == 1 and "客户综合融资分析报告" in pdf_calls[0]
        assert client.get(links["pdfUrl"].replace("/c1/", "/c2/")).status_code == 404


def test_comprehensive_pdf_does_not_call_llm(report_model, analysis_result, monkeypatch):
    from services.ai_service import AIService
    monkeypatch.setattr(AIService, "extract", lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError("LLM called")))
    storage = SnapshotStorage()
    links = asyncio.run(freeze_comprehensive_financing_report(storage, "c1", report_model, analysis_result, "2026-09-20", "md"))
    snapshot = asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", links["reportId"]))
    assert snapshot_html(snapshot).startswith("<!doctype html>")


def test_comprehensive_chat_response_has_frozen_report_data(report_model, analysis_result, monkeypatch):
    from backend.services import assistant_comprehensive_financing_analysis_service as assistant
    storage = SnapshotStorage()
    async def context(_storage, _customer_id):
        return report_model
    async def analyze(_model, llm=None):
        return analysis_result
    monkeypatch.setattr(assistant, "build_comprehensive_financing_report_context", context)
    monkeypatch.setattr(assistant, "analyze_comprehensive_financing_report", analyze)
    result = asyncio.run(assistant.generate_comprehensive_financing_analysis(
        storage, "生成客户综合融资分析报告", selected_customer_id="c1",
    ))
    data = result["data"]
    assert result["message"].startswith("# 客户综合融资分析报告")
    assert data["reportStatus"] == "completed"
    assert data["reportType"] == "comprehensive_financing_analysis_report"
    assert data["templateVersion"] == "comprehensive_financing_analysis_report_v1"
    assert data["reportId"] and data["previewUrl"].endswith("/preview") and data["pdfUrl"].endswith("/export/pdf")
    frozen = asyncio.run(storage.get_comprehensive_financing_report_snapshot("c1", data["reportId"]))
    assert frozen["report_json"]["markdown"] == result["message"]
