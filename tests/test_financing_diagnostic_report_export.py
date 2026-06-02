from __future__ import annotations

import asyncio
from io import BytesIO

from docx import Document

from backend.services.financing_diagnostic_report_export_service import (
    PDF_UNAVAILABLE_MESSAGE,
    FinancingDiagnosticReportExportService,
    PdfExportUnavailableError,
    SnapshotNotFoundError,
    can_export_financing_diagnostic_report_snapshot,
)


def _snapshot(customer_id: str = "customer-1", report_id: str = "report-1") -> dict:
    return {
        "id": report_id,
        "report_id": report_id,
        "customer_id": customer_id,
        "report_version": "v3",
        "report_status": "draft",
        "report_json": {
            "customer_summary": {"customer_name": "示例客户"},
            "comprehensive_financing_advice": {"summary": "可推进"},
        },
        "report_markdown": "# 客户融资诊断报告\n\n## 七、综合融资建议\n- 综合状态：可推进\n",
        "source_summary": {"overall_status": "recommendable"},
        "generated_by": "viewer01",
        "generated_at": "2026-06-02T00:00:00+00:00",
    }


class FakeExportStorage:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or [_snapshot()]
        self.realtime_called = False

    async def get_financing_diagnostic_report_snapshot(self, customer_id: str, report_id: str) -> dict | None:
        for row in self.rows:
            if row["customer_id"] == customer_id and row["report_id"] == report_id:
                return row
        return None


def _doc_text(content: bytes) -> str:
    document = Document(BytesIO(content))
    return "\n".join(paragraph.text for paragraph in document.paragraphs)


def test_can_export_word_from_saved_snapshot():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    result = asyncio.run(service.export_docx("customer-1", "report-1"))

    assert result["filename"].endswith(".docx")
    assert result["content"]


def test_word_export_does_not_regenerate_realtime_report():
    storage = FakeExportStorage()
    service = FinancingDiagnosticReportExportService(storage)

    asyncio.run(service.export_docx("customer-1", "report-1"))

    assert storage.realtime_called is False


def test_cross_customer_export_is_not_allowed_by_snapshot_lookup():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    try:
        asyncio.run(service.export_docx("customer-2", "report-1"))
    except SnapshotNotFoundError:
        assert True
    else:
        raise AssertionError("cross customer export should fail")


def test_missing_report_id_returns_not_found_error():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    try:
        asyncio.run(service.export_docx("customer-1", "missing"))
    except SnapshotNotFoundError as exc:
        assert "未找到" in str(exc)
    else:
        raise AssertionError("missing report should fail")


def test_viewer_can_export():
    assert can_export_financing_diagnostic_report_snapshot("viewer") is True


def test_export_filename_contains_customer_name_and_version():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    result = asyncio.run(service.export_docx("customer-1", "report-1"))

    assert "示例客户" in result["filename"]
    assert "v3" in result["filename"]


def test_pdf_dependency_unavailable_returns_clear_error_without_affecting_word():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    word = asyncio.run(service.export_docx("customer-1", "report-1"))
    try:
        asyncio.run(service.export_pdf("customer-1", "report-1"))
    except PdfExportUnavailableError as exc:
        assert PDF_UNAVAILABLE_MESSAGE in str(exc)
    else:
        raise AssertionError("pdf export should report dependency unavailable")
    assert word["content"]


def test_exported_word_content_contains_customer_financing_report_title():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    result = asyncio.run(service.export_docx("customer-1", "report-1"))

    assert "客户融资诊断报告" in _doc_text(result["content"])


def test_exported_word_content_contains_report_version():
    service = FinancingDiagnosticReportExportService(FakeExportStorage())

    result = asyncio.run(service.export_docx("customer-1", "report-1"))

    assert "报告版本：v3" in _doc_text(result["content"])
