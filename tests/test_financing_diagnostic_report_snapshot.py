from __future__ import annotations

import asyncio

import backend.services.financing_diagnostic_report_snapshot_service as snapshot_module
from backend.services.financing_diagnostic_report_snapshot_service import (
    FinancingDiagnosticReportSnapshotService,
    can_save_financing_diagnostic_report_snapshot,
)


def _report(status: str = "cautious", score: int = 68) -> dict:
    return {
        "customer_id": "customer-1",
        "report_type": "customer_financing_diagnostic",
        "report_status": "draft",
        "kyc_diagnostic": {"readiness_level": "basic_ready"},
        "enterprise_credit_diagnostic": {"has_enterprise_credit_report": True},
        "personal_credit_diagnostic": {"has_personal_credit_report": True},
        "enterprise_bank_flow_diagnostic": {"has_enterprise_bank_flow": True},
        "financial_statement_diagnostic": {"has_financial_statement": False},
        "comprehensive_financing_advice": {
            "overall_status": status,
            "financing_readiness_score": score,
            "summary": "谨慎推进：当前客户可谨慎推进。",
        },
        "financing_readiness": {"summary": "资料基本完整"},
        "report_markdown": "# 客户融资诊断报告\n\n## 七、综合融资建议\n",
    }


class FakeSnapshotStorage:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def create_financing_diagnostic_report_snapshot(self, snapshot_data: dict) -> dict:
        row = {
            **snapshot_data,
            "id": snapshot_data["report_id"],
            "summary": snapshot_data["report_json"]["comprehensive_financing_advice"]["summary"],
        }
        self.rows.append(row)
        return row

    async def list_financing_diagnostic_report_snapshots(self, customer_id: str, limit: int = 20) -> list[dict]:
        return [row for row in reversed(self.rows) if row["customer_id"] == customer_id][:limit]

    async def get_financing_diagnostic_report_snapshot(self, customer_id: str, report_id: str) -> dict | None:
        for row in self.rows:
            if row["customer_id"] == customer_id and row["report_id"] == report_id:
                return row
        return None


def _with_fake_realtime(report: dict, fn):
    original = snapshot_module.build_realtime_financing_diagnostic_report

    async def fake_realtime(_storage, _customer_id, _customer=None):
        return {**report, "customer_id": _customer_id}

    snapshot_module.build_realtime_financing_diagnostic_report = fake_realtime
    try:
        return fn()
    finally:
        snapshot_module.build_realtime_financing_diagnostic_report = original


def test_admin_operator_can_save_report_snapshot():
    assert can_save_financing_diagnostic_report_snapshot("admin") is True
    assert can_save_financing_diagnostic_report_snapshot("operator") is True


def test_viewer_cannot_save_report_snapshot():
    assert can_save_financing_diagnostic_report_snapshot("viewer") is False


def test_save_snapshot_does_not_overwrite_history_versions():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        first = asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        second = asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        return first, second

    first, second = _with_fake_realtime(_report(), run)

    assert first["report_version"] == "v1"
    assert second["report_version"] == "v2"
    assert len(storage.rows) == 2


def test_second_save_generates_v2():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        return asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))

    second = _with_fake_realtime(_report(), run)

    assert second["report_version"] == "v2"


def test_history_list_returns_multiple_versions():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        return asyncio.run(service.list_snapshots("customer-1"))

    items = _with_fake_realtime(_report(), run)

    assert [item["report_version"] for item in items] == ["v2", "v1"]


def test_detail_only_reads_current_customer_report():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        saved = asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        cross_customer = asyncio.run(service.get_snapshot("customer-2", saved["report_id"]))
        same_customer = asyncio.run(service.get_snapshot("customer-1", saved["report_id"]))
        return cross_customer, same_customer

    cross_customer, same_customer = _with_fake_realtime(_report(), run)

    assert cross_customer is None
    assert same_customer["customer_id"] == "customer-1"


def test_empty_history_returns_empty_list():
    service = FinancingDiagnosticReportSnapshotService(FakeSnapshotStorage())

    assert asyncio.run(service.list_snapshots("customer-1")) == []


def test_snapshot_report_json_contains_comprehensive_financing_advice():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        saved = asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        return asyncio.run(service.get_snapshot("customer-1", saved["report_id"]))

    detail = _with_fake_realtime(_report(), run)

    assert "comprehensive_financing_advice" in detail["report_json"]


def test_snapshot_report_markdown_contains_customer_financing_report_title():
    storage = FakeSnapshotStorage()
    service = FinancingDiagnosticReportSnapshotService(storage)

    def run():
        saved = asyncio.run(service.save_current_report_snapshot("customer-1", generated_by="op"))
        return asyncio.run(service.get_snapshot("customer-1", saved["report_id"]))

    detail = _with_fake_realtime(_report(), run)

    assert "客户融资诊断报告" in detail["report_markdown"]
