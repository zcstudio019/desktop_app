from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.exc import StatementError

from backend.database import Base
from backend.db_models import (
    ApplicationBottleneck, FinancingApplication, FinancingApplicationOutcome,
    FinancingApplicationRejectionReason, FinancingApplicationTask,
    FinancingPlan, FinancingProductVersion, FinancingSupplementOutcome,
    ProductRuleFeedback,
)
from backend.routers.financing_application import _require_write
from backend.services.financing_application_service import FinancingApplicationError
from backend.services.financing_outcome_service import (
    REJECTION_REASON_REGISTRY, FinancingOutcomeError, FinancingOutcomeService,
)
from backend.tests.test_financing_application_step9_1 import create_apps, factory, ready_and_submit, under_review
from backend.tests.test_financing_plan_step8_3 import CUSTOMER, REQUIREMENT, seed


@pytest.fixture
def outcome_factory(factory):
    bind = factory.kw["bind"]
    Base.metadata.create_all(bind, tables=[
        FinancingApplicationOutcome.__table__, FinancingApplicationRejectionReason.__table__,
        FinancingSupplementOutcome.__table__, ApplicationBottleneck.__table__,
        ProductRuleFeedback.__table__,
    ])
    return factory


def outcome_service(factory):
    return FinancingOutcomeService(session_factory=factory, ensure_schema=False)


def approve_and_disburse(factory, approved="8000000", disbursed=None):
    app_service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_approval(app_id, approved_amount=Decimal(approved), approved_term_months=12,
        approved_interest_rate=Decimal("0.035"), approval_reference="APR-1", approved_at=None,
        notes="", actor_id="operator", actor_name="经办人")
    if disbursed:
        app_service.record_disbursement(app_id, disbursed_amount=Decimal(disbursed),
            disbursed_at=None, disbursement_reference="PAY-1", actor_id="operator", actor_name="经办人")
    return app_service, app_id


def finalize(service, app_id, **values):
    return service.finalize_outcome(app_id, rejection_reasons=values.get("rejection_reasons", []),
        disbursement_variance_reason=values.get("disbursement_variance_reason"),
        final_notes=values.get("final_notes", "真实结果已确认"), actor_id="operator", actor_name="经办人")


def test_application_outcome_model():
    assert FinancingApplicationOutcome.__tablename__ == "financing_application_outcomes"
    assert hasattr(FinancingApplicationOutcome, "supersedes_outcome_id")


def test_finalize_full_approval_disbursed(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert row["final_status"] == "approved_disbursed"
    assert row["approval_variance"]["amount_delta"] == "0.00"


def test_finalize_partial_approval_disbursed(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "5000000", "5000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert row["final_status"] == "partially_approved_disbursed"
    assert row["approval_variance"]["amount_delta"] == "-3000000.00"


def test_finalize_rejected(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="负债和流水不符", rejection_code="BANK_RULE",
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    row = finalize(outcome_service(outcome_factory), app_id, rejection_reasons=[
        {"reason_code": "debt_issue", "description": "负债较高", "source_type": "bank_feedback"},
        {"reason_code": "cashflow_issue", "description": "流水不足", "source_type": "approval_record"},
    ])
    assert row["final_status"] == "rejected" and len(row["rejection_reasons"]) == 2


def test_outcome_immutable(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    with pytest.raises((ValueError, StatementError)):
        with outcome_factory.begin() as db:
            current = db.scalar(select(FinancingApplicationOutcome).where(
                FinancingApplicationOutcome.outcome_id == row["outcome_id"]))
            current.final_notes = "覆盖旧结果"


def test_outcome_correction_version(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    first = finalize(outcome_service(outcome_factory), app_id)
    second = outcome_service(outcome_factory).correct_outcome(app_id,
        changes={"final_notes": "修正后备注"}, reason="原录入备注错误",
        actor_id="admin", actor_name="管理员")
    assert second["outcome_version"] == 2 and second["supersedes_outcome_id"] == first["outcome_id"]


def test_rejection_reason_registry():
    assert REJECTION_REASON_REGISTRY["credit_issue"] == "征信问题"
    assert len(REJECTION_REASON_REGISTRY) >= 18


def test_multiple_rejection_reasons(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="多项原因", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    result = finalize(outcome_service(outcome_factory), app_id, rejection_reasons=[
        {"reason_code": "debt_issue", "source_type": "bank_feedback"},
        {"reason_code": "cashflow_issue", "source_type": "manual_confirmed"},
    ])
    assert {item["reason_code"] for item in result["rejection_reasons"]} == {"debt_issue", "cashflow_issue"}


def test_supplement_metrics(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    ready_and_submit(app_service, app_id)
    app_service.request_supplement(app_id, description="补税务资料", required_materials=["纳税证明"],
        due_date=None, actor_id="operator", actor_name="经办人")
    current = app_service.get_application(app_id)
    material = [item for item in current["application_materials"] if item["source_type"] == "supplement_request"][0]
    task = [item for item in current["tasks"] if item["source_type"] == "supplement_request"][0]
    app_service.update_application_material(app_id, material["application_material_id"], status="uploaded",
        file_reference="tax.pdf", notes=None, actor_id="operator", actor_name="经办人")
    app_service.complete_task(app_id, task["task_id"], notes="已补齐", actor_id="operator", actor_name="经办人")
    supplement = current["supplements"][0]
    app_service.complete_supplement(app_id, supplement["supplement_id"], actor_id="operator", actor_name="经办人")
    app_service.mark_under_review(app_id, actor_id="operator", actor_name="经办人")
    app_service.record_rejection(app_id, rejection_reason="政策不符", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    finalize(outcome_service(outcome_factory), app_id)
    with outcome_factory() as db:
        row = db.scalar(select(FinancingSupplementOutcome).where(FinancingSupplementOutcome.application_id == app_id))
        assert row.supplement_count == 1 and row.supplement_material_count == 1


def test_bottleneck_duration(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory, materials=["营业执照"]); app = rows[0]
    task = app["tasks"][0]
    app_service.block_task(app["application_id"], task["task_id"], reason="等待盖章",
        actor_id="operator", actor_name="经办人")
    service = outcome_service(outcome_factory)
    current = service.sync_bottlenecks(app["application_id"])
    assert current[0]["bottleneck_type"] == "blocked_task" and current[0]["duration_days"] >= 0


def test_product_execution_metrics(outcome_factory):
    service = outcome_service(outcome_factory)
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    outcomes = []
    for index in range(3):
        under_review(app_service, app_id)
        submitted = Decimal(app_service.get_application(app_id)["submitted_amount"])
        if index == 2:
            app_service.record_rejection(app_id, rejection_reason="拒绝", rejection_code=None,
                rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
        else:
            approved = submitted if index == 0 else submitted - Decimal("100000")
            app_service.record_approval(app_id, approved_amount=approved, approved_term_months=12,
                approved_interest_rate=None, approval_reference=None, approved_at=None, notes="",
                actor_id="operator", actor_name="经办人")
            app_service.record_disbursement(app_id, disbursed_amount=approved, disbursed_at=None,
                disbursement_reference=None, actor_id="operator", actor_name="经办人")
        outcomes.append(finalize(service, app_id))
        if index < 2:
            service.close_application(app_id, close_reason="本次申请结束", final_result_confirmed=True,
                actor_id="operator", actor_name="经办人")
            app_id = app_service.retry_application(app_id, actor_id="operator", actor_name="经办人")["application_id"]
    metrics = service.product_execution_metrics(outcomes[0]["product_id"])
    assert metrics["sample_size"] == 3 and metrics["approved_count"] == 1
    assert metrics["partially_approved_count"] == 1 and metrics["rejected_count"] == 1
    assert "success_rate" not in metrics


def test_metrics_excludes_voided_outcome(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    with outcome_factory.begin() as db:
        app = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == app_id))
        db.add(FinancingApplicationOutcome(outcome_id="voided", application_id=app_id,
            customer_id=app.customer_id, product_id=app.product_id, product_version_id=app.product_version_id,
            requirement_id=app.requirement_id, plan_version_id=app.plan_version_id, outcome_version=2,
            final_status="rejected", status="voided", closed_at=datetime.now(), created_by="admin"))
    assert outcome_service(outcome_factory).product_execution_metrics(row["product_id"])["sample_size"] == 1


def test_metrics_by_product_version(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert outcome_service(outcome_factory).product_execution_metrics(
        row["product_id"], product_version_id=row["product_version_id"])["sample_size"] == 1
    assert outcome_service(outcome_factory).product_execution_metrics(
        row["product_id"], product_version_id="other-version")["sample_size"] == 0


def test_rule_feedback_model():
    assert ProductRuleFeedback.__tablename__ == "product_rule_feedback"


def test_rule_feedback_does_not_modify_product_rule(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app = rows[0]
    with outcome_factory() as db:
        before = db.scalar(select(FinancingProductVersion).where(
            FinancingProductVersion.version_id == app["product_version_id"]))
        before_max = Decimal(before.max_amount)
    feedback = outcome_service(outcome_factory).create_rule_feedback(app["product_id"],
        product_version_id=app["product_version_id"], rule_id=None,
        application_id=app["application_id"], feedback_type="outdated_rule",
        expected_result="最高500万元", actual_bank_feedback="实际批复600万元",
        evidence_source="approval_record", evidence_reference="APR-1", actor_id="operator")
    with outcome_factory() as db:
        after = db.scalar(select(FinancingProductVersion).where(
            FinancingProductVersion.version_id == app["product_version_id"]))
        assert Decimal(after.max_amount) == before_max and feedback["status"] == "pending"


def test_actual_approval_above_catalog_cap_creates_review_feedback(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    with outcome_factory.begin() as db:
        db.execute(update(FinancingProductVersion).where(
            FinancingProductVersion.version_id == rows[0]["product_version_id"]).values(max_amount=Decimal("5000000")))
    under_review(app_service, app_id)
    app_service.record_approval(app_id, approved_amount=Decimal("6000000"), approved_term_months=12,
        approved_interest_rate=None, approval_reference="APR-600", approved_at=None, notes="",
        actor_id="operator", actor_name="经办人")
    app_service.record_disbursement(app_id, disbursed_amount=Decimal("6000000"), disbursed_at=None,
        disbursement_reference="PAY-600", actor_id="operator", actor_name="经办人")
    finalize(outcome_service(outcome_factory), app_id)
    rows = outcome_service(outcome_factory).list_rule_feedback(rows[0]["product_id"])
    assert len(rows) == 1 and rows[0]["feedback_type"] == "outdated_rule"
    with outcome_factory() as db:
        version = db.scalar(select(FinancingProductVersion).where(
            FinancingProductVersion.version_id == rows[0]["product_version_id"]))
        assert Decimal(version.max_amount) == Decimal("5000000")


def test_customer_financing_review(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "5000000", "5000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    review = outcome_service(outcome_factory).customer_financing_review(row["customer_id"], requirement_id=row["requirement_id"])
    assert review["disbursed_amount"] == "5000000.00" and review["funding_gap"] == "3000000.00"


def test_requirement_fully_funded(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "8000000", "8000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert outcome_service(outcome_factory).customer_financing_review(
        row["customer_id"], requirement_id=row["requirement_id"])["execution_status"] == "fully_funded"


def test_requirement_partially_funded(outcome_factory):
    _, app_id = approve_and_disburse(outcome_factory, "5000000", "5000000")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert outcome_service(outcome_factory).customer_financing_review(
        row["customer_id"], requirement_id=row["requirement_id"])["execution_status"] == "partially_funded"


def test_requirement_closed_unfunded(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="拒绝", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    row = finalize(outcome_service(outcome_factory), app_id)
    assert outcome_service(outcome_factory).customer_financing_review(
        row["customer_id"], requirement_id=row["requirement_id"])["execution_status"] == "closed_unfunded"


def test_close_application(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="拒绝", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    result = outcome_service(outcome_factory).close_application(app_id,
        close_reason="拒绝后关闭", final_result_confirmed=True,
        actor_id="operator", actor_name="经办人")
    assert result["status"] == "closed"


def test_closed_application_blocks_business_actions(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="拒绝", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    outcome_service(outcome_factory).close_application(app_id, close_reason="关闭",
        final_result_confirmed=True, actor_id="operator", actor_name="经办人")
    with pytest.raises(FinancingApplicationError, match="已关闭"):
        app_service.add_review_feedback(app_id, feedback_type="general", content="新反馈",
            feedback_date=None, institution_contact=None, actor_id="operator", actor_name="经办人")


def test_unified_lifecycle_timeline(outcome_factory):
    app_service, _, rows = create_apps(outcome_factory); app_id = rows[0]["application_id"]
    under_review(app_service, app_id)
    app_service.record_rejection(app_id, rejection_reason="拒绝", rejection_code=None,
        rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    finalize(outcome_service(outcome_factory), app_id)
    events = app_service.get_application(app_id)["events"]
    assert "outcome_finalized" in {event["event_type"] for event in events}


def test_viewer_read_only():
    with pytest.raises(HTTPException):
        _require_write({"role": "viewer"})


def test_shanghai_yichuan_no_fake_outcome(outcome_factory):
    seed(outcome_factory, [{"status": "manual_review", "max_amount": None}])
    service = outcome_service(outcome_factory)
    review = service.customer_financing_review(CUSTOMER, requirement_id=REQUIREMENT)
    assert review["application_count"] == 0 and review["outcome_count"] == 0
    assert review["message"] == "当前尚无已执行融资申请。"

