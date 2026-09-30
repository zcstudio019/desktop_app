from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import (
    Customer, Document, Extraction, FinancingApplication, FinancingApplicationEvent,
    FinancingApplicationMaterial, FinancingApplicationPackage, FinancingApplicationPackageItem,
    FinancingApplicationStage, FinancingApplicationTask,
    FinancingApplicationContact, FinancingCommunicationRecord, FinancingContact, FinancingFollowUp,
    FinancingApprovalCondition, FinancingApprovalRecord, FinancingDisbursementRecord, FinancingPlan,
    FinancingPlanCondition, FinancingPlanExplanation, FinancingPlanGap,
    FinancingPlanItem, FinancingPlanMaterial, FinancingPlanVersion,
    FinancingProduct, FinancingProductRule, FinancingProductVersion,
    FinancingMaterialEvent, FinancingRequirement, FinancingReviewFeedback,
    FinancingSubmissionPackage, FinancingSupplementPackage, FinancingSupplementPackageItem,
    FinancingSupplementRequest,
    ManualCandidateOverride, ProductMatchItem, ProductMatchSnapshot,
)
from backend.routers.financing_application import _require_write
from backend.services.financing_application_service import (
    ApplicationStateMachine, FinancingApplicationError, FinancingApplicationService, STAGES,
)
from backend.services.financing_plan_service import FinancingPlanService
from backend.tests.test_financing_plan_step8_3 import CUSTOMER, REQUIREMENT, draft, seed


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        FinancingRequirement.__table__, FinancingProduct.__table__, FinancingProductVersion.__table__,
        FinancingProductRule.__table__, ProductMatchSnapshot.__table__, ProductMatchItem.__table__,
        FinancingPlan.__table__, FinancingPlanVersion.__table__, FinancingPlanItem.__table__, FinancingPlanGap.__table__,
        ManualCandidateOverride.__table__, FinancingPlanCondition.__table__, FinancingPlanMaterial.__table__,
        FinancingPlanExplanation.__table__, Document.__table__, Extraction.__table__, FinancingApplication.__table__,
        FinancingApplicationStage.__table__, FinancingApplicationTask.__table__, FinancingApplicationEvent.__table__,
        FinancingSupplementRequest.__table__, FinancingApplicationMaterial.__table__,
        FinancingReviewFeedback.__table__, FinancingApprovalRecord.__table__, FinancingApprovalCondition.__table__,
        FinancingDisbursementRecord.__table__, FinancingApplicationPackage.__table__,
        FinancingApplicationPackageItem.__table__, FinancingSubmissionPackage.__table__,
        FinancingSupplementPackage.__table__, FinancingSupplementPackageItem.__table__,
        FinancingMaterialEvent.__table__, Customer.__table__,
        FinancingContact.__table__, FinancingApplicationContact.__table__,
        FinancingCommunicationRecord.__table__, FinancingFollowUp.__table__,
    ])
    yield sessionmaker(bind=engine)
    engine.dispose()


def services(factory):
    return FinancingPlanService(session_factory=factory, ensure_schema=False), FinancingApplicationService(session_factory=factory, ensure_schema=False)


def make_confirmed_plan(factory, count=1, *, materials=None):
    products = [{"status": "eligible", "max_amount": Decimal("10000000"), "materials": materials or []} for _ in range(count)]
    seed(factory, products)
    plan_service, _ = services(factory)
    plan = plan_service.create_plan_draft(draft(factory, [str(8000000 // count)] * count), created_by="operator")
    return plan_service.confirm_plan(plan["financing_plan_id"], confirmed_by="operator")


def create_apps(factory, count=1, *, materials=None):
    plan = make_confirmed_plan(factory, count, materials=materials)
    _, app_service = services(factory)
    rows = app_service.create_applications_from_confirmed_plan(plan["current_version_id"], created_by="operator")
    return app_service, plan, rows


def ready_and_submit(service, app_id):
    current = service.get_application(app_id)
    for material in current["application_materials"]:
        service.update_application_material(app_id, material["application_material_id"], status="uploaded",
                                            file_reference="fixture.pdf", notes=None,
                                            actor_id="operator", actor_name="经办人")
    for task in current["tasks"]:
        service.update_task(app_id, task["task_id"], status="done", actor_id="operator", actor_name="经办人")
    service.mark_ready_to_submit(app_id, actor_id="operator", actor_name="经办人")
    return service.submit_application(app_id, submitted_amount=None, submission_channel="线下", submission_reference="SUB-1", submission_notes="", actor_id="operator", actor_name="经办人")


def under_review(service, app_id):
    ready_and_submit(service, app_id)
    return service.mark_under_review(app_id, actor_id="operator", actor_name="经办人")


def test_application_model():
    assert FinancingApplication.__tablename__ == "financing_applications" and hasattr(FinancingApplication, "plan_item_id")


def test_stage_model():
    assert FinancingApplicationStage.__tablename__ == "financing_application_stages" and len(STAGES) == 7


def test_task_model():
    assert FinancingApplicationTask.__tablename__ == "financing_application_tasks"
    assert hasattr(FinancingApplicationTask, "related_material_id") and hasattr(FinancingApplicationTask, "source_type")


def test_event_model():
    assert FinancingApplicationEvent.__tablename__ == "financing_application_events" and hasattr(FinancingApplicationEvent, "payload_json")


def test_create_application_from_confirmed_plan(factory):
    _, plan, rows = create_apps(factory)
    assert len(rows) == 1 and rows[0]["plan_version_id"] == plan["current_version_id"] and rows[0]["status"] == "draft"
    assert rows[0]["approved_amount"] is None and rows[0]["target_interest_rate"] is None


def test_two_plan_items_create_two_applications(factory):
    _, _, rows = create_apps(factory, 2)
    assert len(rows) == 2 and len({row["plan_item_id"] for row in rows}) == 2


def test_draft_plan_cannot_create_application(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    plan_service, app_service = services(factory)
    plan = plan_service.create_plan_draft(draft(factory, ["8000000"]), created_by="operator")
    with pytest.raises(FinancingApplicationError, match="已确认"):
        app_service.create_applications_from_confirmed_plan(plan["current_version_id"], created_by="operator")


def test_missing_material_blocks_ready(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"])
    with pytest.raises(FinancingApplicationError, match="材料"):
        service.mark_ready_to_submit(rows[0]["application_id"], actor_id="operator", actor_name="经办人")


def test_pending_condition_blocks_ready(factory):
    service, plan, rows = create_apps(factory)
    with factory.begin() as db:
        db.add(FinancingPlanCondition(condition_id="pending-condition", plan_version_id=plan["current_version_id"],
            condition_type="other", title="核验抵押物", description="", source_type="manual", status="pending", required=1))
    with pytest.raises(FinancingApplicationError, match="条件"):
        service.mark_ready_to_submit(rows[0]["application_id"], actor_id="operator", actor_name="经办人")


def test_pending_required_task_blocks_ready(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"])
    assert rows[0]["tasks"][0]["status"] == "todo"
    assert rows[0]["tasks"][0]["source_type"] == "plan_material"
    with pytest.raises(FinancingApplicationError, match="材料"):
        service.mark_ready_to_submit(rows[0]["application_id"], actor_id="operator", actor_name="经办人")


def test_ready_to_submit_validation(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"]); app_id = rows[0]["application_id"]
    current = service.get_application(app_id); task = current["tasks"][0]
    service.update_application_material(app_id, current["application_materials"][0]["application_material_id"],
                                        status="uploaded", file_reference="license.pdf", notes=None,
                                        actor_id="operator", actor_name="经办人")
    service.update_task(app_id, task["task_id"], status="done", actor_id="operator", actor_name="经办人")
    assert service.mark_ready_to_submit(app_id, actor_id="operator", actor_name="经办人")["status"] == "ready_to_submit"


def test_submit_application(factory):
    service, _, rows = create_apps(factory)
    result = ready_and_submit(service, rows[0]["application_id"])
    assert result["status"] == "submitted" and result["submitted_amount"] == result["target_amount"] and result["submission_reference"] == "SUB-1"


def test_request_supplement(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    result = service.request_supplement(app_id, description="银行要求补件", required_materials=["近6个月流水"], due_date=None, actor_id="operator", actor_name="经办人")
    assert result["status"] == "supplement_required"
    assert any(task["title"] == "补充：近6个月流水" and task["source_type"] == "supplement_request" for task in result["tasks"])


def test_mark_under_review(factory):
    service, _, rows = create_apps(factory)
    assert under_review(service, rows[0]["application_id"])["status"] == "under_review"


def test_record_full_approval(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    current = under_review(service, app_id)
    result = service.record_approval(app_id, approved_amount=Decimal(current["submitted_amount"]), approved_term_months=12, approved_interest_rate=Decimal("0.035"), approval_reference="APR-1", approved_at=None, notes="", actor_id="operator", actor_name="经办人")
    assert result["status"] == "approved" and result["approved_interest_rate"] == "0.035000"


def test_record_partial_approval(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(service, app_id)
    assert service.record_approval(app_id, approved_amount=Decimal("5000000"), approved_term_months=12, approved_interest_rate=None, approval_reference=None, approved_at=None, notes="", actor_id="operator", actor_name="经办人")["status"] == "partially_approved"


def test_record_rejection(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(service, app_id)
    result = service.record_rejection(app_id, rejection_reason="银行准入不符", rejection_code="BANK_RULE", rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    assert result["status"] == "rejected" and result["final_result"] == "rejected"


def test_record_disbursement(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    current = under_review(service, app_id)
    service.record_approval(app_id, approved_amount=Decimal(current["submitted_amount"]), approved_term_months=12, approved_interest_rate=None, approval_reference=None, approved_at=None, notes="", actor_id="operator", actor_name="经办人")
    result = service.record_disbursement(app_id, disbursed_amount=Decimal(current["submitted_amount"]), disbursed_at=None, disbursement_reference="PAY-1", actor_id="operator", actor_name="经办人")
    assert result["status"] == "disbursed" and result["disbursement_reference"] == "PAY-1"


def test_illegal_state_transition(factory):
    service, _, rows = create_apps(factory)
    with pytest.raises(FinancingApplicationError):
        service.record_approval(rows[0]["application_id"], approved_amount=Decimal("1"), approved_term_months=1, approved_interest_rate=None, approval_reference=None, approved_at=None, notes="", actor_id="operator", actor_name="经办人")
    with pytest.raises(FinancingApplicationError): ApplicationStateMachine.validate("draft", "approved")


def test_event_log_created(factory):
    service, _, rows = create_apps(factory)
    result = service.start_preparation(rows[0]["application_id"], actor_id="operator", actor_name="经办人")
    assert [event["event_type"] for event in result["events"]] == ["application_created", "status_changed"]


def test_duplicate_active_application_blocked(factory):
    service, plan, _ = create_apps(factory)
    with pytest.raises(FinancingApplicationError, match="有效申请"):
        service.create_applications_from_confirmed_plan(plan["current_version_id"], created_by="operator")


def test_retry_application(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(service, app_id)
    service.record_rejection(app_id, rejection_reason="暂不准入", rejection_code=None, rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    retry = service.retry_application(app_id, actor_id="operator", actor_name="经办人")
    assert retry["parent_application_id"] == app_id and retry["attempt_no"] == 2 and retry["status"] == "draft"


def test_viewer_cannot_mutate():
    with pytest.raises(HTTPException) as exc: _require_write({"role": "viewer"})
    assert exc.value.status_code == 403


def test_shanghai_yichuan_has_no_fake_application(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}])
    _, service = services(factory)
    assert service.list_applications(CUSTOMER, REQUIREMENT) == []
