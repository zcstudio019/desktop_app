from __future__ import annotations

import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from fastapi import HTTPException

from backend.db_models import (
    FinancingApplicationMaterial, FinancingApprovalCondition,
    FinancingDisbursementRecord, FinancingReviewFeedback,
    FinancingSupplementRequest,
)
from backend.routers.financing_application import _require_write
from backend.services.financing_application_service import FinancingApplicationError
from backend.tests.test_financing_application_step9_1 import (
    create_apps, factory, ready_and_submit, under_review,
)


def approve(service, app_id: str, amount: str = "8000000", conditions=None):
    under_review(service, app_id)
    return service.record_approval(
        app_id, approved_amount=Decimal(amount), approved_term_months=12,
        approved_interest_rate=Decimal("0.035"), approval_reference="APR-1",
        approved_at=None, notes="", actor_id="operator", actor_name="经办人",
        conditions=conditions or [],
    )


def test_dashboard_counts(factory):
    service, _, rows = create_apps(factory)
    service.start_preparation(rows[0]["application_id"], actor_id="operator", actor_name="经办人")
    dashboard = service.dashboard()
    assert dashboard["total_applications"] == 1 and dashboard["status_counts"]["preparing"] == 1


def test_dashboard_amounts(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    dashboard = service.dashboard()
    assert dashboard["amounts"]["target_amount"] == "8000000.00"
    assert dashboard["amounts"]["submitted_amount"] == "8000000.00"
    assert dashboard["amounts"]["pending_approval_amount"] == "8000000.00"


def test_stage_timeline(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    result = service.advance_application_stage(app_id, "02_submission", actor_id="operator", actor_name="经办人")
    assert result["current_stage_code"] == "02_submission"
    assert result["stages"][0]["status"] == "completed" and result["stages"][1]["status"] == "in_progress"


def test_event_timeline(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    service.start_preparation(app_id, actor_id="operator", actor_name="经办人")
    events = service.get_application(app_id)["events"]
    assert [value["event_type"] for value in events] == ["application_created", "status_changed"]


def test_task_complete(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"]); app = rows[0]
    task = app["tasks"][0]
    result = service.complete_task(app["application_id"], task["task_id"], notes="已备齐", actor_id="operator", actor_name="经办人")
    assert result["tasks"][0]["status"] == "done"


def test_task_block(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"]); app = rows[0]
    result = service.block_task(app["application_id"], app["tasks"][0]["task_id"], reason="等待盖章", actor_id="operator", actor_name="经办人")
    assert result["tasks"][0]["status"] == "blocked" and "1个任务已阻塞" in result["blocking_items"]


def test_task_unblock(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"]); app = rows[0]; task_id = app["tasks"][0]["task_id"]
    service.block_task(app["application_id"], task_id, reason="等待盖章", actor_id="operator", actor_name="经办人")
    result = service.unblock_task(app["application_id"], task_id, reason="已解决", target_status="in_progress", actor_id="operator", actor_name="经办人")
    assert result["tasks"][0]["status"] == "in_progress"


def test_task_overdue_display(factory):
    service, _, rows = create_apps(factory, materials=["营业执照"]); app = rows[0]; task = app["tasks"][0]
    service.update_task(app["application_id"], task["task_id"], status="todo", due_date=date.today() - timedelta(days=1), actor_id="operator", actor_name="经办人")
    result = service.get_application(app["application_id"])
    assert result["overdue_task_count"] == 1 and result["tasks"][0]["status"] == "todo"


def test_create_supplement(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    result = service.request_supplement(app_id, description="银行补件", required_materials=["近6个月流水"], due_date=None, actor_id="operator", actor_name="经办人")
    assert result["status"] == "supplement_required" and len(result["supplements"]) == 1


def test_supplement_material(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    result = service.request_supplement(app_id, description="补件", required_materials=["纳税证明"], due_date=None, actor_id="operator", actor_name="经办人")
    assert result["application_materials"][0]["source_type"] == "supplement_request"


def test_supplement_not_auto_complete(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    result = service.request_supplement(app_id, description="补件", required_materials=["纳税证明"], due_date=None, actor_id="operator", actor_name="经办人")
    assert result["supplements"][0]["status"] == "pending" and result["status"] == "supplement_required"


def test_supplement_complete(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    ready_and_submit(service, app_id)
    current = service.request_supplement(app_id, description="补件", required_materials=["纳税证明"], due_date=None, actor_id="operator", actor_name="经办人")
    material = current["application_materials"][0]; task = [value for value in current["tasks"] if value["source_type"] == "supplement_request"][0]
    service.update_application_material(app_id, material["application_material_id"], status="uploaded", file_reference="doc-1", notes=None, actor_id="operator", actor_name="经办人")
    service.complete_task(app_id, task["task_id"], notes="", actor_id="operator", actor_name="经办人")
    done = service.complete_supplement(app_id, current["supplements"][0]["supplement_id"], actor_id="operator", actor_name="经办人")
    assert done["supplements"][0]["status"] == "completed" and done["status"] == "supplement_required"
    assert service.mark_under_review(app_id, actor_id="operator", actor_name="经办人")["status"] == "under_review"


def test_review_feedback_no_status_change(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(service, app_id)
    result = service.add_review_feedback(app_id, feedback_type="approval_progress", content="已进入终审", feedback_date=None, institution_contact="张经理", actor_id="operator", actor_name="经办人")
    assert result["status"] == "under_review" and result["review_feedback"][0]["content"] == "已进入终审"


def test_full_approval_record(factory):
    service, _, rows = create_apps(factory); result = approve(service, rows[0]["application_id"])
    assert result["status"] == "approved" and result["approval_records"][0]["approval_status"] == "full_approval"


def test_partial_approval_record(factory):
    service, _, rows = create_apps(factory); result = approve(service, rows[0]["application_id"], "5000000")
    assert result["status"] == "partially_approved" and result["approval_records"][0]["approval_status"] == "partial_approval"


def test_approval_condition_blocks_disbursement(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    approve(service, app_id, conditions=[{"title": "落实抵押登记"}])
    with pytest.raises(FinancingApplicationError, match="批复条件"):
        service.start_disbursing(app_id, actor_id="operator", actor_name="经办人")


def test_approval_condition_satisfied(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    current = approve(service, app_id, conditions=[{"title": "落实抵押登记"}])
    condition_id = current["approval_conditions"][0]["approval_condition_id"]
    service.update_approval_condition(app_id, condition_id, status="satisfied", actor_id="operator", actor_name="经办人")
    assert service.start_disbursing(app_id, actor_id="operator", actor_name="经办人")["status"] == "disbursing"


def test_create_disbursement(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    approve(service, app_id)
    result = service.record_disbursement(app_id, disbursed_amount=Decimal("8000000"), disbursed_at=None, disbursement_reference="PAY-1", actor_id="operator", actor_name="经办人")
    assert len(result["disbursements"]) == 1 and result["disbursements"][0]["amount"] == "8000000.00"


def test_partial_disbursement_keeps_disbursing(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    approve(service, app_id, "5000000")
    result = service.record_disbursement(app_id, disbursed_amount=Decimal("3000000"), disbursed_at=None, disbursement_reference="PAY-1", actor_id="operator", actor_name="经办人")
    assert result["status"] == "disbursing" and result["disbursed_amount"] == "3000000.00"


def test_full_disbursement_marks_disbursed(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    approve(service, app_id, "5000000")
    service.record_disbursement(app_id, disbursed_amount=Decimal("3000000"), disbursed_at=None, disbursement_reference="PAY-1", actor_id="operator", actor_name="经办人")
    result = service.record_disbursement(app_id, disbursed_amount=Decimal("2000000"), disbursed_at=None, disbursement_reference="PAY-2", actor_id="operator", actor_name="经办人")
    assert result["status"] == "disbursed" and len(result["disbursements"]) == 2


def test_disbursement_cannot_exceed_approved(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    approve(service, app_id, "5000000")
    with pytest.raises(FinancingApplicationError, match="累计放款"):
        service.record_disbursement(app_id, disbursed_amount=Decimal("5000001"), disbursed_at=None, disbursement_reference=None, actor_id="operator", actor_name="经办人")


def test_retry_display(factory):
    service, _, rows = create_apps(factory); app_id = rows[0]["application_id"]
    under_review(service, app_id)
    service.record_rejection(app_id, rejection_reason="准入不符", rejection_code="RULE", rejected_at=None, notes="", actor_id="operator", actor_name="经办人")
    retry = service.retry_application(app_id, actor_id="operator", actor_name="经办人")
    assert retry["attempt_no"] == 2 and retry["parent_application_id"] == app_id


def test_next_action(factory):
    service, _, rows = create_apps(factory)
    assert rows[0]["next_action"] == "开始材料准备"
    assert service.advance_application_stage(rows[0]["application_id"], "02_submission", actor_id="operator", actor_name="经办人")["next_action"] == "提交银行"


def test_viewer_read_only():
    with pytest.raises(HTTPException) as exc:
        _require_write({"role": "viewer"})
    assert exc.value.status_code == 403


def test_shanghai_yichuan_no_fake_execution(factory):
    from backend.services.financing_application_service import FinancingApplicationService
    service = FinancingApplicationService(session_factory=factory, ensure_schema=False)
    assert service.list_applications("enterprise_上海意川建筑科技有限公司") == []
