from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from backend.database import Base
from backend.db_models import (
    FinancingApplicationContact, FinancingApplicationTask, FinancingCommunicationRecord,
    FinancingContact, FinancingFollowUp, FinancingProductVersion, FinancingSupplementRequest,
    ProductMatchSnapshot,
)
from backend.routers.financing_application import _require_write
from backend.services.financing_communication_service import FinancingCommunicationService
from backend.tests.test_financing_application_step9_1 import create_apps, factory, ready_and_submit


@pytest.fixture
def communication_factory(factory):
    bind = factory.kw["bind"]
    Base.metadata.create_all(bind, tables=[FinancingContact.__table__, FinancingApplicationContact.__table__,
        FinancingCommunicationRecord.__table__, FinancingFollowUp.__table__])
    return factory


def service(factory):
    return FinancingCommunicationService(session_factory=factory, ensure_schema=False)


def app_and_contact(factory, *, contact_type="institution"):
    app_service, _, apps = create_apps(factory); app = apps[0]; svc = service(factory)
    contact = svc.create_contact(contact_type=contact_type,
        customer_id=app["customer_id"] if contact_type == "customer" else None,
        institution_name=app["institution_name"] if contact_type == "institution" else None,
        branch_name="上海支行" if contact_type == "institution" else None,
        name="张经理" if contact_type == "institution" else "黎云", title="客户经理", department="普惠部",
        mobile="13812345678", phone=None, email="zhang@example.com", wechat="wx-test",
        is_primary=True, related_person_id="person-1" if contact_type == "customer" else None,
        notes="", actor_id="operator")
    svc.attach_contact(app["application_id"], contact["contact_id"],
        role="relationship_manager" if contact_type == "institution" else "handler",
        is_primary=True, actor_id="operator", actor_name="经办人")
    return app_service, app, svc, contact


def communication(svc, app, contact, **overrides):
    values = dict(contact_id=contact["contact_id"], communication_side=contact["contact_type"],
        channel="phone", direction="inbound", feedback_tag="approval_progress",
        subject="审批进度沟通", content="审批老师正在审核。", occurred_at=None,
        outcome="info_only", follow_up_required=False, next_follow_up_at=None,
        related_task_id=None, related_supplement_id=None, related_review_feedback_id=None,
        related_approval_record_id=None, internal_note="", actor_id="operator", actor_name="经办人")
    values.update(overrides)
    return svc.create_communication(app["application_id"], **values)


def test_contact_model():
    assert FinancingContact.__tablename__ == "financing_contacts"
    assert hasattr(FinancingContact, "related_person_id") and hasattr(FinancingContact, "institution_name")


def test_application_contact_relation(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    rows = svc.list_application_contacts(app["application_id"], reveal_sensitive=True)
    assert len(rows) == 1 and rows[0]["contact_id"] == contact["contact_id"]


def test_customer_communication(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory, contact_type="customer")
    row = communication(svc, app, contact, communication_side="customer", direction="outbound",
                        subject="提醒补材料", content="请明天下午提供材料。", outcome="waiting_customer")
    assert row["communication_side"] == "customer" and row["outcome"] == "waiting_customer"


def test_institution_communication(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    row = communication(svc, app, contact)
    assert row["communication_side"] == "institution" and row["contact_name"] == "张经理"


def test_followup_model():
    assert FinancingFollowUp.__tablename__ == "financing_followups"
    assert hasattr(FinancingFollowUp, "related_task_id")


def test_followup_auto_created(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory, contact_type="customer")
    due = datetime.now() + timedelta(days=1)
    communication(svc, app, contact, communication_side="customer", subject="确认材料",
                  content="客户承诺明天提供。", follow_up_required=True, next_follow_up_at=due)
    rows = svc.list_follow_ups(application_id=app["application_id"])
    assert len(rows) == 1 and rows[0]["status"] == "pending"


def test_followup_completion_closes_communication_loop(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory, contact_type="customer")
    due = datetime.now() + timedelta(days=1)
    record = communication(svc, app, contact, communication_side="customer",
        outcome="waiting_customer", follow_up_required=True, next_follow_up_at=due)
    follow_up = svc.list_follow_ups(application_id=app["application_id"])[0]
    svc.update_follow_up(follow_up["follow_up_id"], status="completed", due_at=None,
        priority=None, result="客户已回复", actor_id="operator", actor_name="经办人")
    assert svc.get_communication(record["communication_id"], include_internal=True)["outcome"] == "resolved"


def test_followup_overdue(communication_factory):
    _, app, svc, _ = app_and_contact(communication_factory)
    row = svc.create_follow_up(app["application_id"], communication_record_id=None,
        related_task_id=None, follow_up_type="institution", title="查询审批进度", description="",
        assignee_user_id="operator", assignee_user_name="经办人",
        due_at=datetime.now() - timedelta(days=1), priority="normal",
        actor_id="operator", actor_name="经办人")
    assert row["overdue"] is True and row["status"] == "pending"


def test_communication_create_task(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    row = communication(svc, app, contact, outcome="action_required")
    svc.create_task_from_communication(row["communication_id"], title=None,
        assignee_user_id="operator", assignee_user_name="经办人", due_date=None,
        priority="high", actor_id="operator", actor_name="经办人")
    with communication_factory() as db:
        task = db.scalar(select(FinancingApplicationTask).where(FinancingApplicationTask.source_ref == row["communication_id"]))
        assert task is not None and task.source_type == "communication"


def test_communication_create_supplement(communication_factory):
    app_service, app, svc, contact = app_and_contact(communication_factory)
    ready_and_submit(app_service, app["application_id"])
    row = communication(svc, app, contact, feedback_tag="material_requirement",
                        subject="补充纳税证明", content="请补充2025年度纳税证明。", outcome="action_required")
    result = svc.create_supplement_from_communication(row["communication_id"],
        required_materials=["2025年度纳税证明"], due_date=None,
        actor_id="operator", actor_name="经办人")
    assert result["status"] == "supplement_required"
    with communication_factory() as db:
        record = db.scalar(select(FinancingCommunicationRecord).where(
            FinancingCommunicationRecord.communication_id == row["communication_id"]))
        assert record.related_supplement_id and db.scalar(select(FinancingSupplementRequest).where(
            FinancingSupplementRequest.supplement_id == record.related_supplement_id))


def test_communication_create_review_feedback(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    row = communication(svc, app, contact, content="银行询问财务数据口径。")
    before = svc.applications.get_application(app["application_id"])["status"]
    result = svc.create_review_feedback_from_communication(row["communication_id"],
        feedback_type="financial_question", actor_id="operator", actor_name="经办人")
    assert result["status"] == before and len(result["review_feedback"]) == 1


def test_communication_does_not_change_application_status(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    before = svc.applications.get_application(app["application_id"])["status"]
    communication(svc, app, contact, subject="银行口头意见", content="审批应该通过。")
    assert svc.applications.get_application(app["application_id"])["status"] == before


def test_customer_statement_does_not_change_matching_facts(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory, contact_type="customer")
    with communication_factory() as db:
        before = [(row.snapshot_id, row.facts_hash) for row in db.scalars(select(ProductMatchSnapshot))]
    communication(svc, app, contact, communication_side="customer", direction="inbound",
                  subject="客户口头说明", content="我有一套房子。")
    with communication_factory() as db:
        after = [(row.snapshot_id, row.facts_hash) for row in db.scalars(select(ProductMatchSnapshot))]
    assert after == before


def test_bank_feedback_does_not_change_product_catalog(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    with communication_factory() as db:
        before = [(row.version_id, str(row.max_amount), row.status) for row in db.scalars(select(FinancingProductVersion))]
    communication(svc, app, contact, feedback_tag="amount", subject="产品额度口径",
                  content="客户经理口头表示当前最高500万元。")
    with communication_factory() as db:
        after = [(row.version_id, str(row.max_amount), row.status) for row in db.scalars(select(FinancingProductVersion))]
    assert after == before


def test_unified_timeline(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    communication(svc, app, contact)
    timeline = svc.unified_timeline(app["application_id"], include_internal=True)
    assert {row["type"] for row in timeline} >= {"application_event", "communication"}
    assert timeline == sorted(timeline, key=lambda row: (row["occurred_at"], row["id"]), reverse=True)


def test_void_communication_audit(communication_factory):
    _, app, svc, contact = app_and_contact(communication_factory)
    row = communication(svc, app, contact)
    voided = svc.void_communication(row["communication_id"], reason="录入错误",
        actor_id="admin", actor_name="管理员")
    assert voided["status"] == "voided" and voided["void_reason"] == "录入错误"
    assert any(item["title"] == "communication_voided" for item in svc.unified_timeline(app["application_id"], include_internal=True))


def test_viewer_read_only():
    with pytest.raises(HTTPException) as exc: _require_write({"role": "viewer"})
    assert exc.value.status_code == 403


def test_shanghai_yichuan_no_fake_communication(communication_factory):
    svc = service(communication_factory)
    with pytest.raises(LookupError, match="融资申请不存在"):
        svc.list_communications("enterprise_上海意川建筑科技有限公司")
