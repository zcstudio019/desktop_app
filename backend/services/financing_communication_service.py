"""Deterministic contacts, communications and follow-up workflow for financing execution."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from backend.database import Base, SessionLocal
from backend.db_models import (
    FinancingApplication, FinancingApplicationContact, FinancingApplicationEvent,
    FinancingApplicationStage, FinancingApplicationTask, FinancingApprovalRecord,
    FinancingCommunicationRecord, FinancingContact, FinancingDisbursementRecord,
    FinancingFollowUp, FinancingReviewFeedback, FinancingSupplementRequest,
)
from backend.services.financing_application_service import FinancingApplicationService


CONTACT_TYPES = {"customer", "institution"}
CONTACT_STATUSES = {"active", "inactive"}
COMMUNICATION_SIDES = {"customer", "institution", "internal"}
CHANNELS = {"phone", "wechat", "email", "meeting", "onsite", "system", "other"}
DIRECTIONS = {"inbound", "outbound", "internal"}
OUTCOMES = {"info_only", "waiting_customer", "waiting_institution", "action_required", "resolved"}
FEEDBACK_TAGS = {"product_policy", "material_requirement", "credit_requirement", "approval_progress",
                 "pricing", "amount", "term", "collateral", "guarantee", "other"}
FOLLOW_UP_TYPES = {"customer", "institution", "material", "approval", "disbursement", "internal"}
FOLLOW_UP_STATUSES = {"pending", "in_progress", "completed", "cancelled"}
PRIORITIES = {"low", "normal", "high", "urgent"}


class FinancingCommunicationError(ValueError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _load(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _mask_mobile(value: str | None) -> str | None:
    if not value: return value
    return f"{value[:3]}****{value[-4:]}" if len(value) >= 7 else "****"


def _mask_email(value: str | None) -> str | None:
    if not value or "@" not in value: return value
    local, domain = value.split("@", 1)
    return f"{local[:1]}***@{domain}"


class FinancingCommunicationService:
    def __init__(self, session_factory=SessionLocal, *, ensure_schema: bool = True):
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema
        self.applications = FinancingApplicationService(session_factory=session_factory, ensure_schema=ensure_schema)

    def _prepare(self) -> None:
        if not self.ensure_schema: return
        bind = self.session_factory.kw.get("bind")
        if bind is not None:
            Base.metadata.create_all(bind, tables=[
                FinancingContact.__table__, FinancingApplicationContact.__table__,
                FinancingCommunicationRecord.__table__, FinancingFollowUp.__table__,
            ])

    @staticmethod
    def _application(db, application_id: str) -> FinancingApplication:
        row = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == application_id))
        if row is None: raise LookupError("融资申请不存在")
        return row

    @staticmethod
    def _event(db, application_id: str, event_type: str, actor_id: str, actor_name: str,
               payload: dict[str, Any]) -> None:
        db.add(FinancingApplicationEvent(event_id=uuid.uuid4().hex, application_id=application_id,
            event_type=event_type, operator_id=actor_id, operator_name=actor_name,
            payload_json=_json(payload)))

    def create_contact(self, *, contact_type: str, customer_id: str | None, institution_name: str | None,
                       branch_name: str | None, name: str, title: str | None, department: str | None,
                       mobile: str | None, phone: str | None, email: str | None, wechat: str | None,
                       is_primary: bool, related_person_id: str | None, notes: str,
                       actor_id: str) -> dict[str, Any]:
        if contact_type not in CONTACT_TYPES: raise FinancingCommunicationError("联系人类型不合法")
        if not name.strip(): raise FinancingCommunicationError("联系人姓名不能为空")
        if contact_type == "customer" and not customer_id: raise FinancingCommunicationError("客户联系人必须关联客户")
        if contact_type == "institution" and not institution_name: raise FinancingCommunicationError("机构联系人必须填写机构")
        self._prepare(); contact_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            db.add(FinancingContact(contact_id=contact_id, contact_type=contact_type,
                customer_id=customer_id, institution_name=institution_name, branch_name=branch_name,
                name=name.strip(), title=title, department=department, mobile=mobile, phone=phone,
                email=email, wechat=wechat, is_primary=int(is_primary), related_person_id=related_person_id,
                status="active", notes=notes, created_by=actor_id))
        return self.get_contact(contact_id, reveal_sensitive=True)

    def update_contact(self, contact_id: str, values: dict[str, Any]) -> dict[str, Any]:
        allowed = {"branch_name", "name", "title", "department", "mobile", "phone", "email", "wechat",
                   "is_primary", "related_person_id", "status", "notes"}
        if set(values) - allowed: raise FinancingCommunicationError("包含不可修改的联系人字段")
        if values.get("status") and values["status"] not in CONTACT_STATUSES:
            raise FinancingCommunicationError("联系人状态不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingContact).where(FinancingContact.contact_id == contact_id))
            if row is None: raise LookupError("联系人不存在")
            for key, value in values.items(): setattr(row, key, int(value) if key == "is_primary" else value)
        return self.get_contact(contact_id, reveal_sensitive=True)

    def get_contact(self, contact_id: str, *, reveal_sensitive: bool) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingContact).where(FinancingContact.contact_id == contact_id))
            if row is None: raise LookupError("联系人不存在")
            return self._contact_dict(row, reveal_sensitive)

    def list_contacts(self, *, customer_id: str | None = None, institution_name: str | None = None,
                      search: str | None = None, reveal_sensitive: bool = False) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingContact)
            if customer_id: statement = statement.where(FinancingContact.customer_id == customer_id)
            if institution_name: statement = statement.where(FinancingContact.institution_name == institution_name)
            rows = list(db.scalars(statement.order_by(FinancingContact.is_primary.desc(), FinancingContact.name)))
        if search:
            needle = search.lower()
            rows = [row for row in rows if needle in " ".join(filter(None, [row.name, row.institution_name, row.mobile, row.customer_id])).lower()]
        return [self._contact_dict(row, reveal_sensitive) for row in rows]

    @staticmethod
    def _contact_dict(row: FinancingContact, reveal: bool) -> dict[str, Any]:
        return {"contact_id": row.contact_id, "contact_type": row.contact_type,
                "customer_id": row.customer_id, "institution_name": row.institution_name,
                "branch_name": row.branch_name, "name": row.name, "title": row.title,
                "department": row.department, "mobile": row.mobile if reveal else _mask_mobile(row.mobile),
                "phone": row.phone if reveal else _mask_mobile(row.phone),
                "email": row.email if reveal else _mask_email(row.email),
                "wechat": row.wechat if reveal else ("已配置" if row.wechat else None),
                "is_primary": bool(row.is_primary), "related_person_id": row.related_person_id,
                "status": row.status, "notes": row.notes,
                "created_at": row.created_at.isoformat() if row.created_at else None}

    def attach_contact(self, application_id: str, contact_id: str, *, role: str, is_primary: bool,
                       actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare(); relation_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            contact = db.scalar(select(FinancingContact).where(FinancingContact.contact_id == contact_id))
            if contact is None: raise LookupError("联系人不存在")
            if contact.contact_type == "customer" and contact.customer_id != app.customer_id:
                raise FinancingCommunicationError("客户联系人不属于当前客户")
            if contact.contact_type == "institution" and contact.institution_name != app.institution_name:
                raise FinancingCommunicationError("机构联系人不属于当前申请机构")
            existing = db.scalar(select(FinancingApplicationContact).where(
                FinancingApplicationContact.application_id == application_id,
                FinancingApplicationContact.contact_id == contact_id))
            if existing:
                existing.role = role; existing.is_primary = int(is_primary); relation_id = existing.application_contact_id
            else:
                db.add(FinancingApplicationContact(application_contact_id=relation_id,
                    application_id=application_id, contact_id=contact_id, role=role,
                    is_primary=int(is_primary), created_by=actor_id))
            self._event(db, application_id, "contact_attached", actor_id, actor_name,
                        {"contact_id": contact_id, "role": role})
        return {"application_contact_id": relation_id, "application_id": application_id,
                "contact_id": contact_id, "role": role, "is_primary": is_primary}

    def list_application_contacts(self, application_id: str, *, reveal_sensitive: bool) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            self._application(db, application_id)
            rows = db.execute(select(FinancingApplicationContact, FinancingContact).join(
                FinancingContact, FinancingContact.contact_id == FinancingApplicationContact.contact_id).where(
                FinancingApplicationContact.application_id == application_id).order_by(
                FinancingApplicationContact.is_primary.desc(), FinancingContact.name)).all()
            return [{**self._contact_dict(contact, reveal_sensitive), "application_contact_id": rel.application_contact_id,
                     "role": rel.role, "application_primary": bool(rel.is_primary)} for rel, contact in rows]

    def create_communication(self, application_id: str, *, contact_id: str | None, communication_side: str,
                             channel: str, direction: str, feedback_tag: str | None, subject: str,
                             content: str, occurred_at: datetime | None, outcome: str,
                             follow_up_required: bool, next_follow_up_at: datetime | None,
                             related_task_id: str | None, related_supplement_id: str | None,
                             related_review_feedback_id: str | None, related_approval_record_id: str | None,
                             internal_note: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if communication_side not in COMMUNICATION_SIDES or channel not in CHANNELS or direction not in DIRECTIONS:
            raise FinancingCommunicationError("沟通类型、渠道或方向不合法")
        if outcome not in OUTCOMES: raise FinancingCommunicationError("沟通结果不合法")
        if feedback_tag and feedback_tag not in FEEDBACK_TAGS: raise FinancingCommunicationError("反馈标签不合法")
        if not subject.strip() or not content.strip(): raise FinancingCommunicationError("沟通主题和内容不能为空")
        if follow_up_required and not next_follow_up_at: raise FinancingCommunicationError("需要跟进时必须设置跟进时间")
        self._prepare(); communication_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            if app.status == "closed" and communication_side != "internal":
                raise FinancingCommunicationError("申请已关闭，只允许新增内部备注")
            if contact_id:
                contact = db.scalar(select(FinancingContact).where(FinancingContact.contact_id == contact_id))
                if contact is None: raise LookupError("联系人不存在")
                relation = db.scalar(select(FinancingApplicationContact).where(
                    FinancingApplicationContact.application_id == application_id,
                    FinancingApplicationContact.contact_id == contact_id))
                if relation is None: raise FinancingCommunicationError("联系人尚未关联当前融资申请")
                if communication_side in {"customer", "institution"} and contact.contact_type != communication_side:
                    raise FinancingCommunicationError("沟通对象与联系人类型不一致")
            row = FinancingCommunicationRecord(communication_id=communication_id,
                application_id=application_id, customer_id=app.customer_id, contact_id=contact_id,
                communication_side=communication_side, channel=channel, direction=direction,
                feedback_tag=feedback_tag, subject=subject.strip(), content=content.strip(),
                occurred_at=occurred_at or _now(), operator_id=actor_id, operator_name=actor_name,
                related_task_id=related_task_id, related_supplement_id=related_supplement_id,
                related_review_feedback_id=related_review_feedback_id,
                related_approval_record_id=related_approval_record_id,
                follow_up_required=int(follow_up_required), next_follow_up_at=next_follow_up_at,
                outcome=outcome, internal_note=internal_note, status="active")
            db.add(row)
            if follow_up_required:
                db.add(FinancingFollowUp(follow_up_id=uuid.uuid4().hex, customer_id=app.customer_id,
                    application_id=application_id, communication_record_id=communication_id,
                    related_task_id=related_task_id,
                    follow_up_type=communication_side if communication_side in {"customer", "institution", "internal"} else "internal",
                    title=subject.strip(), description=content.strip(), assignee_user_id=actor_id,
                    assignee_user_name=actor_name, due_at=next_follow_up_at, status="pending",
                    priority="normal"))
            self._event(db, application_id, "communication_recorded", actor_id, actor_name,
                        {"communication_id": communication_id, "side": communication_side,
                         "follow_up_required": follow_up_required})
        return self.get_communication(communication_id, include_internal=True)

    def get_communication(self, communication_id: str, *, include_internal: bool) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.communication_id == communication_id))
            if row is None: raise LookupError("沟通记录不存在")
            contact = db.scalar(select(FinancingContact).where(FinancingContact.contact_id == row.contact_id)) if row.contact_id else None
            return self._communication_dict(row, contact, include_internal)

    @staticmethod
    def _communication_dict(row: FinancingCommunicationRecord, contact: FinancingContact | None,
                            include_internal: bool) -> dict[str, Any]:
        return {"communication_id": row.communication_id, "application_id": row.application_id,
                "customer_id": row.customer_id, "contact_id": row.contact_id,
                "contact_name": contact.name if contact else None,
                "contact_organization": (contact.institution_name or contact.customer_id) if contact else None,
                "communication_side": row.communication_side, "channel": row.channel,
                "direction": row.direction, "feedback_tag": row.feedback_tag,
                "subject": row.subject, "content": row.content,
                "occurred_at": row.occurred_at.isoformat(), "operator_id": row.operator_id,
                "operator_name": row.operator_name, "related_task_id": row.related_task_id,
                "related_supplement_id": row.related_supplement_id,
                "related_review_feedback_id": row.related_review_feedback_id,
                "related_approval_record_id": row.related_approval_record_id,
                "follow_up_required": bool(row.follow_up_required),
                "next_follow_up_at": row.next_follow_up_at.isoformat() if row.next_follow_up_at else None,
                "outcome": row.outcome, "internal_note": row.internal_note if include_internal else "",
                "status": row.status, "voided_by": row.voided_by,
                "voided_at": row.voided_at.isoformat() if row.voided_at else None,
                "void_reason": row.void_reason if include_internal else ""}

    def list_communications(self, application_id: str, *, side: str | None = None,
                            include_internal: bool = False) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            self._application(db, application_id)
            statement = select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.application_id == application_id)
            if side: statement = statement.where(FinancingCommunicationRecord.communication_side == side)
            rows = list(db.scalars(statement.order_by(FinancingCommunicationRecord.occurred_at.desc(),
                                                       FinancingCommunicationRecord.id.desc())))
            contacts = {row.contact_id: db.scalar(select(FinancingContact).where(
                FinancingContact.contact_id == row.contact_id)) for row in rows if row.contact_id}
        return [self._communication_dict(row, contacts.get(row.contact_id), include_internal) for row in rows
                if include_internal or row.communication_side != "internal"]

    def void_communication(self, communication_id: str, *, reason: str, actor_id: str,
                           actor_name: str) -> dict[str, Any]:
        if not reason.strip(): raise FinancingCommunicationError("请填写作废原因")
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.communication_id == communication_id))
            if row is None: raise LookupError("沟通记录不存在")
            if row.status == "voided": return self._communication_dict(row, None, True)
            row.status = "voided"; row.voided_by = actor_id; row.voided_at = _now(); row.void_reason = reason
            self._event(db, row.application_id, "communication_voided", actor_id, actor_name,
                        {"communication_id": communication_id, "reason": reason})
        return self.get_communication(communication_id, include_internal=True)

    def create_task_from_communication(self, communication_id: str, *, title: str | None,
                                       assignee_user_id: str | None, assignee_user_name: str | None,
                                       due_date, priority: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if priority not in PRIORITIES: raise FinancingCommunicationError("任务优先级不合法")
        self._prepare(); task_id = uuid.uuid4().hex; application_id = ""
        with self.session_factory.begin() as db:
            record = db.scalar(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.communication_id == communication_id))
            if record is None or record.status == "voided": raise LookupError("可用沟通记录不存在")
            if record.related_task_id:
                raise FinancingCommunicationError("该沟通记录已创建执行任务")
            app = self._application(db, record.application_id)
            application_id = app.application_id
            stage = db.scalar(select(FinancingApplicationStage).where(
                FinancingApplicationStage.application_id == app.application_id,
                FinancingApplicationStage.stage_code == app.current_stage_code))
            if stage is None: raise FinancingCommunicationError("当前申请阶段不存在")
            db.add(FinancingApplicationTask(task_id=task_id, application_id=app.application_id,
                stage_id=stage.stage_id, task_type="communication", title=title or record.subject,
                description=record.content, status="todo", priority=priority,
                assignee_user_id=assignee_user_id, assignee_user_name=assignee_user_name,
                due_date=due_date, source_type="communication", source_ref=communication_id,
                required=1))
            record.related_task_id = task_id
            self._event(db, app.application_id, "task_created", actor_id, actor_name,
                        {"task_id": task_id, "source": "communication", "communication_id": communication_id})
        return self.applications.get_application(application_id)

    def create_supplement_from_communication(self, communication_id: str, *, required_materials: list[str],
                                             due_date, actor_id: str, actor_name: str) -> dict[str, Any]:
        record = self.get_communication(communication_id, include_internal=True)
        if record["communication_side"] != "institution":
            raise FinancingCommunicationError("只有金融机构沟通可转为补件要求")
        if record["related_supplement_id"]:
            raise FinancingCommunicationError("该沟通记录已关联补件要求")
        result = self.applications.request_supplement(record["application_id"], description=record["content"],
            required_materials=required_materials, due_date=due_date, actor_id=actor_id,
            actor_name=actor_name, requested_by_bank=record.get("contact_name"), notes=record["subject"])
        supplement_id = result["supplements"][-1]["supplement_id"]
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.communication_id == communication_id))
            row.related_supplement_id = supplement_id
        return result

    def create_review_feedback_from_communication(self, communication_id: str, *, feedback_type: str,
                                                  actor_id: str, actor_name: str) -> dict[str, Any]:
        record = self.get_communication(communication_id, include_internal=True)
        if record["communication_side"] != "institution":
            raise FinancingCommunicationError("只有金融机构沟通可转为审批反馈")
        if record["related_review_feedback_id"]:
            raise FinancingCommunicationError("该沟通记录已记录为审批反馈")
        result = self.applications.add_review_feedback(record["application_id"], feedback_type=feedback_type,
            content=record["content"], feedback_date=datetime.fromisoformat(record["occurred_at"]),
            institution_contact=record.get("contact_name"), actor_id=actor_id, actor_name=actor_name)
        feedback_id = result["review_feedback"][-1]["feedback_id"]
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.communication_id == communication_id))
            row.related_review_feedback_id = feedback_id
        return result

    def create_follow_up(self, application_id: str, *, communication_record_id: str | None,
                         related_task_id: str | None, follow_up_type: str, title: str,
                         description: str, assignee_user_id: str | None, assignee_user_name: str | None,
                         due_at: datetime, priority: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if follow_up_type not in FOLLOW_UP_TYPES or priority not in PRIORITIES:
            raise FinancingCommunicationError("跟进类型或优先级不合法")
        self._prepare(); follow_up_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            db.add(FinancingFollowUp(follow_up_id=follow_up_id, customer_id=app.customer_id,
                application_id=application_id, communication_record_id=communication_record_id,
                related_task_id=related_task_id, follow_up_type=follow_up_type, title=title,
                description=description, assignee_user_id=assignee_user_id,
                assignee_user_name=assignee_user_name, due_at=due_at, status="pending",
                priority=priority))
            self._event(db, application_id, "follow_up_created", actor_id, actor_name,
                        {"follow_up_id": follow_up_id, "due_at": due_at.isoformat()})
        return self.get_follow_up(follow_up_id)

    def get_follow_up(self, follow_up_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingFollowUp).where(FinancingFollowUp.follow_up_id == follow_up_id))
            if row is None: raise LookupError("跟进事项不存在")
            return self._follow_up_dict(row)

    @staticmethod
    def _follow_up_dict(row: FinancingFollowUp) -> dict[str, Any]:
        overdue = row.due_at < _now() and row.status not in {"completed", "cancelled"}
        return {"follow_up_id": row.follow_up_id, "customer_id": row.customer_id,
                "application_id": row.application_id, "communication_record_id": row.communication_record_id,
                "related_task_id": row.related_task_id, "follow_up_type": row.follow_up_type,
                "title": row.title, "description": row.description,
                "assignee_user_id": row.assignee_user_id, "assignee_user_name": row.assignee_user_name,
                "due_at": row.due_at.isoformat(), "status": row.status, "priority": row.priority,
                "overdue": overdue, "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                "completed_by": row.completed_by, "result": row.result}

    def list_follow_ups(self, *, application_id: str | None = None, customer_id: str | None = None,
                        assignee_user_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingFollowUp)
            if application_id: statement = statement.where(FinancingFollowUp.application_id == application_id)
            if customer_id: statement = statement.where(FinancingFollowUp.customer_id == customer_id)
            if assignee_user_id: statement = statement.where(FinancingFollowUp.assignee_user_id == assignee_user_id)
            if status: statement = statement.where(FinancingFollowUp.status == status)
            rows = list(db.scalars(statement.order_by(FinancingFollowUp.due_at, FinancingFollowUp.id)))
        return [self._follow_up_dict(row) for row in rows]

    def update_follow_up(self, follow_up_id: str, *, status: str | None, due_at: datetime | None,
                         priority: str | None, result: str | None, actor_id: str,
                         actor_name: str) -> dict[str, Any]:
        if status and status not in FOLLOW_UP_STATUSES: raise FinancingCommunicationError("跟进状态不合法")
        if priority and priority not in PRIORITIES: raise FinancingCommunicationError("跟进优先级不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingFollowUp).where(FinancingFollowUp.follow_up_id == follow_up_id))
            if row is None: raise LookupError("跟进事项不存在")
            if status: row.status = status
            if due_at: row.due_at = due_at
            if priority: row.priority = priority
            if result is not None: row.result = result
            if status == "completed":
                row.completed_at = _now(); row.completed_by = actor_id
                if row.communication_record_id:
                    communication = db.scalar(select(FinancingCommunicationRecord).where(
                        FinancingCommunicationRecord.communication_id == row.communication_record_id,
                        FinancingCommunicationRecord.status == "active"))
                    if communication is not None: communication.outcome = "resolved"
            self._event(db, row.application_id, "follow_up_updated", actor_id, actor_name,
                        {"follow_up_id": follow_up_id, "status": row.status})
        return self.get_follow_up(follow_up_id)

    def unified_timeline(self, application_id: str, *, include_internal: bool) -> list[dict[str, Any]]:
        self._prepare(); items: list[dict[str, Any]] = []
        with self.session_factory() as db:
            self._application(db, application_id)
            events = list(db.scalars(select(FinancingApplicationEvent).where(
                FinancingApplicationEvent.application_id == application_id)))
            communications = list(db.scalars(select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.application_id == application_id)))
            supplements = list(db.scalars(select(FinancingSupplementRequest).where(
                FinancingSupplementRequest.application_id == application_id)))
            feedback = list(db.scalars(select(FinancingReviewFeedback).where(
                FinancingReviewFeedback.application_id == application_id)))
            approvals = list(db.scalars(select(FinancingApprovalRecord).where(
                FinancingApprovalRecord.application_id == application_id)))
            disbursements = list(db.scalars(select(FinancingDisbursementRecord).where(
                FinancingDisbursementRecord.application_id == application_id)))
            for row in events: items.append({"type": "application_event", "id": row.event_id,
                "occurred_at": row.created_at.isoformat() if row.created_at else "", "title": row.event_type,
                "description": _load(row.payload_json), "operator_name": row.operator_name})
            for row in communications:
                if include_internal or row.communication_side != "internal":
                    items.append({"type": "communication", "id": row.communication_id,
                        "occurred_at": row.occurred_at.isoformat(), "title": row.subject,
                        "description": row.content, "operator_name": row.operator_name, "status": row.status})
            for row in supplements: items.append({"type": "supplement", "id": row.supplement_id,
                "occurred_at": row.requested_at.isoformat(), "title": "补件要求", "description": row.description})
            for row in feedback: items.append({"type": "review_feedback", "id": row.feedback_id,
                "occurred_at": row.feedback_date.isoformat(), "title": "审批反馈", "description": row.content})
            for row in approvals: items.append({"type": "approval", "id": row.approval_record_id,
                "occurred_at": row.approval_date.isoformat(), "title": "批复记录", "description": row.notes})
            for row in disbursements: items.append({"type": "disbursement", "id": row.disbursement_id,
                "occurred_at": row.disbursed_at.isoformat(), "title": "放款记录", "description": str(row.amount)})
        return sorted(items, key=lambda row: (row["occurred_at"], row["id"]), reverse=True)

    def follow_up_metrics(self, application_ids: list[str] | None = None) -> dict[str, Any]:
        rows = self.list_follow_ups()
        if application_ids is not None: rows = [row for row in rows if row["application_id"] in set(application_ids)]
        now = _now(); today = now.date()
        return {"today_count": sum(datetime.fromisoformat(row["due_at"]).date() == today and row["status"] not in {"completed", "cancelled"} for row in rows),
                "overdue_count": sum(row["overdue"] for row in rows),
                "waiting_customer_count": self._waiting_outcomes(application_ids, "waiting_customer"),
                "waiting_institution_count": self._waiting_outcomes(application_ids, "waiting_institution"),
                "items": rows}

    def _waiting_outcomes(self, application_ids: list[str] | None, outcome: str) -> int:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingCommunicationRecord).where(
                FinancingCommunicationRecord.status == "active", FinancingCommunicationRecord.outcome == outcome)
            if application_ids is not None: statement = statement.where(FinancingCommunicationRecord.application_id.in_(application_ids))
            return len(list(db.scalars(statement)))
