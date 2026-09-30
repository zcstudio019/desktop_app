"""Deterministic financing application execution workflow.

Every application is derived from one item of a confirmed plan version. State
changes are explicit user actions and are recorded as immutable events.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    Customer, FinancingApplication, FinancingApplicationEvent, FinancingApplicationMaterial,
    FinancingApplicationPackage, FinancingApplicationPackageItem,
    FinancingApplicationStage, FinancingApplicationTask, FinancingApprovalCondition,
    FinancingApprovalRecord, FinancingDisbursementRecord, FinancingPlan, FinancingPlanCondition,
    FinancingApplicationContact, FinancingCommunicationRecord, FinancingContact, FinancingFollowUp,
    FinancingPlanItem, FinancingPlanMaterial, FinancingPlanVersion,
    FinancingMaterialEvent, FinancingReviewFeedback, FinancingSubmissionPackage,
    FinancingSupplementPackage, FinancingSupplementPackageItem, FinancingSupplementRequest,
)
from backend.services.application_material_service import infer_material_type
from backend.services.financing_application_material_schema import ensure_application_material_schema


APPLICATION_STATUSES = {
    "draft", "preparing", "ready_to_submit", "submitted", "supplement_required",
    "under_review", "approved", "rejected", "partially_approved", "disbursing",
    "disbursed", "cancelled", "closed",
}
TERMINAL_STATUSES = {"rejected", "cancelled", "closed"}
ACTIVE_STATUSES = APPLICATION_STATUSES - TERMINAL_STATUSES
STAGES = [
    ("01_material_preparation", "材料准备"), ("02_submission", "银行进件"),
    ("03_supplement", "补件"), ("04_bank_review", "银行审批"),
    ("05_approval", "批复确认"), ("06_disbursement", "放款"),
    ("07_result_feedback", "结果反馈"),
]


class FinancingApplicationError(ValueError):
    pass


class ApplicationStateMachine:
    transitions = {
        "draft": {"preparing", "cancelled"},
        "preparing": {"ready_to_submit", "cancelled"},
        "ready_to_submit": {"submitted", "preparing", "cancelled"},
        "submitted": {"supplement_required", "under_review", "cancelled"},
        "supplement_required": {"under_review", "submitted", "cancelled"},
        "under_review": {"supplement_required", "approved", "partially_approved", "rejected", "cancelled"},
        "approved": {"disbursing", "cancelled"},
        "partially_approved": {"disbursing", "cancelled"},
        "disbursing": {"disbursed", "cancelled"},
        "disbursed": {"closed"},
        "rejected": {"closed"},
        "cancelled": {"closed"},
        "closed": set(),
    }

    @classmethod
    def validate(cls, current: str, target: str) -> None:
        if target not in cls.transitions.get(current, set()):
            raise FinancingApplicationError(f"不允许从{current}直接变更为{target}")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _load(value: str | None, fallback):
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, type(fallback)) else fallback
    except (TypeError, ValueError):
        return fallback


class FinancingApplicationService:
    def __init__(self, session_factory=SessionLocal, *, ensure_schema: bool = True):
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema

    def _prepare(self) -> None:
        if not self.ensure_schema:
            return
        bind = self.session_factory.kw.get("bind")
        if bind is not None:
            ensure_application_material_schema(bind)
            Base.metadata.create_all(bind, tables=[
                FinancingApplication.__table__, FinancingApplicationStage.__table__,
                FinancingApplicationTask.__table__, FinancingApplicationEvent.__table__,
                FinancingSupplementRequest.__table__, FinancingApplicationMaterial.__table__,
                FinancingReviewFeedback.__table__, FinancingApprovalRecord.__table__,
                FinancingApprovalCondition.__table__, FinancingDisbursementRecord.__table__,
                FinancingApplicationPackage.__table__, FinancingApplicationPackageItem.__table__,
                FinancingSubmissionPackage.__table__, FinancingSupplementPackage.__table__,
                FinancingSupplementPackageItem.__table__, FinancingMaterialEvent.__table__,
                FinancingContact.__table__, FinancingApplicationContact.__table__,
                FinancingCommunicationRecord.__table__, FinancingFollowUp.__table__,
            ])

    def plan_version_customer_id(self, plan_version_id: str) -> str:
        with self.session_factory() as db:
            version = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == plan_version_id))
            if version is None: raise LookupError("融资方案版本不存在")
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == version.financing_plan_id))
            if plan is None: raise LookupError("融资方案不存在")
            return plan.customer_id

    def requirement_customer_id(self, requirement_id: str) -> str:
        from backend.db_models import FinancingRequirement
        with self.session_factory() as db:
            value = db.scalar(select(FinancingRequirement.customer_id).where(
                FinancingRequirement.requirement_id == requirement_id))
            if value is None:
                raise LookupError("融资需求不存在")
            return value

    @staticmethod
    def _event(db, application_id: str, event_type: str, actor_id: str, actor_name: str,
               *, from_status: str | None = None, to_status: str | None = None,
               payload: dict[str, Any] | None = None) -> None:
        db.add(FinancingApplicationEvent(
            event_id=uuid.uuid4().hex, application_id=application_id, event_type=event_type,
            from_status=from_status, to_status=to_status, operator_id=actor_id,
            operator_name=actor_name, payload_json=_json(payload or {}),
        ))

    @staticmethod
    def _stage(db, application_id: str, code: str) -> FinancingApplicationStage:
        row = db.scalar(select(FinancingApplicationStage).where(
            FinancingApplicationStage.application_id == application_id,
            FinancingApplicationStage.stage_code == code,
        ))
        if row is None: raise FinancingApplicationError("申请阶段不存在")
        return row

    def _enter_stage(self, db, app: FinancingApplication, code: str, actor: str) -> None:
        now = _now()
        current = self._stage(db, app.application_id, app.current_stage_code)
        if current.stage_code != code and current.status == "in_progress":
            current.status = "completed"; current.completed_at = now; current.completed_by = actor
        target = self._stage(db, app.application_id, code)
        if target.status in {"pending", "blocked"}:
            target.status = "in_progress"; target.started_at = target.started_at or now; target.entered_by = actor
        app.current_stage_code = code

    def _transition(self, db, app: FinancingApplication, target: str, actor_id: str, actor_name: str,
                    event_type: str = "status_changed", payload: dict[str, Any] | None = None) -> None:
        ApplicationStateMachine.validate(app.status, target)
        previous = app.status; app.status = target
        self._event(db, app.application_id, event_type, actor_id, actor_name,
                    from_status=previous, to_status=target, payload=payload)

    def create_applications_from_confirmed_plan(self, plan_version_id: str, *, created_by: str,
                                                responsible_user_id: str | None = None,
                                                responsible_user_name: str | None = None) -> list[dict[str, Any]]:
        self._prepare()
        created: list[str] = []
        with self.session_factory.begin() as db:
            version = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == plan_version_id))
            if version is None: raise LookupError("融资方案版本不存在")
            if version.status != "confirmed": raise FinancingApplicationError("只有已确认方案版本可以创建融资申请")
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == version.financing_plan_id))
            if plan is None: raise LookupError("融资方案不存在")
            items = list(db.scalars(select(FinancingPlanItem).where(FinancingPlanItem.plan_version_id == plan_version_id).order_by(FinancingPlanItem.sequence_no, FinancingPlanItem.id)))
            if not items: raise FinancingApplicationError("已确认方案没有可执行产品项")
            for item in items:
                duplicate = db.scalar(select(FinancingApplication).where(
                    FinancingApplication.plan_version_id == plan_version_id,
                    FinancingApplication.plan_item_id == item.plan_item_id,
                    FinancingApplication.status.in_(ACTIVE_STATUSES),
                ))
                if duplicate: raise FinancingApplicationError(f"产品{item.product_name}已存在有效申请")
                attempt = len(list(db.scalars(select(FinancingApplication.id).where(
                    FinancingApplication.plan_version_id == plan_version_id,
                    FinancingApplication.plan_item_id == item.plan_item_id,
                )))) + 1
                app_id = uuid.uuid4().hex; now = _now()
                app = FinancingApplication(
                    application_id=app_id, attempt_no=attempt, customer_id=plan.customer_id,
                    requirement_id=plan.requirement_id, requirement_version=version.source_requirement_version,
                    plan_id=plan.financing_plan_id, plan_version_id=plan_version_id, plan_item_id=item.plan_item_id,
                    product_id=item.product_id, product_version_id=item.product_version_id,
                    external_product_code=item.external_product_code, institution_name=item.institution_name,
                    product_name=item.product_name, application_no=f"FA-{now:%Y%m%d}-{uuid.uuid4().hex[:8].upper()}",
                    status="draft", target_amount=item.proposed_amount, target_term_months=item.proposed_term_months,
                    target_interest_rate=None, responsible_user_id=responsible_user_id,
                    responsible_user_name=responsible_user_name, current_stage_code=STAGES[0][0], created_by=created_by,
                )
                db.add(app); db.flush()
                stages = []
                for sequence, (code, name) in enumerate(STAGES, 1):
                    stage = FinancingApplicationStage(
                        stage_id=uuid.uuid4().hex, application_id=app_id, stage_code=code, stage_name=name,
                        status="in_progress" if sequence == 1 else "pending", started_at=now if sequence == 1 else None,
                        entered_by=created_by if sequence == 1 else None, sequence_no=sequence,
                    )
                    db.add(stage); stages.append(stage)
                db.flush(); preparation = stages[0]
                materials = list(db.scalars(select(FinancingPlanMaterial).where(
                    FinancingPlanMaterial.plan_version_id == plan_version_id,
                    FinancingPlanMaterial.required == 1,
                )))
                conditions = list(db.scalars(select(FinancingPlanCondition).where(
                    FinancingPlanCondition.plan_version_id == plan_version_id,
                    FinancingPlanCondition.required == 1,
                )))
                for material in materials:
                    if material.plan_item_id not in {None, item.plan_item_id}: continue
                    app_material_id = uuid.uuid4().hex
                    app_status = "verified" if material.status == "verified" else ("matched" if material.status in {"available", "uploaded"} else "required_missing")
                    db.add(FinancingApplicationMaterial(
                        application_material_id=app_material_id, application_id=app_id,
                        material_type=infer_material_type(material.material_name),
                        material_name=material.material_name, material_category=material.material_category,
                        owner_type="enterprise", required=material.required, required_verified=0,
                        status=app_status, source_type="plan_material", source_id=material.material_id,
                        notes=material.notes,
                    ))
                    db.add(FinancingMaterialEvent(
                        material_event_id=uuid.uuid4().hex, application_id=app_id,
                        application_material_id=app_material_id, event_type="material_required",
                        operator_id=created_by, operator_name=created_by,
                        payload_json=_json({"source_type": "plan_material", "source_id": material.material_id}),
                    ))
                    if app_status in {"matched", "verified"}: continue
                    task = FinancingApplicationTask(
                        task_id=uuid.uuid4().hex, application_id=app_id, stage_id=preparation.stage_id,
                        task_type="material", title=f"准备：{material.material_name}", description=material.notes or "",
                        status="todo", priority="normal", source_type="plan_material", source_ref=material.material_id,
                        related_material_id=app_material_id, required=1,
                    )
                    db.add(task); db.flush()
                    self._event(db, app_id, "task_created", created_by, created_by, payload={"task_id": task.task_id, "source": "plan_material"})
                for condition in conditions:
                    if condition.plan_item_id not in {None, item.plan_item_id} or condition.status != "pending": continue
                    task = FinancingApplicationTask(
                        task_id=uuid.uuid4().hex, application_id=app_id, stage_id=preparation.stage_id,
                        task_type="condition", title=f"处理条件：{condition.title}", description=condition.description,
                        status="todo", priority="normal", source_type="plan_condition", source_ref=condition.condition_id,
                        related_condition_id=condition.condition_id, required=1,
                    )
                    db.add(task); db.flush()
                    self._event(db, app_id, "task_created", created_by, created_by, payload={"task_id": task.task_id, "source": "plan_condition"})
                self._event(db, app_id, "application_created", created_by, created_by, to_status="draft", payload={"plan_item_id": item.plan_item_id})
                created.append(app_id)
        return [self.get_application(value) for value in created]

    def retry_application(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            parent = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == application_id))
            if parent is None: raise LookupError("融资申请不存在")
            if parent.status not in TERMINAL_STATUSES: raise FinancingApplicationError("只有已拒绝、已取消或已关闭申请可以重试")
            duplicate = db.scalar(select(FinancingApplication).where(
                FinancingApplication.plan_version_id == parent.plan_version_id,
                FinancingApplication.plan_item_id == parent.plan_item_id,
                FinancingApplication.status.in_(ACTIVE_STATUSES),
            ))
            if duplicate: raise FinancingApplicationError("该方案产品已有有效重试申请")
            new_id = uuid.uuid4().hex; now = _now()
            row = FinancingApplication(
                application_id=new_id, parent_application_id=parent.application_id, attempt_no=parent.attempt_no + 1,
                customer_id=parent.customer_id, requirement_id=parent.requirement_id, requirement_version=parent.requirement_version,
                plan_id=parent.plan_id, plan_version_id=parent.plan_version_id, plan_item_id=parent.plan_item_id,
                product_id=parent.product_id, product_version_id=parent.product_version_id,
                external_product_code=parent.external_product_code, institution_name=parent.institution_name,
                product_name=parent.product_name, application_no=f"FA-{now:%Y%m%d}-{uuid.uuid4().hex[:8].upper()}",
                status="draft", target_amount=parent.target_amount, target_term_months=parent.target_term_months,
                target_interest_rate=parent.target_interest_rate, responsible_user_id=parent.responsible_user_id,
                responsible_user_name=parent.responsible_user_name, current_stage_code=STAGES[0][0], created_by=actor_id,
            )
            db.add(row); db.flush()
            for sequence, (code, name) in enumerate(STAGES, 1):
                db.add(FinancingApplicationStage(stage_id=uuid.uuid4().hex, application_id=new_id, stage_code=code,
                    stage_name=name, status="in_progress" if sequence == 1 else "pending", started_at=now if sequence == 1 else None,
                    entered_by=actor_id if sequence == 1 else None, sequence_no=sequence))
            self._event(db, new_id, "application_created", actor_id, actor_name, to_status="draft", payload={"parent_application_id": parent.application_id, "attempt_no": row.attempt_no})
        return self.get_application(new_id)

    def start_preparation(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        return self._simple_transition(application_id, "preparing", actor_id, actor_name)

    def mark_ready_to_submit(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.status == "draft": self._transition(db, app, "preparing", actor_id, actor_name)
            self._validate_ready(db, app)
            self._transition(db, app, "ready_to_submit", actor_id, actor_name)
            self._enter_stage(db, app, "02_submission", actor_id)
        return self.get_application(application_id)

    def _validate_ready(self, db, app: FinancingApplication) -> None:
        tasks = list(db.scalars(select(FinancingApplicationTask).where(
            FinancingApplicationTask.application_id == app.application_id, FinancingApplicationTask.required == 1,
        )))
        completed_conditions = {x.related_condition_id for x in tasks if x.status == "done" and x.related_condition_id}
        materials = list(db.scalars(select(FinancingApplicationMaterial).where(
            FinancingApplicationMaterial.application_id == app.application_id,
            FinancingApplicationMaterial.required == 1,
            FinancingApplicationMaterial.status != "rejected",
        )))
        if any(x.status not in {"matched", "uploaded", "verified"}
               or (x.required_verified and x.status != "verified") for x in materials):
            raise FinancingApplicationError("仍有必需材料未准备完成")
        conditions = list(db.scalars(select(FinancingPlanCondition).where(
            FinancingPlanCondition.plan_version_id == app.plan_version_id, FinancingPlanCondition.required == 1,
        )))
        if any(x.plan_item_id in {None, app.plan_item_id} and x.status not in {"satisfied", "waived", "not_applicable"}
               and x.condition_id not in completed_conditions for x in conditions):
            raise FinancingApplicationError("仍有必需条件未处理")
        if any(x.status != "done" for x in tasks): raise FinancingApplicationError("仍有必需任务未完成")

    def submit_application(self, application_id: str, *, submitted_amount: Decimal | None,
                           submission_channel: str | None, submission_reference: str | None,
                           submission_notes: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            amount = submitted_amount if submitted_amount is not None else Decimal(app.target_amount)
            if amount <= 0: raise FinancingApplicationError("提交金额必须大于0")
            self._transition(db, app, "submitted", actor_id, actor_name, "submitted", {"submitted_amount": str(amount)})
            app.submitted_amount = amount; app.submitted_at = _now(); app.submission_channel = submission_channel
            app.submission_reference = submission_reference; app.submission_notes = submission_notes
            self._enter_stage(db, app, "02_submission", actor_id)
        return self.get_application(application_id)

    def request_supplement(self, application_id: str, *, description: str, required_materials: list[str],
                           due_date: date | None, actor_id: str, actor_name: str,
                           requested_by_bank: str | None = None, notes: str = "") -> dict[str, Any]:
        if not description.strip() and not required_materials: raise FinancingApplicationError("补件说明或材料不能为空")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            supplement_id = uuid.uuid4().hex
            supplement = FinancingSupplementRequest(
                supplement_id=supplement_id, application_id=application_id,
                request_no=f"SUP-{_now():%Y%m%d}-{uuid.uuid4().hex[:8].upper()}",
                description=description, requested_by_bank=requested_by_bank,
                requested_at=_now(), due_date=due_date, status="pending", notes=notes,
            )
            db.add(supplement); db.flush()
            self._transition(db, app, "supplement_required", actor_id, actor_name, "supplement_requested", {
                "supplement_id": supplement_id, "description": description,
                "required_materials": required_materials, "bank_notes": notes,
            })
            self._enter_stage(db, app, "03_supplement", actor_id); stage = self._stage(db, application_id, "03_supplement")
            for name in required_materials or [description]:
                material_id = uuid.uuid4().hex
                db.add(FinancingApplicationMaterial(
                    application_material_id=material_id, application_id=application_id,
                    supplement_request_id=supplement_id, material_type=infer_material_type(name),
                    material_name=name, material_category="supplement", owner_type="enterprise",
                    required=1, required_verified=0, status="required_missing",
                    source_type="supplement_request", source_id=supplement_id, notes=notes,
                ))
                task = FinancingApplicationTask(task_id=uuid.uuid4().hex, application_id=application_id,
                    stage_id=stage.stage_id, task_type="supplement", title=f"补充：{name}", description=description,
                    status="todo", priority="high", due_date=due_date, source_type="supplement_request",
                    source_ref=supplement_id, related_material_id=material_id, required=1)
                db.add(task)
                db.add(FinancingMaterialEvent(material_event_id=uuid.uuid4().hex,
                    application_id=application_id, application_material_id=material_id,
                    event_type="material_required", operator_id=actor_id, operator_name=actor_name,
                    payload_json=_json({"source_type": "supplement_request", "supplement_id": supplement_id})))
                db.flush(); self._event(db, application_id, "task_created", actor_id, actor_name, payload={"task_id": task.task_id, "source": "supplement_request"})
        return self.get_application(application_id)

    def update_application_material(self, application_id: str, material_id: str, *, status: str,
                                    file_reference: str | None, notes: str | None,
                                    actor_id: str, actor_name: str) -> dict[str, Any]:
        if status not in {"required_missing", "matched", "uploaded", "verified", "expired", "needs_review", "rejected", "not_applicable", "missing", "available"}:
            raise FinancingApplicationError("申请材料状态不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.status == "closed":
                raise FinancingApplicationError("申请已关闭，不能修改申请材料")
            row = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            ))
            if row is None: raise LookupError("申请材料不存在")
            old = row.status; row.status = status
            if file_reference is not None: row.file_reference = file_reference
            if notes is not None: row.notes = notes
            if status == "verified": row.verified_by = actor_id; row.verified_at = _now()
            event_type = {"verified": "material_verified", "rejected": "material_rejected",
                          "uploaded": "material_uploaded", "matched": "material_matched"}.get(status, "material_status_changed")
            db.add(FinancingMaterialEvent(material_event_id=uuid.uuid4().hex,
                application_id=application_id, application_material_id=material_id,
                event_type=event_type, operator_id=actor_id, operator_name=actor_name,
                payload_json=_json({"from": old, "to": status, "file_reference": file_reference})))
            self._event(db, application_id, "application_material_updated", actor_id, actor_name,
                        payload={"material_id": material_id, "from": old, "to": status})
        return self.get_application(application_id)

    def complete_supplement(self, application_id: str, supplement_id: str, *,
                            actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            self._get(db, application_id)
            row = db.scalar(select(FinancingSupplementRequest).where(
                FinancingSupplementRequest.application_id == application_id,
                FinancingSupplementRequest.supplement_id == supplement_id,
            ))
            if row is None: raise LookupError("补件要求不存在")
            materials = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.supplement_request_id == supplement_id,
                FinancingApplicationMaterial.required == 1,
            )))
            if any(value.status not in {"matched", "available", "uploaded", "verified"} for value in materials):
                raise FinancingApplicationError("补件材料尚未齐备")
            tasks = list(db.scalars(select(FinancingApplicationTask).where(
                FinancingApplicationTask.application_id == application_id,
                FinancingApplicationTask.source_type.in_({"supplement_request", "bank_supplement"}),
                FinancingApplicationTask.source_ref == supplement_id,
                FinancingApplicationTask.required == 1,
            )))
            if any(value.status != "done" for value in tasks):
                raise FinancingApplicationError("补件任务尚未完成")
            row.status = "completed"; row.completed_at = _now(); row.completed_by = actor_id
            self._event(db, application_id, "supplement_completed", actor_id, actor_name,
                        payload={"supplement_id": supplement_id})
        return self.get_application(application_id)

    def mark_under_review(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.status == "supplement_required":
                pending = db.scalar(select(func.count()).select_from(FinancingSupplementRequest).where(
                    FinancingSupplementRequest.application_id == application_id,
                    FinancingSupplementRequest.status.in_(["pending", "in_progress"]),
                ))
                if pending: raise FinancingApplicationError("请先完成所有补件要求")
            self._transition(db, app, "under_review", actor_id, actor_name)
            self._enter_stage(db, app, "04_bank_review", actor_id)
        return self.get_application(application_id)

    def add_review_feedback(self, application_id: str, *, feedback_type: str, content: str,
                            feedback_date: datetime | None, institution_contact: str | None,
                            actor_id: str, actor_name: str) -> dict[str, Any]:
        allowed = {"general", "risk_question", "material_question", "credit_question", "financial_question", "approval_progress", "other"}
        if feedback_type not in allowed: raise FinancingApplicationError("审批反馈类型不合法")
        if not content.strip(): raise FinancingApplicationError("审批反馈内容不能为空")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.status == "closed":
                raise FinancingApplicationError("申请已关闭，不能新增审批反馈")
            row = FinancingReviewFeedback(
                feedback_id=uuid.uuid4().hex, application_id=application_id,
                feedback_type=feedback_type, feedback_date=feedback_date or _now(),
                institution_contact=institution_contact, content=content,
                related_stage=app.current_stage_code, created_by=actor_id,
            )
            db.add(row); db.flush()
            self._event(db, application_id, "review_feedback_added", actor_id, actor_name,
                        payload={"feedback_id": row.feedback_id, "feedback_type": feedback_type, "content": content})
        return self.get_application(application_id)

    def record_approval(self, application_id: str, *, approved_amount: Decimal,
                        approved_term_months: int, approved_interest_rate: Decimal | None,
                        approval_reference: str | None, approved_at: datetime | None,
                        notes: str, actor_id: str, actor_name: str,
                        guarantee_method: str | None = None, repayment_method: str | None = None,
                        approval_expiry_date: date | None = None,
                        conditions: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.submitted_amount is None: raise FinancingApplicationError("申请尚未记录提交金额")
            if approved_amount <= 0 or approved_amount > Decimal(app.submitted_amount): raise FinancingApplicationError("批复金额必须大于0且不能超过提交金额")
            if approved_term_months <= 0: raise FinancingApplicationError("批复期限必须大于0")
            target = "approved" if approved_amount == Decimal(app.submitted_amount) else "partially_approved"
            self._transition(db, app, target, actor_id, actor_name, "approval_recorded", {"approved_amount": str(approved_amount), "notes": notes})
            app.approved_amount = approved_amount; app.approved_term_months = approved_term_months
            app.approved_interest_rate = approved_interest_rate; app.approval_reference = approval_reference
            app.approved_at = approved_at or _now(); self._enter_stage(db, app, "05_approval", actor_id)
            approval_id = uuid.uuid4().hex
            approval_status = "full_approval" if target == "approved" else "partial_approval"
            db.add(FinancingApprovalRecord(
                approval_record_id=approval_id, application_id=application_id, approval_status=approval_status,
                submitted_amount=app.submitted_amount, approved_amount=approved_amount,
                approved_term_months=approved_term_months, approved_interest_rate=approved_interest_rate,
                guarantee_method=guarantee_method, repayment_method=repayment_method,
                approval_reference=approval_reference, approval_date=app.approved_at,
                approval_expiry_date=approval_expiry_date, conditions_json=_json(conditions or []),
                notes=notes, created_by=actor_id,
            ))
            for value in conditions or []:
                title = str(value.get("title") or "").strip()
                if not title: raise FinancingApplicationError("批复条件标题不能为空")
                db.add(FinancingApprovalCondition(
                    approval_condition_id=uuid.uuid4().hex, approval_record_id=approval_id,
                    title=title, description=str(value.get("description") or ""),
                    status="pending", required=1 if value.get("required", True) else 0,
                ))
        return self.get_application(application_id)

    def record_rejection(self, application_id: str, *, rejection_reason: str, rejection_code: str | None,
                         rejected_at: datetime | None, notes: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if not rejection_reason.strip(): raise FinancingApplicationError("拒绝原因不能为空")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            self._transition(db, app, "rejected", actor_id, actor_name, "rejected", {"reason": rejection_reason, "notes": notes})
            app.rejection_reason = rejection_reason; app.rejection_code = rejection_code; app.rejected_at = rejected_at or _now()
            self._enter_stage(db, app, "07_result_feedback", actor_id); app.final_result = "rejected"
            db.add(FinancingApprovalRecord(
                approval_record_id=uuid.uuid4().hex, application_id=application_id,
                approval_status="rejected", submitted_amount=app.submitted_amount,
                approved_amount=None, approval_date=app.rejected_at,
                conditions_json="[]", notes=notes or rejection_reason, created_by=actor_id,
            ))
        return self.get_application(application_id)

    def update_approval_condition(self, application_id: str, condition_id: str, *, status: str,
                                  actor_id: str, actor_name: str) -> dict[str, Any]:
        if status not in {"pending", "satisfied", "waived", "not_applicable"}:
            raise FinancingApplicationError("批复条件状态不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            self._get(db, application_id)
            row = db.scalar(select(FinancingApprovalCondition).join(
                FinancingApprovalRecord,
                FinancingApprovalRecord.approval_record_id == FinancingApprovalCondition.approval_record_id,
            ).where(
                FinancingApprovalRecord.application_id == application_id,
                FinancingApprovalCondition.approval_condition_id == condition_id,
            ))
            if row is None: raise LookupError("批复条件不存在")
            old = row.status; row.status = status; row.updated_by = actor_id
            self._event(db, application_id, "approval_condition_updated", actor_id, actor_name,
                        payload={"condition_id": condition_id, "from": old, "to": status})
        return self.get_application(application_id)

    @staticmethod
    def _validate_approval_conditions(db, application_id: str) -> None:
        pending = db.scalar(select(func.count()).select_from(FinancingApprovalCondition).join(
            FinancingApprovalRecord,
            FinancingApprovalRecord.approval_record_id == FinancingApprovalCondition.approval_record_id,
        ).where(
            FinancingApprovalRecord.application_id == application_id,
            FinancingApprovalCondition.required == 1,
            FinancingApprovalCondition.status == "pending",
        ))
        if pending: raise FinancingApplicationError("仍有必需批复条件未满足，不能进入放款")

    def start_disbursing(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            self._validate_approval_conditions(db, application_id)
            self._transition(db, app, "disbursing", actor_id, actor_name)
            self._enter_stage(db, app, "06_disbursement", actor_id)
        return self.get_application(application_id)

    def record_disbursement(self, application_id: str, *, disbursed_amount: Decimal,
                            disbursed_at: datetime | None, disbursement_reference: str | None,
                            actor_id: str, actor_name: str, recipient_name: str | None = None,
                            recipient_account_masked: str | None = None, purpose: str | None = None,
                            notes: str = "") -> dict[str, Any]:
        if disbursed_amount <= 0: raise FinancingApplicationError("放款金额必须大于0")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.approved_amount is None: raise FinancingApplicationError("申请尚未批复")
            current = Decimal(app.disbursed_amount or 0)
            total = current + disbursed_amount
            if total > Decimal(app.approved_amount): raise FinancingApplicationError("累计放款金额不能超过批复金额")
            if app.status in {"approved", "partially_approved"}:
                self._validate_approval_conditions(db, application_id)
                self._transition(db, app, "disbursing", actor_id, actor_name)
            self._enter_stage(db, app, "06_disbursement", actor_id)
            when = disbursed_at or _now()
            db.add(FinancingDisbursementRecord(
                disbursement_id=uuid.uuid4().hex, application_id=application_id,
                disbursement_no=f"PAY-{when:%Y%m%d}-{uuid.uuid4().hex[:8].upper()}",
                amount=disbursed_amount, disbursed_at=when, bank_reference=disbursement_reference,
                recipient_name=recipient_name, recipient_account_masked=recipient_account_masked,
                purpose=purpose, notes=notes, created_by=actor_id,
            ))
            app.disbursed_amount = total; app.disbursed_at = when
            app.disbursement_reference = disbursement_reference
            if total == Decimal(app.approved_amount):
                app.final_result = "disbursed"
                self._transition(db, app, "disbursed", actor_id, actor_name, "disbursed", {"amount": str(disbursed_amount), "cumulative_amount": str(total)})
                self._enter_stage(db, app, "07_result_feedback", actor_id)
            else:
                self._event(db, application_id, "partial_disbursement_recorded", actor_id, actor_name,
                            payload={"amount": str(disbursed_amount), "cumulative_amount": str(total)})
        return self.get_application(application_id)

    def update_task(self, application_id: str, task_id: str, *, status: str,
                    actor_id: str, actor_name: str, assignee_user_id: str | None = None,
                    assignee_user_name: str | None = None, priority: str | None = None,
                    due_date: date | None = None, notes: str | None = None) -> dict[str, Any]:
        if status not in {"todo", "in_progress", "blocked", "done", "cancelled"}: raise FinancingApplicationError("任务状态不合法")
        if priority is not None and priority not in {"low", "normal", "high", "urgent"}: raise FinancingApplicationError("任务优先级不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            if app.status == "closed":
                raise FinancingApplicationError("申请已关闭，不能修改执行任务")
            task = db.scalar(select(FinancingApplicationTask).where(FinancingApplicationTask.application_id == application_id, FinancingApplicationTask.task_id == task_id))
            if task is None: raise LookupError("申请任务不存在")
            previous = task.status; task.status = status
            if assignee_user_id is not None: task.assignee_user_id = assignee_user_id
            if assignee_user_name is not None: task.assignee_user_name = assignee_user_name
            if priority is not None: task.priority = priority
            if due_date is not None: task.due_date = due_date
            if notes is not None: task.notes = notes
            if status == "done": task.completed_at = _now(); task.completed_by = actor_id
            if status == "done" and task.source_type == "communication" and task.source_ref:
                communication = db.scalar(select(FinancingCommunicationRecord).where(
                    FinancingCommunicationRecord.communication_id == task.source_ref,
                    FinancingCommunicationRecord.status == "active"))
                if communication is not None: communication.outcome = "resolved"
            self._event(db, application_id, "task_completed" if status == "done" else "task_status_changed", actor_id, actor_name, payload={"task_id": task_id, "from": previous, "to": status})
        return self.get_application(application_id)

    def complete_task(self, application_id: str, task_id: str, *, notes: str,
                      actor_id: str, actor_name: str) -> dict[str, Any]:
        return self.update_task(application_id, task_id, status="done", notes=notes,
                                actor_id=actor_id, actor_name=actor_name)

    def block_task(self, application_id: str, task_id: str, *, reason: str,
                   actor_id: str, actor_name: str) -> dict[str, Any]:
        if not reason.strip(): raise FinancingApplicationError("阻塞原因不能为空")
        self.update_task(application_id, task_id, status="blocked", notes=reason,
                         actor_id=actor_id, actor_name=actor_name)
        with self.session_factory.begin() as db:
            task = db.scalar(select(FinancingApplicationTask).where(FinancingApplicationTask.task_id == task_id))
            stage = db.scalar(select(FinancingApplicationStage).where(FinancingApplicationStage.stage_id == task.stage_id)) if task else None
            if stage and stage.status == "in_progress": stage.status = "blocked"; stage.notes = reason
        return self.get_application(application_id)

    def unblock_task(self, application_id: str, task_id: str, *, reason: str,
                     target_status: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if target_status not in {"todo", "in_progress"}: raise FinancingApplicationError("解除阻塞后状态不合法")
        self._prepare()
        with self.session_factory() as db:
            task = db.scalar(select(FinancingApplicationTask).where(
                FinancingApplicationTask.application_id == application_id,
                FinancingApplicationTask.task_id == task_id,
            ))
            if task is None: raise LookupError("申请任务不存在")
            if task.status != "blocked": raise FinancingApplicationError("只能解除已阻塞任务")
        self.update_task(application_id, task_id, status=target_status, notes=reason,
                         actor_id=actor_id, actor_name=actor_name)
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            task = db.scalar(select(FinancingApplicationTask).where(FinancingApplicationTask.task_id == task_id))
            stage = db.scalar(select(FinancingApplicationStage).where(FinancingApplicationStage.stage_id == task.stage_id)) if task else None
            if stage and stage.status == "blocked" and stage.stage_code == app.current_stage_code:
                stage.status = "in_progress"; stage.notes = reason
        return self.get_application(application_id)

    def advance_application_stage(self, application_id: str, target_stage_code: str, *,
                                  actor_id: str, actor_name: str) -> dict[str, Any]:
        current = self.get_application(application_id)
        if target_stage_code == "02_submission":
            return self.mark_ready_to_submit(application_id, actor_id=actor_id, actor_name=actor_name)
        if target_stage_code == "04_bank_review" and current["status"] in {"submitted", "supplement_required"}:
            return self.mark_under_review(application_id, actor_id=actor_id, actor_name=actor_name)
        if target_stage_code == "06_disbursement" and current["status"] in {"approved", "partially_approved"}:
            return self.start_disbursing(application_id, actor_id=actor_id, actor_name=actor_name)
        if target_stage_code == "07_result_feedback" and current["status"] == "disbursed":
            return self._close_application(application_id, actor_id=actor_id, actor_name=actor_name)
        raise FinancingApplicationError("当前状态不允许推进到目标阶段，请使用对应业务操作")

    def _close_application(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id)
            self._transition(db, app, "closed", actor_id, actor_name)
            app.closed_at = _now(); self._enter_stage(db, app, "07_result_feedback", actor_id)
        return self.get_application(application_id)

    def _simple_transition(self, application_id: str, target: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get(db, application_id); self._transition(db, app, target, actor_id, actor_name)
        return self.get_application(application_id)

    @staticmethod
    def _get(db, application_id: str) -> FinancingApplication:
        app = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == application_id))
        if app is None: raise LookupError("融资申请不存在")
        return app

    def task_application_id(self, task_id: str) -> str:
        self._prepare()
        with self.session_factory() as db:
            value = db.scalar(select(FinancingApplicationTask.application_id).where(FinancingApplicationTask.task_id == task_id))
            if value is None: raise LookupError("申请任务不存在")
            return value

    def list_applications(self, customer_id: str, requirement_id: str | None = None) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingApplication.application_id).where(FinancingApplication.customer_id == customer_id)
            if requirement_id: statement = statement.where(FinancingApplication.requirement_id == requirement_id)
            ids = list(db.scalars(statement.order_by(FinancingApplication.created_at.desc(), FinancingApplication.id.desc())))
        return [self.get_application(value) for value in ids]

    def list_all_applications(self, *, customer_id: str | None = None, institution_name: str | None = None,
                              status: str | None = None, responsible_user_id: str | None = None,
                              overdue_only: bool = False, date_from: date | None = None,
                              date_to: date | None = None) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingApplication.application_id)
            if customer_id: statement = statement.where(FinancingApplication.customer_id == customer_id)
            if institution_name: statement = statement.where(FinancingApplication.institution_name == institution_name)
            if status: statement = statement.where(FinancingApplication.status == status)
            if responsible_user_id: statement = statement.where(FinancingApplication.responsible_user_id == responsible_user_id)
            if date_from: statement = statement.where(FinancingApplication.created_at >= datetime.combine(date_from, datetime.min.time()))
            if date_to: statement = statement.where(FinancingApplication.created_at <= datetime.combine(date_to, datetime.max.time()))
            ids = list(db.scalars(statement.order_by(FinancingApplication.updated_at.desc(), FinancingApplication.id.desc())))
        rows = [self.get_application(value) for value in ids]
        return [row for row in rows if not overdue_only or row["overdue_task_count"] > 0]

    def dashboard(self, **filters) -> dict[str, Any]:
        rows = self.list_all_applications(**filters)
        counts = {status: 0 for status in APPLICATION_STATUSES}
        amounts = {"target_amount": Decimal("0"), "submitted_amount": Decimal("0"),
                   "approved_amount": Decimal("0"), "disbursed_amount": Decimal("0"),
                   "pending_approval_amount": Decimal("0"), "undisbursed_amount": Decimal("0")}
        for row in rows:
            counts[row["status"]] += 1
            for key in ("target_amount", "submitted_amount", "approved_amount", "disbursed_amount"):
                amounts[key] += Decimal(row[key] or 0)
            submitted = Decimal(row["submitted_amount"] or 0)
            approved = Decimal(row["approved_amount"] or 0)
            disbursed = Decimal(row["disbursed_amount"] or 0)
            if row["status"] in {"submitted", "supplement_required", "under_review"}:
                amounts["pending_approval_amount"] += max(submitted - approved, Decimal("0"))
            amounts["undisbursed_amount"] += max(approved - disbursed, Decimal("0"))
        return {"total_applications": len(rows), "status_counts": counts,
                "amounts": {key: str(value) for key, value in amounts.items()}, "applications": rows}

    def get_application(self, application_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            app = self._get(db, application_id)
            stages = list(db.scalars(select(FinancingApplicationStage).where(FinancingApplicationStage.application_id == application_id).order_by(FinancingApplicationStage.sequence_no)))
            tasks = list(db.scalars(select(FinancingApplicationTask).where(FinancingApplicationTask.application_id == application_id).order_by(FinancingApplicationTask.created_at, FinancingApplicationTask.id)))
            events = list(db.scalars(select(FinancingApplicationEvent).where(FinancingApplicationEvent.application_id == application_id).order_by(FinancingApplicationEvent.created_at, FinancingApplicationEvent.id)))
            supplements = list(db.scalars(select(FinancingSupplementRequest).where(FinancingSupplementRequest.application_id == application_id).order_by(FinancingSupplementRequest.requested_at, FinancingSupplementRequest.id)))
            materials = list(db.scalars(select(FinancingApplicationMaterial).where(FinancingApplicationMaterial.application_id == application_id).order_by(FinancingApplicationMaterial.created_at, FinancingApplicationMaterial.id)))
            feedback = list(db.scalars(select(FinancingReviewFeedback).where(FinancingReviewFeedback.application_id == application_id).order_by(FinancingReviewFeedback.feedback_date, FinancingReviewFeedback.id)))
            approvals = list(db.scalars(select(FinancingApprovalRecord).where(FinancingApprovalRecord.application_id == application_id).order_by(FinancingApprovalRecord.approval_date, FinancingApprovalRecord.id)))
            approval_ids = [value.approval_record_id for value in approvals]
            approval_conditions = list(db.scalars(select(FinancingApprovalCondition).where(FinancingApprovalCondition.approval_record_id.in_(approval_ids)).order_by(FinancingApprovalCondition.id))) if approval_ids else []
            disbursements = list(db.scalars(select(FinancingDisbursementRecord).where(FinancingDisbursementRecord.application_id == application_id).order_by(FinancingDisbursementRecord.disbursed_at, FinancingDisbursementRecord.id)))
            follow_ups = list(db.scalars(select(FinancingFollowUp).where(
                FinancingFollowUp.application_id == application_id).order_by(FinancingFollowUp.due_at, FinancingFollowUp.id)))
            customer = db.scalar(select(Customer).where(Customer.customer_id == app.customer_id))
            today = date.today()
            active_tasks = [value for value in tasks if value.status not in {"done", "cancelled"}]
            overdue = [value for value in active_tasks if value.due_date and value.due_date < today]
            next_task = sorted(active_tasks, key=lambda value: (value.due_date is None, value.due_date or date.max, value.id))[0] if active_tasks else None
            blockers = []
            missing_material_count = sum(1 for value in materials if value.required and
                (value.status not in {"matched", "available", "uploaded", "verified"}
                 or (value.required_verified and value.status != "verified")))
            plan_material_task_count = sum(1 for value in active_tasks if value.required and value.source_type == "plan_material")
            plan_condition_task_count = sum(1 for value in active_tasks if value.required and value.source_type == "plan_condition")
            pending_condition_count = sum(1 for value in approval_conditions if value.required and value.status == "pending")
            blocked_task_count = sum(1 for value in tasks if value.status == "blocked")
            pending_supplement_count = sum(1 for value in supplements if value.status in {"pending", "in_progress"})
            if missing_material_count: blockers.append(f"{missing_material_count}个申请材料未完成")
            if plan_material_task_count: blockers.append(f"{plan_material_task_count}个方案材料任务未完成")
            if plan_condition_task_count: blockers.append(f"{plan_condition_task_count}个方案条件任务未完成")
            if pending_condition_count: blockers.append(f"{pending_condition_count}个批复条件待满足")
            if overdue: blockers.append(f"{len(overdue)}个任务已逾期")
            if blocked_task_count: blockers.append(f"{blocked_task_count}个任务已阻塞")
            if pending_supplement_count: blockers.append(f"{pending_supplement_count}个银行补件要求待完成")
            overdue_follow_ups = [value for value in follow_ups if value.due_at < _now() and value.status not in {"completed", "cancelled"}]
            if overdue_follow_ups: blockers.append(f"{len(overdue_follow_ups)}个跟进事项已逾期")
            blocked_tasks = [value for value in tasks if value.status == "blocked"]
            required_tasks = [value for value in active_tasks if value.required]
            if blocked_tasks:
                next_action = f"处理阻塞任务：{blocked_tasks[0].title}"
            elif overdue:
                next_action = f"处理逾期任务：{overdue[0].title}"
            elif overdue_follow_ups:
                next_action = f"完成逾期跟进：{overdue_follow_ups[0].title}"
            elif required_tasks:
                next_action = f"完成当前任务：{required_tasks[0].title}"
            else:
                next_action = self._next_action(app.status)
            return {
                "application_id": app.application_id, "parent_application_id": app.parent_application_id,
                "attempt_no": app.attempt_no, "customer_id": app.customer_id,
                "customer_name": customer.name if customer else app.customer_id,
                "requirement_id": app.requirement_id,
                "requirement_version": app.requirement_version, "plan_id": app.plan_id,
                "plan_version_id": app.plan_version_id, "plan_item_id": app.plan_item_id,
                "product_id": app.product_id, "product_version_id": app.product_version_id,
                "external_product_code": app.external_product_code, "institution_name": app.institution_name,
                "product_name": app.product_name, "application_no": app.application_no, "status": app.status,
                "target_amount": str(app.target_amount), "submitted_amount": str(app.submitted_amount) if app.submitted_amount is not None else None,
                "approved_amount": str(app.approved_amount) if app.approved_amount is not None else None,
                "disbursed_amount": str(app.disbursed_amount) if app.disbursed_amount is not None else None,
                "target_term_months": app.target_term_months, "approved_term_months": app.approved_term_months,
                "target_interest_rate": str(app.target_interest_rate) if app.target_interest_rate is not None else None,
                "approved_interest_rate": str(app.approved_interest_rate) if app.approved_interest_rate is not None else None,
                "responsible_user_id": app.responsible_user_id, "responsible_user_name": app.responsible_user_name,
                "current_stage_code": app.current_stage_code, "submission_channel": app.submission_channel,
                "submission_reference": app.submission_reference, "submission_notes": app.submission_notes,
                "approval_reference": app.approval_reference, "rejection_reason": app.rejection_reason,
                "rejection_code": app.rejection_code, "disbursement_reference": app.disbursement_reference,
                "final_result": app.final_result,
                "submitted_at": app.submitted_at.isoformat() if app.submitted_at else None,
                "approved_at": app.approved_at.isoformat() if app.approved_at else None,
                "rejected_at": app.rejected_at.isoformat() if app.rejected_at else None,
                "disbursed_at": app.disbursed_at.isoformat() if app.disbursed_at else None,
                "updated_at": app.updated_at.isoformat() if app.updated_at else None,
                "next_action": next_action,
                "next_task": {"task_id": next_task.task_id, "title": next_task.title,
                              "due_date": next_task.due_date.isoformat() if next_task.due_date else None} if next_task else None,
                "overdue_task_count": len(overdue), "blocking_items": blockers,
                "stages": [{"stage_id": x.stage_id, "stage_code": x.stage_code, "stage_name": x.stage_name,
                            "status": x.status, "started_at": x.started_at.isoformat() if x.started_at else None,
                            "completed_at": x.completed_at.isoformat() if x.completed_at else None, "notes": x.notes,
                            "sequence_no": x.sequence_no} for x in stages],
                "tasks": [{"task_id": x.task_id, "stage_id": x.stage_id, "task_type": x.task_type,
                           "title": x.title, "description": x.description, "status": x.status,
                           "priority": x.priority, "assignee_user_id": x.assignee_user_id,
                           "assignee_user_name": x.assignee_user_name, "due_date": x.due_date.isoformat() if x.due_date else None,
                           "source_type": x.source_type, "source_ref": x.source_ref,
                           "related_material_id": x.related_material_id, "related_condition_id": x.related_condition_id,
                           "required": bool(x.required), "completed_by": x.completed_by,
                           "completed_at": x.completed_at.isoformat() if x.completed_at else None,
                           "notes": x.notes} for x in tasks],
                "events": [{"event_id": x.event_id, "event_type": x.event_type, "from_status": x.from_status,
                            "to_status": x.to_status, "operator_id": x.operator_id, "operator_name": x.operator_name,
                            "payload": _load(x.payload_json, {}), "created_at": x.created_at.isoformat() if x.created_at else None} for x in events],
                "supplements": [{"supplement_id": x.supplement_id, "request_no": x.request_no,
                                 "description": x.description, "requested_by_bank": x.requested_by_bank,
                                 "requested_at": x.requested_at.isoformat(), "due_date": x.due_date.isoformat() if x.due_date else None,
                                 "status": x.status, "completed_at": x.completed_at.isoformat() if x.completed_at else None,
                                 "completed_by": x.completed_by, "notes": x.notes} for x in supplements],
                "application_materials": [{"application_material_id": x.application_material_id,
                                           "supplement_request_id": x.supplement_request_id,
                                           "material_type": x.material_type,
                                           "material_name": x.material_name, "material_category": x.material_category,
                                           "owner_type": x.owner_type, "owner_id": x.owner_id, "owner_name": x.owner_name,
                                           "required": bool(x.required), "required_verified": bool(x.required_verified),
                                           "status": x.status, "source_type": x.source_type, "source_id": x.source_id,
                                           "source_document_id": x.source_document_id,
                                           "customer_material_id": x.customer_material_id, "source_file_id": x.source_file_id,
                                           "file_reference": x.file_reference, "verified_by": x.verified_by,
                                           "verified_at": x.verified_at.isoformat() if x.verified_at else None,
                                           "valid_from": x.valid_from.isoformat() if x.valid_from else None,
                                           "valid_to": x.valid_to.isoformat() if x.valid_to else None,
                                           "coverage_start": x.coverage_start.isoformat() if x.coverage_start else None,
                                           "coverage_end": x.coverage_end.isoformat() if x.coverage_end else None,
                                           "version_no": x.version_no, "replaces_material_id": x.replaces_material_id,
                                           "rejection_reason": x.rejection_reason,
                                           "notes": x.notes} for x in materials],
                "review_feedback": [{"feedback_id": x.feedback_id, "feedback_type": x.feedback_type,
                                     "feedback_date": x.feedback_date.isoformat(),
                                     "institution_contact": x.institution_contact, "content": x.content,
                                     "related_stage": x.related_stage, "created_by": x.created_by} for x in feedback],
                "approval_records": [{"approval_record_id": x.approval_record_id,
                                      "approval_status": x.approval_status,
                                      "submitted_amount": str(x.submitted_amount),
                                      "approved_amount": str(x.approved_amount) if x.approved_amount is not None else None,
                                      "approved_term_months": x.approved_term_months,
                                      "approved_interest_rate": str(x.approved_interest_rate) if x.approved_interest_rate is not None else None,
                                      "guarantee_method": x.guarantee_method, "repayment_method": x.repayment_method,
                                      "approval_reference": x.approval_reference,
                                      "approval_date": x.approval_date.isoformat(),
                                      "approval_expiry_date": x.approval_expiry_date.isoformat() if x.approval_expiry_date else None,
                                      "notes": x.notes} for x in approvals],
                "approval_conditions": [{"approval_condition_id": x.approval_condition_id,
                                         "approval_record_id": x.approval_record_id,
                                         "title": x.title, "description": x.description,
                                         "status": x.status, "required": bool(x.required)} for x in approval_conditions],
                "disbursements": [{"disbursement_id": x.disbursement_id,
                                   "disbursement_no": x.disbursement_no, "amount": str(x.amount),
                                   "disbursed_at": x.disbursed_at.isoformat(), "bank_reference": x.bank_reference,
                                   "recipient_name": x.recipient_name,
                                   "recipient_account_masked": x.recipient_account_masked,
                                   "purpose": x.purpose, "notes": x.notes} for x in disbursements],
                "follow_ups": [{"follow_up_id": x.follow_up_id, "follow_up_type": x.follow_up_type,
                                 "title": x.title, "description": x.description,
                                 "assignee_user_id": x.assignee_user_id,
                                 "assignee_user_name": x.assignee_user_name,
                                 "due_at": x.due_at.isoformat(), "status": x.status,
                                 "priority": x.priority,
                                 "overdue": x.due_at < _now() and x.status not in {"completed", "cancelled"},
                                 "result": x.result} for x in follow_ups],
            }

    @staticmethod
    def _next_action(status: str) -> str:
        return {
            "draft": "开始材料准备", "preparing": "完成材料、条件及必需任务",
            "ready_to_submit": "提交银行", "submitted": "跟进银行受理或记录补件要求",
            "supplement_required": "完成补件材料和任务",
            "under_review": "等待并记录审批反馈", "approved": "完成批复条件并进入放款",
            "partially_approved": "确认部分批复条件并进入放款", "disbursing": "记录后续放款",
            "disbursed": "完成结果反馈并关闭申请", "rejected": "评估是否重新申请",
            "cancelled": "申请已取消", "closed": "流程已结束",
        }.get(status, "人工确认下一步")
