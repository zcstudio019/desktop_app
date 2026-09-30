"""Deterministic execution outcomes and factual financing review analytics.

This layer only consumes recorded execution data.  It never mutates product
catalogue/rules and never infers bank decisions from matching or plan data.
"""
from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    ApplicationBottleneck, FinancingApplication, FinancingApplicationEvent,
    FinancingApplicationMaterial, FinancingApplicationOutcome,
    FinancingApplicationRejectionReason, FinancingApplicationTask,
    FinancingApprovalCondition, FinancingApprovalRecord,
    FinancingDisbursementRecord, FinancingFollowUp, FinancingPlan,
    FinancingPlanVersion, FinancingProduct, FinancingProductRule,
    FinancingProductVersion, FinancingRequirement, FinancingSupplementOutcome,
    FinancingSupplementRequest, ProductMatchSnapshot, ProductRuleFeedback,
)


FINAL_STATUSES = {
    "approved_disbursed", "approved_not_disbursed",
    "partially_approved_disbursed", "partially_approved_not_disbursed",
    "rejected", "cancelled", "closed_without_result",
}
REJECTION_REASON_REGISTRY = {
    "credit_issue": "征信问题", "financial_issue": "财务问题",
    "cashflow_issue": "流水问题", "debt_issue": "负债问题",
    "tax_issue": "税务问题", "invoice_issue": "开票问题",
    "collateral_issue": "抵押物问题", "guarantee_issue": "担保问题",
    "qualification_issue": "资质问题", "industry_restriction": "行业限制",
    "region_restriction": "地区限制", "amount_issue": "额度问题",
    "term_issue": "期限问题", "material_issue": "材料问题",
    "policy_change": "政策变化", "bank_quota": "银行额度限制",
    "customer_withdrawal": "客户主动撤回", "other": "其他",
}
REJECTION_SOURCES = {"bank_feedback", "approval_record", "communication", "manual_confirmed"}
RULE_FEEDBACK_TYPES = {
    "rule_too_strict", "rule_too_loose", "missing_rule", "outdated_rule",
    "ambiguous_rule", "bank_exception", "policy_change", "data_issue", "other",
}
RULE_FEEDBACK_STATUSES = {"pending", "reviewing", "reviewed", "rejected"}
DISBURSEMENT_VARIANCE_REASONS = {
    "customer_reduced", "conditions_unmet", "quota_expired", "partial_drawdown", "other",
}


class FinancingOutcomeError(ValueError):
    pass


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


def _money(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _decimal(value: Any) -> str | None:
    return None if value is None else f"{Decimal(value):.2f}"


class FinancingOutcomeService:
    def __init__(self, session_factory=SessionLocal, *, ensure_schema: bool = True):
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema

    def _prepare(self) -> None:
        if not self.ensure_schema:
            return
        bind = self.session_factory.kw.get("bind")
        if bind is not None:
            Base.metadata.create_all(bind, tables=[
                FinancingApplicationOutcome.__table__, FinancingApplicationRejectionReason.__table__,
                FinancingSupplementOutcome.__table__, ApplicationBottleneck.__table__,
                ProductRuleFeedback.__table__,
            ])

    @staticmethod
    def _get_application(db, application_id: str) -> FinancingApplication:
        row = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == application_id))
        if row is None:
            raise LookupError("融资申请不存在")
        return row

    @staticmethod
    def _event(db, application_id: str, event_type: str, actor_id: str, actor_name: str,
               payload: dict[str, Any]) -> None:
        db.add(FinancingApplicationEvent(
            event_id=uuid.uuid4().hex, application_id=application_id, event_type=event_type,
            operator_id=actor_id, operator_name=actor_name, payload_json=_json(payload),
        ))

    @staticmethod
    def _infer_final_status(app: FinancingApplication) -> str:
        if app.status == "rejected":
            return "rejected"
        if app.status == "cancelled":
            return "cancelled"
        approved, submitted, disbursed = _money(app.approved_amount), _money(app.submitted_amount), _money(app.disbursed_amount)
        partial = bool(submitted and approved < submitted)
        if disbursed > 0:
            return "partially_approved_disbursed" if partial else "approved_disbursed"
        if approved > 0:
            return "partially_approved_not_disbursed" if partial else "approved_not_disbursed"
        return "closed_without_result"

    @staticmethod
    def _serialize_reason(row: FinancingApplicationRejectionReason) -> dict[str, Any]:
        return {
            "rejection_reason_id": row.rejection_reason_id, "reason_code": row.reason_code,
            "reason_label": REJECTION_REASON_REGISTRY.get(row.reason_code, "其他"),
            "description": row.description, "source_type": row.source_type,
            "confirmed_by": row.confirmed_by, "confirmed_at": row.confirmed_at.isoformat(),
        }

    def _serialize_outcome(self, db, row: FinancingApplicationOutcome) -> dict[str, Any]:
        reasons = list(db.scalars(select(FinancingApplicationRejectionReason).where(
            FinancingApplicationRejectionReason.outcome_id == row.outcome_id)))
        submitted, approved, disbursed = _money(row.submitted_amount), _money(row.approved_amount), _money(row.disbursed_amount)
        amount_delta = approved - submitted if row.approved_amount is not None and row.submitted_amount is not None else None
        approval_ratio = approved / submitted if submitted > 0 and row.approved_amount is not None else None
        disbursement_delta = disbursed - approved if row.disbursed_amount is not None and row.approved_amount is not None else None
        return {
            "outcome_id": row.outcome_id, "application_id": row.application_id,
            "customer_id": row.customer_id, "product_id": row.product_id,
            "product_version_id": row.product_version_id, "requirement_id": row.requirement_id,
            "plan_version_id": row.plan_version_id, "outcome_version": row.outcome_version,
            "supersedes_outcome_id": row.supersedes_outcome_id, "final_status": row.final_status,
            "submitted_amount": _decimal(row.submitted_amount), "approved_amount": _decimal(row.approved_amount),
            "disbursed_amount": _decimal(row.disbursed_amount),
            "submitted_term_months": row.submitted_term_months,
            "approved_term_months": row.approved_term_months,
            "submitted_interest_rate": str(row.submitted_interest_rate) if row.submitted_interest_rate is not None else None,
            "approved_interest_rate": str(row.approved_interest_rate) if row.approved_interest_rate is not None else None,
            "approval_date": row.approval_date.isoformat() if row.approval_date else None,
            "disbursement_date": row.disbursement_date.isoformat() if row.disbursement_date else None,
            "rejection_code": row.rejection_code, "rejection_reason": row.rejection_reason,
            "disbursement_variance_reason": row.disbursement_variance_reason,
            "final_notes": row.final_notes, "source_type": row.source_type,
            "status": row.status, "closed_at": row.closed_at.isoformat(),
            "created_by": row.created_by, "created_at": row.created_at.isoformat() if row.created_at else None,
            "rejection_reasons": [self._serialize_reason(value) for value in reasons],
            "approval_variance": {
                "amount_delta": _decimal(amount_delta),
                "approval_ratio": str(approval_ratio.quantize(Decimal("0.0001"))) if approval_ratio is not None else None,
                "term_delta_months": (row.approved_term_months - row.submitted_term_months)
                    if row.approved_term_months is not None and row.submitted_term_months is not None else None,
                "interest_rate_delta": str(row.approved_interest_rate - row.submitted_interest_rate)
                    if row.approved_interest_rate is not None and row.submitted_interest_rate is not None else None,
            },
            "disbursement_variance": {
                "amount_delta": _decimal(disbursement_delta),
                "reason": row.disbursement_variance_reason,
            },
        }

    def _latest_outcome_row(self, db, application_id: str) -> FinancingApplicationOutcome | None:
        return db.scalar(select(FinancingApplicationOutcome).where(
            FinancingApplicationOutcome.application_id == application_id,
            FinancingApplicationOutcome.status == "finalized",
        ).order_by(FinancingApplicationOutcome.outcome_version.desc()))

    def get_outcome(self, application_id: str) -> dict[str, Any] | None:
        self._prepare()
        with self.session_factory() as db:
            self._get_application(db, application_id)
            row = self._latest_outcome_row(db, application_id)
            return self._serialize_outcome(db, row) if row else None

    def _write_rejection_reasons(self, db, app: FinancingApplication, outcome_id: str,
                                 reasons: list[dict[str, Any]], actor_id: str) -> None:
        for value in reasons:
            code, source = str(value.get("reason_code") or ""), str(value.get("source_type") or "")
            if code not in REJECTION_REASON_REGISTRY:
                raise FinancingOutcomeError("拒绝原因代码不合法")
            if source not in REJECTION_SOURCES:
                raise FinancingOutcomeError("拒绝原因必须有明确来源")
            db.add(FinancingApplicationRejectionReason(
                rejection_reason_id=uuid.uuid4().hex, application_id=app.application_id,
                outcome_id=outcome_id, reason_code=code,
                description=str(value.get("description") or ""), source_type=source,
                confirmed_by=actor_id, confirmed_at=_now(),
            ))

    def _sync_supplement_outcome(self, db, application_id: str) -> None:
        supplements = list(db.scalars(select(FinancingSupplementRequest).where(
            FinancingSupplementRequest.application_id == application_id)))
        materials = list(db.scalars(select(FinancingApplicationMaterial).where(
            FinancingApplicationMaterial.application_id == application_id,
            FinancingApplicationMaterial.supplement_request_id.is_not(None))))
        categories = sorted({value.material_category or "other" for value in materials})
        row = db.scalar(select(FinancingSupplementOutcome).where(
            FinancingSupplementOutcome.application_id == application_id))
        if row is None:
            row = FinancingSupplementOutcome(supplement_outcome_id=uuid.uuid4().hex, application_id=application_id)
            db.add(row)
        row.supplement_count = len(supplements)
        row.supplement_material_count = len(materials)
        row.supplement_rounds = len({value.request_no for value in supplements})
        row.categories_json = _json(categories)
        row.generated_at = _now()

    def sync_bottlenecks(self, application_id: str) -> list[dict[str, Any]]:
        self._prepare(); now = _now()
        with self.session_factory.begin() as db:
            self._get_application(db, application_id)
            candidates: list[tuple[str, str, str, datetime, bool]] = []
            for row in db.scalars(select(FinancingApplicationTask).where(
                FinancingApplicationTask.application_id == application_id,
                FinancingApplicationTask.status == "blocked")):
                candidates.append(("blocked_task", row.task_id, row.notes or row.title, row.updated_at or row.created_at, False))
            for row in db.scalars(select(FinancingSupplementRequest).where(
                FinancingSupplementRequest.application_id == application_id,
                FinancingSupplementRequest.status.in_(["pending", "in_progress"]))):
                candidates.append(("supplement_request", row.supplement_id, row.description, row.requested_at, False))
            for row in db.scalars(select(FinancingFollowUp).where(
                FinancingFollowUp.application_id == application_id,
                FinancingFollowUp.status.not_in(["completed", "cancelled"]),
                FinancingFollowUp.due_at < now)):
                candidates.append(("overdue_followup", row.follow_up_id, row.title, row.due_at, False))
            for row in db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.status == "rejected")):
                candidates.append(("material_rejected", row.application_material_id, row.rejection_reason or row.material_name, row.updated_at or row.created_at, False))
            approvals = list(db.scalars(select(FinancingApprovalRecord.approval_record_id).where(
                FinancingApprovalRecord.application_id == application_id)))
            if approvals:
                for row in db.scalars(select(FinancingApprovalCondition).where(
                    FinancingApprovalCondition.approval_record_id.in_(approvals),
                    FinancingApprovalCondition.required == 1,
                    FinancingApprovalCondition.status == "pending")):
                    candidates.append(("pending_approval_condition", row.approval_condition_id, row.title, row.updated_at, False))
            active_keys = {(kind, source_id) for kind, source_id, *_ in candidates}
            existing = list(db.scalars(select(ApplicationBottleneck).where(
                ApplicationBottleneck.application_id == application_id)))
            for row in existing:
                if row.resolved_at is None and (row.bottleneck_type, row.source_id) not in active_keys:
                    row.resolved_at = now
                    row.duration_days = max((now.date() - row.started_at.date()).days, 0)
            existing_keys = {(row.bottleneck_type, row.source_id) for row in existing}
            for kind, source_id, description, started_at, _ in candidates:
                if (kind, source_id) not in existing_keys:
                    db.add(ApplicationBottleneck(
                        bottleneck_id=uuid.uuid4().hex, application_id=application_id,
                        bottleneck_type=kind, source_id=source_id, description=description,
                        started_at=started_at or now,
                    ))
        return self.list_bottlenecks(application_id)

    def list_bottlenecks(self, application_id: str) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            rows = list(db.scalars(select(ApplicationBottleneck).where(
                ApplicationBottleneck.application_id == application_id).order_by(ApplicationBottleneck.started_at)))
            return [{
                "bottleneck_id": row.bottleneck_id, "bottleneck_type": row.bottleneck_type,
                "source_id": row.source_id, "description": row.description,
                "started_at": row.started_at.isoformat(),
                "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
                "duration_days": row.duration_days if row.duration_days is not None else max((_now().date() - row.started_at.date()).days, 0),
            } for row in rows]

    def finalize_outcome(self, application_id: str, *, rejection_reasons: list[dict[str, Any]] | None,
                         disbursement_variance_reason: str | None, final_notes: str,
                         actor_id: str, actor_name: str, source_type: str = "execution") -> dict[str, Any]:
        self._prepare()
        if disbursement_variance_reason and disbursement_variance_reason not in DISBURSEMENT_VARIANCE_REASONS:
            raise FinancingOutcomeError("放款差异原因不合法")
        with self.session_factory.begin() as db:
            app = self._get_application(db, application_id)
            if app.status not in {"disbursed", "rejected", "cancelled", "closed"}:
                raise FinancingOutcomeError("申请尚未形成可定稿的真实执行结果")
            if self._latest_outcome_row(db, application_id):
                raise FinancingOutcomeError("该申请结果已定稿，请使用修正流程")
            final_status = self._infer_final_status(app)
            outcome_id = uuid.uuid4().hex
            row = FinancingApplicationOutcome(
                outcome_id=outcome_id, application_id=app.application_id,
                customer_id=app.customer_id, product_id=app.product_id,
                product_version_id=app.product_version_id, requirement_id=app.requirement_id,
                plan_version_id=app.plan_version_id, outcome_version=1,
                final_status=final_status, submitted_amount=app.submitted_amount,
                approved_amount=app.approved_amount, disbursed_amount=app.disbursed_amount,
                submitted_term_months=app.target_term_months,
                approved_term_months=app.approved_term_months,
                submitted_interest_rate=app.target_interest_rate,
                approved_interest_rate=app.approved_interest_rate,
                approval_date=app.approved_at, disbursement_date=app.disbursed_at,
                rejection_code=app.rejection_code, rejection_reason=app.rejection_reason,
                disbursement_variance_reason=disbursement_variance_reason,
                final_notes=final_notes, source_type=source_type, status="finalized",
                closed_at=app.closed_at or _now(), created_by=actor_id,
            )
            db.add(row); db.flush()
            self._write_rejection_reasons(db, app, outcome_id, rejection_reasons or [], actor_id)
            self._sync_supplement_outcome(db, application_id)
            product_version = db.scalar(select(FinancingProductVersion).where(
                FinancingProductVersion.version_id == app.product_version_id))
            if (product_version is not None and product_version.max_amount is not None and
                    app.approved_amount is not None and _money(app.approved_amount) > _money(product_version.max_amount)):
                existing_feedback = db.scalar(select(ProductRuleFeedback.feedback_id).where(
                    ProductRuleFeedback.application_id == application_id,
                    ProductRuleFeedback.product_version_id == app.product_version_id,
                    ProductRuleFeedback.feedback_type == "outdated_rule"))
                if existing_feedback is None:
                    db.add(ProductRuleFeedback(
                        feedback_id=uuid.uuid4().hex, product_id=app.product_id,
                        product_version_id=app.product_version_id, rule_id=None,
                        application_id=application_id, feedback_type="outdated_rule",
                        expected_result=f"当前结构化最高额度：{product_version.max_amount}",
                        actual_bank_feedback=f"实际批复额度：{app.approved_amount}",
                        evidence_source="approval_record",
                        evidence_reference=app.approval_reference, status="pending", created_by=actor_id,
                    ))
            self._event(db, application_id, "outcome_finalized", actor_id, actor_name,
                        {"outcome_id": outcome_id, "final_status": final_status})
        self.sync_bottlenecks(application_id)
        return self.get_outcome(application_id) or {}

    def correct_outcome(self, application_id: str, *, changes: dict[str, Any], reason: str,
                        actor_id: str, actor_name: str) -> dict[str, Any]:
        if not reason.strip():
            raise FinancingOutcomeError("修正原因不能为空")
        allowed = {
            "final_status", "submitted_amount", "approved_amount", "disbursed_amount",
            "submitted_term_months", "approved_term_months", "submitted_interest_rate",
            "approved_interest_rate", "approval_date", "disbursement_date",
            "rejection_code", "rejection_reason", "disbursement_variance_reason", "final_notes",
        }
        if set(changes) - allowed:
            raise FinancingOutcomeError("包含不可修正字段")
        if changes.get("final_status") and changes["final_status"] not in FINAL_STATUSES:
            raise FinancingOutcomeError("最终结果状态不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get_application(db, application_id)
            previous = self._latest_outcome_row(db, application_id)
            if previous is None:
                raise FinancingOutcomeError("尚无可修正的已定稿结果")
            values = {name: getattr(previous, name) for name in allowed}
            values.update(changes); values["final_notes"] = f"{values.get('final_notes') or ''}\n修正说明：{reason}".strip()
            row = FinancingApplicationOutcome(
                outcome_id=uuid.uuid4().hex, application_id=application_id,
                customer_id=previous.customer_id, product_id=previous.product_id,
                product_version_id=previous.product_version_id, requirement_id=previous.requirement_id,
                plan_version_id=previous.plan_version_id, outcome_version=previous.outcome_version + 1,
                supersedes_outcome_id=previous.outcome_id, source_type="correction",
                status="finalized", closed_at=previous.closed_at, created_by=actor_id, **values,
            )
            db.add(row); db.flush()
            for old in db.scalars(select(FinancingApplicationRejectionReason).where(
                FinancingApplicationRejectionReason.outcome_id == previous.outcome_id)):
                db.add(FinancingApplicationRejectionReason(
                    rejection_reason_id=uuid.uuid4().hex, application_id=application_id,
                    outcome_id=row.outcome_id, reason_code=old.reason_code,
                    description=old.description, source_type=old.source_type,
                    confirmed_by=actor_id, confirmed_at=_now(),
                ))
            self._event(db, app.application_id, "outcome_corrected", actor_id, actor_name,
                        {"outcome_id": row.outcome_id, "supersedes": previous.outcome_id, "reason": reason})
        return self.get_outcome(application_id) or {}

    def close_application(self, application_id: str, *, close_reason: str,
                          final_result_confirmed: bool, actor_id: str, actor_name: str) -> dict[str, Any]:
        if not close_reason.strip() or not final_result_confirmed:
            raise FinancingOutcomeError("关闭申请必须填写原因并确认最终结果")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get_application(db, application_id)
            if app.status not in {"disbursed", "rejected", "cancelled", "approved", "partially_approved"}:
                raise FinancingOutcomeError("当前申请状态不允许关闭")
            if app.status in {"approved", "partially_approved"} and _money(app.disbursed_amount) < _money(app.approved_amount):
                if not final_result_confirmed:
                    raise FinancingOutcomeError("存在未完成放款，需明确确认后关闭")
            previous = app.status; app.status = "closed"; app.closed_at = _now()
            self._event(db, application_id, "application_closed", actor_id, actor_name,
                        {"from_status": previous, "close_reason": close_reason, "final_result_confirmed": True})
        return {"application_id": application_id, "status": "closed", "close_reason": close_reason}

    def create_rule_feedback(self, product_id: str, *, product_version_id: str, rule_id: str | None,
                             application_id: str, feedback_type: str, expected_result: str,
                             actual_bank_feedback: str, evidence_source: str,
                             evidence_reference: str | None, actor_id: str) -> dict[str, Any]:
        if feedback_type not in RULE_FEEDBACK_TYPES:
            raise FinancingOutcomeError("规则反馈类型不合法")
        if not actual_bank_feedback.strip() or not evidence_source.strip():
            raise FinancingOutcomeError("实际银行反馈和证据来源不能为空")
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._get_application(db, application_id)
            if app.product_id != product_id or app.product_version_id != product_version_id:
                raise FinancingOutcomeError("规则反馈产品版本与申请不一致")
            if rule_id and db.scalar(select(FinancingProductRule.rule_id).where(
                FinancingProductRule.rule_id == rule_id,
                FinancingProductRule.version_id == product_version_id)) is None:
                raise FinancingOutcomeError("产品规则不存在或版本不匹配")
            row = ProductRuleFeedback(
                feedback_id=uuid.uuid4().hex, product_id=product_id,
                product_version_id=product_version_id, rule_id=rule_id,
                application_id=application_id, feedback_type=feedback_type,
                expected_result=expected_result, actual_bank_feedback=actual_bank_feedback,
                evidence_source=evidence_source, evidence_reference=evidence_reference,
                status="pending", created_by=actor_id,
            )
            db.add(row); db.flush(); feedback_id = row.feedback_id
        return self.get_rule_feedback(feedback_id)

    def get_rule_feedback(self, feedback_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(ProductRuleFeedback).where(ProductRuleFeedback.feedback_id == feedback_id))
            if row is None: raise LookupError("规则反馈不存在")
            return self._serialize_feedback(row)

    @staticmethod
    def _serialize_feedback(row: ProductRuleFeedback) -> dict[str, Any]:
        return {
            "feedback_id": row.feedback_id, "product_id": row.product_id,
            "product_version_id": row.product_version_id, "rule_id": row.rule_id,
            "application_id": row.application_id, "feedback_type": row.feedback_type,
            "expected_result": row.expected_result,
            "actual_bank_feedback": row.actual_bank_feedback,
            "evidence_source": row.evidence_source, "evidence_reference": row.evidence_reference,
            "status": row.status, "reviewed_by": row.reviewed_by,
            "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    def list_rule_feedback(self, product_id: str) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            rows = list(db.scalars(select(ProductRuleFeedback).where(
                ProductRuleFeedback.product_id == product_id).order_by(ProductRuleFeedback.created_at.desc())))
            return [self._serialize_feedback(row) for row in rows]

    def update_rule_feedback(self, feedback_id: str, *, status: str,
                             actor_id: str) -> dict[str, Any]:
        if status not in RULE_FEEDBACK_STATUSES:
            raise FinancingOutcomeError("规则反馈状态不合法")
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(ProductRuleFeedback).where(ProductRuleFeedback.feedback_id == feedback_id))
            if row is None: raise LookupError("规则反馈不存在")
            row.status = status; row.reviewed_by = actor_id; row.reviewed_at = _now()
        return self.get_rule_feedback(feedback_id)

    def _latest_outcomes(self, db) -> list[FinancingApplicationOutcome]:
        rows = list(db.scalars(select(FinancingApplicationOutcome).where(
            FinancingApplicationOutcome.status == "finalized").order_by(
                FinancingApplicationOutcome.application_id,
                FinancingApplicationOutcome.outcome_version.desc())))
        latest: dict[str, FinancingApplicationOutcome] = {}
        for row in rows:
            latest.setdefault(row.application_id, row)
        return list(latest.values())

    def product_execution_metrics(self, product_id: str, *, product_version_id: str | None = None) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            outcomes = [row for row in self._latest_outcomes(db) if row.product_id == product_id and
                        (product_version_id is None or row.product_version_id == product_version_id)]
            application_ids = [row.application_id for row in outcomes]
            approvals = list(db.scalars(select(FinancingApprovalRecord).where(
                FinancingApprovalRecord.application_id.in_(application_ids)))) if application_ids else []
            supplements = list(db.scalars(select(FinancingSupplementRequest).where(
                FinancingSupplementRequest.application_id.in_(application_ids)))) if application_ids else []
            feedback_pending = db.scalar(select(func.count()).select_from(ProductRuleFeedback).where(
                ProductRuleFeedback.product_id == product_id,
                ProductRuleFeedback.status.in_(["pending", "reviewing"]))) or 0
            approved_statuses = {"approved_disbursed", "approved_not_disbursed"}
            partial_statuses = {"partially_approved_disbursed", "partially_approved_not_disbursed"}
            approval_days, disbursement_days = [], []
            for row in outcomes:
                if row.approval_date and row.created_at:
                    approval_days.append(max((row.approval_date.date() - row.created_at.date()).days, 0))
                if row.disbursement_date and row.approval_date:
                    disbursement_days.append(max((row.disbursement_date.date() - row.approval_date.date()).days, 0))
            return {
                "product_id": product_id, "product_version_id": product_version_id,
                "sample_size": len(outcomes), "application_count": len(outcomes),
                "submitted_count": sum(row.submitted_amount is not None for row in outcomes),
                "approved_count": sum(row.final_status in approved_statuses for row in outcomes),
                "partially_approved_count": sum(row.final_status in partial_statuses for row in outcomes),
                "rejected_count": sum(row.final_status == "rejected" for row in outcomes),
                "disbursed_count": sum(_money(row.disbursed_amount) > 0 for row in outcomes),
                "submitted_amount_total": _decimal(sum((_money(row.submitted_amount) for row in outcomes), Decimal(0))),
                "approved_amount_total": _decimal(sum((_money(row.approved_amount) for row in outcomes), Decimal(0))),
                "disbursed_amount_total": _decimal(sum((_money(row.disbursed_amount) for row in outcomes), Decimal(0))),
                "supplement_application_count": len({row.application_id for row in supplements}),
                "supplement_round_total": len(supplements),
                "average_approval_days": (sum(approval_days) / len(approval_days)) if approval_days else None,
                "average_disbursement_days": (sum(disbursement_days) / len(disbursement_days)) if disbursement_days else None,
                "pending_rule_feedback_count": int(feedback_pending),
                "approval_record_count": len(approvals),
            }

    def customer_financing_review(self, customer_id: str, *, requirement_id: str | None = None) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            req_stmt = select(FinancingRequirement).where(FinancingRequirement.customer_id == customer_id)
            if requirement_id:
                req_stmt = req_stmt.where(FinancingRequirement.requirement_id == requirement_id)
            req = db.scalar(req_stmt.order_by(FinancingRequirement.version.desc()))
            if req is None:
                raise LookupError("融资需求不存在")
            apps = list(db.scalars(select(FinancingApplication).where(
                FinancingApplication.customer_id == customer_id,
                FinancingApplication.requirement_id == req.requirement_id)))
            app_ids = {row.application_id for row in apps}
            outcomes = [row for row in self._latest_outcomes(db) if row.application_id in app_ids]
            requirement_amount = _money(req.requested_amount)
            submitted = sum((_money(row.submitted_amount) for row in outcomes), Decimal(0))
            approved = sum((_money(row.approved_amount) for row in outcomes), Decimal(0))
            disbursed = sum((_money(row.disbursed_amount) for row in outcomes), Decimal(0))
            gap = max(requirement_amount - disbursed, Decimal(0))
            if not apps:
                execution_status = "not_started"
            elif disbursed >= requirement_amount and requirement_amount > 0:
                execution_status = "fully_funded"
            elif disbursed > 0:
                execution_status = "partially_funded"
            elif apps and all(row.status == "cancelled" for row in apps):
                execution_status = "cancelled"
            elif apps and all(row.status in {"closed", "rejected", "cancelled"} for row in apps):
                execution_status = "closed_unfunded"
            else:
                execution_status = "in_progress"
            rejection_rows = list(db.scalars(select(FinancingApplicationRejectionReason).where(
                FinancingApplicationRejectionReason.application_id.in_(app_ids)))) if app_ids else []
            feedback_rows = list(db.scalars(select(ProductRuleFeedback).where(
                ProductRuleFeedback.application_id.in_(app_ids)))) if app_ids else []
            lifecycle: list[dict[str, Any]] = [{
                "type": "requirement_confirmed", "title": "融资需求确认",
                "occurred_at": (req.confirmed_at or req.created_at).isoformat() if (req.confirmed_at or req.created_at) else None,
                "reference_id": req.requirement_id,
            }]
            matches = list(db.scalars(select(ProductMatchSnapshot).where(
                ProductMatchSnapshot.requirement_id == req.requirement_id)))
            lifecycle.extend({"type": "product_matching", "title": "产品匹配完成",
                "occurred_at": row.generated_at.isoformat(), "reference_id": row.snapshot_id} for row in matches)
            plans = list(db.scalars(select(FinancingPlan).where(
                FinancingPlan.requirement_id == req.requirement_id)))
            for plan in plans:
                version = db.scalar(select(FinancingPlanVersion).where(
                    FinancingPlanVersion.plan_version_id == plan.current_version_id)) if plan.current_version_id else None
                if version and version.status == "confirmed":
                    lifecycle.append({"type": "plan_confirmed", "title": "融资方案确认",
                        "occurred_at": version.created_at.isoformat() if version.created_at else None,
                        "reference_id": version.plan_version_id})
            events = list(db.scalars(select(FinancingApplicationEvent).where(
                FinancingApplicationEvent.application_id.in_(app_ids)))) if app_ids else []
            lifecycle.extend({"type": row.event_type, "title": row.event_type,
                "occurred_at": row.created_at.isoformat() if row.created_at else None,
                "reference_id": row.event_id, "application_id": row.application_id} for row in events)
            lifecycle.sort(key=lambda value: value.get("occurred_at") or "")
            return {
                "customer_id": customer_id, "requirement_id": req.requirement_id,
                "requirement_version": req.version,
                "requirement": {"amount": _decimal(req.requested_amount), "purpose": req.purpose_detail or req.financing_purpose,
                                "term_value": req.term_value, "term_unit": req.term_unit},
                "application_count": len(apps), "outcome_count": len(outcomes),
                "submitted_amount": _decimal(submitted), "approved_amount": _decimal(approved),
                "disbursed_amount": _decimal(disbursed), "funding_gap": _decimal(gap),
                "actual_coverage_ratio": str((disbursed / requirement_amount).quantize(Decimal("0.0001"))) if requirement_amount > 0 else None,
                "execution_status": execution_status,
                "message": "当前尚无已执行融资申请。" if not apps else "已按真实执行记录生成融资复盘。",
                "rejection_reasons": [self._serialize_reason(row) for row in rejection_rows],
                "rule_feedback": [self._serialize_feedback(row) for row in feedback_rows],
                "lifecycle_timeline": lifecycle,
            }

