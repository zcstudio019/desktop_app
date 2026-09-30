"""Versioned financing-plan drafts built only from confirmed requirements and match snapshots."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import delete, func, inspect as sa_inspect, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    Document,
    Extraction,
    FinancingPlan,
    FinancingPlanCondition,
    FinancingPlanExplanation,
    FinancingPlanGap,
    FinancingPlanItem,
    FinancingPlanMaterial,
    FinancingPlanVersion,
    FinancingProductRule,
    FinancingProductVersion,
    FinancingRequirement,
    ManualCandidateOverride,
    ProductMatchItem,
    ProductMatchSnapshot,
)
from backend.document_types import normalize_document_type_code
from backend.services.plan_combination_engine import (
    CombinationStrategy,
    PlanCombinationEngine,
    PlanCombinationResult,
)


PlanStatus = Literal["draft", "needs_review", "confirmed", "superseded", "cancelled"]
PlanType = Literal["primary", "backup", "conditional"]
GenerationStatus = Literal["complete", "partial", "insufficient_candidates", "manual_review_required"]
CandidateStatus = Literal["usable", "conditional", "manual_review", "excluded"]


class FinancingPlanError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _list(value: str | None) -> list[Any]:
    try:
        parsed = json.loads(value or "[]")
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def _decimal(value: Any, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FinancingPlanError(f"{label}必须是有效数字") from exc
    if not parsed.is_finite():
        raise FinancingPlanError(f"{label}必须是有限数字")
    return parsed


def _fact_value(facts: dict[str, Any], domain: str, field: str) -> Any:
    fact = (facts.get(domain) or {}).get(field) or {}
    return fact.get("value") if fact.get("status") == "known" else None


MATERIAL_ALIASES = {
    "营业执照": "business_license",
    "法人身份证": "id_card",
    "身份证": "id_card",
    "公司章程": "company_articles",
    "财务报表": "financial_report",
    "银行流水": "bank_statement",
    "企业流水": "bank_statement",
    "个人征信": "personal_credit_report",
    "企业征信": "enterprise_credit_report",
    "房产证": "property_cert",
}


def _material_identity(name: str, explicit_code: str = "") -> tuple[str, str]:
    cleaned = re.sub(r"[\s、，,；;：:（）()]+", "", str(name or "").strip())
    code = str(explicit_code or "").strip()
    if not code:
        code = MATERIAL_ALIASES.get(cleaned) or str(normalize_document_type_code(cleaned) or cleaned)
    return code.lower(), cleaned or str(name or "").strip()


def _material_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            parsed = [raw] if raw.strip() else []
    else:
        parsed = raw or []
    if isinstance(parsed, dict):
        parsed = parsed.get("materials") or parsed.get("items") or [parsed]
    result: list[dict[str, Any]] = []
    for value in parsed if isinstance(parsed, list) else []:
        if isinstance(value, str):
            result.append({"name": value})
        elif isinstance(value, dict):
            name = value.get("material_name") or value.get("name") or value.get("材料")
            if name:
                result.append({
                    "name": str(name), "code": str(value.get("material_code") or value.get("code") or ""),
                    "category": str(value.get("material_category") or value.get("category") or "other"),
                    "required": bool(value.get("required", True)), "notes": str(value.get("notes") or ""),
                })
    return result


class FinancingPlanCandidate(BaseModel):
    product_match_item_id: int
    product_id: str
    product_version_id: str
    external_product_code: str
    institution_name: str
    product_name: str
    match_status: str
    min_amount: Decimal | None = None
    max_amount: Decimal | None = None
    max_term_months: int | None = None
    eligible_amount_cap: Decimal | None = None
    missing_information: list[str] = Field(default_factory=list)
    review_reasons: list[str] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
    candidate_status: CandidateStatus
    manual_override_id: str | None = None


class FinancingPlanItemInput(BaseModel):
    product_match_item_id: int
    proposed_amount: Decimal
    proposed_term_months: int
    item_role: Literal["primary", "supplementary", "fallback"] = "primary"
    sequence_no: int = 0
    reason: str = ""
    notes: str = ""
    conditions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    manual_approved: bool = False


class FinancingPlanDraftInput(BaseModel):
    customer_id: str
    requirement_id: str
    match_snapshot_id: str
    plan_type: PlanType
    items: list[FinancingPlanItemInput]
    summary: str = ""
    rationale: str = ""
    required_actions: list[str] = Field(default_factory=list)
    conditions: list[str] = Field(default_factory=list)


class PlanRuleEngine:
    """Deterministic validation interface for plan candidates and manual drafts."""

    @staticmethod
    def evaluate_candidate(item: ProductMatchItem, requirement_term_months: int | None) -> FinancingPlanCandidate:
        missing = _list(item.missing_information_json)
        reviews = _list(item.review_reasons_json)
        blocking = _list(item.blocking_reasons_json)
        mapping: dict[str, CandidateStatus] = {
            "eligible": "usable",
            "conditional": "conditional",
            "manual_review": "manual_review",
            "ineligible": "excluded",
            "product_configuration_error": "excluded",
        }
        candidate_status = mapping.get(item.overall_status, "excluded")
        minimum = None
        for rule in _list(item.rule_results_json):
            if (
                rule.get("rule_source") == "system_builtin"
                and str(rule.get("rule_id") or "").endswith(":min_amount")
            ):
                minimum = _decimal(rule.get("expected_value"), "产品最低额度")
                break
        max_amount = Decimal(str(item.max_amount)) if item.max_amount is not None else None
        max_term = item.max_term_months
        if candidate_status == "usable" and max_amount is None:
            candidate_status = "manual_review"
            reviews.append("产品最高额度尚未结构化，不能自动确定可融资金额。")
        if candidate_status == "usable" and max_term is None:
            candidate_status = "manual_review"
            reviews.append("产品最长期限尚未结构化，不能自动确认期限兼容。")
        if candidate_status not in {"excluded", "manual_review"} and requirement_term_months is not None and max_term is not None and requirement_term_months > max_term:
            candidate_status = "excluded"
            blocking.append("融资需求期限超过产品最长期限。")
        return FinancingPlanCandidate(
            product_match_item_id=item.id,
            product_id=item.product_id,
            product_version_id=item.version_id,
            external_product_code=item.external_product_code,
            institution_name=item.institution_name,
            product_name=item.product_name,
            match_status=item.overall_status,
            min_amount=minimum,
            max_amount=max_amount,
            max_term_months=max_term,
            eligible_amount_cap=max_amount if candidate_status != "excluded" else None,
            missing_information=missing,
            review_reasons=reviews,
            blocking_reasons=blocking,
            candidate_status=candidate_status,
        )

    @staticmethod
    def validate_plan_item_status(candidate: FinancingPlanCandidate, *, manual_approved: bool) -> None:
        if candidate.candidate_status == "excluded":
            raise FinancingPlanError("不符合条件或配置异常的产品不能进入融资方案")
        if candidate.candidate_status == "manual_review" and not manual_approved:
            raise FinancingPlanError("人工复核产品必须经过显式确认后才能纳入方案")

    @staticmethod
    def validate_plan_amount(candidate: FinancingPlanCandidate, proposed_amount: Decimal) -> None:
        amount = _decimal(proposed_amount, "建议金额")
        if amount <= 0:
            raise FinancingPlanError("建议金额必须大于0")
        if candidate.max_amount is None:
            raise FinancingPlanError("产品最高额度未知，不能自动确认建议金额")
        if candidate.min_amount is not None and amount < candidate.min_amount:
            raise FinancingPlanError("建议金额不能低于产品最低额度")
        if amount > candidate.max_amount:
            raise FinancingPlanError("建议金额不能超过产品最高额度")

    @staticmethod
    def validate_plan_term(candidate: FinancingPlanCandidate, proposed_term_months: int) -> None:
        if proposed_term_months <= 0:
            raise FinancingPlanError("建议期限必须大于0")
        if candidate.max_term_months is None:
            raise FinancingPlanError("产品最长期限未知，不能自动确认建议期限")
        if proposed_term_months > candidate.max_term_months:
            raise FinancingPlanError("建议期限不能超过产品最长期限")

    @staticmethod
    def validate_plan(covered_amount: Decimal, funding_gap: Decimal, generation_status: str) -> None:
        if covered_amount <= 0:
            raise FinancingPlanError("方案覆盖金额必须大于0")
        if funding_gap < 0:
            raise FinancingPlanError("方案覆盖金额不能超过融资需求金额")
        if generation_status == "complete" and funding_gap != 0:
            raise FinancingPlanError("完整方案必须实现零资金缺口")
        if funding_gap > 0 and generation_status == "complete":
            raise FinancingPlanError("存在资金缺口的方案不能标记为完整方案")

    @staticmethod
    def evaluate_plan(
        *,
        target_amount: Decimal,
        covered_amount: Decimal,
        candidates: list[FinancingPlanCandidate],
    ) -> tuple[GenerationStatus, Decimal]:
        funding_gap = target_amount - covered_amount
        has_review = any(
            candidate.candidate_status in {"conditional", "manual_review"}
            for candidate in candidates
        )
        generation_status: GenerationStatus = (
            "manual_review_required"
            if has_review
            else ("complete" if funding_gap == 0 else "partial")
        )
        PlanRuleEngine.validate_plan(covered_amount, funding_gap, generation_status)
        return generation_status, funding_gap


class FinancingPlanService:
    def __init__(self, *, session_factory=SessionLocal, ensure_schema: bool = True) -> None:
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema
        self._schema_ready = False
        self.rules = PlanRuleEngine()
        self.combinations = PlanCombinationEngine()

    def _prepare(self) -> None:
        if self.ensure_schema and not self._schema_ready:
            with self.session_factory() as db:
                Base.metadata.create_all(
                    bind=db.get_bind(),
                    tables=[
                        FinancingPlan.__table__, FinancingPlanVersion.__table__,
                        FinancingPlanItem.__table__, FinancingPlanGap.__table__,
                        ManualCandidateOverride.__table__, FinancingPlanCondition.__table__,
                        FinancingPlanMaterial.__table__, FinancingPlanExplanation.__table__,
                    ],
                    checkfirst=True,
                )
            self._schema_ready = True

    @staticmethod
    def _load_context_from_db(db, customer_id: str, requirement_id: str, snapshot_id: str) -> tuple[FinancingRequirement, ProductMatchSnapshot, list[ProductMatchItem], Decimal, int | None, str]:
        requirement = db.scalar(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.requirement_id == requirement_id,
        ))
        if requirement is None:
            raise FinancingPlanError("融资需求不存在")
        if requirement.status != "confirmed":
            raise FinancingPlanError("融资方案只能基于已确认的融资需求")
        snapshot = db.scalar(select(ProductMatchSnapshot).where(ProductMatchSnapshot.snapshot_id == snapshot_id))
        if snapshot is None:
            raise FinancingPlanError("产品匹配快照不存在")
        if snapshot.customer_id != customer_id or snapshot.requirement_id != requirement_id:
            raise FinancingPlanError("产品匹配快照与客户融资需求不一致")
        if snapshot.requirement_version != requirement.version:
            raise FinancingPlanError("产品匹配快照不是当前融资需求版本")
        items = list(db.scalars(select(ProductMatchItem).where(ProductMatchItem.snapshot_id == snapshot_id).order_by(ProductMatchItem.id)))
        facts = json.loads(snapshot.facts_json or "{}")
        amount = _fact_value(facts, "requirement", "amount")
        if amount is None:
            raise FinancingPlanError("匹配快照缺少已确认融资金额")
        target_amount = _decimal(amount, "融资需求金额")
        term_value = _fact_value(facts, "requirement", "term_months")
        term_months = int(term_value) if term_value is not None else None
        currency = str(_fact_value(facts, "requirement", "currency") or requirement.currency or "CNY")
        return requirement, snapshot, items, target_amount, term_months, currency

    def _load_context(self, customer_id: str, requirement_id: str, snapshot_id: str) -> tuple[FinancingRequirement, ProductMatchSnapshot, list[ProductMatchItem], Decimal, int | None, str]:
        self._prepare()
        with self.session_factory() as db:
            return self._load_context_from_db(db, customer_id, requirement_id, snapshot_id)

    def build_plan_candidates(self, customer_id: str, requirement_id: str, match_snapshot_id: str) -> dict[str, Any]:
        requirement, snapshot, items, target, term, currency = self._load_context(customer_id, requirement_id, match_snapshot_id)
        candidates = [self.rules.evaluate_candidate(item, term) for item in items]
        with self.session_factory() as db:
            overrides = list(db.scalars(select(ManualCandidateOverride).where(
                ManualCandidateOverride.match_snapshot_id == match_snapshot_id,
            ).order_by(ManualCandidateOverride.created_at, ManualCandidateOverride.id))) if sa_inspect(db.connection()).has_table(ManualCandidateOverride.__tablename__) else []
        latest_override = {row.product_match_item_id: row for row in overrides}
        for candidate in candidates:
            override = latest_override.get(candidate.product_match_item_id)
            if override and candidate.candidate_status == "manual_review" and override.new_status == "conditional":
                candidate.candidate_status = "conditional"
                candidate.manual_override_id = override.override_id
                candidate.review_reasons = _list(_json(candidate.review_reasons + [
                    f"人工纳入条件性候选：{override.reason}",
                ]))
        counts = {status: sum(item.candidate_status == status for item in candidates) for status in ("usable", "conditional", "manual_review", "excluded")}
        if counts["usable"] or counts["conditional"]:
            generation_status: GenerationStatus = "partial"
        elif counts["manual_review"]:
            generation_status = "manual_review_required"
        else:
            generation_status = "insufficient_candidates"
        return {
            "customer_id": customer_id,
            "requirement_id": requirement_id,
            "requirement_version": requirement.version,
            "match_snapshot_id": snapshot.snapshot_id,
            "facts_hash": snapshot.facts_hash,
            "catalog_hash": snapshot.catalog_version_hash,
            "target_amount": str(target),
            "covered_amount": "0",
            "funding_gap": str(target),
            "currency": currency,
            "generation_status": generation_status,
            "counts": counts,
            "candidates": [candidate.model_dump(mode="json") for candidate in candidates],
        }

    def create_manual_override(
        self,
        *,
        customer_id: str,
        requirement_id: str,
        match_snapshot_id: str,
        product_match_item_id: int,
        new_status: str,
        operator_id: str,
        operator_name: str,
        reason: str,
    ) -> dict[str, Any]:
        self._prepare()
        if new_status != "conditional":
            raise FinancingPlanError("人工复核产品只允许转为条件性候选")
        if not reason.strip():
            raise FinancingPlanError("人工纳入条件性候选必须填写原因")
        with self.session_factory.begin() as db:
            requirement, snapshot, items, _, term, _ = self._load_context_from_db(
                db, customer_id, requirement_id, match_snapshot_id,
            )
            item = next((value for value in items if value.id == product_match_item_id), None)
            if item is None:
                raise FinancingPlanError("人工复核产品不属于指定匹配快照")
            candidate = self.rules.evaluate_candidate(item, term)
            if candidate.candidate_status != "manual_review":
                raise FinancingPlanError("只有人工复核候选可以人工纳入条件性候选")
            row = ManualCandidateOverride(
                override_id=uuid.uuid4().hex,
                customer_id=customer_id,
                requirement_id=requirement.requirement_id,
                match_snapshot_id=snapshot.snapshot_id,
                product_match_item_id=item.id,
                product_id=item.product_id,
                product_version_id=item.version_id,
                previous_status="manual_review",
                new_status="conditional",
                operator_id=operator_id,
                operator_name=operator_name,
                reason=reason.strip(),
            )
            db.add(row)
            db.flush()
            result = {
                "override_id": row.override_id, "customer_id": row.customer_id,
                "requirement_id": row.requirement_id, "match_snapshot_id": row.match_snapshot_id,
                "product_match_item_id": row.product_match_item_id, "product_id": row.product_id,
                "product_version_id": row.product_version_id, "previous_status": row.previous_status,
                "new_status": row.new_status, "operator_id": row.operator_id,
                "operator_name": row.operator_name, "reason": row.reason,
            }
        return result

    def generate_plan_combinations(
        self,
        customer_id: str,
        requirement_id: str,
        match_snapshot_id: str,
        *,
        strategy: CombinationStrategy | None = None,
    ) -> dict[str, Any]:
        _, _, _, target, term, _ = self._load_context(customer_id, requirement_id, match_snapshot_id)
        if term is None:
            raise FinancingPlanError("匹配快照缺少已确认融资期限")
        pool = self.build_plan_candidates(customer_id, requirement_id, match_snapshot_id)
        result = self.combinations.generate_combinations(
            {"amount": target, "term_months": term}, pool, strategy,
        )
        return result.model_dump(mode="json")

    @staticmethod
    def _find_combination(payload: dict[str, Any], combination_id: str) -> PlanCombinationResult | None:
        for group in ("formal_combinations", "conditional_combinations", "partial_combinations"):
            for value in payload.get(group) or []:
                if value.get("combination_id") == combination_id:
                    return PlanCombinationResult.model_validate(value)
        return None

    def create_plan_from_combination(
        self,
        customer_id: str,
        requirement_id: str,
        match_snapshot_id: str,
        combination_id: str,
        *,
        created_by: str,
    ) -> dict[str, Any]:
        generated = self.generate_plan_combinations(customer_id, requirement_id, match_snapshot_id)
        combination = self._find_combination(generated, combination_id)
        if combination is None:
            raise FinancingPlanError("组合不存在或已不再适用于当前匹配快照")
        plan_type: PlanType = (
            "conditional"
            if combination.plan_type == "conditional" or not combination.is_complete
            else ("primary" if combination.plan_type == "primary_candidate" else "backup")
        )
        input_items = [FinancingPlanItemInput(
            product_match_item_id=item.product_match_item_id,
            proposed_amount=item.proposed_amount,
            proposed_term_months=item.proposed_term_months,
            item_role="primary" if index == 0 else "supplementary",
            sequence_no=index + 1,
            reason="由确定性组合规则生成的规划金额，需业务人员复核。",
            conditions=item.conditions,
            risks=combination.risks,
            manual_approved=bool(item.manual_override_id),
        ) for index, item in enumerate(combination.items)]
        draft = FinancingPlanDraftInput(
            customer_id=customer_id,
            requirement_id=requirement_id,
            match_snapshot_id=match_snapshot_id,
            plan_type=plan_type,
            items=input_items,
            summary="融资方案组合草稿",
            rationale="基于已确认融资需求、客户匹配事实与产品匹配快照生成。",
            required_actions=combination.missing_information,
            conditions=combination.conditions,
        )
        return self.create_plan_draft(draft, created_by=created_by)

    @staticmethod
    def _condition_type(text: str, source_type: str) -> str:
        if source_type == "manual_review":
            return "manual_review"
        mapping = (
            ("抵押", "collateral"), ("房产", "collateral"), ("资质", "qualification"),
            ("科技", "qualification"), ("纳税", "tax"), ("税务", "tax"),
            ("征信", "credit"), ("逾期", "credit"), ("财务", "financial"),
        )
        return next((value for key, value in mapping if key in text), "missing_information" if source_type == "missing_information" else "other")

    @staticmethod
    def _available_material_status(db, customer_id: str) -> tuple[dict[str, str], list[str]]:
        statuses: dict[str, str] = {}
        file_names: list[str] = []
        inspector = sa_inspect(db.connection())
        if inspector.has_table(Extraction.__tablename__):
            rows = list(db.scalars(select(Extraction).where(Extraction.customer_id == customer_id)))
            for row in rows:
                code = str(normalize_document_type_code(row.extraction_type) or row.extraction_type or "").lower()
                if not code:
                    continue
                status = "verified" if row.confirm_status == "confirmed" else "available"
                if statuses.get(code) != "verified":
                    statuses[code] = status
        if inspector.has_table(Document.__tablename__):
            documents = list(db.scalars(select(Document).where(
                Document.customer_id == customer_id, Document.is_active == 1,
            )))
            file_names = [str(row.file_name or "") for row in documents]
        return statuses, file_names

    def _generate_plan_artifacts(
        self,
        db,
        *,
        plan: FinancingPlan,
        version: FinancingPlanVersion,
        selected: list[tuple[FinancingPlanItemInput, FinancingPlanCandidate]],
        item_rows: list[FinancingPlanItem],
        actor: str,
    ) -> None:
        condition_values: list[dict[str, Any]] = []
        for (item_input, candidate), plan_item in zip(selected, item_rows):
            for source_type, values in (
                ("missing_information", candidate.missing_information),
                ("manual_review", candidate.review_reasons),
                ("combination_condition", item_input.conditions),
            ):
                for text in values:
                    condition_values.append({
                        "plan_item_id": plan_item.plan_item_id,
                        "condition_type": self._condition_type(str(text), source_type),
                        "title": str(text), "description": str(text),
                        "source_type": source_type, "required": 1,
                    })
        db.flush()
        gaps = list(db.scalars(select(FinancingPlanGap).where(
            FinancingPlanGap.plan_version_id == version.plan_version_id,
        )))
        for gap in gaps:
            condition_values.append({
                "plan_item_id": None,
                "condition_type": "missing_information" if gap.gap_type == "missing_data" else "other",
                "title": gap.description, "description": gap.description,
                "source_type": "plan_gap", "required": 1,
                "source_fact_field": gap.related_fact_field,
            })
        seen_conditions: set[tuple[str | None, str, str]] = set()
        condition_rows: list[FinancingPlanCondition] = []
        for index, value in enumerate(condition_values):
            identity = (value.get("plan_item_id"), value["source_type"], value["title"])
            if identity in seen_conditions:
                continue
            seen_conditions.add(identity)
            row = FinancingPlanCondition(
                condition_id=uuid.uuid4().hex,
                plan_version_id=version.plan_version_id,
                plan_item_id=value.get("plan_item_id"),
                condition_type=value["condition_type"], title=value["title"],
                description=value["description"], source_type=value["source_type"],
                source_fact_field=value.get("source_fact_field"), status="pending",
                required=value.get("required", 1), sort_order=index, updated_by=actor,
            )
            db.add(row)
            condition_rows.append(row)

        inspector = sa_inspect(db.connection())
        versions: dict[str, FinancingProductVersion] = {}
        rules: list[FinancingProductRule] = []
        version_ids = [candidate.product_version_id for _, candidate in selected]
        if version_ids and inspector.has_table(FinancingProductVersion.__tablename__):
            versions = {row.version_id: row for row in db.scalars(select(FinancingProductVersion).where(
                FinancingProductVersion.version_id.in_(version_ids),
            ))}
        if version_ids and inspector.has_table(FinancingProductRule.__tablename__):
            rules = list(db.scalars(select(FinancingProductRule).where(
                FinancingProductRule.version_id.in_(version_ids),
                FinancingProductRule.rule_group == "materials",
            )))
        material_values: list[dict[str, Any]] = []
        item_by_version = {candidate.product_version_id: row for (_, candidate), row in zip(selected, item_rows)}
        candidate_by_version = {candidate.product_version_id: candidate for _, candidate in selected}
        for version_id, product_version in versions.items():
            candidate = candidate_by_version[version_id]
            for material in _material_rows(product_version.materials_json):
                material.update({
                    "plan_item_id": item_by_version[version_id].plan_item_id,
                    "source_type": "product_version_materials",
                    "required_by": candidate.external_product_code,
                })
                material_values.append(material)
        for rule in rules:
            candidate = candidate_by_version.get(rule.version_id)
            if candidate is None:
                continue
            for material in _material_rows(rule.expected_value_json):
                material.update({
                    "plan_item_id": item_by_version[rule.version_id].plan_item_id,
                    "source_type": "product_rule", "source_rule_id": rule.rule_id,
                    "required_by": candidate.external_product_code,
                })
                material_values.append(material)
        for condition in condition_rows:
            for alias, code in MATERIAL_ALIASES.items():
                if alias in condition.title:
                    material_values.append({
                        "name": alias, "code": code, "category": condition.condition_type,
                        "required": True, "plan_item_id": condition.plan_item_id,
                        "source_type": "plan_condition", "required_by": "",
                    })
        available_statuses, file_names = self._available_material_status(db, plan.customer_id)
        deduped_materials: dict[str, dict[str, Any]] = {}
        for value in material_values:
            code, name = _material_identity(str(value.get("name") or ""), str(value.get("code") or ""))
            if not code or not name:
                continue
            stored = deduped_materials.setdefault(code, {
                **value, "code": code, "name": name, "required_by_products": [],
            })
            required_by = str(value.get("required_by") or "")
            if required_by and required_by not in stored["required_by_products"]:
                stored["required_by_products"].append(required_by)
            stored["required"] = bool(stored.get("required", True) or value.get("required", True))
        for code, value in deduped_materials.items():
            status = available_statuses.get(code, "missing")
            if status == "missing" and any(value["name"] in file_name for file_name in file_names):
                status = "available"
            db.add(FinancingPlanMaterial(
                material_id=uuid.uuid4().hex, plan_version_id=version.plan_version_id,
                plan_item_id=value.get("plan_item_id"), material_code=code,
                material_name=value["name"], material_category=str(value.get("category") or "other"),
                required=1 if value.get("required", True) else 0, status=status,
                source_type=str(value.get("source_type") or "product_version_materials"),
                source_product_rule_id=value.get("source_rule_id"), notes=str(value.get("notes") or ""),
                required_by_products_json=_json(value["required_by_products"]), updated_by=actor,
            ))

        target = Decimal(version.target_amount)
        covered = Decimal(version.covered_amount)
        gap = Decimal(version.funding_gap)
        key_conditions = [value["title"] for value in condition_values]
        next_actions = [f"处理方案条件：{title}" for title in dict.fromkeys(key_conditions)]
        if gap > 0:
            next_actions.append("补充或复核候选产品后重新生成融资组合。")
        risk_notes = list(dict.fromkeys(
            risk for item, _ in selected for risk in item.risks if risk
        ))
        db.add(FinancingPlanExplanation(
            explanation_id=uuid.uuid4().hex, plan_version_id=version.plan_version_id,
            plan_summary=f"本方案目标融资{target}元，由{len(item_rows)}个产品组成。",
            coverage_summary=f"规划覆盖{covered}元，当前资金缺口{gap}元。",
            product_structure_json=_json([{
                "institution_name": row.institution_name, "product_name": row.product_name,
                "proposed_amount": str(row.proposed_amount), "proposed_term_months": row.proposed_term_months,
                "item_role": row.item_role,
            } for row in item_rows]),
            key_conditions_json=_json(list(dict.fromkeys(key_conditions))),
            funding_gap_summary=("融资需求已实现规划金额完整覆盖。" if gap == 0 else f"尚有{gap}元融资需求未覆盖。"),
            risk_notes_json=_json(risk_notes), next_actions_json=_json(next_actions), generated_by="template",
        ))

    def _create_version(self, db, plan: FinancingPlan, payload: FinancingPlanDraftInput, actor: str, version_no: int) -> FinancingPlanVersion:
        requirement, snapshot, match_items, target, requirement_term, currency = self._load_context_from_db(
            db,
            payload.customer_id, payload.requirement_id, payload.match_snapshot_id,
        )
        candidates = {candidate.product_match_item_id: candidate for candidate in (
            self.rules.evaluate_candidate(item, requirement_term) for item in match_items
        )}
        if not payload.items:
            raise FinancingPlanError("融资方案至少需要一个产品项")
        covered = Decimal("0")
        selected: list[tuple[FinancingPlanItemInput, FinancingPlanCandidate]] = []
        for item in payload.items:
            candidate = candidates.get(item.product_match_item_id)
            if candidate is None:
                raise FinancingPlanError("方案产品不属于指定匹配快照")
            self.rules.validate_plan_item_status(candidate, manual_approved=item.manual_approved)
            self.rules.validate_plan_amount(candidate, item.proposed_amount)
            self.rules.validate_plan_term(candidate, item.proposed_term_months)
            covered += _decimal(item.proposed_amount, "建议金额")
            selected.append((item, candidate))
        generation_status, gap = self.rules.evaluate_plan(
            target_amount=target,
            covered_amount=covered,
            candidates=[candidate for _, candidate in selected],
        )
        has_review = any(candidate.candidate_status in {"conditional", "manual_review"} for _, candidate in selected)
        plan_version = FinancingPlanVersion(
            plan_version_id=uuid.uuid4().hex,
            financing_plan_id=plan.financing_plan_id,
            version_no=version_no,
            plan_type=payload.plan_type,
            target_amount=target,
            covered_amount=covered,
            funding_gap=gap,
            currency=currency,
            summary=payload.summary,
            rationale=payload.rationale,
            status="needs_review" if has_review else "draft",
            generation_status=generation_status,
            required_actions_json=_json(payload.required_actions),
            missing_information_json=_json([text for _, candidate in selected for text in candidate.missing_information]),
            conditions_json=_json(payload.conditions),
            source_requirement_version=requirement.version,
            source_facts_hash=snapshot.facts_hash,
            source_match_snapshot_id=snapshot.snapshot_id,
            source_catalog_hash=snapshot.catalog_version_hash,
            created_by=actor,
        )
        db.add(plan_version)
        db.flush()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        item_rows: list[FinancingPlanItem] = []
        for item, candidate in selected:
            row = FinancingPlanItem(
                plan_item_id=uuid.uuid4().hex,
                plan_version_id=plan_version.plan_version_id,
                product_id=candidate.product_id,
                product_version_id=candidate.product_version_id,
                product_match_item_id=candidate.product_match_item_id,
                external_product_code=candidate.external_product_code,
                institution_name=candidate.institution_name,
                product_name=candidate.product_name,
                proposed_amount=item.proposed_amount,
                proposed_term_months=item.proposed_term_months,
                match_status=candidate.match_status,
                item_role=item.item_role,
                sequence_no=item.sequence_no,
                reason=item.reason,
                notes=item.notes,
                conditions_json=_json(item.conditions),
                risks_json=_json(item.risks),
                manual_approved_by=actor if item.manual_approved else None,
                manual_approved_at=now if item.manual_approved else None,
            )
            db.add(row)
            item_rows.append(row)
        if gap > 0:
            db.add(FinancingPlanGap(
                gap_id=uuid.uuid4().hex,
                plan_version_id=plan_version.plan_version_id,
                gap_type="amount_gap",
                description=f"当前方案尚有{gap}元融资需求未覆盖。",
                severity="hard",
            ))
        db.flush()
        self._generate_plan_artifacts(
            db, plan=plan, version=plan_version, selected=selected,
            item_rows=item_rows, actor=actor,
        )
        plan.current_version_id = plan_version.plan_version_id
        plan.source_match_snapshot_id = snapshot.snapshot_id
        plan.requirement_version = requirement.version
        plan.status = plan_version.status
        return plan_version

    def create_plan_draft(self, payload: FinancingPlanDraftInput, *, created_by: str) -> dict[str, Any]:
        self._prepare()
        plan = FinancingPlan(
            financing_plan_id=uuid.uuid4().hex,
            customer_id=payload.customer_id,
            requirement_id=payload.requirement_id,
            requirement_version=0,
            source_match_snapshot_id=payload.match_snapshot_id,
            status="draft",
            created_by=created_by,
        )
        with self.session_factory.begin() as db:
            db.add(plan)
            db.flush()
            self._create_version(db, plan, payload, created_by, 1)
            plan_id = plan.financing_plan_id
        return self.get_plan(plan_id)

    def create_new_plan_version(self, plan_id: str, payload: FinancingPlanDraftInput, *, created_by: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == plan_id))
            if plan is None:
                raise LookupError("融资方案不存在")
            if plan.customer_id != payload.customer_id or plan.requirement_id != payload.requirement_id:
                raise FinancingPlanError("新版本必须属于原客户和融资需求")
            current = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == plan.current_version_id))
            if current is not None and current.status in {"draft", "needs_review", "confirmed"}:
                current.status = "superseded"
                db.flush()
            version_no = int(db.scalar(select(func.max(FinancingPlanVersion.version_no)).where(
                FinancingPlanVersion.financing_plan_id == plan_id,
            )) or 0) + 1
            self._create_version(db, plan, payload, created_by, version_no)
        return self.get_plan(plan_id)

    def update_plan_draft(self, plan_id: str, payload: FinancingPlanDraftInput, *, updated_by: str) -> dict[str, Any]:
        """Create an audited next version after revalidating every edited item."""
        return self.create_new_plan_version(plan_id, payload, created_by=updated_by)

    def confirm_plan(self, plan_id: str, *, confirmed_by: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == plan_id))
            if plan is None:
                raise LookupError("融资方案不存在")
            version = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == plan.current_version_id))
            if version is None:
                raise FinancingPlanError("融资方案当前版本不存在")
            if version.status == "confirmed":
                return self.get_plan(plan_id)
            errors = self._confirmation_errors(db, plan, version)
            if errors:
                raise FinancingPlanError("；".join(errors))
            items = list(db.scalars(select(FinancingPlanItem).where(FinancingPlanItem.plan_version_id == version.plan_version_id)))
            self.rules.validate_plan(Decimal(version.covered_amount), Decimal(version.funding_gap), version.generation_status)
            if version.funding_gap == 0 and version.generation_status not in {"complete", "manual_review_required"}:
                raise FinancingPlanError("零资金缺口方案的完整性状态不正确")
            version.status = "confirmed"
            version.confirmed_by = confirmed_by
            version.confirmed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            plan.status = "confirmed"
        return self.get_plan(plan_id)

    def _confirmation_errors(self, db, plan: FinancingPlan, version: FinancingPlanVersion) -> list[str]:
        errors: list[str] = []
        requirement = db.scalar(select(FinancingRequirement).where(
            FinancingRequirement.requirement_id == plan.requirement_id,
        ))
        if requirement is None or requirement.status != "confirmed":
            errors.append("融资需求当前不是已确认状态")
        elif requirement.version != version.source_requirement_version:
            errors.append("融资需求版本已变化，请重新生成方案")
        snapshot = db.scalar(select(ProductMatchSnapshot).where(
            ProductMatchSnapshot.snapshot_id == version.source_match_snapshot_id,
        ))
        if snapshot is None:
            errors.append("来源产品匹配快照不存在")
        items = list(db.scalars(select(FinancingPlanItem).where(
            FinancingPlanItem.plan_version_id == version.plan_version_id,
        )))
        if not items:
            errors.append("方案没有产品项")
        if any(item.match_status in {"ineligible", "product_configuration_error"} for item in items):
            errors.append("方案包含不可用产品")
        if any(item.match_status == "manual_review" and not item.manual_approved_by for item in items):
            errors.append("方案包含尚未人工确认的产品")
        pending = list(db.scalars(select(FinancingPlanCondition).where(
            FinancingPlanCondition.plan_version_id == version.plan_version_id,
            FinancingPlanCondition.required == 1,
            FinancingPlanCondition.status == "pending",
        )))
        if pending:
            errors.append(f"仍有{len(pending)}项必需条件待处理")
        covered = sum((Decimal(item.proposed_amount) for item in items), Decimal("0"))
        expected_gap = max(Decimal(version.target_amount) - covered, Decimal("0"))
        if Decimal(version.covered_amount) != covered or Decimal(version.funding_gap) != expected_gap:
            errors.append("方案覆盖金额或资金缺口需要重新计算")
        if version.generation_status == "complete" and expected_gap != 0:
            errors.append("完整方案必须实现零资金缺口")
        return errors

    def validate_plan_for_confirmation(self, plan_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == plan_id))
            if plan is None:
                raise LookupError("融资方案不存在")
            version = db.scalar(select(FinancingPlanVersion).where(
                FinancingPlanVersion.plan_version_id == plan.current_version_id,
            ))
            if version is None:
                raise FinancingPlanError("融资方案当前版本不存在")
            errors = self._confirmation_errors(db, plan, version)
            return {"valid": not errors, "errors": errors, "plan_version_id": version.plan_version_id}

    def get_conditions(self, plan_id: str) -> list[dict[str, Any]]:
        plan = self.get_plan(plan_id)
        version_id = plan["current_version_id"]
        with self.session_factory() as db:
            rows = list(db.scalars(select(FinancingPlanCondition).where(
                FinancingPlanCondition.plan_version_id == version_id,
            ).order_by(FinancingPlanCondition.sort_order, FinancingPlanCondition.id)))
            return [{
                "condition_id": row.condition_id, "plan_version_id": row.plan_version_id,
                "plan_item_id": row.plan_item_id, "condition_type": row.condition_type,
                "title": row.title, "description": row.description, "source_type": row.source_type,
                "source_rule_id": row.source_rule_id, "source_fact_field": row.source_fact_field,
                "status": row.status, "required": bool(row.required), "sort_order": row.sort_order,
            } for row in rows]

    def update_condition(self, plan_id: str, condition_id: str, *, status: str, actor: str) -> dict[str, Any]:
        if status not in {"pending", "satisfied", "waived", "not_applicable"}:
            raise FinancingPlanError("条件状态不合法")
        plan = self.get_plan(plan_id)
        with self.session_factory.begin() as db:
            version = db.scalar(select(FinancingPlanVersion).where(
                FinancingPlanVersion.plan_version_id == plan["current_version_id"],
            ))
            if version is None or version.status == "confirmed":
                raise FinancingPlanError("已确认方案的条件不可修改")
            row = db.scalar(select(FinancingPlanCondition).where(
                FinancingPlanCondition.condition_id == condition_id,
                FinancingPlanCondition.plan_version_id == version.plan_version_id,
            ))
            if row is None:
                raise LookupError("方案条件不存在")
            row.status = status
            row.updated_by = actor
        return next(value for value in self.get_conditions(plan_id) if value["condition_id"] == condition_id)

    def get_materials(self, plan_id: str) -> list[dict[str, Any]]:
        plan = self.get_plan(plan_id)
        with self.session_factory() as db:
            rows = list(db.scalars(select(FinancingPlanMaterial).where(
                FinancingPlanMaterial.plan_version_id == plan["current_version_id"],
            ).order_by(FinancingPlanMaterial.id)))
            return [{
                "material_id": row.material_id, "plan_version_id": row.plan_version_id,
                "plan_item_id": row.plan_item_id, "material_code": row.material_code,
                "material_name": row.material_name, "material_category": row.material_category,
                "required": bool(row.required), "status": row.status, "source_type": row.source_type,
                "source_product_rule_id": row.source_product_rule_id, "notes": row.notes,
                "required_by_products": _list(row.required_by_products_json),
            } for row in rows]

    def update_material(self, plan_id: str, material_id: str, *, status: str, actor: str) -> dict[str, Any]:
        if status not in {"missing", "available", "uploaded", "verified", "not_applicable"}:
            raise FinancingPlanError("材料状态不合法")
        plan = self.get_plan(plan_id)
        with self.session_factory.begin() as db:
            version = db.scalar(select(FinancingPlanVersion).where(
                FinancingPlanVersion.plan_version_id == plan["current_version_id"],
            ))
            if version is None or version.status == "confirmed":
                raise FinancingPlanError("已确认方案的材料不可修改")
            row = db.scalar(select(FinancingPlanMaterial).where(
                FinancingPlanMaterial.material_id == material_id,
                FinancingPlanMaterial.plan_version_id == version.plan_version_id,
            ))
            if row is None:
                raise LookupError("方案材料不存在")
            row.status = status
            row.updated_by = actor
        return next(value for value in self.get_materials(plan_id) if value["material_id"] == material_id)

    @staticmethod
    def _version_dict(version: FinancingPlanVersion) -> dict[str, Any]:
        return {
            "plan_version_id": version.plan_version_id,
            "financing_plan_id": version.financing_plan_id,
            "version_no": version.version_no,
            "plan_type": version.plan_type,
            "target_amount": str(version.target_amount),
            "covered_amount": str(version.covered_amount),
            "funding_gap": str(version.funding_gap),
            "currency": version.currency,
            "summary": version.summary,
            "rationale": version.rationale,
            "status": version.status,
            "generation_status": version.generation_status,
            "required_actions": _list(version.required_actions_json),
            "missing_information": _list(version.missing_information_json),
            "conditions": _list(version.conditions_json),
            "source_requirement_version": version.source_requirement_version,
            "source_facts_hash": version.source_facts_hash,
            "source_match_snapshot_id": version.source_match_snapshot_id,
            "source_catalog_hash": version.source_catalog_hash,
            "created_by": version.created_by,
            "created_at": version.created_at.isoformat() if version.created_at else None,
            "confirmed_by": version.confirmed_by,
            "confirmed_at": version.confirmed_at.isoformat() if version.confirmed_at else None,
        }

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            plan = db.scalar(select(FinancingPlan).where(FinancingPlan.financing_plan_id == plan_id))
            if plan is None:
                raise LookupError("融资方案不存在")
            versions = list(db.scalars(select(FinancingPlanVersion).where(
                FinancingPlanVersion.financing_plan_id == plan_id,
            ).order_by(FinancingPlanVersion.version_no)))
            result_versions = []
            for version in versions:
                value = self._version_dict(version)
                rows = list(db.scalars(select(FinancingPlanItem).where(
                    FinancingPlanItem.plan_version_id == version.plan_version_id,
                ).order_by(FinancingPlanItem.sequence_no, FinancingPlanItem.id)))
                gaps = list(db.scalars(select(FinancingPlanGap).where(
                    FinancingPlanGap.plan_version_id == version.plan_version_id,
                ).order_by(FinancingPlanGap.id)))
                value["items"] = [{
                    "plan_item_id": row.plan_item_id,
                    "product_match_item_id": row.product_match_item_id,
                    "product_id": row.product_id,
                    "product_version_id": row.product_version_id,
                    "external_product_code": row.external_product_code,
                    "institution_name": row.institution_name,
                    "product_name": row.product_name,
                    "proposed_amount": str(row.proposed_amount),
                    "proposed_term_months": row.proposed_term_months,
                    "match_status": row.match_status,
                    "item_role": row.item_role,
                    "sequence_no": row.sequence_no,
                    "reason": row.reason,
                    "notes": row.notes,
                    "conditions": _list(row.conditions_json),
                    "risks": _list(row.risks_json),
                    "manual_approved_by": row.manual_approved_by,
                } for row in rows]
                value["gaps"] = [{
                    "gap_id": gap.gap_id,
                    "gap_type": gap.gap_type,
                    "description": gap.description,
                    "related_product_id": gap.related_product_id,
                    "related_fact_field": gap.related_fact_field,
                    "severity": gap.severity,
                } for gap in gaps]
                conditions = list(db.scalars(select(FinancingPlanCondition).where(
                    FinancingPlanCondition.plan_version_id == version.plan_version_id,
                ).order_by(FinancingPlanCondition.sort_order, FinancingPlanCondition.id)))
                materials = list(db.scalars(select(FinancingPlanMaterial).where(
                    FinancingPlanMaterial.plan_version_id == version.plan_version_id,
                ).order_by(FinancingPlanMaterial.id)))
                explanation = db.scalar(select(FinancingPlanExplanation).where(
                    FinancingPlanExplanation.plan_version_id == version.plan_version_id,
                ))
                value["condition_checklist"] = [{
                    "condition_id": row.condition_id, "plan_item_id": row.plan_item_id,
                    "condition_type": row.condition_type, "title": row.title,
                    "description": row.description, "source_type": row.source_type,
                    "status": row.status, "required": bool(row.required),
                } for row in conditions]
                value["material_checklist"] = [{
                    "material_id": row.material_id, "plan_item_id": row.plan_item_id,
                    "material_code": row.material_code, "material_name": row.material_name,
                    "material_category": row.material_category, "required": bool(row.required),
                    "status": row.status, "source_type": row.source_type,
                    "required_by_products": _list(row.required_by_products_json), "notes": row.notes,
                } for row in materials]
                value["explanation"] = ({
                    "plan_summary": explanation.plan_summary,
                    "coverage_summary": explanation.coverage_summary,
                    "product_structure": _list(explanation.product_structure_json),
                    "key_conditions": _list(explanation.key_conditions_json),
                    "funding_gap_summary": explanation.funding_gap_summary,
                    "risk_notes": _list(explanation.risk_notes_json),
                    "next_actions": _list(explanation.next_actions_json),
                    "generated_by": explanation.generated_by,
                } if explanation else None)
                result_versions.append(value)
            return {
                "financing_plan_id": plan.financing_plan_id,
                "customer_id": plan.customer_id,
                "requirement_id": plan.requirement_id,
                "requirement_version": plan.requirement_version,
                "source_match_snapshot_id": plan.source_match_snapshot_id,
                "current_version_id": plan.current_version_id,
                "status": plan.status,
                "created_by": plan.created_by,
                "versions": result_versions,
            }

    def get_latest(self, customer_id: str) -> dict[str, Any] | None:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingPlan).where(
                FinancingPlan.customer_id == customer_id,
            ).order_by(FinancingPlan.updated_at.desc(), FinancingPlan.id.desc()))
            return self.get_plan(row.financing_plan_id) if row else None

    def list_customer_plans(self, customer_id: str, requirement_id: str | None = None) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            statement = select(FinancingPlan.financing_plan_id).where(FinancingPlan.customer_id == customer_id)
            if requirement_id:
                statement = statement.where(FinancingPlan.requirement_id == requirement_id)
            ids = list(db.scalars(statement.order_by(FinancingPlan.updated_at.desc(), FinancingPlan.id.desc())))
        return [self.get_plan(value) for value in ids]


__all__ = [
    "CandidateStatus", "FinancingPlanCandidate", "FinancingPlanDraftInput", "FinancingPlanError",
    "FinancingPlanItemInput", "FinancingPlanService", "GenerationStatus", "PlanRuleEngine",
]

