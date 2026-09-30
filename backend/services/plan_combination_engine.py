"""Deterministic financing-plan combination generation.

The engine consumes only a confirmed requirement projection and the structured
candidate pool created from a ProductMatchSnapshot.  Stable ordering is used
for reproducibility; it is not a recommendation or business ranking.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Any, Literal

from pydantic import BaseModel, Field


CombinationPlanType = Literal["primary_candidate", "backup_candidate", "conditional"]
CombinationGenerationStatus = Literal[
    "complete", "partial", "insufficient_candidates", "manual_review_required"
]


class CombinationStrategy(BaseModel):
    max_products: int = 3
    max_results_per_group: int = 3


class CombinationGap(BaseModel):
    gap_type: Literal["amount_gap", "term_gap", "missing_data", "manual_review", "product_rule_gap"]
    description: str
    related_product_id: str | None = None
    related_fact_field: str | None = None
    severity: Literal["hard", "review", "info"] = "info"


class PlanCombinationItem(BaseModel):
    product_match_item_id: int
    product_id: str
    product_version_id: str
    external_product_code: str
    institution_name: str
    product_name: str
    match_status: str
    candidate_status: str
    manual_override_id: str | None = None
    proposed_amount: Decimal
    proposed_term_months: int
    allocation_cap: Decimal
    min_amount: Decimal | None = None
    conditions: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)
    review_reasons: list[str] = Field(default_factory=list)


class PlanCombinationResult(BaseModel):
    combination_id: str
    plan_type: CombinationPlanType
    generation_status: Literal["complete", "partial"]
    target_amount: Decimal
    covered_amount: Decimal
    funding_gap: Decimal
    items: list[PlanCombinationItem]
    product_count: int
    institution_count: int
    institution_concentration: bool
    conditions: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    gaps: list[CombinationGap] = Field(default_factory=list)
    is_complete: bool
    requires_manual_review: bool
    source_match_snapshot_id: str


class PlanCombinationResponse(BaseModel):
    customer_id: str
    requirement_id: str
    requirement_version: int
    source_match_snapshot_id: str
    target_amount: Decimal
    covered_amount: Decimal
    funding_gap: Decimal
    currency: str
    generation_status: CombinationGenerationStatus
    formal_combinations: list[PlanCombinationResult] = Field(default_factory=list)
    conditional_combinations: list[PlanCombinationResult] = Field(default_factory=list)
    partial_combinations: list[PlanCombinationResult] = Field(default_factory=list)
    manual_review_candidates: list[dict[str, Any]] = Field(default_factory=list)
    excluded_candidates: list[dict[str, Any]] = Field(default_factory=list)


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


class PlanCombinationEngine:
    """Enumerate up to three-product combinations without scoring products."""

    @staticmethod
    def _stable_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            candidates,
            key=lambda value: (
                -(_decimal(value.get("eligible_amount_cap")) or Decimal("-1")),
                str(value.get("product_version_id") or ""),
            ),
        )

    @staticmethod
    def _identity(snapshot_id: str, items: list[PlanCombinationItem], kind: str) -> str:
        payload = {
            "snapshot_id": snapshot_id,
            "kind": kind,
            "products": sorted(
                (item.product_version_id, str(item.proposed_amount)) for item in items
            ),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _allocate(
        subset: tuple[dict[str, Any], ...],
        *,
        target: Decimal,
        term_months: int,
        snapshot_id: str,
        conditional: bool,
    ) -> PlanCombinationResult | None:
        if len({str(value.get("product_version_id")) for value in subset}) != len(subset):
            return None
        remaining = target
        items: list[PlanCombinationItem] = []
        conditions: list[str] = []
        missing: list[str] = []
        gaps: list[CombinationGap] = []
        for candidate in subset:
            cap = _decimal(candidate.get("eligible_amount_cap"))
            minimum = _decimal(candidate.get("min_amount"))
            if cap is None or cap <= 0 or remaining <= 0:
                return None
            amount = min(cap, remaining)
            if minimum is not None and amount < minimum:
                return None
            max_term = candidate.get("max_term_months")
            if max_term is not None and term_months > int(max_term):
                return None
            if max_term is None:
                if candidate.get("candidate_status") != "conditional":
                    return None
                text = f"{candidate.get('product_name') or '该产品'}最长期限尚未明确，需人工核验。"
                missing.append(text)
                gaps.append(CombinationGap(
                    gap_type="term_gap", description=text,
                    related_product_id=str(candidate.get("product_id") or ""),
                    related_fact_field="requirement.term_months", severity="review",
                ))
            item_conditions = list(candidate.get("blocking_reasons") or [])
            item_conditions.extend(candidate.get("review_reasons") or [])
            item_missing = list(candidate.get("missing_information") or [])
            conditions.extend(item_conditions)
            missing.extend(item_missing)
            items.append(PlanCombinationItem(
                product_match_item_id=int(candidate["product_match_item_id"]),
                product_id=str(candidate.get("product_id") or ""),
                product_version_id=str(candidate.get("product_version_id") or ""),
                external_product_code=str(candidate.get("external_product_code") or ""),
                institution_name=str(candidate.get("institution_name") or ""),
                product_name=str(candidate.get("product_name") or ""),
                match_status=str(candidate.get("match_status") or ""),
                candidate_status=str(candidate.get("candidate_status") or ""),
                manual_override_id=candidate.get("manual_override_id"),
                proposed_amount=amount,
                proposed_term_months=term_months,
                allocation_cap=cap,
                min_amount=minimum,
                conditions=_unique(item_conditions),
                missing_information=_unique(item_missing),
                review_reasons=_unique(list(candidate.get("review_reasons") or [])),
            ))
            remaining -= amount
        covered = target - remaining
        if covered <= 0:
            return None
        if remaining > 0:
            gaps.append(CombinationGap(
                gap_type="amount_gap", description=f"尚有{remaining}元融资需求未覆盖。",
                severity="hard",
            ))
        institutions = {item.institution_name for item in items}
        concentration = len(institutions) < len(items)
        risks = ["方案金额为规划金额，不代表银行最终批复金额。"]
        if concentration:
            risks.append("组合包含同一机构的多个产品，需核验授信额度及产品是否可同时使用。")
        is_complete = remaining == 0
        kind = "conditional" if conditional else ("complete" if is_complete else "partial")
        return PlanCombinationResult(
            combination_id=PlanCombinationEngine._identity(snapshot_id, items, kind),
            plan_type="conditional" if conditional else "backup_candidate",
            generation_status="complete" if is_complete else "partial",
            target_amount=target,
            covered_amount=covered,
            funding_gap=remaining,
            items=items,
            product_count=len(items),
            institution_count=len(institutions),
            institution_concentration=concentration,
            conditions=_unique(conditions),
            missing_information=_unique(missing),
            risks=risks,
            gaps=gaps,
            is_complete=is_complete,
            requires_manual_review=conditional or bool(missing),
            source_match_snapshot_id=snapshot_id,
        )

    @staticmethod
    def _representatives(values: list[PlanCombinationResult], limit: int) -> list[PlanCombinationResult]:
        deduped = {value.combination_id: value for value in values}
        ordered = sorted(
            deduped.values(),
            key=lambda value: (
                0 if value.is_complete else 1,
                value.product_count,
                value.funding_gap,
                len(value.conditions) + len(value.missing_information),
                value.combination_id,
            ),
        )
        return ordered[:limit]

    def generate_combinations(
        self,
        requirement: dict[str, Any],
        candidate_pool: dict[str, Any],
        strategy: CombinationStrategy | None = None,
    ) -> PlanCombinationResponse:
        policy = strategy or CombinationStrategy()
        max_products = min(max(policy.max_products, 1), 3)
        limit = min(max(policy.max_results_per_group, 1), 3)
        target = _decimal(requirement.get("amount"))
        term = requirement.get("term_months")
        if target is None or target <= 0:
            raise ValueError("融资需求金额必须是大于0的明确数值")
        if term is None or int(term) <= 0:
            raise ValueError("融资需求期限必须是大于0的明确月份数")
        term_months = int(term)
        snapshot_id = str(candidate_pool.get("match_snapshot_id") or "")
        candidates = list(candidate_pool.get("candidates") or [])
        usable = self._stable_candidates([value for value in candidates if value.get("candidate_status") == "usable"])
        conditional_only = [value for value in candidates if value.get("candidate_status") == "conditional"]
        conditional_pool = self._stable_candidates(usable + conditional_only)
        manual = [value for value in candidates if value.get("candidate_status") == "manual_review"]
        excluded = [value for value in candidates if value.get("candidate_status") == "excluded"]

        formal_all: list[PlanCombinationResult] = []
        for size in range(1, min(max_products, len(usable)) + 1):
            for subset in combinations(usable, size):
                result = self._allocate(
                    subset, target=target, term_months=term_months,
                    snapshot_id=snapshot_id, conditional=False,
                )
                if result is not None:
                    formal_all.append(result)

        conditional_all: list[PlanCombinationResult] = []
        conditional_ids = {str(value.get("product_version_id")) for value in conditional_only}
        for size in range(1, min(max_products, len(conditional_pool)) + 1):
            for subset in combinations(conditional_pool, size):
                if not any(str(value.get("product_version_id")) in conditional_ids for value in subset):
                    continue
                result = self._allocate(
                    subset, target=target, term_months=term_months,
                    snapshot_id=snapshot_id, conditional=True,
                )
                if result is not None:
                    conditional_all.append(result)

        formal_complete = self._representatives([value for value in formal_all if value.is_complete], limit)
        for index, result in enumerate(formal_complete):
            result.plan_type = "primary_candidate" if index == 0 else "backup_candidate"
        conditional_results = self._representatives(conditional_all, limit)
        partial_results = self._representatives([value for value in formal_all if not value.is_complete], limit)

        if formal_complete:
            status: CombinationGenerationStatus = "complete"
            covered = target
        elif conditional_results:
            status = "manual_review_required"
            covered = max((value.covered_amount for value in conditional_results), default=Decimal("0"))
        elif partial_results:
            status = "partial"
            covered = max(value.covered_amount for value in partial_results)
        elif manual:
            status = "manual_review_required"
            covered = Decimal("0")
        else:
            status = "insufficient_candidates"
            covered = Decimal("0")
        return PlanCombinationResponse(
            customer_id=str(candidate_pool.get("customer_id") or ""),
            requirement_id=str(candidate_pool.get("requirement_id") or ""),
            requirement_version=int(candidate_pool.get("requirement_version") or 0),
            source_match_snapshot_id=snapshot_id,
            target_amount=target,
            covered_amount=covered,
            funding_gap=max(target - covered, Decimal("0")),
            currency=str(candidate_pool.get("currency") or "CNY"),
            generation_status=status,
            formal_combinations=formal_complete,
            conditional_combinations=conditional_results,
            partial_combinations=partial_results,
            manual_review_candidates=manual,
            excluded_candidates=excluded,
        )


__all__ = [
    "CombinationGap", "CombinationStrategy", "PlanCombinationEngine", "PlanCombinationItem",
    "PlanCombinationResponse", "PlanCombinationResult",
]
