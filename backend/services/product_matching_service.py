"""Run and persist deterministic product matching snapshots."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

from sqlalchemy import func, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    FinancingProductVersion,
    ProductMatchItem,
    ProductMatchSnapshot,
)
from backend.services.customer_matching_facts_service import (
    CustomerMatchingFacts,
    build_customer_matching_facts,
)
from backend.services.product_catalog_service import ProductCatalogService
from backend.services.product_rule_engine import ProductEvaluationResult, ProductRuleEngine


class ProductMatchingError(ValueError):
    pass


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"不能序列化 {type(value).__name__}")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _facts_payload(facts: CustomerMatchingFacts) -> dict[str, Any]:
    payload = facts.model_dump(mode="json")
    # Generation time is metadata, not a customer fact. Excluding it makes an
    # otherwise identical context idempotent while facts_json still records it.
    payload.pop("generated_at", None)
    return payload


def _builtin_rules(entry: dict[str, Any]) -> list[dict[str, Any]]:
    version = entry["version"]
    version_id = str(version["version_id"])
    rules: list[dict[str, Any]] = []

    def add(name: str, field: str, operator: str, expected: Any, message: str) -> None:
        rules.append({
            "rule_id": f"builtin:{version_id}:{name}", "version_id": version_id,
            "rule_source": "system_builtin", "rule_group": "product_boundary",
            "field_name": field, "operator": operator, "expected_value": expected,
            "severity": "hard", "failure_action": "exclude", "message": message,
            "source_text": "产品已发布结构化字段", "sort_order": -100,
        })

    if version.get("max_amount") not in (None, ""):
        add("max_amount", "requirement.amount", "lte", version["max_amount"], "融资金额不得超过产品最高额度")
    if version.get("min_amount") not in (None, ""):
        add("min_amount", "requirement.amount", "gte", version["min_amount"], "融资金额不得低于产品最低额度")
    if version.get("max_term_months") not in (None, ""):
        add("max_term_months", "requirement.term_months", "lte", version["max_term_months"], "融资期限不得超过产品最长期限")
    return rules


def _counts(items: list[dict[str, Any]]) -> dict[str, int]:
    names = ("eligible", "conditional", "ineligible", "manual_review", "product_configuration_error")
    return {name: sum(item["overall_status"] == name for item in items) for name in names}


class ProductMatchingService:
    def __init__(
        self,
        *,
        session_factory=SessionLocal,
        catalog_service: ProductCatalogService | None = None,
        facts_builder: Callable[..., Awaitable[CustomerMatchingFacts]] = build_customer_matching_facts,
        rule_engine: ProductRuleEngine | None = None,
        ensure_schema: bool = True,
    ) -> None:
        self.session_factory = session_factory
        self.catalog = catalog_service or ProductCatalogService(session_factory=session_factory, ensure_schema=ensure_schema)
        self.facts_builder = facts_builder
        self.rule_engine = rule_engine or ProductRuleEngine()
        self.ensure_schema = ensure_schema
        self._schema_ready = False

    def _prepare(self) -> None:
        if self.ensure_schema and not self._schema_ready:
            with self.session_factory() as db:
                Base.metadata.create_all(
                    bind=db.get_bind(),
                    tables=[ProductMatchSnapshot.__table__, ProductMatchItem.__table__],
                    checkfirst=True,
                )
            self._schema_ready = True

    def _published_count(self) -> int:
        with self.session_factory() as db:
            return int(db.scalar(select(func.count()).select_from(FinancingProductVersion).where(
                FinancingProductVersion.status == "published",
            )) or 0)

    @staticmethod
    def _serialize_evaluation(entry: dict[str, Any], evaluation: ProductEvaluationResult) -> dict[str, Any]:
        value = evaluation.model_dump(mode="json")
        status = "product_configuration_error" if evaluation.evaluation_status == "product_configuration_error" else evaluation.overall_status
        if not entry.get("rules") and status not in {"ineligible", "product_configuration_error"}:
            status = "manual_review"
            reason = "该产品尚未配置足够结构化准入规则，暂不能自动判断。"
            if reason not in value["review_reasons"]:
                value["review_reasons"].append(reason)
        value["overall_status"] = status
        value["product_category"] = entry["product"].get("product_category") or ""
        value["max_amount"] = entry["version"].get("max_amount")
        value["max_term_months"] = entry["version"].get("max_term_months")
        return value

    async def run_product_matching(
        self, customer_id: str, requirement_id: str, *, generated_by: str = "", as_of_date: date | None = None,
    ) -> dict[str, Any]:
        self._prepare()
        day = as_of_date or date.today()
        try:
            facts = await self.facts_builder(customer_id, requirement_id)
        except (LookupError, ValueError) as exc:
            raise ProductMatchingError(str(exc)) from exc
        if facts.customer_id != customer_id or facts.requirement_id != requirement_id:
            raise ProductMatchingError("客户事实与融资需求不一致")

        active = self.catalog.get_active_products(day)
        published_count = self._published_count()
        active_rule_count = sum(len(entry.get("rules") or []) for entry in active)
        precheck = {
            "active_product_count": len(active), "published_product_count": published_count,
            "active_rule_count": active_rule_count,
        }
        if not active:
            return {
                "status": "no_active_products", "snapshot_id": None, "reused": False,
                "message": "当前产品库尚无已发布产品，请先完成产品发布。",
                "precheck": precheck, "summary": {**_counts([]), "total": 0}, "items": [],
                "data_quality": facts.data_quality.model_dump(mode="json"),
            }

        facts_full = facts.model_dump(mode="json")
        facts_hash = _hash(_facts_payload(facts))
        catalog_hash = _hash(active)
        context_hash = _hash({
            "customer_id": customer_id, "requirement_id": requirement_id,
            "requirement_version": facts.requirement_version, "facts_hash": facts_hash,
            "catalog_as_of_date": day.isoformat(), "catalog_version_hash": catalog_hash,
        })
        with self.session_factory() as db:
            existing = db.scalar(select(ProductMatchSnapshot).where(ProductMatchSnapshot.context_hash == context_hash))
        if existing is not None:
            result = self.get_snapshot(existing.snapshot_id)
            result["reused"] = True
            result["precheck"] = precheck
            return result

        items: list[dict[str, Any]] = []
        for entry in active:
            explicit_rules = list(entry.get("rules") or [])
            all_rules = explicit_rules + _builtin_rules(entry)
            evaluation = self.rule_engine.evaluate_product(facts, entry["product"], entry["version"], all_rules)
            items.append(self._serialize_evaluation(entry, evaluation))

        counts = _counts(items)
        summary = {
            **counts, "total": len(items), "data_quality": facts.data_quality.model_dump(mode="json"),
            "boundary_notice": "匹配状态仅表示当前已知硬条件判断，不构成产品推荐或审批结论。",
        }
        snapshot_id = uuid.uuid4().hex
        generated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.session_factory.begin() as db:
            db.add(ProductMatchSnapshot(
                snapshot_id=snapshot_id, context_hash=context_hash, customer_id=customer_id,
                requirement_id=requirement_id, requirement_version=facts.requirement_version,
                facts_hash=facts_hash, facts_json=_canonical(facts_full), catalog_as_of_date=day,
                catalog_version_hash=catalog_hash, generated_at=generated_at, generated_by=generated_by,
                eligible_count=counts["eligible"], conditional_count=counts["conditional"],
                ineligible_count=counts["ineligible"], manual_review_count=counts["manual_review"],
                configuration_error_count=counts["product_configuration_error"],
                summary_json=_canonical(summary),
            ))
            for item in items:
                db.add(ProductMatchItem(
                    snapshot_id=snapshot_id, product_id=item["product_id"], version_id=item["version_id"],
                    external_product_code=item["external_product_code"], institution_name=item["institution_name"],
                    product_name=item["product_name"], product_category=item["product_category"],
                    max_amount=item["max_amount"], max_term_months=item["max_term_months"],
                    overall_status=item["overall_status"], hard_fail_count=item["hard_fail_count"],
                    hard_unknown_count=item["hard_unknown_count"], soft_fail_count=item["soft_fail_count"],
                    review_count=len(item["review_rules"]),
                    blocking_reasons_json=_canonical(item["blocking_reasons"]),
                    missing_information_json=_canonical(item["missing_information"]),
                    review_reasons_json=_canonical(item["review_reasons"]),
                    soft_gaps_json=_canonical(item["soft_gaps"]),
                    rule_results_json=_canonical(item["rule_results"]),
                ))
        result = self.get_snapshot(snapshot_id)
        result.update({"reused": False, "precheck": precheck})
        return result

    @staticmethod
    def _snapshot_dict(row: ProductMatchSnapshot) -> dict[str, Any]:
        return {
            "snapshot_id": row.snapshot_id, "customer_id": row.customer_id,
            "requirement_id": row.requirement_id, "requirement_version": row.requirement_version,
            "facts_snapshot_id": row.facts_snapshot_id, "facts_hash": row.facts_hash,
            "catalog_as_of_date": row.catalog_as_of_date.isoformat(),
            "catalog_version_hash": row.catalog_version_hash,
            "generated_at": row.generated_at.isoformat(), "generated_by": row.generated_by,
            "summary": json.loads(row.summary_json or "{}"),
        }

    @staticmethod
    def _item_dict(row: ProductMatchItem) -> dict[str, Any]:
        return {
            "product_id": row.product_id, "version_id": row.version_id,
            "external_product_code": row.external_product_code, "institution_name": row.institution_name,
            "product_name": row.product_name, "product_category": row.product_category,
            "max_amount": str(row.max_amount) if row.max_amount is not None else None,
            "max_term_months": row.max_term_months, "overall_status": row.overall_status,
            "hard_fail_count": row.hard_fail_count, "hard_unknown_count": row.hard_unknown_count,
            "soft_fail_count": row.soft_fail_count, "review_count": row.review_count,
            "blocking_reasons": json.loads(row.blocking_reasons_json or "[]"),
            "missing_information": json.loads(row.missing_information_json or "[]"),
            "review_reasons": json.loads(row.review_reasons_json or "[]"),
            "soft_gaps": json.loads(row.soft_gaps_json or "[]"),
            "rule_results": json.loads(row.rule_results_json or "[]"),
        }

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(ProductMatchSnapshot).where(ProductMatchSnapshot.snapshot_id == snapshot_id))
            if row is None:
                raise LookupError("产品匹配快照不存在")
            items = db.scalars(select(ProductMatchItem).where(
                ProductMatchItem.snapshot_id == snapshot_id,
            ).order_by(ProductMatchItem.id)).all()
            result = self._snapshot_dict(row)
            result.update({"status": "completed", "items": [self._item_dict(item) for item in items]})
            result["data_quality"] = result["summary"].get("data_quality", {})
            return result

    def get_latest(self, customer_id: str) -> dict[str, Any] | None:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(ProductMatchSnapshot).where(
                ProductMatchSnapshot.customer_id == customer_id,
            ).order_by(ProductMatchSnapshot.generated_at.desc(), ProductMatchSnapshot.id.desc()))
            return self.get_snapshot(row.snapshot_id) if row else None


__all__ = ["ProductMatchingError", "ProductMatchingService"]
