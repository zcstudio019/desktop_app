"""Step 7A product drafting and publishing. No customer matching lives here."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import Base, SessionLocal, engine
from backend.db_models import FinancingProduct, FinancingProductRule, FinancingProductVersion, FinancingProductConflict
from backend.services.feishu_product_import_service import ParsedProduct, parse_product_document
from backend.services.markdown_product_import_service import SOURCE_DIR, SOURCE_FILES, scan_sources
from backend.services.product_catalog_schema import ensure_markdown_catalog_schema


PRODUCT_CATEGORIES = frozenset({"enterprise_credit", "enterprise_mortgage", "personal", "other", *SOURCE_FILES})
VERSION_STATUSES = frozenset({"draft", "needs_review", "published", "expired", "superseded", "disabled"})
OPERATORS = frozenset({"eq", "ne", "in", "not_in", "gt", "gte", "lt", "lte", "between", "contains", "exists", "not_exists"})
SEVERITIES = frozenset({"hard", "soft", "info"})
FAILURE_ACTIONS = frozenset({"exclude", "conditional", "review"})
FIELD_TYPES = {
    "requirement.amount": "number", "requirement.purpose": "string", "requirement.term_months": "number",
    "requirement.accept_mortgage": "boolean", "requirement.accept_additional_guarantee": "boolean",
    "requirement.registered_region": "string", "requirement.operating_region": "string",
    "customer.company_age_months": "number", "customer.registered_region": "string",
    "customer.operating_region": "string", "customer.enterprise_type": "string",
    "credit.enterprise_overdue_count": "number", "credit.personal_overdue_count": "number",
    "credit.overdue_90d_count": "number", "credit.enterprise_outstanding_balance": "number",
    "credit.personal_outstanding_balance": "number", "credit.related_repayment_balance": "number",
    "financial.total_assets": "number", "financial.total_liabilities": "number", "financial.net_assets": "number",
    "financial.debt_asset_ratio": "number", "financial.revenue": "number", "financial.net_profit": "number",
    "cashflow.coverage_months": "number", "cashflow.operating_inflow": "number",
    "cashflow.average_monthly_operating_inflow": "number",
    "asset.has_real_estate": "boolean", "asset.has_vehicle": "boolean",
    "asset.has_equipment": "boolean", "asset.has_confirmed_collateral": "boolean",
}
_JSON_FIELDS = {"region_scope_json", "repayment_methods_json", "guarantee_modes_json", "collateral_types_json", "materials_json", "field_review_json", "raw_fields_json", "review_reasons_json", "source_refs_json"}
_EDIT_FIELDS = {"effective_from", "effective_to", "summary", "region_scope_json", "currency", "min_amount",
                "max_amount", "min_term_months", "max_term_months", "repayment_methods_json", "guarantee_modes_json",
                "collateral_types_json", "materials_json", "rate_text", "notes", "field_review_json",
                "loan_type", "guarantee_type", "company_age_months", "borrower_age_min", "borrower_age_max",
                "tax_grade", "revenue_requirement", "tax_requirement", "invoice_requirement",
                "credit_overdue_requirement", "credit_query_requirement", "debt_requirement",
                "collateral_requirement", "suitable_customer_text", "review_status"}
REVIEW_CORE_FIELDS = frozenset({"external_product_code", "institution_name", "product_name", "product_category"})
REVIEW_OPTIONAL_FIELDS = frozenset({"max_amount", "max_term_months", "region_scope", "guarantee_modes",
                                    "collateral_types", "materials", "company_age_rule"})
REVIEW_FIELDS = REVIEW_CORE_FIELDS | REVIEW_OPTIONAL_FIELDS
REVIEW_CONFIRMED = frozenset({"reviewed", "confirmed"})  # confirmed is retained for existing drafts.
REVIEW_EMPTY_ALLOWED = frozenset({"insufficient_data", "not_applicable", "rejected", "acknowledged_unknown"})
REVIEW_STATUSES = frozenset({"extracted_review", "needs_review"}) | REVIEW_CONFIRMED | REVIEW_EMPTY_ALLOWED
_TABLES_READY = False


class CatalogError(ValueError):
    pass


def _review_field_value(product: FinancingProduct, version: FinancingProductVersion, field_name: str) -> Any:
    values = {
        "external_product_code": product.external_product_code,
        "institution_name": version.institution_name or product.institution_name,
        "product_name": version.product_name or product.product_name,
        "product_category": product.product_category,
        "max_amount": version.max_amount,
        "max_term_months": version.max_term_months,
        "region_scope": json.loads(version.region_scope_json or "[]"),
        "guarantee_modes": json.loads(version.guarantee_modes_json or "[]"),
        "collateral_types": json.loads(version.collateral_types_json or "[]"),
        "materials": json.loads(version.materials_json or "[]"),
        "company_age_rule": version.company_age_months,
    }
    return values[field_name]


def _has_review_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def review_blockers(product: FinancingProduct, version: FinancingProductVersion) -> list[str]:
    """Return key fields that still require an explicit, truthful administrator decision."""
    review = json.loads(version.field_review_json or "{}")
    blockers = []
    for field_name in REVIEW_FIELDS:
        status = review.get(field_name)
        has_value = _has_review_value(_review_field_value(product, version, field_name))
        if field_name in REVIEW_CORE_FIELDS:
            if not has_value or status not in REVIEW_CONFIRMED:
                blockers.append(field_name)
        elif status in {"needs_review", "extracted_review", None, ""}:
            blockers.append(field_name)
        elif has_value and status not in REVIEW_CONFIRMED:
            blockers.append(field_name)
        elif not has_value and status not in REVIEW_CONFIRMED | REVIEW_EMPTY_ALLOWED:
            blockers.append(field_name)
    return sorted(blockers)


def _date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise CatalogError("日期必须采用 YYYY-MM-DD") from exc


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise CatalogError("金额不能是布尔值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CatalogError("金额必须是数字") from exc
    if not result.is_finite() or result < 0:
        raise CatalogError("金额必须是非负有限数字")
    return result


def _serialize(row: Any) -> dict[str, Any]:
    result = {}
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        if column.name in _JSON_FIELDS or column.name == "expected_value_json":
            value = json.loads(value or ("{}" if column.name in {"field_review_json", "raw_fields_json"} else "[]"))
        elif isinstance(value, (date, datetime)):
            value = value.isoformat()
        elif isinstance(value, Decimal):
            value = str(value)
        result[column.name] = value
    return result


def _identity_key(institution_name: str, product_name: str, category: str, region_key: str = "") -> str:
    parts = [" ".join(str(value).strip().casefold().split()) for value in (institution_name, product_name, category, region_key)]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def validate_rule(data: dict[str, Any]) -> dict[str, Any]:
    field_name = data.get("field_name")
    operator = data.get("operator")
    if field_name not in FIELD_TYPES:
        raise CatalogError("规则字段不在白名单")
    if operator not in OPERATORS:
        raise CatalogError("规则操作符不合法")
    severity = data.get("severity", "hard")
    action = data.get("failure_action", "review")
    if severity not in SEVERITIES or action not in FAILURE_ACTIONS:
        raise CatalogError("规则严重程度或失败动作不合法")
    expected = data.get("expected_value")
    kind = FIELD_TYPES[field_name]
    if operator in {"exists", "not_exists"}:
        if expected is not None:
            raise CatalogError("存在性规则的预期值必须为空")
    elif operator in {"in", "not_in", "between"}:
        if not isinstance(expected, list) or not expected or (operator == "between" and len(expected) != 2):
            raise CatalogError("集合或区间规则的预期值类型不正确")
        values = expected
    else:
        values = [expected]
    if operator not in {"exists", "not_exists"}:
        if operator in {"gt", "gte", "lt", "lte", "between"} and kind != "number":
            raise CatalogError("大小比较仅支持数值字段")
        if operator == "contains" and kind != "string":
            raise CatalogError("contains 仅支持文本字段")
        for item in values:
            if kind == "number":
                _decimal(item)
                if item is None:
                    raise CatalogError("数值规则不能使用空值")
            elif kind == "boolean" and not isinstance(item, bool):
                raise CatalogError("布尔规则预期值必须为 true 或 false")
            elif kind == "string" and (not isinstance(item, str) or not item.strip()):
                raise CatalogError("文本规则预期值必须为非空字符串")
        if operator == "between" and _decimal(values[0]) > _decimal(values[1]):
            raise CatalogError("区间上下界颠倒")
    source_text = str(data.get("source_text") or "").strip()
    if not source_text:
        raise CatalogError("规则必须有来源原文")
    return {"rule_group": str(data.get("rule_group") or "eligibility"), "field_name": field_name,
            "operator": operator, "expected_value_json": json.dumps(expected, ensure_ascii=False),
            "severity": severity, "failure_action": action, "message": str(data.get("message") or ""),
            "source_text": source_text, "sort_order": int(data.get("sort_order") or 0)}


def fact_state(facts: dict[str, Any], field_name: str) -> dict[str, Any]:
    """Resolve a future rule fact without converting missing values to false or zero."""
    if field_name not in FIELD_TYPES:
        raise CatalogError("规则字段不在白名单")
    value: Any = facts
    for part in field_name.split("."):
        if not isinstance(value, dict) or part not in value or value[part] is None or (isinstance(value[part], str) and not value[part].strip()):
            return {"status": "unknown", "reason": "insufficient_data", "value": None}
        value = value[part]
    kind = FIELD_TYPES[field_name]
    if kind == "boolean" and not isinstance(value, bool):
        return {"status": "unknown", "reason": "insufficient_data", "value": None}
    if kind == "string" and not isinstance(value, str):
        return {"status": "unknown", "reason": "insufficient_data", "value": None}
    if kind == "number":
        try:
            if _decimal(value) is None:
                raise CatalogError("empty")
        except CatalogError:
            return {"status": "unknown", "reason": "insufficient_data", "value": None}
    return {"status": "known", "value": value}


class ProductCatalogService:
    def __init__(self, session_factory=SessionLocal, *, ensure_schema: bool = True):
        self.session_factory = session_factory
        self.ensure_schema = ensure_schema

    def _prepare(self) -> None:
        global _TABLES_READY
        if self.ensure_schema and not _TABLES_READY:
            Base.metadata.create_all(bind=engine, tables=[FinancingProduct.__table__, FinancingProductVersion.__table__, FinancingProductRule.__table__, FinancingProductConflict.__table__], checkfirst=True)
            ensure_markdown_catalog_schema(engine)
            _TABLES_READY = True

    @staticmethod
    def _version(db: Session, version_id: str) -> FinancingProductVersion:
        row = db.scalar(select(FinancingProductVersion).where(FinancingProductVersion.version_id == version_id))
        if row is None:
            raise CatalogError("产品版本不存在")
        return row

    @staticmethod
    def _editable(row: FinancingProductVersion) -> None:
        if row.status not in {"draft", "needs_review"}:
            raise CatalogError("已发布或已结束的版本不可修改")

    def import_document(self, category: str, node_token: str, document_token: str, content: str,
                        actor: str, source_updated_at: datetime | None = None) -> list[dict[str, Any]]:
        self._prepare()
        if category not in PRODUCT_CATEGORIES - {"other"} or not node_token or not document_token:
            raise CatalogError("飞书来源配置不完整")
        candidates = parse_product_document(category, content)
        result: list[dict[str, Any]] = []
        with self.session_factory.begin() as db:
            for item in candidates:
                snapshot_hash = hashlib.sha256(item.snapshot.encode("utf-8")).hexdigest()
                product = db.scalar(select(FinancingProduct).where(
                    FinancingProduct.identity_key == _identity_key(item.institution_name, item.product_name, category),
                ))
                if product is None:
                    product = FinancingProduct(product_id=uuid.uuid4().hex,
                                               identity_key=_identity_key(item.institution_name, item.product_name, category),
                                               institution_name=item.institution_name,
                                               product_name=item.product_name, product_category=category, region_key="",
                                               source_type="feishu_wiki", source_ref=node_token)
                    db.add(product)
                    db.flush()
                existing = db.scalar(select(FinancingProductVersion).where(
                    FinancingProductVersion.product_id == product.product_id,
                    FinancingProductVersion.source_snapshot_hash == snapshot_hash,
                ))
                if existing:
                    result.append({"product_id": product.product_id, "version_id": existing.version_id, "status": existing.status, "created": False})
                    continue
                last_number = db.scalar(select(FinancingProductVersion.version_number).where(
                    FinancingProductVersion.product_id == product.product_id
                ).order_by(FinancingProductVersion.version_number.desc()).limit(1)) or 0
                version = FinancingProductVersion(
                    version_id=uuid.uuid4().hex, product_id=product.product_id, version_number=last_number + 1,
                    institution_name=item.institution_name, product_name=item.product_name,
                    status="draft", source_snapshot=item.snapshot, source_snapshot_hash=snapshot_hash,
                    source_type="feishu_wiki",
                    source_node_token=node_token, source_document_token=document_token,
                    source_imported_at=datetime.now(timezone.utc).replace(tzinfo=None), source_updated_at=source_updated_at,
                    created_by=actor, field_review_json=json.dumps(item.review, ensure_ascii=False),
                )
                for key, value in item.fields.items():
                    setattr(version, key, json.dumps(value, ensure_ascii=False) if key in _JSON_FIELDS else value)
                db.add(version)
                for candidate_rule in item.rules:
                    db.add(FinancingProductRule(rule_id=uuid.uuid4().hex, version_id=version.version_id,
                                                **validate_rule(candidate_rule)))
                result.append({"product_id": product.product_id, "version_id": version.version_id, "status": "draft", "created": True})
        return result

    def import_markdown_sources(self, actor: str, category: str | None = None, *, directory=SOURCE_DIR) -> dict[str, Any]:
        """Sync approved local files into drafts; conflicting codes never write a version."""
        self._prepare()
        if category is not None and category not in SOURCE_FILES:
            raise CatalogError("不支持的本地产品分类")
        scan = scan_sources(directory)
        selected = [s for s in scan["sources"] if category is None or s.category == category]
        if any(s.missing for s in selected):
            raise CatalogError("本地产品库文件缺失")
        created = unchanged = 0
        database_conflicts: list[dict[str, Any]] = []
        with self.session_factory.begin() as db:
            for source in selected:
                for item in source.products:
                    code = item.external_product_code
                    if code in scan["conflicting_codes"]:
                        continue
                    # Same code and hash in another file is a single source product.
                    if scan["by_code"][code][0] is not item:
                        continue
                    product = db.scalar(select(FinancingProduct).where(
                        FinancingProduct.external_product_code == code
                    ).with_for_update())
                    if product and (product.source_type != "local_markdown" or product.source_ref != item.source_file):
                        database_conflicts.append({"external_product_code": code, "status": "duplicate_conflict", "needs_review": True,
                                                   "sides": [{"source_file": product.source_ref, "product_name": product.product_name,
                                                              "snapshot_hash": "existing_database"},
                                                             {"source_file": item.source_file, "product_name": item.product_name,
                                                              "snapshot_hash": item.source_snapshot_hash}]})
                        continue
                    if product is None:
                        product = FinancingProduct(product_id=uuid.uuid4().hex,
                                                   identity_key=hashlib.sha256(f"code:{code}".encode()).hexdigest(),
                                                   external_product_code=code, institution_name=item.institution_name,
                                                   product_name=item.product_name, product_category=item.product_category,
                                                   region_key="", source_type="local_markdown", source_ref=item.source_file)
                        db.add(product)
                        db.flush()
                    existing = db.scalar(select(FinancingProductVersion).where(
                        FinancingProductVersion.product_id == product.product_id,
                        FinancingProductVersion.source_snapshot_hash == item.source_snapshot_hash,
                    ))
                    if existing:
                        unchanged += 1
                        continue
                    last_number = db.scalar(select(FinancingProductVersion.version_number).where(
                        FinancingProductVersion.product_id == product.product_id
                    ).order_by(FinancingProductVersion.version_number.desc()).limit(1)) or 0
                    version = FinancingProductVersion(
                        version_id=uuid.uuid4().hex, product_id=product.product_id, version_number=last_number + 1,
                        institution_name=item.institution_name, product_name=item.product_name,
                        status="draft", source_snapshot=item.source_snapshot,
                        source_snapshot_hash=item.source_snapshot_hash, source_type="local_markdown",
                        source_file=item.source_file, source_update_date=item.source_update_date,
                        source_node_token="", source_document_token="",
                        source_imported_at=datetime.now(timezone.utc).replace(tzinfo=None), created_by=actor,
                        needs_review=1, field_review_json=json.dumps(item.review, ensure_ascii=False),
                        review_status="unreviewed", review_reasons_json=json.dumps(item.review_reasons, ensure_ascii=False),
                    )
                    for key, value in item.fields.items():
                        setattr(version, key, json.dumps(value, ensure_ascii=False) if key in _JSON_FIELDS else value)
                    db.add(version)
                    for candidate_rule in item.rules:
                        db.add(FinancingProductRule(rule_id=uuid.uuid4().hex, version_id=version.version_id,
                                                    **validate_rule(candidate_rule)))
                    created += 1
        return {"category": category, "created_drafts": created, "unchanged": unchanged,
                "conflicts": scan["conflicts"] + database_conflicts,
                "parsed_count": sum(len(s.products) for s in selected),
                "unique_count": len({p.external_product_code for s in selected for p in s.products}),
                "duplicate_code_count": scan["duplicate_code_count"]}

    def get_version(self, version_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            version = self._version(db, version_id)
            product = db.scalar(select(FinancingProduct).where(FinancingProduct.product_id == version.product_id))
            rules = db.scalars(select(FinancingProductRule).where(FinancingProductRule.version_id == version_id).order_by(FinancingProductRule.sort_order)).all()
            return {"product": _serialize(product), "version": _serialize(version), "rules": [_serialize(rule) for rule in rules]}

    def list_versions(self, product_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            stmt = select(FinancingProductVersion, FinancingProduct).join(FinancingProduct, FinancingProduct.product_id == FinancingProductVersion.product_id)
            if product_id:
                stmt = stmt.where(FinancingProductVersion.product_id == product_id)
            if status:
                stmt = stmt.where(FinancingProductVersion.status == status)
            return [{"product": _serialize(product), "version": _serialize(version)} for version, product in db.execute(stmt.order_by(FinancingProductVersion.id.desc())).all()]

    def list_products(self, *, category: str | None = None, institution: str | None = None,
                      status: str | None = None, needs_review: bool | None = None,
                      search: str | None = None) -> list[dict[str, Any]]:
        """Return one row per product using its newest version for administrator filters."""
        latest: dict[str, dict[str, Any]] = {}
        for row in self.list_versions():
            product, version = row["product"], row["version"]
            if status and version["status"] != status:
                continue
            if needs_review is not None and bool(version["needs_review"]) != needs_review:
                continue
            key = product["product_id"]
            if key not in latest or version["version_number"] > latest[key]["version"]["version_number"]:
                latest[key] = row
        result = []
        for row in latest.values():
            product, version = row["product"], row["version"]
            if category and product["product_category"] != category:
                continue
            if institution and institution.casefold() not in version["institution_name"].casefold():
                continue
            if search and search.casefold() not in " ".join((product.get("external_product_code") or "",
                                                              version["product_name"], version["institution_name"])).casefold():
                continue
            result.append({"product": product, "version": version})
        return sorted(result, key=lambda row: ((row["product"].get("external_product_code") or ""), row["product"]["product_id"]))

    def get_product(self, product_id: str) -> dict[str, Any]:
        versions = self.list_versions(product_id=product_id)
        if not versions:
            raise CatalogError("产品不存在")
        versions.sort(key=lambda row: row["version"]["version_number"], reverse=True)
        return {"product": versions[0]["product"], "latest_version": versions[0]["version"],
                "versions": [row["version"] for row in versions]}

    def update_draft(self, version_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        self._prepare()
        if not patch or set(patch) - (_EDIT_FIELDS | {"institution_name", "product_name", "status"}):
            raise CatalogError("包含不允许编辑的产品字段")
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            self._editable(version)
            product = db.scalar(select(FinancingProduct).where(FinancingProduct.product_id == version.product_id))
            if "status" in patch:
                if patch["status"] not in {"draft", "needs_review"}:
                    raise CatalogError("草稿仅可切换为待审核")
                version.status = patch["status"]
            if ("institution_name" in patch or "product_name" in patch) and version.source_type == "local_markdown":
                for key in ("institution_name", "product_name"):
                    if key in patch:
                        value = str(patch[key]).strip()
                        if not value:
                            raise CatalogError("机构和产品名称不能为空")
                        setattr(version, key, value)
            elif "institution_name" in patch or "product_name" in patch:
                prior = db.scalar(select(FinancingProductVersion.version_id).where(
                    FinancingProductVersion.product_id == version.product_id,
                    FinancingProductVersion.version_id != version_id,
                ).limit(1))
                if prior:
                    raise CatalogError("已有历史版本时不能更改产品身份；请创建独立产品")
                for key in ("institution_name", "product_name"):
                    if key in patch:
                        value = str(patch[key]).strip()
                        if not value:
                            raise CatalogError("机构和产品名称不能为空")
                        setattr(product, key, value)
                product.identity_key = _identity_key(product.institution_name, product.product_name,
                                                     product.product_category, product.region_key)
            for key in _EDIT_FIELDS & patch.keys():
                value = patch[key]
                if key in {"effective_from", "effective_to"}:
                    value = _date(value)
                elif key in {"min_amount", "max_amount"}:
                    value = _decimal(value)
                elif key in {"min_term_months", "max_term_months"}:
                    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                        raise CatalogError("期限必须是非负整数月")
                elif key in _JSON_FIELDS:
                    if key == "field_review_json":
                        if not isinstance(value, dict):
                            raise CatalogError("字段审核结果必须是对象")
                        if any(status not in REVIEW_STATUSES for status in value.values()):
                            raise CatalogError("字段审核结果包含未知状态")
                    elif not isinstance(value, list):
                        raise CatalogError("产品列表字段必须是数组")
                    value = json.dumps(value, ensure_ascii=False)
                elif key in {"company_age_months", "borrower_age_min", "borrower_age_max"}:
                    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                        raise CatalogError("月数和年龄必须是非负整数")
                elif key == "review_status" and value not in {"unreviewed", "reviewing", "reviewed", "rejected"}:
                    raise CatalogError("人工审核状态不合法")
                setattr(version, key, value)
            if version.source_type in {"local_markdown", "merged_markdown"} and ({"field_review_json", "review_status"} & patch.keys()):
                version.needs_review = int(version.review_status != "reviewed" or bool(review_blockers(product, version)))
        return self.get_version(version_id)

    def add_rule(self, version_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._prepare()
        validated = validate_rule(data)
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            self._editable(version)
            rule = FinancingProductRule(rule_id=uuid.uuid4().hex, version_id=version_id, **validated)
            db.add(rule)
            db.flush()
            return _serialize(rule)

    def delete_rule(self, version_id: str, rule_id: str) -> None:
        self._prepare()
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            self._editable(version)
            rule = db.scalar(select(FinancingProductRule).where(FinancingProductRule.rule_id == rule_id, FinancingProductRule.version_id == version_id))
            if rule is None:
                raise CatalogError("规则不存在")
            db.delete(rule)

    def update_rule(self, version_id: str, rule_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._prepare()
        validated = validate_rule(data)
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            self._editable(version)
            rule = db.scalar(select(FinancingProductRule).where(
                FinancingProductRule.rule_id == rule_id, FinancingProductRule.version_id == version_id
            ))
            if rule is None:
                raise CatalogError("规则不存在")
            for key, value in validated.items():
                setattr(rule, key, value)
            db.flush()
            return _serialize(rule)

    @staticmethod
    def _validate_publish(product: FinancingProduct, version: FinancingProductVersion, rules: list[FinancingProductRule]) -> None:
        if not (version.institution_name or product.institution_name).strip() or not (version.product_name or product.product_name).strip() or product.product_category not in PRODUCT_CATEGORIES:
            raise CatalogError("机构、产品名称及类别必填")
        if version.source_type in {"local_markdown", "merged_markdown"} and not product.external_product_code:
            raise CatalogError("本地产品必须有稳定外部编号")
        if version.source_type in {"local_markdown", "merged_markdown"} and version.review_status != "reviewed":
            raise CatalogError("产品草稿尚未通过人工审核")
        if version.effective_from is None:
            raise CatalogError("发布日期起始日必填")
        if version.effective_to and version.effective_to <= version.effective_from:
            raise CatalogError("有效期结束日必须晚于起始日")
        if version.min_amount is not None and version.max_amount is not None and version.max_amount < version.min_amount:
            raise CatalogError("最高额度不能小于最低额度")
        if version.min_term_months is not None and version.max_term_months is not None and version.max_term_months < version.min_term_months:
            raise CatalogError("最长期限不能小于最短期限")
        if version.source_type in {"local_markdown", "merged_markdown"}:
            pending = review_blockers(product, version)
            if pending:
                raise CatalogError("关键产品字段尚未完成管理员复核：" + "、".join(pending))
        for rule in rules:
            validate_rule({"field_name": rule.field_name, "operator": rule.operator,
                           "expected_value": json.loads(rule.expected_value_json), "severity": rule.severity,
                           "failure_action": rule.failure_action, "source_text": rule.source_text})

    def publish(self, version_id: str, actor: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            self._editable(version)
            product = db.scalar(select(FinancingProduct).where(FinancingProduct.product_id == version.product_id).with_for_update())
            rules = db.scalars(select(FinancingProductRule).where(FinancingProductRule.version_id == version_id)).all()
            source_scan = scan_sources()
            if version.conflict_code:
                conflict = db.scalar(select(FinancingProductConflict).where(
                    FinancingProductConflict.external_product_code == version.conflict_code
                ))
                current = [item.source_snapshot_hash for item in source_scan["by_code"].get(version.conflict_code, [])]
                if (conflict is None or not conflict.status.startswith("resolved_")
                        or json.loads(conflict.source_hashes_json) != current
                        or version.conflict_resolution_hash != conflict.decision_hash):
                    raise CatalogError("冲突决议缺失或来源已变化，不能发布")
            elif product.external_product_code in source_scan["conflicting_codes"]:
                raise CatalogError("产品编号存在来源冲突，不能发布")
            self._validate_publish(product, version, rules)
            previous = db.scalars(select(FinancingProductVersion).where(
                FinancingProductVersion.product_id == version.product_id,
                FinancingProductVersion.status == "published",
                FinancingProductVersion.version_id != version_id,
            )).all()
            for old in previous:
                old.status = "superseded"
            version.status = "published"
            version.needs_review = 0
            version.published_by = actor
            version.published_at = datetime.now(timezone.utc).replace(tzinfo=None)
        return self.get_version(version_id)

    def change_lifecycle(self, version_id: str, status: str) -> dict[str, Any]:
        self._prepare()
        if status not in {"disabled", "expired"}:
            raise CatalogError("仅支持停用或过期")
        with self.session_factory.begin() as db:
            version = self._version(db, version_id)
            if version.status != "published":
                raise CatalogError("仅已发布版本可停用或标记过期")
            version.status = status
        return self.get_version(version_id)

    def get_active_products(self, as_of_date: date | str) -> list[dict[str, Any]]:
        self._prepare()
        day = _date(as_of_date)
        if day is None:
            raise CatalogError("查询日期必填")
        with self.session_factory() as db:
            stmt = select(FinancingProductVersion, FinancingProduct).join(
                FinancingProduct, FinancingProduct.product_id == FinancingProductVersion.product_id
            ).where(FinancingProductVersion.status == "published",
                    FinancingProductVersion.effective_from <= day,
                    (FinancingProductVersion.effective_to.is_(None) | (FinancingProductVersion.effective_to >= day)))
            result = []
            for version, product in db.execute(stmt).all():
                rules = db.scalars(select(FinancingProductRule).where(
                    FinancingProductRule.version_id == version.version_id
                ).order_by(FinancingProductRule.sort_order)).all()
                result.append({"product": _serialize(product), "version": _serialize(version),
                               "rules": [_serialize(rule) for rule in rules]})
            return result
