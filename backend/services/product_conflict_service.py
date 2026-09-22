"""Administrator decisions for duplicate Markdown product codes."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
import difflib
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from backend.db_models import FinancingProduct, FinancingProductConflict, FinancingProductRule, FinancingProductVersion
from backend.services.markdown_product_import_service import SOURCE_DIR, MarkdownProduct, scan_sources
from backend.services.product_catalog_service import CatalogError, ProductCatalogService, _JSON_FIELDS, validate_rule


MERGE_FIELDS = (
    "product_name", "institution_name", "product_category", "loan_type", "guarantee_type", "rate_text",
    "min_amount", "max_amount", "min_term_months", "max_term_months", "repayment_methods_json",
    "region_scope_json", "company_age_months", "borrower_age_min", "borrower_age_max", "tax_grade",
    "revenue_requirement", "tax_requirement", "invoice_requirement", "credit_overdue_requirement",
    "credit_query_requirement", "debt_requirement", "collateral_requirement", "materials_json",
    "suitable_customer_text", "notes", "guarantee_modes_json", "collateral_types_json",
)
RESOLUTION_STATUSES = {
    "keep_a": "resolved_keep_a", "keep_b": "resolved_keep_b",
    "merge": "resolved_merged", "split": "resolved_split",
}
_CODE = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*-\d+$")


def _plain(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _field(item: MarkdownProduct, name: str) -> Any:
    if name in {"product_name", "institution_name", "product_category"}:
        return getattr(item, name)
    return item.fields.get(name)


def _different(a: Any, b: Any) -> bool:
    return json.dumps(_plain(a), ensure_ascii=False, sort_keys=True) != json.dumps(_plain(b), ensure_ascii=False, sort_keys=True)


def _source_ref(item: MarkdownProduct, side: str, disposition: str) -> dict[str, Any]:
    return {"side": side, "disposition": disposition, "source_type": "local_markdown",
            "source_file": item.source_file, "source_category": item.product_category,
            "original_external_product_code": item.external_product_code,
            "product_name": item.product_name, "source_update_date": item.source_update_date.isoformat() if item.source_update_date else None,
            "source_snapshot_hash": item.source_snapshot_hash}


def _side(item: MarkdownProduct, side: str) -> dict[str, Any]:
    return {"side": side, "external_product_code": item.external_product_code,
            "product_name": item.product_name, "institution_name": item.institution_name,
            "product_category": item.product_category, "source_file": item.source_file,
            "source_update_date": item.source_update_date.isoformat() if item.source_update_date else None,
            "source_snapshot_hash": item.source_snapshot_hash, "source_snapshot": item.source_snapshot,
            "raw_fields": _plain(item.raw_fields), "structured_fields": _plain(item.fields)}


def _differences(a: MarkdownProduct, b: MarkdownProduct) -> list[dict[str, Any]]:
    result = []
    for name in MERGE_FIELDS:
        left, right = _field(a, name), _field(b, name)
        if _different(left, right):
            result.append({"field_name": name, "a": _plain(left), "b": _plain(right), "kind": "structured"})
    for key in sorted(set(a.raw_fields) | set(b.raw_fields)):
        left, right = a.raw_fields.get(key), b.raw_fields.get(key)
        if _different(left, right):
            result.append({"field_name": f"raw_fields.{key}", "a": _plain(left), "b": _plain(right), "kind": "raw"})
    return result


class ProductConflictService:
    def __init__(self, catalog: ProductCatalogService | None = None, *, directory=SOURCE_DIR):
        self.catalog = catalog or ProductCatalogService()
        self.directory = directory

    def _current(self, code: str) -> tuple[dict[str, Any], list[MarkdownProduct]]:
        scan = scan_sources(self.directory)
        items = scan["by_code"].get(code, [])
        if code not in scan["conflicting_codes"]:
            raise CatalogError("该编号不存在来源冲突")
        if len(items) != 2:
            raise CatalogError("当前仅支持两条来源的人工冲突处理")
        return scan, items

    def list_conflicts(self) -> dict[str, Any]:
        scan = scan_sources(self.directory)
        saved: dict[str, FinancingProductConflict] = {}
        database_status = "available"
        try:
            self.catalog._prepare()
            with self.catalog.session_factory() as db:
                saved = {row.external_product_code: row for row in db.scalars(select(FinancingProductConflict)).all()}
                return self._list_payload(scan, saved, database_status)
        except Exception:
            database_status = "unavailable"
            return self._list_payload(scan, {}, database_status)

    @staticmethod
    def _list_payload(scan: dict[str, Any], saved: dict[str, FinancingProductConflict], database_status: str) -> dict[str, Any]:
        result = []
        for row in scan["conflicts"]:
            code = row["external_product_code"]
            record = saved.get(code)
            hashes = [item.source_snapshot_hash for item in scan["by_code"][code]]
            current = bool(record and json.loads(record.source_hashes_json) == hashes and record.status.startswith("resolved_"))
            result.append({**row, "status": record.status if current else "unresolved",
                           "conflict": not current, "stale_resolution": bool(record and record.status.startswith("resolved_") and not current),
                           "resolved_by": record.resolved_by if current else None,
                           "resolved_at": record.resolved_at.isoformat() if current and record.resolved_at else None})
        return {"items": result, "total": len(result), "unresolved_count": sum(x["conflict"] for x in result),
                "database_status": database_status}

    def get_conflict(self, code: str) -> dict[str, Any]:
        _, items = self._current(code)
        summary = next(row for row in self.list_conflicts()["items"] if row["external_product_code"] == code)
        lines = list(difflib.ndiff(items[0].source_snapshot.splitlines(), items[1].source_snapshot.splitlines()))
        removed = [line[2:] for line in lines if line.startswith("- ")]
        added = [line[2:] for line in lines if line.startswith("+ ")]
        return {**summary, "sides": [_side(items[0], "a"), _side(items[1], "b")],
                "differences": _differences(items[0], items[1]), "source_line_changes": {"a_only": removed, "b_only": added}}

    def resolve(self, code: str, decision: dict[str, Any], actor: str) -> dict[str, Any]:
        self.catalog._prepare()
        scan, items = self._current(code)
        a, b = items
        strategy = decision.get("strategy")
        if strategy not in RESOLUTION_STATUSES:
            raise CatalogError("冲突处理方式不合法")
        field_choices = decision.get("field_choices") or {}
        rename_side = decision.get("rename_side")
        new_code = str(decision.get("new_code") or "").strip().upper()
        if strategy == "merge":
            differing = {row["field_name"] for row in _differences(a, b)}
            if set(field_choices) != differing or any(value not in {"a", "b"} for value in field_choices.values()):
                raise CatalogError("合并时须对每个差异字段选择来源 A 或 B")
        elif field_choices:
            raise CatalogError("非合并处理不接受逐字段选择")
        if strategy == "split":
            if rename_side not in {"a", "b"} or not _CODE.fullmatch(new_code) or new_code in scan["by_code"]:
                raise CatalogError("拆分须指定 A/B 及全库唯一的新产品编号")
        elif rename_side or new_code:
            raise CatalogError("只有拆分处理可以指定新编号")

        hashes = [item.source_snapshot_hash for item in items]
        normalized = {"strategy": strategy, "field_choices": field_choices if strategy == "merge" else {},
                      "rename_side": rename_side if strategy == "split" else None,
                      "new_code": new_code if strategy == "split" else None}
        decision_hash = hashlib.sha256(json.dumps({"code": code, "hashes": hashes, **normalized},
                                                  ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        with self.catalog.session_factory.begin() as db:
            record = db.scalar(select(FinancingProductConflict).where(
                FinancingProductConflict.external_product_code == code
            ).with_for_update())
            if strategy == "split" and db.scalar(select(FinancingProduct.product_id).where(
                FinancingProduct.external_product_code == new_code
            )):
                if record is None or record.decision_hash != decision_hash:
                    raise CatalogError("新产品编号已在数据库使用")
            if record and record.decision_hash == decision_hash and record.status == RESOLUTION_STATUSES[strategy]:
                existing = db.scalars(select(FinancingProductVersion).where(
                    FinancingProductVersion.conflict_code == code,
                    FinancingProductVersion.conflict_resolution_hash == decision_hash
                )).all()
                if existing:
                    return {"external_product_code": code, "status": record.status, "created_drafts": 0,
                            "version_ids": [row.version_id for row in existing], "conflict": False}

            source_sides = [_side(a, "a"), _side(b, "b")]
            if record is None:
                record = FinancingProductConflict(external_product_code=code)
                db.add(record)
            record.status = RESOLUTION_STATUSES[strategy]
            record.source_hashes_json = json.dumps(hashes)
            record.source_sides_json = json.dumps(source_sides, ensure_ascii=False)
            record.decision_json = json.dumps(normalized, ensure_ascii=False)
            record.decision_hash = decision_hash
            record.resolved_by = actor
            record.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)

            planned: list[tuple[str, MarkdownProduct | None, dict[str, Any] | None, str]] = []
            if strategy in {"keep_a", "keep_b"}:
                chosen = a if strategy == "keep_a" else b
                planned.append((code, chosen, None, "local_markdown"))
            elif strategy == "merge":
                planned.append((code, None, self._merge(a, b, field_choices), "merged_markdown"))
            else:
                original = b if rename_side == "a" else a
                renamed = a if rename_side == "a" else b
                planned.extend(((code, original, None, "local_markdown"), (new_code, renamed, None, "local_markdown")))

            version_ids = []
            for assigned_code, item, merged, source_type in planned:
                selected_side = "a" if item is a else "b" if item is b else "merged"
                refs = [_source_ref(source, side, self._disposition(strategy, side, selected_side, rename_side))
                        for side, source in (("a", a), ("b", b))]
                payload = merged if merged is not None else {
                    "institution_name": item.institution_name, "product_name": item.product_name,
                    "product_category": item.product_category, "fields": item.fields,
                    "snapshot": item.source_snapshot, "hash": item.source_snapshot_hash,
                    "source_file": item.source_file, "source_update_date": item.source_update_date,
                    "review": item.review, "reasons": item.review_reasons, "rules": item.rules,
                }
                version_ids.append(self._create_draft(db, assigned_code, code, decision_hash, payload,
                                                      source_type, refs, actor))
        return {"external_product_code": code, "status": RESOLUTION_STATUSES[strategy],
                "created_drafts": len(version_ids), "version_ids": version_ids, "conflict": False}

    @staticmethod
    def _disposition(strategy: str, side: str, selected_side: str, rename_side: str | None) -> str:
        if strategy in {"keep_a", "keep_b"}:
            return "selected" if side == selected_side else "duplicate_ignored"
        return "merged" if strategy == "merge" else ("reassigned_code" if side == rename_side else "original_code")

    @staticmethod
    def _merge(a: MarkdownProduct, b: MarkdownProduct, choices: dict[str, str]) -> dict[str, Any]:
        def pick(name: str) -> Any:
            return _field(a if choices.get(name, "a") == "a" else b, name)

        fields = {key: pick(key) for key in MERGE_FIELDS if key not in {"product_name", "institution_name", "product_category"} and pick(key) is not None}
        raw = {}
        for key in sorted(set(a.raw_fields) | set(b.raw_fields)):
            side = a if choices.get(f"raw_fields.{key}", "a") == "a" else b
            if key in side.raw_fields:
                raw[key] = side.raw_fields[key]
        fields["raw_fields_json"] = raw
        name = pick("product_name")
        fields["summary"] = f"{name}（人工合并来源草稿）"
        snapshot = (f"<!-- 人工合并来源；原始文档未修改 -->\n\n### 来源 A：{a.source_file}\n{a.source_snapshot}"
                    f"\n\n### 来源 B：{b.source_file}\n{b.source_snapshot}")
        return {"institution_name": pick("institution_name"), "product_name": name,
                "product_category": pick("product_category"), "fields": fields,
                "snapshot": snapshot, "hash": hashlib.sha256(snapshot.encode()).hexdigest(),
                "source_file": f"{a.source_file} + {b.source_file}",
                "source_update_date": max((d for d in (a.source_update_date, b.source_update_date) if d), default=None),
                "review": {key: "needs_review" for key in set(a.review) | set(b.review)},
                "reasons": ["人工合并了两个 Markdown 来源，所有字段及准入规则仍须复核"], "rules": []}

    @staticmethod
    def _create_draft(db, assigned_code: str, conflict_code: str, decision_hash: str,
                      payload: dict[str, Any], source_type: str, refs: list[dict[str, Any]], actor: str) -> str:
        product = db.scalar(select(FinancingProduct).where(
            FinancingProduct.external_product_code == assigned_code
        ).with_for_update())
        if product is None:
            product = FinancingProduct(product_id=uuid.uuid4().hex,
                                       identity_key=hashlib.sha256(f"code:{assigned_code}".encode()).hexdigest(),
                                       external_product_code=assigned_code,
                                       institution_name=payload["institution_name"], product_name=payload["product_name"],
                                       product_category=payload["product_category"], region_key="",
                                       source_type=source_type, source_ref=payload["source_file"])
            db.add(product)
            db.flush()
        number = db.scalar(select(FinancingProductVersion.version_number).where(
            FinancingProductVersion.product_id == product.product_id
        ).order_by(FinancingProductVersion.version_number.desc()).limit(1)) or 0
        version = FinancingProductVersion(
            version_id=uuid.uuid4().hex, product_id=product.product_id, version_number=number + 1,
            institution_name=payload["institution_name"], product_name=payload["product_name"], status="draft",
            source_snapshot=payload["snapshot"], source_snapshot_hash=payload["hash"], source_type=source_type,
            source_file=payload["source_file"], source_update_date=payload["source_update_date"],
            source_refs_json=json.dumps(refs, ensure_ascii=False), conflict_code=conflict_code,
            conflict_resolution_hash=decision_hash, source_node_token="", source_document_token="",
            source_imported_at=datetime.now(timezone.utc).replace(tzinfo=None), created_by=actor,
            needs_review=1, review_status="unreviewed",
            review_reasons_json=json.dumps(payload["reasons"] + [f"冲突 {conflict_code} 已人工决议，须复核"], ensure_ascii=False),
            field_review_json=json.dumps(payload["review"], ensure_ascii=False),
        )
        for key, value in payload["fields"].items():
            setattr(version, key, json.dumps(value, ensure_ascii=False, default=str) if key in _JSON_FIELDS else value)
        db.add(version)
        for candidate in payload["rules"]:
            db.add(FinancingProductRule(rule_id=uuid.uuid4().hex, version_id=version.version_id,
                                        **validate_rule(candidate)))
        db.flush()
        return version.version_id
