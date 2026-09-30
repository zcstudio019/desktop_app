"""Deterministic application material matching and package snapshots.

Customer documents remain the source of truth. Application materials and package
items only keep traceable references; original files are never renamed or copied.
"""
from __future__ import annotations

import hashlib
import html
import io
import json
import re
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from backend.database import Base, SessionLocal
from backend.db_models import (
    Customer, Document, Extraction, FinancingApplication, FinancingApplicationMaterial,
    FinancingApplicationPackage, FinancingApplicationPackageItem, FinancingMaterialEvent,
    FinancingSubmissionPackage, FinancingSupplementPackage, FinancingSupplementPackageItem,
    FinancingSupplementRequest,
)
from backend.services.financing_application_material_schema import ensure_application_material_schema


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


@dataclass(frozen=True)
class MaterialTypeDefinition:
    code: str
    label: str
    document_types: tuple[str, ...]
    default_owner_type: str
    package_section: str


MATERIAL_TYPES: tuple[MaterialTypeDefinition, ...] = (
    MaterialTypeDefinition("business_license", "营业执照", ("business_license",), "enterprise", "01_企业基础资料"),
    MaterialTypeDefinition("id_card", "身份证", ("id_card",), "person", "02_法人及实控人资料"),
    MaterialTypeDefinition("marriage_certificate", "结婚证", ("marriage_cert", "marriage_certificate"), "person", "02_法人及实控人资料"),
    MaterialTypeDefinition("real_estate_certificate", "房产证", ("collateral", "property_report"), "person", "06_资产资料"),
    MaterialTypeDefinition("enterprise_credit_report", "企业征信报告", ("enterprise_credit_report",), "enterprise", "03_征信资料"),
    MaterialTypeDefinition("personal_credit_report", "个人征信报告", ("personal_credit_report",), "person", "03_征信资料"),
    MaterialTypeDefinition("enterprise_bank_statement", "企业银行流水", ("enterprise_bank_statement", "enterprise_flow", "bank_statement", "bank_reconciliation_detail"), "enterprise", "04_银行流水"),
    MaterialTypeDefinition("personal_bank_statement", "个人银行流水", ("personal_flow", "personal_bank_statement"), "person", "04_银行流水"),
    MaterialTypeDefinition("financial_statement", "财务报表", ("financial_report",), "enterprise", "05_财务资料"),
    MaterialTypeDefinition("account_opening_permit", "开户许可证", ("account_license",), "enterprise", "01_企业基础资料"),
    MaterialTypeDefinition("company_articles", "公司章程", ("company_articles",), "enterprise", "01_企业基础资料"),
    MaterialTypeDefinition("shareholder_info", "股东信息", ("shareholder_info", "company_articles"), "enterprise", "01_企业基础资料"),
    MaterialTypeDefinition("tax_record", "纳税资料", ("tax_record", "personal_tax"), "enterprise", "07_产品专项材料"),
    MaterialTypeDefinition("invoice_record", "开票资料", ("invoice_record",), "enterprise", "07_产品专项材料"),
    MaterialTypeDefinition("other", "其他", (), "enterprise", "08_补充材料"),
)
MATERIAL_TYPE_REGISTRY = {value.code: value for value in MATERIAL_TYPES}
DOCUMENT_TO_MATERIAL = {doc: value.code for value in MATERIAL_TYPES for doc in value.document_types}

_NAME_HINTS = (
    ("营业执照", "business_license"), ("身份证", "id_card"), ("结婚证", "marriage_certificate"),
    ("房产", "real_estate_certificate"), ("不动产", "real_estate_certificate"),
    ("企业征信", "enterprise_credit_report"), ("个人征信", "personal_credit_report"),
    ("企业流水", "enterprise_bank_statement"), ("对公流水", "enterprise_bank_statement"),
    ("个人流水", "personal_bank_statement"), ("财务报表", "financial_statement"),
    ("开户许可证", "account_opening_permit"), ("公司章程", "company_articles"),
    ("股东", "shareholder_info"), ("纳税", "tax_record"), ("发票", "invoice_record"),
    ("开票", "invoice_record"),
)


class ApplicationMaterialError(ValueError):
    pass


def infer_material_type(material_name: str, document_type: str = "") -> str:
    normalized = str(document_type or "").strip().lower()
    if normalized in DOCUMENT_TO_MATERIAL:
        return DOCUMENT_TO_MATERIAL[normalized]
    for hint, code in _NAME_HINTS:
        if hint in str(material_name or ""):
            return code
    return "other"


def _safe_name(value: str, fallback: str = "材料") -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", "_", str(value or "")).strip(" .")
    return cleaned[:120] or fallback


class ApplicationMaterialMatchingService:
    MATCHED_STATUSES = {"matched", "uploaded", "verified"}

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
                FinancingApplicationMaterial.__table__, FinancingApplicationPackage.__table__,
                FinancingApplicationPackageItem.__table__, FinancingSubmissionPackage.__table__,
                FinancingSupplementPackage.__table__, FinancingSupplementPackageItem.__table__,
                FinancingMaterialEvent.__table__,
            ])

    @staticmethod
    def _event(db, application_id: str, event_type: str, actor_id: str, actor_name: str,
               *, material_id: str | None = None, package_id: str | None = None,
               payload: dict[str, Any] | None = None) -> None:
        db.add(FinancingMaterialEvent(
            material_event_id=uuid.uuid4().hex, application_id=application_id,
            application_material_id=material_id, package_id=package_id,
            event_type=event_type, operator_id=actor_id, operator_name=actor_name,
            payload_json=_json(payload or {}),
        ))

    @staticmethod
    def _application(db, application_id: str) -> FinancingApplication:
        row = db.scalar(select(FinancingApplication).where(FinancingApplication.application_id == application_id))
        if row is None:
            raise LookupError("融资申请不存在")
        return row

    def _document_metadata(self, db, document: Document) -> dict[str, Any]:
        extraction = db.scalar(select(Extraction).where(
            Extraction.doc_id == document.doc_id,
            Extraction.extraction_status == "success",
        ).order_by(Extraction.created_at.desc(), Extraction.id.desc()))
        payload = _load(extraction.confirmed_data if extraction and extraction.confirm_status == "confirmed" else (extraction.extracted_data if extraction else "{}"), {})
        subject = payload.get("subject") if isinstance(payload.get("subject"), dict) else {}
        owner = payload.get("owner") if isinstance(payload.get("owner"), dict) else {}
        return {
            "document_type": str((extraction.extraction_type if extraction else None) or document.file_type or ""),
            "owner_id": str(owner.get("id") or subject.get("id") or payload.get("owner_id") or ""),
            "owner_name": str(owner.get("name") or subject.get("name") or payload.get("owner_name") or payload.get("name") or ""),
            "owner_type": str(owner.get("type") or payload.get("owner_type") or ""),
            "period_start": payload.get("period_start") or payload.get("start_date"),
            "period_end": payload.get("period_end") or payload.get("end_date"),
            "confirmed": bool(extraction and extraction.confirm_status == "confirmed"),
            "extraction_id": extraction.extraction_id if extraction else None,
        }

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        try:
            return date.fromisoformat(str(value)[:10]) if value else None
        except ValueError:
            return None

    def _candidate(self, db, app: FinancingApplication, material: FinancingApplicationMaterial,
                   document: Document) -> dict[str, Any] | None:
        if document.customer_id != app.customer_id or not document.is_active:
            return None
        metadata = self._document_metadata(db, document)
        document_type = metadata["document_type"]
        actual_type = infer_material_type(document.file_name or "", document_type)
        if material.material_type != "other" and actual_type != material.material_type:
            return None
        if material.owner_id and metadata["owner_id"] and material.owner_id != metadata["owner_id"]:
            return None
        if material.owner_name and metadata["owner_name"] and material.owner_name != metadata["owner_name"]:
            return None
        expected_owner = material.owner_type or MATERIAL_TYPE_REGISTRY.get(material.material_type, MATERIAL_TYPE_REGISTRY["other"]).default_owner_type
        actual_owner = metadata["owner_type"]
        if expected_owner == "enterprise" and actual_owner in {"person", "personal", "individual"}:
            return None
        if expected_owner == "person" and actual_owner in {"enterprise", "company"}:
            return None
        valid_to = self._parse_date(document.valid_until)
        status = "expired" if valid_to and valid_to < date.today() else "matched"
        period_start = self._parse_date(metadata["period_start"])
        period_end = self._parse_date(metadata["period_end"])
        if material.material_type in {"enterprise_bank_statement", "personal_bank_statement"} and material.coverage_start:
            if not period_start or not period_end or period_start > material.coverage_start or (material.coverage_end and period_end < material.coverage_end):
                status = "needs_review"
        return {
            "customer_material_id": document.doc_id, "source_file_id": document.doc_id,
            "source_document_id": document.doc_id, "file_name": document.file_name,
            "file_hash": document.file_hash or "", "file_path": document.file_path,
            "document_type": document_type, "owner_id": metadata["owner_id"],
            "owner_name": metadata["owner_name"], "owner_type": metadata["owner_type"],
            "valid_to": document.valid_until or None, "period_start": metadata["period_start"],
            "period_end": metadata["period_end"], "confirmed": metadata["confirmed"],
            "extraction_id": metadata["extraction_id"], "status": status,
        }

    def match_customer_materials_to_application(self, application_id: str, *, actor_id: str,
                                                actor_name: str) -> dict[str, Any]:
        self._prepare()
        results = []
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            materials = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
            ).order_by(FinancingApplicationMaterial.id)))
            documents = list(db.scalars(select(Document).where(
                Document.customer_id == app.customer_id, Document.is_active == 1,
            ).order_by(Document.upload_time.desc(), Document.id.desc())))
            for material in materials:
                candidates = [value for document in documents if (value := self._candidate(db, app, material, document))]
                active = [value for value in candidates if value["status"] != "expired"]
                if not candidates:
                    result = "missing"
                elif not active:
                    result = "expired"
                elif len(active) > 1:
                    result = "multiple_matches"
                elif active[0]["status"] == "needs_review":
                    result = "needs_review"
                    material.status = "needs_review"
                    chosen = active[0]
                    material.customer_material_id = chosen["customer_material_id"]
                    material.source_file_id = chosen["source_file_id"]
                    material.source_document_id = chosen["source_document_id"]
                    material.file_reference = chosen["file_path"]
                else:
                    result = "matched"
                    chosen = active[0]
                    material.customer_material_id = chosen["customer_material_id"]
                    material.source_file_id = chosen["source_file_id"]
                    material.source_document_id = chosen["source_document_id"]
                    material.file_reference = chosen["file_path"]
                    material.valid_to = self._parse_date(chosen["valid_to"])
                    material.coverage_start = self._parse_date(chosen["period_start"])
                    material.coverage_end = self._parse_date(chosen["period_end"])
                    material.status = "verified" if chosen["confirmed"] else "matched"
                    if chosen["confirmed"]:
                        material.verified_by = "confirmed_extraction"
                        material.verified_at = _now()
                if result == "expired":
                    material.status = "expired"
                self._event(db, application_id, "material_matched" if result == "matched" else "material_match_reviewed",
                            actor_id, actor_name, material_id=material.application_material_id,
                            payload={"result": result, "candidate_count": len(candidates)})
                results.append({"application_material_id": material.application_material_id,
                                "material_name": material.material_name, "result": result,
                                "candidates": candidates})
        return {"application_id": application_id, "results": results, "summary": self.material_summary(application_id)}

    def select_customer_material(self, application_id: str, material_id: str, document_id: str, *,
                                 actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            material = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            ))
            document = db.scalar(select(Document).where(Document.doc_id == document_id))
            if material is None or document is None:
                raise LookupError("申请材料或客户资料不存在")
            candidate = self._candidate(db, app, material, document)
            if candidate is None:
                raise ApplicationMaterialError("所选资料不属于当前客户、主体或材料类型")
            if candidate["status"] == "expired":
                material.status = "expired"
            else:
                material.status = "verified" if candidate["confirmed"] else ("needs_review" if candidate["status"] == "needs_review" else "matched")
            material.customer_material_id = document.doc_id; material.source_file_id = document.doc_id
            material.source_document_id = document.doc_id; material.file_reference = document.file_path
            material.valid_to = self._parse_date(candidate["valid_to"])
            material.coverage_start = self._parse_date(candidate["period_start"])
            material.coverage_end = self._parse_date(candidate["period_end"])
            if material.status == "verified": material.verified_by = actor_id; material.verified_at = _now()
            self._event(db, application_id, "material_matched", actor_id, actor_name,
                        material_id=material_id, payload={"document_id": document_id, "status": material.status})
        return self.get_material(application_id, material_id)

    def add_material(self, application_id: str, *, material_type: str, material_name: str,
                     owner_type: str, owner_id: str | None, owner_name: str | None,
                     required: bool, required_verified: bool, source_type: str, source_id: str | None,
                     actor_id: str, actor_name: str, supplement_request_id: str | None = None) -> dict[str, Any]:
        if material_type not in MATERIAL_TYPE_REGISTRY:
            raise ApplicationMaterialError("材料类型不在统一登记表中")
        if source_type not in {"plan_material", "product_requirement", "supplement_request", "manual", "system_mapping"}:
            raise ApplicationMaterialError("材料来源不合法")
        self._prepare(); material_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            self._application(db, application_id)
            db.add(FinancingApplicationMaterial(
                application_material_id=material_id, application_id=application_id,
                supplement_request_id=supplement_request_id, material_type=material_type,
                material_name=material_name, material_category=material_type,
                owner_type=owner_type, owner_id=owner_id, owner_name=owner_name,
                required=int(required), required_verified=int(required_verified), status="required_missing",
                source_type=source_type, source_id=source_id, notes="",
            ))
            self._event(db, application_id, "material_required", actor_id, actor_name,
                        material_id=material_id, payload={"source_type": source_type})
        return self.get_material(application_id, material_id)

    def verify_material(self, application_id: str, material_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            material = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            ))
            if material is None: raise LookupError("申请材料不存在")
            if not material.source_document_id and not material.file_reference:
                raise ApplicationMaterialError("材料尚未关联文件")
            material.status = "verified"; material.verified_by = actor_id; material.verified_at = _now()
            self._event(db, application_id, "material_verified", actor_id, actor_name, material_id=material_id)
        return self.get_material(application_id, material_id)

    def replace_material(self, application_id: str, material_id: str, document_id: str, *,
                         rejection_reason: str, actor_id: str, actor_name: str) -> dict[str, Any]:
        if not rejection_reason.strip(): raise ApplicationMaterialError("请填写原材料退回原因")
        self._prepare(); new_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            old = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            )); document = db.scalar(select(Document).where(Document.doc_id == document_id))
            if old is None or document is None: raise LookupError("申请材料或替换文件不存在")
            candidate = self._candidate(db, app, old, document)
            if candidate is None: raise ApplicationMaterialError("替换文件不属于当前客户、主体或材料类型")
            old.status = "rejected"; old.rejection_reason = rejection_reason
            new = FinancingApplicationMaterial(
                application_material_id=new_id, application_id=application_id,
                supplement_request_id=old.supplement_request_id, material_type=old.material_type,
                material_name=old.material_name, material_category=old.material_category,
                owner_type=old.owner_type, owner_id=old.owner_id, owner_name=old.owner_name,
                required=old.required, required_verified=old.required_verified,
                status="verified" if candidate["confirmed"] else "matched",
                source_type=old.source_type, source_id=old.source_id,
                source_document_id=document.doc_id, customer_material_id=document.doc_id,
                source_file_id=document.doc_id, file_reference=document.file_path,
                version_no=old.version_no + 1, replaces_material_id=old.application_material_id,
                notes=old.notes,
            )
            db.add(new)
            self._event(db, application_id, "material_rejected", actor_id, actor_name,
                        material_id=old.application_material_id, payload={"reason": rejection_reason})
            self._event(db, application_id, "material_replaced", actor_id, actor_name,
                        material_id=new_id, payload={"replaces": old.application_material_id, "document_id": document_id})
        return self.get_material(application_id, new_id)

    def material_summary(self, application_id: str) -> dict[str, int]:
        self._prepare()
        with self.session_factory() as db:
            rows = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.status != "rejected",
            )))
        required = [value for value in rows if value.required]
        return {
            "total_required": len(required),
            "verified_count": sum(value.status == "verified" for value in required),
            "available_count": sum(value.status in self.MATCHED_STATUSES for value in required),
            "missing_count": sum(value.status == "required_missing" for value in required),
            "expired_count": sum(value.status == "expired" for value in required),
            "review_count": sum(value.status == "needs_review" for value in required),
        }

    def _validate_complete(self, rows: list[FinancingApplicationMaterial]) -> list[str]:
        missing = []
        for row in rows:
            if not row.required or row.status == "rejected": continue
            if row.status not in self.MATCHED_STATUSES or (row.required_verified and row.status != "verified"):
                missing.append(row.material_name)
        return missing

    def create_application_package(self, application_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare(); package_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            app = self._application(db, application_id)
            materials = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.status != "rejected",
            ).order_by(FinancingApplicationMaterial.id)))
            missing = self._validate_complete(materials)
            if missing: raise ApplicationMaterialError(f"仍缺必需材料：{'、'.join(missing)}")
            version = (db.scalar(select(func.max(FinancingApplicationPackage.package_version)).where(
                FinancingApplicationPackage.application_id == application_id,
            )) or 0) + 1
            package = FinancingApplicationPackage(package_id=package_id, application_id=application_id,
                package_version=version, status="ready", created_by=actor_id)
            db.add(package); db.flush()
            for index, material in enumerate(materials, 1):
                document = db.scalar(select(Document).where(Document.doc_id == material.source_document_id)) if material.source_document_id else None
                if document is not None and document.customer_id != app.customer_id:
                    raise ApplicationMaterialError("检测到跨客户文件引用，已拒绝生成材料包")
                definition = MATERIAL_TYPE_REGISTRY.get(material.material_type, MATERIAL_TYPE_REGISTRY["other"])
                original = document.file_name if document else Path(material.file_reference or material.material_name).name
                display = f"{index:02d}_{material.material_name}_{material.owner_name or app.customer_id}{Path(original).suffix}"
                db.add(FinancingApplicationPackageItem(
                    package_item_id=uuid.uuid4().hex, package_id=package_id,
                    application_material_id=material.application_material_id,
                    customer_material_id=material.customer_material_id, source_file_id=material.source_file_id,
                    material_type=material.material_type, material_name=material.material_name,
                    owner_type=material.owner_type, owner_name=material.owner_name,
                    file_name=original, file_hash=document.file_hash if document else "",
                    source_file_path=document.file_path if document else material.file_reference,
                    display_name=display, package_file_name=f"{definition.package_section}/{_safe_name(display)}",
                    sort_order=index, required=material.required, included=1, notes=material.notes,
                ))
            self._event(db, application_id, "package_created", actor_id, actor_name,
                        package_id=package_id, payload={"package_version": version, "item_count": len(materials)})
        return self.get_application_package(package_id)

    def freeze_application_package(self, package_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            package = db.scalar(select(FinancingApplicationPackage).where(FinancingApplicationPackage.package_id == package_id))
            if package is None: raise LookupError("申请材料包不存在")
            if package.status not in {"draft", "ready"}: raise ApplicationMaterialError("已冻结或已提交的材料包不可修改")
            package.status = "frozen"
            self._event(db, package.application_id, "package_frozen", actor_id, actor_name, package_id=package_id)
        return self.get_application_package(package_id)

    def update_package_item(self, package_id: str, package_item_id: str, *, included: bool,
                            actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            package = db.scalar(select(FinancingApplicationPackage).where(FinancingApplicationPackage.package_id == package_id))
            if package is None: raise LookupError("申请材料包不存在")
            if package.status in {"frozen", "submitted", "superseded"}:
                raise ApplicationMaterialError("已冻结或已提交的材料包不可修改")
            item = db.scalar(select(FinancingApplicationPackageItem).where(
                FinancingApplicationPackageItem.package_id == package_id,
                FinancingApplicationPackageItem.package_item_id == package_item_id,
            ))
            if item is None: raise LookupError("材料包条目不存在")
            if item.required and not included: raise ApplicationMaterialError("必需材料不能从材料包移除")
            item.included = int(included)
            self._event(db, package.application_id, "package_item_updated", actor_id, actor_name,
                        package_id=package_id, material_id=item.application_material_id,
                        payload={"included": included})
        return self.get_application_package(package_id)

    def _manifest(self, db, package: FinancingApplicationPackage) -> dict[str, Any]:
        app = self._application(db, package.application_id)
        customer = db.scalar(select(Customer).where(Customer.customer_id == app.customer_id))
        items = list(db.scalars(select(FinancingApplicationPackageItem).where(
            FinancingApplicationPackageItem.package_id == package.package_id,
            FinancingApplicationPackageItem.included == 1,
        ).order_by(FinancingApplicationPackageItem.sort_order)))
        customer_name = customer.name if customer and customer.name else re.sub(r"^(enterprise|personal)_", "", app.customer_id)
        return {"customer_id": app.customer_id, "customer_name": customer_name,
                "institution_name": app.institution_name, "product_name": app.product_name,
                "application_no": app.application_no, "target_amount": str(app.target_amount),
                "target_term_months": app.target_term_months, "package_version": package.package_version,
                "items": [{"sequence": index, "material_type": item.material_type,
                           "material_name": item.material_name, "owner": item.owner_name or item.owner_type,
                           "file_name": item.file_name, "file_hash": item.file_hash,
                           "source_file_id": item.source_file_id, "package_file_name": item.package_file_name,
                           "required": bool(item.required)} for index, item in enumerate(items, 1)]}

    def create_submission_package(self, package_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare(); submission_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            package = db.scalar(select(FinancingApplicationPackage).where(FinancingApplicationPackage.package_id == package_id))
            if package is None: raise LookupError("申请材料包不存在")
            if package.status not in {"ready", "frozen"}: raise ApplicationMaterialError("只有已就绪材料包可以生成银行进件包")
            package.status = "frozen"
            app = self._application(db, package.application_id)
            version = (db.scalar(select(func.max(FinancingSubmissionPackage.submission_version)).where(
                FinancingSubmissionPackage.application_id == app.application_id,
            )) or 0) + 1
            manifest = self._manifest(db, package)
            digest = hashlib.sha256(_json(manifest).encode("utf-8")).hexdigest()
            db.add(FinancingSubmissionPackage(
                submission_package_id=submission_id, application_id=app.application_id,
                application_package_id=package_id, submission_version=version,
                institution_name=app.institution_name, product_name=app.product_name,
                status="ready", manifest_json=_json(manifest), package_hash=digest, created_by=actor_id,
            ))
            self._event(db, app.application_id, "package_frozen", actor_id, actor_name, package_id=package_id)
            self._event(db, app.application_id, "submission_package_created", actor_id, actor_name,
                        package_id=submission_id, payload={"submission_version": version, "package_hash": digest})
        return self.get_submission_package(submission_id)

    def mark_submission_package_submitted(self, submission_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingSubmissionPackage).where(FinancingSubmissionPackage.submission_package_id == submission_id))
            if row is None: raise LookupError("银行进件包不存在")
            if row.status == "submitted": return self._submission_dict(row)
            if row.status != "ready": raise ApplicationMaterialError("当前进件包不可提交")
            row.status = "submitted"; row.submitted_at = _now()
            self._event(db, row.application_id, "package_submitted", actor_id, actor_name,
                        package_id=submission_id, payload={"submission_version": row.submission_version})
        return self.get_submission_package(submission_id)

    def create_supplement_package(self, supplement_id: str, *, actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare(); package_id = uuid.uuid4().hex
        with self.session_factory.begin() as db:
            supplement = db.scalar(select(FinancingSupplementRequest).where(FinancingSupplementRequest.supplement_id == supplement_id))
            if supplement is None: raise LookupError("补件要求不存在")
            materials = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.supplement_request_id == supplement_id,
                FinancingApplicationMaterial.status != "rejected",
            ).order_by(FinancingApplicationMaterial.id)))
            missing = self._validate_complete(materials)
            if missing: raise ApplicationMaterialError(f"补件材料尚未齐备：{'、'.join(missing)}")
            version = (db.scalar(select(func.max(FinancingSupplementPackage.package_version)).where(
                FinancingSupplementPackage.supplement_request_id == supplement_id,
            )) or 0) + 1
            manifest = {"supplement_request_id": supplement_id, "request_no": supplement.request_no,
                        "items": [{"material_id": item.application_material_id, "name": item.material_name,
                                   "source_file_id": item.source_file_id} for item in materials]}
            digest = hashlib.sha256(_json(manifest).encode("utf-8")).hexdigest()
            db.add(FinancingSupplementPackage(supplement_package_id=package_id,
                application_id=supplement.application_id, supplement_request_id=supplement_id,
                package_version=version, status="ready", manifest_json=_json(manifest),
                package_hash=digest, created_by=actor_id))
            for material in materials:
                document = db.scalar(select(Document).where(Document.doc_id == material.source_document_id)) if material.source_document_id else None
                db.add(FinancingSupplementPackageItem(package_item_id=uuid.uuid4().hex,
                    supplement_package_id=package_id, application_material_id=material.application_material_id,
                    source_file_id=material.source_file_id, material_type=material.material_type,
                    material_name=material.material_name, file_name=document.file_name if document else None,
                    file_hash=document.file_hash if document else "",
                    source_file_path=document.file_path if document else material.file_reference,
                    required=material.required))
            self._event(db, supplement.application_id, "package_created", actor_id, actor_name,
                        package_id=package_id, payload={"package_type": "supplement", "package_version": version})
        return self.get_supplement_package(package_id)

    def supplement_application_id(self, supplement_id: str) -> str:
        self._prepare()
        with self.session_factory() as db:
            value = db.scalar(select(FinancingSupplementRequest.application_id).where(
                FinancingSupplementRequest.supplement_id == supplement_id))
            if value is None: raise LookupError("补件要求不存在")
            return value

    def submit_supplement_package(self, package_id: str, *, submission_reference: str | None,
                                  actor_id: str, actor_name: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory.begin() as db:
            row = db.scalar(select(FinancingSupplementPackage).where(FinancingSupplementPackage.supplement_package_id == package_id))
            if row is None: raise LookupError("补件包不存在")
            if row.status == "submitted": return self._supplement_dict(row)
            if row.status != "ready": raise ApplicationMaterialError("当前补件包不可提交")
            row.status = "submitted"; row.submitted_at = _now(); row.submitted_by = actor_id
            row.submission_reference = submission_reference
            self._event(db, row.application_id, "package_submitted", actor_id, actor_name,
                        package_id=package_id, payload={"package_type": "supplement", "package_version": row.package_version})
        return self.get_supplement_package(package_id)

    def get_material(self, application_id: str, material_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            ))
            if row is None: raise LookupError("申请材料不存在")
            return self._material_dict(row)

    def get_material_extraction(self, application_id: str, material_id: str) -> dict[str, Any]:
        """读取申请材料所引用的结构化提取结果，不复制提取数据。"""
        self._prepare()
        with self.session_factory() as db:
            app = self._application(db, application_id)
            material = db.scalar(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
                FinancingApplicationMaterial.application_material_id == material_id,
            ))
            if material is None: raise LookupError("申请材料不存在")
            if not material.source_document_id:
                raise ApplicationMaterialError("当前材料尚未关联客户资料")
            document = db.scalar(select(Document).where(Document.doc_id == material.source_document_id))
            if document is None or document.customer_id != app.customer_id:
                raise ApplicationMaterialError("材料来源不属于当前客户")
            extraction = db.scalar(select(Extraction).where(
                Extraction.doc_id == document.doc_id,
                Extraction.extraction_status == "success",
            ).order_by(Extraction.created_at.desc(), Extraction.id.desc()))
            if extraction is None:
                return {"document_id": document.doc_id, "file_name": document.file_name,
                        "extraction_status": "unavailable", "data": {}}
            confirmed = extraction.confirm_status == "confirmed"
            return {
                "document_id": document.doc_id,
                "file_name": document.file_name,
                "extraction_id": extraction.extraction_id,
                "extraction_type": extraction.extraction_type,
                "extraction_status": extraction.extraction_status,
                "confirm_status": extraction.confirm_status,
                "data": _load(extraction.confirmed_data if confirmed else extraction.extracted_data, {}),
            }

    def list_materials(self, application_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            self._application(db, application_id)
            rows = list(db.scalars(select(FinancingApplicationMaterial).where(
                FinancingApplicationMaterial.application_id == application_id,
            ).order_by(FinancingApplicationMaterial.id)))
            events = list(db.scalars(select(FinancingMaterialEvent).where(
                FinancingMaterialEvent.application_id == application_id,
            ).order_by(FinancingMaterialEvent.created_at, FinancingMaterialEvent.id)))
        return {"application_id": application_id, "materials": [self._material_dict(value) for value in rows],
                "summary": self.material_summary(application_id),
                "events": [{"event_id": value.material_event_id, "event_type": value.event_type,
                            "material_id": value.application_material_id, "package_id": value.package_id,
                            "operator_name": value.operator_name, "payload": _load(value.payload_json, {}),
                            "created_at": value.created_at.isoformat() if value.created_at else None} for value in events]}

    def list_application_packages(self, application_id: str) -> list[dict[str, Any]]:
        self._prepare()
        with self.session_factory() as db:
            ids = list(db.scalars(select(FinancingApplicationPackage.package_id).where(
                FinancingApplicationPackage.application_id == application_id,
            ).order_by(FinancingApplicationPackage.package_version.desc())))
        return [self.get_application_package(value) for value in ids]

    def list_package_overview(self, application_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            self._application(db, application_id)
            submission_ids = list(db.scalars(select(FinancingSubmissionPackage.submission_package_id).where(
                FinancingSubmissionPackage.application_id == application_id,
            ).order_by(FinancingSubmissionPackage.submission_version.desc())))
            supplement_ids = list(db.scalars(select(FinancingSupplementPackage.supplement_package_id).where(
                FinancingSupplementPackage.application_id == application_id,
            ).order_by(FinancingSupplementPackage.created_at.desc())))
        return {"packages": self.list_application_packages(application_id),
                "submission_packages": [self.get_submission_package(value) for value in submission_ids],
                "supplement_packages": [self.get_supplement_package(value) for value in supplement_ids]}

    def get_application_package(self, package_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingApplicationPackage).where(FinancingApplicationPackage.package_id == package_id))
            if row is None: raise LookupError("申请材料包不存在")
            items = list(db.scalars(select(FinancingApplicationPackageItem).where(
                FinancingApplicationPackageItem.package_id == package_id,
            ).order_by(FinancingApplicationPackageItem.sort_order)))
            return {"package_id": row.package_id, "application_id": row.application_id,
                    "package_version": row.package_version, "status": row.status,
                    "created_by": row.created_by, "created_at": row.created_at.isoformat() if row.created_at else None,
                    "items": [{"package_item_id": item.package_item_id,
                               "application_material_id": item.application_material_id,
                               "material_type": item.material_type, "material_name": item.material_name,
                               "owner_type": item.owner_type, "owner_name": item.owner_name,
                               "file_name": item.file_name, "file_hash": item.file_hash,
                               "package_file_name": item.package_file_name,
                               "required": bool(item.required), "included": bool(item.included)} for item in items]}

    @staticmethod
    def _submission_dict(row: FinancingSubmissionPackage) -> dict[str, Any]:
        return {"submission_package_id": row.submission_package_id, "application_id": row.application_id,
                "application_package_id": row.application_package_id, "submission_version": row.submission_version,
                "institution_name": row.institution_name, "product_name": row.product_name,
                "status": row.status, "manifest": _load(row.manifest_json, {}),
                "package_hash": row.package_hash, "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None}

    def get_submission_package(self, submission_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingSubmissionPackage).where(FinancingSubmissionPackage.submission_package_id == submission_id))
            if row is None: raise LookupError("银行进件包不存在")
            return self._submission_dict(row)

    @staticmethod
    def _supplement_dict(row: FinancingSupplementPackage) -> dict[str, Any]:
        return {"supplement_package_id": row.supplement_package_id, "application_id": row.application_id,
                "supplement_request_id": row.supplement_request_id, "package_version": row.package_version,
                "status": row.status, "manifest": _load(row.manifest_json, {}), "package_hash": row.package_hash,
                "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
                "submission_reference": row.submission_reference}

    def get_supplement_package(self, package_id: str) -> dict[str, Any]:
        self._prepare()
        with self.session_factory() as db:
            row = db.scalar(select(FinancingSupplementPackage).where(FinancingSupplementPackage.supplement_package_id == package_id))
            if row is None: raise LookupError("补件包不存在")
            return self._supplement_dict(row)

    def manifest(self, submission_id: str) -> dict[str, Any]:
        return self.get_submission_package(submission_id)["manifest"]

    def build_zip(self, submission_id: str) -> tuple[str, bytes]:
        self._prepare()
        with self.session_factory() as db:
            submission = db.scalar(select(FinancingSubmissionPackage).where(FinancingSubmissionPackage.submission_package_id == submission_id))
            if submission is None: raise LookupError("银行进件包不存在")
            package = db.scalar(select(FinancingApplicationPackage).where(FinancingApplicationPackage.package_id == submission.application_package_id))
            app = self._application(db, submission.application_id)
            customer = db.scalar(select(Customer).where(Customer.customer_id == app.customer_id))
            items = list(db.scalars(select(FinancingApplicationPackageItem).where(
                FinancingApplicationPackageItem.package_id == package.package_id,
                FinancingApplicationPackageItem.included == 1,
            ).order_by(FinancingApplicationPackageItem.sort_order)))
            manifest = _load(submission.manifest_json, {})
            customer_name = customer.name if customer and customer.name else re.sub(r"^(enterprise|personal)_", "", app.customer_id)
            root = f"{_safe_name(customer_name)}/{_safe_name(app.institution_name + app.product_name)}"
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
                html = self._manifest_html(manifest)
                archive.writestr(f"{root}/融资申请材料清单.html", html.encode("utf-8"))
                for item in items:
                    if not item.source_file_id: continue
                    document = db.scalar(select(Document).where(Document.doc_id == item.source_file_id))
                    if document is None or document.customer_id != app.customer_id:
                        raise ApplicationMaterialError("材料包包含无权访问的客户文件")
                    path = Path(document.file_path or "")
                    if not path.is_file():
                        raise ApplicationMaterialError(f"原始文件不可用：{document.file_name}")
                    archive.write(path, f"{root}/{item.package_file_name}")
            return f"{_safe_name(customer_name)}_{_safe_name(app.product_name)}_进件材料.zip", buffer.getvalue()

    @staticmethod
    def _manifest_html(manifest: dict[str, Any]) -> str:
        esc = lambda value: html.escape(str(value or ""))
        rows = "".join(f"<tr><td>{item['sequence']}</td><td>{esc(item['material_name'])}</td><td>{esc(item['owner'])}</td><td>{esc(item['file_name'])}</td><td>{'必需' if item['required'] else '可选'}</td></tr>" for item in manifest.get("items", []))
        return ("<!doctype html><meta charset='utf-8'><title>融资申请材料清单</title>"
                f"<h1>融资申请材料清单</h1><p>客户：{esc(manifest.get('customer_name'))}</p>"
                f"<p>银行：{esc(manifest.get('institution_name'))}　产品：{esc(manifest.get('product_name'))}</p>"
                f"<p>申请金额：{esc(manifest.get('target_amount'))}元　申请期限：{esc(manifest.get('target_term_months'))}个月</p>"
                f"<table border='1' cellspacing='0' cellpadding='6'><tr><th>序号</th><th>材料名称</th><th>主体</th><th>文件名称</th><th>要求</th></tr>{rows}</table>")

    @staticmethod
    def _material_dict(row: FinancingApplicationMaterial) -> dict[str, Any]:
        return {"application_material_id": row.application_material_id, "application_id": row.application_id,
                "supplement_request_id": row.supplement_request_id, "material_type": row.material_type,
                "material_name": row.material_name, "material_category": row.material_category,
                "owner_type": row.owner_type, "owner_id": row.owner_id, "owner_name": row.owner_name,
                "required": bool(row.required), "required_verified": bool(row.required_verified),
                "status": row.status, "source_type": row.source_type, "source_id": row.source_id,
                "source_document_id": row.source_document_id, "customer_material_id": row.customer_material_id,
                "source_file_id": row.source_file_id, "file_reference": row.file_reference,
                "valid_from": row.valid_from.isoformat() if row.valid_from else None,
                "valid_to": row.valid_to.isoformat() if row.valid_to else None,
                "coverage_start": row.coverage_start.isoformat() if row.coverage_start else None,
                "coverage_end": row.coverage_end.isoformat() if row.coverage_end else None,
                "version_no": row.version_no, "replaces_material_id": row.replaces_material_id,
                "rejection_reason": row.rejection_reason, "verified_by": row.verified_by,
                "verified_at": row.verified_at.isoformat() if row.verified_at else None, "notes": row.notes}
