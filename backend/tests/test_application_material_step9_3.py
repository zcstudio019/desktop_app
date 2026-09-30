from __future__ import annotations

import json
import sys
import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from backend.db_models import Document, Extraction, FinancingApplicationMaterial, FinancingMaterialEvent
from backend.routers.financing_application import _require_write
from backend.services.application_material_service import (
    MATERIAL_TYPE_REGISTRY, ApplicationMaterialError, ApplicationMaterialMatchingService,
)
from backend.tests.test_financing_application_step9_1 import create_apps, factory, ready_and_submit


def service(factory):
    return ApplicationMaterialMatchingService(session_factory=factory, ensure_schema=False)


def add_requirement(svc, app_id, material_type, name, *, owner_type="enterprise", owner_id=None, owner_name=None):
    return svc.add_material(app_id, material_type=material_type, material_name=name,
        owner_type=owner_type, owner_id=owner_id, owner_name=owner_name,
        required=True, required_verified=False, source_type="manual", source_id=None,
        actor_id="operator", actor_name="经办人")


def add_document(factory, tmp_path, *, customer_id="enterprise_上海意川建筑科技有限公司",
                 doc_id="doc-1", doc_type="business_license", file_name="营业执照.pdf",
                 confirmed=False, owner_type="enterprise", owner_id="", owner_name="",
                 period_start=None, period_end=None, valid_until=""):
    path = tmp_path / f"{doc_id}.pdf"; path.write_bytes(f"fixture-{doc_id}".encode())
    payload = {"owner_type": owner_type, "owner_id": owner_id, "owner_name": owner_name,
               "period_start": period_start, "period_end": period_end}
    with factory.begin() as db:
        db.add(Document(doc_id=doc_id, customer_id=customer_id, file_name=file_name,
            file_path=str(path), file_type="pdf", file_hash=f"hash-{doc_id}", is_active=1,
            valid_until=valid_until))
        db.add(Extraction(extraction_id=f"ex-{doc_id}", doc_id=doc_id, customer_id=customer_id,
            extraction_type=doc_type, extracted_data=json.dumps(payload),
            confirmed_data=json.dumps(payload), confirm_status="confirmed" if confirmed else "unconfirmed",
            extraction_status="success"))
    return doc_id


def prepared(factory, tmp_path, *, confirmed=True):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]
    svc = service(factory); material = add_requirement(svc, app_id, "business_license", "营业执照")
    add_document(factory, tmp_path, confirmed=confirmed)
    svc.match_customer_materials_to_application(app_id, actor_id="operator", actor_name="经办人")
    return svc, app_id, material


def test_material_type_registry():
    assert len(MATERIAL_TYPE_REGISTRY) == 15
    assert MATERIAL_TYPE_REGISTRY["business_license"].label == "营业执照"


def test_material_match_by_doc_type(factory, tmp_path):
    svc, app_id, material = prepared(factory, tmp_path, confirmed=False)
    row = svc.get_material(app_id, material["application_material_id"])
    assert row["status"] == "matched" and row["source_document_id"] == "doc-1"


def test_material_owner_match(factory, tmp_path):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; svc = service(factory)
    material = add_requirement(svc, app_id, "id_card", "法人身份证", owner_type="person", owner_id="p1", owner_name="黎云")
    add_document(factory, tmp_path, doc_type="id_card", owner_type="person", owner_id="p1", owner_name="黎云")
    result = svc.match_customer_materials_to_application(app_id, actor_id="operator", actor_name="经办人")
    assert result["results"][0]["result"] == "matched"
    assert svc.get_material(app_id, material["application_material_id"])["owner_name"] == "黎云"


def test_cross_customer_file_rejected(factory, tmp_path):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; svc = service(factory)
    material = add_requirement(svc, app_id, "business_license", "营业执照")
    add_document(factory, tmp_path, customer_id="another-customer")
    with pytest.raises(ApplicationMaterialError, match="不属于当前客户"):
        svc.select_customer_material(app_id, material["application_material_id"], "doc-1", actor_id="operator", actor_name="经办人")


def test_multiple_matches_requires_selection(factory, tmp_path):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; svc = service(factory)
    add_requirement(svc, app_id, "id_card", "法人身份证", owner_type="person")
    add_document(factory, tmp_path, doc_id="id-1", doc_type="id_card", owner_type="person")
    add_document(factory, tmp_path, doc_id="id-2", doc_type="id_card", owner_type="person")
    result = svc.match_customer_materials_to_application(app_id, actor_id="operator", actor_name="经办人")
    assert result["results"][0]["result"] == "multiple_matches"


def test_verified_material_reused(factory, tmp_path):
    svc, app_id, material = prepared(factory, tmp_path, confirmed=True)
    assert svc.get_material(app_id, material["application_material_id"])["status"] == "verified"


def test_unverified_material_not_auto_verified(factory, tmp_path):
    svc, app_id, material = prepared(factory, tmp_path, confirmed=False)
    assert svc.get_material(app_id, material["application_material_id"])["status"] == "matched"


def test_structured_extraction_is_read_through_source_reference(factory, tmp_path):
    svc, app_id, material = prepared(factory, tmp_path, confirmed=True)
    result = svc.get_material_extraction(app_id, material["application_material_id"])
    assert result["document_id"] == "doc-1"
    assert result["confirm_status"] == "confirmed"
    assert result["data"]["owner_type"] == "enterprise"


def test_statement_coverage_check(factory, tmp_path):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; svc = service(factory)
    material = add_requirement(svc, app_id, "enterprise_bank_statement", "近12个月企业流水")
    with factory.begin() as db:
        row = db.scalar(select(FinancingApplicationMaterial).where(FinancingApplicationMaterial.application_material_id == material["application_material_id"]))
        row.coverage_start = date(2025, 10, 1); row.coverage_end = date(2026, 9, 30)
    add_document(factory, tmp_path, doc_type="enterprise_bank_statement", period_start="2026-04-01", period_end="2026-09-30")
    result = svc.match_customer_materials_to_application(app_id, actor_id="operator", actor_name="经办人")
    assert result["results"][0]["result"] == "needs_review"


def test_financial_multiple_periods(factory, tmp_path):
    _, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; svc = service(factory)
    add_requirement(svc, app_id, "financial_statement", "财务报表")
    add_document(factory, tmp_path, doc_id="fs-2025", doc_type="financial_report", file_name="2025财务报表.pdf")
    add_document(factory, tmp_path, doc_id="fs-2026", doc_type="financial_report", file_name="2026财务报表.pdf")
    result = svc.match_customer_materials_to_application(app_id, actor_id="operator", actor_name="经办人")
    assert result["results"][0]["result"] == "multiple_matches" and len(result["results"][0]["candidates"]) == 2


def test_missing_required_material_blocks_package(factory):
    _, _, apps = create_apps(factory); svc = service(factory); app_id = apps[0]["application_id"]
    add_requirement(svc, app_id, "business_license", "营业执照")
    with pytest.raises(ApplicationMaterialError, match="仍缺必需材料"):
        svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")


def test_create_application_package(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path)
    package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    assert package["package_version"] == 1 and package["status"] == "ready" and len(package["items"]) == 1


def test_package_versioning(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path)
    assert svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")["package_version"] == 1
    assert svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")["package_version"] == 2


def test_frozen_package_immutable(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path)
    package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    frozen = svc.freeze_application_package(package["package_id"], actor_id="operator", actor_name="经办人")
    with pytest.raises(ApplicationMaterialError, match="不可修改"):
        svc.update_package_item(frozen["package_id"], frozen["items"][0]["package_item_id"], included=False,
                                actor_id="operator", actor_name="经办人")


def test_submission_package(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path); package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    submission = svc.create_submission_package(package["package_id"], actor_id="operator", actor_name="经办人")
    assert submission["status"] == "ready" and submission["manifest"]["items"][0]["material_name"] == "营业执照"


def test_submission_package_frozen(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path); package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    submission = svc.create_submission_package(package["package_id"], actor_id="operator", actor_name="经办人")
    submitted = svc.mark_submission_package_submitted(submission["submission_package_id"], actor_id="operator", actor_name="经办人")
    assert submitted["status"] == "submitted" and submitted["submitted_at"]


def test_manifest_generated(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path); package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    submission = svc.create_submission_package(package["package_id"], actor_id="operator", actor_name="经办人")
    manifest = svc.manifest(submission["submission_package_id"])
    assert manifest["customer_name"] == "上海意川建筑科技有限公司" and manifest["target_term_months"] == 12


def test_zip_structure(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path); package = svc.create_application_package(app_id, actor_id="operator", actor_name="经办人")
    submission = svc.create_submission_package(package["package_id"], actor_id="operator", actor_name="经办人")
    _, content = svc.build_zip(submission["submission_package_id"])
    with zipfile.ZipFile(BytesIO(content)) as archive:
        names = archive.namelist()
    assert any(name.endswith("融资申请材料清单.html") for name in names)
    assert any("01_企业基础资料" in name and name.endswith(".pdf") for name in names)


def test_supplement_package(factory, tmp_path):
    app_service, _, apps = create_apps(factory); app_id = apps[0]["application_id"]; ready_and_submit(app_service, app_id)
    current = app_service.request_supplement(app_id, description="补件", required_materials=["纳税资料"], due_date=None, actor_id="operator", actor_name="经办人")
    material = current["application_materials"][0]
    add_document(factory, tmp_path, doc_id="tax-1", doc_type="tax_record", file_name="纳税资料.pdf")
    svc = service(factory); svc.select_customer_material(app_id, material["application_material_id"], "tax-1", actor_id="operator", actor_name="经办人")
    package = svc.create_supplement_package(current["supplements"][0]["supplement_id"], actor_id="operator", actor_name="经办人")
    assert package["status"] == "ready" and package["manifest"]["items"][0]["name"] == "纳税资料"


def test_material_replacement_history(factory, tmp_path):
    svc, app_id, material = prepared(factory, tmp_path)
    add_document(factory, tmp_path, doc_id="doc-2", confirmed=True)
    replacement = svc.replace_material(app_id, material["application_material_id"], "doc-2", rejection_reason="银行退回", actor_id="operator", actor_name="经办人")
    original = svc.get_material(app_id, material["application_material_id"])
    assert original["status"] == "rejected" and replacement["version_no"] == 2 and replacement["replaces_material_id"] == original["application_material_id"]


def test_material_event_audit(factory, tmp_path):
    svc, app_id, _ = prepared(factory, tmp_path)
    events = svc.list_materials(app_id)["events"]
    assert {event["event_type"] for event in events} >= {"material_required", "material_matched"}


def test_viewer_read_only():
    with pytest.raises(HTTPException) as exc: _require_write({"role": "viewer"})
    assert exc.value.status_code == 403


def test_shanghai_yichuan_no_fake_package(factory):
    svc = service(factory)
    with pytest.raises(LookupError, match="融资申请不存在"):
        svc.list_materials("enterprise_上海意川建筑科技有限公司")
