from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import (
    Document, Extraction, FinancingPlan, FinancingPlanCondition, FinancingPlanExplanation,
    FinancingPlanGap, FinancingPlanItem, FinancingPlanMaterial, FinancingPlanVersion,
    FinancingProduct, FinancingProductRule, FinancingProductVersion, FinancingRequirement,
    ManualCandidateOverride, ProductMatchItem, ProductMatchSnapshot,
)
from backend.services.financing_plan_service import (
    FinancingPlanDraftInput, FinancingPlanError, FinancingPlanItemInput, FinancingPlanService,
)


CUSTOMER = "enterprise_上海意川建筑科技有限公司"
REQUIREMENT = "req-yichuan-v2"
SNAPSHOT = "snapshot-yichuan"


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        FinancingRequirement.__table__, FinancingProduct.__table__, FinancingProductVersion.__table__,
        FinancingProductRule.__table__, ProductMatchSnapshot.__table__, ProductMatchItem.__table__,
        FinancingPlan.__table__, FinancingPlanVersion.__table__, FinancingPlanItem.__table__,
        FinancingPlanGap.__table__, ManualCandidateOverride.__table__, FinancingPlanCondition.__table__,
        FinancingPlanMaterial.__table__, FinancingPlanExplanation.__table__, Document.__table__, Extraction.__table__,
    ])
    yield sessionmaker(bind=engine)
    engine.dispose()


def seed(factory, products: list[dict], *, document: str | None = None, confirmed_document=False):
    facts = json.dumps({"requirement": {
        "amount": {"status": "known", "value": "8000000", "unit": "CNY"},
        "term_months": {"status": "known", "value": 12, "unit": "month"},
        "currency": {"status": "known", "value": "CNY"},
    }}, ensure_ascii=False)
    with factory.begin() as db:
        db.add(FinancingRequirement(
            requirement_id=REQUIREMENT, customer_id=CUSTOMER, version=2, status="confirmed",
            borrower_entity="上海意川建筑科技有限公司", requested_amount=Decimal("8000000"),
            currency="CNY", financing_purpose="采购", purpose_detail="材料采购", term_value=12, term_unit="month",
        ))
        db.add(ProductMatchSnapshot(
            snapshot_id=SNAPSHOT, context_hash="context", customer_id=CUSTOMER,
            requirement_id=REQUIREMENT, requirement_version=2, facts_hash="facts-hash",
            facts_json=facts, catalog_as_of_date=date(2026, 9, 28), catalog_version_hash="catalog-hash",
            generated_at=datetime(2026, 9, 28, 7, 27, 51), generated_by="admin", summary_json="{}",
        ))
        db.flush()
        for index, value in enumerate(products, 1):
            product_id, version_id = f"product-{index}", f"version-{index}"
            db.add(FinancingProduct(
                product_id=product_id, identity_key=f"identity-{index}", external_product_code=value.get("code", f"TEST-{index}"),
                institution_name=value.get("institution", "测试银行"), product_name=f"产品{index}",
                product_category="enterprise_credit", region_key="", source_type="local_markdown", source_ref="test.md",
            ))
            db.add(FinancingProductVersion(
                version_id=version_id, product_id=product_id, version_number=1,
                institution_name=value.get("institution", "测试银行"), product_name=f"产品{index}",
                status="published", effective_from=date(2026, 1, 1), source_snapshot="source",
                source_snapshot_hash=f"hash-{index}", source_type="local_markdown", source_file="test.md",
                source_node_token="", source_document_token="", source_imported_at=datetime(2026, 1, 1),
                materials_json=json.dumps(value.get("materials", []), ensure_ascii=False), created_by="admin",
                max_amount=value.get("max_amount"), max_term_months=value.get("max_term", 12),
            ))
            db.add(ProductMatchItem(
                snapshot_id=SNAPSHOT, product_id=product_id, version_id=version_id,
                external_product_code=value.get("code", f"TEST-{index}"), institution_name=value.get("institution", "测试银行"),
                product_name=f"产品{index}", product_category="enterprise_credit",
                max_amount=value.get("max_amount"), max_term_months=value.get("max_term", 12),
                overall_status=value["status"], blocking_reasons_json="[]",
                missing_information_json=json.dumps(value.get("missing", []), ensure_ascii=False),
                review_reasons_json=json.dumps(value.get("review", []), ensure_ascii=False),
                soft_gaps_json="[]", rule_results_json="[]",
            ))
        if document:
            db.add(Document(
                doc_id="doc-1", customer_id=CUSTOMER, file_name=f"{document}.pdf", file_type="pdf", is_active=1,
            ))
            db.add(Extraction(
                extraction_id="ext-1", doc_id="doc-1", customer_id=CUSTOMER,
                extraction_type=document, extracted_data="{}", extraction_status="success",
                confirm_status="confirmed" if confirmed_document else "unconfirmed",
            ))


def service(factory):
    return FinancingPlanService(session_factory=factory, ensure_schema=False)


def item_ids(factory):
    with factory() as db:
        return list(db.scalars(select(ProductMatchItem.id).order_by(ProductMatchItem.id)))


def draft(factory, amounts: list[str], *, manual=False, conditions=None, terms=None):
    ids = item_ids(factory)
    return FinancingPlanDraftInput(
        customer_id=CUSTOMER, requirement_id=REQUIREMENT, match_snapshot_id=SNAPSHOT,
        plan_type="conditional" if manual else "primary",
        items=[FinancingPlanItemInput(
            product_match_item_id=value, proposed_amount=Decimal(amounts[index]),
            proposed_term_months=(terms or [12] * len(ids))[index],
            item_role="primary" if index == 0 else "supplementary", sequence_no=index + 1,
            manual_approved=manual, conditions=conditions or [], notes="业务备注",
        ) for index, value in enumerate(ids[:len(amounts)])],
    )


def override(service, item_id, new_status="conditional"):
    return service.create_manual_override(
        customer_id=CUSTOMER, requirement_id=REQUIREMENT, match_snapshot_id=SNAPSHOT,
        product_match_item_id=item_id, new_status=new_status,
        operator_id="admin", operator_name="管理员", reason="已核验产品边界，纳入条件性候选",
    )


def test_manual_review_product_not_auto_added(factory):
    seed(factory, [{"status": "manual_review", "max_amount": Decimal("8000000")}])
    result = service(factory).generate_plan_combinations(CUSTOMER, REQUIREMENT, SNAPSHOT)
    assert result["formal_combinations"] == [] and result["conditional_combinations"] == []


def test_manual_override_changes_to_conditional_only(factory):
    seed(factory, [{"status": "manual_review", "max_amount": Decimal("8000000")}])
    value = service(factory)
    with pytest.raises(FinancingPlanError, match="只允许"):
        override(value, item_ids(factory)[0], "usable")
    override(value, item_ids(factory)[0])
    result = value.build_plan_candidates(CUSTOMER, REQUIREMENT, SNAPSHOT)
    assert result["candidates"][0]["candidate_status"] == "conditional"


def test_manual_override_has_audit_log(factory):
    seed(factory, [{"status": "manual_review", "max_amount": Decimal("8000000")}])
    result = override(service(factory), item_ids(factory)[0])
    with factory() as db:
        row = db.scalar(select(ManualCandidateOverride))
        assert row.override_id == result["override_id"] and row.operator_name == "管理员" and row.reason


def test_override_does_not_modify_match_snapshot(factory):
    seed(factory, [{"status": "manual_review", "max_amount": Decimal("8000000")}])
    item_id = item_ids(factory)[0]
    override(service(factory), item_id)
    with factory() as db:
        assert db.get(ProductMatchItem, item_id).overall_status == "manual_review"


def test_conditions_generated_from_missing_information(factory):
    seed(factory, [{"status": "conditional", "max_amount": Decimal("8000000"), "missing": ["需补充纳税数据"]}])
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert any(row["title"] == "需补充纳税数据" and row["source_type"] == "missing_information" for row in plan["versions"][0]["condition_checklist"])


def test_conditions_generated_from_review_reasons(factory):
    seed(factory, [{"status": "conditional", "max_amount": Decimal("8000000"), "review": ["需人工确认产品最高额度"]}])
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert any(row["title"] == "需人工确认产品最高额度" for row in plan["versions"][0]["condition_checklist"])


def test_materials_generated_from_product_materials(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000"), "materials": ["营业执照", "财务报表"]}])
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert {row["material_name"] for row in plan["versions"][0]["material_checklist"]} == {"营业执照", "财务报表"}


def test_materials_deduplicated(factory):
    seed(factory, [
        {"status": "eligible", "max_amount": Decimal("5000000"), "materials": ["营业执照"]},
        {"status": "eligible", "max_amount": Decimal("3000000"), "materials": ["营业执照"]},
    ])
    plan = service(factory).create_plan_draft(draft(factory, ["5000000", "3000000"]), created_by="admin")
    rows = plan["versions"][0]["material_checklist"]
    assert len(rows) == 1 and rows[0]["required_by_products"] == ["TEST-1", "TEST-2"]


def test_existing_material_marked_available(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000"), "materials": ["营业执照"]}], document="business_license")
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert plan["versions"][0]["material_checklist"][0]["status"] == "available"


def test_unverified_material_not_marked_verified(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000"), "materials": ["营业执照"]}], document="business_license")
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert plan["versions"][0]["material_checklist"][0]["status"] != "verified"


def test_verified_material_requires_confirmed_extraction(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000"), "materials": ["营业执照"]}], document="business_license", confirmed_document=True)
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert plan["versions"][0]["material_checklist"][0]["status"] == "verified"


def test_plan_summary_template(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000")}])
    plan = service(factory).create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    assert "目标融资8000000" in plan["versions"][0]["explanation"]["plan_summary"]
    assert plan["versions"][0]["explanation"]["generated_by"] == "template"


def test_funding_gap_summary(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("5000000")}])
    plan = service(factory).create_plan_draft(draft(factory, ["5000000"]), created_by="admin")
    assert "3000000" in plan["versions"][0]["explanation"]["funding_gap_summary"]


def test_next_actions_generated_from_gaps(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("5000000")}])
    plan = service(factory).create_plan_draft(draft(factory, ["5000000"]), created_by="admin")
    assert any("重新生成融资组合" in value for value in plan["versions"][0]["explanation"]["next_actions"])


def test_draft_edit_revalidates_amount(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000")}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    with pytest.raises(FinancingPlanError, match="最高额度"):
        value.update_plan_draft(plan["financing_plan_id"], draft(factory, ["9000000"]), updated_by="admin")


def test_draft_edit_revalidates_term(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000"), "max_term": 12}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    with pytest.raises(FinancingPlanError, match="不能进入|期限"):
        value.update_plan_draft(plan["financing_plan_id"], draft(factory, ["8000000"], terms=[24]), updated_by="admin")


def test_pending_required_condition_blocks_confirm(factory):
    seed(factory, [{"status": "conditional", "max_amount": Decimal("8000000"), "missing": ["补充资料"]}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    with pytest.raises(FinancingPlanError, match="必需条件"):
        value.confirm_plan(plan["financing_plan_id"], confirmed_by="admin")


def test_complete_plan_requires_zero_gap(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("5000000")}])
    plan = service(factory).create_plan_draft(draft(factory, ["5000000"]), created_by="admin")
    assert plan["versions"][0]["generation_status"] == "partial" and plan["versions"][0]["funding_gap"] == "3000000.00"


def test_confirm_plan(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000")}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    confirmed = value.confirm_plan(plan["financing_plan_id"], confirmed_by="admin")
    assert confirmed["status"] == "confirmed"


def test_confirmed_version_immutable(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000")}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    confirmed = value.confirm_plan(plan["financing_plan_id"], confirmed_by="admin")
    with pytest.raises(ValueError, match="不可修改"):
        with factory.begin() as db:
            row = db.scalar(select(FinancingPlanVersion).where(FinancingPlanVersion.plan_version_id == confirmed["current_version_id"]))
            row.summary = "覆盖修改"


def test_confirmed_change_creates_v2(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("8000000")}])
    value = service(factory)
    plan = value.create_plan_draft(draft(factory, ["8000000"]), created_by="admin")
    value.confirm_plan(plan["financing_plan_id"], confirmed_by="admin")
    updated = value.update_plan_draft(plan["financing_plan_id"], draft(factory, ["7000000"]), updated_by="admin")
    assert [row["version_no"] for row in updated["versions"]] == [1, 2]
    assert updated["versions"][0]["status"] == "superseded"


def test_shanghai_yichuan_no_fake_materials(factory):
    seed(factory, [
        {"status": "ineligible", "max_amount": Decimal("5000000")},
        {"status": "ineligible", "max_amount": Decimal("3500000")},
        {"status": "ineligible", "max_amount": Decimal("5000000")},
        {"status": "manual_review", "max_amount": None, "max_term": None},
        {"status": "manual_review", "max_amount": None, "max_term": None},
    ])
    service(factory).generate_plan_combinations(CUSTOMER, REQUIREMENT, SNAPSHOT)
    with factory() as db:
        assert db.scalar(select(FinancingPlanMaterial)) is None


def test_shanghai_yichuan_no_fake_plan(factory):
    seed(factory, [
        {"status": "ineligible", "max_amount": Decimal("5000000")},
        {"status": "ineligible", "max_amount": Decimal("3500000")},
        {"status": "ineligible", "max_amount": Decimal("5000000")},
        {"status": "manual_review", "max_amount": None, "max_term": None},
        {"status": "manual_review", "max_amount": None, "max_term": None},
    ])
    result = service(factory).generate_plan_combinations(CUSTOMER, REQUIREMENT, SNAPSHOT)
    assert result["funding_gap"] == "8000000" and len(result["manual_review_candidates"]) == 2
    with factory() as db:
        assert db.scalar(select(FinancingPlan)) is None

