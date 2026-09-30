from __future__ import annotations

import copy
import sys
import types
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
    FinancingPlanGap, FinancingPlanItem, FinancingPlanMaterial, FinancingPlanReportSnapshot,
    FinancingPlanSelection, FinancingPlanVersion, FinancingProduct, FinancingProductRule,
    FinancingProductVersion, FinancingRequirement, ManualCandidateOverride, ProductMatchItem,
    ProductMatchSnapshot,
)
from backend.services.financing_plan_report_service import (
    DISCLAIMER, FinancingPlanReportError, FinancingPlanReportService,
    format_report_amount_wan, render_financing_plan_html, use_validated_llm_enhancement,
    validate_llm_report_enhancement,
)
from backend.services.financing_plan_service import FinancingPlanItemInput, FinancingPlanDraftInput, FinancingPlanService
from backend.tests.test_financing_plan_step8_3 import CUSTOMER, REQUIREMENT, SNAPSHOT, draft, seed


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        FinancingRequirement.__table__, FinancingProduct.__table__, FinancingProductVersion.__table__,
        FinancingProductRule.__table__, ProductMatchSnapshot.__table__, ProductMatchItem.__table__,
        FinancingPlan.__table__, FinancingPlanVersion.__table__, FinancingPlanItem.__table__,
        FinancingPlanGap.__table__, ManualCandidateOverride.__table__, FinancingPlanCondition.__table__,
        FinancingPlanMaterial.__table__, FinancingPlanExplanation.__table__, FinancingPlanSelection.__table__,
        FinancingPlanReportSnapshot.__table__, Document.__table__, Extraction.__table__,
    ])
    yield sessionmaker(bind=engine)
    engine.dispose()


def services(factory):
    return FinancingPlanService(session_factory=factory, ensure_schema=False), FinancingPlanReportService(session_factory=factory, ensure_schema=False)


def confirmed_plan(factory, *, amount="8000000", role="primary"):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000"), "materials": ["营业执照"]}])
    plan_service, _ = services(factory)
    payload = draft(factory, [amount])
    payload.plan_type = role
    plan = plan_service.create_plan_draft(payload, created_by="operator")
    return plan_service.confirm_plan(plan["financing_plan_id"], confirmed_by="operator")


def selection_for(factory, plan=None, *, role="primary"):
    _, reports = services(factory)
    version_id = plan["current_version_id"] if plan else None
    return reports.create_selection(
        customer_id=CUSTOMER, requirement_id=REQUIREMENT,
        primary_plan_version_id=version_id if role == "primary" else None,
        backup_plan_version_ids=[version_id] if version_id and role == "backup" else [],
        conditional_plan_version_ids=[version_id] if version_id and role == "conditional" else [],
        selected_by="operator", notes="人工选择",
    )


def test_selection_without_primary_allowed(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}])
    value = selection_for(factory)
    assert value["primary_plan_version_id"] is None and value["status"] == "draft"


def test_primary_requires_confirmable_plan(factory):
    seed(factory, [{"status": "eligible", "max_amount": Decimal("10000000")}])
    plan_service, reports = services(factory)
    plan = plan_service.create_plan_draft(draft(factory, ["8000000"]), created_by="operator")
    with pytest.raises(FinancingPlanReportError, match="已校验并确认"):
        reports.create_selection(customer_id=CUSTOMER, requirement_id=REQUIREMENT, primary_plan_version_id=plan["current_version_id"], backup_plan_version_ids=[], conditional_plan_version_ids=[], selected_by="operator")


def test_backup_plan_selection(factory):
    plan = confirmed_plan(factory)
    assert selection_for(factory, plan, role="backup")["backup_plan_version_ids"] == [plan["current_version_id"]]


def test_conditional_plan_selection(factory):
    plan = confirmed_plan(factory, role="conditional")
    assert selection_for(factory, plan, role="conditional")["conditional_plan_version_ids"] == [plan["current_version_id"]]


def test_finalize_selection(factory):
    plan = confirmed_plan(factory); _, reports = services(factory)
    selected = selection_for(factory, plan)
    assert reports.finalize_selection(selected["selection_id"], actor="admin")["status"] == "finalized"


def test_finalized_selection_immutable(factory):
    plan = confirmed_plan(factory); _, reports = services(factory)
    selected = selection_for(factory, plan); first = reports.finalize_selection(selected["selection_id"], actor="admin")
    second = reports.finalize_selection(selected["selection_id"], actor="other")
    assert second["finalized_by"] == first["finalized_by"] == "admin"


def two_versions(factory):
    plan = confirmed_plan(factory)
    plan_service, reports = services(factory)
    payload = draft(factory, ["7000000"]); payload.items[0].proposed_term_months = 10
    updated = plan_service.create_new_plan_version(plan["financing_plan_id"], payload, created_by="operator")
    ids = [x["plan_version_id"] for x in updated["versions"]]
    return reports, ids[0], ids[1]


def test_version_diff_amount(factory):
    reports, a, b = two_versions(factory)
    diff = reports.compare_plan_versions(a, b)
    assert "covered_amount" in diff["amount_changes"] and "funding_gap" in diff["amount_changes"]


def test_version_diff_products(factory):
    reports, a, b = two_versions(factory)
    assert reports.compare_plan_versions(a, b)["product_changes"]["changed"][0]["changes"]["proposed_amount"]


def test_version_diff_conditions(factory):
    reports, a, b = two_versions(factory)
    with factory.begin() as db:
        db.add(FinancingPlanCondition(condition_id="extra", plan_version_id=b, condition_type="other", title="补充说明", description="", source_type="manual", status="pending", required=1))
    assert "补充说明" in reports.compare_plan_versions(a, b)["condition_changes"]["added"]


def test_version_diff_materials(factory):
    reports, a, b = two_versions(factory)
    with factory.begin() as db:
        row = db.scalar(select(FinancingPlanMaterial).where(FinancingPlanMaterial.plan_version_id == b))
        row.status = "verified"
    assert reports.compare_plan_versions(a, b)["material_changes"]["changed"][0]["to"] == "verified"


def test_internal_report_contains_audit(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    report = reports.generate_report(selected["selection_id"], "internal", generated_by="admin")
    assert "audit" in report["structured_payload"] and "source" in report["structured_payload"]


def test_customer_report_hides_internal_fields(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    payload = reports.generate_report(selected["selection_id"], "customer", generated_by="operator")["structured_payload"]
    text = str(payload)
    assert "audit" not in payload and "facts_hash" not in text and "catalog_hash" not in text and "source_id" not in text


def test_customer_report_no_rule_ids(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    assert "rule_id" not in str(reports.generate_report(selected["selection_id"], "customer", generated_by="operator")["structured_payload"])


def test_customer_report_no_technical_fact_paths(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    assert "financial.debt_asset_ratio" not in str(reports.generate_report(selected["selection_id"], "customer", generated_by="operator")["structured_payload"])


def test_report_snapshot_frozen(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    report = reports.generate_report(selected["selection_id"], "customer", generated_by="operator")
    with factory.begin() as db: db.scalar(select(FinancingRequirement)).requested_amount = Decimal("9000000")
    assert reports.get_report(report["report_id"])["structured_payload"]["requirement"]["amount"] == "8000000.00"


def test_new_plan_version_requires_new_report(factory):
    plan = confirmed_plan(factory); plan_service, reports = services(factory); selected = selection_for(factory, plan)
    first = reports.generate_report(selected["selection_id"], "internal", generated_by="operator")
    payload = draft(factory, ["7000000"]); plan_service.create_new_plan_version(plan["financing_plan_id"], payload, created_by="operator")
    assert reports.get_report(first["report_id"])["primary_plan_version_id"] == plan["current_version_id"]


def test_html_render(factory):
    plan = confirmed_plan(factory); _, reports = services(factory); selected = selection_for(factory, plan)
    value = reports.generate_report(selected["selection_id"], "customer", generated_by="operator")
    assert "<!doctype html>" in value["rendered_html"] and DISCLAIMER in value["rendered_html"] and "@page" in value["rendered_html"]


def test_report_amount_format_integer():
    assert format_report_amount_wan("8000000") == "800万元"
    assert format_report_amount_wan("5000000") == "500万元"
    assert format_report_amount_wan("3500000") == "350万元"


def test_report_amount_format_decimal():
    assert format_report_amount_wan("125000") == "12.5万元"


def test_report_amount_format_zero():
    assert format_report_amount_wan(0) == "0万元"


def test_report_amount_preserves_precision():
    assert format_report_amount_wan(Decimal("123456.78")) == "12.345678万元"


def test_report_matching_summary_displays_zero():
    payload = {
        "report_type": "internal", "title": "融资规划方案", "customer_name": "测试客户",
        "requirement": {"amount": "8000000", "purpose": "材料采购", "term": "12个月"},
        "primary_status": "暂未形成正式主方案", "plans": [], "next_actions": [],
        "matching_summary": {"eligible": 0, "conditional": 0, "ineligible": 3, "manual_review": 2, "product_configuration_error": 0},
        "selection": {"id": "selection", "selected_by": "operator"},
        "source": {"match_snapshot_id": "snapshot"}, "audit": {"manual_overrides": []}, "disclaimer": DISCLAIMER,
    }
    rendered = render_financing_plan_html(payload)
    assert "符合当前已知硬条件：0" in rendered
    assert "条件性匹配：0" in rendered
    assert "不符合明确硬条件：3" in rendered
    assert "需要人工复核：2" in rendered
    assert "产品配置异常：0" in rendered


def test_report_matching_summary_preserves_unknown():
    payload = {
        "report_type": "internal", "title": "融资规划方案", "customer_name": "测试客户",
        "requirement": {"amount": "8000000", "purpose": "材料采购", "term": "12个月"},
        "primary_status": "暂未形成正式主方案", "plans": [], "next_actions": [],
        "matching_summary": {}, "selection": {"id": "selection", "selected_by": "operator"},
        "source": {"match_snapshot_id": "snapshot"}, "audit": {"manual_overrides": []}, "disclaimer": DISCLAIMER,
    }
    rendered = render_financing_plan_html(payload)
    assert "符合当前已知硬条件：未计算" in rendered
    assert "产品配置异常：未计算" in rendered


def test_internal_report_amount_unit(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}]); _, reports = services(factory)
    selected = selection_for(factory)
    with factory.begin() as db:
        snapshot = db.scalar(select(ProductMatchSnapshot))
        snapshot.summary_json = '{"eligible":0,"conditional":0,"ineligible":3,"manual_review":2,"product_configuration_error":0}'
    html = reports.generate_report(selected["selection_id"], "internal", generated_by="operator")["rendered_html"]
    assert "融资需求：800万元" in html and "当前覆盖：0万元" in html and "当前缺口：800万元" in html
    assert "8,000,000元" not in html


def test_customer_report_amount_unit(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}]); _, reports = services(factory)
    html = reports.generate_report(selection_for(factory)["selection_id"], "customer", generated_by="operator")["rendered_html"]
    assert "融资需求：800万元" in html and "当前覆盖：0万元" in html and "当前缺口：800万元" in html
    assert "8,000,000元" not in html


def test_existing_report_snapshot_immutable(factory, monkeypatch):
    seed(factory, [{"status": "manual_review", "max_amount": None}]); _, reports = services(factory)
    report = reports.generate_report(selection_for(factory)["selection_id"], "customer", generated_by="operator")
    frozen_html = report["rendered_html"]
    monkeypatch.setattr("backend.services.financing_plan_report_service.render_financing_plan_html", lambda payload: "changed")
    assert reports.get_report(report["report_id"])["rendered_html"] == frozen_html


@pytest.mark.asyncio
async def test_pdf_render(monkeypatch):
    class Page:
        async def set_content(self, value, **kwargs): assert "融资规划方案" in value
        async def emulate_media(self, **kwargs): pass
        async def evaluate(self, value): pass
        async def pdf(self, **kwargs): return b"%PDF-1.4 test"
    class Context:
        async def route(self, *args): pass
        async def new_page(self): return Page()
    class Browser:
        async def new_context(self, **kwargs): return Context()
        async def close(self): pass
    class Chromium:
        async def launch(self, **kwargs): return Browser()
    class Manager:
        chromium = Chromium()
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    package = types.ModuleType("playwright")
    async_api = types.ModuleType("playwright.async_api")
    async_api.async_playwright = lambda: Manager()
    package.async_api = async_api
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.async_api", async_api)
    from backend.services.financing_plan_report_service import render_financing_plan_pdf
    assert (await render_financing_plan_pdf("<h1>融资规划方案</h1>")).startswith(b"%PDF")


@pytest.mark.asyncio
async def test_html_pdf_content_consistency(monkeypatch):
    captured = {}
    class Page:
        async def set_content(self, value, **kwargs): captured["html"] = value
        async def emulate_media(self, **kwargs): pass
        async def evaluate(self, value): pass
        async def pdf(self, **kwargs): return b"%PDF-1.4 consistent"
    class Context:
        async def route(self, *args): pass
        async def new_page(self): return Page()
    class Browser:
        async def new_context(self, **kwargs): return Context()
        async def close(self): pass
    class Chromium:
        async def launch(self, **kwargs): return Browser()
    class Manager:
        chromium = Chromium()
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    package = types.ModuleType("playwright")
    async_api = types.ModuleType("playwright.async_api")
    async_api.async_playwright = lambda: Manager()
    package.async_api = async_api
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.async_api", async_api)
    html = render_financing_plan_html({
        "report_type": "customer", "title": "融资规划方案", "customer_name": "上海意川建筑科技有限公司",
        "requirement": {"amount": "8000000", "purpose": "材料采购", "term": "12个月"},
        "primary_status": "暂未形成正式主方案", "plans": [], "next_actions": [], "disclaimer": DISCLAIMER,
    })
    from backend.services.financing_plan_report_service import render_financing_plan_pdf
    await render_financing_plan_pdf(html)
    assert captured["html"] == html


def test_report_without_primary_plan(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}]); _, reports = services(factory)
    report = reports.generate_report(selection_for(factory)["selection_id"], "customer", generated_by="operator")
    assert report["structured_payload"]["primary_status"] == "暂未形成正式主方案"


def test_shanghai_yichuan_report_no_fake_primary(factory):
    seed(factory, [{"status": "manual_review", "max_amount": None}, {"status": "manual_review", "max_amount": None}, {"status": "ineligible", "max_amount": Decimal("5000000")}]); _, reports = services(factory)
    payload = reports.generate_report(selection_for(factory)["selection_id"], "customer", generated_by="operator")["structured_payload"]
    assert payload["plans"] == [] and payload["requirement"]["amount"] == "8000000.00"


def base_payload():
    return {"requirement": {"amount": "8000000"}, "plans": [{"role": "primary", "target_amount": "8000000", "covered_amount": "8000000", "funding_gap": "0", "items": [{"product_code": "A", "institution_name": "银行", "product_name": "产品", "proposed_amount": "8000000", "proposed_term_months": 12}]}]}


def test_llm_cannot_change_amount():
    base = base_payload(); changed = copy.deepcopy(base); changed["plans"][0]["items"][0]["proposed_amount"] = "9000000"
    assert not validate_llm_report_enhancement(base, changed)


def test_llm_cannot_add_product():
    base = base_payload(); changed = copy.deepcopy(base); changed["plans"][0]["items"].append({"product_code": "B", "institution_name": "银行", "product_name": "新增", "proposed_amount": "1", "proposed_term_months": 12})
    assert not validate_llm_report_enhancement(base, changed)


def test_llm_fallback_to_template():
    base = base_payload(); changed = copy.deepcopy(base); changed["plans"] = []
    assert use_validated_llm_enhancement(base, changed) is base
