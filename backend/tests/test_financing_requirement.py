from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.db_models import FinancingRequirement
from backend.services.financing_requirement_service import (
    RequirementPatch, confirm_requirement, create_requirement_draft,
    extract_requirement_patch, get_requirement, suggest_purpose_from_explicit_text,
)


@pytest.fixture
def factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[FinancingRequirement.__table__])
    yield sessionmaker(bind=engine)
    engine.dispose()


def make_draft(factory, **changes):
    return create_requirement_draft("135", {
        "requested_amount": 5_000_000, "amount_confirmed": True,
        "financing_purpose": "采购", "purpose_detail": "材料采购",
        "term_value": 12, "term_unit": "month", "term_confirmed": True, **changes,
    }, "alice", borrower_name="上海意川建筑科技有限公司", session_factory=factory)


def version_count(factory):
    with factory() as db:
        return db.scalar(select(func.count()).select_from(FinancingRequirement))


def test_get_current_requirement_is_read_only(factory):
    for _ in range(3):
        assert get_requirement("135", session_factory=factory) is None
        assert get_requirement("135", status="needs_confirmation", session_factory=factory) is None
    assert version_count(factory) == 0


def test_unchanged_edit_does_not_create_new_version(factory):
    first = make_draft(factory)
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    with pytest.raises(ValueError, match="未发生变化"):
        create_requirement_draft("135", {
            "requested_amount": 5_000_000.0, "financing_purpose": "采购",
            "purpose_detail": "材料采购", "term_value": 12, "term_unit": "month",
            "term_original": "12个月", "guarantee_preference": [],
        }, "alice", session_factory=factory)
    assert version_count(factory) == 1
    assert get_requirement("135", session_factory=factory)["version"] == 1


def test_pending_unchanged_edit_does_not_create_new_version(factory):
    first = make_draft(factory)
    with pytest.raises(ValueError, match="未发生变化"):
        create_requirement_draft("135", {"requested_amount": "5000000.00"}, "alice", session_factory=factory)
    assert version_count(factory) == 1
    assert get_requirement("135", status="needs_confirmation", session_factory=factory)["requirement_id"] == first["requirement_id"]


@pytest.mark.parametrize("patch", [
    {"requested_amount": 8_000_000},
    {"term_value": 24},
    {"financing_purpose": "项目垫资"},
    {"excluded_banks": ["乙银行"]},
])
def test_changed_field_creates_new_version(factory, patch):
    first = make_draft(factory)
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    second = create_requirement_draft("135", patch, "alice", session_factory=factory)
    assert second["version"] == 2
    assert version_count(factory) == 2


def test_list_order_and_empty_strings_do_not_create_version(factory):
    first = make_draft(factory, excluded_banks=["乙银行", "甲银行"])
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    with pytest.raises(ValueError, match="未发生变化"):
        create_requirement_draft("135", {"excluded_banks": ["甲银行", "乙银行"], "registered_region": ""}, "alice", session_factory=factory)
    assert version_count(factory) == 1


def test_draft_sources_are_server_recorded(factory):
    manual = make_draft(factory)
    assert manual["draft_source"] == "manual_form"
    chat = create_requirement_draft("135", {"requested_amount": 8_000_000}, "alice",
                                    draft_source="chat_user_input", session_factory=factory)
    assert chat["draft_source"] == "chat_user_input"


def test_production_regression_does_not_use_real_customer(factory):
    with pytest.raises(ValueError, match="专用测试客户"):
        create_requirement_draft("enterprise_上海意川建筑科技有限公司", {"requested_amount": 5_000_000},
                                 "regression", draft_source="production_regression", session_factory=factory)
    assert version_count(factory) == 0
    with pytest.raises(ValueError, match="专用测试客户"):
        create_requirement_draft("enterprise_上海意川建筑科技有限公司", {"requested_amount": 5_000_000},
                                 "step6_regression", session_factory=factory)
    assert version_count(factory) == 0
    row = create_requirement_draft("enterprise_融资需求回归测试客户", {"requested_amount": 5_000_000},
                                   "regression", draft_source="production_regression", session_factory=factory)
    assert row["version"] == 1 and row["draft_source"] == "production_regression"


def test_requirement_source_chat_user_input(factory, monkeypatch):
    from backend.services import assistant_financing_requirement_service as assistant

    class Storage:
        async def get_customer(self, _customer_id):
            return {"customer_id": "135", "name": "意川", "uploader": "alice"}

    async def access(_customer_id, _user):
        return {"name": "意川"}

    original_create = create_requirement_draft
    monkeypatch.setattr(assistant, "require_customer_access", access)
    monkeypatch.setattr(assistant, "create_requirement_draft", lambda cid, patch, actor, **kw:
                        original_create(cid, patch, actor, session_factory=factory, **kw))
    result = asyncio.run(assistant.handle_financing_requirement(
        Storage(), "这个客户想融资500万，用于材料采购，期限一年", "135", {"username": "alice"}))
    assert result["data"]["requirement"]["draft_source"] == "chat_user_input"
    assert result["data"]["requirement"]["version"] == 1
    assert get_requirement("135", session_factory=factory) is None


def test_create_financing_requirement_draft(factory):
    draft = make_draft(factory)
    assert draft["version"] == 1 and draft["status"] == "needs_confirmation"
    assert draft["requested_amount"] == 5_000_000
    assert get_requirement("135", session_factory=factory) is None


def test_confirm_financing_requirement(factory):
    draft = make_draft(factory)
    confirmed = confirm_requirement("135", draft["requirement_id"], "alice", session_factory=factory)
    assert confirmed["status"] == "confirmed" and confirmed["confirmed_by"] == "alice"
    assert confirmed["confirmed_at"] and confirmed["field_sources"]["requested_amount"] == "user_confirmed"


@pytest.mark.parametrize("missing", ["requested_amount", "financing_purpose", "term_value", "borrower_entity"])
def test_requirement_requires_core_fields(factory, missing):
    patch = {"requested_amount": 5_000_000, "amount_confirmed": True,
             "financing_purpose": "采购", "term_value": 12, "term_unit": "month", "term_confirmed": True}
    if missing != "borrower_entity":
        patch.pop(missing)
    draft = create_requirement_draft("135", patch, "alice", borrower_name=None if missing == "borrower_entity" else "意川", session_factory=factory)
    with pytest.raises(ValueError):
        confirm_requirement("135", draft["requirement_id"], "alice", session_factory=factory)
    assert get_requirement("135", session_factory=factory) is None


def test_requirement_missing_amount_needs_confirmation(factory):
    draft = create_requirement_draft("135", {"financing_purpose": "采购"}, "alice", borrower_name="意川", session_factory=factory)
    assert draft["status"] == "needs_confirmation"


def test_requirement_missing_purpose_needs_confirmation(factory):
    draft = create_requirement_draft("135", {"requested_amount": 5_000_000}, "alice", borrower_name="意川", session_factory=factory)
    assert draft["status"] == "needs_confirmation"


def test_requirement_missing_term_needs_confirmation(factory):
    draft = create_requirement_draft("135", {"financing_purpose": "采购"}, "alice", borrower_name="意川", session_factory=factory)
    assert draft["status"] == "needs_confirmation"


def test_update_requirement_creates_new_version(factory):
    first = make_draft(factory)
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    second = create_requirement_draft("135", {"requested_amount": 8_000_000, "amount_confirmed": True}, "alice", session_factory=factory)
    assert second["version"] == 2 and second["financing_purpose"] == "采购"
    assert get_requirement("135", session_factory=factory)["version"] == 1


def test_old_requirement_becomes_superseded(factory):
    first = make_draft(factory)
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    second = create_requirement_draft("135", {"requested_amount": 8_000_000, "amount_confirmed": True}, "alice", session_factory=factory)
    confirm_requirement("135", second["requirement_id"], "alice", session_factory=factory)
    with factory() as db:
        old = db.execute(select(FinancingRequirement).where(FinancingRequirement.requirement_id == first["requirement_id"])).scalar_one()
        assert old.status == "superseded"
    assert get_requirement("135", session_factory=factory)["version"] == 2


def test_get_current_confirmed_requirement(factory):
    assert get_requirement("135", session_factory=factory) is None
    first = make_draft(factory)
    confirm_requirement("135", first["requirement_id"], "alice", session_factory=factory)
    assert get_requirement("135", session_factory=factory)["requirement_id"] == first["requirement_id"]


def test_llm_cannot_auto_confirm_requirement(factory):
    draft = make_draft(factory)
    assert draft["status"] != "confirmed"
    assert get_requirement("135", session_factory=factory) is None


def test_report_context_cannot_invent_requirement(factory):
    assert get_requirement("135", session_factory=factory) is None
    assert extract_requirement_patch("报告显示企业流水经营入账1949万元") is None


def test_missing_assets_does_not_set_accept_mortgage_false(factory):
    draft = make_draft(factory)
    assert draft["accept_mortgage"] is None


def test_existing_bank_is_not_same_as_preferred_bank(factory):
    draft = make_draft(factory, existing_banks=["甲银行"])
    assert draft["existing_banks"] == ["甲银行"] and draft["preferred_banks"] == []


def test_excluded_bank_is_persisted(factory):
    draft = make_draft(factory, excluded_banks=["乙银行"])
    assert draft["excluded_banks"] == ["乙银行"]


def test_requirement_is_customer_isolated(factory):
    first = make_draft(factory)
    assert get_requirement("other", status="needs_confirmation", session_factory=factory) is None
    with pytest.raises(LookupError):
        confirm_requirement("other", first["requirement_id"], "bob", session_factory=factory)


def test_extract_explicit_yichuan_requirement():
    patch = extract_requirement_patch("上海意川这次想融资500万，用于材料采购，期限一年。")
    assert patch is not None
    assert patch.requested_amount == 5_000_000
    assert patch.financing_purpose == "采购" and patch.purpose_detail == "材料采购"
    assert patch.term_value == 12 and patch.term_unit == "month"


def test_approximate_amount_cannot_be_confirmed(factory):
    patch = extract_requirement_patch("客户想融资大概500万左右，用于材料采购，期限一年")
    assert patch is not None and patch.amount_confirmed is False
    draft = create_requirement_draft("135", patch, "alice", borrower_name="意川", session_factory=factory)
    with pytest.raises(ValueError):
        confirm_requirement("135", draft["requirement_id"], "alice", session_factory=factory)


def test_bank_exclusion_and_mortgage_preference_are_explicit():
    assert extract_requirement_patch("客户不接受抵押").accept_mortgage is False
    assert extract_requirement_patch("客户不做招商银行").excluded_banks == ["招商银行"]


def test_llm_purpose_requires_verbatim_evidence():
    message = "客户需要融资500万，用于支付安装工程进度款"
    valid = lambda _prompt, _text: '{"financing_purpose":"支付工程款","purpose_detail":"支付安装工程进度款","evidence":"用于支付安装工程进度款"}'
    invented = lambda _prompt, _text: '{"financing_purpose":"采购","purpose_detail":"材料采购","evidence":"材料采购"}'
    assert suggest_purpose_from_explicit_text(message, valid).financing_purpose == "支付工程款"
    assert suggest_purpose_from_explicit_text(message, invented) is None
    assert suggest_purpose_from_explicit_text("客户需要融资500万", valid) is None


def test_requirement_intent_does_not_capture_comprehensive_report():
    from backend.services.assistant_comprehensive_financing_analysis_service import is_comprehensive_financing_analysis_request
    from backend.services.assistant_financing_requirement_service import is_financing_requirement_request
    assert is_comprehensive_financing_analysis_request("根据上海意川资料生成综合融资分析报告")
    assert not is_financing_requirement_request("根据上海意川资料生成综合融资分析报告")
    assert is_financing_requirement_request("上海意川这次想融资500万，用于材料采购，期限一年")
    assert is_financing_requirement_request("上海意川当前融资需求是什么？")


def test_requirement_http_access_and_confirmation(factory, monkeypatch):
    import httpx
    from fastapi import FastAPI
    from backend.middleware.auth import get_current_user
    from backend.routers import financing_requirement as route
    from backend.services import financing_requirement_service as service

    class Storage:
        async def get_customer(self, customer_id):
            return {"customer_id": customer_id, "name": "意川", "uploader": "alice"} if customer_id == "135" else None
        async def customer_has_document_uploader(self, _customer_id, _username):
            return False

    monkeypatch.setattr(route, "get_storage_service", lambda: Storage())
    monkeypatch.setattr(route, "create_requirement_draft", lambda cid, patch, actor, **kw: service.create_requirement_draft(cid, patch, actor, session_factory=factory, **kw))
    monkeypatch.setattr(route, "get_requirement", lambda cid, **kw: service.get_requirement(cid, session_factory=factory, **kw))
    monkeypatch.setattr(route, "confirm_requirement", lambda cid, rid, actor: service.confirm_requirement(cid, rid, actor, session_factory=factory))
    app = FastAPI()
    app.include_router(route.router, prefix="/api")
    current = {"username": "bob", "role": "operator"}
    app.dependency_overrides[get_current_user] = lambda: current

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            path = "/api/customers/135/financing-requirements"
            assert (await client.get(path + "/current")).status_code == 403
            current["username"] = "alice"
            assert (await client.get(path + "/current")).json()["requirement"] is None
            assert (await client.get(path + "/pending")).json()["requirement"] is None
            assert version_count(factory) == 0
            draft = (await client.post(path + "/draft", json={"requested_amount": 5_000_000, "amount_confirmed": True,
                "financing_purpose": "采购", "term_value": 12, "term_unit": "month", "term_confirmed": True})).json()["requirement"]
            assert draft["status"] == "needs_confirmation"
            assert draft["draft_source"] == "manual_form"
            assert (await client.get(path + "/pending")).json()["requirement"]["version"] == 1
            unchanged = await client.post(path + "/draft", json={"requested_amount": 5_000_000})
            assert unchanged.status_code == 422 and "未发生变化" in unchanged.json()["detail"]
            assert version_count(factory) == 1
            assert (await client.get(path + "/current")).json()["requirement"] is None
            confirmed = (await client.post(path + "/" + draft["requirement_id"] + "/confirm")).json()["requirement"]
            assert confirmed["status"] == "confirmed"
            assert (await client.get(path + "/current")).json()["requirement"]["version"] == 1
            assert (await client.get("/api/customers/other/financing-requirements/current")).status_code == 404
    asyncio.run(exercise())
