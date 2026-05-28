from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from backend.services.kyc_extraction_review_service import (
    build_confirmed_data,
    build_extraction_review,
    can_update_review,
    is_kyc_extraction,
)
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


def kyc_extraction() -> dict:
    return {
        "extraction_id": "ext-1",
        "doc_id": "doc-1",
        "customer_id": "customer-1",
        "extraction_type": "business_license",
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "business_license",
            "doc_type_name": "营业执照",
            "fields": {
                "company_name": "自动识别公司",
                "legal_representative": "李四",
            },
            "validation": {"warnings": ["统一社会信用代码缺失"], "errors": []},
            "evidence": {"company_name": {"evidence_text": "名称 自动识别公司"}},
            "missing_fields": ["unified_social_credit_code"],
        },
        "confirmed_data": {},
        "confirm_status": "unconfirmed",
        "confirmed_by": "",
        "confirmed_at": "",
    }


class FakeReviewStorage:
    def __init__(self, extraction: dict | None = None) -> None:
        self.extraction = extraction or kyc_extraction()

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return [self.extraction]

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


def test_get_review_returns_extracted_data() -> None:
    review = build_extraction_review("doc-1", kyc_extraction())

    assert review["extracted_data"]["fields"]["company_name"] == "自动识别公司"
    assert review["merged_fields"]["company_name"] == "自动识别公司"
    assert review["confirm_status"] == "unconfirmed"


def test_patch_review_does_not_overwrite_extracted_data() -> None:
    extraction = kyc_extraction()
    original_extracted_data = extraction["extracted_data"].copy()
    confirmed_data = build_confirmed_data(
        existing=extraction.get("confirmed_data"),
        confirmed_fields={"company_name": "人工确认公司"},
        confirm_status="partial",
        confirmed_by="op",
        confirmed_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
    )
    updated = {
        **extraction,
        "confirmed_data": confirmed_data,
        "confirm_status": "partial",
        "confirmed_by": "op",
        "confirmed_at": "2026-05-28T00:00:00+00:00",
    }
    review = build_extraction_review("doc-1", updated)

    assert updated["extracted_data"] == original_extracted_data
    assert review["confirmed_data"]["confirmed_fields"]["company_name"] == "人工确认公司"
    assert review["merged_fields"]["company_name"] == "人工确认公司"


def test_patch_review_saves_confirmed_data() -> None:
    confirmed_data = build_confirmed_data(
        existing={},
        confirmed_fields={"legal_representative": "王五"},
        confirm_status="confirmed",
        confirmed_by="admin",
        confirmed_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
    )

    assert confirmed_data["confirmed_fields"]["legal_representative"] == "王五"
    assert confirmed_data["confirm_status"] == "confirmed"
    assert confirmed_data["confirmed_by"] == "admin"


def test_kyc_profile_prefers_confirmed_data() -> None:
    extraction = kyc_extraction()
    extraction["confirmed_data"] = {
        "confirmed_fields": {"company_name": "人工确认公司"},
        "confirm_status": "confirmed",
    }
    extraction["confirm_status"] = "confirmed"
    profile = asyncio.run(build_customer_kyc_profile(FakeReviewStorage(extraction), "customer-1"))

    assert profile["enterprise_identity"]["company_name"] == "人工确认公司"
    assert profile["enterprise_identity"]["field_sources"]["company_name"]["source"] == "confirmed_data"
    assert profile["enterprise_identity"]["field_sources"]["company_name"]["confirmed"] is True


def test_viewer_cannot_patch() -> None:
    assert can_update_review("viewer") is False
    assert can_update_review("operator") is True
    assert can_update_review("admin") is True


def test_non_kyc_patch_returns_unsupported() -> None:
    extraction = kyc_extraction()
    extraction["extracted_data"] = {"agent_type": "financial_report_agent", "fields": {}}

    assert is_kyc_extraction(extraction) is False


def test_confirm_status_is_saved() -> None:
    confirmed_data = build_confirmed_data(
        existing={},
        confirmed_fields={"company_name": "人工确认公司"},
        confirm_status="confirmed",
        confirmed_by="op",
        confirmed_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
    )
    updated = {**kyc_extraction(), "confirmed_data": confirmed_data, "confirm_status": "confirmed"}
    review = build_extraction_review("doc-1", updated)

    assert review["confirm_status"] == "confirmed"
