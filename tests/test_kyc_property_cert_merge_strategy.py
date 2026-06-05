from __future__ import annotations

import asyncio

from backend.document_types import should_append_same_type_document
from backend.services.kyc_profile_sync_service import (
    build_customer_kyc_profile,
    score_kyc_property_cert_extraction,
)


def _property_extraction(doc_id: str, file_name: str, fields: dict, *, page_role: str = "detail_page", created_at: str = "") -> dict:
    warnings = []
    if page_role == "cover_page":
        warnings.append("仅识别到房产证/不动产权证封面或说明页，未识别到权利人、坐落、面积等字段页，请补充上传正面字段页或人工确认。")
    return {
        "extraction_id": f"ext-{doc_id}",
        "doc_id": doc_id,
        "customer_id": "customer-1",
        "extraction_type": "property_cert",
        "file_name": file_name,
        "created_at": created_at,
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "property_cert",
            "doc_type_name": "房产证/房地产权证",
            "page_role": page_role,
            "fields": fields,
            "validation": {"warnings": warnings, "errors": []},
        },
    }


class FakeStorage:
    def __init__(self, extractions: list[dict]) -> None:
        self.extractions = extractions

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return [item for item in self.extractions if item.get("customer_id") == customer_id]

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


DETAIL_EXTRACTION = _property_extraction(
    "doc-detail",
    "房产正面.pdf",
    {
        "权利人": "沃志方",
        "权证编号": "沪(2018)徐字不动产权第015979号",
        "坐落": "华发路406弄10号",
        "不动产单元号": "310104019001GB00045F00430086",
        "建筑面积": "62.40 平方米",
        "土地用途": "住宅",
        "房屋用途": "居住",
        "竣工日期": "2011年",
    },
    created_at="2026-06-05T10:00:00",
)

COVER_EXTRACTION = _property_extraction(
    "doc-cover",
    "房产.pdf",
    {},
    page_role="cover_page",
    created_at="2026-06-05T10:05:00",
)


def test_property_cert_extractions_are_multi_instance_documents():
    assert should_append_same_type_document("property_cert") is True
    assert should_append_same_type_document("real_estate_cert") is True


def test_detail_page_scores_higher_than_cover_page():
    assert score_kyc_property_cert_extraction(DETAIL_EXTRACTION) > score_kyc_property_cert_extraction(COVER_EXTRACTION)


def test_cover_page_score_is_penalized_when_fields_empty():
    assert score_kyc_property_cert_extraction(COVER_EXTRACTION) < 0


def test_later_cover_page_does_not_override_earlier_detail_page_in_profile():
    profile = asyncio.run(build_customer_kyc_profile(FakeStorage([DETAIL_EXTRACTION, COVER_EXTRACTION]), "customer-1"))
    properties = profile["assets"]["properties"]

    assert len(properties) == 2
    assert properties[0]["source_document_id"] == "doc-detail"
    assert properties[0]["owner"] == "沃志方"
    assert properties[0]["property_address"] == "华发路406弄10号"
    assert properties[0]["quality_score"] > properties[1]["quality_score"]
    assert properties[0]["display_role"] == "主资料 / 字段完整"
    assert properties[1]["display_role"] == "封面页 / 补充页"


def test_empty_cover_page_does_not_become_main_empty_property():
    profile = asyncio.run(build_customer_kyc_profile(FakeStorage([DETAIL_EXTRACTION, COVER_EXTRACTION]), "customer-1"))
    main_property = profile["assets"]["properties"][0]

    assert main_property["source_file"] == "房产正面.pdf"
    assert main_property["owner"]
    assert main_property["building_area"]
