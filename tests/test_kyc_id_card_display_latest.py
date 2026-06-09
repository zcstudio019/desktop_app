from __future__ import annotations

import asyncio

from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


class FakeStorage:
    def __init__(self, extractions):
        self._extractions = extractions

    async def get_extractions_by_customer(self, customer_id: str):
        return self._extractions

    async def list_documents(self, customer_id: str):
        return []


def test_kyc_profile_uses_latest_id_card_with_fields():
    extractions = [
        {
            "extraction_id": "old-empty",
            "doc_id": "doc-old",
            "customer_id": "customer_1",
            "extraction_type": "id_card",
            "created_at": "2026-06-09T10:00:00",
            "extracted_data": {
                "agent_type": "kyc_document_agent",
                "doc_type": "id_card",
                "doc_type_name": "居民身份证",
                "fields": {},
            },
            "confirmed_data": {},
        },
        {
            "extraction_id": "new-fields",
            "doc_id": "doc-new",
            "customer_id": "customer_1",
            "extraction_type": "id_card",
            "created_at": "2026-06-09T11:00:00",
            "extracted_data": {
                "agent_type": "kyc_document_agent",
                "doc_type": "id_card",
                "doc_type_name": "居民身份证",
                "fields": {
                    "name": "黎云",
                    "gender": "男",
                    "ethnicity": "汉",
                    "birth_date": "1981-02-12",
                    "address": "浙江省象山县丹西街道六升村1组39号",
                    "id_number": "330225198102121999",
                    "issuing_authority": "象山县公安局",
                    "valid_from": "2014-06-11",
                    "valid_to": "2034-06-11",
                },
            },
            "confirmed_data": {},
        },
    ]

    profile = asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer_1"))

    assert profile["person_identity"]["name"] == "黎云"
    assert profile["person_identity"]["id_number"] == "330225198102121999"
    assert profile["person_identity"]["source_document_id"] == "doc-new"


def test_kyc_profile_confirmed_id_card_fields_have_priority():
    extractions = [
        {
            "extraction_id": "with-confirmed",
            "doc_id": "doc-confirmed",
            "customer_id": "customer_1",
            "extraction_type": "id_card",
            "created_at": "2026-06-09T11:00:00",
            "extracted_data": {
                "agent_type": "kyc_document_agent",
                "doc_type": "id_card",
                "doc_type_name": "居民身份证",
                "fields": {
                    "name": "OCR姓名",
                    "id_number": "330225198102121999",
                    "address": "OCR地址",
                },
            },
            "confirmed_data": {
                "confirmed_fields": {
                    "name": "黎云",
                    "address": "浙江省象山县丹西街道六升村1组39号",
                }
            },
        }
    ]

    profile = asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer_1"))

    assert profile["person_identity"]["name"] == "黎云"
    assert profile["person_identity"]["address"] == "浙江省象山县丹西街道六升村1组39号"
