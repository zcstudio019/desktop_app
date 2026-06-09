import asyncio

from backend.services.kyc_profile_sync_service import build_customer_kyc_profile
from backend.services.markdown_profile_service import _build_document_sections


class FakeStorage:
    def __init__(self, extractions, documents=None):
        self.extractions = extractions
        self.documents = documents or {}

    async def get_business_extractions_by_customer(self, customer_id):
        return self.extractions

    async def get_extractions_by_customer(self, customer_id):
        return self.extractions

    async def get_document(self, doc_id):
        return self.documents.get(doc_id, {"file_name": "法人身份证扫描件.pdf", "file_path": "/tmp/id-card.pdf"})

    async def list_documents(self, customer_id):
        return list(self.documents.values())


def _id_card_extraction(doc_id, created_at, fields, markdown="- 姓名：暂无"):
    return {
        "extraction_id": f"ext-{doc_id}",
        "doc_id": doc_id,
        "extraction_type": "id_card",
        "created_at": created_at,
        "updated_at": created_at,
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "id_card",
            "doc_type_name": "身份证",
            "fields": fields,
            "markdown": markdown,
        },
    }


def test_markdown_profile_uses_latest_valid_id_card_fields():
    storage = FakeStorage(
        [
            _id_card_extraction("old", "2026-01-01T00:00:00", {}),
            _id_card_extraction(
                "new",
                "2026-01-02T00:00:00",
                {
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
            ),
        ],
        {
            "old": {"file_name": "法人身份证扫描件.pdf", "file_path": "/tmp/old.pdf"},
            "new": {"file_name": "法人身份证扫描件.pdf", "file_path": "/tmp/new.pdf"},
        },
    )

    sections, _ = asyncio.run(_build_document_sections(storage, "customer-1"))
    markdown = "\n".join(sections)

    assert "姓名：黎云" in markdown
    assert "身份证号码：330225198102121999" in markdown
    assert "有效期限：2014-06-11 至 2034-06-11" in markdown
    assert "姓名：暂无" not in markdown
    assert "未识别到身份证正反面关键信息" not in markdown
    assert "未从 OCR 文本中识别到身份证字段" not in markdown


def test_kyc_profile_reads_latest_id_card_fields():
    storage = FakeStorage(
        [
            _id_card_extraction("old", "2026-01-01T00:00:00", {}),
            _id_card_extraction(
                "new",
                "2026-01-02T00:00:00",
                {
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
            ),
        ],
        {"new": {"file_name": "法人身份证扫描件.pdf", "file_path": "/tmp/new.pdf"}},
    )

    profile = asyncio.run(build_customer_kyc_profile(storage, "customer-1"))
    person = profile["person_identity"]

    assert person["name"] == "黎云"
    assert person["id_number"] == "330225198102121999"
    assert person["ethnicity"] == "汉"
    assert person["issuing_authority"] == "象山县公安局"
    assert person["valid_from"] == "2014-06-11"
    assert person["valid_to"] == "2034-06-11"


def test_confirmed_id_card_fields_take_priority():
    extraction = _id_card_extraction(
        "doc",
        "2026-01-02T00:00:00",
        {"name": "旧姓名", "id_number": "330225198102121999"},
    )
    extraction["confirmed_data"] = {
        "confirmed_fields": {
            "name": "黎云",
            "id_number": "330225198102121998",
        }
    }
    storage = FakeStorage([extraction], {"doc": {"file_name": "法人身份证扫描件.pdf", "file_path": "/tmp/id.pdf"}})

    profile = asyncio.run(build_customer_kyc_profile(storage, "customer-1"))

    assert profile["person_identity"]["name"] == "黎云"
    assert profile["person_identity"]["id_number"] == "330225198102121998"
