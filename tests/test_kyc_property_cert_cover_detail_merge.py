from __future__ import annotations

import asyncio

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


COVER_TEXT = """
根据《中华人民共和国物权法》等法律法规
为保护不动产权利人合法权益
经审查核实 准予登记 颁发此证
登记机构
中华人民共和国国土资源部监制
编号 N-D31001337469
"""

DETAIL_TEXT = """
不动产权证书
权利人 沃志方
共有情况 单独所有
沪(2018)徐字不动产权第015979号
坐落 华发路406弄10号
不动产单元号 310104019001GB00045F00430086
权利类型 国有建设用地使用权/房屋所有权
权利性质 土地权利性质：出让
用途：土地用途：住宅 / 房屋用途：居住
面积：宗地面积：135460.00平方米 / 建筑面积：62.40平方米
使用期限 2015年10月16日起2076年12月28日止
地号 徐汇区华泾镇448街坊2/3丘
室号或部位 1705
建筑类型 公寓
总层数 29
竣工日期 2011年
"""


class FakeStorage:
    def __init__(self, extractions):
        self.extractions = extractions

    async def get_extractions_by_customer(self, customer_id: str):
        return self.extractions


def _kyc_result(text: str, filename: str) -> dict:
    return run_kyc_document_agent({
        "text": text,
        "pages": [],
        "metadata": {"filename": filename, "declared_doc_type": "property_cert", "customer_id": "customer-1"},
    })


def test_cover_page_does_not_extract_fake_owner():
    result = _kyc_result(COVER_TEXT, "房产.pdf")

    assert result["doc_type"] == "property_cert"
    assert result.get("page_role") == "cover_page"
    assert result["fields"] == {}
    assert "仅识别到房产证/不动产权证封面或说明页" in " ".join(result["validation"]["warnings"])
    assert "权利人" not in result["fields"]


def test_mixed_pages_use_detail_page_for_extraction():
    result = run_kyc_document_agent({
        "text": f"{COVER_TEXT}\n\n{DETAIL_TEXT}",
        "pages": [
            {"page": 1, "text": COVER_TEXT},
            {"page": 2, "text": DETAIL_TEXT},
        ],
        "metadata": {"filename": "房产合并.pdf", "declared_doc_type": "property_cert", "customer_id": "customer-1"},
    })

    assert result.get("page_role") == "detail_page"
    assert result["fields"]["权利人"] == "沃志方"
    assert result["fields"]["不动产单元号"] == "310104019001GB00045F00430086"


def test_profile_prefers_complete_detail_property_over_empty_cover():
    cover = _kyc_result(COVER_TEXT, "房产.pdf")
    detail = _kyc_result(DETAIL_TEXT, "房产正面.pdf")
    storage = FakeStorage([
        {
            "extraction_id": "cover-ext",
            "doc_id": "cover-doc",
            "customer_id": "customer-1",
            "extraction_type": "property_cert",
            "extracted_data": cover,
            "created_at": "2026-06-05T10:00:00",
        },
        {
            "extraction_id": "detail-ext",
            "doc_id": "detail-doc",
            "customer_id": "customer-1",
            "extraction_type": "property_cert",
            "extracted_data": detail,
            "created_at": "2026-06-05T10:01:00",
        },
    ])

    profile = asyncio.run(build_customer_kyc_profile(storage, "customer-1"))
    properties = profile["assets"]["properties"]

    assert properties[0]["source_document_id"] == "detail-doc"
    assert properties[0]["owner"] == "沃志方"
    assert properties[0]["property_unit_number"] == "310104019001GB00045F00430086"
    assert properties[0]["property_address"] == "华发路406弄10号"
    assert properties[0]["use_type"] == "居住"
    assert properties[0]["land_use"] == "住宅"


def test_property_cert_extraction_does_not_include_financial_context():
    result = run_kyc_document_agent({
        "text": DETAIL_TEXT,
        "pages": [],
        "metadata": {
            "filename": "房产正面.pdf",
            "declared_doc_type": "property_cert",
            "historical_financial_reports": [{"document_type": "financial_report"}],
        },
    })
    payload = str(result)

    assert "historical_financial_reports" not in payload
    assert "financial_report" not in payload
