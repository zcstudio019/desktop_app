from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_document_agent.renderer import get_display_fields
from backend.services.markdown_profile_service import _merge_kyc_property_supplement_fields
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


COVER_TEXT = """
不动产权证书
根据《中华人民共和国物权法》，为保护不动产权利人合法权益，经审查核实，准予登记，颁发此证。
登记机构 上海市徐汇区不动产登记事务中心
上海市不动产登记专用章 2018年10月23日
国土资源部监制
编号№D31001337469
"""


DETAIL_TEXT = """
不动产权证书
权利人：沃志方
共有情况：单独所有
坐落：华发路406弄10号
不动产单元号：310104019001GB00045F00430086
权利类型：国有建设用地使用权/房屋所有权
权利性质：土地权利性质：出让
用途：土地用途：住宅/房屋用途：居住
面积：宗地面积：135460.00平方米/建筑面积：62.40平方米
使用期限：国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止
土地状况：地号：徐汇区华泾镇448街坊2/3丘
房屋状况：室号部位：1705；类型：公寓；总层数：29；竣工日期：2011年
沪(2018)徐字不动产权第015979号
"""


def _extract(text: str, filename: str) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": filename, "declared_doc_type": "property_cert"},
        }
    )


def test_cover_page_extracts_registration_date_and_certificate_number():
    result = _extract(COVER_TEXT, "房产.pdf")
    fields = result["fields"]

    assert result["page_role"] == "cover_page"
    assert fields["登记日期"] == "2018年10月23日"
    assert fields["registration_date"] == "2018年10月23日"
    assert fields["登记机构"] == "上海市不动产登记专用章"
    assert fields["registration_authority"] == "上海市不动产登记专用章"
    assert fields["封面编号"] == "D31001337469"
    assert fields["cover_certificate_number"] == "D31001337469"
    assert "权证编号" not in fields
    assert "权利人" not in fields


def test_cover_page_display_has_fields_and_warning_not_empty_placeholder():
    result = _extract(COVER_TEXT, "房产.pdf")
    display_fields = get_display_fields(result)

    assert display_fields["登记日期"] == "2018年10月23日"
    assert display_fields["登记机构"] == "上海市不动产登记专用章"
    assert display_fields["封面编号"] == "D31001337469"
    assert "暂无可展示字段" not in result["markdown"]
    assert "仅识别到房产证/不动产权证封面" in " ".join(result["validation"]["warnings"])


class _FakeStorage:
    def __init__(self, extractions: list[dict]):
        self._extractions = extractions

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return self._extractions


def _extraction(doc_id: str, file_name: str, extracted_data: dict) -> dict:
    return {
        "doc_id": doc_id,
        "customer_id": "customer-1",
        "file_name": file_name,
        "extraction_type": "property_cert",
        "extracted_data": extracted_data,
        "created_at": doc_id,
    }


async def _build_profile_with_detail_and_cover() -> dict:
    detail = _extract(DETAIL_TEXT, "房产正面.pdf")
    detail["fields"].pop("登记日", None)
    detail["fields"].pop("issue_date", None)
    cover = _extract(COVER_TEXT, "房产.pdf")
    return await build_customer_kyc_profile(
        _FakeStorage(
            [
                _extraction("doc-detail", "房产正面.pdf", detail),
                _extraction("doc-cover", "房产.pdf", cover),
            ]
        ),
        "customer-1",
    )


def test_cover_page_does_not_override_detail_page_and_supplements_issue_date():
    import asyncio

    profile = asyncio.run(_build_profile_with_detail_and_cover())
    properties = profile["assets"]["properties"]

    assert properties[0]["source_document_id"] == "doc-detail"
    assert properties[0]["owner"] == "沃志方"
    assert properties[0]["certificate_number"] == "沪(2018)徐字不动产权第015979号"
    assert properties[0]["registration_date"] == "2018年10月23日"
    assert properties[0]["registration_authority"] == "上海市不动产登记专用章"
    assert properties[0]["cover_certificate_number"] == "D31001337469"
    assert properties[1]["page_role"] == "cover_page"


def test_markdown_property_merge_supplements_cover_fields_without_overriding_detail_cert_number():
    detail = _extract(DETAIL_TEXT, "房产正面.pdf")
    cover = _extract(COVER_TEXT, "房产.pdf")

    merged = _merge_kyc_property_supplement_fields(
        detail,
        [{"extracted_data": cover}],
    )
    fields = merged["fields"]

    assert fields["权证编号"] == "沪(2018)徐字不动产权第015979号"
    assert fields["登记日期"] == "2018年10月23日"
    assert fields["登记机构"] == "上海市不动产登记专用章"
    assert fields["封面编号"] == "D31001337469"
