from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_document_agent.renderer import get_display_fields
from backend.services.kyc_document_agent.skills.property_cert_skill import _extract_cover_registration_date
from backend.services.markdown_profile_service import _merge_kyc_property_supplement_fields


def _extract_cover(text: str) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "房产.pdf", "declared_doc_type": "property_cert"},
        }
    )


def test_cover_registration_date_extracts_plain_date():
    value, _ = _extract_cover_registration_date("登记机构\n不动产登记专用章\n2018年10月23日")

    assert value == "2018年10月23日"


def test_cover_registration_date_extracts_date_with_spaces():
    value, _ = _extract_cover_registration_date("cover_seal_date_ocr\n2018 年 10 月 23 日")

    assert value == "2018年10月23日"


def test_cover_registration_date_cleans_suffix_noise():
    for text in ("2018年10月23日章", "2018年10月23日印", "2018年10月23日（04）"):
        value, _ = _extract_cover_registration_date(text)
        assert value == "2018年10月23日"


def test_cover_registration_date_ignores_stamp_polluted_false_date():
    value, _ = _extract_cover_registration_date("登记机构\n2018年动登专用章日\n(04)")

    assert value == ""


def test_cover_registration_date_prefers_remove_red_stamp_region_text():
    value, _ = _extract_cover_registration_date(
        """
登记机构
2018年动登专用章日
(04)
--- Property Certificate Seal OCR page=1 region=cover_registration_date_region variant=remove_red_stamp_then_gray ---
2018 年 10 月 23 日
"""
    )

    assert value == "2018年10月23日"


def test_cover_page_fields_include_registration_date_alias():
    result = _extract_cover(
        """
不动产权证书
根据《中华人民共和国物权法》，为保护不动产权利人合法权益，经审查核实，准予登记，颁发此证。
--- cover_seal_date_ocr ---
不动产登记专用章
2018 年 10 月 23 日章
编号№D31001337469
"""
    )

    assert result["page_role"] == "cover_page"
    assert result["fields"]["登记日期"] == "2018年10月23日"
    assert result["fields"]["registration_date"] == "2018年10月23日"
    assert result["fields"]["登记日"] == "2018年10月23日"


def test_cover_detail_merge_displays_registration_date_after_completion_date_before_authority():
    detail = run_kyc_document_agent(
        {
            "text": """
不动产权证书
权利人：沃志方
共有情况：单独所有
坐落：华发路406弄10号
不动产单元号：310104019001GB00045F00430086
权利类型：国有建设用地使用权/房屋所有权
权利性质：出让
土地用途：住宅
房屋用途：居住
地号：徐汇区华泾镇448街坊2/3丘
宗地面积：135460.00平方米
室号部位：1705
建筑面积：62.40平方米
类型：公寓
总层数：29
竣工日期：2011年
沪(2018)徐字不动产权第015979号
""",
            "pages": [],
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )
    cover = _extract_cover(
        """
不动产权证书
根据《中华人民共和国物权法》，为保护不动产权利人合法权益，经审查核实，准予登记，颁发此证。
不动产登记专用章
2018年10月23日
编号№D31001337469
"""
    )

    merged = _merge_kyc_property_supplement_fields(detail, [{"extracted_data": cover}])
    display = get_display_fields(merged)
    keys = list(display)

    assert display["登记日期"] == "2018年10月23日"
    assert keys.index("竣工日期") < keys.index("登记日期") < keys.index("登记机构")
    assert display["权利人"] == "沃志方"
