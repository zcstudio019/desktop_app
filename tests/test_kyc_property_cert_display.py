from __future__ import annotations

from pathlib import Path

from backend.services.kyc_document_agent.renderer import get_display_fields, render_markdown
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


PROPERTY_TEXT = """
上海市 房地产权证
沪房地奉字（2014）第004478号
登记日 2014年3月17日
权利人 林勇、黄晓囡
房地坐落 奉贤区泽丰路88弄2号
土地状况
权属性质 国有建设用地使用权
使用权取得方式 出让
用途 住宅用地
宗地号 奉贤区光明镇2街坊1/5丘
宗地(丘)面积 82969 平方米
使用权面积 独用
使用期限 2013年5月14日至2078年4月7日止
房屋状况
室号或部位 1101
建筑面积 148.08 平方米
建筑类型 公寓
用途 居住
总层数 14
竣工日期 2011年
填证单位 奉贤区房地产登记处
"""


def _payload(text: str = PROPERTY_TEXT) -> dict:
    return {
        "text": text,
        "pages": [],
        "metadata": {"filename": "林勇产证.pdf", "customer_id": "customer-1", "source": "unit_test"},
    }


def test_property_cert_markdown_title_uses_chinese_doc_type_name():
    result = run_kyc_document_agent(_payload())

    assert "## 房产证/房地产权证" in result["markdown"]
    assert "## property_cert" not in result["markdown"]


def test_property_cert_markdown_does_not_show_english_raw_labels_or_keys():
    result = run_kyc_document_agent(_payload())
    markdown = result["markdown"].lower()

    forbidden = [
        "doc type",
        "doc type name",
        "owner type",
        "fields",
        "validation",
        "confidence",
        "missing fields",
        "raw text preview",
        "agent type",
        "owner:",
        "co_owners",
        "certificate_number",
        "property_address",
        "right_nature",
        "building_area",
        "land_area",
        "total_area",
        "issue_date",
    ]
    for item in forbidden:
        assert item not in markdown


def test_property_cert_markdown_displays_expected_chinese_fields():
    result = run_kyc_document_agent(_payload())
    markdown = result["markdown"]

    assert "权证编号: 沪房地奉字(2014)第004478号" in markdown
    assert "房地坐落: 奉贤区泽丰路88弄2号" in markdown
    assert "权利人: 林勇、黄晓囡" in markdown
    assert "土地用途: 住宅用地" in markdown
    assert "建筑面积: 148.08 平方米" in markdown
    assert "房屋用途: 居住" in markdown
    assert "登记日: 2014年3月17日" in markdown
    assert "竣工日期: 2011年" in markdown
    assert "共有人:" not in markdown
    assert "使用权面积: 独用" not in markdown


def test_invalid_owner_value_is_filtered_from_display_fields():
    display_fields = get_display_fields({
        "fields": {
            "权利人": "的合法权益，对",
            "owner": "的合法权益，对",
            "权证编号": "沪房地奉字(2014)第004478号",
        }
    })

    assert "权利人" not in display_fields
    assert display_fields["权证编号"] == "沪房地奉字(2014)第004478号"


def test_completion_date_and_house_use_are_not_polluted_by_land_fields():
    result = run_kyc_document_agent(_payload())
    fields = result["fields"]

    assert fields["竣工日期"] == "2011年"
    assert fields["竣工日期"] != "独用"
    assert fields["土地用途"] == "住宅用地"
    assert fields["房屋用途"] == "居住"
    assert fields["房屋用途"] != "住宅用地"


def test_display_fields_format_value_unit_dict_and_filter_empty_values():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "owner_type": "asset",
        "extraction_status": "partial",
        "agent_type": "kyc_document_agent",
        "fields": {
            "建筑面积": {"value": 148.08, "unit": "平方米"},
            "宗地面积": {"value": 82969, "unit": "平方米"},
            "竣工日期": "独用",
            "权利人": "null",
            "使用权面积": "独用",
            "土地用途": "住宅用地",
            "房屋用途": "居住",
        },
        "confidence": {"overall": 0.75},
        "validation": {"warnings": [], "errors": []},
        "missing_fields": [],
        "evidence": {},
    })

    assert "建筑面积: 148.08 平方米" in markdown
    assert "宗地面积: 82969 平方米" in markdown
    assert "竣工日期: 独用" not in markdown
    assert "权利人: null" not in markdown
    assert "使用权面积: 独用" not in markdown
    assert "土地用途: 住宅用地" in markdown
    assert "\n- 用途: 住宅用地" not in markdown
    assert "房屋用途: 居住" in markdown


def test_english_and_chinese_duplicate_fields_display_chinese_only():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "owner_type": "asset",
        "extraction_status": "partial",
        "agent_type": "kyc_document_agent",
        "fields": {
            "权证编号": "沪房地奉字(2014)第004478号",
            "certificate_number": "沪房地奉字(2014)第004478号",
            "房地坐落": "奉贤区泽丰路88弄2号",
            "property_address": "奉贤区泽丰路88弄2号",
        },
        "confidence": {"overall": 0.75},
        "validation": {"warnings": [], "errors": []},
        "missing_fields": [],
        "evidence": {},
    })

    assert markdown.count("权证编号") == 1
    assert markdown.count("房地坐落") == 1
    assert "certificate_number" not in markdown
    assert "property_address" not in markdown


def test_frontend_kyc_component_uses_display_filter():
    source = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")
    util_source = Path("src/utils/kycDisplayFields.ts").read_text(encoding="utf-8")

    assert "getKycDisplayEntries" in source
    assert "renderKycDisplayMarkdown" in source
    assert "{displayMarkdown}" in source
    assert "result.markdown" not in source
    assert "enrichPropertyFieldsForDisplay" in source
    assert "权证编号" in util_source
    assert "doc type" not in source.lower()
    assert "owner type" not in source.lower()
    assert "certificate_number" in util_source
    assert "property_address" in util_source


def test_customer_profile_page_rerenders_legacy_property_cert_markdown():
    source = Path("src/components/CustomerDataPage.tsx").read_text(encoding="utf-8")

    assert "sanitizeKycPropertyMarkdownSections" in source
    assert "extractLegacyKycFieldsFromMarkdownSection" in source
    assert "enrichLegacyKycPropertyFields" in source
    assert "renderKycPropertyDisplayMarkdown" in source
    assert "## 房产证/房地产权证" in source
    assert "getKycDisplayFields(enrichLegacyKycPropertyFields(fields, sourceText), 'property_cert')" in source
    display_source = source[source.index("function renderKycPropertyDisplayMarkdown"):source.index("function sanitizeProfileMarkdown")]
    assert "- fields:" not in display_source
