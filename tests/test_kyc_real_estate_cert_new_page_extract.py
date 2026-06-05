from __future__ import annotations

from backend.services.kyc_document_agent.classifier import classify_with_reason
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


NEW_PAGE_TEXT = (
    "不动产权证书 权利人：沃志方 共有情况：单独所有 "
    "坐落：华发路406弄10号 不动产单元号：310104019001GB00045F00430086 "
    "权利类型：国有建设用地使用权/房屋所有权 权利性质：土地权利性质：出让 "
    "用途：土地用途：住宅/房屋用途：居住 "
    "面积：宗地面积：135460.00平方米/建筑面积：62.40平方米 "
    "使用期限：国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止 "
    "土地状况：地号：徐汇区华泾镇448街坊2/3丘 "
    "房屋状况：室号部位：1705；类型：公寓；总层数：29；竣工日期：2011年。 "
    "沪（2018）徐字 不动产权第015979号"
)


def _extract(text: str = NEW_PAGE_TEXT) -> dict:
    return run_kyc_document_agent({
        "text": text,
        "pages": [],
        "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert", "customer_id": "customer-1"},
    })


def test_unit_number_text_classifies_as_property_cert():
    classification = classify_with_reason("不动产单元号：310104019001GB00045F00430086", filename="房产正面.pdf")

    assert classification["doc_type"] in {"property_cert", "real_estate_cert"}


def test_new_real_estate_page_extracts_all_expected_fields_from_compact_text():
    result = _extract()
    fields = result["fields"]

    assert result["doc_type"] == "property_cert"
    assert fields["权利人"] == "沃志方"
    assert fields["共有情况"] == "单独所有"
    assert fields["坐落"] == "华发路406弄10号"
    assert fields["不动产单元号"] == "310104019001GB00045F00430086"
    assert fields["权利类型"] == "国有建设用地使用权/房屋所有权"
    assert fields["土地用途"] == "住宅"
    assert fields["房屋用途"] == "居住"
    assert fields["宗地面积"] == "135460.00 平方米"
    assert fields["建筑面积"] == "62.40 平方米"
    assert fields["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert fields["地号"] == "徐汇区华泾镇448街坊2/3丘"
    assert fields["室号或部位"] == "1705"
    assert fields["建筑类型"] == "公寓"
    assert fields["总层数"] == "29"
    assert fields["竣工日期"] == "2011年"


def test_new_real_estate_page_markdown_has_display_fields():
    result = _extract()
    markdown = result["markdown"]

    assert "暂无可展示字段" not in markdown
    assert "权利人: 沃志方" in markdown
    assert "共有情况: 单独所有" in markdown
    assert "坐落: 华发路406弄10号" in markdown
    assert "不动产单元号: 310104019001GB00045F00430086" in markdown
    assert "土地用途: 住宅" in markdown
    assert "房屋用途: 居住" in markdown
    assert "建筑面积: 62.40 平方米" in markdown
    assert "竣工日期: 2011年" in markdown


def test_empty_detail_page_gets_actionable_warning_instead_of_silent_empty():
    result = _extract("不动产权证书 权利人 坐落 不动产单元号 权利类型 面积 使用期限")

    assert result["doc_type"] == "property_cert"
    assert result["fields"] == {}
    assert any("字段页 OCR 未能提取关键字段" in item for item in result["validation"]["warnings"])
