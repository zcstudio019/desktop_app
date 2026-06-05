from __future__ import annotations

from backend.services.kyc_document_agent.classifier import classify_with_reason
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


NEW_VERSION_DETAIL_TEXT = """
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


def test_new_version_real_estate_detail_classifies_as_property_cert():
    classification = classify_with_reason(NEW_VERSION_DETAIL_TEXT, filename="房产正面.pdf")

    assert classification["doc_type"] == "property_cert"
    assert classification["doc_type_name"] == "房产证/房地产权证"


def test_real_estate_unit_number_classifies_as_property_cert():
    classification = classify_with_reason("不动产单元号 310104019001GB00045F00430086", filename="资料.pdf")

    assert classification["doc_type"] == "property_cert"


def test_declared_property_cert_never_returns_unknown_for_new_version():
    result = run_kyc_document_agent({
        "text": "弱OCR文本",
        "pages": [],
        "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
    })

    assert result["doc_type"] == "property_cert"
    assert result["doc_type"] != "unknown"


def test_new_version_detail_extracts_core_fields():
    result = run_kyc_document_agent({
        "text": NEW_VERSION_DETAIL_TEXT,
        "pages": [],
        "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
    })
    fields = result["fields"]

    assert result["doc_type"] == "property_cert"
    assert result.get("page_role") == "detail_page"
    assert fields["权利人"] == "沃志方"
    assert fields["共有情况"] == "单独所有"
    assert fields["权证编号"] == "沪(2018)徐字不动产权第015979号"
    assert fields["坐落"] == "华发路406弄10号"
    assert fields["不动产单元号"] == "310104019001GB00045F00430086"
    assert fields["权利类型"] == "国有建设用地使用权/房屋所有权"
    assert fields["权利性质"] == "土地权利性质:出让"
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


def test_new_version_land_and_house_use_do_not_cross_pollute():
    result = run_kyc_document_agent({
        "text": NEW_VERSION_DETAIL_TEXT,
        "pages": [],
        "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
    })
    markdown = result["markdown"]

    assert "土地用途: 住宅" in markdown
    assert "房屋用途: 居住" in markdown
    assert "房屋用途: 住宅" not in markdown
    assert "土地用途: 居住" not in markdown
    assert "\n- 用途:" not in markdown
    assert "house_use" not in markdown
