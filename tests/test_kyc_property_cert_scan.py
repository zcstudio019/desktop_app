from __future__ import annotations

from pathlib import Path

from backend.services.kyc_document_agent.classifier import classify, classify_with_reason
from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


GARBLED_TEXT = "neaing onensumpy uoneA pue uoneunuex abcdefg hijklmnop"


PROPERTY_TEXT = """
上海市 房地产权证
沪房地奉字（2014）第004478号
登记日 2014年3月17日
权利人 林勇、黄晓囡
房地坐落 奉贤区泽丰路88弄2号
权属性质 国有建设用地使用权
使用权取得方式 出让
土地用途 住宅用地
宗地号 奉贤区光明镇2街坊1/5丘
宗地(丘)面积 82969 平方米
使用期限 2013年5月14日至2078年4月7日止
室号或部位 1101
建筑面积 148.08 平方米
建筑类型 公寓
房屋用途 居住
总层数 14
竣工日期 2011年
"""


def _payload(text: str, filename: str = "林勇产证.pdf") -> dict:
    return {
        "text": text,
        "pages": [],
        "metadata": {"filename": filename, "customer_id": "customer-1", "source": "test"},
    }


def test_filename_contains_property_cert_fallback_even_with_garbled_ocr():
    result = run_kyc_document_agent(_payload(GARBLED_TEXT, filename="林勇产证.pdf"))

    assert result["doc_type"] == "property_cert"
    assert result["doc_type_name"] == "房产证/房地产权证"
    assert "文件名包含产证" in result["classification_reason"]


def test_real_estate_property_certificate_keyword_classifies_property_cert():
    result = classify_with_reason("上海市 房地产权证\n权利人 林勇", filename="")

    assert result["doc_type"] == "property_cert"
    assert result["doc_type_name"] == "房产证/房地产权证"


def test_keyword_combination_classifies_property_cert():
    text = "权利人 林勇\n房地坐落 奉贤区泽丰路88弄2号\n建筑面积 148.08 平方米"

    assert classify(text) == "property_cert"


def test_rotated_or_low_quality_scan_does_not_return_unknown_when_filename_matches():
    result = run_kyc_document_agent(_payload("uoneA pue uoneunuex neaing onensumpy", filename="倒置扫描房本.pdf"))

    assert result["doc_type"] == "property_cert"


def test_property_cert_fields_use_chinese_names():
    result = run_kyc_document_agent(_payload(PROPERTY_TEXT))
    fields = result["fields"]

    assert fields["权利人"] == "林勇、黄晓囡"
    assert fields["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert fields["权属性质"] == "国有建设用地使用权"
    assert fields["使用权取得方式"] == "出让"
    assert fields["建筑面积"] == "148.08 平方米"
    assert fields["权证编号"] == "沪房地奉字(2014)第004478号"


def test_property_cert_markdown_contains_chinese_doc_type_name():
    result = run_kyc_document_agent(_payload(PROPERTY_TEXT))

    assert "房产证/房地产权证" in result["markdown"]
    assert "权利人" in result["markdown"]


def test_frontend_kyc_display_uses_chinese_labels_not_english_raw_labels():
    source = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")

    assert "资料类型编码" in source
    assert "关键字段" in source
    assert "缺失字段" in source
    assert "doc type" not in source.lower()
    assert "owner type" not in source.lower()
    assert "missing fields" not in source.lower()
