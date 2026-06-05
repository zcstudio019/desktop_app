from __future__ import annotations

from backend.services.kyc_document_agent.renderer import get_display_fields


def test_new_real_estate_cert_deduplicates_synonyms_with_new_labels():
    display = get_display_fields(
        {
            "doc_type": "property_cert",
            "fields": {
                "权证编号": "沪(2018)徐字不动产权第015979号",
                "坐落": "华发路406弄10号",
                "房地坐落": "华发路406弄10号",
                "不动产单元号": "310104019001GB00045F00430086",
                "权利类型": "国有建设用地使用权/房屋所有权",
                "权属性质": "出让",
                "权利性质": "出让",
                "宗地号": "徐汇区华泾镇448街坊2/3丘",
                "地号": "徐汇区华泾镇448街坊2/3丘",
                "使用期限": "2015年10月16日起2076",
                "土地使用期限": "2015年10月16日起2076年12月28日止",
            },
        }
    )

    assert display["坐落"] == "华发路406弄10号"
    assert "房地坐落" not in display
    assert display["权利性质"] == "出让"
    assert "权属性质" not in display
    assert display["地号"] == "徐汇区华泾镇448街坊2/3丘"
    assert "宗地号" not in display
    assert display["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert "土地使用期限" not in display


def test_old_property_cert_deduplicates_synonyms_with_old_labels():
    display = get_display_fields(
        {
            "doc_type": "property_cert",
            "fields": {
                "权证编号": "沪房地奉字(2014)第004478号",
                "坐落": "奉贤区泽丰路88弄2号",
                "房地坐落": "奉贤区泽丰路88弄2号",
                "权属性质": "国有建设用地使用权",
                "权利性质": "国有建设用地使用权",
                "宗地号": "奉贤区光明镇2街坊1/5丘",
                "地号": "奉贤区光明镇2街坊1/5丘",
                "使用期限": "2013年5月14日至2078年4月7日止",
                "土地使用期限": "2013年5月14日至2078年4月7日止",
            },
        }
    )

    assert display["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "坐落" not in display
    assert display["权属性质"] == "国有建设用地使用权"
    assert "权利性质" not in display
    assert display["宗地号"] == "奉贤区光明镇2街坊1/5丘"
    assert "地号" not in display
    assert display["土地使用期限"] == "2013年5月14日至2078年4月7日止"
    assert "使用期限" not in display
