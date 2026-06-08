from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


def test_old_shanghai_property_cert_extracts_legacy_fields() -> None:
    result = run_property_cert_agent(
        {
            "text": """
上海市房地产权证
沪房地徐字(2016)第123456号
权利人 林勇
房地坐落 上海市徐汇区示例路88弄8号801室
权属性质 商品房
使用权取得方式 出让
土地用途 住宅
宗地号 徐汇区1街坊2丘
宗地面积 66.00平方米
土地使用期限 2016年01月01日起2086年01月01日止
室号或部位 801室
建筑面积 99.99平方米
建筑类型 公寓
房屋用途 居住
总层数 20
竣工日期 2015年12月
登记日 2016年02月03日
填证单位 上海市房地产登记处
""",
            "metadata": {"filename": "旧版房产证.pdf", "declared_doc_type": "property_cert"},
        }
    )

    fields = result["fields"]
    assert fields["权利人"] == "林勇"
    assert fields["房地坐落"] == "上海市徐汇区示例路88弄8号801室"
    assert fields["权属性质"] == "商品房"
    assert fields["宗地号"] == "徐汇区1街坊2丘"
    assert "坐落" not in fields
