from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


def test_new_real_estate_cert_extracts_wzh_sample() -> None:
    result = run_property_cert_agent(
        {
            "text": """
不动产权证书
沪(2022)宝字不动产权第011468号
权利人 沃志方
共有情况 单独所有
坐落 上海市宝山区高境镇示例路999弄1号101室
不动产单元号 310113999999GB00001F00010001
权利类型 国有建设用地使用权/房屋所有权
权利性质 出让/市场化商品房
土地用途 住宅
房屋用途 居住
宗地面积 100.00平方米
建筑面积 88.88平方米
使用期限 2015年10月16日起2076
年12月28日止
地号 宝山区高境镇9街坊73/7丘 使用权面积 100.00平方米
室号或部位 101室
建筑类型 公寓
总层数 18
竣工日期 2015年12月
""",
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )

    fields = result["fields"]
    assert result["agent_type"] == "property_cert_agent"
    assert fields["权利人"] == "沃志方"
    assert fields["权证编号"] == "沪(2022)宝字不动产权第011468号"
    assert fields["土地用途"] == "住宅"
    assert fields["房屋用途"] == "居住"
    assert fields["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert "封面编号" not in fields or fields["封面编号"] != fields["权证编号"]
