from __future__ import annotations

from backend.services.property_cert_agent.normalizer import normalize_property_cert_fields


def test_normalized_output_cleans_dirty_new_real_estate_fields() -> None:
    raw_text = """
沪（2022）宝字
不动产权第011468
号
权利人
智先生数字科技（上海）有限公司
坐落
殷高西路101号
不动产单元号
310113015003GB00011F00020088
土地用途：其它商服用地/房屋用途：办公；宗地面积：8615.00平方米
房屋状况：
室号部位：306；
类型：办公楼；
总层数：17；
竣工日期：2007年。
"""
    fields = {
        "权证编号": "D31003610514",
        "土地用途": "其它商服用地/房屋用途：办公；宗地面积：8615.00平方米",
        "房屋用途": "办公；宗地面积：8615.00平方米",
        "地号": "宝山区高境镇9街坊73/7丘；\\n使用权面积：相应的土地面积；\\n独用面积：；\\n分摊面积：。\\n房屋状况：\\n室号部位：306；\\n类型：办公楼；\\n总层数：17；\\n竣工日期：2007年。",
        "宗地面积": "8615.00平方米/",
        "建筑面积": "800.35",
    }

    normalized = normalize_property_cert_fields(fields, raw_text=raw_text, page_role="new_real_estate_detail_page")

    assert normalized["权利人"] == "智先生数字科技（上海）有限公司"
    assert normalized["权证编号"] == "沪(2022)宝字不动产权第011468号"
    assert normalized["封面编号"] == "D31003610514"
    assert normalized["坐落"] == "殷高西路101号"
    assert normalized["不动产单元号"] == "310113015003GB00011F00020088"
    assert normalized["土地用途"] == "其它商服用地"
    assert normalized["房屋用途"] == "办公"
    assert normalized["宗地面积"] == "8615.00 平方米"
    assert normalized["建筑面积"] == "800.35 平方米"
    assert normalized["室号或部位"] == "306"
    assert normalized["建筑类型"] == "办公楼"
    assert normalized["总层数"] == "17"
    assert normalized["竣工日期"] == "2007年"
    assert "\\n" not in "\n".join(f"{key}:{value}" for key, value in normalized.items())
