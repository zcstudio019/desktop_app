from __future__ import annotations

from backend.services.property_cert_agent.renderer import render_markdown


def test_renderer_outputs_only_normalized_clean_fields() -> None:
    raw_text = """
沪（2022）宝字
不动产权第011468
号
权利人：智先生数字科技（上海）有限公司
坐落：殷高西路101号
土地用途：其它商服用地/房屋用途：办公；宗地面积：8615.00平方米
房屋状况：
室号部位：306；
类型：办公楼；
总层数：17；
竣工日期：2007年。
"""
    markdown = render_markdown(
        {
            "_raw_text": raw_text,
            "page_roles": ["new_real_estate_detail_page"],
            "fields": {
                "权证编号": "D31003610514",
                "土地用途": "其它商服用地/房屋用途：办公；宗地面积：8615.00平方米",
                "房屋用途": "办公；宗地面积：8615.00平方米",
                "地号": "宝山区高境镇9街坊73/7丘；\\n使用权面积：相应的土地面积；\\n房屋状况：\\n室号部位：306；\\n类型：办公楼；",
                "建筑面积": "800.35",
            },
            "metadata": {"filename": "企业房产证.pdf"},
            "validation": {},
        }
    )

    assert "权证编号: D31003610514" not in markdown
    assert "权证编号: 沪(2022)宝字不动产权第011468号" in markdown
    assert "土地用途: 其它商服用地" in markdown
    assert "房屋用途: 办公" in markdown
    assert "建筑类型: 办公楼" in markdown
    assert "\\n" not in markdown
    assert "使用权面积" not in markdown
    assert "房屋状况" not in markdown
    assert "宗地面积：8615.00" not in markdown
