from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent
from backend.services.property_cert_agent.page_role import detect_page_role


LINYONG_OCR_TEXT = """
上海市房地产权证
沪 房 地 奉 字 (2014) 第 004478 号
权利人
林勇、黄晓回
房地坐落
奉贤区泽丰路88弄2号
权属性质
国有建设用地使用权
使用权取得方式
出让
土地状况
用途
住宅用地
宗地号
奉贤区光明镇2街坊1/5丘
宗地(丘)面积
82969
使用权面积
独用
使用期限
2013年5月14日至2078年4月7
日止
房屋状况
室号或部位
1101
建筑面积
148.08
建筑类型
公寓
用途
居住
总层数
14
宗地(丘)面积
82969
2011年
使用权面积
竣工日期
登记日
2014年3月17日
填证单位
奉贤区
"""


def test_linyong_old_shanghai_page_uses_old_skill() -> None:
    assert detect_page_role(LINYONG_OCR_TEXT) == "old_property_detail_page"


def test_linyong_old_shanghai_fields_are_clean() -> None:
    result = run_property_cert_agent(
        {
            "text": LINYONG_OCR_TEXT,
            "metadata": {"filename": "林勇产证.pdf", "declared_doc_type": "property_cert"},
        }
    )

    fields = result["fields"]
    assert result["page_roles"] == ["old_property_detail_page"]
    assert fields["权利人"] == "林勇、黄晓回"
    assert fields["权证编号"] == "沪房地奉字(2014)第004478号"
    assert fields["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert fields["权属性质"] == "国有建设用地使用权"
    assert fields["使用权取得方式"] == "出让"
    assert fields["土地用途"] == "住宅用地"
    assert fields["房屋用途"] == "居住"
    assert fields["宗地号"] == "奉贤区光明镇2街坊1/5丘"
    assert fields["宗地面积"] == "82969 平方米"
    assert fields["土地使用期限"] == "2013年5月14日至2078年4月7日止"
    assert fields["室号或部位"] == "1101"
    assert fields["建筑面积"] == "148.08 平方米"
    assert fields["建筑类型"] == "公寓"
    assert fields["总层数"] == "14"
    assert fields["竣工日期"] == "2011年"
    assert fields["登记日"] == "2014年3月17日"
    assert fields["填证单位"] == "奉贤区"

    assert "土地状况" not in fields["土地用途"]
    assert "宗地号" not in fields["土地用途"]
    assert "总层数" not in fields["土地用途"]
    assert "宗地号" not in fields["房屋用途"]
    assert "总层数" not in fields["房屋用途"]
    assert "宗地面积" not in fields["房屋用途"]

    markdown = result["markdown"]
    assert "土地用途: 住宅用地状况" not in markdown
    assert "房屋用途: 5丘况总层数" not in markdown
    assert "使用权面积: 独用" not in markdown
    assert "- 用途:" not in markdown
    assert "land_use" not in markdown
    assert "house_use" not in markdown
