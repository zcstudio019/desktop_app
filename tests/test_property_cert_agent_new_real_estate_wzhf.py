from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent
from backend.services.property_cert_agent.page_role import detect_page_role
from backend.services.property_cert_agent.skills.new_real_estate_cert_skill import extract as extract_new_real_estate


WZHF_OCR_TEXT = """
沪(2018)徐字
不动产权第015979
号
权利人
沃志方
共有情况
单独所有
坐落
华发路406弄10号
不动产单元号
310104019001GB00045F00430086
权利类型
国有建设用地使用权/房屋所有权
权利性质
土地权利性质：出让
土地用途
住宅
房屋用途
居住
地号
徐汇区华泾镇448街坊2/3丘
宗地面积
135460.00
国有建设用地使用权使用期限：2015年10月16日起2076
使用期限
年12月28日止
房屋状况：
室号部位：1705；
类型：公寓；
建筑面积
62.40
总层数
29
竣工日期
2011年
登记日期
2018年10月23日
封面编号
D31001337469
"""


def test_wzhf_new_real_estate_role_and_fields_are_complete() -> None:
    assert "类型：公寓" in WZHF_OCR_TEXT
    assert detect_page_role(WZHF_OCR_TEXT) == "new_real_estate_detail_page"
    skill_fields = extract_new_real_estate({"text": WZHF_OCR_TEXT})["fields"]
    assert skill_fields["建筑类型"] == "公寓"
    assert skill_fields["权利类型"] == "国有建设用地使用权/房屋所有权"

    result = run_property_cert_agent(
        {
            "text": WZHF_OCR_TEXT,
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )
    fields = result["fields"]

    assert result["page_roles"] == ["new_real_estate_detail_page"]
    assert fields["权利人"] == "沃志方"
    assert fields["共有情况"] == "单独所有"
    assert fields["权证编号"] == "沪(2018)徐字不动产权第015979号"
    assert fields["封面编号"] == "D31001337469"
    assert fields["坐落"] == "华发路406弄10号"
    assert fields["不动产单元号"] == "310104019001GB00045F00430086"
    assert fields["权利类型"] == "国有建设用地使用权/房屋所有权"
    assert fields["权利性质"] == "土地权利性质：出让"
    assert fields["土地用途"] == "住宅"
    assert fields["房屋用途"] == "居住"
    assert fields["地号"] == "徐汇区华泾镇448街坊2/3丘"
    assert fields["宗地面积"] == "135460.00 平方米"
    assert fields["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert fields["室号或部位"] == "1705"
    assert fields["建筑面积"] == "62.40 平方米"
    assert fields["建筑类型"] == "公寓"
    assert fields["总层数"] == "29"
    assert fields["竣工日期"] == "2011年"
    assert fields["登记日期"] == "2018年10月23日"
    assert "房地坐落" not in fields
    assert "权属性质" not in fields
    assert "宗地号" not in fields
    assert "土地使用期限" not in fields

    markdown = result["markdown"]
    assert "权证编号: 沪(2018)徐字不动产权第015979号" in markdown
    assert "坐落: 华发路406弄10号" in markdown
    assert "使用期限: 2015年10月16日起2076年12月28日止" in markdown
    assert "建筑类型: 公寓" in markdown
    assert "房地坐落:" not in markdown
    assert markdown.index("权证编号: 沪(2018)徐字不动产权第015979号") < markdown.index("封面编号: D31001337469") < markdown.index("坐落: 华发路406弄10号")
    assert markdown.index("室号或部位: 1705") < markdown.index("建筑类型: 公寓") < markdown.index("总层数: 29")
