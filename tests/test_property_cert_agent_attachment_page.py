from backend.services.property_cert_agent.page_role import detect_page_role
from backend.services.property_cert_agent.skills.attachment_page_skill import extract


ATTACHMENT_TEXT = """
附记
不动产单元号 房屋用途 类型 建筑面积 总层数 竣工日期
310115012345GB00012F00010001 商业 商场 2783.21平方米 总层数6 1990年
310115012345GB00012F00010002 商业 商场 2638.43平方米 总层数6 1990年
合计
"""


def test_attachment_page_role_has_priority_over_new_detail_role() -> None:
    assert detect_page_role(ATTACHMENT_TEXT) == "attachment_page"


def test_attachment_page_skill_extracts_table_lists() -> None:
    fields = extract({"text": ATTACHMENT_TEXT})["fields"]

    assert fields["不动产单元号列表"] == [
        "310115012345GB00012F00010001",
        "310115012345GB00012F00010002",
    ]
    assert fields["房屋用途列表"] == ["商业"]
    assert fields["建筑类型列表"] == ["商场"]
    assert fields["建筑面积列表"] == ["2783.21 平方米", "2638.43 平方米"]
    assert fields["竣工日期列表"] == ["1990年"]
    assert len(fields["附记明细"]) == 2
