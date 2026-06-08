from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent
from backend.services.property_cert_agent.page_role import detect_page_role
from tests.test_property_cert_agent_old_shanghai_linyong import LINYONG_OCR_TEXT


NEW_REAL_ESTATE_TEXT = """
沪(2018)徐字
不动产权第015979
号
权利人
沃志方
坐落
华发路406弄10号
不动产单元号
310104019001GB00045F00430086
权利类型
国有建设用地使用权/房屋所有权
权利性质
土地权利性质：出让
"""


def test_new_real_estate_cert_number_has_priority_over_old_role() -> None:
    assert detect_page_role(NEW_REAL_ESTATE_TEXT) == "new_real_estate_detail_page"


def test_new_real_estate_unit_number_routes_to_new_skill() -> None:
    result = run_property_cert_agent(
        {
            "text": NEW_REAL_ESTATE_TEXT,
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )

    assert result["page_roles"] == ["new_real_estate_detail_page"]
    assert result["fields"]["权证编号"] == "沪(2018)徐字不动产权第015979号"
    assert result["fields"]["不动产单元号"] == "310104019001GB00045F00430086"
    assert "房地坐落" not in result["fields"]


def test_old_shanghai_linyong_still_uses_old_role() -> None:
    assert detect_page_role(LINYONG_OCR_TEXT) == "old_property_detail_page"
