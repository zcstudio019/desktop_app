from backend.services.property_cert_agent.merger import merge_pages
from backend.services.property_cert_agent.normalizer import normalize_property_cert_fields
from backend.services.property_cert_agent.renderer import render_markdown
from backend.services.property_cert_agent.skills.attachment_page_skill import extract, is_valid_unit_number


INVALID_ATTACHMENT_TEXT = """
附记
不动产单元号 使用权面积 房屋状况 室号或部位 建筑面积 类型 用途 总层数 竣工日期
使用权 100.00 200号1层 2783.21平方米 商业 商场 6 1990年
合计
"""


def test_invalid_unit_number_values_are_rejected() -> None:
    assert not is_valid_unit_number("使用权")
    assert not is_valid_unit_number("使用权面积")
    assert not is_valid_unit_number("商业")
    assert is_valid_unit_number("310113015003GB00011F00020088")


def test_attachment_skill_does_not_emit_invalid_unit_number() -> None:
    result = extract({"text": INVALID_ATTACHMENT_TEXT})
    fields = result["fields"]

    assert "不动产单元号列表" not in fields
    assert result["warnings"] == ["不动产单元号在附记页中，系统未能识别到合法编号，请人工确认。"]
    assert fields["附记明细"][0]["室号或部位"] == "200号1层"
    assert fields["附记明细"][0]["房屋用途"] == "商业"
    assert fields["附记明细"][0]["建筑类型"] == "商场"


def test_invalid_main_unit_number_is_removed_and_warned() -> None:
    merged = merge_pages(
        [
            {
                "page_role": "new_real_estate_detail_page",
                "fields": {"不动产单元号": "使用权", "房屋用途": "详见附记"},
            },
            {"page_role": "attachment_page", "fields": extract({"text": INVALID_ATTACHMENT_TEXT})["fields"]},
        ]
    )
    markdown = render_markdown(
        {
            "fields": merged["fields"],
            "risk_sections": merged.get("risk_sections"),
            "validation": {"warnings": merged.get("warnings", [])},
            "page_roles": ["new_real_estate_detail_page", "attachment_page"],
            "metadata": {"filename": "咏旺不动产权证.pdf"},
        }
    )

    assert "不动产单元号: 使用权" not in markdown
    assert "不动产单元号：使用权" not in markdown
    assert "详见附记" not in markdown
    assert "不动产单元号在附记页中，系统未能识别到合法编号，请人工确认。" in markdown


def test_normalizer_removes_invalid_unit_number() -> None:
    fields = normalize_property_cert_fields({"不动产单元号": "使用权"}, page_role="new_real_estate_detail_page")
    assert "不动产单元号" not in fields
