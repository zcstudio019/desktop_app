from backend.services.property_cert_agent.merger import merge_pages
from backend.services.property_cert_agent.renderer import render_markdown
from backend.services.property_cert_agent.skills.attachment_page_skill import extract


ATTACHMENT_TEXT = """
附记
不动产单元号 房屋用途 类型 建筑面积 总层数 竣工日期
310115012345GB00012F00010001 商业 商场 2783.21平方米 总层数6 1990年
310115012345GB00012F00010002 商业 商场 2638.43平方米 总层数6 1990年
合计
"""


def _attachment_fields() -> dict:
    return extract({"text": ATTACHMENT_TEXT})["fields"]


def test_attachment_backfills_placeholder_fields_and_hides_placeholder() -> None:
    merged = merge_pages(
        [
            {
                "page_role": "new_real_estate_detail_page",
                "fields": {
                    "权利人": "咏旺公司",
                    "不动产单元号": "详见附记",
                    "房屋用途": "详见附记",
                    "建筑面积": "5421.64平方米",
                },
            },
            {"page_role": "attachment_page", "fields": _attachment_fields()},
        ]
    )
    fields = merged["fields"]
    markdown = render_markdown(
        {
            "fields": fields,
            "risk_sections": merged.get("risk_sections"),
            "validation": {"warnings": merged.get("warnings", [])},
            "page_roles": ["new_real_estate_detail_page", "attachment_page"],
            "metadata": {"filename": "咏旺不动产权证.pdf"},
        }
    )

    assert fields["不动产单元号"] == "310115012345GB00012F00010001、310115012345GB00012F00010002"
    assert fields["房屋用途"] == "商业"
    assert fields["建筑类型"] == "商场"
    assert "详见附记" not in markdown
    assert "不动产单元号: 310115012345GB00012F00010001、310115012345GB00012F00010002" in markdown
    assert "房屋用途: 商业" in markdown


def test_attachment_does_not_overwrite_valid_detail_fields() -> None:
    merged = merge_pages(
        [
            {
                "page_role": "new_real_estate_detail_page",
                "fields": {
                    "不动产单元号": "310115999999GB00099F00090099",
                    "房屋用途": "办公",
                    "建筑面积": "5421.64平方米",
                },
            },
            {"page_role": "attachment_page", "fields": _attachment_fields()},
        ]
    )

    assert merged["fields"]["不动产单元号"] == "310115999999GB00099F00090099"
    assert merged["fields"]["房屋用途"] == "办公"
    assert merged["fields"]["建筑面积"] == "5421.64 平方米"


def test_unresolved_attachment_placeholder_becomes_warning_not_markdown_field() -> None:
    merged = merge_pages(
        [
            {
                "page_role": "new_real_estate_detail_page",
                "fields": {"不动产单元号": "详见附记", "房屋用途": "详见附记"},
            },
            {"page_role": "attachment_page", "fields": {"附记": "附记 表格模糊"}},
        ]
    )
    markdown = render_markdown(
        {
            "fields": merged["fields"],
            "validation": {"warnings": merged.get("warnings", [])},
            "page_roles": ["new_real_estate_detail_page", "attachment_page"],
            "metadata": {"filename": "咏旺不动产权证.pdf"},
        }
    )

    assert "详见附记" not in markdown
    assert "需要附件页回填" in markdown
