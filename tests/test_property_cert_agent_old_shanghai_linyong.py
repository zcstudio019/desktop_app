from __future__ import annotations

from pathlib import Path

from backend.services.property_cert_agent import run_property_cert_agent
from backend.services.property_cert_agent.merger import merge_pages
from backend.services.property_cert_agent.normalizer import normalize_fields
from backend.services.property_cert_agent.page_role import detect_page_role
from backend.services.property_cert_agent.renderer import ensure_property_address_for_render, render_markdown
from backend.services.property_cert_agent.skills.old_shanghai_property_cert_skill import extract as extract_old_shanghai
from backend.services.markdown_profile_service import _build_kyc_property_profile_section_lines


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
    assert "房地坐落: 奉贤区泽丰路88弄2号" in markdown
    assert markdown.index("权证编号: 沪房地奉字(2014)第004478号") < markdown.index("房地坐落: 奉贤区泽丰路88弄2号")
    assert markdown.index("房地坐落: 奉贤区泽丰路88弄2号") < markdown.index("权属性质: 国有建设用地使用权")
    assert "土地用途: 住宅用地状况" not in markdown
    assert "房屋用途: 5丘况总层数" not in markdown
    assert "使用权面积: 独用" not in markdown
    assert "- 用途:" not in markdown
    assert "land_use" not in markdown
    assert "house_use" not in markdown


def test_linyong_old_shanghai_frontend_order_keeps_address() -> None:
    source = Path("src/utils/kycDisplayFields.ts").read_text(encoding="utf-8")
    assert "'房地坐落'," in source
    assert source.index("'权证编号'") < source.index("'房地坐落'") < source.index("'封面编号'")
    assert "property_address: '房地坐落'" in source
    assert "address: '坐落'" in source
    assert "[KycDisplayFields][ADDRESS] raw_房地坐落=" in source
    assert "[KycDisplayFields][ADDRESS] raw_坐落=" in source
    assert "[KycDisplayFields][ADDRESS] display_address=" in source


def test_linyong_old_shanghai_skill_extracts_address_variants() -> None:
    variants = [
        "房地坐落\n奉贤区泽丰路88弄2号\n权属性质\n国有建设用地使用权",
        "房地坐落：奉贤区泽丰路88弄2号\n权属性质\n国有建设用地使用权",
        "房 地 坐 落\n奉贤区泽丰路88弄2号\n权属性质\n国有建设用地使用权",
    ]
    for text in variants:
        fields = extract_old_shanghai({"text": text})["fields"]
        assert fields["房地坐落"] == "奉贤区泽丰路88弄2号"


def test_linyong_old_shanghai_normalizer_keeps_old_address_label() -> None:
    normalized = normalize_fields(
        {
            "权证编号": "沪房地奉字(2014)第004478号",
            "坐落": "不应展示的新字段",
            "房地坐落": "奉贤区泽丰路88弄2号",
            "权属性质": "国有建设用地使用权",
        },
        old_version=True,
    )

    assert normalized["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "坐落" not in normalized


def test_linyong_old_shanghai_normalizer_auto_detects_old_version() -> None:
    normalized = normalize_fields(
        {
            "权证编号": "沪房地奉字(2014)第004478号",
            "坐落": "不应展示的新字段",
            "房地坐落": "奉贤区泽丰路88弄2号",
        }
    )

    assert normalized["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "坐落" not in normalized


def test_linyong_old_shanghai_merger_keeps_address() -> None:
    merged = merge_pages(
        [
            {
                "page_role": "old_property_detail_page",
                "fields": {
                    "权证编号": "沪房地奉字(2014)第004478号",
                    "坐落": "",
                    "房地坐落": "奉贤区泽丰路88弄2号",
                    "权属性质": "国有建设用地使用权",
                },
            }
        ]
    )

    assert merged["fields"]["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "坐落" not in merged["fields"]


def test_linyong_old_shanghai_normalizer_keeps_old_address_from_aliases() -> None:
    normalized = normalize_fields(
        {
            "权证编号": "沪房地奉字(2014)第004478号",
            "property_address": "奉贤区泽丰路88弄2号",
            "address": "不应优先展示",
        },
        old_version=True,
    )

    assert normalized["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "坐落" not in normalized
    assert "property_address" not in normalized
    assert "address" not in normalized


def test_linyong_old_shanghai_renderer_prefers_old_address_label() -> None:
    markdown = render_markdown(
        {
            "old_version": True,
            "fields": {
                "权利人": "林勇、黄晓回",
                "权证编号": "沪房地奉字(2014)第004478号",
                "房地坐落": "奉贤区泽丰路88弄2号",
                "坐落": "不应展示的新字段",
                "权属性质": "国有建设用地使用权",
            },
            "metadata": {"filename": "林勇产证.pdf"},
            "validation": {},
        }
    )

    assert "房地坐落: 奉贤区泽丰路88弄2号" in markdown
    assert "坐落: 不应展示的新字段" not in markdown
    assert markdown.index("权证编号: 沪房地奉字(2014)第004478号") < markdown.index("房地坐落: 奉贤区泽丰路88弄2号")
    assert markdown.index("房地坐落: 奉贤区泽丰路88弄2号") < markdown.index("权属性质: 国有建设用地使用权")


def test_linyong_old_shanghai_renderer_recovers_address_from_raw_text() -> None:
    raw_text = """
上海市房地产权证
沪房地奉字(2014)第004478号
房地坐落
奉贤区泽丰路88弄2号
权属性质
国有建设用地使用权
"""
    fields = {
        "权利人": "林勇、黄晓回",
        "权证编号": "沪房地奉字(2014)第004478号",
        "权属性质": "国有建设用地使用权",
    }
    ensured = ensure_property_address_for_render(fields, raw_text)
    markdown = render_markdown(
        {
            "_raw_text": raw_text,
            "fields": fields,
            "metadata": {"filename": "林勇产证.pdf"},
            "validation": {},
        }
    )

    assert ensured["房地坐落"] == "奉贤区泽丰路88弄2号"
    assert "房地坐落: 奉贤区泽丰路88弄2号" in markdown
    assert "\n- 坐落: 奉贤区泽丰路88弄2号" not in markdown


def test_linyong_profile_markdown_builder_keeps_property_address() -> None:
    lines = _build_kyc_property_profile_section_lines(
        ["林勇产证.pdf"],
        True,
        {
            "doc_type": "property_cert",
            "fields": {
                "权利人": "林勇、黄晓回",
                "权证编号": "沪房地奉字(2014)第004478号",
                "房地坐落": "奉贤区泽丰路88弄2号",
                "权属性质": "国有建设用地使用权",
            },
        },
    )
    markdown = "\n".join(lines)

    assert "房地坐落: 奉贤区泽丰路88弄2号" in markdown
    assert "\n- 坐落: 奉贤区泽丰路88弄2号" not in markdown


def test_new_property_cert_renderer_keeps_new_address_label() -> None:
    normalized = normalize_fields({"坐落": "华发路406弄10号"}, old_version=False)
    markdown = render_markdown({"fields": normalized, "metadata": {}, "validation": {}})

    assert normalized["坐落"] == "华发路406弄10号"
    assert "坐落: 华发路406弄10号" in markdown
    assert "房地坐落: 华发路406弄10号" not in markdown
