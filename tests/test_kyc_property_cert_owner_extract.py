from __future__ import annotations

from pathlib import Path

from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_document_agent.skills.property_cert_skill import extract


def _extract(text: str) -> dict:
    return extract({
        "text": text,
        "pages": [],
        "metadata": {"filename": "林勇产证.pdf", "customer_id": "customer-1", "source": "unit_test"},
    })


def test_property_cert_owner_inline_extracts_owner_and_co_owner():
    result = _extract("上海市 房地产权证\n权利人 林勇、黄晓囡\n权证编号 沪房地奉字(2014)第004478号")

    assert result["fields"]["权利人"] == "林勇"
    assert result["fields"]["共有人"] == ["黄晓囡"]
    assert result["fields"]["owner"] == "林勇"
    assert result["fields"]["co_owners"] == ["黄晓囡"]


def test_property_cert_owner_colon_extracts_owner_and_co_owner():
    result = _extract("上海市 房地产权证\n权利人：林勇、黄晓囡\n房地坐落 奉贤区泽丰路88弄2号")

    assert result["fields"]["权利人"] == "林勇"
    assert result["fields"]["共有人"] == ["黄晓囡"]


def test_property_cert_owner_next_line_extracts_owner_and_co_owner():
    result = _extract("上海市 房地产权证\n权利人\n林勇、黄晓囡\n房地坐落 奉贤区泽丰路88弄2号")

    assert result["fields"]["权利人"] == "林勇"
    assert result["fields"]["共有人"] == ["黄晓囡"]


def test_property_cert_owner_spaced_label_extracts_owner_and_co_owner():
    result = _extract("上海市 房地产权证\n权 利 人 林勇、黄晓囡\n房地坐落 奉贤区泽丰路88弄2号")

    assert result["fields"]["权利人"] == "林勇"
    assert result["fields"]["共有人"] == ["黄晓囡"]


def test_property_cert_owner_invalid_values_are_not_used():
    bad_text = "上海市 房地产权证\n权利人 的合法权益，对\n权利人 对\n房地坐落 奉贤区泽丰路88弄2号"
    result = _extract(bad_text)

    assert "权利人" not in result["fields"]
    assert "owner" not in result["fields"]


def test_property_cert_owner_appears_first_in_markdown():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "fields": {
            "权证编号": "沪房地奉字(2014)第004478号",
            "权利人": "林勇",
            "共有人": ["黄晓囡"],
            "房地坐落": "奉贤区泽丰路88弄2号",
        },
    })

    owner_index = markdown.index("权利人: 林勇")
    co_owner_index = markdown.index("共有人: 黄晓囡")
    cert_index = markdown.index("权证编号: 沪房地奉字(2014)第004478号")
    assert owner_index < co_owner_index < cert_index


def test_frontend_property_display_mapping_contains_owner_first():
    source = Path("src/utils/kycDisplayFields.ts").read_text(encoding="utf-8")

    assert "owner: '权利人'" in source
    assert "co_owners: '共有人'" in source
    assert source.index("'权利人'") < source.index("'共有人'") < source.index("'权证编号'")
