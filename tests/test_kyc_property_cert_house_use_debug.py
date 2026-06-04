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


def test_raw_text_house_use_fallback_adds_all_house_use_aliases():
    result = _extract(
        "上海市 房地产权证\n"
        "土地状况\n"
        "用途 住宅用地\n"
        "房屋状况\n"
        "建筑面积 148.08 平方米\n"
        "建筑类型 公寓\n"
        "居住\n"
    )

    assert result["fields"]["房屋用途"] == "居住"
    assert result["fields"]["house_use"] == "居住"
    assert result["fields"]["building_use"] == "居住"
    assert result["fields"]["use_type"] == "居住"


def test_house_use_aliases_are_mapped_to_chinese_display_label():
    for alias in ("house_use", "building_use", "use_type"):
        markdown = render_markdown({
            "doc_type": "property_cert",
            "doc_type_name": "房产证/房地产权证",
            "fields": {
                "土地用途": "住宅用地",
                "建筑类型": "公寓",
                alias: "居住",
                "总层数": "14",
            },
            "validation": {"warnings": [], "errors": []},
            "missing_fields": [],
            "confidence": {"overall": 0.8},
            "evidence": {},
        })

        assert "土地用途: 住宅用地" in markdown
        assert "房屋用途: 居住" in markdown
        assert "\n- 用途:" not in markdown
        assert alias not in markdown


def test_property_cert_house_use_display_order_and_no_english_keys():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "fields": {
            "土地用途": "住宅用地",
            "建筑类型": "公寓",
            "房屋用途": "居住",
            "总层数": "14",
        },
        "validation": {"warnings": [], "errors": []},
        "missing_fields": [],
        "confidence": {"overall": 0.8},
        "evidence": {},
    })

    assert markdown.index("建筑类型: 公寓") < markdown.index("房屋用途: 居住") < markdown.index("总层数: 14")
    assert "房屋用途: 住宅用地" not in markdown
    assert "土地用途: 居住" not in markdown
    assert "house_use" not in markdown
    assert "building_use" not in markdown
    assert "use_type" not in markdown


def test_debug_log_and_frontend_mapping_hooks_exist():
    backend_skill = Path("backend/services/kyc_document_agent/skills/property_cert_skill.py").read_text(encoding="utf-8")
    orchestrator = Path("backend/services/kyc_document_agent/orchestrator.py").read_text(encoding="utf-8")
    display_util = Path("src/utils/kycDisplayFields.ts").read_text(encoding="utf-8")
    result_component = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")
    customer_page = Path("src/components/CustomerDataPage.tsx").read_text(encoding="utf-8")

    assert "[PropertyCertSkill][DEBUG] raw_text_contains_居住" in backend_skill
    assert "[KycDocumentAgent][DEBUG] final_房屋用途" in orchestrator
    assert "[KycDisplayFields][DEBUG] rawFields=" in display_util
    assert "房屋用途" in display_util
    assert "house_use: '房屋用途'" in display_util
    assert "building_use: '房屋用途'" in display_util
    assert "use_type: '房屋用途'" in display_util
    assert "enrichPropertyFieldsForDisplay" in result_component
    assert "enrichLegacyKycPropertyFields" in customer_page
