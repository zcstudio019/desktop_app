from __future__ import annotations

from pathlib import Path

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_document_agent.renderer import get_display_fields


REAL_ESTATE_DETAIL_TEXT = (
    "不动产权证书 权利人：沃志方 共有情况：单独所有 "
    "坐落：华发路406弄10号 不动产单元号：310104019001GB00045F00430086 "
    "权利类型：国有建设用地使用权/房屋所有权 权利性质：土地权利性质：出让 "
    "用途：土地用途：住宅/房屋用途：居住 "
    "面积：宗地面积：135460.00平方米/建筑面积：62.40平方米 "
    "使用期限：国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止 "
    "土地状况：地号：徐汇区华泾镇448街坊2/3丘；使用权面积:相应的土地面积；独用面积:；分摊面积:。 "
    "房屋状况：室号部位：1705；类型：公寓；总层数：29；竣工日期：2011年。 "
    "沪（2018）徐字 不动产权第015979号"
)


def _extract() -> dict:
    return run_kyc_document_agent(
        {
            "text": REAL_ESTATE_DETAIL_TEXT,
            "pages": [],
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )


def test_backend_markdown_title_appears_once():
    result = _extract()

    assert result["markdown"].count("## 房产证/房地产权证") == 1


def test_extracts_shared_status_unit_number_and_right_type():
    fields = _extract()["fields"]

    assert fields["共有情况"] == "单独所有"
    assert fields["不动产单元号"] == "310104019001GB00045F00430086"
    assert fields["权利类型"] == "国有建设用地使用权/房屋所有权"


def test_extracts_complete_use_term_without_truncation():
    fields = _extract()["fields"]

    assert fields["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert fields["土地使用期限"] == "2015年10月16日起2076年12月28日止"
    assert fields["使用期限"] != "2015年10月16日起2076"


def test_splits_land_use_and_house_use():
    fields = _extract()["fields"]

    assert fields["土地用途"] == "住宅"
    assert fields["房屋用途"] == "居住"
    assert fields.get("用途") is None


def test_parcel_number_does_not_include_usage_area_fragments():
    fields = _extract()["fields"]

    for key in ("地号", "宗地号", "parcel_number"):
        assert fields[key] == "徐汇区华泾镇448街坊2/3丘"
        assert "使用权面积" not in str(fields[key])
        assert "独用面积" not in str(fields[key])
        assert "分摊面积" not in str(fields[key])


def test_display_fields_are_ordered_and_only_non_empty_values_rendered():
    display_fields = get_display_fields(_extract())
    keys = list(display_fields)

    assert keys.index("权利人") < keys.index("共有情况") < keys.index("权证编号")
    assert keys.index("土地用途") < keys.index("房屋用途") < keys.index("宗地号")
    assert "使用权面积" not in display_fields


def test_frontend_preview_does_not_add_duplicate_level_two_title():
    source = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")

    assert "const lines = ['### 关键字段'];" in source
    assert "const lines = [`## ${title}`" not in source
