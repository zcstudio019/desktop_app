from __future__ import annotations

from pathlib import Path

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


NEW_REAL_ESTATE_TEXT = (
    "不动产权证书 权利人：沃志方 共有情况：单独所有 "
    "坐落：华发路406弄10号 不动产单元号：310104019001GB00045F00430086 "
    "权利类型：国有建设用地使用权/房屋所有权 权利性质：土地权利性质：出让 "
    "用途：土地用途：住宅/房屋用途：居住 "
    "面积：宗地面积：135460.00平方米/建筑面积：62.40平方米 "
    "使用期限：国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止 "
    "土地状况：地号：徐汇区华泾镇448街坊2/3丘 "
    "房屋状况：室号部位：1705；类型：公寓；总层数：29；竣工日期：2011年。 "
    "沪（2018）徐字 不动产权第015979号"
)


def _extract(text: str = NEW_REAL_ESTATE_TEXT) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {
                "filename": "房产正面.pdf",
                "declared_doc_type": "property_cert",
            },
        }
    )


def test_ocr_text_extracts_owner():
    fields = _extract()["fields"]

    assert fields["权利人"] == "沃志方"


def test_ocr_text_extracts_property_unit_number():
    fields = _extract()["fields"]

    assert fields["不动产单元号"] == "310104019001GB00045F00430086"


def test_ocr_text_splits_land_and_house_use():
    fields = _extract()["fields"]

    assert fields["土地用途"] == "住宅"
    assert fields["房屋用途"] == "居住"


def test_ocr_text_extracts_building_area():
    fields = _extract()["fields"]

    assert fields["建筑面积"] == "62.40 平方米"


def test_ocr_text_extracts_completion_date():
    fields = _extract()["fields"]

    assert fields["竣工日期"] == "2011年"


def test_non_empty_fields_do_not_render_empty_placeholder():
    result = _extract()

    assert result["fields"]
    assert "暂无可展示字段" not in result["markdown"]


def test_property_cert_ocr_pipeline_has_field_page_crop_and_debug_logs():
    source = Path("backend/routers/file.py").read_text(encoding="utf-8")

    assert "file_service.pdf_to_images(file_bytes, dpi=300)" in source
    assert "left_table_70_95" in source
    assert "[PropertyCertOCR] contains_沃志方=%s" in source
    assert "[PropertyCertOCR] contains_不动产单元号=%s" in source
