from __future__ import annotations

from backend.services.kyc_document_agent.renderer import get_display_fields

from tests.test_kyc_real_estate_cert_company_owner import _extract


def test_commercial_land_and_house_usage_are_split():
    result = _extract()

    assert result["fields"]["土地用途"] == "其它商服用地"
    assert result["fields"]["房屋用途"] == "办公"


def test_parcel_number_is_cleaned_without_area_noise():
    result = _extract()

    assert result["fields"]["地号"] == "宝山区高境镇9街坊73/7丘"
    assert "使用权面积" not in result["fields"]["地号"]
    assert "独用面积" not in result["fields"]["地号"]
    assert "分摊面积" not in result["fields"]["地号"]


def test_room_number_and_use_term_are_extracted():
    result = _extract()

    assert result["fields"]["室号或部位"] == "306"
    assert result["fields"]["使用期限"] == "2018年08月28日起2046年08月20日止"


def test_company_real_estate_display_has_no_duplicate_old_fields():
    result = _extract()
    display = get_display_fields(result)

    assert display["权证编号"] == "沪(2022)宝字不动产权第011468号"
    assert display["土地用途"] == "其它商服用地"
    assert display["房屋用途"] == "办公"
    assert "房地坐落" not in display
    assert "权属性质" not in display
    assert "宗地号" not in display
    assert "土地使用期限" not in display
