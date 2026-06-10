from __future__ import annotations

import asyncio

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent
from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


SAMPLE_TEXT = """中华人民共和国机动车行驶证
号牌号码 沪ABC2061
车辆类型 小型轿车
所有人 上海煜楷贸易有限公司
住址 上海市宝山区陆翔路111弄3号楼1104-01
使用性质 非营运
品牌型号 黄海牌HFC7002CSEV1-W
车辆识别代号 LJ1EFAUU2NG108654
发动机号码 AM2FV11376
注册日期 2022-11-28
发证日期 2022-11-28
"""


class FakeStorage:
    def __init__(self, extractions: list[dict] | None = None) -> None:
        self.extractions = extractions or []

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return self.extractions

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


def extract(text: str) -> dict:
    return KycDocumentAgent().extract({"text": text})


def kyc_extraction(fields: dict, confirmed_fields: dict | None = None) -> dict:
    extraction = {
        "doc_id": "doc-vehicle",
        "created_at": "2026-06-10T10:00:00",
        "extraction_type": "vehicle_license",
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": "vehicle_license",
            "doc_type_name": "行驶证",
            "fields": fields,
        },
    }
    if confirmed_fields is not None:
        extraction["confirmed_data"] = {
            "confirmed_fields": confirmed_fields,
            "confirm_status": "partial",
        }
    return extraction


def build_profile(extractions: list[dict]) -> dict:
    return asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer-1"))


def test_complete_vehicle_license_extracts() -> None:
    result = extract(SAMPLE_TEXT)
    fields = result["fields"]

    assert result["doc_type"] == "vehicle_license"
    assert result["doc_type_name"] == "行驶证"
    assert result["owner_type"] == "asset"
    assert fields["plate_number"] == "沪ABC2061"
    assert fields["vehicle_type"] == "小型轿车"
    assert fields["owner"] == "上海煜楷贸易有限公司"
    assert fields["address"] == "上海市宝山区陆翔路111弄3号楼1104-01"
    assert fields["use_character"] == "非营运"
    assert fields["brand_model"] == "黄海牌HFC7002CSEV1-W"
    assert fields["vin"] == "LJ1EFAUU2NG108654"
    assert fields["engine_number"] == "AM2FV11376"
    assert fields["registration_date"] == "2022-11-28"
    assert fields["issue_date"] == "2022-11-28"
    assert result["extraction_status"] == "success"


def test_vehicle_license_dates_with_dots_normalize() -> None:
    result = extract(SAMPLE_TEXT.replace("2022-11-28", "2022.11.28"))

    assert result["fields"]["registration_date"] == "2022-11-28"
    assert result["fields"]["issue_date"] == "2022-11-28"


def test_vehicle_license_vin_lowercase_normalizes() -> None:
    result = extract(SAMPLE_TEXT.replace("LJ1EFAUU2NG108654", "lj1efauu2ng108654"))

    assert result["fields"]["vin"] == "LJ1EFAUU2NG108654"


def test_vehicle_license_vin_length_warning() -> None:
    result = extract(SAMPLE_TEXT.replace("LJ1EFAUU2NG108654", "LJ1EFAUU2NG10865"))

    assert "车辆识别代号长度异常" in result["validation"]["warnings"]
    assert result["extraction_status"] in {"partial", "success"}


def test_vehicle_license_partial_status() -> None:
    result = extract("""机动车行驶证
号牌号码 沪ABC2061
所有人 上海煜楷贸易有限公司
""")

    assert result["extraction_status"] == "partial"
    assert "vin" in result["missing_fields"]


def test_vehicle_license_extracts_between_chinese_english_labels() -> None:
    result = extract("""中华人民共和国机动车行驶证
号牌号码 Plate No. 沪ABC2061 车辆类型 Vehicle Type 小型轿车
所有人 Owner 上海煜禧贸易有限公司
住址 Address 上海市宝山区陆翔路111弄3号楼1104-01
使用性质 Use Character 非营运 品牌型号 Model 蔚来牌HFC7002CSEV1-W
车辆识别代号 VIN LJ1EFAUU2NG108654
发动机号码 Engine No. AM2FV11376
注册日期 Register Date 2022年11月28日 发证日期 Issue Date 2022年11月28日
""")
    fields = result["fields"]

    assert fields["plate_number"] == "沪ABC2061"
    assert fields["vehicle_type"] == "小型轿车"
    assert fields["owner"] == "上海煜禧贸易有限公司"
    assert fields["address"] == "上海市宝山区陆翔路111弄3号楼1104-01"
    assert fields["use_character"] == "非营运"
    assert fields["brand_model"] == "蔚来牌HFC7002CSEV1-W"
    assert fields["vin"] == "LJ1EFAUU2NG108654"
    assert fields["engine_number"] == "AM2FV11376"
    assert fields["registration_date"] == "2022-11-28"
    assert fields["issue_date"] == "2022-11-28"

    forbidden = (
        "VEHICLE",
        "VehicleType",
        "Owner",
        "Address",
        "USECHARACTER",
        "ENGINENO",
        "Plate No",
        "Engine No",
        "Register Date",
        "Issue Date",
    )
    values = "\n".join(str(value) for value in fields.values())
    for item in forbidden:
        assert item not in values


def test_classifier_recognizes_vehicle_license() -> None:
    assert classify(SAMPLE_TEXT) == "vehicle_license"


def test_classifier_does_not_misclassify_driving_license() -> None:
    text = """中华人民共和国机动车驾驶证
姓名 张三
证号 11010519491231002X
准驾车型 C1
"""
    assert classify(text) == "driving_license"


def test_profile_vehicle_license_enters_assets_vehicles() -> None:
    profile = build_profile([kyc_extraction({
        "plate_number": "沪ABC2061",
        "vehicle_type": "小型轿车",
        "owner": "上海煜楷贸易有限公司",
        "address": "上海市宝山区陆翔路111弄3号楼1104-01",
        "use_character": "非营运",
        "brand_model": "黄海牌HFC7002CSEV1-W",
        "vin": "LJ1EFAUU2NG108654",
        "engine_number": "AM2FV11376",
        "registration_date": "2022-11-28",
        "issue_date": "2022-11-28",
    })])

    vehicle = profile["assets"]["vehicles"][0]
    assert vehicle["plate_number"] == "沪ABC2061"
    assert vehicle["owner"] == "上海煜楷贸易有限公司"
    assert vehicle["vin"] == "LJ1EFAUU2NG108654"
    assert vehicle["source_document_id"] == "doc-vehicle"


def test_profile_vehicle_license_prefers_confirmed_data() -> None:
    profile = build_profile([kyc_extraction(
        {
            "plate_number": "沪ABC2061",
            "owner": "自动所有人",
            "vin": "LJ1EFAUU2NG108654",
        },
        confirmed_fields={
            "owner": "人工确认所有人",
            "vin": "LJ1EFAUU2NG108655",
        },
    )])

    vehicle = profile["assets"]["vehicles"][0]
    assert vehicle["owner"] == "人工确认所有人"
    assert vehicle["vin"] == "LJ1EFAUU2NG108655"
    assert vehicle["field_sources"]["owner"]["source"] == "confirmed_data"


def test_renderer_vehicle_license_markdown_is_chinese_not_json() -> None:
    result = extract(SAMPLE_TEXT)
    markdown = render_markdown(result)

    assert "## 行驶证" in markdown
    assert "### 车辆基础信息" in markdown
    assert "号牌号码：沪ABC2061" in markdown
    assert "车辆识别代号：LJ1EFAUU2NG108654" in markdown
    assert "```json" not in markdown
    assert "plate_number" not in markdown
    assert "vin" not in markdown
