from __future__ import annotations

import re

from .schema import DOC_TYPE_NAMES, OWNER_TYPES


CLASSIFICATION_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("id_card", ("居民身份证", "公民身份号码")),
    ("business_license", ("营业执照", "统一社会信用代码")),
    ("account_permit", ("开户许可证", "核准号")),
    ("basic_account_info", ("基本存款账户信息",)),
    ("vehicle_license", ("机动车行驶证", "车辆识别代号")),
    ("marriage_cert", ("结婚证", "婚姻登记员")),
    ("real_estate_cert", ("不动产权证书", "不动产单元号")),
    ("property_cert", ("房屋所有权证", "房地权证")),
    ("driving_license", ("机动车驾驶证", "准驾车型")),
    ("divorce_cert", ("离婚证",)),
    ("household_register", ("居民户口簿", "户口簿")),
    ("lease_contract_keypage", ("租赁合同", "出租方", "承租方")),
    ("real_estate_query", ("不动产登记信息查询", "产调")),
    ("articles_keypage", ("公司章程",)),
    ("special_business_license", ("特许经营许可证",)),
    ("food_business_license", ("食品经营许可证",)),
    ("road_transport_license", ("道路运输经营许可证",)),
    ("account_receipt", ("开户信息回单",)),
    ("taxpayer_qualification", ("一般纳税人资格", "纳税人资格证明")),
]

PROPERTY_KEYWORDS = (
    "房地产权证",
    "房产证",
    "不动产权证",
    "权利人",
    "房地坐落",
    "建筑面积",
    "权属性质",
    "使用权面积",
    "宗地号",
    "室号或部位",
    "总层数",
    "竣工日期",
)

PROPERTY_FILENAME_KEYWORDS = ("产证", "房产证", "房地产权证", "不动产权证", "房本")


def chinese_keyword_score(text: str, keywords: tuple[str, ...] = PROPERTY_KEYWORDS) -> int:
    compact = re.sub(r"\s+", "", text or "")
    return sum(1 for keyword in keywords if keyword in compact)


def is_low_chinese_quality(text: str) -> bool:
    compact = re.sub(r"\s+", "", text or "")
    if not compact:
        return True
    chinese_count = len(re.findall(r"[\u4e00-\u9fff]", compact))
    ascii_count = len(re.findall(r"[A-Za-z]", compact))
    return chinese_count < 8 or (ascii_count >= chinese_count * 2 and chinese_count < 30)


def _filename_suggests_property(filename: str) -> bool:
    return any(keyword in (filename or "") for keyword in PROPERTY_FILENAME_KEYWORDS)


def classify_with_reason(text: str, filename: str = "") -> dict[str, str]:
    normalized = text or ""
    compact = re.sub(r"\s+", "", normalized)

    if "不动产权证书" in compact or "不动产单元号" in compact:
        return {
            "doc_type": "real_estate_cert",
            "doc_type_name": DOC_TYPE_NAMES["real_estate_cert"],
            "owner_type": OWNER_TYPES["real_estate_cert"],
            "reason": "文本包含不动产权证书或不动产单元号",
        }

    if "上海市房地产权证" in compact or "房地产权证" in compact:
        return {
            "doc_type": "property_cert",
            "doc_type_name": DOC_TYPE_NAMES["property_cert"],
            "owner_type": OWNER_TYPES["property_cert"],
            "reason": "文本包含房地产权证关键词",
        }

    property_combinations = (
        ("权利人", "房地坐落"),
        ("建筑面积", "室号或部位"),
        ("权属性质", "使用期限"),
        ("宗地号", "建筑面积"),
        ("权利人", "建筑面积"),
        ("房地坐落", "建筑面积"),
    )
    for left, right in property_combinations:
        if left in compact and right in compact:
            return {
                "doc_type": "property_cert",
                "doc_type_name": DOC_TYPE_NAMES["property_cert"],
                "owner_type": OWNER_TYPES["property_cert"],
                "reason": f"文本命中房产证组合关键词：{left}+{right}",
            }

    if _filename_suggests_property(filename):
        reason = "文件名包含产证/房产证/房本关键词"
        if chinese_keyword_score(normalized) > 0:
            reason = "文件名包含产证，且 OCR/页面关键词命中房产证字段"
        elif is_low_chinese_quality(normalized):
            reason = "文件名包含产证，OCR中文质量较低，按房产证兜底识别"
        return {
            "doc_type": "property_cert",
            "doc_type_name": DOC_TYPE_NAMES["property_cert"],
            "owner_type": OWNER_TYPES["property_cert"],
            "reason": reason,
        }

    for doc_type, keywords in CLASSIFICATION_RULES:
        if all(keyword in normalized for keyword in keywords):
            return {
                "doc_type": doc_type,
                "doc_type_name": DOC_TYPE_NAMES.get(doc_type, doc_type),
                "owner_type": OWNER_TYPES.get(doc_type, "unknown"),
                "reason": f"文本完整命中关键词：{', '.join(keywords)}",
            }
    for doc_type, keywords in CLASSIFICATION_RULES:
        if any(keyword in normalized for keyword in keywords):
            matched = [keyword for keyword in keywords if keyword in normalized]
            return {
                "doc_type": doc_type,
                "doc_type_name": DOC_TYPE_NAMES.get(doc_type, doc_type),
                "owner_type": OWNER_TYPES.get(doc_type, "unknown"),
                "reason": f"文本命中关键词：{', '.join(matched)}",
            }
    return {
        "doc_type": "unknown",
        "doc_type_name": DOC_TYPE_NAMES["unknown"],
        "owner_type": "unknown",
        "reason": "未命中支持的KYC资料关键词",
    }


def classify(text: str, filename: str = "") -> str:
    return classify_with_reason(text, filename=filename)["doc_type"]
