from __future__ import annotations

import re

from .schema import DOC_TYPE_NAMES, OWNER_TYPES


CLASSIFICATION_RULES: list[tuple[str, tuple[str, ...]]] = [
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

ID_CARD_FRONT_KEYWORDS = ("姓名", "性别", "民族", "出生", "住址", "公民身份号码")
ID_CARD_BACK_KEYWORDS = ("签发机关", "有效期限")
ID_CARD_STRONG_KEYWORDS = ("居民身份证", "中华人民共和国居民身份证", "公民身份号码")
ID_CARD_NEGATIVE_KEYWORDS = ("居民户口簿", "户口簿", "户主页", "常住人口登记卡", "结婚证", "婚姻登记员", "离婚证")

PROPERTY_KEYWORDS = (
    "房地产权证",
    "房产证",
    "不动产权证",
    "不动产权证书",
    "不动产单元号",
    "国有建设用地使用权/房屋所有权",
    "权利人",
    "房地坐落",
    "坐落",
    "建筑面积",
    "权属性质",
    "权利性质",
    "权利类型",
    "使用权面积",
    "宗地号",
    "室号或部位",
    "总层数",
    "竣工日期",
)

PROPERTY_FILENAME_KEYWORDS = ("房产", "产证", "房产证", "房地产权证", "不动产权证", "房本")
DECLARED_DOC_TYPE_ALIASES = {
    "营业执照": "business_license",
    "licence": "business_license",
    "license": "business_license",
    "company_license": "business_license",
    "行驶证": "vehicle_license",
    "vehicle_license": "vehicle_license",
    "property_certificate": "property_cert",
    "real_estate_certificate": "real_estate_cert",
    "account_license": "account_permit",
    "hukou": "household_register",
    "marriage_cert": "marriage_certificate",
}


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


def normalize_declared_doc_type(declared_doc_type: str | None, filename: str = "") -> str:
    normalized = (declared_doc_type or "").strip()
    if not normalized:
        return ""
    normalized = DECLARED_DOC_TYPE_ALIASES.get(normalized, normalized)
    if normalized == "collateral" and _filename_suggests_property(filename):
        return "property_cert"
    if normalized in DOC_TYPE_NAMES and normalized != "unknown":
        return normalized
    return ""


def classify_with_reason(text: str, filename: str = "", declared_doc_type: str | None = None) -> dict[str, str]:
    declared = normalize_declared_doc_type(declared_doc_type, filename=filename)
    if declared:
        return {
            "doc_type": declared,
            "doc_type_name": DOC_TYPE_NAMES[declared],
            "owner_type": OWNER_TYPES.get(declared, "unknown"),
            "reason": f"declared_doc_type 指定为 {declared}",
        }

    normalized = text or ""
    compact = re.sub(r"\s+", "", normalized)

    account_negative_keywords = ("营业执照", "银行流水", "交易明细", "对账单")
    if not any(keyword in compact for keyword in account_negative_keywords):
        if "基本存款账户信息" in compact:
            return {
                "doc_type": "basic_account_info",
                "doc_type_name": DOC_TYPE_NAMES["basic_account_info"],
                "owner_type": OWNER_TYPES["basic_account_info"],
                "reason": "文本命中基本存款账户信息",
            }
        if "开户许可证" in compact or "基本账户开户许可证" in compact:
            return {
                "doc_type": "account_permit",
                "doc_type_name": DOC_TYPE_NAMES["account_permit"],
                "owner_type": OWNER_TYPES["account_permit"],
                "reason": "文本命中开户许可证",
            }
        account_keywords = ("开户银行", "账号", "账户号码", "基本存款账户编号", "核准号", "法定代表人", "单位负责人")
        matched_account = [keyword for keyword in account_keywords if keyword in compact]
        if "开户银行" in compact and ("账号" in compact or "账户号码" in compact) and "基本存款账户编号" in compact:
            return {
                "doc_type": "account_permit",
                "doc_type_name": DOC_TYPE_NAMES["account_permit"],
                "owner_type": OWNER_TYPES["account_permit"],
                "reason": f"文本命中账户资料组合关键词：{', '.join(matched_account)}",
            }

    vehicle_license_negative_keywords = (
        "机动车驾驶证",
        "准驾车型",
        "驾驶证",
        "居民身份证",
        "营业执照",
        "不动产权证",
        "房屋所有权证",
    )
    vehicle_license_strong_keywords = (
        "中华人民共和国机动车行驶证",
        "机动车行驶证",
        "VehicleLicense",
    )
    vehicle_license_core_keywords = (
        "号牌号码",
        "PlateNo",
        "车辆类型",
        "VehicleType",
        "所有人",
        "Owner",
        "住址",
        "Address",
        "使用性质",
        "UseCharacter",
        "品牌型号",
        "Model",
        "车辆识别代号",
        "VIN",
        "发动机号码",
        "EngineNo",
        "注册日期",
        "RegisterDate",
        "发证日期",
        "IssueDate",
    )
    vehicle_compact = re.sub(r"[\s.：:]+", "", normalized)
    if not any(keyword in compact for keyword in vehicle_license_negative_keywords):
        has_title = any(keyword in vehicle_compact for keyword in vehicle_license_strong_keywords)
        matched_vehicle = [keyword for keyword in vehicle_license_core_keywords if keyword in vehicle_compact]
        if (has_title and len(matched_vehicle) >= 1) or len(matched_vehicle) >= 4:
            return {
                "doc_type": "vehicle_license",
                "doc_type_name": DOC_TYPE_NAMES["vehicle_license"],
                "owner_type": OWNER_TYPES["vehicle_license"],
                "reason": f"文本命中行驶证关键词：{', '.join(matched_vehicle[:6])}",
            }

    business_license_negative_keywords = (
        "开户许可证",
        "基本存款账户信息",
        "食品经营许可证",
        "特许经营许可证",
        "道路运输经营许可证",
        "纳税人资格证明",
    )
    business_license_keywords = (
        "营业执照",
        "统一社会信用代码",
        "名称",
        "类型",
        "法定代表人",
        "注册资本",
        "成立日期",
        "住所",
        "经营范围",
        "登记机关",
        "市场监督管理局",
        "发照日期",
        "证照编号",
    )
    if "营业执照" in compact and not any(keyword in compact for keyword in business_license_negative_keywords):
        if "统一社会信用代码" in compact or "法定代表人" in compact:
            return {
                "doc_type": "business_license",
                "doc_type_name": DOC_TYPE_NAMES["business_license"],
                "owner_type": OWNER_TYPES["business_license"],
                "reason": "文本命中营业执照及核心关键词",
            }
        matched = [keyword for keyword in business_license_keywords if keyword in compact]
        if len(matched) >= 4:
            return {
                "doc_type": "business_license",
                "doc_type_name": DOC_TYPE_NAMES["business_license"],
                "owner_type": OWNER_TYPES["business_license"],
                "reason": f"文本命中营业执照关键词：{', '.join(matched)}",
            }

    if "居民户口簿" in compact or "户口簿" in compact or "常住人口登记卡" in compact:
        return {
            "doc_type": "household_register",
            "doc_type_name": DOC_TYPE_NAMES["household_register"],
            "owner_type": OWNER_TYPES["household_register"],
            "reason": "文本命中户口本关键词，排除身份证误判",
        }
    if "离婚证" in compact or "离婚登记" in compact:
        return {
            "doc_type": "divorce_cert",
            "doc_type_name": DOC_TYPE_NAMES["divorce_cert"],
            "owner_type": OWNER_TYPES["divorce_cert"],
            "reason": "文本命中离婚证关键词，优先排除结婚证误判",
        }
    marriage_patterns = (
        ("结婚证字号",),
        ("结婚证", "姓名", "身份证件号"),
        ("中华人民共和国民政部监制", "持证人"),
        ("婚姻登记机关", "发给此证"),
        ("符合《中华人民共和国婚姻法》", "准予登记"),
        ("结婚证", "身份证件号"),
        ("婚姻登记员",),
    )
    for keywords in marriage_patterns:
        if all(keyword in compact for keyword in keywords):
            return {
                "doc_type": "marriage_certificate",
                "doc_type_name": DOC_TYPE_NAMES["marriage_certificate"],
                "owner_type": OWNER_TYPES["marriage_certificate"],
                "reason": f"文本命中结婚证关键词：{'+'.join(keywords)}",
            }
    if "结婚证" in compact or "婚姻登记员" in compact:
        return {
            "doc_type": "marriage_certificate",
            "doc_type_name": DOC_TYPE_NAMES["marriage_certificate"],
            "owner_type": OWNER_TYPES["marriage_certificate"],
            "reason": "文本命中结婚证关键词，排除身份证号误判",
        }

    id_front_score = sum(1 for keyword in ID_CARD_FRONT_KEYWORDS if keyword in compact)
    id_back_score = sum(1 for keyword in ID_CARD_BACK_KEYWORDS if keyword in compact)
    id_strong = any(keyword in compact for keyword in ID_CARD_STRONG_KEYWORDS)
    if not any(keyword in compact for keyword in ID_CARD_NEGATIVE_KEYWORDS) and (
        id_strong or id_front_score >= 4 or id_back_score >= 2
    ):
        matched: list[str] = []
        matched.extend(keyword for keyword in ID_CARD_STRONG_KEYWORDS if keyword in compact)
        matched.extend(keyword for keyword in ID_CARD_FRONT_KEYWORDS if keyword in compact)
        matched.extend(keyword for keyword in ID_CARD_BACK_KEYWORDS if keyword in compact)
        return {
            "doc_type": "id_card",
            "doc_type_name": DOC_TYPE_NAMES["id_card"],
            "owner_type": OWNER_TYPES["id_card"],
            "reason": f"文本命中身份证关键词：{', '.join(dict.fromkeys(matched))}",
        }

    if (
        "不动产权证书" in compact
        or "不动产权证" in compact
        or "不动产单元号" in compact
        or "国有建设用地使用权/房屋所有权" in compact
    ):
        return {
            "doc_type": "property_cert",
            "doc_type_name": DOC_TYPE_NAMES["property_cert"],
            "owner_type": OWNER_TYPES["property_cert"],
            "reason": "文本包含不动产权证/不动产单元号关键词",
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
        ("坐落", "不动产单元号"),
        ("权利类型", "权利性质"),
        ("权利人", "不动产单元号"),
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


def classify(text: str, filename: str = "", declared_doc_type: str | None = None) -> str:
    return classify_with_reason(text, filename=filename, declared_doc_type=declared_doc_type)["doc_type"]
