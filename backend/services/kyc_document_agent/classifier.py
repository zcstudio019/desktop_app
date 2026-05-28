from __future__ import annotations


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


def classify(text: str) -> str:
    normalized = text or ""
    for doc_type, keywords in CLASSIFICATION_RULES:
        if all(keyword in normalized for keyword in keywords):
            return doc_type
    for doc_type, keywords in CLASSIFICATION_RULES:
        if any(keyword in normalized for keyword in keywords):
            return doc_type
    return "unknown"
