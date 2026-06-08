from __future__ import annotations

import re

PROPERTY_DOC_TYPES = {"property_cert", "real_estate_cert"}
PROPERTY_FILENAME_KEYWORDS = ("房产", "产证", "不动产", "房本", "房地产权证")
PROPERTY_TEXT_KEYWORDS = (
    "不动产权证书",
    "不动产权证",
    "不动产单元号",
    "上海市房地产权证",
    "房地产权证",
    "房屋所有权证",
    "权利人",
    "房地坐落",
    "建筑面积",
)


def filename_suggests_property_cert(filename: str) -> bool:
    return any(keyword in str(filename or "") for keyword in PROPERTY_FILENAME_KEYWORDS)


def normalize_property_doc_type(value: str | None, filename: str = "") -> str:
    normalized = str(value or "").strip()
    aliases = {
        "property_certificate": "property_cert",
        "real_estate_certificate": "real_estate_cert",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in PROPERTY_DOC_TYPES:
        return "property_cert"
    if normalized == "collateral" and filename_suggests_property_cert(filename):
        return "property_cert"
    return ""


def text_suggests_property_cert(text: str) -> bool:
    compact = re.sub(r"\s+", "", str(text or ""))
    hits = sum(1 for keyword in PROPERTY_TEXT_KEYWORDS if keyword in compact)
    if "权利人" in compact and ("坐落" in compact or "建筑面积" in compact):
        return True
    return hits >= 1


def should_route_to_property_cert(
    *,
    declared_doc_type: str | None = None,
    document_type: str | None = None,
    classified_doc_type: str | None = None,
    filename: str = "",
    text: str = "",
) -> bool:
    for value in (declared_doc_type, document_type, classified_doc_type):
        if normalize_property_doc_type(value, filename=filename):
            return True
    if filename_suggests_property_cert(filename) and text_suggests_property_cert(text):
        return True
    return False
