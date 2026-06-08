from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RegionSpec:
    name: str
    x1: float
    y1: float
    x2: float
    y2: float


REGIONS = {
    "top_certificate_region_ocr": RegionSpec("top_certificate_region_ocr", 0.05, 0.00, 0.95, 0.12),
    "detail_table_region_ocr": RegionSpec("detail_table_region_ocr", 0.05, 0.08, 0.95, 0.90),
    "cover_seal_date_region_ocr": RegionSpec("cover_seal_date_region_ocr", 0.35, 0.45, 0.85, 0.75),
    "cover_bottom_number_region_ocr": RegionSpec("cover_bottom_number_region_ocr", 0.20, 0.70, 0.80, 0.95),
    "use_term_region_ocr": RegionSpec("use_term_region_ocr", 0.15, 0.35, 0.75, 0.55),
}

STANDARD_VARIANTS = ("original", "gray_high_contrast", "binary", "upscale_2x", "upscale_3x")
SEAL_DATE_VARIANTS = STANDARD_VARIANTS + ("remove_red_stamp_then_gray", "remove_red_stamp_then_binary")


def decode_text_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "latin1"):
        try:
            return file_bytes.decode(encoding)
        except Exception:
            continue
    return ""


def full_page_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> str:
    metadata = metadata or {}
    if metadata.get("raw_text"):
        return str(metadata.get("raw_text") or "")
    return decode_text_bytes(file_bytes or b"")


def region_ocr(file_bytes: bytes, region_name: str, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    metadata = metadata or {}
    variants = SEAL_DATE_VARIANTS if region_name == "cover_seal_date_region_ocr" else STANDARD_VARIANTS
    text = str((metadata.get("regions") or {}).get(region_name) or metadata.get("raw_text") or "")
    return {variant: text for variant in variants}


def top_certificate_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "top_certificate_region_ocr", metadata)


def detail_table_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "detail_table_region_ocr", metadata)


def cover_seal_date_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "cover_seal_date_region_ocr", metadata)


def cover_bottom_number_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "cover_bottom_number_region_ocr", metadata)


def use_term_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "use_term_region_ocr", metadata)
