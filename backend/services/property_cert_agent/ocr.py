from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable

from PIL import Image, ImageEnhance, ImageOps

from .page_role import detect_page_role
from .skills.attachment_page_skill import extract as extract_attachment_page
from .skills.cover_page_skill import extract as extract_cover_page
from .skills.new_real_estate_cert_skill import extract as extract_new_detail_page
from .skills.old_shanghai_property_cert_skill import extract as extract_old_detail_page

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RegionSpec:
    name: str
    x1: float
    y1: float
    x2: float
    y2: float


REGIONS = {
    "top_certificate_number_region": RegionSpec("top_certificate_number_region", 0.05, 0.00, 0.95, 0.12),
    "detail_table_region": RegionSpec("detail_table_region", 0.05, 0.08, 0.95, 0.90),
    "left_right_table_region": RegionSpec("left_right_table_region", 0.00, 0.00, 0.95, 0.95),
    "cover_registration_date_region": RegionSpec("cover_registration_date_region", 0.35, 0.45, 0.85, 0.75),
    "cover_bottom_number_region": RegionSpec("cover_bottom_number_region", 0.20, 0.70, 0.80, 0.95),
    "use_term_region": RegionSpec("use_term_region", 0.15, 0.35, 0.75, 0.55),
    "attachment_table_region": RegionSpec("attachment_table_region", 0.02, 0.05, 0.98, 0.96),
}

LEGACY_REGION_ALIASES = {
    "top_certificate_region_ocr": "top_certificate_number_region",
    "detail_table_region_ocr": "detail_table_region",
    "cover_seal_date_region_ocr": "cover_registration_date_region",
    "cover_bottom_number_region_ocr": "cover_bottom_number_region",
    "use_term_region_ocr": "use_term_region",
}

OCR_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "ocr_cache" / "property_cert"
DEFAULT_MAX_OCR_BYTES = 3_500_000
HARD_OCR_LIMIT_BYTES = 4_000_000
ATTACHMENT_UNIT_RE = r"\d{6,}GB[A-Z0-9]+F[A-Z0-9]+"


def decode_text_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "latin1"):
        try:
            return file_bytes.decode(encoding)
        except Exception:
            continue
    return ""


def _as_image(image: Image.Image | bytes) -> Image.Image:
    if isinstance(image, Image.Image):
        return image.convert("RGB")
    return Image.open(BytesIO(image)).convert("RGB")


def _jpeg_bytes(image: Image.Image, quality: int) -> bytes:
    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="JPEG", quality=quality, optimize=True)
    return buffer.getvalue()


def _fit_max_side(image: Image.Image, max_side: int) -> Image.Image:
    width, height = image.size
    longest = max(width, height)
    if longest <= max_side:
        return image
    scale = max_side / float(longest)
    size = (max(1, int(width * scale)), max(1, int(height * scale)))
    return image.resize(size, Image.Resampling.LANCZOS)


def prepare_image_for_ocr(
    image: Image.Image | bytes,
    max_bytes: int = DEFAULT_MAX_OCR_BYTES,
    max_side: int = 2400,
) -> bytes:
    source = _as_image(image)
    before_bytes = len(image) if isinstance(image, bytes) else len(_jpeg_bytes(source, 90))
    working = _fit_max_side(source, max_side)
    quality_used = 90
    output = _jpeg_bytes(working, quality_used)
    side_limit = max_side
    while len(output) > max_bytes:
        changed = False
        for quality in (75, 65, 55):
            quality_used = quality
            output = _jpeg_bytes(working, quality)
            changed = True
            if len(output) <= max_bytes:
                break
        if len(output) <= max_bytes:
            break
        side_limit = max(900, int(side_limit * 0.85))
        resized = _fit_max_side(source, side_limit)
        if resized.size == working.size and not changed:
            break
        working = resized
        quality_used = 55
        output = _jpeg_bytes(working, quality_used)
        if side_limit <= 900 and len(output) <= max_bytes:
            break
    logger.info(
        "[OCRPrepare] before_bytes=%s after_bytes=%s size=%s quality=%s",
        before_bytes,
        len(output),
        working.size,
        quality_used,
    )
    return output


def _cache_path(cache_key: str) -> Path:
    return OCR_CACHE_DIR / f"{cache_key}.json"


def build_cache_key(file_hash: str, page_no: int, region: str, variant: str) -> str:
    raw = f"{file_hash}:{page_no}:{region}:{variant}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def read_ocr_cache(cache_key: str) -> dict[str, Any] | None:
    path = _cache_path(cache_key)
    if not path.exists():
        logger.info("[OCRCache] miss key=%s", cache_key)
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        logger.info("[OCRCache] hit key=%s", cache_key)
        return data if isinstance(data, dict) else None
    except Exception:
        logger.info("[OCRCache] miss key=%s", cache_key)
        return None


def write_ocr_cache(cache_key: str, text: str, confidence: float = 0.0) -> None:
    OCR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(cache_key).write_text(
        json.dumps(
            {
                "ocr_text": text,
                "confidence": confidence,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def score_property_ocr_text(text: str) -> int:
    compact = "".join(str(text or "").split())
    score = 0
    if "权利人" in compact:
        score += 15
    if "不动产权第" in compact or "房地" in compact and "第" in compact and "号" in compact:
        score += 15
    if "坐落" in compact or "房地坐落" in compact:
        score += 15
    if "不动产单元号" in compact:
        score += 10
    if "建筑面积" in compact:
        score += 10
    if "使用期限" in compact or "土地使用期限" in compact:
        score += 10
    if ("土地用途" in compact and "房屋用途" in compact) or "用途" in compact:
        score += 10
    return score


def _attachment_ocr_score(text: str) -> tuple[int, int]:
    compact = "".join(str(text or "").split()).upper()
    unit_count = len(re.findall(ATTACHMENT_UNIT_RE, compact))
    table_hits = sum(1 for keyword in ("室号", "建筑面积", "用途", "总层数", "竣工日期") if keyword in compact)
    return unit_count, table_hits


def property_fields_complete(fields: dict[str, Any]) -> bool:
    return bool(
        fields.get("权利人")
        and fields.get("权证编号")
        and (fields.get("坐落") or fields.get("房地坐落"))
        and fields.get("建筑面积")
        and (fields.get("使用期限") or fields.get("土地使用期限"))
    )


def missing_property_fields(fields: dict[str, Any], role: str) -> list[str]:
    missing: list[str] = []
    if role == "cover_page":
        if not fields.get("登记日期"):
            missing.append("登记日期")
        if not fields.get("封面编号"):
            missing.append("封面编号")
        return missing
    if not fields.get("权证编号"):
        missing.append("权证编号")
    if not fields.get("权利人") or not (fields.get("坐落") or fields.get("房地坐落")) or not fields.get("建筑面积"):
        missing.append("字段表")
    if not fields.get("使用期限") and not fields.get("土地使用期限"):
        missing.append("使用期限")
    return missing


def _variant_image(image: Image.Image, variant: str) -> Image.Image:
    rgb = image.convert("RGB")
    if variant == "original":
        return rgb
    if variant == "rotate90":
        return rgb.rotate(90, expand=True)
    if variant == "rotate270":
        return rgb.rotate(270, expand=True)
    gray = ImageOps.grayscale(rgb)
    if variant in {"gray", "gray_high_contrast"}:
        return ImageEnhance.Contrast(gray).enhance(1.8).convert("RGB")
    if variant == "binary":
        return gray.point(lambda p: 255 if p > 170 else 0).convert("RGB")
    if variant == "remove_red_stamp_then_gray":
        pixels = rgb.load()
        for y in range(rgb.height):
            for x in range(rgb.width):
                r, g, b = pixels[x, y]
                if r > 120 and r > g * 1.2 and r > b * 1.2:
                    pixels[x, y] = (255, 255, 255)
        return ImageEnhance.Contrast(ImageOps.grayscale(rgb)).enhance(1.8).convert("RGB")
    return rgb


def _crop_region(image: Image.Image, region_name: str) -> Image.Image:
    spec = REGIONS[LEGACY_REGION_ALIASES.get(region_name, region_name)]
    width, height = image.size
    box = (
        max(0, int(width * spec.x1)),
        max(0, int(height * spec.y1)),
        min(width, int(width * spec.x2)),
        min(height, int(height * spec.y2)),
    )
    return image.crop(box)


def _extract_fields_for_role(text: str, role: str) -> dict[str, Any]:
    if role == "cover_page":
        return extract_cover_page({"text": text}).get("fields") or {}
    if role == "old_property_detail_page":
        return extract_old_detail_page({"text": text}).get("fields") or {}
    if role == "attachment_page":
        return extract_attachment_page({"text": text}).get("fields") or {}
    if role in {"detail_page", "unknown"}:
        return extract_new_detail_page({"text": text}).get("fields") or {}
    return {}


def _planned_regions_for_role(role: str, fields: dict[str, Any]) -> list[tuple[str, tuple[str, ...], str]]:
    missing = set(missing_property_fields(fields, role))
    if role == "cover_page":
        plan: list[tuple[str, tuple[str, ...], str]] = []
        if "封面编号" in missing:
            plan.append(("cover_bottom_number_region", ("original", "gray"), "封面编号"))
        if "登记日期" in missing:
            plan.append(("cover_registration_date_region", ("remove_red_stamp_then_gray",), "登记日期"))
        return plan
    if role == "old_property_detail_page":
        plan = []
        if "字段表" in missing:
            plan.extend(
                [
                    ("left_right_table_region", ("original", "gray"), "字段表"),
                    ("detail_table_region", ("original", "gray"), "字段表"),
                ]
            )
        return plan
    if role == "attachment_page":
        logger.info("[AttachmentOCR] rotated=false text_length=%s", 0)
        return [("attachment_table_region", ("original", "rotate90", "rotate270", "gray", "binary"), "附记")]
    plan = []
    if "权证编号" in missing:
        plan.append(("top_certificate_number_region", ("original", "gray"), "权证编号"))
    if "字段表" in missing:
        plan.append(("detail_table_region", ("original", "gray"), "字段表"))
    if "使用期限" in missing:
        plan.append(("use_term_region", ("original",), "使用期限"))
    return plan


def _safe_ocr_call(
    *,
    file_hash: str,
    page_no: int,
    region: str,
    variant: str,
    image: Image.Image,
    ocr_func: Callable[[bytes], str],
    stats: dict[str, int],
) -> str:
    cache_key = build_cache_key(file_hash, page_no, region, variant)
    cached = read_ocr_cache(cache_key)
    if cached is not None:
        stats["cache_hits"] += 1
        return str(cached.get("ocr_text") or "")

    prepared = prepare_image_for_ocr(_variant_image(image, variant))
    if len(prepared) > HARD_OCR_LIMIT_BYTES:
        stats["skipped"] += 1
        logger.warning("[PropertyCertOCRSkip] reason=image_too_large page=%s region=%s variant=%s", page_no, region, variant)
        return ""
    for attempt in (0, 1):
        try:
            stats["calls"] += 1
            text = str(ocr_func(prepared) or "").strip()
            write_ocr_cache(cache_key, text, 0.0)
            return text
        except Exception as exc:
            message = str(exc)
            if "image size" not in message.lower() and "4mb" not in message.lower() and "4MB" not in message:
                logger.warning("[PropertyCertOCRSkip] reason=ocr_error page=%s region=%s variant=%s error=%s", page_no, region, variant, exc)
                stats["skipped"] += 1
                return ""
            if attempt == 0:
                prepared = prepare_image_for_ocr(prepared, max_bytes=3_000_000, max_side=1800)
                continue
            logger.warning("[PropertyCertOCRSkip] reason=image_size_error page=%s region=%s variant=%s error=%s", page_no, region, variant, exc)
            stats["skipped"] += 1
            return ""
    return ""


def run_property_cert_ocr_plan(
    *,
    file_bytes: bytes,
    file_type: str,
    filename: str,
    pdf_to_images: Callable[[bytes], list[bytes]],
    ocr_func: Callable[[bytes], str],
    max_calls: int = 6,
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    start = time.perf_counter()
    file_hash = hashlib.sha256(file_bytes or b"").hexdigest()
    if file_type == "pdf":
        source_images = pdf_to_images(file_bytes or b"")[:2]
    elif file_type == "image":
        source_images = [file_bytes]
    else:
        source_images = []
    logger.info("[PropertyCertOCRPlan] filename=%s pages=%s strategy=fast", filename, len(source_images))

    stats = {"calls": 0, "skipped": 0, "cache_hits": 0}
    raw_pages: list[dict[str, Any]] = []
    for page_no, source in enumerate(source_images, start=1):
        image = _as_image(source)
        parts: list[str] = []
        page_text = ""
        for variant in ("original", "gray"):
            if stats["calls"] >= max_calls:
                break
            text = _safe_ocr_call(
                file_hash=file_hash,
                page_no=page_no,
                region="full",
                variant=variant,
                image=image,
                ocr_func=ocr_func,
                stats=stats,
            )
            if text:
                parts.append(f"--- Property Certificate OCR page={page_no} region=full variant={variant} ---\n{text}")
                page_text = f"{page_text}\n{text}".strip()
            score = score_property_ocr_text(page_text)
            if score >= 60:
                logger.info("[PropertyCertOCRStop] reason=score_enough score=%s", score)
                break
        role = detect_page_role(page_text)
        logger.info("[PropertyCertOCRPlan] page=%s role=%s planned_regions=pending", page_no, role)
        fields = _extract_fields_for_role(page_text, role)
        if property_fields_complete(fields):
            logger.info("[PropertyCertOCRStop] reason=fields_complete page=%s", page_no)
            raw_pages.append({"page": page_no, "text": "\n\n".join(parts), "source": "property_cert_ocr_plan"})
            continue

        planned = _planned_regions_for_role(role, fields)
        logger.info("[PropertyCertOCRPlan] page=%s role=%s planned_regions=%s", page_no, role, [item[0] for item in planned])
        for region, variants, target_field in planned:
            if stats["calls"] >= max_calls:
                break
            fields = _extract_fields_for_role(page_text, role)
            if target_field not in missing_property_fields(fields, role):
                stats["skipped"] += len(variants)
                logger.info("[PropertyCertOCRSkip] reason=field_already_extracted field=%s", target_field)
                continue
            crop = _crop_region(image, region)
            best_attachment_variant = ""
            best_attachment_score = (-1, -1)
            for variant in variants:
                if stats["calls"] >= max_calls:
                    break
                text = _safe_ocr_call(
                    file_hash=file_hash,
                    page_no=page_no,
                    region=region,
                    variant=variant,
                    image=crop,
                    ocr_func=ocr_func,
                    stats=stats,
                )
                if role == "attachment_page":
                    unit_count, table_hits = _attachment_ocr_score(text)
                    logger.info("[AttachmentOCR] variant=%s unit_count=%s table_hits=%s", variant, unit_count, table_hits)
                    if (unit_count, table_hits) > best_attachment_score:
                        best_attachment_score = (unit_count, table_hits)
                        best_attachment_variant = variant
                if text:
                    parts.append(f"--- Property Certificate OCR page={page_no} region={region} variant={variant} ---\n{text}")
                    page_text = f"{page_text}\n{text}".strip()
                score = score_property_ocr_text(page_text)
                if score >= 60:
                    logger.info("[PropertyCertOCRStop] reason=score_enough score=%s", score)
                    break
            if role == "attachment_page" and best_attachment_variant:
                logger.info("[AttachmentOCR] selected_variant=%s", best_attachment_variant)
            if score_property_ocr_text(page_text) >= 60:
                break
        if parts:
            raw_pages.append({"page": page_no, "text": "\n\n".join(parts), "source": "property_cert_ocr_plan"})

    text_content = "\n\n".join(str(page.get("text") or "") for page in raw_pages if page.get("text"))
    summary = {
        **stats,
        "cost_ms": int((time.perf_counter() - start) * 1000),
        "pages": len(source_images),
    }
    logger.info(
        "[PropertyCertOCRSummary] calls=%s skipped=%s cache_hits=%s cost_ms=%s",
        summary["calls"],
        summary["skipped"],
        summary["cache_hits"],
        summary["cost_ms"],
    )
    return text_content, raw_pages, summary


def full_page_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> str:
    metadata = metadata or {}
    if metadata.get("raw_text"):
        return str(metadata.get("raw_text") or "")
    return decode_text_bytes(file_bytes or b"")


def region_ocr(file_bytes: bytes, region_name: str, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    metadata = metadata or {}
    region = LEGACY_REGION_ALIASES.get(region_name, region_name)
    text = str((metadata.get("regions") or {}).get(region) or (metadata.get("regions") or {}).get(region_name) or metadata.get("raw_text") or "")
    return {"original": text, "gray": text}


def top_certificate_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "top_certificate_number_region", metadata)


def detail_table_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "detail_table_region", metadata)


def cover_seal_date_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "cover_registration_date_region", metadata)


def cover_bottom_number_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "cover_bottom_number_region", metadata)


def use_term_region_ocr(file_bytes: bytes, metadata: dict[str, Any] | None = None) -> dict[str, str]:
    return region_ocr(file_bytes, "use_term_region", metadata)
