"""身份证专用 OCR 预处理与候选评分。

该服务只做图像候选生成和 OCR 文本质量选择，不参与字段正则提取。
"""

from __future__ import annotations

import logging
import re
from io import BytesIO
from typing import Any, Callable

from PIL import Image, ImageEnhance, ImageFilter, ImageOps

logger = logging.getLogger(__name__)

ID_CARD_LOW_QUALITY_MESSAGE = "身份证图片 OCR 质量较低，建议上传更清晰、方向端正的身份证正反面图片"
MAX_ID_CARD_OCR_VARIANTS = 12


def mask_id_number_for_log(text: str) -> str:
    return re.sub(r"(?<!\d)(\d{6})\d{8}(\d{3}[\dXx])(?!\d)", r"\1********\2", str(text or ""))


def _image_to_png_bytes(image: Image.Image) -> bytes:
    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="PNG")
    return buffer.getvalue()


def _open_image(image_bytes: bytes) -> Image.Image:
    image = Image.open(BytesIO(image_bytes))
    return ImageOps.exif_transpose(image).convert("RGB")


def _crop_non_white(image: Image.Image) -> Image.Image | None:
    gray = image.convert("L")
    mask = gray.point(lambda p: 0 if p > 245 else 255)
    bbox = mask.getbbox()
    if not bbox:
        return None
    width, height = image.size
    left, top, right, bottom = bbox
    margin_x = max(4, int((right - left) * 0.08))
    margin_y = max(4, int((bottom - top) * 0.08))
    left = max(0, left - margin_x)
    top = max(0, top - margin_y)
    right = min(width, right + margin_x)
    bottom = min(height, bottom + margin_y)
    if (right - left) < width * 0.2 or (bottom - top) < height * 0.2:
        return None
    return image.crop((left, top, right, bottom))


def _enhance_image(image: Image.Image) -> Image.Image:
    gray = ImageOps.grayscale(image)
    gray = ImageOps.autocontrast(gray)
    gray = ImageEnhance.Contrast(gray).enhance(1.6)
    gray = gray.filter(ImageFilter.SHARPEN)
    width, height = gray.size
    resized = gray.resize((max(1, width * 2), max(1, height * 2)), Image.Resampling.LANCZOS)
    return resized.convert("RGB")


def _split_regions(image: Image.Image) -> list[tuple[str, Image.Image]]:
    width, height = image.size
    regions: list[tuple[str, Image.Image]] = []
    if width >= 2 and height >= 2:
        regions.extend(
            [
                ("left_half", image.crop((0, 0, width // 2, height))),
                ("right_half", image.crop((width // 2, 0, width, height))),
                ("top_half", image.crop((0, 0, width, height // 2))),
                ("bottom_half", image.crop((0, height // 2, width, height))),
            ]
        )
    return regions


def preprocess_id_card_image(image_bytes: bytes) -> list[dict[str, Any]]:
    """生成身份证 OCR 候选图像，最多 12 个。"""
    image = _open_image(image_bytes)
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()

    def add_candidate(variant: str, img: Image.Image, *, rotation: int = 0, description: str = "") -> None:
        if len(candidates) >= MAX_ID_CARD_OCR_VARIANTS:
            return
        key = (variant, rotation)
        if key in seen:
            return
        seen.add(key)
        candidates.append(
            {
                "variant": variant,
                "image_bytes": _image_to_png_bytes(img),
                "rotation": rotation,
                "description": description or variant,
            }
        )

    rotations = (
        (0, "original", "原图"),
        (90, "rotate_90", "顺时针旋转90度"),
        (180, "rotate_180", "旋转180度"),
        (270, "rotate_270", "顺时针旋转270度"),
    )
    for angle, variant, description in rotations:
        add_candidate(variant, image.rotate(-angle, expand=True), rotation=angle, description=description)

    cropped = _crop_non_white(image)
    if cropped is not None:
        for angle in (0, 90, 180, 270):
            variant = "cropped" if angle == 0 else f"cropped_rotate_{angle}"
            add_candidate(variant, cropped.rotate(-angle, expand=True), rotation=angle, description="裁剪空白边")

        add_candidate("enhanced", _enhance_image(cropped), rotation=0, description="增强对比度/锐化")

        for region_name, region in _split_regions(cropped):
            add_candidate(region_name, region, rotation=0, description=f"区域裁剪 {region_name}")

    if len(candidates) < MAX_ID_CARD_OCR_VARIANTS:
        add_candidate("original_enhanced", _enhance_image(image), rotation=0, description="原图增强")

    logger.info("[IDCardOCR][PREPROCESS] variants_count=%s", len(candidates))
    return candidates[:MAX_ID_CARD_OCR_VARIANTS]


def score_id_card_ocr_text(text: str) -> int:
    source = str(text or "")
    compact = re.sub(r"\s+", "", source)
    score = 0
    keyword_scores = {
        "姓名": 20,
        "性别": 15,
        "民族": 15,
        "出生": 15,
        "住址": 15,
        "公民身份号码": 25,
        "居民身份证": 20,
        "签发机关": 20,
        "有效期限": 20,
    }
    for keyword, value in keyword_scores.items():
        if keyword in compact:
            score += value
    keyword_hits = sum(1 for keyword in keyword_scores if keyword in compact)
    if re.search(r"(?<!\d)\d{17}[\dXx](?!\d)", compact):
        score += 30
    if re.search(r"\d{4}年\d{1,2}月\d{1,2}日", source) or re.search(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", source):
        score += 10
    chinese_chars = re.findall(r"[\u4e00-\u9fff]", source)
    if len(chinese_chars) >= 12:
        score += 10

    length = len(source.strip())
    if length < 20:
        score -= 30
    if length < 8:
        score -= 30
    if length and not chinese_chars and re.search(r"\d", source):
        score -= 25
    special_chars = re.findall(r"[^\w\s\u4e00-\u9fff]", source)
    if length and len(special_chars) / max(length, 1) > 0.25:
        score -= 15
    mojibake_like = re.findall(r"[�□]|[A-Za-z]{6,}", source)
    score -= len(mojibake_like) * 5
    if len(chinese_chars) < 3 and len(re.findall(r"\d", source)) > 10:
        score -= 25
    if keyword_hits == 0 and len(re.findall(r"\d", source)) > max(8, len(chinese_chars) * 2):
        score -= 25
    return score


def _dedupe_text_blocks(texts: list[str]) -> str:
    lines: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for line in str(text or "").splitlines():
            normalized = re.sub(r"\s+", "", line)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            lines.append(line.strip())
    return "\n".join(lines)


def _quality_payload(score: int, variant: str) -> dict[str, Any]:
    if score >= 30:
        return {"status": "ok", "best_score": score, "best_variant": variant}
    return {
        "status": "low_quality",
        "best_score": score,
        "best_variant": variant,
        "message": ID_CARD_LOW_QUALITY_MESSAGE,
    }


def ocr_id_card_with_variants(image_bytes: bytes, ocr_func: Callable[[bytes], str]) -> dict[str, Any]:
    candidates = preprocess_id_card_image(image_bytes)
    candidate_results: list[dict[str, Any]] = []
    best_text = ""
    best_variant = ""
    best_score = -10**9

    for candidate in candidates:
        variant = str(candidate.get("variant") or "")
        try:
            text = str(ocr_func(candidate["image_bytes"]) or "").strip()
        except Exception as exc:  # pragma: no cover - OCR backend defensive guard
            logger.warning("[IDCardOCR][CANDIDATE] variant=%s failed error=%s", variant, exc)
            text = ""
        score = score_id_card_ocr_text(text)
        preview = mask_id_number_for_log(text[:240])
        logger.info("[IDCardOCR][CANDIDATE] variant=%s score=%s text_preview=%s", variant, score, preview)
        candidate_results.append({"variant": variant, "score": score, "text_preview": preview, "text": text})
        if score > best_score:
            best_score = score
            best_variant = variant
            best_text = text

    high_score_texts = [
        item["text"]
        for item in sorted(candidate_results, key=lambda value: int(value.get("score") or 0), reverse=True)[:3]
        if int(item.get("score") or 0) >= max(20, best_score - 15) and str(item.get("text") or "").strip()
    ]
    final_text = _dedupe_text_blocks(high_score_texts) or best_text
    logger.info("[IDCardOCR][BEST] variant=%s score=%s", best_variant, best_score)
    logger.info("[IDCardOCR][FINAL_TEXT] %s", mask_id_number_for_log(final_text[:1000]))
    return {
        "text": final_text,
        "best_variant": best_variant,
        "score": best_score,
        "candidates": [
            {"variant": item["variant"], "score": item["score"], "text_preview": item["text_preview"]}
            for item in candidate_results
        ],
        "ocr_quality": _quality_payload(best_score, best_variant),
    }
