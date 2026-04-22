"""
File processing router for the Upload page.

Supports PDF / image / DOCX / XLSX extraction and structured parsing.
"""

from __future__ import annotations

import logging
import sys
from io import BytesIO
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from PIL import Image, ImageEnhance, ImageOps

desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from backend.document_types import get_document_display_name, normalize_document_type_code
from backend.routers.chat_helpers import extract_customer_name as extract_customer_name_from_content
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code
from services.ai_service import AIService, AIServiceError
from services.file_service import FileService
from services.ocr_service import OCRService, OCRServiceError

from ..middleware.auth import get_current_user
from ..models.schemas import FileProcessResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/file", tags=["File Processing"])

file_service = FileService()
ocr_service = OCRService()
ai_service = AIService()

NO_FILENAME_MESSAGE = "未提供文件名。"
FILE_READ_FAILED_MESSAGE = "文件读取失败，请重新上传后再试。"
EMPTY_FILE_MESSAGE = "上传文件为空，请重新选择文件。"
FILE_TOO_LARGE_MESSAGE = "上传文件过大，请压缩后重试。"
UNSUPPORTED_FILE_FORMAT_MESSAGE = "文件格式不支持，仅支持 PDF、DOCX、XLSX、PNG、JPG、JPEG。"
UNSUPPORTED_FILE_TYPE_MESSAGE = "文件类型不支持，请重新上传后再试。"
OCR_FAILED_MESSAGE = "文件识别失败，请检查文件清晰度后重试。"
FILE_PROCESS_FAILED_MESSAGE = "文件处理失败，请稍后重试。"
NO_TEXT_EXTRACTED_MESSAGE = "未能从文件中提取有效内容，请检查文件是否可读。"
PDF_TO_IMAGE_FAILED_MESSAGE = "PDF 转图片失败，无法继续识别。"
AI_CLASSIFICATION_FAILED_MESSAGE = "文件类型识别失败，请手动选择资料类型后重试。"
AI_EXTRACTION_FAILED_MESSAGE = "资料提取失败，请稍后重试。"
OCR_PAGE_FAILED_PLACEHOLDER = "[本页识别失败]"


async def _validate_and_read_file(file: UploadFile) -> tuple[bytes, str]:
    if not file.filename:
        raise HTTPException(status_code=400, detail=NO_FILENAME_MESSAGE)

    try:
        file_bytes = await file.read()
    except Exception as exc:  # pragma: no cover - IO safety
        logger.error("Failed to read file: %s", exc)
        raise HTTPException(status_code=400, detail=FILE_READ_FAILED_MESSAGE) from exc

    if not file_bytes:
        raise HTTPException(status_code=400, detail=EMPTY_FILE_MESSAGE)

    if not file_service.validate_file_size(file_bytes):
        raise HTTPException(status_code=400, detail=FILE_TOO_LARGE_MESSAGE)

    file_type = file_service.get_file_type(file.filename)
    if file_type == "unknown":
        raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_FORMAT_MESSAGE)

    return file_bytes, file_type


def _ocr_pdf_pages(file_bytes: bytes) -> str:
    images = file_service.pdf_to_images(file_bytes)
    if not images:
        raise HTTPException(status_code=400, detail=PDF_TO_IMAGE_FAILED_MESSAGE)

    ocr_results: list[str] = []
    for index, img_bytes in enumerate(images, start=1):
        compressed = file_service.compress_image(img_bytes)
        try:
            page_text = ocr_service.recognize_image(compressed)
            ocr_results.append(f"--- Page {index} ---\n{page_text}")
        except OCRServiceError as exc:
            logger.warning("OCR failed for page %s: %s", index, exc)
            ocr_results.append(f"--- Page {index} ---\n{OCR_PAGE_FAILED_PLACEHOLDER}")
    return "\n\n".join(ocr_results)


def _crop_image_region(image_bytes: bytes, box: tuple[int, int, int, int]) -> bytes:
    with Image.open(BytesIO(image_bytes)) as image:
        image = image.convert("RGB")
        cropped = image.crop(box)
        output = BytesIO()
        cropped.save(output, format="JPEG", quality=95)
        return output.getvalue()


def _image_to_jpeg_bytes(image: Image.Image) -> bytes:
    output = BytesIO()
    image.convert("RGB").save(output, format="JPEG", quality=95)
    return output.getvalue()


def _build_seal_ocr_variants(region_bytes: bytes) -> list[tuple[str, bytes]]:
    with Image.open(BytesIO(region_bytes)) as image:
        rgb = image.convert("RGB")
        grayscale = ImageOps.grayscale(rgb)
        high_contrast = ImageEnhance.Contrast(grayscale).enhance(2.8)

        red_mask = Image.new("L", rgb.size, 255)
        source_pixels = rgb.load()
        target_pixels = red_mask.load()
        width, height = rgb.size
        for y in range(height):
            for x in range(width):
                red, green, blue = source_pixels[x, y]
                # Red stamp text is often ignored by normal OCR; convert red-dominant pixels to black.
                if red >= 120 and red > green * 1.18 and red > blue * 1.18:
                    target_pixels[x, y] = 0

        red_mask = ImageEnhance.Contrast(red_mask).enhance(2.5)
        return [
            ("original", _image_to_jpeg_bytes(rgb)),
            ("gray_high_contrast", _image_to_jpeg_bytes(high_contrast)),
            ("red_stamp_mask", _image_to_jpeg_bytes(red_mask)),
        ]


def _business_license_seal_crop_boxes(image_bytes: bytes) -> list[tuple[str, tuple[int, int, int, int]]]:
    with Image.open(BytesIO(image_bytes)) as image:
        width, height = image.size
    return [
        ("bottom_full_45", (0, max(0, int(height * 0.55)), width, height)),
        ("bottom_full_35", (0, max(0, int(height * 0.65)), width, height)),
        ("bottom_full_25", (0, max(0, int(height * 0.75)), width, height)),
        ("bottom_left_middle_35", (0, max(0, int(height * 0.65)), max(1, int(width * 0.72)), height)),
        ("bottom_center_35", (max(0, int(width * 0.12)), max(0, int(height * 0.62)), min(width, int(width * 0.88)), height)),
        ("bottom_right_35", (max(0, int(width * 0.35)), max(0, int(height * 0.65)), width, height)),
    ]


def _ocr_business_license_seal_region(file_bytes: bytes, file_type: str, filename: str) -> str:
    logger.info("[business_license] start seal-region extraction filename=%s file_type=%s", filename, file_type)
    try:
        if file_type == "pdf":
            images = file_service.pdf_to_images(file_bytes)
            if not images:
                logger.warning("[business_license] seal-region extraction skipped: no rendered PDF images filename=%s", filename)
                return ""
            source_image = images[0]
        elif file_type == "image":
            source_image = file_bytes
        else:
            logger.info("[business_license] seal-region extraction skipped: unsupported file_type=%s filename=%s", file_type, filename)
            return ""

        ocr_parts: list[str] = []
        for region_name, box in _business_license_seal_crop_boxes(source_image):
            logger.info("[business_license] seal crop box=%s region=%s filename=%s", box, region_name, filename)
            try:
                seal_region = _crop_image_region(source_image, box)
                for variant_name, variant_bytes in _build_seal_ocr_variants(seal_region):
                    compressed = file_service.compress_image(variant_bytes)
                    seal_text = ocr_service.recognize_image(compressed).strip()
                    logger.info(
                        "[business_license] seal_region_ocr_text region=%s variant=%s text=%s",
                        region_name,
                        variant_name,
                        seal_text[:1000] or "(empty)",
                    )
                    if seal_text:
                        ocr_parts.append(f"--- Seal Region {region_name} variant={variant_name} box={box} ---\n{seal_text}")
            except OCRServiceError as exc:
                logger.warning("[business_license] seal region OCR failed region=%s filename=%s error=%s", region_name, filename, exc)
            except Exception as exc:  # pragma: no cover - best-effort per crop
                logger.warning("[business_license] seal region crop/OCR failed region=%s filename=%s error=%s", region_name, filename, exc)

        if not ocr_parts:
            logger.warning("[business_license] registration_authority extraction failed: seal-region OCR produced no text filename=%s", filename)
            return ""
        return "\n\n".join(ocr_parts)
    except OCRServiceError as exc:
        logger.warning("[business_license] seal region OCR failed for %s: %s", filename, exc)
        return ""
    except Exception as exc:  # pragma: no cover - best-effort OCR fallback
        logger.warning("[business_license] seal region crop/OCR failed for %s: %s", filename, exc)
        return ""


def _extract_content_from_file(file_bytes: bytes, file_type: str, filename: str) -> tuple[str, list[dict]]:
    try:
        if file_type == "pdf":
            extracted = file_service.extract_content(file_bytes, file_type, filename=filename)
            text_content = extracted.get("text", "")
            if not file_service.is_pdf_text_valid(text_content):
                logger.info("PDF text extraction invalid for %s, falling back to OCR", filename)
                text_content = _ocr_pdf_pages(file_bytes)
            return text_content, []
        if file_type == "image":
            compressed = file_service.compress_image(file_bytes)
            return ocr_service.recognize_image(compressed), []
        if file_type in {"excel", "word"}:
            extracted = file_service.extract_content(file_bytes, file_type, filename=filename)
            return extracted.get("text", ""), extracted.get("rows", [])
        raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_TYPE_MESSAGE)
    except OCRServiceError as exc:
        logger.error("OCR service error while processing %s: %s", filename, exc)
        raise HTTPException(status_code=400, detail=OCR_FAILED_MESSAGE) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive wrapper
        logger.error("File processing error for %s: %s", filename, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=FILE_PROCESS_FAILED_MESSAGE) from exc


def _resolve_document_type_code(text_content: str, explicit_type: str | None, rows: list[dict]) -> str:
    normalized = normalize_document_type_code(explicit_type)
    if normalized:
        return normalized
    try:
        return detect_document_type_code(text_content, explicit_type, rows=rows, ai_service=ai_service)
    except AIServiceError as exc:
        logger.error("AI classification error: %s", exc)
        raise HTTPException(status_code=500, detail=AI_CLASSIFICATION_FAILED_MESSAGE) from exc


def _extract_structured_data(text_content: str, document_type_code: str, rows: list[dict]) -> FileProcessResponse:
    try:
        content = build_structured_extraction(
            text_content,
            document_type_code,
            rows=rows,
            ai_service=ai_service,
        )
        customer_name = extract_customer_name_from_content(content)
        return FileProcessResponse(
            documentType=document_type_code,
            content=content,
            customerName=customer_name,
        )
    except AIServiceError as exc:
        logger.error("AI extraction error: %s", exc)
        raise HTTPException(status_code=500, detail=AI_EXTRACTION_FAILED_MESSAGE) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive wrapper
        logger.error("Unexpected error during extraction: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=AI_EXTRACTION_FAILED_MESSAGE) from exc


@router.post("/process", response_model=FileProcessResponse)
async def process_file(
    file: UploadFile = File(..., description="待处理文件，支持 PDF、图片、DOCX、XLSX"),
    documentType: str | None = Form(
        default=None,
        description="可选资料类型 code，例如 enterprise_credit、business_license、bank_statement",
    ),
    current_user: dict = Depends(get_current_user),
) -> FileProcessResponse:
    logger.info(
        "Processing file: %s, documentType=%s, user=%s",
        file.filename,
        documentType,
        current_user["username"],
    )

    file_bytes, file_type = await _validate_and_read_file(file)
    text_content, rows = _extract_content_from_file(file_bytes, file_type, file.filename or "")
    if not text_content or not text_content.strip():
        raise HTTPException(status_code=400, detail=NO_TEXT_EXTRACTED_MESSAGE)

    document_type_code = _resolve_document_type_code(text_content, documentType, rows)
    logger.info(
        "Resolved document type for %s: %s (%s)",
        file.filename,
        document_type_code,
        get_document_display_name(document_type_code),
    )
    if document_type_code == "business_license":
        seal_region_text = _ocr_business_license_seal_region(file_bytes, file_type, file.filename or "")
        if seal_region_text:
            text_content = f"{text_content}\n\n--- Business License Seal Region OCR ---\n{seal_region_text}"
    return _extract_structured_data(text_content, document_type_code, rows)
