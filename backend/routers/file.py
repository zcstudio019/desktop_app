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
from PIL import Image

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


def _crop_bottom_region(image_bytes: bytes, ratio: float = 0.35) -> bytes:
    with Image.open(BytesIO(image_bytes)) as image:
        image = image.convert("RGB")
        width, height = image.size
        top = max(0, int(height * (1 - ratio)))
        cropped = image.crop((0, top, width, height))
        output = BytesIO()
        cropped.save(output, format="JPEG", quality=95)
        return output.getvalue()


def _ocr_business_license_seal_region(file_bytes: bytes, file_type: str, filename: str) -> str:
    try:
        if file_type == "pdf":
            images = file_service.pdf_to_images(file_bytes)
            if not images:
                return ""
            source_image = images[0]
        elif file_type == "image":
            source_image = file_bytes
        else:
            return ""

        seal_region = _crop_bottom_region(source_image)
        compressed = file_service.compress_image(seal_region)
        seal_text = ocr_service.recognize_image(compressed).strip()
        if seal_text:
            logger.info("[business_license] seal_region_ocr_text=%s", seal_text[:500])
        else:
            logger.warning("[business_license] seal_region_ocr_text empty for %s", filename)
        return seal_text
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
