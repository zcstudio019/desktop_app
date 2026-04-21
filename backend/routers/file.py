"""
File processing router for the Upload page.

Supports PDF / image / DOCX / XLSX extraction and structured parsing.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

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


def _extract_text_from_file(file_bytes: bytes, file_type: str, filename: str) -> str:
    try:
        if file_type == "pdf":
            text_content = file_service.extract_text(file_bytes, file_type, filename=filename)
            if not file_service.is_pdf_text_valid(text_content):
                logger.info("PDF text extraction invalid for %s, falling back to OCR", filename)
                text_content = _ocr_pdf_pages(file_bytes)
            return text_content
        if file_type == "image":
            compressed = file_service.compress_image(file_bytes)
            return ocr_service.recognize_image(compressed)
        if file_type in {"excel", "word"}:
            return file_service.extract_text(file_bytes, file_type, filename=filename)
        raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_TYPE_MESSAGE)
    except OCRServiceError as exc:
        logger.error("OCR service error while processing %s: %s", filename, exc)
        raise HTTPException(status_code=400, detail=OCR_FAILED_MESSAGE) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive wrapper
        logger.error("File processing error for %s: %s", filename, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=FILE_PROCESS_FAILED_MESSAGE) from exc


def _resolve_document_type_code(text_content: str, explicit_type: str | None) -> str:
    normalized = normalize_document_type_code(explicit_type)
    if normalized:
        return normalized
    try:
        return detect_document_type_code(text_content, explicit_type, ai_service=ai_service)
    except AIServiceError as exc:
        logger.error("AI classification error: %s", exc)
        raise HTTPException(status_code=500, detail=AI_CLASSIFICATION_FAILED_MESSAGE) from exc


def _extract_structured_data(text_content: str, document_type_code: str) -> FileProcessResponse:
    try:
        content = build_structured_extraction(text_content, document_type_code, ai_service=ai_service)
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
    text_content = _extract_text_from_file(file_bytes, file_type, file.filename or "")
    if not text_content or not text_content.strip():
        raise HTTPException(status_code=400, detail=NO_TEXT_EXTRACTED_MESSAGE)

    document_type_code = _resolve_document_type_code(text_content, documentType)
    logger.info(
        "Resolved document type for %s: %s (%s)",
        file.filename,
        document_type_code,
        get_document_display_name(document_type_code),
    )
    return _extract_structured_data(text_content, document_type_code)
