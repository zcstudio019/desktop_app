"""
File Processing Router

Handles file upload and processing for document extraction.
Supports PDF, image, and Excel files.
"""

import logging
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import DATA_TYPE_CONFIG
from backend.routers.chat_helpers import extract_customer_name as extract_customer_name_from_content
from prompts import get_prompt_for_type, load_prompts
from services.ai_service import AIService, AIServiceError
from services.file_service import FileService
from services.ocr_service import OCRService, OCRServiceError
from utils.json_parser import parse_json

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
UNSUPPORTED_FILE_FORMAT_MESSAGE = "文件格式不支持，仅支持 PDF、PNG、JPG、JPEG、XLSX、XLS。"
UNSUPPORTED_FILE_TYPE_MESSAGE = "文件类型不支持，请重新上传后再试。"
OCR_FAILED_MESSAGE = "文件识别失败，请检查文件清晰度后重试。"
FILE_PROCESS_FAILED_MESSAGE = "文件处理失败，请稍后重试。"
NO_TEXT_EXTRACTED_MESSAGE = "未能从文件中提取有效内容，请检查文件是否可读。"
PDF_TO_IMAGE_FAILED_MESSAGE = "PDF 转图片失败，无法继续识别。"
AI_CLASSIFICATION_FAILED_MESSAGE = "文件类型识别失败，请手动选择资料类型后重试。"
AI_EXTRACTION_FAILED_MESSAGE = "资料提取失败，请稍后重试。"
OCR_PAGE_FAILED_PLACEHOLDER = "[本页识别失败]"
UNKNOWN_DOCUMENT_TYPE = "未知"

DOCUMENT_TYPE_MAPPING = {
    "个人征信": "个人征信提取",
    "企业征信": "企业征信提取",
    "个人流水": "个人流水提取",
    "企业流水": "企业流水提取",
    "财务数据": "财务数据提取",
    "抵押物信息": "抵押物信息提取",
    "水母报告": "水母报告提取",
    "个人纳税/公积金": "个人收入纳税/公积金",
}

FALLBACK_EXTRACTION_PROMPT = """请从以下内容中提取关键信息，并以 JSON 格式返回。
提取所有可识别的字段，包括但不限于：
- 企业名称或姓名
- 日期
- 金额
- 其他关键数据

只返回 JSON，不要包含其他说明。"""


def extract_customer_name(content: dict) -> str | None:
    """Backward-compatible wrapper around the shared customer-name extractor."""
    return extract_customer_name_from_content(content)


async def _validate_and_read_file(file: UploadFile) -> tuple[bytes, str]:
    """Validate the uploaded file and return its bytes and detected type."""
    if not file.filename:
        raise HTTPException(status_code=400, detail=NO_FILENAME_MESSAGE)

    try:
        file_bytes = await file.read()
    except Exception as exc:
        logger.error("Failed to read file: %s", exc)
        raise HTTPException(status_code=400, detail=FILE_READ_FAILED_MESSAGE) from exc

    if not file_bytes:
        raise HTTPException(status_code=400, detail=EMPTY_FILE_MESSAGE)

    if not file_service.validate_file_size(file_bytes):
        raise HTTPException(status_code=400, detail=FILE_TOO_LARGE_MESSAGE)

    file_type = file_service.get_file_type(file.filename)
    logger.info("Detected file type: %s", file_type)

    if file_type == "unknown":
        raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_FORMAT_MESSAGE)

    return file_bytes, file_type


def _extract_text_from_file(file_bytes: bytes, file_type: str, filename: str) -> str:
    """Extract text content from file bytes based on file type."""
    text_content = ""

    try:
        if file_type == "pdf":
            text_content = file_service.read_pdf_text(file_bytes)
            if not file_service.is_pdf_text_valid(text_content):
                logger.info("PDF text extraction invalid for %s, falling back to OCR", filename)
                text_content = _ocr_pdf_pages(file_bytes)
        elif file_type == "image":
            compressed = file_service.compress_image(file_bytes)
            text_content = ocr_service.recognize_image(compressed)
        elif file_type == "excel":
            text_content = file_service.read_excel(file_bytes)
        else:
            raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_TYPE_MESSAGE)
    except OCRServiceError as exc:
        logger.error("OCR service error while processing %s: %s", filename, exc)
        raise HTTPException(status_code=400, detail=OCR_FAILED_MESSAGE) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("File processing error for %s: %s", filename, exc)
        raise HTTPException(status_code=500, detail=FILE_PROCESS_FAILED_MESSAGE) from exc

    if not text_content or not text_content.strip():
        raise HTTPException(status_code=400, detail=NO_TEXT_EXTRACTED_MESSAGE)

    return text_content


def _ocr_pdf_pages(file_bytes: bytes) -> str:
    """Convert PDF to images and OCR each page."""
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


def _classify_document_type(text_content: str, document_type: str | None) -> str:
    """Classify the document type using AI if not already provided."""
    if document_type:
        return document_type

    try:
        classified_type = ai_service.classify(text_content)
        logger.info("AI classified document type: %s", classified_type)

        resolved_type = DOCUMENT_TYPE_MAPPING.get(classified_type, classified_type)
        if resolved_type not in DATA_TYPE_CONFIG:
            logger.warning("Classified type '%s' not in config, falling back", resolved_type)
            for key in DATA_TYPE_CONFIG:
                if classified_type in key or key in classified_type:
                    resolved_type = key
                    break
            else:
                resolved_type = UNKNOWN_DOCUMENT_TYPE

        return resolved_type
    except AIServiceError as exc:
        logger.error("AI classification error: %s", exc)
        raise HTTPException(status_code=500, detail=AI_CLASSIFICATION_FAILED_MESSAGE) from exc


def _extract_structured_data(text_content: str, document_type: str) -> FileProcessResponse:
    """Use AI to extract structured data from text content."""
    try:
        load_prompts()
        prompt = get_prompt_for_type(document_type) or FALLBACK_EXTRACTION_PROMPT
        if prompt == FALLBACK_EXTRACTION_PROMPT:
            logger.warning("No prompt found for document type: %s", document_type)

        ai_result = ai_service.extract(prompt, text_content)
        logger.info("AI extraction completed, result length: %s", len(ai_result) if ai_result else 0)

        content = parse_json(ai_result)
        if content is None:
            logger.warning("Failed to parse AI output as JSON")
            content = {"raw_text": ai_result, "parse_error": True}

        customer_name = extract_customer_name_from_content(content)
        logger.info("Extracted customer name: %s", customer_name)

        return FileProcessResponse(documentType=document_type, content=content, customerName=customer_name)
    except AIServiceError as exc:
        logger.error("AI extraction error: %s", exc)
        raise HTTPException(status_code=500, detail=AI_EXTRACTION_FAILED_MESSAGE) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Unexpected error during extraction: %s", exc)
        raise HTTPException(status_code=500, detail=AI_EXTRACTION_FAILED_MESSAGE) from exc


@router.post("/process", response_model=FileProcessResponse)
async def process_file(
    file: UploadFile = File(..., description="待处理文件，支持 PDF、图片或 Excel"),
    documentType: str | None = Form(
        default=None,
        description="可选资料类型提示，例如“企业征信提取”“个人征信提取”",
    ),
    current_user: dict = Depends(get_current_user),
) -> FileProcessResponse:
    """Process an uploaded file and extract structured data."""
    logger.info("Processing file: %s, documentType: %s, user=%s", file.filename, documentType, current_user["username"])

    file_bytes, file_type = await _validate_and_read_file(file)
    text_content = _extract_text_from_file(file_bytes, file_type, file.filename)
    document_type = _classify_document_type(text_content, documentType)
    return _extract_structured_data(text_content, document_type)
