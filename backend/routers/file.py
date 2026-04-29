"""
File processing router for the Upload page.

Supports PDF / image / DOCX / XLSX extraction and structured parsing.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import sys
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from PIL import Image, ImageEnhance, ImageOps

desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from backend.celery_app import TASK_QUEUE_ENABLED
from backend.document_types import get_document_display_name, get_document_storage_label, normalize_document_type_code
from backend.routers.chat_helpers import extract_customer_name as extract_customer_name_from_content
from backend.routers.chat_storage import _save_to_local_storage
from backend.services import get_storage_service, supports_structured_storage
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code
from backend.services.job_display_config import build_job_result_summary, get_job_target_page, get_job_type_label
from backend.services.index_rebuild_service import IndexRebuildService
from backend.services.markdown_profile_service import regenerate_customer_profile
from backend.services.profile_sync_service import ProfileSyncService
from backend.services.sqlalchemy_storage_service import SQLAlchemyStorageService
from services.ai_service import AIService, AIServiceError
from services.file_service import FileService
from services.ocr_service import OCRService, OCRServiceError

from ..middleware.auth import get_current_user
from ..models.schemas import ChatJobCreateResponse, ChatJobStatusResponse, FileProcessResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/file", tags=["File Processing"])

file_service = FileService()
ocr_service = OCRService()
ai_service = AIService()
storage_service = get_storage_service()
job_storage_service = storage_service if all(
    hasattr(storage_service, method_name)
    for method_name in ("create_async_job", "get_async_job", "update_async_job", "get_async_job_execution_payload", "mark_async_job_dispatched")
) else SQLAlchemyStorageService()
profile_sync_service = ProfileSyncService()
index_rebuild_service = IndexRebuildService()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
HAS_ASYNC_JOB_STORAGE = all(
    hasattr(job_storage_service, method_name)
    for method_name in ("create_async_job", "get_async_job", "update_async_job", "get_async_job_execution_payload", "mark_async_job_dispatched")
)
_ACTIVE_FILE_PROCESS_JOB_TASKS: set[asyncio.Task[None]] = set()
_UPLOAD_JOB_TEMP_ROOT = Path(__file__).parent.parent.parent / "data" / "upload_job_files"
FILE_PROCESS_JOB_TYPE = "file_process"

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
CUSTOMER_NAME_UNRESOLVED_MESSAGE = "未能从资料中识别客户名称，请手动选择客户或填写客户名称后重试。"

_CUSTOMER_NAME_FIELDS = (
    "company_name",
    "enterprise_name",
    "customer_name",
    "report_subject_name",
    "被查询企业名称",
    "企业名称",
    "公司名称",
    "名称",
    "name",
    "person_name",
    "borrower_name",
    "姓名",
)

_RAW_TEXT_CUSTOMER_NAME_PATTERNS = (
    r"企业名称[：:\s]*([^\n\r，,；;。]{2,80})",
    r"被查询者名称[：:\s]*([^\n\r，,；;。]{2,80})",
    r"报告主体[：:\s]*([^\n\r，,；;。]{2,80})",
    r"客户名称[：:\s]*([^\n\r，,；;。]{2,80})",
    r"公司名称[：:\s]*([^\n\r，,；;。]{2,80})",
    r"名称[：:\s]*([^\n\r，,；;。]{2,80})",
    r"姓名[：:\s]*([^\n\r，,；;。]{2,20})",
)

_INVALID_CUSTOMER_NAME_VALUES = {"", "未识别", "暂无", "无", "-", "null", "none", "未知"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _update_file_process_job(job_id: str, **updates: Any) -> None:
    await job_storage_service.update_async_job(job_id, updates)
    if "status" in updates:
        logger.info(
            "[File Job] job status updated job_id=%s status=%s progress=%s finished_at=%s",
            job_id,
            updates.get("status") or "",
            updates.get("progress_message") or "",
            updates.get("finished_at") or "",
        )


async def _update_file_process_progress(job_id: str, message: str) -> None:
    await _update_file_process_job(job_id, status="running", progress_message=message)


def _normalize_file_process_job_status(job: dict[str, Any]) -> str:
    raw_status = str(job.get("status") or "").strip().lower() or "pending"
    finished_at = str(job.get("finished_at") or "").strip()
    error_message = str(job.get("error_message") or "").strip()
    result_payload = job.get("result_json") if isinstance(job.get("result_json"), dict) else None

    if raw_status == "submitted":
        raw_status = "running"

    if finished_at:
        if error_message:
            return "failed"
        if result_payload:
            return "success"

    if raw_status in {"pending", "running", "retrying", "success", "failed", "timeout", "interrupted"}:
        return raw_status
    return "pending"


def _clean_customer_name_candidate(value: Any) -> str:
    if value is None:
        return ""
    candidate = str(value).strip().strip("：:，,；;。 \t\r\n")
    candidate = re.sub(r"^(企业名称|公司名称|客户名称|名称|姓名|被查询者名称|报告主体)[：:\s]*", "", candidate)
    candidate = re.split(r"[\r\n]", candidate, maxsplit=1)[0].strip().strip("：:，,；;。 ")
    if not candidate or candidate.lower() in _INVALID_CUSTOMER_NAME_VALUES:
        return ""
    if len(candidate) > 80:
        return ""
    return candidate


def _find_customer_name_in_content(content: Any) -> str:
    if not isinstance(content, dict):
        return ""

    for field in _CUSTOMER_NAME_FIELDS:
        value = content.get(field)
        candidate = _clean_customer_name_candidate(value)
        if candidate:
            return candidate

    for value in content.values():
        if isinstance(value, dict):
            candidate = _find_customer_name_in_content(value)
            if candidate:
                return candidate
        elif isinstance(value, list):
            for item in value:
                candidate = _find_customer_name_in_content(item)
                if candidate:
                    return candidate
    return ""


def _find_customer_name_in_raw_text(text: str) -> str:
    if not text:
        return ""
    for pattern in _RAW_TEXT_CUSTOMER_NAME_PATTERNS:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = _clean_customer_name_candidate(match.group(1))
        if candidate:
            return candidate
    return ""


def _resolve_customer_name_after_extraction(
    requested_customer_name: str,
    process_result: FileProcessResponse,
) -> str:
    requested = _clean_customer_name_candidate(requested_customer_name)
    if requested:
        return requested

    extracted = _clean_customer_name_candidate(process_result.customerName)
    if extracted:
        return extracted

    content = process_result.content if isinstance(process_result.content, dict) else {}
    extracted = _find_customer_name_in_content(content)
    if extracted:
        return extracted

    raw_text = str(content.get("raw_text") or "")
    extracted = _find_customer_name_in_raw_text(raw_text)
    if extracted:
        return extracted

    raw_pages = content.get("raw_pages")
    if isinstance(raw_pages, list):
        joined_pages = "\n".join(str(page.get("text") or "") for page in raw_pages if isinstance(page, dict))
        extracted = _find_customer_name_in_raw_text(joined_pages)
        if extracted:
            return extracted
    return ""


async def _get_customer_name_by_id(customer_id: str) -> str:
    if not customer_id:
        return ""
    try:
        customer = await storage_service.get_customer(customer_id)
    except Exception as exc:
        logger.warning("[File Job] failed to lookup customer by id=%s: %s", customer_id, exc)
        return ""
    if isinstance(customer, dict):
        return str(customer.get("name") or customer.get("customer_name") or "").strip()
    return ""


async def _customer_exists_by_name(customer_name: str) -> bool:
    normalized = _clean_customer_name_candidate(customer_name)
    if not normalized:
        return False
    try:
        customers = await storage_service.list_customers()
    except Exception as exc:
        logger.warning("[File Job] failed to list customers for auto-bind check: %s", exc)
        return False
    for customer in customers:
        if str(customer.get("name") or customer.get("customer_name") or "").strip() == normalized:
            return True
    return False


def _ensure_upload_job_temp_dir(job_id: str) -> Path:
    target_dir = _UPLOAD_JOB_TEMP_ROOT / job_id
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def _persist_upload_job_temp_file(job_id: str, filename: str, file_bytes: bytes) -> Path:
    target_dir = _ensure_upload_job_temp_dir(job_id)
    safe_name = Path(filename or "uploaded_file").name
    temp_path = target_dir / safe_name
    temp_path.write_bytes(file_bytes)
    return temp_path


def _cleanup_upload_job_temp_dir(job_id: str) -> None:
    target_dir = _UPLOAD_JOB_TEMP_ROOT / job_id
    shutil.rmtree(target_dir, ignore_errors=True)


def _build_file_process_job_request_snapshot(
    *,
    document_type: str,
    customer_id: str,
    customer_name: str,
    username: str,
    original_filename: str,
    file_size: int,
) -> dict[str, Any]:
    return {
        "jobType": FILE_PROCESS_JOB_TYPE,
        "customerId": customer_id,
        "customerName": customer_name,
        "username": username,
        "files": [
            {
                "fileName": original_filename,
                "size": file_size,
                "documentType": document_type,
            }
        ],
        "createdFrom": "upload_page_async_job",
    }


def _build_file_process_job_execution_payload(
    *,
    job_id: str,
    temp_file_path: str,
    original_filename: str,
    document_type: str,
    customer_id: str,
    customer_name: str,
    username: str,
    role: str,
    file_size: int,
) -> dict[str, Any]:
    return {
        "jobId": job_id,
        "jobType": FILE_PROCESS_JOB_TYPE,
        "tempFilePath": temp_file_path,
        "originalFilename": original_filename,
        "documentType": document_type,
        "customerId": customer_id,
        "customerName": customer_name,
        "username": username,
        "role": role,
        "fileSize": file_size,
        "createdFrom": "upload_page_async_job",
    }


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


def _build_raw_text_from_pages(raw_pages: list[dict[str, Any]]) -> str:
    return "\n\n".join(
        f"--- 第 {int(item.get('page') or 0)} 页 ---\n{str(item.get('text') or '').strip()}"
        for item in raw_pages
        if str(item.get("text") or "").strip()
    )


def _ocr_pdf_pages(file_bytes: bytes) -> tuple[str, list[dict[str, Any]]]:
    images = file_service.pdf_to_images(file_bytes)
    if not images:
        raise HTTPException(status_code=400, detail=PDF_TO_IMAGE_FAILED_MESSAGE)

    raw_pages: list[dict[str, Any]] = []
    for index, img_bytes in enumerate(images, start=1):
        compressed = file_service.compress_image(img_bytes)
        try:
            page_text = ocr_service.recognize_image(compressed)
            raw_pages.append({"page": index, "text": page_text})
        except OCRServiceError as exc:
            logger.warning("OCR failed for page %s: %s", index, exc)
            raw_pages.append({"page": index, "text": OCR_PAGE_FAILED_PLACEHOLDER})
    return _build_raw_text_from_pages(raw_pages), raw_pages


def _ocr_pdf_selected_pages(file_bytes: bytes, page_indices: list[int], *, log_prefix: str, filename: str) -> str:
    images = file_service.pdf_to_images(file_bytes)
    if not images:
        logger.warning("%s skipped: no rendered PDF images filename=%s", log_prefix, filename)
        return ""

    ocr_results: list[str] = []
    total_pages = len(images)
    for page_index in page_indices:
        if page_index < 0 or page_index >= total_pages:
            continue
        page_number = page_index + 1
        try:
            compressed = file_service.compress_image(images[page_index])
            page_text = ocr_service.recognize_image(compressed).strip()
            logger.info(
                "%s page=%s/%s text=%s",
                log_prefix,
                page_number,
                total_pages,
                page_text[:1000] or "(empty)",
            )
            if page_text:
                ocr_results.append(f"--- OCR Page {page_number} ---\n{page_text}")
        except OCRServiceError as exc:
            logger.warning("%s failed page=%s filename=%s error=%s", log_prefix, page_number, filename, exc)
        except Exception as exc:  # pragma: no cover - best-effort OCR fallback
            logger.warning("%s failed page=%s filename=%s error=%s", log_prefix, page_number, filename, exc)

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


def _ocr_company_articles_front_pages(file_bytes: bytes, file_type: str, filename: str) -> str:
    logger.info("[company_articles] start front-page OCR supplement filename=%s file_type=%s", filename, file_type)
    try:
        if file_type == "pdf":
            return _ocr_pdf_selected_pages(
                file_bytes,
                [0, 1],
                log_prefix="[company_articles] front_page_ocr",
                filename=filename,
            )
        if file_type == "image":
            compressed = file_service.compress_image(file_bytes)
            text = ocr_service.recognize_image(compressed).strip()
            logger.info("[company_articles] image OCR supplement text=%s", text[:1000] or "(empty)")
            return text
        logger.info(
            "[company_articles] front-page OCR supplement skipped: unsupported file_type=%s filename=%s",
            file_type,
            filename,
        )
        return ""
    except OCRServiceError as exc:
        logger.warning("[company_articles] front-page OCR supplement failed for %s: %s", filename, exc)
        return ""
    except Exception as exc:  # pragma: no cover - best-effort OCR fallback
        logger.warning("[company_articles] front-page OCR supplement failed for %s: %s", filename, exc)
        return ""


def _property_certificate_seal_crop_boxes(image_bytes: bytes) -> list[tuple[str, tuple[int, int, int, int]]]:
    with Image.open(BytesIO(image_bytes)) as image:
        width, height = image.size
    return [
        ("bottom_full_35", (0, max(0, int(height * 0.65)), width, height)),
        ("bottom_right_40_35", (max(0, int(width * 0.60)), max(0, int(height * 0.65)), width, height)),
        ("middle_right_45_45", (max(0, int(width * 0.52)), max(0, int(height * 0.48)), width, min(height, int(height * 0.93)))),
        ("bottom_center_45", (max(0, int(width * 0.25)), max(0, int(height * 0.55)), min(width, int(width * 0.95)), height)),
    ]


def _ocr_property_certificate_seal_region(file_bytes: bytes, file_type: str, filename: str) -> str:
    logger.info("[property_certificate] start seal-region extraction filename=%s file_type=%s", filename, file_type)
    try:
        if file_type == "pdf":
            images = file_service.pdf_to_images(file_bytes)
            if not images:
                logger.warning("[property_certificate] seal-region extraction skipped: no rendered PDF images filename=%s", filename)
                return ""
            source_images = images[:2]
        elif file_type == "image":
            source_images = [file_bytes]
        else:
            return ""

        ocr_parts: list[str] = []
        for page_index, source_image in enumerate(source_images, start=1):
            for region_name, box in _property_certificate_seal_crop_boxes(source_image):
                try:
                    seal_region = _crop_image_region(source_image, box)
                    for variant_name, variant_bytes in _build_seal_ocr_variants(seal_region):
                        compressed = file_service.compress_image(variant_bytes)
                        seal_text = ocr_service.recognize_image(compressed).strip()
                        logger.info(
                            "[property_certificate] seal_region_ocr_text page=%s region=%s variant=%s text=%s",
                            page_index,
                            region_name,
                            variant_name,
                            seal_text[:1000] or "(empty)",
                        )
                        if seal_text:
                            ocr_parts.append(
                                f"--- Property Certificate Seal OCR page={page_index} region={region_name} variant={variant_name} box={box} ---\n{seal_text}"
                            )
                except OCRServiceError as exc:
                    logger.warning("[property_certificate] seal region OCR failed page=%s region=%s filename=%s error=%s", page_index, region_name, filename, exc)
                except Exception as exc:  # pragma: no cover - best-effort per crop
                    logger.warning("[property_certificate] seal region crop/OCR failed page=%s region=%s filename=%s error=%s", page_index, region_name, filename, exc)
        return "\n\n".join(ocr_parts)
    except Exception as exc:  # pragma: no cover - best-effort OCR fallback
        logger.warning("[property_certificate] seal region extraction failed filename=%s error=%s", filename, exc)
        return ""


async def _extract_content_from_file(
    file_bytes: bytes,
    file_type: str,
    filename: str,
    *,
    progress_callback: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[str, list[dict], list[dict[str, Any]]]:
    try:
        if file_type == "pdf":
            if progress_callback:
                await progress_callback("正在解析文件")
            extracted = file_service.extract_content(file_bytes, file_type, filename=filename)
            text_content = extracted.get("text", "")
            raw_pages: list[dict[str, Any]] = []
            if not file_service.is_pdf_text_valid(text_content):
                logger.info("PDF text extraction invalid for %s, falling back to OCR", filename)
                if progress_callback:
                    await progress_callback("正在 OCR 识别")
                text_content, raw_pages = _ocr_pdf_pages(file_bytes)
            return text_content, [], raw_pages
        if file_type == "image":
            if progress_callback:
                await progress_callback("正在 OCR 识别")
            compressed = file_service.compress_image(file_bytes)
            text_content = ocr_service.recognize_image(compressed)
            return text_content, [], [{"page": 1, "text": text_content}]
        if file_type == "word":
            if progress_callback:
                await progress_callback("正在解析文件")
            extracted = file_service.extract_content(file_bytes, file_type, filename=filename)
            text_content = extracted.get("text", "")
            if not text_content or not text_content.strip():
                word_images = file_service.extract_word_images(file_bytes)
                if word_images:
                    logger.info("Word text extraction empty for %s, falling back to embedded-image OCR", filename)
                    if progress_callback:
                        await progress_callback("正在 OCR 识别")
                    ocr_parts: list[str] = []
                    for index, image_bytes in enumerate(word_images, start=1):
                        try:
                            compressed = file_service.compress_image(image_bytes)
                            image_text = ocr_service.recognize_image(compressed).strip()
                            if image_text:
                                ocr_parts.append(f"--- DOCX Image {index} ---\n{image_text}")
                        except OCRServiceError as exc:
                            logger.warning("Embedded DOCX image OCR failed for %s image=%s error=%s", filename, index, exc)
                    text_content = "\n\n".join(ocr_parts)
            return text_content, extracted.get("rows", []), []
        if file_type == "excel":
            if progress_callback:
                await progress_callback("正在解析文件")
            extracted = file_service.extract_content(file_bytes, file_type, filename=filename)
            return extracted.get("text", ""), extracted.get("rows", []), []
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


def _extract_structured_data(
    text_content: str,
    document_type_code: str,
    rows: list[dict],
    *,
    raw_pages: list[dict[str, Any]] | None = None,
    filename: str = "",
) -> FileProcessResponse:
    raw_pages = raw_pages or []

    def _fallback_content(exc: Exception) -> FileProcessResponse:
        logger.exception(
            "[File Extract] structured extraction failed document_type=%s filename=%s",
            document_type_code,
            filename,
        )
        fallback_content = {
            "document_type_code": document_type_code,
            "document_type_name": get_document_display_name(document_type_code),
            "storage_label": get_document_storage_label(document_type_code),
            "raw_text": _build_raw_text_from_pages(raw_pages) if raw_pages else (text_content or ""),
            "raw_pages": raw_pages,
            "extraction_error": str(exc),
            "extraction_status": "partial_failed",
        }
        return FileProcessResponse(
            documentType=document_type_code,
            content=fallback_content,
            customerName=None,
        )

    try:
        raw_pages_for_log = raw_pages
        if document_type_code in {"property_report", "collateral", "mortgage_info"}:
            logger.info("[property] document_type=%s filename=%s", document_type_code, filename)
            logger.info("[property] raw_pages count=%s", len(raw_pages_for_log))
            logger.info("[property] raw_text preview=%s", (text_content or "")[:2000])
            for item in raw_pages_for_log:
                logger.info("[property] page=%s text=%s", item.get("page"), str(item.get("text") or "")[:1500])
        content = build_structured_extraction(
            text_content,
            document_type_code,
            rows=rows,
            raw_pages=raw_pages,
            filename=filename,
            ai_service=ai_service,
        )
        if document_type_code in {"property_report", "collateral", "mortgage_info"}:
            logger.info("[property] extracted result=%s", content)
        if raw_pages:
            content["raw_pages"] = raw_pages
            content["raw_text"] = _build_raw_text_from_pages(raw_pages)
        elif document_type_code in {"property_report", "collateral", "mortgage_info"} and text_content and text_content.strip():
            content["raw_pages"] = [{"page": 1, "text": text_content}]
            content["raw_text"] = text_content
        elif document_type_code == "marriage_cert" and text_content and text_content.strip():
            content["raw_text"] = text_content
        customer_name = extract_customer_name_from_content(content)
        return FileProcessResponse(
            documentType=document_type_code,
            content=content,
            customerName=customer_name,
        )
    except AIServiceError as exc:
        return _fallback_content(exc)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive wrapper
        return _fallback_content(exc)


async def _process_file_bytes(
    file_bytes: bytes,
    file_type: str,
    filename: str,
    explicit_document_type: str | None,
    *,
    progress_callback: Callable[[str], Awaitable[None]] | None = None,
) -> FileProcessResponse:
    text_content, rows, raw_pages = await _extract_content_from_file(
        file_bytes,
        file_type,
        filename,
        progress_callback=progress_callback,
    )
    if not text_content or not text_content.strip():
        raise HTTPException(status_code=400, detail=NO_TEXT_EXTRACTED_MESSAGE)

    document_type_code = _resolve_document_type_code(text_content, explicit_document_type, rows)
    logger.info(
        "Resolved document type for %s: %s (%s)",
        filename,
        document_type_code,
        get_document_display_name(document_type_code),
    )
    if document_type_code == "business_license":
        seal_region_text = _ocr_business_license_seal_region(file_bytes, file_type, filename)
        if seal_region_text:
            text_content = f"{text_content}\n\n--- Business License Seal Region OCR ---\n{seal_region_text}"
    if document_type_code == "company_articles":
        front_page_ocr_text = _ocr_company_articles_front_pages(file_bytes, file_type, filename)
        if front_page_ocr_text:
            text_content = f"{text_content}\n\n--- Company Articles Front Page OCR ---\n{front_page_ocr_text}"
    if document_type_code in {"property_report", "collateral", "mortgage_info"}:
        seal_region_text = _ocr_property_certificate_seal_region(file_bytes, file_type, filename)
        if seal_region_text:
            text_content = f"{text_content}\n\n--- Property Certificate Seal Region OCR ---\n{seal_region_text}"
            raw_pages.append({"page": len(raw_pages) + 1, "text": seal_region_text})
    if progress_callback:
        await progress_callback("正在结构化提取")
    return _extract_structured_data(text_content, document_type_code, rows, raw_pages=raw_pages, filename=filename)


async def _run_file_process_job(
    job_id: str,
    execution_payload: dict[str, Any],
) -> None:
    temp_file_path = str(execution_payload.get("tempFilePath") or "").strip()
    original_filename = str(execution_payload.get("originalFilename") or "").strip()
    explicit_document_type = str(execution_payload.get("documentType") or "").strip()
    requested_customer_name = str(execution_payload.get("customerName") or "").strip()
    requested_customer_id = str(execution_payload.get("customerId") or "").strip()
    current_user_payload = {
        "username": str(execution_payload.get("username") or "").strip(),
        "role": str(execution_payload.get("role") or "").strip(),
    }

    if not temp_file_path:
        raise ValueError(f"file process job {job_id} missing tempFilePath")

    temp_path = Path(temp_file_path)
    if not temp_path.exists():
        raise FileNotFoundError(f"temp upload file not found: {temp_file_path}")

    await _update_file_process_job(
        job_id,
        status="running",
        progress_message="文件已接收，等待处理",
        started_at=_utc_now_iso(),
        error_message="",
    )

    try:
        file_bytes = temp_path.read_bytes()
        file_type = file_service.get_file_type(original_filename)
        if file_type == "unknown":
            raise HTTPException(status_code=400, detail=UNSUPPORTED_FILE_FORMAT_MESSAGE)

        process_result = await _process_file_bytes(
            file_bytes,
            file_type,
            original_filename,
            explicit_document_type or None,
            progress_callback=lambda message: _update_file_process_progress(job_id, message),
        )

        final_customer_name = _resolve_customer_name_after_extraction(requested_customer_name, process_result)
        if requested_customer_id and not final_customer_name:
            final_customer_name = await _get_customer_name_by_id(requested_customer_id)
        if not final_customer_name:
            raise ValueError(CUSTOMER_NAME_UNRESOLVED_MESSAGE)

        existing_customer_before_save = bool(requested_customer_id) or await _customer_exists_by_name(final_customer_name)

        await _update_file_process_progress(job_id, "正在保存资料")
        save_result = await _save_to_local_storage(
            original_filename or f"{process_result.documentType}.json",
            process_result.documentType,
            process_result.content,
            final_customer_name,
            current_user_payload,
            storage_service,
            target_customer_id=requested_customer_id or None,
            file_bytes=file_bytes,
        )
        success = bool(save_result[0]) if len(save_result) > 0 else False
        record_id = save_result[1] if len(save_result) > 1 else None
        error_msg = save_result[2] if len(save_result) > 2 else None
        saved_customer_id = save_result[4] if len(save_result) > 4 else None
        document_id = save_result[5] if len(save_result) > 5 else None
        original_available = bool(save_result[6]) if len(save_result) > 6 else False
        final_customer_id = requested_customer_id or saved_customer_id or ""
        if not success:
            raise RuntimeError(error_msg or "资料保存失败")
        customer_auto_created = not requested_customer_id and not existing_customer_before_save and bool(final_customer_id)

        if final_customer_id:
            await _update_file_process_progress(job_id, "正在刷新资料汇总")
            await regenerate_customer_profile(storage_service, final_customer_id)
            await _update_file_process_progress(job_id, "正在重建检索索引")
            await index_rebuild_service.rebuild_customer_index(storage_service, final_customer_id, "document_saved")
            await profile_sync_service.mark_customer_applications_stale(storage_service, final_customer_id)

        result_payload = {
            "documentType": process_result.documentType,
            "content": process_result.content,
            "customerName": final_customer_name,
            "resolvedCustomerId": final_customer_id,
            "resolvedCustomerName": final_customer_name,
            "customerAutoCreated": customer_auto_created,
            "savedToFeishu": True,
            "recordId": record_id,
            "customerId": final_customer_id,
            "documentId": document_id,
            "originalAvailable": original_available,
        }
        partial_failed = process_result.content.get("extraction_status") == "partial_failed"
        await _update_file_process_job(
            job_id,
            status="success",
            customer_id=final_customer_id,
            progress_message="上传已保存，结构化提取部分失败" if partial_failed else "处理完成",
            result_json=result_payload,
            error_message="",
            finished_at=_utc_now_iso(),
        )
    except Exception as exc:
        logger.error("[File Job] failed job_id=%s error=%s", job_id, exc, exc_info=True)
        await _update_file_process_job(
            job_id,
            status="failed",
            progress_message="处理失败",
            error_message=str(exc) or "文件处理任务执行失败",
            finished_at=_utc_now_iso(),
        )
        raise
    finally:
        _cleanup_upload_job_temp_dir(job_id)


async def execute_file_process_job_from_job(job_id: str) -> None:
    execution_payload = await job_storage_service.get_async_job_execution_payload(job_id)
    if not execution_payload:
        raise ValueError(f"async job {job_id} execution payload not found")
    await _run_file_process_job(job_id, execution_payload)


def _launch_file_process_job(job_id: str) -> None:
    task = asyncio.create_task(execute_file_process_job_from_job(job_id))
    _ACTIVE_FILE_PROCESS_JOB_TASKS.add(task)

    def _cleanup(done_task: asyncio.Task[None]) -> None:
        _ACTIVE_FILE_PROCESS_JOB_TASKS.discard(done_task)
        try:
            done_task.result()
        except Exception:
            logger.exception("[File Job] background task crashed job_id=%s", job_id)

    task.add_done_callback(_cleanup)


async def _dispatch_file_process_job(
    job_id: str,
    current_user_payload: dict[str, Any],
    customer_id: str,
) -> None:
    logger.info(
        "[File Job] submit start job_id=%s queue_enabled=%s customer_id=%s username=%s",
        job_id,
        TASK_QUEUE_ENABLED,
        customer_id,
        current_user_payload.get("username") or "",
    )
    if TASK_QUEUE_ENABLED:
        from backend.celery_app import FILE_PROCESS_TASK_NAME, HEAVY_QUEUE_NAME, celery_app

        async_result = celery_app.send_task(FILE_PROCESS_TASK_NAME, args=[job_id], queue=HEAVY_QUEUE_NAME)
        await job_storage_service.mark_async_job_dispatched(
            job_id,
            async_result.id,
            worker_name="celery",
        )
        logger.info(
            "[File Job] dispatched to celery job_id=%s celery_task_id=%s customer_id=%s username=%s",
            job_id,
            async_result.id,
            customer_id,
            current_user_payload.get("username") or "",
        )
        return

    logger.warning(
        "[File Job] fallback to in-process execution job_id=%s customer_id=%s username=%s",
        job_id,
        customer_id,
        current_user_payload.get("username") or "",
    )
    _launch_file_process_job(job_id)


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
    return await _process_file_bytes(
        file_bytes,
        file_type,
        file.filename or "",
        documentType,
    )


@router.post("/process/jobs", response_model=ChatJobCreateResponse)
async def create_file_process_job(
    file: UploadFile = File(...),
    documentType: str | None = Form(default=None),
    customerId: str | None = Form(default=None),
    customerName: str | None = Form(default=None),
    current_user: dict = Depends(get_current_user),
) -> JSONResponse:
    if not HAS_DB_STORAGE or not HAS_ASYNC_JOB_STORAGE:
        raise HTTPException(status_code=503, detail="当前环境不支持上传异步任务，请切换到本地数据库存储。")

    file_bytes, _ = await _validate_and_read_file(file)
    job_id = uuid.uuid4().hex
    username = current_user.get("username") or "anonymous"
    role = current_user.get("role") or ""
    temp_file_path = _persist_upload_job_temp_file(job_id, file.filename or "uploaded_file", file_bytes)
    request_payload = _build_file_process_job_request_snapshot(
        document_type=documentType or "",
        customer_id=customerId or "",
        customer_name=customerName or "",
        username=username,
        original_filename=file.filename or "uploaded_file",
        file_size=len(file_bytes),
    )
    execution_payload = _build_file_process_job_execution_payload(
        job_id=job_id,
        temp_file_path=str(temp_file_path),
        original_filename=file.filename or "uploaded_file",
        document_type=documentType or "",
        customer_id=customerId or "",
        customer_name=customerName or "",
        username=username,
        role=role,
        file_size=len(file_bytes),
    )

    await job_storage_service.create_async_job(
        {
            "job_id": job_id,
            "job_type": FILE_PROCESS_JOB_TYPE,
            "customer_id": customerId or "",
            "username": username,
            "status": "pending",
            "progress_message": "文件已接收，等待处理",
            "request_json": request_payload,
            "execution_payload_json": execution_payload,
        }
    )
    try:
        await _dispatch_file_process_job(job_id, {"username": username, "role": role}, customerId or "")
    except Exception as exc:
        _cleanup_upload_job_temp_dir(job_id)
        await job_storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "任务派发失败",
                "error_message": str(exc) or "上传处理任务派发失败",
                "finished_at": _utc_now_iso(),
            },
        )
        raise HTTPException(status_code=500, detail="上传处理任务创建失败，请稍后重试。") from exc

    return JSONResponse(content={"jobId": job_id, "status": "pending"})


@router.get("/process/jobs/{job_id}", response_model=ChatJobStatusResponse)
async def get_file_process_job(
    job_id: str,
    current_user: dict = Depends(get_current_user),
) -> ChatJobStatusResponse:
    job = await job_storage_service.get_async_job(job_id)
    if not job or (job.get("job_type") or "") != FILE_PROCESS_JOB_TYPE:
        raise HTTPException(status_code=404, detail="未找到该上传处理任务")

    username = current_user.get("username") or "anonymous"
    if job.get("username") and job.get("username") != username:
        raise HTTPException(status_code=403, detail="无权查看该上传处理任务")

    result_payload = job.get("result_json") if isinstance(job.get("result_json"), dict) else None
    job_type = job.get("job_type") or FILE_PROCESS_JOB_TYPE
    customer_name = job.get("customer_name") or ""
    normalized_status = _normalize_file_process_job_status(job)
    progress_message = job.get("progress_message") or ""
    if normalized_status == "success" and not progress_message:
        progress_message = "处理完成"
    elif normalized_status == "failed" and not progress_message:
        progress_message = "处理失败"

    return ChatJobStatusResponse(
        jobId=job.get("job_id") or job_id,
        jobType=job_type,
        jobTypeLabel=get_job_type_label(job_type),
        customerId=job.get("customer_id") or "",
        customerName=customer_name,
        status=normalized_status,
        progressMessage=progress_message,
        result=result_payload,
        errorMessage=job.get("error_message") or None,
        createdAt=job.get("created_at") or "",
        startedAt=job.get("started_at") or "",
        finishedAt=job.get("finished_at") or "",
        targetPage=get_job_target_page(job_type),
        resultSummary=build_job_result_summary(job_type, result_payload, customer_name),
    )
