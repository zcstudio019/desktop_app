"""
Chat Extraction Module

Handles file text extraction, classification, data extraction, merging,
and post-processing. Extracted from chat.py to keep functions under 50 lines.
"""

import base64
import contextlib
import logging
import re
from typing import Any

from prompts import get_prompt_for_type, load_prompts
from services.ai_service import AIService, AIServiceError
from services.file_service import FileService
from services.ocr_service import OCRService, OCRServiceError
from utils.json_parser import parse_json

from ..models.schemas import ChatFile
from .chat_helpers import extract_customer_name, extract_customer_name_from_text
from .chat_storage import MISSING_CUSTOMER_NAME_MESSAGE as STORAGE_MISSING_CUSTOMER_NAME_MESSAGE

logger = logging.getLogger(__name__)

AI_RISK_BLOCKED_ERROR = "ai_risk_blocked"
AI_SERVICE_ERROR = "ai_service_error"
OCR_ERROR = "ocr_error"
PROCESSING_ERROR = "processing_error"

AI_RISK_BLOCKED_MESSAGE = "AI 服务风控拦截，未完成解析。"
AI_GENERIC_FAILURE_MESSAGE = "AI 服务处理失败，未完成解析。"
MISSING_CUSTOMER_NAME_SAVE_MESSAGE = "已提取但未保存：缺少客户名称。请先指定客户或补充企业名称。"
GENERIC_SAVE_FAILURE_MESSAGE = "已提取但未保存：资料入库失败。"

# Services are injected from chat.py at module init
_ai_service: AIService | None = None
_file_service: FileService | None = None
_ocr_service: OCRService | None = None


def init_services(ai_service: AIService, file_service: FileService, ocr_service: OCRService) -> None:
    """Initialize service references. Called once from chat.py.

    Args:
        ai_service: AI service instance
        file_service: File service instance
        ocr_service: OCR service instance
    """
    global _ai_service, _file_service, _ocr_service
    _ai_service = ai_service
    _file_service = file_service
    _ocr_service = ocr_service


def extract_text_from_chat_file(chat_file: ChatFile) -> tuple[str, str]:
    """Extract text content from a chat file.

    Args:
        chat_file: ChatFile object with base64 encoded content

    Returns:
        Tuple of (text_content, file_type).

    Raises:
        OCRServiceError: If OCR processing fails critically
    """
    file_bytes = base64.b64decode(chat_file.content)
    if not file_bytes:
        return ("", "empty")

    file_type = _file_service.get_file_type(chat_file.name)
    if file_type == "unknown":
        return ("", "unknown")

    text_content = _extract_by_type(file_bytes, file_type, chat_file.name)
    return (text_content, file_type)


def _extract_by_type(file_bytes: bytes, file_type: str, filename: str) -> str:
    """Extract text from file bytes based on file type.

    Args:
        file_bytes: Raw file bytes
        file_type: Detected file type ("pdf", "image", "excel")
        filename: Original filename for logging

    Returns:
        Extracted text content
    """
    if file_type == "pdf":
        return _extract_pdf_text(file_bytes, filename)
    elif file_type == "image":
        compressed = _file_service.compress_image(file_bytes)
        return _ocr_service.recognize_image(compressed)
    elif file_type == "excel":
        return _file_service.read_excel(file_bytes)
    return ""


def _extract_pdf_text(file_bytes: bytes, filename: str) -> str:
    """Extract text from PDF, falling back to OCR if needed.

    Args:
        file_bytes: Raw PDF bytes
        filename: Original filename for logging

    Returns:
        Extracted text content
    """
    text_content = _file_service.read_pdf_text(file_bytes)

    if _file_service.is_pdf_text_valid(text_content):
        return text_content

    logger.info(f"PDF text invalid for {filename}, using OCR")
    images = _file_service.pdf_to_images(file_bytes)
    if not images:
        return text_content

    ocr_results = []
    for i, img_bytes in enumerate(images):
        compressed = _file_service.compress_image(img_bytes)
        try:
            page_text = _ocr_service.recognize_image(compressed)
            ocr_results.append(page_text)
        except OCRServiceError as e:
            logger.warning(f"OCR failed for page {i + 1}: {e}")

    return "\n\n".join(ocr_results)


def classify_and_extract_data(text_content: str) -> tuple[str, dict]:
    """Classify document type and extract structured data using AI.

    Args:
        text_content: The extracted text content from a file

    Returns:
        Tuple of (document_type, content_dict)
    """
    classified_type = _ai_service.classify(text_content)

    type_mapping = {
        "个人征信": "个人征信提取",
        "企业征信": "企业征信提取",
        "个人流水": "个人流水提取",
        "企业流水": "企业流水提取",
        "财务数据": "财务数据提取",
        "抵押物信息": "抵押物信息提取",
        "水母报告": "水母报告提取",
        "个人纳税公积金": "个人收入纳税/公积金",
    }
    document_type = type_mapping.get(classified_type, classified_type)

    load_prompts()
    prompt = get_prompt_for_type(document_type)
    if not prompt:
        prompt = """请从以下内容中提取关键信息，以 JSON 格式返回。
提取所有可识别的字段。只返回 JSON，不要其他内容。"""

    ai_result = _ai_service.extract(prompt, text_content)
    content = parse_json(ai_result)
    if content is None:
        content = {"raw_text": ai_result, "parse_error": True}

    return (document_type, content)


def extract_single_chat_file(
    chat_file: ChatFile,
    explicit_customer_name: str | None,
    contextual_customer_name: str | None,
) -> dict:
    """Extract text, classify, and extract data from a single file.

    Args:
        chat_file: ChatFile object with base64 encoded content
        explicit_customer_name: Customer name explicitly specified by user
        contextual_customer_name: Customer name inferred from prior context or cache

    Returns:
        Result dictionary with extraction data

    Raises:
        OCRServiceError: If OCR processing fails
        AIServiceError: If AI processing fails
    """
    text_content, file_type = extract_text_from_chat_file(chat_file)

    if file_type == "empty":
        return {"filename": chat_file.name, "error": "Empty file content"}
    if file_type == "unknown":
        return {"filename": chat_file.name, "error": "Unsupported file format"}
    if not text_content or not text_content.strip():
        return {"filename": chat_file.name, "error": "No text content extracted"}

    document_type, content = classify_and_extract_data(text_content)

    ai_extracted_name = extract_customer_name(content)
    text_extracted_name = None if ai_extracted_name else extract_customer_name_from_text(text_content)
    customer_name = explicit_customer_name or ai_extracted_name or text_extracted_name or contextual_customer_name
    logger.info(
        f"[Customer Name] explicit={explicit_customer_name}, "
        f"contextual={contextual_customer_name}, ai_extracted={ai_extracted_name}, "
        f"text_extracted={text_extracted_name}, "
        f"final={customer_name}"
    )

    return {
        "filename": chat_file.name,
        "documentType": document_type,
        "content": content,
        "customerName": customer_name,
    }


# ===== Merge Helpers =====

def _is_empty_value(v: Any) -> bool:
    """Check if a value is considered empty/placeholder."""
    if v is None:
        return True
    if isinstance(v, str):
        stripped = v.strip()
        return stripped in ("", "无", "暂无", "N/A")
    return False


def _is_placeholder_list(lst: list) -> bool:
    """Check if a list is a placeholder (single item with all empty values)."""
    if len(lst) != 1:
        return False
    item = lst[0]
    if isinstance(item, dict):
        return all(_is_empty_value(v) for v in item.values())
    return _is_empty_value(item)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base dict.

    Merge rules:
    - Non-empty values override empty/"无"/None
    - Nested dicts merge recursively
    - Lists combine non-placeholder items

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    merged = dict(base)
    for key, new_val in override.items():
        if key not in merged:
            merged[key] = new_val
            continue

        old_val = merged[key]

        if isinstance(old_val, dict) and isinstance(new_val, dict):
            merged[key] = _deep_merge(old_val, new_val)
        elif isinstance(old_val, list) and isinstance(new_val, list):
            merged[key] = _merge_lists(old_val, new_val)
        elif (_is_empty_value(old_val) and not _is_empty_value(new_val)) or not _is_empty_value(new_val):
            merged[key] = new_val

    return merged


def _merge_lists(old_val: list, new_val: list) -> list:
    """Merge two lists, handling placeholder detection.

    Args:
        old_val: Original list
        new_val: New list to merge

    Returns:
        Merged list
    """
    old_placeholder = _is_placeholder_list(old_val)
    new_placeholder = _is_placeholder_list(new_val)

    if old_placeholder and not new_placeholder:
        return new_val
    if not old_placeholder and new_placeholder:
        return old_val
    if not old_placeholder and not new_placeholder:
        combined = list(old_val)
        for item in new_val:
            if item not in combined:
                combined.append(item)
        return combined
    return old_val


def merge_contents(contents: list[dict]) -> dict:
    """Deep merge multiple content dicts from the same document type.

    Args:
        contents: List of content dicts to merge

    Returns:
        Merged content dict
    """
    if not contents:
        return {}
    if len(contents) == 1:
        return contents[0]

    result = contents[0]
    for c in contents[1:]:
        result = _deep_merge(result, c)
    return result


def postprocess_extracted_content(content: dict) -> dict:
    """Post-process AI-extracted data: auto-calculate totals, fix formats.

    Currently handles:
    - 纳税信息.近三年纳税总额: auto-sum from yearly amounts if empty/zero

    Args:
        content: Extracted content dictionary

    Returns:
        Post-processed content dictionary
    """
    if not isinstance(content, dict):
        return content

    tax_info = content.get("纳税信息")
    if isinstance(tax_info, dict):
        _recalculate_tax_total(tax_info)

    return content


def _recalculate_tax_total(tax_info: dict) -> None:
    """Recalculate 近三年纳税总额 from yearly amounts if needed.

    Modifies tax_info in place.

    Args:
        tax_info: Tax information dictionary
    """
    total = tax_info.get("近三年纳税总额")

    if not _needs_tax_recalculation(total):
        return

    yearly_amounts = tax_info.get("各年度纳税金额", [])
    if not isinstance(yearly_amounts, list):
        return

    calculated_sum = _sum_yearly_tax(yearly_amounts)
    if calculated_sum > 0:
        tax_info["近三年纳税总额"] = f"{calculated_sum:g}元"
        logger.info(f"[Postprocess] 自动计算近三年纳税总额: {tax_info['近三年纳税总额']}")


def _needs_tax_recalculation(total: Any) -> bool:
    """Check if tax total needs recalculation.

    Args:
        total: Current total value

    Returns:
        True if recalculation is needed
    """
    if total is None or total == "" or total == "无":
        return True
    if isinstance(total, (int, float)) and total == 0:
        return True
    if isinstance(total, str):
        nums = re.findall(r"[\d.]+", str(total).replace(",", ""))
        if not nums or all(float(n) == 0 for n in nums):
            return True
    return False


def _sum_yearly_tax(yearly_amounts: list) -> float:
    """Sum tax amounts from yearly records.

    Args:
        yearly_amounts: List of yearly tax records

    Returns:
        Total sum of valid tax amounts
    """
    calculated_sum = 0.0
    for item in yearly_amounts:
        if not isinstance(item, dict):
            continue
        amount_str = item.get("纳税金额") or "无"
        if amount_str == "无":
            continue
        numbers = re.findall(r"[\d.]+", str(amount_str).replace(",", ""))
        if numbers:
            with contextlib.suppress(ValueError):
                calculated_sum += float(numbers[0])
    return calculated_sum


# ===== File Processing Pipeline =====

def _extract_all_files(
    files: list[ChatFile],
    explicit_customer_name: str | None,
    contextual_customer_name: str | None,
) -> list[dict]:
    """Phase 1: Extract all files (OCR + classify + AI extract).

    Args:
        files: List of ChatFile objects
        explicit_customer_name: Customer name explicitly specified by user
        contextual_customer_name: Customer name inferred from prior context or cache

    Returns:
        List of extraction result dicts
    """
    extractions = []
    for chat_file in files:
        try:
            result = extract_single_chat_file(chat_file, explicit_customer_name, contextual_customer_name)
            extractions.append(result)
        except OCRServiceError as e:
            logger.error(f"OCR error for {chat_file.name}: {e}")
            extractions.append({
                "filename": chat_file.name,
                "errorType": OCR_ERROR,
                "error": f"OCR error: {e!s}",
            })
        except AIServiceError as e:
            logger.error(f"AI error for {chat_file.name}: {e}")
            error_type, error_message = classify_ai_error(e)
            extractions.append({
                "filename": chat_file.name,
                "errorType": error_type,
                "error": error_message,
            })
        except Exception as e:
            logger.error(f"Error processing {chat_file.name}: {e}")
            extractions.append({
                "filename": chat_file.name,
                "errorType": PROCESSING_ERROR,
                "error": f"Processing error: {e!s}",
            })
    return extractions


def classify_ai_error(error: AIServiceError) -> tuple[str, str]:
    """Map provider AI errors to user-facing error types and messages."""
    message = str(error)
    if "Content Exists Risk" in message:
        return AI_RISK_BLOCKED_ERROR, AI_RISK_BLOCKED_MESSAGE
    return AI_SERVICE_ERROR, AI_GENERIC_FAILURE_MESSAGE


def _group_by_doc_type(extractions: list[dict]) -> tuple[dict[str, list[dict]], list[dict]]:
    """Phase 2: Group successful extractions by document type.

    Args:
        extractions: List of extraction results

    Returns:
        Tuple of (type_groups dict, error_results list)
    """
    type_groups: dict[str, list[dict]] = {}
    error_results = []

    for ext in extractions:
        if "error" in ext:
            error_results.append(ext)
            continue
        doc_type = ext.get("documentType", "unknown")
        if doc_type not in type_groups:
            type_groups[doc_type] = []
        type_groups[doc_type].append(ext)

    return type_groups, error_results


def _resolve_group_customer_name(
    group: list[dict], buffered_name: str | None
) -> tuple[str | None, str | None]:
    """Resolve customer name for a document type group.

    Args:
        group: List of extraction results for one doc type
        buffered_name: Previously buffered customer name

    Returns:
        Tuple of (customer_name, updated_buffered_name)
    """
    customer_name = None
    for ext in group:
        name = ext.get("customerName")
        if name:
            customer_name = name
            break

    if not customer_name:
        contents = [ext["content"] for ext in group if ext.get("content")]
        if contents:
            customer_name = extract_customer_name(contents[0])

    if not customer_name and buffered_name:
        customer_name = buffered_name
        logger.info(f"[Customer Name Buffer] Using buffered name: {buffered_name}")

    updated_buffer = buffered_name
    if customer_name and not buffered_name:
        updated_buffer = customer_name
        logger.info(f"[Customer Name Buffer] Buffered name: {customer_name}")

    return customer_name, updated_buffer


def _classify_save_error(error_msg: str | None) -> str | None:
    """Map storage-layer failures to user-friendly chat save errors."""
    if not error_msg:
        return None
    normalized = error_msg.lower()
    if (
        "customer name is required" in normalized
        or error_msg == STORAGE_MISSING_CUSTOMER_NAME_MESSAGE
    ):
        return MISSING_CUSTOMER_NAME_SAVE_MESSAGE
    return GENERIC_SAVE_FAILURE_MESSAGE


async def _process_type_group(
    doc_type: str,
    group: list[dict],
    customer_name: str | None,
    current_user: dict | None,
    save_fn: Any,
    target_customer_id: str | None = None,
) -> list[dict]:
    """Phase 3: Merge contents and save for one document type group.

    Args:
        doc_type: Document type string
        group: List of extraction results
        customer_name: Resolved customer name
        current_user: Current user info
        save_fn: Async function to save to storage
        target_customer_id: If set, merge into this existing customer

    Returns:
        List of result dicts for each file in the group
    """
    from ..services.activity_service import add_activity

    contents = [ext["content"] for ext in group if ext.get("content")]
    merged = merge_contents(contents) if len(contents) > 1 else (contents[0] if contents else {})
    merged = postprocess_extracted_content(merged)

    if len(group) > 1:
        logger.info(
            f"[Multi-file Merge] Merged {len(group)} files of type '{doc_type}', "
            f"filenames: {[ext['filename'] for ext in group]}"
        )

    filenames = [ext["filename"] for ext in group]
    saved, record_id, error_msg, similar_customers, customer_id = await save_fn(
        ", ".join(filenames), doc_type, merged, customer_name, current_user,
        target_customer_id=target_customer_id,
    )
    save_error = None if saved else _classify_save_error(error_msg)

    for ext in group:
        add_activity(
            activity_type="upload", filename=ext["filename"],
            customer=customer_name, status="completed" if saved else "failed", document_type=doc_type,
        )

    results = []
    for i, ext in enumerate(group):
        results.append({
            "filename": ext["filename"],
            "documentType": doc_type,
            "content": merged if i == 0 else ext["content"],
            "customerName": customer_name,
            "customerId": customer_id,
            "savedToFeishu": saved,
            "recordId": record_id,
            "feishuError": error_msg,
            "saveError": save_error,
            "mergedFiles": len(group) if len(group) > 1 else None,
            "similarCustomers": similar_customers if i == 0 else [],
        })

    return results


async def process_chat_files(
    files: list[ChatFile],
    explicit_customer_name: str | None,
    contextual_customer_name: str | None,
    current_user: dict | None,
    save_fn: Any,
    merge_decisions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Process files attached to chat message.

    Three-phase pipeline:
    1. Extract all files (OCR + classify + AI)
    2. Group by document type
    3. Merge + save per type group

    Args:
        files: List of ChatFile objects
        explicit_customer_name: Customer name explicitly specified in current message
        contextual_customer_name: Customer name inferred from prior context or cache
        current_user: Current user info
        save_fn: Async function(filename, doc_type, content, name, user, *, target_customer_id) -> (ok, id, err, similar)
        merge_decisions: Optional dict mapping customerName -> target_customer_id (user's merge choice)

    Returns:
        Dictionary with extraction results. If any file has similarCustomers, caller should prompt user.
    """
    if not files:
        return {"error": "No files provided"}

    extractions = _extract_all_files(files, explicit_customer_name, contextual_customer_name)
    type_groups, error_results = _group_by_doc_type(extractions)

    results = list(error_results)
    buffered_name: str | None = None

    for doc_type, group in type_groups.items():
        customer_name, buffered_name = _resolve_group_customer_name(group, buffered_name)
        # 查找用户是否已做出合并决策
        target_customer_id: str | None = None
        if merge_decisions and customer_name:
            target_customer_id = merge_decisions.get(customer_name)
        group_results = await _process_type_group(
            doc_type, group, customer_name, current_user, save_fn, target_customer_id
        )
        results.extend(group_results)

    return {"files": results}


# ===== Feishu Record Extraction =====

def extract_customer_fields_from_feishu(records: list) -> dict:
    """Extract and flatten customer fields from Feishu records.

    Args:
        records: List of Feishu records

    Returns:
        Flattened customer data dictionary
    """
    if not records:
        return {}

    record = records[0]
    fields = record.get("fields", {})
    if not fields:
        return {}

    customer_data = {}
    for field_name, value in fields.items():
        if field_name in ("record_id", "id"):
            continue
        extracted = _extract_text_value_from_feishu(value)
        if extracted:
            customer_data[field_name] = extracted

    return customer_data


def _extract_text_value_from_feishu(value: Any) -> str:
    """Extract text from Feishu field value (plain/rich text).

    Args:
        value: Feishu field value

    Returns:
        Extracted plain text
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        texts = []
        for item in value:
            if isinstance(item, dict) and "text" in item:
                texts.append(str(item["text"]))
            elif isinstance(item, str):
                texts.append(item)
        return "".join(texts).strip()
    if isinstance(value, dict) and "text" in value:
        return str(value["text"]).strip()
    return str(value).strip()
