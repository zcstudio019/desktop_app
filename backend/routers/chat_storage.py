"""
Chat Storage Module

Handles saving extracted data to storage backends (local SQLite or Feishu).
Extracted from chat.py to keep functions under 50 lines.
"""

import contextlib
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import DATA_TYPE_CONFIG, STORE_ORIGINAL_UPLOAD_FILES

from .feishu import dict_to_markdown

logger = logging.getLogger(__name__)

_LOCAL_UPLOAD_ROOT = Path(__file__).parent.parent.parent / "data" / "uploads"
_PERSONAL_DOC_TYPES = frozenset({
    "个人征信提取",
    "个人流水提取",
    "个人收入纳税/公积金",
})
_ENTERPRISE_NAME_HINTS = ("公司", "企业", "集团", "有限", "股份", "商行", "中心")

MISSING_CUSTOMER_NAME_MESSAGE = "已提取但未保存：缺少客户名称。请先指定客户或补充企业名称。"
LOCAL_SAVE_FAILED_MESSAGE = "已提取但未保存：本地保存失败，请稍后重试。"
LEGACY_SAVE_FAILED_MESSAGE = "资料保存失败，请稍后重试。"
DOCUMENT_TYPE_CONFIG_MISSING_MESSAGE = "资料类型配置缺失，请联系管理员检查配置。"


async def save_to_storage(
    chat_file_name: str,
    document_type: str,
    content: dict,
    customer_name: str | None,
    current_user: dict | None,
    *,
    use_local: bool,
    storage_service: Any,
    feishu_service: Any,
    target_customer_id: str | None = None,
    file_bytes: bytes | None = None,
) -> tuple[bool, str | None, str | None, list[dict], str | None]:
    """Save extracted data to storage (local SQLite or Feishu)."""
    if use_local:
        result = await _save_to_local_storage(
            chat_file_name,
            document_type,
            content,
            customer_name,
            current_user,
            storage_service,
            target_customer_id,
            file_bytes=file_bytes,
        )
        saved, _, _, _, customer_id = result
        if saved and customer_id:
            try:
                from backend.services.profile_sync_service import ProfileSyncService

                await ProfileSyncService().handle_document_saved(storage_service, customer_id)
            except Exception as exc:
                logger.warning(
                    "[Local Save] profile_sync failed customer_id=%s operation_type=document_saved status=failed error=%s",
                    customer_id,
                    exc,
                )
        return result

    saved, record_id, error_msg = _save_to_feishu_legacy(
        chat_file_name,
        document_type,
        content,
        customer_name,
        current_user,
        feishu_service,
    )
    return (saved, record_id, error_msg, [], None)


def _determine_customer_id(customer_name: str, document_type: str) -> tuple[str, str]:
    """Determine customer_id with type prefix based on document type."""
    customer_type = "personal" if document_type in _PERSONAL_DOC_TYPES else "enterprise"
    customer_id = f"{customer_type}_{customer_name}"
    return customer_id, customer_type


def _extract_legal_representative_name(extracted_data: dict[str, Any]) -> str | None:
    """Extract legal representative name from enterprise extraction content."""
    candidate_paths = (
        ("法定代表人信息", "姓名"),
        ("法定代表人信息", "名称"),
        ("法定代表人", None),
        ("法人姓名", None),
        ("法人代表", None),
        ("企业法人信息", "法人姓名"),
    )
    for top_key, nested_key in candidate_paths:
        value = extracted_data.get(top_key)
        if nested_key is None:
            if isinstance(value, str) and value.strip():
                return value.strip()
            continue
        if isinstance(value, dict):
            nested_value = value.get(nested_key)
            if isinstance(nested_value, str) and nested_value.strip():
                return nested_value.strip()
    return None


def _looks_like_person_name(customer_name: str) -> bool:
    """Return True when the extracted customer name looks like a natural person."""
    stripped = (customer_name or "").strip()
    if not stripped:
        return False
    if any(hint in stripped for hint in _ENTERPRISE_NAME_HINTS):
        return False
    return len(stripped) <= 4


async def _find_enterprise_customer_by_legal_rep(
    storage_service: Any,
    customer_name: str,
) -> str | None:
    """Return enterprise customer_id when the personal name matches a legal representative."""
    try:
        customers = await storage_service.list_customers()
    except Exception as exc:
        logger.warning("[Local Save] Failed to load customers for legal representative matching: %s", exc)
        return None

    for customer in customers:
        customer_id = customer.get("customer_id") or ""
        if not customer_id or customer.get("customer_type") == "personal":
            continue
        try:
            extractions = await storage_service.get_extractions_by_customer(customer_id)
        except Exception as exc:
            logger.warning("[Local Save] Failed to load extractions for %s: %s", customer_id, exc)
            continue

        for extraction in extractions:
            extracted_data = extraction.get("extracted_data") or {}
            if not isinstance(extracted_data, dict):
                continue
            if _extract_legal_representative_name(extracted_data) == customer_name:
                logger.info(
                    "[Local Save] Matched personal document '%s' to enterprise customer '%s'",
                    customer_name,
                    customer_id,
                )
                return customer_id
    return None


async def _resolve_customer_target(
    storage_service: Any,
    customer_name: str,
    document_type: str,
) -> tuple[str, str]:
    """Resolve save target to an existing or new customer."""
    customer_id, customer_type = _determine_customer_id(customer_name, document_type)
    existing_customer = None
    with contextlib.suppress(Exception):
        existing_customer = await storage_service.get_customer(customer_id)
    if existing_customer:
        return customer_id, customer_type

    if customer_type == "personal" or _looks_like_person_name(customer_name):
        enterprise_customer_id = await _find_enterprise_customer_by_legal_rep(storage_service, customer_name)
        if enterprise_customer_id:
            return enterprise_customer_id, "enterprise"

    return customer_id, customer_type


async def _ensure_customer_exists(
    storage_service: Any,
    customer_id: str,
    customer_name: str,
    customer_type: str,
    current_user: dict | None,
) -> None:
    """Ensure customer record exists in local storage, create if not."""
    existing = None
    with contextlib.suppress(Exception):
        existing = await storage_service.get_customer(customer_id)

    if not existing:
        customer_data = {
            "customer_id": customer_id,
            "name": customer_name,
            "status": "new",
            "customer_type": customer_type,
            "uploader": (current_user.get("username") or "") if current_user else "",
            "upload_time": datetime.now(tz=timezone.utc).strftime("%Y/%m/%d"),
        }
        try:
            await storage_service.create_customer(customer_data)
            logger.info("[Local Save] Created customer: %s", customer_name)
        except Exception as exc:
            logger.warning("[Local Save] Customer may already exist: %s", exc)
    else:
        try:
            await storage_service.update_customer(
                customer_id,
                {"upload_time": datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")},
            )
        except Exception as exc:
            logger.warning("[Local Save] Failed to update upload_time: %s", exc)


def _save_file_to_disk(customer_id: str, file_name: str, file_bytes: bytes | None) -> tuple[str, int]:
    """Persist the original uploaded file only when raw-file storage is enabled."""
    if not file_bytes:
        return ("", 0)

    file_size = len(file_bytes)
    if not STORE_ORIGINAL_UPLOAD_FILES:
        logger.info("[Local Save] Skipped raw file persistence for %s", file_name)
        return ("", file_size)

    safe_name = Path(file_name or "uploaded_file").name
    safe_customer_dir = hashlib.sha1(customer_id.encode("utf-8")).hexdigest()[:16]
    relative_path = Path("uploads") / safe_customer_dir / safe_name
    target_path = _LOCAL_UPLOAD_ROOT / safe_customer_dir / safe_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(file_bytes)
    return (relative_path.as_posix(), file_size)


async def _save_doc_and_extraction(
    storage_service: Any,
    customer_id: str,
    chat_file_name: str,
    document_type: str,
    content: dict,
    file_bytes: bytes | None = None,
) -> str:
    """Save document metadata and extraction result to local storage."""
    doc_id = str(uuid.uuid4())
    extraction_id = str(uuid.uuid4())

    file_path, file_size = _save_file_to_disk(customer_id, chat_file_name, file_bytes)
    doc_data = {
        "doc_id": doc_id,
        "customer_id": customer_id,
        "file_name": chat_file_name,
        "file_path": file_path,
        "file_type": document_type,
        "file_size": file_size,
    }
    await storage_service.save_document(doc_data)
    logger.info("[Local Save] Saved document: %s", doc_id)

    confidence = content.get("confidence", 0.0) if isinstance(content.get("confidence"), (int, float)) else 0.0
    extraction_data = {
        "extraction_id": extraction_id,
        "doc_id": doc_id,
        "customer_id": customer_id,
        "extraction_type": document_type,
        "extracted_data": content,
        "confidence": confidence,
    }
    await storage_service.save_extraction(extraction_data)
    logger.info("[Local Save] Saved extraction: %s", extraction_id)

    return extraction_id


async def _replace_existing_documents_of_same_type(
    storage_service: Any,
    customer_id: str,
    document_type: str,
) -> None:
    """Delete older documents of the same type before saving the new one.

    The product rule is: for a given customer, the latest upload of the same
    document type should replace earlier uploads. This keeps summary views and
    downstream application generation aligned with the newest material.
    """
    existing_documents = await storage_service.list_documents(customer_id)
    replaced_count = 0

    for document in existing_documents:
        if (document.get("file_type") or "") != document_type:
            continue
        doc_id = document.get("doc_id")
        if not doc_id:
            continue
        try:
            deleted = await storage_service.delete_document(doc_id)
        except Exception as exc:
            logger.error(
                "[Local Save] Failed to replace existing document customer_id=%s document_type=%s old_doc_id=%s error=%s",
                customer_id,
                document_type,
                doc_id,
                exc,
                exc_info=True,
            )
            raise RuntimeError(f"替换旧资料失败，无法删除旧文档：{doc_id}") from exc

        if not deleted:
            logger.warning(
                "[Local Save] Existing document disappeared during replacement customer_id=%s document_type=%s old_doc_id=%s",
                customer_id,
                document_type,
                doc_id,
            )
            raise RuntimeError(f"替换旧资料失败，未找到旧文档：{doc_id}")

        replaced_count += 1

    if replaced_count:
        logger.info(
            "[Local Save] Replaced %s existing document(s) for customer=%s, type=%s",
            replaced_count,
            customer_id,
            document_type,
        )


async def _save_to_local_storage(
    chat_file_name: str,
    document_type: str,
    content: dict,
    customer_name: str | None,
    current_user: dict | None,
    storage_service: Any,
    target_customer_id: str | None = None,
    file_bytes: bytes | None = None,
) -> tuple[bool, str | None, str | None, list[dict], str | None]:
    """Save extracted data to local SQLite database."""
    if not customer_name:
        logger.warning("[Local Save] Missing customer name for %s", chat_file_name)
        return (False, None, MISSING_CUSTOMER_NAME_MESSAGE, [], None)

    try:
        if target_customer_id:
            await _replace_existing_documents_of_same_type(
                storage_service,
                target_customer_id,
                document_type,
            )
            extraction_id = await _save_doc_and_extraction(
                storage_service,
                target_customer_id,
                chat_file_name,
                document_type,
                content,
                file_bytes=file_bytes,
            )
            logger.info("[Local Save] MERGED into %s: %s", target_customer_id, chat_file_name)
            return (True, extraction_id, None, [], target_customer_id)

        customer_id, customer_type = await _resolve_customer_target(
            storage_service,
            customer_name,
            document_type,
        )
        await _ensure_customer_exists(storage_service, customer_id, customer_name, customer_type, current_user)
        await _replace_existing_documents_of_same_type(
            storage_service,
            customer_id,
            document_type,
        )
        extraction_id = await _save_doc_and_extraction(
            storage_service,
            customer_id,
            chat_file_name,
            document_type,
            content,
            file_bytes=file_bytes,
        )
        logger.info("[Local Save] SUCCESS: %s -> extraction_id=%s", chat_file_name, extraction_id)
        return (True, extraction_id, None, [], customer_id)
    except Exception as exc:
        logger.error("[Local Save] Error for %s: %s", chat_file_name, exc, exc_info=True)
        if "替换旧资料失败" in str(exc):
            return (False, None, "替换旧资料失败，请稍后重试。", [], None)
        return (False, None, LOCAL_SAVE_FAILED_MESSAGE, [], None)


def _save_to_feishu_legacy(
    chat_file_name: str,
    document_type: str,
    content: dict,
    customer_name: str | None,
    current_user: dict | None,
    feishu_service: Any,
) -> tuple[bool, str | None, str | None]:
    """Save extracted data to Feishu spreadsheet (legacy mode)."""
    config = DATA_TYPE_CONFIG.get(document_type)
    feishu_field = config.get("feishu_field") if config else None

    if not feishu_field:
        logger.warning("[Feishu Save] Missing feishu_field config for %s", document_type)
        return (False, None, DOCUMENT_TYPE_CONFIG_MISSING_MESSAGE)

    try:
        fields = _build_feishu_fields(feishu_field, content, customer_name, current_user)
        return _execute_feishu_save(feishu_service, customer_name, fields, chat_file_name)
    except Exception as exc:
        logger.error("[Feishu Save] Unexpected error for %s: %s", chat_file_name, exc, exc_info=True)
        return (False, None, LEGACY_SAVE_FAILED_MESSAGE)


def _build_feishu_fields(
    feishu_field: str,
    content: dict,
    customer_name: str | None,
    current_user: dict | None,
) -> dict:
    """Build the fields dict for Feishu smart_merge."""
    content_markdown = dict_to_markdown(content)
    fields = {feishu_field: content_markdown}

    if customer_name:
        fields["企业名称"] = customer_name

    fields["上传时间"] = datetime.now(tz=timezone.utc).strftime("%Y/%m/%d %H:%M")

    if current_user:
        fields["上传账号"] = current_user.get("username") or ""

    return fields


def _sanitize_legacy_error(error_message: str | None) -> str | None:
    """Do not expose legacy Feishu/raw network errors to callers."""
    if not error_message:
        return None
    return LEGACY_SAVE_FAILED_MESSAGE


def _execute_feishu_save(
    feishu_service: Any,
    customer_name: str | None,
    fields: dict,
    chat_file_name: str,
) -> tuple[bool, str | None, str | None]:
    """Execute the Feishu smart_merge call."""
    from services.feishu_service import FeishuServiceError

    try:
        result = feishu_service.smart_merge(customer_name, fields)
        saved = result.get("success", False)
        record_id = result.get("record_id")
        error_msg = _sanitize_legacy_error(result.get("error_message"))

        if saved:
            logger.info("[Feishu Save] SUCCESS: %s -> record_id=%s", chat_file_name, record_id)
        else:
            logger.warning("[Feishu Save] FAILED for %s", chat_file_name)

        return (saved, record_id, error_msg)
    except FeishuServiceError as exc:
        logger.error("[Feishu Save] FeishuServiceError for %s: %s", chat_file_name, exc)
        return (False, None, LEGACY_SAVE_FAILED_MESSAGE)
