"""
Chat Storage Module

Handles saving extracted data to storage backends (local SQLite or Feishu).
Extracted from chat.py to keep functions under 50 lines.
"""

import contextlib
import hashlib
import logging
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.document_types import get_document_type_definition, normalize_document_type_code, should_store_original
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

_PERSONAL_DOCUMENT_TYPE_CODES = frozenset({
    "personal_credit",
    "personal_flow",
    "personal_tax",
    "id_card",
    "hukou",
    "marriage_cert",
    "vehicle_license",
})

_MULTI_INSTANCE_DOCUMENT_TYPE_CODES = frozenset({
    "id_card",
    "bank_statement",
    "enterprise_credit",
})

_PROPERTY_MULTI_FILE_DOCUMENT_TYPE_CODES = frozenset({
    "collateral",
    "property_report",
    "mortgage_info",
    "property_certificate",
})

ENTERPRISE_CREDIT_VALID_DAYS = 90

_DOCUMENT_TYPE_CODE_FALLBACKS = {
    "营业执照": "business_license",
    "营业执照正本": "business_license",
    "营业执照副本": "business_license",
    "营业执照正副本": "business_license",
    "开户许可证": "account_license",
    "开户许可证书": "account_license",
    "基本账户开户许可证": "account_license",
    "身份证": "id_card",
    "居民身份证": "id_card",
    "户口本": "hukou",
    "户籍证明": "hukou",
    "结婚证": "marriage_cert",
    "婚姻登记证": "marriage_cert",
    "产调": "property_report",
    "房产证": "property_report",
    "房产证 / 产调": "property_report",
    "房产证/产调": "property_report",
    "行驶证": "vehicle_license",
    "机动车行驶证": "vehicle_license",
    "特别许可证": "special_license",
    "特殊许可证": "special_license",
    "公司章程": "company_articles",
    "银行对账单": "bank_statement",
    "银行对账明细": "bank_statement_detail",
}


def _normalize_storage_document_type(document_type: str | None) -> str:
    """Normalize incoming document type aliases/display names to canonical codes."""
    raw_value = str(document_type or "").strip()
    normalized = normalize_document_type_code(raw_value)
    if normalized:
        if normalized != raw_value:
            logger.info("[Local Save] normalized document_type=%s -> %s", raw_value, normalized)
        return normalized

    fallback = _DOCUMENT_TYPE_CODE_FALLBACKS.get(raw_value)
    if fallback:
        logger.info("[Local Save] normalized document_type=%s -> %s", raw_value, fallback)
        return fallback

    logger.warning("[Local Save] could not normalize document_type=%s, using raw value", raw_value)
    return raw_value


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
    document_type_code = _normalize_storage_document_type(document_type)
    customer_type = "personal" if document_type_code in _PERSONAL_DOCUMENT_TYPE_CODES or document_type in _PERSONAL_DOC_TYPES else "enterprise"
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
            uploader = (current_user.get("username") or "") if current_user else ""
            updates = {"upload_time": datetime.now(tz=timezone.utc).strftime("%Y/%m/%d")}
            existing_uploader = str(existing.get("uploader") or "") if isinstance(existing, dict) else ""
            if uploader and existing_uploader in {"", "anonymous", uploader}:
                # Local authorization is currently uploader-based. When an upload
                # auto-binds to a newly/previously anonymous customer, ensure the
                # uploader can see it immediately without taking over another
                # user's explicitly-owned customer.
                updates["uploader"] = uploader
            await storage_service.update_customer(
                customer_id,
                updates,
            )
        except Exception as exc:
            logger.warning("[Local Save] Failed to update upload_time: %s", exc)


def _save_file_to_disk(
    customer_id: str,
    file_name: str,
    file_bytes: bytes | None,
    document_type: str,
    *,
    store_original: bool,
) -> tuple[str, int]:
    """Persist the original uploaded file according to the document policy."""
    if not file_bytes:
        return ("", 0)

    file_size = len(file_bytes)
    if not store_original:
        logger.info(
            "[Local Save] Skipped raw file persistence by document policy document_type=%s file_name=%s",
            document_type,
            file_name,
        )
        return ("", file_size)

    if not STORE_ORIGINAL_UPLOAD_FILES:
        logger.warning(
            "[Local Save] STORE_ORIGINAL_UPLOAD_FILES is disabled, but document policy requires raw persistence; continuing to store file document_type=%s file_name=%s",
            document_type,
            file_name,
        )

    safe_name = Path(file_name or "uploaded_file").name
    safe_customer_dir = hashlib.sha1(customer_id.encode("utf-8")).hexdigest()[:16]
    relative_path = Path("uploads") / safe_customer_dir / safe_name
    target_path = _LOCAL_UPLOAD_ROOT / safe_customer_dir / safe_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(file_bytes)
    logger.info("[Local Save] Saved original file: %s", target_path)
    return (relative_path.as_posix(), file_size)


def _normalize_enterprise_credit_report_date(report_date: str | None) -> str:
    raw = str(report_date or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y年%m月%d日", "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
    compact = raw.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-").replace(".", "-")
    with contextlib.suppress(ValueError):
        return datetime.strptime(compact, "%Y-%m-%d").strftime("%Y-%m-%d")
    return raw


def _compute_enterprise_credit_valid_until(report_date: str | None) -> str:
    normalized = _normalize_enterprise_credit_report_date(report_date)
    if not normalized:
        return ""
    with contextlib.suppress(ValueError):
        parsed = datetime.strptime(normalized, "%Y-%m-%d")
        return (parsed + timedelta(days=ENTERPRISE_CREDIT_VALID_DAYS)).strftime("%Y-%m-%d")
    return ""


async def _save_doc_and_extraction(
    storage_service: Any,
    customer_id: str,
    chat_file_name: str,
    document_type: str,
    content: dict,
    file_bytes: bytes | None = None,
    current_user: dict | None = None,
) -> tuple[str, str, bool]:
    """Save document metadata and extraction result to local storage."""
    doc_id = str(uuid.uuid4())
    extraction_id = str(uuid.uuid4())

    document_type_code = _normalize_storage_document_type(document_type)
    definition = get_document_type_definition(document_type_code)
    store_original = definition.store_original if definition else should_store_original(document_type_code)
    file_bytes_size = len(file_bytes) if file_bytes else 0
    logger.info(
        "[Local Save] document_type_code=%s store_original=%s file_bytes_present=%s file_bytes_size=%s",
        document_type_code,
        store_original,
        bool(file_bytes),
        file_bytes_size,
    )
    if store_original and not file_bytes:
        logger.error(
            "[Local Save] save stage missing file bytes, document_type_code=%s file_name=%s",
            document_type_code,
            chat_file_name,
        )
        raise RuntimeError(f"原件应保存但 file_bytes 缺失: {document_type_code} / {chat_file_name}")

    file_path, file_size = _save_file_to_disk(
        customer_id,
        chat_file_name,
        file_bytes,
        document_type_code,
        store_original=store_original,
    )
    if store_original and not file_path:
        logger.error(
            "[Local Save] expected original file to be saved but file_path is empty, document_type_code=%s file_name=%s file_bytes_present=%s",
            document_type_code,
            chat_file_name,
            bool(file_bytes),
        )
        raise RuntimeError(f"原件应保存但 file_path 为空: {document_type_code} / {chat_file_name}")

    doc_data = {
        "doc_id": doc_id,
        "customer_id": customer_id,
        "file_name": chat_file_name,
        "file_path": file_path,
        "file_type": document_type_code,
        "file_size": file_size,
        "uploader": (current_user.get("username") or "") if current_user else "",
        "file_hash": hashlib.sha256(file_bytes).hexdigest() if file_bytes else "",
        "is_active": 0 if document_type_code == "enterprise_credit" else 1,
        "archived_at": None,
        "replaced_by_document_id": "",
        "version_policy": "single_active" if document_type_code == "enterprise_credit" else "",
        "report_date": _normalize_enterprise_credit_report_date(
            ((content.get("extracted_json") or {}).get("report_basic") or {}).get("report_date")
            or content.get("report_date")
        ),
        "valid_until": _compute_enterprise_credit_valid_until(
            ((content.get("extracted_json") or {}).get("report_basic") or {}).get("report_date")
            or content.get("report_date")
        ) if document_type_code == "enterprise_credit" else "",
    }
    await storage_service.save_document(doc_data)
    logger.info(
        "[Local Save] Saved document metadata: %s (store_original=%s, file_path=%s)",
        doc_id,
        store_original,
        file_path or "(empty)",
    )

    confidence = content.get("confidence", 0.0) if isinstance(content.get("confidence"), (int, float)) else 0.0
    extraction_data = {
        "extraction_id": extraction_id,
        "doc_id": doc_id,
        "customer_id": customer_id,
        "extraction_type": document_type_code,
        "extracted_data": content,
        "confidence": confidence,
        "extraction_status": str(content.get("extraction_status") or "success"),
        "extraction_error": str(content.get("extraction_error") or ""),
        "skill_name": str(content.get("skill_name") or ""),
        "skill_version": str(content.get("skill_version") or ""),
        "schema_version": str(content.get("schema_version") or ""),
    }
    await storage_service.save_extraction(extraction_data)
    logger.info("[Local Save] Saved extraction: %s", extraction_id)

    if document_type_code == "enterprise_credit" and extraction_data["extraction_status"] == "success":
        activate_single_active = getattr(storage_service, "activate_single_active_document", None)
        if callable(activate_single_active):
            await activate_single_active(
                customer_id,
                document_type_code,
                doc_id,
                report_date=doc_data.get("report_date") or "",
                valid_until=doc_data.get("valid_until") or "",
            )
            logger.info(
                "[Enterprise Credit] activated current version customer_id=%s doc_id=%s report_date=%s valid_until=%s",
                customer_id,
                doc_id,
                doc_data.get("report_date") or "",
                doc_data.get("valid_until") or "",
            )
        else:
            logger.warning(
                "[Enterprise Credit] storage backend does not support activate_single_active_document customer_id=%s doc_id=%s",
                customer_id,
                doc_id,
            )

    return extraction_id, doc_id, bool(file_path)


async def _replace_existing_documents_of_same_type(
    storage_service: Any,
    customer_id: str,
    document_type: str,
    chat_file_name: str | None = None,
) -> None:
    """Delete older documents of the same type before saving the new one.

    The product rule is: for a given customer, the latest upload of the same
    document type should replace earlier uploads. This keeps summary views and
    downstream application generation aligned with the newest material.
    """
    document_type_code = _normalize_storage_document_type(document_type)
    if document_type_code in _PROPERTY_MULTI_FILE_DOCUMENT_TYPE_CODES:
        await _replace_existing_property_document_with_same_name(
            storage_service,
            customer_id,
            document_type_code,
            chat_file_name,
        )
        return

    if document_type_code == "bank_statement":
        await _replace_existing_bank_statement_with_same_name(
            storage_service,
            customer_id,
            document_type_code,
            chat_file_name,
        )
        return

    if document_type_code in _MULTI_INSTANCE_DOCUMENT_TYPE_CODES:
        logger.info(
            "[Local Save] Skip replacement for multi-instance document type customer_id=%s type=%s",
            customer_id,
            document_type_code,
        )
        return
    existing_documents = await storage_service.list_documents(customer_id)
    replaced_count = 0

    for document in existing_documents:
        if _normalize_storage_document_type(document.get("file_type") or "") != document_type_code:
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
                document_type_code,
                doc_id,
                exc,
                exc_info=True,
            )
            raise RuntimeError(f"替换旧资料失败，无法删除旧文档：{doc_id}") from exc

        if not deleted:
            logger.warning(
                "[Local Save] Existing document disappeared during replacement customer_id=%s document_type=%s old_doc_id=%s",
                customer_id,
                document_type_code,
                doc_id,
            )
            raise RuntimeError(f"替换旧资料失败，未找到旧文档：{doc_id}")

        replaced_count += 1

    if replaced_count:
        logger.info(
            "[Local Save] Replaced %s existing document(s) for customer=%s, type=%s",
            replaced_count,
            customer_id,
            document_type_code,
        )


async def _replace_existing_bank_statement_with_same_name(
    storage_service: Any,
    customer_id: str,
    document_type_code: str,
    chat_file_name: str | None,
) -> None:
    """Bank statements can coexist by filename; only exact filename duplicates are replaced."""
    upload_file_name = Path(chat_file_name or "").name
    if not upload_file_name:
        logger.info(
            "[bank_statement][save] replace_existing same_filename=(empty) customer_id=%s replaced=0",
            customer_id,
        )
        return

    existing_documents = await storage_service.list_documents(customer_id)
    replaced_count = 0

    for document in existing_documents:
        existing_type = _normalize_storage_document_type(document.get("file_type") or "")
        if existing_type != document_type_code:
            continue
        existing_file_name = Path(document.get("file_name") or "").name
        if existing_file_name != upload_file_name:
            continue
        doc_id = document.get("doc_id")
        if not doc_id:
            continue
        try:
            deleted = await storage_service.delete_document(doc_id)
        except Exception as exc:
            logger.error(
                "[bank_statement][save] replace_existing same_filename=%s customer_id=%s old_doc_id=%s error=%s",
                upload_file_name,
                customer_id,
                doc_id,
                exc,
                exc_info=True,
            )
            raise RuntimeError(f"替换同名银行对账单失败，无法删除旧文档：{doc_id}") from exc

        if not deleted:
            logger.warning(
                "[bank_statement][save] replace_existing same_filename=%s customer_id=%s old_doc_id=%s missing",
                upload_file_name,
                customer_id,
                doc_id,
            )
            raise RuntimeError(f"替换同名银行对账单失败，未找到旧文档：{doc_id}")

        replaced_count += 1

    logger.info(
        "[bank_statement][save] replace_existing same_filename=%s customer_id=%s replaced=%s",
        upload_file_name,
        customer_id,
        replaced_count,
    )


async def _replace_existing_property_document_with_same_name(
    storage_service: Any,
    customer_id: str,
    document_type_code: str,
    chat_file_name: str | None,
) -> None:
    """For property certificates, preserve sibling files and only replace exact filename duplicates."""
    upload_file_name = Path(chat_file_name or "").name
    if not upload_file_name:
        logger.info(
            "[Local Save] Property multi-file mode preserves same-type documents customer_id=%s type=%s file_name=(empty)",
            customer_id,
            document_type_code,
        )
        return

    existing_documents = await storage_service.list_documents(customer_id)
    replaced_count = 0

    for document in existing_documents:
        existing_type = _normalize_storage_document_type(document.get("file_type") or "")
        if existing_type not in _PROPERTY_MULTI_FILE_DOCUMENT_TYPE_CODES:
            continue
        existing_file_name = Path(document.get("file_name") or "").name
        if existing_file_name != upload_file_name:
            continue
        doc_id = document.get("doc_id")
        if not doc_id:
            continue
        try:
            deleted = await storage_service.delete_document(doc_id)
        except Exception as exc:
            logger.error(
                "[Local Save] Failed to replace same-name property document customer_id=%s document_type=%s file_name=%s old_doc_id=%s error=%s",
                customer_id,
                document_type_code,
                upload_file_name,
                doc_id,
                exc,
                exc_info=True,
            )
            raise RuntimeError(f"replace same-name property document failed, cannot delete old document: {doc_id}") from exc

        if not deleted:
            logger.warning(
                "[Local Save] Same-name property document disappeared during replacement customer_id=%s document_type=%s file_name=%s old_doc_id=%s",
                customer_id,
                document_type_code,
                upload_file_name,
                doc_id,
            )
            raise RuntimeError(f"replace same-name property document failed, old document not found: {doc_id}")

        replaced_count += 1

    if replaced_count:
        logger.info(
            "[Local Save] Replaced %s same-name property document(s) for customer=%s, type=%s, file_name=%s",
            replaced_count,
            customer_id,
            document_type_code,
            upload_file_name,
        )
    else:
        logger.info(
            "[Local Save] Property multi-file mode preserved existing same-type documents customer_id=%s type=%s file_name=%s",
            customer_id,
            document_type_code,
            upload_file_name,
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
) -> tuple[bool, str | None, str | None, list[dict], str | None, str | None, bool]:
    """Save extracted data to local SQLite database."""
    if not customer_name:
        logger.warning("[Local Save] Missing customer name for %s", chat_file_name)
        return (False, None, MISSING_CUSTOMER_NAME_MESSAGE, [], None, None, False)

    document_type_code = _normalize_storage_document_type(document_type)

    try:
        if target_customer_id:
            await _replace_existing_documents_of_same_type(
                storage_service,
                target_customer_id,
                document_type_code,
                chat_file_name,
            )
            extraction_id, doc_id, original_available = await _save_doc_and_extraction(
                storage_service,
                target_customer_id,
                chat_file_name,
                document_type_code,
                content,
                file_bytes=file_bytes,
                current_user=current_user,
            )
            logger.info("[Local Save] MERGED into %s: %s", target_customer_id, chat_file_name)
            return (True, extraction_id, None, [], target_customer_id, doc_id, original_available)

        customer_id, customer_type = await _resolve_customer_target(
            storage_service,
            customer_name,
            document_type_code,
        )
        await _ensure_customer_exists(storage_service, customer_id, customer_name, customer_type, current_user)
        await _replace_existing_documents_of_same_type(
            storage_service,
            customer_id,
            document_type_code,
            chat_file_name,
        )
        extraction_id, doc_id, original_available = await _save_doc_and_extraction(
            storage_service,
            customer_id,
            chat_file_name,
            document_type_code,
            content,
            file_bytes=file_bytes,
            current_user=current_user,
        )
        logger.info("[Local Save] SUCCESS: %s -> extraction_id=%s", chat_file_name, extraction_id)
        return (True, extraction_id, None, [], customer_id, doc_id, original_available)
    except Exception as exc:
        logger.error("[Local Save] Error for %s: %s", chat_file_name, exc, exc_info=True)
        if "替换旧资料失败" in str(exc):
            return (False, None, "替换旧资料失败，请稍后重试。", [], None)
        return (False, None, LOCAL_SAVE_FAILED_MESSAGE, [], None, None, False)


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
