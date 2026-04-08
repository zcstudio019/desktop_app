"""
Storage save router.

Keeps the legacy /feishu/save endpoint for compatibility while exposing
the new /storage/save endpoint for local-first saves.
"""

import base64
import logging
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import DATA_TYPE_CONFIG
from services.feishu_service import FeishuService, FeishuServiceError

from backend.services import get_storage_service, supports_structured_storage
from backend.services.profile_sync_service import ProfileSyncService

from ..middleware.auth import get_current_user
from ..models.schemas import FeishuSaveRequest, FeishuSaveResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/feishu", tags=["Storage Compatibility"])
storage_router = APIRouter(prefix="/storage", tags=["Storage"])

storage_service = get_storage_service()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
feishu_service = None if HAS_DB_STORAGE else FeishuService()
profile_sync_service = ProfileSyncService()
UNKNOWN_DOCUMENT_TYPE_MESSAGE = "资料类型无效，请重新选择后再保存。"
DOCUMENT_TYPE_CONFIG_MISSING_MESSAGE = "资料类型配置缺失，请联系管理员检查配置。"
LOCAL_SAVE_FAILED_MESSAGE = "本地保存失败，请稍后重试。"
FEISHU_SAVE_FAILED_MESSAGE = "资料保存失败，请稍后重试。"


def _sanitize_storage_error(error_message: str | None) -> str | None:
    """Hide internal/provider details from user-facing storage errors."""
    if not error_message:
        return None

    lowered = error_message.lower()
    raw_markers = [
        "network error",
        "connection timeout",
        "connection failed",
        "feishu service error",
        "local save error",
        "unexpected error",
        "traceback",
        "invalid_request_error",
        "error code",
    ]
    if any(marker in lowered for marker in raw_markers):
        return FEISHU_SAVE_FAILED_MESSAGE

    return error_message

FRONTEND_TO_BACKEND_TYPE = {
    "enterprise_credit": "企业征信提取",
    "personal_credit": "个人征信提取",
    "enterprise_flow": "企业流水提取",
    "personal_flow": "个人流水提取",
    "financial_data": "财务数据提取",
    "collateral": "抵押物信息提取",
    "jellyfish_report": "水母报告提取",
    "personal_tax": "个人收入纳税/公积金",
}


def dict_to_markdown(data: dict | list | Any, level: int = 0) -> str:
    """Convert structured data to Markdown."""
    if not isinstance(data, (dict, list)):
        return str(data) if data is not None else "无"

    lines: list[str] = []
    indent = "  " * level

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(f"{indent}### {key}")
                lines.append(dict_to_markdown(value, level + 1))
            elif isinstance(value, list):
                lines.append(f"{indent}### {key}")
                for item in value:
                    if isinstance(item, dict):
                        lines.append(dict_to_markdown(item, level + 1))
                    else:
                        lines.append(f"{indent}- {item if item is not None else '无'}")
            else:
                lines.append(f"{indent}- **{key}**: {value if value is not None else '无'}")
    else:
        for item in data:
            if isinstance(item, dict):
                lines.append(dict_to_markdown(item, level))
            else:
                lines.append(f"{indent}- {item if item is not None else '无'}")

    return "\n".join(lines)


async def _save_request(
    request: FeishuSaveRequest,
    current_user: dict | None,
) -> FeishuSaveResponse:
    """Handle storage save for both local-first and legacy Feishu routes."""
    backend_type = FRONTEND_TO_BACKEND_TYPE.get(request.documentType) or request.documentType
    logger.info(
        "Saving document - endpoint=%s, documentType=%s, customerName=%s",
        "db" if HAS_DB_STORAGE else "feishu",
        backend_type,
        request.customerName or "(empty)",
    )

    config = DATA_TYPE_CONFIG.get(backend_type)
    if not config:
        return FeishuSaveResponse(
            success=False,
            recordId=None,
            isNew=False,
            error=UNKNOWN_DOCUMENT_TYPE_MESSAGE,
        )

    if HAS_DB_STORAGE:
        from .chat_storage import _determine_customer_id, _resolve_customer_target, _save_to_local_storage

        file_bytes = base64.b64decode(request.fileContent) if request.fileContent else None
        try:
            resolved_customer_id = None
            if request.customerName:
                resolved_customer_id, _ = await _resolve_customer_target(
                    storage_service,
                    request.customerName,
                    backend_type,
                )
                default_customer_id, _ = _determine_customer_id(request.customerName, backend_type)
                is_new = resolved_customer_id == default_customer_id and await storage_service.get_customer(default_customer_id) is None
            else:
                is_new = True

            save_result = await _save_to_local_storage(
                request.fileName or f"{backend_type}.json",
                backend_type,
                request.content,
                request.customerName,
                current_user,
                storage_service,
                target_customer_id=request.customerId,
                file_bytes=file_bytes,
            )
            success = bool(save_result[0]) if len(save_result) > 0 else False
            record_id = save_result[1] if len(save_result) > 1 else None
            error_msg = save_result[2] if len(save_result) > 2 else None
            saved_customer_id = save_result[4] if len(save_result) > 4 else None
            final_customer_id = request.customerId or saved_customer_id or resolved_customer_id
            if success and final_customer_id:
                await profile_sync_service.handle_document_saved(storage_service, final_customer_id)
            return FeishuSaveResponse(
                success=success,
                recordId=record_id,
                customerId=final_customer_id,
                isNew=is_new,
                error=_sanitize_storage_error(error_msg),
            )
        except Exception as e:
            logger.error(f"Unexpected error during local save: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=LOCAL_SAVE_FAILED_MESSAGE) from e

    feishu_field = config.get("feishu_field")
    if not feishu_field:
        return FeishuSaveResponse(
            success=False,
            recordId=None,
            isNew=False,
            error=DOCUMENT_TYPE_CONFIG_MISSING_MESSAGE,
        )

    try:
        content_markdown = dict_to_markdown(request.content)
        fields = {feishu_field: content_markdown}
        if current_user:
            fields["上传账号"] = current_user["username"]
        result = feishu_service.smart_merge(request.customerName, fields)
    except FeishuServiceError as e:
        logger.error(f"Feishu service error: {e}")
        raise HTTPException(status_code=500, detail=FEISHU_SAVE_FAILED_MESSAGE) from e
    except Exception as e:
        logger.error(f"Unexpected error during Feishu save: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=FEISHU_SAVE_FAILED_MESSAGE) from e

    return FeishuSaveResponse(
        success=result.get("success", False),
        recordId=result.get("record_id"),
        customerId=None,
        isNew=not result.get("is_update", False),
        error=_sanitize_storage_error(result.get("error_message")),
    )


@router.post("/save", response_model=FeishuSaveResponse)
async def save_to_feishu(
    request: FeishuSaveRequest,
    current_user: dict = Depends(get_current_user),
) -> FeishuSaveResponse:
    """Legacy compatibility endpoint."""
    return await _save_request(request, current_user)


@storage_router.post("/save", response_model=FeishuSaveResponse)
async def save_to_storage(
    request: FeishuSaveRequest,
    current_user: dict = Depends(get_current_user),
) -> FeishuSaveResponse:
    """Local-first storage endpoint."""
    return await _save_request(request, current_user)
