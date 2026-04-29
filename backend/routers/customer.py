"""
Customer List Router

Provides customer listing with role-based filtering:
- Admin users see all customers
- Normal users see only customers they uploaded (filtered by "上传账号" field)

Endpoints:
- GET /api/customers - List customers with optional search filter
"""

import asyncio
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from services.feishu_service import FeishuService

from backend.celery_app import TASK_QUEUE_ENABLED
from backend.document_types import get_document_display_name, get_document_type_definition, should_store_original
from backend.services import get_storage_service, supports_structured_storage  # 新增：存储服务 factory

from backend.services.markdown_profile_service import (
    get_or_create_customer_profile,
    get_rag_source_priority,
    get_risk_report_schema_template,
    regenerate_customer_profile,
)
from backend.services.document_extractor_service import (
    extract_company_articles_management_roles,
    extract_company_articles_role_evidence_lines,
)
from backend.services.activity_service import add_activity
from backend.services.profile_sync_service import ProfileSyncService
from backend.services.rag_service import RagService
from backend.services.risk_assessment_service import RiskAssessmentService
from services.file_service import FileService
from services.ocr_service import OCRService, OCRServiceError
from ..middleware.auth import get_current_user
from ..models.schemas import (
    ChatJobCreateResponse,
    CustomerDocumentListItem,
    CustomerRiskReportResponse,
    CustomerRiskReportHistoryItem,
    CustomerRiskReportHistoryResponse,
    CustomerRagChatRequest,
    CustomerRagChatResponse,
    CustomerDetail,
    CustomerListItem,
    CustomerProfileMarkdownResponse,
    UpdateCustomerProfileMarkdownRequest,
)

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/customers", tags=["Customers"])
documents_router = APIRouter(prefix="/documents", tags=["Documents"])

# Initialize service
feishu_service = FeishuService()
storage_service = get_storage_service()  # 根据配置返回本地存储或飞书服务
HAS_DB_STORAGE = supports_structured_storage(storage_service)

rag_service = RagService()
risk_assessment_service = RiskAssessmentService(rag_service=rag_service)
profile_sync_service = ProfileSyncService()
_ACTIVE_RISK_JOB_TASKS: set[asyncio.Task[None]] = set()
_DOCUMENT_ROOT = Path(__file__).parent.parent.parent / "data"
DOCUMENT_NOT_RETAINED_MESSAGE = "该资料未保存原件，仅保留提取结果和资料汇总"
DOCUMENT_FILE_MISSING_MESSAGE = "原件文件不存在或已不可用"
file_service = FileService()
ocr_service = OCRService()

_COMPANY_ARTICLES_INVALID_ROLE_FRAGMENTS = {
    "姓名或者名称", "姓名或名称", "姓名名称", "姓名", "名称", "股东",
    "法定代表人", "执行董事", "董事长", "经理", "监事", "负责人",
    "信息", "资料", "说明", "无", "暂无", "待定", "空白",
    "填写", "填报", "填入", "未填写", "未填报", "未填入",
    "签字", "签章", "盖章", "职务", "董事", "报酬", "及其报酬", "其报酬",
    "公司类型", "公司股东", "决定聘任", "印章", "用章", "动用", "使用", "印鉴",
    "利润", "分配", "亏损", "利润分配", "弥补亏损", "委托", "受托", "国家", "机关", "授权",
    "报告", "通知", "通知书", "材料", "文件", "目录", "附件", "立本", "法规", "法律", "条例",
}


async def _build_customer_risk_report_payload(
    customer_id: str,
    current_user: dict,
) -> dict[str, Any]:
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="鏈壘鍒拌瀹㈡埛璁板綍")
    await _ensure_local_customer_access(customer, current_user)

    previous_report = await storage_service.get_latest_customer_risk_report(customer_id)
    result = await risk_assessment_service.generate_report(storage_service, customer_id)
    await storage_service.save_customer_risk_report(
        {
            "report_id": uuid.uuid4().hex,
            "customer_id": customer_id,
            "generated_at": result.get("generated_at") or "",
            "profile_version": result.get("profile_version") or 1,
            "profile_updated_at": result.get("profile_updated_at") or "",
            "report_json": result.get("report_json") or {},
            "report_markdown": result.get("report_markdown") or "",
        }
    )
    profile = await storage_service.get_customer_profile(customer_id) or {}
    add_activity(
        activity_type="risk",
        customer=customer.get("name") or "",
        customer_id=customer_id,
        username=current_user.get("username") or "",
        status="completed",
        title="风险评估报告已生成",
        description="系统已基于当前客户最新资料生成结构化风险评估报告。",
        metadata={
            "generatedAt": result.get("generated_at") or "",
            "riskLevel": (result.get("report_json") or {}).get("overall_assessment", {}).get("risk_level", ""),
            "totalScore": (result.get("report_json") or {}).get("overall_assessment", {}).get("total_score", 0),
            "profileVersion": profile.get("version") or 1,
            "profileUpdatedAt": profile.get("updated_at") or "",
        },
    )

    response_payload = CustomerRiskReportResponse(
        report_json=result.get("report_json") or {},
        report_markdown=result.get("report_markdown") or "",
        generated_at=result.get("generated_at") or "",
        profile_version=result.get("profile_version") or 1,
        profile_updated_at=result.get("profile_updated_at") or "",
        previous_report=CustomerRiskReportHistoryItem(**previous_report) if previous_report else None,
    )
    payload = response_payload.model_dump()
    payload["customerId"] = customer_id
    payload["customerName"] = customer.get("name") or ""
    return payload


async def _run_customer_risk_report_job(
    job_id: str,
    customer_id: str,
    current_user_payload: dict[str, Any],
) -> None:
    async def update_progress(message: str) -> None:
        logger.info("[Risk Job] progress job_id=%s stage=%s", job_id, message)
        await storage_service.update_async_job(job_id, {"progress_message": message})

    await storage_service.update_async_job(
        job_id,
        {
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "progress_message": "已接收任务",
        },
    )

    try:
        await update_progress("正在读取客户资料")
        await update_progress("正在进行规则评估")
        payload = await _build_customer_risk_report_payload(customer_id, current_user_payload)
        await update_progress("正在生成风险报告")
        await storage_service.update_async_job(
            job_id,
            {
                "status": "success",
                "progress_message": "处理完成",
                "result_json": payload,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except HTTPException as exc:
        logger.error("[Risk Job] failed job_id=%s detail=%s", job_id, exc.detail, exc_info=True)
        await storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "风险报告生成失败",
                "error_message": str(exc.detail),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        raise
    except Exception as exc:
        logger.error("[Risk Job] failed job_id=%s error=%s", job_id, exc, exc_info=True)
        await storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "风险报告生成失败",
                "error_message": str(exc) or "风险评估任务执行失败",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        raise


def _launch_customer_risk_report_job(
    job_id: str,
    customer_id: str,
    current_user_payload: dict[str, Any],
) -> None:
    task = asyncio.create_task(
        _run_customer_risk_report_job(job_id, customer_id, current_user_payload)
    )
    _ACTIVE_RISK_JOB_TASKS.add(task)

    def _cleanup(done_task: asyncio.Task[None]) -> None:
        _ACTIVE_RISK_JOB_TASKS.discard(done_task)
        try:
            done_task.result()
        except Exception:
            logger.exception("[Risk Job] background task crashed job_id=%s", job_id)

    task.add_done_callback(_cleanup)


def _build_risk_report_job_execution_payload(
    job_id: str,
    customer_id: str,
    customer_name: str,
    current_user_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "jobId": job_id,
        "jobType": "risk_report",
        "customerId": customer_id,
        "customerName": customer_name,
        "username": current_user_payload.get("username") or "",
        "role": current_user_payload.get("role") or "",
        "createdFrom": "risk_report_job",
    }


async def execute_customer_risk_report_job_from_job(job_id: str) -> None:
    payload = await storage_service.get_async_job_execution_payload(job_id)
    if not payload:
        raise ValueError(f"async job {job_id} execution payload not found")

    customer_id = payload.get("customerId") if isinstance(payload, dict) else ""
    if not customer_id:
        raise ValueError(f"async job {job_id} missing customerId")

    await _run_customer_risk_report_job(
        job_id,
        customer_id,
        {
            "username": payload.get("username") if isinstance(payload, dict) else "",
            "role": payload.get("role") if isinstance(payload, dict) else "",
        },
    )


async def _dispatch_customer_risk_report_job(
    job_id: str,
    customer_id: str,
    current_user_payload: dict[str, Any],
) -> None:
    logger.info(
        "[Risk Job] submit start job_id=%s queue_enabled=%s customer_id=%s username=%s",
        job_id,
        TASK_QUEUE_ENABLED,
        customer_id,
        current_user_payload.get("username") or "",
    )
    if TASK_QUEUE_ENABLED:
        from backend.celery_app import HEAVY_QUEUE_NAME, RISK_REPORT_TASK_NAME, celery_app

        async_result = celery_app.send_task(RISK_REPORT_TASK_NAME, args=[job_id], queue=HEAVY_QUEUE_NAME)
        await storage_service.mark_async_job_dispatched(
            job_id,
            async_result.id,
            worker_name="celery",
        )
        logger.info(
            "[Risk Job] dispatched to celery job_id=%s celery_task_id=%s customer_id=%s username=%s",
            job_id,
            async_result.id,
            customer_id,
            current_user_payload.get("username") or "",
        )
        return

    logger.warning(
        "[Risk Job] fallback to in-process execution job_id=%s customer_id=%s username=%s",
        job_id,
        customer_id,
        current_user_payload.get("username") or "",
    )
    _launch_customer_risk_report_job(job_id, customer_id, current_user_payload)


def _extract_text_value(field_value: Any) -> str:
    """Extract plain text from a Feishu field value.

    Feishu text fields may be rich text format (list of dicts with "text" key).
    See lessons-learned #22.

    Args:
        field_value: The raw field value from Feishu API.

    Returns:
        str: Extracted plain text string.
    """
    if field_value is None:
        return ""
    if isinstance(field_value, str):
        return field_value
    if isinstance(field_value, list):
        # Rich text format: [{"text": "...", "type": "text"}, ...]
        texts = []
        for item in field_value:
            if isinstance(item, dict):
                text = item.get("text") or ""
                texts.append(text)
            else:
                texts.append(str(item))
        return "".join(texts)
    return str(field_value)


def _is_admin(current_user: dict) -> bool:
    return current_user.get("role") == "admin"


def _get_username(current_user: dict) -> str:
    return str(current_user.get("username") or "")


def _has_direct_local_customer_access(customer: dict[str, Any], current_user: dict) -> bool:
    if _is_admin(current_user):
        return True
    username = _get_username(current_user)
    if not username:
        return False
    owner_values = (
        customer.get("uploader"),
        customer.get("owner"),
        customer.get("owner_username"),
        customer.get("creator"),
        customer.get("created_by"),
        customer.get("username"),
    )
    return any(str(value or "") == username for value in owner_values)


def _can_access_local_customer(customer: dict[str, Any], current_user: dict) -> bool:
    return _has_direct_local_customer_access(customer, current_user)


async def _ensure_local_customer_access(customer: dict[str, Any], current_user: dict) -> None:
    if not _can_access_local_customer(customer, current_user):
        raise HTTPException(status_code=403, detail="无权查看该客户记录")


def _resolve_document_absolute_path(file_path: str | None) -> Path | None:
    normalized = str(file_path or "").strip().replace("\\", "/")
    if not normalized:
        return None
    path = Path(normalized)
    if path.is_absolute():
        return path
    return _DOCUMENT_ROOT / path


def _build_document_original_status(document: dict[str, Any]) -> tuple[bool, str]:
    file_type = document.get("file_type") or ""
    if not should_store_original(file_type):
        return False, DOCUMENT_NOT_RETAINED_MESSAGE

    absolute_path = _resolve_document_absolute_path(document.get("file_path"))
    if not absolute_path:
        return False, DOCUMENT_FILE_MISSING_MESSAGE
    if not absolute_path.exists() or not absolute_path.is_file():
        return False, DOCUMENT_FILE_MISSING_MESSAGE
    return True, "可查看原件"


def _is_company_articles_role_missing_or_invalid(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or text == "暂无":
        return True
    if len(text) > 4:
        return True
    if any(fragment in text for fragment in _COMPANY_ARTICLES_INVALID_ROLE_FRAGMENTS):
        return True
    return re.fullmatch(r"[\u4e00-\u9fff·]{2,4}", text) is None


def _ocr_company_articles_front_pages_from_document(document: dict[str, Any]) -> str:
    absolute_path = _resolve_document_absolute_path(document.get("file_path"))
    if not absolute_path or not absolute_path.exists() or not absolute_path.is_file():
        logger.warning(
            "[company_articles] extraction enrich skipped: original file missing doc_id=%s file_path=%s",
            document.get("doc_id"),
            document.get("file_path"),
        )
        return ""

    try:
        file_bytes = absolute_path.read_bytes()
    except Exception as exc:
        logger.warning(
            "[company_articles] extraction enrich read failed doc_id=%s path=%s error=%s",
            document.get("doc_id"),
            absolute_path,
            exc,
        )
        return ""

    file_name = document.get("file_name") or absolute_path.name
    file_type = file_service.get_file_type(file_name)

    try:
        if file_type == "pdf":
            images = file_service.pdf_to_images(file_bytes)
            if not images:
                logger.warning("[company_articles] extraction enrich skipped: no rendered pages doc_id=%s", document.get("doc_id"))
                return ""
            ocr_parts: list[str] = []
            for page_index in (0, 1):
                if page_index >= len(images):
                    continue
                compressed = file_service.compress_image(images[page_index])
                page_text = ocr_service.recognize_image(compressed).strip()
                logger.info(
                    "[company_articles] extraction enrich front-page OCR doc_id=%s page=%s text=%s",
                    document.get("doc_id"),
                    page_index + 1,
                    page_text[:1000] or "(empty)",
                )
                if page_text:
                    ocr_parts.append(f"--- OCR Page {page_index + 1} ---\n{page_text}")
            return "\n\n".join(ocr_parts)

        if file_type == "image":
            compressed = file_service.compress_image(file_bytes)
            image_text = ocr_service.recognize_image(compressed).strip()
            logger.info(
                "[company_articles] extraction enrich image OCR doc_id=%s text=%s",
                document.get("doc_id"),
                image_text[:1000] or "(empty)",
            )
            return image_text
    except OCRServiceError as exc:
        logger.warning("[company_articles] extraction enrich OCR failed doc_id=%s error=%s", document.get("doc_id"), exc)
        return ""
    except Exception as exc:  # pragma: no cover - best-effort
        logger.warning("[company_articles] extraction enrich failed doc_id=%s error=%s", document.get("doc_id"), exc)
        return ""

    return ""


def _merge_company_articles_front_page_roles(
    extracted_data: dict[str, Any],
    document: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(extracted_data, dict) or not document:
        return extracted_data

    current_legal_person = extracted_data.get("legal_person")
    evidence_lines = extracted_data.get("management_role_evidence_lines") or []
    has_legal_evidence = any("法定代表人" in str(line or "") for line in evidence_lines)
    if not _is_company_articles_role_missing_or_invalid(current_legal_person) and has_legal_evidence:
        return extracted_data

    front_page_text = _ocr_company_articles_front_pages_from_document(document)
    if not front_page_text:
        logger.warning(
            "[company_articles] extraction enrich skipped: empty OCR supplement doc_id=%s legal_person=%s",
            document.get("doc_id"),
            current_legal_person,
        )
        return extracted_data

    role_data = extract_company_articles_management_roles(front_page_text)
    role_evidence_lines = extract_company_articles_role_evidence_lines(front_page_text)
    logger.info(
        "[company_articles] extraction enrich role_data doc_id=%s legal_person=%s executive_director=%s supervisor=%s",
        document.get("doc_id"),
        role_data.get("legal_person") or "",
        role_data.get("executive_director") or "",
        role_data.get("supervisor") or "",
    )

    merged = dict(extracted_data)
    for key in ("legal_person", "executive_director", "chairman", "manager", "supervisor", "management_roles_summary"):
        candidate = role_data.get(key)
        if candidate and not _is_company_articles_role_missing_or_invalid(candidate):
            merged[key] = candidate

    existing_lines = [str(line or "").strip() for line in evidence_lines if str(line or "").strip()]
    appended_lines = [str(line or "").strip() for line in role_evidence_lines if str(line or "").strip()]
    deduped_lines: list[str] = []
    seen: set[str] = set()
    for line in existing_lines + appended_lines:
        if line not in seen:
            seen.add(line)
            deduped_lines.append(line)
    if deduped_lines:
        merged["management_role_evidence_lines"] = deduped_lines

    return merged


async def _load_accessible_document(doc_id: str, current_user: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        document = await storage_service.get_document(doc_id)
    except Exception as exc:
        logger.error("Failed to load document metadata %s: %s", doc_id, exc)
        raise HTTPException(status_code=500, detail="获取资料详情失败") from exc

    if not document:
        raise HTTPException(status_code=404, detail="未找到该资料记录")

    customer_id = document.get("customer_id") or ""
    customer = await storage_service.get_customer(customer_id) if customer_id else None
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该资料所属客户")
    await _ensure_local_customer_access(customer, current_user)
    return document, customer


@router.get("", response_model=list[CustomerListItem])
async def list_customers(
    search: str = Query(default="", description="Search filter for customer name"),
    current_user: dict = Depends(get_current_user),
) -> list[CustomerListItem]:
    """List customers from storage (local SQLite or Feishu Bitable).

    For normal users: only returns customers where "上传账号" matches their username.
    For admin users: returns all customers.
    Optionally filters by customer name if search param is provided.

    Args:
        search: Optional search string to filter by customer name.
        current_user: The authenticated user.

    Returns:
        list[CustomerListItem]: List of customer records.
    """
    if HAS_DB_STORAGE:
        return await _list_customers_local(search, current_user)
    else:
        return await _list_customers_feishu(search, current_user)


async def _list_customers_local(
    search: str,
    current_user: dict,
) -> list[CustomerListItem]:
    """List customers from local SQLite database."""
    try:
        customers = await storage_service.list_customers()
    except Exception as e:
        logger.error(f"Failed to fetch customers from local storage: {e}")
        return []

    if not customers:
        return []

    is_admin = _is_admin(current_user)
    username = _get_username(current_user)
    search_text = search.strip().lower()
    result: list[CustomerListItem] = []

    for customer in customers:
        customer_name = customer.get("name") or "未命名客户"
        customer_id = customer.get("customer_id") or ""
        created_at = customer.get("created_at") or ""

        if not is_admin and not _can_access_local_customer(customer, current_user):
            continue

        if search_text and search_text not in customer_name.lower():
            continue

        latest_report = await storage_service.get_latest_customer_risk_report(customer_id) if customer_id else None
        latest_report_json = (latest_report or {}).get("report_json") or {}
        latest_assessment = latest_report_json.get("overall_assessment") or {}

        result.append(
            CustomerListItem(
                name=customer_name,
                record_id=customer_id,
                uploader=customer.get("uploader") or "",
                upload_time=customer.get("upload_time") or created_at,
                customer_type=customer.get("customer_type") or "enterprise",
                risk_level=latest_assessment.get("risk_level") or "",
                last_report_generated_at=(latest_report or {}).get("generated_at") or "",
                profile_version=(latest_report or {}).get("profile_version"),
            )
        )

    return result


async def _list_customers_feishu(
    search: str,
    current_user: dict,
) -> list[CustomerListItem]:
    """List customers from Feishu Bitable (legacy mode).

    Args:
        search: Optional search string to filter by customer name.
        current_user: The authenticated user.

    Returns:
        list[CustomerListItem]: List of customer records.
    """
    try:
        records = feishu_service.get_all_records()
    except Exception as e:
        logger.error(f"Failed to fetch records from Feishu: {e}")
        return []

    if not records:
        return []

    is_admin = current_user.get("role") == "admin"
    username = current_user.get("username") or ""
    search_text = search.strip().lower()

    result: list[CustomerListItem] = []

    for record in records:
        fields = record.get("fields") or {}
        record_id = record.get("record_id") or ""

        # Extract fields - handle rich text format (lessons-learned #22)
        customer_name = _extract_text_value(fields.get("企业名称"))
        uploader = _extract_text_value(fields.get("上传账号"))
        upload_time = _extract_text_value(fields.get("上传时间"))

        # Role-based filtering: non-admin users only see their own uploads
        if not is_admin and uploader != username:
            continue

        # Search filter
        if search_text and search_text not in customer_name.lower():
            continue

        result.append(
            CustomerListItem(
                name=customer_name,
                record_id=record_id,
                uploader=uploader,
                upload_time=upload_time,
            )
        )

    return result


# ============================================
# Dynamic Table Fields + Customer Table Endpoints
# 注意：/fields, /table 必须在 /{record_id} 之前注册，否则 FastAPI 会把它们当成 record_id
# ============================================


class UpdateFieldNameRequest(BaseModel):
    """更新字段显示名的请求体"""
    field_name: str


class UpdateCustomerFieldRequest(BaseModel):
    """更新客户字段的请求体"""
    field: str
    value: str


# 允许通过 PATCH 更新的客户字段（白名单）
_UPDATABLE_CUSTOMER_FIELDS = {
    "name", "loan_amount", "loan_purpose", "income_source",
    "monthly_income", "credit_score", "status",
}


@router.get("/fields")
async def get_table_fields(
    current_user: dict = Depends(get_current_user),
) -> list[dict]:
    """获取动态表头字段配置列表。

    Returns:
        list[dict]: 字段配置，每项含 field_id/field_name/field_key/doc_type/field_order/editable。

    Raises:
        HTTPException: 500 if database error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    try:
        return await storage_service.get_table_fields()
    except Exception as e:
        logger.error(f"Failed to fetch table fields: {e}")
        raise HTTPException(status_code=500, detail="获取字段配置失败") from e


@router.patch("/fields/{field_id}")
async def update_table_field(
    field_id: str,
    body: UpdateFieldNameRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """更新表头字段的显示名称。

    Args:
        field_id: 字段唯一标识。
        body: 包含新 field_name 的请求体。
        current_user: 当前登录用户。

    Returns:
        dict: {"success": True}

    Raises:
        HTTPException: 404 if not found, 500 if error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    try:
        updated = await storage_service.update_table_field(field_id, body.field_name)
    except Exception as e:
        logger.error(f"Failed to update field {field_id}: {e}")
        raise HTTPException(status_code=500, detail="更新字段名称失败") from e

    if not updated:
        raise HTTPException(status_code=404, detail="未找到该字段")

    return {"success": True}


@router.get("/table")
async def get_customers_table(
    current_user: dict = Depends(get_current_user),
) -> list[dict]:
    """获取所有客户的动态汇总表格数据。

    每行 = 一个客户，列 = 动态字段（从 table_fields 配置读取）。
    个人客户若与某企业客户的法定代表人姓名匹配，则合并到企业行，不单独显示。

    Args:
        current_user: 当前登录用户。

    Returns:
        list[dict]: 客户动态数据列表。

    Raises:
        HTTPException: 500 if database error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    try:
        customers = await storage_service.list_customers()
    except Exception as e:
        logger.error(f"Failed to fetch customers for table: {e}")
        raise HTTPException(status_code=500, detail="获取客户数据失败") from e

    is_admin = _is_admin(current_user)
    username = _get_username(current_user)

    # 第一步：构建所有行的基础数据
    all_rows: list[dict] = []
    for c in customers:
        if not is_admin and not _can_access_local_customer(c, current_user):
            continue
        customer_id = c.get("customer_id") or ""
        row: dict = {
            "customer_id": customer_id,
            "name": c.get("name") or "未命名客户",
            "customer_type": c.get("customer_type") or "enterprise",
        }
        try:
            field_data = await storage_service.get_customer_field_data(customer_id)
            row.update(field_data)
        except Exception as e:
            logger.warning(f"Failed to get field data for {customer_id}: {e}")
        all_rows.append(row)

    # 第二步：从企业行的企业征信数据中提取法定代表人姓名
    # 结构：enterprise_credit -> full -> 法定代表人信息 -> 姓名
    def _get_legal_rep_name(row: dict) -> str | None:
        """从企业行的征信数据中提取法定代表人姓名。"""
        ec = row.get("enterprise_credit")
        if not isinstance(ec, dict):
            return None
        full = ec.get("full")
        if not isinstance(full, dict):
            return None
        # 尝试多种可能的字段路径
        for key in ("法定代表人信息", "法定代表人", "企业法人信息"):
            section = full.get(key)
            if isinstance(section, dict):
                name = section.get("姓名") or section.get("名称") or section.get("法人姓名")
                if name and isinstance(name, str):
                    return name.strip()
            elif isinstance(section, str) and section.strip():
                return section.strip()
        # 也尝试顶层直接有法定代表人字段
        for key in ("法定代表人", "法人姓名", "法人代表"):
            val = full.get(key)
            if val and isinstance(val, str):
                return val.strip()
        return None

    # 建立 法人姓名 -> 企业行 的映射（企业客户）
    enterprise_rows = [r for r in all_rows if r.get("customer_type") != "personal"]
    personal_rows = [r for r in all_rows if r.get("customer_type") == "personal"]

    legal_rep_map: dict[str, dict] = {}
    for row in enterprise_rows:
        rep_name = _get_legal_rep_name(row)
        if rep_name:
            legal_rep_map[rep_name] = row
            logger.info(f"[Table] 企业 '{row['name']}' 法定代表人: {rep_name}")

    # 第三步：合并个人行到对应企业行
    merged_personal_ids: set[str] = set()
    for p_row in personal_rows:
        p_name = p_row.get("name") or ""
        if p_name in legal_rep_map:
            enterprise_row = legal_rep_map[p_name]
            # 把个人征信数据合并到企业行的 personal_credit 列
            personal_credit = p_row.get("personal_credit")
            if personal_credit:
                enterprise_row["personal_credit"] = personal_credit
            merged_personal_ids.add(p_row["customer_id"])
            logger.info(f"[Table] 个人 '{p_name}' 合并到企业 '{enterprise_row['name']}'")

    # 第四步：过滤掉已合并的个人行，返回最终结果
    return [r for r in all_rows if r["customer_id"] not in merged_personal_ids]


# ============================================
# Customer Detail Endpoint
# ============================================

@router.get("/{record_id}", response_model=CustomerDetail)
async def get_customer_detail(
    record_id: str,
    current_user: dict = Depends(get_current_user),
) -> CustomerDetail:
    """Get customer detail by record ID from storage (local SQLite or Feishu Bitable).

    Returns ALL fields for a single customer record.
    Non-admin users can only view records they uploaded.

    Args:
        record_id: The record ID (customer_id for local storage, record_id for Feishu).
        current_user: The authenticated user.

    Returns:
        CustomerDetail: Full customer detail with all fields.

    Raises:
        HTTPException: 404 if record not found, 403 if access denied.
    """
    if HAS_DB_STORAGE:
        return await _get_customer_detail_local(record_id, current_user)
    else:
        return await _get_customer_detail_feishu(record_id, current_user)


async def _get_customer_detail_local(
    record_id: str,
    current_user: dict,
) -> CustomerDetail:
    """Get customer detail from local SQLite database.

    Args:
        record_id: The customer_id.
        current_user: The authenticated user.

    Returns:
        CustomerDetail: Full customer detail with all fields.

    Raises:
        HTTPException: 404 if record not found, 403 if access denied.
    """
    try:
        customer = await storage_service.get_customer(record_id)
    except Exception as e:
        logger.error(f"Failed to fetch customer from local storage: {e}")
        raise HTTPException(status_code=500, detail="获取客户记录失败") from e

    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")

    customer_name = customer.get("name") or "未命名客户"
    created_at = customer.get("created_at") or ""
    await _ensure_local_customer_access(customer, current_user)

    # 获取该客户的所有提取结果
    all_fields: dict[str, Any] = {}

    try:
        extractions = await storage_service.get_extractions_by_customer(record_id)

        # 将所有提取结果合并到 all_fields，保持原始数据结构
        for extraction in extractions:
            extracted_data = extraction.get("extracted_data") or {}
            extraction.get("extraction_type") or "未知类型"

            # 如果 extracted_data 是字典，直接添加到 all_fields
            # 使用提取类型作为分组名称，避免字段名冲突
            if isinstance(extracted_data, dict):
                # 如果 extracted_data 已经是嵌套字典，直接使用
                # 例如：{"报告基础信息": {"报告编号": "xxx", "报告时间": "xxx"}}
                for key, value in extracted_data.items():
                    # 保持原始数据结构，不转换为字符串
                    all_fields[key] = value
    except Exception as e:
        logger.error(f"Failed to fetch extractions for customer {record_id}: {e}")
        # 如果获取提取结果失败，至少返回基本信息
        all_fields = {
            "客户ID": customer.get("customer_id") or "",
            "客户名称": customer_name,
            "状态": customer.get("status") or "",
            "创建时间": created_at,
        }

    return CustomerDetail(
        name=customer_name,
        record_id=record_id,
        uploader=customer.get("uploader") or "",
        upload_time=customer.get("upload_time") or created_at,
        fields=all_fields,
    )


async def _get_customer_detail_feishu(
    record_id: str,
    current_user: dict,
) -> CustomerDetail:
    """Get customer detail from Feishu Bitable (legacy mode).

    Args:
        record_id: The Feishu record ID.
        current_user: The authenticated user.

    Returns:
        CustomerDetail: Full customer detail with all fields.

    Raises:
        HTTPException: 404 if record not found, 403 if access denied.
    """
    try:
        records = feishu_service.get_all_records()
    except Exception as e:
        logger.error(f"Failed to fetch records from Feishu: {e}")
        raise HTTPException(status_code=500, detail="获取飞书记录失败") from e

    if not records:
        raise HTTPException(status_code=404, detail="未找到该客户记录")

    # Find the record by record_id
    target_record = None
    for record in records:
        if record.get("record_id") == record_id:
            target_record = record
            break

    if target_record is None:
        raise HTTPException(status_code=404, detail="未找到该客户记录")

    fields = target_record.get("fields") or {}

    # Extract key fields - handle rich text format (lessons-learned #22)
    customer_name = _extract_text_value(fields.get("企业名称"))
    uploader = _extract_text_value(fields.get("上传账号"))
    upload_time = _extract_text_value(fields.get("上传时间"))

    # Role-based access: non-admin can only view their own uploads
    is_admin = current_user.get("role") == "admin"
    username = current_user.get("username") or ""

    if not is_admin and uploader != username:
        raise HTTPException(status_code=403, detail="无权查看该客户记录")

    # Build all fields as key-value string pairs
    # Use _extract_text_value for all values (lessons-learned #22)
    all_fields: dict[str, str] = {}
    for field_name, field_value in fields.items():
        text_value = _extract_text_value(field_value)
        # Use `or ""` to handle None from _extract_text_value (lessons-learned #16)
        all_fields[field_name] = text_value or ""

    return CustomerDetail(
        name=customer_name or "",
        record_id=record_id,
        uploader=uploader or "",
        upload_time=upload_time or "",
        fields=all_fields,
    )


# ============================================
# Extraction Data Endpoints
# ============================================


def _build_profile_response(
    customer: dict[str, Any],
    profile: dict[str, Any],
    auto_generated: bool,
) -> CustomerProfileMarkdownResponse:
    return CustomerProfileMarkdownResponse(
        customer_id=customer.get("customer_id") or "",
        customer_name=customer.get("name") or "",
        markdown_content=profile.get("markdown_content") or "",
        source_mode=profile.get("source_mode") or "auto",
        auto_generated=auto_generated,
        version=profile.get("version") or 1,
        updated_at=profile.get("updated_at"),
        rag_source_priority=profile.get("rag_source_priority") or get_rag_source_priority(),
        risk_report_schema=profile.get("risk_report_schema") or get_risk_report_schema_template(),
    )


@router.get("/{customer_id}/profile-markdown", response_model=CustomerProfileMarkdownResponse)
async def get_customer_profile_markdown(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> CustomerProfileMarkdownResponse:
    """Get markdown profile for a customer."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持资料汇总 Markdown")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        profile, auto_generated = await get_or_create_customer_profile(storage_service, customer_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Failed to get profile markdown for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="获取资料汇总失败") from exc

    return _build_profile_response(customer, profile, auto_generated)


@router.put("/{customer_id}/profile-markdown", response_model=CustomerProfileMarkdownResponse)
async def update_customer_profile_markdown(
    customer_id: str,
    body: UpdateCustomerProfileMarkdownRequest,
    current_user: dict = Depends(get_current_user),
) -> CustomerProfileMarkdownResponse:
    """Save markdown profile edits for a customer."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持资料汇总 Markdown")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        profile = await storage_service.upsert_customer_profile(
            {
                "customer_id": customer_id,
                "title": body.title or f"{customer.get('name') or customer_id}资料汇总",
                "markdown_content": body.markdown_content,
                "source_mode": "manual",
                "source_snapshot": {
                    "saved_by": current_user.get("username") or "",
                    "customer_name": customer.get("name") or "",
                },
                "rag_source_priority": get_rag_source_priority(),
                "risk_report_schema": get_risk_report_schema_template(),
            }
        )
        await profile_sync_service.handle_profile_markdown_saved(storage_service, customer_id)
        add_activity(
            activity_type="profile",
            customer=customer.get("name") or "",
            customer_id=customer_id,
            username=current_user.get("username") or "",
            status="completed",
            title="资料汇总已保存",
            description="已保存客户资料汇总，资料问答和风险评估将优先使用最新版本。",
            metadata={
                "profileVersion": profile.get("version") or 1,
                "updatedAt": profile.get("updated_at") or "",
                "sourceMode": profile.get("source_mode") or "manual",
            },
        )
    except Exception as exc:
        logger.error("Failed to save profile markdown for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="保存资料汇总失败") from exc

    return _build_profile_response(customer, profile, False)


@router.delete("/{customer_id}/profile-markdown")
async def delete_customer_profile_markdown(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """Delete markdown profile and rebuild an auto-generated baseline."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持资料汇总 Markdown")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        await storage_service.delete_customer_profile(customer_id)
        rebuilt_profile = await regenerate_customer_profile(storage_service, customer_id)
        await profile_sync_service.handle_profile_markdown_saved(storage_service, customer_id)
        add_activity(
            activity_type="profile",
            customer=customer.get("name") or "",
            customer_id=customer_id,
            username=current_user.get("username") or "",
            status="completed",
            title="资料汇总已恢复系统整理",
            description="系统已删除手动版本，并按当前客户资料重新生成默认资料汇总。",
            metadata={
                "profileVersion": (rebuilt_profile or {}).get("version") or 1,
                "updatedAt": (rebuilt_profile or {}).get("updated_at") or "",
                "sourceMode": (rebuilt_profile or {}).get("source_mode") or "auto",
            },
        )
    except Exception as exc:
        logger.error("Failed to delete/reset profile markdown for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="删除资料汇总失败") from exc

    return {"success": True}


@router.post("/{customer_id}/rag-chat", response_model=CustomerRagChatResponse)
async def customer_rag_chat(
    customer_id: str,
    body: CustomerRagChatRequest,
    current_user: dict = Depends(get_current_user),
) -> CustomerRagChatResponse:
    """Answer a question strictly from the current customer_id scoped materials."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持客户级资料问答")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    question = (body.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="question is required")

    try:
        result = await rag_service.answer_question(storage_service, customer_id, question)
    except Exception as exc:
        logger.error("RAG chat failed for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="客户资料问答失败") from exc

    return CustomerRagChatResponse(
        answer=result.get("answer") or "",
        evidence=result.get("evidence") or [],
        missing_info=result.get("missing_info") or [],
    )


@router.post("/{customer_id}/risk-report/generate", response_model=CustomerRiskReportResponse)
async def generate_customer_risk_report(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> CustomerRiskReportResponse:
    """Generate a structured risk report strictly scoped to the current customer_id."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="褰撳墠妯″紡鏆備笉鏀寔瀹㈡埛椋庨櫓璇勪及鎶ュ憡")
    try:
        payload = await _build_customer_risk_report_payload(customer_id, current_user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Risk report generation failed for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="鐢熸垚椋庨櫓璇勪及鎶ュ憡澶辫触") from exc

    return CustomerRiskReportResponse(**payload)


@router.post("/{customer_id}/risk-report/jobs", response_model=ChatJobCreateResponse)
async def create_customer_risk_report_job(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> ChatJobCreateResponse:
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="褰撳墠妯″紡鏆備笉鏀寔瀹㈡埛椋庨櫓璇勪及鎶ュ憡")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="鏈壘鍒拌瀹㈡埛璁板綍")
    await _ensure_local_customer_access(customer, current_user)

    job_id = uuid.uuid4().hex
    request_payload = {
        "customerId": customer_id,
        "customerName": customer.get("name") or "",
    }
    execution_payload = _build_risk_report_job_execution_payload(
        job_id,
        customer_id,
        customer.get("name") or "",
        current_user,
    )
    logger.info(
        "[Risk Job] execution payload prepared job_id=%s customer_id=%s username=%s payload_keys=%s",
        job_id,
        customer_id,
        current_user.get("username") or "",
        sorted(execution_payload.keys()),
    )
    await storage_service.create_async_job(
        {
            "job_id": job_id,
            "job_type": "risk_report",
            "customer_id": customer_id,
            "username": current_user.get("username") or "",
            "status": "pending",
            "progress_message": "任务已创建，等待后台处理",
            "request_json": request_payload,
            "execution_payload_json": execution_payload,
        }
    )
    logger.info(
        "[Risk Job] created job_id=%s job_type=%s username=%s customer_id=%s request_snapshot=%s",
        job_id,
        "risk_report",
        current_user.get("username") or "",
        customer_id,
        request_payload,
    )
    stored_execution_payload = await storage_service.get_async_job_execution_payload(job_id)
    if not stored_execution_payload:
        logger.warning(
            "[Risk Job] execution payload missing after create job_id=%s, retrying persistence",
            job_id,
        )
        await storage_service.set_async_job_execution_payload(job_id, execution_payload)
        stored_execution_payload = await storage_service.get_async_job_execution_payload(job_id)
    if not stored_execution_payload:
        logger.error(
            "[Risk Job] execution payload save failed job_id=%s customer_id=%s username=%s",
            job_id,
            customer_id,
            current_user.get("username") or "",
        )
        raise HTTPException(status_code=500, detail="风险报告任务载荷保存失败")
    logger.info(
        "[Risk Job] execution payload saved job_id=%s customer_id=%s username=%s payload_keys=%s payload_customer_name=%s",
        job_id,
        customer_id,
        current_user.get("username") or "",
        sorted(stored_execution_payload.keys()),
        stored_execution_payload.get("customerName") if isinstance(stored_execution_payload, dict) else "",
    )
    await _dispatch_customer_risk_report_job(
        job_id,
        customer_id,
        current_user,
    )
    return ChatJobCreateResponse(jobId=job_id, status="pending")


@router.get("/{customer_id}/risk-reports/history", response_model=CustomerRiskReportHistoryResponse)
async def get_customer_risk_report_history(
    customer_id: str,
    limit: int = Query(default=5, ge=1, le=20),
    current_user: dict = Depends(get_current_user),
) -> CustomerRiskReportHistoryResponse:
    """Get recent risk report history for one customer."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持风险报告历史")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        items = await storage_service.list_customer_risk_reports(customer_id, limit=limit)
    except Exception as exc:
        logger.error("Failed to load risk report history for %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="获取风险报告历史失败") from exc

    return CustomerRiskReportHistoryResponse(
        items=[CustomerRiskReportHistoryItem(**item) for item in items]
    )

class ExtractionItem(BaseModel):
    """单条 extraction 记录"""
    extraction_id: str
    extraction_type: str
    extracted_data: dict[str, Any]
    created_at: str


class ExtractionGroup(BaseModel):
    """按文档类型分组的 extraction 数据"""
    extraction_type: str
    items: list[ExtractionItem]


class UpdateExtractionRequest(BaseModel):
    """更新 extraction 字段的请求体"""
    field: str
    value: str


@router.get("/{customer_id}/extractions", response_model=list[ExtractionGroup])
async def get_customer_extractions(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> list[ExtractionGroup]:
    """获取客户的所有 extraction 数据，按文档类型分组。

    Args:
        customer_id: 客户 ID。
        current_user: 当前登录用户。

    Returns:
        list[ExtractionGroup]: 按文档类型分组的 extraction 列表。

    Raises:
        HTTPException: 500 if database error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        extractions = await storage_service.get_extractions_by_customer(customer_id)
    except Exception as e:
        logger.error(f"Failed to fetch extractions for {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="获取资料数据失败") from e

    groups: dict[str, list[ExtractionItem]] = {}
    for ext in extractions:
        ext_type = ext.get("extraction_type") or "未知类型"
        extracted_data = ext.get("extracted_data") or {}
        if ext_type == "company_articles":
            document = None
            doc_id = ext.get("doc_id") or ""
            if doc_id:
                try:
                    document = await storage_service.get_document(doc_id)
                except Exception as exc:
                    logger.warning("company_articles enrich document load failed doc_id=%s error=%s", doc_id, exc)
            extracted_data = _merge_company_articles_front_page_roles(extracted_data, document)
        item = ExtractionItem(
            extraction_id=ext.get("extraction_id") or "",
            extraction_type=ext_type,
            extracted_data=extracted_data,
            created_at=ext.get("created_at") or "",
        )
        groups.setdefault(ext_type, []).append(item)

    return [
        ExtractionGroup(extraction_type=ext_type, items=items)
        for ext_type, items in groups.items()
    ]


@router.get("/{customer_id}/documents", response_model=list[CustomerDocumentListItem])
async def get_customer_documents(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> list[CustomerDocumentListItem]:
    """Return customer documents with original-retention status for UI lists."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        documents = await storage_service.list_documents(customer_id)
    except Exception as exc:
        logger.error("Failed to load documents for customer %s: %s", customer_id, exc)
        raise HTTPException(status_code=500, detail="获取客户资料列表失败") from exc

    latest_seen_types: set[str] = set()
    items: list[CustomerDocumentListItem] = []

    for document in documents:
        file_type = str(document.get("file_type") or "")
        original_available, original_status = _build_document_original_status(document)
        definition = get_document_type_definition(file_type)
        is_latest = file_type not in latest_seen_types
        latest_seen_types.add(file_type)

        items.append(
            CustomerDocumentListItem(
                doc_id=document.get("doc_id") or "",
                customer_id=document.get("customer_id") or customer_id,
                file_name=document.get("file_name") or "",
                file_type=file_type,
                file_type_name=get_document_display_name(file_type),
                file_size=document.get("file_size") or 0,
                upload_time=document.get("upload_time") or "",
                original_available=original_available,
                original_status=original_status,
                store_original=definition.store_original if definition else True,
                is_latest=is_latest,
            )
        )

    return items


@documents_router.get("/{doc_id}")
async def get_document_detail(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Return document metadata plus original-retention status."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    document, customer = await _load_accessible_document(doc_id, current_user)
    original_available, original_status = _build_document_original_status(document)
    definition = get_document_type_definition(document.get("file_type"))
    return {
        "doc_id": document.get("doc_id") or "",
        "customer_id": document.get("customer_id") or "",
        "customer_name": customer.get("name") or "",
        "file_name": document.get("file_name") or "",
        "file_type": document.get("file_type") or "",
        "file_type_name": get_document_display_name(document.get("file_type")),
        "file_size": document.get("file_size") or 0,
        "upload_time": document.get("upload_time") or "",
        "original_available": original_available,
        "original_status": original_status,
        "store_original": definition.store_original if definition else True,
    }


@documents_router.get("/{doc_id}/download")
async def download_document_original(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Download the retained original file for one document."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    document, _customer = await _load_accessible_document(doc_id, current_user)
    original_available, original_status = _build_document_original_status(document)
    if not original_available:
        logger.warning("document download blocked doc_id=%s reason=%s", doc_id, original_status)
        raise HTTPException(status_code=409, detail=original_status)

    absolute_path = _resolve_document_absolute_path(document.get("file_path"))
    if not absolute_path:
        logger.warning("document download missing path doc_id=%s", doc_id)
        raise HTTPException(status_code=409, detail=DOCUMENT_FILE_MISSING_MESSAGE)

    logger.info("document download ready doc_id=%s path=%s", doc_id, absolute_path)
    return FileResponse(path=absolute_path, filename=document.get("file_name") or absolute_path.name)


@documents_router.get("/{doc_id}/preview")
async def preview_document_original(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Preview the retained original file inline when possible."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    document, _customer = await _load_accessible_document(doc_id, current_user)
    original_available, original_status = _build_document_original_status(document)
    if not original_available:
        logger.warning("document preview blocked doc_id=%s reason=%s", doc_id, original_status)
        raise HTTPException(status_code=409, detail=original_status)

    absolute_path = _resolve_document_absolute_path(document.get("file_path"))
    if not absolute_path:
        logger.warning("document preview missing path doc_id=%s", doc_id)
        raise HTTPException(status_code=409, detail=DOCUMENT_FILE_MISSING_MESSAGE)

    media_type_map = {
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }
    media_type = media_type_map.get(absolute_path.suffix.lower())
    file_name = document.get("file_name") or absolute_path.name
    encoded_file_name = quote(file_name)
    logger.info("document preview ready doc_id=%s path=%s media_type=%s", doc_id, absolute_path, media_type)
    return FileResponse(
        path=absolute_path,
        filename=file_name,
        media_type=media_type,
        headers={"Content-Disposition": f"inline; filename*=UTF-8''{encoded_file_name}"},
    )


@router.patch("/{customer_id}/fields")
async def update_customer_field(
    customer_id: str,
    body: UpdateCustomerFieldRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """更新客户某个字段的值。

    Args:
        customer_id: 客户 ID。
        body: 包含 field 和 value 的请求体。
        current_user: 当前登录用户。

    Returns:
        dict: {"success": True}

    Raises:
        HTTPException: 400 if field not allowed, 404 if not found, 500 if error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    if body.field not in _UPDATABLE_CUSTOMER_FIELDS:
        raise HTTPException(status_code=400, detail=f"字段 '{body.field}' 不允许修改")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        updated = await storage_service.update_customer(customer_id, {body.field: body.value})
    except Exception as e:
        logger.error(f"Failed to update customer {customer_id} field {body.field}: {e}")
        raise HTTPException(status_code=500, detail="更新客户字段失败") from e

    if not updated:
        raise HTTPException(status_code=404, detail="未找到该客户记录")

    return {"success": True}


@router.patch("/{customer_id}/extractions/{extraction_id}")
async def update_customer_extraction(
    customer_id: str,
    extraction_id: str,
    body: UpdateExtractionRequest,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """更新 extraction 中某个字段的值。

    Args:
        customer_id: 客户 ID（用于日志）。
        extraction_id: 要更新的 extraction ID。
        body: 包含 field 和 value 的请求体。
        current_user: 当前登录用户。

    Returns:
        dict: {"success": True}

    Raises:
        HTTPException: 404 if not found, 500 if database error.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    extraction = await storage_service.get_extraction(extraction_id)
    if not extraction or extraction.get("customer_id") != customer_id:
        raise HTTPException(status_code=404, detail="未找到该 extraction 记录")

    try:
        updated = await storage_service.update_extraction(
            extraction_id, body.field, body.value
        )
    except Exception as e:
        logger.error(f"Failed to update extraction {extraction_id}: {e}")
        raise HTTPException(status_code=500, detail="更新资料失败") from e

    if not updated:
        raise HTTPException(status_code=404, detail="未找到该 extraction 记录")

    return {"success": True}


@router.delete("/{customer_id}")
async def delete_customer(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """Delete a customer row and all related documents/extractions."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        deleted = await storage_service.delete_customer(customer_id)
    except Exception as e:
        logger.error(f"Failed to delete customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="删除客户失败") from e

    if not deleted:
        raise HTTPException(status_code=404, detail="未找到该客户记录")

    return {"success": True}


@router.delete("/{customer_id}/documents/{doc_id}")
async def delete_customer_document(
    customer_id: str,
    doc_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, bool]:
    """Delete a single uploaded document and its linked extraction records."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        document = await storage_service.get_document(doc_id)
    except Exception as e:
        logger.error(f"Failed to fetch document {doc_id}: {e}")
        raise HTTPException(status_code=500, detail="删除资料失败") from e

    if not document or document.get("customer_id") != customer_id:
        raise HTTPException(status_code=404, detail="未找到该资料记录")

    try:
        deleted = await storage_service.delete_document(doc_id)
    except Exception as e:
        logger.error(f"Failed to delete document {doc_id}: {e}")
        raise HTTPException(status_code=500, detail="删除资料失败") from e

    if not deleted:
        raise HTTPException(status_code=404, detail="未找到该资料记录")

    return {"success": True}

