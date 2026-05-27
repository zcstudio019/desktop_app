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
import time
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
from backend.document_types import get_document_display_name, get_document_type_definition, normalize_document_type_code, should_store_original
from backend.services import get_storage_service, supports_structured_storage  # 新增：存储服务 factory

from backend.services.markdown_profile_service import (
    get_or_create_customer_profile,
    get_or_reparse_customer_profile,
    get_rag_source_priority,
    get_risk_report_schema_template,
    regenerate_customer_profile,
)
from backend.services.enterprise_bank_statement_agent.customer_flow_aggregator import (
    ENTERPRISE_FLOW_TYPES,
    aggregate_customer_enterprise_flows,
)
from backend.services.personal_bank_statement_agent.customer_flow_aggregator import (
    PERSONAL_FLOW_TYPES,
    aggregate_customer_personal_flows,
)
from backend.services.personal_bank_statement_agent.income_confirmation_service import (
    list_income_confirmations,
    save_income_confirmation,
)
from backend.services.enterprise_bank_statement_agent.flow_rules import (
    get_enterprise_flow_rules,
    save_enterprise_flow_rules,
    update_transaction_review,
)
from backend.services.document_extractor_service import (
    extract_company_articles_management_roles,
    extract_company_articles_role_evidence_lines,
)
from backend.routers.file import _load_historical_financial_reports, _process_file_bytes
from backend.services.activity_service import add_activity
from backend.services.profile_sync_service import ProfileSyncService
from backend.services.rag_service import RagService
from backend.services.risk_assessment_service import RiskAssessmentService
from backend.services.financial_report_agent.display_mapper import to_display_json as to_financial_report_display_json
from backend.services.financial_report_agent.markdown_renderer import render_financial_report_markdown
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


class EnterpriseFlowRulesPayload(BaseModel):
    related_company_names: list[str] = []
    self_account_numbers: list[str] = []
    internal_transfer_keywords: list[str] = []
    operating_counterparty_whitelist: list[str] = []
    internal_counterparty_blacklist: list[str] = []
    personal_counterparty_names: list[str] = []
    manual_overrides: dict[str, Any] = {}


class EnterpriseFlowTransactionReviewPayload(BaseModel):
    nature: str
    exclude_from_operating: bool = False
    manual_reason: str = ""
    reviewed_by: str = ""


class PersonalFlowIncomeConfirmationPayload(BaseModel):
    income_type: str = "suspected_salary"
    target_type: str = "confirmed_salary"
    counterparty_name: str
    amount: float = 0
    months: list[str] = []
    transaction_ids: list[str] = []
    manual_status: str = "confirmed"
    reason: str = ""


def _model_payload(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


# Create router
router = APIRouter(prefix="/customers", tags=["Customers"])
documents_router = APIRouter(prefix="/documents", tags=["Documents"])

# Initialize service
feishu_service = FeishuService()
storage_service = get_storage_service()  # 根据配置返回本地存储或飞书服务
HAS_DB_STORAGE = supports_structured_storage(storage_service)


async def _refresh_customer_after_document_delete(customer_id: str, doc_id: str) -> None:
    try:
        logger.info("[DocumentDelete] background refresh start customer_id=%s document_id=%s", customer_id, doc_id)
        await profile_sync_service.handle_document_saved(storage_service, customer_id)
        logger.info("[DocumentDelete] background refresh finish customer_id=%s document_id=%s", customer_id, doc_id)
    except Exception:
        logger.exception("[DocumentDelete] background refresh failed customer_id=%s document_id=%s", customer_id, doc_id)

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
    try:
        rules = {**get_enterprise_flow_rules(customer_id), "customer_name": customer.get("name") or customer_id, "customer_id": customer_id}
        extractions = await storage_service.list_extractions_by_types(customer_id, list(ENTERPRISE_FLOW_TYPES)) if callable(getattr(storage_service, "list_extractions_by_types", None)) else []
        flow_summary = aggregate_customer_enterprise_flows(extractions, rules=rules)
        flow = flow_summary.get("summary") or {}
        raw_total_flow = float(flow.get("raw_total_inflow") or 0) + float(flow.get("raw_total_outflow") or 0)
        internal_total = float(flow.get("internal_transfer_total") or 0)
        risk_flags = []
        if raw_total_flow and internal_total / raw_total_flow > 0.2:
            risk_flags.append("内部转账占比较高")
        if float(flow.get("operating_inflow") or 0) < float(flow.get("raw_total_inflow") or 0) * 0.7:
            risk_flags.append("原始流水与银行认可流水差异较大")
        if float(flow.get("personal_transfer_inflow") or 0) + float(flow.get("personal_transfer_outflow") or 0) > 0:
            risk_flags.append("个人往来较多")
        if float(flow.get("operating_net_cashflow") or 0) < 0:
            risk_flags.append("经营性净流入偏弱")
        report_json = result.get("report_json") or {}
        report_json["enterprise_flow_analysis"] = {
            "raw_total_inflow": flow.get("raw_total_inflow"),
            "raw_total_outflow": flow.get("raw_total_outflow"),
            "operating_inflow": flow.get("operating_inflow"),
            "operating_outflow": flow.get("operating_outflow"),
            "operating_net_cashflow": flow.get("operating_net_cashflow"),
            "internal_transfer_total": flow.get("internal_transfer_total"),
            "related_party_total": float(flow.get("related_party_inflow") or 0) + float(flow.get("related_party_outflow") or 0),
            "personal_transfer_total": float(flow.get("personal_transfer_inflow") or 0) + float(flow.get("personal_transfer_outflow") or 0),
            "review_status": {
                "manual_reviewed_count": flow.get("reviewed_transaction_count") or 0,
                "unreviewed_suspicious_count": flow.get("unreviewed_suspicious_count") or 0,
            },
            "risk_flags": risk_flags,
        }
        result["report_json"] = report_json
        logger.info("[RiskReport][EnterpriseFlow] operating_inflow=%s internal_transfer_total=%s", flow.get("operating_inflow"), flow.get("internal_transfer_total"))
    except Exception as exc:
        logger.warning("[RiskReport][EnterpriseFlow] attach_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
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


async def _has_uploaded_document_access(customer_id: str, current_user: dict) -> bool:
    if _is_admin(current_user):
        return True
    username = _get_username(current_user)
    if not customer_id or not username:
        return False
    try:
        checker = getattr(storage_service, "customer_has_document_uploader", None)
        if callable(checker):
            return bool(await checker(customer_id, username))
        documents = await storage_service.list_documents(customer_id)
    except Exception as exc:
        logger.warning(
            "[Customer Access] document uploader check failed customer_id=%s username=%s error=%s",
            customer_id,
            username,
            exc,
        )
        return False
    return any(str(document.get("uploader") or "") == username for document in documents)


async def _can_access_local_customer_async(customer: dict[str, Any], current_user: dict) -> bool:
    if _has_direct_local_customer_access(customer, current_user):
        return True
    return await _has_uploaded_document_access(str(customer.get("customer_id") or ""), current_user)


async def _ensure_local_customer_access(customer: dict[str, Any], current_user: dict) -> None:
    if not await _can_access_local_customer_async(customer, current_user):
        raise HTTPException(status_code=403, detail="无权查看该客户记录")


def _customer_access_log_tuple(customer: dict[str, Any]) -> tuple[Any, Any, Any, Any, Any]:
    owner = customer.get("owner") or customer.get("owner_username") or customer.get("uploader") or ""
    created_by = customer.get("created_by") or customer.get("creator") or customer.get("uploader") or ""
    username = customer.get("username") or customer.get("uploader") or ""
    return (
        customer.get("id") or customer.get("customer_id") or "",
        customer.get("name") or "",
        created_by,
        owner,
        username,
    )


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

        has_document_uploader_access = False
        if not is_admin:
            has_direct_access = _has_direct_local_customer_access(customer, current_user)
            if not has_direct_access:
                has_document_uploader_access = await _has_uploaded_document_access(customer_id, current_user)
                if not has_document_uploader_access:
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
                uploader=customer.get("uploader") or (username if has_document_uploader_access else ""),
                upload_time=customer.get("upload_time") or created_at,
                customer_type=customer.get("customer_type") or "enterprise",
                risk_level=latest_assessment.get("risk_level") or "",
                last_report_generated_at=(latest_report or {}).get("generated_at") or "",
                profile_version=(latest_report or {}).get("profile_version"),
            )
        )

    returned_customer_ids = {item.record_id for item in result}
    logger.info(
        "[Customer Access] username=%s role=%s returned_customers=%s",
        username,
        current_user.get("role") or "",
        [
            _customer_access_log_tuple(customer)
            for customer in customers
            if is_admin or customer.get("customer_id") in returned_customer_ids
        ],
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
        if not is_admin and not await _can_access_local_customer_async(c, current_user):
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
        credit_debug=profile.get("credit_debug") or (profile.get("source_snapshot") or {}).get("credit_debug") or {},
    )


@router.get("/{customer_id}/profile-markdown", response_model=CustomerProfileMarkdownResponse)
async def get_customer_profile_markdown(
    customer_id: str,
    force: bool = Query(default=False, description="Force rebuild profile from current uploaded materials"),
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
        profile, auto_generated = await get_or_reparse_customer_profile(
            storage_service,
            customer_id,
            force_reparse=force,
        )
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


@router.post("/{customer_id}/parse-credit-report")
async def force_reparse_credit_report_profile(
    customer_id: str,
    force: bool = Query(False),
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Force rebuild the customer profile from current parsed materials.

    This endpoint intentionally bypasses the saved profile cache. It does not
    re-run OCR when the original file is unavailable, but it does rebuild the
    enterprise-credit display through the latest final normalization layer.
    """
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="当前模式暂不支持资料汇总 Markdown")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        if force:
            await storage_service.delete_customer_profile(customer_id)
        profile = await regenerate_customer_profile(storage_service, customer_id)
        await profile_sync_service.handle_profile_markdown_saved(storage_service, customer_id)
        markdown_content = (profile or {}).get("markdown_content") or ""
        debug = {
            "credit_parser_version": "credit-parser-fix-2026-05-09",
            "parser_path": "profile_markdown_cached_result",
            "force": force,
            "contains_finance_lease_in_short_term": "短期借款" in markdown_content and "融资型租赁" in markdown_content,
            "contains_invalid_institution_company": "机构：公司" in markdown_content,
        }
        logger.warning("[EnterpriseCredit][FORCE_REPARSE] customer_id=%s debug=%s", customer_id, debug)
        return {
            "success": True,
            "customer_id": customer_id,
            "profile_version": (profile or {}).get("version") or 1,
            "parser_version": debug["credit_parser_version"],
            "parser_debug": debug,
        }
    except Exception as exc:
        logger.error("Failed to force reparse credit report profile for %s: %s", customer_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="强制刷新企业征信资料汇总失败") from exc


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
    doc_id: str = ""
    extraction_type: str
    extracted_data: dict[str, Any]
    created_at: str
    extraction_status: str = "success"
    has_extraction: bool = True
    summary_available: bool = True


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
    include_data: bool = Query(default=False, description="Whether to include extracted_data LONGTEXT payloads"),
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

    started_at = time.perf_counter()
    try:
        if not include_data and callable(getattr(storage_service, "list_extraction_status", None)):
            extractions = await storage_service.list_extraction_status(customer_id)
        else:
            extractions = await storage_service.get_extractions_by_customer(customer_id)
    except Exception as e:
        logger.exception("Failed to fetch extractions for %s", customer_id)
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
            doc_id=ext.get("doc_id") or "",
            extraction_type=ext_type,
            extracted_data=extracted_data,
            created_at=ext.get("created_at") or "",
            extraction_status=ext.get("extraction_status") or "success",
            has_extraction=bool(ext.get("has_extraction", True)),
            summary_available=bool(ext.get("summary_available", bool(extracted_data))),
        )
        groups.setdefault(ext_type, []).append(item)

    result = [
        ExtractionGroup(extraction_type=ext_type, items=items)
        for ext_type, items in groups.items()
    ]
    cost_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info("[DocumentStatus] extractions success customer_id=%s count=%s include_data=%s cost_ms=%s", customer_id, sum(len(group.items) for group in result), include_data, cost_ms)
    return result


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
        is_latest = bool(document.get("is_active")) if file_type == "enterprise_credit" else file_type not in latest_seen_types
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


@router.get("/{customer_id}/document-status")
async def get_customer_document_status(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Return lightweight source document status without extracted_data LONGTEXT payloads."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    started_at = time.perf_counter()
    logger.info("[DocumentStatus] start customer_id=%s", customer_id)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        if callable(getattr(storage_service, "list_document_status", None)):
            documents = await storage_service.list_document_status(customer_id)
        else:
            documents = await storage_service.list_documents(customer_id)
    except Exception as exc:
        logger.exception("[DocumentStatus] failed customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail="获取来源文档状态失败") from exc

    latest_seen_types: set[str] = set()
    items: list[dict[str, Any]] = []
    for document in documents:
        document_type = str(document.get("file_type") or document.get("document_type") or "")
        if should_store_original(document_type) and bool(document.get("file_path")):
            original_available, original_status = True, "可查看原件"
        elif should_store_original(document_type):
            original_available, original_status = False, DOCUMENT_FILE_MISSING_MESSAGE
        else:
            original_available, original_status = False, DOCUMENT_NOT_RETAINED_MESSAGE
        definition = get_document_type_definition(document_type)
        is_latest = bool(document.get("is_active")) if document_type == "enterprise_credit" else document_type not in latest_seen_types
        latest_seen_types.add(document_type)
        items.append(
            {
                "document_id": document.get("doc_id") or document.get("document_id") or "",
                "doc_id": document.get("doc_id") or document.get("document_id") or "",
                "customer_id": document.get("customer_id") or customer_id,
                "document_type": document_type,
                "file_type": document_type,
                "document_type_label": get_document_display_name(document_type),
                "file_type_name": get_document_display_name(document_type),
                "file_name": document.get("file_name") or "",
                "original_filename": document.get("original_filename") or document.get("file_name") or "",
                "file_size": document.get("file_size") or 0,
                "uploaded_at": document.get("uploaded_at") or document.get("upload_time") or "",
                "upload_time": document.get("upload_time") or document.get("uploaded_at") or "",
                "updated_at": document.get("updated_at") or document.get("uploaded_at") or document.get("upload_time") or "",
                "has_original_file": original_available,
                "can_view_original": original_available,
                "original_available": original_available,
                "original_status": original_status,
                "source_status": "available" if original_available else original_status,
                "store_original": definition.store_original if definition else True,
                "has_extraction": bool(document.get("has_extraction")),
                "extraction_id": document.get("extraction_id") or "",
                "extraction_status": document.get("extraction_status") or "",
                "summary_available": bool(document.get("summary_available")),
                "is_latest": is_latest,
            }
        )

    cost_ms = int((time.perf_counter() - started_at) * 1000)
    if cost_ms > 1000:
        logger.warning("[DocumentStatus] response_done customer_id=%s count=%s cost_ms=%s", customer_id, len(items), cost_ms)
    else:
        logger.info("[DocumentStatus] response_done customer_id=%s count=%s cost_ms=%s", customer_id, len(items), cost_ms)
    return {"items": items, "total": len(items)}


@router.get("/{customer_id}/enterprise-flow/latest")
async def get_latest_enterprise_flow_extraction(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Return only the latest enterprise_flow extraction payload for structured preview."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    started_at = time.perf_counter()
    logger.info("[LatestExtraction] start customer_id=%s document_type=%s", customer_id, "enterprise_flow")
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    types = ["enterprise_flow", "enterprise_bank_statement", "bank_statement_enterprise", "company_bank_statement", "企业流水", "银行流水"]
    try:
        if callable(getattr(storage_service, "get_latest_extraction_by_types", None)):
            extraction = await storage_service.get_latest_extraction_by_types(customer_id, types)
        else:
            extractions = await storage_service.get_extractions_by_customer(customer_id)
            normalized_types = set(types)
            extraction = next((item for item in extractions if (item.get("extraction_type") or "") in normalized_types), None)
    except Exception as exc:
        logger.exception("[EnterpriseFlowLatest] failed customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail="获取企业流水结构化结果失败") from exc

    if not extraction:
        logger.info("[LatestExtraction] success customer_id=%s document_type=%s selected=false cost_ms=%s", customer_id, "enterprise_flow", int((time.perf_counter() - started_at) * 1000))
        return {"item": None}

    extracted_data = extraction.get("extracted_data") or {}
    if not isinstance(extracted_data, dict):
        extracted_data = {}
    extracted_json = extracted_data.get("extracted_json") or extracted_data.get("data") or extracted_data
    if isinstance(extracted_json, dict) and isinstance(extracted_json.get("transactions"), list):
        extracted_json = {**extracted_json, "transactions": []}
    logger.info(
        "[LatestExtraction] selected document_id=%s extraction_id=%s",
        extraction.get("doc_id") or "",
        extraction.get("extraction_id") or "",
    )
    logger.info("[LatestExtraction] success customer_id=%s document_type=%s cost_ms=%s", customer_id, "enterprise_flow", int((time.perf_counter() - started_at) * 1000))
    return {
        "item": {
            "extraction_id": extraction.get("extraction_id") or "",
            "doc_id": extraction.get("doc_id") or "",
            "document_id": extraction.get("doc_id") or "",
            "document_type": extraction.get("extraction_type") or "enterprise_flow",
            "extraction_type": extraction.get("extraction_type") or "enterprise_flow",
            "created_at": extraction.get("created_at") or "",
            "extracted_json": extracted_json,
            "markdown_summary": "",
        }
    }


@router.get("/{customer_id}/enterprise-flow/summary")
async def get_customer_enterprise_flow_summary(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Return customer-level aggregation across all enterprise_flow extractions."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    started_at = time.perf_counter()
    logger.info("[EnterpriseFlowSummary] start customer_id=%s", customer_id)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        if callable(getattr(storage_service, "list_extractions_by_types", None)):
            extractions = await storage_service.list_extractions_by_types(customer_id, list(ENTERPRISE_FLOW_TYPES))
        else:
            all_extractions = await storage_service.get_extractions_by_customer(customer_id)
            extractions = [
                item for item in all_extractions
                if (item.get("extraction_type") or item.get("document_type") or "") in ENTERPRISE_FLOW_TYPES
            ]
        logger.info("[EnterpriseFlowSummary] loaded_extractions=%s customer_id=%s", len(extractions), customer_id)
        rules = get_enterprise_flow_rules(customer_id)
        customer_name = customer.get("name") or customer_id
        rules = {**rules, "customer_name": customer_name, "customer_id": customer_id}
        aggregated = aggregate_customer_enterprise_flows(extractions, rules=rules)
    except Exception as exc:
        logger.exception("[EnterpriseFlowSummary] failed customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail="获取客户级企业流水汇总失败") from exc

    cost_ms = int((time.perf_counter() - started_at) * 1000)
    summary = aggregated.get("summary") or {}
    logger.info(
        "[EnterpriseFlowSummary] aggregated accounts=%s total_inflow=%s total_outflow=%s cost_ms=%s",
        summary.get("account_count") or len(aggregated.get("accounts") or []),
        summary.get("total_inflow") or 0,
        summary.get("total_outflow") or 0,
        cost_ms,
    )
    return {"item": aggregated if aggregated.get("source_document_count") else None}


@router.get("/{customer_id}/personal-flow/summary")
async def get_customer_personal_flow_summary(
    customer_id: str,
    debug: bool = Query(default=False, description="Include deterministic personal-flow diagnostics"),
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Return customer-level aggregation across all personal_flow extractions."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    started_at = time.perf_counter()
    logger.info("[PersonalFlowSummary] start customer_id=%s", customer_id)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        if callable(getattr(storage_service, "list_extractions_by_types", None)):
            extractions = await storage_service.list_extractions_by_types(customer_id, list(PERSONAL_FLOW_TYPES))
        else:
            all_extractions = await storage_service.get_extractions_by_customer(customer_id)
            extractions = [
                item for item in all_extractions
                if (item.get("extraction_type") or item.get("document_type") or "") in PERSONAL_FLOW_TYPES
            ]
            enriched_extractions = []
            for item in extractions:
                document = await storage_service.get_document(item.get("doc_id")) if item.get("doc_id") else None
                enriched = dict(item)
                enriched["file_name"] = (document or {}).get("file_name") or enriched.get("file_name") or ""
                enriched["file_hash"] = (document or {}).get("file_hash") or ""
                enriched["file_size"] = (document or {}).get("file_size") or 0
                enriched["is_active"] = (document or {}).get("is_active", True)
                enriched_extractions.append(enriched)
            extractions = enriched_extractions
        extractions = [item for item in extractions if item.get("is_active") is not False]
        aggregated = aggregate_customer_personal_flows(
            extractions,
            income_confirmations=list_income_confirmations(customer_id),
        )
        if not debug:
            aggregated.pop("debug", None)
    except Exception as exc:
        logger.exception("[PersonalFlowSummary] failed customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail="获取客户级个人流水汇总失败") from exc

    cost_ms = int((time.perf_counter() - started_at) * 1000)
    summary = aggregated.get("customer_level_summary") or {}
    logger.info(
        "[PersonalFlowSummary] aggregated accounts=%s raw_income=%s raw_expense=%s cost_ms=%s",
        summary.get("account_count") or len(aggregated.get("accounts") or []),
        summary.get("raw_total_income") or 0,
        summary.get("raw_total_expense") or 0,
        cost_ms,
    )
    return {"item": aggregated if aggregated.get("source_document_count") else None}


def _ensure_flow_rule_editor(current_user: dict) -> None:
    role = str(current_user.get("role") or "")
    if role not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="当前账号无权编辑企业流水经营性口径")


def _ensure_personal_flow_income_editor(current_user: dict) -> None:
    role = str(current_user.get("role") or "")
    if role not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="当前账号无权确认或驳回个人流水收入分类")


@router.post("/{customer_id}/personal-flow/{document_id}/income-confirmations")
async def save_customer_personal_flow_income_confirmation(
    customer_id: str,
    document_id: str,
    payload: PersonalFlowIncomeConfirmationPayload,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Persist a reviewer decision for one suspected salary source."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")
    _ensure_personal_flow_income_editor(current_user)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)
    document = await storage_service.get_document(document_id)
    if not document or str(document.get("customer_id") or "") != customer_id:
        raise HTTPException(status_code=404, detail="未找到该个人流水资料")
    document_type = str(document.get("file_type") or document.get("document_type") or "")
    if document_type not in PERSONAL_FLOW_TYPES:
        raise HTTPException(status_code=400, detail="仅个人流水资料支持收入人工确认")

    username = str(current_user.get("username") or "")
    try:
        saved = save_income_confirmation(
            customer_id,
            document_id,
            _model_payload(payload),
            confirmed_by=username,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    add_activity(
        "personal_flow_income_confirmation",
        customer=customer.get("name") or "",
        customer_id=customer_id,
        username=username,
        document_type="personal_flow",
        title="个人流水疑似工资人工确认",
        description=f"{saved.get('manual_status')}：{saved.get('counterparty_name')}",
        metadata={
            "document_id": document_id,
            "income_type": saved.get("income_type"),
            "target_type": saved.get("target_type"),
            "manual_status": saved.get("manual_status"),
            "amount": saved.get("amount"),
            "reason": saved.get("reason"),
        },
    )
    try:
        sync_result = await profile_sync_service.handle_document_saved(storage_service, customer_id)
        profile_refresh = sync_result.get("profile") or {}
        if profile_refresh.get("success") is False:
            raise RuntimeError(str(profile_refresh.get("error") or "资料汇总重建失败"))
        profile = await storage_service.get_customer_profile(customer_id) or {}
        summary_response = await get_customer_personal_flow_summary(
            customer_id,
            debug=False,
            current_user=current_user,
        )
    except Exception as exc:
        logger.exception("[PersonalFlow][INCOME_CONFIRMATION] profile refresh failed customer_id=%s", customer_id)
        raise HTTPException(status_code=500, detail="人工确认已保存，但资料汇总刷新失败，请重试刷新页面") from exc
    return {
        "success": True,
        "item": saved,
        "summary": summary_response.get("item"),
        "profile_markdown": profile.get("markdown_content") or "",
    }


@router.get("/{customer_id}/enterprise-flow/rules")
async def get_customer_enterprise_flow_rules(
    customer_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)
    return get_enterprise_flow_rules(customer_id)


@router.put("/{customer_id}/enterprise-flow/rules")
async def save_customer_enterprise_flow_rules(
    customer_id: str,
    payload: EnterpriseFlowRulesPayload,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_flow_rule_editor(current_user)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)
    username = current_user.get("username") or ""
    before = get_enterprise_flow_rules(customer_id)
    saved = save_enterprise_flow_rules(customer_id, _model_payload(payload), updated_by=username)
    add_activity(
        "enterprise_flow_rules_update",
        customer=customer.get("name") or "",
        customer_id=customer_id,
        username=username,
        document_type="enterprise_flow",
        title="企业流水经营性口径规则更新",
        description="更新关联公司、本方账户、白名单/黑名单和人工复核规则",
        metadata={"before": {k: before.get(k) for k in ("related_company_names", "self_account_numbers")}, "after": {k: saved.get(k) for k in ("related_company_names", "self_account_numbers")}},
    )
    return saved


@router.post("/{customer_id}/enterprise-flow/transactions/{transaction_id}/review")
async def review_enterprise_flow_transaction(
    customer_id: str,
    transaction_id: str,
    payload: EnterpriseFlowTransactionReviewPayload,
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    _ensure_flow_rule_editor(current_user)
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)
    username = current_user.get("username") or payload.reviewed_by or ""
    saved = update_transaction_review(customer_id, transaction_id, _model_payload(payload), reviewed_by=username)
    add_activity(
        "enterprise_flow_transaction_review",
        customer=customer.get("name") or "",
        customer_id=customer_id,
        username=username,
        document_type="enterprise_flow",
        title="企业流水单笔交易人工复核",
        description=f"交易 {transaction_id} 复核为 {payload.nature}",
        metadata={"transaction_id": transaction_id, "nature": payload.nature, "exclude_from_operating": payload.exclude_from_operating},
    )
    return {"success": True, "rules": saved}


@router.get("/{customer_id}/enterprise-flow/excluded-transactions")
async def get_enterprise_flow_excluded_transactions(
    customer_id: str,
    nature: str = Query("all"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)
    rules = {**get_enterprise_flow_rules(customer_id), "customer_name": customer.get("name") or customer_id, "customer_id": customer_id}
    if callable(getattr(storage_service, "list_extractions_by_types", None)):
        extractions = await storage_service.list_extractions_by_types(customer_id, list(ENTERPRISE_FLOW_TYPES))
    else:
        all_extractions = await storage_service.get_extractions_by_customer(customer_id)
        extractions = [
            item for item in all_extractions
            if (item.get("extraction_type") or item.get("document_type") or "") in ENTERPRISE_FLOW_TYPES
        ]
    aggregated = aggregate_customer_enterprise_flows(extractions, rules=rules)
    excluded = ((aggregated.get("views") or {}).get("excluded") or {}).get("transactions") or []
    if nature and nature != "all":
        excluded = [item for item in excluded if item.get("nature") == nature]
    total = len(excluded)
    logger.info("[EnterpriseFlowSummary] view=excluded counts=%s customer_id=%s", total, customer_id)
    return {"items": excluded[offset : offset + limit], "total": total}


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
    extractions = await storage_service.get_extractions_by_doc(doc_id)
    latest_extraction = extractions[0] if extractions else {}
    extracted_data = latest_extraction.get("extracted_data") or {}
    if not isinstance(extracted_data, dict):
        extracted_data = {}
    extracted_json = extracted_data.get("extracted_json") or extracted_data.get("data") or extracted_data
    if not isinstance(extracted_json, dict):
        extracted_json = {}
    structured_json = (
        extracted_data.get("structured_json")
        or extracted_json.get("structured_json")
        or (extracted_json if extracted_json.get("document_type") == "financial_report" else {})
    )
    if not isinstance(structured_json, dict):
        structured_json = {}
    document_type = normalize_document_type_code(document.get("file_type") or "") or str(document.get("file_type") or "")
    report_markdown = str(
        extracted_data.get("report_markdown")
        or extracted_data.get("markdown_report")
        or extracted_data.get("markdown_summary")
        or extracted_json.get("report_markdown")
        or extracted_json.get("markdown_report")
        or structured_json.get("report_markdown")
        or ""
    )
    if document_type == "financial_report" and not report_markdown and structured_json:
        display_json = extracted_data.get("display_json") or to_financial_report_display_json(structured_json)
        report_markdown = render_financial_report_markdown(display_json)
    return {
        "document_id": document.get("doc_id") or "",
        "doc_id": document.get("doc_id") or "",
        "customer_id": document.get("customer_id") or "",
        "customer_name": customer.get("name") or "",
        "document_type": document_type,
        "source_file": document.get("file_name") or "",
        "original_filename": document.get("file_name") or "",
        "file_name": document.get("file_name") or "",
        "file_type": document_type,
        "file_type_name": get_document_display_name(document_type),
        "file_size": document.get("file_size") or 0,
        "upload_time": document.get("upload_time") or "",
        "created_at": latest_extraction.get("created_at") or document.get("upload_time") or "",
        "updated_at": latest_extraction.get("created_at") or document.get("upload_time") or "",
        "report_markdown": report_markdown,
        "reportMarkdown": report_markdown,
        "extraction": latest_extraction,
        "latest_extraction": latest_extraction,
        "latestExtraction": latest_extraction,
        "extracted_json": extracted_json,
        "structured_json": structured_json,
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


@documents_router.post("/{doc_id}/re-extract")
async def re_extract_document(
    doc_id: str,
    make_active: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Re-run structured extraction for a retained document."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持重新提取")

    document, customer = await _load_accessible_document(doc_id, current_user)
    document_type = str(document.get("file_type") or "")
    if document_type not in {"enterprise_credit", "enterprise_credit_report", "financial_report"}:
        raise HTTPException(status_code=400, detail="当前仅支持企业征信或财务报表重新提取")

    absolute_path = _resolve_document_absolute_path(document.get("file_path"))
    if not absolute_path or not absolute_path.exists():
        raise HTTPException(status_code=409, detail=DOCUMENT_FILE_MISSING_MESSAGE)

    file_bytes = absolute_path.read_bytes()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="原件为空，无法重新提取")

    historical_financial_reports = (
        await _load_historical_financial_reports(
            document.get("customer_id") or "",
            exclude_doc_id=doc_id,
        )
        if document_type == "financial_report"
        else []
    )
    process_result = await _process_file_bytes(
        file_bytes,
        FileService().get_file_type(document.get("file_name") or absolute_path.name),
        document.get("file_name") or absolute_path.name,
        document_type,
        customer_id=document.get("customer_id") or "",
        customer_name=customer.get("name") or "",
        historical_financial_reports=historical_financial_reports,
    )

    content = process_result.content or {}
    extraction_record = await storage_service.save_extraction(
        {
            "extraction_id": uuid.uuid4().hex,
            "doc_id": doc_id,
            "customer_id": document.get("customer_id") or "",
            "extraction_type": document_type,
            "extracted_data": content,
            "confidence": float(content.get("confidence") or 0.0),
            "extraction_status": str(content.get("extraction_status") or "success"),
            "extraction_error": str(content.get("extraction_error") or ""),
            "skill_name": str(content.get("skill_name") or ""),
            "skill_version": str(content.get("skill_version") or ""),
            "schema_version": str(content.get("schema_version") or ""),
        }
    )
    if (
        make_active
        and document_type == "enterprise_credit"
        and (extraction_record.get("extraction_status") or "success") == "success"
    ):
        extracted_json = (content.get("extracted_json") or {}) if isinstance(content, dict) else {}
        report_basic = extracted_json.get("report_basic") or {}
        activate_single_active = getattr(storage_service, "activate_single_active_document", None)
        if callable(activate_single_active):
            await activate_single_active(
                document.get("customer_id") or "",
                document_type,
                doc_id,
                report_date=str(report_basic.get("report_date") or content.get("report_date") or ""),
                valid_until=str(document.get("valid_until") or ""),
            )
    await profile_sync_service.handle_document_saved(storage_service, document.get("customer_id") or "")
    return {
        "success": True,
        "doc_id": doc_id,
        "customer_id": document.get("customer_id") or "",
        "document_type": document_type,
        "extraction_id": extraction_record.get("extraction_id") or "",
        "extraction_status": extraction_record.get("extraction_status") or "success",
        "schema_version": extraction_record.get("schema_version") or "",
        "skill_name": extraction_record.get("skill_name") or "",
        "skill_version": extraction_record.get("skill_version") or "",
        "customer_name": customer.get("name") or "",
        "made_active": make_active and (extraction_record.get("extraction_status") or "success") == "success",
    }


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
) -> dict[str, Any]:
    """Delete a single uploaded document and its linked extraction records."""
    if not HAS_DB_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    started_at = time.perf_counter()
    logger.info("[DocumentDelete] start customer_id=%s document_id=%s", customer_id, doc_id)

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    await _ensure_local_customer_access(customer, current_user)

    try:
        document = await storage_service.get_document(doc_id)
    except Exception as e:
        logger.exception("[DocumentDelete] failed to fetch customer_id=%s document_id=%s", customer_id, doc_id)
        raise HTTPException(status_code=500, detail="删除资料失败") from e

    if not document or document.get("customer_id") != customer_id:
        raise HTTPException(status_code=404, detail="未找到该资料记录")

    try:
        deleted = await storage_service.delete_document(doc_id)
    except Exception as e:
        logger.exception("[DocumentDelete] failed customer_id=%s document_id=%s", customer_id, doc_id)
        raise HTTPException(status_code=500, detail="删除资料失败") from e

    if not deleted:
        raise HTTPException(status_code=404, detail="未找到该资料记录")

    cost_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info("[DocumentDelete] db_delete_done customer_id=%s document_id=%s cost_ms=%s", customer_id, doc_id, cost_ms)
    asyncio.create_task(_refresh_customer_after_document_delete(customer_id, doc_id))
    logger.info("[DocumentDelete] scheduled_background_refresh customer_id=%s document_id=%s", customer_id, doc_id)
    logger.info("[DocumentDelete] response_return customer_id=%s document_id=%s cost_ms=%s", customer_id, doc_id, cost_ms)

    return {
        "success": True,
        "document_id": doc_id,
        "message": "删除成功，资料汇总和检索索引将在后台刷新",
    }

