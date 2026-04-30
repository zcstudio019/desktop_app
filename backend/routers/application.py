"""
Application Generation Router

Handles loan application form generation.
Searches for customer data in Feishu and uses AI to fill application templates.

Requirements:
- 3.1: Search for customer data using customerName
- 3.2: Use AI to fill application template with customer information
- 3.3: Generate blank template with "待补充" placeholders when customer not found
- 3.4: Validate no fabrication of critical fields
- 3.5: Return applicationContent in Markdown format
- 3.6: Return 500 for service errors
"""

import asyncio
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from prompts import get_cached_prompts, load_prompts
from backend.celery_app import TASK_QUEUE_ENABLED
from backend.routers.chat_helpers import get_customer_data_local
from backend.services import get_storage_service, supports_structured_storage
from backend.services.activity_service import add_activity, update_customer_status
from backend.services.markdown_profile_service import get_or_create_customer_profile
from backend.services.profile_sync_service import ProfileSyncService
from services.ai_service import AIService, AIServiceError, validate_no_fabrication
from services.feishu_service import FeishuService, FeishuServiceError

from ..middleware.auth import get_current_user
from ..models.schemas import ApplicationRequest, ApplicationResponse, ChatJobCreateResponse

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/application", tags=["Application Generation"])

# Initialize services
feishu_service = FeishuService()
ai_service = AIService()
storage_service = get_storage_service()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
profile_sync_service = ProfileSyncService()
_ACTIVE_APPLICATION_JOB_TASKS: set[asyncio.Task[None]] = set()


def _build_application_generate_job_execution_payload(
    job_id: str,
    request: ApplicationRequest,
    current_user: dict[str, Any],
) -> dict[str, Any]:
    return {
        "jobId": job_id,
        "jobType": "application_generate",
        "customerId": request.customerId or "",
        "customerName": request.customerName or "",
        "loanType": request.loanType,
        "username": current_user.get("username") or "",
        "role": current_user.get("role") or "",
        "createdFrom": "application_generate_job",
    }


async def _run_application_generate_job(
    job_id: str,
    request_payload: dict[str, Any],
    current_user_payload: dict[str, Any],
) -> None:
    async def update_progress(message: str) -> None:
        logger.info("[Application Job] progress job_id=%s stage=%s", job_id, message)
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
        request = ApplicationRequest(**request_payload)
        await update_progress("正在读取客户资料")
        await update_progress("正在加载申请表模板")
        await update_progress("正在生成申请表")
        response = await generate_application(request, current_user_payload)
        await update_progress("正在更新申请表摘要")
        await storage_service.update_async_job(
            job_id,
            {
                "status": "success",
                "progress_message": "处理完成",
                "result_json": response.model_dump(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except HTTPException as exc:
        logger.error("[Application Job] failed job_id=%s detail=%s", job_id, exc.detail, exc_info=True)
        await storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "申请表生成失败",
                "error_message": str(exc.detail),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        raise
    except Exception as exc:
        logger.error("[Application Job] failed job_id=%s error=%s", job_id, exc, exc_info=True)
        await storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "申请表生成失败",
                "error_message": str(exc) or "申请表生成任务执行失败",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        raise


async def execute_application_generate_job_from_job(job_id: str) -> None:
    execution_payload = await storage_service.get_async_job_execution_payload(job_id)
    if not execution_payload:
        raise ValueError(f"async job {job_id} execution payload not found")

    request_payload = {
        "customerName": execution_payload.get("customerName") or "",
        "customerId": execution_payload.get("customerId") or "",
        "loanType": execution_payload.get("loanType") or "enterprise",
    }
    current_user_payload = {
        "username": execution_payload.get("username") or "",
        "role": execution_payload.get("role") or "",
    }
    await _run_application_generate_job(job_id, request_payload, current_user_payload)


def _launch_application_generate_job(job_id: str) -> None:
    task = asyncio.create_task(execute_application_generate_job_from_job(job_id))
    _ACTIVE_APPLICATION_JOB_TASKS.add(task)

    def _cleanup(done_task: asyncio.Task[None]) -> None:
        _ACTIVE_APPLICATION_JOB_TASKS.discard(done_task)
        try:
            done_task.result()
        except Exception:
            logger.exception("[Application Job] background task crashed job_id=%s", job_id)

    task.add_done_callback(_cleanup)


async def _dispatch_application_generate_job(
    job_id: str,
    customer_id: str,
    current_user_payload: dict[str, Any],
) -> None:
    logger.info(
        "[Application Job] submit start job_id=%s queue_enabled=%s customer_id=%s username=%s",
        job_id,
        TASK_QUEUE_ENABLED,
        customer_id,
        current_user_payload.get("username") or "",
    )
    if TASK_QUEUE_ENABLED:
        from backend.celery_app import APPLICATION_GENERATE_TASK_NAME, HEAVY_QUEUE_NAME, celery_app

        async_result = celery_app.send_task(APPLICATION_GENERATE_TASK_NAME, args=[job_id], queue=HEAVY_QUEUE_NAME)
        await storage_service.mark_async_job_dispatched(
            job_id,
            async_result.id,
            worker_name="celery",
        )
        logger.info(
            "[Application Job] dispatched to celery job_id=%s celery_task_id=%s customer_id=%s username=%s",
            job_id,
            async_result.id,
            customer_id,
            current_user_payload.get("username") or "",
        )
        return

    logger.warning(
        "[Application Job] fallback to in-process execution job_id=%s customer_id=%s username=%s",
        job_id,
        customer_id,
        current_user_payload.get("username") or "",
    )
    _launch_application_generate_job(job_id)

INVALID_LOAN_TYPE_MESSAGE = "贷款类型无效，请选择企业贷款或个人贷款。"
CUSTOMER_DATA_SEARCH_FAILED_MESSAGE = "客户资料查询失败，请稍后重试。"
APPLICATION_TEMPLATE_LOAD_FAILED_MESSAGE = "申请表模板加载失败，请稍后重试。"
APPLICATION_GENERATION_FAILED_MESSAGE = "申请表生成失败，请稍后重试。"
APPLICATION_VALIDATION_WARNING_MESSAGE = "申请表生成后校验未完成，请人工复核关键信息。"


DEFAULT_NONE_TEXT = "无"
DEFAULT_NO_TEXT = "否"
DEFAULT_ACTIVE_STATUS = "在业"
DEFAULT_SAME_AS_REGISTERED = "同上"
MISSING_VALUE_MARKERS = {"", "-", "--", "待补充", "暂无", "未提供", "未知", "null", "none", "n/a"}


def _merge_extraction_data(extractions: list[dict[str, Any]]) -> dict[str, str]:
    """Merge extraction payloads into a flat dict for prompting and fallback fill."""
    merged: dict[str, str] = {}
    for extraction in extractions:
        extracted_data = extraction.get("extracted_data") or {}
        if not isinstance(extracted_data, dict):
            continue
        for key, value in extracted_data.items():
            if isinstance(value, (dict, list)):
                merged[key] = json.dumps(value, ensure_ascii=False)
            else:
                merged[key] = str(value) if value is not None else ""
    return merged


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, dict)):
        return len(value) == 0
    text = str(value).strip()
    if not text:
        return True
    return text.lower() in MISSING_VALUE_MARKERS or "待补充" in text


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _safe_json_parse(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except Exception:
        return value


def _build_structured_sources(customer_data: dict[str, Any]) -> dict[str, Any]:
    return {key: _safe_json_parse(value) for key, value in (customer_data or {}).items()}


def _pick_first_value(structured_sources: dict[str, Any], *candidates: Any, default: str = "") -> str:
    for candidate in candidates:
        value: Any = None
        if isinstance(candidate, tuple) and len(candidate) == 2:
            section_name, field_name = candidate
            section = structured_sources.get(section_name)
            if isinstance(section, dict):
                value = section.get(field_name)
        elif isinstance(candidate, str):
            value = structured_sources.get(candidate)

        if not _is_missing_value(value):
            return _stringify_value(value)
    return default


def _extract_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?", text, re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _format_number(value: float, suffix: str = "", decimals: int = 2) -> str:
    formatted = f"{value:,.{decimals}f}".rstrip("0").rstrip(".")
    return f"{formatted}{suffix}"


def _format_percentage(numerator: float, denominator: float) -> str:
    if denominator <= 0:
        return DEFAULT_NONE_TEXT
    ratio = numerator / denominator * 100
    text = f"{ratio:.2f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _normalise_no_value(value: str, default: str = DEFAULT_NONE_TEXT) -> str:
    return default if _is_missing_value(value) else str(value).strip()


def _set_field_if_missing(section_data: dict[str, str], field_name: str, value: str, default: str = DEFAULT_NONE_TEXT) -> None:
    if _is_missing_value(section_data.get(field_name)):
        section_data[field_name] = _normalise_no_value(value, default=default)


def _set_field_with_preferred_value(
    section_data: dict[str, str],
    field_name: str,
    preferred_value: str,
    *,
    fallback_default: str = DEFAULT_NONE_TEXT,
    replace_none_text: bool = True,
) -> None:
    current_value = section_data.get(field_name)
    should_replace = _is_missing_value(current_value) or (replace_none_text and str(current_value).strip() == DEFAULT_NONE_TEXT)
    if should_replace:
        section_data[field_name] = _normalise_no_value(preferred_value, default=fallback_default)


def _sum_numeric_from_items(items: list[Any], *field_names: str) -> float:
    total = 0.0
    for item in items:
        if not isinstance(item, dict):
            continue
        for field_name in field_names:
            amount = _extract_number(item.get(field_name))
            if amount is not None:
                total += amount
                break
    return total


def _compute_latest_tax_amount(structured_sources: dict[str, Any]) -> str:
    tax_info = structured_sources.get("纳税信息")
    if isinstance(tax_info, dict):
        yearly = tax_info.get("各年度纳税金额")
        if isinstance(yearly, list) and yearly:
            ranked_items: list[tuple[int, str]] = []
            for item in yearly:
                if not isinstance(item, dict):
                    continue
                year = int(_extract_number(item.get("年度")) or 0)
                amount = _stringify_value(item.get("纳税金额"))
                if year and not _is_missing_value(amount):
                    ranked_items.append((year, amount))
            if ranked_items:
                ranked_items.sort(key=lambda item: item[0], reverse=True)
                return ranked_items[0][1]
        return _pick_first_value(structured_sources, ("纳税信息", "近三年纳税总额"))
    return ""


def _search_raw_field_value(customer_data: dict[str, Any], field_name: str) -> str:
    patterns = [
        re.compile(rf'"{re.escape(field_name)}"\s*:\s*"([^"]+)"'),
        re.compile(rf"{re.escape(field_name)}\s*[：:]\s*([^\n,}}]+)"),
    ]

    for raw_value in (customer_data or {}).values():
        if not isinstance(raw_value, str) or field_name not in raw_value:
            continue
        for pattern in patterns:
            match = pattern.search(raw_value)
            if match:
                candidate = match.group(1).strip()
                if not _is_missing_value(candidate):
                    return candidate
    return ""


def _extract_control_share_ratio(structured_sources: dict[str, Any]) -> str:
    base_info = structured_sources.get("企业基本信息")
    if isinstance(base_info, dict):
        shareholders = base_info.get("股权结构")
        if isinstance(shareholders, list) and shareholders:
            first_holder = shareholders[0]
            if isinstance(first_holder, dict):
                ratio = first_holder.get("持股比例")
                if not _is_missing_value(ratio):
                    return _stringify_value(ratio)
    return _pick_first_value(structured_sources, ("实际控制人", "持股比例"))


def _extract_litigation_fields(structured_sources: dict[str, Any]) -> dict[str, str]:
    public_record = structured_sources.get("公共记录")
    if not isinstance(public_record, dict):
        return {
            "诉讼/仲裁记录": DEFAULT_NONE_TEXT,
            "案件类型": DEFAULT_NONE_TEXT,
            "立案时间": DEFAULT_NONE_TEXT,
            "涉案金额": DEFAULT_NONE_TEXT,
            "审理进度": DEFAULT_NONE_TEXT,
            "对经营影响": DEFAULT_NONE_TEXT,
            "其他负面记录": DEFAULT_NONE_TEXT,
            "负面记录类型": DEFAULT_NONE_TEXT,
            "负面记录具体情况": DEFAULT_NONE_TEXT,
        }

    civil_count = int(_extract_number(public_record.get("民事判决数")) or 0)
    enforce_count = int(_extract_number(public_record.get("强制执行数")) or 0)
    admin_count = int(_extract_number(public_record.get("行政处罚数")) or 0)
    tax_count = int(_extract_number(public_record.get("欠税记录数")) or 0)

    has_litigation = civil_count > 0 or enforce_count > 0
    has_other_negative = admin_count > 0 or tax_count > 0

    negative_type_parts: list[str] = []
    if tax_count > 0:
        negative_type_parts.append("欠税")
    if admin_count > 0:
        negative_type_parts.append("行政处罚")

    return {
        "诉讼/仲裁记录": "有" if has_litigation else DEFAULT_NONE_TEXT,
        "案件类型": DEFAULT_NONE_TEXT,
        "立案时间": DEFAULT_NONE_TEXT,
        "涉案金额": DEFAULT_NONE_TEXT,
        "审理进度": DEFAULT_NONE_TEXT,
        "对经营影响": DEFAULT_NONE_TEXT,
        "其他负面记录": "有" if has_other_negative else DEFAULT_NONE_TEXT,
        "负面记录类型": "、".join(negative_type_parts) if negative_type_parts else DEFAULT_NONE_TEXT,
        "负面记录具体情况": DEFAULT_NONE_TEXT,
    }


def _compute_credit_card_usage(structured_sources: dict[str, Any]) -> str:
    summary = structured_sources.get("信贷记录概要")
    details = structured_sources.get("信用卡明细")

    if isinstance(details, list) and details:
        total_limit = _sum_numeric_from_items(details, "授信额度", "信用额度", "共享额度")
        used_limit = _sum_numeric_from_items(details, "已使用额度", "已用额度", "透支余额", "当前余额")
        if total_limit > 0:
            return _format_percentage(used_limit, total_limit)

    if isinstance(summary, dict):
        open_cards = _extract_number(summary.get("信用卡未销户数"))
        if open_cards == 0:
            return "0%"
        total_limit = _extract_number(summary.get("信用卡授信总额"))
        used_limit = _extract_number(summary.get("信用卡已用额度"))
        if total_limit and used_limit is not None:
            return _format_percentage(used_limit, total_limit)

    return DEFAULT_NONE_TEXT


def _compute_overdue_fields(structured_sources: dict[str, Any]) -> dict[str, str]:
    loans = structured_sources.get("贷款明细")
    current_periods: list[int] = []
    history_periods: list[int] = []
    current_overdue_amount = 0.0

    if isinstance(loans, list):
        for item in loans:
            if not isinstance(item, dict):
                continue
            current_period = int(_extract_number(item.get("当前逾期期数")) or 0)
            history_period = int(_extract_number(item.get("历史最高逾期期数")) or 0)
            current_periods.append(current_period)
            history_periods.append(history_period)
            current_overdue_amount += _extract_number(item.get("当前逾期金额")) or 0.0

    max_current_period = max(current_periods, default=0)
    max_history_period = max(history_periods, default=0)
    has_current_overdue = current_overdue_amount > 0 or max_current_period > 0

    if max_history_period >= 3:
        near_two_years = "连3累6及以上"
    elif max_history_period > 0:
        near_two_years = f"其他（{max_history_period}期）"
    else:
        near_two_years = DEFAULT_NONE_TEXT

    return {
        "当前逾期": "有" if has_current_overdue else DEFAULT_NONE_TEXT,
        "当前逾期金额": _format_number(current_overdue_amount, "元") if current_overdue_amount > 0 else "0",
        "当前逾期时长": f"{max_current_period}期" if max_current_period > 0 else DEFAULT_NONE_TEXT,
        "近2年逾期": near_two_years,
    }


def _compute_guarantee_fields(structured_sources: dict[str, Any]) -> dict[str, str]:
    guarantee_details = structured_sources.get("对外担保明细")
    if not isinstance(guarantee_details, list) or not guarantee_details:
        return {
            "对外担保": DEFAULT_NONE_TEXT,
            "被担保人": DEFAULT_NONE_TEXT,
            "担保金额": DEFAULT_NONE_TEXT,
            "担保期限": DEFAULT_NONE_TEXT,
            "是否有代偿风险": DEFAULT_NO_TEXT,
        }

    guaranteed_names: list[str] = []
    guarantee_terms: list[str] = []
    total_amount = 0.0

    for item in guarantee_details:
        if not isinstance(item, dict):
            continue
        target_name = _stringify_value(item.get("被担保企业"))
        if target_name and target_name not in guaranteed_names and not _is_missing_value(target_name):
            guaranteed_names.append(target_name)
        term = _stringify_value(item.get("到期日期"))
        if term and not _is_missing_value(term):
            guarantee_terms.append(term)
        total_amount += _extract_number(item.get("担保余额")) or _extract_number(item.get("担保金额")) or 0.0

    return {
        "对外担保": "是",
        "被担保人": "、".join(guaranteed_names) if guaranteed_names else DEFAULT_NONE_TEXT,
        "担保金额": _format_number(total_amount, "元") if total_amount > 0 else DEFAULT_NONE_TEXT,
        "担保期限": "；".join(guarantee_terms) if guarantee_terms else DEFAULT_NONE_TEXT,
        "是否有代偿风险": DEFAULT_NO_TEXT,
    }


def _compute_public_record_defaults(structured_sources: dict[str, Any]) -> dict[str, str]:
    has_negative_public_record = False
    for section_name in ("公共记录", "信息概要"):
        section = structured_sources.get(section_name)
        if not isinstance(section, dict):
            continue
        for _, value in section.items():
            if isinstance(value, dict):
                for nested_value in value.values():
                    number = _extract_number(nested_value)
                    if number and number > 0:
                        has_negative_public_record = True
                        break
            else:
                number = _extract_number(value)
                if number and number > 0:
                    has_negative_public_record = True
                    break
        if has_negative_public_record:
            break

    if has_negative_public_record:
        return {
            "诉讼/仲裁记录": "有",
            "案件类型": DEFAULT_NONE_TEXT,
            "立案时间": DEFAULT_NONE_TEXT,
            "涉案金额": DEFAULT_NONE_TEXT,
            "审理进度": DEFAULT_NONE_TEXT,
            "对经营影响": DEFAULT_NONE_TEXT,
            "其他负面记录": "有",
            "负面记录类型": DEFAULT_NONE_TEXT,
            "负面记录具体情况": DEFAULT_NONE_TEXT,
        }

    return {
        "诉讼/仲裁记录": DEFAULT_NONE_TEXT,
        "案件类型": DEFAULT_NONE_TEXT,
        "立案时间": DEFAULT_NONE_TEXT,
        "涉案金额": DEFAULT_NONE_TEXT,
        "审理进度": DEFAULT_NONE_TEXT,
        "对经营影响": DEFAULT_NONE_TEXT,
        "其他负面记录": DEFAULT_NONE_TEXT,
        "负面记录类型": DEFAULT_NONE_TEXT,
        "负面记录具体情况": DEFAULT_NONE_TEXT,
    }


def _render_application_markdown(loan_type: str, application_data: dict[str, dict[str, str]]) -> str:
    title = "# 企业贷款申请表" if loan_type == "enterprise" else "# 个人贷款申请表"
    sections: list[str] = [title, ""]
    for section_name, section_data in application_data.items():
        sections.append(f"## {section_name}")
        sections.append("| 项目 | 内容 |")
        sections.append("| --- | --- |")
        for field_name, value in section_data.items():
            safe_value = (value or DEFAULT_NONE_TEXT).replace("\n", "<br/>")
            sections.append(f"| {field_name} | {safe_value} |")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def _enhance_enterprise_application_data(
    application_data: dict[str, dict[str, str]],
    customer_data: dict[str, Any],
) -> dict[str, dict[str, str]]:
    structured_sources = _build_structured_sources(customer_data)
    enhanced = {section: dict(fields) for section, fields in application_data.items()}

    basic_info = enhanced.setdefault("企业基本信息", {})
    repayment_info = enhanced.setdefault("还款能力信息", {})
    credit_info = enhanced.setdefault("征信及负债相关声明", {})

    registration_address = _pick_first_value(
        structured_sources,
        ("企业身份信息", "注册地址"),
        ("企业基本信息", "注册地址"),
        default=DEFAULT_NONE_TEXT,
    )
    actual_address = _pick_first_value(
        structured_sources,
        ("企业身份信息", "办公/经营地址"),
        ("企业基本信息", "办公/经营地址"),
    )
    if _is_missing_value(actual_address):
        actual_address = _search_raw_field_value(customer_data, "办公/经营地址")
    if _is_missing_value(actual_address):
        actual_address = _search_raw_field_value(customer_data, "实际经营地址")

    business_status = _pick_first_value(structured_sources, ("企业身份信息", "经营状态"))
    if _is_missing_value(business_status):
        business_status = _search_raw_field_value(customer_data, "经营状态")
    if _is_missing_value(business_status):
        business_status = DEFAULT_ACTIVE_STATUS

    industry_type = _pick_first_value(structured_sources, ("企业身份信息", "所属行业"))
    if _is_missing_value(industry_type):
        industry_type = _search_raw_field_value(customer_data, "所属行业")
    if _is_missing_value(industry_type):
        industry_type = _search_raw_field_value(customer_data, "行业类型")

    fixed_asset = _pick_first_value(structured_sources, ("资产", "固定资产"), default=DEFAULT_NONE_TEXT)
    net_asset = _pick_first_value(
        structured_sources,
        ("所有者权益", "所有者权益合计"),
        ("所有者权益", "净资产总额"),
        default=DEFAULT_NONE_TEXT,
    )

    asset_total = _extract_number(_pick_first_value(structured_sources, ("资产", "资产总计")))
    liability_total = _extract_number(_pick_first_value(structured_sources, ("负债", "负债合计")))
    current_assets = _extract_number(_pick_first_value(structured_sources, ("资产", "流动资产合计")))
    current_liabilities = _extract_number(_pick_first_value(structured_sources, ("负债", "流动负债合计")))
    revenue = _extract_number(_pick_first_value(structured_sources, ("利润表", "营业收入")))
    cost = _extract_number(_pick_first_value(structured_sources, ("利润表", "营业成本")))
    operation_cash = _pick_first_value(structured_sources, ("现金流量表", "经营活动现金流量净额"))

    if asset_total and liability_total is not None and asset_total > 0:
        asset_liability_ratio = f"{liability_total / asset_total * 100:.2f}%"
    else:
        asset_liability_ratio = DEFAULT_NONE_TEXT

    if current_assets and current_liabilities and current_liabilities > 0:
        current_ratio = f"{current_assets / current_liabilities:.2f}"
    else:
        current_ratio = DEFAULT_NONE_TEXT

    if revenue and cost is not None and revenue > 0:
        gross_margin = f"{(revenue - cost) / revenue * 100:.2f}%"
        avg_month_revenue = _format_number(revenue / 12, "元")
        year_revenue = _format_number(revenue, "元")
    else:
        gross_margin = DEFAULT_NONE_TEXT
        avg_month_revenue = DEFAULT_NONE_TEXT
        year_revenue = DEFAULT_NONE_TEXT

    latest_tax = _compute_latest_tax_amount(structured_sources) or DEFAULT_NONE_TEXT
    credit_usage = _compute_credit_card_usage(structured_sources)
    overdue_fields = _compute_overdue_fields(structured_sources)
    guarantee_fields = _compute_guarantee_fields(structured_sources)
    public_record_defaults = _compute_public_record_defaults(structured_sources)

    _set_field_if_missing(basic_info, "企业名称", _pick_first_value(structured_sources, ("企业身份信息", "企业名称"), ("企业基本信息", "企业名称")))
    _set_field_if_missing(basic_info, "统一社会信用代码", _pick_first_value(structured_sources, ("企业身份信息", "统一社会信用代码"), ("企业基本信息", "统一社会信用代码")))
    _set_field_with_preferred_value(basic_info, "注册地址", registration_address)
    _set_field_with_preferred_value(
        basic_info,
        "实际经营地址",
        actual_address or (DEFAULT_SAME_AS_REGISTERED if not _is_missing_value(registration_address) else DEFAULT_NONE_TEXT),
    )
    _set_field_if_missing(basic_info, "成立时间", _pick_first_value(structured_sources, ("企业身份信息", "成立日期"), ("企业基本信息", "成立时间")))
    _set_field_with_preferred_value(basic_info, "经营状态", business_status, fallback_default=DEFAULT_ACTIVE_STATUS)
    _set_field_with_preferred_value(basic_info, "近1年是否变更", DEFAULT_NONE_TEXT)
    _set_field_with_preferred_value(
        basic_info,
        "经营场地",
        "与实际经营地址一致" if not _is_missing_value(actual_address or registration_address) else DEFAULT_NONE_TEXT,
    )
    _set_field_if_missing(basic_info, "法定代表人姓名", _pick_first_value(structured_sources, ("法定代表人信息", "姓名"), ("企业基本信息", "法定代表人")))
    _set_field_if_missing(basic_info, "法定代表人身份证号", _pick_first_value(structured_sources, ("报告基础信息", "证件号码"), ("法定代表人信息", "身份证号")))
    _set_field_if_missing(basic_info, "法定代表人年龄", _pick_first_value(structured_sources, ("企业基本信息", "法人年龄")))
    _set_field_if_missing(basic_info, "实际控制人姓名", _pick_first_value(structured_sources, ("实际控制人", "姓名"), ("法定代表人信息", "姓名")))
    _set_field_if_missing(basic_info, "实际控制人持股比例", _pick_first_value(structured_sources, ("企业基本信息", "股权结构")))
    _set_field_if_missing(basic_info, "实际控制人控制路径", _pick_first_value(structured_sources, ("实际控制人", "控制方式")))
    _set_field_if_missing(basic_info, "纳税评级", _pick_first_value(structured_sources, ("纳税信息", "纳税信用等级")))
    _set_field_with_preferred_value(basic_info, "近12个月纳税", latest_tax)
    _set_field_with_preferred_value(basic_info, "行业类型", industry_type)
    _set_field_with_preferred_value(basic_info, "是否禁入行业", DEFAULT_NO_TEXT, fallback_default=DEFAULT_NO_TEXT)
    _set_field_with_preferred_value(basic_info, "科技属性", DEFAULT_NONE_TEXT)

    _set_field_if_missing(repayment_info, "近1年营业收入", year_revenue)
    _set_field_if_missing(repayment_info, "近1年月均收入", avg_month_revenue)
    _set_field_if_missing(repayment_info, "近12个月开票", _pick_first_value(structured_sources, ("发票信息", "近12个月开票金额")))
    _set_field_if_missing(repayment_info, "资产负债率", asset_liability_ratio)
    _set_field_if_missing(repayment_info, "流动比率", current_ratio)
    _set_field_if_missing(repayment_info, "毛利率", gross_margin)
    _set_field_if_missing(repayment_info, "近1年经营活动现金流净额", operation_cash)
    _set_field_if_missing(repayment_info, "信用卡使用率", credit_usage)
    _set_field_if_missing(repayment_info, "固定资产", fixed_asset)
    _set_field_if_missing(repayment_info, "固定资产成新率", DEFAULT_NONE_TEXT)
    _set_field_if_missing(repayment_info, "净资产总额", net_asset)

    for field_name, value in overdue_fields.items():
        _set_field_if_missing(credit_info, field_name, value)
    for field_name, value in guarantee_fields.items():
        _set_field_if_missing(credit_info, field_name, value, default=value)

    _set_field_if_missing(credit_info, "隐形负债", DEFAULT_NONE_TEXT)
    _set_field_if_missing(credit_info, "隐形负债类型", DEFAULT_NONE_TEXT)
    _set_field_if_missing(credit_info, "隐形负债金额", DEFAULT_NONE_TEXT)
    _set_field_if_missing(credit_info, "隐形负债还款期限", DEFAULT_NONE_TEXT)

    for field_name, value in public_record_defaults.items():
        _set_field_if_missing(credit_info, field_name, value)

    return enhanced


async def _load_customer_context(customer_name: str, customer_id: str | None) -> tuple[bool, dict[str, str], str]:
    """Load latest customer context, prioritising customer_id scoped profile markdown."""
    customer_found = False
    customer_data: dict[str, str] = {}
    profile_markdown = ""

    if HAS_DB_STORAGE and customer_id:
        customer = await storage_service.get_customer(customer_id)
        if customer:
            customer_found = True
            get_business_extractions = getattr(storage_service, "get_business_extractions_by_customer", None)
            if callable(get_business_extractions):
                extractions = await get_business_extractions(customer_id)
            else:
                extractions = await storage_service.get_extractions_by_customer(customer_id)
            customer_data = _merge_extraction_data(extractions)
            profile, _ = await get_or_create_customer_profile(storage_service, customer_id)
            profile_markdown = (profile or {}).get("markdown_content") or ""
            if profile_markdown:
                customer_data["资料汇总"] = profile_markdown
                customer_data["资料汇总Markdown"] = profile_markdown
            logger.info(
                "Loaded application context by customer_id=%s fields=%s profile_markdown=%s",
                customer_id,
                len(customer_data),
                bool(profile_markdown),
            )
            return customer_found, customer_data, profile_markdown

    if customer_name and customer_name.strip():
        customer_found, customer_data = await _load_customer_data(customer_name.strip())
        if HAS_DB_STORAGE and customer_id:
            profile, _ = await get_or_create_customer_profile(storage_service, customer_id)
            profile_markdown = (profile or {}).get("markdown_content") or ""
            if profile_markdown:
                customer_data["资料汇总"] = profile_markdown
                customer_data["资料汇总Markdown"] = profile_markdown
        return customer_found, customer_data, profile_markdown

    return False, {}, ""


async def _load_customer_data(customer_name: str) -> tuple[bool, dict]:
    """Load customer data from the active storage backend."""
    if HAS_DB_STORAGE:
        return await get_customer_data_local(customer_name, prefer_latest_per_type=True)

    records = feishu_service.search_records(customer_name)
    if not records:
        return (False, {})

    return (True, extract_customer_fields(records))


def load_application_template(loan_type: str) -> str:
    """Load the appropriate application template based on loan type.

    Args:
        loan_type: Either "enterprise" or "personal"

    Returns:
        Template content as string

    Raises:
        ValueError: If loan_type is invalid
    """
    # Ensure prompts are loaded
    prompts = get_cached_prompts()
    if not prompts:
        prompts = load_prompts()

    # Map loan type to template file
    template_files = {
        "enterprise": "申请表模板_企业贷款.md",
        "personal": "申请表模板_个人贷款.md",
    }

    template_file = template_files.get(loan_type)
    if not template_file:
        raise ValueError(INVALID_LOAN_TYPE_MESSAGE)

    template = prompts.get(template_file, "")

    if not template:
        logger.warning(f"Template file not found: {template_file}")
        # Return a minimal template as fallback
        if loan_type == "enterprise":
            return _get_fallback_enterprise_template()
        else:
            return _get_fallback_personal_template()

    return template


def _get_fallback_enterprise_template() -> str:
    """Get fallback enterprise loan template."""
    return """# 企业贷款申请表

## 企业基本信息
| 项目 | 填写内容 |
|------|---------|
| 企业名称 | |
| 统一社会信用代码 | |
| 法定代表人 | |
| 注册地址 | |

## 贷款申请信息
| 项目 | 填写内容 |
|------|---------|
| 期望额度 | |
| 期望期限 | |
| 贷款用途 | |
"""


def _get_fallback_personal_template() -> str:
    """Get fallback personal loan template."""
    return """# 个人贷款申请表

## 个人基本信息
| 项目 | 填写内容 |
|------|---------|
| 姓名 | |
| 身份证号 | |
| 手机号码 | |
| 居住地址 | |

## 贷款申请信息
| 项目 | 填写内容 |
|------|---------|
| 期望额度 | |
| 期望期限 | |
| 贷款用途 | |
"""


def build_generation_prompt(
    template: str,
    customer_data: dict,
    customer_found: bool,
    profile_markdown: str = "",
) -> str:
    """Build the prompt for AI to generate the application.

    Args:
        template: The application template
        customer_data: Customer data from Feishu (empty dict if not found)
        customer_found: Whether customer data was found

    Returns:
        Complete prompt for AI generation
    """
    if customer_found and customer_data:
        # Customer found - fill template with data
        customer_json = json.dumps(customer_data, ensure_ascii=False, indent=2)
        profile_block = f"\n## 最新资料汇总 Markdown\n{profile_markdown}\n" if profile_markdown else ""
        prompt = f"""你是一个专业的贷款申请表填写助手。请根据以下客户资料，填写贷款申请表。

## 重要规则
1. 只使用客户资料中明确提供的信息填写表格
2. 对于客户资料中没有的信息，填写"待补充"
3. **绝对禁止编造以下关键字段**：
   - 期望额度
   - 期望期限
   - 利率
   - 贷款金额
   这些字段如果客户资料中没有，必须填写"待补充"
4. 保持表格的 Markdown 格式
5. 复选框根据客户资料选择，未知的保持空白

## 客户资料
{customer_json}
{profile_block}

## 申请表模板
{template}

请根据客户资料填写申请表，直接输出填写后的完整申请表（Markdown 格式）。
"""
    else:
        # Customer not found - generate blank template
        prompt = f"""你是一个专业的贷款申请表填写助手。请生成一份空白的贷款申请表模板。

## 重要规则
1. 所有需要填写的字段都填写"待补充"
2. 复选框保持空白（不勾选）
3. 保持表格的 Markdown 格式
4. 保留所有表格结构和说明

## 申请表模板
{template}

请输出空白申请表模板，所有填写内容字段填写"待补充"。
"""

    return prompt


def extract_customer_fields(records: list) -> dict:
    """Extract and flatten customer fields from Feishu records.

    Args:
        records: List of Feishu records

    Returns:
        Flattened customer data dictionary
    """
    if not records:
        return {}

    # Get the first matching record
    record = records[0]
    fields = record.get("fields", {})

    if not fields:
        return {}

    # Extract text values from Feishu rich text format
    customer_data = {}

    for field_name, value in fields.items():
        # Skip internal fields
        if field_name in ["record_id", "id"]:
            continue

        # Extract text from Feishu format
        extracted = _extract_text_value(value)
        if extracted:
            customer_data[field_name] = extracted

    return customer_data


def build_application_data(customer_data: dict, loan_type: str) -> dict:
    """Build structured application data for card rendering.

    Args:
        customer_data: Extracted customer data from Feishu
        loan_type: Either "enterprise" or "personal"

    Returns:
        Structured data as {section_name: {field_name: value}}
    """
    if loan_type == "enterprise":
        # Enterprise loan sections
        sections = {
            "企业基本信息": [
                "企业名称",
                "统一社会信用代码",
                "法定代表人",
                "注册地址",
                "成立日期",
                "注册资本",
                "实缴资本",
                "经营范围",
                "联系电话",
            ],
            "贷款申请信息": ["期望额度", "期望期限", "贷款用途", "还款来源"],
            "财务信息": ["年营业收入", "年净利润", "资产总额", "负债总额", "财务数据"],
            "征信信息": ["企业征信报告", "个人征信报告", "征信查询次数", "逾期记录"],
            "流水信息": ["企业流水", "个人流水", "月均流水", "结息金额"],
            "抵押物信息": ["抵押物信息", "抵押物类型", "抵押物价值", "抵押物地址"],
            "其他信息": ["水母报告", "备注"],
        }
    else:
        # Personal loan sections
        sections = {
            "个人基本信息": ["姓名", "身份证号", "手机号码", "居住地址", "婚姻状况", "学历", "工作单位", "职务"],
            "贷款申请信息": ["期望额度", "期望期限", "贷款用途", "还款来源"],
            "收入信息": ["月收入", "年收入", "收入来源", "个人收入纳税/公积金"],
            "征信信息": ["企业征信报告", "个人征信报告", "征信查询次数", "逾期记录"],
            "流水信息": ["企业流水", "个人流水", "月均流水", "结息金额"],
            "资产信息": ["房产", "车辆", "存款", "其他资产"],
            "其他信息": ["水母报告", "备注"],
        }

    # Build structured data
    application_data = {}
    used_fields = set()

    for section_name, field_names in sections.items():
        section_data = {}
        for field_name in field_names:
            # Use dict.get() or "" to handle None (踩坑点 #16)
            value = customer_data.get(field_name) or "待补充"
            section_data[field_name] = value
            used_fields.add(field_name)

        # Only add section if it has at least one non-empty field
        if section_data:
            application_data[section_name] = section_data

    # Add any remaining fields to "其他信息"
    other_fields = {}
    for field_name, value in customer_data.items():
        if field_name not in used_fields:
            other_fields[field_name] = value or "待补充"

    if other_fields:
        if "其他信息" in application_data:
            application_data["其他信息"].update(other_fields)
        else:
            application_data["其他信息"] = other_fields

    return application_data


def parse_application_markdown_to_data(application_content: str) -> dict[str, dict[str, str]]:
    """Parse markdown table sections into structured application data."""
    structured: dict[str, dict[str, str]] = {}
    current_section: str | None = None

    for raw_line in (application_content or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            current_section = line[3:].strip()
            structured.setdefault(current_section, {})
            continue
        if not current_section or not line.startswith("|"):
            continue

        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2:
            continue

        field_name, field_value = cells[0], cells[1]
        if not field_name or field_name in {"项目", "椤圭洰"}:
            continue
        if set(field_name) <= {"-", ":"}:
            continue
        structured.setdefault(current_section, {})[field_name] = field_value or "待补充"

    return {section: fields for section, fields in structured.items() if fields}


def _extract_text_value(value) -> str:
    """Extract text from Feishu field value.

    Feishu API returns text fields in various formats:
    - Plain string: "xxx"
    - Rich text array: [{"text": "xxx"}, {"text": "yyy"}]
    - Single object: {"text": "xxx"}

    Args:
        value: Feishu field value

    Returns:
        Extracted plain text
    """
    if value is None:
        return ""

    # If it's a string, return directly
    if isinstance(value, str):
        return value.strip()

    # If it's a list (rich text array)
    if isinstance(value, list):
        texts = []
        for item in value:
            if isinstance(item, dict) and "text" in item:
                texts.append(str(item["text"]))
            elif isinstance(item, str):
                texts.append(item)
        return "".join(texts).strip()

    # If it's a dict (single rich text object)
    if isinstance(value, dict):
        if "text" in value:
            return str(value["text"]).strip()
        # Other cases, try to convert to string
        return str(value).strip()

    # Other types, convert to string
    return str(value).strip()


@router.post("/generate", response_model=ApplicationResponse)
async def generate_application(
    request: ApplicationRequest,
    current_user: dict = Depends(get_current_user),
) -> ApplicationResponse:
    """
    Generate a loan application form.

    This endpoint:
    1. Searches for customer data in Feishu using customerName
    2. Loads the appropriate template based on loanType
    3. Uses AI to fill the template with customer data
    4. Validates that critical fields are not fabricated
    5. Returns the generated application in Markdown format

    **Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**

    Args:
        request: ApplicationRequest with customerName and loanType

    Returns:
        ApplicationResponse with applicationContent, customerFound, and warnings

    Raises:
        HTTPException 500: Service errors (Feishu, AI)
    """
    logger.info(
        "Generating application for user=%s, customer=%s, type=%s",
        current_user["username"],
        request.customerName,
        request.loanType,
    )

    # Validate loan type
    if request.loanType not in ["enterprise", "personal"]:
        raise HTTPException(status_code=400, detail=INVALID_LOAN_TYPE_MESSAGE)

    # Step 1: Search for customer data
    # Requirement 3.1: Search for customer data using customerName
    customer_found = False
    customer_data = {}
    profile_markdown = ""

    if request.customerName and request.customerName.strip():
        try:
            customer_found, customer_data, profile_markdown = await _load_customer_context(
                request.customerName.strip(),
                request.customerId,
            )
            if customer_found:
                logger.info(f"Found customer data with {len(customer_data)} fields")
            else:
                logger.info(f"No customer data found for: {request.customerName}")
        except FeishuServiceError as e:
            logger.error(f"Feishu service error: {e}")
            raise HTTPException(status_code=500, detail=CUSTOMER_DATA_SEARCH_FAILED_MESSAGE) from e
        except Exception as e:
            logger.error(f"Unexpected error searching customer: {e}")
            raise HTTPException(status_code=500, detail=CUSTOMER_DATA_SEARCH_FAILED_MESSAGE) from e
    else:
        logger.info("No customer name provided, generating blank template")

    # Step 2: Load application template
    try:
        template = load_application_template(request.loanType)
        logger.info(f"Loaded template for {request.loanType}, length: {len(template)}")
    except ValueError as e:
        logger.warning(f"Application template validation failed: {e}")
        raise HTTPException(status_code=400, detail=INVALID_LOAN_TYPE_MESSAGE) from e
    except Exception as e:
        logger.error(f"Error loading template: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_TEMPLATE_LOAD_FAILED_MESSAGE) from e

    # Step 3: Generate application using AI
    # Requirement 3.2: Use AI to fill application template with customer information
    # Requirement 3.3: Generate blank template with "待补充" placeholders when customer not found
    try:
        prompt = build_generation_prompt(template, customer_data, customer_found, profile_markdown)

        # Call AI to generate the application
        application_content = ai_service.extract(prompt, "请生成申请表")

        if not application_content:
            raise HTTPException(status_code=500, detail=APPLICATION_GENERATION_FAILED_MESSAGE)

        logger.info(f"Generated application, length: {len(application_content)}")

    except AIServiceError as e:
        logger.error(f"AI service error: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_GENERATION_FAILED_MESSAGE) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error generating application: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_GENERATION_FAILED_MESSAGE) from e

    # Step 4: Validate no fabrication
    # Requirement 3.4: Validate no fabrication of critical fields
    warnings: list[str] = []

    try:
        validation_result = validate_no_fabrication(application_content, customer_data)

        if not validation_result["is_valid"]:
            warnings.extend(validation_result["warnings"])
            logger.warning(f"Fabrication detected: {validation_result['fabricated_fields']}")
    except Exception as e:
        logger.warning(f"Validation error (non-fatal): {e}")
        warnings.append(APPLICATION_VALIDATION_WARNING_MESSAGE)

    # Step 5: Build structured application data for card rendering
    application_data = parse_application_markdown_to_data(application_content)
    if not application_data:
        application_data = build_application_data(customer_data, request.loanType)
    if request.loanType == "enterprise" and application_data:
        application_data = _enhance_enterprise_application_data(application_data, customer_data)
        application_content = _render_application_markdown(request.loanType, application_data)

    if HAS_DB_STORAGE and request.customerId and application_data:
        try:
            await profile_sync_service.handle_application_generated(
                storage_service=storage_service,
                customer_name=request.customerName.strip(),
                customer_id=request.customerId,
                loan_type=request.loanType,
                application_data=application_data,
                owner_username=current_user.get("username") or "",
            )
        except Exception as sync_exc:
            logger.warning(
                "application_sync finish customer_id=%s operation_type=application_generated status=failed error=%s",
                request.customerId,
                sync_exc,
            )

    generation_metadata: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "customer_id": request.customerId or "",
        "stale": False,
        "stale_reason": "",
        "stale_at": "",
        "data_sources": (
            ["customer_profile_markdown", "parsed_document_text", "application_summary"]
            if request.customerId
            else ["manual_customer_name"]
        ),
    }
    if HAS_DB_STORAGE and request.customerId:
        try:
            profile, _ = await get_or_create_customer_profile(storage_service, request.customerId)
            generation_metadata["profile_version"] = (profile or {}).get("version") or 1
            generation_metadata["profile_updated_at"] = (profile or {}).get("updated_at") or ""
        except Exception as profile_exc:
            logger.warning("Failed to load profile metadata for application %s: %s", request.customerId, profile_exc)
            generation_metadata["profile_version"] = 1
            generation_metadata["profile_updated_at"] = ""

    add_activity(
        activity_type="application",
        customer=request.customerName.strip() if request.customerName else "",
        customer_id=request.customerId,
        username=current_user.get("username") or "",
        status="completed",
        title="贷款申请表已生成",
        description="系统已基于当前客户资料生成最新申请表，可继续保存、复制或查看依据。",
        metadata={
            "loanType": request.loanType,
            "customerFound": customer_found,
            "warningCount": len(warnings),
            "profileMarkdownIncluded": bool(profile_markdown),
        },
    )
    if request.customerName and request.customerName.strip():
        update_customer_status(request.customerName.strip(), has_application=True)

    # Step 6: Return response
    # Requirement 3.5: Return applicationContent in Markdown format
    return ApplicationResponse(
        applicationContent=application_content,
        applicationData=application_data,
        customerFound=customer_found,
        warnings=warnings,
        metadata=generation_metadata,
    )


async def _create_application_job(
    request: ApplicationRequest,
    current_user: dict,
) -> ChatJobCreateResponse:
    job_id = uuid.uuid4().hex
    request_payload = request.model_dump()
    execution_payload = _build_application_generate_job_execution_payload(job_id, request, current_user)
    logger.info(
        "[Application Job] execution payload prepared job_id=%s customer_id=%s username=%s payload_keys=%s",
        job_id,
        request.customerId or "",
        current_user.get("username") or "",
        sorted(execution_payload.keys()),
    )
    await storage_service.create_async_job(
        {
            "job_id": job_id,
            "job_type": "application_generate",
            "customer_id": request.customerId or "",
            "username": current_user.get("username") or "",
            "status": "pending",
            "progress_message": "已接收任务",
            "request_json": request_payload,
            "execution_payload_json": execution_payload,
        }
    )
    logger.info(
        "[Application Job] created job_id=%s job_type=%s username=%s customer_id=%s request_snapshot=%s",
        job_id,
        "application_generate",
        current_user.get("username") or "",
        request.customerId or "",
        {
            "customerName": request.customerName,
            "loanType": request.loanType,
        },
    )
    persisted_execution_payload = await storage_service.get_async_job_execution_payload(job_id)
    if not persisted_execution_payload:
        logger.warning(
            "[Application Job] execution payload missing after create job_id=%s, retrying payload save",
            job_id,
        )
        await storage_service.set_async_job_execution_payload(job_id, execution_payload)
        persisted_execution_payload = await storage_service.get_async_job_execution_payload(job_id)
    if not persisted_execution_payload:
        raise HTTPException(status_code=500, detail="申请表生成任务载荷保存失败")
    logger.info(
        "[Application Job] execution payload saved job_id=%s customer_id=%s username=%s payload_keys=%s payload_customer_name=%s",
        job_id,
        request.customerId or "",
        current_user.get("username") or "",
        sorted(persisted_execution_payload.keys()),
        persisted_execution_payload.get("customerName") or "",
    )
    await _dispatch_application_generate_job(job_id, request.customerId or "", current_user)
    return ChatJobCreateResponse(jobId=job_id, status="pending")


async def _create_application_job_safe(
    request: ApplicationRequest,
    current_user: dict,
) -> ChatJobCreateResponse | JSONResponse:
    try:
        return await _create_application_job(request, current_user)
    except HTTPException as exc:
        logger.exception("error detail")
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})
    except Exception as exc:
        logger.exception("error detail")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/jobs", response_model=ChatJobCreateResponse)
async def create_application_job(
    request: ApplicationRequest,
    current_user: dict = Depends(get_current_user),
) -> ChatJobCreateResponse:
    return await _create_application_job_safe(request, current_user)
