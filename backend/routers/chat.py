"""
Chat Router

Handles conversational interactions with intent recognition.
Supports file extraction, application generation, scheme matching, and general chat.

Requirements:
- 5.1: Accept messages array with role and content
- 5.2: Recognize intents: extract, application, matching, chat
- 5.3: Handle "extract" intent - process files and return extraction data
- 5.4: Handle "application" intent - trigger application generation
- 5.5: Handle "matching" intent - trigger scheme matching
- 5.6: Handle "chat" intent - return conversational response
- 5.7: Return ChatResponse with message, intent, data
"""

import asyncio
import json
import logging
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
from services.ai_service import AIService, AIServiceError
from services.feishu_service import FeishuService, FeishuServiceError
from services.file_service import FileService
from services.ocr_service import OCRService
from services.wiki_service import WikiService, WikiServiceError
from utils.json_parser import parse_json

from backend.services import get_storage_service, supports_structured_storage
from backend.services.job_display_config import (
    build_job_result_summary,
    get_job_target_page,
    get_job_type_label,
)
from backend.services.markdown_profile_service import get_or_create_customer_profile
from backend.services.product_cache_service import get_cache_content
from backend.services.profile_sync_service import ProfileSyncService
from backend.services.sqlalchemy_storage_service import SQLAlchemyStorageService

from ..middleware.auth import get_current_user_optional
from ..models.schemas import (
    ChatMessageRecordResponse,
    ChatJobCreateResponse,
    ChatJobSummaryResponse,
    ChatJobStatusResponse,
    ChatRequest,
    ChatResponse,
    ChatSessionCreateRequest,
    ChatSessionSummary,
)
from ..services.activity_service import add_activity, update_customer_status
from .chat_extraction import (
    extract_customer_fields_from_feishu,
)
from .chat_extraction import (
    init_services as init_extraction_services,
)
from .chat_extraction import (
    process_chat_files as _process_chat_files,
)
from .chat_extraction import AI_RISK_BLOCKED_ERROR

# Import helper modules
from .chat_helpers import (
    convert_matching_result_to_json,
    determine_loan_type_from_description,
    extract_customer_from_history,
    extract_customer_info_from_description,
    extract_customer_name_from_message,
    extract_params_from_message,
    get_customer_data_local,
    get_latest_application_for_customer,
    is_application_based_matching,
    is_instant_matching_request,
)
from .chat_storage import save_to_storage

logger = logging.getLogger(__name__)
_ACTIVE_CHAT_JOB_TASKS: set[asyncio.Task[None]] = set()

# Create router
router = APIRouter(prefix="/chat", tags=["Chat"])

# Initialize services
ai_service = AIService()
file_service = FileService()
ocr_service = OCRService()
feishu_service = FeishuService()
wiki_service = WikiService()
storage_service = get_storage_service()
chat_storage_service = SQLAlchemyStorageService()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
profile_sync_service = ProfileSyncService()

# Initialize extraction module services
init_extraction_services(ai_service, file_service, ocr_service)

# Cross-request customer name cache (desktop single-user scenario)
_customer_name_cache: str | None = None

MATCHING_SERVICE_UNAVAILABLE_MESSAGE = "方案匹配服务暂时不可用，请稍后重试。"
CUSTOMER_DATA_FETCH_FAILED_MESSAGE = "搜索客户资料时出错，请稍后重试。"
APPLICATION_GENERATION_FAILED_MESSAGE = "申请表生成失败，请稍后重试。"
CHAT_PROCESSING_FAILED_MESSAGE = "聊天处理失败，请稍后重试。"


# ===== Product Cache =====

def get_products_with_cache(credit_type: str) -> str:
    """获取产品库，优先使用本地缓存。

    Args:
        credit_type: "personal" 或 "enterprise_credit"/"enterprise_mortgage"

    Returns:
        产品库内容字符串
    """
    cache_key = _get_cache_key(credit_type)
    cached = get_cache_content(cache_key)
    if cached:
        logger.info(f"Using cached products [{cache_key}], length: {len(cached)}")
        return cached

    logger.info(f"Cache miss [{cache_key}], fetching from Feishu")
    return _fetch_products(credit_type)

def _get_cache_key(credit_type: str) -> str:
    """Map credit_type to cache key."""
    cache_key_map = {
        "personal": "personal",
        "enterprise_credit": "enterprise_credit",
        "enterprise_mortgage": "enterprise_mortgage",
        "enterprise": "enterprise_credit",
    }
    return cache_key_map.get(credit_type, "enterprise_credit")


def _fetch_products(credit_type: str) -> str:
    """Fetch products from wiki service."""
    if credit_type == "personal":
        return wiki_service.get_personal_products()
    if credit_type == "enterprise_mortgage":
        return wiki_service.get_document_content(wiki_service.PRODUCT_DOCS["enterprise_mortgage"])
    return wiki_service.get_document_content(wiki_service.PRODUCT_DOCS["enterprise_credit"])


# ===== Intent Recognition =====

INTENT_PROMPT = """你是一个贷款助手的意图识别模块。根据用户消息，识别用户意图。

可能的意图：
- extract: 用户想要提取文件中的数据（如上传文件、提取信息、识别文档等）
- application: 用户想要生成贷款申请表（如填写申请表、生成申请、申请贷款等）
- matching: 用户想要匹配贷款方案，包括：
  - 明确说"匹配方案"、"推荐产品"、"哪个贷款适合"
  - 描述客户情况并询问推荐（如"年开票3000万，负债1500万，可以推荐哪个产品？"）
  - 提供财务数据并询问贷款建议（如"征信良好，想贷100万，有什么方案？"）
- chat: 一般对话（如问候、咨询、其他问题等）

判断规则：
1. 如果用户描述了具体的财务数据（年开票、负债、征信、抵押等）并询问推荐/方案，识别为 matching
2. 如果用户只是问"可以贷多少"、"利率多少"等问题，识别为 matching
3. 如果用户只是闲聊或问一般问题，识别为 chat

只返回意图名称（extract/application/matching/chat），不要其他内容。

用户消息：{message}
"""

CHAT_SYSTEM_PROMPT = """你是一个专业的贷款助手，可以帮助用户：
1. 提取和分析贷款相关文件（征信报告、流水、财务数据等）
2. 生成贷款申请表
3. 匹配合适的贷款方案

请用友好、专业的语气回答用户的问题。如果用户需要上述功能，请引导他们使用相应的功能。
"""


def recognize_intent(message: str) -> str:
    """Recognize user intent from message.

    Args:
        message: The user's message content

    Returns:
        Intent string: "extract", "application", "matching", or "chat"
    """
    if not message or not message.strip():
        return "chat"

    try:
        prompt = INTENT_PROMPT.format(message=message)
        result = ai_service.extract(prompt, "请识别意图")

        if result:
            intent = result.strip().lower()
            valid_intents = ["extract", "application", "matching", "chat"]
            if intent in valid_intents:
                return intent
            for valid_intent in valid_intents:
                if valid_intent in intent:
                    return valid_intent

        logger.warning("Could not recognize intent, defaulting to chat")
        return "chat"

    except AIServiceError as e:
        logger.error(f"Intent recognition failed: {e}")
        return "chat"
    except Exception as e:
        logger.error(f"Unexpected error in intent recognition: {e}")
        return "chat"


# ===== Chat Response =====

def generate_chat_response(
    message: str, conversation_history: list[dict[str, str]]
) -> tuple[str, str]:
    """Generate a conversational response with reasoning.

    Args:
        message: The user's message
        conversation_history: Previous messages

    Returns:
        Tuple of (response, reasoning)
    """
    try:
        context = _build_conversation_context(conversation_history)
        system_prompt = f"{CHAT_SYSTEM_PROMPT}\n\n对话历史：\n{context}\n\n请回复用户的最新消息。"
        response, reasoning = ai_service.chat_with_reasoning(system_prompt, message)
        return (response or "抱歉，我暂时无法回答您的问题。请稍后再试。", reasoning)
    except AIServiceError as e:
        logger.error(f"Chat response generation failed: {e}")
        return ("抱歉，服务暂时不可用。请稍后再试。", "")
    except Exception as e:
        logger.error(f"Unexpected error in chat response: {e}")
        return ("抱歉，发生了意外错误。请稍后再试。", "")


def _build_conversation_context(conversation_history: list[dict[str, str]]) -> str:
    """Build conversation context string from history.

    Args:
        conversation_history: Previous messages

    Returns:
        Formatted context string
    """
    if not conversation_history:
        return ""
    recent = conversation_history[-5:]
    lines = []
    for msg in recent:
        role = "用户" if msg.get("role") == "user" else "助手"
        lines.append(f"{role}: {msg.get('content', '')}")
    return "\n".join(lines)


# ===== Application Template =====

def load_application_template(loan_type: str) -> str:
    """Load application template based on loan type.

    Args:
        loan_type: Either "enterprise" or "personal"

    Returns:
        Template content string
    """
    prompts = get_cached_prompts()
    if not prompts:
        prompts = load_prompts()

    template_files = {
        "enterprise": "申请表模板_企业贷款.md",
        "personal": "申请表模板_个人贷款.md",
    }
    template_file = template_files.get(loan_type)
    if not template_file:
        return _get_fallback_template(loan_type)

    template = prompts.get(template_file, "")
    return template if template else _get_fallback_template(loan_type)


def _get_fallback_template(loan_type: str) -> str:
    """Get fallback template when template file not found."""
    if loan_type == "enterprise":
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
    template: str, customer_data: dict, customer_found: bool, loan_type: str = "enterprise"
) -> str:
    """Build prompt for AI application generation. Delegates to chat_prompts.

    Args:
        template: The application template
        customer_data: Customer data dict
        customer_found: Whether customer data was found
        loan_type: "enterprise" or "personal"

    Returns:
        Complete prompt string
    """
    from .chat_prompts import build_generation_prompt as _build_prompt
    return _build_prompt(template, customer_data, customer_found, loan_type)


def _convert_application_data_to_markdown(
    data: dict, loan_type: str, customer_name: str
) -> str:
    """Convert application JSON data to Markdown for download.

    Args:
        data: Application data as JSON object
        loan_type: "enterprise" or "personal"
        customer_name: Customer name for title

    Returns:
        Markdown formatted string
    """
    title = "企业贷款申请表" if loan_type == "enterprise" else "个人贷款申请表"
    lines = [f"# {title}", ""]

    if customer_name:
        lines.extend([f"**客户名称**: {customer_name}", ""])

    for section_name, section_data in data.items():
        lines.extend([f"## {section_name}", "", "| 项目 | 填写内容 |", "|------|---------|"])
        if isinstance(section_data, dict):
            for field_name, field_value in section_data.items():
                value_str = str(field_value) if field_value else "待补充"
                lines.append(f"| {field_name} | {value_str} |")
        else:
            lines.append(f"| 数据 | {section_data} |")
        lines.append("")

    return "\n".join(lines)


# ===== Storage Wrapper =====

async def _save_to_feishu(
    chat_file_name: str,
    document_type: str,
    content: dict,
    customer_name: str | None,
    current_user: dict | None = None,
    target_customer_id: str | None = None,
) -> tuple[bool, str | None, str | None, list[dict]]:
    """Save extracted data to storage. Delegates to chat_storage module.

    Args:
        chat_file_name: Original filename
        document_type: Document type string
        content: Extracted content dict
        customer_name: Customer name
        current_user: Current user info
        target_customer_id: If set, merge into this existing customer

    Returns:
        Tuple of (saved, record_id, error_msg, similar_customers, customer_id)
    """
    return await save_to_storage(
        chat_file_name, document_type, content, customer_name, current_user,
        use_local=HAS_DB_STORAGE,
        storage_service=storage_service,
        feishu_service=feishu_service,
        target_customer_id=target_customer_id,
    )


# ===== Customer Data Fetching =====

async def _fetch_customer_data(
    customer_name: str, use_application: bool = False
) -> tuple[bool, dict, str | None, str | None]:
    """Fetch customer data from available sources.

    优先级：申请表缓存 > 本地 OCR 数据 > 飞书数据
    有申请表时自动使用申请表，无需用户显式指定。

    Args:
        customer_name: Customer name to search
        use_application: Deprecated, kept for compatibility. Auto-detection is now always on.

    Returns:
        Tuple of (found, data, loan_type_override, error_message)
    """
    # 1. 优先查申请表缓存（无论 use_application 是否为 True）
    app_found, app_data, app_loan_type = await get_latest_application_for_customer(customer_name)
    if app_found and app_data:
        logger.info(f"[Matching] 使用申请表数据匹配客户「{customer_name}」")
        return (True, app_data, app_loan_type, None)

    # 2. 没有申请表，用 OCR 提取数据
    logger.info(f"[Matching] 未找到申请表，使用 OCR 数据匹配客户「{customer_name}」")

    if HAS_DB_STORAGE:
        found, data = await get_customer_data_local(customer_name)
        if not found or not data:
            return (False, {}, None, f"未找到客户「{customer_name}」的资料，请先上传客户资料后再进行方案匹配。")
        return (True, data, None, None)

    try:
        records = feishu_service.search_records(customer_name.strip())
        if records:
            data = extract_customer_fields_from_feishu(records)
            return (True, data, None, None)
        return (False, {}, None, f"未找到客户「{customer_name}」的资料，请先上传客户资料后再进行方案匹配。")
    except FeishuServiceError as e:
        logger.error(f"Feishu service error: {e}")
        return (False, {}, None, CUSTOMER_DATA_FETCH_FAILED_MESSAGE)


def _extract_intent_params(
    message: str, conversation_history: list[dict[str, str]]
) -> tuple[str | None, str]:
    """Extract customer name and loan type from message/history.

    Args:
        message: User message
        conversation_history: Previous messages

    Returns:
        Tuple of (customer_name, loan_type)
    """
    params = extract_params_from_message(message, conversation_history)
    customer_name = params.get("customerName")
    loan_type = params.get("loanType") or "enterprise"

    if not customer_name:
        customer_name = extract_customer_from_history(conversation_history)
        if customer_name:
            logger.info(f"Extracted customer name from history: {customer_name}")

    return customer_name, loan_type


# ===== Scheme Matching =====

async def _execute_scheme_matching(
    customer_name: str, customer_data: dict, credit_type: str
) -> dict[str, Any]:
    """Execute AI scheme matching and return result.

    Args:
        customer_name: Customer name
        customer_data: Customer data dict
        credit_type: Credit type for product lookup

    Returns:
        Result dict with message and data
    """
    match_result = ""
    products = get_products_with_cache(credit_type)
    if not products:
        return {
            "message": "获取产品库失败，产品库为空。",
            "data": {"action": "matching", "error": "Empty product library"},
        }

    match_result = ai_service.match_scheme(
        customer_data=customer_data, products=products, credit_type=credit_type
    )
    if not match_result:
        return {
            "message": "方案匹配失败，AI 返回空结果。",
            "data": {"action": "matching", "error": "AI returned empty result"},
        }

    matching_data = convert_matching_result_to_json(match_result, customer_name, credit_type, products)
    add_activity(activity_type="matching", customer=customer_name, status="completed")
    update_customer_status(customer_name, has_matching=True)

    return {
        "message": f"✅ 已为客户「{customer_name}」完成方案匹配。",
        "data": {
            "action": "matching",
            "customerFound": True,
            "customerName": customer_name,
            "creditType": credit_type,
            "matchingData": matching_data,
            "matchResult": match_result,
        },
    }


# ===== Intent Handlers =====

_CREDIT_TYPE_MAP = {
    "personal": "personal",
    "enterprise": "enterprise_credit",
}

_GENERIC_SELECTED_CUSTOMER_OVERRIDES = (
    "申请表",
    "贷款申请",
    "贷款方案",
    "匹配方案",
    "企业贷款",
    "个人贷款",
)


def _should_prefer_selected_customer(customer_name: str | None) -> bool:
    if not customer_name:
        return True
    normalized = customer_name.strip()
    if not normalized:
        return True
    return any(keyword in normalized for keyword in _GENERIC_SELECTED_CUSTOMER_OVERRIDES)


async def handle_matching_intent(
    message: str,
    conversation_history: list[dict[str, str]],
    selected_customer_id: str | None = None,
    selected_customer_name: str | None = None,
) -> dict[str, Any]:
    """Handle matching intent - get customer data and match schemes.

    Args:
        message: The user's message
        conversation_history: Previous messages

    Returns:
        Dictionary with message and data
    """
    logger.info("Handling matching intent")

    customer_name, loan_type = _extract_intent_params(message, conversation_history)
    credit_type = _CREDIT_TYPE_MAP.get(loan_type, "enterprise_credit")

    if _should_prefer_selected_customer(customer_name) and selected_customer_name:
        customer_name = selected_customer_name
        logger.info(
            "Using selected customer context for matching intent customer_id=%s customer_name=%s",
            selected_customer_id,
            selected_customer_name,
        )

    if not customer_name:
        return {
            "message": (
                "好的，我可以帮您匹配贷款方案。\n\n"
                "请告诉我客户名称，例如：\n"
                "- 「帮王东海匹配方案」\n"
                "- 「北京科技有限公司，匹配贷款方案」"
            ),
            "data": {"action": "matching", "needsInput": True, "requiredFields": ["customerName"]},
        }

    logger.info(f"Matching scheme for customer: {customer_name}, credit_type: {credit_type}")

    use_app = is_application_based_matching(message)
    found, customer_data, loan_override, error_msg = await _fetch_customer_data(customer_name, use_app)

    if loan_override:
        loan_type = loan_override
        credit_type = _CREDIT_TYPE_MAP.get(loan_type, "enterprise_credit")

    if not found:
        action = "matching"
        return {"message": error_msg, "data": {"action": action, "customerFound": False, "customerName": customer_name}}

    try:
        return await _execute_scheme_matching(customer_name, customer_data, credit_type)
    except WikiServiceError as e:
        logger.error(f"Wiki service error: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "matching", "error": "wiki_service_error"},
        }
    except AIServiceError as e:
        logger.error(f"AI service error: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "matching", "error": "ai_service_error"},
        }
    except Exception as e:
        logger.error(f"Unexpected error matching schemes: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "matching", "error": "unexpected_error"},
        }


async def handle_instant_matching_intent(
    message: str, conversation_history: list[dict[str, str]]
) -> dict[str, Any]:
    """Handle instant matching - match from user description without customer data.

    Args:
        message: User message with customer situation
        conversation_history: Previous messages

    Returns:
        Dictionary with message and data
    """
    logger.info("Handling instant matching intent")

    customer_info = await extract_customer_info_from_description(message)
    if not customer_info:
        return {
            "message": (
                "抱歉，我无法从您的描述中提取足够的信息进行方案匹配。\n\n"
                "请提供更多客户信息，例如：\n"
                "- 年开票/年收入\n- 负债情况\n- 征信状况\n- 抵押物情况\n- 贷款需求金额"
            ),
            "data": {"action": "instant_matching", "needsInput": True, "extractedInfo": {}},
        }

    loan_type = determine_loan_type_from_description(customer_info, message)
    credit_type = "personal" if loan_type == "personal" else "enterprise_credit"

    try:
        products = get_products_with_cache(credit_type)
        if not products:
            return {"message": "获取产品库失败，产品库为空。", "data": {"action": "instant_matching", "error": "Empty product library"}}

        match_result = ai_service.match_scheme(customer_data=customer_info, products=products, credit_type=credit_type)
        matching_data = (
            convert_matching_result_to_json(match_result, "即时匹配", credit_type, products)
            if match_result
            else None
        )
        if not match_result:
            return {"message": "方案匹配失败，AI 返回空结果。", "data": {"action": "instant_matching", "error": "AI returned empty result"}}

        return {
            "message": "✅ 已根据您提供的信息完成方案匹配。",
            "data": {
                "action": "instant_matching",
                "customerFound": True,
                "customerName": "即时匹配",
                "creditType": credit_type,
                "extractedInfo": customer_info,
                "matchingData": matching_data,
                "matchResult": match_result,
            },
        }
    except WikiServiceError as e:
        logger.error(f"Wiki service error: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "instant_matching", "error": "wiki_service_error"},
        }
    except AIServiceError as e:
        logger.error(f"AI service error: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "instant_matching", "error": "ai_service_error"},
        }
    except Exception as e:
        logger.error(f"Unexpected error in instant matching: {e}")
        return {
            "message": MATCHING_SERVICE_UNAVAILABLE_MESSAGE,
            "data": {"action": "instant_matching", "error": "unexpected_error"},
        }


async def handle_application_intent(
    message: str,
    conversation_history: list[dict[str, str]],
    selected_customer_id: str | None = None,
    selected_customer_name: str | None = None,
) -> dict[str, Any]:
    """Handle application intent - search customer and generate application.

    Args:
        message: The user's message
        conversation_history: Previous messages

    Returns:
        Dictionary with message and data
    """
    logger.info("Handling application intent")

    customer_name, loan_type = _extract_intent_params(message, conversation_history)
    if loan_type not in ("enterprise", "personal"):
        loan_type = "enterprise"

    if _should_prefer_selected_customer(customer_name) and selected_customer_name:
        customer_name = selected_customer_name
        logger.info(
            "Using selected customer context for application intent customer_id=%s customer_name=%s",
            selected_customer_id,
            selected_customer_name,
        )

    if not customer_name:
        return {
            "message": (
                "好的，我可以帮您生成贷款申请表。\n\n"
                "请告诉我客户名称（企业名称或个人姓名），例如：\n"
                "- 「王东海，输出申请表」\n"
                "- 「帮我生成北京科技有限公司的申请表」"
            ),
            "data": {"action": "application", "needsInput": True, "requiredFields": ["customerName"]},
        }

    logger.info(f"Generating application for customer: {customer_name}, type: {loan_type}")

    customer_found, customer_data = await _fetch_application_customer_data(customer_name)
    if customer_found is None:
        # Error case - customer_data contains error message
        return customer_data

    try:
        return await _generate_application(
            customer_name,
            customer_data,
            customer_found,
            loan_type,
            selected_customer_id=selected_customer_id,
        )
    except AIServiceError as e:
        logger.error(f"AI service error: {e}")
        return {
            "message": APPLICATION_GENERATION_FAILED_MESSAGE,
            "data": {"action": "application", "error": "ai_service_error"},
        }
    except Exception as e:
        logger.error(f"Unexpected error generating application: {e}")
        return {
            "message": APPLICATION_GENERATION_FAILED_MESSAGE,
            "data": {"action": "application", "error": "unexpected_error"},
        }


async def _fetch_application_customer_data(
    customer_name: str,
) -> tuple[bool | None, dict]:
    """Fetch customer data for application generation.

    Args:
        customer_name: Customer name to search

    Returns:
        Tuple of (found, data). found=None means error, data=error_response.
    """
    if HAS_DB_STORAGE:
        found, data = await get_customer_data_local(customer_name)
        return (found, data)

    try:
        records = feishu_service.search_records(customer_name.strip())
        if records:
            data = extract_customer_fields_from_feishu(records)
            return (True, data)
        return (False, {})
    except FeishuServiceError as e:
        logger.error(f"Feishu service error: {e}")
        return (
            None,
            {
                "message": CUSTOMER_DATA_FETCH_FAILED_MESSAGE,
                "data": {"action": "application", "error": "customer_data_fetch_failed"},
            },
        )


async def _generate_application(
    customer_name: str,
    customer_data: dict,
    customer_found: bool,
    loan_type: str,
    selected_customer_id: str | None = None,
) -> dict[str, Any]:
    """Generate application using AI.

    Args:
        customer_name: Customer name
        customer_data: Customer data dict
        customer_found: Whether data was found
        loan_type: "enterprise" or "personal"

    Returns:
        Result dict with message and data
    """
    from services.ai_service import validate_no_fabrication

    template = load_application_template(loan_type)
    prompt = build_generation_prompt(template, customer_data, customer_found, loan_type)
    ai_result = ai_service.extract(prompt, "请生成申请表")

    if not ai_result:
        return {
            "message": "生成申请表失败，请稍后重试。",
            "data": {"action": "application", "error": "AI returned empty content"},
        }

    application_data = parse_json(ai_result)
    if application_data and loan_type == "enterprise":
        try:
            from backend.routers.application import (
                _enhance_enterprise_application_data,
                _render_application_markdown,
            )

            application_data = _enhance_enterprise_application_data(application_data, customer_data)
            application_content = _render_application_markdown(loan_type, application_data)
        except Exception as enhancement_exc:
            logger.warning("Failed to enhance enterprise application data: %s", enhancement_exc)
            application_content = _convert_application_data_to_markdown(application_data, loan_type, customer_name)
    else:
        application_content = (
            _convert_application_data_to_markdown(application_data, loan_type, customer_name)
            if application_data else ai_result
        )
    if application_data is None:
        logger.warning("Failed to parse application JSON, using raw content as Markdown")

    warnings = _validate_application(application_content or ai_result, customer_data, validate_no_fabrication)
    metadata: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "customer_id": selected_customer_id or "",
        "stale": False,
        "stale_reason": "",
        "stale_at": "",
        "data_sources": (
            ["customer_profile_markdown", "parsed_document_text", "application_summary"]
            if selected_customer_id
            else ["manual_customer_name"]
        ),
    }
    if HAS_DB_STORAGE and selected_customer_id:
        try:
            profile, _ = await get_or_create_customer_profile(storage_service, selected_customer_id)
            metadata["profile_version"] = (profile or {}).get("version") or 1
            metadata["profile_updated_at"] = (profile or {}).get("updated_at") or ""
        except Exception as profile_exc:
            logger.warning("Failed to load chat application profile metadata: %s", profile_exc)
            metadata["profile_version"] = 1
            metadata["profile_updated_at"] = ""
    response_message = _build_application_response_message(customer_name, customer_found, warnings)

    add_activity(activity_type="application", customer=customer_name, status="completed")
    update_customer_status(customer_name, has_application=True)

    return {
        "message": response_message,
        "data": {
            "action": "application",
            "customerFound": customer_found,
            "customerName": customer_name,
            "loanType": loan_type,
            "applicationData": application_data,
            "applicationContent": application_content,
            "warnings": warnings,
            "metadata": metadata,
        },
    }


def _validate_application(content: str, customer_data: dict, validate_fn) -> list[str]:
    """Validate application content for fabrication.

    Args:
        content: Application content to validate
        customer_data: Original customer data
        validate_fn: Validation function

    Returns:
        List of warning strings
    """
    try:
        result = validate_fn(content, customer_data)
        if not result["is_valid"]:
            return result["warnings"]
    except Exception as e:
        logger.warning(f"Validation error (non-fatal): {e}")
    return []


def _build_application_response_message(
    customer_name: str, customer_found: bool, warnings: list[str]
) -> str:
    """Build response message for application generation.

    Args:
        customer_name: Customer name
        customer_found: Whether data was found
        warnings: Validation warnings

    Returns:
        Response message string
    """
    if customer_found:
        msg = f"✅ 已找到客户「{customer_name}」的资料，申请表已生成。"
    else:
        msg = f"💡 未找到客户「{customer_name}」的资料，已生成空白模板供手动填写。"
    if warnings:
        msg += f"\n\n⚠️ 注意：{'; '.join(warnings)}"
    return msg


# ===== Chat Session Persistence =====


def _build_session_title(message: str, customer_name: str | None = None) -> str:
    base = (message or "").strip().replace("\n", " ")
    if not base:
        base = "新对话"
    if len(base) > 20:
        base = f"{base[:20]}..."
    if customer_name:
        return f"{customer_name} · {base}"
    return base


def _to_session_summary(session_data: dict[str, Any]) -> ChatSessionSummary:
    return ChatSessionSummary(
        sessionId=session_data.get("session_id") or "",
        title=session_data.get("title") or "",
        customerId=session_data.get("customer_id") or "",
        customerName=session_data.get("customer_name") or "",
        lastMessagePreview=session_data.get("last_message_preview") or "",
        createdAt=session_data.get("created_at") or "",
        updatedAt=session_data.get("updated_at") or "",
    )


def _to_chat_message_response(message_data: dict[str, Any]) -> ChatMessageRecordResponse:
    return ChatMessageRecordResponse(
        messageId=message_data.get("message_id") or "",
        sessionId=message_data.get("session_id") or "",
        role=message_data.get("role") or "user",
        content=message_data.get("content") or "",
        sequence=int(message_data.get("sequence") or 0),
        createdAt=message_data.get("created_at") or "",
    )


async def _ensure_chat_session(
    request: ChatRequest,
    user_message: str,
    current_user: dict | None,
) -> tuple[dict[str, Any], bool]:
    username = (current_user or {}).get("username") or "anonymous"
    session_id = request.sessionId
    session = None
    created = False
    if session_id:
        session = await chat_storage_service.get_chat_session(session_id)
    if not session:
        created = True
        session = await chat_storage_service.create_chat_session(
            {
                "session_id": session_id or uuid.uuid4().hex,
                "username": username,
                "customer_id": request.customerId or "",
                "customer_name": request.customerName or "",
                "title": _build_session_title(user_message, request.customerName),
                "last_message_preview": user_message.strip()[:500],
            }
        )
    return session, created


async def _persist_chat_exchange(
    request: ChatRequest,
    response_message: str,
    current_user: dict | None,
) -> dict[str, Any]:
    if not request.messages:
        return {}

    user_message = request.messages[-1].content
    session, created = await _ensure_chat_session(request, user_message, current_user)
    session_id = session.get("session_id") or ""
    existing_messages = await chat_storage_service.get_chat_messages(session_id)
    should_save_full_history = created or not existing_messages
    next_sequence = len(existing_messages)

    if should_save_full_history:
        for index, message in enumerate(request.messages):
            await chat_storage_service.save_chat_message(
                {
                    "message_id": uuid.uuid4().hex,
                    "session_id": session_id,
                    "role": message.role,
                    "content": message.content,
                    "sequence": index,
                }
            )
        next_sequence = len(request.messages)
    else:
        await chat_storage_service.save_chat_message(
            {
                "message_id": uuid.uuid4().hex,
                "session_id": session_id,
                "role": "user",
                "content": user_message,
                "sequence": next_sequence,
            }
        )
        next_sequence += 1

    await chat_storage_service.save_chat_message(
        {
            "message_id": uuid.uuid4().hex,
            "session_id": session_id,
            "role": "assistant",
            "content": response_message,
            "sequence": next_sequence,
        }
    )
    return session


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    if hasattr(model, "dict"):
        return model.dict()
    return dict(model)


async def _build_chat_response_payload(
    request: ChatRequest,
    intent: str,
    result: dict[str, Any],
    current_user: dict | None,
) -> dict[str, Any]:
    persisted_session: dict[str, Any] | None = None
    try:
        persisted_session = await _persist_chat_exchange(request, result["message"], current_user)
    except Exception as persist_exc:
        logger.warning("chat persistence failed in async job: %s", persist_exc)

    response_data = result.get("data")
    if not isinstance(response_data, dict):
        response_data = {} if response_data is None else {"value": response_data}
    if persisted_session:
        response_data["chatSession"] = _to_session_summary(persisted_session).dict()

    return {
        "message": result["message"],
        "intent": intent,
        "data": response_data or None,
        "reasoning": result.get("reasoning"),
    }


async def _run_chat_extract_job(
    job_id: str,
    request_payload: dict[str, Any],
    current_user_payload: dict[str, Any] | None,
) -> None:
    async def update_progress(message: str) -> None:
        logger.info("[Chat Job] progress job_id=%s stage=%s", job_id, message)
        await chat_storage_service.update_async_job(
            job_id,
            {"progress_message": message},
        )

    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("[Chat Job] start job_id=%s stage=%s", job_id, "已接收文件")
    await chat_storage_service.update_async_job(
        job_id,
        {
            "status": "running",
            "started_at": started_at,
            "progress_message": "正在解析上传资料",
        },
    )

    try:
        request = ChatRequest(**request_payload)
        if not request.files:
            raise ValueError("当前任务没有可处理的文件")
        if not request.messages:
            raise ValueError("当前任务没有有效的聊天消息")

        user_message = request.messages[-1].content
        conversation_history = [{"role": msg.role, "content": msg.content} for msg in request.messages[:-1]]
        await update_progress("已接收文件")
        await update_progress("正在识别资料类型")

        await chat_storage_service.update_async_job(
            job_id,
            {"progress_message": "正在提取文件内容并识别资料类型"},
        )
        result = await _handle_extract_intent(
            request,
            user_message,
            conversation_history,
            current_user_payload,
            progress_callback=update_progress,
        )

        await chat_storage_service.update_async_job(
            job_id,
            {"progress_message": "正在写入资料汇总并同步客户资料"},
        )
        response_payload = await _build_chat_response_payload(request, "extract", result, current_user_payload)

        await chat_storage_service.update_async_job(
            job_id,
            {
                "status": "success",
                "progress_message": "资料提取已完成",
                "result_json": response_payload,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        await update_progress("处理完成")
        logger.info("[Chat Job] success job_id=%s stage=finished", job_id)
    except Exception as exc:
        logger.error("[Chat Job] failed job_id=%s error=%s", job_id, exc, exc_info=True)
        await chat_storage_service.update_async_job(
            job_id,
            {
                "status": "failed",
                "progress_message": "资料提取失败",
                "error_message": str(exc) or "资料提取任务执行失败，请稍后重试。",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            },
        )


def _launch_chat_extract_job(
    job_id: str,
    request_payload: dict[str, Any],
    current_user_payload: dict[str, Any] | None,
) -> None:
    task = asyncio.create_task(_run_chat_extract_job(job_id, request_payload, current_user_payload))
    _ACTIVE_CHAT_JOB_TASKS.add(task)

    def _cleanup(done_task: asyncio.Task[None]) -> None:
        _ACTIVE_CHAT_JOB_TASKS.discard(done_task)
        try:
            done_task.result()
        except Exception:
            logger.exception("[Chat Job] background task crashed job_id=%s", job_id)

    task.add_done_callback(_cleanup)


@router.post("/sessions", response_model=ChatSessionSummary)
async def create_chat_session(
    request: ChatSessionCreateRequest,
    current_user: dict | None = Depends(get_current_user_optional),
) -> ChatSessionSummary:
    session = await chat_storage_service.create_chat_session(
        {
            "session_id": uuid.uuid4().hex,
            "username": (current_user or {}).get("username") or "anonymous",
            "customer_id": request.customerId or "",
            "customer_name": request.customerName or "",
            "title": request.title or _build_session_title("新对话", request.customerName),
            "last_message_preview": "",
        }
    )
    return _to_session_summary(session)


@router.get("/sessions", response_model=list[ChatSessionSummary])
async def list_chat_sessions(
    customer_id: str | None = None,
    current_user: dict | None = Depends(get_current_user_optional),
) -> list[ChatSessionSummary]:
    sessions = await chat_storage_service.list_chat_sessions(
        username=(current_user or {}).get("username") or "anonymous",
        customer_id=customer_id,
    )
    return [_to_session_summary(item) for item in sessions]


@router.get("/sessions/{session_id}/messages", response_model=list[ChatMessageRecordResponse])
async def list_chat_messages(
    session_id: str,
    _current_user: dict | None = Depends(get_current_user_optional),
) -> list[ChatMessageRecordResponse]:
    messages = await chat_storage_service.get_chat_messages(session_id)
    return [_to_chat_message_response(item) for item in messages]


@router.post("/jobs", response_model=ChatJobCreateResponse)
async def create_chat_job(
    request: ChatRequest,
    current_user: dict | None = Depends(get_current_user_optional),
) -> JSONResponse:
    if not request.files:
        raise HTTPException(status_code=400, detail="当前任务没有可处理的文件")
    if not request.messages:
        raise HTTPException(status_code=400, detail="当前任务没有有效的聊天消息")

    job_id = uuid.uuid4().hex
    username = (current_user or {}).get("username") or "anonymous"
    request_payload = _model_to_dict(request)

    await chat_storage_service.create_async_job(
        {
            "job_id": job_id,
            "job_type": "chat_extract",
            "customer_id": request.customerId or "",
            "username": username,
            "status": "pending",
            "progress_message": "任务已创建，等待后台处理",
            "request_json": request_payload,
        }
    )
    logger.info("[Chat Job] created job_id=%s username=%s customer_id=%s", job_id, username, request.customerId or "")
    _launch_chat_extract_job(job_id, request_payload, current_user)
    return JSONResponse(content={"jobId": job_id, "status": "pending"})


@router.get("/jobs/{job_id}", response_model=ChatJobStatusResponse)
async def get_chat_job(
    job_id: str,
    current_user: dict | None = Depends(get_current_user_optional),
) -> ChatJobStatusResponse:
    job = await chat_storage_service.get_async_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="未找到该任务")

    username = (current_user or {}).get("username") or "anonymous"
    if job.get("username") and job.get("username") != username:
        raise HTTPException(status_code=403, detail="无权查看该任务")

    result_payload = job.get("result_json") or None

    job_type = job.get("job_type") or "chat_extract"
    customer_name = job.get("customer_name") or ""
    typed_result = result_payload if isinstance(result_payload, dict) and result_payload else None

    return ChatJobStatusResponse(
        jobId=job.get("job_id") or job_id,
        jobType=job_type,
        jobTypeLabel=get_job_type_label(job_type),
        customerId=job.get("customer_id") or "",
        customerName=customer_name,
        status=job.get("status") or "pending",
        progressMessage=job.get("progress_message") or "",
        result=typed_result,
        errorMessage=job.get("error_message") or None,
        createdAt=job.get("created_at") or "",
        startedAt=job.get("started_at") or "",
        finishedAt=job.get("finished_at") or "",
        targetPage=get_job_target_page(job_type),
        resultSummary=build_job_result_summary(job_type, typed_result, customer_name),
    )


@router.get("/jobs", response_model=list[ChatJobSummaryResponse])
async def list_chat_jobs(
    limit: int = 10,
    current_user: dict | None = Depends(get_current_user_optional),
) -> list[ChatJobSummaryResponse]:
    username = (current_user or {}).get("username") or "anonymous"
    jobs = await chat_storage_service.list_async_jobs(username=username, limit=max(1, min(limit, 30)))
    return [
        ChatJobSummaryResponse(
            jobId=job.get("job_id") or "",
            jobType=(job_type := job.get("job_type") or "chat_extract"),
            jobTypeLabel=get_job_type_label(job_type),
            customerId=job.get("customer_id") or "",
            customerName=(customer_name := job.get("customer_name") or ""),
            status=job.get("status") or "pending",
            progressMessage=job.get("progress_message") or "",
            errorMessage=job.get("error_message") or None,
            createdAt=job.get("created_at") or "",
            startedAt=job.get("started_at") or "",
            finishedAt=job.get("finished_at") or "",
            targetPage=get_job_target_page(job_type),
            resultSummary=build_job_result_summary(job_type, job.get("result_json") if isinstance(job.get("result_json"), dict) else None, customer_name),
        )
        for job in jobs
    ]


# ===== Main Chat Route =====

@router.post("", response_model=ChatResponse)
async def chat(
    request: ChatRequest, current_user: dict | None = Depends(get_current_user_optional)
) -> ChatResponse:
    """Process a chat message and return an AI response.

    Args:
        request: ChatRequest with messages array and optional files

    Returns:
        ChatResponse with message, intent, and data
    """
    logger.info(f"Chat request received with {len(request.messages)} messages")

    if not request.messages:
        raise HTTPException(status_code=400, detail="Messages array is required")

    last_message = request.messages[-1]
    user_message = last_message.content
    if not user_message or not user_message.strip():
        raise HTTPException(status_code=400, detail="Last message content is empty")

    conversation_history = [{"role": msg.role, "content": msg.content} for msg in request.messages[:-1]]
    intent = recognize_intent(user_message)
    logger.info(f"Recognized intent: {intent}")

    try:
        result = await _dispatch_intent(intent, request, user_message, conversation_history, current_user)
        persisted_session: dict[str, Any] | None = None
        try:
            persisted_session = await _persist_chat_exchange(request, result["message"], current_user)
        except Exception as persist_exc:
            logger.warning("chat persistence failed: %s", persist_exc)
        response_data = result.get("data")
        if not isinstance(response_data, dict):
            response_data = {} if response_data is None else {"value": response_data}
        if persisted_session:
            response_data["chatSession"] = _to_session_summary(persisted_session).dict()
        return ChatResponse(
            message=result["message"], intent=intent,
            data=response_data or None, reasoning=result.get("reasoning"),
        )
    except HTTPException:
        raise
    except AIServiceError as e:
        logger.error(f"AI service error in chat: {e}")
        raise HTTPException(status_code=500, detail=CHAT_PROCESSING_FAILED_MESSAGE) from e
    except Exception as e:
        logger.error(f"Unexpected error in chat: {e}")
        raise HTTPException(status_code=500, detail=CHAT_PROCESSING_FAILED_MESSAGE) from e


async def _dispatch_intent(
    intent: str,
    request: ChatRequest,
    user_message: str,
    conversation_history: list[dict[str, str]],
    current_user: dict | None,
) -> dict[str, Any]:
    """Dispatch to the appropriate intent handler.

    Args:
        intent: Recognized intent string
        request: Original chat request
        user_message: Last user message
        conversation_history: Previous messages
        current_user: Current user info

    Returns:
        Result dict with message, data, and optional reasoning
    """
    if intent == "extract":
        return await _handle_extract_intent(request, user_message, conversation_history, current_user)
    elif intent == "application":
        return await handle_application_intent(
            user_message,
            conversation_history,
            selected_customer_id=request.customerId,
            selected_customer_name=request.customerName,
        )
    elif intent == "matching":
        if is_instant_matching_request(user_message):
            logger.info("Detected instant matching request")
            return await handle_instant_matching_intent(user_message, conversation_history)
        return await handle_matching_intent(
            user_message,
            conversation_history,
            selected_customer_id=request.customerId,
            selected_customer_name=request.customerName,
        )
    else:
        msg, reasoning = generate_chat_response(user_message, conversation_history)
        return {"message": msg, "data": None, "reasoning": reasoning}


async def _handle_extract_intent(
    request: ChatRequest,
    user_message: str,
    conversation_history: list[dict[str, str]],
    current_user: dict | None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    """Handle extract intent - process uploaded files.

    Args:
        request: Chat request with files
        user_message: User message
        conversation_history: Previous messages
        current_user: Current user info

    Returns:
        Result dict with message and data
    """
    if not request.files or len(request.files) == 0:
        return {"message": "请上传需要提取数据的文件（支持 PDF、图片、Excel 格式）。", "data": None}

    global _customer_name_cache

    explicit_customer_name, contextual_customer_name = _resolve_customer_name_for_extract(
        user_message, conversation_history
    )

    response_data = await _process_chat_files(
        request.files,
        explicit_customer_name,
        contextual_customer_name,
        current_user,
        _save_to_feishu,
        merge_decisions=request.mergeDecisions,
        progress_callback=progress_callback,
    )

    if progress_callback:
        await progress_callback("正在更新资料汇总")
    for customer_id in {
        file_result.get("customerId")
        for file_result in response_data.get("files", [])
        if file_result.get("savedToFeishu") and file_result.get("customerId")
    }:
        try:
            await profile_sync_service.handle_document_saved(storage_service, customer_id)
        except Exception as exc:
            logger.warning(
                "profile_sync finish customer_id=%s operation_type=chat_extract status=failed error=%s",
                customer_id,
                exc,
            )

    _update_customer_name_cache(response_data)

    file_count = len(request.files)
    success_count = sum(
        1
        for f in response_data.get("files", [])
        if "error" not in f and not f.get("saveError")
    )
    message = _build_extract_summary(file_count, success_count, response_data.get("files", []))

    return {"message": message, "data": response_data}


def _resolve_customer_name_for_extract(
    user_message: str, conversation_history: list[dict[str, str]]
) -> tuple[str | None, str | None]:
    """Resolve customer name for extract intent.

    Split current explicit name from contextual fallback.

    Args:
        user_message: User message
        conversation_history: Previous messages

    Returns:
        Tuple of (explicit_name, contextual_name)
    """
    explicit_name = extract_customer_name_from_message(user_message)
    if explicit_name:
        logger.info(f"User specified customer name in message: {explicit_name}")
        return explicit_name, None

    history_name = extract_customer_from_history(conversation_history)
    if history_name:
        logger.info(f"[Customer Cache] Using name from conversation history: {history_name}")
        return None, history_name

    if _customer_name_cache:
        logger.info(f"[Customer Cache] Using cached name: {_customer_name_cache}")
        return None, _customer_name_cache

    return None, None


def _update_customer_name_cache(response_data: dict[str, Any]) -> None:
    """Update cross-request customer name cache from extraction results.

    Args:
        response_data: Response from process_chat_files
    """
    global _customer_name_cache
    for file_result in response_data.get("files", []):
        name = file_result.get("customerName")
        if name and "error" not in file_result:
            _customer_name_cache = name
            logger.info(f"[Customer Cache] Updated cache: {name}")
            return


@router.post("/clear-customer-cache")
async def clear_customer_cache() -> dict[str, str]:
    """Clear the cross-request customer name cache.

    Returns:
        Success message
    """
    global _customer_name_cache
    old_name = _customer_name_cache
    _customer_name_cache = None
    logger.info(f"[Customer Cache] Cleared (was: {old_name})")
    return {"message": f"已清除客户缓存（之前为：{old_name}）"}


def _build_extract_summary(
    file_count: int, success_count: int, files: list[dict[str, Any]]
) -> str:
    """Build summary message for file extraction."""
    failed_files = [file for file in files if file.get("error")]
    save_failed_files = [file for file in files if file.get("saveError")]
    ai_risk_only = bool(failed_files) and all(
        file.get("errorType") == AI_RISK_BLOCKED_ERROR for file in failed_files
    )
    missing_customer_name_only = bool(save_failed_files) and all(
        "缺少客户名称" in str(file.get("saveError", "")) for file in save_failed_files
    )

    if success_count == file_count:
        return f"已成功提取 {file_count} 个文件的数据。"
    if success_count > 0:
        if save_failed_files:
            if missing_customer_name_only:
                return f"已提取 {success_count}/{file_count} 个文件，其余文件因缺少客户名称未保存。"
            return f"已提取 {success_count}/{file_count} 个文件，部分文件提取成功但未保存。"
        if ai_risk_only:
            return f"已提取 {success_count}/{file_count} 个文件，部分文件被 AI 风控拦截。"
        return f"已提取 {success_count}/{file_count} 个文件的数据，部分文件处理失败。"
    if save_failed_files:
        if missing_customer_name_only:
            return "已提取文件内容，但因缺少客户名称未保存到资料汇总。"
        return "已提取文件内容，但未能保存到资料汇总。"
    if ai_risk_only:
        return "AI 服务风控拦截，未完成解析。"
    return "文件处理失败，请检查文件格式是否正确。"
