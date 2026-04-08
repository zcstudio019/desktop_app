"""
Scheme Matching Router

Handles loan scheme matching based on customer data and product libraries.
Retrieves product information from Feishu Wiki and uses AI to match schemes.

Requirements:
- 4.1: Retrieve product library from Feishu Wiki based on creditType
- 4.2: When creditType is "personal", retrieve personal loan products
- 4.3: When creditType is "enterprise" or "enterprise_credit", retrieve enterprise credit loan products
- 4.4: When creditType is "enterprise_mortgage", retrieve enterprise mortgage loan products
- 4.5: Use AI to match customer data against product requirements
- 4.6: Return matchResult in Markdown format with recommended schemes
- 4.7: Return 500 for service errors
"""

import json
import logging
import sys
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from backend.database import SessionLocal
from backend.db_models import SavedApplicationRecord
from backend.routers.chat_helpers import get_customer_data_local
from backend.services import get_storage_service, supports_structured_storage
from backend.services.activity_service import add_activity, update_customer_status
from backend.services.product_cache_service import get_cache_content, save_cache_map
from backend.services.profile_sync_service import ProfileSyncService
from services.ai_service import AIService, AIServiceError
from services.feishu_service import FeishuAuthError, FeishuNetworkError, FeishuService, FeishuServiceError
from services.wiki_service import WikiService, WikiServiceError

from ..middleware.auth import get_current_user
from ..models.schemas import (
    NaturalLanguageRequest,
    NaturalLanguageResponse,
    SaveApplicationRequest,
    SavedApplication,
    SavedApplicationListItem,
    SchemeMatchRequest,
    SchemeMatchResponse,
    SearchCustomerRequest,
    SearchCustomerResponse,
)

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/scheme", tags=["Scheme Matching"])

# Initialize services
wiki_service = WikiService()
ai_service = AIService()
feishu_service = FeishuService()
storage_service = get_storage_service()
HAS_DB_STORAGE = supports_structured_storage(storage_service)
profile_sync_service = ProfileSyncService()

INVALID_CREDIT_TYPE_MESSAGE = "贷款方案类型无效，请重新选择。"
PRODUCT_LIBRARY_FETCH_FAILED_MESSAGE = "产品库获取失败，请稍后重试。"
PRODUCT_LIBRARY_EMPTY_MESSAGE = "产品库暂时为空，请先刷新产品库缓存。"
SCHEME_MATCH_FAILED_MESSAGE = "方案匹配失败，请稍后重试。"
APPLICATION_LIST_FAILED_MESSAGE = "获取申请表列表失败，请稍后重试。"
APPLICATION_SAVE_FAILED_MESSAGE = "保存申请表失败，请稍后重试。"
APPLICATION_NOT_FOUND_MESSAGE = "申请表不存在。"
APPLICATION_DETAIL_FAILED_MESSAGE = "获取申请表详情失败，请稍后重试。"
APPLICATION_DELETE_FAILED_MESSAGE = "删除申请表失败，请稍后重试。"
NATURAL_LANGUAGE_PARSE_FAILED_MESSAGE = "自然语言解析失败，请稍后重试。"
CUSTOMER_SEARCH_FAILED_MESSAGE = "客户搜索失败，请稍后重试。"

# 自然语言解析提示词
NATURAL_LANGUAGE_PARSE_PROMPT = """你是一个贷款信息提取助手。请从用户的自然语言描述中提取贷款相关信息。

用户描述：{text}

请提取以下可能的字段（如果提到的话）：
- 年销售额/年开票
- 现有负债
- 期望贷款金额
- 资产负债率
- 征信情况
- 抵押物信息
- 企业成立时间
- 行业类型
- 年纳税额
- 企业名称
- 法人姓名
- 其他相关信息

以 JSON 格式返回提取的信息，只返回 JSON，不要其他内容。
如果某个字段没有提到，不要包含在结果中。
数值请保留原始表述（如"1500万"而不是"15000000"）。

示例输入："1500万的销售额，负债已经有1500万了，再要贷款100万"
示例输出：
{{
  "年销售额": "1500万",
  "现有负债": "1500万",
  "期望贷款金额": "100万"
}}
"""


def save_product_cache(enterprise: str, personal: str) -> None:
    """保存产品库到数据库缓存。"""
    try:
        save_cache_map(enterprise, personal)
    except Exception as e:
        logger.error(f"保存产品库缓存失败: {e}")


def get_products_by_credit_type(credit_type: str) -> str:
    """Get product library content based on credit type.

    优先使用本地缓存，缓存不存在时从飞书获取并缓存。

    Maps the creditType parameter to the appropriate wiki_service method
    and retrieves the product library content.

    Args:
        credit_type: One of "personal", "enterprise_credit", or "enterprise_mortgage"

    Returns:
        Product library content as string

    Raises:
        ValueError: If credit_type is invalid
        WikiServiceError: If product retrieval fails

    **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

    注意：
    - #16: dict.get() 使用 `or ""` 处理 None
    """
    logger.info(f"Getting products for credit type: {credit_type}")

    # 1. 尝试从缓存加载
    if credit_type == "personal":
        # 尝试从缓存获取
        cached_products = get_cache_content("personal")
        if cached_products:
            logger.info(f"Using cached personal products, length: {len(cached_products)}")
            return cached_products

        # 缓存不存在，从飞书获取
        logger.info("Personal products cache miss, fetching from Feishu")
        products = wiki_service.get_personal_products()
        logger.info(f"Retrieved personal products, length: {len(products)}")

        # 保存到缓存（同时获取企业产品以完整缓存）
        enterprise_products = get_cache_content("enterprise_credit")
        if not enterprise_products:
            try:
                enterprise_products = wiki_service.get_enterprise_products()
            except Exception as e:
                logger.warning(f"Failed to fetch enterprise products for cache: {e}")
                enterprise_products = ""
        save_product_cache(enterprise_products, products)

        return products

    elif credit_type in ["enterprise_credit", "enterprise_mortgage", "enterprise"]:
        # 尝试从缓存获取
        cache_key = "enterprise_mortgage" if credit_type == "enterprise_mortgage" else "enterprise_credit"
        cached_products = get_cache_content(cache_key)
        if cached_products:
            logger.info(f"Using cached enterprise products, length: {len(cached_products)}")
            return cached_products

        # 缓存不存在，从飞书获取
        logger.info("Enterprise products cache miss, fetching from Feishu")
        if credit_type == "enterprise_mortgage":
            products = wiki_service.get_document_content(wiki_service.PRODUCT_DOCS["enterprise_mortgage"])
        else:
            products = wiki_service.get_enterprise_products()
        logger.info(f"Retrieved enterprise products, length: {len(products)}")

        # 保存到缓存（同时获取个人产品以完整缓存）
        personal_products = get_cache_content("personal")
        if not personal_products:
            try:
                personal_products = wiki_service.get_personal_products()
            except Exception as e:
                logger.warning(f"Failed to fetch personal products for cache: {e}")
                personal_products = ""
        if credit_type == "enterprise_mortgage":
            save_cache_map(get_cache_content("enterprise_credit"), personal_products, enterprise_mortgage=products)
        else:
            save_product_cache(products, personal_products)

        return products

    else:
        raise ValueError(
            f"Invalid creditType: {credit_type}. Must be 'personal', 'enterprise_credit', or 'enterprise_mortgage'"
        )


@router.post("/match", response_model=SchemeMatchResponse)
async def match_scheme(
    request: SchemeMatchRequest,
    current_user: dict = Depends(get_current_user),
) -> SchemeMatchResponse:
    """
    Match customer data against product libraries.

    This endpoint:
    1. Retrieves the appropriate product library from Feishu Wiki based on creditType
    2. Uses AI to match customer data against product requirements
    3. Returns recommended schemes in Markdown format

    **Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7**

    Args:
        request: SchemeMatchRequest with customerData and creditType

    Returns:
        SchemeMatchResponse with matchResult in Markdown format

    Raises:
        HTTPException 400: Invalid creditType
        HTTPException 500: Service errors (Wiki, AI)
    """
    logger.info("Matching scheme for user=%s, credit type=%s", current_user["username"], request.creditType)
    logger.debug(f"Customer data keys: {list(request.customerData.keys())}")

    # Step 1: Validate creditType
    valid_credit_types = ["personal", "enterprise_credit", "enterprise_mortgage"]
    if request.creditType not in valid_credit_types:
        raise HTTPException(status_code=400, detail=INVALID_CREDIT_TYPE_MESSAGE)

    # Step 2: Get product library from Feishu Wiki
    # Requirement 4.1: Retrieve product library from Feishu Wiki based on creditType
    try:
        products = get_products_by_credit_type(request.creditType)

        if not products:
            logger.warning(f"Empty product library for credit type: {request.creditType}")
            raise HTTPException(status_code=500, detail=PRODUCT_LIBRARY_EMPTY_MESSAGE)

        logger.info(f"Retrieved product library, length: {len(products)} characters")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=INVALID_CREDIT_TYPE_MESSAGE) from e

    except WikiServiceError as e:
        logger.error(f"Wiki service error: {e}")
        raise HTTPException(status_code=500, detail=PRODUCT_LIBRARY_FETCH_FAILED_MESSAGE) from e

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Unexpected error retrieving products: {e}")
        raise HTTPException(status_code=500, detail=PRODUCT_LIBRARY_FETCH_FAILED_MESSAGE) from e

    # Step 3: Call AI to match schemes
    # Requirement 4.5: Use AI to match customer data against product requirements
    try:
        match_result = ai_service.match_scheme(
            customer_data=request.customerData, products=products, credit_type=request.creditType
        )

        if not match_result:
            logger.warning("AI returned empty match result")
            raise HTTPException(status_code=500, detail=SCHEME_MATCH_FAILED_MESSAGE)

        logger.info(f"Scheme matching completed, result length: {len(match_result)} characters")

    except AIServiceError as e:
        logger.error(f"AI service error: {e}")
        raise HTTPException(status_code=500, detail=SCHEME_MATCH_FAILED_MESSAGE) from e

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Unexpected error matching schemes: {e}")
        raise HTTPException(status_code=500, detail=SCHEME_MATCH_FAILED_MESSAGE) from e

    if HAS_DB_STORAGE and request.customerId:
        try:
            await profile_sync_service.handle_scheme_matched(
                storage_service=storage_service,
                customer_id=request.customerId,
                customer_name=request.customerName or "",
                match_result=match_result,
            )
        except Exception as sync_exc:
            logger.warning(
                "scheme_snapshot finish customer_id=%s operation_type=scheme_matched status=failed error=%s",
                request.customerId,
                sync_exc,
            )

    add_activity(
        activity_type="matching",
        customer=request.customerName or "",
        customer_id=request.customerId,
        username=current_user.get("username") or "",
        status="completed",
        title="融资方案匹配已完成",
        description="系统已基于当前客户资料完成最新一轮融资方案匹配。",
        metadata={
            "creditType": request.creditType,
            "hasCustomerContext": bool(request.customerId),
        },
    )
    if request.customerName:
        update_customer_status(request.customerName, has_matching=True)

    # Step 4: Return response
    # Requirement 4.6: Return matchResult in Markdown format with recommended schemes
    return SchemeMatchResponse(matchResult=match_result)


# =============================================================================
# 申请表缓存管理 API
# =============================================================================


def _legacy_load_application_cache_file() -> list:
    """Deprecated local JSON helper kept only for historical reference."""
    logger.info("Legacy JSON application cache helper is disabled in SQLAlchemy mode")
    return []


def _legacy_save_application_cache_file(applications: list) -> None:
    """Deprecated local JSON helper kept only for historical reference."""
    logger.info("Legacy JSON application cache save helper is disabled in SQLAlchemy mode")

def load_application_cache() -> list:
    """Load saved applications from SQLAlchemy-backed storage."""
    try:
        with SessionLocal() as db:
            rows = (
                db.query(SavedApplicationRecord)
                .order_by(SavedApplicationRecord.saved_at.desc(), SavedApplicationRecord.id.desc())
                .all()
            )
            applications = []
            for row in rows:
                try:
                    application_data = json.loads(row.application_data) if row.application_data else {}
                except Exception:
                    application_data = {}
                applications.append(
                    {
                        "id": row.application_id,
                        "customerName": row.customer_name or "",
                        "customerId": row.customer_id or "",
                        "loanType": row.loan_type or "enterprise",
                        "applicationData": application_data,
                        "savedAt": row.saved_at or "",
                        "ownerUsername": row.owner_username or "",
                        "source": row.source or "manual",
                        "stale": bool(row.stale),
                        "stale_reason": row.stale_reason or "",
                        "stale_at": row.stale_at or "",
                        "profile_version": row.profile_version or 1,
                        "profile_updated_at": row.profile_updated_at or "",
                    }
                )
            logger.info("Loaded %s saved applications from SQLAlchemy storage", len(applications))
            return applications
    except Exception as exc:
        logger.error("Failed to load saved applications from SQLAlchemy storage: %s", exc)
        return []


def save_application_cache(applications: list) -> None:
    """Persist saved applications with SQLAlchemy instead of JSON cache."""
    try:
        with SessionLocal() as db:
            db.query(SavedApplicationRecord).delete()
            for app in applications:
                db.add(
                    SavedApplicationRecord(
                        application_id=app.get("id") or str(uuid.uuid4()),
                        customer_name=app.get("customerName") or "",
                        customer_id=app.get("customerId") or "",
                        loan_type=app.get("loanType") or "enterprise",
                        application_data=json.dumps(app.get("applicationData") or {}, ensure_ascii=False),
                        saved_at=app.get("savedAt") or datetime.now(tz=timezone.utc).isoformat(),
                        owner_username=app.get("ownerUsername") or "",
                        source=app.get("source") or "manual",
                        stale=1 if app.get("stale") else 0,
                        stale_reason=app.get("stale_reason") or "",
                        stale_at=app.get("stale_at") or "",
                        profile_version=int(app.get("profile_version") or 1),
                        profile_updated_at=app.get("profile_updated_at") or "",
                    )
                )
            db.commit()
        logger.info("Persisted %s saved applications to SQLAlchemy storage", len(applications))
    except Exception as exc:
        logger.error("Failed to save saved applications to SQLAlchemy storage: %s", exc)
        raise


def _is_admin(current_user: dict) -> bool:
    return current_user.get("role") == "admin"


def _can_access_application(app: dict, current_user: dict) -> bool:
    owner_username = app.get("ownerUsername")
    if not owner_username:
        return _is_admin(current_user)
    return owner_username == current_user.get("username") or _is_admin(current_user)


@router.get("/applications", response_model=list[SavedApplicationListItem])
async def list_saved_applications(current_user: dict = Depends(get_current_user)) -> list[SavedApplicationListItem]:
    """获取所有已保存的申请表列表

    Returns:
        List[SavedApplicationListItem]: 申请表列表（不含完整数据）
    """
    logger.info("获取已保存的申请表列表")

    try:
        applications = await storage_service.list_saved_applications()

        # 转换为列表项格式（不含完整 applicationData）
        result = []
        for app in applications:
            if not _can_access_application(app, current_user):
                continue
            result.append(
                SavedApplicationListItem(
                    id=app.get("id") or "",
                    customerName=app.get("customerName") or "",
                    customerId=app.get("customerId"),
                    loanType=app.get("loanType") or "",
                    savedAt=app.get("savedAt") or "",
                )
            )

        logger.info(f"返回 {len(result)} 条申请表记录")
        return result

    except Exception as e:
        logger.error(f"获取申请表列表失败: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_LIST_FAILED_MESSAGE) from e


@router.post("/applications", response_model=SavedApplication)
async def save_application(
    request: SaveApplicationRequest,
    current_user: dict = Depends(get_current_user),
) -> SavedApplication:
    """保存申请表到本地缓存

    Args:
        request: SaveApplicationRequest 包含 customerName, loanType, applicationData

    Returns:
        SavedApplication: 保存后的申请表（含生成的 ID 和时间戳）
    """
    logger.info(f"保存申请表: {request.customerName}")

    try:
        # 加载现有缓存
        applications = await storage_service.list_saved_applications()

        # 生成唯一 ID 和时间戳
        app_id = str(uuid.uuid4())
        saved_at = datetime.now(tz=timezone.utc).isoformat()
        profile_version = 1
        profile_updated_at = ""
        if request.customerId:
            profile = await storage_service.get_customer_profile(request.customerId)
            if profile:
                profile_version = int(profile.get("version") or 1)
                profile_updated_at = profile.get("updated_at") or ""

        # 创建新申请表记录
        new_application = {
            "id": app_id,
            "customerName": request.customerName,
            "customerId": request.customerId,
            "loanType": request.loanType,
            "applicationData": request.applicationData,
            "savedAt": saved_at,
            "ownerUsername": current_user["username"],
            "source": "manual",
            "stale": False,
            "profile_version": profile_version,
            "profile_updated_at": profile_updated_at,
        }

        # 添加到列表（新记录在前）
        applications.insert(0, new_application)
        # saved via SQLAlchemy storage

        # 保存到缓存
        # SQLAlchemy delete already persisted

        if request.customerId:
            await profile_sync_service.refresh_profile_and_index(
                storage_service=storage_service,
                customer_id=request.customerId,
                operation_type="application_summary_saved",
                refresh_profile=True,
            )

        logger.info(f"申请表保存成功: {app_id}")

        add_activity(
            activity_type="application",
            customer=request.customerName,
            customer_id=request.customerId,
            username=current_user.get("username") or "",
            status="completed",
            title="申请表已保存",
            description="系统已保存申请表，并同步更新资料汇总与问答索引。",
            metadata={
                "applicationId": app_id,
                "loanType": request.loanType,
                "savedAt": saved_at,
            },
        )
        update_customer_status(request.customerName, has_application=True)

        return SavedApplication(
            id=app_id,
            customerName=request.customerName,
            customerId=request.customerId,
            loanType=request.loanType,
            applicationData=request.applicationData,
            savedAt=saved_at,
        )

    except Exception as e:
        logger.error(f"保存申请表失败: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_SAVE_FAILED_MESSAGE) from e


@router.get("/applications/{application_id}", response_model=SavedApplication)
async def get_application(
    application_id: str,
    current_user: dict = Depends(get_current_user),
) -> SavedApplication:
    """获取单个申请表详情

    Args:
        application_id: 申请表 ID

    Returns:
        SavedApplication: 申请表详情

    Raises:
        HTTPException 404: 申请表不存在
    """
    logger.info(f"获取申请表详情: {application_id}")

    try:
        applications = await storage_service.list_saved_applications()

        # 查找指定 ID 的申请表
        for app in applications:
            if app.get("id") == application_id:
                if not _can_access_application(app, current_user):
                    break
                return SavedApplication(
                    id=app.get("id") or "",
                    customerName=app.get("customerName") or "",
                    customerId=app.get("customerId"),
                    loanType=app.get("loanType") or "",
                    applicationData=app.get("applicationData") or {},
                    savedAt=app.get("savedAt") or "",
                )

        # 未找到
        logger.warning(f"申请表不存在: {application_id}")
        raise HTTPException(status_code=404, detail=APPLICATION_NOT_FOUND_MESSAGE)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取申请表详情失败: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_DETAIL_FAILED_MESSAGE) from e


@router.delete("/applications/{application_id}")
async def delete_application(
    application_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict:
    """删除申请表

    Args:
        application_id: 申请表 ID

    Returns:
        dict: {"success": True, "message": "..."}

    Raises:
        HTTPException 404: 申请表不存在
    """
    logger.info(f"删除申请表: {application_id}")

    try:
        matched_application = await storage_service.get_saved_application(application_id)

        if not matched_application or not _can_access_application(matched_application, current_user):
            logger.warning(f"申请表不存在: {application_id}")
            raise HTTPException(status_code=404, detail=APPLICATION_NOT_FOUND_MESSAGE)

        await storage_service.delete_saved_application(application_id)

        # 保存更新后的缓存
        save_application_cache(applications)

        logger.info(f"申请表删除成功: {application_id}")
        return {"success": True, "message": f"申请表 {application_id} 已删除"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除申请表失败: {e}")
        raise HTTPException(status_code=500, detail=APPLICATION_DELETE_FAILED_MESSAGE) from e


# =============================================================================
# 自然语言解析 API
# =============================================================================


def parse_json_safely(text: str) -> dict:
    """安全解析 JSON，处理可能的格式问题

    Args:
        text: 可能包含 JSON 的文本

    Returns:
        dict: 解析后的字典，解析失败返回空字典

    注意：
    - #8: AI 输出 JSON 可能被截断或包含 markdown 代码块
    """
    if not text:
        return {}

    # 清理可能的 markdown 代码块标记
    clean_text = text.strip()
    if clean_text.startswith("```json"):
        clean_text = clean_text[7:]
    if clean_text.startswith("```"):
        clean_text = clean_text[3:]
    if clean_text.endswith("```"):
        clean_text = clean_text[:-3]
    clean_text = clean_text.strip()

    try:
        return json.loads(clean_text)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON 解析失败: {e}，尝试修复")

        # 尝试修复截断的 JSON
        # 补全缺失的括号
        open_braces = clean_text.count("{") - clean_text.count("}")
        open_brackets = clean_text.count("[") - clean_text.count("]")

        fixed_text = clean_text
        if open_braces > 0:
            fixed_text += "}" * open_braces
        if open_brackets > 0:
            fixed_text += "]" * open_brackets

        try:
            return json.loads(fixed_text)
        except json.JSONDecodeError:
            logger.error(f"JSON 修复失败，原始文本: {text[:200]}...")
            return {}


async def _resolve_local_customer_record_id(customer_name: str) -> str | None:
    """Resolve a local customer name to its stored customer_id."""
    clean_name = customer_name.strip()
    for candidate_id in [f"enterprise_{clean_name}", f"personal_{clean_name}", clean_name]:
        customer = await storage_service.get_customer(candidate_id)
        if customer:
            return candidate_id
    return None


@router.post("/parse-natural-language", response_model=NaturalLanguageResponse)
async def parse_natural_language(
    request: NaturalLanguageRequest,
    current_user: dict = Depends(get_current_user),
) -> NaturalLanguageResponse:
    """将自然语言描述转换为结构化客户数据

    输入示例："1500万的销售额，负债已经有1500万了，再要贷款100万"
    输出：{
        "年销售额": "1500万",
        "现有负债": "1500万",
        "期望贷款金额": "100万"
    }

    Args:
        request: NaturalLanguageRequest 包含 text 和 creditType

    Returns:
        NaturalLanguageResponse: 包含解析后的客户数据和已解析字段列表
    """
    logger.info("解析自然语言，user=%s，文本长度=%s 字符", current_user["username"], len(request.text))

    if not request.text.strip():
        return NaturalLanguageResponse(customerData={}, parsedFields=[])

    try:
        # 构建提示词
        prompt = NATURAL_LANGUAGE_PARSE_PROMPT.format(text=request.text)

        # 调用 AI 解析
        response = ai_service.client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "system", "content": prompt}, {"role": "user", "content": request.text}],
            temperature=0.1,
            max_tokens=2048,
        )

        result_text = response.choices[0].message.content or ""
        logger.debug(f"AI 返回结果: {result_text[:200]}...")

        # 解析 JSON
        # #8: 使用安全解析处理可能的格式问题
        customer_data = parse_json_safely(result_text)

        # 提取已解析的字段列表
        parsed_fields = list(customer_data.keys())

        logger.info(f"自然语言解析完成，提取了 {len(parsed_fields)} 个字段: {parsed_fields}")

        return NaturalLanguageResponse(customerData=customer_data, parsedFields=parsed_fields)

    except Exception as e:
        logger.error(f"自然语言解析失败: {e}")
        raise HTTPException(status_code=500, detail=NATURAL_LANGUAGE_PARSE_FAILED_MESSAGE) from e


# =============================================================================
# 飞书客户搜索 API
# =============================================================================


@router.post("/search-customer", response_model=SearchCustomerResponse)
async def search_customer(
    request: SearchCustomerRequest,
    current_user: dict = Depends(get_current_user),
) -> SearchCustomerResponse:
    """Search customer data from local storage or Feishu.

    This endpoint first checks local storage when enabled. Otherwise it falls
    back to Feishu record search and normalizes the returned field values into
    plain text.

    Args:
        request: Search request containing customerName.

    Returns:
        SearchCustomerResponse with found flag, normalized customerData, and recordId.

    Raises:
        HTTPException: Raised when customer search fails.
    """
    logger.info("Search customer request: %s, user=%s", request.customerName, current_user["username"])

    if not request.customerName or not request.customerName.strip():
        return SearchCustomerResponse(found=False, customerData={}, recordId=None)

    try:
        if HAS_DB_STORAGE:
            customer_found, customer_data = await get_customer_data_local(request.customerName.strip())
            if not customer_found:
                logger.info("Local customer not found: %s", request.customerName)
                return SearchCustomerResponse(found=False, customerData={}, recordId=None)

            record_id = await _resolve_local_customer_record_id(request.customerName.strip())
            logger.info(
                "Local customer found: %s, customer_id: %s, field_count: %s",
                request.customerName,
                record_id,
                len(customer_data),
            )
            return SearchCustomerResponse(found=True, customerData=customer_data, recordId=record_id)

        records = feishu_service.search_records(request.customerName.strip())

        if not records or len(records) == 0:
            logger.info("Feishu customer not found: %s", request.customerName)
            return SearchCustomerResponse(found=False, customerData={}, recordId=None)

        record = records[0]
        record_id = record.get("record_id") if record else None
        fields = record.get("fields") if record else None

        if not fields:
            logger.warning("Feishu record %s has no fields", record_id)
            return SearchCustomerResponse(found=True, customerData={}, recordId=record_id)

        customer_data = {}
        for field_name, field_value in fields.items():
            text_value = feishu_service._extract_text_value(field_value)
            customer_data[field_name] = text_value or ""

        logger.info(
            "Feishu customer found: %s, record_id: %s, field_count: %s",
            request.customerName,
            record_id,
            len(customer_data),
        )
        return SearchCustomerResponse(found=True, customerData=customer_data, recordId=record_id)

    except FeishuAuthError as e:
        logger.error("Feishu auth error during customer search: %s", e)
        raise HTTPException(status_code=500, detail=CUSTOMER_SEARCH_FAILED_MESSAGE) from e
    except FeishuNetworkError as e:
        logger.error("Feishu network error during customer search: %s", e)
        raise HTTPException(status_code=500, detail=CUSTOMER_SEARCH_FAILED_MESSAGE) from e
    except FeishuServiceError as e:
        logger.error("Feishu service error during customer search: %s", e)
        raise HTTPException(status_code=500, detail=CUSTOMER_SEARCH_FAILED_MESSAGE) from e
    except Exception as e:
        logger.error("Unexpected error during customer search: %s", e)
        raise HTTPException(status_code=500, detail=CUSTOMER_SEARCH_FAILED_MESSAGE) from e
