"""
Wiki Router - 产品库缓存管理

提供产品库本地缓存功能，避免每次都从飞书获取。

Endpoints:
- GET /wiki/cache - 获取缓存的产品库内容
- POST /wiki/refresh - 从飞书刷新产品库缓存
- GET /wiki/cache-status - 获取缓存状态（最后更新时间等）
"""

import logging
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from services.wiki_service import WikiService, WikiServiceError
from backend.services.product_cache_service import get_cache_map, save_cache_map
from ..middleware.auth import get_current_user, require_admin

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/wiki", tags=["Wiki"])

# Initialize services
wiki_service = WikiService()
WIKI_REFRESH_FAILED_MESSAGE = "刷新产品库缓存失败，请稍后重试。"


# ==================== Response Models ====================


class CacheStatusResponse(BaseModel):
    """缓存状态响应模型"""

    cached: bool
    lastUpdated: str | None = None
    enterpriseProductCount: int = 0
    personalProductCount: int = 0


class CacheContentResponse(BaseModel):
    """缓存内容响应模型"""

    enterprise: str
    personal: str
    lastUpdated: str


class RefreshResponse(BaseModel):
    """刷新响应模型"""

    success: bool
    message: str
    lastUpdated: str


# ==================== Helper Functions ====================


def save_cache(enterprise: str, personal: str) -> str:
    """保存缓存到数据库。"""
    return save_cache_map(enterprise, personal)


def count_products(content: str) -> int:
    """统计产品数量（简单估算）

    通过统计内容中的产品标记来估算产品数量。

    Args:
        content: 产品库内容

    Returns:
        int: 估算的产品数量
    """
    if not content:
        return 0

    # 简单统计：按换行分割后统计非空行数作为粗略估计
    # 或者统计特定标记（如产品名称模式）
    lines = [line.strip() for line in content.split("\n") if line.strip()]

    # 粗略估计：每 10 行约 1 个产品
    return max(1, len(lines) // 10) if lines else 0


# ==================== Endpoints ====================


@router.get("/cache-status", response_model=CacheStatusResponse)
async def get_cache_status(_current_user: dict = Depends(get_current_user)) -> CacheStatusResponse:
    """
    获取缓存状态

    Returns:
        CacheStatusResponse with:
        - cached: 是否有缓存
        - lastUpdated: 最后更新时间
        - enterpriseProductCount: 企业产品数量估算
        - personalProductCount: 个人产品数量估算
    """
    logger.info("获取缓存状态")

    cache = get_cache_map()

    if not cache or (not cache.get("enterprise") and not cache.get("personal")):
        return CacheStatusResponse(cached=False, lastUpdated=None, enterpriseProductCount=0, personalProductCount=0)

    # #16: 使用 `or ""` 处理可能的 None 值
    enterprise_content = cache.get("enterprise") or ""
    personal_content = cache.get("personal") or ""
    last_updated = cache.get("lastUpdated") or ""

    return CacheStatusResponse(
        cached=True,
        lastUpdated=last_updated,
        enterpriseProductCount=count_products(enterprise_content),
        personalProductCount=count_products(personal_content),
    )


@router.get("/cache", response_model=CacheContentResponse)
async def get_cache(_current_user: dict = Depends(get_current_user)) -> CacheContentResponse:
    """
    获取缓存的产品库内容

    Returns:
        CacheContentResponse with:
        - enterprise: 企业产品库内容
        - personal: 个人产品库内容
        - lastUpdated: 最后更新时间

    Raises:
        HTTPException 404: 缓存不存在
    """
    logger.info("获取缓存内容")

    cache = get_cache_map()

    if not cache or (not cache.get("enterprise") and not cache.get("personal")):
        logger.warning("缓存不存在，返回 404")
        raise HTTPException(status_code=404, detail="产品库缓存不存在，请先刷新缓存")

    # #16: 使用 `or ""` 处理可能的 None 值
    enterprise = cache.get("enterprise") or ""
    personal = cache.get("personal") or ""
    last_updated = cache.get("lastUpdated") or ""

    if not enterprise and not personal:
        logger.warning("缓存内容为空")
        raise HTTPException(status_code=404, detail="产品库缓存内容为空，请刷新缓存")

    return CacheContentResponse(enterprise=enterprise, personal=personal, lastUpdated=last_updated)


@router.post("/refresh", response_model=RefreshResponse)
async def refresh_cache(_current_user: dict = Depends(require_admin)) -> RefreshResponse:
    """
    从飞书刷新产品库缓存

    调用 WikiService 获取最新的产品库数据并保存到本地缓存。

    Returns:
        RefreshResponse with:
        - success: 是否成功
        - message: 结果消息
        - lastUpdated: 更新时间

    Raises:
        HTTPException 500: 刷新失败
    """
    logger.info("开始刷新产品库缓存")

    try:
        # 获取企业产品库
        logger.info("获取企业产品库...")
        enterprise_products = wiki_service.get_enterprise_products()

        # 获取个人产品库
        logger.info("获取个人产品库...")
        personal_products = wiki_service.get_personal_products()

        # #12: 检查返回值是否为 None
        if enterprise_products is None:
            enterprise_products = ""
            logger.warning("企业产品库返回 None，使用空字符串")

        if personal_products is None:
            personal_products = ""
            logger.warning("个人产品库返回 None，使用空字符串")

        # 保存到缓存
        last_updated = save_cache(enterprise_products, personal_products)

        enterprise_len = len(enterprise_products)
        personal_len = len(personal_products)

        logger.info(f"产品库缓存刷新成功: 企业 {enterprise_len} 字符, 个人 {personal_len} 字符")

        return RefreshResponse(
            success=True,
            message=f"产品库缓存刷新成功（企业: {enterprise_len} 字符, 个人: {personal_len} 字符）",
            lastUpdated=last_updated,
        )

    except WikiServiceError as e:
        logger.error(f"刷新产品库缓存失败: {e}")
        raise HTTPException(status_code=500, detail=WIKI_REFRESH_FAILED_MESSAGE) from e
    except Exception as e:
        logger.error(f"刷新产品库缓存时发生未知错误: {e}")
        raise HTTPException(status_code=500, detail=WIKI_REFRESH_FAILED_MESSAGE) from e
