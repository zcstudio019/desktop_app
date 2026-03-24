"""
Customer List Router

Provides customer listing with role-based filtering:
- Admin users see all customers
- Normal users see only customers they uploaded (filtered by "上传账号" field)

Endpoints:
- GET /api/customers - List customers with optional search filter
"""

import logging
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import USE_LOCAL_STORAGE  # 新增：存储模式配置
from services.feishu_service import FeishuService

from backend.services import get_storage_service  # 新增：存储服务 factory

from ..middleware.auth import get_current_user
from ..models.schemas import CustomerDetail, CustomerListItem

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/customers", tags=["Customers"])

# Initialize service
feishu_service = FeishuService()
storage_service = get_storage_service()  # 根据配置返回本地存储或飞书服务


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


def _can_access_local_customer(customer: dict[str, Any], current_user: dict) -> bool:
    if _is_admin(current_user):
        return True
    uploader = str(customer.get("uploader") or "")
    username = _get_username(current_user)
    return bool(username) and uploader == username


def _ensure_local_customer_access(customer: dict[str, Any], current_user: dict) -> None:
    if not _can_access_local_customer(customer, current_user):
        raise HTTPException(status_code=403, detail="无权查看该客户记录")


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
    if USE_LOCAL_STORAGE:
        return await _list_customers_local(search, current_user)
    else:
        return await _list_customers_feishu(search, current_user)


async def _list_customers_local(
    search: str,
    current_user: dict,
) -> list[CustomerListItem]:
    """List customers from local SQLite database.

    Args:
        search: Optional search string to filter by customer name.
        current_user: The authenticated user.

    Returns:
        list[CustomerListItem]: List of customer records.
    """
    try:
        # 从本地数据库获取所有客户
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

        if not is_admin and (customer.get("uploader") or "") != username:
            continue

        # Search filter
        if search_text and search_text not in customer_name.lower():
            continue

        result.append(
            CustomerListItem(
                name=customer_name,
                record_id=customer_id,  # 使用 customer_id 作为 record_id
                uploader=customer.get("uploader") or "",
                upload_time=customer.get("upload_time") or created_at,
                customer_type=customer.get("customer_type") or "enterprise",
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
    if not USE_LOCAL_STORAGE:
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
    if not USE_LOCAL_STORAGE:
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
    if not USE_LOCAL_STORAGE:
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
        if not is_admin and (c.get("uploader") or "") != username:
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
    if USE_LOCAL_STORAGE:
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
    _ensure_local_customer_access(customer, current_user)

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
    if not USE_LOCAL_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    _ensure_local_customer_access(customer, current_user)

    try:
        extractions = await storage_service.get_extractions_by_customer(customer_id)
    except Exception as e:
        logger.error(f"Failed to fetch extractions for {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="获取资料数据失败") from e

    groups: dict[str, list[ExtractionItem]] = {}
    for ext in extractions:
        ext_type = ext.get("extraction_type") or "未知类型"
        item = ExtractionItem(
            extraction_id=ext.get("extraction_id") or "",
            extraction_type=ext_type,
            extracted_data=ext.get("extracted_data") or {},
            created_at=ext.get("created_at") or "",
        )
        groups.setdefault(ext_type, []).append(item)

    return [
        ExtractionGroup(extraction_type=ext_type, items=items)
        for ext_type, items in groups.items()
    ]


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
    if not USE_LOCAL_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    if body.field not in _UPDATABLE_CUSTOMER_FIELDS:
        raise HTTPException(status_code=400, detail=f"字段 '{body.field}' 不允许修改")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    _ensure_local_customer_access(customer, current_user)

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
    if not USE_LOCAL_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    _ensure_local_customer_access(customer, current_user)

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
    if not USE_LOCAL_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    _ensure_local_customer_access(customer, current_user)

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
    if not USE_LOCAL_STORAGE:
        raise HTTPException(status_code=400, detail="飞书模式暂不支持此功能")

    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="未找到该客户记录")
    _ensure_local_customer_access(customer, current_user)

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
