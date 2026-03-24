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

import json
import logging
import sys
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import USE_LOCAL_STORAGE
from prompts import get_cached_prompts, load_prompts
from backend.routers.chat_helpers import get_customer_data_local
from services.ai_service import AIService, AIServiceError, validate_no_fabrication
from services.feishu_service import FeishuService, FeishuServiceError

from ..middleware.auth import get_current_user
from ..models.schemas import ApplicationRequest, ApplicationResponse

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/application", tags=["Application Generation"])

# Initialize services
feishu_service = FeishuService()
ai_service = AIService()

INVALID_LOAN_TYPE_MESSAGE = "贷款类型无效，请选择企业贷款或个人贷款。"
CUSTOMER_DATA_SEARCH_FAILED_MESSAGE = "客户资料查询失败，请稍后重试。"
APPLICATION_TEMPLATE_LOAD_FAILED_MESSAGE = "申请表模板加载失败，请稍后重试。"
APPLICATION_GENERATION_FAILED_MESSAGE = "申请表生成失败，请稍后重试。"
APPLICATION_VALIDATION_WARNING_MESSAGE = "申请表生成后校验未完成，请人工复核关键信息。"


async def _load_customer_data(customer_name: str) -> tuple[bool, dict]:
    """Load customer data from the active storage backend."""
    if USE_LOCAL_STORAGE:
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


def build_generation_prompt(template: str, customer_data: dict, customer_found: bool) -> str:
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

    if request.customerName and request.customerName.strip():
        try:
            customer_found, customer_data = await _load_customer_data(request.customerName.strip())
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
        prompt = build_generation_prompt(template, customer_data, customer_found)

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
    application_data = build_application_data(customer_data, request.loanType)

    # Step 6: Return response
    # Requirement 3.5: Return applicationContent in Markdown format
    return ApplicationResponse(
        applicationContent=application_content,
        applicationData=application_data,
        customerFound=customer_found,
        warnings=warnings,
    )
