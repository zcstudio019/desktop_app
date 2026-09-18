"""Read-only customer material access and credit one-page report generation.

This module deliberately sits above the existing extraction/agent pipeline.  It only
reads persisted customer, profile, document and extraction records; it never runs
OCR, reparses a source document, or writes a derived report record.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any

from services.ai_service import AIService

logger = logging.getLogger(__name__)

CREDIT_REPORT_INTENT = "credit_one_page_report"
MAX_REPORT_CONTEXT_CHARS = 48_000
MAX_SINGLE_MATERIAL_CHARS = 14_000

_REPORT_PATTERNS = (
    re.compile(r"(?:生成|出|做|整理|分析).{0,12}(?:征信一页纸|征信速览|征信分析报告|标准征信报告|征信报告)"),
    re.compile(r"^(?:征信一页纸|征信速览|征信分析报告|标准征信报告)[，,。！!]*(?:生成|分析|报告)?$"),
    re.compile(r"分析.{0,12}(?:客户|申请人|主体).{0,5}征信(?:情况|信息)?$"),
)

_EXPLICIT_NAME_PATTERNS = (
    re.compile(r"(?:根据|用|读取|分析)\s*[“\"']?([\u4e00-\u9fffA-Za-z0-9·（）()\-]{2,40})[”\"']?\s*的(?:已有)?资料"),
    re.compile(r"(?:为|给)\s*[“\"']?([\u4e00-\u9fffA-Za-z0-9·（）()\-]{2,40})[”\"']?\s*(?:生成|做|出)"),
)

_TYPE_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("个人征信", ("personal_credit", "个人征信", "个人信用报告")),
    ("企业征信", ("enterprise_credit", "企业征信", "企业信用报告")),
    ("企业流水", ("enterprise_flow", "enterprise_bank", "企业流水", "对公流水")),
    ("个人流水", ("personal_flow", "personal_bank", "个人流水")),
    ("财务资料", ("financial_report", "financial_data", "财务报表", "财务资料")),
    (
        "KYC资料",
        (
            "id_card", "身份证", "business_license", "营业执照", "marriage", "结婚证",
            "hukou", "户口", "property", "房产", "real_estate", "不动产", "vehicle_license",
        ),
    ),
)

CREDIT_ONE_PAGE_REPORT_TEMPLATE = """# 征信速览报告

客户姓名 / 主体：{customer_name}

身份证号或统一社会信用代码脱敏：{masked_identifier}

报告生成时间：{generated_at}

---

## 🚨 紧急关注

（仅列真正需要优先处理的事项；没有明确紧急事项时写“暂无需要立即处理的明确风险事项。”）

# 一、申请人基本信息

（姓名、年龄、婚姻、身份、征信报告编号、征信历史、关联企业及其他有证据的重要身份信息）

## 核心指标

| 指标 | 当前情况 | 资料说明 |
|---|---:|---|
| 未结清贷款总额 | 资料不足 | 资料不足 |
| 未结清贷款笔数 | 资料不足 | 资料不足 |
| 信用卡使用率 | 资料不足 | 资料不足 |
| 贷款逾期次数 | 资料不足 | 资料不足 |
| 信用卡逾期次数 | 资料不足 | 资料不足 |
| 银行贷款余额 | 资料不足 | 资料不足 |
| 非银/网贷余额 | 资料不足 | 资料不足 |
| 对外担保余额 | 资料不足 | 资料不足 |
| 近6个月硬查询次数 | 资料不足 | 资料不足 |

# 二、未结清贷款明细

| 序号 | 贷款机构 | 机构类别 | 贷款类型 | 合同金额 | 当前余额 | 发放日期 | 到期日期 | 状态/备注 |
|---:|---|---|---|---:|---:|---|---|---|
| - | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |

**分类汇总：** 资料不足

**风险观察：** 资料不足

# 三、信用卡明细

| 发卡行 | 币种 | 信用额度 | 已用额度 | 使用率 | 逾期 | 备注 |
|---|---|---:|---:|---:|---|---|
| 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |

**分析：** 资料不足

**建议：** 资料不足

# 四、对外担保明细

| 被担保主体 | 与客户关系 | 贷款机构 | 担保金额 | 当前余额 | 担保日期 | 状态 |
|---|---|---|---:|---:|---|---|
| 资料不足 | 需核实 | 资料不足 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |

# 五、逾期与公共记录

- 贷款逾期：资料不足
- 信用卡逾期：资料不足
- 90天以上逾期：资料不足
- 诉讼/执行：资料不足
- 欠税：资料不足
- 非信贷交易记录：资料不足

# 六、征信查询记录

| 时间范围 | 贷款审批 | 信用卡审批 | 担保资格审查 | 法人资信审查 |
|---|---:|---:|---:|---:|
| 近1月 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近2月 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近3月 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近6月 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近9月 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近1年 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 近2年 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |

**查询频率分析：** 资料不足

**近期贷款审批查询明细：** 资料不足

# 七、历史贷款特征分析

（依次分析银行偏好、历史借款数量、还款表现、银行认可度、短期周转、网贷历史、负债集中度、近期新增贷款；无依据的项目写“资料不足”。）

# 八、银行征信准入指标检查

| 检查项 | 状态 | 当前情况 | 判断依据 | 优化方向 |
|---|---|---|---|---|
| 逾期记录 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 硬查询次数 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 信用卡使用率 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 负债收入比 DTI | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 网贷笔数 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 大额循环授信到期集中度 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 对外担保 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |
| 信用卡数量 | 资料不足 | 资料不足 | 资料不足 | 资料不足 |

# 九、征信优化路线图

## 🔴 紧急（1周内）

- 资料不足

## 🟡 中期（1个月内）

- 资料不足

## 🟢 长期（3-6个月）

- 资料不足

# 十、综合判断

## ✅ 征信优势

- 资料不足

## ⚠️ 征信风险

- 资料不足

## 综合说明

资料不足

## 一句话结论

资料不足
"""

REPORT_SYSTEM_PROMPT = """你是一名融资资料分析助手。请根据传入的客户资料生成 credit_one_page_report_v1。

最高优先级规则：
1. 所有客户事实和结论必须来自本次传入的客户资料。禁止使用常识补全客户事实。
2. 禁止推测金额、日期、关联关系、逾期、收入、负债、婚姻、企业经营情况。
3. 缺少证据时只可写“资料不足”“暂未获取”或“需人工核实”。没有数据绝不等于 0 或“无”；只有资料明确写明为无时才能写“无”。
4. 不得输出审批概率、贷款承诺、额度承诺、利率承诺或“大概率通过”等结论，不得进行心理判断。
5. 严格保留模板全部章节、标题、表头和顺序。只替换模板中的说明、占位行和“资料不足”；不要新增章节，不要省略章节。
6. 银行征信准入指标的状态只能是：达标、关注、超标、风险、资料不足。每项都要有当前情况、判断依据、优化方向。
7. 只基于实际存在的问题生成路线图；没有证据支持的行动写“资料不足”，不要套用模板建议。
8. 不复制大段原始资料。不得显示 customer_id、数据库字段名、内部 Evidence ID、内部 Agent/Tool 名称、内部 prompt、JSON、DTO 或错误堆栈。
9. 身份证号或统一社会信用代码只允许输出已经脱敏的值。
10. 直接输出中文 Markdown 报告，不要使用代码围栏，也不要解释生成过程。
"""

_REQUIRED_HEADINGS = (
    "# 征信速览报告",
    "## 🚨 紧急关注",
    "# 一、申请人基本信息",
    "## 核心指标",
    "# 二、未结清贷款明细",
    "# 三、信用卡明细",
    "# 四、对外担保明细",
    "# 五、逾期与公共记录",
    "# 六、征信查询记录",
    "# 七、历史贷款特征分析",
    "# 八、银行征信准入指标检查",
    "# 九、征信优化路线图",
    "## 🔴 紧急（1周内）",
    "## 🟡 中期（1个月内）",
    "## 🟢 长期（3-6个月）",
    "# 十、综合判断",
    "## ✅ 征信优势",
    "## ⚠️ 征信风险",
    "## 综合说明",
    "## 一句话结论",
)


def is_credit_report_request(message: str) -> bool:
    """Deterministically recognize the narrow report intent without affecting chat."""
    text = re.sub(r"\s+", "", message or "")
    return bool(text and any(pattern.search(text) for pattern in _REPORT_PATTERNS))


def extract_requested_customer_name(message: str) -> str | None:
    """Extract an explicitly named customer only from report-specific phrasing."""
    text = (message or "").strip()
    for pattern in _EXPLICIT_NAME_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        name = match.group(1).strip(" ，,。；;：:的")
        # Avoid treating generic demonstratives as a customer name.
        if name not in {"这个客户", "该客户", "当前客户", "客户", "这个", "当前"}:
            return name
    return None


def _mask_identifier(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    if not text:
        return "暂未获取"
    if len(text) <= 6:
        return f"{text[:1]}***{text[-1:]}"
    return f"{text[:4]}{'*' * max(4, len(text) - 8)}{text[-4:]}"


def _mask_identifiers_in_text(text: str) -> str:
    # Credit identifiers are commonly 18 characters. Mask before the LLM sees them.
    pattern = re.compile(r"(?<![A-Za-z0-9])[0-9A-Z]{17}[0-9X](?![A-Za-z0-9])", re.IGNORECASE)
    return pattern.sub(lambda match: _mask_identifier(match.group(0)), text)


def _safe_json(value: Any, limit: int = MAX_SINGLE_MATERIAL_CHARS) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        rendered = str(value)
    return _mask_identifiers_in_text(rendered[:limit])


def _material_group(extraction_type: str) -> str:
    normalized = str(extraction_type or "").lower()
    for group, aliases in _TYPE_GROUPS:
        if any(alias.lower() in normalized for alias in aliases):
            return group
    return "其他资料"


def _extract_material_text(extraction: dict[str, Any]) -> str:
    data = extraction.get("confirmed_data") or extraction.get("extracted_data") or {}
    if not isinstance(data, dict):
        return _safe_json(data)
    for key in ("markdown", "markdown_summary", "report_markdown", "summary_markdown", "content_markdown"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return _mask_identifiers_in_text(value.strip()[:MAX_SINGLE_MATERIAL_CHARS])
    return _safe_json(data)


async def get_customer_materials(storage_service: Any, customer_id: str) -> dict[str, Any]:
    """High-level read-only Tool: load an existing customer's persisted materials."""
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        return {"status": "customer_not_found", "customer": None, "materials": [], "processing": False}

    profile, extractions, documents = await asyncio.gather(
        storage_service.get_customer_profile(customer_id),
        storage_service.get_extractions_by_customer(customer_id),
        storage_service.list_documents(customer_id),
    )
    extractions = [item for item in (extractions or []) if isinstance(item, dict)]
    documents = [item for item in (documents or []) if isinstance(item, dict) and item.get("is_active", True)]
    processing = any(
        str(item.get("extraction_status") or "").lower() in {"pending", "running", "processing", "queued"}
        for item in extractions
    )
    processing = processing or bool(documents and not extractions)
    successful = [
        item for item in extractions
        if str(item.get("extraction_status") or "success").lower()
        in {"success", "completed", "partial", "partial_success", "partial_failed"}
    ]

    materials: list[dict[str, Any]] = []
    for item in successful:
        materials.append(
            {
                "category": _material_group(str(item.get("extraction_type") or "")),
                "type": str(item.get("extraction_type") or "其他资料"),
                "status": str(item.get("extraction_status") or "success"),
                "content": _extract_material_text(item),
                "created_at": str(item.get("created_at") or ""),
            }
        )

    profile_markdown = ""
    if isinstance(profile, dict):
        profile_markdown = _mask_identifiers_in_text(str(profile.get("markdown_content") or "").strip())

    return {
        "status": "ok",
        "customer": {
            "name": customer.get("name") or "暂未获取",
            "customer_type": customer.get("customer_type") or "暂未获取",
            "id_card_masked": _mask_identifier(customer.get("id_card")),
            "phone_masked": _mask_identifier(customer.get("phone")),
            "monthly_income": customer.get("monthly_income"),
            "income_source": customer.get("income_source") or None,
        },
        "profile_markdown": profile_markdown,
        "materials": materials,
        "document_count": len(documents),
        "successful_material_count": len(successful),
        "processing": processing,
    }


def _split_markdown_sections(markdown: str) -> list[str]:
    if not markdown.strip():
        return []
    chunks = re.split(r"(?=^#{1,4}\s+)", markdown, flags=re.MULTILINE)
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def _section_category(section: str) -> str:
    heading = section.splitlines()[0].lower() if section else ""
    if "个人征信" in heading or "个人信用" in heading:
        return "个人征信"
    if "企业征信" in heading or "企业信用" in heading:
        return "企业征信"
    if "企业流水" in heading or "对公流水" in heading:
        return "企业流水"
    if "个人流水" in heading:
        return "个人流水"
    if "财务" in heading:
        return "财务资料"
    if any(word in heading for word in ("客户基础", "身份证", "营业执照", "婚姻", "结婚", "户口", "房产", "不动产")):
        return "KYC资料"
    return "其他资料"


def build_report_context(material_result: dict[str, Any]) -> str:
    """Build a bounded, source-labelled LLM context from persisted results only."""
    customer = material_result.get("customer") or {}
    base = {
        "客户姓名/主体": customer.get("name") or "暂未获取",
        "客户类型": customer.get("customer_type") or "暂未获取",
        "身份证号（已脱敏）": customer.get("id_card_masked") or "暂未获取",
        "手机号（已脱敏）": customer.get("phone_masked") or "暂未获取",
        "月收入": customer.get("monthly_income") if customer.get("monthly_income") is not None else "资料不足",
        "收入来源": customer.get("income_source") or "资料不足",
    }
    buckets: dict[str, list[str]] = {name: [] for name, _ in _TYPE_GROUPS}
    buckets["其他资料"] = []

    for section in _split_markdown_sections(str(material_result.get("profile_markdown") or "")):
        buckets[_section_category(section)].append(section)
    for material in material_result.get("materials") or []:
        if not isinstance(material, dict):
            continue
        category = str(material.get("category") or "其他资料")
        content = str(material.get("content") or "").strip()
        if content:
            buckets.setdefault(category, []).append(content)

    # Credit evidence receives most of the context budget. Other categories are
    # deliberately smaller to prevent large transaction tables from overflowing.
    category_limits = {
        "个人征信": 16_000,
        "企业征信": 12_000,
        "企业流水": 4_000,
        "个人流水": 4_000,
        "财务资料": 4_000,
        "KYC资料": 4_000,
        "其他资料": 2_000,
    }
    parts = ["## 客户基本信息\n" + _safe_json(base, 3_000)]
    for category in ("个人征信", "企业征信", "企业流水", "个人流水", "财务资料", "KYC资料", "其他资料"):
        unique: list[str] = []
        seen: set[str] = set()
        for raw in buckets.get(category) or []:
            compact = raw.strip()
            fingerprint = compact[:300]
            if not compact or fingerprint in seen:
                continue
            seen.add(fingerprint)
            unique.append(compact)
        joined = "\n\n".join(unique)
        if joined:
            parts.append(f"## {category}\n{joined[:category_limits[category]]}")
        else:
            parts.append(f"## {category}\n未提供或未解析")

    processing_note = "有资料仍在解析，当前报告仅基于已成功保存的结果。" if material_result.get("processing") else "未发现仍在解析的资料。"
    parts.append(f"## 资料状态\n{processing_note}")
    return _mask_identifiers_in_text("\n\n".join(parts))[:MAX_REPORT_CONTEXT_CHARS]


async def resolve_report_customer(
    storage_service: Any,
    message: str,
    selected_customer_id: str | None,
) -> dict[str, Any]:
    """Resolve explicit name first, otherwise use the currently selected customer."""
    explicit_name = extract_requested_customer_name(message)
    if not explicit_name:
        if not selected_customer_id:
            return {"status": "missing_context"}
        customer = await storage_service.get_customer(selected_customer_id)
        return {"status": "ok", "customer": customer} if customer else {"status": "customer_not_found"}

    customers = await storage_service.list_customers()
    matches = [item for item in customers or [] if str(item.get("name") or "").strip() == explicit_name]
    if not matches:
        return {"status": "customer_not_found", "requested_name": explicit_name}
    if len(matches) > 1:
        choices = [
            {
                "name": item.get("name") or explicit_name,
                "customer_type": item.get("customer_type") or "类型未记录",
                "phone_masked": _mask_identifier(item.get("phone")),
                "updated_at": str(item.get("updated_at") or "更新时间未记录")[:19],
            }
            for item in matches
        ]
        return {"status": "ambiguous", "requested_name": explicit_name, "choices": choices}
    return {"status": "ok", "customer": matches[0]}


def _strip_internal_output(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:markdown)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = _mask_identifiers_in_text(cleaned)
    # Internal identifiers must never be displayed in the assistant response.
    cleaned = re.sub(r"(?im)^.*\b(?:customer_id|evidence_id|extraction_id|doc_id)\b.*$\n?", "", cleaned)
    return cleaned.strip()


def _has_complete_structure(report: str) -> bool:
    return all(heading in report for heading in _REQUIRED_HEADINGS)


async def generate_credit_one_page_report(
    storage_service: Any,
    ai_service: AIService,
    message: str,
    selected_customer_id: str | None = None,
) -> dict[str, Any]:
    """Complete report flow with business-safe failures and no writes."""
    try:
        resolved = await resolve_report_customer(storage_service, message, selected_customer_id)
    except Exception as exc:
        logger.exception("report customer resolution failed: %s", exc)
        return {"message": "读取客户信息失败，请稍后重试。", "data": {"reportStatus": "tool_failed"}}
    status = resolved.get("status")
    if status == "missing_context":
        return {
            "message": "请先选择需要生成报告的客户，或输入“根据客户名称的资料生成征信一页纸”。",
            "data": {"reportStatus": "waiting_for_input"},
        }
    if status == "customer_not_found":
        requested = resolved.get("requested_name")
        suffix = f"「{requested}」" if requested else "该客户"
        return {"message": f"未找到{suffix}的客户资料，请核对客户名称或先选择客户。", "data": {"reportStatus": "customer_not_found"}}
    if status == "ambiguous":
        choice_lines = []
        for index, item in enumerate(resolved.get("choices") or [], start=1):
            choice_lines.append(
                f"{index}. {item['name']}（{item['customer_type']}，联系方式 {item['phone_masked']}，更新 {item['updated_at']}）"
            )
        return {
            "message": "找到多个同名客户，请选择需要生成报告的客户。\n\n" + "\n".join(choice_lines),
            "data": {"reportStatus": "waiting_for_input", "reason": "duplicate_customer_name"},
        }

    customer = resolved.get("customer") or {}
    customer_id = str(customer.get("customer_id") or "")
    try:
        materials = await get_customer_materials(storage_service, customer_id)
    except Exception as exc:
        logger.exception("get_customer_materials failed: %s", exc)
        return {"message": "读取客户资料失败，请稍后重试。", "data": {"reportStatus": "tool_failed"}}

    if materials.get("status") != "ok":
        return {"message": "未找到该客户的已保存资料，请核对客户后重试。", "data": {"reportStatus": "customer_not_found"}}
    if not materials.get("profile_markdown") and not materials.get("materials"):
        if materials.get("processing"):
            message_text = "该客户资料仍在解析中，暂时没有可用于生成报告的已保存结果，请稍后再试。"
            report_status = "materials_processing"
        else:
            message_text = "该客户暂无已成功解析并保存的资料，暂时无法生成征信速览报告。"
            report_status = "no_materials"
        return {"message": message_text, "data": {"reportStatus": report_status}}

    customer_data = materials.get("customer") or {}
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    template = CREDIT_ONE_PAGE_REPORT_TEMPLATE.format(
        customer_name=customer_data.get("name") or "暂未获取",
        masked_identifier=customer_data.get("id_card_masked") or "暂未获取",
        generated_at=generated_at,
    )
    context = build_report_context(materials)
    user_prompt = f"""以下是只读获取的客户资料上下文。上下文之外的事实一律视为未知。

<customer_materials>
{context}
</customer_materials>

请填写以下固定模板。若个人征信未提供，也必须保留全部章节并明确写“未提供/资料不足”；不得把缺失项写成 0 或“无”。

<fixed_template>
{template}
</fixed_template>
"""
    try:
        raw_report = await asyncio.to_thread(
            ai_service.extract,
            REPORT_SYSTEM_PROMPT,
            user_prompt,
            "deepseek-chat",
            120,
            8192,
        )
    except Exception as exc:
        logger.exception("credit one-page report model call failed: %s", exc)
        return {"message": "征信速览报告生成失败，请稍后重试。", "data": {"reportStatus": "model_failed"}}

    report = _strip_internal_output(raw_report)
    if not report or not _has_complete_structure(report):
        logger.warning("credit one-page report failed structure validation")
        return {
            "message": "报告生成结果不完整，系统未向您展示可能缺失关键信息的内容，请稍后重试。",
            "data": {"reportStatus": "model_failed", "reason": "invalid_structure"},
        }
    return {
        "message": report,
        "data": {
            "reportStatus": "completed",
            "reportType": CREDIT_REPORT_INTENT,
            "templateVersion": "credit_one_page_report_v1",
            "materialsProcessing": bool(materials.get("processing")),
        },
    }
