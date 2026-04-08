"""
Helper functions for chat router.

Extracted from chat.py to keep functions under 50 lines.
Contains:
- Customer name extraction and validation
- Parameter extraction from messages
- Instant matching detection
- Loan type determination
- Matching result conversion
- Customer data retrieval
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

# Add desktop_app to path for imports
desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from services.ai_service import AIService, AIServiceError
from utils.json_parser import parse_json

from backend.services import get_storage_service

logger = logging.getLogger(__name__)

# Initialize services (shared with chat.py)
ai_service = AIService()
storage_service = get_storage_service()

# ===== 客户名称相关常量 =====

_PRONOUN_EXCLUSIONS = {"该公司", "该企业", "该客户", "这个公司", "这家公司"}
_PRONOUN_WORDS = [
    "该公司", "该企业", "该客户", "这个公司", "这家公司",
    "这个企业", "这家企业", "他", "她", "它", "我", "我们",
]
_CUSTOMER_NAME_PLACEHOLDERS = {
    "无",
    "暂无",
    "待补充",
    "待确认",
    "未知",
    "未提供",
    "未提取",
    "空",
    "空白",
    "-",
    "--",
    "/",
    "n/a",
    "na",
    "null",
    "none",
}
_GENERIC_CUSTOMER_REFERENCE_PATTERNS = [
    r"^(一份)?申请表$",
    r"^(根据)?(这份|该份|上述|此份)?(资料|文件|申请表|模板|方案|内容)$",
    r"^(这位|这个|该)(客户|公司|企业)$",
    r"^(客户|公司|企业)$",
]
_GENERIC_CUSTOMER_REFERENCE_KEYWORDS = ("申请表", "资料", "文件", "模板", "方案", "内容")
_GENERIC_INTENT_ONLY_KEYWORDS = (
    "贷款",
    "融资",
    "匹配",
    "生成",
    "输出",
    "填写",
    "问答",
    "风险报告",
)
_COMPANY_NAME_HINTS = (
    "公司", "企业", "集团", "有限", "股份", "工作室", "事务所",
    "合作社", "商行", "门市部", "中心", "银行", "医院", "学校", "厂",
)


def _is_not_pronoun(name: str) -> bool:
    """Check if name is not a pronoun/reference word."""
    return name not in _PRONOUN_EXCLUSIONS


def _is_valid_customer_name(name: str) -> bool:
    """Check if name is a valid customer name (not null/None)."""
    return bool(name) and name != "null" and name != "None"


def _normalize_customer_name_candidate(name: str) -> str:
    """Normalize raw customer name candidates extracted from text."""
    if not name:
        return ""

    prefixes = [
        r"^名称[：:]\s*",
        r"^客户[：:]\s*",
        r"^客户名称[：:]\s*",
        r"^企业名称[：:]\s*",
        r"^公司名称[：:]\s*",
        r"^客户是[：:]\s*",
        r"^客户为[：:]\s*",
    ]

    normalized = name.strip().strip("，,。；;：:")
    for prefix in prefixes:
        normalized = re.sub(prefix, "", normalized)
    return normalized.strip().strip("，,。；;：:")


def _looks_like_customer_name(name: str) -> bool:
    """Check whether a candidate looks like a real customer name."""
    if not name:
        return False

    normalized = _normalize_customer_name_candidate(name)
    if not normalized:
        return False

    if normalized.lower() in _CUSTOMER_NAME_PLACEHOLDERS:
        return False

    if normalized in _PRONOUN_EXCLUSIONS or normalized in _PRONOUN_WORDS:
        return False

    if any(re.fullmatch(pattern, normalized) for pattern in _GENERIC_CUSTOMER_REFERENCE_PATTERNS):
        return False

    if (
        any(keyword in normalized for keyword in _GENERIC_CUSTOMER_REFERENCE_KEYWORDS)
        and not any(hint in normalized for hint in _COMPANY_NAME_HINTS)
    ):
        return False

    # 避免把“企业贷款申请表”“匹配贷款方案”之类的任务短语误识别为客户名称。
    if any(keyword in normalized for keyword in _GENERIC_CUSTOMER_REFERENCE_KEYWORDS):
        if any(keyword in normalized for keyword in _GENERIC_INTENT_ONLY_KEYWORDS):
            if not any(
                company_hint in normalized
                for company_hint in ("有限公司", "有限责任公司", "股份有限公司", "集团", "合作社", "事务所", "工作室")
            ):
                return False

    return True


def _finalize_customer_name_candidate(name: str | None) -> str | None:
    """Clean and validate extracted customer name candidates."""
    if not name:
        return None

    normalized = _normalize_customer_name_candidate(name)
    if not _looks_like_customer_name(normalized):
        return None
    return normalized


_CUSTOMER_NAME_PATTERNS: list[tuple[str, int, Any]] = [
    (r"客户「([^」]+)」", 1, None),
    (r"(帮|给|为)([^\s,，、]+)(生成|输出|填写|匹配)", 2, _is_not_pronoun),
    (r"^([^\s,，、]+)[,，、\s]+(输出|生成|填写|匹配)", 1, _is_not_pronoun),
    (r"已找到客户「([^」]+)」的资料", 1, None),
    (r"未找到客户「([^」]+)」", 1, None),
    (r"名称[：:]\s*([^\s,，、\n]+)", 1, None),
    (r"客户名称[：:]\s*([^\s,，、\n]+)", 1, None),
    (r"企业名称[：:]\s*([^\s,，、\n]+)", 1, None),
    (r"\*\*([^*]+)\*\*\s*的资料", 1, None),
    (r"已保存到本地[：:]\s*([^\s,，、\n]+)", 1, None),
    (r'"?customerName"?\s*[：:]\s*"?([^",\s\n}]+)"?', 1, _is_valid_customer_name),
]


# ===== 瀹㈡埛鍚嶇О鎻愬彇鍑芥暟 =====


def extract_customer_name(content: dict) -> str | None:
    """Extract customer name from parsed content.

    Args:
        content: Parsed JSON content from AI extraction

    Returns:
        Customer name if found, None otherwise
    """
    if not content or not isinstance(content, dict):
        return None

    name = _finalize_customer_name_candidate(_extract_customer_name_direct(content))
    if name:
        return name

    return _finalize_customer_name_candidate(_extract_customer_name_nested(content))


def _extract_customer_name_direct(content: dict) -> str | None:
    """Extract customer name from top-level fields.

    Args:
        content: Parsed JSON content

    Returns:
        Customer name if found, None otherwise
    """
    name_fields = [
        "企业名称",
        "公司名称",
        "编制单位",
        "单位名称",
        "企业全称",
        "纳税人名称",
        "纳税人姓名",
        "纳税主体",
        "纳税义务人",
        "名称",
        "姓名",
        "被查询者姓名",
        "客户姓名",
        "客户名称",
        "借款人",
        "申请人",
        "户名",
        "账户持有人",
        "账户所有人",
        "账户所有人姓名",
        "缴存人",
        "缴存人姓名",
        "法定代表人",
        "权属人",
    ]
    for field in name_fields:
        if field in content:
            value = content[field]
            if value and isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _normalize_customer_name_lookup_key(key: str) -> str:
    """Normalize section/field labels for customer-name lookup."""
    cleaned = str(key).strip()
    cleaned = re.sub(
        r"^\s*(?:第?[一二三四五六七八九十]+[、.．)]|[（(][一二三四五六七八九十]+[)）]|第?\d+[、.．)])\s*",
        "",
        cleaned,
    )
    return re.sub(r"[\s【】\[\]（）()：:、，,.\-_/]", "", cleaned)


def _get_customer_name_value(data: dict, key: str) -> Any:
    """Get a value from dict by exact or normalized key match."""
    if key in data:
        return data[key]

    target = _normalize_customer_name_lookup_key(key)
    for candidate_key, candidate_value in data.items():
        if not isinstance(candidate_key, str):
            continue
        if _normalize_customer_name_lookup_key(candidate_key) == target:
            return candidate_value
    return None


def _extract_customer_name_nested(content: dict) -> str | None:
    """Extract customer name from nested paths.

    Args:
        content: Parsed JSON content

    Returns:
        Customer name if found, None otherwise
    """
    nested_paths = [
        ("基本信息", "企业名称"),
        ("基本信息", "公司名称"),
        ("基本信息", "姓名"),
        ("报表基础信息", "编制单位"),
        ("报表基础信息", "企业名称"),
        ("报表基础信息", "公司名称"),
        ("报告基础信息", "姓名"),
        ("被查询者信息", "姓名"),
        ("企业身份信息", "企业名称"),
        ("企业身份信息", "公司名称"),
        ("企业身份信息", "名称"),
        ("企业基本信息", "企业名称"),
        ("企业基本信息", "公司名称"),
        ("企业基本信息", "法定代表人"),
        ("财务数据", "企业名称"),
        ("财务信息", "企业名称"),
        ("纳税信息", "纳税人名称"),
        ("纳税信息", "纳税人姓名"),
        ("纳税信息", "纳税主体"),
        ("纳税信息", "纳税义务人"),
        ("基础账户信息", "开户名称"),
        ("基础账户信息", "账户持有人"),
        ("账户基础信息", "户名"),
        ("账户基础信息", "开户名称"),
        ("账户基础信息", "账户持有人"),
        ("账户基础信息", "账户所有人"),
        ("账户基础信息", "账户所有人姓名"),
        ("收入纳税明细", "纳税人姓名"),
        ("收入纳税明细", "纳税人"),
        ("收入纳税明细", "姓名"),
        ("公积金账户", "账户所有人姓名"),
        ("公积金账户", "账户所有人"),
        ("公积金账户", "缴存人姓名"),
        ("公积金账户", "缴存人"),
        ("公积金账户", "姓名"),
        ("法定代表人信息", "姓名"),
        ("法人代表信息", "姓名"),
        ("抵押物基本信息", "权属人"),
    ]
    for path in nested_paths:
        current = content
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = _get_customer_name_value(current, key)
            if current is None:
                break
        if current and isinstance(current, str) and current.strip():
            return current.strip()
    return None


def extract_customer_name_from_message(message: str) -> str | None:
    """Extract customer name from user message.

    Looks for patterns like 鍚嶇О锛歺xx, 瀹㈡埛锛歺xx, etc.

    Args:
        message: The user's message

    Returns:
        Customer name if found, None otherwise
    """
    if not message:
        return None

    patterns = [
        r"名称[：:]\s*([^\s,，、\n]+)",
        r"客户[：:]\s*([^\s,，、\n]+)",
        r"客户名称[：:]\s*([^\s,，、\n]+)",
        r"企业名称[：:]\s*([^\s,，、\n]+)",
        r"公司名称[：:]\s*([^\s,，、\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, message)
        if match:
            name = _finalize_customer_name_candidate(match.group(1))
            if name:
                return name
    return None


def extract_customer_name_from_text(text: str) -> str | None:
    """Extract customer name directly from OCR/plain text content."""
    if not text or not text.strip():
        return None

    patterns = [
        r"您好[，,\s]+([一-龥·]{2,8})",
        r"姓名[：:\s]+([一-龥·]{2,8})",
        r"户名[：:\s]+([一-龥·]{2,8})",
        r"客户姓名[：:\s]+([一-龥·]{2,8})",
        r"纳税人姓名[：:\s]+([一-龥·]{2,8})",
        r"账户所有人姓名[：:\s]+([一-龥·]{2,8})",
        r"账户持有人[：:\s]+([一-龥·]{2,8})",
        r"缴存人姓名[：:\s]+([一-龥·]{2,8})",
        r"缴存人[：:\s]+([一-龥·]{2,8})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        name = _finalize_customer_name_candidate(match.group(1))
        if name:
            return name

    return None


def _match_customer_name_patterns(content: str) -> str | None:
    """Match customer name patterns against a single message content.

    Args:
        content: Message content to search

    Returns:
        Customer name if a pattern matches, None otherwise
    """
    for pattern, group_index, validator_fn in _CUSTOMER_NAME_PATTERNS:
        match = re.search(pattern, content)
        if match:
            name = _finalize_customer_name_candidate(match.group(group_index))
            if not name:
                continue
            if validator_fn is None:
                return name
            if validator_fn(name):
                return name
    return None


def extract_customer_from_history(
    conversation_history: list[dict[str, str]],
) -> str | None:
    """Extract customer name from conversation history.

    Searches from most recent to oldest for customer name patterns.

    Args:
        conversation_history: Previous messages in the conversation

    Returns:
        Customer name if found, None otherwise
    """
    if not conversation_history:
        return None

    for msg in reversed(conversation_history):
        content = msg.get("content", "")
        if not content:
            continue
        name = _match_customer_name_patterns(content)
        if name:
            return name
    return None


# ===== 鍙傛暟鎻愬彇 =====


def _clean_customer_name(name: str) -> str:
    """Remove common prefixes from customer name.

    Args:
        name: Raw customer name string

    Returns:
        Cleaned customer name
    """
    if not name:
        return name
    original = name
    result = _normalize_customer_name_candidate(name)
    if original != result:
        logger.info(f"Cleaned customer name: '{original}' -> '{result}'")
    return result


def _infer_loan_type_from_name(customer_name: str) -> str:
    """Infer loan type from customer name.

    Args:
        customer_name: Customer name

    Returns:
        "enterprise" or "personal"
    """
    if any(kw in customer_name for kw in ["公司", "企业", "集团", "有限"]):
        return "enterprise"
    if len(customer_name) <= 4:
        return "personal"
    return "enterprise"


# Parameter extraction prompt
_EXTRACT_PARAMS_PROMPT = """请从用户消息中提取以下参数，并以 JSON 格式返回：
- customerName: 客户名称（企业名称或个人姓名），如未明确提及则返回 null
- loanType: 贷款类型（enterprise 或 personal），如无法判断则返回 null

用户消息：{message}

只返回 JSON，不要包含其他说明，例如：
{{"customerName": "xxx", "loanType": "enterprise"}}

如果无法提取某个字段，请将该字段值设为 null。
"""

_EXTRACT_PARAMS_CONTEXT_RULES = """补充规则：
- customerName 必须是真实的客户名称（企业名称或个人姓名）
- 不要把“申请表”“一份申请表”“这份资料”“这份文件”“方案”“模板”等泛指内容当成客户名称
- 如果当前消息没有明确客户名称，但最近对话中刚刚提到过明确客户名称，可以使用最近那个客户名称
- 如果上下文仍无法确定客户名称，请返回 null
"""


def _build_extract_params_prompt(
    message: str, conversation_history: list[dict[str, str]] | None = None,
) -> str:
    """Build the parameter extraction prompt with optional chat history."""
    prompt = _EXTRACT_PARAMS_PROMPT.format(message=message)
    if not conversation_history:
        return f"{prompt}\n\n{_EXTRACT_PARAMS_CONTEXT_RULES}"

    recent_history = conversation_history[-6:]
    history_lines = []
    for msg in recent_history:
        role = "用户" if msg.get("role") == "user" else "助手"
        content = (msg.get("content") or "").strip()
        if content:
            history_lines.append(f"{role}: {content}")

    if not history_lines:
        return f"{prompt}\n\n{_EXTRACT_PARAMS_CONTEXT_RULES}"

    history_block = "\n".join(history_lines)
    return (
        f"{prompt}\n\n最近对话上下文：\n{history_block}\n\n"
        f"{_EXTRACT_PARAMS_CONTEXT_RULES}"
    )


def extract_params_from_message(
    message: str, conversation_history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Extract customerName and loanType from user message.

    Uses rule-based extraction first, falls back to AI.

    Args:
        message: The user's message

    Returns:
        Dictionary with customerName and loanType (may be None)
    """
    # 先检查用户是否显式指定了贷款类型
    loan_type = _detect_explicit_loan_type(message)

    # 再提取客户名称
    customer_name = _extract_customer_name_from_rules(message)

    # 如果规则提取到了客户名称，直接返回
    if customer_name:
        if loan_type is None:
            loan_type = _infer_loan_type_from_name(customer_name)
        logger.info(f"Rule-based extraction: customerName={customer_name}, loanType={loan_type}")
        return {"customerName": customer_name, "loanType": loan_type}

    if conversation_history:
        history_customer_name = _finalize_customer_name_candidate(
            extract_customer_from_history(conversation_history)
        )
        if history_customer_name:
            if loan_type is None:
                loan_type = _infer_loan_type_from_name(history_customer_name)
            logger.info(
                "Resolved customer name from conversation history: "
                f"customerName={history_customer_name}, loanType={loan_type}"
            )
            return {"customerName": history_customer_name, "loanType": loan_type}

    # 规则提取失败时，再用 AI 兜底
    return _extract_params_with_ai(message, conversation_history)


def _detect_explicit_loan_type(message: str) -> str | None:
    """Detect if user explicitly specified loan type.

    Args:
        message: User message

    Returns:
        "enterprise", "personal", or None
    """
    if re.search(r"企业.*(贷款|申请表|方案)", message) or re.search(
        r"(贷款|申请表|方案).*企业", message
    ):
        logger.info("User explicitly specified enterprise loan")
        return "enterprise"
    if re.search(r"个人.*(贷款|申请表|方案)", message) or re.search(
        r"(贷款|申请表|方案).*个人", message
    ):
        logger.info("User explicitly specified personal loan")
        return "personal"
    return None


def _extract_customer_name_from_rules(message: str) -> str | None:
    """Extract customer name using rule-based patterns.

    Args:
        message: User message

    Returns:
        Customer name or None
    """
    # 优先提取“名称：XXX”这一类显式写法
    explicit_name = extract_customer_name_from_message(message)
    if explicit_name:
        logger.info(f"Extracted customer name from explicit pattern: {explicit_name}")
        return explicit_name

    # 模式0：“根据 XXX，输出/生成/填写申请表”
    match0 = re.search(r"根据([^\s,，、]+)[,，、\s]+(输出|生成|填写|匹配)", message)
    if match0:
        extracted = _finalize_customer_name_candidate(match0.group(1))
        if extracted:
            return extracted

    # 模式1：“XXX，输出/生成/填写/匹配”
    match1 = re.search(r"^([^\s,，、]+)[,，、\s]+(输出|生成|填写|匹配)", message)
    if match1:
        extracted = _finalize_customer_name_candidate(match1.group(1))
        if extracted:
            return extracted

    # 模式2：“帮/给/为 XXX 生成/输出/填写/匹配”
    match2 = re.search(r"(帮|给|为)([^\s,，、]+)(生成|输出|填写|匹配)", message)
    if match2:
        extracted = _finalize_customer_name_candidate(match2.group(2))
        if extracted:
            return extracted

    # 模式3：“输出/生成/填写/匹配 XXX 的…”
    # 这里限制必须出现“的”，减少把“生成一份企业贷款申请表”这类任务短语误识别成客户名。
    match3 = re.search(r"(输出|生成|填写|匹配)\s*([^\s,，、]+?)\s*的", message)
    if match3:
        extracted = _finalize_customer_name_candidate(match3.group(2))
        if extracted:
            return extracted

    return None


def _extract_params_with_ai(
    message: str, conversation_history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Extract params using AI as fallback.

    Args:
        message: User message

    Returns:
        Dictionary with customerName and loanType
    """
    try:
        prompt = _build_extract_params_prompt(message, conversation_history)
        result = ai_service.extract(prompt, "提取客户参数")
        logger.info(f"AI extraction result: {result}")

        if result:
            parsed = parse_json(result)
            logger.info(f"Parsed params: {parsed}")
            if parsed:
                customer_name = _finalize_customer_name_candidate(parsed.get("customerName"))
                loan_type = parsed.get("loanType")
                if loan_type not in {"enterprise", "personal"}:
                    loan_type = None
                return {
                    "customerName": customer_name,
                    "loanType": loan_type,
                }
        return {"customerName": None, "loanType": None}
    except Exception as e:
        logger.error(f"Failed to extract params from message: {e}")
        return {"customerName": None, "loanType": None}


# ===== 鍗虫椂鍖归厤妫€娴?=====

# 甯︽暟瀛楃殑璐㈠姟鏁版嵁鍏抽敭璇?
_NUMERIC_KEYWORDS = [
    r"年开票\d+",
    r"开票\d+万?",
    r"负债\d+",
    r"纳税\d+",
    r"流水\d+",
    r"(想要|需要|申请|贷款|借款)\d+万?",
    r"再贷\d+",
    r"月收入\d+",
    r"年收入\d+",
    r"资产\d+",
    r"房产.*\d+万?",
    r"\d+万?房产",
    r"抵押.*\d+万?",
    r"\d+万?抵押",
]

# 不带数字的贷款关键词
_LOAN_KEYWORDS = [
    r"房产", r"抵押", r"转单", r"到期", r"征信", r"贷款", r"融资",
    r"额度", r"利率", r"年限", r"还款", r"月供", r"首付", r"评估",
    r"银行", r"信贷", r"经营贷", r"消费贷", r"抵押贷", r"信用贷",
]

# 描述客户情况的句式
_SITUATION_PATTERNS = [
    r"客户情况",
    r"客户.*情况",
    r"(已经|有|可以|能否|想要)?抵押",
    r"征信(很好|良好|一般|较差|有逾期|无逾期|正常|没问题)",
    r"(没有|无)逾期",
    r"(想要|需要|申请|办理)(贷款|融资|抵押)",
]

# 鎺掗櫎鐨勬寚浠ｈ瘝
_MATCHING_PRONOUNS = [
    "该公司", "该企业", "该客户", "这个公司", "这家公司",
    "这个企业", "这家企业", "他", "她", "它", "我", "客户", "客户情况",
]


def _has_numeric_financial_keywords(message: str) -> bool:
    """Check if message contains numeric financial keywords.

    Args:
        message: User message

    Returns:
        True if any numeric keyword found
    """
    return any(re.search(p, message) for p in _NUMERIC_KEYWORDS)


def _has_loan_keywords(message: str) -> bool:
    """Check if message contains enough loan keywords (>=2).

    Args:
        message: User message

    Returns:
        True if 2+ loan keywords found
    """
    count = sum(1 for p in _LOAN_KEYWORDS if re.search(p, message))
    return count >= 2


def _has_situation_pattern(message: str) -> bool:
    """Check if message describes a customer situation.

    Args:
        message: User message

    Returns:
        True if situation pattern found
    """
    return any(re.search(p, message) for p in _SITUATION_PATTERNS)


def _has_explicit_customer_name(message: str) -> bool:
    """Check if message specifies a customer name for matching.

    If a specific customer name is found, the request should use
    the standard matching flow instead of instant matching.

    Args:
        message: User message

    Returns:
        True if explicit customer name found
    """
    customer_name_patterns = [
        r"(帮|给|为)([^\s,，、]+)(匹配|生成)",
        r"^([^\s,，、]+)[,，、\s]+(匹配|生成)",
        r"(匹配|生成)([^\s,，、的]+)的?",
    ]
    for pattern in customer_name_patterns:
        match = re.search(pattern, message)
        if not match:
            continue
        potential_name = (
            match.group(2).strip()
            if len(match.groups()) >= 2
            else match.group(1).strip()
        )
        if not potential_name or potential_name in _MATCHING_PRONOUNS:
            continue
        if any(kw in potential_name for kw in ["公司", "企业", "集团", "有限"]):
            logger.info(f"Found customer name '{potential_name}', using standard matching flow")
            return True
        if re.match(r"^[\u4e00-\u9fa5]{2,4}$", potential_name):
            logger.info(f"Found customer name '{potential_name}', using standard matching flow")
            return True
    return False


def is_instant_matching_request(message: str) -> bool:
    """判断用户消息是否属于即时匹配请求。

    即时匹配的典型特征：包含贷款或财务关键词，但没有明确指定客户名称。

    Args:
        message: 用户消息

    Returns:
        True 表示属于即时匹配请求
    """
    if not message:
        return False

    has_trigger = (
        _has_numeric_financial_keywords(message)
        or _has_loan_keywords(message)
        or _has_situation_pattern(message)
    )
    if not has_trigger:
        return False

    if _has_explicit_customer_name(message):
        return False

    logger.info("Detected instant matching request (has financial data, no specific customer name)")
    return True


# ===== 贷款类型判断 =====

_ENTERPRISE_KEYWORDS = [
    "企业", "公司", "年开票", "纳税", "经营", "法人", "股东",
    "营业执照", "对公", "企业流水", "企业征信",
]

_PERSONAL_KEYWORDS = [
    "个人", "工资", "社保", "公积金", "信用卡", "房贷", "车贷",
    "个人流水", "个人征信", "月收入", "年收入",
]


def determine_loan_type_from_description(
    customer_info: dict, message: str,
) -> str:
    """根据提取后的客户信息和原始消息判断贷款类型。

    Args:
        customer_info: 提取后的客户信息字典
        message: 原始用户消息

    Returns:
        "enterprise" 或 "personal"
    """
    enterprise_count = sum(1 for kw in _ENTERPRISE_KEYWORDS if kw in message)
    personal_count = sum(1 for kw in _PERSONAL_KEYWORDS if kw in message)

    # 使用结构化结果中的企业/财务字段增强判断。
    basic_info = customer_info.get("基本信息", {}) or {}
    if basic_info.get("企业名称") and basic_info.get("企业名称") != "暂无":
        enterprise_count += 2

    financial_info = customer_info.get("财务信息", {}) or {}
    if financial_info.get("年开票") and financial_info.get("年开票") != "暂无":
        enterprise_count += 1
    if financial_info.get("年纳税") and financial_info.get("年纳税") != "暂无":
        enterprise_count += 1

    logger.info(f"Loan type detection: enterprise={enterprise_count}, personal={personal_count}")

    if personal_count > enterprise_count:
        return "personal"
    return "enterprise"


# ===== 瀹㈡埛淇℃伅鎻愬彇 =====


def _build_customer_info_extraction_prompt(message: str) -> str:
    """Build prompt for extracting customer info from description.

    Args:
        message: User description message

    Returns:
        Complete prompt string
    """
    return f"""请从用户描述中提取贷款相关的客户信息，并以 JSON 格式返回。

## 用户描述
{message}

## 提取要求
请提取以下信息；如果用户没有明确提到，就填“暂无”：

### 企业/个人基本信息
- 企业名称或姓名
- 行业类型
- 成立年限或工作年限
- 经营状态或就业状态

### 财务信息
- 年开票
- 年收入
- 月均收入
- 年纳税
- 负债总额
- 资产负债率
- 流水情况

### 征信信息
- 征信情况（如良好、一般、较差）
- 逾期记录
- 查询次数
- 对外担保

### 抵押物信息
- 抵押物类型（房产、车辆、设备等）
- 抵押物状态（已抵押、未抵押、可抵押）
- 抵押物估值

### 贷款需求
- 贷款金额
- 贷款期限
- 贷款用途
- 还款方式偏好

## 输出格式
请严格按照以下 JSON 输出，只返回 JSON，不要添加解释：

```json
{{
  "基本信息": {{
    "企业名称": "xxx 或 暂无",
    "行业类型": "xxx 或 暂无",
    "成立年限": "xxx 或 暂无",
    "经营状态": "xxx 或 暂无"
  }},
  "财务信息": {{
    "年开票": "xxx 或 暂无",
    "月均收入": "xxx 或 暂无",
    "年纳税": "xxx 或 暂无",
    "负债总额": "xxx 或 暂无",
    "资产负债率": "xxx 或 暂无",
    "流水情况": "xxx 或 暂无"
  }},
  "征信信息": {{
    "征信情况": "xxx 或 暂无",
    "逾期记录": "xxx 或 暂无",
    "查询次数": "xxx 或 暂无",
    "对外担保": "xxx 或 暂无"
  }},
  "抵押物信息": {{
    "抵押物类型": "xxx 或 暂无",
    "抵押物状态": "xxx 或 暂无",
    "抵押物估值": "xxx 或 暂无"
  }},
  "贷款需求": {{
    "贷款金额": "xxx 或 暂无",
    "贷款期限": "xxx 或 暂无",
    "贷款用途": "xxx 或 暂无",
    "还款方式偏好": "xxx 或 暂无"
  }}
}}
```

## 注意
- 只提取用户明确提到的信息
- 保留用户原始表述，不要自行换算金额或补充事实
- 对模糊描述可做标准化表达，但不要编造未提及信息
"""


async def extract_customer_info_from_description(message: str) -> dict:
    """从用户描述中提取客户信息，用于即时匹配。

    Args:
        message: 用户的描述消息

    Returns:
        提取后的客户信息字典，失败时返回空字典

    Note:
        - 使用 parse_json() 处理可能被截断的 JSON (#8)
    """
    try:
        prompt = _build_customer_info_extraction_prompt(message)
        ai_result = ai_service.extract(prompt, "提取客户信息")

        if not ai_result:
            logger.warning("AI returned empty result for customer info extraction")
            return {}

        customer_info = parse_json(ai_result)
        if customer_info is None:
            logger.warning("Failed to parse customer info JSON")
            return {}

        logger.info(f"Successfully extracted customer info with {len(customer_info)} sections")
        return customer_info

    except AIServiceError as e:
        logger.error(f"AI error during customer info extraction: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error during customer info extraction: {e}")
        return {}


# ===== 匹配结果转换 =====


def _build_matching_conversion_prompt(match_result: str) -> str:
    """Build prompt for converting matching result to JSON.

    Args:
        match_result: Markdown formatted matching result

    Returns:
        Complete prompt string
    """
    return _build_matching_conversion_prompt_clean(match_result)


def _build_matching_conversion_prompt_clean(match_result: str) -> str:
    """Build a clean Chinese prompt for matching-result JSON conversion.

    The legacy prompt above contains mojibake text. Keep it in place for
    compatibility, but use this clean prompt for runtime conversion.
    """
    return f"""请将以下方案匹配结果转换为 JSON 格式。

## 输入（Markdown 格式的匹配结果）
{match_result}

## 输出要求
请严格按照以下 JSON 结构输出，只返回 JSON，不要包含其他说明：

```json
{{
  "核心发现": {{
    "资料类型": "从匹配结果中提取",
    "数据完整性": "完整/部分缺失/信息不足",
    "匹配结论": "一句话总结匹配结果"
  }},
  "客户资料摘要": {{
    "企业名称": "xxx",
    "成立年限": "xxx",
    "经营状态": "xxx",
    "未结清贷款": "xxx",
    "对外担保": "xxx",
    "信用评价": "xxx"
  }},
  "待补充资料": {{
    "必须补充": ["项目1", "项目2"],
    "建议补充": ["项目1", "项目2"]
  }},
  "推荐方案": [
    {{
      "方案名称": "xxx",
      "银行名称": "xxx",
      "产品名称": "xxx",
      "可贷额度": "xxx",
      "参考利率": "xxx",
      "贷款期限": "xxx",
      "还款方式": "xxx",
      "匹配理由": "xxx",
      "准备材料": {{
        "基础材料": ["..."],
        "经营证明": ["..."],
        "其他材料": ["..."]
      }},
      "审批流程": [
        {{
          "步骤": "提交申请",
          "内容": "提交申请资料",
          "预计时间": "1个工作日"
        }}
      ]
    }}
  ],
  "不推荐产品": [
    {{
      "产品名称": "xxx",
      "银行名称": "xxx",
      "不符合原因": "xxx",
      "补充材料后可匹配": "xxx"
    }}
  ],
  "下一步建议": "xxx",
  "准备材料": {{
    "基础材料": ["..."],
    "经营证明": ["..."],
    "其他材料": ["..."]
  }},
  "审批流程": [
    {{
      "步骤": "提交申请",
      "内容": "提交申请资料",
      "预计时间": "1个工作日"
    }}
  ]
}}
```

## 注意事项
1. 原文没有的信息填“暂无”或空数组。
2. 推荐方案有几条就输出几条，不要编造。
3. 不推荐产品只保留原文中明确提到的不符合项。
4. 只返回合法 JSON。
"""


_MATCHING_SECTION_TITLES = [
    "客户资料摘要",
    "推荐方案",
    "不推荐产品",
    "替代建议",
    "需补充信息",
    "待补充资料",
    "准备材料",
    "审批流程",
    "下一步建议",
]

_SCHEME_TITLE_PATTERN = re.compile(
    r"(?:^|\n)(?:#{2,4}\s*)?(?:\*\*)?方案\s*(\d+)\s*[：:]\s*(?:【([^】]+)】)?([^\n*]+?)(?:\*\*)?\s*(?:\n|$)",
    re.M,
)
_PRODUCT_BLOCK_TITLE_PATTERN = re.compile(
    r"^(?P<bank>.+?银行)\s*[-—–]{1,2}\s*(?P<product>.*?)(?=\s+(?:可贷额度|贷款额度|最高额度|最高可贷额度|额度|参考利率|年化利率|利率区间|贷款利率|利率|贷款期限|借款期限|期限|授信期限|还款方式|还款模式|还款规则)\s*[：:]|$)",
    re.M,
)
_PRODUCT_CANONICAL_FIELDS = ("可贷额度", "参考利率", "贷款期限", "还款方式")
_PRODUCT_EXTRA_TEXT_FIELDS = (
    "成立时间",
    "纳税要求",
    "税务等级",
    "负债",
    "销贷比",
    "征信",
    "征信查询次数",
    "企业授信",
    "准入条件",
    "特别说明",
)
_PRODUCT_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "可贷额度": ("可贷额度", "贷款额度", "最高额度", "最高可贷额度", "额度"),
    "参考利率": ("参考利率", "年化利率", "利率区间", "贷款利率", "利率"),
    "贷款期限": ("贷款期限", "借款期限", "期限", "授信期限"),
    "还款方式": ("还款方式", "还款模式", "还款规则"),
}
_PRODUCT_KV_LINE_PATTERN = re.compile(
    r"^(?:[-*•]\s*)?(?:\*\*)?([^：:\n]{1,30}?)(?:\*\*)?\s*(?:[：:]|\t+| {2,})\s*(.+)$"
)


def _strip_markdown(text: str) -> str:
    """Normalize a markdown fragment into plain text."""
    cleaned = text.strip()
    cleaned = re.sub(r"^\s*[-*•]+\s*", "", cleaned)
    cleaned = re.sub(r"^\s*\d+[.)、](?!\d)\s*", "", cleaned)
    cleaned = cleaned.replace("**", "").replace("__", "").replace("`", "")
    return cleaned.strip(" ：:-")


def _normalize_lookup_key(text: str) -> str:
    """Normalize bank/product names for matching."""
    cleaned = _strip_markdown(text)
    cleaned = re.sub(r"[【】\[\]（）()：:、，,\-/\s]", "", cleaned)
    return cleaned.lower()


def _normalize_field_label(text: str) -> str:
    """Normalize a field label for alias matching."""
    return re.sub(r"[\s（）()【】\[\]：:]", "", _strip_markdown(text))


def _to_canonical_product_field(field_name: str) -> str | None:
    """Map knowledge-base field names to canonical matching output keys."""
    normalized = _normalize_field_label(field_name)
    if not normalized:
        return None

    for canonical, aliases in _PRODUCT_FIELD_ALIASES.items():
        for alias in aliases:
            alias_normalized = _normalize_field_label(alias)
            if normalized == alias_normalized or alias_normalized in normalized:
                return canonical
    return None


def _extract_product_basic_fields(block: str) -> dict[str, str]:
    """Extract canonical product fields from one knowledge-base product block."""
    basic_fields: dict[str, str] = {}
    inline_field_pattern = re.compile(
        r"(可贷额度|贷款额度|最高额度|最高可贷额度|额度|参考利率|年化利率|利率区间|贷款利率|利率|贷款期限|借款期限|期限|授信期限|还款方式|还款模式|还款规则)\s*[：:]\s*(.+?)(?=\s+(?:可贷额度|贷款额度|最高额度|最高可贷额度|额度|参考利率|年化利率|利率区间|贷款利率|利率|贷款期限|借款期限|期限|授信期限|还款方式|还款模式|还款规则)\s*[：:]|$)"
    )
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        kv_match = re.match(r"^(?:[-*•]\s*)?(?:\*\*)?([^：:\n]+?)(?:\*\*)?\s*[：:]\s*(.+)$", line)
        if not kv_match:
            kv_match = re.match(
                r"^(?:[-*•]\s*)?(可贷额度|贷款额度|最高额度|最高可贷额度|额度|参考利率|年化利率|利率区间|贷款利率|利率|贷款期限|借款期限|期限|授信期限|还款方式|还款模式|还款规则)\s+(.+)$",
                line,
            )
        if not kv_match:
            continue

        key = _strip_markdown(kv_match.group(1))
        value = _strip_markdown(kv_match.group(2))
        if not key or not value:
            continue

        canonical_key = _to_canonical_product_field(key)
        if canonical_key:
            basic_fields[canonical_key] = value

        for inline_match in inline_field_pattern.finditer(line):
            key = _strip_markdown(inline_match.group(1))
            value = _strip_markdown(inline_match.group(2))
            canonical_key = _to_canonical_product_field(key)
            if canonical_key and value:
                basic_fields[canonical_key] = value

    return basic_fields


def _extract_product_extra_text_fields(block: str) -> dict[str, str]:
    """Extract non-canonical textual constraints (e.g. 征信、征信查询次数)."""
    extracted: dict[str, str] = {}
    current_key: str | None = None
    section_stops = {"材料准备清单", "申请材料", "准备材料", "审批流程", "申请流程", "审批说明", "经验总结"}

    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line in section_stops:
            current_key = None
            continue

        kv_match = _PRODUCT_KV_LINE_PATTERN.match(line)
        if kv_match:
            key = _strip_markdown(kv_match.group(1))
            value = _strip_markdown(kv_match.group(2))
            current_key = None

            if not key or not value:
                continue

            canonical_key = _to_canonical_product_field(key)
            if canonical_key:
                continue

            if key in _PRODUCT_EXTRA_TEXT_FIELDS:
                extracted[key] = value
                current_key = key
            continue

        # Support wiki layouts where the field label is on its own line and the
        # actual requirements are listed in the following numbered/bulleted rows.
        standalone_key = _strip_markdown(line)
        if standalone_key in _PRODUCT_EXTRA_TEXT_FIELDS:
            extracted.setdefault(standalone_key, "")
            current_key = standalone_key
            continue

        if current_key and re.match(r"^(?:\d+[.)、]|[-*•])\s*.+$", line):
            continuation = _strip_markdown(line)
            if continuation:
                existing = extracted.get(current_key, "").strip()
                extracted[current_key] = f"{existing}；{continuation}".strip("；")
            continue

        if current_key and not extracted.get(current_key):
            continuation = _strip_markdown(line)
            if continuation:
                extracted[current_key] = continuation
            continue

        if current_key and extracted.get(current_key):
            # Stop appending once we already collected content and encounter a
            # non-list line. This prevents bleeding into the next field.
            current_key = None

    return extracted


def _extract_markdown_section(markdown: str, titles: list[str]) -> str:
    """Extract a top-level markdown section by heading title."""
    title_pattern = "|".join(re.escape(title) for title in titles)
    stop_pattern = "|".join(re.escape(title) for title in _MATCHING_SECTION_TITLES)
    pattern = (
        rf"(?:^|\n)(?:#{{1,4}}\s*)?(?:[一二三四五六七八九十]+、)?(?:{title_pattern})[^\n]*\n"
        rf"([\s\S]*?)(?=\n(?:#{{1,4}}\s*)?(?:[一二三四五六七八九十]+、)?(?:{stop_pattern})[^\n]*|\Z)"
    )
    match = re.search(pattern, markdown, re.I)
    return match.group(1).strip() if match else ""


def _parse_markdown_table(section: str) -> list[list[str]]:
    """Parse a markdown table into rows."""
    rows: list[list[str]] = []
    for line in section.splitlines():
        stripped = line.strip()
        if "|" not in stripped:
            continue
        cells = [_strip_markdown(cell) for cell in stripped.split("|")[1:-1]]
        if not cells or all(not cell for cell in cells):
            continue
        if all(set(cell) <= {"-", ":"} for cell in cells if cell):
            continue
        rows.append(cells)
    return rows


def _parse_matching_summary(section: str) -> dict[str, str]:
    """Parse the 客户资料摘要 table."""
    rows = _parse_markdown_table(section)
    summary: dict[str, str] = {}
    for row in rows:
        if len(row) < 2:
            continue
        key = row[0]
        value = row[1]
        if not key or key in {"项目", "字段"}:
            continue
        if value:
            summary[key] = value
    return summary


def _parse_materials_block(section: str) -> dict[str, list[str]]:
    """Parse a 准备材料 block into categorized lists."""
    materials: dict[str, list[str]] = {}
    current_category = "其他材料"

    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        category_match = re.match(r"^(?:[-*]\s*)?(?:\*\*)?([^:*：\n]+?)(?:\*\*)?\s*[：:]?\s*$", stripped)
        if category_match and any(keyword in category_match.group(1) for keyword in ("材料", "证明", "资料")):
            current_category = _strip_markdown(category_match.group(1))
            materials.setdefault(current_category, [])
            continue
        item_match = re.match(r"^\s*[-*•]+\s+(.+)$", stripped)
        if item_match:
            materials.setdefault(current_category, []).append(_strip_markdown(item_match.group(1)))

    return {key: value for key, value in materials.items() if value}


def _parse_plain_materials_block(section: str) -> dict[str, list[str]]:
    """Parse raw knowledge-base materials text."""
    materials: dict[str, list[str]] = {}
    current_category = "基础材料"

    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped in {"审批说明", "经验总结", "审批流程", "申请流程"}:
            break
        if any(keyword in stripped for keyword in ("材料", "证明", "资料")) and len(stripped) <= 12:
            current_category = stripped
            if current_category == "必备材料":
                current_category = "基础材料"
            materials.setdefault(current_category, [])
            continue
        if stripped.startswith("总周期"):
            continue
        materials.setdefault(current_category, []).append(stripped)

    return {key: value for key, value in materials.items() if value}


def _parse_process_block(section: str) -> list[dict[str, str]]:
    """Parse an 审批流程 block from a markdown table or list."""
    steps: list[dict[str, str]] = []
    rows = _parse_markdown_table(section)
    if rows:
        for row in rows:
            if len(row) < 2:
                continue
            step = re.sub(r"^\d+[.)、]\s*", "", row[0]).strip()
            content = row[1].strip() if len(row) > 1 else ""
            eta = row[2].strip() if len(row) > 2 else ""
            if step in {"步骤"} or not step:
                continue
            steps.append({"步骤": step, "内容": content, "预计时间": eta})
        if steps:
            return steps

    for line in section.splitlines():
        stripped = _strip_markdown(line)
        if not stripped:
            continue
        match = re.match(r"^(\d+)[.)、]?\s*([^（(：:]+?)(?:[：:]\s*([^（(]+))?(?:[（(]([^）)]+)[）)])?$", stripped)
        if not match:
            continue
        step = match.group(2).strip()
        content = (match.group(3) or "").strip()
        eta = (match.group(4) or "").strip()
        steps.append({"步骤": step, "内容": content, "预计时间": eta})

    return steps


def _parse_plain_process_block(section: str) -> list[dict[str, str]]:
    """Parse raw knowledge-base process text."""
    steps: list[dict[str, str]] = []
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped in {"材料准备清单", "申请材料", "准备材料", "审批说明", "经验总结"}:
            break
        if stripped.startswith("总周期"):
            continue
        numbered_match = re.match(r"^\d+[.)、]\s*([^：:]+)(?:[：:]\s*(.+))?$", stripped)
        if numbered_match:
            steps.append(
                {
                    "步骤": numbered_match.group(1).strip(),
                    "内容": (numbered_match.group(2) or "").strip(),
                    "预计时间": "",
                }
            )
            continue
        steps.append({"步骤": stripped, "内容": "", "预计时间": ""})
    return steps


def _extract_plain_process_note(section: str) -> str:
    """Extract process note text such as 总周期说明 from knowledge-base process section."""
    notes: list[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped in {"材料准备清单", "申请材料", "准备材料", "审批说明", "经验总结"}:
            break
        if stripped.startswith("总周期"):
            notes.append(stripped)
    return "；".join(notes)


def _parse_simple_list(section: str) -> list[str]:
    """Parse bullet or numbered list items."""
    items: list[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^\s*[-*•]+\s+.+$", stripped) or re.match(r"^\s*\d+[.)、]\s+.+$", stripped):
            items.append(_strip_markdown(stripped))
    return items


def _parse_supplement_section(section: str) -> dict[str, list[str]]:
    """Parse 待补充资料 / 需补充信息."""
    supplements = {"必须补充": [], "建议补充": []}
    current_key = "必须补充"

    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "必须补充" in stripped:
            current_key = "必须补充"
            continue
        if "建议补充" in stripped:
            current_key = "建议补充"
            continue
        if re.match(r"^\s*[-*•]+\s+.+$", stripped) or re.match(r"^\s*\d+[.)、]\s+.+$", stripped):
            supplements[current_key].append(_strip_markdown(stripped))

    if not supplements["必须补充"] and not supplements["建议补充"]:
        supplements["必须补充"] = _parse_simple_list(section)

    return {key: value for key, value in supplements.items() if value}


def _extract_next_step(section: str) -> str | None:
    """Extract 下一步建议 text."""
    for line in section.splitlines():
        stripped = _strip_markdown(line)
        if stripped:
            return stripped
    return None


def _parse_scheme_details(block: str) -> tuple[dict[str, Any], dict[str, list[str]], list[dict[str, str]]]:
    """Parse one recommendation scheme block."""
    scheme: dict[str, Any] = {}
    materials: dict[str, list[str]] = {}
    process_steps: list[dict[str, str]] = []
    current_subsection: str | None = None
    current_material_category = "其他材料"

    for line in block.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        normalized = _strip_markdown(stripped)
        if "准备材料" in normalized:
            current_subsection = "materials"
            continue
        if "审批流程" in normalized:
            current_subsection = "process"
            continue

        if current_subsection == "materials":
            if re.search(r"(不推荐产品|替代建议|需补充信息|待补充资料|下一步建议)", normalized):
                current_subsection = None
            else:
                category_match = re.match(r"^(?:\*\*)?([^:*：\n]+?)(?:\*\*)?\s*[：:]?\s*$", stripped)
                if category_match and any(keyword in category_match.group(1) for keyword in ("材料", "证明", "资料")):
                    current_material_category = _strip_markdown(category_match.group(1))
                    materials.setdefault(current_material_category, [])
                    continue
                bullet_match = re.match(r"^\s*[-*•]+\s+(.+)$", stripped)
                if bullet_match:
                    materials.setdefault(current_material_category, []).append(_strip_markdown(bullet_match.group(1)))
                    continue

        if current_subsection == "process":
            if re.search(r"(不推荐产品|替代建议|需补充信息|待补充资料|下一步建议)", normalized):
                current_subsection = None
            else:
                parsed_steps = _parse_process_block(stripped)
                if parsed_steps:
                    process_steps.extend(parsed_steps)
                    continue

        kv_match = re.match(r"^(?:[-*•]\s*)?(?:\*\*)?([^：:\n]+?)(?:\*\*)?\s*[：:]\s*(.+)$", stripped)
        if not kv_match:
            continue

        key = _strip_markdown(kv_match.group(1))
        value = _strip_markdown(kv_match.group(2))
        if not key or not value:
            continue

        if key == "准备材料":
            current_subsection = "materials"
        elif key == "审批流程":
            current_subsection = "process"
        elif key in {"银行名称", "产品名称", "可贷额度", "参考利率", "贷款期限", "还款方式", "匹配理由", "来源"}:
            scheme[key] = value
        else:
            scheme[key] = value

    return scheme, {k: v for k, v in materials.items() if v}, process_steps


def _parse_matching_schemes(markdown: str) -> list[dict[str, Any]]:
    """Parse all 推荐方案 blocks."""
    matches = list(_SCHEME_TITLE_PATTERN.finditer(markdown))
    if not matches:
        return []

    schemes: list[dict[str, Any]] = []
    for idx, match in enumerate(matches):
        next_scheme_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(markdown)
        next_global_section = re.search(
            r"\n(?:#{1,4}\s*)?(?:[一二三四五六七八九十]+、)?(?:不推荐产品|替代建议|需补充信息|待补充资料|下一步建议)[^\n]*",
            markdown[match.end():],
        )
        section_end = next_scheme_start
        if next_global_section:
            section_end = min(section_end, match.end() + next_global_section.start())

        block = markdown[match.end():section_end]
        bank_name = _strip_markdown(match.group(2) or "")
        product_name = _strip_markdown(match.group(3) or "")
        scheme: dict[str, Any] = {
            "方案名称": f"方案{match.group(1)}：{f'【{bank_name}】' if bank_name else ''}{product_name}".strip(),
        }
        if bank_name:
            scheme["银行名称"] = bank_name
        if product_name:
            scheme["产品名称"] = product_name

        detail_fields, materials, process_steps = _parse_scheme_details(block)
        scheme.update(detail_fields)
        if materials:
            scheme["准备材料"] = materials
        if process_steps:
            scheme["审批流程"] = process_steps
        schemes.append(scheme)

    return schemes


def _extract_plain_section(block: str, titles: list[str], stop_titles: list[str]) -> str:
    """Extract a raw-text section from product library content."""
    lines = block.splitlines()
    capturing = False
    collected: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if capturing:
                collected.append("")
            continue
        if stripped in titles:
            capturing = True
            continue
        if capturing and stripped in stop_titles:
            break
        if capturing:
            collected.append(stripped)

    return "\n".join(collected).strip()


def _extract_product_details_from_products(products: str) -> dict[tuple[str, str], dict[str, Any]]:
    """Build a bank/product -> details index from wiki raw content."""
    if not products:
        return {}

    matches = list(_PRODUCT_BLOCK_TITLE_PATTERN.finditer(products))
    details_map: dict[tuple[str, str], dict[str, Any]] = {}

    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(products)
        block = products[start:end]
        bank_name = _strip_markdown(match.group("bank"))
        product_name = _strip_markdown(match.group("product"))
        materials_text = _extract_plain_section(
            block,
            ["材料准备清单", "申请材料", "准备材料"],
            ["审批流程", "申请流程", "审批说明", "经验总结"],
        )
        process_text = _extract_plain_section(
            block,
            ["审批流程", "申请流程"],
            ["材料准备清单", "申请材料", "准备材料", "审批说明", "经验总结"],
        )
        basic_fields = _extract_product_basic_fields(block)
        extra_text_fields = _extract_product_extra_text_fields(block)
        materials = _parse_plain_materials_block(materials_text)
        process_steps = _parse_plain_process_block(process_text)
        process_note = _extract_plain_process_note(process_text)
        extra_details: dict[str, Any] = {}
        if process_note:
            extra_details["审批说明"] = process_note
        details_map[(_normalize_lookup_key(bank_name), _normalize_lookup_key(product_name))] = {
            **basic_fields,
            **extra_text_fields,
            **extra_details,
            "准备材料": materials,
            "审批流程": process_steps,
        }

    return details_map


def _find_product_details(
    bank_name: str, product_name: str, details_map: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any] | None:
    """Find product-specific details with exact and fuzzy matching."""
    bank_key = _normalize_lookup_key(bank_name)
    product_key = _normalize_lookup_key(product_name)

    exact = details_map.get((bank_key, product_key))
    if exact:
        return exact

    for (candidate_bank, candidate_product), details in details_map.items():
        if candidate_bank != bank_key:
            continue
        if candidate_product in product_key or product_key in candidate_product:
            return details
    return None


def _enrich_schemes_with_product_details(
    schemes: list[dict[str, Any]], products: str | None,
) -> None:
    """Attach materials/process from wiki product library to each scheme."""
    if not schemes or not products:
        return

    details_map = _extract_product_details_from_products(products)
    if not details_map:
        return

    for scheme in schemes:
        bank_name = str(scheme.get("银行名称") or "")
        product_name = str(scheme.get("产品名称") or "")
        if not bank_name or not product_name:
            continue
        details = _find_product_details(bank_name, product_name, details_map)
        if not details:
            continue

        for field in _PRODUCT_CANONICAL_FIELDS:
            value = details.get(field)
            if value:
                scheme[field] = value

        for field in _PRODUCT_EXTRA_TEXT_FIELDS:
            value = details.get(field)
            if value:
                scheme[field] = value

        if details.get("审批说明"):
            scheme["审批说明"] = details["审批说明"]

        if not scheme.get("准备材料") and details.get("准备材料"):
            scheme["准备材料"] = details["准备材料"]
        if not scheme.get("审批流程") and details.get("审批流程"):
            scheme["审批流程"] = details["审批流程"]


def _build_matching_core_findings(
    customer_name: str,
    credit_type: str,
    recommended_count: int,
    summary: dict[str, str],
) -> dict[str, str]:
    """Build a minimal 核心发现 section."""
    data_completeness = "完整" if summary else "部分缺失"
    match_conclusion = (
        f"已为{customer_name}匹配到 {recommended_count} 个推荐方案"
        if recommended_count
        else f"已完成{customer_name}的方案匹配"
    )
    return {
        "资料类型": "企业贷款" if credit_type == "enterprise_credit" else "个人贷款",
        "数据完整性": data_completeness,
        "匹配结论": match_conclusion,
    }


def _parse_matching_result_locally(
    match_result: str, customer_name: str, credit_type: str, products: str | None = None,
) -> dict[str, Any] | None:
    """Parse match_result markdown locally without a second model call."""
    if not match_result.strip():
        return None

    summary = _parse_matching_summary(_extract_markdown_section(match_result, ["客户资料摘要"]))
    schemes = _parse_matching_schemes(match_result)
    _enrich_schemes_with_product_details(schemes, products)
    not_recommended_rows = _parse_markdown_table(_extract_markdown_section(match_result, ["不推荐产品"]))
    next_step = _extract_next_step(_extract_markdown_section(match_result, ["下一步建议"]))
    supplements = _parse_supplement_section(_extract_markdown_section(match_result, ["待补充资料", "需补充信息"]))
    materials = _parse_materials_block(_extract_markdown_section(match_result, ["准备材料"]))
    process_steps = _parse_process_block(_extract_markdown_section(match_result, ["审批流程"]))

    not_recommended: list[dict[str, str]] = []
    for row in not_recommended_rows:
        if len(row) < 2 or row[0] in {"产品", "产品名称"}:
            continue
        item = {
            "产品名称": row[0],
            "不符合原因": row[1],
        }
        if len(row) > 2 and row[2]:
            item["补充材料后可匹配"] = row[2]
        not_recommended.append(item)

    if materials or process_steps:
        for scheme in schemes:
            scheme.setdefault("准备材料", materials)
            scheme.setdefault("审批流程", process_steps)

    result: dict[str, Any] = {
        "核心发现": _build_matching_core_findings(customer_name, credit_type, len(schemes), summary),
    }
    if summary:
        result["客户资料摘要"] = summary
    if supplements:
        result["待补充资料"] = supplements
    if schemes:
        result["推荐方案"] = schemes
    if not_recommended:
        result["不推荐产品"] = not_recommended
    if next_step:
        result["下一步建议"] = next_step
    if materials:
        result["准备材料"] = materials
    if process_steps:
        result["审批流程"] = process_steps

    if len(result) == 1 and not schemes and not summary and not supplements:
        return None
    return result


def convert_matching_result_to_json(
    match_result: str, customer_name: str, credit_type: str, products: str | None = None,
) -> dict | None:
    """Convert matching result Markdown to JSON structured data.

    Args:
        match_result: Markdown formatted matching result from AI
        customer_name: Customer name for context
        credit_type: Credit type (personal/enterprise_credit)

    Returns:
        JSON structured data or None if conversion fails

    Note:
        - Uses parse_json() to handle truncated JSON (#8)
    """
    if not match_result:
        return None

    try:
        matching_data = _parse_matching_result_locally(match_result, customer_name, credit_type, products)
        if matching_data is None:
            logger.warning("Failed to parse matching markdown locally")
            return None

        logger.info(
            f"Successfully parsed matching result locally with {len(matching_data)} sections"
        )
        return matching_data

    except Exception as e:
        logger.error(f"Unexpected error during local matching conversion: {e}")
        return None


# ===== 申请表匹配检测 =====


def is_application_based_matching(message: str) -> bool:
    """判断用户消息是否要求“根据申请表匹配”。

    Args:
        message: 用户消息

    Returns:
        True 表示用户要求根据申请表匹配
    """
    if not message:
        return False
    according_to_application = "\u6839\u636e\u7533\u8bf7\u8868"
    by_application = "\u6309\u7533\u8bf7\u8868"
    normalized = re.sub(r"\s+", "", message)
    if according_to_application in normalized or by_application in normalized:
        return True
    return bool(re.search(r"\u7533\u8bf7\u8868.{0,10}\u5339\u914d", normalized))


# ===== 申请表缓存查询 =====


async def get_latest_application_for_customer(
    customer_name: str,
) -> tuple[bool, dict, str]:
    """从业务存储中获取指定客户的最新申请表数据。

    Args:
        customer_name: 客户名称

    Returns:
        Tuple of (found, flattened_data_dict, loan_type)
    """
    try:
        applications = await storage_service.list_saved_applications()
        if not applications:
            logger.info("[AppMatch] saved applications storage is empty")
            return (False, {}, "enterprise")

        target_app = await _find_customer_application(applications, customer_name)
        if not target_app:
            logger.info(f"[AppMatch] no saved application for {customer_name}")
            return (False, {}, "enterprise")

        loan_type = target_app.get("loanType") or "enterprise"
        app_data = target_app.get("applicationData") or {}

        if not app_data:
            logger.info(f"[AppMatch] cached application data is empty for {customer_name}")
            return (False, {}, loan_type)

        flattened = _flatten_application_data(app_data)
        logger.info(
            f"[AppMatch] found cached application for {customer_name}, "
            f"fields={len(flattened)}, loan_type={loan_type}"
        )
        return (True, flattened, loan_type)

    except Exception as e:
        logger.error(f"[AppMatch] 读取申请表存储失败: {e}")
        return (False, {}, "enterprise")

async def _find_customer_application(
    applications: list, customer_name: str,
) -> dict | None:
    """Find the latest non-stale application for a customer.

    Args:
        applications: List of application records
        customer_name: Customer name to search

    Returns:
        Application dict or None
    """
    _, resolved_customer_id = await _resolve_customer_id(customer_name)
    for app in applications:
        if app.get("stale"):
            continue
        app_customer_id = app.get("customerId") or ""
        if resolved_customer_id and app_customer_id == resolved_customer_id:
            return app
        app_name = app.get("customerName") or ""
        if app_name == customer_name:
            return app
    return None


def _flatten_application_data(app_data: dict) -> dict[str, str]:
    """Flatten nested application data to key-value pairs.

    Args:
        app_data: Nested application data dict

    Returns:
        Flattened dictionary
    """
    flattened: dict[str, str] = {}
    for section_name, section_data in app_data.items():
        if isinstance(section_data, dict):
            for key, value in section_data.items():
                if isinstance(value, (dict, list)):
                    flattened[key] = json.dumps(value, ensure_ascii=False)
                else:
                    flattened[key] = str(value) if value is not None else ""
        else:
            flattened[section_name] = str(section_data) if section_data is not None else ""
    return flattened


# ===== 本地存储客户数据查询 =====


async def get_customer_data_local(
    customer_name: str,
    prefer_latest_per_type: bool = False,
) -> tuple[bool, dict]:
    """Get customer data from local SQLite storage.

    Fetches all extraction results for a customer and merges them.

    Args:
        customer_name: Customer name

    Returns:
        Tuple of (customer_found, customer_data_dict)
    """
    try:
        customer, resolved_id = await _resolve_customer_id(customer_name)
        if not customer:
            logger.info(f"[Local] No customer found for: {customer_name}")
            return (False, {})

        extractions = await storage_service.get_extractions_by_customer(resolved_id)
        if not extractions:
            logger.info(f"[Local] Customer found but no extractions: {customer_name}")
            return (True, {})

        if prefer_latest_per_type:
            extractions = _select_latest_extractions_per_type(extractions)

        merged = _merge_extraction_data(extractions)
        logger.info(f"[Local] Found customer data with {len(merged)} fields")
        return (True, merged)

    except Exception as e:
        logger.error(f"[Local] Error fetching customer data: {e}")
        return (False, {})


async def _resolve_customer_id(
    customer_name: str,
) -> tuple[dict | None, str]:
    """Resolve customer name to customer_id with prefix fallback.

    Supports both prefixed (enterprise_/personal_) and legacy unprefixed IDs.

    Args:
        customer_name: Customer name

    Returns:
        Tuple of (customer_record, resolved_customer_id)
    """
    # 去掉前缀后的纯名称，兼容传入带前缀的情况
    clean_name = customer_name
    for prefix in ("enterprise_", "personal_"):
        if customer_name.startswith(prefix):
            clean_name = customer_name[len(prefix):]
            break

    for candidate_id in [
        f"enterprise_{clean_name}",
        f"personal_{clean_name}",
        clean_name,  # 兼容旧数据（无前缀）
    ]:
        customer = await storage_service.get_customer(candidate_id)
        if customer:
            return (customer, candidate_id)
    return (None, customer_name)


def _merge_extraction_data(extractions: list) -> dict[str, Any]:
    """Merge multiple extraction results into a single dict.

    Args:
        extractions: List of extraction records

    Returns:
        Merged data dictionary
    """
    merged: dict[str, Any] = {}
    for extraction in extractions:
        extracted_data = extraction.get("extracted_data") or {}
        if isinstance(extracted_data, dict):
            for key, value in extracted_data.items():
                if isinstance(value, (dict, list)):
                    merged[key] = json.dumps(value, ensure_ascii=False)
                else:
                    merged[key] = str(value) if value is not None else ""
    return merged


def _select_latest_extractions_per_type(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the newest extraction for each extraction_type.

    The storage layer returns rows ordered by ``created_at DESC``. For application
    generation we only want the latest document of each type to contribute data,
    so the first extraction encountered for each type should win.
    """
    latest_extractions: list[dict[str, Any]] = []
    seen_types: set[str] = set()

    for extraction in extractions:
        extraction_type = str(extraction.get("extraction_type") or "").strip()
        if not extraction_type:
            latest_extractions.append(extraction)
            continue
        if extraction_type in seen_types:
            continue
        seen_types.add(extraction_type)
        latest_extractions.append(extraction)

    return latest_extractions

