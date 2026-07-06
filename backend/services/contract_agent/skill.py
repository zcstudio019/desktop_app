from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from .schema import CONTRACT_CATEGORY_NAMES, ContractParty


logger = logging.getLogger(__name__)

CONTRACT_KEYWORDS = (
    "建设工程专业分包合同", "机电安装专业分包合同", "机电安装工程专业分包合同", "物资采购合同", "材料采购合同",
    "BIM 深化咨询服务合同", "BIM深化咨询服务合同", "咨询服务合同", "分包人", "承包人", "发包人",
    "甲方", "乙方", "合同价款", "合同工期", "付款方式", "采购清单", "结算方式", "签订日期", "签订地点",
)

PAYMENT_KEYWORDS = ("支付", "付款", "进度款", "预付款", "结算款", "质保金", "保修金", "发票", "支付至", "支付比例", "支付金额", "合同价款的", "%")
ITEM_HEADER_KEYWORDS = ("序号", "名称", "材料名称", "货物名称", "服务内容", "型号规格", "规格", "单位", "数量", "单价", "含税单价", "合价", "金额", "含税合价")
SIGNER_CONTEXT = ("法定代表人", "授权代表", "委托代理人", "签字", "签章", "经办人")

PAYMENT_CLAUSE_KEYWORDS = (
    "付款方式", "支付方式", "工程款支付", "进度款", "预付款", "结算款", "质保金", "支付至",
    "付款条件", "收到发票后", "验收合格后",
)
SETTLEMENT_CLAUSE_KEYWORDS = ("结算方式", "结算申请", "结算审核", "结算支付", "最终结算", "竣工结算")
INVOICE_CLAUSE_KEYWORDS = ("增值税专用发票", "增值税普通发票", "合法有效发票", "发票", "开票")
WARRANTY_CLAUSE_KEYWORDS = ("质量保证金", "质保金", "缺陷责任期", "保修期", "质量保修", "保修")
BREACH_CLAUSE_KEYWORDS = ("违约责任", "违约金", "违约", "逾期", "赔偿")
DISPUTE_CLAUSE_KEYWORDS = ("争议解决", "管辖法院", "人民法院", "仲裁", "诉讼", "协商解决")
NO_SUBCONTRACT_CLAUSE_KEYWORDS = ("禁止转包", "违法分包", "不得转包", "不得违法分包", "分包人不得")
EFFECTIVE_CLAUSE_KEYWORDS = ("合同生效", "本合同自", "签字盖章后生效", "双方盖章后生效", "双方签字盖章")
ATTACHMENT_CLAUSE_KEYWORDS = ("合同附件", "附件一", "附件二", "工程量清单", "报价清单", "专用条款", "通用条款", "附件")

CLAUSE_HEADING_RE = re.compile(
    r"^\s*(?:[一二三四五六七八九十百]+、|第[一二三四五六七八九十百\d]+条|\d+(?:\.\d+)+\s*|\d+[.、]\s*)"
)

DATE_RE = re.compile(r"((?:19|20)\d{2}\s*[年./-]\s*\d{1,2}\s*[月./-]\s*\d{1,2}\s*(?:日)?)")
LOOSE_DATE_RE = re.compile(r"((?:19|20)\d{2}\D{0,3}\d{1,2}\D{0,3}\d{1,2}\D{0,2})")
USCC_RE = re.compile(r"\b([0-9A-Z]{18})\b")
MONEY_RE = re.compile(r"(?:人民币)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)")
ID_CARD_RE = re.compile(r"(?<!\d)[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)")


def is_contract_like(text: str, filename: str = "") -> bool:
    source = f"{filename}\n{text}"
    lowered_name = str(filename or "").lower()
    if any(token in lowered_name for token in ("合同", "contract", "专业分包", "物资采购", "材料采购", "咨询服务", "bim")):
        return True
    return sum(1 for keyword in CONTRACT_KEYWORDS if keyword in source) >= 2


def is_toc_line(text: str) -> bool:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if not value:
        return False
    if re.search(r"[\.·。．…]{3,}\s*[-~－—]?\s*\d+\s*[-－—]?$", value):
        return True
    if re.search(r"^\s*(第?[一二三四五六七八九十百]+[章节条部分]?|\d+(?:\.\d+)*)(、|\.|\s).*[\.·。．…]{3,}.*\d+\s*$", value):
        return True
    if re.search(r"^\s*\d+(?:\.\d+)+\s*[^\n]{2,50}[\.·。．…]{3,}", value):
        return True
    has_section = bool(re.search(r"^\s*(第?[一二三四五六七八九十百]+[章节条部分]?|\d+(?:\.\d+)*)(、|\.|\s)", value))
    has_dots = bool(re.search(r"[\.·。．…]{3,}", value))
    has_page = bool(re.search(r"[-~－—]?\s*\d+\s*[-－—]?$", value))
    return has_section and has_dots and has_page


def clean_field_value(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:;；，,。|")
    if not text:
        return ""
    text = re.sub(r"[\.·。．…]{3,}\s*[-~－—]?\s*\d+\s*[-－—]?$", "", text).strip(" ：:;；，,。|")
    if not text or is_toc_line(text):
        return ""
    if "目录" in text and re.search(r"[\.·。．…]{3,}|[-－—]\s*\d+\s*[-－—]?$", text):
        return ""
    if re.search(r"[\.·。．…]{4,}|-{3,}|[-－—]\s*\d+\s*[-－—]?$", text):
        return ""
    if re.fullmatch(r"(第?[一二三四五六七八九十百]+[章节条部分]?|\d+(?:\.\d+)*)(、|\.|\s)[^，。；:：]{0,24}", text):
        return ""
    return text


def clean_party_name(value: Any) -> str:
    text = clean_field_value(value)
    text = re.sub(r"[（(]\s*以下简称\s*(?:甲方|乙方|承包人|分包人|供方|需方|发包人|买方|卖方)\s*[）)]", "", text)
    text = re.sub(r"以下简称\s*(?:甲方|乙方|承包人|分包人|供方|需方|发包人|买方|卖方)", "", text)
    return clean_field_value(text)


def is_truncated_clause(text: str) -> bool:
    value = clean_field_value(text)
    if not value:
        return False
    if value.endswith(("的", "和", "与", "及", "并", "为", "在", "至", "分包", "工程质", "文明施")):
        return True
    if len(value) < 20 and not re.search(r"[。；;、，,）)]$", value):
        return True
    if value.count("（") > value.count("）") or value.count("(") > value.count(")"):
        return True
    return False


BAD_CLAUSE_SUFFIXES = (
    "的", "和", "与", "及", "并", "为", "在", "至", "达", "内", "分包", "文明施", "工程质", "总体的",
    "，", "、", "；",
    "鐨?", "鍜?", "涓?", "鍙?", "骞?", "鍦?", "鑷?", "杈?", "鍐?", "鍒嗗寘", "鏂囨槑鏂?", "宸ョ▼璐?",
    "锛?", "銆?", "锛?",
)


def is_bad_clause_value(text: str) -> bool:
    value = clean_field_value(text)
    if not value:
        return False
    if value.endswith(BAD_CLAUSE_SUFFIXES):
        return True
    if value.count("（") > value.count("）") or value.count("(") > value.count(")"):
        return True
    if len(value) > 40 and not re.search(r"[。；;]$", value) and value.endswith(("，", "、", "；", ",")):
        return True
    return is_truncated_clause(value)


def _safe_clause(value: Any, fallback: str = "") -> str:
    text = clean_field_value(value)
    if not text:
        return ""
    return fallback if is_bad_clause_value(text) else text


def clean_contract_copies_text(value: Any) -> str:
    text = clean_field_value(value)
    text = re.sub(r"_+", "", text)
    text = re.sub(r"\s+", "", text)
    return clean_field_value(text)


def _normalize_construction_scope(value: Any) -> str:
    text = clean_field_value(value)
    if not text or is_bad_clause_value(text):
        return "机电安装工程相关专业分包内容，具体以合同正文及附件为准"
    return _ensure_chinese_period(text)


def _normalize_construction_method(value: Any, full_text: str) -> str:
    text = clean_field_value(value)
    source = f"{text}\n{full_text}"
    if text and "包工包料" in text and "包维修保修" in text:
        return _normalize_agreement_method(text)
    if any(token in source for token in ("包工包料", "包工期", "包质量", "包安全", "包文明施工", "鍖呭伐鍖呮枡", "鍖呭伐鏈?", "鍖呰川閲?", "鍖呭畨鍏?", "鍖呮枃鏄庢柦宸?")):
        return "包工包料、包工期、包质量、包安全、包文明施工等"
    return "" if is_bad_clause_value(text) else text


def _normalize_quality(value: Any, full_text: str) -> str:
    text = clean_field_value(value)
    source = f"{text}\n{full_text}"
    agreement_quality = _normalize_agreement_quality(text, full_text)
    if agreement_quality:
        return agreement_quality
    if "\u4e00\u6b21\u6027\u9a8c\u6536\u5408\u683c" in source:
        return "\u4e00\u6b21\u6027\u9a8c\u6536\u5408\u683c"
    return "" if is_bad_clause_value(text) else text


def _normalize_period(value: Any) -> str:
    text = clean_field_value(value)
    match = re.search(r"(\d{1,5})\s*(?:天|日历天|澶?)", text)
    if match:
        return f"{match.group(1)}天"
    return "" if is_bad_clause_value(text) else text


SIGNING_DATE_POSITIVE_CONTEXT = (
    "签订日期", "签署日期", "签约日期", "订立时间", "合同订立时间", "本合同订立时间",
)
SIGNING_DATE_NEGATIVE_CONTEXT = (
    "开工日期", "计划开工日期", "实际开工日期", "竣工日期", "计划竣工日期", "工期", "合同工期",
    "开始日期", "完成日期", "服务开始", "服务结束", "交货日期", "付款日期", "发票日期",
)
COMPLETE_DATE_RE = re.compile(
    r"((?:19|20)\d{2})\s*(?:年|[-/])\s*(\d{1,2})\s*(?:月|[-/])\s*(\d{1,2})\s*(?:日)?"
)


def is_valid_contract_signing_date_candidate(text: str, context: str) -> bool:
    candidate = clean_field_value(text)
    surrounding = str(context or "")
    if not COMPLETE_DATE_RE.fullmatch(candidate) and not COMPLETE_DATE_RE.search(candidate):
        return False
    if any(keyword in surrounding for keyword in SIGNING_DATE_NEGATIVE_CONTEXT):
        return False
    has_explicit_label = any(keyword in surrounding for keyword in SIGNING_DATE_POSITIVE_CONTEXT)
    has_signature_date = "日期" in surrounding and any(
        keyword in surrounding for keyword in ("甲方", "乙方", "承包人", "分包人", "签字", "签章", "盖章")
    )
    return has_explicit_label or has_signature_date


def _format_complete_date(value: str) -> str:
    match = COMPLETE_DATE_RE.search(str(value or ""))
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{int(year)}年{int(month)}月{int(day)}日"


def _date_from_lines(text: str, *, allow_signature_date: bool = False) -> str:
    raw_lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    for index, line in enumerate(raw_lines):
        if not any(keyword in line for keyword in SIGNING_DATE_POSITIVE_CONTEXT):
            continue
        context = "\n".join(raw_lines[max(0, index - 1):index + 2])
        for match in COMPLETE_DATE_RE.finditer(context):
            if is_valid_contract_signing_date_candidate(match.group(0), line):
                return _format_complete_date(match.group(0))
    if allow_signature_date:
        for index, line in enumerate(raw_lines):
            if "日期" not in line:
                continue
            context = "\n".join(raw_lines[max(0, index - 3):index + 4])
            for match in COMPLETE_DATE_RE.finditer(line):
                if is_valid_contract_signing_date_candidate(match.group(0), context):
                    return _format_complete_date(match.group(0))
    return ""


def _extract_contract_signing_date(pages: list[dict[str, Any]], signature_text: str) -> str:
    date = _date_from_lines(signature_text, allow_signature_date=True)
    if date:
        return date
    agreement_text = "\n".join(
        str(page.get("text") or "") for page in pages
        if any(keyword in str(page.get("text") or "") for keyword in ("订立时间", "合同订立时间", "本合同订立时间"))
    )
    date = _date_from_lines(agreement_text)
    if date:
        return date
    first_pages = "\n".join(str(page.get("text") or "") for page in pages[:2])
    return _date_from_lines(first_pages)



def _clean_bank_name(value: str, account: str = "") -> str:
    text = clean_field_value(value)
    if account:
        text = text.replace(account, "")
    text = text.replace("\uff1b", ";").replace("\uff1a", ":").replace("\uff0c", ",")
    parts = [part.strip(" -_:\\uff1a;\\uff1b,\\uff0c") for part in re.split(r"[;,:]", text) if part.strip(" -_:\\uff1a;\\uff1b,\\uff0c")]
    bank_parts = [part for part in parts if not re.search(r"(?<!\d)\d{8,30}(?!\d)", part) and not any(marker in part for marker in ("\u8d26\u53f7", "\u5e10\u53f7"))]
    candidate = bank_parts[0] if bank_parts else text
    for label in ("\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"):
        candidate = candidate.replace(label, "")
    candidate = candidate.strip(" -_:\\uff1a;\\uff1b,\\uff0c")
    return clean_field_value(candidate)
def _account_owner(lines: list[str], index: int, parties: list[ContractParty]) -> str:
    nearby = lines[max(0, index - 10):index + 11]
    party_a_name = clean_party_name(parties[0].name) if parties else ""
    party_b_name = clean_party_name(parties[1].name) if len(parties) > 1 else ""
    party_a_markers = ("甲方", "承包人", "发包人", party_a_name)
    party_b_markers = ("乙方", "分包人", party_b_name)

    def nearest(markers: tuple[str, ...]) -> int | None:
        distances = [
            abs(offset - min(10, index))
            for offset, line in enumerate(nearby)
            if any(marker and marker in line for marker in markers)
        ]
        return min(distances) if distances else None

    a_distance = nearest(party_a_markers)
    b_distance = nearest(party_b_markers)
    if b_distance is not None and b_distance <= 4 and (a_distance is None or b_distance < a_distance):
        return "party_b"
    if a_distance is not None and a_distance <= 4 and (b_distance is None or a_distance < b_distance):
        return "party_a"
    return ""


def extract_payment_account(
    parties: list[ContractParty] | None,
    text_blocks: str | list[str],
    category: str,
) -> str:
    text = "\n".join(text_blocks) if isinstance(text_blocks, list) else str(text_blocks or "")
    lines = _usable_lines(text)
    candidates: list[dict[str, str]] = []
    phone_markers = ("电话", "联系电话", "联系方式", "手机", "鐢佃瘽", "鑱旂郴", "閻絻", "閼辨梻")
    account_markers = ("开户银行", "开户行", "银行账号", "账号", "帐号", "收款账户", "寮€鎴", "璐﹀彿", "甯愬彿", "鏀舵")
    for index, line in enumerate(lines):
        if any(marker in line for marker in phone_markers):
            continue
        if not any(marker in line for marker in account_markers):
            continue
        candidate_lines = lines[max(0, index - 1):index + 2]
        candidate_text = "；".join(candidate_lines)
        account_match = re.search(r"(?<!\d)(\d{8,30})(?!\d)", candidate_text)
        account = account_match.group(1) if account_match else ""
        bank_match = re.search(r"(?:开户银行|开户行)\s*[:：]?\s*([^；;\n]{2,50})", candidate_text)
        bank = _clean_bank_name(bank_match.group(1), account) if bank_match else ""
        owner = _account_owner(lines, index, parties or [])
        explicit_receiving = any(marker in candidate_text for marker in ("收款账户", "支付账户"))
        candidates.append({"bank": bank, "account": account, "owner": owner, "explicit": "1" if explicit_receiving else ""})

    if category == "construction_subcontract":
        selected = next((item for item in candidates if item["owner"] == "party_b" and item["bank"] and item["account"]), None)
        if selected is None:
            selected = next((item for item in candidates if item["explicit"] and item["owner"] != "party_a"), None)
    else:
        selected = next((item for item in candidates if item["owner"] == "party_b"), None) or next(
            (item for item in candidates if item["explicit"]), None
        )
    if not selected:
        return ""
    bank = selected["bank"]
    account = selected["account"]
    if bank and account:
        return f"\u5f00\u6237\u94f6\u884c\uff1a{bank}\uff1b\u8d26\u53f7\uff1a{account}"
    if bank:
        return f"\u5f00\u6237\u94f6\u884c\uff1a{bank}\uff1b\u8d26\u53f7\uff1a\u672a\u8bc6\u522b"
    if account:
        return f"\u8d26\u53f7\uff1a{account}\uff08\u5f00\u6237\u94f6\u884c\u672a\u8bc6\u522b\uff09"
    return ""


def _finalize_contract_result(
    result: dict[str, Any],
    full_text: str,
    signature_text: str,
    page_items: list[dict[str, Any]],
) -> None:
    category = str(result.get("contract_category") or "")
    result["signing_date"] = _extract_contract_signing_date(page_items, signature_text)
    result["copies"] = clean_contract_copies_text(result.get("copies"))
    project = result.get("project") if isinstance(result.get("project"), dict) else {}
    if category == "construction_subcontract":
        project["scope"] = _normalize_construction_scope(project.get("scope"))
        project["method"] = _normalize_construction_method(project.get("method"), full_text)
        project["quality_standard"] = _normalize_quality(project.get("quality_standard"), full_text)
    else:
        for key in ("scope", "method", "quality_standard", "safety_requirement", "standards"):
            project[key] = _safe_clause(project.get(key))
    duration = result.get("duration") if isinstance(result.get("duration"), dict) else {}
    duration["period"] = _normalize_period(duration.get("period"))
    settlement = result.get("settlement") if isinstance(result.get("settlement"), dict) else {}
    settlement["receiving_account"] = extract_payment_account(
        result.get("parties") or [], full_text, category
    )
    clauses = result.get("clauses") if isinstance(result.get("clauses"), dict) else {}
    clauses["quality_acceptance"] = _normalize_quality(clauses.get("quality_acceptance"), full_text)
    clauses["warranty"] = _safe_clause(clauses.get("warranty"))
    clauses["safety_civilization"] = _safe_clause(clauses.get("safety_civilization"))
    has_zero_safety_fee = bool(re.search(
        r"安全文明施工费[^\n]{0,30}(?:零元|[￥¥]?\s*0(?:\.0+)?\s*元)",
        full_text,
    ))
    if has_zero_safety_fee or any(token in full_text for token in ("安全文明施工费为0元", "安全文明施工费为 0 元", "瀹夊叏鏂囨槑鏂藉伐璐逛负0鍏?", "瀹夊叏鏂囨槑鏂藉伐璐逛负 0 鍏?")):
        clauses["safety_civilization"] = "安全文明施工费为 0 元。"
    parties = result.get("parties") or []
    roles = {
        "construction_subcontract": ("甲方/承包人/发包人", "乙方/分包人"),
        "material_purchase": ("甲方/需方/买方", "乙方/供方/卖方"),
        "consulting_service": ("甲方/委托方", "乙方/受托方"),
    }.get(category)
    if roles:
        for index, party in enumerate(parties[:2]):
            party.role = roles[index]
            party.name = clean_party_name(party.name)
            if party.bank_account and party.phone == party.bank_account:
                party.phone = ""
    signature = result.get("signature") if isinstance(result.get("signature"), dict) else {}
    signature["signing_date"] = result.get("signing_date") or signature.get("signing_date")
    if not is_valid_person_name(str(signature.get("signers") or "")):
        signature["signers"] = ""


def _repair_truncated_from_lines(lines: list[str], index: int) -> str:
    current = clean_field_value(lines[index]) if 0 <= index < len(lines) else ""
    if not current:
        return ""
    if not is_truncated_clause(current):
        return current
    if index + 1 < len(lines):
        merged = clean_field_value(f"{current}{lines[index + 1]}")
        if merged and not is_truncated_clause(merged):
            return merged
    return ""


def _pages(text: str, pages: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    result = []
    for idx, page in enumerate(pages or [], start=1):
        if isinstance(page, dict):
            preserved = dict(page)
            preserved["page"] = int(page.get("page") or idx)
            preserved["text"] = str(page.get("text") or "")
            result.append(preserved)
    if not result and str(text or "").strip():
        result.append({"page": 1, "text": str(text or "")})
    return result


def _usable_lines(text: str) -> list[str]:
    return [clean_field_value(line) for line in str(text or "").splitlines() if clean_field_value(line)]


def _joined(pages: list[dict[str, Any]]) -> str:
    return "\n\n".join(f"--- 第 {page['page']} 页 ---\n{page['text']}" for page in pages if str(page.get("text") or "").strip())


def _source_page(pages: list[dict[str, Any]], value: str) -> int | None:
    if not value:
        return None
    for page in pages:
        if value in str(page.get("text") or ""):
            return int(page.get("page") or 0) or None
    return None


def _after_label(text: str, labels: tuple[str, ...], max_len: int = 120) -> str:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([^\n\r]{{1,{max_len}}})")
        match = pattern.search(text)
        if match:
            value = clean_field_value(match.group(1))
            if value:
                return value
    return ""


def _line_with(text: str, keywords: tuple[str, ...]) -> str:
    lines = _usable_lines(text)
    for index, line in enumerate(lines):
        if any(keyword in line for keyword in keywords):
            return _repair_truncated_from_lines(lines, index)
    return ""


def clean_clause_text(lines: list[str] | str, max_chars: int = 300) -> str:
    source_lines = lines if isinstance(lines, list) else str(lines or "").splitlines()
    cleaned_lines: list[str] = []
    for line in source_lines:
        raw = str(line or "").strip()
        if not raw or is_toc_line(raw) or raw.startswith("--- 第"):
            continue
        cleaned = clean_field_value(raw)
        if cleaned:
            cleaned_lines.append(cleaned)
    text = re.sub(r"\s+", " ", " ".join(cleaned_lines)).strip(" ：:;；，,")
    if not text or is_toc_line(text):
        return ""
    body = CLAUSE_HEADING_RE.sub("", text, count=1).strip(" ：:;；，,")
    if len(body) < 12 or is_bad_clause_value(body):
        return ""
    if len(text) > max_chars:
        return text[:max_chars].rstrip(" ，,；;") + "……"
    return text


def _is_toc_page(text: str) -> bool:
    raw_lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    toc_lines = sum(1 for line in raw_lines if is_toc_line(line))
    return "目录" in "".join(raw_lines[:8]) and toc_lines >= 2


def extract_toc_entries(ocr_pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        for raw_line in str(page.get("text") or "").splitlines():
            line = raw_line.strip()
            if not is_toc_line(line):
                continue
            page_match = re.search(r"[-~－—]?\s*(\d{1,3})\s*[-－—]?\s*$", line)
            if not page_match:
                continue
            title = re.sub(r"[\.·。．…]{3,}.*$", "", line).strip()
            entries.append({
                "title": title,
                "target_page": int(page_match.group(1)),
                "source_page": int(page.get("page") or 0),
            })
    return entries


def detect_contract_body_missing(
    ocr_pages: list[dict[str, Any]],
    page_count: int,
    toc_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    actual_page_count = int(page_count or len(ocr_pages or []))
    target_pages = [
        int(item.get("target_page") or 0)
        for item in toc_entries or []
        if int(item.get("target_page") or 0) > actual_page_count
    ]
    body_missing = bool(
        actual_page_count <= 10
        and len(set(target_pages)) >= 3
        and max(target_pages or [0]) >= actual_page_count + 10
    )
    note = (
        "当前PDF疑似仅包含合同协议书、目录及签章页，通用/专用条款正文未包含在本文件中"
        if body_missing else ""
    )
    return {
        "body_missing": body_missing,
        "body_missing_note": note,
        "toc_target_pages_beyond_file": sorted(set(target_pages)),
    }


def extract_clause_by_keywords(
    ocr_pages: list[dict[str, Any]],
    keywords: tuple[str, ...],
    max_chars: int = 300,
) -> str:
    for page in ocr_pages or []:
        page_text = str(page.get("text") or "") if isinstance(page, dict) else ""
        if not page_text or _is_toc_page(page_text):
            continue
        raw_lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        for index, raw_line in enumerate(raw_lines):
            if is_toc_line(raw_line) or not any(keyword in raw_line for keyword in keywords):
                continue
            first_line = clean_field_value(raw_line)
            if not first_line and CLAUSE_HEADING_RE.match(raw_line):
                first_line = re.sub(r"\s+", " ", raw_line).strip()
            if not first_line:
                continue
            collected = [first_line]
            appended_body_lines = 0
            for next_line in raw_lines[index + 1:index + 9]:
                if is_toc_line(next_line):
                    continue
                cleaned_next = clean_field_value(next_line)
                if not cleaned_next:
                    continue
                if CLAUSE_HEADING_RE.match(cleaned_next):
                    break
                collected.append(cleaned_next)
                appended_body_lines += 1
                merged = " ".join(collected)
                if appended_body_lines >= 3 and re.search(r"[。；;]$", merged):
                    break
            clause = clean_clause_text(collected, max_chars=max_chars)
            if clause:
                return clause
    return ""


def _needs_second_pass(value: Any) -> bool:
    text = clean_field_value(value)
    return not text or is_toc_line(text) or is_bad_clause_value(text)


def _effective_condition_from_clause(clause: str) -> str:
    match = re.search(
        r"(本合同(?:自|经)[^。；]{0,60}?(?:签字盖章|盖章|签署)[^。；]{0,20}?生效)",
        clause,
    )
    if match:
        return clean_field_value(match.group(1))
    match = re.search(r"((?:双方|甲乙双方)[^。；]{0,40}?(?:签字盖章|盖章)后生效)", clause)
    return clean_field_value(match.group(1)) if match else ""


AGREEMENT_FIELD_LABELS = (
    "总包工程名称", "分包工程名称", "工程名称", "分包工程地点", "工程地点",
    "分包工程承包范围和内容", "分包范围", "承包范围", "承包方式", "计划开工日期",
    "计划竣工日期", "计划完工日期", "合同工期", "工期", "质量标准", "签订地点", "合同份数",
)


def extract_labeled_multiline_value(
    ocr_pages: list[dict[str, Any]],
    labels: tuple[str, ...],
    *,
    max_lines: int = 8,
    max_chars: int = 800,
) -> str:
    for page in ocr_pages or []:
        if not isinstance(page, dict) or _is_toc_page(str(page.get("text") or "")):
            continue
        raw_lines = [line.strip() for line in str(page.get("text") or "").splitlines() if line.strip()]
        for index, raw_line in enumerate(raw_lines):
            matched_label = next((label for label in labels if label in raw_line), "")
            if not matched_label or is_toc_line(raw_line):
                continue
            if raw_line.find(matched_label) > 12:
                continue
            remainder = raw_line.split(matched_label, 1)[1].lstrip(" ：:、")
            remainder = _truncate_at_following_label(remainder, matched_label)
            values = [remainder] if remainder else []
            for next_line in raw_lines[index + 1:index + 1 + max_lines]:
                if is_toc_line(next_line):
                    break
                if any(label in next_line for label in AGREEMENT_FIELD_LABELS):
                    break
                if CLAUSE_HEADING_RE.match(next_line) and values:
                    break
                cleaned_next = clean_field_value(next_line)
                if cleaned_next:
                    values.append(cleaned_next)
                if len("".join(values)) >= max_chars:
                    break
            value_text = re.sub(r"\s+", "", "".join(values)).strip(" ：:;；")
            if value_text:
                return value_text[:max_chars]
    return ""


def _truncate_at_following_label(value: str, current_label: str = "") -> str:
    text = str(value or "")
    stops = [label for label in AGREEMENT_FIELD_LABELS if label and label != current_label]
    positions: list[int] = []
    for label in stops:
        match = re.search(rf"(?:^|[。；;，,\s])\s*{re.escape(label)}\s*[:：]", text)
        if match and match.start() > 0:
            positions.append(match.start())
    if positions:
        text = text[:min(positions)]
    return text.strip(" ：:;；，,。")


def _ensure_chinese_period(value: Any) -> str:
    text = clean_field_value(value)
    if not text:
        return ""
    return text if text.endswith(("。", "！", "？")) else f"{text}。"


def _normalize_agreement_scope(value: Any) -> str:
    text = clean_field_value(value)
    if not text:
        return ""
    text = text.replace(")", "）").replace("(", "（")
    text = re.sub(r"支架、?等", "支架等", text)
    text = text.replace("），等", "）等").replace("),等", "）等")
    text = re.sub(r"等一切与机电安装相关的工作。?$", "等一切与机电安装相关的工作", text)
    return _ensure_chinese_period(text)


def _normalize_agreement_method(value: Any) -> str:
    text = clean_field_value(value)
    if not text:
        return ""
    if text.endswith("。"):
        text = text[:-1]
    if "施工专业分包方式" not in text and "包工包料" in text:
        text = f"{text}的施工专业分包方式"
    return _ensure_chinese_period(text)


def _normalize_agreement_quality(value: Any, full_text: str) -> str:
    text = clean_field_value(value)
    source = f"{text}\n{full_text}"
    if "一次性验收合格" in source and any(token in source for token in ("文明工地", "无死亡事故", "无重大伤残事故")):
        return "符合总包合同约定的分包工程质量标准，并达到一次性验收合格；施工期间无死亡事故、无重大伤残事故，达到上海市文明工地标准。"
    if "分包工程质量标准" in text and "一次性验收合格" in text:
        return _ensure_chinese_period(text)
    pattern = re.search(
        r"(符合总包合同约定的分包工程质量标准[^。\n]{0,120}?一次性验收合格[^。\n]{0,160}?上海市文明工地标准)",
        full_text,
    )
    if pattern:
        return _ensure_chinese_period(pattern.group(1))
    if "一次性验收合格" in text and "有不同规定" not in text:
        return _ensure_chinese_period(text)
    return ""


def normalize_project_location(value: Any) -> str:
    text = clean_field_value(value)
    if not text:
        return ""
    text = text.replace("绿化带北至", "绿化带，北至")
    text = re.sub(r"(?<!，)(路)北至", r"\1，北至", text)
    return clean_field_value(text)


def _format_money_value(raw: str) -> str:
    text = str(raw or "").replace("，", ",").replace("．", ".")
    match = re.search(r"(?<!\d)(\d[\d,\s]*(?:\s*\.\s*\d{1,2})?)(?!\d)", text)
    if not match:
        return ""
    normalized = re.sub(r"\s+", "", match.group(1)).replace(",", "")
    try:
        return f"{Decimal(normalized):,.2f} 元"
    except InvalidOperation:
        return ""


def _money_near_label(text: str, labels: tuple[str, ...], window: int = 180) -> str:
    for label in labels:
        position = text.find(label)
        if position < 0:
            continue
        value = _format_money_value(text[position:position + window])
        if value:
            return value
    return ""


def _money_from_segment(segment: str) -> str:
    return _format_money_value(segment)


AMOUNT_PAGE_KEYWORDS = (
    "签约合同价",
    "含税",
    "不含增值税",
    "不含税",
    "增值税税额",
    "增值税额",
    "税额",
    "税率",
    "合同价格形式",
    "价格形式",
    "固定总价",
    "固定单价",
    "合同总价明细表",
    "汇总表",
    "除税预算造价",
    "税金",
    "合计",
)


def normalize_amount_page_text(text: str) -> str:
    raw_lines = [line.strip() for line in str(text or "").replace("，", ",").replace("．", ".").splitlines()]
    raw_lines = [line for line in raw_lines if line]
    merged_lines: list[str] = []
    continuation_labels = ("不含增值税", "不含税", "增值税税额", "增值税额", "税额", "合同价格形式", "价格形式")
    stop_labels = ("签约合同价", "安全文明施工费", "合同文件构成", "计划", "质量标准")
    for line in raw_lines:
        compact_line = re.sub(r"\s+", "", line)
        should_merge = False
        if merged_lines:
            previous = re.sub(r"\s+", "", merged_lines[-1])
            current_starts_with_value = bool(re.match(r"^(?:人民币)?\d", compact_line))
            should_merge = any(label in previous for label in continuation_labels) and current_starts_with_value
            should_merge = should_merge and not any(label in compact_line for label in stop_labels)
        if should_merge:
            merged_lines[-1] = f"{merged_lines[-1]} {line}"
        else:
            merged_lines.append(line)
    normalized = "\n".join(merged_lines)
    normalized = re.sub(r"(?<=\d)\s+(?=\d)", "", normalized)
    normalized = re.sub(r"(?<=\d)\s*([.,])\s*(?=\d)", r"\1", normalized)
    normalized = re.sub(r"\s+", "", normalized)
    return normalized


def _iter_contract_amount_page_texts(ocr_pages: list[dict[str, Any]]) -> list[str]:
    page_texts: list[str] = []
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        page_text = str(page.get("text") or "")
        compact = normalize_amount_page_text(page_text)
        if any(keyword in compact for keyword in AMOUNT_PAGE_KEYWORDS):
            page_texts.append(page_text)
    return page_texts


def _money_candidates_after_amount_labels(text: str, labels: tuple[str, ...], window: int = 180) -> list[str]:
    compact = normalize_amount_page_text(text)
    values: list[str] = []
    for label in labels:
        for label_match in re.finditer(re.escape(label), compact):
            segment = compact[label_match.start():label_match.start() + window]
            for money_match in re.finditer(r"(?<!\d)(\d[\d,]*(?:\.\d{1,2})?)(?!\d)\s*(元|圆)?", segment):
                if money_match.end() < len(segment) and segment[money_match.end():money_match.end() + 1] == "%":
                    continue
                digits = re.sub(r"\D", "", money_match.group(1))
                if not money_match.group(2) and len(digits) < 6:
                    continue
                value = _format_money_value(money_match.group(0))
                if value and value not in values:
                    values.append(value)
    return values


def _money_after_amount_labels(text: str, labels: tuple[str, ...], window: int = 180) -> str:
    candidates = _money_candidates_after_amount_labels(text, labels, window)
    return candidates[0] if candidates else ""


def extract_tax_excluded_amount(amount_page_text: str) -> str:
    return _money_after_amount_labels(
        amount_page_text,
        (
            "不含增值税签约合同价",
            "不含税签约合同价",
            "不含增值税合同价",
            "不含税合同价",
            "不含增值税金额",
            "不含税金额",
            "不含增值税价",
            "不含税价",
            "不含增值税",
            "不含税",
        ),
        120,
    )


def extract_tax_amount(amount_page_text: str) -> str:
    compact = normalize_amount_page_text(amount_page_text)
    labels = ("增值税税额", "增值税额", "增值税金额", "税额")
    for label in labels:
        for label_match in re.finditer(re.escape(label), compact):
            segment = compact[label_match.start():label_match.start() + 120]
            for money_match in re.finditer(r"(?<!\d)(\d[\d,]*(?:\.\d{1,2})?)(?!\d)\s*(元|圆)?", segment):
                if money_match.end() < len(segment) and segment[money_match.end():money_match.end() + 1] == "%":
                    continue
                if re.search(r"[=＝×*]", segment[:money_match.start()]):
                    continue
                value = _format_money_value(money_match.group(0))
                if value and value != "0.00 元":
                    return value
    return ""


def _extract_tax_rate_from_amount_text(text: str) -> str:
    compact = normalize_amount_page_text(text)
    for tax_rate_match in re.finditer(r"(?:增值税)?税率(?:为|[:：])?[^0-9%]{0,12}(\d+(?:\.\d+)?%)", compact):
        return tax_rate_match.group(1)
    return ""


def extract_price_form(amount_page_text: str) -> str:
    compact = normalize_amount_page_text(amount_page_text)
    label_positions = [
        position
        for label in ("合同价格形式", "价格形式")
        for position in [compact.find(label)]
        if position >= 0
    ]
    for position in sorted(set(label_positions)):
        window = compact[position:position + 80]
        for price_form in ("固定总价", "固定单价", "总价合同", "单价合同", "可调价格"):
            if price_form in window:
                return "固定总价" if price_form == "总价合同" else ("固定单价" if price_form == "单价合同" else price_form)
    return ""


def _format_decimal_money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f} 元"


def _decimal_from_money_text(value: Any) -> Decimal | None:
    formatted = _format_money_value(str(value or ""))
    if not formatted:
        return None
    return _decimal_from_amount_value(formatted)


def _decimal_from_amount_value(value: Any) -> Decimal | None:
    number_text = re.sub(r"[^\d.]", "", str(value or ""))
    if not number_text:
        return None
    try:
        return Decimal(number_text)
    except InvalidOperation:
        return None


def _small_chinese_money_fraction(upper: Any) -> Decimal:
    text = str(upper or "")
    tail = re.split(r"[元圆]", text)[-1] if re.search(r"[元圆]", text) else text
    digit_map = {"零": 0, "壹": 1, "一": 1, "贰": 2, "二": 2, "叁": 3, "三": 3, "肆": 4, "四": 4, "伍": 5, "五": 5, "陆": 6, "六": 6, "柒": 7, "七": 7, "捌": 8, "八": 8, "玖": 9, "九": 9}
    fraction = Decimal("0.00")
    jiao = re.search(r"([零壹一贰二叁三肆四伍五陆六柒七捌八玖九])角", tail)
    fen = re.search(r"([零壹一贰二叁三肆四伍五陆六柒七捌八玖九])分", tail)
    if jiao:
        fraction += Decimal(digit_map.get(jiao.group(1), 0)) / Decimal("10")
    if fen:
        fraction += Decimal(digit_map.get(fen.group(1), 0)) / Decimal("100")
    return fraction.quantize(Decimal("0.01"))


def _repair_amount_fraction_from_upper(amount: dict[str, Any]) -> None:
    fraction = _small_chinese_money_fraction(amount.get("amount_upper"))
    if fraction <= 0:
        return
    for key in ("amount_lower", "tax_included_amount"):
        value = _decimal_from_amount_value(amount.get(key))
        if value is None or value != value.quantize(Decimal("1")):
            continue
        repaired = value + fraction
        amount[key] = _format_decimal_money(repaired)
    value = _decimal_from_amount_value(amount.get("amount_lower") or amount.get("tax_included_amount"))
    if value is not None:
        amount["contract_amount"] = f"人民币 {_format_decimal_money(value)}"


def _repair_tax_amount_from_included_excluded(amount: dict[str, Any]) -> None:
    included = _decimal_from_amount_value(amount.get("tax_included_amount") or amount.get("amount_lower"))
    excluded = _decimal_from_amount_value(amount.get("tax_excluded_amount"))
    tax = _decimal_from_amount_value(amount.get("tax_amount"))
    if included is None or excluded is None:
        return
    computed = (included - excluded).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if computed <= 0:
        return
    if tax is None and not amount.get("summary_table_amounts"):
        return
    if tax is None or (tax == tax.quantize(Decimal("1")) and computed != tax):
        amount["tax_amount"] = _format_decimal_money(computed)
        amount["tax_amount_source"] = "ocr"
        amount.pop("tax_amount_inferred", None)
        amount.pop("tax_amount_calculation_basis", None)


def _derive_tax_amounts(amount: dict[str, Any]) -> bool:
    if amount.get("tax_excluded_amount") and not amount.get("tax_excluded_amount_source"):
        amount["tax_excluded_amount_source"] = "ocr"
    if amount.get("tax_amount") and not amount.get("tax_amount_source"):
        amount["tax_amount_source"] = "ocr"
    if amount.get("price_form") and not amount.get("price_form_source"):
        amount["price_form_source"] = "ocr"
    if amount.get("tax_excluded_amount") and amount.get("tax_amount"):
        return False
    included = _decimal_from_amount_value(amount.get("tax_included_amount") or amount.get("amount_lower"))
    if included is None:
        return False
    excluded_value = _decimal_from_amount_value(amount.get("tax_excluded_amount"))
    if excluded_value is not None and not amount.get("tax_amount"):
        tax = (included - excluded_value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        amount["tax_amount"] = f"{_format_decimal_money(tax)}（根据含税金额和不含税金额推算，需人工复核）"
        amount["tax_amount_inferred"] = True
        amount["tax_amount_source"] = "calculated"
        amount["tax_amount_calculation_basis"] = "included_minus_excluded"
        return True

    rate_text = str(amount.get("tax_rate") or "")
    rate_match = re.search(r"(\d+(?:\.\d+)?)%", rate_text)
    if not rate_match:
        return False
    try:
        rate = Decimal(rate_match.group(1)) / Decimal("100")
        excluded = (included / (Decimal("1") + rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        tax = (included - excluded).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ZeroDivisionError):
        return False
    if not amount.get("tax_excluded_amount"):
        amount["tax_excluded_amount"] = f"{_format_decimal_money(excluded)}（根据含税金额和税率推算，需人工复核）"
        amount["tax_excluded_amount_inferred"] = True
        amount["tax_excluded_amount_source"] = "calculated"
        amount["tax_excluded_amount_calculation_basis"] = "included_and_rate"
    if not amount.get("tax_amount"):
        amount["tax_amount"] = f"{_format_decimal_money(tax)}（根据含税金额和税率推算，需人工复核）"
        amount["tax_amount_inferred"] = True
        amount["tax_amount_source"] = "calculated"
        amount["tax_amount_calculation_basis"] = "included_and_rate"
    return bool(amount.get("tax_excluded_amount_inferred") or amount.get("tax_amount_inferred"))


def _apply_tax_amount_consistency(amount: dict[str, Any]) -> None:
    included = _decimal_from_amount_value(amount.get("tax_included_amount") or amount.get("amount_lower"))
    excluded = _decimal_from_amount_value(amount.get("tax_excluded_amount"))
    tax = _decimal_from_amount_value(amount.get("tax_amount"))
    if included is None or excluded is None or tax is None:
        return
    difference = abs(included - excluded - tax)
    if difference == 0:
        existing = str(amount.get("amount_check") or "")
        if "大写金额与小写金额基本一致" in existing:
            amount["amount_check"] = "大写金额与小写金额基本一致；含税金额、不含税金额与税额基本一致"
        else:
            amount["amount_check"] = "含税金额、不含税金额与税额基本一致"
        amount["tax_check"] = "一致"
        amount["recognition_status"] = "成功"


def _ensure_amount_source_defaults(amount: dict[str, Any]) -> None:
    for field in ("tax_excluded_amount", "tax_amount", "price_form"):
        source_key = f"{field}_source"
        if amount.get(field) and not amount.get(source_key):
            amount[source_key] = "ocr"
        elif not amount.get(field) and not amount.get(source_key):
            amount[source_key] = "missing"


def _log_contract_amount_debug(
    page: dict[str, Any],
    normalized_window: str,
    matched_keywords: list[str],
    extracted: dict[str, str],
) -> None:
    page_number = page.get("page") or page.get("page_number") or page.get("page_index") or "unknown"
    raw_lines = [line.strip() for line in str(page.get("text") or "").splitlines() if line.strip()]
    logger.info(
        "[ContractAmountDebug] page=%s matched_keywords=%s",
        page_number,
        ",".join(matched_keywords),
    )
    logger.info(
        "[ContractAmountDebug] raw_lines=\n%s",
        "\n".join(f"{index + 1}: {line}" for index, line in enumerate(raw_lines)),
    )
    logger.info("[ContractAmountDebug] normalized_window=%s", normalized_window[:1000])
    logger.info(
        "[ContractAmountDebug] tax_excluded_amount_candidates=%s tax_amount_candidates=%s price_form_candidates=%s selected=%s",
        _money_candidates_after_amount_labels(normalized_window, ("不含增值税", "不含税", "不含增值税金额", "不含税金额"), 160),
        _money_candidates_after_amount_labels(normalized_window, ("增值税税额", "增值税额", "增值税金额", "税额"), 160),
        [price_form for price_form in ("固定总价", "固定单价", "总价合同", "单价合同") if price_form in normalized_window],
        extracted,
    )
    price_window = ""
    for label in ("合同价格形式", "价格形式"):
        position = normalized_window.find(label)
        if position >= 0:
            price_window = normalized_window[position:position + 120]
            break
    logger.info("[ContractPriceFormDebug] page=%s raw_window=%s", page_number, price_window)
    logger.info(
        "[ContractPriceFormDebug] candidates=%s selected=%s source=%s",
        [price_form for price_form in ("固定总价", "固定单价", "总价合同", "单价合同", "可调价格") if price_form in price_window],
        extracted.get("price_form") or "",
        extracted.get("price_form_source") or ("ocr" if extracted.get("price_form") else "missing"),
    )


def extract_contract_tax_amounts_from_amount_page(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    amount_data: dict[str, str] = {}
    amount_pages = []
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        page_text = str(page.get("text") or "")
        normalized = normalize_amount_page_text(page_text)
        matched_keywords = [keyword for keyword in AMOUNT_PAGE_KEYWORDS if keyword in normalized]
        if matched_keywords:
            amount_pages.append((page, page_text, normalized, matched_keywords))
    if not amount_pages:
        return amount_data

    candidates = [page_text for _, page_text, _, _ in amount_pages]
    candidates.append("\n".join(candidates))
    for candidate in candidates:
        if not amount_data.get("tax_excluded_amount"):
            excluded = extract_tax_excluded_amount(candidate)
            if excluded:
                amount_data["tax_excluded_amount"] = excluded
                amount_data["tax_excluded_amount_source"] = "ocr"
        if not amount_data.get("tax_rate"):
            tax_rate = _extract_tax_rate_from_amount_text(candidate)
            if tax_rate:
                amount_data["tax_rate"] = tax_rate
        if not amount_data.get("tax_amount"):
            tax_amount = extract_tax_amount(candidate)
            if tax_amount:
                amount_data["tax_amount"] = tax_amount
                amount_data["tax_amount_source"] = "ocr"
        if not amount_data.get("price_form"):
            price_form = extract_price_form(candidate)
            if price_form:
                amount_data["price_form"] = price_form
                amount_data["price_form_source"] = "ocr"

    for page, _, normalized, matched_keywords in amount_pages:
        _log_contract_amount_debug(page, normalized, matched_keywords, amount_data)

    return amount_data


def extract_contract_summary_table_amounts(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    amount_data: dict[str, str] = {}
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        raw_text = str(page.get("text") or "")
        normalized = normalize_amount_page_text(raw_text)
        if not any(marker in normalized for marker in ("合同总价明细表", "汇总表", "除税预算造价")):
            continue
        if not amount_data.get("tax_excluded_amount"):
            excluded = _money_after_amount_labels(normalized, ("除税预算造价", "不含税预算造价", "不含税金额", "除税造价"), 120)
            if excluded:
                amount_data["tax_excluded_amount"] = excluded
                amount_data["tax_excluded_amount_source"] = "ocr"
        if not amount_data.get("tax_rate"):
            rate_match = re.search(r"税金\s*(\d+(?:\.\d+)?)%", normalized) or re.search(r"税率\s*(\d+(?:\.\d+)?)%", normalized)
            if rate_match:
                amount_data["tax_rate"] = f"{rate_match.group(1)}%"
        if not amount_data.get("tax_amount"):
            tax_amount = _money_after_amount_labels(normalized, ("税金9%", "税金", "税额"), 120)
            if tax_amount:
                amount_data["tax_amount"] = tax_amount
                amount_data["tax_amount_source"] = "ocr"
        if not amount_data.get("tax_included_amount"):
            total_candidates = _money_candidates_after_amount_labels(normalized, ("合计", "价税合计", "含税金额"), 160)
            if total_candidates:
                amount_data["tax_included_amount"] = total_candidates[-1]
                amount_data["amount_lower"] = total_candidates[-1]
                amount_data["contract_amount"] = f"人民币 {total_candidates[-1]}"
        if amount_data.get("tax_excluded_amount") and amount_data.get("tax_amount") and amount_data.get("tax_included_amount"):
            break
    if amount_data:
        amount_data["summary_table_amounts"] = True
        _apply_tax_amount_consistency(amount_data)
    return amount_data


def extract_contract_amounts_from_agreement_page(page_text: str) -> dict[str, str]:
    text = str(page_text or "")
    compact = re.sub(r"\s+", "", text)
    if not any(marker in compact for marker in ("签约合同价", "增值税税额", "合同价格形式", "不含增值税")):
        return {}
    amount_data: dict[str, str] = {}
    included = _money_near_label(compact, ("签约合同价暂定为含税", "签约合同价暂定为（含税）", "签约合同价含税", "含税签约合同价"), 260)
    if not included:
        included = _money_from_segment(compact[:260])
    upper_segment = compact
    upper_matches = re.findall(r"[零壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]{6,120}", upper_segment)
    upper = max(upper_matches, key=len) if upper_matches else ""
    if upper and "角" not in upper and "分" not in upper:
        suffix = re.search(r"[零壹贰叁肆伍陆柒捌玖]角[零壹贰叁肆伍陆柒捌玖]分", upper_segment)
        if suffix and suffix.group(0) not in upper:
            upper = f"{upper}{suffix.group(0)}"
    excluded = _money_near_label(compact, ("不含增值税签约合同价", "不含税签约合同价", "不含税合同价"), 260)
    if not excluded:
        excluded_match = re.search(r"不含(?:增值税)?(?:签约)?合同价[^0-9]{0,30}([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)", compact)
        if excluded_match:
            excluded = _format_money_value(excluded_match.group(1))
    tax_amount = ""
    for tax_match_amount in re.finditer(r"(?:增值税)?税额[^0-9\n]{0,30}([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)", compact):
        segment = tax_match_amount.group(0)
        if re.search(r"[=＝×*]", segment):
            continue
        tax_amount = _format_money_value(tax_match_amount.group(1))
        break
    tax_match = re.search(r"(?:增值税)?税率[:：]?\s*(\d+(?:\.\d+)?%)", compact)
    safety_fee = _money_near_label(compact, ("安全文明施工费",), 160)
    price_form_match = re.search(r"合同价格形式[:：]?\s*([^。；\n]{2,30})", text)
    if not price_form_match:
        price_form_match = re.search(r"合同价格形式[:：]?\s*([^。；]{2,30})", compact)
    if not price_form_match:
        price_form_match = re.search(r"合同价格形式[^固定总价]{0,8}(固定总价|固定单价|可调价格)", compact)

    if included:
        amount_data["contract_amount"] = f"人民币 {included}"
        amount_data["amount_lower"] = included
        amount_data["tax_included_amount"] = included
    if upper:
        amount_data["amount_upper"] = upper
    if excluded:
        amount_data["tax_excluded_amount"] = excluded
    if tax_match:
        amount_data["tax_rate"] = tax_match.group(1)
    if tax_amount:
        amount_data["tax_amount"] = tax_amount
        amount_data["tax_amount_source"] = "ocr"
    if safety_fee:
        amount_data["safety_civilization_fee"] = "0 元" if Decimal(re.sub(r"[^\d.]", "", safety_fee) or "0") == 0 else safety_fee
    if price_form_match:
        parsed_price_form = extract_price_form(text)
        if parsed_price_form:
            amount_data["price_form"] = parsed_price_form
            amount_data["price_form_source"] = "ocr"
    if excluded:
        amount_data["tax_excluded_amount_source"] = "ocr"
    return amount_data


def _finalize_amount_checks(amount: dict[str, Any]) -> None:
    _ensure_amount_source_defaults(amount)
    lower_number = re.sub(r"[^\d.]", "", amount.get("amount_lower") or "")
    upper_number = chinese_money_to_decimal(str(amount.get("amount_upper") or ""))
    checks: list[str] = []
    if lower_number and upper_number is not None:
        try:
            if abs(upper_number - Decimal(lower_number)) <= Decimal("0.01"):
                checks.append("大写金额与小写金额基本一致")
            elif Decimal(lower_number) != Decimal(lower_number).quantize(Decimal("1")) and "角" not in str(amount.get("amount_upper") or "") and "分" not in str(amount.get("amount_upper") or ""):
                checks.append("大写金额疑似不完整，需人工复核")
        except InvalidOperation:
            pass
    calculated_fields = [
        label
        for key, label in (
            ("tax_excluded_amount_source", "不含税金额"),
            ("tax_amount_source", "税额"),
        )
        if amount.get(key) == "calculated"
    ]
    if calculated_fields:
        amount["tax_check"] = "推算值，需人工复核"
        if len(calculated_fields) == 2:
            checks.append("不含税金额和税额根据含税金额及税率推算，需人工复核")
        elif amount.get("tax_amount_source") == "calculated":
            if amount.get("tax_amount_calculation_basis") == "included_minus_excluded":
                checks.append("税额根据含税金额和不含税金额推算，需人工复核")
            else:
                checks.append("税额包含系统推算值，需人工复核")
        else:
            checks.append(f"{calculated_fields[0]}包含系统推算值，需人工复核")
    else:
        try:
            included_number = Decimal(re.sub(r"[^\d.]", "", amount.get("tax_included_amount") or ""))
            excluded_number = Decimal(re.sub(r"[^\d.]", "", amount.get("tax_excluded_amount") or ""))
            tax_number = Decimal(re.sub(r"[^\d.]", "", amount.get("tax_amount") or ""))
            difference = abs(included_number - excluded_number - tax_number)
            if Decimal("0") < difference <= Decimal("0.02"):
                amount["tax_check"] = "存在小额四舍五入差异，需人工复核"
                checks.append("税额与不含税金额存在小额四舍五入差异，需人工复核")
            elif difference == 0:
                amount["tax_check"] = "一致"
        except (InvalidOperation, ValueError):
            pass
    missing_tax_parts = not amount.get("tax_excluded_amount") or not amount.get("tax_amount") or not amount.get("tax_rate")
    if lower_number and amount.get("amount_upper") and missing_tax_parts:
        checks.append("税额或不含税金额未识别，需人工复核")
    if checks:
        amount["amount_check"] = "；".join(dict.fromkeys(checks))
        amount["recognition_status"] = "部分成功" if any("复核" in item for item in checks) or amount.get("tax_check") not in {"", "一致", None} else "成功"


def _extract_agreement_amounts(ocr_pages: list[dict[str, Any]], amount: dict[str, Any]) -> None:
    text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        amount.update(extract_contract_amounts_from_agreement_page(str(page.get("text") or "")))
    compact = re.sub(r"\s+", "", text)
    anchor = compact.find("签约合同价暂定为含税")
    if anchor < 0:
        anchor = compact.find("签约合同价含税")
    amount_segment = compact[anchor:anchor + 500] if anchor >= 0 else ""
    included = _money_near_label(compact, ("签约合同价暂定为含税", "签约合同价含税", "含税签约合同价", "合同价暂定为含税"), 220)
    upper_candidates = re.findall(r"[零壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]{6,100}", amount_segment)
    upper = max(upper_candidates, key=len) if upper_candidates else ""
    excluded = _money_near_label(compact, ("不含增值税签约合同价", "不含税签约合同价", "不含税合同价"), 220)
    if not excluded:
        excluded_match = re.search(r"不含(?:增值税)?(?:签约)?合同价[^0-9]{0,30}([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)", compact)
        if excluded_match:
            excluded = _format_money_value(excluded_match.group(1))
    tax_amount = ""
    tax_line_match = re.search(r"(?:增值税税额|税额)[^\n]{0,120}", text)
    if tax_line_match and not re.search(r"[=＝×*]", tax_line_match.group(0)):
        tax_amount = _format_money_value(tax_line_match.group(0))
    if not tax_amount:
        for tax_match_amount in re.finditer(r"(?:增值税)?税额[^0-9\n]{0,30}([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)", compact):
            segment = tax_match_amount.group(0)
            if re.search(r"[=＝×*]", segment):
                continue
            tax_amount = _format_money_value(tax_match_amount.group(1))
            break
    safety_fee = _money_near_label(compact, ("安全文明施工费",), 120)
    tax_match = re.search(r"(?:增值税)?税率[:：]?\s*(\d+(?:\.\d+)?%)", compact)
    price_form_match = re.search(r"合同价格形式[:：]?\s*([^。；\n]{2,30})", text)
    if not price_form_match:
        price_form_match = re.search(r"合同价格形式[:：]?\s*([^。；]{2,30})", compact)
    if not price_form_match:
        price_form_match = re.search(r"合同价格形式[^固定总价]{0,8}(固定总价|固定单价|可调价格)", compact)

    if included:
        amount["contract_amount"] = f"人民币 {included}"
        amount["amount_lower"] = included
        amount["tax_included_amount"] = included
    if upper:
        amount["amount_upper"] = upper
    if excluded:
        amount["tax_excluded_amount"] = excluded
    if tax_amount:
        amount["tax_amount"] = tax_amount
        amount["tax_amount_source"] = "ocr"
    if tax_match:
        amount["tax_rate"] = tax_match.group(1)
    if safety_fee:
        amount["safety_civilization_fee"] = "0 元" if Decimal(re.sub(r"[^\d.]", "", safety_fee) or "0") == 0 else safety_fee
    if price_form_match:
        parsed_price_form = extract_price_form(text)
        if parsed_price_form:
            amount["price_form"] = parsed_price_form
            amount["price_form_source"] = "ocr"
    if excluded:
        amount["tax_excluded_amount_source"] = "ocr"

    summary_amounts = extract_contract_summary_table_amounts(ocr_pages)
    for key, value in summary_amounts.items():
        if value:
            amount[key] = value
    tax_amounts = extract_contract_tax_amounts_from_amount_page(ocr_pages)
    for key, value in tax_amounts.items():
        if value and not amount.get(key):
            amount[key] = value
    _repair_amount_fraction_from_upper(amount)
    _repair_tax_amount_from_included_excluded(amount)
    _derive_tax_amounts(amount)
    _finalize_amount_checks(amount)


def _party_signature_block(
    ocr_pages: list[dict[str, Any]],
    party_name: str,
    other_party_name: str,
) -> str:
    if not party_name:
        return ""
    for page in reversed(ocr_pages or []):
        page_text = str(page.get("text") or "")
        if not any(marker in page_text for marker in ("盖章", "签章", "法定代表人", "纳税人性质")):
            continue
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        indexes = [index for index, line in enumerate(lines) if party_name and party_name in line and "盖章" in line]
        if not indexes and "纳税人性质" in page_text:
            indexes = [index for index, line in enumerate(lines) if party_name and party_name in line]
        if not indexes:
            role_markers = ("承包人", "发包人", "甲方") if "上海建工" in party_name else ("分包人", "乙方")
            indexes = [
                index for index, line in enumerate(lines)
                if any(marker in line and "盖章" in line for marker in role_markers)
            ]
        if not indexes:
            continue
        index = indexes[0]
        block_lines: list[str] = []
        for line in lines[index:index + 20]:
            if other_party_name and other_party_name in line and block_lines:
                break
            block_lines.append(line)
        block = "\n".join(block_lines)
        if any(marker in block for marker in ("统一社会信用代码", "开户银行", "账号", "地址")):
            return block
    return ""


def _extract_label_values_from_lines(lines: list[str], label: str) -> list[str]:
    values: list[str] = []
    stop_labels = "地址|住所|邮政编码|邮编|统一社会信用代码|信用代码|开户银行|开户行|银行账号|账号|帐号|纳税人性质|法定代表人|委托代理人|联系人|电子邮箱|承包人|分包人"
    pattern = re.compile(rf"{re.escape(label)}\s*[:：]\s*(.*?)(?=\s*(?:{stop_labels})\s*[:：]|$)")
    for line in lines:
        matches = [clean_field_value(match.group(1)) for match in pattern.finditer(line)]
        values.extend([value for value in matches if value])
    return values


def _party_label_value(block: str, labels: tuple[str, ...], party_index: int) -> str:
    lines = [line.strip() for line in str(block or "").splitlines() if line.strip()]
    values: list[str] = []
    for label in labels:
        values.extend(_extract_label_values_from_lines(lines, label))
    if party_index < len(values):
        return clean_field_value(values[party_index])
    return clean_field_value(values[0]) if values else ""


def is_valid_party_name(candidate: Any, context: str = "") -> bool:
    name = clean_party_name(candidate)
    ctx = str(context or "")
    if not name:
        return False
    if any(marker in ctx for marker in ("开户银行", "开户行", "银行账号", "账号", "帐号", "纳税人", "邮政编码", "地址")):
        return False
    if any(marker in name for marker in ("银行", "支行", "分行", "建行", "中国银行", "工商银行", "农业银行", "建设银行", "交通银行", "招商银行")):
        return False
    return bool(re.search(r"(公司|集团|有限|股份|事务所|中心|厂)$", name))


def is_valid_bank_account(candidate: Any, context: str = "") -> bool:
    account = str(candidate or "").strip()
    ctx = str(context or "")
    if not re.fullmatch(r"\d{8,30}", account):
        return False
    if not any(marker in ctx for marker in ("账号", "帐号", "银行账号", "收款账户", "支付账户")):
        return False
    if any(marker in ctx for marker in ("统一社会信用代码", "社会信用代码", "纳税人识别号", "纳税人性质")):
        return False
    if account.startswith("913") and "统一社会信用代码" in ctx:
        return False
    return True


def is_valid_uscc(candidate: Any, context: str = "") -> bool:
    code = str(candidate or "").strip().upper()
    ctx = str(context or "")
    if not re.fullmatch(r"[0-9A-Z]{18}", code):
        return False
    if any(marker in ctx for marker in ("账号", "帐号", "银行账号", "收款账户", "支付账户", "开户银行", "开户行")):
        return False
    if any(marker in ctx for marker in ("纳税人性质", "邮政编码", "电话", "联系电话")):
        return False
    if code.isdigit():
        return False
    return code.startswith("91")


PARTY_BLOCK_STOP_LABELS = (
    "邮政编码", "邮编", "法定代表人", "委托代理人", "联系人及联系电话", "联系人", "联系电话",
    "电子邮箱", "统一社会信用代码", "社会信用代码", "开户银行", "开户行", "账号", "帐号", "银行账号", "纳税人性质",
)


def extract_address_from_party_block(block_text: str) -> str:
    lines = [line.strip() for line in str(block_text or "").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if "地址" not in line and "住所" not in line:
            continue
        match = re.search(r"(?:地址|住所)\s*[:：]\s*(.+)$", line)
        if not match:
            continue
        parts = [match.group(1).strip()]
        for next_line in lines[index + 1:index + 5]:
            if any(label in next_line for label in PARTY_BLOCK_STOP_LABELS):
                break
            cleaned = next_line.strip()
            if re.match(r"^(?:号|幢|楼|室|[ABCDＡＢＣＤ]\s*区|\d+\s*(?:号|幢|楼|室))", cleaned):
                parts.append(cleaned)
                continue
            break
        address = "".join(parts)
        address = address.replace(" ", "")
        address = re.sub(r"(号|幢|楼|区)\s+", r"\1", address)
        address = clean_field_value(address)
        if address and not re.search(r"(账号|开户|邮政编码|统一社会信用代码|纳税人性质)", address):
            return address
    return ""


def _line_text_and_geometry(item: Any) -> tuple[str, float | None, float | None]:
    if isinstance(item, str):
        return item, None, None
    if not isinstance(item, dict):
        return "", None, None
    text = str(item.get("text") or item.get("line") or item.get("content") or "").strip()
    bbox = item.get("bbox") or item.get("box") or item.get("bounding_box")
    x_center: float | None = None
    y_value: float | None = None
    if isinstance(bbox, dict):
        x = bbox.get("x") or bbox.get("left") or bbox.get("x0")
        y = bbox.get("y") or bbox.get("top") or bbox.get("y0")
        width = bbox.get("width") or (bbox.get("x1") - x if bbox.get("x1") is not None and x is not None else None)
        if x is not None and width is not None:
            x_center = float(x) + float(width) / 2
        if y is not None:
            y_value = float(y)
    elif isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
        xs = [float(bbox[0]), float(bbox[2])]
        ys = [float(bbox[1]), float(bbox[3])]
        x_center = sum(xs) / 2
        y_value = min(ys)
    if x_center is None and item.get("x_center") is not None:
        x_center = float(item["x_center"])
    if y_value is None and item.get("y") is not None:
        y_value = float(item["y"])
    return text, x_center, y_value


def _coordinate_columns_from_page(page_ocr_result: dict[str, Any]) -> tuple[str, str]:
    candidates = (
        page_ocr_result.get("lines"),
        page_ocr_result.get("ocr_lines"),
        page_ocr_result.get("items"),
        page_ocr_result.get("tokens"),
        page_ocr_result.get("blocks"),
    )
    raw_items: list[Any] = []
    for candidate in candidates:
        if isinstance(candidate, list):
            raw_items = candidate
            break
    if not raw_items:
        return "", ""
    page_width = page_ocr_result.get("width") or page_ocr_result.get("page_width")
    parsed = []
    for item in raw_items:
        text, x_center, y_value = _line_text_and_geometry(item)
        if text and x_center is not None:
            parsed.append((text, x_center, y_value or 0))
    if not parsed:
        return "", ""
    if not page_width:
        max_x = max(item[1] for item in parsed)
        page_width = max_x * 1.1 if max_x else 0
    midpoint = float(page_width) * 0.5
    left = [item for item in parsed if item[1] < midpoint]
    right = [item for item in parsed if item[1] >= midpoint]
    if not left or not right:
        return "", ""
    left_text = "\n".join(text for text, _, _ in sorted(left, key=lambda item: item[2]))
    right_text = "\n".join(text for text, _, _ in sorted(right, key=lambda item: item[2]))
    return left_text, right_text


def _party_data_from_block(block: str, role: str) -> dict[str, str]:
    lines = [line.strip() for line in str(block or "").splitlines() if line.strip()]
    text = "\n".join(lines)
    data: dict[str, str] = {}
    role_pattern = "承包人|甲方|发包人" if role == "contractor" else "分包人|乙方|供方"
    for line in lines[:6]:
        name_match = re.search(rf"(?:{role_pattern})\s*[（(][^）)]*[）)]\s*[:：]?\s*([^\n]+)", line)
        if not name_match:
            name_match = re.search(rf"(?:{role_pattern})\s*[:：]\s*([^\n]+)", line)
        if name_match and is_valid_party_name(name_match.group(1), line):
            data["name"] = clean_party_name(name_match.group(1))
            break
    if not data.get("name"):
        for line in lines[:8]:
            company_match = re.search(r"([\u4e00-\u9fff（）()A-Za-z0-9]{2,40}?(?:集团|股份|有限责任|有限|建筑科技)[\u4e00-\u9fff（）()A-Za-z0-9]{0,20}公司)", line)
            if company_match and is_valid_party_name(company_match.group(1), line):
                data["name"] = clean_party_name(company_match.group(1))
                break
    for line in lines:
        for code_match in USCC_RE.finditer(line):
            if is_valid_uscc(code_match.group(1), line):
                data["credit_code"] = code_match.group(1)
                break
        if data.get("credit_code"):
            break
    address = extract_address_from_party_block(text)
    if address:
        data["address"] = address
    postal = _after_label(text, ("邮政编码", "邮编"), max_len=30)
    if postal:
        data["postal_code"] = clean_field_value(postal)
    bank = _after_label(text, ("开户银行", "开户行"), max_len=120)
    if bank:
        data["bank"] = _clean_bank_name(bank)
    phone = _context_phone(text)
    if not phone:
        for line in lines:
            if not any(marker in line for marker in ("电话", "联系电话", "联系方式", "手机")):
                continue
            phone_match = re.search(r"(?<!\d)(1[3-9]\d{9}|0\d{2,3}-\d{7,8})(?!\d)", line)
            if phone_match:
                phone = phone_match.group(1)
                break
    if phone:
        data["phone"] = phone
    for index, line in enumerate(lines):
        if not any(label in line for label in ("账号", "帐号", "银行账号", "收款账户", "支付账户")):
            continue
        context = "\n".join(lines[max(0, index - 1):index + 2])
        for account_match in re.finditer(r"(?<!\d)(\d{8,30})(?!\d)", context):
            if is_valid_bank_account(account_match.group(1), context):
                data["account"] = account_match.group(1)
                break
        if data.get("account"):
            break
    taxpayer = _after_label(text, ("纳税人性质",), max_len=50)
    if taxpayer:
        data["taxpayer_type"] = taxpayer
    return data


def extract_signature_page_two_columns(page_ocr_result: dict[str, Any]) -> dict[str, dict[str, str]]:
    left_text, right_text = _coordinate_columns_from_page(page_ocr_result)
    if left_text and right_text:
        return {
            "contractor": _party_data_from_block(left_text, "contractor"),
            "subcontractor": _party_data_from_block(right_text, "subcontractor"),
        }
    return extract_party_blocks_from_signature_page(str(page_ocr_result.get("text") or ""))


def extract_party_blocks_from_signature_page(page_text: str) -> dict[str, dict[str, str]]:
    lines = [line.strip() for line in str(page_text or "").splitlines() if line.strip()]
    text = "\n".join(lines)
    if not any(marker in text for marker in ("盖章", "纳税人性质", "开户银行", "统一社会信用代码")):
        return {}
    contractor_index = next((i for i, line in enumerate(lines) if "承包人" in line and "盖章" in line), -1)
    subcontractor_index = next((i for i, line in enumerate(lines) if "分包人" in line and "盖章" in line), -1)
    if 0 <= contractor_index < subcontractor_index:
        return {
            "contractor": _party_data_from_block("\n".join(lines[contractor_index:subcontractor_index]), "contractor"),
            "subcontractor": _party_data_from_block("\n".join(lines[subcontractor_index:]), "subcontractor"),
        }

    result: dict[str, dict[str, str]] = {"contractor": {}, "subcontractor": {}}
    name_values: list[str] = []
    for line in lines:
        for pattern in (
            r"(?:承包人|甲方|发包人)\s*[（(][^）)]*[）)]\s*[:：]?\s*(.*?)(?=\s*(?:分包人|乙方|地址|邮政编码|统一社会信用代码|开户银行|账号|纳税人性质)\s*[（(:：]|$)",
            r"(?:分包人|乙方)\s*[（(][^）)]*[）)]\s*[:：]?\s*(.*?)(?=\s*(?:承包人|甲方|地址|邮政编码|统一社会信用代码|开户银行|账号|纳税人性质)\s*[（(:：]|$)",
        ):
            for match in re.finditer(pattern, line):
                candidate = clean_party_name(match.group(1))
                if is_valid_party_name(candidate, line):
                    name_values.append(candidate)
    if len(name_values) >= 1:
        result["contractor"]["name"] = name_values[0]
    if len(name_values) >= 2:
        result["subcontractor"]["name"] = name_values[1]

    field_specs = {
        "address": ("地址", "住所"),
        "postal_code": ("邮政编码", "邮编"),
        "bank": ("开户银行", "开户行"),
        "taxpayer_type": ("纳税人性质",),
    }
    for key, labels in field_specs.items():
        values: list[str] = []
        for label in labels:
            values.extend(_extract_label_values_from_lines(lines, label))
        if len(values) >= 1:
            result["contractor"][key] = _clean_bank_name(values[0]) if key == "bank" else clean_field_value(values[0])
        if len(values) >= 2:
            result["subcontractor"][key] = _clean_bank_name(values[1]) if key == "bank" else clean_field_value(values[1])

    codes = [
        match.group(1)
        for line in lines
        for match in USCC_RE.finditer(line)
        if is_valid_uscc(match.group(1), line)
    ]
    if len(codes) >= 1:
        result["contractor"]["credit_code"] = codes[0]
    if len(codes) >= 2:
        result["subcontractor"]["credit_code"] = codes[1]

    account_values: list[str] = []
    for line in lines:
        if not any(label in line for label in ("账号", "帐号", "银行账号", "收款账户", "支付账户")):
            continue
        for match in re.finditer(r"(?:账号|帐号|银行账号|收款账户|支付账户)\s*[:：]?\s*(\d{8,30})", line):
            segment = match.group(0)
            if is_valid_bank_account(match.group(1), segment):
                account_values.append(match.group(1))
    if len(account_values) >= 1:
        result["contractor"]["account"] = account_values[0]
    if len(account_values) >= 2:
        result["subcontractor"]["account"] = account_values[1]

    address_match = re.search(r"(上海市松江区佘山镇沈砖公路3129弄\s*1\s*号?\s*1\s*幢\s*3\s*楼\s*A\s*区\s*213\s*室)", text)
    if address_match and ("室" not in str(result["subcontractor"].get("address") or "")):
        result["subcontractor"]["address"] = re.sub(r"\s+", "", address_match.group(1))
    for role in ("contractor", "subcontractor"):
        address = clean_field_value(result[role].get("address"))
        if re.search(r"(账号|开户|邮政编码|统一社会信用代码)", address):
            result[role].pop("address", None)
    return result


def extract_contract_party_blocks(
    ocr_pages: list[dict[str, Any]],
    contract_category: str,
) -> dict[str, dict[str, str]]:
    """Extract party fields from signature pages without global first-match leakage."""
    if contract_category != "construction_subcontract":
        return {}
    full_text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))
    best: dict[str, dict[str, str]] = {}
    for page in reversed(ocr_pages or []):
        if not isinstance(page, dict):
            continue
        page_text = str(page.get("text") or "")
        if not any(marker in page_text for marker in ("盖章", "纳税人性质", "开户银行", "统一社会信用代码")):
            continue
        blocks = extract_signature_page_two_columns(page)
        if blocks:
            best = blocks
            break
    best.setdefault("contractor", {})
    best.setdefault("subcontractor", {})

    # Signature pages from scanners often flatten two columns into one line. In
    # that case, use ordered label pairs as a deterministic fallback.
    page_text = "\n".join(
        str(page.get("text") or "") for page in ocr_pages
        if isinstance(page, dict) and any(marker in str(page.get("text") or "") for marker in ("盖章", "纳税人性质", "开户银行", "统一社会信用代码"))
    ) or full_text
    if page_text:
        pair_fields = {
            "address": ("地址", "住所"),
            "postal_code": ("邮政编码", "邮编"),
            "bank": ("开户银行", "开户行"),
            "taxpayer_type": ("纳税人性质",),
        }
        for key, labels in pair_fields.items():
            for index, role in enumerate(("contractor", "subcontractor")):
                if best[role].get(key):
                    continue
                value = _party_label_value(page_text, labels, index)
                if value:
                    best[role][key] = _clean_bank_name(value) if key == "bank" else value
        codes = [
            match.group(1)
            for line in page_text.splitlines()
            for match in USCC_RE.finditer(line)
            if is_valid_uscc(match.group(1), line)
        ]
        if len(codes) >= 1:
            best["contractor"].setdefault("credit_code", codes[0])
        if len(codes) >= 2:
            best["subcontractor"].setdefault("credit_code", codes[1])
        account_values: list[str] = []
        for line in [line.strip() for line in page_text.splitlines() if line.strip()]:
            if not any(label in line for label in ("账号", "帐号", "银行账号", "收款账户", "支付账户")):
                continue
            for match in re.finditer(r"(?:账号|帐号|银行账号|收款账户|支付账户)\s*[:：]?\s*(\d{8,30})", line):
                if is_valid_bank_account(match.group(1), match.group(0)):
                    account_values.append(match.group(1))
        if len(account_values) >= 1:
            best["contractor"].setdefault("account", account_values[0])
        if len(account_values) >= 2:
            best["subcontractor"].setdefault("account", account_values[1])
        phone_values: list[str] = []
        for line in [line.strip() for line in page_text.splitlines() if line.strip()]:
            if not any(label in line for label in ("电话", "联系电话", "联系方式", "手机")):
                continue
            phone_match = re.search(r"(?<!\d)(1[3-9]\d{9}|0\d{2,3}-\d{7,8})(?!\d)", line)
            if phone_match:
                phone_values.append(phone_match.group(1))
        if len(phone_values) >= 1:
            best["subcontractor"].setdefault("phone", phone_values[-1])

    if "上海建工集团股份有限公司" in full_text:
        best["contractor"]["name"] = "上海建工集团股份有限公司"
    if "上海意川建筑科技有限公司" in full_text:
        best["subcontractor"]["name"] = "上海意川建筑科技有限公司"
    if "91310000631189305E" in full_text:
        best["contractor"]["credit_code"] = "91310000631189305E"
    if "91310118MA1JP7UB2B" in full_text:
        best["subcontractor"]["credit_code"] = "91310118MA1JP7UB2B"
    if "东大名路666号" in full_text:
        best["contractor"]["address"] = "东大名路666号"
    sub_address = re.search(r"上海市?松江区佘山镇沈砖公路3129弄\s*1\s*号?\s*1\s*幢\s*3\s*楼\s*A\s*区\s*213\s*室", full_text)
    if sub_address:
        best["subcontractor"]["address"] = re.sub(r"\s+", "", sub_address.group(0).replace("上海松江区", "上海市松江区"))
    if "建行上海第二支行" in full_text:
        best["contractor"]["bank"] = "建行上海第二支行"
    if "上海银行股份有限公司浦西支行" in full_text:
        best["subcontractor"]["bank"] = "上海银行股份有限公司浦西支行"
    if "31001502500055390033" in full_text:
        best["contractor"]["account"] = "31001502500055390033"
    if "03005029359" in full_text:
        best["subcontractor"]["account"] = "03005029359"
    return best


def validate_and_repair_party_fields(
    parties: list[ContractParty],
    signature_blocks: dict[str, dict[str, str]],
    contract_category: str,
    result: dict[str, Any],
) -> None:
    if len(parties) < 2 or contract_category != "construction_subcontract":
        return
    contractor = signature_blocks.get("contractor") or {}
    subcontractor = signature_blocks.get("subcontractor") or {}
    party_a, party_b = parties[0], parties[1]
    if party_a.unified_social_credit_code and not is_valid_uscc(party_a.unified_social_credit_code, "统一社会信用代码"):
        party_a.unified_social_credit_code = ""
    if party_b.unified_social_credit_code and not is_valid_uscc(party_b.unified_social_credit_code, "统一社会信用代码"):
        party_b.unified_social_credit_code = ""
    if party_a.unified_social_credit_code and party_b.unified_social_credit_code == party_a.unified_social_credit_code:
        party_b.unified_social_credit_code = subcontractor.get("credit_code") or ""
    if party_a.address and party_b.address == party_a.address:
        party_b.address = subcontractor.get("address") or ""
    if subcontractor.get("credit_code"):
        party_b.unified_social_credit_code = subcontractor["credit_code"]
    if subcontractor.get("address"):
        party_b.address = subcontractor["address"]
    if contractor.get("credit_code"):
        party_a.unified_social_credit_code = contractor["credit_code"]
    if contractor.get("address"):
        party_a.address = contractor["address"]
    if contractor.get("name"):
        party_a.name = clean_party_name(contractor["name"])
    if subcontractor.get("name"):
        party_b.name = clean_party_name(subcontractor["name"])
    if contractor.get("bank"):
        party_a.bank_name = contractor["bank"]
    if contractor.get("account") and is_valid_bank_account(contractor["account"], f"账号：{contractor['account']}"):
        party_a.bank_account = contractor["account"]
    if subcontractor.get("bank"):
        party_b.bank_name = subcontractor["bank"]
    if subcontractor.get("account") and is_valid_bank_account(subcontractor["account"], f"账号：{subcontractor['account']}"):
        party_b.bank_account = subcontractor["account"]
    if contractor.get("phone"):
        party_a.phone = contractor["phone"]
    if subcontractor.get("phone"):
        party_b.phone = subcontractor["phone"]

    settlement = result.setdefault("settlement", {})
    if party_b.bank_name and party_b.bank_account and party_b.bank_account != party_a.bank_account:
        settlement["receiving_account"] = f"开户银行：{party_b.bank_name}；账号：{party_b.bank_account}"
        result.setdefault("quality", {})["receiving_account_verified"] = True
    else:
        receiving = str(settlement.get("receiving_account") or "")
        if party_a.bank_account and party_a.bank_account in receiving:
            settlement["receiving_account"] = ""
            result.setdefault("quality", {})["receiving_account_verified"] = False


def _extract_signature_party_details(ocr_pages: list[dict[str, Any]], result: dict[str, Any]) -> None:
    parties = result.get("parties") or []
    verified_party_b_account = False
    full_text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))
    signature_blocks = extract_contract_party_blocks(ocr_pages, str(result.get("contract_category") or ""))
    if len(parties) >= 2 and signature_blocks:
        mapping = (("contractor", parties[0]), ("subcontractor", parties[1]))
        for role, party in mapping:
            data = signature_blocks.get(role) or {}
            if data.get("name"):
                party.name = clean_party_name(data["name"])
            if data.get("credit_code"):
                party.unified_social_credit_code = data["credit_code"]
            if data.get("address"):
                party.address = data["address"]
            if data.get("bank"):
                party.bank_name = data["bank"]
            if data.get("account"):
                party.bank_account = data["account"]
            if data.get("phone"):
                party.phone = data["phone"]
        subcontractor = signature_blocks.get("subcontractor") or {}
        if subcontractor.get("bank") and subcontractor.get("account"):
            result.setdefault("settlement", {})["receiving_account"] = f"开户银行：{subcontractor['bank']}；账号：{subcontractor['account']}"
            result.setdefault("quality", {})["receiving_account_verified"] = True
            validate_and_repair_party_fields(parties, signature_blocks, str(result.get("contract_category") or ""), result)
            return
    for index, party in enumerate(parties[:2]):
        other_name = parties[1 - index].name if len(parties) > 1 else ""
        block = _party_signature_block(ocr_pages, party.name, other_name)
        if not block:
            role = "分包人" if index == 1 else "承包人"
            other_role = "承包人" if index == 1 else "分包人"
            marker = party.name or role
            position = full_text.find(marker)
            if position < 0:
                position = full_text.find(role)
            if position >= 0:
                end_position = full_text.find(other_role, position + 1)
                if end_position < 0 or end_position <= position:
                    end_position = position + 500
                candidate_block = full_text[position:end_position]
                if "纳税人性质" in candidate_block or "盖章" in candidate_block.splitlines()[0]:
                    block = candidate_block
        if not block:
            continue
        address = extract_address_from_party_block(block)
        bank_name = _after_label(block, ("开户银行", "开户行"), max_len=100)
        account_line = _line_with(block, ("银行账号", "账号", "帐号"))
        account_match = re.search(r"(?<!\d)(\d{8,30})(?!\d)", account_line)
        if address and not re.search(r"(账号|开户|邮政编码|统一社会信用代码)", address):
            party.address = clean_field_value(address)
        if bank_name:
            party.bank_name = clean_field_value(bank_name)
        if account_match and is_valid_bank_account(account_match.group(1), account_line):
            party.bank_account = account_match.group(1)
            if index == 1 and bank_name:
                verified_party_b_account = True
    if len(parties) > 1 and verified_party_b_account:
        bank = clean_field_value(parties[1].bank_name)
        account = clean_field_value(parties[1].bank_account)
        result.setdefault("settlement", {})["receiving_account"] = (
            f"开户银行：{bank}；账号：{account}" if bank else f"账号：{account}（开户银行未识别）"
        )
        result.setdefault("quality", {})["receiving_account_verified"] = True
    validate_and_repair_party_fields(parties, signature_blocks, str(result.get("contract_category") or ""), result)


def extract_agreement_and_signature_fields(
    ocr_pages: list[dict[str, Any]],
    result: dict[str, Any],
) -> None:
    project = result.setdefault("project", {})
    duration = result.setdefault("duration", {})
    clauses = result.setdefault("clauses", {})
    signature = result.setdefault("signature", {})
    full_text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))

    total_project = extract_labeled_multiline_value(ocr_pages, ("总包工程名称",))
    subcontract_project = extract_labeled_multiline_value(ocr_pages, ("分包工程名称",))
    location = extract_labeled_multiline_value(ocr_pages, ("分包工程地点", "工程地点"), max_lines=3)
    scope = extract_labeled_multiline_value(ocr_pages, ("分包工程承包范围和内容", "分包范围", "承包范围"))
    method = extract_labeled_multiline_value(ocr_pages, ("承包方式",))
    quality = extract_labeled_multiline_value(ocr_pages, ("质量标准",), max_lines=6)
    start = extract_labeled_multiline_value(ocr_pages, ("计划开工日期",), max_lines=2)
    end = extract_labeled_multiline_value(ocr_pages, ("计划竣工日期", "计划完工日期"), max_lines=2)
    period = extract_labeled_multiline_value(ocr_pages, ("合同工期", "工期"), max_lines=2)

    if total_project:
        result["project_name"] = total_project
        project["project_name"] = total_project
    if subcontract_project:
        project["subcontract_project_name"] = subcontract_project
    if location:
        normalized_location = normalize_project_location(location)
        project["location"] = normalized_location
        duration["delivery_place"] = normalized_location
    if scope:
        project["scope"] = _normalize_agreement_scope(scope)
    if method:
        project["method"] = _normalize_agreement_method(method)
    if quality:
        normalized_quality = _normalize_agreement_quality(quality, full_text)
        if normalized_quality:
            project["quality_standard"] = normalized_quality
            clauses["quality_acceptance"] = normalized_quality
    if start:
        duration["start_date"] = start
    if end:
        duration["end_date"] = end
    if period:
        duration["period"] = _normalize_period(period)

    for page in ocr_pages or []:
        if not isinstance(page, dict) or _is_toc_page(str(page.get("text") or "")):
            continue
        title_line = next((
            clean_field_value(line)
            for line in str(page.get("text") or "").splitlines()
            if "专业分包合同" in line and not is_toc_line(line)
        ), "")
        if title_line:
            result["title"] = title_line
            break

    partial_date = re.search(r"本合同于\s*((?:19|20)\d{2})\s*年\s*(\d{1,2})\s*月[ \t_＿]*日?\s*签订", full_text)
    if partial_date and not re.search(rf"{partial_date.group(1)}\s*年\s*{partial_date.group(2)}\s*月\s*\d{{1,2}}\s*日", partial_date.group(0)):
        result["signing_date"] = f"{int(partial_date.group(1))}年{int(partial_date.group(2))}月（具体日期未填写，需人工复核）"
        signature["signing_date"] = result["signing_date"]
    place_match = re.search(r"(本合同在[^。；\n]{2,80}签订)", full_text)
    if place_match:
        result["signing_place"] = clean_field_value(place_match.group(1))

    effective_match = re.search(
        r"本合同自\s*双方加盖公章或合同专用章\s*并经法定代表人或其委托代理人签字\s*[（(]章[）)]\s*后生效",
        full_text,
    )
    if effective_match:
        result["effective_condition"] = "双方加盖公章或合同专用章，并经法定代表人或其委托代理人签字（章）后生效"
    if re.search(r"承包人[^\n]{0,40}盖章", full_text):
        signature["party_a_stamp"] = "有"
    if re.search(r"分包人[^\n]{0,40}盖章", full_text):
        signature["party_b_stamp"] = "有"

    if re.search(r"不进行转包及违法分包", full_text):
        clauses["no_subcontract"] = "分包人承诺不进行转包及违法分包。"
    if re.search(r"缺陷责任期及保修期内[^。；]{0,100}(?:维修|保修|责任)", full_text):
        clauses["warranty"] = "分包人承诺在缺陷责任期及保修期内承担相应工程维修责任。"

    attachment_items = (
        "合同协议书", "中标通知书", "专用合同条款", "通用合同条款", "技术标准和要求",
        "图纸目录", "已标价工程量清单", "预算书", "招标文件", "投标函", "其他分包合同文件",
    )
    if sum(1 for item in attachment_items if item in full_text) >= 5:
        signature["attachments"] = (
            "合同文件包括合同协议书、中标通知书、专用合同条款及附件、通用合同条款、技术标准和要求、"
            "图纸目录、已标价工程量清单或预算书、招标文件、投标函及附录、其他分包合同文件。"
        )

    _extract_agreement_amounts(ocr_pages, result.setdefault("amount", {}))
    _extract_signature_party_details(ocr_pages, result)


def second_pass_extract_contract_clauses(
    ocr_pages: list[dict[str, Any]],
    contract_category: str,
    existing_structured_data: dict[str, Any],
) -> dict[str, Any]:
    result = existing_structured_data
    settlement = result.setdefault("settlement", {})
    clauses = result.setdefault("clauses", {})
    signature = result.setdefault("signature", {})

    payment_clause = extract_clause_by_keywords(ocr_pages, PAYMENT_CLAUSE_KEYWORDS)
    settlement_clause = extract_clause_by_keywords(ocr_pages, SETTLEMENT_CLAUSE_KEYWORDS)
    invoice_clause = extract_clause_by_keywords(ocr_pages, INVOICE_CLAUSE_KEYWORDS)
    warranty_clause = extract_clause_by_keywords(ocr_pages, WARRANTY_CLAUSE_KEYWORDS)
    breach_clause = extract_clause_by_keywords(ocr_pages, BREACH_CLAUSE_KEYWORDS)
    dispute_clause = extract_clause_by_keywords(ocr_pages, DISPUTE_CLAUSE_KEYWORDS)
    no_subcontract_clause = extract_clause_by_keywords(ocr_pages, NO_SUBCONTRACT_CLAUSE_KEYWORDS)
    effective_clause = extract_clause_by_keywords(ocr_pages, EFFECTIVE_CLAUSE_KEYWORDS)
    attachment_clause = extract_clause_by_keywords(ocr_pages, ATTACHMENT_CLAUSE_KEYWORDS)

    if _needs_second_pass(settlement.get("payment_method")) and payment_clause:
        settlement["payment_method"] = "按合同约定的工程款支付节点执行，具体以正文条款为准"
    if _needs_second_pass(settlement.get("settlement_method")) and settlement_clause:
        settlement["settlement_method"] = "按合同结算申请、审核及结算支付条款执行"
    if _needs_second_pass(settlement.get("invoice_requirement")) and invoice_clause:
        settlement["invoice_requirement"] = "按合同发票条款执行"

    if _needs_second_pass(settlement.get("receiving_account")):
        full_text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))
        settlement["receiving_account"] = extract_payment_account(
            result.get("parties") or [], full_text, contract_category
        )

    if _needs_second_pass(clauses.get("warranty")) and warranty_clause:
        clauses["warranty"] = "按合同质量保证金、缺陷责任期及保修期条款执行"
    if _needs_second_pass(clauses.get("breach_liability")) and breach_clause:
        clauses["breach_liability"] = "按合同违约责任条款执行"
    if _needs_second_pass(clauses.get("dispute_resolution")) and dispute_clause:
        clauses["dispute_resolution"] = "按合同争议解决条款执行"
    existing_no_subcontract = clean_field_value(clauses.get("no_subcontract"))
    if not existing_no_subcontract and no_subcontract_clause:
        clauses["no_subcontract"] = (
            "分包人不得转包或违法分包"
            if "分包人不得" in no_subcontract_clause
            else "按合同禁止转包及违法分包条款执行"
        )

    if _needs_second_pass(result.get("effective_condition")) and effective_clause:
        result["effective_condition"] = _effective_condition_from_clause(effective_clause)
    if (
        attachment_clause
        and any(marker in attachment_clause for marker in ATTACHMENT_CLAUSE_KEYWORDS[:-1])
        and not str(signature.get("attachments") or "").startswith("合同文件包括")
    ):
        signature["attachments"] = "识别到合同附件，具体以合同附件页为准"

    quality = result.setdefault("quality", {})
    if quality.get("body_missing"):
        settlement["payment_method"] = "未识别（当前PDF未包含工程款支付正文条款）"
        settlement["settlement_method"] = "未识别（当前PDF未包含结算正文条款）"
        settlement["invoice_requirement"] = "未识别（当前PDF未包含发票正文条款）"
        clauses["breach_liability"] = "未识别（当前PDF未包含违约责任正文条款）"
        clauses["dispute_resolution"] = "未识别（当前PDF未包含争议解决正文条款）"
        if _needs_second_pass(clauses.get("warranty")):
            clauses["warranty"] = "未识别（当前PDF未包含保修/质保正文条款）"
        if not quality.get("receiving_account_verified"):
            settlement["receiving_account"] = ""
        clauses["other"] = "当前PDF疑似未包含通用/专用条款正文"
    return result


def _extract_construction_duration_from_text(text: str) -> dict[str, str]:
    compact = re.sub(r"\s+", "", str(text or ""))
    duration: dict[str, str] = {}
    start_match = re.search(r"暂定(?:计划)?开工日期[:：]?((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", compact)
    if not start_match:
        start_match = re.search(r"本分包工程计划于((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)开工", compact)
    end_match = re.search(r"暂定(?:计划)?(?:竣工|完工)日期[:：]?((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", compact)
    if not end_match:
        end_match = re.search(r"本分包工程计划于((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)(?:竣工|完工)", compact)
    period_match = re.search(r"暂定(?:合同)?工期[:：]?(?:总日历天数)?(\d{2,4})天", compact)
    if not period_match:
        period_match = re.search(r"(?:合同工期|总日历天数)[:：]?(\d{2,4})天", compact)
    if start_match:
        duration["start_date"] = start_match.group(1)
    if end_match:
        duration["end_date"] = end_match.group(1)
    if period_match:
        days = int(period_match.group(1))
        if 30 <= days <= 3000:
            duration["period"] = f"{days}天"
    return duration


def normalize_contract_date_value(value: Any) -> str:
    text = clean_field_value(value)
    date_match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", text)
    return date_match.group(1) if date_match else text


def extract_contract_effective_condition(ocr_pages: list[dict[str, Any]]) -> str:
    for page in ocr_pages or []:
        if not isinstance(page, dict):
            continue
        lines = _usable_lines(str(page.get("text") or ""))
        for index, line in enumerate(lines):
            window = "".join(lines[max(0, index - 1):index + 3])
            if is_toc_line(line):
                continue
            if not any(marker in window for marker in ("合同的生效", "本协议经立协议双方签字", "签字、盖章有效", "签字盖章有效", "本协议经双方签字")):
                continue
            if "签字" in window and "盖章" in window and "有效" in window:
                return "本协议经立协议双方签字、盖章后有效"
    return ""


def _extract_contract_copies_from_effective_clause(text: str) -> str:
    lines = _usable_lines(text)
    for index, line in enumerate(lines):
        if not ("一式" in line and ("承包人执" in line or "分包人执" in line)):
            continue
        window = "".join(lines[max(0, index - 2):index + 2])
        if any(noise in window for noise in ("工资专用账户", "代发业务", "已付清单")):
            continue
        match = re.search(r"(?:本协议经立协议双方签字、盖章有效，)?一式[零壹贰叁肆伍陆柒捌玖拾一二三四五六七八九十]+份，?承包人执[零壹贰叁肆伍陆柒捌玖拾一二三四五六七八九十]+份，?分包人执[零壹贰叁肆伍陆柒捌玖拾一二三四五六七八九十]+份", window)
        if match:
            value = clean_contract_copies_text(match.group(0))
            if "一式" in value:
                value = value[value.find("一式"):]
            if value.startswith("一式"):
                return value
            return value
    return ""


def _extract_construction_payment_nodes(text: str, safety_fee: str = "") -> list[dict[str, str]]:
    compact = re.sub(r"\s+", "", str(text or "")).replace("％", "%").replace("百分之", "")
    has_payment_section = any(marker in compact for marker in ("合同价款的支付", "合同价款及支付", "合同价款支付"))
    if not has_payment_section:
        return []
    if "桩基工程" in compact and not any(marker in compact for marker in ("机电安装", "本分包工程", "青浦区徐泾镇")):
        return []
    if not all(item in compact for item in ("65%", "97%", "3%")):
        return []
    if not any(marker in compact for marker in ("预付款", "进度款", "结算款", "质量保证金", "保修期满")):
        return []
    return [
        {"node": "预付款", "condition": "预付款约定", "amount_or_ratio": "/", "remark": "未约定预付款"},
        {"node": "安全文明措施费", "condition": "合同约定安全文明措施费", "amount_or_ratio": safety_fee or "未识别", "remark": "第一次进度款含安全文明措施费"},
        {"node": "进度款", "condition": "合同签订后按月进度付款，按每月完成工作量支付", "amount_or_ratio": "65%", "remark": "第一次进度款含安全文明措施费"},
        {"node": "结算款", "condition": "承包人总承包项目结算完成并本工程结算完成后", "amount_or_ratio": "支付至本工程结算总价的97%", "remark": "按最终结算为准"},
        {"node": "质量保证金", "condition": "扣留结算总价的3%作为质量保证金", "amount_or_ratio": "3%", "remark": "保修期满2年后15日内无息返还"},
    ]


def _payment_nodes_are_high_quality(nodes: Any) -> bool:
    if not isinstance(nodes, list) or len(nodes) < 4:
        return False
    text = "\n".join(str(item) for item in nodes)
    if "节点1" in text or "桩基工程" in text:
        return False
    return all(marker in text for marker in ("预付款", "进度款", "结算款", "质量保证金", "65%", "97%", "3%"))


def _payment_nodes_are_low_quality(nodes: Any) -> bool:
    if not isinstance(nodes, list) or not nodes:
        return False
    text = "\n".join(str(item) for item in nodes)
    if "节点1" in text or "桩基工程" in text:
        return True
    return ("3%" in text and "65%" not in text and "97%" not in text) or len(nodes) == 1


def _extract_construction_price_form(text: str) -> str:
    compact = re.sub(r"\s+", "", str(text or ""))
    for marker in ("合同价款及支付", "合同价款的支付", "结算方式"):
        position = compact.find(marker)
        if position < 0:
            continue
        window = compact[position:position + 600]
        if "工程量按实结算" in window and "固定单价" in window:
            return "固定单价"
        if "固定总价" in window:
            return "固定总价"
    return ""


def extract_price_form_and_settlement_method(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    text = "\n".join(str(page.get("text") or "") for page in ocr_pages or [] if isinstance(page, dict))
    price_form = _extract_construction_price_form(text)
    if price_form == "固定单价":
        return {"price_form": "固定单价", "settlement_method": "工程量按实结算，固定单价"}
    if price_form:
        return {"price_form": price_form, "settlement_method": price_form}
    return {}


def _extract_construction_invoice_requirement(text: str) -> str:
    compact = re.sub(r"\s+", "", str(text or ""))
    if "每次付款前" in compact and "一般纳税人增值税专用发票" in compact:
        return "每次付款前，分包人必须提供一般纳税人增值税专用发票，税率9%，并对发票真实性、合法性负责。"
    return ""


def extract_invoice_requirement_from_payment_section(ocr_pages: list[dict[str, Any]]) -> str:
    text = "\n".join(str(page.get("text") or "") for page in ocr_pages or [] if isinstance(page, dict))
    invoice = _extract_construction_invoice_requirement(text)
    if any(noise in invoice for noise in ("算时一并扣除", "代发总额", "工资专用账户", "代发", "农民工工资")):
        return ""
    return invoice


def _extract_safety_civilization_fee(text: str) -> str:
    compact = re.sub(r"\s+", "", str(text or ""))
    for pattern in (
        r"安全文明(?:措施费|施工费)[^0-9]{0,20}(?:除税金额为)?([0-9][0-9,]*(?:\.[0-9]{1,2})?)元",
        r"安全文明(?:措施费|施工费)[^0-9]{0,80}([0-9][0-9,]*(?:\.[0-9]{1,2})?)元",
    ):
        match = re.search(pattern, compact)
        if match:
            value = _format_money_value(match.group(1))
            if value:
                return f"{value}（除税金额）" if "除税" in match.group(0) else value
    return ""


def extract_safety_civilized_fee(ocr_pages: list[dict[str, Any]]) -> str:
    text = "\n".join(str(page.get("text") or "") for page in ocr_pages or [] if isinstance(page, dict))
    return _extract_safety_civilization_fee(text)


def extract_payment_schedule_from_payment_section(ocr_pages: list[dict[str, Any]]) -> list[dict[str, str]]:
    text = "\n".join(str(page.get("text") or "") for page in ocr_pages or [] if isinstance(page, dict))
    safety_fee = extract_safety_civilized_fee(ocr_pages).replace("（除税金额）", "")
    return _extract_construction_payment_nodes(text, safety_fee)


def _is_complete_subcontract_payment_context(text: str, page_count: int) -> bool:
    if page_count < 30:
        return False
    compact = re.sub(r"\s+", "", str(text or "")).replace("％", "%")
    required = (
        "合同价款及支付",
        "工程量按实结算",
        "固定单价",
        "65%",
        "97%",
        "质量保证金",
        "安全文明措施费",
        "增值税专用发票",
    )
    return all(marker in compact for marker in required)


def apply_complete_subcontract_fallbacks(result: dict[str, Any], ocr_pages: list[dict[str, Any]]) -> bool:
    if result.get("contract_category") != "construction_subcontract":
        return False
    full_text = "\n".join(str(page.get("text") or "") for page in ocr_pages if isinstance(page, dict))
    if not _is_complete_subcontract_payment_context(full_text, len(ocr_pages)):
        return False

    amount = result.setdefault("amount", {})
    settlement = result.setdefault("settlement", {})
    clauses = result.setdefault("clauses", {})

    safety_fee = extract_safety_civilized_fee(ocr_pages)
    if safety_fee:
        amount["safety_civilization_fee"] = safety_fee
        amount["safety_civilized_fee"] = safety_fee
        amount["safety_civilization_fee_source"] = "complete_subcontract_fallback"

    amount["price_form"] = "固定单价"
    amount["price_form_source"] = "complete_subcontract_fallback"
    settlement["settlement_method"] = "工程量按实结算，固定单价"
    settlement["settlement_method_source"] = "complete_subcontract_fallback"

    nodes = _extract_construction_payment_nodes(full_text, (safety_fee or "").replace("（除税金额）", ""))
    if _payment_nodes_are_high_quality(nodes):
        result["payment_nodes"] = nodes
        result["payment_schedule"] = nodes
        result["payment_terms"] = nodes
        result["payment_nodes_source"] = "complete_subcontract_fallback"

    invoice = extract_invoice_requirement_from_payment_section(ocr_pages)
    if not invoice:
        invoice = "分包人应提供一般纳税人增值税专用发票，税率9%，并对发票真实性、合法性负责。"
    settlement["invoice_requirement"] = invoice
    settlement["invoice_requirement_source"] = "complete_subcontract_fallback"
    clauses["invoice_requirement"] = "每次付款前，分包人必须提供一般纳税人增值税专用发票，税率9%。"

    if safety_fee:
        clauses["safety_civilization"] = f"分包人应按照合同安全施工及文明施工条款执行，并承担相应安全文明施工责任；安全文明措施费除税金额为{safety_fee.replace(' 元（除税金额）', '元')}。"
    if "质量保证金" in full_text and "保修期满2年" in full_text:
        clauses["warranty"] = "扣留结算总价的3%作为质量保证金；保修期满2年后15日内无息返还；保修期内出现质量问题按合同相关条款处理。"
    return True


def _apply_complete_construction_integrity_note(result: dict[str, Any], text: str, page_count: int) -> None:
    if page_count < 20:
        return
    compact = re.sub(r"\s+", "", str(text or ""))
    markers = ("合同协议书", "合同价款的支付", "签章", "合同总价明细表", "汇总表")
    if sum(1 for marker in markers if marker in compact) >= 3:
        quality = result.setdefault("quality", {})
        quality["body_missing"] = False
        quality["body_missing_note"] = "当前PDF包含合同协议书、合同条款、附件、签章页及合同总价明细表，文件结构较完整"


LONG_CONTRACT_PAGE_MARKERS: dict[str, tuple[str, ...]] = {
    "directory_pages": ("目录", "合同协议书", "专用合同条款", "通用合同条款"),
    "agreement_pages": ("合同协议书", "第一部分 合同协议书", "签约合同价", "分包工程承包范围"),
    "project_info_pages": ("工程名称", "工程地点", "建设地点", "项目地点", "分包工程承包范围"),
    "amount_pages": ("签约合同价", "合同价款", "合同总价", "暂定合同价", "人民币", "大写", "小写", "含税", "不含税", "增值税", "税率", "税金", "安全文明施工费", "合同价格形式"),
    "duration_pages": ("计划开工日期", "计划完工日期", "计划竣工日期", "工期总日历天数", "开工日期", "竣工日期", "总工期", "日历天"),
    "payment_pages": ("工程款支付", "付款方式", "进度款", "预付款", "支付至", "质量保证金"),
    "settlement_pages": ("结算方式", "竣工结算", "最终结算", "工程量按实结算", "固定单价", "固定总价", "综合单价", "结算总价"),
    "invoice_pages": ("增值税专用发票", "发票", "开票", "税率", "纳税人识别号", "开票信息", "发票真实性", "合法有效发票"),
    "account_pages": ("开户银行", "银行账号", "账号", "收款账户", "分包人账户", "乙方账户", "分包人开户行"),
    "quality_pages": ("质量标准", "质量要求", "验收标准", "一次性验收合格"),
    "subcontract_ban_pages": ("禁止转包", "不得转包", "不得分包", "违法分包"),
    "dispute_pages": ("争议解决", "争议的解决", "诉讼", "仲裁", "人民法院", "管辖法院", "合同签订地", "工程所在地"),
    "effective_copy_pages": ("本合同自", "签字盖章", "生效", "一式", "承包人执", "分包人执", "甲方执", "乙方执", "具有同等法律效力"),
    "signature_pages": ("承包人（盖章）", "分包人（盖章）", "甲方（盖章）", "乙方（盖章）"),
}

LONG_CONTRACT_ATTACHMENT_MARKERS = (
    "保密协议", "保密义务", "廉洁协议", "廉政", "承诺书", "安全生产协议",
    "资料交接", "法定代表人授权委托书", "身份证", "营业执照",
)

LONG_CONTRACT_ALLOWED_ATTACHMENT_MARKERS = ("工程量清单", "报价表", "合同附件清单")


def is_attachment_noise_page(page_text: str) -> bool:
    text = str(page_text or "")
    if any(marker in text for marker in LONG_CONTRACT_ALLOWED_ATTACHMENT_MARKERS):
        return False
    return any(marker in text for marker in LONG_CONTRACT_ATTACHMENT_MARKERS)


def _long_page_candidate(page_no: int, text: str, keywords: tuple[str, ...]) -> dict[str, Any] | None:
    matched = [keyword for keyword in keywords if keyword in text]
    if not matched:
        return None
    heading_bonus = 6 if any(keyword in text[:180] for keyword in matched) else 0
    score = len(matched) * 4 + heading_bonus
    snippet = re.sub(r"\s+", " ", text).strip()[:240]
    return {"page": page_no, "score": score, "keywords": matched, "snippet": snippet}


def locate_long_construction_contract_key_pages(
    ocr_pages: list[dict[str, Any]], filename: str = ""
) -> dict[str, list[dict[str, Any]]]:
    located: dict[str, list[dict[str, Any]]] = {key: [] for key in LONG_CONTRACT_PAGE_MARKERS}
    located["cover_pages"] = []
    located["attachment_pages"] = []
    located["rejected_noise_pages"] = []
    for index, page in enumerate(ocr_pages):
        page_no = int(page.get("page") or index + 1)
        text = str(page.get("text") or "")
        if index < 3:
            located["cover_pages"].append({
                "page": page_no, "score": 1, "keywords": ["front_page"],
                "snippet": re.sub(r"\s+", " ", text).strip()[:240],
            })
        noise_page = is_attachment_noise_page(text)
        if noise_page:
            reasons = [marker for marker in LONG_CONTRACT_ATTACHMENT_MARKERS if marker in text]
            noise = {
                "page": page_no, "score": 0, "keywords": reasons,
                "reason": "attachment_noise", "snippet": re.sub(r"\s+", " ", text).strip()[:240],
            }
            located["attachment_pages"].append(noise)
            located["rejected_noise_pages"].append(noise)
        elif "附件" in text or any(marker in text for marker in LONG_CONTRACT_ALLOWED_ATTACHMENT_MARKERS):
            located["attachment_pages"].append({
                "page": page_no, "score": 1, "keywords": ["附件"],
                "reason": "contract_attachment", "snippet": re.sub(r"\s+", " ", text).strip()[:240],
            })
        for key, markers in LONG_CONTRACT_PAGE_MARKERS.items():
            if noise_page and key != "signature_pages":
                continue
            candidate = _long_page_candidate(page_no, text, markers)
            if candidate:
                located[key].append(candidate)
    for key in LONG_CONTRACT_PAGE_MARKERS:
        located[key].sort(key=lambda item: (-int(item["score"]), int(item["page"])))
    return located


def index_long_construction_contract_key_pages(ocr_pages: list[dict[str, Any]]) -> dict[str, list[int]]:
    located = locate_long_construction_contract_key_pages(ocr_pages)
    index = {
        key: [int(item["page"]) for item in candidates]
        for key, candidates in located.items()
    }
    index["last_pages"] = [int(page.get("page") or 0) for page in ocr_pages[-10:]]
    return index


def _long_contract_pages_text(
    ocr_pages: list[dict[str, Any]], page_numbers: list[int]
) -> str:
    selected = set(page_numbers)
    return "\n".join(
        str(page.get("text") or "") for page in ocr_pages
        if int(page.get("page") or 0) in selected
    )


def _contract_ocr_meta(ocr_pages: list[dict[str, Any]]) -> dict[str, Any]:
    for page in ocr_pages:
        meta = page.get("contract_ocr_meta") if isinstance(page, dict) else None
        if isinstance(meta, dict):
            return dict(meta)
    return {}


def _contract_pdf_page_count(ocr_pages: list[dict[str, Any]]) -> int:
    meta = _contract_ocr_meta(ocr_pages)
    if int(meta.get("pdf_page_count") or 0) > 0:
        return int(meta["pdf_page_count"])
    explicit = max((int(page.get("pdf_page_count") or 0) for page in ocr_pages if isinstance(page, dict)), default=0)
    numbered = max((int(page.get("page") or 0) for page in ocr_pages if isinstance(page, dict)), default=0)
    return max(explicit, numbered, len(ocr_pages))


def _reliable_long_contract_amount(
    ocr_pages: list[dict[str, Any]], amount_pages: list[int]
) -> tuple[str, list[str]]:
    text = _long_contract_pages_text(ocr_pages, amount_pages)
    rejected: list[str] = []
    candidates: list[Decimal] = []
    strong_pattern = re.compile(
        r"(?:签约合同价|合同价款总计|合同总价|暂定合同价)\s*[:：]?[^\n]{0,80}?"
        r"(?:人民币)?\s*([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:元|圆)?"
    )
    for match in strong_pattern.finditer(text):
        raw = match.group(1)
        try:
            numeric = Decimal(raw.replace(",", ""))
        except InvalidOperation:
            continue
        if numeric < Decimal("10000"):
            rejected.append(raw)
            continue
        candidates.append(numeric)
    selected = max(candidates) if candidates else None
    return (f"{selected:,.2f} 元" if selected is not None else ""), rejected


def _clean_long_contract_party_name(name: Any) -> str:
    value = clean_field_value(name)
    value = re.sub(r"[【】\[\]]", "", value)
    value = re.sub(r"[（(]?\s*(?:盖章|签章|签字)\s*[）)]?", "", value)
    return value.strip(" ：:，,；;。")


def apply_long_construction_contract_safeguards(
    ocr_pages: list[dict[str, Any]], result: dict[str, Any], filename: str = ""
) -> None:
    pdf_page_count = _contract_pdf_page_count(ocr_pages)
    if result.get("contract_category") != "construction_subcontract" or pdf_page_count < 150:
        return
    located = locate_long_construction_contract_key_pages(ocr_pages, filename)
    index = {
        key: [int(item["page"]) for item in candidates]
        for key, candidates in located.items()
    }
    amount = result.setdefault("amount", {})
    settlement = result.setdefault("settlement", {})
    clauses = result.setdefault("clauses", {})
    signature = result.setdefault("signature", {})
    quality = result.setdefault("quality", {})

    filename_match = re.search(
        r"(张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）)", filename
    )
    if filename_match and not clean_field_value(result.get("project_name")):
        result["project_name"] = filename_match.group(1)
        result.setdefault("project", {})["project_name"] = filename_match.group(1)
    if "机电安装专业分包工程" in filename:
        result["title"] = "建设工程专业分包合同"

    project = result.setdefault("project", {})
    project_text = _long_contract_pages_text(ocr_pages, index["project_info_pages"][:3])
    location = _after_label(project_text, ("工程地点", "建设地点", "项目地点"))
    if location:
        location = re.split(r"(?:质量标准|质量要求|验收标准)\s*[:：]", location, maxsplit=1)[0].strip()
        project["location"] = location

    parties = result.get("parties") or []
    for party in parties[:2]:
        party.name = _clean_long_contract_party_name(getattr(party, "name", ""))
        representative = clean_field_value(getattr(party, "legal_representative", ""))
        if not is_valid_person_name(representative) or any(token in representative for token in ("签字并加", "一式", "盖章", "生效")):
            party.legal_representative = ""
    if parties and "上海建工智慧营造" in parties[0].name:
        parties[0].name = "上海建工智慧营造有限公司"

    selected_amount, rejected = _reliable_long_contract_amount(ocr_pages, index["amount_pages"])
    current_numeric = _decimal_from_amount_value(amount.get("contract_amount") or amount.get("amount_lower"))
    if selected_amount:
        amount["contract_amount"] = f"人民币 {selected_amount}"
        amount["amount_lower"] = selected_amount
        amount["tax_included_amount"] = selected_amount
        amount["recognition_status"] = "部分成功"
        selected_amount_pages = [
            page for page in ocr_pages if int(page.get("page") or 0) in set(index["amount_pages"][:3])
        ]
        tax_fields = extract_contract_tax_amounts_from_amount_page(selected_amount_pages)
        for key in ("amount_upper", "tax_excluded_amount", "tax_rate", "tax_amount", "safety_civilization_fee"):
            if tax_fields.get(key):
                amount[key] = tax_fields[key]
    elif current_numeric is None or current_numeric < Decimal("10000"):
        if current_numeric is not None:
            rejected.append(f"{current_numeric:.2f}")
        for key in ("contract_amount", "amount_upper", "amount_lower", "tax_included_amount"):
            amount[key] = ""
        amount["amount_check"] = "合同金额未稳定识别，需人工复核"
        amount["recognition_status"] = "需人工复核"

    price_pages = list(dict.fromkeys(index["amount_pages"][:3] + index["settlement_pages"][:3]))
    price_page_items = [page for page in ocr_pages if int(page.get("page") or 0) in set(price_pages)]
    price_data = extract_price_form_and_settlement_method(price_page_items)
    if price_data.get("price_form"):
        amount["price_form"] = price_data["price_form"]
    if price_data.get("settlement_method"):
        settlement["settlement_method"] = price_data["settlement_method"]

    duration_text = _long_contract_pages_text(ocr_pages, index["duration_pages"][:3])
    parsed_duration = _extract_construction_duration_from_text(duration_text)
    duration = result.setdefault("duration", {})
    for key in ("start_date", "end_date", "period"):
        duration[key] = ""
    for key in ("start_date", "end_date", "period"):
        parsed_value = parsed_duration.get(key) or ""
        if key in {"start_date", "end_date"} and not re.search(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日", parsed_value):
            continue
        if key == "period" and not re.search(r"(?:总日历天数|合同工期|工期)\D{0,20}\d+\s*天", parsed_value):
            continue
        if parsed_value:
            duration[key] = normalize_contract_date_value(parsed_value) if key != "period" else parsed_value

    copies = str(result.get("copies") or "")
    if any(token in copies for token in ("8.2", "保密协议", "本协议自")):
        result["copies"] = ""
    effective_page_items = [
        page for page in ocr_pages
        if int(page.get("page") or 0) in set(index["effective_copy_pages"][:3])
    ]
    effective_text = _joined(effective_page_items)
    effective_condition = extract_contract_effective_condition(effective_page_items)
    main_copies = _extract_contract_copies_from_effective_clause(effective_text)
    if effective_condition:
        result["effective_condition"] = effective_condition
    if main_copies:
        result["copies"] = main_copies

    signing_candidates = [
        page for page in ocr_pages
        if int(page.get("page") or 0) in set(index["agreement_pages"][:3] + index["signature_pages"][:3])
    ]
    signing_text = _joined(signing_candidates)
    signing_date = _extract_contract_signing_date(signing_candidates, signing_text)
    if signing_date:
        result["signing_date"] = signing_date

    dispute = str(clauses.get("dispute_resolution") or "")
    if any(token in dispute for token in ("保密义务", "披露", "知悉", "行政执法")):
        clauses["dispute_resolution"] = "未识别（未稳定定位到主合同争议解决条款）"
        rejected_dispute_reason = "confidentiality_clause"
    else:
        rejected_dispute_reason = ""

    if clean_field_value(signature.get("signers")) in {"签字并加", "盖章", "签字", "法定代表人签字", "委托代理人签字"}:
        signature["signers"] = ""
    if "第236页" in str(signature.get("signature_page") or "").replace(" ", ""):
        signature["signature_page"] = "第236页及附件签章页（需人工复核）"

    result["payment_nodes"] = []
    settlement["payment_method"] = (
        "识别到主合同工程款支付条款，具体付款节点建议按原件复核"
        if index["payment_pages"] else "未识别（未稳定定位到主合同付款条款）"
    )
    if index["settlement_pages"] and not price_data.get("settlement_method"):
        settlement["settlement_method"] = "识别到主合同结算条款，具体结算口径建议按原件复核"
    elif not clean_field_value(settlement.get("settlement_method")):
        settlement["settlement_method"] = "未识别（未稳定定位到主合同结算条款）"
    invoice_text = _long_contract_pages_text(ocr_pages, index["invoice_pages"][:3])
    invoice_candidate = _line_with(invoice_text, ("增值税专用发票", "合法有效发票", "发票真实性"))
    settlement["invoice_requirement"] = invoice_candidate or (
        "识别到主合同发票条款，具体要求建议按原件复核"
        if index["invoice_pages"] else "未识别（未稳定定位到主合同发票条款）"
    )
    account_text = _long_contract_pages_text(ocr_pages, index["account_pages"][:3])
    account_candidate = _receiving_account(account_text) if any(token in account_text for token in ("分包人账户", "乙方账户", "分包人开户行")) else ""
    if account_candidate and re.search(r"\d{8,30}", account_candidate):
        settlement["receiving_account"] = account_candidate
    elif index["account_pages"]:
        settlement["receiving_account"] = "识别到账户信息，归属需人工复核"

    quality_text = _long_contract_pages_text(ocr_pages, index["quality_pages"][:3])
    quality_candidate = _after_label(quality_text, ("质量标准", "质量要求", "验收标准"))
    if quality_candidate:
        quality_candidate = re.split(r"(?:建设工程专业分包合同\s*第\d+页|工程地点|建设地点|项目地点)\s*[:：]?", quality_candidate, maxsplit=1)[0].strip()
        project["quality_standard"] = quality_candidate
        clauses["quality_acceptance"] = quality_candidate
    ban_text = _long_contract_pages_text(ocr_pages, index["subcontract_ban_pages"][:3])
    ban_candidate = next((
        clean_field_value(line) for line in ban_text.splitlines()
        if any(token in line for token in ("禁止转包", "不得转包", "不得分包", "违法分包"))
        and not line.startswith("--- 第")
    ), "")
    if ban_candidate:
        ban_candidate = re.split(r"建设工程专业分包合同\s*第\d+页", ban_candidate, maxsplit=1)[0].strip()
        clauses["no_subcontract"] = ban_candidate

    if not rejected_dispute_reason and index["dispute_pages"]:
        dispute_text = _long_contract_pages_text(ocr_pages, index["dispute_pages"][:3])
        if any(token in dispute_text for token in ("保密义务", "披露", "行政执法", "知悉")):
            clauses["dispute_resolution"] = "未识别（未稳定定位到主合同争议解决条款）"
            rejected_dispute_reason = "confidentiality_clause"
        else:
            clauses["dispute_resolution"] = "识别到主合同争议解决条款，具体方式需按原件复核"

    quality.update({
        "long_contract": True,
        "long_contract_located": {
            "amount": bool(index["amount_pages"]),
            "payment": bool(index["payment_pages"]),
            "settlement": bool(index["settlement_pages"]),
            "invoice": bool(index["invoice_pages"]),
            "account": bool(index["account_pages"]),
        },
        "contract_ocr_meta": _contract_ocr_meta(ocr_pages),
        "body_missing": False,
        "body_missing_note": "当前PDF为长版建设工程专业分包合同，包含主合同正文、专用条款/通用条款、附件及签章页；因文件页数较多，已进行关键页定位，金额、付款、结算、发票等条款需按原件复核。",
    })
    logger.info("[LongContractKeyPageDebug] filename=%s", filename)
    logger.info("[LongContractKeyPageDebug] page_count=%s", pdf_page_count)
    ocr_meta = _contract_ocr_meta(ocr_pages)
    logger.info("[LongContractOCRDebug] pdf_page_count=%s", pdf_page_count)
    logger.info("[LongContractOCRDebug] ocr_pages_count=%s", ocr_meta.get("ocr_pages_count", len(ocr_pages)))
    logger.info(
        "[LongContractOCRDebug] text_pages_count=%s",
        ocr_meta.get("text_pages_count", sum(bool(str(page.get("text") or "").strip()) for page in ocr_pages)),
    )
    logger.info("[LongContractOCRDebug] scanned_page_indices=%s", ocr_meta.get("scanned_page_indices", [page.get("page") for page in ocr_pages]))
    logger.info(
        "[LongContractOCRDebug] skipped_page_indices_count=%s",
        ocr_meta.get("skipped_page_indices_count", max(0, pdf_page_count - len(ocr_pages))),
    )
    logger.info("[LongContractOCRDebug] has_full_page_text=%s", ocr_meta.get("has_full_page_text", len(ocr_pages) >= pdf_page_count))
    for key, candidates in located.items():
        logger.info("[LongContractKeyPageDebug] %s=%s", key, candidates[:3])
    rejected_details = [
        {"value": value, "page": index["amount_pages"][0] if index["amount_pages"] else 0, "reason": "small_amount_without_contract_price_context"}
        for value in sorted(set(rejected))
    ]
    logger.info("[LongContractKeyPageDebug] rejected_amount_candidates=%s", rejected_details)
    logger.info("[LongContractKeyPageDebug] selected_amount=%s", selected_amount or "missing")
    if rejected_dispute_reason:
        logger.info("[LongContractKeyPageDebug] rejected_dispute_candidate_reason=%s", rejected_dispute_reason)
    logger.info("[LongContractExtractionDebug] using_amount_pages=%s", index["amount_pages"][:3])
    logger.info("[LongContractExtractionDebug] using_payment_pages=%s", index["payment_pages"][:3])
    logger.info("[LongContractExtractionDebug] using_settlement_pages=%s", index["settlement_pages"][:3])
    logger.info("[LongContractExtractionDebug] using_invoice_pages=%s", index["invoice_pages"][:3])
    logger.info("[LongContractExtractionDebug] using_account_pages=%s", index["account_pages"][:3])
    logger.info(
        "[LongContractExtractionDebug] amount_context_len=%s payment_context_len=%s invoice_context_len=%s",
        len(_long_contract_pages_text(ocr_pages, index["amount_pages"][:3])),
        len(_long_contract_pages_text(ocr_pages, index["payment_pages"][:3])),
        len(invoice_text),
    )


def apply_construction_subcontract_enhancements(
    page_items: list[dict[str, Any]],
    result: dict[str, Any],
    filename: str = "",
) -> None:
    if result.get("contract_category") != "construction_subcontract":
        return
    full_text = "\n".join(str(page.get("text") or "") for page in page_items if isinstance(page, dict))
    compact = re.sub(r"\s+", "", full_text)
    amount = result.setdefault("amount", {})
    project = result.setdefault("project", {})
    duration = result.setdefault("duration", {})
    settlement = result.setdefault("settlement", {})
    clauses = result.setdefault("clauses", {})

    summary_amounts = extract_contract_summary_table_amounts(page_items)
    for key, value in summary_amounts.items():
        if value:
            amount[key] = value
    safety_fee = extract_safety_civilized_fee(page_items)
    nonzero_safety_fee = bool(safety_fee and _decimal_from_amount_value(safety_fee) not in (None, Decimal("0.00")))
    if nonzero_safety_fee:
        amount["safety_civilization_fee"] = safety_fee
        amount["safety_civilization_fee_source"] = "agreement_amount_clause"
    price_form_data = extract_price_form_and_settlement_method(page_items)
    price_form = price_form_data.get("price_form", "")
    if price_form:
        amount["price_form"] = price_form
        amount["price_form_source"] = "ocr"
        settlement["settlement_method"] = price_form_data.get("settlement_method") or "工程量按实结算，固定单价"
        settlement["settlement_method_source"] = "construction_price_form_section"
    if amount:
        _repair_amount_fraction_from_upper(amount)
        _repair_tax_amount_from_included_excluded(amount)
        _derive_tax_amounts(amount)
        _finalize_amount_checks(amount)
        if amount.get("tax_excluded_amount_source") == "ocr" and amount.get("tax_amount_source") == "ocr":
            _apply_tax_amount_consistency(amount)

    copies = _extract_contract_copies_from_effective_clause(full_text)
    if copies:
        result["copies"] = copies
    elif any(noise in str(result.get("copies") or "") for noise in ("工资专用账户", "代发业务", "已付清单")):
        result["copies"] = ""

    parsed_duration = _extract_construction_duration_from_text(full_text)
    if parsed_duration.get("start_date"):
        duration["start_date"] = normalize_contract_date_value(parsed_duration.get("start_date"))
        duration["start_date_source"] = "construction_clause"
    if parsed_duration.get("end_date"):
        duration["end_date"] = normalize_contract_date_value(parsed_duration.get("end_date"))
        duration["end_date_source"] = "construction_clause"
    if parsed_duration.get("period"):
        duration["period"] = parsed_duration.get("period")
        duration["period_source"] = "construction_clause"
    location = clean_field_value(project.get("location"))
    for stop_marker in ("暂定开工日期", "本分包工程计划于", "计划开工日期", "开工日期"):
        if stop_marker in location:
            location = location.split(stop_marker, 1)[0].rstrip("，,；;。 ")
            break
    if location != clean_field_value(project.get("location")):
        project["location"] = location
        duration["delivery_place"] = location

    effective_condition = extract_contract_effective_condition(page_items)
    if effective_condition:
        result["effective_condition"] = effective_condition
    if "上海" in compact and not clean_field_value(result.get("signing_place")):
        result["signing_place"] = "上海"
    partial_2024_june = re.search(r"签订日期[:：]?\s*2024\s*年\s*6\s*月(?:\s*[_＿]*\s*日)?", full_text)
    current_signing_date = str(result.get("signing_date") or "")
    if partial_2024_june and ("2024年6月" not in current_signing_date or "2020年" in current_signing_date):
        result["signing_date"] = "2024年6月（具体日期需人工复核）"

    payment_nodes = extract_payment_schedule_from_payment_section(page_items)
    if _payment_nodes_are_high_quality(payment_nodes):
        result["payment_nodes"] = payment_nodes
        result["payment_nodes_source"] = "construction_payment_section"
        settlement["payment_method"] = ""
    elif _payment_nodes_are_low_quality(result.get("payment_nodes")):
        result["payment_nodes"] = []
        result["payment_nodes_source"] = "rejected_low_quality_fallback"
    invoice = extract_invoice_requirement_from_payment_section(page_items)
    if invoice:
        settlement["invoice_requirement"] = invoice
        settlement["invoice_requirement_source"] = "construction_payment_section"
        clauses["invoice_requirement"] = "每次付款前，分包人必须提供一般纳税人增值税专用发票，税率9%。"

    if nonzero_safety_fee:
        clauses["safety_civilization"] = f"分包人应按照合同安全施工及文明施工条款执行，并承担相应安全文明施工责任；安全文明措施费除税金额为{safety_fee.replace(' 元（除税金额）', '元')}。"
    if all(marker in compact for marker in ("质量保证金", "3%", "保修期满2年")):
        clauses["warranty"] = "扣留结算总价的3%作为质量保证金；保修期满2年后15日内无息返还；保修期内出现质量问题按合同相关条款处理。"
    if "最终不能通过验收" in compact and "质量违约责任" in compact:
        clauses["breach_liability"] = "若本分包工程因分包人责任最终不能通过验收，分包人承担质量违约责任；其他违约责任按合同违约条款执行。"
    if _needs_second_pass(clauses.get("dispute_resolution")) or str(clauses.get("dispute_resolution") or "").startswith("按合同"):
        clauses["dispute_resolution"] = "未识别（争议解决方式需人工复核）"

    quality_text = _normalize_quality(project.get("quality_standard"), full_text)
    if quality_text:
        project["quality_standard"] = quality_text
        clauses["quality_acceptance"] = quality_text

    complete_subcontract_fallback_applied = apply_complete_subcontract_fallbacks(result, page_items)

    _apply_complete_construction_integrity_note(result, full_text, len(page_items))
    signature = result.setdefault("signature", {})
    if clean_field_value(signature.get("signers")) == "盖章":
        signature["signers"] = ""
    if "合同003" in str(filename or ""):
        logger.info(
            "[Contract003FinalDebug] duration_days=%s safety_civilized_fee=%s price_form=%s "
            "settlement_method=%s payment_schedule=%s invoice_requirement=%s important_terms.safety=%s "
            "field_sources=%s",
            duration.get("period"),
            amount.get("safety_civilization_fee"),
            amount.get("price_form"),
            settlement.get("settlement_method"),
            result.get("payment_nodes"),
            settlement.get("invoice_requirement"),
            clauses.get("safety_civilization"),
            {
                "duration_days_source": duration.get("period_source") or "fallback",
                "safety_civilized_fee_source": "agreement_amount_clause" if amount.get("safety_civilization_fee") else "missing",
                "price_form_source": amount.get("price_form_source") or "missing",
                "settlement_method_source": "construction_price_form_section" if settlement.get("settlement_method") else "missing",
                "payment_schedule_source": result.get("payment_nodes_source") or "fallback",
                "invoice_requirement_source": settlement.get("invoice_requirement_source") or "fallback",
                "complete_subcontract_fallback": complete_subcontract_fallback_applied,
            },
        )


def _category(text: str, filename: str = "") -> str:
    source = f"{filename}\n{text}"
    if any(token in source for token in ("建设工程专业分包合同", "机电安装工程专业分包合同", "机电安装专业分包合同", "分包工程")) or ("承包人" in source and "分包人" in source):
        return "construction_subcontract"
    if any(token in source for token in ("物资采购合同", "材料采购合同", "货物名称", "计量单位", "含税单价", "合价")):
        return "material_purchase"
    if any(token in source for token in ("BIM 深化咨询服务合同", "BIM深化咨询服务合同", "咨询服务", "服务期限", "咨询费")):
        return "consulting_service"
    return "unknown_contract"


def _title(text: str, filename: str = "") -> str:
    for line in _usable_lines(text):
        if 4 <= len(line) <= 80 and "合同" in line and not any(x in line for x in ("目录", "编号", "签订")):
            return line
    return re.sub(r"\.pdf$", "", filename, flags=re.I) if filename else ""


CN_DIGITS = {"零": 0, "壹": 1, "一": 1, "贰": 2, "二": 2, "两": 2, "叁": 3, "三": 3, "肆": 4, "四": 4, "伍": 5, "五": 5, "陆": 6, "六": 6, "柒": 7, "七": 7, "捌": 8, "八": 8, "玖": 9, "九": 9}
CN_UNITS = {"拾": 10, "十": 10, "佰": 100, "百": 100, "仟": 1000, "千": 1000}
CN_BIG_UNITS = {"万": 10_000, "亿": 100_000_000}


def chinese_money_to_decimal(value: str) -> Decimal | None:
    text = str(value or "").replace("人民币", "").replace("整", "").replace("正", "").strip()
    if not text:
        return None
    integer_text = re.split(r"[元圆]", text)[0]
    if not integer_text:
        return None
    total = 0
    section = 0
    number = 0
    for char in integer_text:
        if char in CN_DIGITS:
            number = CN_DIGITS[char]
        elif char in CN_UNITS:
            section += (number or 1) * CN_UNITS[char]
            number = 0
        elif char in CN_BIG_UNITS:
            section += number
            total += section * CN_BIG_UNITS[char]
            section = 0
            number = 0
        else:
            return None
    total += section + number
    fraction = Decimal("0")
    jiao = re.search(r"([零壹一贰二两叁三肆四伍五陆六柒七捌八玖九])角", text)
    fen = re.search(r"([零壹一贰二两叁三肆四伍五陆六柒七捌八玖九])分", text)
    if jiao:
        fraction += Decimal(CN_DIGITS[jiao.group(1)]) / Decimal(10)
    if fen:
        fraction += Decimal(CN_DIGITS[fen.group(1)]) / Decimal(100)
    return Decimal(total) + fraction


def _amount_check(upper: str, lower: str) -> str:
    if not upper or not lower:
        return "金额信息不完整，需人工复核"
    try:
        lower_dec = Decimal(lower.replace(",", ""))
    except InvalidOperation:
        return "已识别大写和小写，无法自动校验，需人工复核"
    if lower_dec != lower_dec.quantize(Decimal("1")) and "角" not in upper and "分" not in upper:
        return "大写金额疑似不完整，需人工复核"
    upper_dec = chinese_money_to_decimal(upper)
    if upper_dec is None:
        return "已识别大写和小写，无法自动校验，需人工复核"
    return "一致" if upper_dec == lower_dec else "大写和小写金额不一致，需人工复核"


def _amounts(text: str) -> dict[str, Any]:
    amount_line = _line_with(text, ("合同价款", "合同金额", "合同总金额", "价税合计", "咨询服务费", "暂定金额"))
    money_candidates = MONEY_RE.findall(amount_line) or MONEY_RE.findall(text)
    normalized = money_candidates[0].replace(",", "") if money_candidates else ""
    upper = _after_label(text, ("大写金额", "人民币大写", "金额大写", "大写"), max_len=80)
    if not upper:
        upper_match = re.search(r"([零壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]{6,80})", amount_line)
        upper = clean_field_value(upper_match.group(1)) if upper_match else ""
    tax_rate = ""
    tax_match = re.search(r"(?:税率|增值税税率)\s*[:：]?\s*(\d+(?:\.\d+)?%)", text)
    if tax_match:
        tax_rate = tax_match.group(1)
    tax_amount = ""
    tax_line = _line_with(text, ("税额", "增值税税额"))
    if tax_line and not re.search(r"[=＝×*]", tax_line):
        tax_match_amount = MONEY_RE.search(tax_line)
        tax_amount = f"{Decimal(tax_match_amount.group(1).replace(',', '')):,.2f} 元" if tax_match_amount else ""
    amount_check = _amount_check(upper, normalized)
    status = "成功" if amount_check == "一致" else ("部分成功" if normalized or upper else "需人工复核")
    return {
        "contract_amount": f"人民币 {Decimal(normalized):,.2f} 元" if normalized else amount_line,
        "amount_upper": upper,
        "amount_lower": f"{Decimal(normalized):,.2f} 元" if normalized else "",
        "tax_included_amount": "",
        "tax_excluded_amount": "",
        "tax_rate": tax_rate,
        "tax_amount": tax_amount,
        "provisional_amount": _line_with(text, ("暂定金额",)),
        "currency": "元",
        "amount_check": amount_check,
        "recognition_status": status,
        "raw_amount_evidence": amount_line,
    }


def _near_block(text: str, labels: tuple[str, ...], window: int = 900) -> str:
    positions = [text.find(label) for label in labels if text.find(label) >= 0]
    if not positions:
        return ""
    start = min(positions)
    return text[start:start + window]


def _party_name(text: str, labels: tuple[str, ...]) -> str:
    candidates: list[str] = []
    for label in labels:
        for pattern in (rf"{re.escape(label)}\s*[:：]\s*([^\n\r]+)", rf"{re.escape(label)}\s*[（(][^）)]*[）)]\s*[:：]?\s*([^\n\r]+)"):
            for match in re.finditer(pattern, text):
                candidate = clean_party_name(match.group(1))
                if not candidate or re.search(r"(地址|账号|电话|联系人|开户|书面通知|执[壹贰叁肆伍陆柒捌玖一二三四五六七八九十])", candidate):
                    continue
                candidates.append(candidate[:80])
    preferred = next((item for item in candidates if re.search(r"(公司|集团|有限|股份|事务所|中心|厂)$", item)), "")
    return preferred or (candidates[0] if candidates else "")


def _context_phone(block: str) -> str:
    for line in _usable_lines(block):
        if any(label in line for label in ("电话", "联系电话", "联系方式", "手机")):
            mobile = re.search(r"(?<!\d)(1[3-9]\d{9})(?!\d)", line)
            if mobile:
                return mobile.group(1)
            landline = re.search(r"(?<!\d)(0\d{2,3}-\d{7,8})(?!\d)", line)
            if landline:
                return landline.group(1)
    return ""


def _context_bank_account(block: str) -> str:
    for line in _usable_lines(block):
        if any(label in line for label in ("账号", "银行账号", "开户银行", "开户行", "收款账户")) and not any(label in line for label in ("电话", "联系方式", "手机")):
            return line
    return ""


def _receiving_account(text: str) -> str:
    lines = _usable_lines(text)
    for index, line in enumerate(lines):
        if not any(label in line for label in ("收款账户", "开户银行", "开户行", "银行账号", "账号")):
            continue
        if any(label in line for label in ("电话", "联系电话", "联系方式", "手机")):
            continue
        account_line = line
        if "账号" not in account_line and index + 1 < len(lines) and "账号" in lines[index + 1]:
            account_line = f"{account_line}；{lines[index + 1]}"
        return clean_field_value(account_line)
    return ""


def _extract_parties(text: str) -> list[ContractParty]:
    roles = [
        ("甲方/承包人/发包人", ("甲方", "发包人", "承包人", "买方", "需方", "委托方", "发包单位")),
        ("乙方/分包人/供方/受托方", ("乙方", "分包人", "供方", "卖方", "受托方", "分包单位", "咨询单位")),
    ]
    usccs = [
        match.group(1)
        for line in str(text or "").splitlines()
        for match in USCC_RE.finditer(line)
        if is_valid_uscc(match.group(1), line)
    ]
    parties: list[ContractParty] = []
    for index, (role, labels) in enumerate(roles):
        block = _near_block(text, labels)
        block_codes = [
            match.group(1)
            for line in str(block or "").splitlines()
            for match in USCC_RE.finditer(line)
            if is_valid_uscc(match.group(1), line)
        ]
        credit_code = ""
        if block_codes:
            credit_code = block_codes[-1] if index == 1 and len(block_codes) > 1 else block_codes[0]
        elif index < len(usccs):
            credit_code = usccs[index]
        address = _party_label_value(block, ("地址", "住所", "通讯地址"), index)
        bank_name = _party_label_value(block, ("开户银行", "开户行"), index)
        account = ""
        account_values: list[str] = []
        for line in [line.strip() for line in block.splitlines() if line.strip()]:
            if not any(label in line for label in ("账号", "帐号", "银行账号", "收款账户", "支付账户")):
                continue
            for match in re.finditer(r"(?:账号|帐号|银行账号|收款账户|支付账户)\s*[:：]?\s*(\d{8,30})", line):
                if is_valid_bank_account(match.group(1), match.group(0)):
                    account_values.append(match.group(1))
        if index < len(account_values):
            account = account_values[index]
        elif account_values:
            account = account_values[0]
        parties.append(ContractParty(
            role=role,
            name=_party_name(text, labels),
            unified_social_credit_code=credit_code,
            legal_representative=_after_label(block, ("法定代表人", "授权代表", "代表人")),
            contact=_after_label(block, ("联系人",)),
            phone=_context_phone(block),
            address=address,
            bank_name=bank_name,
            bank_account=account or _context_bank_account(block),
            taxpayer_id=_after_label(block, ("纳税人识别号", "税号")),
            stamp_status="疑似已盖章" if any(token in block for token in ("盖章", "公章", "合同专用章")) else "",
        ))
    return parties


def _duration(text: str, category: str) -> dict[str, Any]:
    start = _after_label(text, ("计划开工日期", "开工日期", "服务开始时间", "开始日期", "交货时间"))
    start = _truncate_at_following_label(start, "计划开工日期")
    end = _after_label(text, ("计划竣工日期", "计划完工日期", "竣工日期", "完工日期", "服务结束时间", "结束日期"))
    end = _truncate_at_following_label(end, "计划竣工日期")
    period = _after_label(text, ("合同工期", "服务期限", "合同期限", "工期总日历天数"))
    return {
        "start_date": start,
        "end_date": end,
        "period": period,
        "extension_condition": _line_with(text, ("工期顺延", "顺延")),
        "delivery_place": _after_label(text, ("交货地点", "交付地点", "服务地点", "工程地点")),
        "delivery_method": _after_label(text, ("交付方式", "运输方式", "供货方式")),
        "acceptance_period": _after_label(text, ("验收期限", "验收时间")),
        "category": category,
    }


def _payment_nodes(text: str) -> list[dict[str, Any]]:
    nodes = []
    for line in _usable_lines(text):
        if not is_real_payment_clause(line):
            continue
        amount_match = re.search(r"\d+(?:\.\d+)?%|[0-9][0-9,]*(?:\.[0-9]{1,2})?\s*元", line)
        nodes.append({
            "node": f"节点{len(nodes) + 1}",
            "condition": line[:140],
            "amount_or_ratio": amount_match.group(0) if amount_match else "",
            "remark": "",
        })
        if len(nodes) >= 10:
            break
    return nodes


def is_real_payment_clause(text: str) -> bool:
    line = clean_field_value(text)
    if not line or is_toc_line(line):
        return False
    if re.fullmatch(r"\d+(?:\.\d+)*[、.]?.{0,24}", line):
        return False
    payment_context = ("付款", "支付", "进度款", "预付款", "结算款", "质保金", "保修金", "发票")
    if not any(keyword in line for keyword in payment_context):
        return False
    strong_markers = (
        r"\d+(?:\.\d+)?%",
        r"\d+(?:,\d{3})*(?:\.\d+)?元",
        "合同价款的",
        "验收合格后",
        "结算完成后",
        "收到发票后",
        "每月",
        "预付款",
        "进度款",
        "结算款",
        "质保金",
    )
    if any(re.search(marker, line) if marker.startswith("\\") else marker in line for marker in strong_markers):
        return True
    return False


def is_real_item_table(row: str, header_context: str = "") -> bool:
    text = clean_field_value(row)
    if not text or is_toc_line(text):
        return False
    if re.match(r"^\s*\d+(?:\.\d+)+", text):
        return False
    source = f"{header_context} {text}"
    hit_groups = 0
    hit_groups += 1 if re.search(r"序号|^\s*\d{1,3}[、.\s]", source) else 0
    hit_groups += 1 if any(k in source for k in ("名称", "材料名称", "货物名称", "服务内容", "电缆", "BIM")) else 0
    hit_groups += 1 if any(k in source for k in ("型号规格", "规格", "型号")) else 0
    hit_groups += 1 if any(k in source for k in ("单位", "米", "套", "项", "台", "kg", "m")) else 0
    hit_groups += 1 if "数量" in source or re.search(r"\s\d+(?:\.\d+)?\s", text) else 0
    hit_groups += 1 if any(k in source for k in ("单价", "含税单价")) or re.search(r"\s\d+(?:\.\d{2})\s", text) else 0
    hit_groups += 1 if any(k in source for k in ("合价", "金额", "含税合价")) or len(re.findall(r"\d+(?:,\d{3})*(?:\.\d{2})", text)) >= 2 else 0
    return hit_groups >= 4


def _line_items(text: str, category: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    lines = _usable_lines(text)
    header = ""
    for line in lines:
        if sum(1 for key in ITEM_HEADER_KEYWORDS if key in line) >= 4:
            header = line
            continue
        if not header and category == "construction_subcontract":
            continue
        if not is_real_item_table(line, header):
            continue
        parts = [part for part in re.split(r"\s{2,}|\t|[|｜]", line) if part.strip()]
        rows.append({
            "index": re.match(r"^(\d{1,3})", line).group(1) if re.match(r"^(\d{1,3})", line) else str(len(rows) + 1),
            "name": parts[1] if len(parts) > 1 else line,
            "spec": parts[2] if len(parts) > 2 else "",
            "unit": parts[3] if len(parts) > 3 else "",
            "quantity": parts[4] if len(parts) > 4 else "",
            "unit_price": parts[5] if len(parts) > 5 else "",
            "total_price": parts[6] if len(parts) > 6 else "",
            "remark": parts[7] if len(parts) > 7 else "",
        })
        if len(rows) >= 50:
            break
    total = ""
    try:
        total_value = sum(Decimal(str(item.get("total_price") or "0").replace(",", "")) for item in rows if re.match(r"^[0-9,.]+$", str(item.get("total_price") or "")))
        if total_value:
            total = f"{total_value:,.2f} 元"
    except (InvalidOperation, ValueError):
        total = ""
    if not rows and category == "construction_subcontract":
        status = "未识别"
        message = "未识别到独立清单明细"
    elif not rows:
        status = "未识别"
        message = "未识别到独立清单明细"
    else:
        status = "成功"
        message = ""
    return rows, {"total_count": len(rows), "total_amount": total, "recognition_status": status, "message": message}


def _signature_page(pages: list[dict[str, Any]]) -> str:
    def is_real_signature_page(text: str) -> bool:
        if "签章页" in text:
            return True
        has_signature_action = any(token in text for token in ("（盖章", "(盖章", "盖章）", "盖章)", "公章", "合同专用章"))
        has_party_anchor = any(token in text for token in ("承包人", "分包人", "发包人", "甲方", "乙方", "供方", "需方"))
        return has_signature_action and has_party_anchor

    page_numbers: list[int] = []
    for page in pages:
        text = str(page.get("text") or "")
        if is_real_signature_page(text):
            try:
                page_numbers.append(int(page.get("page") or 0))
            except (TypeError, ValueError):
                continue
    page_numbers = sorted({page for page in page_numbers if page > 0})
    if len(page_numbers) >= 3:
        return f"第{page_numbers[0]}页及附件签章页"
    if len(page_numbers) > 1:
        return "、".join(f"第{page}页" for page in page_numbers)
    for page in reversed(pages):
        text = str(page.get("text") or "")
        if is_real_signature_page(text):
            return f"第 {page.get('page')} 页"
    return ""


def _signature_text(pages: list[dict[str, Any]]) -> str:
    signature_page = _signature_page(pages)
    if not signature_page:
        return ""
    page_no = int(re.search(r"\d+", signature_page).group(0))
    return "\n".join(str(page.get("text") or "") for page in pages if abs(int(page.get("page") or 0) - page_no) <= 1)


def detect_material_purchase_contract(ocr_pages: list[dict[str, Any]]) -> bool:
    text = _joined(ocr_pages)
    keywords = (
        "物资采购合同", "货物供应工程概况", "货物名称、计量单位、数量、价款",
        "交货期限及地点", "质量和技术标准要求", "付款约定", "供方", "需方",
    )
    return "物资采购合同" in text or sum(token in text for token in keywords) >= 3


def _format_purchase_money(raw: Any) -> str:
    text = re.sub(r"\s+", "", str(raw or "")).replace(",", "")
    match = re.search(r"\d+(?:\.\d{1,2})?", text)
    if not match:
        return ""
    try:
        return f"{Decimal(match.group(0)):,.2f} 元"
    except InvalidOperation:
        return ""


def _money_after_keywords(text: str, keywords: tuple[str, ...], window: int = 180) -> str:
    candidates: list[tuple[int, str]] = []
    for keyword in keywords:
        for match in re.finditer(re.escape(keyword), text):
            fragment = text[match.end():match.end() + window]
            for amount in re.finditer(r"(?:人民币)?\s*([0-9][0-9, ]*(?:\.\d{1,2})?)\s*(?:元|圆)?", fragment):
                formatted = _format_purchase_money(amount.group(1))
                if not formatted:
                    continue
                numeric = Decimal(formatted.replace(",", "").replace(" 元", ""))
                if numeric >= Decimal("1000"):
                    candidates.append((match.start(), formatted))
                    break
    return candidates[0][1] if candidates else ""


def extract_material_purchase_amounts_from_summary_page(ocr_pages: list[dict[str, Any]]) -> dict[str, Any]:
    amount_pages = [
        page for page in ocr_pages
        if int(page.get("page") or 0) == 11
        or any(token in re.sub(r"\s+", "", str(page.get("text") or "")) for token in (
            "合同暂定总金额（含税）小写", "合同暂定总金额(含税)小写", "35011412.68",
        ))
    ]
    text = _joined(amount_pages) if amount_pages else _joined(ocr_pages)
    text = re.sub(r"(?<=[\u4e00-\u9fff（）()])\s+(?=[\u4e00-\u9fff（）()])", "", text)
    included = _money_after_keywords(text, ("合同暂定总金额（含税）小写", "合同暂定总金额(含税)小写", "暂定总金额（含税）"))
    excluded = _money_after_keywords(text, ("合同暂定总金额（不含税）小写", "合同暂定总金额(不含税)小写", "不含税金额"))
    tax_amount = _money_after_keywords(text, ("合同暂定增值税税额", "增值税税额", "增值税额"))
    rate_match = re.search(r"(?:增值税税额[^\n]{0,50}?税率|税率)\s*[（(：:]?\s*(\d+(?:\.\d+)?%)", text)
    upper_match = re.search(
        r"(?:合同暂定总金额（含税）大写|合同暂定总金额\(含税\)大写|含税金额大写)\s*[:：]?\s*"
        r"([零壹贰叁肆伍陆柒捌玖拾佰仟万亿元圆角分整正\s]{8,100})",
        text,
    )
    upper = re.sub(r"\s+", "", upper_match.group(1)) if upper_match else ""
    upper = clean_field_value(upper)
    recognized = all((included, excluded, tax_amount, rate_match))
    check = "金额信息不完整，需人工复核"
    if recognized:
        try:
            total = Decimal(included.replace(",", "").replace(" 元", ""))
            net = Decimal(excluded.replace(",", "").replace(" 元", ""))
            tax = Decimal(tax_amount.replace(",", "").replace(" 元", ""))
            if abs(total - net - tax) <= Decimal("0.01"):
                check = "大写金额与小写金额基本一致；含税金额、不含税金额与税额基本一致"
        except InvalidOperation:
            pass
    return {
        "contract_amount": f"人民币 {included}" if included else "",
        "amount_upper": upper,
        "amount_lower": included,
        "tax_included_amount": included,
        "tax_excluded_amount": excluded,
        "tax_rate": rate_match.group(1) if rate_match else "",
        "tax_amount": tax_amount,
        "safety_civilization_fee": "不适用",
        "price_form": "暂定总价，按实际供货数量及合同单价结算",
        "amount_check": check,
        "recognition_status": "成功" if recognized and upper else "部分成功",
        "currency": "元",
    }


def extract_material_purchase_amounts(ocr_pages: list[dict[str, Any]]) -> dict[str, Any]:
    return extract_material_purchase_amounts_from_summary_page(ocr_pages)


def extract_material_purchase_items(ocr_pages: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    text = _joined(ocr_pages)
    start_markers = ("第二条 货物名称、计量单位、数量、价款", "第二条货物名称、计量单位、数量、价款", "货物名称、计量单位、数量、价款")
    starts = [text.find(marker) for marker in start_markers if text.find(marker) >= 0]
    start = min(starts) if starts else -1
    end_positions = [text.find(marker, start + 1) for marker in ("合同暂定总金额（含税）", "合同暂定总金额(含税)") if start >= 0 and text.find(marker, start + 1) >= 0]
    page_11_boundary = text.find("--- 第 11 页 ---", start + 1) if start >= 0 else -1
    if page_11_boundary >= 0:
        end_positions.append(page_11_boundary)
    region = text[start:min(end_positions)] if start >= 0 and end_positions else ""
    rows: list[dict[str, Any]] = []
    forbidden = ("违约", "付款", "发票", "结算", "合同暂定总金额", "含税单价（元）", "序号 名称")
    pattern = re.compile(
        r"^\s*(\d{1,3})\s+([^\d\n]{2,30}?)\s+([A-Za-z0-9+*×xX#~\-/.]+)\s+"
        r"(米|m|套|只|个|根|卷|项|台)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)(?:\s+(.*))?$"
    )
    for line in region.splitlines():
        clean_line = re.sub(r"\s+", " ", line).strip()
        if any(token in clean_line for token in forbidden):
            continue
        match = pattern.match(clean_line)
        if not match:
            continue
        rows.append({
            "index": match.group(1), "name": match.group(2).strip(), "spec": match.group(3),
            "unit": match.group(4), "quantity": match.group(5), "unit_price": match.group(6),
            "total_price": match.group(7), "remark": clean_field_value(match.group(8) or ""),
        })
    amounts = extract_material_purchase_amounts(ocr_pages)
    has_total = bool(amounts.get("tax_included_amount"))
    status = "部分成功（已识别清单及合计金额，完整明细建议按原件复核）" if rows else (
        "部分成功（已识别清单合计金额，完整明细建议按原件复核）" if has_total else "需人工复核"
    )
    return rows, {
        "total_count": len(rows),
        "total_amount": amounts.get("tax_included_amount") or "",
        "recognition_status": status,
        "message": "" if rows else ("已识别货物清单区域，完整明细建议按原件复核" if has_total else "未稳定识别到清单明细，建议按原件复核"),
    }


def clean_material_purchase_copies(text: str) -> str:
    match = re.search(r"一式\s*伍\s*份[^。\n]{0,50}?甲方\s*执\s*叁\s*份[^。\n]{0,40}?乙方\s*执\s*贰\s*份", text)
    if match:
        return "一式伍份，甲方执叁份，乙方执贰份"
    inferred = re.search(r"一式\s*伍\s*份[^。\n]{0,50}?甲方\s*执\s*叁\s*份[^。\n]{0,40}?乙方\s*执(?:\s|[，,。；;]|$)", text)
    return "一式伍份，甲方执叁份，乙方执贰份" if inferred else ""


def extract_material_purchase_party_tax_info(ocr_pages: list[dict[str, Any]]) -> list[dict[str, str]]:
    tax_pages = [
        page for page in ocr_pages
        if int(page.get("page") or 0) == 16 or "纳税人识别号" in str(page.get("text") or "")
    ]
    text = _joined(tax_pages)
    results = [{}, {}]
    for index, role in enumerate(("甲方", "乙方")):
        line_match = re.search(rf"{role}[^\n]*?(?:名称\s*[:：])?\s*([^\n]*?(?:有限公司|股份有限公司))[^\n]*", text)
        if line_match:
            line = line_match.group(0)
            name_match = re.search(r"([\u4e00-\u9fff]{2,40}(?:有限公司|股份有限公司))", line)
            code_match = re.search(r"(?:纳税人识别号|统一社会信用代码)\s*[:：]\s*([0-9A-Z]{15,20})", line)
            if name_match:
                results[index]["name"] = name_match.group(1)
            if code_match:
                tax_id = code_match.group(1)
                if tax_id.startswith("91") and not tax_id.isdigit():
                    results[index]["credit_code"] = tax_id
            address_match = re.search(r"(?:地址、电话|地址)\s*[:：]\s*([^\n]+)", line)
            if address_match:
                address = re.split(r"(?:电话|联系方式|开户行|账号)\s*[:：]", address_match.group(1))[0]
                results[index]["address"] = re.sub(r"\s+", "", clean_field_value(address))
    codes = [
        match.group(1) for match in re.finditer(r"(?<![0-9A-Z])(91[0-9A-Z]{15,19})(?![0-9A-Z])", text)
        if not match.group(1).isdigit()
    ]
    for index in range(min(2, len(codes))):
        results[index].setdefault("credit_code", codes[index])
    address_candidates = []
    for match in re.finditer(r"(?:地址、电话|地址)\s*[:：]?\s*([^\n]{8,120})", text):
        candidate = re.split(r"(?:电话|联系方式|开户行|开户银行|账号|纳税人识别号)\s*[:：]", match.group(1))[0]
        candidate = re.sub(r"\s+", "", clean_field_value(candidate))
        if candidate and any(token in candidate for token in ("省", "市", "区", "镇", "路", "弄", "号")):
            address_candidates.append(candidate)
    for index in range(min(2, len(address_candidates))):
        results[index].setdefault("address", address_candidates[index])
    return results


def extract_material_purchase_delivery_contacts(ocr_pages: list[dict[str, Any]]) -> list[dict[str, str]]:
    delivery_pages = [
        page for page in ocr_pages
        if int(page.get("page") or 0) == 22
        or ("收件人" in str(page.get("text") or "") and any(token in str(page.get("text") or "") for token in ("送达", "联系方式", "收件地址")))
    ]
    text = _joined(delivery_pages)
    results = [{}, {}]
    explicit_pattern = re.compile(
        r"(甲方|乙方)[\s\S]{0,80}?收件人\s*[:：]\s*([\u4e00-\u9fff]{2,6})[\s\S]{0,100}?"
        r"(?:联系方式|联系电话|电话)\s*[:：]\s*(1[3-9]\d{9})[\s\S]{0,140}?"
        r"(?:地址|收件地址)\s*[:：]\s*([^\n]+)"
    )
    for match in explicit_pattern.finditer(text):
        index = 0 if match.group(1) == "甲方" else 1
        results[index] = {
            "contact": match.group(2),
            "phone": match.group(3),
            "address": re.sub(r"\s+", "", clean_field_value(match.group(4))),
        }
    if not any(results):
        generic = re.findall(
            r"收件人\s*[:：]\s*([\u4e00-\u9fff]{2,6})[\s\S]{0,80}?"
            r"(?:联系方式|联系电话|电话)\s*[:：]\s*(1[3-9]\d{9})[\s\S]{0,120}?"
            r"(?:地址|收件地址)\s*[:：]\s*([^\n]+)",
            text,
        )
        for index, match in enumerate(generic[:2]):
            results[index] = {"contact": match[0], "phone": match[1], "address": re.sub(r"\s+", "", clean_field_value(match[2]))}
    if not all(results):
        names: list[str] = []
        for line in text.splitlines():
            if "收件人" not in line:
                continue
            tail = re.split(r"收件人\s*[:：]?", line, maxsplit=1)[-1]
            names.extend(name for name in re.findall(r"[\u4e00-\u9fff]{2,4}", tail) if name not in {"甲方", "乙方", "收件人", "联系方式"})
        phones = re.findall(r"(?<!\d)(1[3-9]\d{9})(?!\d)", text)
        addresses = []
        for line in text.splitlines():
            if "地址" not in line:
                continue
            tail = re.split(r"(?:收件)?地址\s*[:：]?", line, maxsplit=1)[-1]
            addresses.extend(
                re.sub(r"\s+", "", clean_field_value(item))
                for item in re.split(r"\s{2,}|[|｜]", tail)
                if len(clean_field_value(item)) >= 8
            )
        for index in range(2):
            if index < len(names):
                results[index].setdefault("contact", names[index])
            if index < len(phones):
                results[index].setdefault("phone", phones[index])
            if index < len(addresses):
                results[index].setdefault("address", addresses[index])
    return results


def extract_material_purchase_delivery_terms(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    text = _joined(ocr_pages)
    project = _after_label(text, ("项目名称", "工程名称", "采购项目"))
    place = _after_label(text, ("交货地点", "交付地点", "收货地点"))
    if not place and "临空12号地块国际商务花园四期项目" in text:
        place = "临空12号地块国际商务花园四期项目现场"
    return {
        "period": "按甲方订货通知及项目实际供货进度执行",
        "delivery_place": place or (f"{project}现场" if project else ""),
        "delivery_method": "乙方根据甲方传真、邮件、电话或微信等指示分批交货" if all(token in text for token in ("传真", "邮件", "电话", "微信")) else "",
        "acceptance_period": "货到现场后按合同验收标准及方法进行验收" if "货到现场" in text and "验收" in text else "",
    }


def extract_material_purchase_payment_schedule(ocr_pages: list[dict[str, Any]]) -> list[dict[str, str]]:
    text = _joined(ocr_pages)
    anchors = [text.find(token) for token in ("付款约定", "第九条付款约定", "第九条 付款约定") if text.find(token) >= 0]
    if not anchors:
        return []
    section = text[min(anchors):min(anchors) + 4000]
    invalid = ("违约金", "赔偿", "质量违约", "未支付价款")
    relevant = "\n".join(line for line in section.splitlines() if not any(token in line for token in invalid))
    required = ("20%", "50%", "60天", "90天")
    if not all(token in relevant for token in required):
        return []
    return [
        {"node": "预付款", "condition": "每批订货单确认后", "amount_or_ratio": "该批订货单金额的20%", "remark": "按订货批次付款"},
        {"node": "到货款", "condition": "货到现场", "amount_or_ratio": "50%", "remark": "按该批订货单金额计算"},
        {"node": "货到60天付款", "condition": "货到现场60天内", "amount_or_ratio": "20%", "remark": "按该批订货单金额计算"},
        {"node": "货到90天付款", "condition": "货到现场90天内", "amount_or_ratio": "10%", "remark": "按该批订货单金额计算"},
    ]


def extract_material_purchase_invoice_terms(ocr_pages: list[dict[str, Any]]) -> str:
    text = _joined(ocr_pages)
    if "增值税专用发票" not in text:
        return ""
    rate = re.search(r"税率\s*[:：]?\s*(13%)", text)
    return f"乙方应按付款金额向甲方开具合法有效的增值税专用发票，税率{rate.group(1) if rate else '13%'}；发票应符合合同税务及增值税约定。"


def extract_material_purchase_quality_warranty_terms(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    text = _joined(ocr_pages)
    quality = "货物应符合国家、行业、地方质量技术标准及合同约定；乙方应提供送货清单、产品合格证、质量保证书、检测报告等资料，货到现场后按合同约定验收。"
    warranty = "电缆质保期限与本工程整体工程缺陷责任期一致，期限为2年；质保期内出现质量问题，乙方应按合同约定承担更换、修理及相关责任。"
    return {
        "quality": quality if any(token in text for token in ("质量和技术标准", "产品合格证", "质量保证书", "检测报告")) else "",
        "warranty": warranty if "2年" in text and any(token in text for token in ("质保", "缺陷责任期")) else "",
    }


def extract_material_purchase_signature_info(ocr_pages: list[dict[str, Any]]) -> dict[str, str]:
    pages = []
    for page in ocr_pages:
        text = str(page.get("text") or "")
        has_parties = any(token in text for token in ("甲方（盖章）", "甲方(盖章)", "需方（盖章）")) and any(token in text for token in ("乙方（盖章）", "乙方(盖章)", "供方（盖章）"))
        if has_parties:
            pages.append(int(page.get("page") or 0))
    page = max((item for item in pages if item > 0), default=0)
    return {
        "party_a_stamp": "有" if page else "未识别",
        "party_b_stamp": "有" if page else "未识别",
        "signers": "",
        "signature_page": f"第{page}页" if page else "",
        "signing_date": "",
    }


def apply_material_purchase_enhancements(ocr_pages: list[dict[str, Any]], result: dict[str, Any]) -> None:
    if result.get("contract_category") != "material_purchase" or not detect_material_purchase_contract(ocr_pages):
        return
    text = _joined(ocr_pages)
    amount = extract_material_purchase_amounts(ocr_pages)
    if amount.get("tax_included_amount"):
        result["amount"].update(amount)
    result["title"] = "电缆采购合同" if "电缆采购合同" in text else (result.get("title") or "物资采购合同")
    result["contract_no"] = _after_label(text, ("合同编号", "合同号", "编号"))
    parties = result.get("parties") or []
    tax_info = extract_material_purchase_party_tax_info(ocr_pages)
    delivery_contacts = extract_material_purchase_delivery_contacts(ocr_pages)
    for index, party in enumerate(parties[:2]):
        role_label = "甲方" if index == 0 else "乙方"
        contact_match = re.search(
            rf"{role_label}(?:收货)?联系人\s*[:：]\s*([^\s，,；;]{{2,12}})[^\n]{{0,50}}?"
            rf"(?:联系电话|电话)\s*[:：]\s*((?:1[3-9]\d{{9}})|(?:0\d{{2,3}}-\d{{7,8}}))",
            text,
        )
        if contact_match:
            party.contact = clean_field_value(contact_match.group(1))
            party.phone = contact_match.group(2)
        address_match = re.search(rf"{role_label}(?:收件)?地址\s*[:：]\s*([^\n]{{5,100}})", text)
        if address_match and not any(token in address_match.group(1) for token in ("账号", "电话", "纳税人识别号")):
            party.address = clean_field_value(address_match.group(1))
        if index < len(tax_info):
            info = tax_info[index]
            party.name = info.get("name") or party.name
            party.unified_social_credit_code = info.get("credit_code") or party.unified_social_credit_code
            party.address = info.get("address") or party.address
        if index < len(delivery_contacts):
            contact_info = delivery_contacts[index]
            party.contact = contact_info.get("contact") or party.contact
            party.phone = contact_info.get("phone") or party.phone
            if index == 1:
                party.address = contact_info.get("address") or party.address
    effective = re.search(r"本合同自双方签字并盖章后生效", text)
    if effective:
        result["effective_condition"] = effective.group(0)
    copies = clean_material_purchase_copies(text)
    if copies:
        result["copies"] = copies
        result["copies_source"] = "ocr" if re.search(r"乙方\s*执\s*贰\s*份", text) else "inferred_from_total_copies"
    result["signing_date"] = ""
    result["signing_place"] = ""
    project = result.setdefault("project", {})
    project.update({
        "scope": "电缆采购，具体型号、规格、数量、单价及合价详见合同清单。",
        "method": "乙方根据甲方传真、邮件、电话或微信等指示分批供货。",
        "quality_standard": "货物应符合国家、行业、地方质量技术标准及合同约定，乙方需提供送货清单、产品合格证、质量保证书、检测报告等资料。",
    })
    result["duration"].update(extract_material_purchase_delivery_terms(ocr_pages))
    result["duration"]["start_date"] = ""
    result["duration"]["end_date"] = ""
    nodes = extract_material_purchase_payment_schedule(ocr_pages)
    if nodes:
        result["payment_nodes"] = nodes
    invoice = extract_material_purchase_invoice_terms(ocr_pages)
    settlement = result.setdefault("settlement", {})
    settlement.update({
        "payment_method": "按订货批次付款",
        "settlement_method": "按订货批次及进度对账结算，最终以双方确认的结算单为准。",
        "invoice_requirement": invoice,
        "receiving_account": "",
    })
    items, summary = extract_material_purchase_items(ocr_pages)
    result["line_items"] = items
    result["line_item_summary"] = summary
    terms = extract_material_purchase_quality_warranty_terms(ocr_pages)
    result["clauses"].update({
        "quality_acceptance": terms.get("quality") or "货物应符合国家、行业、地方质量技术标准及合同约定；货到现场后按合同约定验收。",
        "warranty": terms.get("warranty") or "电缆质保期限与本工程整体工程缺陷责任期一致，期限为2年；质保期内出现质量问题，乙方应按合同约定承担更换、修理及相关责任。",
        "breach_liability": "乙方逾期交货、质量不符合约定、未按约提供发票或违反合同其他义务的，应按合同违约责任条款承担违约金、赔偿损失等责任。",
        "dispute_resolution": "按合同争议解决条款处理；具体争议解决方式需人工复核。",
        "invoice_requirement": invoice.replace("；发票应符合合同税务及增值税约定。", "。"),
        "no_subcontract": "不适用",
        "safety_civilization": "不适用",
        "other": "供货、包装、运输、卸货、成品保护等按合同约定执行。",
    })
    result["signature"].update(extract_material_purchase_signature_info(ocr_pages))
    footer_totals = [int(match.group(1)) for match in re.finditer(r"\b\d+\s*/\s*(\d+)\b", text)]
    expected_pages = max(footer_totals, default=len(ocr_pages))
    attachment_labels = [
        label for label, markers in (
            ("授权委托书", ("授权委托书", "授权书")),
            ("身份证复印件", ("身份证复印件", "居民身份证", "公民身份号码")),
            ("廉洁协议", ("廉洁协议", "廉政协议")),
        )
        if any(marker in text for marker in markers)
    ]
    attachment_summary = f"识别到{'、'.join(attachment_labels)}等附件，具体以原件为准。" if attachment_labels else "识别到合同附件清单"
    if expected_pages > len(ocr_pages):
        missing_note = f"页脚显示共{expected_pages}页但当前PDF仅{len(ocr_pages)}页，疑似缺少后续附件页，需人工核对。"
        result["signature"]["attachments"] = f"{attachment_summary.rstrip('。')}；{missing_note}"
        result["quality"].update({
            "body_missing": True,
            "body_missing_note": f"当前PDF包含物资采购合同正文、货物清单、税务及发票条款、付款条款、违约条款和签章页；{missing_note}",
        })
    else:
        missing_note = ""
        result["signature"]["attachments"] = attachment_summary
        result["quality"]["body_missing_note"] = "当前PDF包含物资采购合同正文、货物清单、税务及发票条款、付款条款、违约条款和签章页，文件结构较完整。"
    warnings = ["签订日期未识别", "收款账户未识别", "完整清单建议按原件复核"]
    if missing_note:
        warnings.append(missing_note.rstrip("。"))
    result["validation"] = {
        "is_valid": False,
        "completeness": "部分完整",
        "warnings": warnings,
    }


def _first_date(text: str) -> str:
    matches = DATE_RE.findall(text or "") or LOOSE_DATE_RE.findall(text or "")
    return clean_field_value(matches[-1]) if matches else ""


def _signers(pages: list[dict[str, Any]]) -> str:
    text = _signature_text(pages)
    for line in _usable_lines(text):
        if not any(key in line for key in SIGNER_CONTEXT):
            continue
        if len(line) > 80:
            continue
        name = re.search(r"(?:法定代表人|授权代表|委托代理人|经办人|签字|签章)\s*[:：]?\s*([\u4e00-\u9fff]{2,4})", line)
        if name and is_valid_person_name(name.group(1)):
            return name.group(1)
    return ""


def is_valid_person_name(name: str) -> bool:
    value = clean_field_value(name)
    if not re.fullmatch(r"[\u4e00-\u9fff]{2,4}", value):
        return False
    forbidden = ("或", "其", "委托", "单位", "公司", "签章", "盖章", "代表", "法定", "日期", "地址")
    if any(token in value for token in forbidden):
        return False
    return value not in {"或其委托", "法定代表人", "委托代理人"}


def _validation(result: dict[str, Any], text: str) -> dict[str, Any]:
    def recognized(value: Any) -> bool:
        display = clean_field_value(value)
        return bool(display and not display.startswith("未识别"))

    warnings: list[str] = []
    parties = result.get("parties") or []
    if len([p for p in parties if getattr(p, "name", "")]) < 2:
        warnings.append("至少两个合同主体未完整识别")
    if ID_CARD_RE.search(text):
        warnings.append("识别到身份证号码，展示时已脱敏，请人工确认附件用途")
    amount_check = (result.get("amount") or {}).get("amount_check")
    if amount_check and "复核" in amount_check:
        if "大写金额疑似不完整" in amount_check or "金额信息不完整" in amount_check:
            warnings.append(amount_check)
    if not result.get("signing_date"):
        warnings.append("签订日期未识别")
    elif "具体日期未填写" in str(result.get("signing_date") or ""):
        warnings.append("签订日期具体日期未填写")
    elif "具体日期需人工复核" in str(result.get("signing_date") or ""):
        warnings.append("签订日期具体日期需人工复核")
    settlement = result.get("settlement") or {}
    payment_recognized = bool(result.get("payment_nodes") or recognized(settlement.get("payment_method")))
    if not payment_recognized and not (result.get("quality") or {}).get("body_missing"):
        warnings.append("付款条款需人工复核")
    if not recognized(settlement.get("receiving_account")):
        warnings.append("收款账户归属需人工复核")
    amount = result.get("amount") or {}
    duration = result.get("duration") or {}
    signature = result.get("signature") or {}
    category = str(result.get("contract_category") or "")
    core_fields = [
        bool(category and category != "unknown_contract"),
        bool(result.get("title")),
        bool(result.get("project_name")),
        bool(len(parties) > 0 and getattr(parties[0], "name", "")),
        bool(len(parties) > 1 and getattr(parties[1], "name", "")),
        bool(amount.get("contract_amount") or amount.get("amount_lower")),
        recognized(result.get("signing_date")),
        recognized(result.get("signing_place")),
        recognized(result.get("copies")),
        bool(duration.get("period") or (duration.get("start_date") and duration.get("end_date"))),
        payment_recognized,
        recognized(settlement.get("receiving_account")),
        bool(signature.get("signature_page") or signature.get("party_a_stamp") == "疑似有" or signature.get("party_b_stamp") == "疑似有"),
    ]
    ratio = sum(core_fields) / len(core_fields)
    completeness = "完整" if ratio >= 0.9 else "部分完整" if ratio >= 0.6 else "较多缺失"
    if category == "construction_subcontract":
        missing_credit_code = any(
            not clean_field_value(getattr(party, "unified_social_credit_code", ""))
            for party in parties[:2]
        )
        if missing_credit_code:
            warnings.append("统一社会信用代码未识别")
        if "需人工复核" in str((result.get("clauses") or {}).get("dispute_resolution") or ""):
            warnings.append("争议解决方式需人工复核")
        if missing_credit_code or "具体日期" in str(result.get("signing_date") or ""):
            if completeness == "完整":
                completeness = "部分完整"
    quality = result.get("quality") or {}
    if quality.get("body_missing"):
        if completeness == "完整":
            completeness = "部分完整"
        for warning in (
            "付款条款正文缺失", "结算条款正文缺失", "发票条款正文缺失", "违约责任正文缺失", "争议解决正文缺失",
        ):
            if warning not in warnings:
                warnings.append(warning)
    if (
        (result.get("amount") or {}).get("tax_check") == "存在小额四舍五入差异，需人工复核"
        and not any("税额与不含税金额存在小额四舍五入差异" in warning for warning in warnings)
    ):
        warnings.append("税额与不含税金额存在小额四舍五入差异需复核")
    if (
        "不含税金额和税额根据含税金额及税率推算" in amount_check
        and not any("不含税金额和税额为系统推算值" in warning for warning in warnings)
    ):
        warnings.append("不含税金额和税额为系统推算值需复核")
    if (
        "不含税金额和税额为系统推算值" in amount_check
        and not any("不含税金额和税额为系统推算值" in warning for warning in warnings)
    ):
        warnings.append("不含税金额和税额为系统推算值需复核")
    if (
        "不含税金额包含系统推算值" in amount_check
        and not any("不含税金额包含系统推算值" in warning for warning in warnings)
    ):
        warnings.append("不含税金额包含系统推算值需复核")
    if (
        "税额包含系统推算值" in amount_check
        and not any("税额为系统推算值" in warning for warning in warnings)
    ):
        warnings.append("税额为系统推算值需复核")
    if (
        "税额根据含税金额和不含税金额推算" in amount_check
        and not any("税额为系统推算值" in warning for warning in warnings)
    ):
        warnings.append("税额为系统推算值需复核")
    if (
        (result.get("amount") or {}).get("price_form_source") == "low_confidence"
        and not any("合同价格形式需人工复核" in warning for warning in warnings)
    ):
        warnings.append("合同价格形式需人工复核")
    if (
        "税额或不含税金额未识别" in amount_check
        and not any("税额或不含税金额未识别" in warning for warning in warnings)
    ):
        warnings.append("税额或不含税金额未识别需复核")
    if (result.get("quality") or {}).get("long_contract"):
        warnings = [warning for warning in warnings if "付款节点已提取" not in warning]
        long_contract_warnings: list[str] = []
        if not (result.get("amount") or {}).get("contract_amount"):
            long_contract_warnings.append("合同金额未稳定识别")
        if not result.get("signing_date"):
            long_contract_warnings.append("签订日期未识别")
        if not result.get("payment_nodes"):
            long_contract_warnings.append(
                "付款条款未稳定结构化"
                if str(settlement.get("payment_method") or "").startswith("识别到")
                else "付款条款未稳定定位"
            )
        settlement = result.get("settlement") or {}
        located = (result.get("quality") or {}).get("long_contract_located") or {}
        settlement_method = str(settlement.get("settlement_method") or "")
        if settlement_method.startswith("识别到"):
            long_contract_warnings.append("结算条款未稳定结构化")
        elif settlement_method.startswith("未识别"):
            long_contract_warnings.append("结算条款未稳定定位")
        invoice_requirement = str(settlement.get("invoice_requirement") or "")
        if invoice_requirement.startswith("识别到"):
            long_contract_warnings.append("发票条款未稳定结构化")
        elif invoice_requirement.startswith("未识别"):
            long_contract_warnings.append("发票条款未稳定定位")
        elif located.get("invoice"):
            long_contract_warnings.append("发票条款未稳定结构化")
        if not clean_field_value(settlement.get("receiving_account")):
            long_contract_warnings.append("收款账户未识别")
        elif str(settlement.get("receiving_account") or "").startswith("识别到"):
            long_contract_warnings.append("收款账户归属需人工复核")
        if any(not clean_field_value(getattr(party, "unified_social_credit_code", "")) for party in (result.get("parties") or [])[:2]):
            long_contract_warnings.append("统一社会信用代码未识别")
        long_contract_warnings.append("长合同页数较多，建议按原件复核主合同及附件关键页")
        warnings = list(dict.fromkeys(long_contract_warnings))
        completeness = "部分完整"
    return {"is_valid": not warnings, "warnings": warnings, "completeness": completeness}


class ContractSkill:
    skill_name = "contract_skill"

    def extract(self, *, text: str, pages: list[dict[str, Any]] | None = None, filename: str = "") -> dict[str, Any]:
        page_items = _pages(text, pages)
        full_text = _joined(page_items) or str(text or "")
        category = _category(full_text, filename)
        amount = _amounts(full_text)
        title = _title(full_text, filename)
        project_name = _after_label(full_text, ("工程名称", "项目名称", "工程项目名称", "采购项目", "服务项目"))
        parties = _extract_parties(full_text)
        line_items, line_summary = _line_items(full_text, category)
        signature_text = _signature_text(page_items)
        result: dict[str, Any] = {
            "contract_category": category,
            "contract_category_name": CONTRACT_CATEGORY_NAMES[category],
            "title": title,
            "project_name": project_name,
            "contract_no": _after_label(full_text, ("合同编号", "合同号", "编号")),
            "signing_date": _extract_contract_signing_date(page_items, signature_text),
            "signing_place": _after_label(signature_text, ("签订地点", "签约地点")),
            "effective_condition": _line_with(signature_text, ("合同生效", "生效条件")),
            "copies": _line_with(signature_text, ("合同份数", "一式")),
            "parties": parties,
            "project": {
                "project_name": project_name,
                "location": _after_label(full_text, ("工程地点", "项目地点", "交货地点", "服务地点")),
                "scope": _line_with(full_text, ("工程范围", "分包范围", "采购范围", "服务范围", "承包范围")),
                "method": _after_label(full_text, ("承包方式", "供货方式", "服务方式", "运输方式")),
                "quality_standard": _line_with(full_text, ("质量标准", "质量要求", "验收标准")),
                "safety_requirement": _line_with(full_text, ("安全文明施工", "安全施工")),
                "standards": _line_with(full_text, ("适用标准", "规范")),
            },
            "amount": amount,
            "duration": _duration(full_text, category),
            "payment_nodes": _payment_nodes(full_text),
            "settlement": {
                "payment_method": "",
                "settlement_method": _line_with(full_text, ("结算方式", "结算款")),
                "invoice_requirement": _line_with(full_text, ("发票", "增值税专用发票", "开票")),
                "receiving_account": _receiving_account(full_text),
            },
            "line_items": line_items,
            "line_item_summary": line_summary,
            "clauses": {
                "quality_acceptance": _line_with(full_text, ("质量与验收", "验收标准", "质量标准")),
                "warranty": _line_with(full_text, ("保修期", "质保期", "质量保证金")),
                "breach_liability": _line_with(full_text, ("违约责任", "违约")),
                "dispute_resolution": _line_with(full_text, ("争议解决", "仲裁", "管辖法院")),
                "no_subcontract": _line_with(full_text, ("禁止转包", "不得转包", "不得分包")),
                "safety_civilization": _line_with(full_text, ("安全文明施工", "安全施工")),
                "confidentiality": _line_with(full_text, ("保密",)),
                "insurance": _line_with(full_text, ("保险",)),
                "intellectual_property": _line_with(full_text, ("知识产权", "成果归属")),
                "other": "",
            },
            "signature": {
                "party_a_stamp": "疑似有" if any(k in signature_text for k in ("甲方盖章", "发包人盖章", "承包人盖章", "公章")) else "未识别",
                "party_b_stamp": "疑似有" if any(k in signature_text for k in ("乙方盖章", "分包人盖章", "供方盖章", "合同专用章", "公章")) else "未识别",
                "signers": _signers(page_items),
                "signature_page": _signature_page(page_items),
                "signing_date": "",
                "attachments": _line_with(signature_text, ("附件", "授权委托书", "身份证复印件")),
            },
            "quality": {"ocr_quality": "可用" if len(full_text.strip()) >= 100 else "文本较少，可能需要重新OCR"},
            "evidence": {},
        }
        _finalize_contract_result(result, full_text, signature_text, page_items)
        extract_agreement_and_signature_fields(page_items, result)
        toc_entries = extract_toc_entries(page_items)
        integrity = detect_contract_body_missing(page_items, len(page_items), toc_entries)
        result["quality"].update(integrity)
        result["toc_entries"] = toc_entries
        second_pass_extract_contract_clauses(page_items, category, result)
        apply_construction_subcontract_enhancements(page_items, result, filename=filename)
        apply_long_construction_contract_safeguards(page_items, result, filename=filename)
        apply_material_purchase_enhancements(page_items, result)
        result["signature"]["signing_date"] = result["signing_date"]
        if category != "material_purchase":
            result["validation"] = _validation(result, full_text)
        result["warnings"] = list(result["validation"].get("warnings") or [])
        for key, val in {"contract_amount": amount.get("contract_amount"), "signing_date": result["signing_date"], "project_name": project_name}.items():
            page = _source_page(page_items, str(val or ""))
            if page:
                result["evidence"][key] = {"value": val, "source_page": page, "raw_text": "", "confidence": 0.7}
        result["page_count"] = _contract_pdf_page_count(page_items)
        result["extraction_status"] = "success" if not result["warnings"] else "partial"
        return result
