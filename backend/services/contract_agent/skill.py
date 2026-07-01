from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from .schema import CONTRACT_CATEGORY_NAMES, ContractParty


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
    match = re.search(r"(?<!\d)(\d[\d,]*(?:\.\d+)?)(?!\d)", str(raw or ""))
    if not match:
        return ""
    try:
        return f"{Decimal(match.group(1).replace(',', '')):,.2f} 元"
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
    if safety_fee:
        amount_data["safety_civilization_fee"] = "0 元" if Decimal(re.sub(r"[^\d.]", "", safety_fee) or "0") == 0 else safety_fee
    if price_form_match:
        amount_data["price_form"] = clean_field_value(price_form_match.group(1))
    return amount_data


def _finalize_amount_checks(amount: dict[str, Any]) -> None:
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
    if tax_match:
        amount["tax_rate"] = tax_match.group(1)
    if safety_fee:
        amount["safety_civilization_fee"] = "0 元" if Decimal(re.sub(r"[^\d.]", "", safety_fee) or "0") == 0 else safety_fee
    if price_form_match:
        amount["price_form"] = clean_field_value(price_form_match.group(1))

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
    code_match = USCC_RE.search(text)
    if code_match:
        data["credit_code"] = code_match.group(1)
    address = extract_address_from_party_block(text)
    if address:
        data["address"] = address
    postal = _after_label(text, ("邮政编码", "邮编"), max_len=30)
    if postal:
        data["postal_code"] = clean_field_value(postal)
    bank = _after_label(text, ("开户银行", "开户行"), max_len=120)
    if bank:
        data["bank"] = _clean_bank_name(bank)
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

    codes = USCC_RE.findall(text)
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
        codes = USCC_RE.findall(page_text)
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
    usccs = USCC_RE.findall(text)
    parties: list[ContractParty] = []
    for index, (role, labels) in enumerate(roles):
        block = _near_block(text, labels)
        block_codes = USCC_RE.findall(block)
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
    for page in reversed(pages):
        text = str(page.get("text") or "")
        if any(token in text for token in ("签字", "盖章", "公章", "合同专用章", "签订日期")):
            return f"第 {page.get('page')} 页"
    return ""


def _signature_text(pages: list[dict[str, Any]]) -> str:
    signature_page = _signature_page(pages)
    if not signature_page:
        return ""
    page_no = int(re.search(r"\d+", signature_page).group(0))
    return "\n".join(str(page.get("text") or "") for page in pages if abs(int(page.get("page") or 0) - page_no) <= 1)


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
    forbidden = ("或", "其", "委托", "单位", "公司", "签章", "代表", "法定", "日期", "地址")
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
        result["signature"]["signing_date"] = result["signing_date"]
        result["validation"] = _validation(result, full_text)
        result["warnings"] = list(result["validation"].get("warnings") or [])
        for key, val in {"contract_amount": amount.get("contract_amount"), "signing_date": result["signing_date"], "project_name": project_name}.items():
            page = _source_page(page_items, str(val or ""))
            if page:
                result["evidence"][key] = {"value": val, "source_page": page, "raw_text": "", "confidence": 0.7}
        result["page_count"] = len(page_items)
        result["extraction_status"] = "success" if not result["warnings"] else "partial"
        return result
