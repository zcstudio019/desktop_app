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
    if re.fullmatch(r"(第?[一二三四五六七八九十百]+[章节条部分]?|\d+(?:\.\d+)*)(、|\.|\s)?[^，。；:：]{0,24}", text):
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
    text = re.sub(r"_+\s*([一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]+)\s*_+", r"\1", text)
    text = re.sub(r"\s+", "", text)
    return clean_field_value(text)


def _normalize_construction_scope(value: Any) -> str:
    text = clean_field_value(value)
    if not text or is_bad_clause_value(text):
        return "机电安装工程相关专业分包内容，具体以合同正文及附件为准"
    return text


def _normalize_construction_method(value: Any, full_text: str) -> str:
    text = clean_field_value(value)
    source = f"{text}\n{full_text}"
    if any(token in source for token in ("包工包料", "包工期", "包质量", "包安全", "包文明施工", "鍖呭伐鍖呮枡", "鍖呭伐鏈?", "鍖呰川閲?", "鍖呭畨鍏?", "鍖呮枃鏄庢柦宸?")):
        return "包工包料、包工期、包质量、包安全、包文明施工等"
    return "" if is_bad_clause_value(text) else text


def _normalize_quality(value: Any, full_text: str) -> str:
    text = clean_field_value(value)
    source = f"{text}\n{full_text}"
    if "\u4e00\u6b21\u6027\u9a8c\u6536\u5408\u683c" in source:
        return "\u4e00\u6b21\u6027\u9a8c\u6536\u5408\u683c"
    return "" if is_bad_clause_value(text) else text


def _normalize_period(value: Any) -> str:
    text = clean_field_value(value)
    match = re.search(r"(\d{1,5})\s*(?:天|日历天|澶?)", text)
    if match:
        return f"{match.group(1)}天"
    return "" if is_bad_clause_value(text) else text


def _normalize_signing_date(*texts: str) -> str:
    date_re = re.compile(r"((?:19|20)\d{2})\s*(?:年|[-/])\s*(\d{1,2})\s*(?:月|[-/])\s*(\d{1,2})\s*(?:日)?")
    for text in texts:
        for match in date_re.finditer(text or ""):
            year, month, day = match.groups()
            if month and day:
                return f"{int(year)}年{int(month)}月{int(day)}日"
    return ""



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
def _merge_receiving_account(text: str, parties: list[ContractParty] | None = None) -> str:
    lines = _usable_lines(text)
    bank = ""
    account = ""
    phone_markers = ("电话", "联系电话", "联系方式", "手机", "鐢佃瘽", "鑱旂郴", "閻絻", "閼辨梻")
    account_markers = ("开户银行", "开户行", "银行账号", "账号", "帐号", "收款账户", "寮€鎴", "璐﹀彿", "甯愬彿", "鏀舵")
    for index, line in enumerate(lines):
        if any(marker in line for marker in phone_markers):
            continue
        if not any(marker in line for marker in account_markers):
            continue
        match = re.search(r"(?<!\d)(\d{8,30})(?!\d)", line)
        if match and not account:
            account = match.group(1)
        normalized_line = line.replace("\uff1b", ";").replace("\uff1a", ":").replace("\uff0c", ",")
        if match and not bank:
            before_account = normalized_line[:match.start()]
            bank_segment = before_account.split(";")[0].split(",")[0].strip(" -_:")
            for label in ("\u5f00\u6237\u94f6\u884c", "\u5f00\u6237\u884c"):
                bank_segment = bank_segment.replace(label, "")
            if bank_segment:
                bank = clean_field_value(bank_segment)
        if not bank:
            bank_candidate = re.sub(r"(?<!\d)\d{8,30}(?!\d)", "", line)
            bank_candidate = bank_candidate.replace("\uff1b", ";").replace("\uff1a", ":").replace("\uff0c", ",")
            bank_candidate = re.split(r"[;,:]", bank_candidate)[0].strip(" -_:\\uff1a;\\uff1b,\\uff0c")
            if bank_candidate and not re.fullmatch(r"[\W_]+", bank_candidate):
                bank = bank_candidate
        if not account and index + 1 < len(lines):
            next_match = re.search(r"(?<!\d)(\d{8,30})(?!\d)", lines[index + 1])
            if next_match and not any(marker in lines[index + 1] for marker in phone_markers):
                account = next_match.group(1)
    for party in parties or []:
        account_text = str(getattr(party, "bank_account", "") or "")
        match = re.search(r"(?<!\d)(\d{8,30})(?!\d)", account_text)
        if match and not account:
            account = match.group(1)
        bank_name = str(getattr(party, "bank_name", "") or "")
        if bank_name and not bank:
            bank = bank_name
    bank = _clean_bank_name(bank, account)
    if bank and account:
        return f"\u5f00\u6237\u94f6\u884c\uff1a{bank}\uff1b\u8d26\u53f7\uff1a{account}"
    if bank:
        return f"\u5f00\u6237\u94f6\u884c\uff1a{bank}"
    if account:
        return f"\u8d26\u53f7\uff1a{account}"
    return ""


def _finalize_contract_result(result: dict[str, Any], full_text: str, signature_text: str) -> None:
    category = str(result.get("contract_category") or "")
    if not result.get("signing_date"):
        result["signing_date"] = _normalize_signing_date(signature_text, "\n".join(full_text.splitlines()[-80:]), full_text)
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
    settlement["receiving_account"] = _merge_receiving_account(full_text, result.get("parties") or []) or clean_field_value(settlement.get("receiving_account"))
    clauses = result.get("clauses") if isinstance(result.get("clauses"), dict) else {}
    clauses["quality_acceptance"] = _normalize_quality(clauses.get("quality_acceptance"), full_text)
    clauses["warranty"] = _safe_clause(clauses.get("warranty"))
    clauses["safety_civilization"] = _safe_clause(clauses.get("safety_civilization"))
    if any(token in full_text for token in ("安全文明施工费为0元", "安全文明施工费为 0 元", "瀹夊叏鏂囨槑鏂藉伐璐逛负0鍏?", "瀹夊叏鏂囨槑鏂藉伐璐逛负 0 鍏?")):
        clauses["safety_civilization"] = "安全文明施工费为 0 元"
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
            result.append({"page": int(page.get("page") or idx), "text": str(page.get("text") or "")})
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
    for label in labels:
        for pattern in (rf"{re.escape(label)}\s*[:：]\s*([^\n\r]+)", rf"{re.escape(label)}\s*[（(][^）)]*[）)]\s*[:：]?\s*([^\n\r]+)"):
            match = re.search(pattern, text)
            if match:
                candidate = clean_party_name(match.group(1))
                if candidate and not re.search(r"(地址|账号|电话|联系人|开户)", candidate):
                    return candidate[:80]
    return ""


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
        parties.append(ContractParty(
            role=role,
            name=_party_name(text, labels),
            unified_social_credit_code=USCC_RE.search(block).group(1) if USCC_RE.search(block) else (usccs[index] if index < len(usccs) else ""),
            legal_representative=_after_label(block, ("法定代表人", "授权代表", "代表人")),
            contact=_after_label(block, ("联系人",)),
            phone=_context_phone(block),
            address=_after_label(block, ("地址", "住所", "通讯地址")),
            bank_name=_after_label(block, ("开户银行", "开户行")),
            bank_account=_context_bank_account(block),
            taxpayer_id=_after_label(block, ("纳税人识别号", "税号")),
            stamp_status="疑似已盖章" if any(token in block for token in ("盖章", "公章", "合同专用章")) else "",
        ))
    return parties


def _duration(text: str, category: str) -> dict[str, Any]:
    start = _after_label(text, ("计划开工日期", "开工日期", "服务开始时间", "开始日期", "交货时间"))
    end = _after_label(text, ("计划竣工日期", "竣工日期", "服务结束时间", "结束日期"))
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
    warnings: list[str] = []
    parties = result.get("parties") or []
    if len([p for p in parties if getattr(p, "name", "")]) < 2:
        warnings.append("至少两个合同主体未完整识别")
    if ID_CARD_RE.search(text):
        warnings.append("识别到身份证号码，展示时已脱敏，请人工确认附件用途")
    amount_check = (result.get("amount") or {}).get("amount_check")
    if amount_check and amount_check != "一致":
        warnings.append(amount_check)
    for label, key in (("付款条款", "payment_nodes"),):
        if not result.get(key):
            warnings.append(f"{label}需人工复核")
    recognized_keys = 0
    for key in ("title", "project_name", "contract_no", "signing_date"):
        recognized_keys += 1 if result.get(key) else 0
    recognized_keys += sum(1 for p in parties if getattr(p, "name", ""))
    completeness = "完整" if recognized_keys >= 5 else "部分完整" if recognized_keys >= 2 else "较低"
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
            "signing_date": _after_label(signature_text or full_text, ("签订日期", "签约日期")) or _first_date(signature_text or full_text),
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
        _finalize_contract_result(result, full_text, signature_text)
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
