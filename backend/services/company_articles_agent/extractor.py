from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass
from itertools import combinations
from typing import Any

from .schema import Shareholder

logger = logging.getLogger(__name__)
SHAREHOLDER_LOG_PREFIX = "[CompanyArticles][ShareholderExtract]"


@dataclass
class ShareholderCandidate:
    shareholder: Shareholder
    source: str
    in_shareholder_block: bool
    confidence: float
    raw_text: str = ""


@dataclass(slots=True)
class ExternalNameCandidate:
    name: str
    source_page: int
    source_page_type: str
    raw_context: str
    confidence: float


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or ""))


def clean_value(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[ \t\r\f\v\u3000_—–]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip(" ：:，,。；;")


def clean_clause(value: str) -> str:
    text = clean_value(value)
    text = re.sub(r"\s*\n\s*", "", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"(?:第[一二三四五六七八九十]+章.*|第[一二三四五六七八九十]+条.*|公司经营范围.*)$", "", text)
    return text.strip(" ：:，,。；;")


def first_match(text: str, patterns: list[str], flags: int = re.S) -> str:
    for pattern in patterns:
        match = re.search(pattern, text or "", flags)
        if match:
            return clean_value(match.group(1))
    return ""


def parse_amount_number(value: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", str(value or "").replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


COMPANY_SUFFIX_PATTERN = r"(?:股份有限公司|有限责任公司|有限公司)"
INVALID_ARTICLES_TITLES = {
    "公司章程", "本公司章程", "及本公司章程", "新的公司章程",
    "通过公司新的章程", "修改公司章程", "本章程",
    "),股份有限公司章程", "),有限公司章程", ",股份有限公司章程",
    "，股份有限公司章程",
}


def is_valid_articles_title(value: str) -> bool:
    title = clean_clause(value)
    if not title or title in INVALID_ARTICLES_TITLES:
        return False
    if title[0] in "),，,、（() ":
        return False
    match = re.fullmatch(
        rf"(?P<name>[\u4e00-\u9fffA-Za-z0-9（）()·\-]{{4,80}}{COMPANY_SUFFIX_PATTERN})章程",
        title,
    )
    if not match:
        return False
    company_part = match.group("name")
    chinese_count = len(re.findall(r"[\u4e00-\u9fff]", company_part))
    suffix = re.search(rf"{COMPANY_SUFFIX_PATTERN}$", company_part)
    stem = company_part[:suffix.start()] if suffix else ""
    return chinese_count >= 4 and len(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]", stem)) >= 2


def is_valid_company_name(value: str) -> bool:
    name = clean_clause(value)
    if not name or name[0] in "),，,、（() ":
        return False
    match = re.fullmatch(
        rf"[\u4e00-\u9fffA-Za-z0-9（）()·\-]{{4,80}}{COMPANY_SUFFIX_PATTERN}",
        name,
    )
    if not match:
        return False
    suffix = re.search(rf"{COMPANY_SUFFIX_PATTERN}$", name)
    stem = name[:suffix.start()] if suffix else ""
    return len(re.findall(r"[\u4e00-\u9fffA-Za-z0-9]", stem)) >= 2


def clean_articles_title(title: str, company_name: str = "") -> str:
    value = clean_clause(title)
    if not is_valid_articles_title(value):
        value = ""
    if not value and is_valid_company_name(company_name):
        value = f"{company_name}章程"
    return value


def extract_title(text: str) -> tuple[str, str]:
    head = str(text or "")[:800]
    boundaries = [position for position in (head.find("依据《"), head.find("第一章")) if position >= 0]
    title_region = head[:min(boundaries)] if boundaries else head
    candidates = re.findall(
        rf"([\u4e00-\u9fffA-Za-z0-9（）()·\-]{{4,80}}{COMPANY_SUFFIX_PATTERN}章程)",
        title_region,
    )
    title = ""
    for candidate in candidates:
        candidate = clean_articles_title(candidate)
        if candidate:
            title = candidate
            break
    company_name = title[:-2] if title.endswith("章程") else ""
    return title, company_name


def extract_company_name(text: str, title_company_name: str = "") -> str:
    value = first_match(
        text,
        [
            rf"第一条\s*公司名称[：:\s]*([^\n。；;]{{4,80}}{COMPANY_SUFFIX_PATTERN})",
            rf"公司名称[：:\s]*([^\n。；;]{{4,80}}{COMPANY_SUFFIX_PATTERN})",
            rf"名称[：:\s]*([^\n。；;]{{4,80}}{COMPANY_SUFFIX_PATTERN})",
        ],
    )
    value = clean_clause(value)
    if is_valid_company_name(value):
        return value
    return title_company_name if is_valid_company_name(title_company_name) else ""


def clean_company_address(address: str, company_name: str = "") -> str:
    value = clean_clause(address)
    value = re.split(
        r"(?:第二章|第三条|公司经营范围|经营范围|公司注册资本|第[一二三四五六七八九十]+章|第[三四五六七八九十]+条)",
        value,
        maxsplit=1,
    )[0]
    if "；上海" in value:
        left, right = value.split("；", 1)
        if any(token in right for token in ("公司", "项目管理", "有限")):
            value = left
    if company_name:
        value = value.split(company_name, 1)[0]
        for size in range(min(len(company_name), 12), 5, -1):
            fragment = company_name[:size]
            if fragment in value:
                value = value.split(fragment, 1)[0]
                break
    value = re.sub(r"[；;、，,]\s*上海[\u4e00-\u9fff]{2,20}(?:公司|有限|项目管.*)?$", "", value)
    return value.rstrip("；;、，,。 ")


ADDRESS_FEATURES = ("省", "市", "区", "县", "路", "街", "弄", "号", "室", "楼", "镇", "村", "园", "大厦")
ADDRESS_INVALID_TOKENS = (
    "法定代表人", "注册资本", "公司类型", "经营范围", "股东", "股东名称",
    "出资额", "出资方式", "申请", "备案", "登记", "有限责任公司股东",
)


def is_valid_company_address(value: str) -> bool:
    address = clean_clause(value)
    if not address or any(token in address for token in ADDRESS_INVALID_TOKENS):
        return False
    return sum(1 for token in ADDRESS_FEATURES if token in address) >= 2


def extract_company_address(text: str, company_name: str = "") -> str:
    value = first_match(
        text,
        [
            r"第二条\s*公司住所[：:\s]*([\s\S]{4,180}?)(?=\s*(?:第二章|第三条|公司经营范围|经营范围|公司注册资本|\n第[一二三四五六七八九十]+章|\n第[三四五六七八九十]+条))",
            r"公司住所[：:\s]*([\s\S]{4,180}?)(?=\s*(?:第二章|第三条|公司经营范围|经营范围|公司注册资本|\n第[一二三四五六七八九十]+章|\n第[三四五六七八九十]+条))",
            r"住所[：:\s]*([^\n。；;]{4,180})",
        ],
    )
    value = clean_company_address(value, company_name)
    return value if is_valid_company_address(value) else ""


def extract_business_scope(text: str) -> str:
    value = first_match(
        text,
        [
            r"第三条\s*公司经营范围[：:\s]*([\s\S]+?)(?=\s*(?:第三章|第四条|公司注册资本))",
            r"经营范围[：:\s]*([\s\S]+?)(?=\s*(?:第三章|第四条|公司注册资本))",
        ],
    )
    return clean_clause(value)


def normalize_for_amount_match(text: str) -> str:
    return re.sub(r"[\s\u3000_—–]+", "", str(text or ""))


def extract_registered_capital(text: str) -> tuple[str, float | None, str]:
    source = normalize_for_amount_match(text)
    patterns = [
        r"第四条公司注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
        r"公司注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
        r"注册资本[:：]?人民币([0-9]+(?:\.[0-9]+)?)万元",
    ]
    for pattern in patterns:
        match = re.search(pattern, source)
        if match:
            amount = float(match.group(1))
            return f"人民币{amount:g}万元", amount, "人民币"
    value = first_match(
        text,
        [
            r"公司注册资本[：:\s]*((?:人民币|RMB)?\s*[_—–]?\s*\d+(?:\.\d+)?\s*万\s*元)",
            r"注册资本[：:\s]*((?:人民币|RMB)?\s*[_—–]?\s*\d+(?:\.\d+)?\s*万\s*元)",
        ],
    )
    value = clean_clause(value).replace("万 元", "万元")
    if value and not value.startswith("人民币") and re.search(r"\d", value):
        value = f"人民币{value}"
    return value, parse_amount_number(value), "人民币" if value else ""


def normalize_shareholder_text(text: str) -> str:
    fullwidth = str.maketrans("０１２３４５６７８９．／－：，｜（）", "0123456789./-:,|()")
    source = str(text or "").translate(fullwidth)
    source = source.replace("│", "|").replace("┃", "|").replace("｜", "|")
    source = re.sub(r"[-—–_]{2,}", " ", source)
    source = re.sub(r"[|]+", " | ", source)
    source = re.sub(r"[ \t\u3000]+", " ", source)
    source = re.sub(r"\n\s*\|?\s*(?:-+\s*\|?\s*){2,}\n", "\n", source)
    return source


def strip_shareholder_headers(text: str) -> str:
    source = str(text or "")
    headers = [
        "股东的姓名或者名称",
        "股东姓名/名称",
        "股东姓名",
        "股东名称",
        "发起人姓名或者名称",
        "发起人姓名",
        "发起人名称",
        "发起人",
        "姓名或者名称",
        "姓名或名称",
        "出资额",
        "认缴出资额",
        "实缴出资额",
        "出资方式",
        "出资日期",
        "出资时间",
        "认缴日期",
        "实缴日期",
        "缴付期限",
        "认购股份数",
        "认购股份",
        "股份数",
        "持股比例",
        "出资比例",
    ]
    for header in headers:
        source = source.replace(header, " ")
    return source


def extract_shareholder_block(text: str) -> str:
    source = normalize_shareholder_text(text)
    markers = [
        "股东的姓名或者名称",
        "股东姓名",
        "股东名称",
        "发起人姓名或者名称",
        "发起人姓名",
        "发起人名称",
        "发起人",
        "认缴出资额",
        "认购股份",
        "持股比例",
        "股份数",
        "出资方式、出资额和出资",
        "出资额 出资方式 出资",
        "出资日期",
        "出资时间",
    ]
    starts = [source.find(marker) for marker in markers if source.find(marker) >= 0]
    if not starts:
        return source
    start = min(starts)
    tail = source[start:]
    boundaries = [
        r"(?:\n|\s)第六条",
        r"(?:\n|\s)第五章",
        r"(?:\n|\s)公司成立后",
        r"(?:\n|\s)公司机构",
        r"(?:\n|\s)股东会",
        r"(?:\n|\s)董事会",
        r"(?:\n|\s)监事会",
        r"(?:\n|\s)第[一二三四五六七八九十]+章",
        r"(?:\n|\s)第[六七八九十]+条",
    ]
    end = len(tail)
    for pattern in boundaries:
        match = re.search(pattern, tail)
        if match and match.start() > 20:
            end = min(end, match.start())
    return tail[:end]


def compact_shareholder_text(text: str) -> str:
    source = strip_shareholder_headers(normalize_shareholder_text(text))
    return re.sub(r"[\s|,，、]+", "", source)


def normalize_contribution_date(value: str) -> str:
    text = clean_clause(value)
    text = re.sub(r"\s+", "", text)
    match = re.search(r"((?:19|20)\d{2})年(1[0-2]|0?[1-9])月(3[01]|[12]\d|0?[1-9])日", text)
    if match:
        return f"{int(match.group(1)):04d}.{int(match.group(2)):02d}.{int(match.group(3)):02d}"
    match = re.search(r"((?:19|20)\d{2})[./-](1[0-2]|0?[1-9])[./-](3[01]|[12]\d|0?[1-9])", text)
    if match:
        return f"{int(match.group(1)):04d}.{int(match.group(2)):02d}.{int(match.group(3)):02d}"
    return re.sub(r"[/-]", ".", text)


def is_valid_shareholder_name(name: str) -> bool:
    text = clean_clause(name)
    if not re.fullmatch(r"[\u4e00-\u9fffA-Za-z0-9（）()·\-]{2,40}", text):
        return False
    forbidden = (
        "股东", "股权", "转让", "姓名", "名称", "出资", "方式", "时间", "日期",
        "注册资本", "会议", "决议", "董事", "监事", "经理", "法定代表人",
        "财务", "清算", "解散", "章程", "成立后", "签发", "证明书", "名册",
        "货币", "万元", "人民币", "建筑科科", "章", "条",
    )
    invalid_phrases = ("股权转让后", "转让后", "公司成立后", "股东会会议", "本章程", "出资证明书")
    invalid_suffixes = ("后", "前", "时", "的", "和", "或", "及", "章", "条")
    clean_context = (
        not any(item in text for item in forbidden)
        and text not in invalid_phrases
        and not text.endswith(invalid_suffixes)
    )
    if not clean_context:
        return False
    return bool(
        re.fullmatch(r"[\u4e00-\u9fff·]{2,10}", text)
        or re.search(r"(?:股份有限公司|有限责任公司|有限公司|合伙企业)$", text)
    )


def is_valid_renderable_shareholder_name(name: str) -> bool:
    text = clean_clause(name)
    return is_valid_shareholder_name(text) and text not in EXTERNAL_NAME_STOPWORDS


def clean_shareholder_name_candidate(name: str) -> str:
    text = clean_clause(name)
    # Compact OCR parsing may leave table-header glue before the real name, for example
    # "出资额和沃志方495万元..." -> candidate name "和沃志方".
    text = re.sub(r"^(?:和|及|与|、|如下|为|是)+", "", text)
    text = re.sub(r"^(?:姓名或者名称|姓名|名称)+", "", text)
    return text


def make_shareholder(
    name: str,
    amount: str,
    method: str,
    date: str,
    registered_capital_amount: float | None = None,
) -> Shareholder | None:
    clean_name = clean_shareholder_name_candidate(name)
    amount_number = parse_amount_number(amount)
    if not is_valid_shareholder_name(clean_name) or amount_number is None:
        return None
    ratio = ""
    if registered_capital_amount:
        ratio = f"{amount_number / registered_capital_amount * 100:.2f}%"
    return Shareholder(
        name=clean_name,
        subscribed_amount=f"{amount_number:g}万元",
        subscribed_amount_number=amount_number,
        contribution_method=clean_clause(method),
        contribution_deadline=normalize_contribution_date(date),
        contribution_ratio=ratio,
    )


def dedupe_shareholders(items: list[Shareholder]) -> list[Shareholder]:
    deduped: list[Shareholder] = []
    seen: dict[tuple[str, str], int] = {}
    for item in items:
        key = (f"{float(item.subscribed_amount_number or 0):g}", item.contribution_deadline)
        existing_index = seen.get(key)
        if existing_index is not None:
            existing = deduped[existing_index]
            if existing.name == item.name:
                continue
            if existing.name.endswith(item.name):
                deduped[existing_index] = item
                continue
            if item.name.endswith(existing.name):
                continue
        exact_key = (item.name, f"{float(item.subscribed_amount_number or 0):g}", item.contribution_deadline)
        if any((row.name, f"{float(row.subscribed_amount_number or 0):g}", row.contribution_deadline) == exact_key for row in deduped):
            continue
        seen[key] = len(deduped)
        deduped.append(item)
    return deduped


def shareholder_total_matches_registered(
    shareholders: list[Shareholder],
    registered_capital_amount: float | None,
) -> bool:
    if not shareholders or registered_capital_amount is None:
        return False
    total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
    return abs(total - float(registered_capital_amount)) <= 0.01


EXTERNAL_NAME_STOPWORDS = {
    "类型", "姓名", "名称", "股东", "发起人", "出资", "出资额", "出资方式",
    "出资日期", "出资时间", "证件", "证件号码", "身份证", "身份证件",
    "认缴", "实缴", "比例", "公司", "住所", "电话", "邮箱", "职务",
    "董事", "监事", "经理", "法定代表人", "财务负责人", "联络员",
    "经办人", "登记机关", "营业执照", "承诺书", "申请书", "通知书",
    "股东会", "决议", "章程", "签字", "签名", "盖章", "日期", "年月日",
    "情况", "号码", "承诺", "认缴出资", "公司印章", "之日起生", "本章程",
    "登记", "材料证明章", "市场监督管理局",
}


def is_valid_external_shareholder_name(
    value: str,
    source_page_type: str = "",
    raw_context: str = "",
) -> bool:
    name = clean_clause(value)
    if name in EXTERNAL_NAME_STOPWORDS:
        return False
    if any(token in name for token in EXTERNAL_NAME_STOPWORDS if len(token) >= 2):
        return False
    return (
        bool(re.fullmatch(r"[\u4e00-\u9fff·]{2,4}", name))
        and is_natural_person_name(name)
        and source_page_type
        in {
            "",
            "shareholder_contribution_attachment",
            "shareholder_resolution",
            "articles_signature_page",
        }
    )


def _extract_external_names_from_text(
    text: str,
    *,
    allow_standalone_names: bool = False,
    source_page_type: str = "",
) -> list[str]:
    source = normalize_shareholder_text(text)
    names: list[str] = []
    labelled_patterns = (
        r"(?:^|\n)\s*(?:股东签字|股东签名|股东姓名|股东的姓名或者名称|股东（签字、盖章）)\s*[：:]?\s+([^\n]{2,120})",
        r"(?:^|\n)\s*(?:签字|签名)\s*[：:]?\s+([^\n]{2,120})",
    )
    for pattern in labelled_patterns:
        for match in re.finditer(pattern, source):
            for candidate in re.findall(r"[\u4e00-\u9fff·]{2,4}", match.group(1)):
                if is_valid_external_shareholder_name(
                    candidate, source_page_type, match.group(0)
                ) and candidate not in names:
                    names.append(candidate)
    for line in source.splitlines():
        match = re.match(
            r"\s*([\u4e00-\u9fff·]{2,4})\s+(?:(?:\d{6,18}[0-9Xx]?)|(?:\d+(?:\.\d+)?\s*万))",
            line,
        )
        if match and is_valid_external_shareholder_name(
            match.group(1), source_page_type, line
        ) and match.group(1) not in names:
            names.append(match.group(1))
    if allow_standalone_names:
        for line in source.splitlines():
            candidate = clean_clause(line)
            if is_valid_external_shareholder_name(
                candidate, source_page_type, line
            ) and candidate not in names:
                names.append(candidate)
    return names


def extract_external_shareholder_name_candidates(
    pages_or_text: list[dict[str, Any]] | str,
    page_classes: list[Any] | None = None,
) -> list[ExternalNameCandidate]:
    if isinstance(pages_or_text, str):
        return [
            ExternalNameCandidate(name, 0, "", pages_or_text[:200], 70)
            for name in _extract_external_names_from_text(pages_or_text)
        ]
    allowed_types = {
        "shareholder_contribution_attachment",
        "shareholder_resolution",
        "articles_signature_page",
    }
    class_by_page = {
        int(getattr(item, "page", 0)): str(getattr(item, "page_type", ""))
        for item in (page_classes or [])
    }
    base_confidence = {
        "shareholder_contribution_attachment": 90,
        "shareholder_resolution": 80,
        "articles_signature_page": 80,
    }
    candidates: list[ExternalNameCandidate] = []
    for index, page in enumerate(pages_or_text or [], start=1):
        if not isinstance(page, dict):
            continue
        page_no = int(page.get("page") or page.get("page_index") or index)
        if class_by_page.get(page_no) not in allowed_types:
            continue
        page_type = class_by_page.get(page_no)
        for name in _extract_external_names_from_text(
            str(page.get("text") or ""),
            allow_standalone_names=page_type == "shareholder_contribution_attachment",
            source_page_type=page_type or "",
        ):
            candidates.append(
                ExternalNameCandidate(
                    name=name,
                    source_page=page_no,
                    source_page_type=page_type or "",
                    raw_context=str(page.get("text") or "")[:300],
                    confidence=base_confidence.get(page_type or "", -100),
                )
            )
    return candidates


def extract_external_shareholder_names(
    pages_or_text: list[dict[str, Any]] | str,
    page_classes: list[Any] | None = None,
) -> list[str]:
    candidates = extract_external_shareholder_name_candidates(
        pages_or_text,
        page_classes,
    )
    occurrence_count = Counter(
        item.name for item in candidates if item.confidence >= 70
    )
    first_seen = list(dict.fromkeys(
        item.name for item in candidates if item.confidence >= 70
    ))
    selected = sorted(first_seen, key=lambda name: (-occurrence_count[name], first_seen.index(name)))
    logger.debug(
        "%s external_shareholder_names=%s occurrence_count=%s candidates=%s",
        SHAREHOLDER_LOG_PREFIX,
        selected,
        dict(occurrence_count),
        [
            {
                "name": item.name,
                "source_page": item.source_page,
                "source_page_type": item.source_page_type,
                "confidence": item.confidence,
            }
            for item in candidates
        ],
    )
    return selected


def repair_duplicate_shareholder_names_by_external_names(
    shareholders: list[Shareholder],
    external_names: list[str],
    registered_capital_amount: float | None = None,
) -> list[Shareholder]:
    if len(shareholders) < 2:
        return shareholders
    if registered_capital_amount is not None:
        total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
        if abs(total - float(registered_capital_amount)) > 0.01:
            return shareholders
    cleaned_external = list(dict.fromkeys(
        name for name in external_names
        if is_valid_external_shareholder_name(name)
    ))
    counts = Counter(item.name for item in shareholders)
    duplicate_names = [name for name, count in counts.items() if count > 1]
    missing_names = [name for name in cleaned_external if name not in counts]
    duplicate_slots = sum(counts[name] - 1 for name in duplicate_names)
    if not duplicate_names or len(missing_names) < duplicate_slots:
        return shareholders
    replacement_index = 0
    original_names = [item.name for item in shareholders]
    original_total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
    for duplicate_name in duplicate_names:
        duplicate_indexes = [
            index for index, item in enumerate(shareholders) if item.name == duplicate_name
        ]
        for replace_index in duplicate_indexes[1:]:
            replacement = missing_names[replacement_index]
            if not is_valid_external_shareholder_name(replacement):
                continue
            shareholders[replace_index].name = replacement
            replacement_index += 1
            logger.debug(
                "%s repaired_shareholder_name index=%s old=%s new=%s external_names=%s",
                SHAREHOLDER_LOG_PREFIX,
                replace_index,
                duplicate_name,
                replacement,
                cleaned_external,
            )
    repaired_names = [item.name for item in shareholders]
    repaired_total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
    repair_invalid = (
        len(repaired_names) != len(set(repaired_names))
        or any(not is_valid_external_shareholder_name(name) for name in repaired_names)
        or abs(repaired_total - original_total) > 0.01
    )
    if repair_invalid:
        for item, original_name in zip(shareholders, original_names):
            item.name = original_name
        logger.warning(
            "%s repair_rolled_back=true repaired_names=%s",
            SHAREHOLDER_LOG_PREFIX,
            repaired_names,
        )
    return shareholders


def repair_shareholder_names_by_external_names(
    shareholders: list[Shareholder],
    external_names: list[str],
) -> list[Shareholder]:
    return repair_duplicate_shareholder_names_by_external_names(
        shareholders,
        external_names,
        None,
    )


def repair_shareholder_dates_by_majority(
    shareholders: list[Shareholder],
) -> list[Shareholder]:
    if len(shareholders) < 3:
        return shareholders
    dates = [item.contribution_deadline for item in shareholders if item.contribution_deadline]
    if len(dates) < 3:
        return shareholders
    counts = Counter(dates)
    majority_date, majority_count = counts.most_common(1)[0]
    if majority_count <= len(shareholders) / 2:
        return shareholders
    majority_match = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})", majority_date)
    if not majority_match:
        return shareholders
    for item in shareholders:
        if item.contribution_deadline == majority_date:
            continue
        current = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})", item.contribution_deadline)
        if current and current.group(1, 2) == majority_match.group(1, 2):
            logger.debug(
                "%s repaired_shareholder_date name=%s old=%s new=%s",
                SHAREHOLDER_LOG_PREFIX,
                item.name,
                item.contribution_deadline,
                majority_date,
            )
            item.contribution_deadline = majority_date
    return shareholders


def is_natural_person_name(name: str) -> bool:
    text = clean_clause(name)
    return bool(re.fullmatch(r"[\u4e00-\u9fff·路]{2,4}", text)) and not any(
        word in text for word in ("公司", "有限", "科技", "贸易", "建筑", "股权", "转让", "章程")
    )


def candidate_has_abnormal_name(name: str) -> bool:
    text = clean_clause(name)
    abnormal = (
        "股权", "转让", "会议", "董事", "监事", "经理", "清算", "解散", "章程",
        "注册资本", "出资额", "人民币", "万元", "建筑科科", "高级管理人员", "财务",
    )
    return any(word in text for word in abnormal)


def make_candidates(
    shareholders: list[Shareholder],
    source: str,
    in_shareholder_block: bool,
    raw_text: str,
) -> list[ShareholderCandidate]:
    base_scores = {
        "table_row": 100,
        "shareholder_block_regex": 90,
        "compact_regex": 80,
        "token_fallback": 70,
        "amount_recovery": 60,
        "full_text_fallback": 40,
    }
    candidates: list[ShareholderCandidate] = []
    for item in shareholders:
        confidence = float(base_scores.get(source, 50))
        if in_shareholder_block:
            confidence += 20
        else:
            confidence -= 30
        if any(keyword in raw_text for keyword in ("股东的姓名或者名称", "出资额", "出资方式", "出资日期", "出资时间")):
            confidence += 20
        if is_natural_person_name(item.name):
            confidence += 15
        if source == "full_text_fallback":
            confidence -= 30
        if candidate_has_abnormal_name(item.name):
            confidence -= 100
        candidates.append(ShareholderCandidate(item, source, in_shareholder_block, confidence, raw_text[:200]))
    return candidates


def shareholder_candidate_debug(items: list[ShareholderCandidate]) -> list[dict[str, Any]]:
    return [
        {
            "name": item.shareholder.name,
            "amount": item.shareholder.subscribed_amount,
            "date": item.shareholder.contribution_deadline,
            "source": item.source,
            "in_shareholder_block": item.in_shareholder_block,
            "confidence": item.confidence,
            "raw_text": item.raw_text,
        }
        for item in items
    ]


def dedupe_candidates(candidates: list[ShareholderCandidate]) -> list[ShareholderCandidate]:
    best_by_exact: dict[tuple[str, str, str, str], ShareholderCandidate] = {}
    for candidate in candidates:
        item = candidate.shareholder
        key = (
            item.name,
            f"{float(item.subscribed_amount_number or 0):g}",
            item.contribution_method,
            item.contribution_deadline,
        )
        current = best_by_exact.get(key)
        if current is None or candidate.confidence > current.confidence:
            best_by_exact[key] = candidate
    return list(best_by_exact.values())


def final_select_shareholders(
    candidates: list[ShareholderCandidate],
    registered_capital_amount: float | None,
    shareholder_block: str,
) -> list[Shareholder]:
    candidate_order = {id(candidate): index for index, candidate in enumerate(candidates)}
    normalized_candidates = dedupe_candidates([
        candidate for candidate in candidates if is_valid_shareholder_name(candidate.shareholder.name)
    ])
    rejected: list[dict[str, str]] = []
    grouped_by_amount_date: dict[tuple[str, str, str], list[ShareholderCandidate]] = {}
    for candidate in normalized_candidates:
        item = candidate.shareholder
        key = (
            f"{float(item.subscribed_amount_number or 0):g}",
            item.contribution_method,
            item.contribution_deadline,
        )
        grouped_by_amount_date.setdefault(key, []).append(candidate)
    filtered: list[ShareholderCandidate] = []
    for group in grouped_by_amount_date.values():
        max_confidence = max(item.confidence for item in group)
        for candidate in group:
            if candidate.confidence == max_confidence:
                filtered.append(candidate)
            else:
                rejected.append({
                    "name": candidate.shareholder.name,
                    "reason": "duplicate_amount_date_lower_confidence",
                })
    selected_candidates: list[ShareholderCandidate] = []
    if registered_capital_amount and filtered:
        matching_groups: list[tuple[float, int, int, int, tuple[ShareholderCandidate, ...]]] = []
        for size in range(1, min(len(filtered), 8) + 1):
            for group in combinations(filtered, size):
                total = sum(float(item.shareholder.subscribed_amount_number or 0) for item in group)
                if total - float(registered_capital_amount) > 0.01:
                    continue
                if abs(total - float(registered_capital_amount)) <= 0.01:
                    score = sum(item.confidence for item in group)
                    block_count = sum(1 for item in group if item.in_shareholder_block)
                    table_count = sum(1 for item in group if item.source in {"table_row", "shareholder_block_regex"})
                    natural_count = sum(1 for item in group if is_natural_person_name(item.shareholder.name))
                    matching_groups.append((score, block_count, table_count, natural_count, group))
        if matching_groups:
            matching_groups.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
            selected_candidates = list(matching_groups[0][4])
    if not selected_candidates:
        filtered.sort(key=lambda item: item.confidence, reverse=True)
        running_total = 0.0
        for candidate in filtered:
            amount = float(candidate.shareholder.subscribed_amount_number or 0)
            if registered_capital_amount and running_total + amount - float(registered_capital_amount) > 0.01:
                rejected.append({"name": candidate.shareholder.name, "reason": "total_exceeds_registered_capital"})
                continue
            selected_candidates.append(candidate)
            running_total += amount
    selected_names = {id(item) for item in selected_candidates}
    for candidate in filtered:
        if id(candidate) not in selected_names and not any(row.get("name") == candidate.shareholder.name for row in rejected):
            rejected.append({"name": candidate.shareholder.name, "reason": "not_in_best_capital_combination"})
    selected_candidates.sort(key=lambda item: candidate_order.get(id(item), len(candidate_order)))
    logger.debug("%s candidates=%s", SHAREHOLDER_LOG_PREFIX, shareholder_candidate_debug(candidates))
    logger.debug("%s selected_shareholders=%s", SHAREHOLDER_LOG_PREFIX, shareholder_candidate_debug(selected_candidates))
    logger.debug("%s rejected_candidates=%s", SHAREHOLDER_LOG_PREFIX, rejected)
    return [item.shareholder for item in selected_candidates]


def parse_shareholders_by_regex(block: str, registered_capital_amount: float | None) -> list[Shareholder]:
    normalized = strip_shareholder_headers(normalize_shareholder_text(block))
    date = r"(?:19|20)\d{2}\s*(?:年|[./-])\s*(?:1[0-2]|0?[1-9])\s*(?:月|[./-])\s*(?:3[01]|[12]\d|0?[1-9])\s*日?"
    amount = r"\d+(?:\.\d+)?\s*万\s*元?"
    method = r"货币|现金|实物|知识产权|土地使用权|股权|债权|其他"
    separators = r"(?:\s+|\s*\|\s*|\s*[,，、]\s*)"
    patterns = [
        re.compile(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,20}}){separators}"
            rf"(?P<amount>{amount}){separators}"
            rf"(?P<method>{method}){separators}"
            rf"(?P<date>{date})"
        ),
        re.compile(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,10}})\s*"
            rf"(?P<amount>{amount})\s*"
            rf"(?P<method>{method})\s*"
            rf"(?P<date>{date})"
        ),
    ]
    items: list[Shareholder] = []
    for pattern in patterns:
        for match in pattern.finditer(normalized):
            shareholder = make_shareholder(
                match.group("name"),
                match.group("amount"),
                match.group("method"),
                match.group("date"),
                registered_capital_amount,
            )
            if shareholder:
                items.append(shareholder)
    return dedupe_shareholders(items)


def parse_shareholders_by_share_subscription(
    block: str,
    registered_capital_amount: float | None,
) -> list[Shareholder]:
    normalized = strip_shareholder_headers(normalize_shareholder_text(block))
    date = r"(?:19|20)\d{2}\s*(?:年|[./-])\s*(?:1[0-2]|0?[1-9])\s*(?:月|[./-])\s*(?:3[01]|[12]\d|0?[1-9])\s*日?"
    method = r"货币|现金|实物|知识产权|土地使用权|股权|债权|其他"
    amount = r"(?P<amount>\d+(?:\.\d+)?\s*(?:万股|股|万元|万))"
    ratio = r"(?P<ratio>\d+(?:\.\d+)?\s*%)"
    name = r"(?P<name>[\u4e00-\u9fffA-Za-z0-9（）()·\-]{2,40})"
    patterns = (
        re.compile(
            rf"{name}\s+{amount}\s+{ratio}\s+(?P<method>{method})\s+(?P<date>{date})"
        ),
        re.compile(
            rf"{name}\s+{amount}\s+(?P<method>{method})\s+(?P<date>{date})(?:\s+{ratio})?"
        ),
    )
    items: list[Shareholder] = []
    for pattern in patterns:
        for match in pattern.finditer(normalized):
            clean_name = clean_shareholder_name_candidate(match.group("name"))
            amount_text = re.sub(r"\s+", "", match.group("amount"))
            amount_number = parse_amount_number(amount_text)
            if not is_valid_shareholder_name(clean_name) or amount_number is None:
                continue
            ratio_text = clean_clause(match.groupdict().get("ratio") or "")
            if not ratio_text and registered_capital_amount:
                ratio_text = f"{amount_number / registered_capital_amount * 100:.2f}%"
            if amount_text.endswith("万"):
                amount_text = f"{amount_text}元"
            items.append(
                Shareholder(
                    name=clean_name,
                    subscribed_amount=amount_text,
                    subscribed_amount_number=amount_number,
                    contribution_method=clean_clause(match.group("method")),
                    contribution_deadline=normalize_contribution_date(match.group("date")),
                    contribution_ratio=ratio_text,
                )
            )
    return dedupe_shareholders(items)


def parse_shareholders_by_compact(block: str, registered_capital_amount: float | None) -> list[Shareholder]:
    source = compact_shareholder_text(block)
    date = r"(?:19|20)\d{2}(?:年|[./-])(?:1[0-2]|0?[1-9])(?:月|[./-])(?:3[01]|[12]\d|0?[1-9])日?"
    pattern = re.compile(
        rf"(?P<name>[\u4e00-\u9fff·路]{{2,10}})"
        rf"(?P<amount>\d+(?:\.\d+)?万元?)"
        rf"(?P<method>货币|现金|实物|知识产权|土地使用权|股权|债权|其他)"
        rf"(?P<date>{date})"
    )
    items: list[Shareholder] = []
    for match in pattern.finditer(source):
        shareholder = make_shareholder(
            match.group("name"),
            match.group("amount"),
            match.group("method"),
            match.group("date"),
            registered_capital_amount,
        )
        if shareholder:
            items.append(shareholder)
    return dedupe_shareholders(items)


def parse_shareholders_by_tokens(block: str, registered_capital_amount: float | None) -> list[Shareholder]:
    source = strip_shareholder_headers(normalize_shareholder_text(block))
    token_pattern = re.compile(
        r"[\u4e00-\u9fff·路]{2,20}|\d+(?:\.\d+)?\s*万\s*元?|(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日|(?:19|20)\d{2}[./-]\d{1,2}[./-]\d{1,2}"
    )
    raw_tokens = [clean_clause(item.group(0)) for item in token_pattern.finditer(source)]
    methods = {"货币", "现金", "实物", "知识产权", "土地使用权", "股权", "债权", "其他"}
    items: list[Shareholder] = []
    index = 0
    while index < len(raw_tokens):
        name = raw_tokens[index]
        if not is_valid_shareholder_name(name):
            index += 1
            continue
        amount_index = method_index = date_index = -1
        for offset in range(index + 1, min(len(raw_tokens), index + 8)):
            token = raw_tokens[offset]
            if amount_index < 0 and re.fullmatch(r"\d+(?:\.\d+)?万元?", token):
                amount_index = offset
                continue
            if amount_index > 0 and method_index < 0 and token in methods:
                method_index = offset
                continue
            if method_index > 0 and re.search(r"(?:19|20)\d{2}", token):
                date_index = offset
                break
        if amount_index > 0 and method_index > 0 and date_index > 0:
            shareholder = make_shareholder(
                name,
                raw_tokens[amount_index],
                raw_tokens[method_index],
                raw_tokens[date_index],
                registered_capital_amount,
            )
            if shareholder:
                items.append(shareholder)
                index = date_index + 1
                continue
        index += 1
    return dedupe_shareholders(items)


def shareholder_debug_dict(items: list[Shareholder]) -> list[dict[str, Any]]:
    return [item.to_dict() for item in items]


def shareholder_tokens_for_debug(block: str) -> list[str]:
    source = strip_shareholder_headers(normalize_shareholder_text(block))
    token_pattern = re.compile(
        r"[\u4e00-\u9fff·路]{2,20}|\d+(?:\.\d+)?\s*万\s*元?|(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日|(?:19|20)\d{2}[./-]\d{1,2}[./-]\d{1,2}"
    )
    return [clean_clause(item.group(0)) for item in token_pattern.finditer(source)]


def recover_missing_shareholders(
    block: str,
    current: list[Shareholder],
    registered_capital_amount: float | None,
) -> list[Shareholder]:
    if not registered_capital_amount:
        return current
    total = sum(float(item.subscribed_amount_number or 0) for item in current)
    diff = round(float(registered_capital_amount) - total, 2)
    if diff <= 0.01:
        return current
    source = normalize_shareholder_text(block)
    source = strip_shareholder_headers(source)
    diff_pattern = rf"{diff:g}\s*万\s*元?"
    date = r"(?:19|20)\d{2}\s*(?:年|[./-])\s*(?:1[0-2]|0?[1-9])\s*(?:月|[./-])\s*(?:3[01]|[12]\d|0?[1-9])\s*日?"
    method = r"货币|现金|实物|知识产权|土地使用权|股权|债权|其他"
    for match in re.finditer(diff_pattern, source):
        window = source[max(0, match.start() - 80): min(len(source), match.end() + 120)]
        row_match = re.search(
            rf"(?P<name>[\u4e00-\u9fff·路]{{2,20}})[\s|,，、]+(?P<amount>{diff_pattern})[\s|,，、]+(?P<method>{method})[\s|,，、]+(?P<date>{date})",
            window,
        )
        if not row_match:
            tokens = parse_shareholders_by_tokens(window, registered_capital_amount)
            recovered = [item for item in tokens if abs(float(item.subscribed_amount_number or 0) - diff) < 0.01]
        else:
            item = make_shareholder(
                row_match.group("name"),
                row_match.group("amount"),
                row_match.group("method"),
                row_match.group("date"),
                registered_capital_amount,
            )
            recovered = [item] if item else []
        if recovered:
            return dedupe_shareholders([*current, *recovered])
    return current


def extract_shareholders(text: str, registered_capital_amount: float | None = None) -> list[Shareholder]:
    block = extract_shareholder_block(text)
    logger.debug(
        "[CompanyArticles][Shareholders] block_text_preview=%s",
        block[:2000],
    )
    normalized_block = normalize_shareholder_text(block)
    if not block.strip():
        logger.debug("%s shareholder_block_empty=true", SHAREHOLDER_LOG_PREFIX)
    logger.debug("%s full_ocr_text_head=%s", SHAREHOLDER_LOG_PREFIX, str(text or "")[:2000])
    logger.debug("%s full_ocr_text_tail=%s", SHAREHOLDER_LOG_PREFIX, str(text or "")[-2000:])
    logger.debug("%s shareholder_block=%s", SHAREHOLDER_LOG_PREFIX, block)
    logger.debug("%s normalized_shareholder_block=%s", SHAREHOLDER_LOG_PREFIX, normalized_block)

    row_matches = parse_shareholders_by_regex(block, registered_capital_amount)
    share_subscription_matches = parse_shareholders_by_share_subscription(
        block,
        registered_capital_amount,
    )
    logger.debug("%s row_regex_matches=%s", SHAREHOLDER_LOG_PREFIX, shareholder_debug_dict(row_matches))
    logger.debug(
        "%s share_subscription_matches=%s",
        SHAREHOLDER_LOG_PREFIX,
        shareholder_debug_dict(share_subscription_matches),
    )
    if not row_matches:
        logger.debug("%s regex_match_count=0", SHAREHOLDER_LOG_PREFIX)

    compact_matches = parse_shareholders_by_compact(block, registered_capital_amount)
    logger.debug("%s compact_regex_matches=%s", SHAREHOLDER_LOG_PREFIX, shareholder_debug_dict(compact_matches))

    candidates = [
        *make_candidates(row_matches, "table_row", True, block),
        *make_candidates(share_subscription_matches, "table_row", True, block),
        *make_candidates(compact_matches, "compact_regex", True, block),
    ]
    logger.debug(
        "[CompanyArticles][Shareholders] candidates=%s",
        shareholder_candidate_debug(candidates),
    )
    shareholders = final_select_shareholders(candidates, registered_capital_amount, block)
    capital_matched = shareholder_total_matches_registered(shareholders, registered_capital_amount)
    if not capital_matched and len(shareholders) < 2:
        token_rows = parse_shareholders_by_tokens(block, registered_capital_amount)
        logger.debug("%s token_fallback_tokens=%s", SHAREHOLDER_LOG_PREFIX, shareholder_tokens_for_debug(block))
        logger.debug("%s token_fallback_rows=%s", SHAREHOLDER_LOG_PREFIX, shareholder_debug_dict(token_rows))
        candidates.extend(make_candidates(token_rows, "token_fallback", True, block))
        shareholders = final_select_shareholders(candidates, registered_capital_amount, block)
        capital_matched = shareholder_total_matches_registered(shareholders, registered_capital_amount)

    if not capital_matched and (not shareholders or (
        registered_capital_amount
        and abs(sum(float(item.subscribed_amount_number or 0) for item in shareholders) - registered_capital_amount) >= 0.01
    )):
        full_text_matches = dedupe_shareholders([
            *parse_shareholders_by_regex(text, registered_capital_amount),
            *parse_shareholders_by_compact(text, registered_capital_amount),
            *parse_shareholders_by_tokens(text, registered_capital_amount),
        ])
        candidates.extend(make_candidates(full_text_matches, "full_text_fallback", False, text))
        shareholders = final_select_shareholders(candidates, registered_capital_amount, block)
        capital_matched = shareholder_total_matches_registered(shareholders, registered_capital_amount)

    if not capital_matched:
        recovered = recover_missing_shareholders(block, shareholders, registered_capital_amount)
        logger.debug("%s recovered_rows=%s", SHAREHOLDER_LOG_PREFIX, shareholder_debug_dict(recovered))
        candidates.extend(make_candidates(recovered, "amount_recovery", True, block))
        shareholders = final_select_shareholders(candidates, registered_capital_amount, block)
    if not shareholders:
        logger.debug("%s shareholders_empty_after_all_strategies=true", SHAREHOLDER_LOG_PREFIX)
    logger.debug("%s final_shareholders=%s", SHAREHOLDER_LOG_PREFIX, shareholder_debug_dict(shareholders))
    logger.debug(
        "[CompanyArticles][Shareholders] final=%s",
        shareholder_debug_dict(shareholders),
    )
    return shareholders


def short_sentence(text: str, keyword: str, max_chars: int = 80) -> str:
    source = re.sub(r"\s+", "", text or "")
    index = source.find(keyword)
    if index < 0:
        return ""
    end_candidates = [pos for pos in (source.find("。", index), source.find("；", index), source.find(";", index)) if pos >= 0]
    end = min(end_candidates) + 1 if end_candidates else min(len(source), index + max_chars)
    return source[index:end][:max_chars].strip("。；;")


def extract_first_shareholders_meeting(text: str) -> str:
    sentence = short_sentence(text, "首次股东会会议", 80)
    if sentence:
        return sentence
    if "首次股东会" in text:
        return "首次股东会会议由出资最多的股东召集和主持，依照公司法规定行使职权"
    return "未识别"


def extract_major_rules(text: str) -> dict[str, str]:
    compact = compact_text(text)
    shareholder_rule = "须经代表全体股东三分之二以上表决权的股东通过"
    other_rule = "须经代表全体股东三分之二以上表决权通过"
    has_two_thirds = "三分之二以上表决权" in compact
    has_major_topics = any(token in compact for token in ("修改公司章程", "修改章程", "增加或者减少注册资本", "合并", "分立", "解散", "变更公司形式"))
    major_rule = shareholder_rule if has_two_thirds and has_major_topics else "未识别"
    return {
        "amendment_rule": major_rule if any(token in compact for token in ("修改公司章程", "修改章程")) else "未识别",
        "capital_change_rule": major_rule if "注册资本" in compact and any(token in compact for token in ("增加", "减少", "增减")) else "未识别",
        "merger_split_dissolution_rule": major_rule if any(token in compact for token in ("合并", "分立", "解散", "变更公司形式")) else "未识别",
        "other_rule": other_rule if "除前款以外" in compact and has_two_thirds else (other_rule if has_two_thirds else "未识别"),
    }


def extract_signature_info(text: str, pages: list[dict[str, Any]]) -> dict[str, Any]:
    signature_page = ""
    for page in reversed(pages or []):
        page_text = str(page.get("text") or "")
        if any(token in page_text for token in ("股东", "签字", "盖章", "签名", "年月日", "印章", "章")):
            page_no = page.get("page") or page.get("page_index") or len(pages)
            signature_page = f"第{page_no}页"
            break
    if not signature_page and pages:
        signature_page = f"第{len(pages)}页"
    tail = "\n".join(str(page.get("text") or "") for page in (pages or [])[-2:]) if pages else text[-1500:]
    has_signature = any(token in tail for token in ("签字", "盖章", "签名", "印章", "股东"))
    signing_date = first_match(tail, [r"((?:19|20)\d{2}\s*年\s*(?:1[0-2]|0?[1-9])\s*月\s*(?:3[01]|[12]\d|0?[1-9])\s*日)"])
    if "股东签字" in tail and "公司印章" in tail:
        detection_summary = "识别到股东签字和公司印章"
    elif has_signature:
        detection_summary = "识别到手写签名和红色印章"
    else:
        detection_summary = "未识别到签字或盖章"
    return {
        "signature_page": signature_page or "未识别",
        "has_signature_or_stamp": "有" if has_signature else "未识别",
        "detected_signature_count": 1 if has_signature else 0,
        "signing_date": clean_clause(signing_date) or "未填写/未识别",
        "signature_detection_summary": detection_summary,
    }


def extract_fields(text: str, pages: list[dict[str, Any]] | None = None, filename: str = "") -> dict[str, Any]:
    pages = pages or []
    title, title_company = extract_title(text)
    company_name = extract_company_name(text, title_company)
    title = clean_articles_title(title, company_name)
    company_address = extract_company_address(text, company_name)
    registered_capital, registered_amount, currency = extract_registered_capital(text)
    shareholders = extract_shareholders(text, registered_amount)
    shareholder_total = sum(float(item.subscribed_amount_number or 0) for item in shareholders)
    if not registered_amount and shareholder_total and "公司注册资本" in compact_text(text):
        registered_amount = shareholder_total
        registered_capital = f"人民币{shareholder_total:g}万元"
        currency = "人民币"
    if registered_amount:
        shareholders = recover_missing_shareholders(extract_shareholder_block(text), shareholders, registered_amount)
        shareholders = repair_shareholder_dates_by_majority(shareholders)
        for shareholder in shareholders:
            if shareholder.subscribed_amount_number is not None:
                shareholder.contribution_ratio = f"{shareholder.subscribed_amount_number / registered_amount * 100:.2f}%"

    compact = compact_text(text)
    governance = {
        "authority_body": "股东会" if "股东会" in text else "未识别",
        "first_shareholders_meeting": extract_first_shareholders_meeting(text),
        "voting_rule": "股东会会议由股东按照出资比例行使表决权" if "按照出资比例行使表决权" in compact else short_sentence(text, "表决权") or "未识别",
        "executive_director": "公司不设董事会，设执行董事一名，任期三年，由股东会选举产生" if "不设董事会" in text and "执行董事" in text else short_sentence(text, "执行董事") or "未识别",
        "manager": "由股东会决定聘任或者解聘，任期三年，可以连任" if "经理" in text and "聘任或者解聘" in text else short_sentence(text, "经理") or "未识别",
        "supervisor": "公司不设监事会，设监事一人，任期三年，可以连任" if "不设监事会" in text and "监事" in text else short_sentence(text, "监事") or "未识别",
        "legal_representative": "由执行董事担任" if "法定代表人" in text and "执行董事" in text else short_sentence(text, "法定代表人") or "未识别",
    }
    return {
        "title": title,
        "company_name": company_name,
        "company_address": company_address,
        "business_scope": extract_business_scope(text),
        "registered_capital": registered_capital,
        "registered_capital_amount": registered_amount,
        "currency": currency or "人民币",
        "shareholders": shareholders,
        "governance": governance,
        "major_resolution_rules": extract_major_rules(text),
        "equity_transfer_summary": "股东之间可以相互转让全部或者部分股权；向股东以外的人转让股权，应经其他股东过半数同意；其他股东自接到书面通知之日起满三十日未答复的，视为同意转让；同等条件下其他股东有优先购买权。" if "优先购买权" in text or "股权转让" in text else "未识别",
        "finance_and_profit_summary": "依照法律、行政法规和国务院财政主管部门规定建立财务会计制度；会计年度终了编制财务会计报告；股东按照出资比例分取红利；聘用或解聘会计师事务所由股东会决定。" if "财务会计制度" in text or "分取红利" in text else "未识别",
        "dissolution_and_liquidation_summary": "营业期限为长期；股东会决议可以解散；公司合并或者分立需要解散；依法被吊销营业执照、责令关闭或者被撤销；人民法院依法予以解散；清算组由股东组成。" if "清算组" in text or "营业期限为长期" in text else "未识别",
        "senior_management_obligations_summary": "高级管理人员包括经理、副经理、财务负责人；不得侵占公司财产；不得挪用公司资金；不得未经同意订立合同或者交易；不得泄露公司秘密。" if "高级管理人员" in text or "不得挪用公司资金" in text else "未识别",
        "articles_effective_rule": "本章程自全体股东盖章、签字之日起生效" if "全体股东盖章" in text or "签字之日起生效" in text else short_sentence(text, "生效") or "未识别",
        "signature_info": extract_signature_info(text, pages),
        "page_count": len(pages) if pages else len(re.findall(r"---\s*第?\s*\d+\s*页", text or "")) or 1,
        "field_evidence": {
            "title": {
                "candidate": title,
                "source": "articles_title_top",
                "confidence": 100 if title else 0,
            },
            "company_name": {
                "candidate": company_name,
                "source": "articles_clause" if "公司名称" in text else "articles_title_top",
                "confidence": 90 if company_name else 0,
            },
            "company_address": {
                "candidate": company_address,
                "source": "articles_clause",
                "confidence": 90 if company_address else 0,
            },
            "registered_capital": {
                "candidate": registered_capital,
                "source": "articles_clause",
                "confidence": 90 if registered_capital else 0,
            },
        },
    }


def detect_company_articles(text: str = "", filename: str = "") -> bool:
    compact = compact_text(text)
    lower_filename = str(filename or "").lower()
    if any(token in lower_filename for token in ("章程", "公司章程", "articles", "articles of association")):
        return True
    strong = "公司章程" in compact
    feature_count = sum(1 for token in ("股东会", "执行董事", "注册资本", "出资额") if token in compact)
    if strong and feature_count >= 2:
        return True
    chapter_count = sum(1 for token in ("第一章公司的名称和住所", "公司注册资本", "股东的姓名或者名称", "本章程") if token in compact)
    return chapter_count >= 2 and feature_count >= 1
