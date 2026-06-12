from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any, Callable

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


logger = logging.getLogger(__name__)
ID_NUMBER_PATTERN = re.compile(r"\d{17}[\dXx]")


HOUSEHOLD_INFO_FIELDS = (
    "household_type",
    "household_number",
    "household_head",
    "household_address",
    "booklet_number",
    "issuing_authority",
    "issue_date",
    "undertaker",
    "address_change_records",
)

MEMBER_FIELDS = (
    "name",
    "former_name",
    "relationship_to_head",
    "gender",
    "ethnicity",
    "birth_place",
    "native_place",
    "birth_date",
    "other_address",
    "id_number",
    "education_level",
    "marital_status",
    "military_status",
    "height",
    "blood_type",
    "religion",
    "service_place",
    "occupation",
    "migration_to_city",
    "migration_to_address",
    "registration_date",
    "page_index",
    "source_pages",
)

HOUSEHOLD_TYPE_VALUES = (
    "非农业家庭户口",
    "非农业家庭",
    "非农家庭户口",
    "农业家庭户口",
    "居民家庭户口",
    "居民家庭户",
    "居民户口",
    "非农业集体户口",
    "农业集体户口",
    "非农业户口",
    "农业户口",
    "家庭户",
    "集体户",
)

RELATION_VALUES = ("户主", "儿媳", "女婿", "外孙女", "外孙", "孙子", "孙女", "长子", "次子", "长女", "次女", "妻", "夫", "子", "女", "父", "母", "兄", "弟", "姐", "妹", "其他")
EDUCATION_VALUES = (
    "中等专业学校或中等技术学校",
    "文盲或半文盲",
    "中等专业学校",
    "中等技术学校",
    "职业高中",
    "职业学校",
    "大学专科",
    "大学本科",
    "半文盲",
    "研究生",
    "博士",
    "硕士",
    "本科",
    "大专",
    "职高",
    "职校",
    "技校",
    "中专",
    "高中",
    "初中",
    "小学",
    "文盲",
    "不详",
)
MARITAL_VALUES = ("有配偶", "已婚", "未婚", "离异", "离婚", "丧偶", "复婚")
MILITARY_VALUES = ("未服兵役", "服兵役", "退役", "不详", "无")
BLOOD_VALUES = ("AB型", "A型", "B型", "O型", "不明")
RELIGION_VALUES = ("无", "不详", "佛教", "道教", "基督教", "天主教", "伊斯兰教")
OCCUPATION_VALUES = ("不详", "无业", "学生", "职员", "个体", "工人", "农民", "教师")

NOTICE_NOISE = (
    "须立即报告",
    "进行户籍调查",
    "核对",
    "法律效力",
    "妥善保管",
    "严禁私自涂改",
    "注意事项",
    "应主动交验",
    "主要依据",
    "居民户口簿具有",
)

ADDRESS_NOISE = (
    "省级公安机关",
    "户口专用章",
    "专用章",
    "派出所",
    "公安局",
    "承办人签章",
    "签发",
    "注意事项",
    "户主姓名",
    "户号",
    "No.",
    "NO.",
    "编号",
)

MEMBER_STOP_LABELS = (
    "姓名",
    "姓 名",
    "曾用名",
    "曾 用 名",
    "户主或与户主关系",
    "户主关系",
    "与户主关系",
    "性别",
    "民族",
    "出生地",
    "籍贯",
    "出生日期",
    "本市",
    "公民身份号码",
    "身份证件编号",
    "身份证号码",
    "居民身份证号码",
    "文化程度",
    "婚姻状况",
    "兵役状况",
    "身高",
    "血型",
    "宗教信仰",
    "服务处所",
    "职业",
    "何时由何地迁来本市",
    "何时由何地迁来本县",
    "何时由何地迁来本址",
    "登记日期",
    "承办人签章",
    "常住人口登记卡",
)

HOME_STOP_LABELS = (
    "户别",
    "户 别",
    "户号",
    "户 号",
    "户主姓名",
    "户 主 姓 名",
    "住址",
    "住 址",
    "承办人签章",
    "签发",
    "户口专用章",
    "公安机关",
    "公安局",
    "派出所",
    "省级公安机关",
    "注意事项",
    "No",
    "编号",
    "常住人口登记卡",
    "住址变动登记",
)

NOISY_NAME_FRAGMENTS = ("或与", "关系", "户主或", "性别", "民族", "出生", "公民身份号码", "何时由何地")
NAME_FORBIDDEN_VALUES = set(RELATION_VALUES) | {"男", "女", "男性", "女性", "汉", "汉族", "姓名", "曾用名"}


def _text(value: Any) -> str:
    return str(value or "").replace("\u3000", " ").strip()


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", _text(text)).strip()


def _canonical_member_labels(text: str) -> str:
    value = text
    replacements = (
        (r"户主或与\s*户主关系", "户主或与户主关系"),
        (r"公民\s*身份\s*号码", "公民身份号码"),
        (r"姓\s+名", "姓名"),
        (r"曾\s+用\s+名", "曾用名"),
        (r"文\s*化\s*程度", "文化程度"),
        (r"婚姻\s*状况", "婚姻状况"),
        (r"兵役\s*状况", "兵役状况"),
        (r"宗教\s*信仰", "宗教信仰"),
        (r"服务\s*处所", "服务处所"),
        (r"出生\s*日期", "出生日期"),
        (r"出生\s*地", "出生地"),
    )
    for pattern, replacement in replacements:
        value = re.sub(pattern, replacement, value)
    return value


def _compact(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip(" :：,，;；")


def _lines(text: str) -> list[str]:
    return [_flat(line) for line in re.split(r"[\r\n]+", text or "") if _flat(line)]


def _date_to_iso(value: Any) -> str:
    text = _text(value)
    patterns = (
        r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        r"(\d{4})[-./]\s*(\d{1,2})[-./]\s*(\d{1,2})",
        r"(\d{4})\s+(\d{1,2})\s+(\d{1,2})",
        r"(\d{4})(\d{2})(\d{2})",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        year, month, day = (int(part) for part in match.groups())
        try:
            date(year, month, day)
        except ValueError:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def _id_numbers(text: str) -> list[str]:
    return list(dict.fromkeys(match.group(0).upper() for match in ID_NUMBER_PATTERN.finditer(text or "")))


def is_valid_chinese_id_number(id_number: str) -> bool:
    value = str(id_number or "").upper()
    if not re.fullmatch(r"\d{17}[\dX]", value):
        return False
    birth = value[6:14]
    try:
        birth_date = date(int(birth[:4]), int(birth[4:6]), int(birth[6:8]))
    except ValueError:
        return False
    if not (1900 <= birth_date.year <= date.today().year):
        return False
    factors = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
    checks = "10X98765432"
    total = sum(int(value[index]) * factors[index] for index in range(17))
    return checks[total % 11] == value[-1]


def _id_birth_date(id_number: str) -> str:
    value = str(id_number or "").upper()
    if not re.fullmatch(r"\d{17}[\dX]", value):
        return ""
    try:
        birth_date = date(int(value[6:10]), int(value[10:12]), int(value[12:14]))
    except ValueError:
        return ""
    if not (1900 <= birth_date.year <= date.today().year):
        return ""
    return f"{birth_date.year:04d}-{birth_date.month:02d}-{birth_date.day:02d}"


def _normalize_dates_in_text(value: Any) -> str:
    text = _compact(value)

    def replace(match: re.Match[str]) -> str:
        iso = _date_to_iso(match.group(0))
        return iso or match.group(0)

    text = re.sub(r"\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日", replace, text)
    text = re.sub(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", replace, text)
    text = re.sub(r"(\d{4}-\d{2}-\d{2})(?=[\u4e00-\u9fff])", r"\1 ", text)
    return text


def _make_evidence(value: str, evidence_text: str, page: int | None, confidence: float = 0.82) -> dict[str, Any]:
    return {"value": value, "evidence_text": evidence_text, "page": page, "confidence": confidence}


def _page_texts(data: dict[str, Any]) -> list[tuple[int | None, str]]:
    pages = data.get("pages") or []
    page_texts: list[tuple[int | None, str]] = []
    for index, page in enumerate(pages, start=1):
        if isinstance(page, dict):
            text = page.get("text") or page.get("content") or page.get("raw_text") or ""
            page_index = page.get("page") or page.get("page_index") or index
        else:
            text = str(page or "")
            page_index = index
        if _text(text):
            page_texts.append((int(page_index) if str(page_index).isdigit() else index, str(text)))
    if page_texts:
        return page_texts

    raw = str(data.get("text") or "")
    chunks = [chunk for chunk in re.split(r"\f+", raw) if _text(chunk)]
    if len(chunks) > 1:
        return [(index, chunk) for index, chunk in enumerate(chunks, start=1)]

    # Fallback for pasted OCR: keep home/notice/member chunks separate enough to avoid global cross-field matches.
    markers = r"(?=(?:居民户口簿|常住人口登记卡|住址变动登记|登记事项变更和更正记载|注意事项))"
    split_chunks = [chunk for chunk in re.split(markers, raw) if _text(chunk)]
    if len(split_chunks) > 1:
        return [(index, chunk) for index, chunk in enumerate(split_chunks, start=1)]
    return [(None, raw)]


def _page_type(text: str) -> str:
    compact = _compact(text)
    if "常住人口登记卡" in compact or ("公民身份号码" in compact and "户主或与户主关系" in compact):
        return "member_card"
    if "注意事项" in compact and not any(label in compact for label in ("户别", "户号", "户主姓名", "常住人口登记卡")):
        return "notice"
    if "住址变动登记" in compact:
        return "address_change"
    if "登记事项变更和更正记载" in compact:
        return "change_record"
    home_score = sum(1 for label in ("户别", "户号", "户主姓名", "住址") if label in compact)
    if home_score >= 3 or ("居民户口簿" in compact and "住址" in compact) or "承办人签章" in compact or re.search(r"No\.?\s*\d{6,}", text, re.I):
        return "household_home_page"
    return "unknown"


def _has_home_page(text: str) -> bool:
    compact = _compact(text)
    home_score = sum(1 for label in ("户别", "户号", "户主姓名", "住址") if label in compact)
    return (
        home_score >= 2
        or ("居民户口簿" in compact and "住址" in compact)
        or ("承办人签章" in compact and ("签发" in compact or "户别" in compact or "住址" in compact))
        or bool(re.search(r"(?:No\.?|NO\.?|Nº|N°|户口簿编号)\s*[:：]?\s*[0-9A-Za-z]{6,12}", text, re.I))
    )


def _home_feature_count(text: str) -> int:
    compact = _compact(text)
    features = [
        "户别" in compact,
        "户号" in compact,
        "户主姓名" in compact,
        "住址" in compact,
        bool(re.search(r"(?:No\.?|NO\.?|Nº|N°|户口簿编号)\s*[:：]?\s*[0-9A-Za-z]{6,12}", text or "", re.I)),
        "承办人签章" in compact,
        "签发" in compact,
    ]
    return sum(1 for item in features if item)


def _looks_like_id_prefix(value: Any, member_id_numbers: set[str] | None = None) -> bool:
    text = _compact(value)
    if not text:
        return False
    if re.fullmatch(r"\d{17}[\dXx]", text):
        return True
    if re.fullmatch(r"\d{12,17}", text) and re.match(r"\d{6}(?:19|20)\d{2}", text):
        return True
    for id_number in member_id_numbers or set():
        if len(text) >= 6 and str(id_number).startswith(text):
            return True
    return False


def _clean_household_record(record: dict[str, Any], member_id_numbers: set[str] | None = None) -> dict[str, Any]:
    cleaned = dict(record)
    if _looks_like_id_prefix(cleaned.get("booklet_number"), member_id_numbers):
        logger.info("[HUKOU_HOUSEHOLD_RECORD_DROPPED] reason=invalid_booklet_number value=%s", cleaned.get("booklet_number"))
        cleaned["booklet_number"] = ""
    if cleaned.get("undertaker") in {"公民身份", "身份号码", "户口登记机关", "户口专用章"}:
        logger.info("[HUKOU_HOUSEHOLD_RECORD_DROPPED] reason=invalid_undertaker value=%s", cleaned.get("undertaker"))
        cleaned["undertaker"] = ""
    if any(noise in str(cleaned.get("undertaker") or "") for noise in ("派出所", "公安局")):
        cleaned["undertaker"] = ""
    return cleaned


def is_valid_household_record(
    record: dict[str, Any],
    source_area: str = "household_home_area",
    member_id_numbers: set[str] | None = None,
) -> bool:
    if source_area != "household_home_area":
        logger.info("[HUKOU_HOUSEHOLD_RECORD_DROPPED] reason=not_from_home_page")
        return False
    cleaned = _clean_household_record(record, member_id_numbers)
    core_fields = ("household_number", "household_head", "household_address", "issue_date", "booklet_number")
    core_count = sum(1 for field in core_fields if cleaned.get(field))
    if not any(cleaned.get(field) for field in ("household_number", "household_head", "household_address")):
        logger.info("[HUKOU_HOUSEHOLD_RECORD_DROPPED] reason=missing_number_head_address")
        return False
    if core_count < 2:
        logger.info("[HUKOU_HOUSEHOLD_RECORD_DROPPED] reason=low_quality_record")
        return False
    return True


def _has_address_change_area(text: str) -> bool:
    compact = _compact(text)
    return "住址变动登记" in compact and ("变动后的住址" in compact or "变动日期" in compact)


def _has_member_card(text: str) -> bool:
    compact = _compact(text)
    has_id_number = bool(ID_NUMBER_PATTERN.search(compact))
    return (
        ("常住人口登记卡" in compact and ("姓名" in compact or "公民身份号码" in compact))
        or ("姓名" in compact and "户主或与户主关系" in compact and "公民身份号码" in compact)
        or ("姓名" in compact and "户主或与户主关系" in compact and "出生日期" in compact)
        or (has_id_number and "出生日期" in compact and "性别" in compact and "民族" in compact)
        or (has_id_number and "姓名" in compact and "文化程度" in compact)
        or (has_id_number and "姓名" in compact and "性别" in compact)
        or ("出生日期" in compact and "公民身份号码" in compact and ("身高" in compact or "血型" in compact))
    )


def _value_between(text: str, labels: tuple[str, ...], stops: tuple[str, ...], max_chars: int = 120) -> tuple[str, str]:
    flat = _canonical_member_labels(_flat(text))
    label_re = "|".join(re.escape(label) for label in sorted(labels, key=len, reverse=True))
    stop_re = "|".join(re.escape(stop) for stop in sorted(stops, key=len, reverse=True) if stop not in labels)
    pattern = re.compile(rf"(?:{label_re})\s*[:：]?\s*(.{{0,{max_chars}}}?)(?=\s*(?:{stop_re})|$)")
    for match in pattern.finditer(flat):
        value = match.group(1).strip(" :：,，;；")
        if value:
            return value, match.group(0)
    return "", ""


def _first_choice(value: str, choices: tuple[str, ...]) -> str:
    compact = _compact(value)
    for choice in choices:
        if choice in compact:
            return choice
    return ""


def _clean_person_name(value: Any) -> str:
    text = _compact(value)
    for bad in ("或与", "性别", "民族", "出生", "籍贯", "公民身份号码", "文化程度", "婚姻状况", "兵役状况", "服务处所", "职业", "姓名", "曾用名", "何时由何地", "登记日期", "常住人口", "登记卡", "登记事项"):
        if bad in text:
            return ""
    text = re.sub(r"^(?:户主或与户主关系|户主关系|与户主关系)", "", text)
    text = re.sub(r"(关系.*|关$)", "", text)
    match = re.search(r"[\u4e00-\u9fff]{2,8}", text)
    if not match:
        return ""
    name = match.group(0)
    if name in {"姓名", "户主", "关系", "或与"} or any(bad in name for bad in ("关系", "或与")):
        return ""
    if name in NAME_FORBIDDEN_VALUES:
        return ""
    return name


def _is_valid_member_name(value: Any) -> bool:
    text = _compact(value)
    if not text:
        return False
    if any(fragment in text for fragment in NOISY_NAME_FRAGMENTS):
        return False
    if text in NAME_FORBIDDEN_VALUES:
        return False
    return bool(re.fullmatch(r"[\u4e00-\u9fff]{2,8}", text))


def _split_name_relation(value: str) -> tuple[str, str]:
    compact = _compact(value)
    relation_re = "|".join(re.escape(item) for item in RELATION_VALUES)
    match = re.match(rf"([\u4e00-\u9fff]{{2,8}})(?:关系|关)?({relation_re})$", compact)
    if match:
        return _clean_person_name(match.group(1)), match.group(2)
    return "", ""


def _clean_address(value: Any) -> str:
    text = _compact(value)
    for noise in ADDRESS_NOISE:
        index = text.find(noise)
        if index >= 0:
            text = text[:index]
    text = re.sub(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日.*$", "", text)
    text = re.sub(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}.*$", "", text)
    terminal_indexes = [text.rfind(char) for char in ("室", "村", "号", "楼", "幢", "单元") if text.rfind(char) >= 0]
    if terminal_indexes:
        text = text[: max(terminal_indexes) + 1]
    if not (6 <= len(text) <= 80):
        return ""
    if not any(keyword in text for keyword in ("省", "市", "县", "区", "镇", "乡", "村", "路", "街", "弄", "号", "室", "幢", "单元", "楼")):
        return ""
    return text


def _split_leading_household_number_from_address(address: str) -> tuple[str, str]:
    match = re.match(r"^([A-Za-z0-9]{4,12})(?=[\u4e00-\u9fff]*(?:省|市|区|县|镇|乡|村|路|街|弄|号|室|幢|单元|楼))(.+)$", address or "")
    if not match:
        return "", address
    number = match.group(1)
    if re.fullmatch(r"\d{17}[\dXx]", number):
        return "", address
    return number, match.group(2)


def _clean_authority(value: Any) -> str:
    text = _compact(value)
    if not text or len(text) > 35:
        return ""
    if any(noise in text for noise in NOTICE_NOISE):
        return ""
    if not any(keyword in text for keyword in ("公安局", "派出所")):
        return ""
    patterns = (
        r"[\u4e00-\u9fff]{2,18}公安局[\u4e00-\u9fff]{0,12}派出所",
        r"[\u4e00-\u9fff]{2,18}公安局[\u4e00-\u9fff]{0,12}",
        r"[\u4e00-\u9fff]{2,18}派出所",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match and len(match.group(0)) <= 35:
            return match.group(0)
    return text


def _extract_issuing_authority(text: str) -> tuple[str, str]:
    for line in _lines(text):
        if not any(keyword in line for keyword in ("公安局", "派出所", "户口登记机关")):
            continue
        cleaned = _clean_authority(line)
        if cleaned:
            return cleaned, line
    cleaned = _clean_authority(text)
    return (cleaned, cleaned) if cleaned else ("", "")


def _extract_household_info_from_page(text: str, page: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    info = {field: "" for field in HOUSEHOLD_INFO_FIELDS}
    info["address_change_records"] = []
    evidence: dict[str, Any] = {}

    if not _has_home_page(text):
        return info, evidence

    value, ev = _value_between(text, ("户别", "户 别"), HOME_STOP_LABELS)
    household_type = _first_choice(value, HOUSEHOLD_TYPE_VALUES)
    if not household_type:
        household_type = _first_choice(text, HOUSEHOLD_TYPE_VALUES)
    if household_type:
        info["household_type"] = household_type
        evidence["household_info.household_type"] = _make_evidence(household_type, ev, page)

    match = re.search(r"户\s*号\s*[:：]?\s*([A-Za-z0-9]{4,20})", _flat(text))
    if match:
        candidate = match.group(1)
        if not re.fullmatch(r"\d{17}[\dXx]", candidate):
            info["household_number"] = candidate
            evidence["household_info.household_number"] = _make_evidence(candidate, match.group(0), page)

    value, ev = _value_between(text, ("户主姓名", "户 主 姓 名"), HOME_STOP_LABELS, max_chars=40)
    head = _clean_person_name(value)
    if not head:
        match = re.search(r"户\s*主\s*姓\s*名\s*[:：]?\s*([\u4e00-\u9fff]{2,8})", _flat(text))
        if match:
            head = _clean_person_name(match.group(1))
    if head and not any(word in head for word in ("非农", "家庭户", "户口")):
        info["household_head"] = head
        evidence["household_info.household_head"] = _make_evidence(head, ev, page)

    value, ev = _value_between(text, ("住址", "住 址"), HOME_STOP_LABELS, max_chars=160)
    address = _clean_address(value)
    if address:
        number_from_address, clean_address = _split_leading_household_number_from_address(address)
        if number_from_address and not info.get("household_number"):
            info["household_number"] = number_from_address
            evidence["household_info.household_number"] = _make_evidence(number_from_address, ev, page)
        address = clean_address
        info["household_address"] = address
        evidence["household_info.household_address"] = _make_evidence(address, ev, page)

    match = re.search(r"(?:No\.?|NO|Nº|N°|户口簿编号|(?<!身份)编号|N)\s*[:：]?\s*([0-9A-Za-z]{6,12})", text, re.I)
    if match:
        candidate = match.group(1)
        if candidate.upper().startswith("N") and len(candidate) > 1:
            candidate = candidate[1:]
        if candidate != info.get("household_number") and not re.fullmatch(r"\d{17}[\dXx]", candidate):
            info["booklet_number"] = candidate
            evidence["household_info.booklet_number"] = _make_evidence(candidate, match.group(0), page)

    match = re.search(r"承办人签章\s*[:：]?\s*([\u4e00-\u9fff]{2,4})", _flat(text))
    if match and match.group(1) not in {"公民身份", "身份号码"}:
        info["undertaker"] = match.group(1)
        evidence["household_info.undertaker"] = _make_evidence(match.group(1), match.group(0), page)

    issue_date = ""
    evidence_text = ""
    for line in _lines(text):
        if "签发" in line:
            issue_date = _date_to_iso(line)
            evidence_text = line
            if issue_date:
                break
    if not issue_date:
        flat_text = _flat(text)
        match = re.search(r"((?:19|20)\d{2})\s*年\s*月\s*日\s*签发\s*(\d{4})", flat_text)
        if not match:
            match = re.search(r"((?:19|20)\d{2}).{0,20}?签发.{0,20}?(\d{4})", flat_text)
        if match:
            month_day = match.group(2)
            try:
                issue_date = f"{int(match.group(1)):04d}-{int(month_day[:2]):02d}-{int(month_day[2:]):02d}"
                evidence_text = match.group(0)
            except ValueError:
                issue_date = ""
    if not issue_date:
        match = re.search(r"((?:19|20)\d{6})", text)
        if match:
            issue_date = _date_to_iso(match.group(1))
            evidence_text = match.group(0)
    if issue_date:
        info["issue_date"] = issue_date
        evidence["household_info.issue_date"] = _make_evidence(issue_date, evidence_text, page)

    authority, ev = _extract_issuing_authority(text)
    if authority:
        info["issuing_authority"] = authority
        evidence["household_info.issuing_authority"] = _make_evidence(authority, ev, page, 0.76)

    return info, evidence


def _extract_valid_address_change_records(text: str) -> list[str]:
    if not _has_address_change_area(text):
        return []
    records: list[str] = []
    for line in _lines(text):
        if any(noise in line for noise in NOTICE_NOISE):
            continue
        compact_line = _compact(line)
        if compact_line in {"变动后的住址", "变动日期", "变动后的住址变动日期", "变动后的住址变动日期承办人签章"}:
            logger.info("[HUKOU_ADDRESS_RECORD_DROPPED] reason=table_header_only")
            continue
        if ("变动后的住址" in line or "变动日期" in line) and len(line) <= 120:
            records.append(line)
    return list(dict.fromkeys(records))


def _extract_field(segment: str, labels: tuple[str, ...], max_chars: int = 120) -> tuple[str, str]:
    return _value_between(segment, labels, MEMBER_STOP_LABELS, max_chars=max_chars)


def _is_label_line(line: str) -> bool:
    compact = _compact(line)
    if not compact:
        return True
    if compact in {"户主或与", "户主关系"}:
        return True
    return any(compact == _compact(label) for label in MEMBER_STOP_LABELS)


def extract_value_after_label(
    lines: list[str],
    label: str,
    stop_labels: tuple[str, ...],
    *,
    max_lookahead: int = 5,
    validator: Callable[[Any], bool] | None = None,
) -> tuple[str, str, list[str]]:
    candidates: list[str] = []
    compact_label = _compact(label)
    compact_stops = {_compact(item) for item in stop_labels}
    for index, line in enumerate(lines):
        compact_line = _compact(line)
        if compact_label not in compact_line:
            continue
        inline_value = compact_line.split(compact_label, 1)[1]
        inline_value = inline_value.strip(" :：,，;；")
        if inline_value:
            candidate = _clean_person_name(inline_value) if validator is _is_valid_member_name else inline_value
            if candidate:
                candidates.append(candidate)
                if validator is None or validator(candidate):
                    return candidate, line, candidates
        for offset in range(1, max_lookahead + 1):
            if index + offset >= len(lines):
                break
            next_line = _compact(lines[index + offset])
            if not next_line:
                continue
            if next_line in compact_stops and next_line != compact_label:
                break
            if _is_label_line(next_line):
                continue
            candidate = _clean_person_name(next_line) if validator is _is_valid_member_name else next_line
            if candidate:
                candidates.append(candidate)
                if validator is None or validator(candidate):
                    return candidate, lines[index + offset], candidates
    return "", "", candidates


def _name_candidates_from_block(segment: str) -> list[str]:
    lines = _lines(segment)
    candidates: list[str] = []
    value, _ = _extract_field(segment, ("姓名", "姓 名"), max_chars=60)
    for raw in (value,):
        split_name, _ = _split_name_relation(raw)
        name = split_name or _clean_person_name(raw)
        if name:
            candidates.append(name)
    name, _, line_candidates = extract_value_after_label(lines, "姓名", MEMBER_STOP_LABELS, max_lookahead=5, validator=_is_valid_member_name)
    candidates.extend(line_candidates)
    if name:
        candidates.append(name)
    id_line_index = None
    for index, line in enumerate(lines):
        if ID_NUMBER_PATTERN.search(line):
            id_line_index = index
            break
    search_lines = lines if id_line_index is None else lines[max(0, id_line_index - 8): id_line_index]
    for line in search_lines:
        for match in re.finditer(r"[\u4e00-\u9fff]{2,8}", line):
            name = _clean_person_name(match.group(0))
            if name:
                candidates.append(name)
    return list(dict.fromkeys(candidates))


def _extract_name(segment: str) -> tuple[str, str, str]:
    value, ev = _extract_field(segment, ("姓名", "姓 名"), max_chars=60)
    relation_from_name = ""
    split_name, split_relation = _split_name_relation(value)
    if split_name:
        return split_name, split_relation, ev
    name = _clean_person_name(value)
    if name:
        return name, relation_from_name, ev
    for line in _lines(segment):
        if "姓名" not in line and not re.search(r"姓\s*名", line):
            continue
        match = re.search(r"姓\s*名\s*[:：]?\s*([\u4e00-\u9fff]{2,8})", line)
        if match:
            name = _clean_person_name(match.group(1))
            if name:
                return name, "", line
    lines = _lines(segment)
    name, ev, candidates = extract_value_after_label(lines, "姓名", MEMBER_STOP_LABELS, max_lookahead=5, validator=_is_valid_member_name)
    if name:
        return name, "", ev
    candidates = _name_candidates_from_block(segment)
    if candidates:
        return candidates[0], "", candidates[0]
    return "", "", ""


def _extract_relation(segment: str, relation_hint: str = "") -> tuple[str, str]:
    if relation_hint in RELATION_VALUES:
        return relation_hint, relation_hint
    value, ev = _extract_field(segment, ("户主或与户主关系", "户主关系", "与户主关系"), max_chars=50)
    relation = _first_choice(value, RELATION_VALUES)
    if relation:
        return relation, ev
    relation_re = "|".join(re.escape(item) for item in RELATION_VALUES)
    match = re.search(rf"(?:户主或与户主关系|与户主关系|户主关系|关系)\s*[:：]?\s*({relation_re})", _flat(segment))
    if match:
        return match.group(1), match.group(0)
    return "", ""


def _extract_gender(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("性别",), max_chars=20)
    if "男" in value:
        return "男", ev
    if "女" in value:
        return "女", ev
    return "", ""


def _extract_ethnicity(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("民族",), max_chars=30)
    if "汉" in value:
        return "汉族", ev
    match = re.search(r"([\u4e00-\u9fff]{1,4})族", value)
    if match:
        return f"{match.group(1)}族", ev
    return "", ""


def _clean_place(value: str) -> str:
    text = _compact(value)
    for stop in ("出生日期", "公民身份号码", "身份证件编号", "文化程度", "婚姻状况", "兵役状况", "身高", "血型", "宗教信仰", "服务处所", "职业"):
        index = text.find(stop)
        if index >= 0:
            text = text[:index]
    if len(text) % 2 == 0:
        half = len(text) // 2
        if text[:half] == text[half:]:
            text = text[:half]
    for city in ("北京市", "上海市", "天津市", "重庆市"):
        if text.startswith(city):
            tail = text[len(city):]
            if re.fullmatch(r"[\u4e00-\u9fff]{1,4}市", tail):
                text = city
            break
    text = text.replace("上海市明市", "上海市")
    if text == "上海市江阴市":
        text = "上海市"
    return text if 2 <= len(text) <= 30 else ""


def _extract_date_field(segment: str, labels: tuple[str, ...]) -> tuple[str, str]:
    value, ev = _extract_field(segment, labels, max_chars=80)
    found = _date_to_iso(value)
    if found:
        return found, ev
    return "", ""


def _extract_id_number(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("公民身份号码", "身份证件编号", "身份证号码", "居民身份证号码"), max_chars=80)
    match = ID_NUMBER_PATTERN.search(value)
    if not match:
        match = ID_NUMBER_PATTERN.search(segment)
        ev = match.group(0) if match else ""
    if match:
        return match.group(0).upper(), ev
    return "", ""


def _extract_choice_field(segment: str, labels: tuple[str, ...], choices: tuple[str, ...], *, normalize_marital: bool = False) -> tuple[str, str]:
    value, ev = _extract_field(segment, labels, max_chars=80)
    choice = _first_choice(value, choices)
    if not choice:
        return "", ""
    if normalize_marital and choice == "有配偶":
        return "已婚", ev
    if normalize_marital and choice == "离婚":
        return "离异", ev
    return choice, ev


def _extract_education_level(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("文化程度",), max_chars=100)
    text = _compact(value)
    for invalid in ("已婚", "未婚", "有配偶", "孙魁", "退休工人", "统计人员", "未服兵役", "服务处所", "职业"):
        if text == invalid:
            return "", ""
    education = _normalize_education_level(text)
    if education:
        return education, ev
    return "", ""


def _extract_height(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("身高",), max_chars=40)
    match = re.search(r"(\d{2,3})", value)
    if not match:
        return "", ""
    height = int(match.group(1))
    if 150 <= height <= 220:
        return f"{height}cm", ev
    return "", ""


def _extract_text_field(segment: str, labels: tuple[str, ...], *, max_chars: int = 120, reject: tuple[str, ...] = ()) -> tuple[str, str]:
    value, ev = _extract_field(segment, labels, max_chars=max_chars)
    text = _compact(value)
    is_migration_field = any("何时由何地迁来" in label for label in labels)
    stops = ("何时由何地", "迁来", "登记日期", "公民身份号码") if not is_migration_field else ("登记日期", "公民身份号码")
    for stop in stops:
        if stop in text:
            text = text[: text.find(stop)]
    if any(item in text for item in reject):
        return "", ""
    return (text, ev) if text else ("", "")


def _clean_migration_address(value: str) -> str:
    text = _normalize_dates_in_text(value)
    for noise in (
        "承办人签章",
        "登记日期",
        "常住人口登记卡",
        "登记事项变更",
        "户口受理章",
        "户口受",
        "户口专用章",
        "户口",
        "受莲章",
        "章门口",
        "门口如",
        "专用章",
        "公民身份号码",
        "姓名",
        "性别",
        "民族",
        "出生日期",
        "文化程度",
        "服务处所",
        "职业",
        "派出所",
        "公安局",
    ):
        index = text.find(noise)
        if index >= 0:
            text = text[:index]
    return text.strip(" :：,，;；")


def _normalize_education_level(value: Any) -> str:
    text = _compact(value)
    if not text:
        return ""
    for stop in ("婚姻状况", "兵役状况", "服务处所", "职业", "身高", "血型", "何时由何地", "登记日期", "承办人签章", "户口受理章"):
        index = text.find(stop)
        if index >= 0:
            text = text[:index]
    if "逸夫职校" in text or "职业学校" in text:
        return "职校"
    if "职业高中" in text:
        return "职高"
    if "中等专业学校或中等技术学校" in text:
        return "中专"
    if "中等专业学校" in text or "中等技术学校" in text:
        return "中专"
    education = _first_choice(text, EDUCATION_VALUES)
    if education in {"职业学校"}:
        return "职校"
    if education == "职业高中":
        return "职高"
    if education in {"中等专业学校", "中等技术学校", "中等专业学校或中等技术学校"}:
        return "中专"
    return education


def clean_hukou_field_value(field_name: str, value: Any) -> str:
    text = _compact(value)
    if not text:
        return ""
    if field_name in {"migration_to_city", "migration_to_address"}:
        return _clean_migration_address(text)
    if field_name == "education_level":
        return _normalize_education_level(text)
    if field_name == "service_place":
        education = _normalize_education_level(text)
        if education and any(token in text for token in ("职校", "技校", "学校", "文化程度", "户口受理章", "承办人签章")):
            return ""
    for noise in ("承办人签章", "户口受理章", "户口专用章", "专用章", "派出所", "公安局", "文化程度"):
        index = text.find(noise)
        if index >= 0:
            text = text[:index]
    if field_name in {"service_place", "occupation"} and any(noise in text for noise in ("何时由何地", "迁来", "登记日期", "公民身份号码", "户口", "章")):
        return ""
    return text.strip(" :：,，;；")


def _split_member_segments(text: str) -> list[str]:
    if "常住人口登记卡" in text:
        parts = re.split(r"常住人口登记卡", text)
        return [part for part in parts[1:] if _text(part)]
    if _has_member_card(text):
        return [text]
    return []


def _member_block_around_id(text: str, id_number: str, page_index: int | None = None) -> str:
    positions = [match.start() for match in re.finditer(re.escape(id_number), text or "", re.I)]
    if not positions:
        return ""
    marker = "常住人口登记卡"
    best: tuple[int, int] | None = None
    for pos in positions:
        start = text.rfind(marker, 0, pos + 1)
        score = start if start >= 0 else -1
        if best is None or score > best[0]:
            best = (score, pos)
    marker_start, pos = best or (-1, positions[0])
    if marker_start >= 0:
        next_boundaries = [
            index
            for index in (
                text.find(marker, marker_start + len(marker)),
                text.find("登记事项变更和更正记载", marker_start + len(marker)),
            )
            if index >= 0
        ]
        end = min(next_boundaries) if next_boundaries else len(text)
        block = text[marker_start:end]
        if "姓名" not in block:
            block = text[max(0, pos - 1600): min(len(text), pos + 1600)]
        logger.info(
            "[HUKOU_BLOCK_BY_ID] id_number=%s page=%s block_start=%s block_end=%s block_preview=%s",
            id_number,
            page_index,
            marker_start,
            end,
            raw_preview(block),
        )
        return block

    lines = re.split(r"[\r\n]+", text or "")
    current = 0
    id_line_index = 0
    for index, line in enumerate(lines):
        current += len(line) + 1
        if current >= pos:
            id_line_index = index
            break
    start_line = max(0, id_line_index - 30)
    for index in range(id_line_index, max(-1, id_line_index - 31), -1):
        compact_line = _compact(lines[index])
        if "常住人口登记卡" in compact_line or compact_line in {"姓名", "姓名栏"} or compact_line.startswith("姓名"):
            start_line = index
            break
    end_line = min(len(lines), id_line_index + 31)
    for index in range(id_line_index + 1, min(len(lines), id_line_index + 31)):
        compact_line = _compact(lines[index])
        if "常住人口登记卡" in compact_line or "登记事项变更和更正记载" in compact_line:
            end_line = index
            break
        if compact_line.startswith("姓名") and index > id_line_index + 1:
            end_line = index
            break
        if ID_NUMBER_PATTERN.search(compact_line) and index > id_line_index + 1:
            end_line = index
            break
    logger.info(
        "[HUKOU_BLOCK_BY_ID] id_number=%s page=%s block_start=%s block_end=%s block_preview=%s",
        id_number,
        page_index,
        start_line,
        end_line,
        raw_preview("\n".join(lines[start_line:end_line])),
    )
    return "\n".join(lines[start_line:end_line])


def score_id_number_candidate(id_number: str, member_block: str, page_index: int | None = None) -> int:
    score = 0
    block = member_block or ""
    compact = _compact(block)
    if "常住人口登记卡" in compact:
        score += 60
    if re.search(r"(?:公民身份号码|身份证件编号|身份证号码|居民身份证号码)\s*[:：]?\s*" + re.escape(id_number), _canonical_member_labels(_flat(block)), re.I):
        score += 40
    elif "公民身份号码" in compact or "身份证件编号" in compact:
        score += 25
    block_birth = _date_to_iso(_value_between(block, ("出生日期", "出生年月"), MEMBER_STOP_LABELS, max_chars=80)[0])
    id_birth = _id_birth_date(id_number)
    if block_birth and id_birth:
        if block_birth == id_birth:
            score += 30
        else:
            score -= 100
    parsed_member, _ = parse_member_from_block(block, page_index)
    parsed_id = str(parsed_member.get("id_number") or "")
    if parsed_id and parsed_id != str(id_number).upper():
        score -= 100
    if parsed_member.get("name"):
        score += 20
    if parsed_member.get("gender") or parsed_member.get("ethnicity"):
        score += 20
    if "登记事项变更和更正记载" in compact and "常住人口登记卡" not in compact:
        score -= 80
    if not is_valid_chinese_id_number(id_number):
        score -= 100
    if str(id_number).startswith("210108") and any(candidate.startswith("310108") for candidate in _id_numbers(block) if candidate != id_number):
        score -= 100
    logger.info(
        "[HUKOU_ID_CANDIDATE] id_number=%s page=%s area=%s score=%s",
        id_number,
        page_index,
        "member_card" if "常住人口登记卡" in compact else ("change_record" if "登记事项变更和更正记载" in compact else "unknown"),
        score,
    )
    return score


def parse_member_from_block(block_text: str, page_index: int | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    page = page_index
    member: dict[str, Any] = {"page_index": page, "source_pages": [page] if page is not None else []}
    evidence: dict[str, Any] = {}
    segment = block_text
    initial_id, _ = _extract_id_number(segment)
    if initial_id:
        logger.info("[HUKOU_MEMBER_BLOCK] page=%s id_number=%s block_preview=%s", page, initial_id, raw_preview(segment))

    name, relation_hint, ev = _extract_name(segment)
    if name:
        member["name"] = name
        evidence["name"] = _make_evidence(name, ev, page)

    relation, ev = _extract_relation(segment, relation_hint)
    if relation:
        member["relationship_to_head"] = relation
        evidence["relationship_to_head"] = _make_evidence(relation, ev, page)

    for field, extractor in (
        ("gender", _extract_gender),
        ("ethnicity", _extract_ethnicity),
    ):
        value, ev = extractor(segment)
        if value:
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    for field, labels in (
        ("former_name", ("曾用名", "曾 用 名")),
        ("birth_place", ("出生地",)),
        ("native_place", ("籍贯",)),
        ("other_address", ("本市(县)其他住址", "本市（县）其他住址", "本市县其他住址")),
    ):
        value, ev = _extract_text_field(segment, labels, max_chars=100)
        value = _clean_place(value)
        if value:
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    for field, labels in (
        ("birth_date", ("出生日期", "出生年月")),
        ("registration_date", ("登记日期",)),
    ):
        value, ev = _extract_date_field(segment, labels)
        if value:
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    id_number, ev = _extract_id_number(segment)
    if id_number:
        member["id_number"] = id_number
        evidence["id_number"] = _make_evidence(id_number, ev, page, 0.86)
        if not member.get("birth_date"):
            member["birth_date"] = _id_birth_date(id_number)
        candidates = _name_candidates_from_block(segment)
        logger.info("[HUKOU_NAME_CANDIDATES] page=%s id_number=%s candidates=%s", page, id_number, candidates)
        if member.get("name"):
            logger.info("[HUKOU_NAME_SELECTED] page=%s id_number=%s name=%s source=member_block", page, id_number, member.get("name"))
        else:
            logger.info(
                "[HUKOU_NAME_EMPTY] page=%s id_number=%s reason=no_valid_name_candidate block_preview=%s",
                page,
                id_number,
                raw_preview(segment),
            )

    education, ev = _extract_education_level(segment)
    if education:
        member["education_level"] = education
        evidence["education_level"] = _make_evidence(education, ev, page)

    for field, labels, choices, marital in (
        ("marital_status", ("婚姻状况",), MARITAL_VALUES, True),
        ("military_status", ("兵役状况",), MILITARY_VALUES, False),
        ("blood_type", ("血型",), BLOOD_VALUES, False),
        ("religion", ("宗教信仰",), RELIGION_VALUES, False),
    ):
        value, ev = _extract_choice_field(segment, labels, choices, normalize_marital=marital)
        if value:
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    height, ev = _extract_height(segment)
    if height:
        member["height"] = height
        evidence["height"] = _make_evidence(height, ev, page)

    for field, labels, reject in (
        ("service_place", ("服务处所",), ("派出所", "公安局", "何时由何地", "迁来", "登记日期", "承办人签章")),
        ("occupation", ("职业",), ("何时由何地", "首次申报", "迁来", "登记日期", "身份号码")),
    ):
        value, ev = _extract_text_field(segment, labels, max_chars=80, reject=reject)
        if value:
            if field == "service_place" and not member.get("education_level"):
                education = _normalize_education_level(value)
                if education and any(token in value for token in ("职校", "技校", "学校")):
                    member["education_level"] = education
                    evidence["education_level"] = _make_evidence(education, ev, page)
            cleaned = clean_hukou_field_value(field, value)
            if cleaned:
                member[field] = cleaned
                evidence[field] = _make_evidence(member[field], ev, page)

    for field, labels in (
        ("migration_to_city", ("何时由何地迁来本市（县）", "何时由何地迁来本市(县)", "何时由何地迁来本市")),
        ("migration_to_address", ("何时由何地迁来本址",)),
    ):
        value, ev = _extract_text_field(segment, labels, max_chars=160, reject=("注意事项",))
        if value:
            value = _normalize_dates_in_text(value)
            value = clean_hukou_field_value(field, value)
            if field == "migration_to_address" and not any(keyword in value for keyword in ("省", "市", "县", "区", "镇", "村", "路", "弄", "号", "室", "迁来", "迁入")):
                continue
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    for field in ("migration_to_city", "migration_to_address", "service_place", "occupation", "education_level"):
        if member.get(field):
            cleaned = clean_hukou_field_value(field, member.get(field))
            if cleaned:
                member[field] = cleaned
            else:
                member.pop(field, None)

    return member, evidence


def _extract_member(segment: str, page: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    return parse_member_from_block(segment, page)


def _meaningful_member(member: dict[str, Any]) -> bool:
    return bool(
        member.get("id_number")
        or (member.get("name") and member.get("birth_date"))
        or (member.get("name") and member.get("relationship_to_head") and member.get("gender"))
    )


def _member_score(member: dict[str, Any]) -> int:
    return sum(1 for field in MEMBER_FIELDS if member.get(field) not in (None, "", [], {}))


def _merge_member(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    current_id = str(current.get("id_number") or "")
    candidate_id = str(candidate.get("id_number") or "")
    if current_id and candidate_id and current_id != candidate_id:
        logger.info("[HUKOU_MERGE_SKIP] from_id=%s to_id=%s reason=different_id_number", candidate_id, current_id)
        return current
    merged = dict(current)
    current_pages = set(merged.get("source_pages") or ([] if merged.get("page_index") is None else [merged.get("page_index")]))
    candidate_pages = set(candidate.get("source_pages") or ([] if candidate.get("page_index") is None else [candidate.get("page_index")]))
    merged["source_pages"] = sorted(page for page in current_pages | candidate_pages if page is not None)
    if not merged.get("page_index") and candidate.get("page_index"):
        merged["page_index"] = candidate.get("page_index")
    for field, value in candidate.items():
        if field in {"page_index", "source_pages"}:
            continue
        if not value:
            continue
        old_value = merged.get(field)
        if old_value in (None, "", "未识别"):
            if old_value != value and merged.get("id_number"):
                logger.info(
                    "[HUKOU_MEMBER_FIELD_MERGE] id_number=%s field=%s old=%s new=%s",
                    merged.get("id_number"),
                    field,
                    old_value or "未识别",
                    value,
                )
            merged[field] = value
    return merged


def validate_member_identity_consistency(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, list[dict[str, Any]]] = {}
    for member in members:
        name = str(member.get("name") or "")
        if name:
            by_name.setdefault(name, []).append(member)

    for name, items in by_name.items():
        birth_values = {str(item.get("birth_date") or _id_birth_date(str(item.get("id_number") or ""))) for item in items}
        birth_values.discard("")
        if len(birth_values) <= 1:
            continue
        best = max(items, key=lambda item: int(item.get("_id_score") or 0))
        for item in items:
            if item is best:
                continue
            logger.info(
                "[HUKOU_IDENTITY_CONFLICT] id_number=%s wrong_name=%s reason=name_birth_conflict",
                item.get("id_number") or "",
                name,
            )
            for field in ("name", "relationship_to_head", "gender", "ethnicity", "birth_place", "native_place"):
                item.pop(field, None)
    return members


def _dedupe_members(members: list[dict[str, Any]], evidences: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    bucket_evidence: dict[str, dict[str, Any]] = {}
    for member, member_evidence in zip(members, evidences):
        id_number = str(member.get("id_number") or "").strip()
        key = id_number if id_number else f"no_id:{len(buckets)}"
        if not id_number and not _meaningful_member(member):
            continue
        if key not in buckets:
            if member.get("id_number"):
                logger.info("[HUKOU_MEMBER_MERGE] id_number=%s action=keep_new", member.get("id_number"))
            buckets[key] = dict(member)
            bucket_evidence[key] = dict(member_evidence)
            continue
        if member.get("id_number"):
            logger.info("[HUKOU_MEMBER_MERGE] id_number=%s action=merge_existing", member.get("id_number"))
        if _member_score(member) > _member_score(buckets[key]):
            buckets[key], member = dict(member), buckets[key]
            bucket_evidence[key], member_evidence = dict(member_evidence), bucket_evidence[key]
        buckets[key] = _merge_member(buckets[key], member)
        bucket_evidence[key].update({field: item for field, item in member_evidence.items() if field not in bucket_evidence[key] and item})

    relation_order = {"户主": 0, "妻": 1, "夫": 1, "子": 2, "长子": 2, "次子": 2, "女": 3, "长女": 3, "次女": 3}
    ordered = sorted(
        buckets.values(),
        key=lambda item: (
            relation_order.get(str(item.get("relationship_to_head") or ""), 9),
            str(item.get("birth_date") or ""),
            str(item.get("name") or ""),
        ),
    )
    id_to_name = {
        str(member.get("id_number") or ""): str(member.get("name") or "")
        for member in ordered
        if member.get("id_number") and _is_valid_member_name(member.get("name"))
    }
    for member in ordered:
        id_number = str(member.get("id_number") or "")
        name = str(member.get("name") or "")
        if not _is_valid_member_name(name):
            fallback = id_to_name.get(id_number)
            if fallback:
                member["name"] = fallback

    ordered = validate_member_identity_consistency(ordered)
    by_name_birth: dict[str, dict[str, Any]] = {}
    for member in ordered:
        name = str(member.get("name") or "")
        birth_date = str(member.get("birth_date") or "")
        key = f"{name}|{birth_date}" if name and birth_date else f"unique:{len(by_name_birth)}"
        existing = by_name_birth.get(key)
        if not existing:
            by_name_birth[key] = member
            continue
        existing_score = int(existing.get("_id_score") or 0)
        member_score = int(member.get("_id_score") or 0)
        keep, drop = (member, existing) if member_score > existing_score else (existing, member)
        merged = _merge_member(dict(keep), drop)
        logger.info(
            "[HUKOU_MEMBER_DEDUPE] action=merge_by_name_birth name=%s keep_id=%s drop_id=%s",
            name,
            keep.get("id_number") or "",
            drop.get("id_number") or "",
        )
        by_name_birth[key] = merged
    ordered = list(by_name_birth.values())
    for member in ordered:
        member.pop("_id_score", None)

    evidence: dict[str, Any] = {}
    for index, member in enumerate(ordered):
        key = str(member.get("id_number") or "").strip() or f"{member.get('name') or ''}|{member.get('birth_date') or ''}"
        for field, item in (bucket_evidence.get(key) or {}).items():
            evidence[f"members[{index}].{field}"] = item
    return ordered, evidence


def _extract_member_candidates_from_ids(page_text: str, page: int | None) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
    candidates: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for id_number in _id_numbers(page_text):
        block = _member_block_around_id(page_text, id_number, page)
        if not block:
            continue
        score = score_id_number_candidate(id_number, block, page)
        if score < 50:
            logger.info("[HUKOU_ID_REJECTED] id_number=%s score=%s reason=low_confidence_or_invalid", id_number, score)
            continue
        member, member_evidence = _extract_member(block, page)
        if not member.get("id_number"):
            member["id_number"] = id_number
            member["birth_date"] = _id_birth_date(id_number)
            member_evidence["id_number"] = _make_evidence(id_number, id_number, page, 0.75)
        member["_id_score"] = score
        candidates.append((member, member_evidence, "valid_id_number"))
    return candidates


def _backfill_household_info_from_members(household_info: dict[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    info = dict(household_info)
    number_from_address, clean_address = _split_leading_household_number_from_address(str(info.get("household_address") or ""))
    if number_from_address:
        if not info.get("household_number"):
            info["household_number"] = number_from_address
        info["household_address"] = clean_address
    head_member = next(
        (
            member
            for member in members
            if isinstance(member, dict)
            and member.get("relationship_to_head") == "户主"
            and _is_valid_member_name(member.get("name"))
        ),
        None,
    )
    if head_member and not _is_valid_member_name(info.get("household_head")):
        info["household_head"] = head_member.get("name")
    if not info.get("household_type"):
        for record_type in HOUSEHOLD_TYPE_VALUES:
            if record_type in str(household_info):
                info["household_type"] = record_type
                break
    return info


def _dedupe_household_records(records: list[dict[str, Any]], member_id_numbers: set[str] | None = None) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        source_area = str(record.get("_source_area") or "household_home_area")
        record = _clean_household_record(record, member_id_numbers)
        if not is_valid_household_record(record, source_area, member_id_numbers):
            continue
        record = {key: value for key, value in record.items() if not str(key).startswith("_")}
        if not any(value for key, value in record.items() if key != "address_change_records"):
            continue
        key = "|".join(str(record.get(field) or "") for field in ("household_number", "household_address", "issue_date"))
        if key not in deduped:
            deduped[key] = dict(record)
            continue
        for field, value in record.items():
            if value and not deduped[key].get(field):
                deduped[key][field] = value
    return sorted(deduped.values(), key=lambda item: str(item.get("issue_date") or ""), reverse=True)


def _select_primary_household(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {field: ([] if field == "address_change_records" else "") for field in HOUSEHOLD_INFO_FIELDS}
    return dict(records[0])


def _confidence(fields: dict[str, Any]) -> dict[str, Any]:
    household_info = fields.get("household_info") if isinstance(fields.get("household_info"), dict) else {}
    members = fields.get("members") if isinstance(fields.get("members"), list) else []
    present = sum(1 for key in ("household_head", "household_address", "household_type", "issue_date") if household_info.get(key))
    present += min(len(members), 4)
    return {"overall": round(min(0.95, 0.25 + present * 0.1), 2), "fields": {}}


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    raw_text = str(data.get("text") or "")
    page_texts = _page_texts(data)
    if not raw_text and page_texts:
        raw_text = "\n".join(text for _, text in page_texts)

    household_records: list[dict[str, Any]] = []
    address_change_records: list[str] = []
    members: list[dict[str, Any]] = []
    member_evidences: list[dict[str, Any]] = []
    evidence: dict[str, Any] = {}

    for page, page_text in page_texts:
        page_ids = _id_numbers(page_text)
        logger.info(
            "[HUKOU_DEBUG] page=%s text_len=%s has_member_card=%s has_change_record=%s id_numbers=%s",
            page,
            len(page_text or ""),
            _has_member_card(page_text),
            "登记事项变更和更正记载" in _compact(page_text),
            page_ids,
        )
        if page_ids:
            logger.info("[HUKOU_DEBUG] page=%s id_numbers=%s", page, page_ids)

        if _has_home_page(page_text) and _home_feature_count(page_text) >= 3:
            info, info_evidence = _extract_household_info_from_page(page_text, page)
            if any(value for key, value in info.items() if key != "address_change_records"):
                info["_source_area"] = "household_home_area"
                household_records.append(info)
                evidence.update(info_evidence)
        if _has_address_change_area(page_text):
            address_change_records.extend(_extract_valid_address_change_records(page_text))

        if _has_member_card(page_text):
            for segment in _split_member_segments(page_text):
                member, member_evidence = _extract_member(segment, page)
                if member.get("id_number"):
                    member["_id_score"] = score_id_number_candidate(str(member.get("id_number")), segment, page)
                if _meaningful_member(member):
                    logger.info(
                        "[HUKOU_MEMBER_CANDIDATE] page=%s name=%s id_number=%s relation=%s keep=true reason=member_card",
                        page,
                        member.get("name") or "",
                        member.get("id_number") or "",
                        member.get("relationship_to_head") or "",
                    )
                    members.append(member)
                    member_evidences.append(member_evidence)
                else:
                    logger.info(
                        "[HUKOU_MEMBER_DROPPED] page=%s id_number=%s reason=not_meaningful_member_card",
                        page,
                        member.get("id_number") or "",
                    )
        for member, member_evidence, reason in _extract_member_candidates_from_ids(page_text, page):
            if _meaningful_member(member):
                logger.info(
                    "[HUKOU_MEMBER_CANDIDATE] page=%s name=%s id_number=%s relation=%s keep=true reason=%s",
                    page,
                    member.get("name") or "",
                    member.get("id_number") or "",
                    member.get("relationship_to_head") or "",
                    reason,
                )
                members.append(member)
                member_evidences.append(member_evidence)
            else:
                logger.info(
                    "[HUKOU_MEMBER_DROPPED] page=%s id_number=%s reason=not_meaningful_%s",
                    page,
                    member.get("id_number") or "",
                    reason,
                )

    members, member_evidence = _dedupe_members(members, member_evidences)
    seen_member_ids = {str(member.get("id_number") or "") for member in members if isinstance(member, dict) and member.get("id_number")}
    all_ocr_ids = list(dict.fromkeys(id_number for _, page_text in page_texts for id_number in _id_numbers(page_text)))
    missing_ocr_ids = [id_number for id_number in all_ocr_ids if id_number not in seen_member_ids]
    if missing_ocr_ids:
        for missing_id in missing_ocr_ids:
            for page, page_text in page_texts:
                if missing_id not in _id_numbers(page_text):
                    continue
                logger.info("[HUKOU_MEMBER_BACKFILL] id_number=%s reason=id_seen_in_ocr_but_missing_from_members", missing_id)
                block = _member_block_around_id(page_text, missing_id, page)
                score = score_id_number_candidate(missing_id, block, page)
                if score < 50:
                    logger.info("[HUKOU_ID_REJECTED] id_number=%s score=%s reason=low_confidence_or_invalid", missing_id, score)
                    break
                logger.info(
                    "[HUKOU_BACKFILL_BLOCK] id_number=%s page=%s block_preview=%s",
                    missing_id,
                    page,
                    raw_preview(block),
                )
                member, member_evidence_item = _extract_member(block, page)
                logger.info(
                    "[HUKOU_BACKFILL_MEMBER] id_number=%s name=%s relation=%s gender=%s ethnicity=%s",
                    missing_id,
                    member.get("name") or "",
                    member.get("relationship_to_head") or "",
                    member.get("gender") or "",
                    member.get("ethnicity") or "",
                )
                member["id_number"] = missing_id
                if not member.get("birth_date"):
                    member["birth_date"] = _id_birth_date(missing_id)
                member["_id_score"] = score
                members.append(member)
                member_index = len(members) - 1
                for field, item in member_evidence_item.items():
                    member_evidence[f"members[{member_index}].{field}"] = item
                break
    evidence.update(member_evidence)
    logger.info(
        "[HUKOU_MEMBERS_FINAL] count=%s ids=%s",
        len(members),
        [member.get("id_number") for member in members if isinstance(member, dict) and member.get("id_number")],
    )
    member_id_numbers = {str(member.get("id_number") or "") for member in members if isinstance(member, dict) and member.get("id_number")}
    household_records = _dedupe_household_records(household_records, member_id_numbers)
    household_info = _select_primary_household(household_records)
    household_info = _backfill_household_info_from_members(household_info, members)
    if household_records:
        household_records[0] = _backfill_household_info_from_members(household_records[0], members)
    if address_change_records:
        household_info["address_change_records"] = list(dict.fromkeys(address_change_records))

    fields = {
        "household_info": household_info,
        "household_records": household_records,
        "members": members,
    }
    result = build_result("household_register", fields, evidence)
    result["raw_text_preview"] = raw_preview(raw_text)
    result["confidence"] = _confidence(fields)
    result["extraction_status"] = "partial" if (members or household_records) else "failed"
    return result
