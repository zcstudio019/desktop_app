from __future__ import annotations

import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


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
EDUCATION_VALUES = ("文盲或半文盲", "半文盲", "研究生", "博士", "硕士", "本科", "大专", "中专", "高中", "初中", "小学", "文盲", "不详")
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


def _text(value: Any) -> str:
    return str(value or "").replace("\u3000", " ").strip()


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", _text(text)).strip()


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
        or bool(re.search(r"(?:No\.?|NO\.?|Nº|N°|编号|户口簿编号)\s*[:：]?\s*[0-9A-Za-z]{6,12}", text, re.I))
    )


def _has_address_change_area(text: str) -> bool:
    compact = _compact(text)
    return "住址变动登记" in compact and ("变动后的住址" in compact or "变动日期" in compact)


def _has_member_card(text: str) -> bool:
    compact = _compact(text)
    return (
        ("常住人口登记卡" in compact and ("姓名" in compact or "公民身份号码" in compact))
        or ("姓名" in compact and "户主或与户主关系" in compact and "公民身份号码" in compact)
        or ("姓名" in compact and "户主或与户主关系" in compact and "出生日期" in compact)
        or (bool(re.search(r"\d{17}[\dXx]", compact)) and "出生日期" in compact and "性别" in compact and "民族" in compact)
        or ("出生日期" in compact and "公民身份号码" in compact and ("身高" in compact or "血型" in compact))
    )


def _value_between(text: str, labels: tuple[str, ...], stops: tuple[str, ...], max_chars: int = 120) -> tuple[str, str]:
    flat = _flat(text)
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
    for bad in ("或与", "性别", "民族", "出生", "公民身份号码", "姓名", "曾用名", "何时由何地"):
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
    return name


def _is_valid_member_name(value: Any) -> bool:
    text = _compact(value)
    if not text:
        return False
    if any(fragment in text for fragment in NOISY_NAME_FRAGMENTS):
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

    match = re.search(r"(?:No\.?|NO|Nº|N°|编号|户口簿编号)\s*[:：]?\s*([0-9A-Za-z]{6,12})", text, re.I)
    if match:
        candidate = match.group(1)
        if candidate != info.get("household_number") and not re.fullmatch(r"\d{17}[\dXx]", candidate):
            info["booklet_number"] = candidate
            evidence["household_info.booklet_number"] = _make_evidence(candidate, match.group(0), page)

    match = re.search(r"承办人签章\s*[:：]?\s*([\u4e00-\u9fff]{2,4})", _flat(text))
    if match:
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
        if ("变动后的住址" in line or "变动日期" in line) and len(line) <= 120:
            records.append(line)
    return list(dict.fromkeys(records))


def _extract_field(segment: str, labels: tuple[str, ...], max_chars: int = 120) -> tuple[str, str]:
    return _value_between(segment, labels, MEMBER_STOP_LABELS, max_chars=max_chars)


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
    for stop in ("出生日期", "公民身份号码", "文化程度", "婚姻状况"):
        index = text.find(stop)
        if index >= 0:
            text = text[:index]
    return text if 2 <= len(text) <= 30 else ""


def _extract_date_field(segment: str, labels: tuple[str, ...]) -> tuple[str, str]:
    value, ev = _extract_field(segment, labels, max_chars=80)
    found = _date_to_iso(value)
    if found:
        return found, ev
    return "", ""


def _extract_id_number(segment: str) -> tuple[str, str]:
    value, ev = _extract_field(segment, ("公民身份号码", "身份证件编号", "身份证号码", "居民身份证号码"), max_chars=80)
    match = re.search(r"\d{17}[\dXx]", value)
    if not match:
        match = re.search(r"\d{17}[\dXx]", segment)
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
    for noise in ("户口受", "户口", "受莲章", "章门口", "门口如", "公民身份号码", "登记日期", "派出所", "公安局"):
        index = text.find(noise)
        if index >= 0:
            text = text[:index]
    return text.strip(" :：,，;；")


def _split_member_segments(text: str) -> list[str]:
    if "常住人口登记卡" in text:
        parts = re.split(r"常住人口登记卡", text)
        return [part for part in parts[1:] if _text(part)]
    if _has_member_card(text):
        return [text]
    return []


def _extract_member(segment: str, page: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    member: dict[str, Any] = {"page_index": page, "source_pages": [page] if page is not None else []}
    evidence: dict[str, Any] = {}

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
            member["birth_date"] = f"{id_number[6:10]}-{id_number[10:12]}-{id_number[12:14]}"

    for field, labels, choices, marital in (
        ("education_level", ("文化程度",), EDUCATION_VALUES, False),
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
            member[field] = value
            evidence[field] = _make_evidence(member[field], ev, page)

    for field, labels in (
        ("migration_to_city", ("何时由何地迁来本市（县）", "何时由何地迁来本市(县)", "何时由何地迁来本市")),
        ("migration_to_address", ("何时由何地迁来本址",)),
    ):
        value, ev = _extract_text_field(segment, labels, max_chars=160, reject=("注意事项",))
        if value:
            value = _normalize_dates_in_text(value)
            if field == "migration_to_address":
                value = _clean_migration_address(value)
            if field == "migration_to_address" and not any(keyword in value for keyword in ("省", "市", "县", "区", "镇", "村", "路", "弄", "号", "室", "迁来", "迁入")):
                continue
            member[field] = value
            evidence[field] = _make_evidence(value, ev, page)

    return member, evidence


def _meaningful_member(member: dict[str, Any]) -> bool:
    return bool(member.get("id_number") or (member.get("name") and member.get("birth_date")) or (member.get("name") and member.get("relationship_to_head")))


def _member_score(member: dict[str, Any]) -> int:
    return sum(1 for field in MEMBER_FIELDS if member.get(field) not in (None, "", [], {}))


def _merge_member(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
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
        if not merged.get(field):
            merged[field] = value
    return merged


def _dedupe_members(members: list[dict[str, Any]], evidences: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    bucket_evidence: dict[str, dict[str, Any]] = {}
    for member, member_evidence in zip(members, evidences):
        key = str(member.get("id_number") or "").strip() or f"{member.get('name') or ''}|{member.get('birth_date') or ''}"
        if not key.strip("|"):
            continue
        if key not in buckets:
            buckets[key] = dict(member)
            bucket_evidence[key] = dict(member_evidence)
            continue
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

    evidence: dict[str, Any] = {}
    for index, member in enumerate(ordered):
        key = str(member.get("id_number") or "").strip() or f"{member.get('name') or ''}|{member.get('birth_date') or ''}"
        for field, item in (bucket_evidence.get(key) or {}).items():
            evidence[f"members[{index}].{field}"] = item
    return ordered, evidence


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


def _dedupe_household_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
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
        if _has_home_page(page_text):
            info, info_evidence = _extract_household_info_from_page(page_text, page)
            if any(value for key, value in info.items() if key != "address_change_records"):
                household_records.append(info)
                evidence.update(info_evidence)
        if _has_address_change_area(page_text):
            address_change_records.extend(_extract_valid_address_change_records(page_text))

        if _has_member_card(page_text):
            for segment in _split_member_segments(page_text):
                member, member_evidence = _extract_member(segment, page)
                if _meaningful_member(member):
                    members.append(member)
                    member_evidences.append(member_evidence)

    members, member_evidence = _dedupe_members(members, member_evidences)
    evidence.update(member_evidence)
    household_records = _dedupe_household_records(household_records)
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
