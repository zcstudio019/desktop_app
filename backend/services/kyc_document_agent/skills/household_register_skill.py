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
)

HOUSEHOLD_INFO_LABELS: dict[str, tuple[str, ...]] = {
    "household_type": ("户别",),
    "household_number": ("户号",),
    "household_head": ("户主姓名",),
    "household_address": ("住址", "家庭住址"),
    "booklet_number": ("户口簿编号", "编号", "No.", "No"),
    "issuing_authority": ("签发机关", "户口登记机关"),
    "issue_date": ("签发日期",),
    "undertaker": ("承办人签章", "承办人"),
}

MEMBER_LABELS: dict[str, tuple[str, ...]] = {
    "name": ("姓名",),
    "former_name": ("曾用名",),
    "relationship_to_head": ("户主或与户主关系", "与户主关系"),
    "gender": ("性别",),
    "ethnicity": ("民族",),
    "birth_place": ("出生地",),
    "native_place": ("籍贯",),
    "birth_date": ("出生日期", "出生年月"),
    "other_address": ("本市(县)其他住址", "本市（县）其他住址", "本市县其他住址"),
    "id_number": ("公民身份号码", "身份证号码", "居民身份证号码"),
    "education_level": ("文化程度",),
    "marital_status": ("婚姻状况",),
    "military_status": ("兵役状况",),
    "height": ("身高",),
    "blood_type": ("血型",),
    "religion": ("宗教信仰",),
    "service_place": ("服务处所",),
    "occupation": ("职业",),
    "migration_to_city": ("何时由何地迁来本市（县）", "何时由何地迁来本市(县)", "何时由何地迁来本市县"),
    "migration_to_address": ("何时由何地迁来本址",),
    "registration_date": ("登记日期",),
}

ALL_LABELS = tuple(dict.fromkeys(sum((list(v) for v in HOUSEHOLD_INFO_LABELS.values()), []) + sum((list(v) for v in MEMBER_LABELS.values()), [])))

VALUE_STOP_LABELS = (
    "户别",
    "户号",
    "户主姓名",
    "住址",
    "签发机关",
    "户口登记机关",
    "签发日期",
    "承办人",
    "姓名",
    "曾用名",
    "户主或与户主关系",
    "与户主关系",
    "性别",
    "民族",
    "出生地",
    "籍贯",
    "出生日期",
    "本市",
    "公民身份号码",
    "身份证号码",
    "文化程度",
    "婚姻状况",
    "兵役状况",
    "身高",
    "血型",
    "宗教信仰",
    "服务处所",
    "职业",
    "何时由何地迁来",
    "登记日期",
    "签发",
    "常住人口登记卡",
    "登记事项变更",
    "住址变动登记",
)

INVALID_PERSON_VALUES = {
    "户主",
    "姓名",
    "性别",
    "民族",
    "住址",
    "户号",
    "户别",
    "不详",
    "无",
}


def _text(value: Any) -> str:
    return str(value or "").replace("\u3000", " ").strip()


def _collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", _text(value)).strip(" :：,，;；")


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", "", _text(value)).strip(" :：,，;；")


def _normalize_lines(text: str) -> list[str]:
    return [_collapse_spaces(line) for line in re.split(r"[\r\n]+", text or "") if _collapse_spaces(line)]


def _date_to_iso(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
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


def _make_evidence(value: str, evidence_text: str, page: int | None, confidence: float = 0.82) -> dict[str, Any]:
    return {
        "value": value,
        "evidence_text": evidence_text,
        "page": page,
        "confidence": confidence,
    }


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
    if chunks:
        return [(index, chunk) for index, chunk in enumerate(chunks, start=1)]
    return [(None, raw)]


def _label_pattern(labels: tuple[str, ...]) -> str:
    escaped = [re.escape(label) for label in labels]
    return "|".join(sorted(escaped, key=len, reverse=True))


def _extract_after_label(
    text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = VALUE_STOP_LABELS,
    *,
    max_chars: int = 80,
) -> tuple[str, str]:
    if not text:
        return "", ""
    label_re = _label_pattern(labels)
    stop_re = _label_pattern(tuple(label for label in stop_labels if label not in labels))
    pattern = re.compile(rf"({label_re})\s*[:：]?\s*(.{{0,{max_chars}}})", re.S)
    for match in pattern.finditer(text):
        value_part = match.group(2)
        stop_match = re.search(rf"\s*(?:{stop_re})", value_part) if stop_re else None
        if stop_match:
            value_part = value_part[: stop_match.start()]
        raw_value = value_part.strip()
        cleaned = _clean_common_value(raw_value)
        if cleaned:
            return cleaned, _collapse_spaces(match.group(0)[: len(match.group(1)) + len(value_part) + 6])
    return "", ""


def _extract_near_lines(text: str, labels: tuple[str, ...], *, max_following_lines: int = 3) -> tuple[str, str]:
    lines = _normalize_lines(text)
    for index, line in enumerate(lines):
        if not any(label in line for label in labels):
            continue
        inline, evidence = _extract_after_label(line, labels, max_chars=40)
        if inline:
            return inline, evidence or line
        for candidate in lines[index + 1 : index + 1 + max_following_lines]:
            if any(stop in candidate for stop in VALUE_STOP_LABELS):
                cleaned = _clean_common_value(candidate)
                if re.fullmatch(r"[\u4e00-\u9fff]{2,6}", cleaned) and cleaned not in INVALID_PERSON_VALUES:
                    return cleaned, f"{line} {candidate}"
                continue
            cleaned = _clean_common_value(candidate)
            if cleaned:
                return cleaned, f"{line} {candidate}"
    return "", ""


def _clean_common_value(value: Any) -> str:
    text = _collapse_spaces(str(value or ""))
    text = re.sub(r"^(?:[:：、，,;；\s]+)", "", text)
    text = re.sub(r"(?:[:：、，,;；\s]+)$", "", text)
    for label in ALL_LABELS:
        text = re.sub(rf"^{re.escape(label)}\s*[:：]?", "", text).strip()
    return text.strip(" :：,，;；")


def _normalize_person_name(value: Any) -> str:
    text = _compact_text(value)
    text = re.sub(r"(?:户主姓名|姓名|户主或与户主关系|与户主关系|户主|签发|登记)", "", text)
    if text in INVALID_PERSON_VALUES:
        return ""
    match = re.search(r"[\u4e00-\u9fff]{2,6}", text)
    return match.group(0) if match else ""


def _normalize_gender(value: Any) -> str:
    text = _compact_text(value)
    if "男" in text:
        return "男"
    if "女" in text:
        return "女"
    return ""


def _normalize_ethnicity(value: Any) -> str:
    text = _compact_text(value)
    text = re.sub(r"(?:民族|族别)", "", text)
    match = re.search(r"[\u4e00-\u9fff]{1,6}族?", text)
    if not match:
        return ""
    value = match.group(0)
    return value if value.endswith("族") else f"{value}族"


def _normalize_id_number(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or "")).upper()
    match = re.search(r"\d{17}[\dX]", text)
    return match.group(0) if match else ""


def _normalize_marital(value: Any) -> str:
    text = _compact_text(value)
    if not text:
        return ""
    if "未婚" in text:
        return "未婚"
    if "有配偶" in text or "已婚" in text:
        return "已婚"
    return text


def _normalize_height(value: Any) -> str:
    text = _compact_text(value)
    match = re.search(r"(\d{2,3})", text)
    if not match:
        return text
    return f"{match.group(1)}cm"


def _normalize_member_field(field: str, value: Any) -> str:
    if field in {"name", "former_name"}:
        return _normalize_person_name(value)
    if field == "gender":
        return _normalize_gender(value)
    if field == "ethnicity":
        return _normalize_ethnicity(value)
    if field in {"birth_date", "registration_date"}:
        return _date_to_iso(value)
    if field == "id_number":
        return _normalize_id_number(value)
    if field == "marital_status":
        return _normalize_marital(value)
    if field == "height":
        return _normalize_height(value)
    if field in {"relationship_to_head", "education_level", "military_status", "blood_type", "religion", "occupation"}:
        return _compact_text(value)
    return _clean_common_value(value)


def _normalize_household_info_field(field: str, value: Any) -> Any:
    if field == "issue_date":
        return _date_to_iso(value)
    if field in {"household_head"}:
        return _normalize_person_name(value)
    if field == "household_number":
        return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")
    if field == "household_address":
        text = _clean_common_value(value)
        text = re.sub(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日\s*签发.*$", "", text)
        text = re.sub(r"(?:19|20)\d{2}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日.*$", "", text)
        text = re.sub(r"(?:19|20)\d{2}[-./]\d{1,2}[-./]\d{1,2}.*$", "", text)
        text = re.sub(r"(?:签发|常住人口登记卡).*$", "", text)
        return _compact_text(text)
    if field == "address_change_records":
        return value if isinstance(value, list) else []
    return _clean_common_value(value)


def _extract_household_info_from_page(text: str, page: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    info = {field: "" for field in HOUSEHOLD_INFO_FIELDS}
    info["address_change_records"] = []
    evidence: dict[str, Any] = {}
    for field, labels in HOUSEHOLD_INFO_LABELS.items():
        if field == "household_address":
            value, ev = _extract_after_label(text, labels, max_chars=160)
        else:
            value, ev = _extract_after_label(text, labels, max_chars=80)
        if not value:
            value, ev = _extract_near_lines(text, labels)
        value = _normalize_household_info_field(field, value)
        if value:
            info[field] = value
            evidence[f"household_info.{field}"] = _make_evidence(str(value), ev or str(value), page)

    if not info.get("issue_date"):
        for line in _normalize_lines(text):
            if "签发" not in line:
                continue
            issue_date = _date_to_iso(line)
            if issue_date:
                info["issue_date"] = issue_date
                evidence["household_info.issue_date"] = _make_evidence(issue_date, line, page)
                break

    if not info.get("issuing_authority"):
        for line in _normalize_lines(text):
            if any(keyword in line for keyword in ("公安局", "派出所", "户口登记机关")):
                authority = re.sub(r"(?:签发机关|户口登记机关|签发|盖章|印章)[:：]?", "", line).strip()
                if len(authority) >= 4:
                    info["issuing_authority"] = authority
                    evidence["household_info.issuing_authority"] = _make_evidence(authority, line, page, 0.72)
                    break

    address_records = _extract_address_change_records(text)
    if address_records:
        info["address_change_records"] = address_records
        evidence["household_info.address_change_records"] = _make_evidence("；".join(address_records), "；".join(address_records), page, 0.7)
    return info, evidence


def _extract_address_change_records(text: str) -> list[str]:
    records: list[str] = []
    capture = False
    for line in _normalize_lines(text):
        if "住址变动登记" in line:
            capture = True
            continue
        if capture and any(stop in line for stop in ("常住人口登记卡", "登记事项变更", "户别", "户号")):
            capture = False
        if capture and len(line) >= 4:
            records.append(line)
        elif any(keyword in line for keyword in ("迁来", "迁往", "迁入", "迁出")) and len(line) >= 6:
            records.append(line)
    return list(dict.fromkeys(records))


def _split_member_segments(text: str) -> list[str]:
    parts = re.split(r"常住人口登记卡", text or "")
    if len(parts) > 1:
        return [part for part in parts[1:] if _text(part)]
    if any(label in text for label in ("户主或与户主关系", "公民身份号码", "出生日期")):
        return [text]
    return []


def _extract_member(segment: str, page: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    member: dict[str, Any] = {field: "" for field in MEMBER_FIELDS}
    member["page_index"] = page
    evidence: dict[str, Any] = {}
    for field, labels in MEMBER_LABELS.items():
        max_chars = 160 if field in {"other_address", "service_place", "migration_to_city", "migration_to_address"} else 80
        value, ev = _extract_after_label(segment, labels, max_chars=max_chars)
        if not value:
            value, ev = _extract_near_lines(segment, labels, max_following_lines=2)
        normalized = _normalize_member_field(field, value)
        if normalized:
            member[field] = normalized
            evidence[field] = _make_evidence(normalized, ev or str(value), page)
    if not member.get("id_number"):
        id_number = _normalize_id_number(segment)
        if id_number:
            member["id_number"] = id_number
            evidence["id_number"] = _make_evidence(id_number, id_number, page, 0.78)
    if not member.get("birth_date") and member.get("id_number"):
        code = str(member["id_number"])
        member["birth_date"] = f"{code[6:10]}-{code[10:12]}-{code[12:14]}"
    member = {key: value for key, value in member.items() if value not in ("", None, [], {}) or key == "page_index"}
    return member, evidence


def _is_meaningful_member(member: dict[str, Any]) -> bool:
    return bool(member.get("name") or member.get("id_number") or member.get("relationship_to_head"))


def _dedupe_members(members: list[dict[str, Any]], evidences: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    deduped_evidence: dict[str, dict[str, Any]] = {}
    for member, member_evidence in zip(members, evidences):
        key = str(member.get("id_number") or "").strip()
        if not key:
            key = f"{member.get('name') or ''}|{member.get('birth_date') or ''}"
        if not key.strip("|"):
            continue
        if key not in deduped:
            deduped[key] = dict(member)
            deduped_evidence[key] = dict(member_evidence)
            continue
        current = deduped[key]
        for field, value in member.items():
            if field == "page_index":
                continue
            if not current.get(field) and value:
                current[field] = value
                deduped_evidence[key][field] = member_evidence.get(field)
    ordered_members = list(deduped.values())
    evidence: dict[str, Any] = {}
    for index, member in enumerate(ordered_members):
        key = str(member.get("id_number") or "").strip() or f"{member.get('name') or ''}|{member.get('birth_date') or ''}"
        for field, item in (deduped_evidence.get(key) or {}).items():
            if item:
                evidence[f"members[{index}].{field}"] = item
    return ordered_members, evidence


def _dedupe_household_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        if not any(value for key, value in record.items() if key != "address_change_records"):
            continue
        key = "|".join(
            str(record.get(field) or "")
            for field in ("household_number", "household_address", "issue_date")
        )
        if key not in deduped:
            deduped[key] = record
            continue
        current = deduped[key]
        for field, value in record.items():
            if not current.get(field) and value:
                current[field] = value
    return list(deduped.values())


def _select_primary_household(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {field: ([] if field == "address_change_records" else "") for field in HOUSEHOLD_INFO_FIELDS}

    def sort_key(item: dict[str, Any]) -> tuple[str, int]:
        date_value = str(item.get("issue_date") or "")
        completeness = sum(1 for value in item.values() if value not in ("", None, [], {}))
        return date_value, completeness

    return dict(sorted(records, key=sort_key, reverse=True)[0])


def _confidence(fields: dict[str, Any]) -> dict[str, Any]:
    household_info = fields.get("household_info") if isinstance(fields.get("household_info"), dict) else {}
    members = fields.get("members") if isinstance(fields.get("members"), list) else []
    present = sum(1 for key in ("household_head", "household_address", "household_type", "issue_date") if household_info.get(key))
    present += min(len(members), 4)
    overall = min(0.95, 0.25 + present * 0.1)
    return {"overall": round(overall, 2), "fields": {}}


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    data = normalize_input(payload)
    raw_text = str(data.get("text") or "")
    if not raw_text and data.get("pages"):
        raw_text = "\n".join(text for _, text in _page_texts(data))

    household_records: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    member_evidences: list[dict[str, Any]] = []
    evidence: dict[str, Any] = {}

    for page, page_text in _page_texts(data):
        info, info_evidence = _extract_household_info_from_page(page_text, page)
        if any(value for key, value in info.items() if key != "address_change_records") or info.get("address_change_records"):
            household_records.append(info)
            evidence.update(info_evidence)
        for segment in _split_member_segments(page_text):
            member, member_evidence = _extract_member(segment, page)
            if _is_meaningful_member(member):
                members.append(member)
                member_evidences.append(member_evidence)

    members, member_evidence = _dedupe_members(members, member_evidences)
    evidence.update(member_evidence)
    household_records = _dedupe_household_records(household_records)
    household_info = _select_primary_household(household_records)

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
