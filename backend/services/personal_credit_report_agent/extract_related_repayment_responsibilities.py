from __future__ import annotations

import re
from typing import Any

from .evidence import clean_amount, clean_value
from .schema import RELATED_REPAYMENT_RESPONSIBILITY_FIELDS, ensure_record_fields

STOP_SECTION_KEYWORDS = (
    "查询记录",
    "查询记录明细",
    "公共记录",
    "公共信息",
    "本人声明",
    "异议标注",
)


def _normalize_text(text: str) -> str:
    source = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    source = re.sub(r"[ \t\u3000]+", " ", source)
    return source.strip()


def clean_ocr_wrapped_text(text: str) -> str:
    source = _normalize_text(text)
    source = re.sub(r"第\s*\d+\s*页\s*[,，]\s*共\s*\d+\s*页", " ", source)
    source = re.sub(r"(?m)^\s*\d+\.\s*$", " ", source)
    source = re.sub(r"(?<=[\u4e00-\u9fff])\n(?=[\u4e00-\u9fff])", "", source)
    source = re.sub(r"(?<=[A-Za-z0-9])\n(?=[A-Za-z0-9])", "", source)
    source = re.sub(r"\n+", " ", source)
    source = re.sub(r"股份有\s*限公司", "股份有限公司", source)
    source = re.sub(r"有限公\s*司", "有限公司", source)
    source = re.sub(r"支\s*行", "支行", source)
    source = re.sub(r"\s+", " ", source)
    return clean_value(source)


def _normalize_date(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日", str(value or ""))
    if not match:
        return clean_value(value)
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"


def _section_text(sections: dict[str, Any], text: str) -> str:
    source = _normalize_text(str(sections.get("full_text") or text or ""))
    start = source.find("相关还款责任信息")
    if start < 0:
        start = source.find("相关还款责任")
    if start < 0:
        return ""
    tail = source[start:]
    stop_positions = [tail.find(keyword) for keyword in STOP_SECTION_KEYWORDS if tail.find(keyword) > 0]
    end = min(stop_positions) if stop_positions else len(tail)
    return tail[:end]


def _split_records(section: str) -> list[str]:
    source = _normalize_text(section)
    source = re.sub(r"第\s*\d+\s*页\s*[,，]\s*共\s*\d+\s*页", " ", source)
    source = re.sub(r"(?m)^\s*\d+\.\s*$", " ", source)
    source = re.sub(r"^相关还款责任信息\s*", "", source)
    matches = list(re.finditer(r"(?=(?:19|20)\d{2}年\d{1,2}月\d{1,2}日[，,]\s*为)", source))
    if not matches:
        return [source] if "相关还款责任" in source else []
    records: list[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(source)
        record = source[match.start():end].strip()
        if record:
            records.append(record)
    return records


def _first(pattern: str, text: str) -> str:
    match = re.search(pattern, text, flags=re.S)
    return clean_value(match.group(1)) if match else ""


def _extract_contract_no(block: str) -> str:
    match = re.search(r"(?:保证合同编号|合同编号)\s*[:：]?\s*([BDbd][A-Za-z0-9\s\r\n]{10,80})", block, flags=re.S)
    if match:
        value = re.sub(r"[^A-Za-z0-9]", "", match.group(1))
        if re.fullmatch(r"[BDbd][A-Za-z0-9]{10,}", value):
            return value
    match = re.search(r"\b([BDbd][A-Za-z0-9]{10,})\b", block)
    return match.group(1) if match else ""


def _parse_record(block: str) -> dict[str, Any]:
    raw_block = block
    block = clean_ocr_wrapped_text(block)
    related_party = _first(r"为\s*([^（(，,]+?)\s*[（(]\s*证件类型", block)
    if not related_party:
        related_party = _first(r"为\s*([^，,]+?)\s*在", block)
    institution = _first(r"在\s*(.+?)\s*办理的贷款承担相关还款责任", block)
    institution = re.split(r"(?:办理的贷款承担相关还款责任|责任人类型|证件类型|证件号码)", institution, maxsplit=1)[0]
    responsibility_type = _first(r"责任人类型为\s*([^，,。.)）]+)", block)
    responsibility_amount = _first(r"相关还款责任金额\s*([0-9][0-9,]*(?:\.\d+)?|--|——|-)", block)
    contract_no = _extract_contract_no(raw_block)
    as_of_date = _normalize_date(_first(r"截至\s*((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", block))
    loan_balance = _first(r"贷款余额\s*([0-9][0-9,]*(?:\.\d+)?|--|——|-)", block)
    return ensure_record_fields(
        {
            "related_party": clean_ocr_wrapped_text(related_party),
            "responsibility_type": responsibility_type,
            "institution": clean_ocr_wrapped_text(institution),
            "responsibility_amount": clean_amount(clean_ocr_wrapped_text(responsibility_amount)),
            "loan_balance": clean_amount(clean_ocr_wrapped_text(loan_balance)),
            "contract_no": contract_no,
            "as_of_date": as_of_date,
            "evidence": clean_ocr_wrapped_text(block)[:1200],
        },
        RELATED_REPAYMENT_RESPONSIBILITY_FIELDS,
    )


def extract_related_repayment_responsibilities(sections: dict[str, Any], text: str) -> list[dict[str, Any]]:
    try:
        section = _section_text(sections, text)
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, ...]] = set()
        for block in _split_records(section):
            record = _parse_record(block)
            if not any(record.get(key) for key in ("related_party", "institution", "contract_no", "loan_balance")):
                continue
            signature = (
                str(record.get("contract_no") or ""),
                str(record.get("related_party") or ""),
                str(record.get("institution") or ""),
                str(record.get("as_of_date") or ""),
                str(record.get("loan_balance") or ""),
            )
            if signature in seen:
                continue
            seen.add(signature)
            records.append(record)
        return records
    except Exception:
        return []
