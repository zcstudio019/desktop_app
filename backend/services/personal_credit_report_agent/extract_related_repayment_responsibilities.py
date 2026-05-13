from __future__ import annotations

import re
import logging
from typing import Any

from .evidence import clean_amount, clean_value
from .schema import RELATED_REPAYMENT_RESPONSIBILITY_FIELDS, ensure_record_fields

logger = logging.getLogger(__name__)

STOP_SECTION_KEYWORDS = (
    "查询记录",
    "查询记录明细",
    "公共记录",
    "公共信息",
    "本人声明",
    "异议标注",
    "机构查询记录",
    "本人查询记录",
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
    source = _normalize_text(str(text or sections.get("full_text") or ""))
    start = source.find("相关还款责任信息")
    if start < 0:
        start = source.find("相关还款责任")
    if start < 0:
        marker = source.find("承担相关还款责任")
        if marker < 0:
            return ""
        date_matches = list(re.finditer(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", source[:marker]))
        start = date_matches[-1].start() if date_matches else marker
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


def extract_related_repayment_records_from_full_text(text: str) -> list[dict[str, Any]]:
    try:
        source = _normalize_text(text)
        source = re.sub(r"第\s*\d+\s*页\s*[,，]\s*共\s*\d+\s*页", " ", source)
        source = re.sub(r"(?m)^\s*\d+\.\s*$", " ", source)
        starts = list(re.finditer(r"(?=(?:19|20)\d{2}年\d{1,2}月\d{1,2}日[，,]\s*为)", source))
        records: list[dict[str, Any]] = []
        for index, match in enumerate(starts):
            end = starts[index + 1].start() if index + 1 < len(starts) else len(source)
            block = source[match.start():end].strip()
            if (
                "办理的贷款承担相关还款责任" not in block
                or "责任人类型" not in block
                or not re.search(r"为\s*[^，,（(]+", block)
            ):
                continue
            record = _parse_record(block)
            if any(record.get(key) for key in ("related_party", "institution", "contract_no", "loan_balance")):
                records.append(record)
        return records
    except Exception:
        return []


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
    start_date = _normalize_date(_first(r"^.*?((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", block))
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
            "start_date": start_date,
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


def _record_signature(record: dict[str, Any]) -> tuple[str, ...]:
    contract_no = str(record.get("contract_no") or "").strip()
    if contract_no:
        return ("contract", contract_no)
    return (
        "fallback",
        str(record.get("start_date") or ""),
        str(record.get("related_party") or ""),
        str(record.get("institution") or ""),
        str(record.get("responsibility_amount") or ""),
        str(record.get("loan_balance") or ""),
        str(record.get("as_of_date") or ""),
    )


def extract_related_repayment_responsibilities(sections: dict[str, Any], text: str) -> list[dict[str, Any]]:
    try:
        section = _section_text(sections, text)
        logger.info("[PersonalCredit][RelatedRepayment] section_len=%s", len(section))
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, ...]] = set()
        blocks = _split_records(section)
        logger.info("[PersonalCredit][RelatedRepayment] section_candidates_count=%s", len(blocks))
        section_candidates: list[dict[str, Any]] = []
        for block in blocks:
            record = _parse_record(block)
            if not any(record.get(key) for key in ("related_party", "institution", "contract_no", "loan_balance")):
                continue
            section_candidates.append(record)

        full_text_candidates = extract_related_repayment_records_from_full_text(text or sections.get("full_text") or "")
        logger.info("[PersonalCredit][RelatedRepayment] full_text_candidates_count=%s", len(full_text_candidates))
        candidates = [*full_text_candidates, *section_candidates]

        for record in candidates:
            signature = _record_signature(record)
            if signature in seen:
                continue
            seen.add(signature)
            logger.info(
                "[PersonalCredit][RelatedRepayment] parsed start_date=%s institution=%s contract_no=%s loan_balance=%s",
                record.get("start_date"),
                record.get("institution"),
                record.get("contract_no"),
                record.get("loan_balance"),
            )
            records.append(record)
        logger.info("[PersonalCredit][RelatedRepayment] merged_count=%s", len(records))
        logger.info("[PersonalCredit][RelatedRepayment] parsed_dates=%s", [record.get("start_date") for record in records])
        logger.info("[PersonalCredit][RelatedRepayment] parsed_contracts=%s", [record.get("contract_no") for record in records])
        logger.info("[PersonalCredit][RelatedRepayment] parsed_count=%s", len(records))
        return records
    except Exception:
        return []
