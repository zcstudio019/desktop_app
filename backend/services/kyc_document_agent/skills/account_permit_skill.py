from __future__ import annotations

import re
from datetime import date
from typing import Any

from backend.services.kyc_document_agent.evidence import raw_preview
from backend.services.kyc_document_agent.schema import build_result, normalize_input


ACCOUNT_FIELDS = (
    "company_name",
    "bank_account_name",
    "bank_account_number",
    "opening_bank",
    "account_type",
    "approval_number",
    "basic_account_number",
    "legal_representative",
    "issue_date",
    "account_status",
)

ACCOUNT_LABELS = (
    "存款人名称",
    "单位名称",
    "账户名称",
    "开户单位",
    "名称",
    "账号",
    "账户号码",
    "银行账号",
    "基本账号",
    "开户银行名称",
    "开户银行",
    "开户行",
    "账户性质",
    "账户类型",
    "基本存款账户编号",
    "基本账户编号",
    "基本户编号",
    "核准号",
    "许可证编号",
    "编号",
    "法定代表人",
    "单位负责人",
    "负责人",
    "发证日期",
    "打印日期",
    "开户日期",
    "日期",
    "账户状态",
)


def normalize_ocr_text(text: str) -> tuple[str, str]:
    normalized = str(text or "").replace("\u3000", " ").replace("：", ":")
    replacements = (
        (r"基本\s*存款\s*账户\s*编号", "基本存款账户编号"),
        (r"基本\s*账户\s*开户\s*许可证", "基本账户开户许可证"),
        (r"基本\s*存款\s*账户\s*信息", "基本存款账户信息"),
        (r"开户\s*银行\s*名称", "开户银行名称"),
        (r"开户\s*银行", "开户银行"),
        (r"账户\s*号码", "账户号码"),
        (r"银行\s*账号", "银行账号"),
        (r"法定\s*代表人", "法定代表人"),
        (r"单位\s*负责人", "单位负责人"),
        (r"打印\s*日期", "打印日期"),
        (r"发证\s*日期", "发证日期"),
    )
    for pattern, replacement in replacements:
        normalized = re.sub(pattern, replacement, normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    line_text = "\n".join(line.strip(" :：,，;；") for line in re.split(r"[\r\n]+", normalized) if line.strip(" :：,，;；"))
    compact_text = re.sub(r"\s+", "", line_text)
    return line_text, compact_text


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def _clean_line_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip(" :：,，;；")


def _date_to_iso(value: Any) -> str:
    text = str(value or "").strip()
    compact = re.sub(r"\s+", "", text)
    patterns = (
        r"(\d{4})年(\d{1,2})月(\d{1,2})日?",
        r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})",
        r"(\d{4})/(\d{1,2})/(\d{1,2})",
        r"(\d{4})(\d{2})(\d{2})",
    )
    for pattern in patterns:
        match = re.search(pattern, compact)
        if not match:
            continue
        try:
            year, month, day = (int(part) for part in match.groups())
            date(year, month, day)
        except ValueError:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"
    return ""


def _strip_account_labels(value: str) -> str:
    text = str(value or "")
    for label in sorted(ACCOUNT_LABELS, key=len, reverse=True):
        text = re.sub(re.escape(label), " ", text)
    return re.sub(r"\s+", " ", text).strip(" :：,，;；")


def _extract_after_label(
    text: str,
    labels: tuple[str, ...],
    stop_labels: tuple[str, ...] = ACCOUNT_LABELS,
    max_chars: int = 120,
) -> tuple[str, str]:
    label_pattern = "|".join(re.escape(label) for label in sorted(labels, key=len, reverse=True))
    stop_pattern = "|".join(re.escape(label) for label in sorted([item for item in stop_labels if item not in labels], key=len, reverse=True))
    for line in text.splitlines():
        match = re.search(label_pattern, line)
        if not match:
            continue
        value_start = match.end()
        value_end = min(len(line), value_start + max_chars)
        stop_match = re.search(stop_pattern, line[value_start:]) if stop_pattern else None
        if stop_match:
            value_end = min(value_end, value_start + stop_match.start())
        raw_value = line[value_start:value_end]
        cleaned = _strip_account_labels(raw_value)
        if cleaned:
            return cleaned, line

    compact = re.sub(r"\s+", "", text)
    for label in sorted(labels, key=len, reverse=True):
        start = compact.find(label)
        if start < 0:
            continue
        value_start = start + len(label)
        value_end = min(len(compact), value_start + max_chars)
        for stop in sorted([item for item in stop_labels if item not in labels], key=len, reverse=True):
            stop_index = compact.find(stop, value_start)
            if stop_index >= 0:
                value_end = min(value_end, stop_index)
        raw_value = compact[value_start:value_end]
        cleaned = _strip_account_labels(raw_value)
        if cleaned:
            return cleaned, compact[start:value_end]
    return "", ""


def _extract_name(line_text: str) -> tuple[str, str]:
    return _extract_after_label(
        line_text,
        ("存款人名称", "单位名称", "账户名称", "开户单位", "名称"),
        ("账号", "账户号码", "银行账号", "基本账号", "开户银行", "开户行", "账户性质", "账户类型", "基本存款账户编号", "核准号", "法定代表人", "单位负责人", "负责人", "日期"),
        max_chars=100,
    )


def _extract_bank_account_number(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(
        line_text,
        ("账户号码", "银行账号", "基本账号", "账号"),
        ("开户银行", "开户行", "开户银行名称", "基本存款账户编号", "基本账户编号", "基本户编号", "核准号", "法定代表人", "单位负责人", "负责人", "日期"),
        max_chars=60,
    )
    compact = re.sub(r"\s+", "", value)
    match = re.search(r"\d{6,40}", compact)
    return (match.group(0), evidence) if match else ("", "")


def _extract_opening_bank(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(
        line_text,
        ("开户银行名称", "开户银行", "开户行"),
        ("账号", "账户号码", "银行账号", "基本存款账户编号", "核准号", "法定代表人", "单位负责人", "负责人", "日期", "账户状态"),
        max_chars=120,
    )
    return _clean_line_text(value), evidence


def _extract_legal_representative(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(
        line_text,
        ("法定代表人", "单位负责人", "负责人"),
        ("基本存款账户编号", "核准号", "开户银行", "账号", "日期", "账户状态"),
        max_chars=60,
    )
    value = re.sub(r"[（(][^）)]*负责人[^）)]*[）)]", "", value)
    value = re.sub(r"单位负责人|法定代表人|负责人", "", value)
    match = re.search(r"[\u4e00-\u9fff·]{2,20}", value)
    return (match.group(0), evidence) if match else ("", "")


def _extract_basic_account_number(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(
        line_text,
        ("基本存款账户编号", "基本账户编号", "基本户编号"),
        ("核准号", "法定代表人", "单位负责人", "开户银行", "账号", "日期", "账户状态"),
        max_chars=50,
    )
    compact = re.sub(r"\s+", "", value).upper()
    match = re.search(r"[A-Z][A-Z0-9]{8,24}", compact)
    return (match.group(0), evidence) if match else ("", "")


def _extract_approval_number(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(
        line_text,
        ("核准号", "许可证编号"),
        ("基本存款账户编号", "开户银行", "账号", "法定代表人", "单位负责人", "日期", "账户状态"),
        max_chars=50,
    )
    compact = re.sub(r"\s+", "", value).upper()
    match = re.search(r"[A-Z0-9-]{4,40}", compact)
    return (match.group(0), evidence) if match else ("", "")


def _extract_issue_date(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(line_text, ("发证日期", "打印日期", "开户日期", "日期"), max_chars=40)
    date_value = _date_to_iso(value)
    if date_value:
        return date_value, evidence
    for pattern in (
        r"\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日",
        r"\d{4}[./-]\d{1,2}[./-]\d{1,2}",
        r"\d{8}",
    ):
        match = re.search(pattern, line_text)
        if match:
            date_value = _date_to_iso(match.group(0))
            if date_value:
                return date_value, match.group(0)
    return "", ""


def _extract_account_status(line_text: str) -> tuple[str, str]:
    value, evidence = _extract_after_label(line_text, ("账户状态",), max_chars=30)
    for status in ("正常", "已撤销", "已变更", "久悬"):
        if status in value or status in line_text:
            return status, evidence or status
    return "", ""


def _detect_account_type(line_text: str) -> tuple[str, str]:
    if "基本存款账户" in line_text or "基本账户开户许可证" in line_text:
        return "基本存款账户", "基本存款账户"
    value, evidence = _extract_after_label(line_text, ("账户性质", "账户类型"), max_chars=40)
    if "基本" in value:
        return "基本存款账户", evidence
    return _clean_line_text(value), evidence


def _build_maps(value_map: dict[str, tuple[Any, str, float]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, float]]:
    fields: dict[str, Any] = {}
    evidence: dict[str, Any] = {}
    confidences: dict[str, float] = {}
    for field, (value, evidence_text, confidence) in value_map.items():
        if value in ("", None, [], {}):
            continue
        fields[field] = value
        confidences[field] = confidence
        evidence[field] = {
            "value": value,
            "evidence_text": evidence_text or str(value),
            "page": None,
            "confidence": confidence,
        }
    return fields, evidence, confidences


def _extract_account(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    line_text, _compact_text = normalize_ocr_text(data["text"])

    company_name, company_evidence = _extract_name(line_text)
    account_name, account_name_evidence = _extract_after_label(
        line_text,
        ("账户名称", "存款人名称", "单位名称", "开户单位"),
        ("账号", "账户号码", "银行账号", "开户银行", "基本存款账户编号", "核准号", "法定代表人", "日期"),
        max_chars=100,
    )
    bank_account_number, account_number_evidence = _extract_bank_account_number(line_text)
    opening_bank, bank_evidence = _extract_opening_bank(line_text)
    account_type, type_evidence = _detect_account_type(line_text)
    approval_number, approval_evidence = _extract_approval_number(line_text)
    basic_account_number, basic_evidence = _extract_basic_account_number(line_text)
    legal, legal_evidence = _extract_legal_representative(line_text)
    issue_date, issue_evidence = _extract_issue_date(line_text)
    account_status, status_evidence = _extract_account_status(line_text)

    fields, evidence, confidences = _build_maps(
        {
            "company_name": (company_name, company_evidence, 0.78),
            "bank_account_name": (account_name or company_name, account_name_evidence or company_evidence, 0.76),
            "bank_account_number": (bank_account_number, account_number_evidence, 0.88),
            "opening_bank": (opening_bank, bank_evidence, 0.84),
            "account_type": (account_type, type_evidence, 0.74),
            "approval_number": (approval_number, approval_evidence, 0.78),
            "basic_account_number": (basic_account_number, basic_evidence, 0.86),
            "legal_representative": (legal, legal_evidence, 0.78),
            "issue_date": (issue_date, issue_evidence, 0.76),
            "account_status": (account_status, status_evidence, 0.68),
        }
    )
    result = build_result(doc_type, fields, evidence)
    result["confidence"]["fields"] = confidences
    result["confidence"]["overall"] = round(sum(confidences.values()) / len(confidences), 4) if confidences else 0.0
    result["raw_text_preview"] = raw_preview(line_text)
    return result


def extract(payload: dict[str, Any] | str) -> dict[str, Any]:
    return _extract_account(payload, "account_permit")
