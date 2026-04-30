from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from backend.document_types import get_document_display_name, get_document_storage_label

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult

logger = logging.getLogger(__name__)

DATE_TEXT_RE = re.compile(r"((?:19|20)\d{2}[年/\-.](?:0?[1-9]|1[0-2])[月/\-.](?:0?[1-9]|[12]\d|3[01])日?)")
DATE_COMPACT_RE = re.compile(r"\b((?:19|20)\d{2})(\d{2})(\d{2})\b")
MONEY_RE = re.compile(r"-?\d[\d,]*\.?\d*")
CREDIT_CODE_RE = re.compile(r"\b[0-9A-Z]{18}\b")
ZHONGZHENG_CODE_RE = re.compile(r"\b\d{6}[A-Z0-9]{10}\b")
REPORT_NO_RE = re.compile(r"\b\d{16,30}\b")
PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

ENTERPRISE_CREDIT_VALID_DAYS = 90


def _safe_print(*parts: Any) -> None:
    try:
        print(*parts)
    except UnicodeEncodeError:
        sanitized = " ".join(str(part) for part in parts).encode("gbk", errors="ignore").decode("gbk", errors="ignore")
        print(sanitized)


def _normalize_text(value: str | None) -> str:
    text = str(value or "")
    replacements = {
        "\u3000": " ",
        "\r\n": "\n",
        "\r": "\n",
        "（": "(",
        "）": ")",
        "：": ":",
        "／": "/",
        "，": ",",
        "。": ".",
        "；": ";",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_value(value: str | None) -> str:
    text = _normalize_text(value).strip()
    for marker in ("信息来源机构", "更新日期"):
        if marker in text:
            text = text.split(marker, 1)[0]
    text = text.strip(":;,.- ")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[)）]有限公司$", "", text).strip()
    text = re.sub(r"(?:--|－|—)+$", "", text).strip()
    return text


def _normalize_for_search(value: str | None) -> str:
    text = _normalize_text(value)
    text = re.sub(r"[\s:;,.\-_/()（）]+", "", text)
    return text


def _normalize_company_name(value: str | None) -> str:
    text = _clean_value(value)
    text = re.sub(r"(报告编号|报告时间|查询机构|统一社会信用代码|中征码).*$", "", text).strip()
    return text


def _customer_name_from_customer_id(customer_id: str) -> str:
    raw = str(customer_id or "").strip()
    for prefix in ("enterprise_", "personal_"):
        if raw.startswith(prefix):
            return raw[len(prefix):].strip()
    return ""


def _normalize_date(value: str | None) -> str:
    text = _clean_value(value)
    if not text:
        return ""
    match = DATE_TEXT_RE.search(text)
    if match:
        raw = match.group(1)
        normalized = raw.replace("年", "-").replace("月", "-").replace("日", "")
        normalized = normalized.replace("/", "-").replace(".", "-")
        parts = [part.zfill(2) if idx else part for idx, part in enumerate(normalized.split("-"))]
        if len(parts) == 3:
            return f"{parts[0]}-{parts[1]}-{parts[2]}"
    compact = DATE_COMPACT_RE.search(text)
    if compact:
        return f"{compact.group(1)}-{compact.group(2)}-{compact.group(3)}"
    return text


def _normalize_year(value: str | None) -> str:
    text = _clean_value(value)
    match = re.search(r"((?:19|20)\d{2})", text)
    return match.group(1) if match else text


def _normalize_numeric(value: str | None) -> str:
    text = _clean_value(value)
    if not text:
        return ""
    match = MONEY_RE.search(text.replace("万元", "").replace("元", ""))
    if not match:
        return text
    return match.group(0).replace(",", "")


def _to_float(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"未识别", "暂无", "-"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_compact_preview(text: str, limit: int = 3000) -> str:
    return str(text or "").strip()[:limit]


def _extract_count(text: str | None) -> int | None:
    cleaned = _clean_value(text)
    match = re.search(r"(-?\d+)", cleaned)
    if match:
        return int(match.group(1))
    return None


def _merge_fragment_lines(lines: list[str]) -> list[str]:
    merged: list[str] = []
    idx = 0
    while idx < len(lines):
        current = _clean_value(lines[idx])
        if not current:
            idx += 1
            continue
        combined = current
        lookahead = idx + 1
        while lookahead < len(lines):
            candidate = _clean_value(lines[lookahead])
            if not candidate:
                lookahead += 1
                continue
            if any(sep in combined for sep in (":", "：")) and len(combined) > 8:
                break
            if re.search(r"[0-9A-Z]{8,}", combined) and len(combined) > 12:
                break
            if len(combined) >= 18 and len(candidate) >= 12:
                break
            if len(candidate) > 40 and len(combined) > 12:
                break
            combined = _clean_value(f"{combined} {candidate}")
            if len(combined) >= 24 or any(sep in combined for sep in (":", "：")):
                lookahead += 1
                break
            lookahead += 1
        merged.append(combined)
        idx = max(lookahead, idx + 1)
    return merged


def _find_line_index(lines: list[str], keywords: tuple[str, ...]) -> int:
    for idx, line in enumerate(lines):
        normalized_line = _normalize_for_search(line)
        if any(keyword in line or _normalize_for_search(keyword) in normalized_line for keyword in keywords):
            return idx
    return -1


def _collect_block(lines: list[str], start_keywords: tuple[str, ...], stop_keywords: tuple[str, ...]) -> list[str]:
    start = _find_line_index(lines, start_keywords)
    if start < 0:
        return []
    block: list[str] = []
    for idx in range(start, len(lines)):
        line = lines[idx]
        if idx > start and any(keyword in line for keyword in stop_keywords):
            break
        block.append(line)
    return block


def _find_after_labels(lines: list[str], labels: tuple[str, ...], *, max_scan: int = 4, stop_labels: tuple[str, ...] = ()) -> str:
    for idx, line in enumerate(lines):
        normalized_line = _normalize_for_search(line)
        for label in labels:
            normalized_label = _normalize_for_search(label)
            if label not in line and normalized_label not in normalized_line:
                continue
            if ":" in line:
                after = line.split(":", 1)[1].strip()
                if after and not any(stop in after for stop in stop_labels):
                    return _clean_value(after)
            collected: list[str] = []
            for offset in range(1, max_scan + 1):
                if idx + offset >= len(lines):
                    break
                candidate = lines[idx + offset].strip()
                if not candidate:
                    continue
                normalized_candidate = _normalize_for_search(candidate)
                if any(stop in candidate or _normalize_for_search(stop) in normalized_candidate for stop in stop_labels):
                    break
                if any(candidate.startswith(other) or normalized_candidate.startswith(_normalize_for_search(other)) for other in labels):
                    continue
                collected.append(candidate)
                if len(collected) >= 2:
                    break
            if collected:
                return _clean_value(" ".join(collected))
    return ""


def _find_value_in_text_window(
    text: str,
    labels: tuple[str, ...],
    *,
    stop_labels: tuple[str, ...] = (),
    window: int = 120,
) -> str:
    normalized = _normalize_text(text)
    normalized_compact = _normalize_for_search(text)
    for label in labels:
        idx = normalized.find(label)
        if idx < 0:
            compact_label = _normalize_for_search(label)
            compact_idx = normalized_compact.find(compact_label)
            if compact_idx >= 0:
                # approximate mapping back to original text slice by scanning original string
                original_idx = 0
                matched_count = 0
                for char in normalized:
                    if _normalize_for_search(char):
                        if matched_count == compact_idx:
                            break
                        matched_count += 1
                    original_idx += 1
                idx = original_idx
        if idx < 0:
            continue
        snippet = normalized[idx + len(label): idx + len(label) + window]
        snippet = re.sub(r"^[:：\s]+", "", snippet)
        if stop_labels:
            stop_positions = [snippet.find(stop) for stop in stop_labels if stop in snippet]
            stop_positions = [pos for pos in stop_positions if pos >= 0]
            if stop_positions:
                snippet = snippet[: min(stop_positions)]
        lines = [part.strip() for part in snippet.split("\n") if part.strip()]
        if lines:
            return _clean_value(" ".join(lines[:2]))
        if snippet.strip():
            return _clean_value(snippet)
    return ""


def _extract_inline_or_window(text: str, keyword: str, pattern: str, window: int = 80) -> str:
    idx = text.find(keyword)
    if idx < 0:
        return ""
    snippet = text[idx: idx + window]
    match = re.search(pattern, snippet)
    if match:
        return _clean_value(match.group(1))
    return ""


def _is_valid_report_no(value: str | None) -> bool:
    text = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9]{10,40}", text):
        return False
    banned = ("担保方式", "余额", "五级分类", "逾期总额", "账户数", "信息概要", "借贷交易")
    return not any(word in text for word in banned)


def _extract_report_no(text: str, lines: list[str]) -> str:
    head = text[:2000]
    for pattern in (r"\bNO\.?\s*([A-Za-z0-9]{10,40})", r"\bNo\.?\s*([A-Za-z0-9]{10,40})"):
        match = re.search(pattern, head)
        if match and _is_valid_report_no(match.group(1)):
            return match.group(1)
    for line in lines[:80]:
        if "报告编号" not in line and "报告编码" not in line:
            continue
        match = re.search(r"(?:报告编号|报告编码)\s*[:：]?\s*([A-Za-z0-9]{10,40})", line)
        if match and _is_valid_report_no(match.group(1)):
            return match.group(1)
    return ""


def _extract_line_field_value(lines: list[str], labels: tuple[str, ...], *, max_scan: int = 2) -> str:
    field_stops = (
        "经济类型",
        "组织机构类型",
        "企业规模",
        "所属行业",
        "成立年份",
        "登记证书有效截止日期",
        "登记地址",
        "办公/经营地址",
        "办公地址",
        "经营地址",
        "存续状态",
        "注册资本",
        "信贷记录明细",
        "公共记录明细",
    )
    for idx, line in enumerate(lines):
        normalized_line = _normalize_for_search(line)
        for label in labels:
            normalized_label = _normalize_for_search(label)
            if label not in line and normalized_label not in normalized_line:
                continue
            value = line
            if label in value:
                value = value.split(label, 1)[1]
            elif normalized_label in normalized_line:
                # Fall back to the text after the visible label length when OCR inserted spaces.
                value = line
            for stop in field_stops:
                if stop not in labels and stop in value:
                    value = value.split(stop, 1)[0]
            value = _clean_value(value)
            if value and value not in labels:
                return value
            for offset in range(1, max_scan + 1):
                if idx + offset >= len(lines):
                    break
                candidate = _clean_value(lines[idx + offset])
                if candidate and not any(stop in candidate for stop in labels):
                    return candidate
    return ""


def _numbers_after_heading(lines: list[str], heading_keywords: tuple[str, ...], *, max_scan: int = 5) -> list[str]:
    for idx, line in enumerate(lines):
        heading_source = " ".join(lines[idx: idx + max_scan])
        if not all(keyword in heading_source for keyword in heading_keywords):
            continue
        source = " ".join(lines[idx: idx + max_scan])
        return re.findall(r"-?\d+(?:\.\d+)?", source)
    return []


def _first_index(lines: list[str], keywords: tuple[str, ...], start: int = 0) -> int:
    for idx in range(max(start, 0), len(lines)):
        normalized = _normalize_for_search(lines[idx])
        if any(keyword in lines[idx] or _normalize_for_search(keyword) in normalized for keyword in keywords):
            return idx
    return -1


def _split_sections(lines: list[str]) -> dict[str, list[str]]:
    anchors = {
        "identity": ("身份标识",),
        "summary": ("信息概要",),
        "basic": ("基本信息",),
        "credit_detail": ("信贷记录明细",),
        "public_records": ("公共记录明细",),
        "appendix": ("附件", "信用记录补充信息"),
    }
    positions = {name: _first_index(lines, keywords) for name, keywords in anchors.items()}
    summary_candidates = [
        idx for idx, line in enumerate(lines)
        if "信息概要" in line and (
            "首次有信贷交易" in " ".join(lines[idx: idx + 80])
            or "借贷交易" in " ".join(lines[idx: idx + 80])
        )
    ]
    if summary_candidates:
        positions["summary"] = summary_candidates[0]
    basic_candidates = [
        idx for idx, line in enumerate(lines)
        if "基本信息" in line and (
            "经济类型" in " ".join(lines[idx: idx + 120])
            or "企业规模" in " ".join(lines[idx: idx + 120])
            or "注册资本折人民币合计" in " ".join(lines[idx: idx + 120])
        )
    ]
    if basic_candidates:
        positions["basic"] = basic_candidates[0]
    report_note = _first_index(lines, ("报告说明",))
    header_end_candidates = [idx for idx in (report_note, positions["identity"]) if idx >= 0]
    header_end = min(header_end_candidates) if header_end_candidates else len(lines)

    def section_between(start_name: str, end_names: tuple[str, ...]) -> list[str]:
        start = positions.get(start_name, -1)
        if start < 0:
            return []
        end_candidates = [positions.get(name, -1) for name in end_names]
        end_candidates = [idx for idx in end_candidates if idx > start]
        end = min(end_candidates) if end_candidates else len(lines)
        return lines[start:end]

    return {
        "header": lines[:header_end],
        "identity": section_between("identity", ("summary", "basic", "credit_detail")),
        "summary": section_between("summary", ("basic", "credit_detail", "public_records", "appendix")),
        "basic": section_between("basic", ("credit_detail", "public_records", "appendix")),
        "credit_detail": section_between("credit_detail", ("public_records", "appendix")),
        "public_records": section_between("public_records", ("appendix",)),
    }


def _section_text(lines: list[str]) -> str:
    return "\n".join(lines)


def _extract_report_basic(text: str, lines: list[str], customer_id: str, customer_name: str, raw_pages: list[dict[str, Any]]) -> dict[str, Any]:
    company_name = (
        _normalize_company_name(customer_name)
        or _normalize_company_name(_customer_name_from_customer_id(customer_id))
        or _normalize_company_name(
            _find_after_labels(lines, ("企业名称", "被查询者名称", "报告主体", "本方账号户名"), max_scan=2, stop_labels=("统一社会信用代码", "中征码", "报告编号"))
        )
    )
    if not company_name:
        for line in lines[:40]:
            if "有限公司" in line or "股份有限公司" in line:
                company_name = _normalize_company_name(line)
                break
    if not company_name:
        company_name = _normalize_company_name(
            _find_value_in_text_window(text, ("企业名称", "被查询者名称", "报告主体"), stop_labels=("统一社会信用代码", "中征码", "报告编号"))
        )

    credit_code = _find_after_labels(lines, ("统一社会信用代码", "信用代码"), max_scan=2) or ""
    if not credit_code:
        credit_code = _find_value_in_text_window(text, ("统一社会信用代码", "信用代码"), stop_labels=("中征码", "报告编号"))
    if not credit_code:
        match = CREDIT_CODE_RE.search(text)
        credit_code = match.group(0) if match else ""

    zhongzheng_code = _find_after_labels(lines, ("中征码",), max_scan=2) or ""
    if not zhongzheng_code:
        zhongzheng_code = _find_value_in_text_window(text, ("中征码",), stop_labels=("报告编号", "报告时间", "查询机构"))
    if not zhongzheng_code:
        match = ZHONGZHENG_CODE_RE.search(text)
        zhongzheng_code = match.group(0) if match else ""

    report_no = _extract_report_no(text, lines)
    if not report_no:
        match = REPORT_NO_RE.search(text[:2000])
        report_no = match.group(0) if match and len(match.group(0)) >= 20 and _is_valid_report_no(match.group(0)) else ""

    report_date = _normalize_date(
        _find_after_labels(lines, ("报告时间", "报告日期", "查询时间"), max_scan=2, stop_labels=("查询机构", "中征码", "统一社会信用代码"))
    )
    if not report_date:
        report_date = _normalize_date(
            _find_value_in_text_window(text, ("报告时间", "报告日期", "查询时间"), stop_labels=("查询机构", "报告编号", "统一社会信用代码"))
        )

    query_institution = _find_after_labels(lines, ("查询机构", "查询人", "查询单位"), max_scan=2, stop_labels=("报告编号", "报告时间"))
    if not query_institution:
        query_institution = _find_value_in_text_window(text, ("查询机构", "查询人", "查询单位"), stop_labels=("报告编号", "报告时间", "统一社会信用代码"))

    page_count = None
    for page in raw_pages:
        text_value = str(page.get("text") or "")
        match = re.search(r"共\s*(\d+)\s*页", text_value)
        if match:
            page_count = int(match.group(1))
            break

    return {
        "company_name": company_name or None,
        "credit_code": credit_code or None,
        "zhongzheng_code": zhongzheng_code or None,
        "report_no": report_no or None,
        "report_date": report_date or None,
        "query_institution": query_institution or None,
        "currency_unit": "万元",
        "page_count": page_count,
    }


def _extract_identity_info(lines: list[str], text: str) -> dict[str, Any]:
    return {
        "organization_code": _find_after_labels(lines, ("组织机构代码",), max_scan=2) or _find_value_in_text_window(text, ("组织机构代码",), stop_labels=("工商登记注册号", "纳税人识别号")) or None,
        "business_registration_no": _find_after_labels(lines, ("工商登记注册号", "营业执照注册号", "注册号"), max_scan=2) or _find_value_in_text_window(text, ("工商登记注册号", "营业执照注册号", "注册号"), stop_labels=("纳税人识别号", "经济类型")) or None,
        "taxpayer_id_national": _find_after_labels(lines, ("纳税人识别号(国税)", "国税纳税人识别号", "国税识别号"), max_scan=2) or _find_value_in_text_window(text, ("纳税人识别号(国税)", "国税纳税人识别号", "国税识别号"), stop_labels=("纳税人识别号(地税)", "经济类型")) or None,
        "taxpayer_id_local": _find_after_labels(lines, ("纳税人识别号(地税)", "地税纳税人识别号", "地税识别号"), max_scan=2) or _find_value_in_text_window(text, ("纳税人识别号(地税)", "地税纳税人识别号", "地税识别号"), stop_labels=("经济类型", "组织机构类型")) or None,
    }


def _extract_registration_info(lines: list[str], text: str) -> dict[str, Any]:
    capital_match = re.search(r"注册资本折人民币合计\s*([0-9,.]+\s*万元)", text)
    registered_capital_raw = capital_match.group(1).replace(" ", "") if capital_match else (
        _extract_line_field_value(lines, ("注册资本", "注册资金"))
        or _find_value_in_text_window(text, ("注册资本", "注册资金"), stop_labels=("法定代表人", "经济类型"))
    )
    established_year = _normalize_year(
        _extract_line_field_value(lines, ("成立年份", "成立日期", "设立日期", "注册日期"))
        or _find_value_in_text_window(text, ("成立年份", "成立日期", "设立日期", "注册日期"), stop_labels=("登记证书有效截止日期", "注册资本"))
    )
    return {
        "legal_representative": _extract_line_field_value(lines, ("法定代表人", "负责人", "法定负责人")) or _find_value_in_text_window(text, ("法定代表人", "负责人", "法定负责人"), stop_labels=("经济类型", "组织机构类型")) or None,
        "economic_type": _extract_line_field_value(lines, ("经济类型",)) or _find_value_in_text_window(text, ("经济类型",), stop_labels=("组织机构类型", "企业规模")) or None,
        "organization_type": _extract_line_field_value(lines, ("组织机构类型", "组织类型")) or _find_value_in_text_window(text, ("组织机构类型", "组织类型"), stop_labels=("企业规模", "所属行业")) or None,
        "enterprise_size": _extract_line_field_value(lines, ("企业规模", "规模")) or _find_value_in_text_window(text, ("企业规模",), stop_labels=("所属行业", "成立年份")) or None,
        "industry": _extract_line_field_value(lines, ("所属行业", "行业")) or _find_value_in_text_window(text, ("所属行业", "行业"), stop_labels=("成立年份", "登记证书有效截止日期")) or None,
        "established_year": established_year or None,
        "registration_valid_until": _normalize_date(_extract_line_field_value(lines, ("登记证书有效截止日期", "登记有效期至", "营业期限至")) or _find_value_in_text_window(text, ("登记证书有效截止日期", "登记有效期至", "营业期限至"), stop_labels=("登记地址", "注册地址"))) or None,
        "registered_address": _extract_line_field_value(lines, ("登记地址", "注册地址", "住所"), max_scan=1) or _find_value_in_text_window(text, ("登记地址", "注册地址", "住所"), stop_labels=("办公地址", "经营地址", "存续状态")) or None,
        "business_address": _extract_line_field_value(lines, ("办公/经营地址", "办公地址", "经营地址", "通讯地址"), max_scan=1) or _find_value_in_text_window(text, ("办公/经营地址", "办公地址", "经营地址", "通讯地址"), stop_labels=("存续状态", "注册资本", "股东信息")) or None,
        "business_status": _extract_line_field_value(lines, ("存续状态", "经营状态", "登记状态")) or _find_value_in_text_window(text, ("存续状态", "经营状态", "登记状态"), stop_labels=("注册资本", "股东信息", "主要组成人员")) or None,
        "registered_capital_rmb": registered_capital_raw or None,
        "registered_capital": registered_capital_raw or None,
    }


def _extract_credit_summary(lines: list[str], text: str) -> dict[str, Any]:
    summary = {
        "first_credit_year": None,
        "credit_institution_count": None,
        "current_active_credit_institution_count": None,
        "active_borrowing_balance": None,
        "active_recourse_balance": None,
        "active_special_mention_balance": None,
        "active_non_performing_balance": None,
        "guarantee_balance": None,
        "guarantee_special_mention_balance": None,
        "guarantee_non_performing_balance": None,
        "non_credit_account_count": None,
        "tax_arrear_record_count": None,
        "civil_judgment_record_count": None,
        "enforcement_record_count": None,
        "administrative_penalty_record_count": None,
    }
    header_counts = _numbers_after_heading(
        lines,
        ("首次有信贷交易", "发生信贷交易", "当前有未结清信贷交易"),
        max_scan=4,
    )
    if len(header_counts) >= 3:
        summary["first_credit_year"] = header_counts[0]
        summary["credit_institution_count"] = _extract_count(header_counts[1])
        summary["current_active_credit_institution_count"] = _extract_count(header_counts[2])

    account_counts = _numbers_after_heading(
        lines,
        ("非信贷", "欠税", "民事判决", "强制执行", "行政处罚"),
        max_scan=4,
    )
    if len(account_counts) >= 5:
        summary["non_credit_account_count"] = _extract_count(account_counts[0])
        summary["tax_arrear_record_count"] = _extract_count(account_counts[1])
        summary["civil_judgment_record_count"] = _extract_count(account_counts[2])
        summary["enforcement_record_count"] = _extract_count(account_counts[3])
        summary["administrative_penalty_record_count"] = _extract_count(account_counts[4])

    info_block = _collect_block(
        lines,
        ("信息概要",),
        ("未结清信贷及授信信息概要", "基本信息", "身份标识", "股东信息"),
    )
    info_source = " ".join(info_block) if info_block else text[:6000]
    loan_idx = info_source.find("借贷交易")
    if loan_idx >= 0:
        balance_source = info_source[loan_idx: loan_idx + 500]
        balance_numbers = re.findall(r"-?\d+(?:\.\d+)?", balance_source)
        if len(balance_numbers) >= 2:
            summary["active_borrowing_balance"] = _normalize_numeric(balance_numbers[0])
            summary["guarantee_balance"] = _normalize_numeric(balance_numbers[1])
        if len(balance_numbers) >= 3:
            summary["active_recourse_balance"] = _normalize_numeric(balance_numbers[2])
        if len(balance_numbers) >= 4:
            summary["guarantee_special_mention_balance"] = _normalize_numeric(balance_numbers[3])
        if len(balance_numbers) >= 5:
            summary["active_special_mention_balance"] = _normalize_numeric(balance_numbers[4])
        if len(balance_numbers) >= 6:
            summary["guarantee_non_performing_balance"] = _normalize_numeric(balance_numbers[5])
        if len(balance_numbers) >= 7:
            summary["active_non_performing_balance"] = _normalize_numeric(balance_numbers[6])
    return summary


def _parse_summary_row(line: str) -> dict[str, Any] | None:
    normalized = _normalize_text(line)
    row_type = None
    for candidate in ("中长期借款", "短期借款", "合计"):
        if candidate in normalized:
            row_type = candidate
            normalized = normalized.split(candidate, 1)[1]
            break
    if not row_type:
        return None
    numbers = re.findall(r"\d+(?:\.\d+)?", normalized)
    if len(numbers) < 8:
        return None
    return {
        "type": row_type,
        "normal_account_count": _extract_count(numbers[0]),
        "normal_balance": _normalize_numeric(numbers[1]),
        "special_mention_account_count": _extract_count(numbers[2]),
        "special_mention_balance": _normalize_numeric(numbers[3]),
        "non_performing_account_count": _extract_count(numbers[4]),
        "non_performing_balance": _normalize_numeric(numbers[5]),
        "total_account_count": _extract_count(numbers[6]),
        "total_balance": _normalize_numeric(numbers[7]),
    }


def _parse_summary_row_from_lines(block: list[str], start_index: int) -> tuple[dict[str, Any] | None, int]:
    combined = _normalize_text(block[start_index])
    # Enterprise credit summary rows are often OCR-split across many tiny lines.
    for end_index in range(start_index + 1, min(len(block), start_index + 8)):
        row = _parse_summary_row(combined)
        if row:
            return row, end_index
        combined = _normalize_text(f"{combined} {block[end_index]}")
    return _parse_summary_row(combined), min(len(block) - 1, start_index + 7)


def _extract_active_credit_summary_by_type(lines: list[str], text: str) -> list[dict[str, Any]]:
    block = _collect_block(
        lines,
        ("未结清信贷及授信信息概要", "未结清信贷信息概要"),
        ("授信额度", "基本信息", "公共记录", "股东信息", "主要组成人员"),
    )
    if not block:
        window = _find_value_in_text_window(
            text,
            ("未结清信贷及授信信息概要", "未结清信贷信息概要"),
            stop_labels=("授信额度", "基本信息", "公共记录", "股东信息", "主要组成人员"),
            window=500,
        )
        block = [part.strip() for part in window.split("\n") if part.strip()]
    rows: list[dict[str, Any]] = []
    idx = 0
    while idx < len(block):
        line = block[idx]
        row = _parse_summary_row(line)
        if row:
            rows.append(row)
            idx += 1
            continue
        merged_row, consumed_to = _parse_summary_row_from_lines(block, idx)
        if merged_row:
            rows.append(merged_row)
            idx = consumed_to + 1
            continue
        idx += 1
    deduped: list[dict[str, Any]] = []
    seen_types: set[str] = set()
    for row in rows:
        row_type = str(row.get("type") or "")
        if row_type in seen_types:
            continue
        seen_types.add(row_type)
        deduped.append(row)
    return deduped


def _extract_credit_facility_summary(lines: list[str], text: str) -> dict[str, Any]:
    result = {
        "non_revolving": {"total_limit": None, "used_limit": None, "available_limit": None},
        "revolving": {"total_limit": None, "used_limit": None, "available_limit": None},
    }
    combined_heading_numbers = _numbers_after_heading(
        lines,
        ("非循环信用额度", "循环信用额度"),
        max_scan=5,
    )
    if len(combined_heading_numbers) >= 6:
        result["non_revolving"] = {
            "total_limit": _normalize_numeric(combined_heading_numbers[0]),
            "used_limit": _normalize_numeric(combined_heading_numbers[1]),
            "available_limit": _normalize_numeric(combined_heading_numbers[2]),
        }
        result["revolving"] = {
            "total_limit": _normalize_numeric(combined_heading_numbers[3]),
            "used_limit": _normalize_numeric(combined_heading_numbers[4]),
            "available_limit": _normalize_numeric(combined_heading_numbers[5]),
        }
        return result
    for idx, line in enumerate(lines):
        normalized = _normalize_text(line)
        if "非循环信用额度" in normalized:
            source = " ".join(lines[idx: idx + 6])
            numbers = re.findall(r"\d+(?:\.\d+)?", source)
            if len(numbers) >= 3:
                result["non_revolving"] = {
                    "total_limit": _normalize_numeric(numbers[0]),
                    "used_limit": _normalize_numeric(numbers[1]),
                    "available_limit": _normalize_numeric(numbers[2]),
                }
        if "循环信用额度" in normalized and "非循环" not in normalized:
            source = " ".join(lines[idx: idx + 6])
            numbers = re.findall(r"\d+(?:\.\d+)?", source)
            if len(numbers) >= 3:
                result["revolving"] = {
                    "total_limit": _normalize_numeric(numbers[0]),
                    "used_limit": _normalize_numeric(numbers[1]),
                    "available_limit": _normalize_numeric(numbers[2]),
                }
    if all(value is None for value in result["non_revolving"].values()):
        snippet = _find_value_in_text_window(text, ("非循环信用额度",), stop_labels=("循环信用额度", "股东信息", "基本信息"), window=120)
        numbers = re.findall(r"\d+(?:\.\d+)?", snippet)
        if len(numbers) >= 3:
            result["non_revolving"] = {
                "total_limit": _normalize_numeric(numbers[0]),
                "used_limit": _normalize_numeric(numbers[1]),
                "available_limit": _normalize_numeric(numbers[2]),
            }
    if all(value is None for value in result["revolving"].values()):
        snippet = _find_value_in_text_window(text, ("循环信用额度",), stop_labels=("股东信息", "基本信息", "主要组成人员"), window=120)
        numbers = re.findall(r"\d+(?:\.\d+)?", snippet)
        if len(numbers) >= 3:
            result["revolving"] = {
                "total_limit": _normalize_numeric(numbers[0]),
                "used_limit": _normalize_numeric(numbers[1]),
                "available_limit": _normalize_numeric(numbers[2]),
            }
    return result


def _extract_shareholders(lines: list[str], text: str) -> list[dict[str, Any]]:
    block = _collect_block(
        lines,
        ("股东信息", "股东情况"),
        ("主要组成人员", "实际控制人", "信息概要", "公共记录"),
    )
    if not block:
        window = _find_value_in_text_window(text, ("股东信息", "股东情况"), stop_labels=("主要组成人员", "实际控制人", "信息概要", "公共记录"), window=500)
        block = [part.strip() for part in window.split("\n") if part.strip()]
    if not block:
        return []
    rows: list[dict[str, Any]] = []
    joined_block = "\n".join(block)
    for match in re.finditer(
        r"(股东|自然人|企业|法人)\s+([\u4e00-\u9fa5]{2,16})\s+(身份证|统一社会信用代码|营业执照)\s+([0-9A-ZxX]{8,24})\s+(\d+(?:\.\d+)?)\s*%",
        joined_block,
    ):
        rows.append(
            {
                "type": match.group(1),
                "shareholder_type": match.group(1),
                "name": match.group(2),
                "identity_type": match.group(3),
                "id_type": match.group(3),
                "identity_no": match.group(4),
                "id_no": match.group(4),
                "contribution_ratio": match.group(5) + "%",
                "shareholding_ratio": match.group(5) + "%",
            }
        )
    record_candidates = re.split(r"(?=(?:自然人|企业|法人|股东))", joined_block)
    for candidate in record_candidates:
        normalized = _normalize_text(candidate)
        if not normalized or "股东信息" in normalized or "出资比例" in normalized:
            continue
        ratio = PERCENT_RE.search(normalized)
        if not ratio:
            continue
        names = [name for name in re.findall(r"[\u4e00-\u9fa5]{2,16}", normalized) if name not in {"股东信息", "身份证", "营业执照", "统一社会信用代码", "证件号码", "实际控制人"}]
        shareholder_name = names[1] if len(names) >= 2 and names[0] in {"自然人", "企业", "法人"} else (names[0] if names else "")
        identity_no_match = re.search(r"([0-9]{17}[0-9Xx]|[0-9A-Z]{8,24})", normalized)
        id_type = None
        if "身份证" in normalized:
            id_type = "身份证"
        elif "统一社会信用代码" in normalized:
            id_type = "统一社会信用代码"
        elif "营业执照" in normalized:
            id_type = "营业执照"
        row_type = "自然人" if "自然人" in normalized else ("企业" if "企业" in normalized else ("法人" if "法人" in normalized else None))
        rows.append(
            {
                "type": row_type,
                "shareholder_type": row_type,
                "name": shareholder_name or None,
                "identity_type": id_type,
                "id_type": id_type,
                "identity_no": identity_no_match.group(1) if identity_no_match else None,
                "id_no": identity_no_match.group(1) if identity_no_match else None,
                "contribution_ratio": ratio.group(1) + "%",
                "shareholding_ratio": ratio.group(1) + "%",
            }
        )
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in rows:
        key = f"{item.get('name')}|{item.get('identity_no')}|{item.get('contribution_ratio')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_key_personnel(lines: list[str], text: str) -> list[dict[str, Any]]:
    block = _collect_block(
        lines,
        ("主要组成人员", "主要人员"),
        ("实际控制人", "公共记录", "信息概要", "股东信息"),
    )
    if not block:
        window = _find_value_in_text_window(text, ("主要组成人员", "主要人员"), stop_labels=("实际控制人", "公共记录", "信息概要", "股东信息"), window=500)
        block = [part.strip() for part in window.split("\n") if part.strip()]
    if not block:
        return []
    roles = ("法定代表人", "负责人", "执行董事", "董事长", "董事", "监事", "经理", "总经理", "财务负责人")
    people: list[dict[str, Any]] = []
    joined_block = " ".join(block)
    legal_match = re.search(
        r"法定代表人/非法人组织负责\s*人?\s*([\u4e00-\u9fa5]{2,4})\s+身份证\s+([0-9Xx]{15,18})",
        joined_block,
    )
    if legal_match:
        people.append(
            {
                "position": "法定代表人/非法人组织负责人",
                "name": legal_match.group(1),
                "identity_type": "身份证",
                "identity_no": legal_match.group(2),
            }
        )
    for idx, line in enumerate(block):
        normalized = _normalize_text(line)
        role = next((role for role in roles if role in normalized), "")
        if not role:
            continue
        name = ""
        id_type = None
        id_no = None
        if ":" in normalized:
            after = normalized.split(":", 1)[1].strip()
            name_match = re.search(r"([\u4e00-\u9fa5]{2,8})", after)
            name = name_match.group(1) if name_match else ""
        if not name and idx + 1 < len(block):
            name_match = re.search(r"([\u4e00-\u9fa5]{2,8})", block[idx + 1])
            name = name_match.group(1) if name_match else ""
        joined = " ".join(block[idx: idx + 5])
        if "身份证" in joined:
            id_type = "身份证"
        elif "统一社会信用代码" in joined:
            id_type = "统一社会信用代码"
        id_match = re.search(r"([0-9]{17}[0-9Xx]|[0-9A-Z]{8,24})", joined)
        id_no = id_match.group(1) if id_match else None
        people.append(
            {
                "position": role,
                "name": name or None,
                "identity_type": id_type,
                "identity_no": id_no,
            }
        )
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in people:
        key = f"{item.get('position')}|{item.get('name')}|{item.get('identity_no')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_actual_controller(lines: list[str], text: str) -> dict[str, Any]:
    block = _collect_block(lines, ("实际控制人",), ("信贷记录明细", "公共记录", "信息概要", "主要组成人员"))
    if not block:
        window = _find_value_in_text_window(text, ("实际控制人",), stop_labels=("信贷记录明细", "公共记录", "信息概要", "主要组成人员"), window=200)
        block = [part.strip() for part in window.split("\n") if part.strip()]
    if not block:
        return {}
    joined = " ".join(block)
    controller_match = re.search(r"([\u4e00-\u9fa5]{2,16})\s+(身份证|统一社会信用代码|营业执照)\s+([0-9A-ZxX]{8,24})", joined)
    name = _find_after_labels(block, ("名称", "姓名", "实际控制人"), max_scan=2)
    if not name:
        match = re.search(r"实际控制人[: ]*([\u4e00-\u9fa5]{2,12})", joined)
        name = match.group(1) if match else ""
    if controller_match and (not name or "实际控制人" in name):
        name = controller_match.group(1)
    identity_type = "身份证" if "身份证" in joined else ("统一社会信用代码" if "统一社会信用代码" in joined else None)
    identity_no_match = re.search(r"([0-9]{17}[0-9Xx]|[0-9A-Z]{8,24})", joined)
    if controller_match:
        identity_type = controller_match.group(2)
        identity_no_match = re.match(r"(.+)", controller_match.group(3))
    return {
        "name": name or None,
        "identity_type": identity_type,
        "id_type": identity_type,
        "identity_no": identity_no_match.group(1) if identity_no_match else None,
        "id_no": identity_no_match.group(1) if identity_no_match else None,
    }


def _pick_actual_controller_from_shareholders(shareholders: list[dict[str, Any]]) -> dict[str, Any]:
    best_item: dict[str, Any] | None = None
    best_ratio = -1.0
    for item in shareholders:
        ratio_text = str(item.get("contribution_ratio") or "")
        match = re.search(r"(\d+(?:\.\d+)?)", ratio_text)
        ratio = float(match.group(1)) if match else -1.0
        if ratio > best_ratio:
            best_ratio = ratio
            best_item = item
    if not best_item:
        return {}
    return {
        "name": best_item.get("name"),
        "identity_type": best_item.get("identity_type"),
        "identity_no": best_item.get("identity_no"),
    }


def _backfill_personnel_identity_numbers(
    personnel: list[dict[str, Any]],
    shareholders: list[dict[str, Any]],
    actual_controller: dict[str, Any],
) -> list[dict[str, Any]]:
    shareholder_map = {str(item.get("name") or ""): item for item in shareholders if item.get("name")}
    controller_name = str(actual_controller.get("name") or "")
    for item in personnel:
        if item.get("identity_no"):
            continue
        name = str(item.get("name") or "")
        shareholder = shareholder_map.get(name)
        if shareholder:
            item["identity_type"] = item.get("identity_type") or shareholder.get("identity_type")
            item["identity_no"] = shareholder.get("identity_no")
        elif controller_name and name == controller_name:
            item["identity_type"] = item.get("identity_type") or actual_controller.get("identity_type")
            item["identity_no"] = actual_controller.get("identity_no")
    return personnel


def _extract_public_record_items(text: str, section_keywords: tuple[str, ...], field_map: dict[str, str]) -> list[dict[str, Any]]:
    block = _find_value_in_text_window(
        text,
        section_keywords,
        stop_labels=("公共记录", "查询记录", "未结清信贷", "股东信息", "主要组成人员", "实际控制人"),
        window=1200,
    )
    if not block:
        return []
    lines = [line.strip() for line in block.split("\n") if line.strip()]
    items: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    for line in lines:
        if re.match(r"^(第?\d+条|序号[:：]?\d+|\d+\.)", line) and current:
            items.append(current)
            current = {}
        matched = False
        for label, key in field_map.items():
            if label in line:
                value = line.split(":", 1)[1].strip() if ":" in line else line.replace(label, "", 1).strip()
                current[key] = _clean_value(value) or None
                matched = True
                break
        if not matched and current:
            current.setdefault("raw_text", "")
            current["raw_text"] = _clean_value(f"{current['raw_text']} {line}")
    if current:
        items.append(current)
    return items


def _extract_public_records(lines: list[str], text: str, credit_summary: dict[str, Any]) -> dict[str, Any]:
    result = {
        "licenses": [],
        "tax_arrears": _extract_public_record_items(text, ("欠税记录",), {"金额": "amount", "时间": "date", "税种": "tax_type", "机关": "authority"}),
        "civil_judgments": _extract_public_record_items(text, ("民事判决记录",), {"案号": "case_no", "日期": "date", "法院": "court", "金额": "amount"}),
        "enforcements": _extract_public_record_items(text, ("强制执行记录",), {"案号": "case_no", "日期": "date", "法院": "court", "执行标的": "amount"}),
        "administrative_penalties": _extract_public_record_items(text, ("行政处罚记录",), {"决定书": "document_no", "日期": "date", "机关": "authority", "内容": "content"}),
    }
    if not result["tax_arrears"] and (credit_summary.get("tax_arrear_record_count") or 0) > 0:
        result["tax_arrears"] = [{"count": credit_summary.get("tax_arrear_record_count")}]
    if not result["civil_judgments"] and (credit_summary.get("civil_judgment_record_count") or 0) > 0:
        result["civil_judgments"] = [{"count": credit_summary.get("civil_judgment_record_count")}]
    if not result["enforcements"] and (credit_summary.get("enforcement_record_count") or 0) > 0:
        result["enforcements"] = [{"count": credit_summary.get("enforcement_record_count")}]
    if not result["administrative_penalties"] and (credit_summary.get("administrative_penalty_record_count") or 0) > 0:
        result["administrative_penalties"] = [{"count": credit_summary.get("administrative_penalty_record_count")}]
    return result


def _extract_detail_records_from_block(
    text: str,
    section_keywords: tuple[str, ...],
    stop_keywords: tuple[str, ...],
    field_map: dict[str, str],
) -> list[dict[str, Any]]:
    block = _find_value_in_text_window(text, section_keywords, stop_labels=stop_keywords, window=2500)
    if not block:
        return []
    lines = [line.strip() for line in block.split("\n") if line.strip()]
    records: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    anchor_keywords = tuple(field_map.keys())
    for line in lines:
        if any(keyword in line for keyword in anchor_keywords) and current and len(current) >= 2 and any(k in current for k in ("institution", "business_type", "balance", "amount")):
            records.append(current)
            current = {}
        for label, key in field_map.items():
            if label in line:
                value = line.split(":", 1)[1].strip() if ":" in line else line.replace(label, "", 1).strip()
                cleaned = _clean_value(value)
                if key in {"balance", "amount", "limit_amount", "used_limit", "available_limit", "overdue_amount"}:
                    cleaned = _normalize_numeric(cleaned)
                current[key] = cleaned or None
    if current:
        records.append(current)
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in records:
        key = "|".join(str(item.get(field) or "") for field in sorted(item.keys()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _derive_risk_indicators(extracted_json: dict[str, Any]) -> dict[str, Any]:
    credit_summary = extracted_json.get("credit_summary") or {}
    active_credit_rows = extracted_json.get("active_credit_summary_by_type") or []
    credit_facility_summary = extracted_json.get("credit_facility_summary") or {}
    active_loans = extracted_json.get("active_loans") or []

    special_balance = _to_float(credit_summary.get("active_special_mention_balance")) or 0.0
    non_performing_balance = _to_float(credit_summary.get("active_non_performing_balance")) or 0.0
    borrowing_balance = _to_float(credit_summary.get("active_borrowing_balance")) or 0.0
    guarantee_balance = _to_float(credit_summary.get("guarantee_balance")) or 0.0

    short_term_total = None
    long_term_total = None
    for item in active_credit_rows:
        row_type = item.get("type")
        total_balance = _to_float(item.get("total_balance"))
        if row_type == "短期借款":
            short_term_total = total_balance
        elif row_type == "中长期借款":
            long_term_total = total_balance
        special_balance = max(special_balance, _to_float(item.get("special_mention_balance")) or 0.0)
        non_performing_balance = max(non_performing_balance, _to_float(item.get("non_performing_balance")) or 0.0)

    has_overdue = any((_to_float(item.get("overdue_amount")) or 0.0) > 0 for item in active_loans)

    revolving = credit_facility_summary.get("revolving") or {}
    non_revolving = credit_facility_summary.get("non_revolving") or {}
    total_limit = (_to_float(revolving.get("total_limit")) or 0.0) + (_to_float(non_revolving.get("total_limit")) or 0.0)
    used_limit = (_to_float(revolving.get("used_limit")) or 0.0) + (_to_float(non_revolving.get("used_limit")) or 0.0)

    current_lender_count = credit_summary.get("current_active_credit_institution_count")
    if current_lender_count is None:
        multi_lender_risk = None
    elif current_lender_count <= 2:
        multi_lender_risk = "low"
    elif current_lender_count <= 5:
        multi_lender_risk = "medium"
    else:
        multi_lender_risk = "high"

    short_term_ratio = None
    if borrowing_balance and short_term_total is not None:
        short_term_ratio = round(short_term_total / borrowing_balance, 4)

    credit_utilization_ratio = None
    if total_limit > 0:
        credit_utilization_ratio = round(used_limit / total_limit, 4)

    risk_tags: list[str] = []
    if special_balance > 0:
        risk_tags.append("存在关注类余额")
    if non_performing_balance > 0:
        risk_tags.append("存在不良类余额")
    if guarantee_balance > 0:
        risk_tags.append("存在担保余额")
    if multi_lender_risk == "high":
        risk_tags.append("多头授信风险高")
    elif multi_lender_risk == "medium":
        risk_tags.append("多头授信风险中")
    if short_term_ratio is not None and short_term_ratio >= 0.7:
        risk_tags.append("短期负债占比较高")
    if credit_utilization_ratio is not None and credit_utilization_ratio >= 0.8:
        risk_tags.append("授信使用率较高")

    summary_parts = []
    if risk_tags:
        summary_parts.append("；".join(risk_tags))
    if not summary_parts:
        summary_parts.append("当前未识别到明显高风险信号")

    return {
        "has_overdue": has_overdue,
        "has_non_performing": non_performing_balance > 0,
        "has_special_mention": special_balance > 0,
        "has_tax_arrears": bool((credit_summary.get("tax_arrear_record_count") or 0) > 0),
        "has_civil_judgment": bool((credit_summary.get("civil_judgment_record_count") or 0) > 0),
        "has_enforcement": bool((credit_summary.get("enforcement_record_count") or 0) > 0),
        "has_administrative_penalty": bool((credit_summary.get("administrative_penalty_record_count") or 0) > 0),
        "multi_lender_risk": multi_lender_risk,
        "short_term_debt_ratio": short_term_ratio,
        "credit_utilization_ratio": credit_utilization_ratio,
        "active_debt_total": credit_summary.get("active_borrowing_balance"),
        "active_short_term_debt_total": _normalize_numeric(short_term_total) if short_term_total is not None else None,
        "active_long_term_debt_total": _normalize_numeric(long_term_total) if long_term_total is not None else None,
        "risk_tags": risk_tags,
        "summary": "；".join(summary_parts),
    }


def _build_markdown_summary(extracted_json: dict[str, Any]) -> str:
    report_basic = extracted_json.get("report_basic") or {}
    registration_info = extracted_json.get("registration_info") or {}
    credit_summary = extracted_json.get("credit_summary") or {}
    risk_indicators = extracted_json.get("risk_indicators") or {}
    facility_summary = extracted_json.get("credit_facility_summary") or {}
    shareholders = extracted_json.get("shareholders") or []
    actual_controller = extracted_json.get("actual_controller") or {}
    personnel = extracted_json.get("key_personnel") or []
    active_rows = extracted_json.get("active_credit_summary_by_type") or []

    short_term = next((item for item in active_rows if item.get("type") == "短期借款"), {})
    long_term = next((item for item in active_rows if item.get("type") == "中长期借款"), {})

    shareholder_lines = []
    for item in shareholders[:6]:
        shareholder_lines.append(
            f"- {item.get('name') or '未识别'}｜{item.get('type') or '未识别'}｜{item.get('contribution_ratio') or '未识别'}"
        )
    if not shareholder_lines:
        shareholder_lines.append("- 暂未识别到股东信息")

    personnel_names = []
    for item in personnel:
        if item.get("position") in {"法定代表人", "负责人"} and item.get("name"):
            personnel_names.append(f"{item.get('position')}：{item.get('name')}")
    if actual_controller.get("name"):
        personnel_names.append(f"实际控制人：{actual_controller.get('name')}")
    if not personnel_names:
        personnel_names.append("- 暂未识别到关键人员信息")

    risk_tags = risk_indicators.get("risk_tags") or []
    risk_tags_text = "、".join(risk_tags) if risk_tags else "未识别"

    lines = [
        "## 企业征信摘要",
        "",
        "### 报告基础信息",
        f"- 企业名称：{report_basic.get('company_name') or '未识别'}",
        f"- 统一社会信用代码：{report_basic.get('credit_code') or '未识别'}",
        f"- 中征码：{report_basic.get('zhongzheng_code') or '未识别'}",
        f"- 报告编号：{report_basic.get('report_no') or '未识别'}",
        f"- 报告时间：{report_basic.get('report_date') or '未识别'}",
        f"- 查询机构：{report_basic.get('query_institution') or '未识别'}",
        "",
        "### 信贷概要",
        f"- 当前未结清借贷余额：{credit_summary.get('active_borrowing_balance') or '未识别'}",
        f"- 当前未结清信贷机构数：{credit_summary.get('current_active_credit_institution_count') if credit_summary.get('current_active_credit_institution_count') is not None else '未识别'}",
        f"- 短期借款余额：{short_term.get('total_balance') or '未识别'}",
        f"- 中长期借款余额：{long_term.get('total_balance') or '未识别'}",
        f"- 关注类余额：{credit_summary.get('active_special_mention_balance') or '未识别'}",
        f"- 不良类余额：{credit_summary.get('active_non_performing_balance') or '未识别'}",
        f"- 对外担保余额：{credit_summary.get('guarantee_balance') or '未识别'}",
        "",
        "### 授信额度",
        f"- 非循环额度：总额 {((facility_summary.get('non_revolving') or {}).get('total_limit') or '未识别')} / 已用 {((facility_summary.get('non_revolving') or {}).get('used_limit') or '未识别')} / 可用 {((facility_summary.get('non_revolving') or {}).get('available_limit') or '未识别')}",
        f"- 循环额度：总额 {((facility_summary.get('revolving') or {}).get('total_limit') or '未识别')} / 已用 {((facility_summary.get('revolving') or {}).get('used_limit') or '未识别')} / 可用 {((facility_summary.get('revolving') or {}).get('available_limit') or '未识别')}",
        "",
        "### 企业基本信息",
        f"- 企业规模：{registration_info.get('enterprise_size') or '未识别'}",
        f"- 所属行业：{registration_info.get('industry') or '未识别'}",
        f"- 成立年份：{registration_info.get('established_year') or '未识别'}",
        f"- 注册资本：{registration_info.get('registered_capital_rmb') or '未识别'}",
        f"- 经营状态：{registration_info.get('business_status') or '未识别'}",
        f"- 注册地址：{registration_info.get('registered_address') or '未识别'}",
        f"- 经营地址：{registration_info.get('business_address') or '未识别'}",
        "",
        "### 股东与人员",
        *shareholder_lines,
        *personnel_names,
        "",
        "### 风险指标",
        f"- 是否逾期：{'是' if risk_indicators.get('has_overdue') else '否'}",
        f"- 是否不良：{'是' if risk_indicators.get('has_non_performing') else '否'}",
        f"- 是否关注：{'是' if risk_indicators.get('has_special_mention') else '否'}",
        f"- 多头授信风险：{risk_indicators.get('multi_lender_risk') or '未识别'}",
        f"- 短期负债占比：{risk_indicators.get('short_term_debt_ratio') if risk_indicators.get('short_term_debt_ratio') is not None else '未识别'}",
        f"- 授信使用率：{risk_indicators.get('credit_utilization_ratio') if risk_indicators.get('credit_utilization_ratio') is not None else '未识别'}",
        f"- 风险标签：{risk_tags_text}",
        f"- 风险总结：{risk_indicators.get('summary') or '未识别'}",
    ]
    return "\n".join(lines).strip()


class EnterpriseCreditSkill(BaseExtractionSkill):
    document_type = "enterprise_credit"
    supported_extensions = {".pdf", ".png", ".jpg", ".jpeg"}

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        _safe_print("[EnterpriseCreditSkill] 被调用", input_data.document_type, input_data.file_name)
        try:
            raw_text = _normalize_text(input_data.raw_text or "")
            raw_pages = input_data.metadata.get("raw_pages") or []
            lines = [_clean_value(line) for line in raw_text.split("\n")]
            lines = [line for line in lines if line and not re.fullmatch(r"第?\s*\d+\s*页(?:/共\s*\d+\s*页)?", line)]

            sections = _split_sections(lines)
            header_lines = _merge_fragment_lines(sections.get("header") or [])
            identity_lines = _merge_fragment_lines(sections.get("identity") or [])
            summary_lines = _merge_fragment_lines(sections.get("summary") or [])
            basic_lines = _merge_fragment_lines(sections.get("basic") or [])
            credit_detail_text = _section_text(sections.get("credit_detail") or [])
            public_record_text = _section_text(sections.get("public_records") or [])

            report_basic = _extract_report_basic(
                _section_text(sections.get("header") or []) or raw_text[:2000],
                header_lines,
                input_data.customer_id,
                str(input_data.metadata.get("customer_name") or ""),
                raw_pages,
            )
            identity_info = _extract_identity_info(identity_lines, _section_text(sections.get("identity") or []))
            registration_info = _extract_registration_info(basic_lines, _section_text(sections.get("basic") or []))
            credit_summary = _extract_credit_summary(summary_lines, _section_text(sections.get("summary") or []))
            active_credit_summary_by_type = _extract_active_credit_summary_by_type(summary_lines, _section_text(sections.get("summary") or []))
            credit_facility_summary = _extract_credit_facility_summary(summary_lines, _section_text(sections.get("summary") or []))
            shareholders = _extract_shareholders(basic_lines, _section_text(sections.get("basic") or []))
            key_personnel = _extract_key_personnel(basic_lines, _section_text(sections.get("basic") or []))
            actual_controller = _extract_actual_controller(basic_lines, _section_text(sections.get("basic") or []))
            if not actual_controller.get("name"):
                actual_controller = _pick_actual_controller_from_shareholders(shareholders)
            key_personnel = _backfill_personnel_identity_numbers(key_personnel, shareholders, actual_controller)
            if actual_controller.get("name") and not actual_controller.get("identity_no"):
                for person in key_personnel:
                    if person.get("name") == actual_controller.get("name") and person.get("identity_no"):
                        actual_controller["identity_type"] = actual_controller.get("identity_type") or person.get("identity_type")
                        actual_controller["identity_no"] = person.get("identity_no")
                        break
            public_records = _extract_public_records(_merge_fragment_lines(sections.get("public_records") or []), public_record_text, credit_summary)
            active_loans = _extract_detail_records_from_block(
                credit_detail_text,
                ("未结清贷款明细", "未结清借款明细", "未结清信贷明细"),
                ("授信明细", "已结清贷款明细", "公共记录", "查询记录"),
                {
                    "机构": "institution",
                    "业务种类": "business_type",
                    "五级分类": "five_level_classification",
                    "余额": "balance",
                    "逾期金额": "overdue_amount",
                    "到期日": "maturity_date",
                    "开户日": "start_date",
                },
            )
            credit_facilities = _extract_detail_records_from_block(
                credit_detail_text,
                ("授信明细", "授信额度明细"),
                ("已结清贷款明细", "公共记录", "查询记录"),
                {
                    "机构": "institution",
                    "授信种类": "facility_type",
                    "授信总额": "limit_amount",
                    "已用额度": "used_limit",
                    "剩余可用额度": "available_limit",
                    "到期日": "maturity_date",
                },
            )
            closed_loans = _extract_detail_records_from_block(
                credit_detail_text,
                ("已结清贷款明细", "已结清借款明细"),
                ("公共记录", "查询记录", "附注"),
                {
                    "机构": "institution",
                    "业务种类": "business_type",
                    "结清日期": "settled_date",
                    "发生额": "amount",
                    "开户日": "start_date",
                },
            )

            extracted_json: dict[str, Any] = {
                "schema_version": "enterprise_credit.v2",
                "report_basic": {
                    **report_basic,
                    "page_count": report_basic.get("page_count"),
                },
                "identity_info": identity_info,
                "registration_info": registration_info,
                "shareholders": shareholders,
                "key_personnel": key_personnel,
                "actual_controller": actual_controller,
                "credit_summary": credit_summary,
                "active_credit_summary_by_type": active_credit_summary_by_type,
                "credit_facility_summary": credit_facility_summary,
                "closed_credit_summary_by_type": [],
                "loan_records": [],
                "guarantee_records": [],
                "queries": [],
                "active_loans": active_loans,
                "credit_facilities": credit_facilities,
                "closed_loans": closed_loans,
                "public_records": public_records,
                "risk_signals": [],
                "risk_indicators": {},
                "source_pages": [item.get("page") for item in raw_pages if isinstance(item, dict) and item.get("page") is not None],
                "raw_text_preview": _extract_compact_preview(raw_text),
            }
            extracted_json["risk_indicators"] = _derive_risk_indicators(extracted_json)
            extracted_json["risk_signals"] = [
                {
                    "type": tag,
                    "level": "high" if "不良" in tag else ("medium" if "关注" in tag or "较高" in tag else "low"),
                    "text": tag,
                }
                for tag in (extracted_json.get("risk_indicators") or {}).get("risk_tags", [])
            ]
            markdown_summary = _build_markdown_summary(extracted_json)

            warnings: list[str] = []
            if not report_basic.get("company_name"):
                warnings.append("未稳定识别企业名称，建议人工复核报告首页。")

            return ExtractionResult(
                document_type=self.document_type,
                schema_version="enterprise_credit.v2",
                extracted_json=extracted_json,
                markdown_summary=markdown_summary,
                confidence=0.86,
                warnings=warnings,
                errors=[],
                skill_name="enterprise_credit",
                skill_version="v2",
            )
        except Exception as exc:
            logger.exception("enterprise_credit_skill_extract_failed file=%s error=%s", input_data.file_name, exc)
            _safe_print("[EnterpriseCreditSkill] 提取失败", str(exc))
            return ExtractionResult(
                document_type="enterprise_credit",
                schema_version="enterprise_credit.v2",
                extracted_json={},
                markdown_summary="",
                confidence=0,
                warnings=[],
                errors=[str(exc)],
                skill_name="enterprise_credit",
                skill_version="v2",
            )


def build_enterprise_credit_content(
    *,
    text: str,
    customer_id: str = "",
    customer_name: str = "",
    file_name: str = "",
    file_path: str = "",
    document_id: str = "",
    raw_pages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    skill = EnterpriseCreditSkill()
    result = skill.extract(
        ExtractionInput(
            customer_id=customer_id,
            document_id=document_id,
            document_type=skill.document_type,
            file_name=file_name,
            file_path=file_path,
            raw_text=text,
            metadata={
                "customer_name": customer_name,
                "raw_pages": raw_pages or [],
            },
        )
    )

    report_basic = result.extracted_json.get("report_basic") or {}
    registration_info = result.extracted_json.get("registration_info") or {}

    return {
        "document_type_code": "enterprise_credit",
        "document_type_name": get_document_display_name("enterprise_credit"),
        "storage_label": get_document_storage_label("enterprise_credit"),
        "skill_name": result.skill_name,
        "skill_version": result.skill_version,
        "schema_version": result.schema_version,
        "extraction_status": "success" if not result.errors else "failed",
        "extraction_error": "；".join(result.errors) if result.errors else "",
        "confidence": result.confidence,
        "warnings": result.warnings,
        "errors": result.errors,
        "markdown_summary": result.markdown_summary,
        "extracted_json": result.extracted_json,
        "company_name": report_basic.get("company_name") or customer_name,
        "customer_name": report_basic.get("company_name") or customer_name,
        "credit_code": report_basic.get("credit_code") or "",
        "report_no": report_basic.get("report_no") or "",
        "report_date": report_basic.get("report_date") or "",
        "legal_representative": registration_info.get("legal_representative") or "",
        "registered_capital": registration_info.get("registered_capital_rmb") or "",
        "business_status": registration_info.get("business_status") or "",
        "address": registration_info.get("registered_address") or registration_info.get("business_address") or "",
        "risk_indicators": result.extracted_json.get("risk_indicators") or {},
        "raw_text_preview": result.extracted_json.get("raw_text_preview") or "",
    }
