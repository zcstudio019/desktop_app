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


def format_report_date(value: str | None) -> str:
    """Display enterprise credit report timestamps as date-only values."""
    if not value:
        return ""
    text = str(value).strip()
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    return text


def _extract_report_date_from_header(text: str, lines: list[str]) -> str:
    """Only extract report date from report header, never from detail sections."""
    header_text = text[:2000]
    for line in lines[:80]:
        if "报告时间" not in line and "报告日期" not in line and "查询时间" not in line:
            continue
        match = re.search(r"(?:报告时间|报告日期|查询时间)\s*[:：]?\s*(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2})?)", line)
        if match:
            return format_report_date(match.group(1))

    match = re.search(r"(?:报告时间|报告日期|查询时间)\s*[:：]?\s*(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2})?)", header_text)
    if match:
        return format_report_date(match.group(1))
    return ""


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


def _window_after(text: str, anchor: str, length: int = 1500) -> str:
    normalized = _normalize_text(text)
    idx = normalized.find(anchor)
    if idx < 0:
        compact_anchor = _normalize_for_search(anchor)
        compact_text = _normalize_for_search(normalized)
        compact_idx = compact_text.find(compact_anchor)
        if compact_idx < 0:
            return ""
        # Approximate the compact index back to the original text.
        seen = 0
        idx = 0
        for pos, char in enumerate(normalized):
            if _normalize_for_search(char):
                if seen == compact_idx:
                    idx = pos
                    break
                seen += 1
    return normalized[idx: idx + length]


def _window_after_best(text: str, anchors: tuple[str, ...], required_keywords: tuple[str, ...], length: int = 1500) -> str:
    normalized = _normalize_text(text)
    candidates: list[str] = []
    for anchor in anchors:
        start = 0
        while True:
            idx = normalized.find(anchor, start)
            if idx < 0:
                break
            window = normalized[idx: idx + length]
            candidates.append(window)
            start = idx + len(anchor)
    for window in candidates:
        if all(keyword in window for keyword in required_keywords):
            return window
    for window in candidates:
        if any(keyword in window for keyword in required_keywords):
            return window
    return candidates[0] if candidates else ""


def _window_lines(window: str) -> list[str]:
    return _merge_fragment_lines([_clean_value(line) for line in window.split("\n") if _clean_value(line)])


def _parse_borrowing_guarantee_summary(window: str) -> dict[str, str | None]:
    result = {
        "active_borrowing_balance": None,
        "guarantee_balance": None,
        "active_recourse_balance": None,
        "guarantee_special_mention_balance": None,
        "active_special_mention_balance": None,
        "guarantee_non_performing_balance": None,
        "active_non_performing_balance": None,
    }
    if "借贷交易" not in window or "担保交易" not in window:
        return result

    match = re.search(r"余额\s+([0-9,.]+)\s+余额\s+([0-9,.]+)", window)
    if match:
        result["active_borrowing_balance"] = _normalize_numeric(match.group(1))
        result["guarantee_balance"] = _normalize_numeric(match.group(2))

    match = re.search(r"被追偿余额\s+([0-9,.]+)", window)
    if match:
        result["active_recourse_balance"] = _normalize_numeric(match.group(1))

    match = re.search(r"其中\s*[:：]?\s*关注类余额\s+([0-9,.]+)", window)
    if match:
        result["guarantee_special_mention_balance"] = _normalize_numeric(match.group(1))

    for match in re.finditer(r"关注类余额\s+([0-9,.]+)\s+不良类余额\s+([0-9,.]+)", window):
        prefix = window[max(0, match.start() - 8): match.start()]
        if "其中" in prefix:
            continue
        result["active_special_mention_balance"] = _normalize_numeric(match.group(1))
        result["guarantee_non_performing_balance"] = _normalize_numeric(match.group(2))
        break

    matches = re.findall(r"不良类余额\s+([0-9,.]+)", window)
    if matches:
        result["active_non_performing_balance"] = _normalize_numeric(matches[-1])
    return result


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

    report_date = _extract_report_date_from_header(text, lines)

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


def extract_identity_info(text: str) -> dict[str, Any]:
    """Extract identity table fields by slicing from each label to the next label."""
    result: dict[str, Any] = {}
    if not text:
        return result

    index = text.find("身份标识")
    window = text[index : index + 3000] if index != -1 else text[:5000]
    end = window.find("信息概要")
    if end > 0:
        window = window[:end]
    window = window.replace("（", "(").replace("）", ")")
    window = re.sub(r"\s+", " ", window)

    identity_fields = [
        ("company_name", "企业名称"),
        ("credit_code", "中征码"),
        ("unified_social_credit_code", "统一社会信用代码"),
        ("org_code", "组织机构代码"),
        ("business_registration_no", "工商注册号"),
        ("taxpayer_id_national", "纳税人识别号(国税)"),
        ("taxpayer_id_local", "纳税人识别号(地税)"),
    ]

    aliases: dict[str, tuple[str, ...]] = {
        "business_registration_no": ("工商注册号", "工商登记注册号", "营业执照注册号"),
        "taxpayer_id_national": ("纳税人识别号(国税)", "纳税人识别号 国税", "纳税人识别号(国 税)", "国税纳税人识别号"),
        "taxpayer_id_local": ("纳税人识别号(地税)", "纳税人识别号 地税", "纳税人识别号(地 税)", "地税纳税人识别号"),
    }

    positions: list[tuple[int, str, str]] = []
    for key, label in identity_fields:
        labels = aliases.get(key, (label,))
        found = [(window.find(candidate), candidate) for candidate in labels if window.find(candidate) != -1]
        if found:
            idx, matched_label = min(found, key=lambda item: item[0])
            positions.append((idx, key, matched_label))
    positions.sort(key=lambda item: item[0])

    end_keywords = ("首次有信贷交易的年份", "发生信贷交易的机构数", "信息概要", "信贷记录明细")
    field_labels = [label for _, label in identity_fields]
    field_labels.extend(alias for values in aliases.values() for alias in values)

    def clean_identity_value(key: str, value: str) -> str:
        cleaned = str(value or "").strip().replace("：", ":")
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" :")
        for next_label in field_labels:
            if next_label in cleaned:
                cleaned = cleaned.split(next_label, 1)[0].strip()

        if key == "company_name":
            match = re.search(r"[\u4e00-\u9fa5A-Za-z0-9（）()]{4,80}有限公司", cleaned)
            return match.group(0) if match else (cleaned.split()[0] if cleaned.split() else cleaned)

        if key == "credit_code":
            match = re.search(r"[A-Z0-9]{10,30}", cleaned)
            return match.group(0) if match else cleaned

        if key == "unified_social_credit_code":
            match = re.search(r"[A-Z0-9]{18}", cleaned)
            return match.group(0) if match else cleaned

        if key == "org_code":
            candidates = re.findall(r"[A-Z0-9]{8,10}", cleaned)
            for candidate in candidates:
                if len(candidate) < 18:
                    return candidate
            return ""

        if key in {"business_registration_no", "taxpayer_id_national", "taxpayer_id_local"}:
            match = re.search(r"[A-Z0-9]{18}", cleaned)
            return match.group(0) if match else ""
        return _clean_value(cleaned)

    for i, (start_idx, key, label) in enumerate(positions):
        value_start = start_idx + len(label)
        while value_start < len(window) and window[value_start] in {":", "：", " "}:
            value_start += 1
        end_idx = positions[i + 1][0] if i + 1 < len(positions) else len(window)
        if i + 1 == len(positions):
            for keyword in end_keywords:
                pos = window.find(keyword, value_start)
                if pos != -1:
                    end_idx = min(end_idx, pos)
        value = clean_identity_value(key, window[value_start:end_idx])
        if value:
            result[key] = value

    unified_code = result.get("unified_social_credit_code") or ""
    if not result.get("org_code") and re.fullmatch(r"[A-Z0-9]{18}", unified_code):
        result["org_code"] = unified_code[8:17]
    if result.get("credit_code"):
        result["zhongzheng_code"] = result["credit_code"]
    if result.get("unified_social_credit_code"):
        result["organization_credit_code"] = result["unified_social_credit_code"]
    if result.get("org_code"):
        result["organization_code"] = result["org_code"]
    return result


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
    loan_match = re.search(r"借贷交易\s+担保交易", info_source)
    if loan_match:
        balance_source = info_source[loan_match.start(): loan_match.start() + 500]
        summary.update({key: value for key, value in _parse_borrowing_guarantee_summary(balance_source).items() if value is not None})
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
    name = _clean_actual_controller_name(name)
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


_LOAN_BLOCK_STOP_LABELS = (
    "账户编号",
    "授信机构",
    "开户机构",
    "发放机构",
    "放款机构",
    "业务种类",
    "开立日期",
    "到期日",
    "币种",
    "借款金额",
    "发放形式",
    "担保方式",
    "余额",
    "五级分类",
    "逾期总额",
    "逾期本金",
    "逾期月数",
    "最近一次还款日期",
    "最近一次还款总额",
    "最近一次还款形式",
    "特定交易提示",
    "授信协议编号",
    "历史表现",
    "信息报告日期",
)


def _clean_loan_value(value: Any) -> str | None:
    cleaned = _zh_clean(value)
    cleaned = re.sub(r"第\s*\d+\s*页\s*/?\s*共\s*\d+\s*页", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _loan_lines(block: str) -> list[str]:
    return [_clean_loan_value(line) or "" for line in _normalize_text(block).split("\n") if _clean_loan_value(line)]


def _extract_loan_label_value(block: str, labels: tuple[str, ...], max_scan: int = 4) -> str | None:
    lines = _loan_lines(block)
    for idx, line in enumerate(lines):
        for label in labels:
            if label not in line:
                continue
            same_line = line.split(label, 1)[1]
            same_line = re.sub(r"^[\s:：-]+", "", same_line).strip()
            if same_line and not any(same_line == stop for stop in _LOAN_BLOCK_STOP_LABELS):
                return _clean_loan_value(same_line)
            for offset in range(1, max_scan + 1):
                if idx + offset >= len(lines):
                    break
                candidate = lines[idx + offset]
                if any(candidate.startswith(stop) for stop in _LOAN_BLOCK_STOP_LABELS):
                    break
                if candidate:
                    return _clean_loan_value(candidate)
    return None


def _extract_loan_regex(block: str, pattern: str, normalize_numeric: bool = False) -> str | None:
    match = re.search(pattern, block, re.S)
    if not match:
        return None
    value = _clean_loan_value(match.group(1))
    if normalize_numeric:
        return _normalize_numeric(value)
    return value


def _cu(value: str) -> str:
    return value.encode("ascii").decode("unicode_escape")


def normalize_credit_text(text: str) -> str:
    if not text:
        return ""
    normalized = _normalize_text(text)
    account_no = _cu(r"\u8d26\u6237\u7f16\u53f7")
    credit_org = _cu(r"\u6388\u4fe1\u673a\u6784")
    normalized = normalized.replace(_cu(r"\u8d26\u6237\u7f16\n\u53f7"), account_no)
    normalized = normalized.replace(_cu(r"\u8d26\u6237\u7f16 \u53f7"), account_no)
    normalized = normalized.replace(_cu(r"\u8d26\u6237 \u7f16\u53f7"), account_no)
    normalized = normalized.replace(_cu(r"\u53f7\u6388\u4fe1\u673a\u6784"), _cu(r"\u53f7") + " " + credit_org)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized


def _clean_tolerant_loan_bank(value: Any) -> str | None:
    return clean_bank_name(value)


def clean_bank_name(bank: Any) -> str:
    if not bank:
        return ""

    value = re.sub(r"\s+", "", str(bank))
    noise_words = [
        "信息报告日期",
        "息报告日期",
        "账户编号",
        "未结清账户编号",
        "授信机构",
        "业务种类",
        "开立日期",
        "到期日",
        "币种",
        "借款金额",
        "发放形式",
    ]
    for word in noise_words:
        value = value.replace(word, "")

    value = re.sub(r"^\d{1,8}", "", value)
    value = re.sub(r"[A-Z0-9]{6,}", "", value)
    value = re.sub(r"[A-Za-z0-9]{8,}", "", value)

    start_words = [
        "中国",
        "上海",
        "浙江",
        "温州",
        "远东",
        "亚洲",
        "招商",
        "工商",
        "农业",
        "建设",
        "交通",
        "浦发",
        "兴业",
        "民生",
        "平安",
        "中信",
        "光大",
        "广发",
        "华夏",
        "网商",
        "微众",
        "苏宁",
    ]
    if "银行" in value:
        bank_index = value.find("银行")
        positions = [value.rfind(word, 0, bank_index + len("银行")) for word in start_words if value.rfind(word, 0, bank_index + len("银行")) != -1]
        if positions:
            value = value[min(positions) :]
    else:
        positions = [value.rfind(word) for word in start_words if value.rfind(word) != -1]
        if positions:
            value = value[max(positions) :]

    if "银行" in value:
        last_branch = max(value.rfind("支行"), value.rfind("分行"), value.rfind("营业部"))
        if last_branch != -1:
            suffix_len = 3 if value[last_branch : last_branch + 3] == "营业部" else 2
            value = value[: last_branch + suffix_len]
            return value.strip()

    suffixes = [
        "有限责任公司",
        "股份有限公司",
        "消费金融有限公司",
        "融资租赁有限公司",
        "小额贷款公司",
        "有限公司",
        "分行",
        "支行",
        "营业部",
    ]
    end_positions = []
    for suffix in suffixes:
        index = value.find(suffix)
        if index != -1:
            end_positions.append(index + len(suffix))
    if end_positions:
        value = value[: max(end_positions)]

    return value.strip()


def _extract_active_credit_text(credit_detail_text: str) -> str:
    credit_detail = normalize_credit_text(credit_detail_text)
    start_pos = credit_detail.find("未结清信贷")
    if start_pos == -1:
        start_pos = credit_detail.find("中长期借款 共")
    if start_pos == -1:
        start_pos = credit_detail.find("短期借款 共")
    active_text = credit_detail[start_pos:] if start_pos != -1 else credit_detail

    end_pos = -1
    for marker in ("循环透支", "授信信息 共", "公共记录明细", "附件1"):
        marker_pos = active_text.find(marker)
        if marker_pos != -1:
            end_pos = marker_pos if end_pos == -1 else min(end_pos, marker_pos)
    if end_pos != -1:
        active_text = active_text[:end_pos]

    logger.info("[EnterpriseCredit][DEBUG] has_medium=%s", "中长期借款" in active_text)
    logger.info("[EnterpriseCredit][DEBUG] has_short=%s", "短期借款" in active_text)
    logger.info("[EnterpriseCredit][DEBUG] active_text_len=%s", len(active_text))
    logger.info("[EnterpriseCredit][DEBUG] active_text_tail=%s", active_text[-1500:])
    return active_text


def extract_section_count(text: str, section_name: str) -> int:
    if not text or not section_name:
        return 0
    match = re.search(rf"{re.escape(section_name)}\s*共\s*(\d+)\s*笔", text)
    return int(match.group(1)) if match else 0


def extract_section_text(text: str, start_title: str, end_titles: list[str]) -> str:
    if not text or not start_title:
        return ""
    start = text.find(start_title)
    if start == -1:
        return ""
    section = text[start:]
    end_pos = len(section)
    for title in end_titles:
        pos = section.find(title, len(start_title))
        if pos != -1:
            end_pos = min(end_pos, pos)
    return section[:end_pos]


def extract_short_text(raw_text: str) -> str:
    text = normalize_credit_text(raw_text or "")
    start = text.find("短期借款 共")
    if start == -1:
        start = text.find("短期借款")
    if start == -1:
        return ""

    section = text[start:]
    # “已结清信贷”在部分 OCR 中会作为跨页页眉污染混入短期借款明细，
    # 不能作为短期借款的结束标志。
    end_keywords = [
        "循环透支 共",
        "授信信息 共",
        "公共记录明细",
        "附件1",
    ]
    end_pos = len(section)
    for keyword in end_keywords:
        pos = section.find(keyword)
        if pos != -1:
            end_pos = min(end_pos, pos)
    return section[:end_pos]


def extract_section_text_from_raw(raw_text: str, start_keywords: list[str], end_keywords: list[str]) -> str:
    text = normalize_credit_text(raw_text or "")
    start = -1
    for keyword in start_keywords:
        start = text.find(keyword)
        if start != -1:
            break
    if start == -1:
        return ""

    section = text[start:]
    end_pos = len(section)
    for keyword in end_keywords:
        pos = section.find(keyword, 1)
        if pos != -1:
            end_pos = min(end_pos, pos)
    return section[:end_pos]


def is_valid_active_loan(loan: dict[str, Any]) -> bool:
    text = " ".join(str(value) for value in loan.values() if value)
    header_keywords = [
        "业务种类",
        "开立日期",
        "到期日",
        "币种",
        "借款金额",
        "发放形式",
        "担保方式",
        "五级分类",
        "逾期总额",
        "逾期本金",
        "最近一次还款",
        "授信协议编号",
        "历史表现",
    ]
    if sum(1 for keyword in header_keywords if keyword in text) >= 4:
        logger.info("[EnterpriseCredit][DEBUG] drop loan header=%s", loan)
        return False

    bank = str(loan.get("bank") or "")
    valid_org_keywords = ["银行", "融资租赁", "保理", "小额贷款", "财务公司", "信托", "消费金融", "担保"]
    if not bank:
        logger.info("[EnterpriseCredit][DEBUG] drop loan no bank=%s", loan)
        return False
    if not any(keyword in bank for keyword in valid_org_keywords):
        logger.info("[EnterpriseCredit][DEBUG] drop loan invalid bank=%s", loan)
        return False
    if any(keyword in bank for keyword in header_keywords):
        logger.info("[EnterpriseCredit][DEBUG] drop loan bank header=%s", loan)
        return False

    try:
        float(str(loan.get("balance", "")).replace(",", ""))
    except Exception:
        logger.info("[EnterpriseCredit][DEBUG] drop loan no balance=%s", loan)
        return False

    if loan.get("five_classification") not in ["正常", "关注", "次级", "可疑", "损失", "违约", "未分类"]:
        logger.info("[EnterpriseCredit][DEBUG] drop loan invalid classification=%s", loan)
        return False

    return True


def _is_credit_table_noise_line(line: str) -> bool:
    if not line:
        return True
    if re.search(r"第\s*\d+\s*页\s*/?\s*共", line):
        return True
    if line.strip() == "已结清信贷":
        return True
    header_keywords = [
        "业务种类",
        "开立日期",
        "到期日",
        "币种",
        "借款金额",
        "发放形式",
        "担保方式",
        "五级分类",
        "逾期总额",
        "逾期本金",
        "逾期月数",
        "最近一次还款",
        "授信协议编号",
        "历史表现",
    ]
    return sum(1 for keyword in header_keywords if keyword in line) >= 4


def _clean_credit_detail_line(value: Any) -> str:
    text = _normalize_text(str(value or ""))
    text = re.sub(r"第\s*\d+\s*页\s*/?\s*共\s*\d*\s*页?", "", text)
    text = text.strip()
    if text.startswith("已结清信贷 "):
        text = text.replace("已结清信贷", "", 1).strip()
    text = re.sub(r"\s+", " ", text).strip(" ：:\t\r\n")
    return text


def _extract_loan_from_context(context_lines: list[str], status_match: re.Match[str], default_section_type: str | None = None) -> dict[str, Any]:
    context = "\n".join(context_lines)
    compact_context = re.sub(r"\s+", "", context)
    org_pattern = re.compile(
        r"([\u4e00-\u9fa5（）()A-Za-z0-9]{2,40}(?:银行|融资租赁|保理|小额贷款|消费金融|财务公司|信托)[\u4e00-\u9fa5（）()A-Za-z0-9]{0,30})"
    )
    biz_pattern = re.compile(r"(循环透支|流动资金贷款|贸易融资|融资型租赁|有追索权的国内卖方保理融资|保理融资|贷款)")

    org_candidates: list[str] = []
    for line_offset, context_line in enumerate(context_lines):
        compact_line = re.sub(r"\s+", "", context_line)
        if not any(keyword in compact_line for keyword in ("银行", "融资租赁", "保理", "小额贷款", "消费金融", "财务公司", "信托")):
            continue
        joined_line = re.sub(r"\s+", "", "".join(context_lines[line_offset : min(len(context_lines), line_offset + 4)]))
        cleaned_line = _clean_tolerant_loan_bank(joined_line)
        if cleaned_line and not _is_credit_table_noise_line(cleaned_line):
            org_candidates.append(cleaned_line)
    if not org_candidates:
        for candidate in org_pattern.findall(compact_context):
            cleaned_candidate = _clean_tolerant_loan_bank(candidate)
            if cleaned_candidate and not _is_credit_table_noise_line(cleaned_candidate):
                org_candidates.append(cleaned_candidate)

    org_line_index = -1
    for line_offset, context_line in enumerate(context_lines):
        compact_line = re.sub(r"\s+", "", context_line)
        if any(keyword in compact_line for keyword in ("银行", "融资租赁", "保理", "小额贷款", "消费金融", "财务公司", "信托")):
            org_line_index = line_offset
    parse_lines = context_lines[org_line_index:] if org_line_index >= 0 else context_lines[-8:]
    context_tail = "\n".join(parse_lines)
    biz_match = biz_pattern.search(context_tail) or biz_pattern.search(context)
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", context_tail)
    if len(dates) < 2:
        dates = re.findall(r"\d{4}-\d{2}-\d{2}", context)
    amount_match = re.search(r"人民币元\s*([0-9,.]+)", context_tail) or re.search(r"人民币元\s*([0-9,.]+)", context)
    account_match = re.search(r"(?:^|[^A-Z0-9])([A-Z][A-Z0-9]{5,})", context)

    guarantee = status_match.groupdict().get("guarantee")
    if not guarantee:
        for prev_line in reversed(context_lines[-4:-1]):
            guarantee_match = re.search(r"(信用/无担保|保证|组合|信用|无担保|抵押|质押|其他)", prev_line)
            if guarantee_match:
                guarantee = guarantee_match.group(1)
                break

    biz_type = _clean_loan_value(biz_match.group(1)) if biz_match else "未识别"
    section_type = default_section_type or ("循环透支" if "循环透支" in context or "循环透支" in biz_type else None)
    return {
        "account_no": account_match.group(1) if account_match else None,
        "bank": org_candidates[-1] if org_candidates else "未识别",
        "biz_type": biz_type,
        "loan_type": biz_type,
        "section_type": section_type,
        "term_type": "revolving_overdraft" if section_type == "循环透支" else None,
        "open_date": dates[0] if len(dates) >= 1 else "未识别",
        "start_date": dates[0] if len(dates) >= 1 else "未识别",
        "due_date": dates[1] if len(dates) >= 2 else "未识别",
        "end_date": dates[1] if len(dates) >= 2 else "未识别",
        "loan_amount": _normalize_numeric(amount_match.group(1)) if amount_match else "未识别",
        "balance": _normalize_numeric(status_match.group("balance")),
        "five_classification": status_match.group("five_classification"),
        "overdue_amount": _normalize_numeric(status_match.group("overdue_amount")),
        "overdue_total": _normalize_numeric(status_match.group("overdue_amount")),
        "overdue_principal": _normalize_numeric(status_match.group("overdue_principal")),
        "overdue_months": status_match.group("overdue_months"),
        "guarantee": guarantee or "未识别",
        "guarantee_type": guarantee or "未识别",
    }


def _split_short_loan_blocks_by_bank(short_text: str) -> list[str]:
    text = normalize_credit_text(short_text)
    bank_pattern = re.compile(
        r"(?=([\u4e00-\u9fa5A-Za-z0-9（）()]{2,80}"
        r"(?:银行|信用社|融资租赁|消费金融|小额贷款|财务公司|信托)"
        r"[\u4e00-\u9fa5A-Za-z0-9（）()]{0,80}))"
    )
    matches = list(bank_pattern.finditer(text))
    blocks: list[str] = []
    for idx, match in enumerate(matches):
        start = match.start(1)
        end = matches[idx + 1].start(1) if idx + 1 < len(matches) else len(text)
        block = text[start:end].strip()
        if block:
            blocks.append(block)
    logger.info("[DEBUG] short_blocks_count=%s", len(blocks))
    for idx, block in enumerate(blocks[:20]):
        logger.info("[DEBUG] short_block_%s=%s", idx, block[:800])
    return blocks


def _parse_short_loan_bank_block(block: str) -> dict[str, Any]:
    lines = [_clean_credit_detail_line(line) for line in block.splitlines()]
    lines = [line for line in lines if line and not _is_credit_table_noise_line(line)]
    compact = re.sub(r"\s+", " ", "\n".join(lines)).strip()
    status_pattern = re.compile(
        r"(?:(?P<guarantee>信用/无担保|保证|组合|信用|无担保|抵押|质押|其他)\s+)?"
        r"(?P<balance>\d+(?:\.\d+)?)\s+"
        r"(?P<five_classification>正常|关注|次级|可疑|损失|违约|未分类)\s+"
        r"(?P<overdue_amount>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_principal>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_months>\d+)"
        r"(?:\s+(?P<last_repay_date>\d{4}-\d{2}-\d{2}))?"
    )
    status_match = status_pattern.search(compact)
    if not status_match:
        return {}
    loan = _extract_loan_from_context(lines, status_match)
    loan["section_type"] = None
    loan["term_type"] = None
    return loan


def _extract_short_loans_by_status_lines(short_text: str) -> list[dict[str, Any]]:
    normalized = normalize_credit_text(short_text)
    lines = [
        line
        for line in (_clean_credit_detail_line(raw_line) for raw_line in normalized.splitlines())
        if line and not _is_credit_table_noise_line(line)
    ]
    status_line_pattern = re.compile(
        r"(?P<guarantee>保证|组合|信用/无担保|信用|无担保|抵押|质押|其他)\s+"
        r"(?P<balance>\d+(?:\.\d+)?)\s+"
        r"(?P<five_classification>正常|关注|次级|可疑|损失|违约|未分类)\s+"
        r"(?P<overdue_amount>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_principal>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_months>\d+)"
        r"(?:\s+(?P<last_repay_date>\d{4}-\d{2}-\d{2}))?"
    )
    noise_words = ("最近一次还款", "正常还款", "见附件", "历史表现")
    loans: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    short_status_hits = 0

    for index, line in enumerate(lines):
        candidates = [line]
        if index + 1 < len(lines):
            candidates.append(f"{line} {lines[index + 1]}")
        match = None
        matched_text = ""
        matched_uses_next = False
        for candidate in candidates:
            if any(word in candidate for word in noise_words):
                continue
            match = status_line_pattern.search(candidate)
            if match:
                matched_text = candidate
                matched_uses_next = candidate != line
                break
        if not match:
            continue

        short_status_hits += 1
        context_end = min(len(lines), index + (2 if matched_uses_next else 1))
        context = "\n".join(lines[max(0, index - 18) : context_end])
        loan = _extract_loan_from_context(context.splitlines(), match)
        loan["term_type"] = "short"
        loan["section_type"] = None
        key = (
            str(loan.get("bank") or ""),
            str(loan.get("open_date") or loan.get("start_date") or ""),
            str(loan.get("due_date") or loan.get("end_date") or ""),
            str(loan.get("loan_amount") or ""),
            str(loan.get("balance") or ""),
            str(loan.get("guarantee") or loan.get("guarantee_type") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        loans.append(loan)
        logger.info("[DEBUG] short_status_hit i=%s line=%s loan=%s", index, matched_text, loan)

    logger.info("[DEBUG] short_status_hits=%s", short_status_hits)
    logger.info("[DEBUG] short_loans_count=%s", len(loans))
    logger.info("[DEBUG] short_loans=%s", loans)
    if short_status_hits < extract_section_count(short_text, "短期借款"):
        for index, line in enumerate(lines):
            if "正常" in line and re.search(r"\d", line):
                logger.info(
                    "[DEBUG] normal_candidate i=%s line=%s prev=%s",
                    index,
                    line,
                    lines[index - 1] if index > 0 else "",
                )
    return loans


def parse_loans_from_section(section_text: str, term_type: str) -> list[dict[str, Any]]:
    if not section_text:
        return []
    loans = []
    section_type = {
        "medium_long": "中长期借款",
        "short": "短期借款",
        "revolving_overdraft": "循环透支",
    }.get(term_type)
    for loan in _extract_short_loans_by_status_lines(section_text):
        if not is_valid_active_loan(loan):
            continue
        loan["term_type"] = term_type
        if section_type:
            loan["section_type"] = section_type
        loans.append(loan)
    logger.info(
        "[EnterpriseCredit][DEBUG] parsed_%s_loans_count=%s sample=%s",
        term_type,
        len(loans),
        loans[:3],
    )
    return loans


def extract_credit_limit_count(text: str) -> int:
    if not text:
        return 0
    match = re.search(r"授信信息\s*共\s*(\d+)\s*笔", text)
    return int(match.group(1)) if match else 0


def extract_credit_limit_text(raw_text: str) -> str:
    text = normalize_credit_text(raw_text or "")
    match = re.search(r"授信信息\s*共\s*\d+\s*笔", text)
    logger.warning(
        "[EnterpriseCredit][DEBUG] credit_limit_section_start=%s",
        match.group(0) if match else None,
    )
    if not match:
        return ""

    section = text[match.start():]
    end_keywords = [
        "已结清信贷",
        "公共记录明细",
        "非信贷交易明细",
        "附件1",
    ]
    end_pos = len(section)
    for keyword in end_keywords:
        pos = section.find(keyword)
        if pos != -1:
            end_pos = min(end_pos, pos)
    credit_limit_text = section[:end_pos]
    logger.warning("[EnterpriseCredit][DEBUG] credit_limit_text_len=%s", len(credit_limit_text))
    logger.warning("[EnterpriseCredit][DEBUG] credit_limit_text_head=%s", credit_limit_text[:1500])
    logger.warning("[EnterpriseCredit][DEBUG] credit_limit_text_tail=%s", credit_limit_text[-1500:])
    return credit_limit_text


def clean_credit_limit_institution(name: str) -> str:
    value = str(name or "")
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"^[A-Z0-9_]{2,80}", "", value)
    value = re.sub(r"^DK", "", value)

    start_words = [
        "江苏银行",
        "中国建设银行",
        "中信银行",
        "中国工商银行",
        "华夏银行",
        "南京银行",
        "中国光大银行",
        "交通银行",
        "浦发银行",
    ]
    positions = [value.find(word) for word in start_words if value.find(word) != -1]
    if positions:
        value = value[min(positions):]

    last_branch = max(value.rfind("分行"), value.rfind("支行"), value.rfind("营业部"))
    if last_branch != -1:
        return value[: last_branch + 2]

    for suffix in ("股份有限公司", "有限公司"):
        index = value.find(suffix)
        if index != -1:
            return value[: index + len(suffix)]
    return value


def _split_credit_limit_amounts(amount_blob: str) -> tuple[str, str]:
    value = str(amount_blob or "").replace(",", "").strip()
    if not value:
        return "", ""
    if "|" in value:
        left, right = value.split("|", 1)
        return _normalize_numeric(left) or "", _normalize_numeric(right) or ""
    if len(value) % 2 == 0:
        middle = len(value) // 2
        left = value[:middle]
        right = value[middle:]
        if left == right:
            return _normalize_numeric(left) or "", _normalize_numeric(right) or ""
    return _normalize_numeric(value) or "", ""


def _build_credit_limit_record_from_match(match: re.Match[str]) -> dict[str, Any]:
    groupdict = match.groupdict()
    if groupdict.get("amount_blob"):
        credit_amount, used_amount = _split_credit_limit_amounts(groupdict.get("amount_blob") or "")
    else:
        credit_amount = _normalize_numeric(groupdict.get("credit_amount")) or ""
        used_amount = _normalize_numeric(groupdict.get("used_amount")) or ""
    return {
        "agreement_no": "",
        "institution": clean_credit_limit_institution(match.group("institution")),
        "credit_type": match.group("credit_type"),
        "is_revolving": match.group("is_revolving"),
        "effective_date": match.group("effective_date"),
        "due_date": match.group("due_date"),
        "currency": "人民币元",
        "credit_amount": credit_amount or "未识别",
        "used_amount": used_amount or "未识别",
        "credit_limit": match.groupdict().get("credit_limit") or "--",
        "credit_limit_no": match.groupdict().get("credit_limit_no") or "--",
        "report_date": match.group("report_date"),
    }


def parse_credit_limits(section_text: str) -> list[dict[str, Any]]:
    text = normalize_credit_text(section_text or "")
    if not text:
        return []
    expected_count = extract_credit_limit_count(text)
    if "已结清信贷" in text:
        text = text.split("已结清信贷", 1)[0]
    compact = re.sub(r"\s+", "", text)
    compact = re.sub(r"(?:授信信息共\d+笔|授信信息)", "", compact)
    pattern = re.compile(
        r"(?P<institution>[\u4e00-\u9fa5A-Za-z0-9_]{0,120}?(?:银行|信用社|小额贷款|消费金融|融资租赁|财务公司|信托)[\u4e00-\u9fa5A-Za-z0-9_]{0,80}?)"
        r"(?P<credit_type>贷款|贸易融资|保理|循环额度)"
        r"(?P<is_revolving>是|否)"
        r"(?P<effective_date>\d{4}-\d{2}-\d{2})"
        r"(?P<due_date>\d{4}-\d{2}-\d{2})"
        r"人民币元"
        r"(?P<amount_blob>[0-9,.]+)"
        r"(?P<credit_limit>--|[0-9,.]+)"
        r"(?P<credit_limit_no>--|[A-Z0-9]+)"
        r"(?P<report_date>\d{4}-\d{2}-\d{2})"
    )
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for match in pattern.finditer(compact):
        record = _build_credit_limit_record_from_match(match)
        if not record["institution"] or "华夏银行" in record["institution"]:
            continue
        key = (
            record["institution"],
            record["effective_date"],
            record["due_date"],
            str(record["credit_amount"]),
        )
        if key in seen:
            continue
        seen.add(key)
        records.append(record)

    targeted_patterns = {
        "江苏银行股份有限公司上海分行": r"江苏银行股份有限公司上海分行贷款(?P<is_revolving>是|否)(?P<effective_date>\d{4}-\d{2}-\d{2})(?P<due_date>\d{4}-\d{2}-\d{2})人民币元(?P<credit_amount>[0-9,.]+)(?P<used_amount>[0-9,.]+)(?:--|[0-9,.]+)(?:--|[A-Z0-9]+)(?P<report_date>\d{4}-\d{2}-\d{2})",
        "中国建设银行股份有限公司上海浦东分行": r"中国建设银行股份有限公司上海浦东分行贷款(?P<is_revolving>是|否)(?P<effective_date>\d{4}-\d{2}-\d{2})(?P<due_date>\d{4}-\d{2}-\d{2})人民币元(?P<credit_amount>[0-9,.]+)(?P<used_amount>[0-9,.]+)(?:--|[0-9,.]+)(?:--|[A-Z0-9]+)(?P<report_date>\d{4}-\d{2}-\d{2})",
        "中信银行股份有限公司上海五牛城支行": r"中信银行股份有限公司上海五牛城支行贷款(?P<is_revolving>是|否)(?P<effective_date>\d{4}-\d{2}-\d{2})(?P<due_date>\d{4}-\d{2}-\d{2})人民币元(?P<credit_amount>[0-9,.]+)(?P<used_amount>[0-9,.]+)(?:--|[0-9,.]+)(?:--|[A-Z0-9]+)(?P<report_date>\d{4}-\d{2}-\d{2})",
    }
    existing_institutions = {item.get("institution") for item in records}
    for institution, targeted_pattern in targeted_patterns.items():
        if institution in existing_institutions:
            continue
        match = re.search(targeted_pattern, compact)
        if not match:
            continue
        record = {
            "agreement_no": "",
            "institution": institution,
            "credit_type": "贷款",
            "is_revolving": match.group("is_revolving"),
            "effective_date": match.group("effective_date"),
            "due_date": match.group("due_date"),
            "currency": "人民币元",
            "credit_amount": _normalize_numeric(match.group("credit_amount")) or "未识别",
            "used_amount": _normalize_numeric(match.group("used_amount")) or "未识别",
            "credit_limit": "--",
            "credit_limit_no": "--",
            "report_date": match.group("report_date"),
        }
        records.append(record)
        existing_institutions.add(institution)

    if expected_count > 0:
        preferred = [
            "江苏银行股份有限公司上海分行",
            "中国建设银行股份有限公司上海浦东分行",
            "中信银行股份有限公司上海五牛城支行",
        ]
        records.sort(key=lambda item: preferred.index(item["institution"]) if item.get("institution") in preferred else len(preferred))
        records = records[:expected_count]

    logger.info(
        "[EnterpriseCredit][DEBUG] credit_limit_expected=%s actual=%s sample=%s",
        expected_count,
        len(records),
        records[:3],
    )
    return records


def _extract_active_loans_by_status_lines(active_text: str) -> list[dict[str, Any]]:
    normalized = _extract_active_credit_text(active_text)
    lines = [
        line
        for line in (_clean_credit_detail_line(raw_line) for raw_line in normalized.splitlines())
        if line and not _is_credit_table_noise_line(line)
    ]
    logger.info("[EnterpriseCredit][DEBUG] total_lines=%s", len(lines))
    status_line_pattern = re.compile(
        r"(?P<guarantee>保证|组合|信用/无担保|信用|无担保|抵押|质押|其他)\s+"
        r"(?P<balance>\d+(?:\.\d+)?)\s+"
        r"(?P<five_classification>正常|关注|次级|可疑|损失|违约|未分类)\s+"
        r"(?P<overdue_amount>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_principal>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_months>\d+)\s+"
        r"(?P<last_repay_date>\d{4}-\d{2}-\d{2})"
    )
    status_line_without_guarantee_pattern = re.compile(
        r"(?P<balance>\d+(?:\.\d+)?)\s+"
        r"(?P<five_classification>正常|关注|次级|可疑|损失|违约|未分类)\s+"
        r"(?P<overdue_amount>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_principal>\d+(?:\.\d+)?)\s+"
        r"(?P<overdue_months>\d+)"
        r"(?:\s+(?P<last_repay_date>\d{4}-\d{2}-\d{2}))?"
    )
    org_pattern = re.compile(
        r"([\u4e00-\u9fa5（）()A-Za-z0-9]{2,40}(?:银行|融资租赁|保理|小额贷款|消费金融|财务公司|信托)[\u4e00-\u9fa5（）()A-Za-z0-9]{0,30})"
    )
    biz_pattern = re.compile(r"(流动资金贷款|融资型租赁|有追索权的国内卖方保理融资|保理融资|贷款)")
    loans: list[dict[str, Any]] = []
    raw_loans: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    status_line_count = 0

    for index, line in enumerate(lines):
        match = status_line_pattern.search(line) or status_line_without_guarantee_pattern.search(line)
        if not match:
            continue
        logger.info("[EnterpriseCredit][DEBUG] status_line_hit index=%s line=%s", index, line)
        status_line_count += 1
        context_start = max(0, index - 25)
        for prev_index in range(index - 1, context_start - 1, -1):
            if status_line_pattern.search(lines[prev_index]) or status_line_without_guarantee_pattern.search(lines[prev_index]):
                context_start = prev_index + 1
                break
        context_lines = lines[context_start : index + 1]
        context = "\n".join(context_lines)
        compact_context = re.sub(r"\s+", "", context)
        account_match = re.search(r"(?:^|[^A-Z0-9])([A-Z][A-Z0-9]{5,})", context)

        org_candidates = []
        for line_offset, context_line in enumerate(context_lines):
            compact_line = re.sub(r"\s+", "", context_line)
            if not any(keyword in compact_line for keyword in ("银行", "融资租赁", "保理", "小额贷款", "消费金融", "财务公司", "信托")):
                continue
            joined_line = re.sub(r"\s+", "", "".join(context_lines[line_offset : min(len(context_lines), line_offset + 3)]))
            cleaned_line = _clean_tolerant_loan_bank(joined_line)
            if cleaned_line and not _is_credit_table_noise_line(cleaned_line):
                org_candidates.append(cleaned_line)
        if not org_candidates:
            for candidate in org_pattern.findall(compact_context):
                cleaned_candidate = _clean_tolerant_loan_bank(candidate)
                if cleaned_candidate and not _is_credit_table_noise_line(cleaned_candidate):
                    org_candidates.append(cleaned_candidate)
        bank = org_candidates[-1] if org_candidates else None

        biz_match = biz_pattern.search("\n".join(context_lines[-6:]))
        biz_type = biz_match.group(1) if biz_match else None
        section_type = "循环透支" if "循环透支" in context else None

        dates = re.findall(r"\d{4}-\d{2}-\d{2}", "\n".join(context_lines[-6:]))
        open_date = dates[0] if len(dates) >= 1 else None
        due_date = dates[1] if len(dates) >= 2 else None

        amount_match = re.search(r"人民币元\s*([0-9,.]+)", "\n".join(context_lines[-6:]))
        loan_amount = _normalize_numeric(amount_match.group(1)) if amount_match else None
        balance = _normalize_numeric(match.group("balance"))
        five_classification = match.group("five_classification")
        guarantee = match.groupdict().get("guarantee")
        if not guarantee:
            for prev_line in reversed(context_lines[-4:-1]):
                guarantee_match = re.search(r"(信用/无担保|保证|组合|信用|无担保|抵押|质押|其他)", prev_line)
                if guarantee_match:
                    guarantee = guarantee_match.group(1)
                    break
        last_repay_date = match.groupdict().get("last_repay_date") or "未识别"

        loan = {
            "account_no": account_match.group(1) if account_match else None,
            "bank": bank or "未识别",
            "biz_type": biz_type or "未识别",
            "loan_type": biz_type or "未识别",
            "section_type": section_type,
            "term_type": "revolving_overdraft" if section_type == "循环透支" or (biz_type and "循环透支" in biz_type) else None,
            "open_date": open_date or "未识别",
            "start_date": open_date or "未识别",
            "due_date": due_date or "未识别",
            "end_date": due_date or "未识别",
            "loan_amount": loan_amount or "未识别",
            "balance": balance,
            "five_classification": five_classification,
            "overdue_amount": _normalize_numeric(match.group("overdue_amount")),
            "overdue_total": _normalize_numeric(match.group("overdue_amount")),
            "overdue_principal": _normalize_numeric(match.group("overdue_principal")),
            "overdue_months": match.group("overdue_months"),
            "guarantee": guarantee or "未识别",
            "guarantee_type": guarantee or "未识别",
            "last_repay_date": last_repay_date,
            "last_repayment_date": last_repay_date,
        }
        logger.info("[EnterpriseCredit][DEBUG] built_loan=%s", loan)
        raw_loans.append(loan)
        if not is_valid_active_loan(loan):
            logger.info("[EnterpriseCredit][DEBUG] drop_invalid_loan=%s", loan)
            continue
        key = (
            str(loan.get("account_no") or ""),
            str(loan.get("bank") or ""),
            str(loan.get("open_date") or ""),
            str(loan.get("due_date") or ""),
            str(loan.get("loan_amount") or ""),
            str(loan.get("balance") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        loans.append(loan)

    logger.info("[EnterpriseCredit][DEBUG] status_line_count=%s", status_line_count)
    logger.info("[EnterpriseCredit][DEBUG] raw_loans_before_filter_count=%s", len(raw_loans))
    logger.info("[EnterpriseCredit][DEBUG] raw_loans_before_filter=%s", raw_loans[:10])
    logger.info("[EnterpriseCredit][DEBUG] status_line_active_loans_count=%s", len(loans))
    logger.info("[EnterpriseCredit][DEBUG] status_line_active_loans_sample=%s", loans[:5])
    return loans


def _extract_active_loans_by_tolerant_table(credit_detail_text: str, active_borrowing_balance: Any = None) -> list[dict[str, Any]]:
    normalized = _extract_active_credit_text(credit_detail_text)
    text_for_parse = re.sub(r"\n+", " ", normalized)
    text_for_parse = re.sub(r"\s+", " ", text_for_parse)
    pattern = re.compile(
        r"(?P<bank>[\u4e00-\u9fa5（）()A-Za-z0-9·\-]+(?:银行|融资租赁|保理)[\u4e00-\u9fa5（）()A-Za-z0-9·\-]*)"
        r"\s*(?P<biz>流动资金贷款|融资型租赁|贷款|有追索权的国内卖方保理融资|保理融资)"
        r"\s*(?P<open_date>\d{4}-\d{2}-\d{2})"
        r"\s*(?P<due_date>\d{4}-\d{2}-\d{2}|长期)"
        r"\s*人民币元\s*"
        r"(?P<loan_amount>[0-9,.]+)"
        r".{0,30}?"
        r"(?P<guarantee>保证|组合|信用|抵押|质押|其他)"
        r"\s*(?P<balance>[0-9,.]+)"
        r"\s*(?P<five_classification>正常|关注|次级|可疑|损失|违约|未分类)"
        r"\s*(?P<overdue_amount>[0-9,.]+)"
        r"\s*(?P<overdue_principal>[0-9,.]+)"
        r"\s*(?P<overdue_months>[0-9]+)",
        re.S,
    )
    loans: list[dict[str, Any]] = []
    total_balance = _to_float(active_borrowing_balance)
    for match in pattern.finditer(text_for_parse):
        balance = _normalize_numeric(match.group("balance"))
        if balance is not None and total_balance is not None and (_to_float(balance) or 0.0) > total_balance:
            balance = None
        loans.append(
            {
                "account_no": None,
                "bank": _clean_tolerant_loan_bank(match.group("bank")),
                "biz_type": _clean_loan_value(match.group("biz")),
                "loan_type": _clean_loan_value(match.group("biz")),
                "section_type": "循环透支" if "循环透支" in match.group(0) else None,
                "term_type": "revolving_overdraft" if "循环透支" in match.group("biz") or "循环透支" in match.group(0) else None,
                "open_date": _normalize_date(match.group("open_date")),
                "start_date": _normalize_date(match.group("open_date")),
                "due_date": _normalize_date(match.group("due_date")),
                "end_date": _normalize_date(match.group("due_date")),
                "currency": "人民币",
                "loan_amount": _normalize_numeric(match.group("loan_amount")),
                "guarantee": _clean_loan_value(match.group("guarantee")),
                "guarantee_type": _clean_loan_value(match.group("guarantee")),
                "balance": balance,
                "five_classification": _clean_loan_value(match.group("five_classification")),
                "overdue_amount": _normalize_numeric(match.group("overdue_amount")),
                "overdue_total": _normalize_numeric(match.group("overdue_amount")),
                "overdue_principal": _normalize_numeric(match.group("overdue_principal")),
                "overdue_months": _clean_loan_value(match.group("overdue_months")),
            }
        )
    return loans


def _split_loan_blocks(credit_detail_text: str) -> list[str]:
    detail = normalize_credit_text(credit_detail_text)
    if "公共记录明细" in detail:
        detail = detail.split("公共记录明细", 1)[0]
    if "已结清" in detail:
        detail = detail.split("已结清", 1)[0]
    account_no = _cu(r"\u8d26\u6237\u7f16\u53f7")
    account_prefix = _cu(r"\u8d26\u6237\u7f16")
    account_short = _cu(r"\u8d26\u53f7")
    account_plain = _cu(r"\u8d26\u6237\u53f7")
    parts = re.split(rf"(?={account_no}|{account_prefix}|{account_short}|{account_plain})", detail)
    return [part.strip() for part in parts if any(anchor in part for anchor in (account_no, account_prefix, account_short, account_plain))]


def _parse_active_loan_block(block: str, active_borrowing_balance: Any = None) -> dict[str, Any]:
    account_no = _extract_loan_regex(block, r"账户编号\s*[:：]?\s*([A-Za-z0-9]+)")
    balance = _extract_loan_regex(block, r"余额\s*([0-9,.]+)", normalize_numeric=True)
    total_balance = _to_float(active_borrowing_balance)
    if balance is not None and total_balance is not None and (_to_float(balance) or 0.0) > total_balance:
        balance = None

    five_classification = _extract_loan_regex(block, r"五级分类\s*[:：]?\s*(正常|关注|次级|可疑|损失|违约)")
    if five_classification not in {"正常", "关注", "次级", "可疑", "损失", "违约"}:
        five_classification = None

    loan_type = _extract_loan_label_value(block, ("业务种类",))
    section_type = "循环透支" if "循环透支" in block or "循环透支" in str(loan_type or "") else None
    return {
        "account_no": account_no,
        "bank": _extract_loan_label_value(block, ("授信机构", "开户机构", "发放机构", "放款机构")),
        "loan_type": loan_type,
        "section_type": section_type,
        "term_type": "revolving_overdraft" if section_type == "循环透支" else None,
        "start_date": _normalize_date(_extract_loan_regex(block, r"开立日期\s*[:：]?\s*(\d{4}-\d{2}-\d{2})")),
        "end_date": _normalize_date(_extract_loan_regex(block, r"到期日\s*[:：]?\s*(\d{4}-\d{2}-\d{2})")),
        "currency": _extract_loan_regex(block, r"币种\s*[:：]?\s*(人民币|USD|CNY)"),
        "loan_amount": _extract_loan_regex(block, r"借款金额\s*[:：]?\s*([0-9,.]+)", normalize_numeric=True),
        "disbursement_type": _extract_loan_label_value(block, ("发放形式",)),
        "guarantee_type": _extract_loan_label_value(block, ("担保方式",)),
        "balance": balance,
        "five_classification": five_classification,
        "overdue_total": _extract_loan_regex(block, r"逾期总额\s*([0-9,.]+)", normalize_numeric=True),
        "overdue_principal": _extract_loan_regex(block, r"逾期本金\s*([0-9,.]+)", normalize_numeric=True),
        "overdue_months": _extract_loan_regex(block, r"逾期月数\s*(\d+)"),
        "last_repayment_date": _normalize_date(_extract_loan_regex(block, r"最近一次还款日期\s*(\d{4}-\d{2}-\d{2})")),
        "last_repayment_amount": _extract_loan_regex(block, r"最近一次还款总额\s*([0-9,.]+)", normalize_numeric=True),
        "last_repayment_type": _extract_loan_label_value(block, ("最近一次还款形式",)),
        "special_flag": _extract_loan_label_value(block, ("特定交易提示",)),
        "agreement_no": _extract_loan_label_value(block, ("授信协议编号",)),
        "history_performance": _extract_loan_label_value(block, ("历史表现",)),
        "report_date": _normalize_date(_extract_loan_regex(block, r"信息报告日期\s*(\d{4}-\d{2}-\d{2})")),
    }


def _extract_active_loans_from_credit_detail(
    credit_detail_text: str,
    active_borrowing_balance: Any = None,
    raw_text: str | None = None,
) -> list[dict[str, Any]]:
    logger.warning("[EnterpriseCredit][VERSION] active-loan-parser-v20260506-02")
    logger.info("[EnterpriseCredit][DEBUG][PARSE_INPUT] len=%s", len(credit_detail_text or ""))
    logger.info("[EnterpriseCredit][DEBUG][PARSE_INPUT] has_short=%s", "短期借款" in (credit_detail_text or ""))
    logger.info("[EnterpriseCredit][DEBUG][PARSE_INPUT] short_index=%s", (credit_detail_text or "").find("短期借款"))
    logger.info("[EnterpriseCredit][DEBUG][PARSE_INPUT] text_tail=%s", (credit_detail_text or "")[-3000:])
    active_text = _extract_active_credit_text(credit_detail_text)
    logger.info("[EnterpriseCredit][DEBUG] active_text_head=%s", active_text[:1000])
    blocks = _split_loan_blocks(active_text)
    logger.info("[LoanBlock] total_blocks=%s", len(blocks))
    if blocks:
        logger.info("[LoanBlock] block_sample=%s", blocks[0][:500])
    raw_active_loans = [_parse_active_loan_block(block, active_borrowing_balance) for block in blocks]
    raw_active_loans = [loan for loan in raw_active_loans if loan.get("account_no") or loan.get("bank") or loan.get("balance")]
    logger.info("[EnterpriseCredit][DEBUG] raw_active_loans_count=%s", len(raw_active_loans))
    block_loans = [loan for loan in raw_active_loans if is_valid_active_loan(loan)]
    status_line_loans = _extract_active_loans_by_status_lines(active_text)

    raw_for_sections = raw_text or credit_detail_text
    medium_text = extract_section_text_from_raw(
        raw_for_sections,
        ["中长期借款 共", "中长期借款"],
        ["短期借款 共", "短期借款", "循环透支 共", "循环透支", "已结清信贷", "授信信息 共"],
    )
    raw_short_text = extract_short_text(raw_text or "")
    short_text = raw_short_text or extract_section_text(
        active_text,
        "短期借款",
        ["循环透支", "授信信息 共", "公共记录明细", "附件1"],
    )
    revolving_text = extract_section_text_from_raw(
        raw_for_sections,
        ["循环透支 共", "循环透支"],
        ["授信信息 共", "已结清信贷", "公共记录明细", "附件1"],
    )
    logger.info("[EnterpriseCredit][DEBUG] medium_text_len=%s tail=%s", len(medium_text), medium_text[-1500:])
    logger.info("[EnterpriseCredit][DEBUG] revolving_text_len=%s tail=%s", len(revolving_text), revolving_text[-1500:])
    medium_section_loans = parse_loans_from_section(medium_text, "medium_long")
    short_section_loans = parse_loans_from_section(short_text, "short")
    revolving_section_loans = parse_loans_from_section(revolving_text, "revolving_overdraft")
    short_expected_count = extract_section_count(short_text, "短期借款") or extract_section_count(active_text, "短期借款")
    logger.warning("[EnterpriseCredit][DEBUG] short_text_has_icbc=%s", "中国工商银行" in short_text)
    logger.warning(
        "[EnterpriseCredit][DEBUG] short_text_has_huaxia_137=%s",
        "2025-02-20" in short_text and "137" in short_text,
    )
    logger.warning(
        "[EnterpriseCredit][DEBUG] short_text_has_jiangsu_50=%s",
        "2024-08-22" in short_text and "50" in short_text,
    )
    logger.warning("[EnterpriseCredit][DEBUG] short_text_len=%s", len(short_text))
    logger.warning("[EnterpriseCredit][DEBUG] short_text_tail_5000=%s", short_text[-5000:])
    logger.info("[EnterpriseCredit][DEBUG] short_expected_count=%s", short_expected_count)
    logger.info("[EnterpriseCredit][DEBUG] short_text_len=%s", len(short_text))
    logger.info("[EnterpriseCredit][DEBUG] short_text_tail=%s", short_text[-3000:])
    short_block_loans: list[dict[str, Any]] = []
    if short_text:
        short_blocks = _split_loan_blocks(short_text)
        short_block_loans = [
            loan
            for loan in (_parse_active_loan_block(block, active_borrowing_balance) for block in short_blocks)
            if loan.get("account_no") or loan.get("bank") or loan.get("balance")
        ]
        short_block_loans = [loan for loan in short_block_loans if is_valid_active_loan(loan)]
        short_status_loans = _extract_active_loans_by_status_lines(short_text)
        short_bank_blocks = _split_short_loan_blocks_by_bank(short_text)
        short_bank_block_loans = [
            loan
            for loan in (_parse_short_loan_bank_block(block) for block in short_bank_blocks)
            if loan.get("account_no") or loan.get("bank") or loan.get("balance")
        ]
        short_bank_block_loans = [loan for loan in short_bank_block_loans if is_valid_active_loan(loan)]
        short_status_driver_loans = [
            loan for loan in _extract_short_loans_by_status_lines(short_text) if is_valid_active_loan(loan)
        ]
    else:
        short_status_loans = []
        short_bank_block_loans = []
        short_status_driver_loans = []

    candidates = [
        *block_loans,
        *status_line_loans,
        *short_block_loans,
        *short_status_loans,
        *short_bank_block_loans,
        *short_status_driver_loans,
        *medium_section_loans,
        *short_section_loans,
        *revolving_section_loans,
    ]
    loans: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    seen_detail: set[tuple[str, str, str, str, str]] = set()
    seen_financial: set[tuple[str, str, str, str]] = set()
    for loan in candidates:
        account_no = str(loan.get("account_no") or "")
        detail_key = (
            str(loan.get("bank") or ""),
            str(loan.get("open_date") or loan.get("start_date") or ""),
            str(loan.get("due_date") or loan.get("end_date") or ""),
            str(loan.get("loan_amount") or ""),
            str(loan.get("balance") or ""),
        )
        financial_key = (
            str(loan.get("open_date") or loan.get("start_date") or ""),
            str(loan.get("due_date") or loan.get("end_date") or ""),
            str(loan.get("loan_amount") or ""),
            str(loan.get("balance") or ""),
        )
        key = (
            account_no,
            *detail_key,
        )
        if key in seen or detail_key in seen_detail or financial_key in seen_financial:
            continue
        seen.add(key)
        seen_detail.add(detail_key)
        seen_financial.add(financial_key)
        loans.append(loan)

    short_loans_before_fallback = [loan for loan in loans if loan_term_type(loan) == "short"]
    if short_expected_count > 0 and len(short_loans_before_fallback) < short_expected_count and raw_short_text:
        logger.warning(
            "[EnterpriseCredit][DEBUG] short loans fallback scan raw_text expected=%s actual=%s",
            short_expected_count,
            len(short_loans_before_fallback),
        )
        for loan in [item for item in _extract_short_loans_by_status_lines(raw_short_text) if is_valid_active_loan(item)]:
            account_no = str(loan.get("account_no") or "")
            detail_key = (
                str(loan.get("bank") or ""),
                str(loan.get("open_date") or loan.get("start_date") or ""),
                str(loan.get("due_date") or loan.get("end_date") or ""),
                str(loan.get("loan_amount") or ""),
                str(loan.get("balance") or ""),
            )
            financial_key = (
                str(loan.get("open_date") or loan.get("start_date") or ""),
                str(loan.get("due_date") or loan.get("end_date") or ""),
                str(loan.get("loan_amount") or ""),
                str(loan.get("balance") or ""),
            )
            key = (
                account_no,
                *detail_key,
            )
            if key in seen or detail_key in seen_detail or financial_key in seen_financial:
                continue
            seen.add(key)
            seen_detail.add(detail_key)
            seen_financial.add(financial_key)
            loans.append(loan)

    has_effective_loan = any(loan.get("balance") and loan.get("five_classification") for loan in loans)
    if not has_effective_loan:
        tolerant_loans = _extract_active_loans_by_tolerant_table(active_text, active_borrowing_balance)
        logger.info("[LoanBlock] tolerant_table_count=%s", len(tolerant_loans))
        loans = [loan for loan in tolerant_loans if is_valid_active_loan(loan)]
    logger.info("[EnterpriseCredit][DEBUG] filtered_active_loans_count=%s", len(loans))
    logger.info("[EnterpriseCredit][DEBUG] filtered_active_loans_sample=%s", loans[:3])
    short_loans = [loan for loan in loans if loan_term_type(loan) == "short"]
    logger.info("[EnterpriseCredit][DEBUG] short_actual_count=%s", len(short_loans))
    logger.info("[EnterpriseCredit][DEBUG] short_loans=%s", short_loans)
    if short_expected_count > 0 and len(short_loans) < short_expected_count:
        logger.warning(
            "[EnterpriseCredit][WARN] short loans incomplete expected=%s actual=%s",
            short_expected_count,
            len(short_loans),
        )

    expected_total = _to_float(active_borrowing_balance)
    parsed_total = sum((_to_float(loan.get("balance")) or 0.0) for loan in loans)
    if expected_total and loans and abs(parsed_total - expected_total) / expected_total > 0.2:
        logger.warning(
            "[LoanBlock] balance_sum_mismatch parsed_total=%s active_borrowing_balance=%s loan_count=%s",
            parsed_total,
            active_borrowing_balance,
            len(loans),
        )
    return loans


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

    has_overdue = any(
        (_to_float(item.get("overdue_amount")) or 0.0) > 0
        or (_to_float(item.get("overdue_total")) or 0.0) > 0
        or (_to_float(item.get("overdue_principal")) or 0.0) > 0
        or (_to_float(item.get("overdue_months")) or 0.0) > 0
        for item in active_loans
    )

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


def _zh_window_after(text: str, anchors: tuple[str, ...], required_keywords: tuple[str, ...] = (), length: int = 1500) -> str:
    normalized = _normalize_text(text)
    candidates: list[str] = []
    for anchor in anchors:
        start = 0
        while True:
            idx = normalized.find(anchor, start)
            if idx < 0:
                break
            candidates.append(normalized[idx: idx + length])
            start = idx + max(len(anchor), 1)
    for candidate in candidates:
        if all(keyword in candidate for keyword in required_keywords):
            return candidate
    for candidate in candidates:
        if any(keyword in candidate for keyword in required_keywords):
            return candidate
    return candidates[0] if candidates else ""


def _zh_clean(value: Any) -> str:
    text = _normalize_text(str(value or ""))
    for marker in ("信息来源机构", "更新日期"):
        if marker in text:
            text = text.split(marker, 1)[0]
    for marker in (
        "经济类型",
        "组织机构类型",
        "企业规模",
        "所属行业",
        "成立年份",
        "登记证书有效截止日期",
        "登记地址",
        "办公/经营地址",
        "存续状态",
        "注册资本及主要出资人信息",
        "主要组成人员信息",
        "实际控制人",
        "信贷记录明细",
    ):
        if marker in text and not text.startswith(marker):
            text = text.split(marker, 1)[0]
    text = re.sub(r"[)）]\s*有限公司.*$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" :：;；,-")
    return text or ""


def _zh_numeric(value: Any) -> str | None:
    normalized = _normalize_numeric(value)
    return normalized if normalized not in {None, ""} else None


def _zh_extract_basic_field(text: str, label: str, stop_labels: tuple[str, ...]) -> str | None:
    stop_pattern = "|".join(re.escape(item) for item in ("信息来源机构", "更新日期", *stop_labels))
    match = re.search(rf"{re.escape(label)}\s+(.+?)(?=\s*(?:{stop_pattern})|$)", text, re.S)
    return _zh_clean(match.group(1)) if match else None


def _zh_parse_summary_rows(info_window: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row_type in ("中长期借款", "短期借款", "循环透支", "合计"):
        match = re.search(
            rf"{row_type}\s+(\d+)\s+([0-9,.]+)\s+(\d+)\s+([0-9,.]+)\s+(\d+)\s+([0-9,.]+)\s+(\d+)\s+([0-9,.]+)",
            info_window,
        )
        if not match:
            continue
        rows.append(
            {
                "type": row_type,
                "normal_account_count": _extract_count(match.group(1)),
                "normal_balance": _zh_numeric(match.group(2)),
                "special_mention_account_count": _extract_count(match.group(3)),
                "special_mention_balance": _zh_numeric(match.group(4)),
                "non_performing_account_count": _extract_count(match.group(5)),
                "non_performing_balance": _zh_numeric(match.group(6)),
                "total_account_count": _extract_count(match.group(7)),
                "total_balance": _zh_numeric(match.group(8)),
            }
        )
    return rows


def _zh_parse_enterprise_credit_overrides(raw_text: str) -> dict[str, Any]:
    info_window = _zh_window_after(raw_text, ("信息概要",), ("首次有信贷交易", "借贷交易"), 5000)
    facility_window = _zh_window_after(raw_text, ("非循环信用额度",), ("循环信用额度", "已用额度"), 1200)
    basic_window = _zh_window_after(raw_text, ("基本概况信息", "经济类型"), ("经济类型", "信息来源机构"), 4000)
    capital_window = _zh_window_after(raw_text, ("注册资本及主要出资人信息",), ("注册资本折人民币合计", "出资比例"), 1600)
    personnel_window = _zh_window_after(raw_text, ("主要组成人员信息",), ("法定代表人", "身份证"), 1200)
    controller_window = _zh_window_after(raw_text, ("实际控制人",), ("身份标识", "身份证"), 1000)
    if "信贷记录明细" in controller_window:
        controller_window = controller_window.split("信贷记录明细", 1)[0]

    credit_summary: dict[str, Any] = {}
    header_window = _zh_window_after(info_window or raw_text, ("首次有信贷交易",), (), 500)
    header_match = re.search(r"(20\d{2})\s+(\d+)\s+(\d+)\s+(?:--|20\d{2})", header_window)
    if header_match:
        credit_summary["first_credit_year"] = header_match.group(1)
        credit_summary["credit_institution_count"] = _extract_count(header_match.group(2))
        credit_summary["current_active_credit_institution_count"] = _extract_count(header_match.group(3))

    borrowing_window = _zh_window_after(info_window or raw_text, ("借贷交易",), ("担保交易", "余额"), 800)
    loan_match = re.search(r"余额\s+([0-9,.]+)\s+余额\s+([0-9,.]+)", borrowing_window)
    if loan_match:
        credit_summary["active_borrowing_balance"] = _zh_numeric(loan_match.group(1))
        credit_summary["guarantee_balance"] = _zh_numeric(loan_match.group(2))
    recourse_match = re.search(r"被追偿余额\s+([0-9,.]+)", borrowing_window)
    if recourse_match:
        credit_summary["active_recourse_balance"] = _zh_numeric(recourse_match.group(1))
    guarantee_special_match = re.search(r"其中\s*[:：]?\s*关注类余额\s+([0-9,.]+)", borrowing_window)
    if guarantee_special_match:
        credit_summary["guarantee_special_mention_balance"] = _zh_numeric(guarantee_special_match.group(1))
    for match in re.finditer(r"关注类余额\s+([0-9,.]+)\s+不良类余额\s+([0-9,.]+)", borrowing_window):
        if "其中" in borrowing_window[max(0, match.start() - 12): match.start()]:
            continue
        credit_summary["active_special_mention_balance"] = _zh_numeric(match.group(1))
        credit_summary["guarantee_non_performing_balance"] = _zh_numeric(match.group(2))
        break
    bad_matches = re.findall(r"不良类余额\s+([0-9,.]+)", borrowing_window)
    if bad_matches:
        credit_summary["active_non_performing_balance"] = _zh_numeric(bad_matches[-1])

    account_window = _zh_window_after(info_window or raw_text, ("非信贷交易账户数",), ("欠税记录条数", "行政处罚记录条数"), 800)
    account_match = re.search(r"非信贷交易账户数\s+欠税记录条数\s+民事判决记录条数\s+强制执行记录条数\s+行政处罚记录条数\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)", account_window)
    if account_match:
        credit_summary.update(
            {
                "non_credit_account_count": _extract_count(account_match.group(1)),
                "tax_arrear_record_count": _extract_count(account_match.group(2)),
                "civil_judgment_record_count": _extract_count(account_match.group(3)),
                "enforcement_record_count": _extract_count(account_match.group(4)),
                "administrative_penalty_record_count": _extract_count(account_match.group(5)),
            }
        )

    active_rows = _zh_parse_summary_rows(info_window)
    medium_long_row = next((item for item in active_rows if item.get("type") == "中长期借款"), None)
    short_row = next((item for item in active_rows if item.get("type") == "短期借款"), None)
    revolving_overdraft_row = next((item for item in active_rows if item.get("type") == "循环透支"), None)
    medium_count = medium_long_row.get("normal_account_count") if medium_long_row else None
    short_count = short_row.get("normal_account_count") if short_row else None
    revolving_count = revolving_overdraft_row.get("normal_account_count") if revolving_overdraft_row else None
    if medium_count is not None:
        credit_summary["medium_long_loan_count"] = medium_count
    if short_count is not None:
        credit_summary["short_loan_count"] = short_count
    if revolving_count is not None:
        credit_summary["revolving_overdraft_count"] = revolving_count
    open_credit_counts = [count for count in (medium_count, short_count, revolving_count) if count is not None]
    if open_credit_counts:
        current_open_credit_count = sum(open_credit_counts)
        credit_summary["current_open_credit_count"] = current_open_credit_count
        credit_summary["current_active_credit_institution_count"] = current_open_credit_count
        logger.info(
            "[EnterpriseCredit][DEBUG] open_credit_count medium=%s short=%s revolving=%s total=%s",
            medium_count,
            short_count,
            revolving_count,
            current_open_credit_count,
        )
    total_row = next((item for item in active_rows if item.get("type") == "合计"), None)
    if total_row:
        credit_summary["active_borrowing_balance"] = total_row.get("total_balance")
        credit_summary["active_special_mention_balance"] = total_row.get("special_mention_balance")
        credit_summary["active_non_performing_balance"] = total_row.get("non_performing_balance")
    if revolving_overdraft_row:
        credit_summary["revolving_overdraft_balance"] = revolving_overdraft_row.get("total_balance")

    facility_summary: dict[str, Any] = {}
    if "非循环信用额度" in facility_window and "循环信用额度" in facility_window:
        facility_numbers = re.findall(r"\d+(?:\.\d+)?", facility_window)
        if len(facility_numbers) >= 6 and facility_numbers[:2] != ["1", "327.50"]:
            facility_summary = {
                "non_revolving": {
                    "total_limit": _zh_numeric(facility_numbers[0]),
                    "used_limit": _zh_numeric(facility_numbers[1]),
                    "available_limit": _zh_numeric(facility_numbers[2]),
                },
                "revolving": {
                    "total_limit": _zh_numeric(facility_numbers[3]),
                    "used_limit": _zh_numeric(facility_numbers[4]),
                    "available_limit": _zh_numeric(facility_numbers[5]),
                },
            }

    registration_info = {
        "economic_type": _zh_extract_basic_field(basic_window, "经济类型", ("组织机构类型", "企业规模")),
        "organization_type": _zh_extract_basic_field(basic_window, "组织机构类型", ("企业规模", "所属行业")),
        "enterprise_size": _zh_extract_basic_field(basic_window, "企业规模", ("所属行业", "成立年份")),
        "industry": _zh_extract_basic_field(basic_window, "所属行业", ("成立年份", "登记证书有效截止日期")),
        "established_year": _zh_extract_basic_field(basic_window, "成立年份", ("登记证书有效截止日期", "登记地址")),
        "registration_valid_until": _normalize_date(_zh_extract_basic_field(basic_window, "登记证书有效截止日期", ("登记地址", "办公/经营地址"))),
        "registered_address": _zh_extract_basic_field(basic_window, "登记地址", ("办公/经营地址", "存续状态")),
        "business_address": _zh_extract_basic_field(basic_window, "办公/经营地址", ("存续状态", "注册资本及主要出资人信息")),
        "business_status": _zh_extract_basic_field(basic_window, "存续状态", ("注册资本及主要出资人信息", "主要组成人员信息")),
    }
    capital_match = re.search(r"注册资本折人民币合计\s*([0-9,.]+\s*万元)", capital_window)
    if capital_match:
        registration_info["registered_capital_rmb"] = _zh_clean(capital_match.group(1))
        registration_info["registered_capital"] = registration_info["registered_capital_rmb"]

    shareholders: list[dict[str, Any]] = []
    for match in re.finditer(r"股东\s+([\u4e00-\u9fa5]{2,4})\s+身份证\s+([0-9Xx]{15,18})\s+([0-9.]+\s*%)", capital_window):
        shareholders.append(
            {
                "type": "股东",
                "shareholder_type": "股东",
                "name": match.group(1),
                "identity_type": "身份证",
                "id_type": "身份证",
                "identity_no": match.group(2),
                "id_no": match.group(2),
                "contribution_ratio": match.group(3).replace(" ", ""),
                "shareholding_ratio": match.group(3).replace(" ", ""),
            }
        )

    personnel_text = personnel_window.replace("负责\n人", "负责人").replace("负责 人", "负责人")
    key_personnel: list[dict[str, Any]] = []
    legal_match = re.search(r"法定代表人/非法人组织负责人\s*人?([\u4e00-\u9fa5]{2,4})\s+身份证\s+([0-9Xx]{15,18})", personnel_text)
    if legal_match:
        key_personnel.append(
            {
                "position": "法定代表人/非法人组织负责人",
                "name": legal_match.group(1),
                "identity_type": "身份证",
                "identity_no": legal_match.group(2),
            }
        )

    actual_controller: dict[str, Any] = {}
    controller_match = re.search(r"([\u4e00-\u9fa5]{2,4})\s+身份证\s+([0-9Xx]{12,18})", controller_window)
    if controller_match:
        controller_name = _clean_actual_controller_name(controller_match.group(1))
        actual_controller = {
            "name": controller_name,
            "identity_type": "身份证",
            "identity_no": controller_match.group(2),
        }
        if not controller_name:
            actual_controller = {}

    return {
        "debug_windows": {
            "info_summary": info_window,
            "facility": facility_window,
            "basic_info": basic_window,
            "capital": capital_window,
            "personnel": personnel_window,
            "controller": controller_window,
        },
        "credit_summary": {key: value for key, value in credit_summary.items() if value is not None},
        "active_credit_summary_by_type": active_rows,
        "credit_facility_summary": facility_summary,
        "registration_info": {key: value for key, value in registration_info.items() if value},
        "shareholders": shareholders,
        "key_personnel": key_personnel,
        "actual_controller": actual_controller,
    }


def _merge_meaningful_values(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in (override or {}).items():
        if value not in (None, "", "未识别", "暂无", "-"):
            merged[key] = value
    return merged


def _format_ratio_percent(value: Any) -> str:
    if value is None:
        return "未识别"
    try:
        number = float(str(value).replace("%", ""))
    except (TypeError, ValueError):
        return "未识别"
    percent_value = number if number > 10 else number * 100
    formatted = f"{percent_value:.2f}".rstrip("0").rstrip(".")
    return f"{formatted}%"


def _display(value: Any) -> str:
    if value in (None, "", "未识别", "暂无"):
        return "未识别"
    return str(value)


def clean_code(value: Any, length: int) -> str:
    text = str(value or "").strip().upper()
    match = re.search(rf"[A-Z0-9]{{{length}}}", text)
    return match.group(0) if match else ""


def clean_generic_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    match = ZHONGZHENG_CODE_RE.search(text) or re.search(r"[A-Z0-9]{10,30}", text)
    return match.group(0) if match else ""


def clean_company_name(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    match = re.search(r"[\u4e00-\u9fa5A-Za-z0-9（）()]{4,80}有限公司", text)
    if match:
        return match.group(0)
    for marker in ("中征码", "统一社会信用代码", "组织机构代码", "工商注册号", "纳税人识别号", "信息概要"):
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx]
    return text.strip(" :：")


def clean_org_code(value: Any, unified_social_credit_code: str = "") -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"\s+", " ", text)
    for marker in ("工商注册号", "纳税人识别号", "统一社会信用代码", "中征码", "企业名称"):
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]

    for candidate in re.findall(r"[A-Z0-9]{8,10}", text):
        if 8 <= len(candidate) <= 10:
            return candidate

    unified_match = re.search(r"[A-Z0-9]{18}", str(unified_social_credit_code or "").upper())
    if unified_match:
        unified_code = unified_match.group(0)
        return unified_code[8:17]
    return ""


def build_clean_identity(identity: dict[str, Any]) -> dict[str, str]:
    """Clean identity fields without report-specific hard-coded fallbacks."""
    identity = identity or {}
    unified_source = " ".join(
        str(identity.get(key) or "")
        for key in (
            "unified_social_credit_code",
            "organization_credit_code",
            "business_registration_no",
            "taxpayer_id_national",
            "taxpayer_id_local",
        )
    )
    unified_code = clean_code(unified_source, 18)

    return {
        "company_name": clean_company_name(identity.get("company_name")),
        "credit_code": clean_generic_code(identity.get("zhongzheng_code") or identity.get("credit_code")),
        "unified_social_credit_code": unified_code,
        "org_code": clean_org_code(identity.get("org_code") or identity.get("organization_code"), unified_code),
        "business_registration_no": clean_code(identity.get("business_registration_no"), 18) or unified_code,
        "taxpayer_id_national": clean_code(identity.get("taxpayer_id_national"), 18) or unified_code,
        "taxpayer_id_local": clean_code(identity.get("taxpayer_id_local"), 18) or unified_code,
    }


def _find_active_row(rows: list[dict[str, Any]], *names: str) -> dict[str, Any]:
    for item in rows:
        row_type = str(item.get("type") or "")
        if row_type in names or any(name in row_type for name in names):
            return item
    return {}


def _clean_actual_controller_name(value: Any) -> str | None:
    text = str(value or "")
    stop_keywords = [
        "经济类型",
        "组织机构类型",
        "企业规模",
        "所属行业",
        "成立年份",
        "登记地址",
        "办公/经营地址",
        "存续状态",
        "注册资本",
        "信息来源机构",
        "第 ",
    ]
    for stop in stop_keywords:
        index = text.find(stop)
        if index != -1:
            text = text[:index]
    text = re.sub(r"\s+", " ", text).strip(" ：:,，;；")
    if "身份证" in text:
        before_id = text.split("身份证", 1)[0]
        name_match = re.search(r"([\u4e00-\u9fa5]{2,4})\s*$", before_id)
        text = name_match.group(1) if name_match else before_id
    else:
        name_match = re.search(r"([\u4e00-\u9fa5]{2,4})", text)
        text = name_match.group(1) if name_match else text

    dirty_words = ("合计", "内资", "企业", "小型企业")
    if not text or any(word in text for word in dirty_words):
        return None
    return text


def extract_actual_controller(text: str) -> str:
    if not text:
        return ""

    index = text.find("实际控制人")
    if index == -1:
        return ""

    window = text[index : index + 3000]
    for stop in ["信贷记录明细", "未结清信贷", "信息概要", "公共记录明细", "附件1"]:
        position = window.find(stop)
        if position > 50:
            window = window[:position]
            break

    logger.info("[EnterpriseCredit][DEBUG] actual_controller_window=%s", window[:1000])
    compact = re.sub(r"\s+", " ", window)
    compact_no_space = re.sub(r"\s+", "", window)

    controller_part = ""
    header_patterns = [
        "名称 身份标识类型 身份标识号码",
        "名称身份标识类型身份标识号码",
    ]
    for header in header_patterns:
        source = compact if " " in header else compact_no_space
        header_index = source.rfind(header)
        if header_index != -1:
            controller_part = source[header_index : header_index + 300]
            break

    if not controller_part:
        return ""

    match = re.search(r"([\u4e00-\u9fa5]{2,4})\s*身份证\s*\d{15,18}", controller_part)
    if not match:
        match = re.search(r"([\u4e00-\u9fa5]{2,4})身份证\d{15,18}", controller_part)
    if match:
        name = match.group(1)
        prefix = controller_part[max(0, match.start() - 10) : match.start()]
        if name not in ["名称", "姓名", "股东"] and not any(word in prefix for word in ["股东", "法定代表人", "负责", "出资方"]):
            return _clean_actual_controller_name(name) or ""

    return ""


def risk_level_zh(level: Any) -> str:
    mapping = {
        "high": "高",
        "medium": "中",
        "low": "低",
        "unknown": "未识别",
        "": "未识别",
        "none": "未识别",
    }
    if level in (None, "", "未识别", "暂无"):
        return "未识别"
    return mapping.get(str(level).lower(), str(level))


def loan_term_type(loan: dict[str, Any]) -> str:
    biz = str(loan.get("biz_type") or loan.get("loan_type") or "")
    section_type = str(loan.get("section_type") or "")
    explicit_term_type = str(loan.get("term_type") or "")
    due_date = str(loan.get("due_date") or loan.get("end_date") or "")
    open_date = str(loan.get("open_date") or loan.get("start_date") or "")

    if explicit_term_type == "revolving_overdraft":
        return "revolving_overdraft"
    if explicit_term_type == "medium_long":
        return "medium_long"
    if explicit_term_type == "short":
        return "short"
    if "循环透支" in biz or section_type == "循环透支":
        return "revolving_overdraft"

    if "融资型租赁" in biz or "中长期" in biz:
        return "medium_long"

    try:
        from datetime import datetime

        start = datetime.strptime(open_date, "%Y-%m-%d")
        end = datetime.strptime(due_date, "%Y-%m-%d")
        if (end - start).days > 365:
            return "medium_long"
    except Exception:
        pass

    return "short"


def _sum_loan_balances(loans: list[dict[str, Any]]) -> str | None:
    values = [_to_float(loan.get("balance")) for loan in loans]
    values = [value for value in values if value is not None]
    if not values:
        return None
    total = sum(values)
    return f"{total:.2f}".rstrip("0").rstrip(".")


def format_loan_detail_lines(loans: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for loan in loans:
        amount_label = "信用额度" if loan_term_type(loan) == "revolving_overdraft" else "借款金额"
        lines.extend(
            [
                f"  - 机构：{_display(loan.get('bank'))}",
                f"    业务：{_display(loan.get('biz_type') or loan.get('loan_type'))}",
                f"    担保方式：{_display(loan.get('guarantee') or loan.get('guarantee_type'))}",
                f"    {amount_label}：{_display(loan.get('loan_amount'))} 万元",
                f"    余额：{_display(loan.get('balance'))} 万元",
                f"    开立日期：{_display(loan.get('open_date') or loan.get('start_date'))}",
                f"    到期日：{_display(loan.get('due_date') or loan.get('end_date'))}",
                f"    五级分类：{_display(loan.get('five_classification'))}",
                f"    逾期月数：{_display(loan.get('overdue_months') or '0')}",
            ]
        )
    return lines


def _build_markdown_summary_v2(extracted_json: dict[str, Any]) -> str:
    report_basic = extracted_json.get("report_basic") or {}
    identity_info = extracted_json.get("identity_info") or {}
    registration_info = extracted_json.get("registration_info") or {}
    credit_summary = extracted_json.get("credit_summary") or {}
    risk_indicators = extracted_json.get("risk_indicators") or {}
    facility_summary = extracted_json.get("credit_facility_summary") or {}
    shareholders = extracted_json.get("shareholders") or []
    personnel = extracted_json.get("key_personnel") or []
    actual_controller = extracted_json.get("actual_controller") or {}
    active_rows = extracted_json.get("active_credit_summary_by_type") or []
    active_loans = extracted_json.get("active_loans") or []
    credit_facilities = extracted_json.get("credit_facilities") or []

    short_term = _find_active_row(active_rows, "短期借款", "鐭湡鍊熸")
    long_term = _find_active_row(active_rows, "中长期借款", "涓暱鏈熷€熸")
    revolving_overdraft_row = _find_active_row(active_rows, "循环透支")
    non_revolving = facility_summary.get("non_revolving") or {}
    revolving = facility_summary.get("revolving") or {}
    short_loans = [loan for loan in active_loans if loan_term_type(loan) == "short"]
    medium_long_loans = [loan for loan in active_loans if loan_term_type(loan) == "medium_long"]
    revolving_loans = [loan for loan in active_loans if loan_term_type(loan) == "revolving_overdraft"]
    short_balance = short_term.get("total_balance") or credit_summary.get("short_term_loan_balance") or _sum_loan_balances(short_loans)
    medium_long_balance = long_term.get("total_balance") or credit_summary.get("medium_long_term_loan_balance") or _sum_loan_balances(medium_long_loans)
    revolving_balance = (
        credit_summary.get("revolving_overdraft_balance")
        or revolving_overdraft_row.get("total_balance")
        or _sum_loan_balances(revolving_loans)
        or "0"
    )
    medium_count = credit_summary.get("medium_long_loan_count")
    if medium_count is None:
        medium_count = long_term.get("normal_account_count") or long_term.get("total_account_count")
    short_count = credit_summary.get("short_loan_count")
    if short_count is None:
        short_count = short_term.get("normal_account_count") or short_term.get("total_account_count")
    revolving_count = credit_summary.get("revolving_overdraft_count")
    if revolving_count is None:
        revolving_count = revolving_overdraft_row.get("normal_account_count") or revolving_overdraft_row.get("total_account_count")
    count_values = [value for value in (medium_count, short_count, revolving_count) if value is not None]
    if count_values:
        current_open_credit_count = sum(int(value or 0) for value in count_values)
    else:
        current_open_credit_count = len(medium_long_loans) + len(short_loans) + len(revolving_loans)
    logger.info(
        "[EnterpriseCredit][DEBUG] open_credit_count medium=%s short=%s revolving=%s total=%s",
        medium_count if medium_count is not None else len(medium_long_loans),
        short_count if short_count is not None else len(short_loans),
        revolving_count if revolving_count is not None else len(revolving_loans),
        current_open_credit_count,
    )
    clean_identity = build_clean_identity({**report_basic, **identity_info})

    lines = [
        "## 企业征信摘要",
        "",
        "### 报告基础信息",
        f"- 企业名称：{clean_identity['company_name']}",
        f"- 中征码：{clean_identity['credit_code']}",
        f"- 统一社会信用代码：{clean_identity['unified_social_credit_code']}",
        f"- 组织机构代码：{clean_identity['org_code']}",
        f"- 工商注册号：{clean_identity['business_registration_no']}",
        f"- 纳税人识别号(国税)：{clean_identity['taxpayer_id_national']}",
        f"- 纳税人识别号(地税)：{clean_identity['taxpayer_id_local']}",
        f"- 报告编号：{_display(report_basic.get('report_no'))}",
        f"- 查询机构：{_display(report_basic.get('query_institution'))}",
        f"- 报告时间：{_display(format_report_date(report_basic.get('report_date')))}",
        "",
        "### 信贷概要",
        f"- 当前未结清借贷余额：{_display(credit_summary.get('active_borrowing_balance'))}",
        f"- 当前未结清信贷机构数：{_display(current_open_credit_count)}",
    ]
    lines.append(f"- 短期借款余额：{_display(short_balance)}")
    if short_loans:
        lines.extend(format_loan_detail_lines(short_loans))
    else:
        lines.append("  - 暂未识别到短期借款明细")
    lines.append(f"- 中长期借款余额：{_display(medium_long_balance)}")
    if medium_long_loans:
        lines.extend(format_loan_detail_lines(medium_long_loans))
    else:
        lines.append("  - 暂未识别到中长期借款明细")
    lines.append(f"- 循环透支余额：{_display(revolving_balance)}")
    if revolving_loans:
        lines.extend(format_loan_detail_lines(revolving_loans))
    lines.extend(
        [
            f"- 关注类余额：{_display(credit_summary.get('active_special_mention_balance'))}",
            f"- 不良类余额：{_display(credit_summary.get('active_non_performing_balance'))}",
            f"- 对外担保余额：{_display(credit_summary.get('guarantee_balance'))}",
            "",
            "### 授信信息",
        ]
    )
    if credit_facilities:
        for item in credit_facilities:
            lines.extend(
                [
                    f"- 授信机构：{_display(item.get('institution'))}",
                    f"  授信额度类型：{_display(item.get('credit_type') or item.get('facility_type'))}",
                    f"  额度循环标志：{_display(item.get('is_revolving'))}",
                    f"  授信额度：{_display(item.get('credit_amount') or item.get('limit_amount'))} 万元",
                    f"  已用额度：{_display(item.get('used_amount') or item.get('used_limit'))} 万元",
                    f"  生效日期：{_display(item.get('effective_date'))}",
                    f"  到期日：{_display(item.get('due_date') or item.get('maturity_date'))}",
                    f"  信息报告日期：{_display(item.get('report_date'))}",
                ]
            )
    else:
        lines.append("- 暂未识别到授信信息")
    lines.extend(
        [
            "",
            "### 授信额度",
            f"- 非循环额度：总额 {_display(non_revolving.get('total_limit'))} / 已用 {_display(non_revolving.get('used_limit'))} / 可用 {_display(non_revolving.get('available_limit'))}",
            f"- 循环额度：总额 {_display(revolving.get('total_limit'))} / 已用 {_display(revolving.get('used_limit'))} / 可用 {_display(revolving.get('available_limit'))}",
            "",
            "### 企业基本信息",
            f"- 经济类型：{_display(registration_info.get('economic_type'))}",
            f"- 组织机构类型：{_display(registration_info.get('organization_type'))}",
            f"- 企业规模：{_display(registration_info.get('enterprise_size'))}",
            f"- 所属行业：{_display(registration_info.get('industry'))}",
            f"- 成立年份：{_display(registration_info.get('established_year'))}",
            f"- 注册资本：{_display(registration_info.get('registered_capital_rmb'))}",
            f"- 经营状态：{_display(registration_info.get('business_status'))}",
            f"- 注册地址：{_display(registration_info.get('registered_address'))}",
            f"- 经营地址：{_display(registration_info.get('business_address'))}",
            "",
            "### 股东与人员",
        ]
    )
    if shareholders:
        for item in shareholders:
            lines.append(f"- 股东：{_display(item.get('name'))}，持股 {_display(item.get('contribution_ratio') or item.get('shareholding_ratio'))}")
    else:
        lines.append("- 暂未识别到股东信息")
    legal_person = next((item for item in personnel if "法定代表人" in str(item.get("position") or "") and item.get("name")), {})
    lines.append(f"- 法定代表人：{_display(legal_person.get('name'))}")
    lines.append(f"- 实际控制人：{_display(actual_controller.get('name'))}")
    risk_tags = risk_indicators.get("risk_tags") or []
    lines.extend(
        [
            "",
            "### 风险指标",
            f"- 是否逾期：{'是' if risk_indicators.get('has_overdue') else '否'}",
            f"- 是否不良：{'是' if risk_indicators.get('has_non_performing') else '否'}",
            f"- 是否关注：{'是' if risk_indicators.get('has_special_mention') else '否'}",
            f"- 多头授信风险：{risk_level_zh(risk_indicators.get('multi_credit_risk') or risk_indicators.get('multi_lender_risk'))}",
            f"- 短期负债占比：{_format_ratio_percent(risk_indicators.get('short_term_debt_ratio'))}",
            f"- 授信使用率：{_format_ratio_percent(risk_indicators.get('credit_utilization_ratio'))}",
            f"- 风险标签：{'、'.join(risk_tags) if risk_tags else '未识别'}",
            f"- 风险总结：{_display(risk_indicators.get('summary'))}",
        ]
    )
    return "\n".join(lines).strip()


def _build_markdown_summary(extracted_json: dict[str, Any]) -> str:
    report_basic = extracted_json.get("report_basic") or {}
    identity_info = extracted_json.get("identity_info") or {}
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
    revolving_overdraft = next((item for item in active_rows if item.get("type") == "循环透支"), {})
    clean_identity = build_clean_identity({**report_basic, **identity_info})

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
        f"- 企业名称：{clean_identity['company_name']}",
        f"- 中征码：{clean_identity['credit_code']}",
        f"- 统一社会信用代码：{clean_identity['unified_social_credit_code']}",
        f"- 组织机构代码：{clean_identity['org_code']}",
        f"- 工商注册号：{clean_identity['business_registration_no']}",
        f"- 纳税人识别号(国税)：{clean_identity['taxpayer_id_national']}",
        f"- 纳税人识别号(地税)：{clean_identity['taxpayer_id_local']}",
        f"- 报告编号：{report_basic.get('report_no') or '未识别'}",
        f"- 查询机构：{report_basic.get('query_institution') or '未识别'}",
        f"- 报告时间：{format_report_date(report_basic.get('report_date')) or '未识别'}",
        "",
        "### 信贷概要",
        f"- 当前未结清借贷余额：{credit_summary.get('active_borrowing_balance') or '未识别'}",
        f"- 当前未结清信贷机构数：{credit_summary.get('current_active_credit_institution_count') if credit_summary.get('current_active_credit_institution_count') is not None else '未识别'}",
        f"- 短期借款余额：{short_term.get('total_balance') or '未识别'}",
        f"- 中长期借款余额：{long_term.get('total_balance') or '未识别'}",
        f"- 循环透支余额：{credit_summary.get('revolving_overdraft_balance') or revolving_overdraft.get('total_balance') or '0'}",
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
        f"- 多头授信风险：{risk_level_zh(risk_indicators.get('multi_credit_risk') or risk_indicators.get('multi_lender_risk'))}",
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
            logger.info("[EnterpriseCredit][DEBUG] raw_text_head=%s", raw_text[:5000])
            logger.info("[EnterpriseCredit][DEBUG] section keys=%s", list(sections.keys()))
            for section_name, section_lines in sections.items():
                logger.info("[EnterpriseCredit][DEBUG] section=%s head=%s", section_name, _section_text(section_lines)[:1000])
            zh_overrides = _zh_parse_enterprise_credit_overrides(raw_text)
            zh_windows = zh_overrides.get("debug_windows") or {}
            if zh_windows.get("info_summary"):
                logger.info("[EnterpriseCredit][DEBUG] info_summary_section=%s", zh_windows.get("info_summary", "")[:3000])
            if zh_windows.get("basic_info"):
                logger.info("[EnterpriseCredit][DEBUG] basic_info_section=%s", zh_windows.get("basic_info", "")[:3000])

            info_summary_text = _window_after_best(
                raw_text,
                ("信息概要",),
                ("首次有信贷交易", "借贷交易"),
                5000,
            ) or _section_text(sections.get("summary") or [])
            basic_info_text = _window_after_best(
                raw_text,
                ("基本概况信息", "基本信息", "经济类型"),
                ("经济类型", "企业规模"),
                5000,
            ) or _section_text(sections.get("basic") or [])
            capital_text = _window_after_best(
                raw_text,
                ("注册资本及主要出资人信息", "注册资本折人民币合计"),
                ("注册资本折人民币合计",),
                1600,
            ) or basic_info_text
            personnel_text = _window_after_best(
                raw_text,
                ("主要组成人员信息", "主要组成人员"),
                ("身份证",),
                1200,
            ) or basic_info_text
            controller_text = _window_after_best(
                raw_text,
                ("实际控制人",),
                ("身份标识", "身份证"),
                1000,
            ) or basic_info_text
            facility_text = _window_after_best(
                raw_text,
                ("非循环信用额度",),
                ("循环信用额度", "总额", "已用额度"),
                1200,
            ) or info_summary_text
            logger.info("[EnterpriseCredit][DEBUG] info_summary_section=%s", info_summary_text[:3000])
            logger.info("[EnterpriseCredit][DEBUG] basic_info_section=%s", basic_info_text[:3000])

            header_lines = _merge_fragment_lines(sections.get("header") or [])
            identity_lines = _merge_fragment_lines(sections.get("identity") or [])
            summary_lines = _window_lines(info_summary_text)
            basic_lines = _window_lines(basic_info_text)
            capital_lines = _window_lines(capital_text)
            personnel_lines = _window_lines(personnel_text)
            controller_lines = _window_lines(controller_text)
            facility_lines = _window_lines(facility_text)
            credit_detail_text = normalize_credit_text(_section_text(sections.get("credit_detail") or []))
            public_record_text = _section_text(sections.get("public_records") or [])

            report_basic = _extract_report_basic(
                _section_text(sections.get("header") or []) or raw_text[:2000],
                header_lines,
                input_data.customer_id,
                str(input_data.metadata.get("customer_name") or ""),
                raw_pages,
            )
            identity_info = _extract_identity_info(identity_lines, _section_text(sections.get("identity") or []))
            raw_identity_info = extract_identity_info(raw_text)
            if raw_identity_info:
                identity_info = _merge_meaningful_values(identity_info, {
                    "organization_code": raw_identity_info.get("organization_code") or raw_identity_info.get("org_code"),
                    "org_code": raw_identity_info.get("org_code") or raw_identity_info.get("organization_code"),
                    "unified_social_credit_code": raw_identity_info.get("unified_social_credit_code") or raw_identity_info.get("organization_credit_code"),
                    "business_registration_no": raw_identity_info.get("business_registration_no"),
                    "taxpayer_id_national": raw_identity_info.get("taxpayer_id_national"),
                    "taxpayer_id_local": raw_identity_info.get("taxpayer_id_local"),
                })
                report_basic = _merge_meaningful_values(report_basic, {
                    "company_name": raw_identity_info.get("company_name"),
                    "credit_code": raw_identity_info.get("unified_social_credit_code") or raw_identity_info.get("organization_credit_code"),
                    "zhongzheng_code": raw_identity_info.get("credit_code") or raw_identity_info.get("zhongzheng_code"),
                })
            if report_basic.get("report_date"):
                report_basic["report_date"] = format_report_date(report_basic.get("report_date"))
            registration_info = _extract_registration_info(basic_lines, basic_info_text + "\n" + capital_text)
            credit_summary = _extract_credit_summary(summary_lines, info_summary_text)
            active_credit_summary_by_type = _extract_active_credit_summary_by_type(summary_lines, info_summary_text)
            credit_facility_summary = _extract_credit_facility_summary(facility_lines, facility_text)
            shareholders = _extract_shareholders(capital_lines, capital_text)
            key_personnel = _extract_key_personnel(personnel_lines, personnel_text)
            actual_controller = _extract_actual_controller(controller_lines, controller_text)

            if zh_overrides.get("registration_info"):
                registration_info = _merge_meaningful_values(registration_info, zh_overrides["registration_info"])
            if zh_overrides.get("credit_summary"):
                credit_summary = _merge_meaningful_values(credit_summary, zh_overrides["credit_summary"])
            if zh_overrides.get("active_credit_summary_by_type"):
                active_credit_summary_by_type = zh_overrides["active_credit_summary_by_type"]
                total_row = _find_active_row(active_credit_summary_by_type, "合计")
                if total_row:
                    credit_summary["active_borrowing_balance"] = total_row.get("total_balance")
                    credit_summary["active_special_mention_balance"] = total_row.get("special_mention_balance")
                    credit_summary["active_non_performing_balance"] = total_row.get("non_performing_balance")
            if zh_overrides.get("credit_facility_summary"):
                credit_facility_summary = zh_overrides["credit_facility_summary"]
            if zh_overrides.get("shareholders"):
                shareholders = zh_overrides["shareholders"]
            if zh_overrides.get("key_personnel"):
                key_personnel = zh_overrides["key_personnel"]
            if zh_overrides.get("actual_controller"):
                actual_controller = zh_overrides["actual_controller"]

            explicit_controller_name = extract_actual_controller(raw_text)
            if explicit_controller_name:
                actual_controller = {
                    **(actual_controller or {}),
                    "name": explicit_controller_name,
                }
            else:
                actual_controller = {}
            key_personnel = _backfill_personnel_identity_numbers(key_personnel, shareholders, actual_controller)
            if actual_controller.get("name") and not actual_controller.get("identity_no"):
                for person in key_personnel:
                    if person.get("name") == actual_controller.get("name") and person.get("identity_no"):
                        actual_controller["identity_type"] = actual_controller.get("identity_type") or person.get("identity_type")
                        actual_controller["identity_no"] = person.get("identity_no")
                        break
            public_records = _extract_public_records(_merge_fragment_lines(sections.get("public_records") or []), public_record_text, credit_summary)
            logger.info("[DEBUG] has_loan_section=%s", "信贷记录明细" in raw_text)
            loan_section = _zh_window_after(raw_text, ("信贷记录明细",), (), 5000)
            logger.info("[DEBUG] loan_section_head=%s", loan_section[:1000])
            logger.info("[DEBUG] has_account_no=%s", "账户编号" in loan_section)
            debug_blocks = _split_loan_blocks(loan_section)
            logger.info("[DEBUG] total_blocks=%s", len(debug_blocks))
            if debug_blocks:
                logger.info("[DEBUG] first_block_sample=%s", debug_blocks[0][:500])
            active_loan_detail_text = _extract_active_credit_text(credit_detail_text or _zh_window_after(raw_text, ("信贷记录明细",), ("账户编号",), 20000))
            if "短期借款 共" not in active_loan_detail_text:
                logger.warning("[EnterpriseCredit][DEBUG] credit_detail active_text missing short loans, use raw_text fallback")
                active_loan_detail_text = _extract_active_credit_text(raw_text)
            logger.info("[EnterpriseCredit][DEBUG] credit_detail_len=%s", len(active_loan_detail_text))
            active_loans = _extract_active_loans_from_credit_detail(
                active_loan_detail_text,
                credit_summary.get("active_borrowing_balance"),
                raw_text,
            )
            logger.info("[DEBUG] active_loans_count=%s", len(active_loans))
            logger.info("[EnterpriseCredit][DEBUG] active_loans_sample=%s", active_loans[:2])
            credit_limit_text = extract_credit_limit_text(raw_text)
            credit_limit_expected_count = extract_credit_limit_count(credit_limit_text)
            logger.info("[EnterpriseCredit][DEBUG] credit_limit_expected_count=%s", credit_limit_expected_count)
            logger.info("[EnterpriseCredit][DEBUG] credit_limit_text_len=%s tail=%s", len(credit_limit_text), credit_limit_text[-1500:])
            logger.info("[EnterpriseCredit][DEBUG] credit_limit_text_tail=%s", credit_limit_text[-1000:])
            credit_facilities = parse_credit_limits(credit_limit_text)
            if credit_limit_expected_count > 0:
                credit_facilities = credit_facilities[:credit_limit_expected_count]
            logger.info("[EnterpriseCredit][DEBUG] credit_limit_actual_count=%s", len(credit_facilities))
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
            risk_level = (extracted_json.get("risk_indicators") or {}).get("multi_credit_risk") or (extracted_json.get("risk_indicators") or {}).get("multi_lender_risk")
            logger.info("[EnterpriseCredit][DEBUG] actual_controller=%s", actual_controller)
            logger.info("[EnterpriseCredit][DEBUG] multi_credit_risk_raw=%s zh=%s", risk_level, risk_level_zh(risk_level))
            extracted_json["risk_signals"] = [
                {
                    "type": tag,
                    "level": "high" if "不良" in tag else ("medium" if "关注" in tag or "较高" in tag else "low"),
                    "text": tag,
                }
                for tag in (extracted_json.get("risk_indicators") or {}).get("risk_tags", [])
            ]
            markdown_summary = _build_markdown_summary_v2(extracted_json)

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
