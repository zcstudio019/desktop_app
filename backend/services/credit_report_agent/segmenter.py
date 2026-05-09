from __future__ import annotations

import re


SECTION_KEYS = [
    "basic_info",
    "credit_summary",
    "short_term_loans",
    "medium_long_term_loans",
    "credit_lines",
    "bills",
    "letters_of_credit",
    "guarantees",
    "external_guarantees",
    "overdue_or_abnormal",
    "public_records",
    "unknown_sections",
]


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = str(text).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"第\s*\d+\s*页\s*/\s*共\s*\d+\s*页", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def _find_first(text: str, patterns: list[str], start: int = 0) -> re.Match[str] | None:
    best: re.Match[str] | None = None
    for pattern in patterns:
        match = re.search(pattern, text[start:], re.S)
        if not match:
            continue
        abs_start = start + match.start()
        if best is None or abs_start < start + best.start():
            best = match
    return best


def extract_section_by_title(text: str, start_patterns: list[str], end_patterns: list[str]) -> str:
    text = normalize_text(text)
    match = _find_first(text, start_patterns)
    if not match:
        return ""
    start = match.start()
    section = text[start:]
    end = len(section)
    for pattern in end_patterns:
        m = re.search(pattern, section[1:], re.S)
        if m:
            end = min(end, 1 + m.start())
    return section[:end].strip()


def extract_unsettled_section(raw_text: str) -> str:
    text = normalize_text(raw_text)
    match = re.search(r"未结清信贷", text)
    if not match:
        return ""
    section = text[match.start():]
    end = len(section)
    for pattern in [r"已结清信贷", r"公共记录明细", r"非信贷交易明细", r"附件\s*1"]:
        m = re.search(pattern, section[1:], re.S)
        if m:
            end = min(end, 1 + m.start())
    return section[:end].strip()


def extract_loan_subsection(unsettled_text: str, title: str) -> tuple[str, int]:
    text = normalize_text(unsettled_text)
    match = re.search(rf"{re.escape(title)}\s*共\s*(\d+)\s*笔", text)
    if not match:
        return "", 0
    expected = int(match.group(1))
    start = match.start()
    section = text[start:]
    end = len(section)
    next_titles = [
        "中长期借款",
        "短期借款",
        "循环透支",
        "银行承兑汇票和信用证",
        "银行保函及其他业务",
        "授信信息",
        "已结清信贷",
        "公共记录明细",
    ]
    for next_title in next_titles:
        if next_title == title:
            continue
        m = re.search(rf"{re.escape(next_title)}\s*(?:共\s*\d+\s*笔)?", section[1:], re.S)
        if m:
            end = min(end, 1 + m.start())
    return section[:end].strip(), expected


def extract_credit_limit_section(raw_text: str) -> tuple[str, int]:
    text = normalize_text(raw_text)
    match = re.search(r"授信信息\s*共\s*(\d+)\s*笔", text, re.S)
    if not match:
        return "", 0
    expected = int(match.group(1))
    # Credit lines often cross page headers that contain "已结清信贷"; take a bounded
    # window and stop by expected count during parsing rather than truncating early.
    section = text[match.start(): match.start() + 12000]
    section = re.sub(r"\n\s*已结清信贷\s*\n", "\n", section)
    return section.strip(), expected


def segment_report(raw_text: str) -> dict[str, str | int | dict[str, int]]:
    text = normalize_text(raw_text)
    unsettled = extract_unsettled_section(text)
    short_text, short_count = extract_loan_subsection(unsettled, "短期借款")
    medium_text, medium_count = extract_loan_subsection(unsettled, "中长期借款")
    revolving_text, revolving_count = extract_loan_subsection(unsettled, "循环透支")
    credit_line_text, credit_line_count = extract_credit_limit_section(text)

    bill_lc_text = extract_section_by_title(
        text,
        [r"银行承兑汇票和信用证\s*共\s*\d+\s*笔"],
        [r"授信信息\s*共", r"银行保函及其他业务\s*共", r"已结清信贷", r"公共记录明细", r"附件\s*1"],
    )
    guarantee_text = extract_section_by_title(
        text,
        [r"银行保函及其他业务\s*共\s*\d+\s*笔"],
        [r"授信信息\s*共", r"银行承兑汇票和信用证\s*共", r"已结清信贷", r"公共记录明细", r"附件\s*1"],
    )
    info_summary = extract_section_by_title(text, [r"信息概要"], [r"基本信息", r"基本概况信息", r"信贷记录明细"])
    basic_info = extract_section_by_title(text, [r"身份标识", r"基本信息", r"基本概况信息"], [r"信息概要", r"信贷记录明细"])
    public_records = extract_section_by_title(text, [r"公共记录明细"], [r"附件\s*1", r"信用记录补充信息"])

    return {
        "full_text": text,
        "basic_info": basic_info or text[:5000],
        "credit_summary": info_summary,
        "unsettled_credit": unsettled,
        "short_term_loans": short_text,
        "medium_long_term_loans": medium_text,
        "revolving_overdraft": revolving_text,
        "credit_lines": credit_line_text,
        "bills": bill_lc_text,
        "letters_of_credit": bill_lc_text,
        "guarantees": guarantee_text,
        "external_guarantees": "",
        "overdue_or_abnormal": "",
        "public_records": public_records,
        "unknown_sections": "",
        "expected_counts": {
            "short_term_loans": short_count,
            "medium_long_term_loans": medium_count,
            "revolving_overdraft": revolving_count,
            "credit_lines": credit_line_count,
        },
    }
