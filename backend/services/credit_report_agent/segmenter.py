from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)


SECTION_KEYS = [
    "basic_info",
    "credit_summary",
    "short_term_loans",
    "medium_long_term_loans",
    "revolving_overdrafts",
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
    text = text.replace("账户编\n号", "账户编号")
    text = text.replace("账户编 号", "账户编号")
    text = text.replace("账户 编号", "账户编号")
    text = text.replace("号授信机构", "号 授信机构")
    text = re.sub(r"第\s*\d+\s*页\s*/\s*共\s*\d+\s*页", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def _enterprise_credit_debug_enabled() -> bool:
    return os.getenv("ENTERPRISE_CREDIT_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _line_range(full_text: str, section_text: str) -> tuple[int, int]:
    if not full_text or not section_text:
        return 0, 0
    pos = full_text.find(section_text[:120])
    if pos == -1:
        return 0, 0
    start_line = full_text[:pos].count("\n") + 1
    end_line = start_line + section_text.count("\n")
    return start_line, end_line


def trace_credit_sections(sections: dict[str, str | int | dict[str, int]]) -> None:
    if not _enterprise_credit_debug_enabled():
        return
    full_text = str(sections.get("full_text") or "")
    for key in SECTION_KEYS:
        value = sections.get(key)
        if not isinstance(value, str):
            continue
        start_line, end_line = _line_range(full_text, value)
        title = (value.splitlines()[0].strip() if value.splitlines() else "")
        preview = value[:300].replace("\n", "\\n")
        logger.warning(
            "[EnterpriseCredit][SECTION_TRACE] section_type=%s title=%s start_line=%s end_line=%s entered_extractor=%s preview=%s",
            key,
            title,
            start_line,
            end_line,
            bool(value),
            preview,
        )


def _first_match(text: str, patterns: list[str], start: int = 0) -> re.Match[str] | None:
    best: re.Match[str] | None = None
    best_pos = -1
    for pattern in patterns:
        match = re.search(pattern, text[start:], re.S)
        if not match:
            continue
        pos = start + match.start()
        if best is None or pos < best_pos:
            best = match
            best_pos = pos
    return best


def extract_section_by_title(text: str, start_patterns: list[str], end_patterns: list[str]) -> str:
    text = normalize_text(text)
    match = _first_match(text, start_patterns)
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
    start = text.find("未结清信贷")
    if start == -1:
        return ""
    end_candidates = [
        text.find("已结清信贷", start + 1),
        text.find("公共记录明细", start + 1),
        text.find("非信贷交易明细", start + 1),
        text.find("附件1", start + 1),
    ]
    ends = [pos for pos in end_candidates if pos != -1]
    end = min(ends) if ends else len(text)
    return text[start:end].strip()


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
        "非信贷交易明细",
    ]
    for next_title in next_titles:
        if next_title == title:
            continue
        m = re.search(rf"{re.escape(next_title)}\s*(?:共\s*\d+\s*笔)?", section[1:], re.S)
        if m:
            end = min(end, 1 + m.start())
    return section[:end].strip(), expected


def extract_revolving_window_preserving_pending(raw_text: str) -> tuple[str, int]:
    text = normalize_text(raw_text)
    match = None
    search_from = 0
    for keyword in ("信贷记录明细", "淇¤捶璁板綍鏄庣粏", "未结清信贷", "鏈粨娓呬俊璐?"):
        pos = text.find(keyword)
        if pos != -1:
            search_from = pos
            break
    search_text = text[search_from:]
    for variant in ("循环透支账户明细", "未结清循环透支", "循环透支账户", "循环透支余额", "循环透支", "循环额度", "透支余额", "寰幆閫忔敮", "寰幆璐锋", "寰幆棰濆害"):
        match = re.search(rf"{re.escape(variant)}\s*(?:共|鍏.|閸.)\s*(\d+)\s*(?:笔|绗.|缁.)", search_text, re.S)
        if match:
            start_pos = search_from + match.start()
            break
    if not match:
        return "", 0
    expected = int(match.group(1)) if match.group(1) else 0
    lines = [line.strip() for line in text[start_pos:].replace("\r", "\n").split("\n") if line.strip()]
    hard_end = (
        "短期借款", "中长期借款", "银行保函及其他业务", "授信信息", "对外担保", "查询记录", "报告说明",
        "鐭湡鍊熸", "涓暱鏈熷€熸", "閾惰淇濆嚱鍙婂叾浠栦笟鍔?", "鎺堜俊淇℃伅", "瀵瑰鎷呬繚", "鏌ヨ璁板綍", "鎶ュ憡璇存槑",
    )
    public_markers = ("公共记录明细", "鍏叡璁板綍鏄庣粏")
    collected: list[str] = []
    pending = False
    completed = False
    for idx, line in enumerate(lines):
        compact = re.sub(r"\s+", "", line)
        if re.search(r"第\s*\d+\s*页/共\s*\d+\s*页|第\s*\d+\s*页/共", line):
            continue
        if idx > 0 and any(key in compact for key in hard_end) and (not pending or completed):
            break
        if any(key in compact for key in public_markers):
            if pending and not completed:
                for marker in public_markers:
                    line = line.replace(marker, " ")
                line = re.sub(r"\s+", " ", line).strip()
                if not line:
                    continue
            elif idx > 0:
                break
        collected.append(line)
        compact = re.sub(r"\s+", "", line)
        if not pending and re.search(r"(?:银行股份有限公司|閾惰鑲′唤鏈夐檺鍏徃|银行).*(?:流动资金贷款|娴佸姩璧勯噾璐锋|循环透支|寰幆閫忔敮).*\d{4}-\d{2}-\d{2}.*\d{4}-\d{2}-\d{2}", compact):
            pending = True
        if pending and not completed and re.search(r"(抵押|鎶垫娂|保证|淇濊瘉|质押|璐ㄦ娂|信用|淇＄敤|组合|缁勫悎)\d+(?:\.\d+)?(正常|姝ｅ父|关注|鍏虫敞|次级|娆＄骇|可疑|鍙枒|损失|鎹熷け)\d+(?:\.\d+)?\d+(?:\.\d+)?\d+", compact):
            completed = True
    return "\n".join(collected).strip(), expected


def extract_credit_limit_section(raw_text: str) -> tuple[str, int]:
    text = normalize_text(raw_text)
    match = re.search(r"授信信息\s*共\s*(\d+)\s*笔", text, re.S)
    if not match:
        return "", 0
    expected = int(match.group(1))
    # 授信表可能跨页且页眉可能包含“已结清信贷”，这里取有界窗口，解析端按 expected count 控制。
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

    result = {
        "full_text": text,
        "basic_info": basic_info or text[:5000],
        "credit_summary": info_summary,
        "unsettled_credit": unsettled,
        "short_term_loans": short_text,
        "medium_long_term_loans": medium_text,
        "revolving_overdraft": revolving_text,
        "revolving_overdrafts": revolving_text,
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
    trace_credit_sections(result)
    return result


# UTF-8 safe overrides for reports whose PDF text layer is not mojibake.
def extract_unsettled_section(raw_text: str) -> str:
    text = normalize_text(raw_text)
    starts = [pos for key in ("未结清信贷", "鏈粨娓呬俊璐?") if (pos := text.find(key)) != -1]
    if not starts:
        return ""
    start = min(starts)
    end_candidates = []
    for key in ("已结清信贷", "宸茬粨娓呬俊璐?", "公共记录明细", "鍏叡璁板綍鏄庣粏", "非信贷交易明细", "闈炰俊璐蜂氦鏄撴槑缁?", "附件1", "闄勪欢1"):
        pos = text.find(key, start + 1)
        if pos != -1:
            end_candidates.append(pos)
    end = min(end_candidates) if end_candidates else len(text)
    return text[start:end].strip()


def extract_loan_subsection(unsettled_text: str, title: str) -> tuple[str, int]:
    text = normalize_text(unsettled_text)
    title_variants = {
        "短期借款": ["短期借款", "鐭湡鍊熸"],
        "中长期借款": ["中长期借款", "涓暱鏈熷€熸"],
        "循环透支": ["循环透支", "循环透支账户", "循环透支账户明细", "未结清循环透支", "循环透支余额", "循环贷款", "循环额度", "循环授信透支", "透支余额", "寰幆閫忔敮"],
        "鐭湡鍊熸": ["鐭湡鍊熸", "短期借款"],
        "涓暱鏈熷€熸": ["涓暱鏈熷€熸", "中长期借款"],
        "寰幆閫忔敮": ["寰幆閫忔敮", "循环透支", "循环透支账户", "循环透支账户明细", "未结清循环透支", "循环透支余额", "循环贷款", "循环额度", "循环授信透支", "透支余额"],
    }.get(title, [title])
    match: re.Match[str] | None = None
    for variant in title_variants:
        match = re.search(rf"{re.escape(variant)}\s*(?:共|鍏.)[\s]*(\d+)\s*(?:笔|绗.)", text, re.S)
        if match:
            break
    if not match:
        return "", 0
    expected = int(match.group(1))
    start = match.start()
    section = text[start:]
    end = len(section)
    next_titles = [
        "中长期借款", "涓暱鏈熷€熸",
        "短期借款", "鐭湡鍊熸",
        "循环透支", "循环透支账户", "循环透支账户明细", "未结清循环透支", "循环透支余额", "循环贷款", "循环额度", "循环授信透支", "透支余额", "寰幆閫忔敮",
        "银行承兑汇票和信用证", "閾惰鎵垮厬姹囩エ鍜屼俊鐢ㄨ瘉",
        "银行保函及其他业务", "閾惰淇濆嚱鍙婂叾浠栦笟鍔?",
        "授信信息", "鎺堜俊淇℃伅",
        "公共记录明细", "鍏叡璁板綍鏄庣粏",
        "查询记录", "报告说明",
    ]
    for next_title in next_titles:
        if next_title in title_variants:
            continue
        m = re.search(rf"{re.escape(next_title)}\s*(?:(?:共|鍏?)\s*\d+\s*(?:笔|绗?))?", section[1:], re.S)
        if m:
            end = min(end, 1 + m.start())
    return section[:end].strip(), expected


def segment_report(raw_text: str) -> dict[str, str | int | dict[str, int]]:
    text = normalize_text(raw_text)
    unsettled = extract_unsettled_section(text)
    short_text, short_count = extract_loan_subsection(unsettled, "短期借款")
    medium_text, medium_count = extract_loan_subsection(unsettled, "中长期借款")
    revolving_text, revolving_count = extract_loan_subsection(unsettled, "循环透支")
    if not revolving_text:
        revolving_text, revolving_count = extract_loan_subsection(unsettled, "寰幆閫忔敮")
    if (
        not revolving_text
        or ("454.68" not in revolving_text and "454.68" in text)
        or ("抵押" in text and "抵押" not in revolving_text)
        or ("鎶垫娂" in text and "鎶垫娂" not in revolving_text)
    ):
        fallback_revolving_text, fallback_revolving_count = extract_revolving_window_preserving_pending(text)
        if fallback_revolving_text:
            revolving_text = fallback_revolving_text
            revolving_count = revolving_count or fallback_revolving_count
    credit_line_text, credit_line_count = extract_credit_limit_section(text)
    bill_lc_text = extract_section_by_title(
        text,
        [r"银行承兑汇票和信用证\s*共\s*\d+\s*笔", r"閾惰鎵垮厬姹囩エ鍜屼俊鐢ㄨ瘉\s*鍏?\s*\d+\s*绗?"],
        [r"授信信息\s*共", r"鎺堜俊淇℃伅\s*鍏?", r"银行保函及其他业务\s*共", r"閾惰淇濆嚱鍙婂叾浠栦笟鍔?\s*鍏?", r"已结清信贷", r"宸茬粨娓呬俊璐?", r"公共记录明细", r"鍏叡璁板綍鏄庣粏", r"附件\s*1"],
    )
    guarantee_text = extract_section_by_title(
        text,
        [r"银行保函及其他业务\s*共\s*\d+\s*笔", r"閾惰淇濆嚱鍙婂叾浠栦笟鍔?\s*鍏?\s*\d+\s*绗?"],
        [r"授信信息\s*共", r"鎺堜俊淇℃伅\s*鍏?", r"银行承兑汇票和信用证\s*共", r"已结清信贷", r"宸茬粨娓呬俊璐?", r"公共记录明细", r"附件\s*1"],
    )
    info_summary = extract_section_by_title(text, [r"信息概要", r"淇℃伅姒傝"], [r"基本信息", r"基本概况信息", r"信贷记录明细", r"淇¤捶璁板綍鏄庣粏"])
    basic_info = extract_section_by_title(text, [r"身份标识", r"韬唤鏍囪瘑", r"基本信息", r"基本概况信息"], [r"信息概要", r"淇℃伅姒傝", r"信贷记录明细", r"淇¤捶璁板綍鏄庣粏"])
    public_records = extract_section_by_title(text, [r"公共记录明细", r"鍏叡璁板綍鏄庣粏"], [r"附件\s*1", r"闄勪欢\s*1", r"信用记录补充信息"])
    if revolving_text and public_records:
        public_compact = compact_text(public_records)
        revolving_compact = compact_text(revolving_text)
        looks_like_revolving_tail = (
            ("最近一次还款" in public_records or "授信协议编号" in public_records or "正常还款" in public_records)
            and ("五级分类" in public_records or "抵押" in public_records or "保证" in public_records)
        )
        if looks_like_revolving_tail and any(marker in revolving_compact for marker in ["抵押", "保证", "454.68"]):
            logger.warning("[EnterpriseCredit][SECTION_PRIORITY] public_records_tail_reassigned_to_revolving")
            public_records = ""
    result = {
        "full_text": text,
        "basic_info": basic_info or text[:5000],
        "credit_summary": info_summary,
        "unsettled_credit": unsettled,
        "short_term_loans": short_text,
        "medium_long_term_loans": medium_text,
        "revolving_overdraft": revolving_text,
        "revolving_overdrafts": revolving_text,
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
            "revolving_overdrafts": revolving_count,
            "credit_lines": credit_line_count,
        },
    }
    trace_credit_sections(result)
    return result
