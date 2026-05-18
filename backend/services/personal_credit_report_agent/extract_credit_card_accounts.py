from __future__ import annotations

import logging
import re
from typing import Any

from .evidence import clean_amount, clean_value, first_match, split_numbered_blocks, value_after_label
from .schema import CREDIT_CARD_ACCOUNT_FIELDS, ensure_record_fields

logger = logging.getLogger(__name__)

CLOSED_STATUS_WORDS = ("已销户", "销户", "已注销", "注销", "已关闭", "关闭")
CARD_TYPES = ("准贷记卡", "贷记卡", "信用卡")
STATUS_WORDS = ("未销户", "已销户", "销户", "已注销", "注销", "已关闭", "关闭", "正常", "逾期", "冻结", "止付", "呆账")
ABNORMAL_WORDS = ("当前逾期", "逾期", "90天以上逾期", "呆账", "代偿", "核销")
ACTIVE_STATUS_WORDS = ("未销户", "正常", "当前有效")
FOREIGN_CURRENCY_WORDS = ("美元", "USD", "日元", "欧元", "澳大利亚元", "加元", "瑞士法郎", "新加坡元", "英镑", "新西兰元", "香港元", "港币", "外币")
SUMMARY_ONLY_WORDS = ("信用卡账户数", "信用卡90天以上逾期账户数", "信用卡 90 天以上逾期账户数", "贷款账户数", "信贷记录概要")
DETAIL_WORDS = ("贷记卡账户明细", "准贷记卡账户明细", "授信额度", "已用额度", "共享授信额度", "最近一次还款", "当前逾期", "发卡机构", "账户状态", "卡号")
LOAN_DETAIL_WORDS = ("贷款", "消费贷款", "购房贷款", "其他贷款", "五级分类")
STOP_SECTION_HEADINGS = (
    "贷款",
    "相关还款责任信息",
    "相关还款责任",
    "查询记录",
    "查询记录明细",
    "公共信息",
    "公共记录",
    "担保信息",
)
CARD_WINDOW_ANCHORS = (
    "信用卡",
    "贷记卡账户明细",
    "准贷记卡账户明细",
    "从未逾期过的贷记卡",
)


def _normalize_block(block: str) -> str:
    text = str(block or "").replace("\r", "\n")
    text = re.sub(r"[ \t\u3000]+", " ", text)
    text = re.sub(r"截至\s*((?:19|20)\d{2})年\s*\n+\s*(\d{1,2})月", lambda m: f"截至{m.group(1)}年{int(m.group(2)):02d}月", text)
    text = re.sub(r"信用\s*\n+\s*额度", "信用额度", text)
    text = re.sub(r"已\s*使用\s*\n+\s*额度", "已使用额度", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"截至\s*((?:19|20)\d{2})年\s*(\d{1,2})月", lambda m: f"截至{m.group(1)}年{int(m.group(2)):02d}月", text)
    text = re.sub(r"信用\s*额度", "信用额度", text)
    text = re.sub(r"已\s*使用\s*额度", "已使用额度", text)
    text = re.sub(r"卡片尾号\s*[:：]\s*", "卡片尾号:", text)
    return clean_value(text)


def _normalize_date(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    match = re.search(r"((?:19|20)\d{2})[-./](\d{1,2})[-./](\d{1,2})", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return clean_value(value)


def _normalize_year_month(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2})年(\d{1,2})月", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    match = re.search(r"((?:19|20)\d{2})[-./](\d{1,2})", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    return clean_value(value)


def _extract_credit_card_window(text: str) -> str:
    source = str(text or "")
    if not source.strip():
        return ""
    starts = [source.find(anchor) for anchor in CARD_WINDOW_ANCHORS if source.find(anchor) >= 0]
    if not starts:
        return source
    start = min(starts)
    tail = source[start:]
    stop_positions: list[int] = []
    for heading in STOP_SECTION_HEADINGS:
        pattern = rf"(?m)^\s*{re.escape(heading)}\s*[:：]?\s*$"
        match = re.search(pattern, tail)
        if match and match.start() > 0:
            stop_positions.append(match.start())
    end = min(stop_positions) if stop_positions else len(tail)
    return tail[:end]


def _active_card_sentence_blocks(text: str) -> list[str]:
    source = _normalize_block(text)
    if not source:
        return []
    starts = [
        match.start()
        for match in re.finditer(
            r"(?:\d+[\.、)]\s*)?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日[^。；;]{0,120}?发放的(?:准贷记卡|贷记卡|信用卡)",
            source,
        )
    ]
    blocks: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(source)
        block = source[start:end].strip()
        if block:
            blocks.append(block)
    return blocks


def _date_card_blocks(text: str) -> list[str]:
    source = _normalize_block(text)
    if not source:
        return []
    starts = [
        match.start()
        for match in re.finditer(r"(?:\d+[\.、)]\s*)?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", source)
    ]
    blocks: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(source)
        block = source[start:end].strip()
        if "发放的" in block and any(card_type in block for card_type in CARD_TYPES):
            blocks.append(block)
    return blocks


def _looks_like_card(block: str) -> bool:
    if any(keyword in block for keyword in SUMMARY_ONLY_WORDS) and not any(keyword in block for keyword in DETAIL_WORDS):
        return False
    if any(keyword in block for keyword in LOAN_DETAIL_WORDS) and not any(keyword in block for keyword in ("授信额度", "已用额度", "共享授信额度", "发卡机构", "信用卡")):
        return False
    return any(keyword in block for keyword in DETAIL_WORDS) or any(keyword in block for keyword in ("贷记卡", "准贷记卡"))


def _extract_label(block: str, labels: tuple[str, ...], *, max_chars: int = 120) -> str:
    value = value_after_label(block, labels, max_chars=max_chars)
    if value:
        return value
    compact = _normalize_block(block)
    for label in labels:
        match = re.search(rf"{re.escape(label)}\s*[:：]?\s*([^,，;；。|｜]{{1,{max_chars}}})", compact)
        if match:
            return clean_value(match.group(1))
    return ""


def _extract_money(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=80)
    if labeled:
        money = first_match(labeled, (r"((?:人民币)?\s*[0-9][0-9,]*(?:\.\d+)?\s*(?:万?元|万元)?)",))
        return clean_amount(money or labeled)
    return ""


def _extract_amount_after_label(block: str, labels: tuple[str, ...]) -> str:
    source = _normalize_block(block)
    for label in labels:
        match = re.search(rf"{re.escape(label)}\s*[:：]?\s*([0-9][0-9,]*(?:\.\d+)?)(?:\s*元)?", source)
        if match:
            return clean_amount(match.group(1))
    return ""


def _extract_date(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=50)
    match = re.search(r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?", labeled or "")
    return clean_value(match.group(0)) if match else ""


def _extract_institution(block: str) -> str:
    value = first_match(block, (
        r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日\s*([\u4e00-\u9fffA-Za-z0-9（）()·]{2,80}?)\s*发放的(?:准贷记卡|贷记卡|信用卡)",
    ))
    if value:
        return clean_value(value)
    value = (
        _extract_label(block, ("发卡机构", "机构", "授信机构", "管理机构"), max_chars=80)
        or first_match(block, (r"([\u4e00-\u9fffA-Za-z0-9（）()·]{2,50}(?:银行|信用社|金融公司|消费金融)[\u4e00-\u9fffA-Za-z0-9（）()·]{0,30})",))
    )
    value = re.sub(r"^(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", "", value or "")
    value = re.split(r"发放的(?:准贷记卡|贷记卡|信用卡)", value, maxsplit=1)[0]
    return clean_value(value)


def _extract_card_type(block: str) -> str:
    labeled = _extract_label(block, ("账户类型", "卡类型", "业务类型"), max_chars=40)
    if "准贷记卡" in labeled or "准贷记卡" in block:
        return "准贷记卡"
    if "贷记卡" in labeled or "贷记卡" in block:
        return "贷记卡"
    for item in CARD_TYPES:
        if item in labeled or item in block:
            return item
    return clean_value(labeled)


def _extract_currency(block: str) -> str:
    match = re.search(r"[（(]\s*(美元|人民币|欧元|港币|日元|英镑|澳大利亚元|加元|瑞士法郎|新加坡元|新西兰元|香港元)\s*账户[^）)]*[）)]", block)
    if match:
        return match.group(1)
    labeled = _extract_label(block, ("币种", "账户币种"), max_chars=20)
    if labeled:
        return labeled
    return "人民币" if "人民币" in block else ""


def _extract_status(block: str) -> str:
    labeled = _extract_label(block, ("账户状态", "状态", "当前状态"), max_chars=50)
    closed_text = f"{labeled} {block}"
    for word in CLOSED_STATUS_WORDS:
        if word in closed_text and "未销户" not in closed_text:
            return "销户" if word in {"销户", "已销户"} else word
    for word in STATUS_WORDS:
        if word in labeled:
            return word
    for word in STATUS_WORDS:
        if word in block:
            return word
    if _looks_like_active_card(block):
        return "当前有效"
    return clean_value(labeled)


def _extract_account_no(block: str) -> str:
    value = _extract_label(block, ("账户编号", "账户号", "卡号", "账号"), max_chars=80)
    if value:
        match = re.search(r"([A-Za-z0-9\-*]{4,40})", value)
        return match.group(1) if match else value
    return first_match(block, (r"(?:账户编号|账户号|卡号|账号)\s*[:：]?\s*([A-Za-z0-9\-*]{4,40})",))


def _extract_open_date(block: str) -> str:
    return _normalize_date(first_match(block, (r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日)",)))


def _extract_report_cutoff(block: str) -> str:
    source = _normalize_block(block)
    return _normalize_year_month(first_match(source, (r"截至\s*((?:19|20)\d{2}年\d{1,2}月)",)))


def _extract_card_tail_no(block: str) -> str:
    return first_match(block, (r"卡片尾号\s*[:：]\s*([A-Za-z0-9]{2,})",))


def _looks_like_active_card(block: str) -> bool:
    source = _normalize_block(block)
    return bool(
        re.search(r"截至\s*(?:19|20)\d{2}年\d{1,2}月", source)
        and "信用额度" in source
        and ("已使用额度" in source or "已用额度" in source or "使用额度" in source)
        and not any(word in source for word in CLOSED_STATUS_WORDS)
    )


def _extract_overdue_months(block: str) -> str:
    labeled = _extract_label(block, ("逾期月份", "逾期月数", "逾期期数", "当前逾期"), max_chars=80)
    source = labeled or block
    match = re.search(r"逾期\s*(\d+)\s*(?:个)?月", source)
    if match:
        return match.group(1)
    match = re.search(r"(\d+)\s*(?:个)?月", labeled)
    return match.group(1) if match else ""


def _amount_to_number(value: Any) -> float:
    text = re.sub(r"[,\s，人民币元]", "", str(value or ""))
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return 0.0
    number = float(match.group(1))
    if "万" in str(value or ""):
        number *= 10000
    return number


def _expected_active_credit_card_count(text: str) -> int:
    match = re.search(r"当前有效信用卡账户数\s*[:：]?\s*(\d+)", str(text or ""))
    return int(match.group(1)) if match else 0


def _is_rmb_active_card(record: dict[str, Any]) -> bool:
    evidence = str(record.get("evidence") or record.get("evidence_text") or "")
    return (
        str(record.get("currency") or "") == "人民币"
        and "销户" not in evidence
        and "注销" not in evidence
        and "关闭" not in evidence
        and (
            str(record.get("account_status") or "") in {"当前有效", "正常", "未销户"}
            or bool(record.get("report_cutoff") and record.get("credit_limit"))
        )
    )


def _dedupe_signature(record: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(record.get(key) or "")
        for key in (
            "open_date",
            "institution",
            "card_type",
            "currency",
            "card_tail_no",
            "credit_limit",
            "report_cutoff",
        )
    )


def _recovery_match_key(record: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(record.get(key) or "")
        for key in ("open_date", "institution", "currency", "card_tail_no", "report_cutoff")
    )


def _merge_recovered_card(existing: dict[str, Any], recovered: dict[str, Any]) -> None:
    for key in ("credit_limit", "used_limit", "used_amount", "account_status", "report_cutoff", "card_type", "currency", "open_date"):
        if not existing.get(key) and recovered.get(key):
            existing[key] = recovered.get(key)
    if str(existing.get("account_status") or "") in {"", "未识别", "未知", "不详"} and recovered.get("account_status"):
        existing["account_status"] = recovered.get("account_status")
    if recovered.get("credit_limit") and existing.get("credit_limit") == existing.get("used_amount"):
        existing["credit_limit"] = recovered.get("credit_limit")
    if recovered.get("used_amount"):
        existing["used_amount"] = recovered.get("used_amount")
        existing["used_limit"] = recovered.get("used_limit") or recovered.get("used_amount")


def recover_rmb_active_credit_cards(
    sections: dict[str, Any],
    records: list[dict[str, Any]],
    credit_summary: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Append or repair RMB active cards found in raw text."""
    try:
        del credit_summary
        source_text = "\n".join(
            str(sections.get(key) or "")
            for key in ("full_text", "credit_transaction_details", "credit_card_accounts")
            if sections.get(key)
        )
        if not source_text.strip():
            return records
        text = _extract_credit_card_window(source_text)
        result = list(records)
        seen = {_dedupe_signature(item) for item in result if isinstance(item, dict)}
        index_by_key = {
            _recovery_match_key(item): item
            for item in result
            if isinstance(item, dict) and str(item.get("currency") or "") == "人民币"
        }
        for block in _date_card_blocks(text) or _active_card_sentence_blocks(text):
            record = parse_credit_card_account_block(block)
            if not _is_rmb_active_card(record):
                continue
            match_key = _recovery_match_key(record)
            if match_key in index_by_key:
                existing = index_by_key[match_key]
                before = (existing.get("credit_limit"), existing.get("used_amount"), existing.get("account_status"))
                _merge_recovered_card(existing, record)
                after = (existing.get("credit_limit"), existing.get("used_amount"), existing.get("account_status"))
                if before != after:
                    logger.info(
                        "[PersonalCredit][CreditCard][RECOVERY_UPDATE] issuer=%s tail_no=%s credit_limit=%s used_amount=%s",
                        existing.get("issuer") or existing.get("institution"),
                        existing.get("card_tail_no") or "未识别",
                        existing.get("credit_limit"),
                        existing.get("used_amount") or existing.get("used_limit"),
                    )
                continue
            signature = _dedupe_signature(record)
            if signature in seen:
                continue
            seen.add(signature)
            index_by_key[match_key] = record
            logger.info(
                "[PersonalCredit][CreditCard][RECOVERY_APPEND] issuer=%s currency=%s tail_no=%s credit_limit=%s used_amount=%s",
                record.get("issuer") or record.get("institution"),
                record.get("currency"),
                record.get("card_tail_no") or "未识别",
                record.get("credit_limit"),
                record.get("used_amount") or record.get("used_limit"),
            )
            result.append(ensure_record_fields(record, CREDIT_CARD_ACCOUNT_FIELDS))
        return result
    except Exception:
        return records


def _is_abnormal_account(block: str, record: dict[str, Any]) -> bool:
    combined = " ".join(str(item or "") for item in (
        record.get("account_status"),
        record.get("overdue_amount"),
        record.get("overdue_months"),
        record.get("history_performance"),
    ))
    if "信贷记录概要" not in block and "信用卡账户数" not in block and "贷款账户数" not in block:
        combined = f"{combined} {block}"
    if any(word in combined for word in ABNORMAL_WORDS):
        return True
    return _amount_to_number(record.get("overdue_amount")) > 0


def is_closed_credit_card_account(record: dict[str, Any], evidence_text: str) -> bool:
    combined = " ".join(str(item or "") for item in (
        record.get("account_status"),
        record.get("status"),
        evidence_text,
        record.get("raw_text"),
        record.get("history_performance"),
    ))
    return "未销户" not in combined and any(word in combined for word in CLOSED_STATUS_WORDS)


def _should_skip_closed_card(block: str, record: dict[str, Any]) -> bool:
    if not is_closed_credit_card_account(record, block):
        return False
    return not _is_abnormal_account(block, record)


def _should_skip_inactive_card(block: str, record: dict[str, Any]) -> bool:
    if _is_abnormal_account(block, record):
        return False
    if _should_skip_closed_card(block, record):
        return True
    if _looks_like_active_card(block) or record.get("report_cutoff"):
        return False
    status_text = " ".join(str(item or "") for item in (record.get("account_status"), block))
    if any(word in status_text for word in ACTIVE_STATUS_WORDS):
        return False
    used_value = _amount_to_number(record.get("used_limit") or record.get("used_amount"))
    if used_value > 0:
        return False
    if not record.get("account_status") or str(record.get("account_status")) in {"未识别", "未知", "不详"}:
        return True
    return False


def _candidate_blocks(text: str) -> list[str]:
    source = str(text or "")
    date_blocks = _date_card_blocks(source)
    sentence_blocks = _active_card_sentence_blocks(source)
    blocks = split_numbered_blocks(source)
    if not blocks:
        blocks = re.split(r"(?=(?:\d+[\.、)]\s*)?(?:[\u4e00-\u9fffA-Za-z0-9（）()·]{2,50})?(?:准贷记卡|贷记卡|信用卡))", source)
    expanded: list[str] = []
    for block in blocks:
        pieces = re.split(r"(?=\s*\d+[\.、)]\s*(?:19|20)\d{2}年\d{1,2}月\d{1,2}日)", block)
        expanded.extend(piece for piece in pieces if piece.strip())
    blocks = [*date_blocks, *sentence_blocks, *(expanded or blocks)]
    result: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        cleaned = block.strip()
        signature = _normalize_block(cleaned)
        if cleaned and signature not in seen and _looks_like_card(cleaned):
            seen.add(signature)
            result.append(cleaned)
    return result


def parse_credit_card_account_block(block: str) -> dict[str, Any]:
    normalized_block = _normalize_block(block)
    source = normalized_block
    institution = _extract_institution(source)
    card_type = _extract_card_type(source)
    credit_limit = _extract_amount_after_label(source, ("信用额度", "授信额度", "共享授信额度")) or _extract_money(source, ("授信额度", "信用额度", "共享授信额度"))
    used_limit = _extract_amount_after_label(source, ("已使用额度", "已用额度", "使用额度", "透支余额", "已用授信额度")) or _extract_money(source, ("已使用额度", "已用额度", "使用额度", "透支余额", "已用授信额度"))
    latest_date = _extract_date(source, ("最近一次还款日期", "最近还款日期", "最近一次还款", "最近还款"))
    latest_amount = _extract_money(source, ("最近一次还款金额", "最近还款金额"))
    status = _extract_status(source)
    is_closed = is_closed_credit_card_account({"account_status": status}, source)
    return {
        "account_no": _extract_account_no(source),
        "open_date": _extract_open_date(source),
        "institution": institution,
        "issuer": institution,
        "card_type": card_type,
        "account_type": card_type,
        "currency": _extract_currency(source),
        "card_tail_no": _extract_card_tail_no(source),
        "account_status": status,
        "is_closed": is_closed,
        "credit_limit": credit_limit,
        "used_limit": used_limit,
        "used_amount": used_limit,
        "report_cutoff": _extract_report_cutoff(source),
        "overdue_amount": _extract_money(source, ("当前逾期金额", "逾期金额")),
        "overdue_months": _extract_overdue_months(source),
        "latest_repayment_date": latest_date,
        "latest_repayment_amount": latest_amount,
        "last_repayment": " ".join(item for item in (latest_date, latest_amount) if item),
        "history_performance": _extract_label(source, ("历史表现", "还款表现", "还款记录"), max_chars=160),
        "information_report_date": _extract_date(source, ("信息报告日期", "报送日期")),
        "evidence": normalized_block[:1000],
        "evidence_text": normalized_block[:1000],
    }


def extract_credit_card_accounts(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        explicit_text = str(sections.get("credit_card_accounts") or "").strip()
        source_text = "\n".join(
            str(sections.get(key) or "")
            for key in ("full_text", "credit_transaction_details", "credit_card_accounts")
            if sections.get(key)
        )
        if explicit_text:
            text = _extract_credit_card_window(explicit_text)
        else:
            text = _extract_credit_card_window(source_text)
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, ...]] = set()
        for index, block in enumerate(_candidate_blocks(text), start=1):
            logger.info("[PersonalCredit][CreditCard][CANDIDATE] index=%s raw_start=%s", index, _normalize_block(block)[:300])
            try:
                record = parse_credit_card_account_block(block)
            except Exception as exc:
                logger.info("[PersonalCredit][CreditCard][PARSE_FAIL] index=%s reason=%s raw_start=%s", index, exc, _normalize_block(block)[:300])
                continue
            logger.info(
                "[PersonalCredit][CreditCard][PARSE_OK] index=%s issuer=%s currency=%s tail_no=%s credit_limit=%s used_amount=%s cutoff=%s",
                index,
                record.get("issuer") or record.get("institution"),
                record.get("currency"),
                record.get("card_tail_no"),
                record.get("credit_limit"),
                record.get("used_amount"),
                record.get("report_cutoff"),
            )
            if _should_skip_inactive_card(block, record):
                reason = "closed_or_inactive"
                if _should_skip_closed_card(block, record):
                    reason = "closed_without_abnormal"
                logger.info(
                    "[PersonalCredit][CreditCard][FILTER_DROP] index=%s reason=%s issuer=%s currency=%s tail_no=%s",
                    index,
                    reason,
                    record.get("issuer") or record.get("institution"),
                    record.get("currency"),
                    record.get("card_tail_no"),
                )
                continue
            if any(value for key, value in record.items() if key not in {"evidence", "evidence_text"}):
                signature = _dedupe_signature(record)
                if signature in seen:
                    logger.info("[PersonalCredit][CreditCard][DEDUP_DROP] index=%s reason=duplicate key=%s", index, signature)
                    continue
                seen.add(signature)
                logger.info(
                    "[PersonalCredit][CreditCard][DISPLAY_KEEP] index=%s issuer=%s currency=%s tail_no=%s",
                    index,
                    record.get("issuer") or record.get("institution"),
                    record.get("currency"),
                    record.get("card_tail_no"),
                )
                records.append(ensure_record_fields(record, CREDIT_CARD_ACCOUNT_FIELDS))
        if not records and _expected_active_credit_card_count(source_text) > 0:
            fallback_window = _extract_credit_card_window(source_text)
            for index, block in enumerate(_active_card_sentence_blocks(fallback_window), start=1):
                logger.info("[PersonalCredit][CreditCard][CANDIDATE] index=fallback-%s raw_start=%s", index, _normalize_block(block)[:300])
                try:
                    record = parse_credit_card_account_block(block)
                except Exception as exc:
                    logger.info("[PersonalCredit][CreditCard][PARSE_FAIL] index=fallback-%s reason=%s raw_start=%s", index, exc, _normalize_block(block)[:300])
                    continue
                logger.info(
                    "[PersonalCredit][CreditCard][PARSE_OK] index=fallback-%s issuer=%s currency=%s tail_no=%s credit_limit=%s used_amount=%s cutoff=%s",
                    index,
                    record.get("issuer") or record.get("institution"),
                    record.get("currency"),
                    record.get("card_tail_no"),
                    record.get("credit_limit"),
                    record.get("used_amount"),
                    record.get("report_cutoff"),
                )
                if not _looks_like_active_card(block) or _should_skip_closed_card(block, record):
                    logger.info(
                        "[PersonalCredit][CreditCard][FILTER_DROP] index=fallback-%s reason=not_active_or_closed issuer=%s currency=%s tail_no=%s",
                        index,
                        record.get("issuer") or record.get("institution"),
                        record.get("currency"),
                        record.get("card_tail_no"),
                    )
                    continue
                signature = _dedupe_signature(record)
                if signature in seen:
                    logger.info("[PersonalCredit][CreditCard][DEDUP_DROP] index=fallback-%s reason=duplicate key=%s", index, signature)
                    continue
                seen.add(signature)
                logger.info(
                    "[PersonalCredit][CreditCard][DISPLAY_KEEP] index=fallback-%s issuer=%s currency=%s tail_no=%s",
                    index,
                    record.get("issuer") or record.get("institution"),
                    record.get("currency"),
                    record.get("card_tail_no"),
                )
                records.append(ensure_record_fields(record, CREDIT_CARD_ACCOUNT_FIELDS))
        rmb_records = [record for record in records if str(record.get("currency") or "") == "人民币"]
        logger.info("[PersonalCredit][CreditCard][RMB_DISPLAY_COUNT]=%s", len(rmb_records))
        for record in rmb_records:
            logger.info(
                "[PersonalCredit][CreditCard][RMB_DISPLAY] %s | %s | %s | %s | %s",
                record.get("issuer") or record.get("institution"),
                record.get("card_tail_no") or "未识别",
                record.get("credit_limit"),
                record.get("used_amount") or record.get("used_limit"),
                record.get("report_cutoff"),
            )
        return records
    except Exception:
        return []
