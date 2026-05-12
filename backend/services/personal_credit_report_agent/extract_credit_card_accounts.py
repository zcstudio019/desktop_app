from __future__ import annotations

import re
from typing import Any

from .evidence import clean_amount, clean_value, first_match, split_numbered_blocks, value_after_label
from .schema import CREDIT_CARD_ACCOUNT_FIELDS, ensure_record_fields

CARD_TYPES = ("准贷记卡", "贷记卡", "信用卡")
STATUS_WORDS = ("未销户", "已销户", "正常", "逾期", "冻结", "止付", "呆账", "销户")


def _normalize_block(block: str) -> str:
    text = str(block or "").replace("\r", "\n")
    text = re.sub(r"[ \t\u3000]+", " ", text)
    text = re.sub(r"\n+", " ", text)
    return clean_value(text)


def _looks_like_card(block: str) -> bool:
    return any(keyword in block for keyword in (*CARD_TYPES, "授信额度", "已用额度", "共享授信额度", "最近一次还款", "当前逾期"))


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


def _extract_date(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=50)
    match = re.search(r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?", labeled or "")
    return clean_value(match.group(0)) if match else ""


def _extract_institution(block: str) -> str:
    return (
        _extract_label(block, ("发卡机构", "机构", "授信机构", "管理机构"), max_chars=80)
        or first_match(block, (r"([\u4e00-\u9fffA-Za-z0-9（）()·]{2,50}(?:银行|信用社|金融公司|消费金融)[\u4e00-\u9fffA-Za-z0-9（）()·]{0,30})",))
    )


def _extract_card_type(block: str) -> str:
    labeled = _extract_label(block, ("账户类型", "卡类型", "业务类型"), max_chars=40)
    for item in CARD_TYPES:
        if item in labeled or item in block:
            return item
    return clean_value(labeled)


def _extract_status(block: str) -> str:
    labeled = _extract_label(block, ("账户状态", "状态", "当前状态"), max_chars=50)
    for word in STATUS_WORDS:
        if word in labeled:
            return word
    for word in STATUS_WORDS:
        if word in block:
            return word
    return clean_value(labeled)


def _extract_account_no(block: str) -> str:
    value = _extract_label(block, ("账户编号", "账户号", "卡号", "账号"), max_chars=80)
    if value:
        match = re.search(r"([A-Za-z0-9\-*]{4,40})", value)
        return match.group(1) if match else value
    return first_match(block, (r"(?:账户编号|账户号|卡号|账号)\s*[:：]?\s*([A-Za-z0-9\-*]{4,40})",))


def _extract_overdue_months(block: str) -> str:
    labeled = _extract_label(block, ("逾期月份", "逾期月数", "逾期期数", "当前逾期"), max_chars=80)
    source = labeled or block
    match = re.search(r"逾期\s*(\d+)\s*(?:个)?月", source)
    if match:
        return match.group(1)
    match = re.search(r"(\d+)\s*(?:个)?月", labeled)
    return match.group(1) if match else ""


def _candidate_blocks(text: str) -> list[str]:
    source = str(text or "")
    blocks = split_numbered_blocks(source)
    if not blocks:
        blocks = re.split(r"(?=(?:\d+[\.、)]\s*)?(?:[\u4e00-\u9fffA-Za-z0-9（）()·]{2,50})?(?:准贷记卡|贷记卡|信用卡))", source)
    return [block.strip() for block in blocks if block and _looks_like_card(block)]


def extract_credit_card_accounts(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = "\n".join(str(sections.get(key) or "") for key in ("credit_card_accounts", "credit_transaction_details"))
        records: list[dict[str, Any]] = []
        for block in _candidate_blocks(text):
            normalized_block = _normalize_block(block)
            institution = _extract_institution(block)
            card_type = _extract_card_type(block)
            used_limit = _extract_money(block, ("已用额度", "使用额度", "透支余额", "已用授信额度", "余额"))
            latest_date = _extract_date(block, ("最近一次还款日期", "最近还款日期", "最近一次还款", "最近还款"))
            latest_amount = _extract_money(block, ("最近一次还款金额", "最近还款金额"))
            record = {
                "account_no": _extract_account_no(block),
                "institution": institution,
                "issuer": institution,
                "card_type": card_type,
                "account_type": card_type,
                "currency": _extract_label(block, ("币种", "账户币种"), max_chars=20) or ("人民币" if "人民币" in block else ""),
                "account_status": _extract_status(block),
                "credit_limit": _extract_money(block, ("授信额度", "信用额度", "共享授信额度", "额度")),
                "used_limit": used_limit,
                "used_amount": used_limit,
                "overdue_amount": _extract_money(block, ("当前逾期金额", "逾期金额")),
                "overdue_months": _extract_overdue_months(block),
                "latest_repayment_date": latest_date,
                "latest_repayment_amount": latest_amount,
                "last_repayment": " ".join(item for item in (latest_date, latest_amount) if item),
                "history_performance": _extract_label(block, ("历史表现", "还款表现", "还款记录"), max_chars=160),
                "information_report_date": _extract_date(block, ("信息报告日期", "报送日期")),
                "evidence": normalized_block[:1000],
                "evidence_text": normalized_block[:1000],
            }
            if any(value for key, value in record.items() if key not in {"evidence", "evidence_text"}):
                records.append(ensure_record_fields(record, CREDIT_CARD_ACCOUNT_FIELDS))
        return records
    except Exception:
        return []
