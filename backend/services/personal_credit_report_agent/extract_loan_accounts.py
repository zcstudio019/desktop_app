from __future__ import annotations

import re
from typing import Any

from .evidence import clean_amount, clean_value, first_match, split_numbered_blocks, value_after_label
from .schema import LOAN_ACCOUNT_FIELDS, ensure_record_fields

LOAN_TYPES = ("购房贷款", "住房贷款", "经营性贷款", "经营贷款", "消费贷款", "汽车贷款", "其他贷款", "贷款")
STATUS_WORDS = ("未结清", "已结清", "正常", "逾期", "结清", "关闭", "销户")
FIVE_CATEGORY_WORDS = ("正常", "关注", "次级", "可疑", "损失")


def _normalize_block(block: str) -> str:
    text = str(block or "").replace("\r", "\n")
    text = re.sub(r"[ \t\u3000]+", " ", text)
    text = re.sub(r"\n+", " ", text)
    return clean_value(text)


def _looks_like_loan(block: str) -> bool:
    return any(keyword in block for keyword in LOAN_TYPES)


def _extract_label(block: str, labels: tuple[str, ...], *, max_chars: int = 120) -> str:
    value = value_after_label(block, labels, max_chars=max_chars)
    if value:
        return value
    compact = _normalize_block(block)
    for label in labels:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*([^,，;；。|｜]{{1,{max_chars}}})"
        match = re.search(pattern, compact)
        if match:
            return clean_value(match.group(1))
    return ""


def _extract_institution(block: str) -> str:
    return (
        _extract_label(block, ("机构", "贷款机构", "发放机构", "授信机构", "管理机构"), max_chars=80)
        or first_match(block, (r"([\u4e00-\u9fffA-Za-z0-9（）()·]{2,50}(?:银行|小额贷款|消费金融|汽车金融|信托|财务公司|信用社|金融公司)[\u4e00-\u9fffA-Za-z0-9（）()·]{0,30})",))
    )


def _extract_business_type(block: str) -> str:
    direct = _extract_label(block, ("业务类型", "贷款类型", "业务种类", "账户类型"), max_chars=60)
    if direct:
        for item in LOAN_TYPES:
            if item in direct:
                return item
        return direct
    return first_match(block, (r"(购房贷款|住房贷款|经营性贷款|经营贷款|消费贷款|汽车贷款|其他贷款|贷款)",))


def _extract_date(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=40)
    match = re.search(r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?", labeled or "")
    if match:
        return clean_value(match.group(0))
    return ""


def _extract_money(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=80)
    if labeled:
        money = first_match(labeled, (r"((?:人民币)?\s*[0-9][0-9,]*(?:\.\d+)?\s*(?:万?元|万元)?)",))
        return clean_amount(money or labeled)
    return ""


def _extract_status(block: str) -> str:
    labeled = _extract_label(block, ("账户状态", "状态", "当前状态"), max_chars=40)
    for word in STATUS_WORDS:
        if word in labeled:
            return word
    for word in STATUS_WORDS:
        if word in block:
            return word
    return clean_value(labeled)


def _extract_five_category(block: str) -> str:
    labeled = _extract_label(block, ("五级分类", "分类"), max_chars=40)
    for word in FIVE_CATEGORY_WORDS:
        if word in labeled:
            return word
    for word in FIVE_CATEGORY_WORDS:
        if f"五级分类{word}" in block or f"分类{word}" in block:
            return word
    return clean_value(labeled)


def _extract_overdue_months(block: str) -> str:
    labeled = _extract_label(block, ("逾期月份", "逾期月数", "逾期期数", "逾期信息"), max_chars=80)
    source = labeled or block
    match = re.search(r"逾期\s*(\d+)\s*(?:个)?月", source)
    if match:
        return match.group(1)
    match = re.search(r"(\d+)\s*(?:个)?月", labeled)
    return match.group(1) if match else ""


def _extract_account_no(block: str) -> str:
    value = _extract_label(block, ("账户编号", "账户号", "账号", "合同编号"), max_chars=80)
    if value:
        match = re.search(r"([A-Za-z0-9\-*]{4,40})", value)
        return match.group(1) if match else value
    return first_match(block, (r"(?:账户编号|账户号|账号|合同编号)\s*[:：]?\s*([A-Za-z0-9\-*]{4,40})",))


def _candidate_blocks(text: str) -> list[str]:
    explicit = str(text or "")
    blocks = split_numbered_blocks(explicit)
    if not blocks:
        blocks = re.split(r"(?=(?:\d+[\.、)]\s*)?(?:[\u4e00-\u9fffA-Za-z0-9（）()·]{2,50})?(?:购房贷款|住房贷款|经营性贷款|经营贷款|消费贷款|汽车贷款|其他贷款))", explicit)
    return [block.strip() for block in blocks if block and _looks_like_loan(block)]


def extract_loan_accounts(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = "\n".join(str(sections.get(key) or "") for key in ("loan_accounts", "credit_transaction_details"))
        records: list[dict[str, Any]] = []
        for block in _candidate_blocks(text):
            normalized_block = _normalize_block(block)
            amount = _extract_money(block, ("发放金额", "借款金额", "贷款金额", "授信金额", "金额"))
            latest_date = _extract_date(block, ("最近一次还款日期", "最近还款日期", "最近一次还款", "最近还款"))
            latest_amount = _extract_money(block, ("最近一次还款金额", "最近还款金额"))
            overdue_amount = _extract_money(block, ("当前逾期金额", "逾期金额"))
            record = {
                "account_no": _extract_account_no(block),
                "institution": _extract_institution(block),
                "business_type": _extract_business_type(block),
                "open_date": _extract_date(block, ("发放日期", "开户日期", "开立日期", "起始日期")),
                "due_date": _extract_date(block, ("到期日期", "结清日期", "结束日期")),
                "amount": amount,
                "issued_amount": amount,
                "balance": _extract_money(block, ("余额", "本金余额", "贷款余额")),
                "account_status": _extract_status(block),
                "five_category": _extract_five_category(block),
                "overdue_amount": overdue_amount,
                "overdue_months": _extract_overdue_months(block),
                "latest_repayment_date": latest_date,
                "latest_repayment_amount": latest_amount,
                "overdue_info": _extract_label(block, ("逾期信息", "逾期记录"), max_chars=120),
                "last_repayment": " ".join(item for item in (latest_date, latest_amount) if item),
                "history_performance": _extract_label(block, ("历史表现", "还款表现", "还款记录"), max_chars=160),
                "information_report_date": _extract_date(block, ("信息报告日期", "报送日期")),
                "evidence": normalized_block[:1000],
                "evidence_text": normalized_block[:1000],
            }
            if any(value for key, value in record.items() if key not in {"evidence", "evidence_text"}):
                records.append(ensure_record_fields(record, LOAN_ACCOUNT_FIELDS))
        return records
    except Exception:
        return []
