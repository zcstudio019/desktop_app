from __future__ import annotations

import re
from typing import Any

from .schema import default_credit_summary


FIELD_KEYWORDS: dict[str, tuple[str, ...]] = {
    "credit_card_account_count": ("贷记卡账户", "信用卡账户", "贷记卡"),
    "credit_card_active_count": ("贷记卡未销户", "信用卡未销户", "贷记卡正常"),
    "credit_card_overdue_count": ("贷记卡发生过逾期", "信用卡发生过逾期", "贷记卡逾期"),
    "credit_card_90d_overdue_count": ("贷记卡发生过90天以上逾期", "信用卡发生过90天以上逾期", "90天以上逾期"),
    "housing_loan_account_count": ("购房贷款账户", "住房贷款账户"),
    "housing_loan_outstanding_count": ("购房贷款未结清", "住房贷款未结清"),
    "housing_loan_overdue_count": ("购房贷款发生过逾期", "住房贷款发生过逾期"),
    "other_loan_account_count": ("其他贷款账户",),
    "other_loan_outstanding_count": ("其他贷款未结清",),
    "other_loan_overdue_count": ("其他贷款发生过逾期",),
    "other_business_account_count": ("其他业务账户", "其他业务"),
    "guarantee_count": ("担保", "对外担保"),
}


def _extract_near_count(text: str, keywords: tuple[str, ...]) -> int | None:
    for keyword in keywords:
        pattern = rf"{re.escape(keyword)}[^\n\r\d]{{0,30}}(\d+)\s*(?:个|笔|户|张|条)?"
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
        pattern = rf"(\d+)\s*(?:个|笔|户|张|条)?[^\n\r]{{0,20}}{re.escape(keyword)}"
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def extract_credit_summary(sections: dict[str, Any]) -> dict[str, Any]:
    try:
        text = "\n".join(
            str(sections.get(key) or "")
            for key in ("credit_summary", "full_text")
        )
        result = default_credit_summary()
        for field, keywords in FIELD_KEYWORDS.items():
            result[field] = _extract_near_count(text, keywords)
        return result
    except Exception:
        return default_credit_summary()
