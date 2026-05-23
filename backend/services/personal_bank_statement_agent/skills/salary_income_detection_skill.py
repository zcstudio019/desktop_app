from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from math import sqrt
from typing import Any

from ..normalizer import normalize_text, round2


STRONG_SALARY_KEYWORDS = (
    "工资",
    "代发工资",
    "工资发放",
    "薪资",
    "薪酬",
    "工薪",
    "工资收入",
    "工资款",
    "发工资",
    "月工资",
    "基本工资",
    "绩效工资",
    "奖金",
    "绩效奖金",
    "年终奖",
    "补贴",
    "津贴",
    "代发工资入账",
)

SUSPECTED_SALARY_KEYWORDS = (
    "代发款项",
    "代发",
    "批量代发",
    "企业代发",
    "单位代发",
    "代发入账",
    "代发业务",
    "批量转账",
    "对公代发",
    "银联代付",
    "网联收款",
    "代付入账",
    "转账",
    "网银转账",
    "企业网银转账",
    "对公转账",
    "收入",
    "入账",
    "汇款",
    "电子汇入",
    "跨行汇入",
    "普通汇款",
    "实时代发",
    "批量入账",
)

EXCLUDE_SALARY_KEYWORDS = (
    "报销",
    "借款",
    "还款",
    "往来款",
    "备用金",
    "差旅费",
    "货款",
    "劳务费",
    "服务费",
    "分红",
    "投资收益",
    "退款",
    "贷款",
    "理财",
    "基金",
    "证券",
    "信用卡还款",
    "转存",
    "赎回",
    "保险",
    "代付",
    "代收",
)

EMPLOYER_COUNTERPARTY_KEYWORDS = (
    "有限公司",
    "有限责任公司",
    "股份有限公司",
    "集团",
    "公司",
    "科技",
    "软件",
    "信息",
    "网络",
    "工厂",
    "厂",
    "商贸",
    "贸易",
    "实业",
    "工程",
    "建筑",
    "劳务",
    "人力资源",
    "财务",
    "银行股份有限公司",
    "银行代发",
    "代发工资专户",
)

EXCLUDE_CATEGORY_MAP = {
    "报销": "reimbursement_or_advance_income",
    "差旅费": "reimbursement_or_advance_income",
    "备用金": "reimbursement_or_advance_income",
    "借款": "borrowing_or_transfer_income",
    "往来款": "borrowing_or_transfer_income",
    "还款": "borrowing_or_transfer_income",
    "货款": "operating_income",
    "服务费": "operating_income",
    "分红": "investment_income",
    "投资收益": "investment_income",
    "退款": "refund",
}


def _matched(text: str, keywords: tuple[str, ...]) -> list[str]:
    return [keyword for keyword in keywords if keyword in text]


def _employer_like(name: Any) -> bool:
    text = normalize_text(name)
    return bool(text and _matched(text, EMPLOYER_COUNTERPARTY_KEYWORDS))


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "")[:10]
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def _month_index(date: datetime) -> int:
    return date.year * 12 + date.month


def _longest_consecutive_months(dates: list[datetime]) -> int:
    months = sorted({_month_index(date) for date in dates})
    if not months:
        return 0
    best = current = 1
    for prev, item in zip(months, months[1:]):
        current = current + 1 if item == prev + 1 else 1
        best = max(best, current)
    return best


def _periodic_payment(dates: list[datetime]) -> bool:
    if len(dates) < 2:
        return False
    days = sorted(date.day for date in dates)
    median = days[len(days) // 2]
    return sum(1 for day in days if abs(day - median) <= 3) >= max(2, int(len(days) * 0.7))


def _amount_stable(amounts: list[float]) -> bool:
    amounts = [amount for amount in amounts if amount > 0]
    if len(amounts) < 2:
        return False
    max_amount = max(amounts)
    min_amount = min(amounts)
    if max_amount and min_amount / max_amount >= 0.7:
        return True
    avg = sum(amounts) / len(amounts)
    if not avg:
        return False
    variance = sum((amount - avg) ** 2 for amount in amounts) / len(amounts)
    return sqrt(variance) / avg <= 0.3


def _group_key(tx: dict[str, Any], matched_keywords: list[str]) -> str:
    counterparty = normalize_text(tx.get("counterparty_name"))
    if counterparty:
        return f"counterparty:{counterparty}"
    if any(keyword in matched_keywords for keyword in STRONG_SALARY_KEYWORDS):
        return "summary:strong_salary"
    return f"summary:{(matched_keywords[0] if matched_keywords else normalize_text(tx.get('summary')))}"


def _exclude_category(exclude_keywords: list[str]) -> str:
    for keyword in exclude_keywords:
        if keyword in EXCLUDE_CATEGORY_MAP:
            return EXCLUDE_CATEGORY_MAP[keyword]
    return "non_salary_income"


def detect_salary_income(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    income_transactions: list[dict[str, Any]] = []
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for tx in transactions:
        credit = float(tx.get("credit_amount") or 0)
        if tx.get("direction") != "income" or credit <= 0:
            continue
        text = normalize_text(f"{tx.get('summary')} {tx.get('counterparty_name')}")
        summary_text = normalize_text(tx.get("summary"))
        strong = _matched(summary_text, STRONG_SALARY_KEYWORDS)
        suspected = _matched(summary_text, SUSPECTED_SALARY_KEYWORDS)
        exclude = _matched(text, EXCLUDE_SALARY_KEYWORDS)
        detection = {
            "salary_type": "unknown",
            "confidence": 0.0,
            "matched_keywords": strong or suspected,
            "exclude_keywords": exclude,
            "employer_like_counterparty": _employer_like(tx.get("counterparty_name")),
            "periodic_payment": False,
            "amount_stable": False,
            "continuous_months": 0,
            "evidence": "",
        }
        tx["salary_detection"] = detection
        tx["need_manual_review"] = False
        tx["salary_exclusion_category"] = _exclude_category(exclude) if exclude else ""
        income_transactions.append(tx)
        if strong or suspected:
            groups[_group_key(tx, strong or suspected)].append(tx)

    for items in groups.values():
        dates = [date for date in (_parse_date(item.get("transaction_date")) for item in items) if date]
        amounts = [float(item.get("credit_amount") or 0) for item in items]
        continuous_months = _longest_consecutive_months(dates)
        periodic = _periodic_payment(dates)
        stable = _amount_stable(amounts)
        for item in items:
            detection = item["salary_detection"]
            detection["continuous_months"] = continuous_months
            detection["periodic_payment"] = periodic
            detection["amount_stable"] = stable

    for tx in income_transactions:
        detection = tx["salary_detection"]
        strong = [item for item in detection["matched_keywords"] if item in STRONG_SALARY_KEYWORDS]
        suspected = [item for item in detection["matched_keywords"] if item in SUSPECTED_SALARY_KEYWORDS]
        exclude = detection["exclude_keywords"]
        employer_like = bool(detection["employer_like_counterparty"])
        continuous = int(detection["continuous_months"] or 0)
        periodic = bool(detection["periodic_payment"])
        stable = bool(detection["amount_stable"])

        if exclude:
            detection["salary_type"] = "non_salary"
            detection["confidence"] = 0.9
            detection["evidence"] = f"命中排除关键词：{'、'.join(exclude)}，不认定为工资"
            continue

        if strong:
            confidence = 0.82 + (0.08 if employer_like else 0) + (0.05 if continuous >= 3 else 0) + (0.03 if stable else 0)
            detection["salary_type"] = "confirmed_salary"
            detection["confidence"] = round(min(confidence, 0.98), 2)
            detection["evidence"] = f"摘要命中强工资关键词：{'、'.join(strong)}"
            continue

        score = sum([bool(suspected), employer_like, continuous >= 2, periodic, stable])
        if suspected and employer_like:
            if continuous >= 6 and stable:
                confidence = 0.85
            elif continuous >= 3:
                confidence = 0.75
            elif continuous >= 2:
                confidence = 0.65
            else:
                confidence = 0.6
            confidence += 0.04 if periodic else 0
            confidence += 0.03 if stable else 0
            detection["salary_type"] = "suspected_salary"
            detection["confidence"] = round(min(confidence, 0.9), 2)
            detection["evidence"] = "摘要命中疑似代发类关键词，付款方为公司/单位主体，识别为疑似工资收入，需人工核实"
            tx["need_manual_review"] = True
            continue
        if suspected and score >= 3:
            confidence = 0.45 + (0.15 if employer_like else 0) + (0.12 if continuous >= 3 else 0.08 if continuous >= 2 else 0) + (0.1 if periodic else 0) + (0.1 if stable else 0)
            detection["salary_type"] = "suspected_salary"
            detection["confidence"] = round(min(confidence, 0.82), 2)
            detection["evidence"] = "疑似代发/转账收入结合付款方、周期或金额稳定性，需人工核实"
            tx["need_manual_review"] = True
            continue

        detection["salary_type"] = "non_salary" if suspected else "unknown"
        detection["confidence"] = 0.55 if suspected else 0.0
        detection["evidence"] = "缺少明确工资关键词、单位付款方或稳定发放规律，不能认定为工资"

    confirmed = [tx for tx in income_transactions if tx.get("salary_detection", {}).get("salary_type") == "confirmed_salary"]
    suspected_txs = [tx for tx in income_transactions if tx.get("salary_detection", {}).get("salary_type") == "suspected_salary"]
    confirmed_amount = sum(float(tx.get("credit_amount") or 0) for tx in confirmed)
    suspected_amount = sum(float(tx.get("credit_amount") or 0) for tx in suspected_txs)
    salary_months = len({str(tx.get("transaction_date") or "")[:7] for tx in confirmed if str(tx.get("transaction_date") or "")[:7]})
    suspected_months = len({str(tx.get("transaction_date") or "")[:7] for tx in suspected_txs if str(tx.get("transaction_date") or "")[:7]})
    continuity_basis = confirmed if confirmed else suspected_txs
    confirmed_dates = [date for date in (_parse_date(tx.get("transaction_date")) for tx in continuity_basis) if date]
    confirmed_amounts = [float(tx.get("credit_amount") or 0) for tx in continuity_basis]
    continuity_months = _longest_consecutive_months(confirmed_dates)
    stable = _amount_stable(confirmed_amounts)
    if continuity_months >= 6 and stable:
        continuity_level = "strong"
    elif continuity_months >= 3:
        continuity_level = "medium"
    elif continuity_months > 0:
        continuity_level = "weak"
    else:
        continuity_level = "none"
    salary_sources = []
    by_source: dict[str, dict[str, Any]] = defaultdict(lambda: {"counterparty_name": "", "amount": 0.0, "count": 0, "months": set(), "salary_type": ""})
    for tx in confirmed + suspected_txs:
        name = normalize_text(tx.get("counterparty_name")) or "未知付款方"
        target = by_source[name]
        target["counterparty_name"] = name
        target["amount"] += float(tx.get("credit_amount") or 0)
        target["count"] += 1
        target["months"].add(str(tx.get("transaction_date") or "")[:7])
        tx_salary_type = tx.get("salary_detection", {}).get("salary_type")
        if tx_salary_type == "confirmed_salary":
            target["salary_type"] = "confirmed_salary"
        elif not target["salary_type"]:
            target["salary_type"] = "suspected_salary"
    for item in by_source.values():
        salary_sources.append({
            "counterparty_name": item["counterparty_name"],
            "amount": round2(item["amount"]),
            "count": item["count"],
            "months": sorted(month for month in item["months"] if month),
            "salary_type": item["salary_type"],
        })
    salary_sources.sort(key=lambda item: float(item.get("amount") or 0), reverse=True)
    notes: list[str] = []
    if not confirmed_amount:
        notes.append("未识别到明确工资收入")
    if suspected_amount:
        top_source = salary_sources[0]["counterparty_name"] if salary_sources else "疑似单位付款方"
        notes.append(f"摘要为代发类款项，付款方为{top_source}等公司主体，连续多月出现，识别为疑似工资收入，需人工核实是否为工资")
    if any(
        normalize_text(tx.get("summary")) in {"汇款汇入", "转账", "转账收入"} and not normalize_text(tx.get("counterparty_name"))
        for tx in income_transactions
    ):
        notes.append("汇款汇入/转账且缺少付款方时，不认定为工资")
    confidence_basis = confirmed if confirmed else suspected_txs
    confidence_values = [float(tx.get("salary_detection", {}).get("confidence") or 0) for tx in confidence_basis]
    return {
        "confirmed_salary_income": round2(confirmed_amount),
        "suspected_salary_income": round2(suspected_amount),
        "verified_salary_income": round2(confirmed_amount),
        "salary_income_count": len(confirmed),
        "suspected_salary_count": len(suspected_txs),
        "salary_months": salary_months or suspected_months,
        "salary_avg_monthly_amount": round2(confirmed_amount / salary_months) if salary_months else 0.0,
        "salary_continuity_level": continuity_level,
        "salary_confidence": round2(sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0,
        "salary_sources": salary_sources[:10],
        "salary_detection_notes": notes,
    }
