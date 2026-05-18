from __future__ import annotations

import logging
import re
from typing import Any

from .schema import (
    CREDIT_CARD_ACCOUNT_FIELDS,
    GUARANTEE_FIELDS,
    LOAN_ACCOUNT_FIELDS,
    NON_CREDIT_TRANSACTION_FIELDS,
    OVERDUE_RECORD_FIELDS,
    PUBLIC_RECORD_FIELDS,
    QUERY_RECORD_FIELDS,
    RELATED_REPAYMENT_RESPONSIBILITY_FIELDS,
    clone_default_report_json,
    default_basic_info,
    default_credit_summary,
    default_query_statistics,
    ensure_record_fields,
)

logger = logging.getLogger(__name__)

LIST_FIELDS = (
    "loan_accounts",
    "credit_card_accounts",
    "related_repayment_responsibilities",
    "guarantees",
    "non_credit_transactions",
    "overdue_records",
    "public_records",
    "query_records",
    "risk_flags",
    "missing_fields",
    "warnings",
)

RECORD_FIELDS_BY_LIST = {
    "loan_accounts": LOAN_ACCOUNT_FIELDS,
    "credit_card_accounts": CREDIT_CARD_ACCOUNT_FIELDS,
    "related_repayment_responsibilities": RELATED_REPAYMENT_RESPONSIBILITY_FIELDS,
    "guarantees": GUARANTEE_FIELDS,
    "non_credit_transactions": NON_CREDIT_TRANSACTION_FIELDS,
    "overdue_records": OVERDUE_RECORD_FIELDS,
    "public_records": PUBLIC_RECORD_FIELDS,
    "query_records": QUERY_RECORD_FIELDS,
}

AMOUNT_KEYS = {
    "issued_amount",
    "balance",
    "credit_limit",
    "used_amount",
    "overdue_amount",
    "guarantee_amount",
    "guarantee_balance",
    "amount",
    "used_limit",
    "latest_repayment_amount",
    "responsibility_amount",
    "loan_balance",
}

LOAN_CLOSED_STATUS_WORDS = ("已结清", "结清", "已关闭", "关闭")
CARD_CLOSED_STATUS_WORDS = ("销户", "已销户", "注销", "已注销")
CARD_CLOSED_EVIDENCE_WORDS = ("销户", "已销户", "注销", "已注销", "关闭", "已关闭")
CARD_FOREIGN_CURRENCY_WORDS = ("美元", "USD", "usd", "外币", "欧元", "港币", "日元", "英镑")
CARD_RMB_CURRENCY_WORDS = ("人民币", "RMB", "rmb", "CNY", "cny")
ABNORMAL_WORDS = ("逾期", "呆账", "代偿", "核销", "强制执行", "90天以上逾期")
ABNORMAL_FIVE_CATEGORY_WORDS = ("关注", "次级", "可疑", "损失")
NEGATIVE_ABNORMAL_PHRASES = ("当前无逾期", "无逾期", "未发生逾期", "没有逾期")
POLLUTED_LOAN_INSTITUTION_KEYWORDS = ("查询记录", "查询记录明细", "机构查询", "本人查询", "相关还款责任", "担保信息", "公共记录")
POLLUTED_LOAN_EVIDENCE_KEYWORDS = (
    "为企业相关还款责任",
    "为个人相关还款责任",
    "相关还款责任信息",
    "承担相关还款责任",
    "责任人类型",
    "保证合同编号",
    "保证人",
    "共同借款人",
    "查询记录明细",
    "查询日期",
    "查询机构",
    "查询原因",
    "贷款审批",
    "信用卡审批",
    "贷后管理",
)

ID_CARD_PATTERN = re.compile(
    r"(?<!\d)([1-9]\d{5}(?:(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]|\d{9}))(?!\d)"
)
MARRIAGE_VALUES = ("未婚", "已婚", "离异", "丧偶")

SUMMARY_ALIASES = {
    "active_credit_card_account_count": ("credit_card_active_count",),
    "credit_card_overdue_account_count": ("credit_card_overdue_count",),
    "credit_card_90d_overdue_account_count": ("credit_card_90d_overdue_count",),
}


def _warn_once(report: dict[str, Any], warning: str) -> None:
    warnings = report.setdefault("warnings", [])
    if isinstance(warnings, list) and warning not in warnings:
        warnings.append(warning)


def _clean_scalar(value: Any, *, is_amount: bool = False) -> Any:
    if value is None:
        return "" if is_amount else value
    if isinstance(value, str):
        text = re.sub(r"[ \t\u3000]+", " ", value).strip()
        if is_amount:
            text = re.sub(r"\s+", "", text)
        return text
    return value


def _clean_ocr_wrapped_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<=[\u4e00-\u9fff])\n(?=[\u4e00-\u9fff])", "", text)
    text = re.sub(r"(?<=[A-Za-z0-9])\n(?=[A-Za-z0-9])", "", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"股份有\s*限公司", "股份有限公司", text)
    text = re.sub(r"有限公\s*司", "有限公司", text)
    text = re.sub(r"支\s*行", "支行", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_record(record: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(record, dict):
        record = {}
    normalized = ensure_record_fields(record, fields)
    for key, value in list(normalized.items()):
        normalized[key] = _clean_scalar(value, is_amount=key in AMOUNT_KEYS)
        if key in {"related_party", "institution", "contract_no", "loan_balance", "responsibility_amount"}:
            normalized[key] = _clean_ocr_wrapped_scalar(normalized[key])
    return normalized


def _amount_number(value: Any) -> float:
    text = re.sub(r"[,\s，人民币元]", "", str(value or ""))
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return 0.0
    number = float(match.group(1))
    if "万" in str(value or ""):
        number *= 10000
    return number


def _record_has_abnormal(record: dict[str, Any]) -> bool:
    combined = " ".join(
        str(value or "")
        for key, value in record.items()
        if key not in {"evidence", "evidence_text"}
    )
    for phrase in NEGATIVE_ABNORMAL_PHRASES:
        combined = combined.replace(phrase, "")
    if any(word in combined for word in ABNORMAL_WORDS):
        return True
    if any(word in str(record.get("five_category") or "") for word in ABNORMAL_FIVE_CATEGORY_WORDS):
        return True
    return _amount_number(record.get("overdue_amount")) > 0


def is_polluted_loan_account(record: dict[str, Any], evidence_text: str = "") -> bool:
    institution = str(record.get("institution") or "").strip()
    evidence = str(evidence_text or record.get("evidence") or record.get("evidence_text") or "")
    account_no = str(record.get("account_no") or "").strip()
    if any(keyword in institution for keyword in POLLUTED_LOAN_INSTITUTION_KEYWORDS):
        return True
    if any(keyword in evidence for keyword in POLLUTED_LOAN_EVIDENCE_KEYWORDS):
        return True
    if account_no.upper().startswith("D") and "保证合同编号" in evidence:
        return True
    if "相关还款责任金额" in evidence or "承担相关还款责任" in evidence:
        return True
    meaningful_keys = ("account_no", "institution", "business_type", "open_date", "due_date", "amount", "balance", "account_status", "five_category", "overdue_amount")
    meaningful_count = sum(1 for key in meaningful_keys if str(record.get(key) or "").strip() and str(record.get(key) or "").strip() != "未识别")
    if bool(record.get("balance") or record.get("amount")) and meaningful_count <= 4:
        own_loan_hints = ("发放的", "发放贷款", "贷款授信", "余额为", "当前无逾期", "五级分类")
        if not any(keyword in evidence for keyword in own_loan_hints):
            return True
    return False


def _keep_loan_record(record: dict[str, Any]) -> bool:
    if _record_has_abnormal(record):
        return True
    status = str(record.get("account_status") or "")
    if any(word in status for word in LOAN_CLOSED_STATUS_WORDS):
        return False
    balance = record.get("balance")
    if balance not in (None, "") and _amount_number(balance) <= 0:
        return False
    if not status and not balance:
        return False
    return True


def _credit_card_combined_text(record: dict[str, Any]) -> str:
    return " ".join(
        str(record.get(key) or "")
        for key in (
            "currency",
            "account_status",
            "status",
            "evidence",
            "evidence_text",
            "raw_text",
            "card_description",
            "history_performance",
        )
    )


def _is_foreign_currency_credit_card(record: dict[str, Any]) -> bool:
    combined = _credit_card_combined_text(record)
    return any(word in combined for word in CARD_FOREIGN_CURRENCY_WORDS)


def _is_rmb_credit_card(record: dict[str, Any]) -> bool:
    combined = _credit_card_combined_text(record)
    return any(word in combined for word in CARD_RMB_CURRENCY_WORDS)


def is_displayable_credit_card_account(record: dict[str, Any]) -> bool:
    """Return whether a credit-card record belongs in the user-facing detail list."""
    combined = _credit_card_combined_text(record)
    if _is_foreign_currency_credit_card(record):
        return False
    is_closed = bool(record.get("is_closed")) or (
        "未销户" not in combined and any(word in combined for word in CARD_CLOSED_EVIDENCE_WORDS)
    )
    if is_closed:
        return _record_has_abnormal(record)
    if _is_rmb_credit_card(record):
        return True
    return not is_closed


def _keep_card_record(record: dict[str, Any]) -> bool:
    if not is_displayable_credit_card_account(record):
        logger.info(
            "[PersonalCredit][CreditCard][FILTER_DROP] reason=not_displayable issuer=%s currency=%s tail_no=%s",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
        )
        return False
    if _record_has_abnormal(record):
        logger.info(
            "[PersonalCredit][CreditCard][DISPLAY_KEEP] issuer=%s currency=%s tail_no=%s reason=abnormal",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
        )
        return True
    status = str(record.get("account_status") or "")
    evidence = str(record.get("evidence") or record.get("evidence_text") or record.get("raw_text") or record.get("history_performance") or "")
    closed_text = f"{status} {evidence}"
    if "未销户" not in closed_text and any(word in closed_text for word in CARD_CLOSED_EVIDENCE_WORDS):
        return False
    if record.get("is_closed") is True:
        return False
    if "当前有效" in status:
        logger.info(
            "[PersonalCredit][CreditCard][DISPLAY_KEEP] issuer=%s currency=%s tail_no=%s",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
        )
        return True
    if record.get("report_cutoff") and record.get("credit_limit") and (record.get("used_limit") or record.get("used_amount")) not in (None, ""):
        logger.info(
            "[PersonalCredit][CreditCard][DISPLAY_KEEP] issuer=%s currency=%s tail_no=%s reason=active_limit",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
        )
        return True
    used = record.get("used_limit") or record.get("used_amount")
    if any(word in evidence for word in ("贷款", "五级分类", "消费贷款", "购房贷款")) and _amount_number(used) <= 0 and not record.get("credit_limit"):
        return False
    if not status and _amount_number(used) <= 0:
        return False
    return True


def _dedupe_related_repayment(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    seen_contract_records: dict[str, list[dict[str, Any]]] = {}
    for item in records:
        contract_no = str(item.get("contract_no") or "").strip()
        if contract_no:
            signature = (
                "contract",
                contract_no,
                str(item.get("start_date") or "").strip(),
                str(item.get("responsibility_amount") or "").strip(),
                str(item.get("loan_balance") or "").strip(),
                str(item.get("as_of_date") or "").strip(),
            )
        else:
            signature = (
                "fallback",
                str(item.get("start_date") or "").strip(),
                str(item.get("related_party") or "").strip(),
                str(item.get("institution") or "").strip(),
                str(item.get("responsibility_amount") or "").strip(),
                str(item.get("as_of_date") or "").strip(),
                str(item.get("loan_balance") or "").strip(),
            )
        if signature in seen:
            logger.info(
                "[PersonalCredit][RelatedRepayment][DEDUP_DROP] source=normalizer reason=duplicate key=%s raw=%s",
                signature,
                str(item.get("evidence") or "")[:300],
            )
            continue
        if contract_no and contract_no in seen_contract_records:
            for previous in seen_contract_records[contract_no]:
                message = "合同编号与其他记录重复，但起始日期或贷款余额不同，已保留待核验"
                previous["_duplicate_contract_no_warning"] = True
                item["_duplicate_contract_no_warning"] = True
                previous["duplicate_contract_no_warning"] = True
                item["duplicate_contract_no_warning"] = True
                previous["warning"] = message
                item["warning"] = message
                logger.info(
                    "[PersonalCredit][RelatedRepayment][KEEP_DUP_CONTRACT] source=normalizer contract_no=%s start_dates=%s,%s balances=%s,%s",
                    contract_no,
                    previous.get("start_date"),
                    item.get("start_date"),
                    previous.get("loan_balance"),
                    item.get("loan_balance"),
                )
        seen.add(signature)
        if contract_no:
            seen_contract_records.setdefault(contract_no, []).append(item)
        result.append(item)
    return result


def _clean_id_number(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    text = re.split(r"[:：]", text)[-1] if "证件号码" in text else text
    match = ID_CARD_PATTERN.search(text)
    return match.group(1).upper() if match else ""


def _split_report_number_time(report_number: Any, report_time: Any) -> tuple[str, str]:
    number = _clean_scalar(report_number) or ""
    time = _clean_scalar(report_time) or ""
    if "报告时间" in number:
        parts = re.split(r"报告时间\s*[:：]?", number, maxsplit=1)
        number = re.sub(r"报告编号\s*[:：]?", "", parts[0]).strip()
        if len(parts) > 1 and not time:
            time = parts[1].strip()
    number_match = re.search(r"([A-Za-z0-9\-]{6,80})", str(number))
    cleaned_number = number_match.group(1) if number_match else str(number).strip()
    time_match = re.search(r"((?:19|20)\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}(?:日)?(?:\s+\d{1,2}:\d{1,2}:\d{1,2})?)", str(time))
    cleaned_time = time_match.group(1) if time_match else str(time).strip()
    return cleaned_number, cleaned_time


def _clean_marital_status(value: Any) -> str:
    text = str(value or "")
    for item in MARRIAGE_VALUES:
        if item in text:
            return item
    return ""


def _summary_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    text = str(value or "").strip()
    if text in {"--", "——", "-", "未显示", "0 / 未显示", "0 / 未显示为有效"}:
        return 0
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _summary_sum(*values: Any) -> str:
    numbers = [_summary_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


def _normalize_credit_summary(summary: dict[str, Any]) -> dict[str, Any]:
    normalized = {**default_credit_summary(), **summary}
    for target, aliases in SUMMARY_ALIASES.items():
        if normalized.get(target) in (None, ""):
            for alias in aliases:
                if summary.get(alias) not in (None, ""):
                    normalized[target] = summary.get(alias)
                    break
    for key in (
        "housing_loan_account_count",
        "other_loan_account_count",
        "housing_loan_outstanding_count",
        "other_loan_outstanding_count",
    ):
        if normalized.get(key) in {"--", "——", "-", "未显示"}:
            normalized[key] = "0 / 未显示"
    if normalized.get("loan_account_count") in (None, ""):
        normalized["loan_account_count"] = _summary_sum(
            normalized.get("housing_loan_account_count"),
            normalized.get("other_loan_account_count"),
        ) or None
    if normalized.get("outstanding_loan_account_count") in (None, ""):
        normalized["outstanding_loan_account_count"] = _summary_sum(
            normalized.get("housing_loan_outstanding_count"),
            normalized.get("other_loan_outstanding_count"),
        ) or None
    if normalized.get("loan_overdue_account_count") in (None, ""):
        normalized["loan_overdue_account_count"] = _summary_sum(
            summary.get("housing_loan_overdue_count"),
            summary.get("other_loan_overdue_count"),
        ) or None
    for key, value in list(normalized.items()):
        normalized[key] = value if isinstance(value, int) or value is None else _clean_scalar(value)
    return normalized


def normalize_report_json(report: dict[str, Any] | None) -> dict[str, Any]:
    normalized = clone_default_report_json()
    if isinstance(report, dict):
        normalized.update({key: value for key, value in report.items() if key not in {"basic_info", "credit_summary"}})
        basic = report.get("basic_info") if isinstance(report.get("basic_info"), dict) else {}
        summary = report.get("credit_summary") if isinstance(report.get("credit_summary"), dict) else {}
        normalized["basic_info"] = {**default_basic_info(), **basic}
        normalized["credit_summary"] = _normalize_credit_summary(summary)

    for key, value in list(normalized["basic_info"].items()):
        normalized["basic_info"][key] = _clean_scalar(value) or ""
    report_number, report_time = _split_report_number_time(
        normalized["basic_info"].get("report_number"),
        normalized["basic_info"].get("report_time"),
    )
    normalized["basic_info"]["report_number"] = report_number
    normalized["basic_info"]["report_time"] = report_time
    cleaned_id_number = _clean_id_number(normalized["basic_info"].get("id_number"))
    if normalized["basic_info"].get("id_number") and not cleaned_id_number:
        _warn_once(normalized, "证件号码未识别或格式异常")
    normalized["basic_info"]["id_number"] = cleaned_id_number
    normalized["basic_info"]["marital_status"] = _clean_marital_status(normalized["basic_info"].get("marital_status"))
    normalized["credit_summary"] = _normalize_credit_summary(normalized["credit_summary"])

    for field in LIST_FIELDS:
        value = normalized.get(field)
        if not isinstance(value, list):
            value = []
        record_fields = RECORD_FIELDS_BY_LIST.get(field)
        if record_fields:
            records = [_normalize_record(item, record_fields) for item in value]
            if field == "loan_accounts":
                filtered_records: list[dict[str, Any]] = []
                filtered_count = 0
                for item in records:
                    if is_polluted_loan_account(item):
                        filtered_count += 1
                        account_no = str(item.get("account_no") or "").strip()
                        suffix = f"：{account_no}" if account_no else ""
                        _warn_once(normalized, f"已过滤疑似相关还款责任/查询记录污染的贷款账户{suffix}")
                        continue
                    if _keep_loan_record(item):
                        filtered_records.append(item)
                if filtered_count:
                    _warn_once(normalized, f"已过滤疑似非本人贷款账户/相关还款责任污染记录 {filtered_count} 条")
                records = filtered_records
            elif field == "credit_card_accounts":
                records = [item for item in records if _keep_card_record(item)]
            elif field == "related_repayment_responsibilities":
                records = _dedupe_related_repayment(records)
            normalized[field] = records
        else:
            normalized[field] = [item for item in value if item is not None]

    normalized["report_type"] = "personal_credit_report"
    raw_query_statistics = normalized.get("query_statistics")
    default_statistics = default_query_statistics()
    if isinstance(raw_query_statistics, dict):
        for group in ("institution_query", "personal_query"):
            group_value = raw_query_statistics.get(group)
            if isinstance(group_value, dict):
                for key in ("last_1_month", "last_3_months", "last_6_months"):
                    try:
                        default_statistics[group][key] = int(group_value.get(key) or 0)
                    except Exception:
                        default_statistics[group][key] = 0
        if isinstance(raw_query_statistics.get("warnings"), list):
            normalized["warnings"] = [*list(normalized.get("warnings") or []), *raw_query_statistics["warnings"]]
    normalized["query_statistics"] = default_statistics
    if not isinstance(normalized.get("personal_credit_indicators"), dict):
        normalized["personal_credit_indicators"] = {}
    return normalized
