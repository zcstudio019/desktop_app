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
    "special_installment_balance",
    "overdue_amount",
    "guarantee_amount",
    "guarantee_balance",
    "amount",
    "used_limit",
    "latest_repayment_amount",
    "responsibility_amount",
    "loan_balance",
    "loan_amount",
}

LOAN_CLOSED_STATUS_WORDS = ("已结清", "结清", "已关闭", "关闭")
CARD_CLOSED_STATUS_WORDS = ("销户", "已销户", "注销", "已注销")
CARD_CLOSED_EVIDENCE_WORDS = ("销户", "已销户", "注销", "已注销", "关闭", "已关闭")
CARD_FOREIGN_CURRENCY_WORDS = ("美元", "USD", "usd", "外币", "欧元", "港币", "日元", "英镑", "澳大利亚元", "加元", "瑞士法郎", "新加坡元", "新西兰元", "香港元")
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
        if key in {"related_party", "institution", "contract_no", "loan_balance", "responsibility_amount", "business_type", "balance_type", "balance"}:
            normalized[key] = _clean_ocr_wrapped_scalar(normalized[key])
    if fields == RELATED_REPAYMENT_RESPONSIBILITY_FIELDS:
        if not normalized.get("balance") and normalized.get("loan_balance"):
            normalized["balance"] = normalized.get("loan_balance")
        if not normalized.get("loan_balance") and normalized.get("balance"):
            normalized["loan_balance"] = normalized.get("balance")
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
    if "未结清" not in status and any(word in status for word in LOAN_CLOSED_STATUS_WORDS):
        return False
    evidence = str(record.get("evidence") or record.get("evidence_text") or "")
    if "授信" in evidence and record.get("due_date"):
        return True
    if status in {"当前有效", "未结清", "正常"} and record.get("due_date"):
        return True
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
    if "尚未激活" in combined:
        return False
    if _is_rmb_credit_card(record):
        return True
    return not is_closed


def _keep_card_record(record: dict[str, Any]) -> bool:
    if not is_displayable_credit_card_account(record):
        reason = "foreign_currency" if _is_foreign_currency_credit_card(record) else "not_displayable"
        if any(word in _credit_card_combined_text(record) for word in CARD_CLOSED_EVIDENCE_WORDS):
            reason = "closed"
        if "尚未激活" in _credit_card_combined_text(record):
            reason = "not_activated"
        logger.info(
            "[PersonalCredit][CreditCard][FILTER_DROP] reason=%s issuer=%s currency=%s tail_no=%s",
            reason,
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
            "[PersonalCredit][CreditCard][DISPLAY_KEEP] issuer=%s currency=%s tail_no=%s card_type=%s credit_limit=%s balance=%s",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
            record.get("card_type"),
            record.get("credit_limit"),
            record.get("balance"),
        )
        return True
    if record.get("report_cutoff") and record.get("credit_limit"):
        logger.info(
            "[PersonalCredit][CreditCard][DISPLAY_KEEP] issuer=%s currency=%s tail_no=%s card_type=%s credit_limit=%s balance=%s reason=active_limit",
            record.get("issuer") or record.get("institution"),
            record.get("currency"),
            record.get("card_tail_no"),
            record.get("card_type"),
            record.get("credit_limit"),
            record.get("balance"),
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
    if text in SUMMARY_ZERO_VALUES:
        return 0
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else None


def _summary_sum(*values: Any) -> str:
    numbers = [_summary_int(value) for value in values]
    numbers = [number for number in numbers if number is not None]
    return str(sum(numbers)) if numbers else ""


SUMMARY_ZERO_VALUES = {"", "--", "——", "-", "—", "未显示", "0 / 未显示", "0 / 未显示为有效"}
SUMMARY_DIAGNOSTIC_FIELDS = (
    "credit_card_90d_overdue_account_count",
    "loan_90d_overdue_account_count",
    "personal_related_repayment_responsibility_account_count",
    "enterprise_related_repayment_responsibility_account_count",
)


def _normalize_summary_quantity(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, int):
        return str(value)
    cleaned = _clean_scalar(value)
    return "0" if str(cleaned or "").strip() in SUMMARY_ZERO_VALUES else str(cleaned).strip()


def _summary_token_value(token: Any) -> str:
    text = str(token or "").strip()
    return "0" if text in SUMMARY_ZERO_VALUES else text


def _summary_matrix_tokens(region: str, max_tokens: int) -> list[str]:
    text = re.sub(r"[\r\n\t\u3000]+", " ", str(region or ""))
    for marker in (
        "逾期记录可能影响对您的信用评价",
        "购房贷款,包括",
        "购房贷款，包括",
        "发生过逾期的信用卡账户",
        "指曾经",
        "透支超过",
        "超过60天",
        "60天",
    ):
        marker_index = text.find(marker)
        if marker_index >= 0:
            ignored_number = re.search(r"(?<!\d)(\d{1,3})(?!\d)", text[marker_index:])
            if ignored_number:
                logger.info(
                    "[PersonalCredit][Summary][IGNORE_EXPLANATION_NUMBER] value=%s reason=right_side_explanation",
                    ignored_number.group(1),
                )
            text = text[:marker_index]
            break
    text = text[:100]
    tokens: list[str] = []
    for match in re.finditer(r"(--|——|—|-|(?<!\d)\d{1,3}(?!\d))", text):
        tokens.append(match.group(1).strip())
        if len(tokens) >= max_tokens:
            break
    return tokens


def _has_personal_enterprise_header(source_text: str, row_start: int) -> bool:
    prefix = re.sub(r"\s+", " ", str(source_text or "")[max(0, row_start - 120):row_start])
    return "为个人" in prefix and "为企业" in prefix


def _parse_related_responsibility_values(source_text: str) -> tuple[str, str] | None:
    compact = re.sub(r"\s+", " ", str(source_text or ""))
    direct = re.search(
        r"为个人\s*(--|——|—|-)?\s*为企业\s*(--|——|—|-|(?<!\d)\d{1,3}(?!\d))",
        compact,
    )
    if direct and direct.group(2):
        personal_raw = direct.group(1) or "--"
        enterprise_raw = direct.group(2)
        personal = _summary_token_value(personal_raw)
        enterprise = _summary_token_value(enterprise_raw)
        logger.info(
            "[PersonalCredit][Summary][RELATED_RESPONSIBILITY_MATRIX] personal_raw=%s enterprise_raw=%s personal=%s enterprise=%s",
            personal_raw,
            enterprise_raw,
            personal,
            enterprise,
        )
        return personal, enterprise

    row_match = None
    for candidate in re.finditer(r"相关还款责任账户数", compact):
        prefix = compact[max(0, candidate.start() - 4):candidate.start()]
        if prefix.endswith("为个人") or prefix.endswith("为企业"):
            continue
        row_match = candidate
        break
    if not row_match:
        return None
    region = _summary_row_region(compact, r"相关还款责任账户数")
    tokens = _summary_matrix_tokens(region, 2)
    if not tokens:
        return None
    has_header = _has_personal_enterprise_header(compact, row_match.start())
    if has_header and len(tokens) == 1 and re.fullmatch(r"\d{1,3}", tokens[0]):
        personal_raw = "--"
        enterprise_raw = tokens[0]
    elif has_header and len(tokens) >= 2 and re.fullmatch(r"\d{1,3}", tokens[0]) and tokens[1] == "60" and re.search(r"(?:超过|透支超过)?\s*60\s*天", region):
        personal_raw = "--"
        enterprise_raw = tokens[0]
    else:
        personal_raw = tokens[0]
        enterprise_raw = tokens[1] if len(tokens) >= 2 else ""
    personal = _summary_token_value(personal_raw)
    enterprise = _summary_token_value(enterprise_raw)
    logger.info(
        "[PersonalCredit][Summary][RELATED_RESPONSIBILITY_MATRIX] personal_raw=%s enterprise_raw=%s personal=%s enterprise=%s",
        personal_raw,
        enterprise_raw,
        personal,
        enterprise,
    )
    return personal, enterprise


def _summary_row_region(source_text: str, row_pattern: str) -> str:
    text = re.sub(r"\s+", " ", str(source_text or ""))
    match = re.search(row_pattern, text)
    if not match:
        return ""
    next_start = len(text)
    for pattern in (
        r"账户数",
        r"未结清\s*/\s*未销户账户数",
        r"发生过逾期的账户数",
        r"发生过\s*90\s*天以上逾期的账户数",
        r"相关还款责任账户数",
        r"为个人\s+为企业",
    ):
        for next_match in re.finditer(pattern, text[match.end():]):
            absolute_start = match.end() + next_match.start()
            if absolute_start > match.start() and absolute_start < next_start:
                next_start = absolute_start
    return text[match.end():next_start]


def apply_credit_summary_matrix_corrections(summary: dict[str, Any], raw_text: str = "") -> dict[str, Any]:
    """Correct personal-credit summary fields from the original matrix rows."""
    if not isinstance(summary, dict):
        return {}
    corrected = dict(summary)
    source_text = str(raw_text or corrected.get("_summary_source_text") or "")
    if not source_text:
        return corrected

    overdue_region = _summary_row_region(source_text, r"发生过\s*90\s*天以上逾期的账户数")
    overdue_tokens = _summary_matrix_tokens(overdue_region, 4)
    if overdue_tokens:
        logger.info("[PersonalCredit][Summary][MATRIX_ROW] row=发生过90天以上逾期的账户数 tokens=%s", overdue_tokens)
        old_credit_card_90d = corrected.get("credit_card_90d_overdue_account_count")
        credit_card_90d = _summary_token_value(overdue_tokens[0])
        corrected["credit_card_90d_overdue_account_count"] = credit_card_90d
        if str(old_credit_card_90d or "") != credit_card_90d:
            reason = "right_side_explanation_60" if str(old_credit_card_90d) == "60" else "matrix_90d_row"
            logger.info(
                "[PersonalCredit][Summary][FINAL_CORRECTION] credit_card_90d %s -> %s reason=%s",
                old_credit_card_90d,
                credit_card_90d,
                reason,
            )
        if len(overdue_tokens) >= 3:
            old_loan_90d = corrected.get("loan_90d_overdue_account_count")
            loan_90d = _summary_token_value(overdue_tokens[2])
            corrected["loan_90d_overdue_account_count"] = loan_90d
            if str(old_loan_90d or "") != loan_90d:
                logger.info(
                    "[PersonalCredit][Summary][FINAL_CORRECTION] loan_90d %s -> %s reason=matrix_90d_row",
                    old_loan_90d,
                    loan_90d,
                )

    responsibility_values = _parse_related_responsibility_values(source_text)
    if responsibility_values:
        personal_value, enterprise_value = responsibility_values
        old_personal = corrected.get("personal_related_repayment_responsibility_account_count")
        old_enterprise = corrected.get("enterprise_related_repayment_responsibility_account_count")
        corrected["personal_related_repayment_responsibility_account_count"] = personal_value
        corrected["enterprise_related_repayment_responsibility_account_count"] = enterprise_value
        if str(old_personal or "") != personal_value:
            logger.info(
                "[PersonalCredit][Summary][FINAL_CORRECTION] personal_related %s -> %s reason=matrix_personal_dash",
                old_personal,
                personal_value,
            )
        if str(old_enterprise or "") != enterprise_value:
            logger.info(
                "[PersonalCredit][Summary][FINAL_CORRECTION] enterprise_related %s -> %s reason=matrix_enterprise_%s",
                old_enterprise,
                enterprise_value,
                enterprise_value,
            )
        logger.info("[PersonalCredit][Summary][RELATED_RESPONSIBILITY] personal=%s enterprise=%s", personal_value, enterprise_value)
        return corrected
    return corrected


def _apply_summary_matrix_sanity(summary: dict[str, Any]) -> None:
    corrected = apply_credit_summary_matrix_corrections(summary)
    summary.update(corrected)


def _normalize_credit_summary(summary: dict[str, Any]) -> dict[str, Any]:
    normalized = {**default_credit_summary(), **summary}
    for target, aliases in SUMMARY_ALIASES.items():
        if normalized.get(target) in (None, ""):
            for alias in aliases:
                if summary.get(alias) not in (None, ""):
                    normalized[target] = summary.get(alias)
                    break
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
    _apply_summary_matrix_sanity(normalized)
    for key, value in list(normalized.items()):
        if key.startswith("_"):
            normalized[key] = value
        else:
            normalized[key] = _normalize_summary_quantity(value)
    logger.info(
        "[PersonalCredit][Summary][AFTER_NORMALIZE] credit_card_90d=%s loan_90d=%s personal_related=%s enterprise_related=%s",
        normalized.get("credit_card_90d_overdue_account_count"),
        normalized.get("loan_90d_overdue_account_count"),
        normalized.get("personal_related_repayment_responsibility_account_count"),
        normalized.get("enterprise_related_repayment_responsibility_account_count"),
    )
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
