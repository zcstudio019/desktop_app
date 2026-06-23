from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.extraction_skills.bank_statement import (
    _normalize_entity_name,
    _related_person_map_from_metadata,
    normalize_person_name,
    validate_account_name,
)


UNKNOWN = "未识别"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _payload(extraction: dict[str, Any]) -> dict[str, Any]:
    data = _dict(extraction.get("extracted_data"))
    payload = data.get("extracted_json") or data.get("data") or data
    if not payload:
        payload = extraction.get("extracted_json") or extraction.get("data") or extraction
    return _dict(payload)


def _money_value(value: Any) -> Decimal | None:
    if value in (None, "", UNKNOWN, "无法计算"):
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def _money(value: Any, *, unavailable: bool = False) -> str:
    if unavailable:
        return "无法计算"
    amount = _money_value(value)
    return f"{amount:,.2f}" if amount is not None else UNKNOWN


def _date(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", text)
    if not match:
        return ""
    try:
        dt = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return ""
    if dt.year < 2000 or dt.year > datetime.now().year + 1:
        return ""
    return dt.strftime("%Y-%m-%d")


def _month(value: Any) -> str:
    date_text = _date(value)
    if not date_text:
        return ""
    month = date_text[:7]
    if not _valid_year_month(month):
        return ""
    return month


def _valid_year_month(value: Any) -> bool:
    text = str(value or "")
    match = re.fullmatch(r"(20\d{2})-(\d{2})", text)
    if not match:
        return False
    year, month = int(match.group(1)), int(match.group(2))
    return 2000 <= year <= datetime.now().year + 1 and 1 <= month <= 12


def _norm_name(value: Any) -> str:
    return _normalize_entity_name(value)


def _tx_value(tx: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if tx.get(key) not in (None, ""):
            return tx.get(key)
    return ""


def _clean_account_name(value: Any) -> str:
    name = str(value or "").strip()
    ok, _reason = validate_account_name(name)
    return name if ok else ""


def _statement_subtype_label(subtype: str) -> str:
    return {
        "account_statement": "标准账户流水",
        "receipt_bundle": "疑似银行回单集合",
        "unknown_bank_statement": "非标准银行流水文件",
    }.get(subtype or "", "未识别银行文件")


def _quality_status_label(status: str, subtype: str = "") -> str:
    if subtype == "receipt_bundle":
        return "回单集合，未形成账户流水"
    return {
        "success": "已形成标准流水明细",
        "partial": "待人工复核",
        "failed": "解析失败",
        "invalid_account_info": "账户信息无效",
        "invalid_transaction_structure": "未形成标准流水明细",
    }.get(status or "", "待复核")


def _status_problem(payload: dict[str, Any], account_name_valid: bool, transactions_valid: bool) -> str:
    issues: list[str] = []
    if payload.get("statement_subtype") == "receipt_bundle":
        issues.append("回单集合，未纳入经营流水聚合")
    if payload.get("account_name") and not account_name_valid:
        issues.append("疑似将交易对手名称误识别为本方户名，已拦截")
    if not payload.get("account_no"):
        issues.append("未识别账号")
    if not payload.get("bank_name"):
        issues.append("未识别银行名称")
    if not transactions_valid:
        issues.append("未形成有效交易明细")
    return "；".join(issues) or "无"


def _transaction_structurally_valid(tx: dict[str, Any]) -> bool:
    if tx.get("is_page_block") or tx.get("invalid_reason") == "整页列块误合并为单条交易":
        return False
    trade_time = _tx_value(tx, "trade_time", "transaction_time", "交易时间")
    direction = _tx_value(tx, "direction", "收支方向")
    amount = _money_value(_tx_value(tx, "amount", "金额", "交易金额"))
    summary = _tx_value(tx, "summary", "摘要", "purpose", "用途")
    return bool(_date(trade_time) and (direction in {"入账", "出账"} or amount is not None) and (amount is not None or summary))


def _infer_quality(payload: dict[str, Any]) -> dict[str, Any]:
    account_name = _clean_account_name(payload.get("account_name"))
    account_name_valid = bool(account_name)
    account_info_valid = bool(account_name_valid and payload.get("account_no") and payload.get("bank_name"))
    txs = [_dict(tx) for tx in _list(payload.get("transactions")) if _transaction_structurally_valid(_dict(tx))]
    transactions_valid = bool(txs)
    amounts_valid = bool(transactions_valid and any(_money_value(_tx_value(tx, "amount", "金额", "交易金额")) is not None for tx in txs))

    if "account_info_valid" in payload:
        account_info_valid = bool(payload.get("account_info_valid")) and account_name_valid
    if "transactions_valid" in payload:
        transactions_valid = bool(payload.get("transactions_valid")) and bool(txs)
    if "amounts_valid" in payload:
        amounts_valid = bool(payload.get("amounts_valid")) and amounts_valid

    can_join_amount = bool(payload.get("can_join_amount_statistics", amounts_valid))
    can_join_effective = bool(payload.get("can_join_effective_flow_statistics", transactions_valid and amounts_valid))
    if payload.get("statement_subtype") == "receipt_bundle":
        can_join_amount = False
        can_join_effective = False

    status = str(payload.get("parse_quality_status") or "")
    if not transactions_valid and payload.get("statement_subtype") != "receipt_bundle":
        status = "invalid_transaction_structure"
    elif not status:
        if account_info_valid and transactions_valid and amounts_valid:
            status = "success"
        elif not transactions_valid:
            status = "invalid_transaction_structure"
        else:
            status = "partial"

    return {
        "account_name_clean": account_name,
        "account_name_valid": account_name_valid,
        "account_info_valid": account_info_valid,
        "transactions_valid": transactions_valid,
        "amounts_valid": amounts_valid,
        "can_join_amount_statistics": can_join_amount,
        "can_join_effective_flow_statistics": can_join_effective,
        "parse_quality_status": status,
        "valid_transactions": txs,
    }


def _canonical_tx(tx: dict[str, Any], statement: dict[str, Any], source_file: str) -> dict[str, Any]:
    amount = _money_value(_tx_value(tx, "amount", "金额", "交易金额"))
    balance = _money_value(_tx_value(tx, "balance", "余额"))
    direction = str(_tx_value(tx, "direction", "收支方向") or "")
    if direction not in {"入账", "出账"}:
        flag = str(_tx_value(tx, "debit_credit_flag", "借贷标志") or "")
        direction = "入账" if flag == "贷" else ("出账" if flag == "借" else "未识别")
    trade_time = str(_tx_value(tx, "trade_time", "transaction_time", "交易时间") or "")
    category = str(_tx_value(tx, "category", "交易分类") or "其他")
    canonical = {
        "source_file": source_file,
        "bank_name": statement.get("bank_name") or "",
        "account_no": statement.get("account_no") or "",
        "account_name": statement.get("account_name") or "",
        "opening_bank": statement.get("opening_bank") or "",
        "transaction_id": _tx_value(tx, "transaction_id", "id") or "",
        "serial_no": _tx_value(tx, "serial_no", "voucher_no", "凭证号", "交易流水号") or "",
        "trade_time": trade_time,
        "book_date": _tx_value(tx, "book_date", "记账日期") or _date(trade_time),
        "direction": direction,
        "amount": amount,
        "balance": balance,
        "counterparty_account": _tx_value(tx, "counterparty_account", "对方账号") or "",
        "counterparty_name": _tx_value(tx, "counterparty_name", "对方单位") or "",
        "summary": _tx_value(tx, "summary", "摘要") or "",
        "purpose": _tx_value(tx, "purpose", "用途") or "",
        "category": category,
        "can_join_effective_flow_statistics": bool(statement.get("can_join_effective_flow_statistics", True)),
        "is_self_transfer": bool(tx.get("is_self_transfer")),
        "is_related_person_transfer": bool(tx.get("is_related_person_transfer")),
        "is_bank_fee": bool(tx.get("is_bank_fee")),
        "is_tax_payment": bool(tx.get("is_tax_payment")),
        "is_salary_payment": bool(tx.get("is_salary_payment")),
        "is_loan_related": bool(tx.get("is_loan_related")),
        "is_interest_related": bool(tx.get("is_interest_related")),
        "exclude_from_effective_flow": bool(tx.get("exclude_from_effective_flow")),
        "exclude_reason": tx.get("exclude_reason") or "",
    }
    canonical["fingerprint"] = transaction_fingerprint(canonical)
    return canonical


def transaction_fingerprint(tx: dict[str, Any]) -> str:
    parts = [
        str(tx.get("account_no") or ""),
        str(tx.get("trade_time") or ""),
        str(tx.get("direction") or ""),
        str(_money_value(tx.get("amount")) or ""),
        str(_money_value(tx.get("balance")) or ""),
        str(tx.get("counterparty_account") or ""),
        str(tx.get("counterparty_name") or ""),
        str(tx.get("summary") or ""),
        str(tx.get("purpose") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _unified_category(tx: dict[str, Any]) -> str:
    text = f"{tx.get('summary') or ''} {tx.get('purpose') or ''} {tx.get('counterparty_name') or ''}"
    direction = tx.get("direction")
    if tx.get("is_internal_account_transfer") or tx.get("is_self_transfer"):
        return "内部账户划转"
    if tx.get("is_related_person_transfer"):
        return "关联人转账"
    if any(item in text for item in ("ETC", "车辆", "油卡")):
        return "ETC/车辆费用"
    if tx.get("is_bank_fee") or any(item in text for item in ("手续费", "短信费", "网银费", "电子银行")):
        return "银行费用"
    if tx.get("is_tax_payment") or any(item in text for item in ("缴税", "扣款（缴税）")):
        return "税费"
    if tx.get("is_salary_payment") or any(item in text for item in ("工资", "年终奖", "代发专用账户")):
        return "工资代发"
    if any(item in text for item in ("贷款发放",)):
        return "贷款发放"
    if tx.get("is_loan_related") or any(item in text for item in ("贷款归还", "还贷款", "融资还款", "融资租赁", "担保费")):
        return "贷款归还"
    if tx.get("is_interest_related") or "利息" in text:
        return "利息收入" if direction == "入账" else "利息支出"
    if any(item in text for item in ("往来款", "借款", "还借款", "归还借款")):
        return "往来入账" if direction == "入账" else ("往来出账" if direction == "出账" else "资金拆借")
    if any(item in text for item in ("工程款", "项目款", "材料款", "劳务款", "货款", "扶持资金", "工程款安装")):
        return "经营入账" if direction == "入账" else ("经营出账" if direction == "出账" else "其他")
    if any(item in text for item in ("电缆款", "桥架款", "风管", "灯具", "房租", "服务费", "咨询费", "快递费", "水费", "餐费", "报销")) and direction == "出账":
        return "经营出账"
    return str(tx.get("category") or "其他")


def _related_roles(customer_profile: dict[str, Any] | None, related_person_names: list[str] | None, related_person_roles: dict[str, str] | None) -> dict[str, dict[str, str]]:
    metadata = {"customer_profile": customer_profile or {}, "related_person_names": related_person_names or [], "related_person_roles": related_person_roles or {}}
    return _related_person_map_from_metadata(metadata)


def _description(tx: dict[str, Any]) -> str:
    return str(tx.get("purpose") or tx.get("summary") or "").strip()


def _aggregate_counterparties(transactions: list[dict[str, Any]], total_amount: Decimal) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for tx in transactions:
        name = str(tx.get("counterparty_name") or "").strip()
        if not name:
            continue
        item = groups.setdefault(name, {"name": name, "count": 0, "amount": Decimal("0"), "descriptions": Counter()})
        item["count"] += 1
        item["amount"] += _money_value(tx.get("amount")) or Decimal("0")
        desc = _description(tx)
        if desc:
            item["descriptions"][desc] += 1
    ranked = sorted(groups.values(), key=lambda item: (-item["amount"], -item["count"], item["name"]))
    result = []
    for index, item in enumerate(ranked, start=1):
        amount = item["amount"]
        result.append({
            "rank": index,
            "name": item["name"],
            "count": item["count"],
            "amount": amount,
            "ratio": float(amount / total_amount) if total_amount else 0.0,
            "main_purpose": "、".join(desc for desc, _ in item["descriptions"].most_common(3)),
        })
    return result


def _account_time_range(account: dict[str, Any]) -> str:
    start = account.get("period_start") or ""
    end = account.get("period_end") or ""
    if start and end:
        return f"{start} 至 {end}"
    return start or end or UNKNOWN


def aggregate_customer_bank_statements(
    extractions: list[dict[str, Any]],
    *,
    customer_id: str | int = "",
    customer_profile: dict[str, Any] | None = None,
    related_person_names: list[str] | None = None,
    related_person_roles: dict[str, str] | None = None,
) -> dict[str, Any]:
    roles = _related_roles(customer_profile, related_person_names, related_person_roles)
    customer_name = str((customer_profile or {}).get("customer_name") or (customer_profile or {}).get("name") or "")
    source_files: list[dict[str, Any]] = []
    file_quality: list[dict[str, Any]] = []
    included_payloads: list[dict[str, Any]] = []
    period_starts: list[str] = []
    period_ends: list[str] = []
    amount_status_counter = Counter()
    accounts: dict[str, dict[str, Any]] = {}
    raw_transactions: list[dict[str, Any]] = []

    for extraction in extractions:
        payload = _payload(extraction)
        if (payload.get("doc_type") or payload.get("document_type_code") or extraction.get("extraction_type")) != "bank_statement":
            continue
        source_file = str(extraction.get("file_name") or payload.get("source_file") or payload.get("file_name") or "")
        quality = _infer_quality(payload)
        valid_account_name = quality["account_name_clean"]
        bank_name = str(payload.get("bank_name") or "")
        account_no = str(payload.get("account_no") or "")
        subtype = str(payload.get("statement_subtype") or "unknown_bank_statement")
        transactions_valid = bool(quality["transactions_valid"])
        included = bool(transactions_valid and quality["valid_transactions"])
        if subtype == "receipt_bundle":
            included = False

        problem = _status_problem(payload, bool(valid_account_name), transactions_valid)
        quality_row = {
            "source_file": source_file,
            "bank_name": bank_name or UNKNOWN,
            "statement_subtype": subtype,
            "statement_subtype_label": _statement_subtype_label(subtype),
            "account_no": account_no or UNKNOWN,
            "account_name": valid_account_name or UNKNOWN,
            "parse_quality_status": quality["parse_quality_status"],
            "parse_quality_label": _quality_status_label(quality["parse_quality_status"], subtype),
            "included": included,
            "problem": problem,
        }
        file_quality.append(quality_row)
        source_files.append({"file_name": source_file, "status": quality["parse_quality_status"], "account_no": account_no})

        if not included:
            continue

        payload = {
            **payload,
            "account_name": valid_account_name,
            "can_join_effective_flow_statistics": quality["can_join_effective_flow_statistics"],
        }
        included_payloads.append(payload)
        start = _date(payload.get("period_start"))
        end = _date(payload.get("period_end"))
        if start:
            period_starts.append(start)
        if end:
            period_ends.append(end)
        amount_status_counter[str(payload.get("amount_recognition_status") or UNKNOWN)] += 1

        if account_no:
            account = accounts.setdefault(account_no, {
                "bank_name": bank_name,
                "opening_bank": payload.get("opening_bank") or "",
                "account_no": account_no,
                "account_name": valid_account_name or UNKNOWN,
                "period_start": start,
                "period_end": end,
                "file_count": 0,
                "transaction_count": 0,
                "amount_recognition_status": payload.get("amount_recognition_status") or UNKNOWN,
                "source_files": set(),
            })
            account["file_count"] += 1
            account["source_files"].add(source_file)
            account["transaction_count"] += len(quality["valid_transactions"])
            if start and (not account.get("period_start") or start < account["period_start"]):
                account["period_start"] = start
            if end and (not account.get("period_end") or end > account["period_end"]):
                account["period_end"] = end

        for tx in quality["valid_transactions"]:
            raw_transactions.append(_canonical_tx(tx, payload, source_file))

    own_account_nos = {str(item.get("account_no") or "") for item in accounts.values() if item.get("account_no")}
    own_account_names = {_norm_name(item.get("account_name")) for item in accounts.values() if item.get("account_name") and item.get("account_name") != UNKNOWN}
    base_company_names = {name for name in own_account_names if name}

    seen: dict[str, dict[str, Any]] = {}
    duplicates: list[dict[str, Any]] = []
    transactions: list[dict[str, Any]] = []
    for tx in raw_transactions:
        fp = tx["fingerprint"]
        if fp in seen:
            duplicates.append(tx)
            continue
        seen[fp] = tx
        cp_name_norm = _norm_name(tx.get("counterparty_name"))
        cp_account = str(tx.get("counterparty_account") or "")
        is_internal = bool(cp_account and cp_account in own_account_nos) or bool(cp_name_norm and cp_name_norm in own_account_names)
        if not is_internal and cp_name_norm:
            is_internal = any(cp_name_norm.startswith(company) and any(marker in cp_name_norm for marker in ("农民工", "工资专户", "一般户", "基本户", "专户", "保证金户")) for company in base_company_names)
        person_name = normalize_person_name(tx.get("counterparty_name"))
        related = roles.get(person_name) if person_name else None
        tx["is_internal_account_transfer"] = bool(is_internal)
        if is_internal:
            tx["exclude_from_effective_flow"] = True
            tx["exclude_reason"] = "客户名下账户之间内部划转，已从有效经营流水中剔除"
        if related:
            tx["is_related_person_transfer"] = True
            tx["related_person_name"] = related.get("name") or person_name
            tx["related_person_role"] = related.get("role") or "关联人"
            tx["exclude_from_effective_flow"] = True
            tx["exclude_reason"] = "公司账户与法人/关联人之间转账，已从有效经营流水中剔除"
        tx["unified_category"] = _unified_category(tx)
        if tx["unified_category"] in {"银行费用", "ETC/车辆费用"}:
            tx["is_bank_fee"] = True
            tx["exclude_from_effective_flow"] = True
        if tx["unified_category"] == "税费":
            tx["is_tax_payment"] = True
            tx["exclude_from_effective_flow"] = True
        if tx["unified_category"] == "工资代发":
            tx["is_salary_payment"] = True
            tx["exclude_from_effective_flow"] = True
        transactions.append(tx)

    effective_in = [
        tx for tx in transactions
        if tx.get("can_join_effective_flow_statistics") and tx.get("direction") == "入账" and tx.get("unified_category") == "经营入账"
        and not tx.get("exclude_from_effective_flow") and tx.get("amount") is not None
    ]
    effective_out = [
        tx for tx in transactions
        if tx.get("can_join_effective_flow_statistics") and tx.get("direction") == "出账" and tx.get("unified_category") == "经营出账"
        and not tx.get("exclude_from_effective_flow") and tx.get("amount") is not None
    ]
    effective_in_amount = sum((_money_value(tx.get("amount")) or Decimal("0") for tx in effective_in), Decimal("0"))
    effective_out_amount = sum((_money_value(tx.get("amount")) or Decimal("0") for tx in effective_out), Decimal("0"))

    monthly: dict[str, dict[str, Any]] = defaultdict(lambda: {"month": "", "effective_in_count": 0, "effective_in_amount": Decimal("0"), "effective_out_count": 0, "effective_out_amount": Decimal("0"), "internal_transfer_amount": Decimal("0"), "related_person_transfer_amount": Decimal("0")})
    for tx in transactions:
        month = _month(tx.get("trade_time") or tx.get("book_date"))
        if not month:
            continue
        item = monthly[month]
        item["month"] = month
        amount = _money_value(tx.get("amount")) or Decimal("0")
        if tx in effective_in:
            item["effective_in_count"] += 1
            item["effective_in_amount"] += amount
        if tx in effective_out:
            item["effective_out_count"] += 1
            item["effective_out_amount"] += amount
        if tx.get("is_internal_account_transfer") or tx.get("is_self_transfer"):
            item["internal_transfer_amount"] += amount
        if tx.get("is_related_person_transfer"):
            item["related_person_transfer_amount"] += amount
    monthly_summary = []
    for month in sorted(monthly):
        item = monthly[month]
        item["operating_net_inflow"] = item["effective_in_amount"] - item["effective_out_amount"]
        monthly_summary.append(item)

    excluded_types = {
        "重复交易": duplicates,
        "本方同名划转": [tx for tx in transactions if tx.get("is_self_transfer")],
        "客户名下账户互转": [tx for tx in transactions if tx.get("is_internal_account_transfer")],
        "法人/关联人转账": [tx for tx in transactions if tx.get("is_related_person_transfer")],
        "银行费用": [tx for tx in transactions if tx.get("is_bank_fee")],
        "税费": [tx for tx in transactions if tx.get("is_tax_payment")],
        "工资代发": [tx for tx in transactions if tx.get("is_salary_payment")],
        "贷款及利息": [tx for tx in transactions if tx.get("is_loan_related") or tx.get("is_interest_related") or tx.get("unified_category") in {"贷款发放", "贷款归还", "利息收入", "利息支出"}],
        "金额缺失/解析异常": [tx for tx in transactions if tx.get("amount") is None],
    }
    descriptions = {
        "重复交易": "多份文件重复上传或时间段重叠",
        "本方同名划转": "同一公司名下账户划转",
        "客户名下账户互转": "多账户之间内部转账",
        "法人/关联人转账": "公司与法人、股东、高管等个人往来",
        "银行费用": "手续费、短信费、网银费、ETC 等",
        "税费": "缴税、扣款缴税",
        "工资代发": "工资、年终奖、代发专用账户",
        "贷款及利息": "贷款发放、归还、利息、融资租赁",
        "金额缺失/解析异常": "金额或交易结构不完整",
    }
    excluded_summary = [
        {"type": name, "count": len(items), "amount": sum((_money_value(tx.get("amount")) or Decimal("0") for tx in items), Decimal("0")), "description": descriptions[name]}
        for name, items in excluded_types.items()
    ]

    included_count = len(included_payloads)
    account_count = len(accounts)
    unrecognized_account_file_count = sum(1 for row in file_quality if row["account_no"] == UNKNOWN)
    amounts_available = bool(included_count and any(_infer_quality(payload)["amounts_valid"] for payload in included_payloads))
    months_count = max(1, len(monthly_summary))
    aggregate_status = "可用" if included_count else "未达标"

    result = {
        "customer_id": str(customer_id or ""),
        "customer_name": customer_name or UNKNOWN,
        "source_files": source_files,
        "file_quality": file_quality,
        "included_files_count": included_count,
        "receipt_bundle_file_count": sum(1 for row in file_quality if row["statement_subtype"] == "receipt_bundle"),
        "nonstandard_bank_file_count": sum(1 for row in file_quality if row["statement_subtype"] == "unknown_bank_statement"),
        "file_only_files_count": sum(1 for row in file_quality if not row["included"] and row["parse_quality_status"] in {"partial", "invalid_transaction_structure"}),
        "failed_or_review_files_count": sum(1 for row in file_quality if not row["included"]),
        "bank_accounts": [
            {**{k: v for k, v in account.items() if k != "source_files"}, "source_files": sorted(account["source_files"]), "time_range": _account_time_range(account)}
            for account in accounts.values()
        ],
        "period_start": min(period_starts) if period_starts else "",
        "period_end": max(period_ends) if period_ends else "",
        "file_count": len(source_files),
        "success_file_count": included_count,
        "failed_file_count": sum(1 for row in file_quality if row["parse_quality_status"] in {"failed", "invalid_account_info", "invalid_transaction_structure"}),
        "account_count": account_count,
        "unrecognized_account_file_count": unrecognized_account_file_count,
        "raw_transaction_count": len(raw_transactions),
        "deduplicated_transaction_count": len(transactions),
        "duplicate_transaction_count": len(duplicates),
        "effective_in_count": len(effective_in),
        "effective_in_amount": effective_in_amount if amounts_available else None,
        "effective_out_count": len(effective_out),
        "effective_out_amount": effective_out_amount if amounts_available else None,
        "operating_net_inflow": (effective_in_amount - effective_out_amount) if amounts_available else None,
        "average_monthly_effective_in": (effective_in_amount / Decimal(months_count)) if amounts_available else None,
        "average_monthly_effective_out": (effective_out_amount / Decimal(months_count)) if amounts_available else None,
        "internal_transfer_count": len(excluded_types["客户名下账户互转"]) + len(excluded_types["本方同名划转"]),
        "internal_transfer_amount": excluded_summary[1]["amount"] + excluded_summary[2]["amount"] if amounts_available else None,
        "related_person_transfer_count": len(excluded_types["法人/关联人转账"]),
        "related_person_transfer_amount": excluded_summary[3]["amount"] if amounts_available else None,
        "loan_related_amount": excluded_summary[7]["amount"] if amounts_available else None,
        "bank_fee_tax_amount": (excluded_summary[4]["amount"] + excluded_summary[5]["amount"]) if amounts_available else None,
        "monthly_summary": monthly_summary,
        "customer_inflow_summary": _aggregate_counterparties(effective_in, effective_in_amount) if amounts_available else [],
        "supplier_outflow_summary": _aggregate_counterparties(effective_out, effective_out_amount) if amounts_available else [],
        "excluded_summary": excluded_summary,
        "loan_related_summary": excluded_types["贷款及利息"],
        "bank_fee_summary": excluded_types["银行费用"],
        "tax_salary_summary": excluded_types["税费"] + excluded_types["工资代发"],
        "risk_tips": [],
        "manual_review_items": [],
        "transactions": transactions,
        "internal_related_transactions": excluded_types["本方同名划转"] + excluded_types["客户名下账户互转"] + excluded_types["法人/关联人转账"],
        "amount_complete_file_count": amount_status_counter.get("完整识别", 0),
        "amount_partial_file_count": amount_status_counter.get("部分识别", 0),
        "amount_unrecognized_file_count": len(source_files) - amount_status_counter.get("完整识别", 0) - amount_status_counter.get("部分识别", 0),
        "parse_completion_rate": f"{len(transactions)}/{len(raw_transactions)}" if raw_transactions else "不可计算，原因：未形成有效交易明细。",
        "amount_integrity": "完整" if amounts_available and amount_status_counter.get("完整识别", 0) == included_count else ("部分" if amounts_available else "不可评估"),
        "aggregate_status": aggregate_status,
        "amount_statistics_available": amounts_available,
    }
    if result["file_count"] == 1:
        result["manual_review_items"].append("当前仅基于 1 个银行账户/1 份对账单进行聚合分析。")
    if not included_count:
        result["manual_review_items"].append("当前文件可能不是标准银行对账单，或未形成标准账户流水明细，暂不能生成经营流水统计。")
        result["manual_review_items"].append("建议上传银行账户明细/账户流水 PDF 或 Excel，并确认本方账号、本方户名和交易时间范围。")
    if any(row["statement_subtype"] == "receipt_bundle" for row in file_quality):
        result["manual_review_items"].append("存在疑似银行回单集合，未纳入经营流水聚合。")
    if not roles:
        result["manual_review_items"].append("关联人名单缺失，建议维护法人/股东/高管名单。")
    if result["duplicate_transaction_count"]:
        result["manual_review_items"].append("存在重复交易，可能由重复上传或时间段重叠导致，已去重。")
    if result["related_person_transfer_count"]:
        result["risk_tips"].append("存在公司账户与法人/关联人之间资金往来，已从有效经营流水中剔除。")
    if result["internal_transfer_count"]:
        result["risk_tips"].append("存在客户名下账户之间内部划转，未计入有效经营收入或支出。")
    if aggregate_status == "未达标":
        result["risk_tips"].append("当前文件均未形成有效交易明细，暂不能据此判断客户经营流水。")
    if not result["risk_tips"]:
        result["risk_tips"].append("未从聚合结果中发现需要特别提示的事项。")
    return result


def render_customer_bank_flow_aggregate_markdown(data: dict[str, Any]) -> str:
    unavailable = not bool(data.get("amount_statistics_available"))
    period_text = "未识别" if not (data.get("period_start") and data.get("period_end")) else f"{data.get('period_start')} 至 {data.get('period_end')}"
    lines = [
        "## 银行流水聚合分析",
        "",
        f"- 客户名称：{data.get('customer_name') or UNKNOWN}",
        f"- 覆盖文件数：{data.get('file_count', 0)} 份",
        f"- 已识别银行账户数：{data.get('account_count', 0)} 个",
        f"- 未识别账户文件数：{data.get('unrecognized_account_file_count', 0)} 份",
        f"- 覆盖时间范围：{period_text}",
        f"- 聚合状态：{data.get('aggregate_status') or UNKNOWN}",
        f"- 原始交易笔数：{data.get('raw_transaction_count', 0)}",
        f"- 去重后交易笔数：{data.get('deduplicated_transaction_count', 0)}",
        f"- 重复交易笔数：{data.get('duplicate_transaction_count', 0)}",
        f"- 金额识别完整度：{data.get('amount_integrity') or UNKNOWN}",
    ]
    if data.get("aggregate_status") == "未达标":
        lines.append(f"- 聚合说明：当前 {data.get('file_count', 0)} 份文件均未形成标准账户流水明细，疑似为银行回单集合或非标准银行流水文件，暂不能生成客户级有效经营流水分析。当前交易统计仅统计已形成标准流水明细的交易，未达标文件不计入交易笔数。建议上传银行账户明细/账户流水 PDF 或 Excel。")
    elif data.get("file_count") == 1:
        lines.append("- 提示：当前仅基于 1 个银行账户/1 份对账单进行聚合分析。")
    else:
        lines.append(f"- 提示：已合并 {data.get('file_count', 0)} 份银行对账单，覆盖 {data.get('account_count', 0)} 个已识别银行账户。")

    lines += [
        "",
        "### 文件解析质量清单",
        "| 序号 | 来源文件 | 识别银行 | 文件类型 | 账号 | 户名 | 交易明细状态 | 是否纳入经营流水聚合 | 问题说明 |",
        "|---:|---|---|---|---|---|---|---|---|",
    ]
    for index, row in enumerate(data.get("file_quality") or [], start=1):
        included = "是" if row.get("included") else "否"
        lines.append(f"| {index} | {row.get('source_file') or '—'} | {row.get('bank_name') or UNKNOWN} | {row.get('statement_subtype_label') or '银行对账单'} | {row.get('account_no') or UNKNOWN} | {row.get('account_name') or UNKNOWN} | {row.get('parse_quality_label') or UNKNOWN} | {included} | {row.get('problem') or '—'} |")

    lines += [
        "",
        "### 账户清单",
        "| 序号 | 银行名称 | 开户行 | 账号 | 户名 | 时间范围 | 文件数 | 交易笔数 |",
        "|---:|---|---|---|---|---|---:|---:|",
    ]
    accounts = data.get("bank_accounts") or []
    if accounts:
        for index, account in enumerate(accounts, start=1):
            lines.append(f"| {index} | {account.get('bank_name') or '—'} | {account.get('opening_bank') or '—'} | {account.get('account_no') or '—'} | {account.get('account_name') or UNKNOWN} | {account.get('time_range') or UNKNOWN} | {account.get('file_count', 0)} | {account.get('transaction_count', 0)} |")
    else:
        lines = lines[:-2]
        lines.append("暂无已识别银行账户。")

    if unavailable:
        lines += [
            "",
            "### 暂无法生成的统计",
            "- 有效经营入账金额：无法计算",
            "- 有效经营出账金额：无法计算",
            "- 经营净流入：无法计算",
            "- 月均有效经营入账：无法计算",
            "- 月均有效经营出账：无法计算",
            "- 主要入账客户：无法计算",
            "- 主要出账供应商：无法计算",
            "- 内部划转及关联人往来：无法计算",
            "- 贷款及利息相关金额：无法计算",
            "- 银行费用及税费金额：无法计算",
            "- 原因：当前文件未形成标准账户流水明细，无法进行客户级经营流水统计。",
        ]
    else:
        lines += [
            "",
            "### 客户级流水摘要",
            f"- 有效经营入账金额：{_money(data.get('effective_in_amount'))}",
            f"- 有效经营出账金额：{_money(data.get('effective_out_amount'))}",
            f"- 经营净流入：{_money(data.get('operating_net_inflow'))}",
            f"- 月均有效经营入账：{_money(data.get('average_monthly_effective_in'))}",
            f"- 月均有效经营出账：{_money(data.get('average_monthly_effective_out'))}",
            f"- 内部账户划转金额：{_money(data.get('internal_transfer_amount'))}",
            f"- 法人/关联人往来金额：{_money(data.get('related_person_transfer_amount'))}",
            f"- 贷款及利息相关金额：{_money(data.get('loan_related_amount'))}",
            f"- 银行费用及税费金额：{_money(data.get('bank_fee_tax_amount'))}",
        ]

    lines += [
        "",
        "### 月度经营流水",
    ]
    if data.get("monthly_summary"):
        lines += [
            "| 月份 | 有效入账金额 | 有效出账金额 | 经营净流入 | 有效入账笔数 | 有效出账笔数 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for item in data.get("monthly_summary") or []:
            lines.append(f"| {item.get('month')} | {_money(item.get('effective_in_amount'))} | {_money(item.get('effective_out_amount'))} | {_money(item.get('operating_net_inflow'))} | {item.get('effective_in_count', 0)} | {item.get('effective_out_count', 0)} |")
    else:
        lines.append("暂无可用交易日期。")

    if data.get("deduplicated_transaction_count", 0):
        lines += ["", "### 主要入账客户", "| 排名 | 入账方名称 | 金额 | 笔数 | 占比 | 主要用途 |", "|---:|---|---:|---:|---:|---|"]
        for item in data.get("customer_inflow_summary") or []:
            lines.append(f"| {item.get('rank')} | {item.get('name')} | {_money(item.get('amount'))} | {item.get('count')} | {item.get('ratio', 0):.2%} | {item.get('main_purpose') or '—'} |")
        lines += ["", "### 主要出账供应商", "| 排名 | 出账方名称 | 金额 | 笔数 | 占比 | 主要用途 |", "|---:|---|---:|---:|---:|---|"]
        for item in data.get("supplier_outflow_summary") or []:
            lines.append(f"| {item.get('rank')} | {item.get('name')} | {_money(item.get('amount'))} | {item.get('count')} | {item.get('ratio', 0):.2%} | {item.get('main_purpose') or '—'} |")

        lines += ["", "### 内部划转及关联人往来", "| 类型 | 交易时间 | 收支方向 | 对方名称 | 关系/原因 | 金额 | 来源账户 |", "|---|---|---|---|---|---:|---|"]
        for tx in data.get("internal_related_transactions") or []:
            kind = "关联人转账" if tx.get("is_related_person_transfer") else "内部账户划转"
            reason = tx.get("related_person_role") or tx.get("exclude_reason") or "内部往来"
            lines.append(f"| {kind} | {tx.get('trade_time') or tx.get('book_date') or '—'} | {tx.get('direction') or '—'} | {tx.get('counterparty_name') or '—'} | {reason} | {_money(tx.get('amount'), unavailable=unavailable)} | {tx.get('bank_name') or ''} {tx.get('account_no') or ''} |")

        lines += ["", "### 剔除项汇总", "| 剔除类型 | 笔数 | 金额 | 说明 |", "|---|---:|---:|---|"]
        for item in data.get("excluded_summary") or []:
            lines.append(f"| {item.get('type')} | {item.get('count', 0)} | {_money(item.get('amount'), unavailable=unavailable)} | {item.get('description') or ''} |")
    else:
        lines += ["", "### 剔除项汇总", "暂无可统计的有效交易明细，无法计算剔除项。"]

    lines += ["", "### 解析质量与需复核事项"]
    lines += [
        f"- 总文件数：{data.get('file_count', 0)}",
        f"- 纳入聚合文件数：{data.get('included_files_count', 0)}",
        f"- 待复核文件数：{data.get('failed_or_review_files_count', 0)}",
        f"- 已识别账户数：{data.get('account_count', 0)}",
        f"- 有效交易笔数：{data.get('raw_transaction_count', 0)}",
        f"- 已去重交易笔数：{data.get('deduplicated_transaction_count', 0)}",
        f"- 解析完整率：{data.get('parse_completion_rate') or UNKNOWN}",
    ]
    if unavailable:
        lines.append("- 当前文件均未形成标准账户流水明细，暂不能生成经营流水统计。")
        subtype_parts = []
        if data.get("receipt_bundle_file_count", 0):
            subtype_parts.append(f"{data.get('receipt_bundle_file_count', 0)} 份疑似银行回单集合")
        if data.get("nonstandard_bank_file_count", 0):
            subtype_parts.append(f"{data.get('nonstandard_bank_file_count', 0)} 份为非标准银行流水文件")
        if subtype_parts:
            lines.append(f"- 其中 {'，'.join(subtype_parts)}。")
        lines.append("- 建议上传银行账户明细/账户流水 PDF 或 Excel，并确认本方账号、本方户名和交易时间范围。")
        if any("关联人名单缺失" in str(item) for item in data.get("manual_review_items") or []):
            lines.append("- 关联人名单缺失，建议维护法人/股东/高管名单。")
    else:
        lines += [
            f"- 金额完整识别文件数：{data.get('amount_complete_file_count', 0)}",
            f"- 金额部分识别文件数：{data.get('amount_partial_file_count', 0)}",
            f"- 金额未识别文件数：{data.get('amount_unrecognized_file_count', 0)}",
        ]
        lines += [f"- {item}" for item in data.get("manual_review_items") or []]
    if unavailable:
        lines += [
            "",
            "### 分析限制",
            "- 当前文件均未形成有效交易明细，暂不能据此判断客户经营流水、主要客户、主要供应商、经营净流入和内部往来情况。",
        ]
    else:
        lines += ["", "### 风险提示"] + [f"- {item}" for item in data.get("risk_tips") or []]
    return "\n".join(lines)
