from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.extraction_skills.bank_statement import (
    _normalize_entity_name,
    _related_person_map_from_metadata,
    normalize_person_name,
)


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
    if value in (None, "", "未识别"):
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _money(value: Any) -> str:
    amount = _money_value(value)
    return f"{amount:,.2f}" if amount is not None else "未识别"


def _float(value: Any) -> float:
    amount = _money_value(value)
    return float(amount) if amount is not None else 0.0


def _date(value: Any) -> str:
    text = str(value or "")
    return text[:10] if len(text) >= 10 else ""


def _month(value: Any) -> str:
    date_text = _date(value)
    return date_text[:7] if len(date_text) >= 7 else ""


def _norm_name(value: Any) -> str:
    return _normalize_entity_name(value)


def _tx_value(tx: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if tx.get(key) not in (None, ""):
            return tx.get(key)
    return ""


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


def aggregate_customer_bank_statements(
    extractions: list[dict[str, Any]],
    *,
    customer_id: str | int = "",
    customer_profile: dict[str, Any] | None = None,
    related_person_names: list[str] | None = None,
    related_person_roles: dict[str, str] | None = None,
) -> dict[str, Any]:
    roles = _related_roles(customer_profile, related_person_names, related_person_roles)
    source_files: list[dict[str, Any]] = []
    accounts: dict[str, dict[str, Any]] = {}
    raw_transactions: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    duplicates: list[dict[str, Any]] = []
    period_starts: list[str] = []
    period_ends: list[str] = []
    amount_status_counter = Counter()
    failed_file_count = 0
    success_file_count = 0
    customer_name = str((customer_profile or {}).get("customer_name") or (customer_profile or {}).get("name") or "")

    statements: list[dict[str, Any]] = []
    for extraction in extractions:
        payload = _payload(extraction)
        if (payload.get("doc_type") or payload.get("document_type_code") or extraction.get("extraction_type")) != "bank_statement":
            continue
        status = str(payload.get("extract_status") or payload.get("extraction_status") or extraction.get("extraction_status") or "").lower()
        source_file = str(extraction.get("file_name") or payload.get("source_file") or payload.get("file_name") or "")
        source_files.append({"file_name": source_file, "status": status or "success", "account_no": payload.get("account_no") or ""})
        if status in {"failed", "失败", "partial_failed"}:
            failed_file_count += 1
            continue
        success_file_count += 1
        statements.append(payload)
        if not customer_name and payload.get("account_name"):
            customer_name = str(payload.get("account_name") or "")
        start = _date(payload.get("period_start"))
        end = _date(payload.get("period_end"))
        if start:
            period_starts.append(start)
        if end:
            period_ends.append(end)
        amount_status_counter[str(payload.get("amount_recognition_status") or "未识别")] += 1
        account_no = str(payload.get("account_no") or "")
        account_key = account_no or f"{payload.get('bank_name') or ''}|{payload.get('opening_bank') or ''}|{payload.get('account_name') or ''}|{source_file}"
        account = accounts.setdefault(account_key, {
            "bank_name": payload.get("bank_name") or "",
            "opening_bank": payload.get("opening_bank") or "",
            "account_no": account_no,
            "account_name": payload.get("account_name") or "",
            "period_start": start,
            "period_end": end,
            "file_count": 0,
            "transaction_count": 0,
            "amount_recognition_status": payload.get("amount_recognition_status") or "未识别",
            "source_files": set(),
        })
        account["file_count"] += 1
        account["source_files"].add(source_file)
        account["transaction_count"] += int(payload.get("transaction_count") or len(_list(payload.get("transactions"))))
        if start and (not account.get("period_start") or start < account["period_start"]):
            account["period_start"] = start
        if end and (not account.get("period_end") or end > account["period_end"]):
            account["period_end"] = end
        for tx in _list(payload.get("transactions")):
            raw_transactions.append(_canonical_tx(_dict(tx), payload, source_file))

    own_account_nos = {str(item.get("account_no") or "") for item in accounts.values() if item.get("account_no")}
    own_account_names = {_norm_name(item.get("account_name")) for item in accounts.values() if item.get("account_name")}
    base_company_names = {name for name in own_account_names if name}

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
        if is_internal:
            tx["is_internal_account_transfer"] = True
            tx["exclude_from_effective_flow"] = True
            tx["exclude_reason"] = "客户名下账户之间内部划转，已从有效经营流水中剔除"
        else:
            tx["is_internal_account_transfer"] = False
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

    effective_in = [tx for tx in transactions if tx.get("direction") == "入账" and tx.get("unified_category") == "经营入账" and not tx.get("exclude_from_effective_flow") and tx.get("amount") is not None]
    effective_out = [tx for tx in transactions if tx.get("direction") == "出账" and tx.get("unified_category") == "经营出账" and not tx.get("exclude_from_effective_flow") and tx.get("amount") is not None]
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

    months_count = max(1, len(monthly_summary))
    result = {
        "customer_id": str(customer_id or ""),
        "customer_name": customer_name,
        "source_files": source_files,
        "bank_accounts": [
            {**{k: v for k, v in account.items() if k != "source_files"}, "source_files": sorted(account["source_files"]), "time_range": f"{account.get('period_start') or ''} 至 {account.get('period_end') or ''}".strip(" 至")}
            for account in accounts.values()
        ],
        "period_start": min(period_starts) if period_starts else "",
        "period_end": max(period_ends) if period_ends else "",
        "file_count": len(source_files),
        "success_file_count": success_file_count,
        "failed_file_count": failed_file_count,
        "account_count": len(accounts),
        "raw_transaction_count": len(raw_transactions),
        "deduplicated_transaction_count": len(transactions),
        "duplicate_transaction_count": len(duplicates),
        "effective_in_count": len(effective_in),
        "effective_in_amount": effective_in_amount,
        "effective_out_count": len(effective_out),
        "effective_out_amount": effective_out_amount,
        "operating_net_inflow": effective_in_amount - effective_out_amount,
        "average_monthly_effective_in": effective_in_amount / Decimal(months_count),
        "average_monthly_effective_out": effective_out_amount / Decimal(months_count),
        "internal_transfer_count": len(excluded_types["客户名下账户互转"]) + len(excluded_types["本方同名划转"]),
        "internal_transfer_amount": excluded_summary[1]["amount"] + excluded_summary[2]["amount"],
        "related_person_transfer_count": len(excluded_types["法人/关联人转账"]),
        "related_person_transfer_amount": excluded_summary[3]["amount"],
        "loan_related_amount": excluded_summary[7]["amount"],
        "bank_fee_tax_amount": excluded_summary[4]["amount"] + excluded_summary[5]["amount"],
        "monthly_summary": monthly_summary,
        "customer_inflow_summary": _aggregate_counterparties(effective_in, effective_in_amount),
        "supplier_outflow_summary": _aggregate_counterparties(effective_out, effective_out_amount),
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
        "amount_unrecognized_file_count": amount_status_counter.get("未识别", 0),
        "parse_completion_rate": f"{len(transactions)}/{len(raw_transactions)}" if raw_transactions else "0/0",
        "amount_integrity": "完整" if amount_status_counter and amount_status_counter.get("完整识别", 0) == success_file_count else ("不完整" if amount_status_counter.get("未识别", 0) else "部分"),
    }
    if result["file_count"] == 1:
        result["manual_review_items"].append("当前仅基于 1 个银行账户/1 份对账单进行聚合分析。")
    if not roles:
        result["manual_review_items"].append("关联人名单缺失，建议维护法人/股东/高管名单。")
    if result["duplicate_transaction_count"]:
        result["manual_review_items"].append("存在重复交易，可能由重复上传或时间段重叠导致，已去重。")
    if result["related_person_transfer_count"]:
        result["risk_tips"].append("存在公司账户与法人/关联人之间资金往来，已从有效经营流水中剔除。")
    if result["internal_transfer_count"]:
        result["risk_tips"].append("存在客户名下账户之间内部划转，未计入有效经营收入或支出。")
    if not result["risk_tips"]:
        result["risk_tips"].append("未从聚合结果中发现需要特别提示的事项。")
    return result


def render_customer_bank_flow_aggregate_markdown(data: dict[str, Any]) -> str:
    lines = [
        "## 银行流水聚合分析",
        "",
        f"- 客户名称：{data.get('customer_name') or '未识别'}",
        f"- 覆盖文件数：{data.get('file_count', 0)} 份",
        f"- 覆盖银行账户数：{data.get('account_count', 0)} 个",
        f"- 覆盖时间范围：{data.get('period_start') or '未识别'} 至 {data.get('period_end') or '未识别'}",
        f"- 原始交易笔数：{data.get('raw_transaction_count', 0)}",
        f"- 去重后交易笔数：{data.get('deduplicated_transaction_count', 0)}",
        f"- 重复交易笔数：{data.get('duplicate_transaction_count', 0)}",
        f"- 金额识别完整度：{data.get('amount_integrity') or '未识别'}",
    ]
    if data.get("file_count") == 1:
        lines.append("- 提示：当前仅基于 1 个银行账户/1 份对账单进行聚合分析。")
    else:
        lines.append(f"- 提示：已合并 {data.get('file_count', 0)} 份银行对账单，覆盖 {data.get('account_count', 0)} 个银行账户。")
    lines += ["", "### 账户清单", "| 序号 | 银行名称 | 开户行 | 账号 | 户名 | 时间范围 | 文件数 | 交易笔数 |", "|---:|---|---|---|---|---|---:|---:|"]
    for index, account in enumerate(data.get("bank_accounts") or [], start=1):
        lines.append(f"| {index} | {account.get('bank_name') or '—'} | {account.get('opening_bank') or '—'} | {account.get('account_no') or '—'} | {account.get('account_name') or '—'} | {account.get('time_range') or '—'} | {account.get('file_count', 0)} | {account.get('transaction_count', 0)} |")
    lines += [
        "", "### 客户级流水摘要",
        f"- 有效经营入账金额：{_money(data.get('effective_in_amount'))}",
        f"- 有效经营出账金额：{_money(data.get('effective_out_amount'))}",
        f"- 经营净流入：{_money(data.get('operating_net_inflow'))}",
        f"- 月均有效经营入账：{_money(data.get('average_monthly_effective_in'))}",
        f"- 月均有效经营出账：{_money(data.get('average_monthly_effective_out'))}",
        f"- 内部账户划转金额：{_money(data.get('internal_transfer_amount'))}",
        f"- 法人/关联人往来金额：{_money(data.get('related_person_transfer_amount'))}",
        f"- 贷款及利息相关金额：{_money(data.get('loan_related_amount'))}",
        f"- 银行费用及税费金额：{_money(data.get('bank_fee_tax_amount'))}",
        "", "### 月度经营流水",
        "| 月份 | 有效入账金额 | 有效出账金额 | 经营净流入 | 有效入账笔数 | 有效出账笔数 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in data.get("monthly_summary") or []:
        lines.append(f"| {item.get('month')} | {_money(item.get('effective_in_amount'))} | {_money(item.get('effective_out_amount'))} | {_money(item.get('operating_net_inflow'))} | {item.get('effective_in_count', 0)} | {item.get('effective_out_count', 0)} |")
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
        lines.append(f"| {kind} | {tx.get('trade_time') or tx.get('book_date') or '—'} | {tx.get('direction') or '—'} | {tx.get('counterparty_name') or '—'} | {reason} | {_money(tx.get('amount'))} | {tx.get('bank_name') or ''} {tx.get('account_no') or ''} |")
    lines += ["", "### 剔除项汇总", "| 剔除类型 | 笔数 | 金额 | 说明 |", "|---|---:|---:|---|"]
    for item in data.get("excluded_summary") or []:
        lines.append(f"| {item.get('type')} | {item.get('count', 0)} | {_money(item.get('amount'))} | {item.get('description') or ''} |")
    lines += ["", "### 解析质量与需复核事项"]
    lines += [
        f"- 总文件数：{data.get('file_count', 0)}",
        f"- 成功解析文件数：{data.get('success_file_count', 0)}",
        f"- 解析失败文件数：{data.get('failed_file_count', 0)}",
        f"- 总账户数：{data.get('account_count', 0)}",
        f"- 总交易笔数：{data.get('raw_transaction_count', 0)}",
        f"- 已去重交易笔数：{data.get('deduplicated_transaction_count', 0)}",
        f"- 金额完整识别文件数：{data.get('amount_complete_file_count', 0)}",
        f"- 金额部分识别文件数：{data.get('amount_partial_file_count', 0)}",
        f"- 金额未识别文件数：{data.get('amount_unrecognized_file_count', 0)}",
        f"- 解析完整率：{data.get('parse_completion_rate') or '未识别'}",
    ]
    lines += [f"- {item}" for item in data.get("manual_review_items") or []]
    lines += ["", "### 风险提示"] + [f"- {item}" for item in data.get("risk_tips") or []]
    return "\n".join(lines)
