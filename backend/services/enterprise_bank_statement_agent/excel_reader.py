from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from .normalizer import normalize_account_number, normalize_amount, normalize_currency, normalize_date, normalize_text


COUNTERPARTY_ACCOUNT_HEADERS = ("对方账号", "对方账户", "收付款方账号", "交易对手账号", "对手方账号")
COUNTERPARTY_BANK_HEADERS = ("对方开户行", "对方机构", "对方银行", "收付款方开户行", "对方账户开户行", "counterparty_bank")
OWN_ACCOUNT_HEADERS = ("账号", "本方账号", "企业账号", "账户账号", "银行账号", "账户号码", "account_number")

HEADER_SYNONYMS: dict[str, tuple[str, ...]] = {
    "transaction_date": ("交易日期", "记账日期", "入账日期", "交易时间", "发生日期", "日期", "提交时间", "transaction_date", "date"),
    "post_date": ("记账日期", "入账日期", "过账日期", "清算日期", "post_date", "posting_date"),
    "credit_amount": ("贷方金额", "收入", "转入", "入账金额", "发生额收入", "收方金额", "来账金额", "贷方发生额", "收入金额", "借方金额（收）", "借方金额(收)", "credit", "credit_amount", "inflow"),
    "debit_amount": ("借方金额", "支出", "转出", "出账金额", "发生额支出", "付方金额", "往账金额", "借方发生额", "支出金额", "贷方金额（支）", "贷方金额(支)", "debit", "debit_amount", "outflow"),
    "balance": ("余额", "账户余额", "交易后余额", "可用余额", "balance"),
    "summary": ("摘要", "交易摘要", "交易名称", "备注", "附言", "客户附言", "summary", "remark"),
    "purpose": ("用途", "交易用途", "附言", "备注", "purpose", "usage"),
    "counterparty_name": ("对方户名", "对方名称", "交易对手", "对方账户名称", "收付款方名称", "counterparty_name", "counterparty"),
    "counterparty_account": COUNTERPARTY_ACCOUNT_HEADERS,
    "counterparty_bank": COUNTERPARTY_BANK_HEADERS,
    "account_number": OWN_ACCOUNT_HEADERS,
    "currency": ("币种", "货币", "currency"),
}

SHEET_BANK_ALIASES: tuple[tuple[str, str], ...] = (
    ("民生", "民生银行"),
    ("平安", "平安银行"),
    ("泰隆", "泰隆银行"),
    ("浙江网商", "浙江网商"),
    ("网商", "浙江网商"),
)


def infer_bank_from_sheet_name(sheet_name: Any) -> str | None:
    text = normalize_text(sheet_name)
    for keyword, bank_name in SHEET_BANK_ALIASES:
        if keyword in text:
            return bank_name
    if "银行" in text:
        return text
    return None


def _compact(value: Any) -> str:
    return normalize_text(value).replace(" ", "")


def _canonical_header(value: Any) -> str | None:
    compact = _compact(value)
    if not compact:
        return None
    if any(name in compact for name in COUNTERPARTY_BANK_HEADERS):
        return "counterparty_bank"
    if any(name in compact for name in COUNTERPARTY_ACCOUNT_HEADERS):
        return "counterparty_account"
    if "借方金额" in compact and "收" in compact:
        return "credit_amount"
    if "贷方金额" in compact and "支" in compact:
        return "debit_amount"
    if compact in OWN_ACCOUNT_HEADERS or compact in ("本方账户", "企业账户"):
        return "account_number"
    for canonical, names in HEADER_SYNONYMS.items():
        if canonical in {"counterparty_account", "counterparty_bank", "account_number"}:
            continue
        if any(name in compact for name in names):
            return canonical
    if compact == "户名":
        return "counterparty_name"
    return None


def _find_header_row(rows: list[list[Any]], warnings: list[str], sheet_name: str) -> tuple[int | None, dict[int, str]]:
    best_index: int | None = None
    best_mapping: dict[int, str] = {}
    best_score = 0
    for index, row in enumerate(rows[:30]):
        mapping: dict[int, str] = {}
        for col_index, value in enumerate(row):
            canonical = _canonical_header(value)
            if canonical and canonical not in mapping.values():
                mapping[col_index] = canonical
        fields = set(mapping.values())
        amount_score = int("credit_amount" in fields) + int("debit_amount" in fields)
        score = amount_score * 2 + int("transaction_date" in fields) + int("balance" in fields) + int("summary" in fields)
        if score > best_score:
            best_score = score
            best_index = index
            best_mapping = mapping
        if score >= 4 and "transaction_date" in fields:
            return index, mapping
    if not best_mapping:
        warnings.append(f"sheet {sheet_name} 未能在前30行识别交易表头")
    return best_index, best_mapping


def _iter_label_value_pairs(rows: list[list[Any]], limit: int = 20) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for row in rows[:limit]:
        cells = [cell for cell in row]
        for index, cell in enumerate(cells):
            label = _compact(cell).rstrip(":：")
            if not label:
                continue
            value = cells[index + 1] if index + 1 < len(cells) else None
            if value not in (None, ""):
                pairs.append((label, value))
            text = normalize_text(cell)
            for sep in (":", "："):
                if sep in text:
                    left, right = text.split(sep, 1)
                    if left.strip() and right.strip():
                        pairs.append((_compact(left), right.strip()))
    return pairs


def _extract_account_from_column(rows: list[dict[str, Any]]) -> str | None:
    values = [normalize_account_number(row.get("account_number")) for row in rows]
    values = [value for value in values if value]
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def _clean_enterprise_account(value: Any) -> str | None:
    text = normalize_text(value)
    text = re.sub(r"[（(]\s*人民币\s*[）)]", "", text)
    return normalize_account_number(text)


def _summary_label(label: str) -> str:
    text = _compact(label)
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\((?:¥|￥|元|人民币)?\)", "", text)
    text = text.replace("¥", "").replace("￥", "").replace("元", "")
    return text


def parse_sheet_account_info(sheet_name: str, rows: list[list[Any]], customer_name: str | None = None) -> dict[str, Any]:
    bank_name = infer_bank_from_sheet_name(sheet_name) or sheet_name
    account_info: dict[str, Any] = {
        "bank_name": bank_name,
        "account_name": customer_name,
        "account_number": None,
        "branch_name": None,
        "currency": "人民币",
        "period_start": None,
        "period_end": None,
        "summary_inflow": None,
        "summary_outflow": None,
        "summary_inflow_count": None,
        "summary_outflow_count": None,
    }

    for label, value in _iter_label_value_pairs(rows):
        normalized_label = _summary_label(label)
        if any(key in normalized_label for key in ("对方账号", "对方账户", "收付款方账号", "交易对手账号")):
            continue
        if any(key in normalized_label for key in ("对方开户行", "对方机构", "对方银行", "收付款方开户行")):
            continue
        if normalized_label in {"账户名称", "户名", "企业名称", "客户名称", "单位名称"}:
            account_info["account_name"] = normalize_text(value) or account_info["account_name"]
        elif normalized_label in {"账号", "企业账号", "本方账号", "账户账号", "银行账号", "账户号码"}:
            account_info["account_number"] = _clean_enterprise_account(value)
        elif normalized_label in {"开户机构", "开户行", "开户网点"}:
            account_info["branch_name"] = normalize_text(value) or None
        elif normalized_label in {"币种", "货币"}:
            account_info["currency"] = normalize_currency(value)
        elif normalized_label in {"起始日期", "开始日期", "流水开始日期"}:
            account_info["period_start"] = normalize_date(value)
        elif normalized_label in {"截止日期", "结束日期", "流水结束日期"}:
            account_info["period_end"] = normalize_date(value)
        elif normalized_label in {"借方累计发生额", "总支出", "贷方交易金额"}:
            account_info["summary_outflow"] = normalize_amount(value)
        elif normalized_label in {"贷方累计发生额", "总收入", "借方交易金额"}:
            account_info["summary_inflow"] = normalize_amount(value)
        elif normalized_label in {"借方累计笔数", "总支出笔数", "贷方交易笔数"}:
            amount = normalize_amount(value)
            account_info["summary_outflow_count"] = int(amount) if amount is not None else None
        elif normalized_label in {"贷方累计笔数", "总收入笔数", "借方交易笔数"}:
            amount = normalize_amount(value)
            account_info["summary_inflow_count"] = int(amount) if amount is not None else None

    account_id = f"{account_info['bank_name']}:{account_info.get('account_number') or sheet_name}"
    account_info["account_id"] = account_id
    return account_info


def read_excel_workbook(file_path: str | None = None, rows: list[dict[str, Any]] | None = None, filename: str | None = None) -> dict[str, Any]:
    warnings: list[str] = []
    sheets: list[dict[str, Any]] = []
    source_file = filename or (Path(file_path).name if file_path else "")
    if file_path:
        path = Path(file_path)
        suffix = path.suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            warnings.append(f"{suffix or 'unknown'} 暂未直接读取 workbook，已尝试使用上传解析 rows fallback")
        else:
            try:
                workbook = load_workbook(path, read_only=True, data_only=True)
                for ws in workbook.worksheets:
                    raw_rows = [[cell for cell in row] for row in ws.iter_rows(values_only=True)]
                    header_index, header_map = _find_header_row(raw_rows, warnings, ws.title)
                    data_rows: list[dict[str, Any]] = []
                    if header_index is not None and header_map:
                        header_row = raw_rows[header_index]
                        for row_number, raw_row in enumerate(raw_rows[header_index + 1 :], start=header_index + 2):
                            raw = {str(header_row[idx] or f"col_{idx + 1}"): raw_row[idx] if idx < len(raw_row) else None for idx in range(len(header_row))}
                            normalized = {field: raw_row[idx] if idx < len(raw_row) else None for idx, field in header_map.items()}
                            normalized["raw"] = raw
                            normalized["row_number"] = row_number
                            data_rows.append(normalized)
                    meta = parse_sheet_account_info(ws.title, raw_rows)
                    if not meta.get("account_number"):
                        meta["account_number"] = _extract_account_from_column(data_rows)
                        meta["account_id"] = f"{meta['bank_name']}:{meta.get('account_number') or ws.title}"
                    sheets.append({"sheet_name": ws.title, "meta": meta, "rows": data_rows})
                workbook.close()
                return {"source_file": source_file, "sheets": sheets, "warnings": warnings}
            except Exception as exc:
                warnings.append(f"Excel workbook 读取失败：{exc}")

    if rows:
        data_rows = []
        for index, row in enumerate(rows, start=2):
            normalized: dict[str, Any] = {}
            for key, value in row.items():
                canonical = _canonical_header(key)
                if canonical:
                    normalized[canonical] = value
            normalized["raw"] = dict(row)
            normalized["row_number"] = index
            data_rows.append(normalized)
        sheet_name = "上传解析结果"
        meta = parse_sheet_account_info(sheet_name, [list(item.keys()) for item in rows[:20]])
        meta["account_number"] = meta.get("account_number") or _extract_account_from_column(data_rows)
        meta["account_id"] = f"{meta['bank_name']}:{meta.get('account_number') or sheet_name}"
        sheets.append({"sheet_name": sheet_name, "meta": meta, "rows": data_rows})
    return {"source_file": source_file, "sheets": sheets, "warnings": warnings}
