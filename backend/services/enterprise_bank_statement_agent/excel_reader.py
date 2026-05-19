from __future__ import annotations

from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from .normalizer import guess_bank_name, normalize_text


HEADER_SYNONYMS: dict[str, tuple[str, ...]] = {
    "transaction_date": ("交易日期", "记账日期", "入账日期", "交易时间", "发生日期", "日期", "transaction_date", "date"),
    "post_date": ("记账日期", "入账日期", "过账日期", "清算日期", "post_date", "posting_date"),
    "credit_amount": ("贷方金额", "收入", "转入", "入账金额", "发生额收入", "收方金额", "来账金额", "credit", "credit_amount", "inflow"),
    "debit_amount": ("借方金额", "支出", "转出", "出账金额", "发生额支出", "付方金额", "往账金额", "debit", "debit_amount", "outflow"),
    "balance": ("余额", "账户余额", "交易后余额", "可用余额", "balance"),
    "summary": ("摘要", "交易摘要", "备注", "附言", "交易用途", "summary", "remark"),
    "purpose": ("用途", "交易用途", "附言", "备注", "purpose", "usage"),
    "counterparty_name": ("对方户名", "对方名称", "交易对手", "对方账户名称", "收付款方名称", "户名", "counterparty_name", "counterparty"),
    "counterparty_account": ("对方账号", "对方账户", "收付款方账号", "账号", "counterparty_account"),
    "account_name": ("户名", "账户名称", "客户名称", "单位名称", "account_name"),
    "account_number": ("账号", "账户", "银行账号", "账户号码", "account_number"),
    "currency": ("币种", "货币", "currency"),
}


def _canonical_header(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    compact = text.replace(" ", "")
    for canonical, names in HEADER_SYNONYMS.items():
        if any(name in compact for name in names):
            return canonical
    return None


def _find_header_row(rows: list[list[Any]], warnings: list[str], sheet_name: str) -> tuple[int | None, dict[int, str]]:
    best_index: int | None = None
    best_mapping: dict[int, str] = {}
    best_score = 0
    for index, row in enumerate(rows[:20]):
        mapping: dict[int, str] = {}
        for col_index, value in enumerate(row):
            canonical = _canonical_header(value)
            if canonical and canonical not in mapping.values():
                mapping[col_index] = canonical
        score = len(set(mapping.values()) & {"transaction_date", "credit_amount", "debit_amount", "balance", "summary"})
        if score > best_score:
            best_score = score
            best_index = index
            best_mapping = mapping
        if score >= 3 and "transaction_date" in mapping.values():
            return index, mapping
    if not best_mapping:
        warnings.append(f"sheet {sheet_name} 未能在前20行识别交易表头")
    return best_index, best_mapping


def _sheet_meta(rows: list[list[Any]], sheet_name: str) -> dict[str, Any]:
    text = "\n".join(" ".join(normalize_text(cell) for cell in row if normalize_text(cell)) for row in rows[:20])
    account_number = None
    account_name = None
    currency = "人民币"
    for line in text.splitlines():
        if not account_number and any(label in line for label in ("账号", "账户号码", "银行账号")):
            import re

            match = re.search(r"\b\d{8,32}\b", line.replace(" ", ""))
            if match:
                account_number = match.group(0)
        if not account_name and any(label in line for label in ("客户名称", "账户名称", "户名", "单位名称")):
            account_name = line.split("：")[-1].split(":")[-1].strip()[:80]
        if "美元" in line or "USD" in line.upper():
            currency = "美元"
    return {
        "bank_name": guess_bank_name(text) or guess_bank_name(sheet_name),
        "account_name": account_name,
        "account_number": account_number,
        "currency": currency,
    }


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
                        for row_number, raw_row in enumerate(raw_rows[header_index + 1 :], start=header_index + 2):
                            raw = {str(raw_rows[header_index][idx] or f"col_{idx + 1}"): raw_row[idx] if idx < len(raw_row) else None for idx in range(len(raw_row))}
                            normalized = {field: raw_row[idx] if idx < len(raw_row) else None for idx, field in header_map.items()}
                            normalized["raw"] = raw
                            normalized["row_number"] = row_number
                            data_rows.append(normalized)
                    sheets.append({"sheet_name": ws.title, "meta": _sheet_meta(raw_rows, ws.title), "rows": data_rows})
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
        sheets.append({"sheet_name": "上传解析结果", "meta": _sheet_meta([list(item.keys()) for item in rows[:20]], "上传解析结果"), "rows": data_rows})
    return {"source_file": source_file, "sheets": sheets, "warnings": warnings}
