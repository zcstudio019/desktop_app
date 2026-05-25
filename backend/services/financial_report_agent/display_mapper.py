from __future__ import annotations

import re
from typing import Any

from .field_labels import (
    BALANCE_SHEET_LABELS,
    BANK_CREDIT_ANALYSIS_LABELS,
    CASH_FLOW_LABELS,
    COMPANY_INFO_LABELS,
    EVIDENCE_FIELD_LABELS,
    FIELD_LABELS,
    FINANCIAL_RATIO_LABELS,
    INCOME_STATEMENT_LABELS,
    VALUE_LABELS,
)


KEY_LABELS = {
    **FIELD_LABELS,
    **COMPANY_INFO_LABELS,
    **BALANCE_SHEET_LABELS,
    **INCOME_STATEMENT_LABELS,
    **CASH_FLOW_LABELS,
    **FINANCIAL_RATIO_LABELS,
    **BANK_CREDIT_ANALYSIS_LABELS,
    **EVIDENCE_FIELD_LABELS,
}


def _label_key(key: Any) -> str:
    text = str(key)
    if text in KEY_LABELS:
        return KEY_LABELS[text]
    if re.search(r"[A-Za-z_]", text):
        return "其他信息"
    return text


def _unique_key(result: dict[str, Any], label: str) -> str:
    if label not in result:
        return label
    index = 2
    while f"{label}{index}" in result:
        index += 1
    return f"{label}{index}"


def to_display_json(data: dict) -> dict:
    """
    将财务报表 structured_json 转成中文字段 display_json。

    内部结构继续保留英文字段；此转换结果仅用于用户界面和报告展示。
    """
    def display_path(value: str) -> str:
        return " / ".join(KEY_LABELS.get(part, "其他信息") for part in value.split("."))

    def convert(value: Any, parent_key: str = "") -> Any:
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for key, item in value.items():
                label = _unique_key(result, _label_key(key))
                result[label] = convert(item, str(key))
            return result
        if isinstance(value, list):
            return [convert(item, parent_key) for item in value]
        if isinstance(value, str):
            if parent_key == "field_path":
                return display_path(value)
            return VALUE_LABELS.get(value, value)
        return value

    return convert(data)
