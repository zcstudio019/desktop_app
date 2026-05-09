from __future__ import annotations

import json
import re
from typing import Any


PHONE_RE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
ID_CARD_RE = re.compile(r"(?<!\d)[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)")
USC_RE = re.compile(r"\b([0-9A-Z]{6})([0-9A-Z]{6,8})([0-9A-Z]{4})\b")
BANK_ACCOUNT_RE = re.compile(r"(?<!\d)\d{12,30}(?!\d)")


def _mask_name(value: str) -> str:
    if not value:
        return value
    if len(value) <= 1:
        return "*"
    return value[0] + "*" * (len(value) - 1)


def _mask_company(value: str) -> str:
    if not value:
        return value
    return value[:4] + "*" * max(0, len(value) - 4)


def _sanitize_string(value: str) -> str:
    text = PHONE_RE.sub(lambda m: m.group(0)[:3] + "****" + m.group(0)[-4:], value)
    text = ID_CARD_RE.sub(lambda m: m.group(0)[:6] + "********" + m.group(0)[-4:], text)
    text = USC_RE.sub(lambda m: m.group(1) + "******" + m.group(3), text)
    text = BANK_ACCOUNT_RE.sub(lambda m: m.group(0)[:4] + "****" + m.group(0)[-4:], text)
    text = re.sub(r"(地址|住所|住址|经营场所|注册地址)[:：]?[^\n,，；;]{6,80}", r"\1：***", text)
    return text


def sanitize_for_debug(data: dict | list | str | Any) -> dict | list | str | Any:
    if isinstance(data, str):
        return _sanitize_string(data)
    if isinstance(data, list):
        return [sanitize_for_debug(item) for item in data]
    if isinstance(data, dict):
        sanitized: dict[str, Any] = {}
        for key, value in data.items():
            key_text = str(key).lower()
            if key_text in {"agent_name", "model", "risk_type", "document_type", "credit_type", "business_type"}:
                sanitized[key] = sanitize_for_debug(value)
            elif isinstance(value, str) and any(k in key_text for k in ["name", "姓名", "法人", "legal_person"]):
                sanitized[key] = _mask_name(value)
            elif isinstance(value, str) and any(k in key_text for k in ["company", "enterprise", "企业", "公司", "customer"]):
                sanitized[key] = _mask_company(value)
            else:
                sanitized[key] = sanitize_for_debug(value)
        return sanitized
    return data


def to_debug_json(data: Any) -> str:
    return json.dumps(sanitize_for_debug(data), ensure_ascii=False, indent=2)
