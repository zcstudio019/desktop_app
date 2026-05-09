from __future__ import annotations

import json
import re
from typing import Any


def repair_common_json_issues(text: str) -> str:
    value = (text or "").strip()
    value = re.sub(r"^```(?:json)?\s*", "", value, flags=re.I)
    value = re.sub(r"\s*```$", "", value)
    start = value.find("{")
    end = value.rfind("}")
    if start != -1 and end != -1 and end > start:
        value = value[start:end + 1]
    value = re.sub(r",\s*([}\]])", r"\1", value)
    return value.strip()


def parse_json_safely(text: str) -> tuple[dict[str, Any] | None, str]:
    repaired = repair_common_json_issues(text)
    if not repaired:
        return None, "empty output"
    try:
        parsed = json.loads(repaired)
    except json.JSONDecodeError as exc:
        return None, f"json decode failed: {exc}"
    if not isinstance(parsed, dict):
        return None, "json root is not object"
    return parsed, ""


def validate_agent_output(payload: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    _validate_node(payload, schema, "$", errors)
    return errors


def _validate_node(value: Any, schema: dict[str, Any], path: str, errors: list[str]) -> None:
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(value, dict):
            errors.append(f"{path}: expected object")
            return
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}.{key}: required")
        properties = schema.get("properties") or {}
        for key, child_schema in properties.items():
            if key in value:
                _validate_node(value[key], child_schema, f"{path}.{key}", errors)
        return
    if expected_type == "array":
        if not isinstance(value, list):
            errors.append(f"{path}: expected array")
            return
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                _validate_node(item, item_schema, f"{path}[{index}]", errors)
        return
    if expected_type == "string":
        if not isinstance(value, str):
            errors.append(f"{path}: expected string")
            return
        enum = schema.get("enum")
        if enum and value not in enum:
            errors.append(f"{path}: expected one of {enum}")
        return
    if expected_type == "boolean":
        if not isinstance(value, bool):
            errors.append(f"{path}: expected boolean")
        return
    if expected_type == "number":
        if not isinstance(value, (int, float)):
            errors.append(f"{path}: expected number")
        return
