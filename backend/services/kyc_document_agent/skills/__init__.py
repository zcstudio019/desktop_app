from __future__ import annotations

from typing import Any, Callable

from backend.services.kyc_document_agent.schema import build_result, normalize_input


def empty_extract(payload: dict[str, Any] | str, doc_type: str) -> dict[str, Any]:
    data = normalize_input(payload)
    result = build_result(doc_type)
    result["raw_text_preview"] = data["text"][:240]
    result["validation"]["warnings"].append("该资料类型的字段模板尚未完整实现")
    return result


SkillExtract = Callable[[dict[str, Any] | str], dict[str, Any]]
