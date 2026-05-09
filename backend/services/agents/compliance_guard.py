from __future__ import annotations

import json
from typing import Any


BLOCKED_TERMS = [
    "包过",
    "必过",
    "必下款",
    "保证放款",
    "一定放款",
    "无视征信",
    "黑户可做",
    "最低利率",
    "秒批",
    "100%通过",
]

DISCLAIMER = "以上为资料初判，不构成贷款承诺，最终以银行审批为准。"


def check_financing_compliance(text_or_dict: Any) -> dict[str, Any]:
    text = json.dumps(text_or_dict, ensure_ascii=False) if not isinstance(text_or_dict, str) else text_or_dict
    blocked = [term for term in BLOCKED_TERMS if term in text]
    return {
        "passed": not blocked,
        "blocked_terms": blocked,
        "message": "" if not blocked else f"输出包含违规承诺词：{', '.join(blocked)}",
    }


def ensure_disclaimer(payload: dict[str, Any]) -> dict[str, Any]:
    payload["compliance_disclaimer"] = DISCLAIMER
    return payload
