from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


def save_debug_artifacts(*, customer_id: str | None, payload: dict[str, Any]) -> str:
    if os.getenv("CREDIT_REPORT_AGENT_DEBUG", "false").lower() not in {"1", "true", "yes", "on"}:
        return ""
    root = Path("data") / "debug" / "credit_report_agent"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_customer = "".join(ch for ch in str(customer_id or "unknown") if ch.isalnum() or ch in {"-", "_"})[:64]
    out_dir = root / f"{stamp}_{safe_customer}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "agent_debug.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
