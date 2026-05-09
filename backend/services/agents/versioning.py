from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AGENT_VERSION = "agent-platform-v1"
SNAPSHOT_VERSION = "v1"


def build_agent_version_fingerprint() -> dict[str, Any]:
    root = _repo_root()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_value(["rev-parse", "HEAD"], root),
        "git_branch": _git_value(["rev-parse", "--abbrev-ref", "HEAD"], root),
        "python_version": platform.python_version(),
        "app_version": _package_version(root),
        "agent_version": AGENT_VERSION,
        "model_config": {
            "provider": os.getenv("AGENT_LLM_PROVIDER") or "AIService",
            "model": os.getenv("AGENT_LLM_MODEL") or "deepseek-chat",
            "temperature": _env_float("AGENT_LLM_TEMPERATURE"),
            "max_tokens": _env_int("AGENT_LLM_MAX_TOKENS") or 4096,
        },
        "prompt_hashes": _prompt_hashes(),
        "schema_hashes": _schema_hashes(),
        "rule_hashes": {
            "RiskAgent": _file_hash(root / "backend" / "services" / "agents" / "risk_agent.py"),
            "CreditAnalysisAgent": _file_hash(root / "backend" / "services" / "agents" / "credit_analysis_agent.py"),
            "MissingMaterialAgent": _file_hash(root / "backend" / "services" / "agents" / "missing_material_agent.py"),
        },
        "compliance_guard_hash": _file_hash(root / "backend" / "services" / "agents" / "compliance_guard.py"),
        "snapshot_version": SNAPSHOT_VERSION,
    }


def stable_fingerprint_subset(fingerprint: dict[str, Any]) -> dict[str, Any]:
    return {
        "prompt_hashes": fingerprint.get("prompt_hashes") or {},
        "schema_hashes": fingerprint.get("schema_hashes") or {},
        "rule_hashes": fingerprint.get("rule_hashes") or {},
        "model_config": fingerprint.get("model_config") or {},
        "compliance_guard_hash": fingerprint.get("compliance_guard_hash") or "unknown",
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _file_hash(path: Path) -> str:
    try:
        return _sha(path.read_text(encoding="utf-8"))
    except Exception:
        return "unknown"


def _json_hash(value: Any) -> str:
    try:
        return _sha(json.dumps(value, ensure_ascii=False, sort_keys=True))
    except Exception:
        return "unknown"


def _prompt_hashes() -> dict[str, str]:
    try:
        from .prompts import FINANCING_JUDGEMENT_AGENT_SYSTEM_PROMPT, RISK_AGENT_SYSTEM_PROMPT

        return {
            "RiskAgent": _sha(RISK_AGENT_SYSTEM_PROMPT),
            "FinancingJudgementAgent": _sha(FINANCING_JUDGEMENT_AGENT_SYSTEM_PROMPT),
        }
    except Exception:
        return {"RiskAgent": "unknown", "FinancingJudgementAgent": "unknown"}


def _schema_hashes() -> dict[str, str]:
    try:
        from .financing_judgement_agent import JUDGEMENT_SCHEMA
        from .risk_agent import RISK_SCHEMA

        return {
            "RiskAgent": _json_hash(RISK_SCHEMA),
            "FinancingJudgementAgent": _json_hash(JUDGEMENT_SCHEMA),
        }
    except Exception:
        return {"RiskAgent": "unknown", "FinancingJudgementAgent": "unknown"}


def _git_value(args: list[str], cwd: Path) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return (result.stdout or "").strip() or "unknown"
    except Exception:
        return "unknown"


def _package_version(root: Path) -> str:
    try:
        package_json = json.loads((root / "package.json").read_text(encoding="utf-8"))
        return str(package_json.get("version") or "unknown")
    except Exception:
        return "unknown"


def _env_int(name: str) -> int | None:
    try:
        value = os.getenv(name)
        return int(value) if value not in (None, "") else None
    except Exception:
        return None


def _env_float(name: str) -> float | None:
    try:
        value = os.getenv(name)
        return float(value) if value not in (None, "") else None
    except Exception:
        return None
