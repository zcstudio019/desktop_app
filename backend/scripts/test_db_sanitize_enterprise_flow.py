"""Smoke test database sanitization for large enterprise_flow payloads.

Run:
    python backend/scripts/test_db_sanitize_enterprise_flow.py

Set SAVE_DB_TEST=1 to also call the configured storage save_extraction method.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.utils.text_sanitize import sanitize_payload_for_db  # noqa: E402


def build_payload() -> dict:
    long_raw = "材料款\x00控制字符\x01" + "很长文本" * 300 + "😀"
    return {
        "document_type": "enterprise_flow",
        "normalized_document_type": "enterprise_bank_statement",
        "title": "企业流水分析报告😀",
        "markdown_summary": "# 企业流水分析报告\n\n含 emoji 😀 与 NUL \x00",
        "extracted_json": {
            "document_type": "enterprise_flow",
            "normalized_document_type": "enterprise_bank_statement",
            "summary": {"total_inflow": 100000.0, "total_outflow": 85000.0, "net_cashflow": 15000.0},
            "accounts": [{"bank_name": "测试银行", "account_number": "6222\x00", "transaction_count": 1}],
            "monthly_summary": [{"month": "2026-05", "inflow": 100000.0, "outflow": 85000.0}],
            "transactions": [
                {
                    "transaction_id": "tx-1",
                    "summary": "材料款😀",
                    "raw": {
                        "摘要": long_raw,
                        "空值": "",
                        "控制": "A\x02B",
                        "嵌套": {"备注": "正常中文😀\x00"},
                    },
                }
            ],
            "warnings": ["含特殊字符\x00"],
        },
        "data": {
            "transactions": [{"raw": {"备注": long_raw}}],
        },
    }


async def maybe_save_to_db(payload: dict) -> None:
    if os.getenv("SAVE_DB_TEST") != "1":
        print("SAVE_DB_TEST not set; skipped real DB save_extraction call.")
        return
    from backend.services import get_storage_service

    storage = get_storage_service()
    extraction_id = f"sanitize-test-{uuid.uuid4().hex}"
    await storage.save_extraction(
        {
            "extraction_id": extraction_id,
            "doc_id": f"sanitize-doc-{uuid.uuid4().hex}",
            "customer_id": "sanitize-test-customer",
            "extraction_type": "enterprise_flow",
            "extracted_data": payload,
            "confidence": 0.8,
            "extraction_status": "success",
            "skill_name": "sanitize_smoke",
        }
    )
    print(f"Saved extraction successfully: {extraction_id}")


def main() -> None:
    payload = build_payload()
    safe_payload = sanitize_payload_for_db(payload)
    dumped = json.dumps(safe_payload, ensure_ascii=False, default=str)
    assert "\x00" not in dumped
    assert "\x01" not in dumped
    assert "\x02" not in dumped
    tx_raw = safe_payload["extracted_json"]["transactions"][0]["raw"]
    assert len(tx_raw["摘要"]) <= 500
    assert safe_payload["extracted_json"]["summary"]["total_inflow"] == 100000.0
    print(f"sanitize ok, json_len={len(dumped)}, raw_keys={list(tx_raw.keys())}")
    asyncio.run(maybe_save_to_db(safe_payload))


if __name__ == "__main__":
    main()
