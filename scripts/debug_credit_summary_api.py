from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _get_json(url: str, token: str | None = None) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"request failed: {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug final customer profile credit_debug from API.")
    parser.add_argument("--customer-id", required=True)
    parser.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--token", default=os.getenv("AUTH_TOKEN"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    query = "?force=true" if args.force else ""
    url = f"{args.base_url.rstrip('/')}/api/customers/{args.customer_id}/profile-markdown{query}"
    payload = _get_json(url, args.token)
    debug = payload.get("credit_debug") or {}
    summary = {
        "parser_version": debug.get("parser_version"),
        "from_cache": debug.get("from_cache"),
        "force_reparse": debug.get("force_reparse"),
        "revolving_section_len": debug.get("revolving_section_len"),
        "extractor_called": debug.get("extractor_called") or debug.get("revolving_extractor_called"),
        "extractor_output_count": debug.get("extractor_output_count") or debug.get("revolving_extracted_count"),
        "after_final_normalize_revolving_count": debug.get("after_final_normalize_revolving_count"),
        "api_return_revolving_count": debug.get("api_return_revolving_count") or debug.get("revolving_returned_count"),
        "validation_warnings": debug.get("validation_warnings"),
        "frontend_expected_field": debug.get("frontend_expected_field"),
    }
    print(json.dumps({"credit_debug": debug, "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
