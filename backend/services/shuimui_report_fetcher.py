"""Fetch and read Shuimui report links supplied by users."""

from __future__ import annotations

from dataclasses import dataclass
import html
import logging
import re
from urllib.parse import parse_qs, urlparse

import httpx

logger = logging.getLogger(__name__)

ALLOWED_HOST = "shuimui.szsmjr.com"
SN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
HTTP_TIMEOUT_SECONDS = 15.0
PLAYWRIGHT_TIMEOUT_MS = 30_000


ERROR_MESSAGES = {
    "invalid_domain": "当前仅支持 shuimui.szsmjr.com 的水母报告链接。",
    "invalid_scheme": "当前仅支持 https 的水母报告链接。",
    "missing_sn": "未识别到水母报告编号，请确认链接是否完整。",
    "invalid_sn": "水母报告编号格式不正确，请确认链接是否完整。",
    "link_unreachable": "水母报告链接无法访问，可能已过期或网络不可达。",
    "auth_required": "当前水母报告需要登录或企业授权后才能查看，系统无法直接读取。",
    "empty_content": "已打开链接，但未读取到有效报告内容。",
    "playwright_unavailable": "已读取到动态页面，但服务器未安装 Playwright，无法渲染水母报告页面。",
}


@dataclass(frozen=True)
class ShuimuiFetchResult:
    success: bool
    sn: str
    source_url: str
    raw_text: str = ""
    error_code: str = ""
    error_message: str = ""

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "success": self.success,
            "sn": self.sn,
            "source_url": self.source_url,
            "raw_text": self.raw_text,
            "error_code": self.error_code,
            "error_message": self.error_message,
        }


def _failure(source_url: str, sn: str, code: str) -> ShuimuiFetchResult:
    return ShuimuiFetchResult(
        success=False,
        sn=sn,
        source_url=source_url,
        error_code=code,
        error_message=ERROR_MESSAGES.get(code, "水母报告链接读取失败。"),
    )


def parse_shuimui_sn(source_url: str) -> str:
    """Parse sn from query string or hash-route query string."""
    parsed = urlparse(str(source_url or "").strip())
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() != "https":
        raise ValueError(ERROR_MESSAGES["invalid_scheme"])
    if host != ALLOWED_HOST:
        raise ValueError(ERROR_MESSAGES["invalid_domain"])

    candidates: list[str] = []
    candidates.extend(parse_qs(parsed.query).get("sn", []))
    if parsed.fragment:
        fragment = parsed.fragment
        fragment_query = fragment.split("?", 1)[1] if "?" in fragment else fragment
        candidates.extend(parse_qs(fragment_query).get("sn", []))

    sn = next((item.strip() for item in candidates if item and item.strip()), "")
    if not sn:
        raise ValueError(ERROR_MESSAGES["missing_sn"])
    if not SN_PATTERN.fullmatch(sn):
        raise ValueError(ERROR_MESSAGES["invalid_sn"])
    return sn


def validate_shuimui_url(source_url: str) -> tuple[str, str]:
    url = str(source_url or "").strip()
    sn = parse_shuimui_sn(url)
    return url, sn


def _html_to_text(markup: str) -> str:
    text = re.sub(r"(?is)<script\b[^>]*>.*?</script>", "\n", markup or "")
    text = re.sub(r"(?is)<style\b[^>]*>.*?</style>", "\n", text)
    text = re.sub(r"(?i)</?(?:tr|p|div|section|article|li|h[1-6]|table|thead|tbody|tfoot)[^>]*>", "\n", text)
    text = re.sub(r"(?i)</(?:td|th)>", "\t", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _looks_like_auth_required(text: str) -> bool:
    compact = re.sub(r"\s+", "", text or "")
    return any(token in compact for token in ("登录", "授权", "验证码", "无权限", "未授权", "请先登录", "访问受限"))


def _looks_like_report_text(text: str, sn: str) -> bool:
    compact = re.sub(r"\s+", "", text or "")
    if len(compact) < 80:
        return False
    useful_tokens = ("水母", "报告", "企业", "发票", "税务", "司法", "风险", sn)
    return sum(1 for token in useful_tokens if token and token in compact) >= 2


async def _fetch_http_text(source_url: str) -> tuple[str, int]:
    headers = {
        "User-Agent": "Mozilla/5.0 ShuimuiReportReader/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with httpx.AsyncClient(follow_redirects=True, timeout=HTTP_TIMEOUT_SECONDS, headers=headers) as client:
        response = await client.get(source_url)
        status_code = response.status_code
        if status_code in {401, 403}:
            return "__AUTH_REQUIRED__", status_code
        if status_code >= 400:
            return "", status_code
        return _html_to_text(response.text), status_code


async def _fetch_playwright_text(source_url: str) -> tuple[str, str]:
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except Exception as exc:  # pragma: no cover - exercised by tests through monkeypatch
        return "", f"playwright_unavailable:{exc}"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(source_url, wait_until="networkidle", timeout=PLAYWRIGHT_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                await page.goto(source_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            await page.wait_for_timeout(800)
            title = await page.title()
            body_text = await page.locator("body").inner_text(timeout=5_000)
            table_text = await page.locator("table, [role=table], .table, .card, .list, ul, ol").evaluate_all(
                "(nodes) => nodes.map((node) => node.innerText || '').join('\\n')"
            )
            await browser.close()
            return "\n".join(part for part in (title, body_text, table_text) if part).strip(), ""
    except Exception as exc:
        return "", f"playwright_error:{exc}"


async def fetch_shuimui_report(source_url: str) -> ShuimuiFetchResult:
    try:
        safe_url, sn = validate_shuimui_url(source_url)
    except ValueError as exc:
        message = str(exc)
        code = next((key for key, value in ERROR_MESSAGES.items() if value == message), "invalid_domain")
        return _failure(str(source_url or ""), "", code)

    try:
        http_text, status_code = await _fetch_http_text(safe_url)
    except httpx.HTTPError as exc:
        logger.info("[ShuimuiFetch] http failed sn=%s error=%s", sn, str(exc)[:160])
        http_text, status_code = "", 0

    if http_text == "__AUTH_REQUIRED__":
        return _failure(safe_url, sn, "auth_required")
    if http_text and _looks_like_auth_required(http_text):
        return _failure(safe_url, sn, "auth_required")
    if _looks_like_report_text(http_text, sn):
        return ShuimuiFetchResult(True, sn, safe_url, raw_text=http_text)

    if status_code >= 400:
        return _failure(safe_url, sn, "link_unreachable")

    playwright_text, playwright_error = await _fetch_playwright_text(safe_url)
    if playwright_text and _looks_like_auth_required(playwright_text):
        return _failure(safe_url, sn, "auth_required")
    if _looks_like_report_text(playwright_text, sn):
        return ShuimuiFetchResult(True, sn, safe_url, raw_text=playwright_text)
    if playwright_error.startswith("playwright_unavailable"):
        return _failure(safe_url, sn, "playwright_unavailable")
    if playwright_error:
        logger.info("[ShuimuiFetch] playwright failed sn=%s error=%s", sn, playwright_error[:160])
        return _failure(safe_url, sn, "link_unreachable")
    return _failure(safe_url, sn, "empty_content")
