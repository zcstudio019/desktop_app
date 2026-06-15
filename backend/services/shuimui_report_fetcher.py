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
MIN_REPORT_TEXT_LENGTH = 80

REPORT_SUCCESS_KEYWORDS = (
    "水母报告",
    "企业名称",
    "统一信用代码",
    "当前法人姓名",
    "报告创建时间",
    "基本信息",
    "纳税信息",
    "发票信息",
    "供应商信息",
    "股东明细",
    "社保人数",
    "法人/股东变更",
)

AUTH_BLOCK_KEYWORDS = (
    "请登录",
    "登录后查看",
    "未登录",
    "无权访问",
    "暂无权限",
    "请完成企业授权",
    "授权后查看",
    "需要企业授权",
    "验证码",
    "认证后查看",
)

ERROR_MESSAGES = {
    "INVALID_DOMAIN": "当前仅支持 shuimui.szsmjr.com 的水母报告链接。",
    "INVALID_SCHEME": "当前仅支持 https 的水母报告链接。",
    "MISSING_SN": "未识别到水母报告编号，请确认链接是否完整。",
    "INVALID_SN": "水母报告编号格式不正确，请确认链接是否完整。",
    "NETWORK_ERROR": "服务器无法访问水母报告域名。",
    "NEED_AUTH": "当前水母报告需要登录或企业授权后才能查看。",
    "EXPIRED": "水母报告链接可能已过期。",
    "EMPTY_CONTENT": "页面已打开，但未读取到有效报告内容。",
    "PLAYWRIGHT_UNAVAILABLE": "当前链接为动态页面，服务器未安装 Playwright/Chromium，无法读取动态报告内容。",
}

LEGACY_ERROR_ALIASES = {
    "invalid_domain": "INVALID_DOMAIN",
    "invalid_scheme": "INVALID_SCHEME",
    "missing_sn": "MISSING_SN",
    "invalid_sn": "INVALID_SN",
    "link_unreachable": "NETWORK_ERROR",
    "auth_required": "NEED_AUTH",
    "empty_content": "EMPTY_CONTENT",
    "playwright_unavailable": "PLAYWRIGHT_UNAVAILABLE",
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


def _normalize_error_code(code: str) -> str:
    raw = str(code or "").strip()
    return LEGACY_ERROR_ALIASES.get(raw, raw.upper())


def _failure(source_url: str, sn: str, code: str) -> ShuimuiFetchResult:
    normalized_code = _normalize_error_code(code)
    return ShuimuiFetchResult(
        success=False,
        sn=sn,
        source_url=source_url,
        error_code=normalized_code,
        error_message=ERROR_MESSAGES.get(normalized_code, "水母报告链接读取失败。"),
    )


def _log_fetch_event(
    *,
    source_url: str,
    parsed_sn: str,
    fetch_method: str,
    http_status_code: int = 0,
    html_length: int = 0,
    raw_text_length: int = 0,
    error_code: str = "",
    error_message: str = "",
    page_title: str = "",
    matched_success_keywords: list[str] | None = None,
    matched_auth_block_keywords: list[str] | None = None,
    final_status: str = "",
) -> None:
    """Log fetch diagnostics without report body."""
    logger.info(
        "[ShuimuiFetch] source_url=%s parsed_sn=%s fetch_method=%s http_status_code=%s html_length=%s raw_text_length=%s page_title=%s matched_success_keywords=%s matched_auth_block_keywords=%s final_status=%s error_code=%s error_message=%s",
        source_url,
        parsed_sn,
        fetch_method,
        http_status_code,
        html_length,
        raw_text_length,
        page_title,
        ",".join(matched_success_keywords or []),
        ",".join(matched_auth_block_keywords or []),
        final_status,
        error_code,
        (error_message or "")[:200],
    )


def parse_shuimui_sn(source_url: str) -> str:
    """Parse sn from normal query string or hash-route query string."""
    parsed = urlparse(str(source_url or "").strip())
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() != "https":
        raise ValueError(ERROR_MESSAGES["INVALID_SCHEME"])
    if host != ALLOWED_HOST:
        raise ValueError(ERROR_MESSAGES["INVALID_DOMAIN"])

    candidates: list[str] = []
    candidates.extend(parse_qs(parsed.query).get("sn", []))
    if parsed.fragment:
        fragment_query = parsed.fragment.split("?", 1)[1] if "?" in parsed.fragment else parsed.fragment
        candidates.extend(parse_qs(fragment_query).get("sn", []))

    sn = next((item.strip() for item in candidates if item and item.strip()), "")
    if not sn:
        raise ValueError(ERROR_MESSAGES["MISSING_SN"])
    if not SN_PATTERN.fullmatch(sn):
        raise ValueError(ERROR_MESSAGES["INVALID_SN"])
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


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def _looks_like_auth_required(text: str) -> bool:
    return bool(_matched_auth_block_keywords(text))


def _matched_auth_block_keywords(text: str) -> list[str]:
    compact = _compact_text(text)
    return [token for token in AUTH_BLOCK_KEYWORDS if token in compact]


def _looks_like_expired(text: str) -> bool:
    compact = _compact_text(text)
    return any(token in compact for token in ("过期", "已失效", "链接失效", "报告失效", "不存在", "无法查看"))


def _looks_like_report_text(text: str, sn: str) -> bool:
    return bool(_matched_success_keywords(text, sn))


def _matched_success_keywords(text: str, sn: str) -> list[str]:
    compact = _compact_text(text)
    matched = [token for token in REPORT_SUCCESS_KEYWORDS if token in compact]
    if sn and sn in compact:
        matched.append(sn)
    if len(matched) >= 2:
        return matched
    if len(compact) >= MIN_REPORT_TEXT_LENGTH:
        useful_tokens = ("水母", "报告", "企业", "发票", "税务", "司法", "风险", sn)
        fallback = [token for token in useful_tokens if token and token in compact]
        if len(fallback) >= 2:
            return fallback
    return []


async def _fetch_http_text(source_url: str) -> tuple[str, int, int]:
    headers = {
        "User-Agent": "Mozilla/5.0 ShuimuiReportReader/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    async with httpx.AsyncClient(follow_redirects=True, timeout=HTTP_TIMEOUT_SECONDS, headers=headers) as client:
        response = await client.get(source_url)
        html_text = response.text or ""
        if response.status_code in {401, 403}:
            return "__AUTH_REQUIRED__", response.status_code, len(html_text)
        if response.status_code >= 400:
            return "", response.status_code, len(html_text)
        return _html_to_text(html_text), response.status_code, len(html_text)


async def _fetch_playwright_text(source_url: str) -> tuple[str, str, str]:
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except Exception as exc:  # pragma: no cover - exercised by tests through monkeypatch
        return "", f"playwright_unavailable:{exc}", ""

    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(source_url, wait_until="networkidle", timeout=PLAYWRIGHT_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                await page.goto(source_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            try:
                await page.wait_for_function(
                    "() => document.body && document.body.innerText && document.body.innerText.trim().length > 80",
                    timeout=10_000,
                )
            except PlaywrightTimeoutError:
                await page.wait_for_timeout(800)
            title = await page.title()
            body_text = await page.locator("body").inner_text(timeout=5_000)
            table_text = await page.evaluate(
                """() => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const blocks = [];
                    document.querySelectorAll('table, [role="table"]').forEach((table) => {
                        const rows = Array.from(table.querySelectorAll('tr, [role="row"]'))
                            .map((row) => Array.from(row.querySelectorAll('th, td, [role="cell"], [role="columnheader"]'))
                                .map((cell) => normalize(cell.innerText || cell.textContent))
                                .filter(Boolean)
                                .join('\\t'))
                            .filter(Boolean);
                        if (rows.length) blocks.push(rows.join('\\n'));
                    });
                    document.querySelectorAll('[class*="table"], [class*="row"], [class*="list"], [class*="card"]').forEach((node) => {
                        const children = Array.from(node.children || [])
                            .map((child) => normalize(child.innerText || child.textContent))
                            .filter(Boolean);
                        if (children.length >= 2 && children.length <= 8) {
                            const row = children.join('\\t');
                            if (row.length <= 300 && !blocks.includes(row)) blocks.push(row);
                        }
                    });
                    return blocks.join('\\n');
                }"""
            )
            return "\n".join(part for part in (title, body_text, table_text) if part).strip(), "", title
    except Exception as exc:
        message = str(exc)
        if "Executable doesn't exist" in message or "playwright install" in message or "Chromium" in message:
            return "", f"playwright_unavailable:{exc}", ""
        return "", f"playwright_error:{exc}", ""
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass


async def fetch_shuimui_report(source_url: str) -> ShuimuiFetchResult:
    try:
        safe_url, sn = validate_shuimui_url(source_url)
    except ValueError as exc:
        message = str(exc)
        code = next((key for key, value in ERROR_MESSAGES.items() if value == message), "INVALID_DOMAIN")
        result = _failure(str(source_url or ""), "", code)
        _log_fetch_event(
            source_url=str(source_url or ""),
            parsed_sn="",
            fetch_method="validate",
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result

    http_error_message = ""
    try:
        http_text, status_code, html_length = await _fetch_http_text(safe_url)
    except httpx.HTTPError as exc:
        http_text, status_code, html_length = "", 0, 0
        http_error_message = str(exc)[:160]
        _log_fetch_event(
            source_url=safe_url,
            parsed_sn=sn,
            fetch_method="http",
            error_code="HTTP_ERROR",
            error_message=http_error_message,
        )
        # Some SPA report hosts reject non-browser clients. Try Playwright before
        # classifying the link as unreachable.

    _log_fetch_event(
        source_url=safe_url,
        parsed_sn=sn,
        fetch_method="http",
        http_status_code=status_code,
        html_length=html_length,
        raw_text_length=len(http_text or ""),
        matched_success_keywords=_matched_success_keywords(http_text, sn),
        matched_auth_block_keywords=_matched_auth_block_keywords(http_text),
    )

    http_success_keywords = _matched_success_keywords(http_text, sn)
    if http_success_keywords:
        _log_fetch_event(
            source_url=safe_url,
            parsed_sn=sn,
            fetch_method="http",
            http_status_code=status_code,
            html_length=html_length,
            raw_text_length=len(http_text),
            matched_success_keywords=http_success_keywords,
            matched_auth_block_keywords=_matched_auth_block_keywords(http_text),
            final_status="success",
        )
        return ShuimuiFetchResult(True, sn, safe_url, raw_text=http_text)

    if http_text == "__AUTH_REQUIRED__" or (http_text and _looks_like_auth_required(http_text)):
        result = _failure(safe_url, sn, "NEED_AUTH")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="http", http_status_code=status_code, html_length=html_length, raw_text_length=0 if http_text == "__AUTH_REQUIRED__" else len(http_text), matched_auth_block_keywords=_matched_auth_block_keywords(http_text), final_status="failed", error_code=result.error_code, error_message=result.error_message)
        return result
    if http_text and _looks_like_expired(http_text):
        result = _failure(safe_url, sn, "EXPIRED")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="http", http_status_code=status_code, html_length=html_length, raw_text_length=len(http_text), error_code=result.error_code, error_message=result.error_message)
        return result
    if status_code >= 400:
        result = _failure(safe_url, sn, "NETWORK_ERROR")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="http", http_status_code=status_code, html_length=html_length, raw_text_length=len(http_text or ""), error_code=result.error_code, error_message=result.error_message)
        return result

    playwright_text, playwright_error, page_title = await _fetch_playwright_text(safe_url)
    playwright_success_keywords = _matched_success_keywords(playwright_text, sn)
    playwright_auth_keywords = _matched_auth_block_keywords(playwright_text)
    _log_fetch_event(
        source_url=safe_url,
        parsed_sn=sn,
        fetch_method="playwright",
        http_status_code=status_code,
        html_length=html_length,
        raw_text_length=len(playwright_text or ""),
        page_title=page_title,
        matched_success_keywords=playwright_success_keywords,
        matched_auth_block_keywords=playwright_auth_keywords,
        error_code="PLAYWRIGHT_ERROR" if playwright_error else "",
        error_message=playwright_error or http_error_message,
    )
    if playwright_success_keywords:
        _log_fetch_event(
            source_url=safe_url,
            parsed_sn=sn,
            fetch_method="playwright",
            http_status_code=status_code,
            html_length=html_length,
            raw_text_length=len(playwright_text or ""),
            page_title=page_title,
            matched_success_keywords=playwright_success_keywords,
            matched_auth_block_keywords=playwright_auth_keywords,
            final_status="success",
        )
        return ShuimuiFetchResult(True, sn, safe_url, raw_text=playwright_text)
    if playwright_text and _looks_like_auth_required(playwright_text):
        result = _failure(safe_url, sn, "NEED_AUTH")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="playwright", http_status_code=status_code, html_length=html_length, raw_text_length=len(playwright_text or ""), page_title=page_title, matched_success_keywords=playwright_success_keywords, matched_auth_block_keywords=playwright_auth_keywords, final_status="failed", error_code=result.error_code, error_message=result.error_message)
        return result
    if playwright_text and _looks_like_expired(playwright_text):
        result = _failure(safe_url, sn, "EXPIRED")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="playwright", http_status_code=status_code, html_length=html_length, raw_text_length=len(playwright_text or ""), page_title=page_title, matched_success_keywords=playwright_success_keywords, matched_auth_block_keywords=playwright_auth_keywords, final_status="failed", error_code=result.error_code, error_message=result.error_message)
        return result
    if playwright_error.startswith("playwright_unavailable"):
        result = _failure(safe_url, sn, "PLAYWRIGHT_UNAVAILABLE")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="playwright", http_status_code=status_code, html_length=html_length, raw_text_length=len(playwright_text or ""), page_title=page_title, matched_success_keywords=playwright_success_keywords, matched_auth_block_keywords=playwright_auth_keywords, final_status="failed", error_code=result.error_code, error_message=result.error_message)
        return result
    if playwright_error:
        result = _failure(safe_url, sn, "NETWORK_ERROR")
        _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="playwright", http_status_code=status_code, html_length=html_length, raw_text_length=len(playwright_text or ""), page_title=page_title, matched_success_keywords=playwright_success_keywords, matched_auth_block_keywords=playwright_auth_keywords, final_status="failed", error_code=result.error_code, error_message=result.error_message)
        return result
    result = _failure(safe_url, sn, "EMPTY_CONTENT")
    _log_fetch_event(source_url=safe_url, parsed_sn=sn, fetch_method="playwright", http_status_code=status_code, html_length=html_length, raw_text_length=len(playwright_text or ""), page_title=page_title, matched_success_keywords=playwright_success_keywords, matched_auth_block_keywords=playwright_auth_keywords, final_status="failed", error_code=result.error_code, error_message=result.error_message)
    return result
