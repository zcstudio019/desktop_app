"""Fetch and read Shuimui report links supplied by users."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import html
import json
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
TAB_DEFINITIONS: tuple[tuple[str, str], ...] = (
    ("basic_info", "基本信息"),
    ("tax_info", "纳税信息"),
    ("invoice_info", "发票信息"),
    ("supplier_info", "供应商信息"),
)
CAPTURE_JSON_START = "__SHUIMUI_REPORT_CAPTURE_JSON__"
CAPTURE_JSON_END = "__END_SHUIMUI_REPORT_CAPTURE_JSON__"

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


def _log_tab_capture_event(
    *,
    parsed_sn: str,
    opened_url: str,
    page_title: str,
    clicked_tabs: list[str],
    tab_text_lengths: dict[str, int],
    captured_api_count: int,
    captured_json_api_urls: list[str],
) -> None:
    logger.info(
        "[ShuimuiFetchTabs] parsed_sn=%s opened_url=%s page_title=%s clicked_tabs=%s basic_info_text_length=%s tax_info_text_length=%s invoice_info_text_length=%s supplier_info_text_length=%s captured_api_count=%s captured_json_api_urls=%s",
        parsed_sn,
        opened_url,
        page_title,
        ",".join(clicked_tabs),
        tab_text_lengths.get("basic_info", 0),
        tab_text_lengths.get("tax_info", 0),
        tab_text_lengths.get("invoice_info", 0),
        tab_text_lengths.get("supplier_info", 0),
        captured_api_count,
        ",".join(captured_json_api_urls[:20]),
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


async def _extract_dom_text_and_tables(page: object) -> tuple[str, str]:
    await page.evaluate("() => window.scrollTo(0, document.body ? document.body.scrollHeight : 0)")
    await page.wait_for_timeout(600)
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
    return str(body_text or ""), str(table_text or "")


async def _count_table_like_rows(page: object) -> int:
    try:
        return int(
            await page.evaluate(
                """() => {
                    let count = 0;
                    document.querySelectorAll('table, [role="table"]').forEach((table) => {
                        count += table.querySelectorAll('tr, [role="row"]').length;
                    });
                    document.querySelectorAll('[class*="table"], [class*="row"]').forEach((node) => {
                        if ((node.innerText || '').trim()) count += 1;
                    });
                    return count;
                }"""
            )
        )
    except Exception:
        return 0


async def _expand_current_tax_tab(page: object, source_url: str) -> dict[str, int | bool]:
    stats: dict[str, int | bool] = {
        "tax_tab_opened": True,
        "expand_buttons_found": 0,
        "expand_buttons_clicked": 0,
        "tax_tables_count_before_expand": await _count_table_like_rows(page),
        "tax_tables_count_after_expand": 0,
    }
    try:
        await page.evaluate("() => window.scrollTo(0, 0)")
        await page.wait_for_timeout(500)
    except Exception:
        pass

    for _ in range(3):
        try:
            locator = page.get_by_text("展开", exact=True)
            count = await locator.count()
            stats["expand_buttons_found"] = int(stats["expand_buttons_found"]) + count
        except Exception:
            count = 0
        clicked_this_round = 0
        for index in range(count):
            try:
                button = locator.nth(index)
                if not await button.is_visible(timeout=1_000):
                    continue
                before_rows = await _count_table_like_rows(page)
                await button.scroll_into_view_if_needed(timeout=2_000)
                await button.click(timeout=2_000)
                await page.wait_for_timeout(900)
                try:
                    await page.wait_for_function(
                        "(before) => document.querySelectorAll('tr, [role=\"row\"]').length !== before",
                        arg=before_rows,
                        timeout=1_500,
                    )
                except Exception:
                    pass
                stats["expand_buttons_clicked"] = int(stats["expand_buttons_clicked"]) + 1
                clicked_this_round += 1
            except Exception as exc:
                logger.warning("[ShuimuiFetchTax] expand click failed url=%s error=%s", source_url, str(exc)[:160])
        if clicked_this_round == 0:
            break
    try:
        await page.evaluate("() => window.scrollTo(0, document.body ? document.body.scrollHeight : 0)")
        await page.wait_for_timeout(800)
    except Exception:
        pass
    stats["tax_tables_count_after_expand"] = await _count_table_like_rows(page)
    logger.info(
        "[ShuimuiFetchTax] tax_tab_opened=%s expand_buttons_found=%s expand_buttons_clicked=%s tax_tables_count_before_expand=%s tax_tables_count_after_expand=%s",
        stats["tax_tab_opened"],
        stats["expand_buttons_found"],
        stats["expand_buttons_clicked"],
        stats["tax_tables_count_before_expand"],
        stats["tax_tables_count_after_expand"],
    )
    return stats


async def _visible_modal_text(page: object) -> str:
    try:
        result = await page.evaluate(
            """() => {
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const selectors = [
                    '[role="dialog"]',
                    '.el-dialog',
                    '.ant-modal',
                    '[class*="popup"]',
                    '[class*="dialog"]',
                    '[class*="modal"]'
                ];
                const nodes = selectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
                for (let i = nodes.length - 1; i >= 0; i -= 1) {
                    const node = nodes[i];
                    const style = window.getComputedStyle(node);
                    const rect = node.getBoundingClientRect();
                    const visible = style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                    const text = normalize(node.innerText || node.textContent);
                    if (visible && /详情信息|详细信息|税务处罚详情|违法违章/.test(text)) return { text, source: 'dialog' };
                }
                const bodyText = normalize(document.body ? document.body.innerText : '');
                const start = bodyText.search(/详情信息|详细信息|税务处罚详情/);
                if (start >= 0) {
                    const tail = bodyText.slice(start);
                    const closeIndex = tail.indexOf('关闭');
                    return { text: closeIndex >= 0 ? tail.slice(0, closeIndex) : tail.slice(0, 800), source: 'body_slice' };
                }
                return { text: '', source: '' };
            }"""
        )
        if isinstance(result, dict):
            text = str(result.get("text") or "").strip()
            source = str(result.get("source") or "")
        else:
            text = str(result or "").strip()
            source = ""
        logger.info(
            "[ShuimuiFetchTaxPenalty] tax_penalty_modal_scope_found=%s tax_penalty_modal_text_length=%s tax_penalty_modal_text_source=%s",
            bool(text),
            len(text),
            source,
        )
        return text
    except Exception:
        return ""


async def _close_visible_modal(page: object) -> None:
    for label in ("关闭", "确定", "取消"):
        try:
            locator = page.get_by_text(label, exact=True)
            count = await locator.count()
            for index in range(count):
                button = locator.nth(index)
                if await button.is_visible(timeout=500):
                    await button.click(timeout=1_500)
                    await page.wait_for_timeout(400)
                    return
        except Exception:
            continue
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)
    except Exception:
        pass


async def _capture_tax_penalty_details(page: object, source_url: str) -> tuple[list[str], dict[str, int]]:
    details: list[str] = []
    stats = {
        "tax_penalty_rows_count": 0,
        "tax_penalty_detail_buttons_found": 0,
        "tax_penalty_detail_buttons_clicked": 0,
        "tax_penalty_modals_opened": 0,
        "tax_penalty_detail_click_failed_count": 0,
    }
    try:
        candidates = page.get_by_text("查看", exact=True)
        count = await candidates.count()
    except Exception:
        count = 0
    stats["tax_penalty_detail_buttons_found"] = count

    for index in range(count):
        try:
            button = candidates.nth(index)
            if not await button.is_visible(timeout=700):
                continue
            context = await button.evaluate(
                """(el) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    let node = el;
                    for (let i = 0; node && i < 6; i += 1, node = node.parentElement) {
                        const text = normalize(node.innerText || node.textContent);
                        if (text.length > 8) return text;
                    }
                    return normalize(el.innerText || el.textContent);
                }"""
            )
            context_text = str(context or "")
            if "查看详细路径" in context_text:
                continue
            if not any(token in context_text for token in ("违法违章", "登记待处理", "责令限期", "详细信息")):
                continue
            stats["tax_penalty_rows_count"] += 1
            await button.scroll_into_view_if_needed(timeout=2_000)
            await button.click(timeout=2_000)
            stats["tax_penalty_detail_buttons_clicked"] += 1
            await page.wait_for_timeout(800)
            try:
                await page.wait_for_function(
                    "() => document.body && /详情信息|详细信息|税务处罚详情/.test(document.body.innerText || '')",
                    timeout=3_000,
                )
            except Exception:
                pass
            modal_text = await _visible_modal_text(page)
            if modal_text:
                stats["tax_penalty_modals_opened"] += 1
                details.append(modal_text)
            else:
                stats["tax_penalty_detail_click_failed_count"] += 1
            await _close_visible_modal(page)
        except Exception as exc:
            stats["tax_penalty_detail_click_failed_count"] += 1
            logger.warning("[ShuimuiFetchTaxPenalty] detail click failed url=%s error=%s", source_url, str(exc)[:160])
            try:
                await _close_visible_modal(page)
            except Exception:
                pass
    logger.info(
        "[ShuimuiFetchTaxPenalty] tax_penalty_rows_count=%s tax_penalty_detail_buttons_found=%s tax_penalty_detail_buttons_clicked=%s tax_penalty_modals_opened=%s tax_penalty_detail_click_failed_count=%s",
        stats["tax_penalty_rows_count"],
        stats["tax_penalty_detail_buttons_found"],
        stats["tax_penalty_detail_buttons_clicked"],
        stats["tax_penalty_modals_opened"],
        stats["tax_penalty_detail_click_failed_count"],
    )
    return details, stats


def _json_field_summary(value: object, max_items: int = 20) -> list[str]:
    keys: list[str] = []

    def visit(item: object) -> None:
        if len(keys) >= max_items:
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                text = str(key)
                if text not in keys:
                    keys.append(text)
                visit(nested)
                if len(keys) >= max_items:
                    return
        elif isinstance(item, list):
            for nested in item[:3]:
                visit(nested)
                if len(keys) >= max_items:
                    return

    visit(value)
    return keys


async def _click_report_tab(page: object, tab_label: str) -> bool:
    candidates = [
        lambda: page.get_by_text(tab_label, exact=True).first,
        lambda: page.locator(f"text={tab_label}").first,
        lambda: page.locator(f"button:has-text('{tab_label}'), [role=tab]:has-text('{tab_label}'), div:has-text('{tab_label}')").first,
    ]
    for make_locator in candidates:
        try:
            locator = make_locator()
            await locator.click(timeout=5_000)
            return True
        except Exception:
            continue
    return False


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
            captured_json_apis: list[dict[str, object]] = []

            async def capture_response(response: object) -> None:
                try:
                    parsed = urlparse(response.url)
                    if (parsed.hostname or "").lower() != ALLOWED_HOST:
                        return
                    content_type = str(response.headers.get("content-type") or "").lower()
                    if "json" not in content_type:
                        return
                    payload = await response.json()
                    captured_json_apis.append(
                        {
                            "url": response.url,
                            "status": response.status,
                            "field_summary": _json_field_summary(payload),
                            "payload": payload,
                        }
                    )
                except Exception as exc:
                    logger.info("[ShuimuiFetchAPI] capture failed url=%s error=%s", getattr(response, "url", ""), str(exc)[:160])

            page.on("response", lambda response: asyncio.create_task(capture_response(response)))
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
            sections: dict[str, dict[str, str]] = {}
            clicked_tabs: list[str] = []
            tax_expand_stats: dict[str, int | bool] = {}
            tax_penalty_detail_stats: dict[str, int] = {}

            for tab_key, tab_label in TAB_DEFINITIONS:
                before_text = ""
                if tab_key != "basic_info":
                    try:
                        before_text = await page.locator("body").inner_text(timeout=3_000)
                    except Exception:
                        before_text = ""
                    clicked = await _click_report_tab(page, tab_label)
                    if clicked:
                        clicked_tabs.append(tab_label)
                    else:
                        logger.warning("[ShuimuiFetchTabs] tab click failed label=%s url=%s", tab_label, source_url)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8_000)
                    except PlaywrightTimeoutError:
                        pass
                    if before_text:
                        try:
                            await page.wait_for_function(
                                "(previous) => document.body && document.body.innerText && document.body.innerText !== previous",
                                arg=before_text,
                                timeout=8_000,
                            )
                        except PlaywrightTimeoutError:
                            logger.warning("[ShuimuiFetchTabs] tab content unchanged label=%s url=%s", tab_label, source_url)
                    await page.wait_for_timeout(1_200)

                if tab_key == "tax_info":
                    tax_expand_stats = await _expand_current_tax_tab(page, source_url)
                    tax_penalty_details, tax_penalty_detail_stats = await _capture_tax_penalty_details(page, source_url)
                else:
                    tax_penalty_details = []

                body_text, table_text = await _extract_dom_text_and_tables(page)
                if tab_key == "tax_info" and tax_penalty_details:
                    detail_text = "\n\n".join(f"税务处罚详情\n{item}" for item in tax_penalty_details if item)
                    body_text = "\n\n".join(part for part in (body_text, detail_text) if part)
                sections[tab_key] = {
                    "label": tab_label,
                    "text": body_text,
                    "tables_text": table_text,
                    "expand_stats": tax_expand_stats if tab_key == "tax_info" else {},
                    "tax_penalty_detail_stats": tax_penalty_detail_stats if tab_key == "tax_info" else {},
                }

            captured_urls = [str(item.get("url") or "") for item in captured_json_apis]
            _log_tab_capture_event(
                parsed_sn=parse_shuimui_sn(source_url),
                opened_url=source_url,
                page_title=title,
                clicked_tabs=clicked_tabs,
                tab_text_lengths={key: len(value.get("text") or "") for key, value in sections.items()},
                captured_api_count=len(captured_json_apis),
                captured_json_api_urls=captured_urls,
            )
            capture_payload = {
                "sections": sections,
                "api_json": captured_json_apis,
                "clicked_tabs": clicked_tabs,
                "page_title": title,
                "tax_expand_stats": tax_expand_stats,
                "tax_penalty_detail_stats": tax_penalty_detail_stats,
            }
            parts = [
                title,
                CAPTURE_JSON_START,
                json.dumps(capture_payload, ensure_ascii=False, default=str),
                CAPTURE_JSON_END,
            ]
            for tab_key, tab_label in TAB_DEFINITIONS:
                section = sections.get(tab_key) or {}
                text = "\n".join(part for part in (section.get("text"), section.get("tables_text")) if part).strip()
                if text:
                    parts.extend([f"### 页签：{tab_label}", text])
            return "\n\n".join(part for part in parts if part).strip(), "", title
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
