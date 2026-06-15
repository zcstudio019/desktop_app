import pytest

from backend.services.extraction_skills.shuimui_report import extract_shuimui_report
from backend.services.shuimui_report_fetcher import fetch_shuimui_report, parse_shuimui_sn


SAMPLE_URL = "https://shuimui.szsmjr.com/index.html#/query/result?sn=S_207001c4662c4d9ab17881b3051f62ab"


def test_parse_sn_from_hash_route():
    assert parse_shuimui_sn(SAMPLE_URL) == "S_207001c4662c4d9ab17881b3051f62ab"


def test_reject_non_shuimui_domain():
    with pytest.raises(ValueError, match="当前仅支持 shuimui.szsmjr.com"):
        parse_shuimui_sn("https://example.com/index.html#/query/result?sn=S_123")


def test_missing_sn_has_clear_error():
    with pytest.raises(ValueError, match="未识别到水母报告编号"):
        parse_shuimui_sn("https://shuimui.szsmjr.com/index.html#/query/result")


@pytest.mark.asyncio
async def test_http_empty_shell_falls_back_to_playwright(monkeypatch):
    async def fake_http(_url: str):
        return "root app", 200, 1024

    async def fake_playwright(_url: str):
        return (
            "水母报告\n企业名称：上海测试有限公司\n统一社会信用代码：91310000MA1TEST123\n"
            "报告编号：S_207001c4662c4d9ab17881b3051f62ab\n税务 发票 风险",
            "",
        )

    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_http_text", fake_http)
    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_playwright_text", fake_playwright)

    result = await fetch_shuimui_report(SAMPLE_URL)

    assert result.success is True
    assert "上海测试有限公司" in result.raw_text


@pytest.mark.asyncio
async def test_playwright_unavailable_returns_clear_error(monkeypatch):
    async def fake_http(_url: str):
        return "root app", 200, 1024

    async def fake_playwright(_url: str):
        return "", "playwright_unavailable:No module named playwright"

    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_http_text", fake_http)
    monkeypatch.setattr("backend.services.shuimui_report_fetcher._fetch_playwright_text", fake_playwright)

    result = await fetch_shuimui_report(SAMPLE_URL)

    assert result.success is False
    assert result.error_code == "PLAYWRIGHT_UNAVAILABLE"
    assert "服务器未安装 Playwright/Chromium" in result.error_message


def test_shuimui_markdown_is_chinese_structured_not_json():
    content = extract_shuimui_report(
        "水母报告\n企业名称：上海测试有限公司\n统一社会信用代码：91310000MA1TEST123\n法定代表人：张三",
        source_url=SAMPLE_URL,
        sn="S_207001c4662c4d9ab17881b3051f62ab",
        ai_service=None,
    )

    markdown = content["report_markdown"]
    assert markdown.startswith("## 水母报告")
    assert "### 企业基本信息" in markdown
    assert "* 企业名称：上海测试有限公司" in markdown
    assert "```json" not in markdown
    assert '"doc_type"' not in markdown
    assert "raw_text" not in markdown
