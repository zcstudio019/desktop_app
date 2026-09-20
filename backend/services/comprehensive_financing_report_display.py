"""Presentation-only wording and formatting for comprehensive reports."""

from __future__ import annotations

import re
from decimal import Decimal
from html.parser import HTMLParser
from typing import Any, Iterable


_TECHNICAL_TERMS = (
    "KYC", "count", "partial", "available", "missing", "confirmed",
    "needs_review", "potential", "conditional", "insufficient_data",
    "source_sections", "derived_metrics", "data_quality", "risk_context",
    "existing_financing_plan",
)
_TECHNICAL_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])(?:" + "|".join(map(re.escape, _TECHNICAL_TERMS)) + r")(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_MONEY_YUAN = re.compile(r"(?<![\d,.])(?P<amount>-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*元")
_DEBT_RATIO = re.compile(r"(?P<prefix>资产负债率\s*(?:为|约|达|达到|是|[:：])?\s*)(?P<value>\d+(?:\.\d+)?)(?P<suffix>\s*%?)")

_FIELD_LABELS = {
    "subject_profile": "主体及身份资料",
    "enterprise_credit": "企业征信", "personal_credit": "个人征信",
    "enterprise_cashflow": "企业流水", "personal_cashflow": "个人流水",
    "financials": "财务报表", "assets": "资产资料",
    "financing_requirement": "融资需求", "derived_metrics": "程序计算指标",
    "data_quality": "资料完整度", "risk_context": "风险评估结果",
    "existing_financing_plan": "已有融资方案",
    "source_sections": "数据来源",
}
_STATUS_LABELS = {
    "needs_review": "待核验", "insufficient_data": "资料不足",
    "conditional": "有条件", "confirmed": "已确认",
    "available": "已获取", "potential": "可进一步评估",
    "partial": "部分资料", "missing": "资料不足",
}


class _VisibleText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"style", "script"}:
            self.hidden += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"style", "script"} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def assert_no_internal_english_terms(text: str, *, html: bool = False) -> None:
    """Reject internal vocabulary in the final user-visible report."""
    if html:
        parser = _VisibleText()
        parser.feed(text)
        text = " ".join(parser.parts)
    match = _TECHNICAL_PATTERN.search(text)
    if match:
        raise ValueError(f"综合报告展示层含内部词：{match.group()}")


def localize_report_text(
    text: str, *, debt_asset_ratio: Any = None,
    debt_asset_ratios: Iterable[Any] = (), html: bool = False,
) -> str:
    """Change presentation only; do not add a fact or alter an unknown value."""
    text = text.replace("KYC主体资料", "主体及身份资料")
    text = re.sub(r"企业征信逾期概要\s*count\s*(?:为|是|[:：=])?\s*(\d+)",
                  r"企业征信逾期记录数为\1", text, flags=re.IGNORECASE)
    text = re.sub(r"企业流水为\s*partial\s*状态",
                  "企业流水可用于初步分析，但部分交易分类仍需进一步核验", text, flags=re.IGNORECASE)
    text = text.replace("主体科技企业标签为空", "当前未获取可核验的科技企业资质或相关认定资料")
    for field, label in _FIELD_LABELS.items():
        text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(field)}(?![A-Za-z0-9_])", label, text, flags=re.IGNORECASE)
    for status, label in _STATUS_LABELS.items():
        text = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(status)}(?![A-Za-z0-9_])", label, text, flags=re.IGNORECASE)
    text = re.sub(r"(?<![A-Za-z0-9_])KYC(?![A-Za-z0-9_])", "主体及身份资料", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<![A-Za-z0-9_])count(?![A-Za-z0-9_])", "记录数", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<![A-Za-z0-9_])(?:null|None)(?![A-Za-z0-9_])|\[\]", "资料不足", text)
    text = re.sub(r"(?<![A-Za-z0-9_])PDF(?![A-Za-z0-9_])", "报告文件", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<![A-Za-z0-9_])BIM(?![A-Za-z0-9_])", "建筑信息模型", text, flags=re.IGNORECASE)

    def money(match: re.Match[str]) -> str:
        return f"{Decimal(match.group('amount').replace(',', '')):,.2f}元"

    text = _MONEY_YUAN.sub(money, text)
    ratios = [float(value) for value in (debt_asset_ratio, *debt_asset_ratios)
              if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if ratios:

        def debt_ratio(match: re.Match[str]) -> str:
            value = float(match.group("value"))
            if match.group("suffix").strip() == "%":
                return match.group(0)
            for ratio in ratios:
                if abs(value - ratio) < 0.00005 or abs(value - ratio * 100) < 0.005:
                    return f"{match.group('prefix')}{ratio * 100:,.2f}%"
            return match.group(0)

        text = _DEBT_RATIO.sub(debt_ratio, text)
    assert_no_internal_english_terms(text, html=html)
    return text
