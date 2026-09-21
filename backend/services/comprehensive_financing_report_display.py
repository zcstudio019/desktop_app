"""Presentation-only wording and formatting for comprehensive reports."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Any, Iterable


_TECHNICAL_TERMS = (
    "KYC", "count", "partial", "available", "missing", "confirmed",
    "needs_review", "potential", "conditional", "insufficient_data",
    "source_sections", "derived_metrics", "data_quality", "risk_context",
    "existing_financing_plan", "status",
)
_TECHNICAL_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_])(?:" + "|".join(map(re.escape, _TECHNICAL_TERMS)) + r")(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_MONEY_YUAN = re.compile(r"(?<![\d,.])(?P<amount>-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*元")
_DEBT_RATIO = re.compile(r"(?P<prefix>资产负债率\s*(?:为|约|达|达到|是|[:：])?\s*)(?P<value>\d+(?:\.\d+)?)(?P<suffix>\s*%?)")
_FINANCIAL_TREND_RATIO = re.compile(
    r"(?P<label>营业收入增长率|利润增长率|应收账款增长率)"
    r"(?P<link>\s*(?:为|是|约|[:：])?\s*)"
    r"(?P<value>-?\d+(?:\.\d+)?)(?P<percent>\s*%?)(?![\d.])"
)
_CASHFLOW_CHANGE = re.compile(
    r"(?P<label>经营现金流变化)(?P<link>\s*(?:为|是|约|[:：])?\s*)"
    r"(?P<value>-?\d+(?:\.\d+)?)(?![\d.])"
)
_TREND_FIELDS = {"营业收入增长率": "revenue_growth", "利润增长率": "profit_growth",
                 "应收账款增长率": "receivable_growth"}
_UNCERTAIN_CASHFLOW = (
    "按当前已保存分类口径，流出高于流入；但内部互转、关联关系及未识别交易尚未完全核验，"
    "当前不能据此直接判断企业真实经营现金流覆盖能力"
)
_CASHFLOW_IMPACT = (
    "当前流水分类尚未完全稳定，需要剔除内部互转并核验关联方交易后，"
    "再评价真实经营收支及现金流覆盖情况"
)
_MISSING_ASSET_DATA = "当前未获取可用于本次分析的稳定结构化资产资料"
_TECH_MATERIALS = "当前未获取可核验的科技企业资质、认定或相关证明资料"
_TECH_PATH_GAP = "需补充可核验的科技企业资质、认定或相关证明资料，并明确融资需求"
_QUERY_RULE_NOTE = "当前仅作事实展示，具体影响需结合拟申请机构正式准入规则评估"

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
    "status": "状态",
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


def display_path_missing_conditions(path: str, conditions: Iterable[str]) -> list[str]:
    """Use business wording for a technology path without changing its status or basis."""
    if path == "抵押融资":
        return [
            item.replace("资产清单及权属证明", "可核验的资产权属及可抵押状态资料")
                .replace("资产评估或价值依据", "可核验的资产估值依据")
            for item in conditions
        ]
    if path != "科技企业专项融资":
        return list(conditions)
    return [
        _TECH_PATH_GAP if "科技企业" in item and any(term in item for term in ("标签", "资质", "资格", "认定")) else item
        for item in conditions
    ]


def localize_report_text(
    text: str, *, debt_asset_ratio: Any = None,
    debt_asset_ratios: Iterable[Any] = (),
    financial_trends: dict[str, Any] | None = None,
    financial_unit: str | None = None,
    html: bool = False,
) -> str:
    """Change presentation only; do not add a fact or alter an unknown value."""
    text = text.replace(
        "逾期概要为零与连续企业流水构成有限信用基础，但高杠杆、流水净流出、"
        "个人相关还款责任规模显著及融资需求、个人流水、资产资料缺失，"
        "使融资推进需先补足关键材料并作有条件评估。",
        "企业及个人征信逾期概要为零，连续企业流水可供核验；资本结构、流水分类及法人相关还款责任仍需核验，"
        "融资需求、个人流水与稳定结构化资产资料待补充后，再作有条件评估。",
    )
    text = text.replace("KYC主体资料", "主体及身份资料")
    text = re.sub(r"企业征信逾期概要\s*count\s*(?:为|是|[:：=])?\s*(\d+)",
                  r"企业征信逾期记录数为\1", text, flags=re.IGNORECASE)
    text = re.sub(r"企业流水为\s*partial\s*状态",
                  "企业流水可用于初步分析，但部分交易分类仍需进一步核验", text, flags=re.IGNORECASE)
    text = re.sub(r"(?:企业流水)?数据质量\s*status\s*为\s*(?:partial|部分资料)",
                  "企业流水可用于初步分析，但部分交易分类及关联关系仍需复核", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?:(?:当前)?(?:未获取|缺少)(?:可核验的?)?)?(?:主体)?科技企业(?:资格)?标签(?:为空|资料)?|"
        r"当前缺少可核验科技企业资质或标签资料|"
        r"科技企业资质或标签资料|科技标签(?:为空|资料)?",
        _TECH_MATERIALS, text,
    )
    text = text.replace("标签资料", "相关证明资料")
    text = text.replace("资产资料缺失，未获取稳定结构化资产资料，各类资产清单均为空", _MISSING_ASSET_DATA)
    text = text.replace("资产资料未获取，未获取稳定结构化资产资料", _MISSING_ASSET_DATA)
    text = re.sub(
        r"(?:房产、车辆、设备、知识产权、股权、存款及其他抵押物|各类资产|资产资料|资产)清单均?为空",
        _MISSING_ASSET_DATA, text,
    )
    text = re.sub(r"(?:当前)?未获取(?:可用于本次分析的)?资产清单|资产清单为空", _MISSING_ASSET_DATA, text)
    text = text.replace("补充个人流水与资产清单（含权属与估值依据）", "补充个人流水，并补充可核验的资产权属、估值及可抵押状态资料")
    text = text.replace("补充资产清单及权属与估值依据", "补充可核验的资产权属、估值及可抵押状态资料")
    text = text.replace("资产清单、权属证明及估值依据", "可核验的资产权属、估值及可抵押状态资料")
    text = text.replace("补充稳定结构化资产资料，包括权属、估值及可抵押状态", "补充可核验的资产权属、估值及可抵押状态资料")
    text = text.replace("个人流水、资产资料缺失，无法核验稳定可采信个人收入与可抵押资产", "个人流水与稳定结构化资产资料尚未获取，相关收入和增信条件待核验")
    text = text.replace("融资需求、个人流水、资产资料缺失", "融资需求与个人流水待补充，稳定结构化资产资料尚未获取")
    text = text.replace("个人流水与资产资料缺失", "个人流水待补充，稳定结构化资产资料尚未获取")
    text = text.replace("个人流水、资产资料缺失", "个人流水待补充，稳定结构化资产资料尚未获取")
    text = text.replace("资产资料未获取", _MISSING_ASSET_DATA)
    text = text.replace("企业流水净流入为负且内部互转规模较大", "企业流水收支结构仍需进一步核验")
    text = text.replace("企业流水净流入为负且经营流出高于经营流入", "企业流水收支结构仍需进一步核验")
    text = re.sub(
        r"企业流水净流入为负\s*[，,；;]\s*(?:削弱流水对经营偿债能力的支撑|"
        r"流水对经营偿债能力的支撑不足|(?:经营)?现金流覆盖能力不足)",
        _UNCERTAIN_CASHFLOW, text,
    )
    text = text.replace("企业流水净流入为负", _UNCERTAIN_CASHFLOW)
    text = text.replace("按已保存分类初步统计的现金流覆盖能力不足", _UNCERTAIN_CASHFLOW)
    for phrase in (
        "削弱流水对经营偿债能力的支撑", "流水对经营偿债能力的支撑不足",
        "偿债能力支撑不足", "真实经营现金流为负", "现金流覆盖能力不足", "经营现金流恶化",
    ):
        text = text.replace(phrase, _CASHFLOW_IMPACT)
    text = text.replace("个人相关还款责任规模显著及融资需求缺失", "个人相关还款责任需核验，融资需求尚未明确")
    text = re.sub(
        r"个人相关还款责任余额([\d,.]+\s*元)[，,]规模显著",
        r"相关还款责任\1，构成待核验", text,
    )
    text = text.replace("相关还款责任规模显著", "相关还款责任需结合被担保主体、余额构成及到期安排进一步评估")
    text = text.replace("相关还款责任余额极高", "相关还款责任余额需结合构成及到期安排进一步评估")
    text = text.replace("潜在代偿压力较大", "相关责任需结合被担保主体、余额构成及到期安排进一步评估")
    text = text.replace("财务杠杆处于极高水平", "资本结构需核验")
    text = text.replace("财务杠杆极高且净资产缓冲极薄", "资产负债率与净资产基础需评估")
    text = text.replace("规模显著", "需核验构成")
    text = text.replace("流水净流出", "流水分类仍需核验")
    text = re.sub(
        r"近1年担保(?:资格)?审查\s*(\d+)次[、，,]\s*近2年(?:担保(?:资格)?审查)?\s*(\d+)次"
        r"[，,；;]\s*反映个人层面存在(?:较多|频繁|偏高|异常)担保相关审查记录",
        lambda match: f"近1年担保资格审查{match.group(1)}次，近2年{match.group(2)}次；{_QUERY_RULE_NOTE}",
        text,
    )
    text = re.sub(
        r"反映个人层面存在(?:较多|频繁|偏高|异常)担保相关审查记录",
        _QUERY_RULE_NOTE, text,
    )
    text = re.sub(
        r"(担保(?:资格)?审查|征信查询|查询)(?:次数)?(?:较多|频繁|偏高|异常)",
        lambda match: f"{match.group(1)}情况需结合拟申请机构正式准入规则评估", text,
    )
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

    trends = financial_trends or {}

    def matches_saved_value(raw: str, field: str) -> bool:
        saved = trends.get(field)
        if saved is None or isinstance(saved, bool):
            return False
        try:
            return Decimal(raw) == Decimal(str(saved))
        except (InvalidOperation, ValueError):
            return False

    def trend_ratio(match: re.Match[str]) -> str:
        if match.group("percent").strip() or not matches_saved_value(
            match.group("value"), _TREND_FIELDS[match.group("label")]
        ):
            return match.group(0)
        percent = Decimal(match.group("value")) * 100
        return f"{match.group('label')}{match.group('link')}{percent:,.2f}%"

    def cashflow_change(match: re.Match[str]) -> str:
        if financial_unit != "元" or not matches_saved_value(match.group("value"), "operating_cashflow_change"):
            return match.group(0)
        amount = Decimal(match.group("value"))
        return f"{match.group('label')}{match.group('link')}{amount:,.2f}元"

    text = _FINANCIAL_TREND_RATIO.sub(trend_ratio, text)
    text = _CASHFLOW_CHANGE.sub(cashflow_change, text)
    ratios = [float(value) for value in (debt_asset_ratio, *debt_asset_ratios)
              if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if ratios:
        text = text.replace("资产负债率极高", f"资产负债率为{ratios[0] * 100:,.2f}%")

        def debt_ratio(match: re.Match[str]) -> str:
            value = float(match.group("value"))
            if match.group("suffix").strip() == "%":
                return match.group(0)
            for ratio in ratios:
                if abs(value - ratio) < 0.00005 or abs(value - ratio * 100) < 0.005:
                    return f"{match.group('prefix')}{ratio * 100:,.2f}%"
            return match.group(0)

        text = _DEBT_RATIO.sub(debt_ratio, text)
        text = re.sub(
            r"资产负债率长期处于(?P<value>0\.\d+)以上",
            lambda match: (
                "近三个已保存财务期间的资产负债率均在99%以上"
                if len(ratios) >= 3 and all(value >= 0.99 for value in ratios[-3:])
                else f"资产负债率长期处于{float(match.group('value')) * 100:.2f}%以上"
            ),
            text,
        )
    assert_no_internal_english_terms(text, html=html)
    return text
