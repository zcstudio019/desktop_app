from __future__ import annotations

import logging
from typing import Any

from .extractor import is_valid_renderable_shareholder_name
from .schema import CompanyArticlesResult

logger = logging.getLogger(__name__)


def _value(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null", "undefined"}:
        return "未识别"
    return text


def _capital_text(result: CompanyArticlesResult) -> str:
    if result.registered_capital and result.registered_capital != "未识别":
        return result.registered_capital
    if result.registered_capital_amount:
        return f"人民币{float(result.registered_capital_amount):g}万元"
    return "未识别"


def render_company_articles_markdown(result: CompanyArticlesResult, *, filename: str = "") -> str:
    signature = result.signature_info or {}
    capital_check = result.capital_check or {}
    governance = result.governance or {}
    rules = result.major_resolution_rules or {}
    registered_capital = _capital_text(result)
    rows = []
    ordered_shareholders = sorted(
        enumerate(result.shareholders),
        key=lambda pair: pair[1].row_index if pair[1].row_index is not None else pair[0],
    )
    for _, item in ordered_shareholders:
        shareholder_name = item.name
        if not is_valid_renderable_shareholder_name(shareholder_name):
            shareholder_name = "未识别"
            warning = "股东姓名包含无效表头字段，请人工复核"
            if warning not in result.warnings:
                result.warnings.append(warning)
        ratio = item.contribution_ratio
        if not ratio and result.registered_capital_amount and item.subscribed_amount_number is not None:
            ratio = f"{item.subscribed_amount_number / result.registered_capital_amount * 100:.2f}%"
        deadline = item.contribution_deadline or "未识别"
        logger.debug(
            "[CompanyArticles][ShareholderDateFlow] stage=renderer name=%s contribution_deadline=%s",
            item.name,
            deadline,
        )
        rows.append(
            f"| {_value(shareholder_name)} | {_value(item.subscribed_amount)} | {_value(item.contribution_method)} | "
            f"{_value(deadline)} | {_value(ratio)} |"
        )
    shareholder_table = [
        "### 股东及出资信息",
        "| 股东姓名/名称 | 出资额 | 出资方式 | 出资时间 | 出资比例 |",
        "|---|---:|---|---|---:|",
        *rows,
    ] if rows else [
        "### 股东及出资信息",
        "- 股东信息：未识别",
    ]
    warnings = "；".join(result.warnings) if result.warnings else "无"
    page_count_check = f"共识别 {result.page_count} 页" if result.page_count else "未识别"
    articles_pages = result.metadata.get("articles_page_numbers") if isinstance(result.metadata, dict) else []
    if isinstance(articles_pages, list) and articles_pages:
        if len(articles_pages) > 1:
            articles_range = f"第{articles_pages[0]}-{articles_pages[-1]}页"
        else:
            articles_range = f"第{articles_pages[0]}页"
        page_count_check = f"{page_count_check}；章程正文页：{articles_range}"
    return "\n".join(
        [
            "## 公司章程",
            "- 资料类型：公司章程",
            f"- 来源文件：{_value(filename or result.metadata.get('filename'))}",
            "- 原件状态：可查看",
            "",
            "### 基本信息",
            f"- 章程标题：{_value(result.title)}",
            f"- 公司名称：{_value(result.company_name)}",
            f"- 公司住所：{_value(result.company_address)}",
            f"- 注册资本：{registered_capital}",
            f"- 经营范围：{_value(result.business_scope)}",
            f"- 章程生效规则：{_value(result.articles_effective_rule)}",
            f"- 签署日期：{_value(signature.get('signing_date'))}",
            "",
            *shareholder_table,
            "",
            f"- 注册资本合计：{registered_capital}",
            f"- 股东出资额合计：{_value(capital_check.get('shareholder_total_amount_text'))}",
            f"- 出资校验：{_value(capital_check.get('message'))}",
            "",
            "### 公司治理信息",
            f"- 权力机构：{_value(governance.get('authority_body'))}",
            f"- 执行董事：{_value(governance.get('executive_director'))}",
            f"- 经理：{_value(governance.get('manager'))}",
            f"- 监事：{_value(governance.get('supervisor'))}",
            f"- 法定代表人：{_value(governance.get('legal_representative'))}",
            "",
            "### 表决及重大事项规则",
            f"- 表决权规则：{_value(governance.get('voting_rule'))}",
            f"- 修改章程规则：{_value(rules.get('amendment_rule'))}",
            f"- 增减注册资本规则：{_value(rules.get('capital_change_rule'))}",
            f"- 合并、分立、解散或变更公司形式规则：{_value(rules.get('merger_split_dissolution_rule'))}",
            "",
            "### 股权转让",
            _value(result.equity_transfer_summary),
            "",
            "### 财务、会计与利润分配",
            _value(result.finance_and_profit_summary),
            "",
            "### 解散与清算",
            _value(result.dissolution_and_liquidation_summary),
            "",
            "### 高级管理人员义务",
            _value(result.senior_management_obligations_summary),
            "",
            "### 签章信息",
            f"- 签章页：{_value(signature.get('signature_page'))}",
            f"- 股东签字/盖章：{_value(signature.get('has_signature_or_stamp'))}",
            f"- 签章识别结果：{_value(signature.get('signature_detection_summary'))}",
            f"- 签署日期：{_value(signature.get('signing_date'))}",
            "",
            "### 提取校验",
            f"- 页数校验：{page_count_check}",
            f"- 股东出资校验：{_value(capital_check.get('message'))}",
            f"- 需人工复核：{warnings}",
        ]
    )
