from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PageClassResult:
    page: int
    page_type: str
    articles_score: int
    text: str
    matched_features: list[str] = field(default_factory=list)


NON_ARTICLE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("change_registration_notice", ("准予变更登记通知书", "准予变更登记", "经审查")),
    ("company_registration_application", ("公司登记(备案)申请书", "公司登记备案申请书", "申请人声明")),
    ("shareholder_contribution_attachment", ("股东(发起人)出资情况", "股东发起人出资情况", "认缴出资额", "证件号码")),
    ("legal_representative_information", ("法定代表人信息", "身份证件号码", "移动电话", "电子邮箱")),
    ("management_information", ("董事监事经理信息", "董事、监事、经理信息")),
    ("shareholder_resolution", ("股东会决议", "通过公司新的章程", "同意变更后的经营范围")),
    ("business_license", ("营业执照", "统一社会信用代码", "成立日期", "登记机关")),
)


ARTICLE_SCORE_RULES: tuple[tuple[str, int], ...] = (
    ("有限公司章程", 100),
    ("公司章程", 100),
    ("第一章公司的名称和住所", 50),
    ("第一章公司名称和住所", 50),
    ("依据《中华人民共和国公司法》", 40),
    ("公司注册资本", 40),
    ("股东的姓名或者名称", 40),
    ("公司的机构及其产生办法、职权、议事规则", 40),
    ("股东会是公司的权力机构", 30),
    ("公司的法定代表人", 30),
    ("股权转让", 30),
    ("财务、会计、利润分配", 30),
    ("公司的解散事由与清算办法", 30),
    ("本章程自全体股东盖章、签字之日起生效", 40),
    ("本章程", 20),
)


def _compact(value: str) -> str:
    return re.sub(r"[\s：:，,。；;（）()]+", "", str(value or ""))


def _page_number(page: dict[str, Any], index: int) -> int:
    value = page.get("page") or page.get("page_index") or index
    try:
        return int(value)
    except (TypeError, ValueError):
        return index


def classify_company_articles_page(page: dict[str, Any], index: int = 1) -> PageClassResult:
    text = str(page.get("text") or "")
    compact = _compact(text)
    matched: list[str] = []
    score = 0
    for feature, points in ARTICLE_SCORE_RULES:
        if _compact(feature) in compact:
            score += points
            matched.append(feature)

    explicit_type = "other"
    for page_type, keywords in NON_ARTICLE_RULES:
        hits = sum(1 for keyword in keywords if _compact(keyword) in compact)
        required_hits = 2
        if hits >= required_hits:
            explicit_type = page_type
            break

    if score >= 80:
        page_type = "company_articles_page"
    elif explicit_type != "other":
        page_type = explicit_type
    elif re.search(r"第[一二三四五六七八九十百]+章", compact) and any(
        token in compact
        for token in ("股东会", "执行董事", "监事", "注册资本", "股权", "财务会计", "清算", "法定代表人")
    ):
        page_type = "company_articles_continuation"
    else:
        page_type = "other"
    return PageClassResult(
        page=_page_number(page, index),
        page_type=page_type,
        articles_score=score,
        text=text,
        matched_features=matched,
    )


def classify_company_articles_pages(pages: list[dict[str, Any]]) -> list[PageClassResult]:
    return [
        classify_company_articles_page(page, index)
        for index, page in enumerate(pages or [], start=1)
        if isinstance(page, dict)
    ]
