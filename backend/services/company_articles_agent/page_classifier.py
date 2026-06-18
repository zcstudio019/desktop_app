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
    ("notice", ("准予变更登记通知书", "准予设立登记通知书", "准予变更登记", "经审查")),
    ("application_form", ("公司登记(备案)申请书", "公司登记备案申请书", "申请人声明", "基本信息")),
    ("shareholder_contribution_attachment", ("股东(发起人)出资情况", "股东发起人出资情况", "认缴出资额", "证件号码")),
    ("legal_representative_info", ("法定代表人信息", "身份证件号码", "移动电话", "电子邮箱")),
    ("director_supervisor_manager_info", ("董事监事经理信息", "董事、监事、经理信息")),
    ("shareholder_resolution", ("股东会决议", "通过公司新的章程", "同意变更后的经营范围")),
    ("business_license", ("营业执照", "统一社会信用代码", "成立日期", "登记机关")),
    ("commitment_letter", ("承诺书", "申请人承诺", "郑重承诺")),
    ("material_catalog", ("材料目录", "材料证明", "提交材料", "申请材料")),
)


ARTICLE_SCORE_RULES: tuple[tuple[str, int], ...] = (
    ("股份有限公司章程", 100),
    ("有限责任公司章程", 100),
    ("有限公司章程", 100),
    ("第一章公司的名称和住所", 50),
    ("第一章公司名称和住所", 50),
    ("依据《中华人民共和国公司法》", 50),
    ("公司注册资本", 40),
    ("股东的姓名或者名称", 40),
    ("发起人", 40),
    ("认购股份", 40),
    ("持股比例", 40),
    ("公司的机构及其产生办法、职权、议事规则", 40),
    ("股东会是公司的权力机构", 30),
    ("公司的法定代表人", 30),
    ("股权转让", 30),
    ("财务、会计、利润分配", 30),
    ("公司的解散事由与清算办法", 30),
    ("本章程自全体股东盖章、签字之日起生效", 40),
    ("本章程", 20),
)


def _valid_articles_title_in_top(text: str) -> bool:
    top = str(text or "")[:1000]
    return bool(
        re.search(
            r"(?<![),，、])[\u4e00-\u9fff（）()·A-Za-z0-9]{4,80}"
            r"(?:股份有限公司|有限责任公司|有限公司)章程",
            top,
        )
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
    if _valid_articles_title_in_top(text):
        score = max(score, 100)
        matched.append("valid_articles_title")

    explicit_type = "other"
    for page_type, keywords in NON_ARTICLE_RULES:
        hits = sum(1 for keyword in keywords if _compact(keyword) in compact)
        required_hits = 2
        if page_type == "shareholder_contribution_attachment" and any(
            _compact(keyword) in compact
            for keyword in ("股东(发起人)出资情况", "股东发起人出资情况")
        ):
            required_hits = 1
        if page_type == "shareholder_resolution" and re.search(
            r"(?:^|\n)\s*股东会决议(?:\s|$)",
            text,
        ):
            required_hits = 1
        if hits >= required_hits:
            explicit_type = page_type
            break

    structural_count = sum(
        1
        for token in (
            "第一章", "公司名称", "公司住所", "公司注册资本", "股东的姓名或者名称",
            "发起人", "认购股份", "股东会", "董事会", "股权转让", "财务会计",
            "利润分配", "解散", "清算", "本章程",
        )
        if token in compact
    )
    if explicit_type != "other" and not _valid_articles_title_in_top(text) and structural_count < 3:
        page_type = explicit_type
    elif score >= 80:
        page_type = "company_articles_page"
    elif explicit_type != "other":
        page_type = explicit_type
    elif re.search(r"第[一二三四五六七八九十百]+章", compact) and any(
        token in compact
        for token in ("股东会", "执行董事", "监事", "注册资本", "股权", "财务会计", "清算", "法定代表人")
    ):
        page_type = "company_articles_continuation"
    elif any(
        token in text
        for token in ("股东签字：", "股东签字:", "股东签名：", "股东签名:", "股东（签字、盖章）")
    ):
        page_type = "articles_signature_page"
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
