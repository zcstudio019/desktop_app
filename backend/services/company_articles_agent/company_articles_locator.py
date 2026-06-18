from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .page_classifier import PageClassResult, classify_company_articles_pages


@dataclass(slots=True)
class ArticlesBlock:
    start_page: int
    end_page: int
    page_numbers: list[int]
    pages: list[dict[str, Any]]
    text: str
    confidence: float
    page_classes: list[PageClassResult] = field(default_factory=list)


STOP_PAGE_TYPES = {
    "business_license",
    "change_registration_notice",
    "company_registration_application",
    "shareholder_resolution",
}


def _is_continuation(item: PageClassResult, started: bool) -> bool:
    if item.page_type in {"company_articles_page", "company_articles_continuation"}:
        return True
    if not started or item.page_type in STOP_PAGE_TYPES:
        return False
    text = item.text
    return any(
        token in text
        for token in (
            "第六章", "第七章", "第八章", "第九章", "第十章",
            "股东会", "执行董事", "监事", "经理", "法定代表人",
            "股权转让", "财务会计", "利润分配", "解散", "清算",
            "高级管理人员", "本章程", "股东签字", "股东盖章",
        )
    )


def locate_articles_block(pages: list[dict[str, Any]]) -> ArticlesBlock | None:
    page_classes = classify_company_articles_pages(pages)
    start_index = next(
        (
            index
            for index, item in enumerate(page_classes)
            if item.page_type == "company_articles_page" and item.articles_score >= 80
        ),
        None,
    )
    if start_index is None:
        return None

    selected_classes: list[PageClassResult] = []
    for item in page_classes[start_index:]:
        if selected_classes and item.page_type in STOP_PAGE_TYPES:
            break
        if not _is_continuation(item, bool(selected_classes)):
            if selected_classes:
                break
            continue
        selected_classes.append(item)
    if not selected_classes:
        return None

    selected_numbers = {item.page for item in selected_classes}
    selected_pages = [
        page for index, page in enumerate(pages, start=1)
        if isinstance(page, dict)
        and int(page.get("page") or page.get("page_index") or index) in selected_numbers
    ]
    scores = [item.articles_score for item in selected_classes]
    confidence = min(1.0, (max(scores) + sum(1 for score in scores if score > 0) * 10) / 150)
    return ArticlesBlock(
        start_page=selected_classes[0].page,
        end_page=selected_classes[-1].page,
        page_numbers=[item.page for item in selected_classes],
        pages=selected_pages,
        text="\n\n".join(item.text.strip() for item in selected_classes if item.text.strip()),
        confidence=round(confidence, 2),
        page_classes=page_classes,
    )
