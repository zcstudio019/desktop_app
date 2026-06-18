from __future__ import annotations

import logging
from typing import Any

from .company_articles_locator import locate_articles_block
from .extractor import (
    detect_company_articles,
    extract_external_shareholder_names,
    repair_shareholder_dates_by_majority,
    repair_shareholder_names_by_external_names,
)
from .markdown_renderer import render_company_articles_markdown
from .normalizer import normalize_company_articles
from .schema import DOC_TYPE, DOC_TYPE_NAME, SCHEMA_VERSION, CompanyArticlesResult
from .skill import CompanyArticlesSkill
from .validator import validate_company_articles

logger = logging.getLogger(__name__)


def merge_ocr_pages(raw_text: str, pages: list[dict[str, Any]] | None = None) -> str:
    page_texts = [str(page.get("text") or "").strip() for page in (pages or []) if isinstance(page, dict) and str(page.get("text") or "").strip()]
    if page_texts:
        return "\n\n".join(page_texts)
    return str(raw_text or "")


def _is_missing(value: Any) -> bool:
    return value is None or str(value).strip() in {"", "未识别"}


def _fill_basic_fallback_fields(
    extracted: dict[str, Any],
    fallback_data: dict[str, Any],
) -> None:
    for key in ("company_name", "company_address", "business_scope"):
        if _is_missing(extracted.get(key)) and not _is_missing(fallback_data.get(key)):
            extracted[key] = fallback_data[key]
    if _is_missing(extracted.get("registered_capital")) and not _is_missing(fallback_data.get("registered_capital")):
        extracted["registered_capital"] = fallback_data["registered_capital"]
        extracted["registered_capital_amount"] = fallback_data.get("registered_capital_amount")
        extracted["currency"] = fallback_data.get("currency") or "人民币"


class CompanyArticlesAgent:
    doc_type = DOC_TYPE
    doc_type_name = DOC_TYPE_NAME
    schema_version = SCHEMA_VERSION

    def can_handle(self, context: dict[str, Any]) -> bool:
        return detect_company_articles(str(context.get("text") or ""), str(context.get("filename") or ""))

    def run(self, context: dict[str, Any]) -> CompanyArticlesResult:
        raw_pages = context.get("raw_pages") if isinstance(context.get("raw_pages"), list) else context.get("pages")
        pages = raw_pages if isinstance(raw_pages, list) else []
        filename = str(context.get("filename") or "")
        for index, page in enumerate(pages, start=1):
            if not isinstance(page, dict):
                continue
            page_no = page.get("page") or page.get("page_index") or index
            page_text = str(page.get("text") or "")
            logger.debug(
                "[CompanyArticles][OCR] page=%s pdf_text_len=%s image_ocr_text_len=%s merged_text_preview=%s",
                page_no,
                len(str(page.get("pdf_text") or "")),
                len(str(page.get("image_ocr_text") or "")),
                page_text[:1000],
            )
            try:
                is_page_13 = int(page_no) == 13
            except (TypeError, ValueError):
                is_page_13 = False
            if is_page_13 and not any(
                token in page_text
                for token in (
                    "有限公司章程", "公司章程", "第一章", "公司名称", "公司住所",
                    "公司经营范围", "公司注册资本", "股东的姓名", "出资额", "出资方式",
                )
            ):
                logger.error("[CompanyArticles][OCR] page=13 articles_keywords_missing=true")
        full_text = merge_ocr_pages(str(context.get("text") or context.get("raw_text") or ""), pages)
        articles_block = locate_articles_block(pages) if pages else None
        main_text = articles_block.text if articles_block else full_text
        main_pages = articles_block.pages if articles_block else pages
        extracted = CompanyArticlesSkill().extract(text=main_text, pages=main_pages, filename=filename)
        if articles_block:
            external_name_texts = [
                item.text
                for item in articles_block.page_classes
                if item.page_type in {
                    "shareholder_contribution_attachment",
                    "shareholder_resolution",
                }
            ]
            if articles_block.pages:
                external_name_texts.append(str(articles_block.pages[-1].get("text") or ""))
            external_names: list[str] = []
            for evidence_text in external_name_texts:
                for name in extract_external_shareholder_names(evidence_text):
                    if name not in external_names:
                        external_names.append(name)
            shareholders = extracted.get("shareholders")
            if isinstance(shareholders, list):
                extracted["shareholders"] = repair_shareholder_dates_by_majority(
                    repair_shareholder_names_by_external_names(shareholders, external_names)
                )
                logger.debug(
                    "[CompanyArticles][ShareholderRepair] external_names=%s final_names=%s",
                    external_names,
                    [getattr(item, "name", "") for item in extracted["shareholders"]],
                )
            for fallback_type in ("shareholder_resolution", "business_license"):
                fallback_pages = [
                    {"page": item.page, "text": item.text}
                    for item in articles_block.page_classes
                    if item.page_type == fallback_type
                ]
                if not fallback_pages:
                    continue
                fallback_text = merge_ocr_pages("", fallback_pages)
                fallback_data = CompanyArticlesSkill().extract(
                    text=fallback_text,
                    pages=fallback_pages,
                    filename=filename,
                )
                _fill_basic_fallback_fields(extracted, fallback_data)
        extracted["page_count"] = len(pages) if pages else extracted.get("page_count")
        normalized = normalize_company_articles(extracted, filename=filename, raw_text=main_text)
        normalized.metadata.update(
            {
                "filename": filename,
                "customer_id": str(context.get("customer_id") or ""),
                "customer_name": str(context.get("customer_name") or ""),
                "source": str(context.get("source") or "upload"),
                "articles_page_numbers": articles_block.page_numbers if articles_block else [],
                "articles_start_page": articles_block.start_page if articles_block else None,
                "articles_end_page": articles_block.end_page if articles_block else None,
                "articles_locator_confidence": articles_block.confidence if articles_block else 0,
            }
        )
        if not articles_block and len(pages) > 1:
            normalized.warnings.append("未定位到独立章程正文页，已使用全文兜底")
        validated = validate_company_articles(normalized)
        validated.markdown = render_company_articles_markdown(validated, filename=filename)
        validated.display_markdown = validated.markdown
        validated.evidence = {
            "source_pages": [page.get("page") or page.get("page_index") for page in pages if isinstance(page, dict)],
            "articles_pages": articles_block.page_numbers if articles_block else [],
            "text_length": len(full_text),
        }
        return validated


def run_company_articles_agent(payload: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(payload, str):
        payload = {"text": payload, "metadata": {}}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    result = CompanyArticlesAgent().run(
        {
            "text": payload.get("text") or payload.get("raw_text") or "",
            "raw_pages": payload.get("raw_pages") or payload.get("pages") or metadata.get("raw_pages") or [],
            "filename": metadata.get("filename") or payload.get("filename") or "",
            "customer_id": metadata.get("customer_id") or payload.get("customer_id") or "",
            "customer_name": metadata.get("customer_name") or payload.get("customer_name") or "",
            "source": metadata.get("source") or "upload",
        }
    )
    return result.to_dict()
