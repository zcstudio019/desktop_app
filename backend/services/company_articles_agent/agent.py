from __future__ import annotations

from typing import Any

from .extractor import detect_company_articles
from .markdown_renderer import render_company_articles_markdown
from .normalizer import normalize_company_articles
from .schema import DOC_TYPE, DOC_TYPE_NAME, SCHEMA_VERSION, CompanyArticlesResult
from .skill import CompanyArticlesSkill
from .validator import validate_company_articles


def merge_ocr_pages(raw_text: str, pages: list[dict[str, Any]] | None = None) -> str:
    page_texts = [str(page.get("text") or "").strip() for page in (pages or []) if isinstance(page, dict) and str(page.get("text") or "").strip()]
    if page_texts:
        return "\n\n".join(page_texts)
    return str(raw_text or "")


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
        full_text = merge_ocr_pages(str(context.get("text") or context.get("raw_text") or ""), pages)
        extracted = CompanyArticlesSkill().extract(text=full_text, pages=pages, filename=filename)
        normalized = normalize_company_articles(extracted, filename=filename, raw_text=full_text)
        normalized.metadata.update(
            {
                "filename": filename,
                "customer_id": str(context.get("customer_id") or ""),
                "customer_name": str(context.get("customer_name") or ""),
                "source": str(context.get("source") or "upload"),
            }
        )
        validated = validate_company_articles(normalized)
        validated.markdown = render_company_articles_markdown(validated, filename=filename)
        validated.evidence = {
            "source_pages": [page.get("page") or page.get("page_index") for page in pages if isinstance(page, dict)],
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
