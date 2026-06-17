from __future__ import annotations

from typing import Any

from .extractor import extract_fields


class CompanyArticlesSkill:
    skill_name = "company_articles_skill"

    def extract(self, *, text: str, pages: list[dict[str, Any]] | None = None, filename: str = "") -> dict[str, Any]:
        return extract_fields(text=text, pages=pages or [], filename=filename)
