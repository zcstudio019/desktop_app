from __future__ import annotations

import logging
from typing import Any

from .markdown_renderer import apply_complete_subcontract_markdown_patch, render_contract_markdown, sanitize_contract_result_payload
from .schema import DOC_TYPE, DOC_TYPE_NAME, SCHEMA_VERSION, ContractResult
from .skill import ContractSkill, is_contract_like


logger = logging.getLogger(__name__)


class ContractAgent:
    doc_type = DOC_TYPE
    doc_type_name = DOC_TYPE_NAME
    schema_version = SCHEMA_VERSION

    def can_handle(self, context: dict[str, Any]) -> bool:
        return is_contract_like(str(context.get("text") or ""), str(context.get("filename") or ""))

    def run(self, context: dict[str, Any]) -> ContractResult:
        pages = context.get("raw_pages") if isinstance(context.get("raw_pages"), list) else context.get("pages")
        extracted = ContractSkill().extract(
            text=str(context.get("text") or context.get("raw_text") or ""),
            pages=pages if isinstance(pages, list) else [],
            filename=str(context.get("filename") or ""),
        )
        result = ContractResult(
            contract_category=extracted.get("contract_category") or "unknown_contract",
            contract_category_name=extracted.get("contract_category_name") or "其他合同",
            extraction_status=extracted.get("extraction_status") or "partial",
            title=extracted.get("title") or "",
            project_name=extracted.get("project_name") or "",
            contract_no=extracted.get("contract_no") or "",
            source_file=str(context.get("filename") or ""),
            page_count=int(extracted.get("page_count") or 0),
            signing_date=extracted.get("signing_date") or "",
            signing_place=extracted.get("signing_place") or "",
            effective_condition=extracted.get("effective_condition") or "",
            copies=extracted.get("copies") or "",
            parties=extracted.get("parties") or [],
            project=extracted.get("project") or {},
            amount=extracted.get("amount") or {},
            duration=extracted.get("duration") or {},
            payment_nodes=extracted.get("payment_nodes") or [],
            settlement=extracted.get("settlement") or {},
            line_items=extracted.get("line_items") or [],
            line_item_summary=extracted.get("line_item_summary") or {},
            clauses=extracted.get("clauses") or {},
            signature=extracted.get("signature") or {},
            quality=extracted.get("quality") or {},
            validation=extracted.get("validation") or {},
            evidence=extracted.get("evidence") or {},
            warnings=extracted.get("warnings") or [],
        )
        if "合同003" in result.source_file:
            logger.info(
                "[Contract003RenderInput] safety_civilized_fee=%s price_form=%s settlement_method=%s "
                "payment_schedule=%s invoice_requirement=%s important_terms_invoice=%s important_terms_safety=%s",
                result.amount.get("safety_civilization_fee"),
                result.amount.get("price_form"),
                result.settlement.get("settlement_method"),
                result.payment_nodes,
                result.settlement.get("invoice_requirement"),
                result.clauses.get("invoice_requirement"),
                result.clauses.get("safety_civilization"),
            )
        result.markdown = apply_complete_subcontract_markdown_patch(
            render_contract_markdown(result),
            result,
            pages if isinstance(pages, list) else [],
            result.source_file,
        )
        result.display_markdown = result.markdown
        return result


def run_contract_agent(payload: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(payload, str):
        payload = {"text": payload, "metadata": {}}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    result = ContractAgent().run(
        {
            "text": payload.get("text") or payload.get("raw_text") or "",
            "raw_pages": payload.get("raw_pages") or payload.get("pages") or metadata.get("raw_pages") or [],
            "filename": metadata.get("filename") or payload.get("filename") or "",
        }
    ).to_dict()
    return sanitize_contract_result_payload(result, force=True)
