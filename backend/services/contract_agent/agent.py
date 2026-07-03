from __future__ import annotations

import logging
from typing import Any

from .markdown_renderer import (
    apply_bohui_material_purchase_markdown_patch,
    apply_complete_subcontract_markdown_patch,
    apply_material_purchase_markdown_patch,
    apply_zhangjiang_consulting_markdown_patch,
    render_contract_markdown,
    sanitize_contract_result_payload,
)
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
        result.markdown = apply_material_purchase_markdown_patch(
            result.markdown,
            result,
            pages if isinstance(pages, list) else [],
            result.source_file,
        )
        result.markdown = apply_bohui_material_purchase_markdown_patch(
            result.markdown,
            result,
            pages if isinstance(pages, list) else [],
            result.source_file,
        )
        result.markdown = apply_zhangjiang_consulting_markdown_patch(
            result.markdown,
            result,
            pages if isinstance(pages, list) else [],
            result.source_file,
        )
        result.display_markdown = result.markdown
        if result.contract_category == "material_purchase" and "博汇盛" in result.source_file:
            buyer = result.parties[0] if result.parties else None
            seller = result.parties[1] if len(result.parties) > 1 else None
            logger.info("[MaterialPurchaseBohuiFinalDebug] contract_no=%s", result.contract_no)
            logger.info("[MaterialPurchaseBohuiFinalDebug] buyer_tax_id=%s", getattr(buyer, "unified_social_credit_code", ""))
            logger.info("[MaterialPurchaseBohuiFinalDebug] seller_tax_id=%s", getattr(seller, "unified_social_credit_code", ""))
            logger.info("[MaterialPurchaseBohuiFinalDebug] payment_schedule=%s", result.payment_nodes)
            logger.info("[MaterialPurchaseBohuiFinalDebug] invoice_requirement=%s", result.settlement.get("invoice_requirement"))
            logger.info("[MaterialPurchaseBohuiFinalDebug] seller_bank_account=%s", result.settlement.get("receiving_account"))
            logger.info("[MaterialPurchaseBohuiFinalDebug] final_markdown_contains_payment_70=%s", "70%" in result.markdown)
            logger.info(
                "[MaterialPurchaseBohuiFinalDebug] final_markdown_contains_invalid_payment_5=%s",
                "合同价款5%的违约金" in result.markdown or "廉政规定" in result.markdown,
            )
        if result.contract_category == "material_purchase":
            buyer = result.parties[0] if result.parties else None
            seller = result.parties[1] if len(result.parties) > 1 else None
            logger.info("[MaterialPurchaseFinalDebug] amount_fields_before_render=%s", result.amount)
            logger.info("[MaterialPurchaseFinalDebug] buyer_tax_id=%s", getattr(buyer, "unified_social_credit_code", ""))
            logger.info("[MaterialPurchaseFinalDebug] seller_tax_id=%s", getattr(seller, "unified_social_credit_code", ""))
            logger.info("[MaterialPurchaseFinalDebug] buyer_contact=%s", getattr(buyer, "contact", ""))
            logger.info("[MaterialPurchaseFinalDebug] buyer_phone=%s", getattr(buyer, "phone", ""))
            logger.info("[MaterialPurchaseFinalDebug] seller_contact=%s", getattr(seller, "contact", ""))
            logger.info("[MaterialPurchaseFinalDebug] seller_phone=%s", getattr(seller, "phone", ""))
            logger.info("[MaterialPurchaseFinalDebug] copy_count=%s", result.copies)
            logger.info("[MaterialPurchaseFinalDebug] final_markdown_contains_amount=%s", "35,011,412.68 元" in result.markdown)
            logger.info(
                "[MaterialPurchaseFinalDebug] final_markdown_contains_dirty_contact=%s",
                any(token in result.markdown for token in ("徐志良联系方", "系方式")),
            )
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
