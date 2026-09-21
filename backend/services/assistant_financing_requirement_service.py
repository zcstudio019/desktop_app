"""Chat flow for explicit financing needs; confirmation is a separate write."""

from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from backend.routers.financing_requirement import require_customer_access
from backend.services.financing_requirement_service import (
    confirm_requirement, create_requirement_draft, extract_requirement_patch,
    get_requirement,
    suggest_purpose_from_explicit_text,
)


FINANCING_REQUIREMENT_INTENT = "financing_requirement"


def is_financing_requirement_request(message: str) -> bool:
    text = re.sub(r"\s+", "", message or "")
    return bool(re.search(r"融资需求|(?:想|要|需要|计划|打算|准备|改成|改为).{0,6}融资|融资.{0,10}\d+(?:\.\d+)?(?:亿|万|元)|(?:金额|期限).{0,4}改成|不接受抵押|只考虑信用贷款|排除.{0,8}银行", text))


def _display(data: dict[str, Any]) -> str:
    amount = data.get("requested_amount")
    term = data.get("term_value")
    unit = {"day": "天", "month": "个月", "year": "年"}.get(data.get("term_unit"), "")
    return "\n".join([
        f"融资需求 V{data['version']}",
        f"融资主体：{data.get('borrower_entity') or '待确认'}",
        f"融资金额：{f'{amount:,.0f}元' if amount else '待确认'}{'（约数，需确认）' if amount and not data.get('amount_confirmed') else ''}",
        f"融资用途：{data.get('purpose_detail') or data.get('financing_purpose') or '待确认'}",
        f"融资期限：{str(term) + unit if term else '待确认'}{'（约数，需确认）' if term and not data.get('term_confirmed') else ''}",
        f"用款时间：{data.get('expected_funding_date') or '待确认'}",
        f"还款方式：{data.get('repayment_preference') or '待确认'}",
        f"担保偏好：{'、'.join(data.get('guarantee_preference') or []) or '待确认'}",
        f"是否接受抵押：{('接受' if data['accept_mortgage'] else '不接受') if data.get('accept_mortgage') is not None else '待确认'}",
        f"是否接受新增保证：{('接受' if data['accept_additional_guarantee'] else '不接受') if data.get('accept_additional_guarantee') is not None else '待确认'}",
    ])


async def handle_financing_requirement(storage: Any, message: str, selected_customer_id: str | None,
                                       current_user: dict | None, llm: Any = None) -> dict[str, Any]:
    if not current_user:
        raise HTTPException(status_code=401, detail="请先登录后管理融资需求")
    customer_id = selected_customer_id
    if not customer_id:
        names = re.findall(r"[\u4e00-\u9fffA-Za-z0-9（）()·]{2,60}(?:有限公司|有限责任公司)", message)
        if names:
            customers = await storage.list_customers()
            matches = [item for item in customers if str(item.get("name") or "") in names]
            if len(matches) == 1:
                customer_id = str(matches[0].get("customer_id") or "")
            elif len(matches) > 1:
                return {"message": "找到多个同名客户，请先选择具体客户。", "data": {"requirementStatus": "ambiguous_customer"}}
    if not customer_id:
        return {"message": "请先选择客户，再确认本次融资需求。", "data": {"requirementStatus": "missing_customer"}}
    customer = await require_customer_access(customer_id, current_user)
    username = current_user["username"]
    text = message.strip()
    if re.search(r"现在.*融资需求是什么|当前.*融资需求是什么|查看.*融资需求", text):
        current = get_requirement(customer_id)
        return {"message": (_display(current) if current else "当前尚未确认融资需求。"),
                "data": {"requirementStatus": "confirmed" if current else "missing", "requirement": current}}
    if text in {"确认", "确认融资需求", "确认修改"}:
        pending = get_requirement(customer_id, status="needs_confirmation")
        if not pending:
            return {"message": "当前没有待确认的融资需求。", "data": {"requirementStatus": "missing_pending"}}
        try:
            confirmed = confirm_requirement(customer_id, pending["requirement_id"], username)
        except ValueError as exc:
            return {"message": f"{exc}。请补充后再确认。", "data": {"requirementStatus": "needs_confirmation", "requirement": pending}}
        return {"message": "本次融资需求已确认。\n\n" + _display(confirmed),
                "data": {"requirementStatus": "confirmed", "requirement": confirmed}}
    patch = extract_requirement_patch(text)
    if llm and (patch is None or patch.financing_purpose is None):
        suggestion = suggest_purpose_from_explicit_text(text, llm)
        if suggestion:
            values = patch.model_dump(exclude_unset=True) if patch else {}
            values.update(suggestion.model_dump(exclude_unset=True))
            from backend.services.financing_requirement_service import RequirementPatch
            patch = RequirementPatch.model_validate(values)
    if patch is None:
        return {"message": "请说明本次融资金额、用途和期限；我会先整理待确认需求。", "data": {"requirementStatus": "missing_fields"}}
    draft = create_requirement_draft(customer_id, patch, username, borrower_name=str(customer.get("name") or ""))
    missing = []
    for key, label in (("requested_amount", "金额"), ("financing_purpose", "用途"), ("term_value", "期限")):
        if not draft.get(key):
            missing.append(label)
    prompt = ("还需要确认" + "和".join(missing) + "。") if missing else "请核对后点击确认需求；如有变化可修改。"
    return {"message": _display(draft) + "\n\n状态：待确认。" + prompt,
            "data": {"requirementStatus": "needs_confirmation", "requirement": draft}}
