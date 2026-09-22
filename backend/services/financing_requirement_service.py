"""Explicit, versioned financing needs. No report or credit facts are inferred here."""

from __future__ import annotations

import json
import logging
import re
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import desc, func, select

from backend.database import SessionLocal
from backend.db_models import FinancingRequirement


PURPOSES = {"流动资金", "采购", "项目垫资", "支付工程款", "设备采购", "置换存量融资", "归还借款", "扩大经营", "其他"}
STATUSES = {"draft", "needs_confirmation", "confirmed", "superseded", "cancelled"}
LIST_FIELDS = {"guarantee_preference", "collateral_available", "existing_banks", "preferred_banks", "excluded_banks"}
BOOL_FIELDS = {"accept_additional_guarantee", "accept_mortgage", "accept_refinancing"}
CORE_FIELDS = {"borrower_entity", "requested_amount", "financing_purpose", "term_value", "term_unit"}
DRAFT_SOURCES = {"chat_user_input", "manual_form", "application_form", "production_regression", "migration", "system_import"}
logger = logging.getLogger(__name__)
VERSION_FIELDS = {
    "borrower_entity", "requested_amount", "currency", "amount_confirmed", "financing_purpose",
    "purpose_detail", "term_value", "term_unit", "term_confirmed", "expected_funding_date",
    "repayment_preference", "guarantee_preference", "collateral_available", "registered_region",
    "operating_region", "existing_banks", "preferred_banks", "excluded_banks",
    "accept_additional_guarantee", "accept_mortgage", "accept_refinancing",
}


def normalized_requirement(data: dict[str, Any]) -> dict[str, Any]:
    """Compare business values, ignoring presentation and list order."""
    result: dict[str, Any] = {}
    for key in VERSION_FIELDS:
        value = data.get(key)
        if key in LIST_FIELDS:
            result[key] = tuple(sorted({str(item).strip() for item in (value or []) if str(item).strip()}))
        elif key == "requested_amount":
            try:
                result[key] = Decimal(str(value)).quantize(Decimal("0.01")) if value not in (None, "") else None
            except InvalidOperation as exc:
                raise ValueError("融资金额格式无效") from exc
        elif key in {"term_value"}:
            result[key] = int(value) if value not in (None, "") else None
        elif isinstance(value, str):
            result[key] = value.strip() or None
        else:
            result[key] = value
    if result["term_value"] is not None and result["term_unit"] == "year":
        result["term_value"] *= 12
        result["term_unit"] = "month"
    result["currency"] = result["currency"] or "CNY"
    return result


class RequirementPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    borrower_entity: str | None = None
    requested_amount: float | None = None
    currency: str = "CNY"
    amount_confirmed: bool | None = None
    financing_purpose: str | None = None
    purpose_detail: str | None = None
    term_value: int | None = None
    term_unit: str | None = None
    term_confirmed: bool | None = None
    term_original: str | None = None
    expected_funding_date: str | None = None
    repayment_preference: str | None = None
    guarantee_preference: list[str] | None = None
    collateral_available: list[str] | None = None
    registered_region: str | None = None
    operating_region: str | None = None
    existing_banks: list[str] | None = None
    preferred_banks: list[str] | None = None
    excluded_banks: list[str] | None = None
    accept_additional_guarantee: bool | None = None
    accept_mortgage: bool | None = None
    accept_refinancing: bool | None = None

    @field_validator("requested_amount")
    @classmethod
    def amount_positive(cls, value: float | None) -> float | None:
        if value is not None and (not 0 < value <= 1_000_000_000_000):
            raise ValueError("融资金额必须大于0")
        return value

    @field_validator("currency")
    @classmethod
    def cny_only(cls, value: str) -> str:
        if value != "CNY":
            raise ValueError("当前只支持人民币融资需求")
        return value

    @field_validator("financing_purpose")
    @classmethod
    def purpose_known(cls, value: str | None) -> str | None:
        if value is not None and value not in PURPOSES:
            raise ValueError("融资用途不在支持范围内")
        return value

    @field_validator("term_value")
    @classmethod
    def term_positive(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("融资期限必须大于0")
        return value

    @field_validator("term_unit")
    @classmethod
    def term_unit_known(cls, value: str | None) -> str | None:
        if value is not None and value not in {"day", "month", "year"}:
            raise ValueError("融资期限单位只支持天、月或年")
        return value


def _loads(value: str | None, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except (TypeError, ValueError):
        return default


def requirement_to_dict(row: FinancingRequirement) -> dict[str, Any]:
    result = {key: getattr(row, key) for key in RequirementPatch.model_fields if key not in LIST_FIELDS | BOOL_FIELDS}
    if result["requested_amount"] is not None:
        result["requested_amount"] = float(result["requested_amount"])
    result["amount_confirmed"] = bool(result["amount_confirmed"])
    result["term_confirmed"] = bool(result["term_confirmed"])
    result.update({key: _loads(getattr(row, key + "_json"), []) for key in LIST_FIELDS})
    result.update({key: (None if getattr(row, key) is None else bool(getattr(row, key))) for key in BOOL_FIELDS})
    result.update({
        "requirement_id": row.requirement_id, "customer_id": row.customer_id,
        "version": row.version, "status": row.status,
        "field_sources": _loads(row.field_sources_json, {}),
        "draft_source": row.draft_source,
        "created_by": row.created_by, "confirmed_by": row.confirmed_by,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "confirmed_at": row.confirmed_at.isoformat() if row.confirmed_at else None,
    })
    return result


def get_requirement(customer_id: str, *, status: str = "confirmed", session_factory=SessionLocal) -> dict[str, Any] | None:
    with session_factory() as db:
        row = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.status == status,
        ).order_by(desc(FinancingRequirement.version)).limit(1)).scalar_one_or_none()
        return requirement_to_dict(row) if row else None


def create_requirement_draft(customer_id: str, patch: RequirementPatch | dict[str, Any], actor: str,
                             *, borrower_name: str | None = None, draft_source: str = "manual_form",
                             session_factory=SessionLocal) -> dict[str, Any]:
    if draft_source not in DRAFT_SOURCES:
        raise ValueError("融资需求草稿来源无效")
    if (draft_source == "production_regression" or actor == "step6_regression") and not customer_id.startswith("enterprise_融资需求回归测试客户"):
        raise ValueError("生产回归只能使用专用测试客户")
    patch = patch if isinstance(patch, RequirementPatch) else RequirementPatch.model_validate(patch)
    changes = patch.model_dump(exclude_unset=True)
    if not changes:
        raise ValueError("没有可记录的融资需求字段")
    with session_factory() as db:
        latest = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.status.in_(["needs_confirmation", "draft", "confirmed"]),
        ).order_by(desc(FinancingRequirement.version)).limit(1).with_for_update()).scalar_one_or_none()
        base = requirement_to_dict(latest) if latest else {}
        version = (db.execute(select(func.max(FinancingRequirement.version)).where(
            FinancingRequirement.customer_id == customer_id)).scalar() or 0) + 1
        if borrower_name and not base.get("borrower_entity") and "borrower_entity" not in changes:
            changes["borrower_entity"] = borrower_name
            borrower_source = "customer_profile"
        else:
            borrower_source = None
        data = {key: base.get(key) for key in RequirementPatch.model_fields}
        data.update(changes)
        data["currency"] = "CNY"
        if latest and normalized_requirement(data) == normalized_requirement(base):
            raise ValueError("融资需求未发生变化，无需保存新版本。")
        sources = dict(base.get("field_sources") or {})
        for key in changes:
            sources[key] = borrower_source if key == "borrower_entity" and borrower_source else "user_explicit"
        row = FinancingRequirement(
            requirement_id=str(uuid.uuid4()), customer_id=customer_id, version=version,
            status="needs_confirmation", created_by=actor, field_sources_json=json.dumps(sources, ensure_ascii=False),
            draft_source=draft_source,
        )
        for key, value in data.items():
            if key in LIST_FIELDS:
                setattr(row, key + "_json", json.dumps(value or [], ensure_ascii=False))
            elif key in BOOL_FIELDS:
                setattr(row, key, None if value is None else int(value))
            elif key in {"amount_confirmed", "term_confirmed"}:
                setattr(row, key, bool(value) if value is not None else False)
            else:
                setattr(row, key, value)
        db.add(row)
        db.commit()
        db.refresh(row)
        logger.info("financing_requirement_draft_created requirement_id=%s version=%s source=%s created_by=%s",
                    row.requirement_id, row.version, row.draft_source, row.created_by)
        return requirement_to_dict(row)


def confirm_requirement(customer_id: str, requirement_id: str, actor: str,
                        *, session_factory=SessionLocal) -> dict[str, Any]:
    with session_factory() as db:
        row = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.requirement_id == requirement_id,
        ).with_for_update()).scalar_one_or_none()
        if row is None:
            raise LookupError("融资需求草稿不存在")
        if row.status == "confirmed":
            return requirement_to_dict(row)
        if row.status not in {"needs_confirmation", "draft"}:
            raise ValueError("该融资需求版本不能确认")
        newer_confirmed = db.execute(select(FinancingRequirement.id).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.status == "confirmed",
            FinancingRequirement.version > row.version,
        ).limit(1)).first()
        if newer_confirmed:
            raise ValueError("已有更新的已确认融资需求，请重新修改最新版本")
        missing = [key for key in CORE_FIELDS if not getattr(row, key)]
        if not row.amount_confirmed:
            missing.append("amount_confirmed")
        if not row.term_confirmed:
            missing.append("term_confirmed")
        if missing:
            raise ValueError("请先确认融资主体、准确金额、用途和期限")
        previous = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.status == "confirmed",
        ).with_for_update()).scalars().all()
        for old in previous:
            old.status = "superseded"
        older_drafts = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.version < row.version,
            FinancingRequirement.status.in_(["draft", "needs_confirmation"]),
        )).scalars().all()
        for old in older_drafts:
            old.status = "superseded"
        row.status = "confirmed"
        row.confirmed_by = actor
        row.confirmed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        sources = _loads(row.field_sources_json, {})
        for key in RequirementPatch.model_fields:
            if getattr(row, key + "_json" if key in LIST_FIELDS else key, None) not in (None, "", "[]"):
                sources[key] = "user_confirmed"
        row.field_sources_json = json.dumps(sources, ensure_ascii=False)
        db.commit()
        db.refresh(row)
        return requirement_to_dict(row)


def cancel_requirement(customer_id: str, requirement_id: str, *, session_factory=SessionLocal) -> dict[str, Any]:
    with session_factory() as db:
        row = db.execute(select(FinancingRequirement).where(
            FinancingRequirement.customer_id == customer_id,
            FinancingRequirement.requirement_id == requirement_id,
        ).with_for_update()).scalar_one_or_none()
        if row is None:
            raise LookupError("融资需求版本不存在")
        if row.status not in {"draft", "needs_confirmation", "confirmed"}:
            raise ValueError("该融资需求版本不能取消")
        row.status = "cancelled"
        db.commit()
        db.refresh(row)
        return requirement_to_dict(row)


def format_requirement_summary(data: dict[str, Any]) -> str:
    amount = data.get("requested_amount")
    amount_text = f"{amount / 10000:,.0f}万元" if isinstance(amount, (float, int)) and amount % 10000 == 0 else (f"{amount:,.2f}元" if amount else "待确认")
    term = data.get("term_value")
    term_text = f"{term}{ {'day': '天', 'month': '个月', 'year': '年'}.get(data.get('term_unit'), '')}" if term else "待确认"
    return f"{amount_text} / {data.get('purpose_detail') or data.get('financing_purpose') or '用途待确认'} / {term_text}"


def extract_requirement_patch(message: str) -> RequirementPatch | None:
    """Conservative extraction of explicit user statements, never document metadata."""
    text = re.sub(r"\[AIContext:.*?\]|\[Model:.*?\]|\[WebSearch:.*?\]|\[KnowledgeBase:.*?\]", "", message, flags=re.S)
    data: dict[str, Any] = {}
    amount = re.search(r"(?:融资|金额|要|做|改成|改为|需要).{0,8}?(\d+(?:\.\d+)?)\s*(亿|千万|万|元)", text)
    if amount:
        data["requested_amount"] = float(amount.group(1)) * {"亿": 100_000_000, "千万": 10_000_000, "万": 10_000, "元": 1}[amount.group(2)]
        data["amount_confirmed"] = not bool(re.search(r"大概|左右|约|差不多", text))
    for word, purpose in (("材料采购", "采购"), ("采购材料", "采购"), ("项目垫资", "项目垫资"), ("支付工程款", "支付工程款"),
                          ("设备采购", "设备采购"), ("置换存量融资", "置换存量融资"), ("归还借款", "归还借款"),
                          ("流动资金", "流动资金"), ("扩大经营", "扩大经营")):
        if word in text:
            data["financing_purpose"] = purpose
            data["purpose_detail"] = word
            break
    term = re.search(r"(?:期限|借|贷|做).{0,5}?(\d+)\s*(年|个月|月|天)", text)
    if not term:
        term = re.search(r"(?:期限|借款期限|贷款期限)\s*(?:改成|改为|是)?\s*(一年|两年|三年|半年)(?:左右)?", text)
    if term:
        raw = term.group(0)
        if term.group(1) in {"一年", "两年", "三年", "半年"}:
            data["term_value"] = {"一年": 12, "两年": 24, "三年": 36, "半年": 6}[term.group(1)]
        else:
            data["term_value"] = int(term.group(1)) * (12 if term.group(2) == "年" else 1)
        data["term_unit"] = "month" if "天" not in raw else "day"
        data["term_confirmed"] = "左右" not in text
        data["term_original"] = raw
    if re.search(r"不接受抵押|不要抵押|只考虑信用|只做信用", text):
        data["accept_mortgage"] = False
        data["guarantee_preference"] = ["信用"]
    elif re.search(r"接受抵押|可以抵押", text):
        data["accept_mortgage"] = True
    if re.search(r"不接受(?:新增)?担保|不要(?:新增)?担保", text):
        data["accept_additional_guarantee"] = False
    elif re.search(r"接受(?:新增)?担保|可以(?:新增)?担保", text):
        data["accept_additional_guarantee"] = True
    if re.search(r"不接受(?:置换|借新还旧)|不做(?:置换|借新还旧)", text):
        data["accept_refinancing"] = False
    elif re.search(r"接受(?:置换|借新还旧)|可以(?:置换|借新还旧)", text):
        data["accept_refinancing"] = True
    for phrase in ("按月付息到期还本", "等额本息", "随借随还", "无明确偏好"):
        if phrase in text:
            data["repayment_preference"] = phrase
            break
    deadline = re.search(r"(?:用款|到账|资金到位).{0,6}?(20\d{2}[-年/]\d{1,2}[-月/]\d{1,2}日?)", text)
    if deadline:
        data["expected_funding_date"] = deadline.group(1).replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-")
    for phrase in ("法人保证", "第三方保证", "房产抵押", "设备抵押", "应收账款", "知识产权"):
        if phrase in text and re.search(r"(?:担保|抵押|质押|接受|选择|偏好)", text):
            data.setdefault("guarantee_preference", []).append(phrase)
    excluded = re.search(r"(?:排除|不做|不考虑)\s*([\u4e00-\u9fffA-Za-z]{2,14}银行)", text)
    if excluded:
        data["excluded_banks"] = [excluded.group(1)]
    preferred = re.search(r"(?:优先考虑|偏好|首选)\s*([\u4e00-\u9fffA-Za-z]{2,14}银行)", text)
    if preferred:
        data["preferred_banks"] = [preferred.group(1)]
    if not data:
        return None
    return RequirementPatch.model_validate(data)


def suggest_purpose_from_explicit_text(message: str, llm: Any) -> RequirementPatch | None:
    """Optional AI classification with a verbatim evidence gate; never extract money or term."""
    if not llm or not re.search(r"融资|贷款|借款|用途", message):
        return None
    if not re.search(r"用于|用来|用途|采购|垫资|支付|周转|置换|扩大经营", message):
        return None
    prompt = (
        "只从用户这句话识别明确说出的融资用途，不参考客户资料或报告。"
        "仅输出 JSON：{\"financing_purpose\":\"枚举\",\"purpose_detail\":\"原文用途\",\"evidence\":\"用户原文连续片段\"}。"
        "枚举只能是：流动资金、采购、项目垫资、支付工程款、设备采购、置换存量融资、归还借款、扩大经营、其他。"
        "没有明确用途则输出 {}。不得生成金额、期限、担保或银行。"
    )
    try:
        raw = llm(prompt, message)
        match = re.search(r"\{.*\}", str(raw), flags=re.S)
        data = json.loads(match.group(0)) if match else {}
        evidence = data.get("evidence")
        purpose = data.get("financing_purpose")
        detail = data.get("purpose_detail")
        if not isinstance(evidence, str) or not evidence.strip() or evidence not in message:
            return None
        if purpose not in PURPOSES or not isinstance(detail, str) or not detail.strip() or detail not in message:
            return None
        if detail.strip() in {"融资", "贷款", "借款", "资金"}:
            return None
        return RequirementPatch(financing_purpose=purpose, purpose_detail=detail)
    except Exception:
        return None
