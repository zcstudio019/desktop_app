from __future__ import annotations

from typing import Any


SERIOUS_CONFLICT_KEYWORDS = ("企业名称", "账户名称", "法定代表人", "身份证姓名")


def _section(profile: dict[str, Any], key: str) -> dict[str, Any]:
    value = profile.get(key)
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], {})


def _status_for_section(section: dict[str, Any], required_fields: list[str]) -> str:
    if not section or not any(_has_value(section.get(field)) for field in required_fields + ["source_document_id"]):
        return "missing"
    if all(_has_value(section.get(field)) for field in required_fields):
        return "complete"
    return "partial"


def _asset_status(profile: dict[str, Any]) -> str:
    assets = _section(profile, "assets")
    properties = [item for item in _list(assets.get("properties")) if isinstance(item, dict)]
    vehicles = [item for item in _list(assets.get("vehicles")) if isinstance(item, dict)]
    if not properties and not vehicles:
        return "none"

    incomplete = False
    for item in properties:
        if not all(_has_value(item.get(field)) for field in ("owner", "certificate_number", "property_address")):
            incomplete = True
    for item in vehicles:
        if not all(_has_value(item.get(field)) for field in ("plate_number", "vehicle_owner", "vehicle_identification_number")):
            incomplete = True
    return "partial" if incomplete else "complete"


def _has_serious_conflict(conflicts: list[str]) -> bool:
    for conflict in conflicts:
        if any(keyword in conflict for keyword in SERIOUS_CONFLICT_KEYWORDS):
            return True
    return False


def build_financing_kyc_diagnostic(
    profile: dict[str, Any] | None,
    completeness: dict[str, Any] | None,
) -> dict[str, Any]:
    profile = profile or {}
    completeness = completeness or {}
    customer_id = str(profile.get("customer_id") or "")
    score = int(completeness.get("completeness_score") or 0)
    required_missing = [str(item) for item in _list(completeness.get("required_missing")) if str(item).strip()]
    conflicts = [str(item) for item in _list(completeness.get("conflicts")) if str(item).strip()]

    enterprise = _section(profile, "enterprise_identity")
    identity = _section(profile, "person_identity")
    bank = _section(profile, "bank_account")
    marriage = _section(profile, "marriage")

    enterprise_status = _status_for_section(enterprise, ["company_name", "unified_social_credit_code", "legal_representative"])
    identity_status = _status_for_section(identity, ["name", "id_number"])
    bank_account_status = _status_for_section(bank, ["account_name", "account_number", "opening_bank"])
    asset_status = _asset_status(profile)

    missing_materials: list[str] = []
    recommended_actions: list[str] = []
    key_risks: list[str] = []

    if "营业执照" in required_missing or enterprise_status == "missing":
        enterprise_status = "missing"
        missing_materials.append("营业执照")
        recommended_actions.append("请补充营业执照，用于确认企业主体资格")

    if "法人身份证" in required_missing or identity_status == "missing":
        identity_status = "missing"
        missing_materials.append("法人身份证")
        recommended_actions.append("请补充法人身份证，用于确认借款主体和法定代表人身份")

    account_missing_label = "开户许可证/基本存款账户信息"
    if account_missing_label in required_missing or bank_account_status == "missing":
        bank_account_status = "missing"
        missing_materials.append(account_missing_label)
        recommended_actions.append("请补充开户许可证或基本存款账户信息，用于核验企业收款账户")

    if conflicts:
        key_risks.extend(conflicts)
        for conflict in conflicts:
            recommended_actions.append(f"请核对字段冲突：{conflict}")

    if marriage.get("marital_status") == "已婚" and not marriage.get("holder_2_id_number"):
        recommended_actions.append("建议补充结婚证或配偶身份证，用于部分银行核验配偶信息")

    serious_conflict = _has_serious_conflict(conflicts)
    has_required_missing = bool(missing_materials)
    usable_for_financing = not has_required_missing and not serious_conflict

    if has_required_missing or serious_conflict:
        readiness_level = "not_ready"
    elif score < 80:
        readiness_level = "basic_ready"
    else:
        readiness_level = "ready"

    if readiness_level == "not_ready":
        summary = "当前KYC资料尚未具备初步融资评估条件，请先补齐关键资料并处理字段冲突。"
        next_step = "优先补充必备资料，完成营业执照、法人身份证和企业账户信息核验后再进入融资测算。"
    elif readiness_level == "basic_ready":
        summary = "客户必备KYC资料基本齐全，可进行初步融资评估，但仍建议补充可选资料或完善低完整度字段。"
        next_step = "可先进入初步融资评估，同时补充资产、经营资质或缺失字段以提升材料质量。"
    else:
        summary = "客户KYC资料较完整，已具备初步融资评估条件。"
        next_step = "可继续结合征信、流水和财报数据开展融资额度、风险和产品匹配分析。"

    if not profile.get("documents"):
        summary = "暂无足够KYC资料进行融资诊断，请先上传营业执照、法人身份证和企业账户资料。"

    return {
        "customer_id": customer_id,
        "diagnostic_type": "kyc_financing_readiness",
        "material_completeness_score": score,
        "usable_for_financing": usable_for_financing,
        "readiness_level": readiness_level,
        "summary": summary,
        "identity_status": identity_status,
        "enterprise_status": enterprise_status,
        "bank_account_status": bank_account_status,
        "asset_status": asset_status,
        "key_risks": list(dict.fromkeys(key_risks)),
        "missing_materials": list(dict.fromkeys(missing_materials)),
        "conflicts": conflicts,
        "recommended_actions": list(dict.fromkeys(recommended_actions)),
        "next_step": next_step,
    }
