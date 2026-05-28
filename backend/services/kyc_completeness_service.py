from __future__ import annotations

from typing import Any


def _section(profile: dict[str, Any], key: str) -> dict[str, Any]:
    value = profile.get(key)
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def evaluate_kyc_completeness(profile: dict[str, Any] | None) -> dict[str, Any]:
    profile = profile or {}
    enterprise = _section(profile, "enterprise_identity")
    person = _section(profile, "person_identity")
    bank = _section(profile, "bank_account")
    assets = _section(profile, "assets")
    properties = assets.get("properties") if isinstance(assets.get("properties"), list) else []
    vehicles = assets.get("vehicles") if isinstance(assets.get("vehicles"), list) else []

    required_missing: list[str] = []
    optional_missing: list[str] = []
    warnings: list[str] = []
    conflicts: list[str] = []
    suggestions: list[str] = []

    if not enterprise.get("source_document_id"):
        required_missing.append("营业执照")
    if not person.get("source_document_id"):
        required_missing.append("法人身份证")
    if not bank.get("source_document_id"):
        required_missing.append("开户许可证/基本存款账户信息")

    company_name = _text(enterprise.get("company_name"))
    account_name = _text(bank.get("account_name"))
    if company_name and account_name and company_name != account_name:
        conflicts.append(f"企业名称与账户名称不一致：{company_name} / {account_name}")

    legal_representative = _text(enterprise.get("legal_representative"))
    person_name = _text(person.get("name"))
    if legal_representative and person_name and legal_representative != person_name:
        conflicts.append(f"营业执照法定代表人与身份证姓名不一致：{legal_representative} / {person_name}")

    if enterprise.get("source_document_id") and not enterprise.get("unified_social_credit_code"):
        warnings.append("统一社会信用代码缺失")
    if person.get("source_document_id") and not person.get("id_number"):
        warnings.append("身份证号缺失")

    for index, item in enumerate(properties, start=1):
        if isinstance(item, dict) and not item.get("owner"):
            warnings.append(f"第{index}份房产证权利人缺失")
    for index, item in enumerate(vehicles, start=1):
        if isinstance(item, dict) and not item.get("vehicle_owner"):
            warnings.append(f"第{index}份行驶证所有人缺失")

    if not properties:
        optional_missing.append("房产证/不动产权证")
    if not vehicles:
        optional_missing.append("行驶证")

    for item in required_missing:
        suggestions.append(f"请补充上传{item}")
    for item in conflicts:
        suggestions.append(f"请人工核对：{item}")
    for item in warnings:
        suggestions.append(f"请补充或复核字段：{item}")

    score = 100
    score -= len(required_missing) * 30
    score -= len(conflicts) * 10
    score -= len(warnings) * 5
    score -= min(len(optional_missing) * 3, 10)
    score = max(0, min(100, score))

    return {
        "completeness_score": score,
        "required_missing": required_missing,
        "optional_missing": optional_missing,
        "warnings": warnings,
        "conflicts": conflicts,
        "suggestions": list(dict.fromkeys(suggestions)),
    }
