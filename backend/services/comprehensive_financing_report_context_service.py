"""Read persisted customer facts without running extraction, OCR, or an LLM."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from backend.document_types import normalize_document_type_code
from backend.services.comprehensive_financing_report_model import (
    ComprehensiveFinancingReportModel,
    MaterialRecord,
)
from backend.services.enterprise_bank_statement_agent.customer_flow_aggregator import aggregate_customer_enterprise_flows
from backend.services.enterprise_bank_flow_diagnostic_service import build_enterprise_bank_flow_diagnostic_from_aggregated
from backend.services.enterprise_credit_diagnostic_service import build_enterprise_credit_diagnostic_from_payload
from backend.services.financial_report_agent.customer_report_aggregator import aggregate_customer_financial_reports
from backend.services.financial_statement_diagnostic_service import build_financial_statement_diagnostic_from_report
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile
from backend.services.personal_bank_statement_agent.customer_flow_aggregator import aggregate_customer_personal_flows
from backend.services.personal_credit_diagnostic_service import build_personal_credit_diagnostic_from_payload


MATERIAL_TYPES = (
    "enterprise_kyc", "enterprise_credit", "personal_credit", "enterprise_cashflow",
    "personal_cashflow", "financial_statements", "assets", "financing_requirement",
    "risk_assessment", "financing_plan",
)
ENTERPRISE_FLOW_TYPES = {"enterprise_flow", "enterprise_bank_statement", "bank_statement_enterprise", "company_bank_statement", "bank_statement", "企业流水"}
PERSONAL_FLOW_TYPES = {"personal_flow", "personal_bank_statement", "个人流水"}
ENTERPRISE_CREDIT_TYPES = {"enterprise_credit_report", "enterprise_credit"}
PERSONAL_CREDIT_TYPES = {"personal_credit_report", "personal_credit"}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first(*values: Any) -> Any:
    return next((value for value in values if value not in (None, "", [], {})), None)


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    result = str(value).strip()
    return result if result and result not in {"-", "未知", "未识别", "暂无"} else None


def _number(value: Any) -> float | None:
    if isinstance(value, dict):
        value = _first(value.get("normalized_value"), value.get("current_value"), value.get("value"), value.get("amount"))
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _date(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt != "%Y%m%d" else text[:8], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _latest_date(items: list[dict[str, Any]]) -> str | None:
    values = [_date(_first(item.get("uploaded_at"), item.get("created_at"), item.get("updated_at"))) for item in items]
    return max((value for value in values if value), default=None)


def _kind(item: dict[str, Any]) -> str:
    raw = str(item.get("extraction_type") or item.get("document_type") or "")
    return normalize_document_type_code(raw) or raw


def _payload(item: dict[str, Any]) -> dict[str, Any]:
    data = _dict(item.get("extracted_data"))
    return _dict(_first(*(data.get(key) for key in ("structured_data", "structured_json", "extracted_json", "parsed_json", "data", "report_json")), data))


def _section(status: str = "missing", source: str | None = None, as_of: str | None = None, **facts: Any) -> dict[str, Any]:
    return {"status": status, "source": source, "as_of": as_of, **facts}


def _source_date(payload: dict[str, Any], extraction: dict[str, Any] | None = None) -> str | None:
    info = _dict(_first(payload.get("basic_info"), payload.get("report_info"), payload.get("company_info"), payload.get("report_basic")))
    return _date(_first(info.get("report_date"), info.get("report_time"), info.get("information_report_date"), info.get("report_period_end"), payload.get("report_date"), payload.get("report_time"), (extraction or {}).get("created_at")))


def _candidate_fields(extraction: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    confirmed = _dict(extraction.get("confirmed_data"))
    confirmed = _dict(_first(confirmed.get("confirmed_fields"), confirmed.get("fields"), confirmed))
    payload = _payload(extraction)
    extracted = _dict(_first(payload.get("fields"), payload))
    return confirmed, extracted


def _subject(customer: dict[str, Any], profile: dict[str, Any], extractions: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    identity = _dict(profile.get("enterprise_identity"))
    person = _dict(profile.get("person_identity"))
    candidates: dict[str, list[str]] = {key: [] for key in ("enterprise_name", "legal_representative", "actual_controller")}
    fields: dict[str, Any] = {
        "enterprise_name": _first(identity.get("company_name"), customer.get("name")),
        "unified_social_credit_code": identity.get("unified_social_credit_code"),
        "legal_representative": identity.get("legal_representative"),
        "actual_controller": None,
        "shareholders": [],
        "registered_capital": identity.get("registered_capital"),
        "established_date": identity.get("establishment_date"),
        "enterprise_type": identity.get("company_type"),
        "registered_address": identity.get("registered_address"),
        "operating_address": None,
        "business_scope": identity.get("business_scope"),
        "industry": None,
        "technology_enterprise_tags": [],
    }
    for key, value in (("enterprise_name", customer.get("name")), ("enterprise_name", identity.get("company_name")), ("legal_representative", identity.get("legal_representative"))):
        if _text(value) and _text(value) not in candidates[key]:
            candidates[key].append(_text(value))
    for item in extractions:
        if _kind(item) in ENTERPRISE_CREDIT_TYPES:
            payload = _payload(item)
            report_basic = _dict(payload.get("report_basic"))
            credit_name = _text(report_basic.get("company_name"))
            if credit_name and credit_name not in candidates["enterprise_name"]:
                candidates["enterprise_name"].append(credit_name)
            controller = _text(_dict(payload.get("actual_controller")).get("name"))
            if controller and controller not in candidates["actual_controller"]:
                candidates["actual_controller"].append(controller)
            for personnel in _list(payload.get("key_personnel")):
                row = _dict(personnel)
                if "法定代表人" in str(row.get("position") or ""):
                    representative = _text(row.get("name"))
                    if representative and representative not in candidates["legal_representative"]:
                        candidates["legal_representative"].append(representative)
        if _kind(item) not in {"business_license", "company_articles", "id_card", "shareholder_id_card"}:
            continue
        confirmed, extracted = _candidate_fields(item)
        source_fields = {**extracted, **{key: value for key, value in confirmed.items() if value not in (None, "", [], {})}}
        if _kind(item) in {"business_license", "company_articles"}:
                for key, aliases in {
                    "enterprise_name": ("company_name", "enterprise_name"),
                    "legal_representative": ("legal_representative",),
                    "actual_controller": ("actual_controller", "ultimate_controller"),
                }.items():
                    value = _text(_first(*(source_fields.get(alias) for alias in aliases)))
                    if value and value not in candidates[key]:
                        candidates[key].append(value)
                for target, aliases in {
                    "operating_address": ("operating_address", "business_address"),
                    "industry": ("industry", "industry_name"),
                }.items():
                    fields[target] = _first(fields[target], *(source_fields.get(alias) for alias in aliases))
                shareholders = _list(source_fields.get("shareholders"))
                if shareholders and not fields["shareholders"]:
                    fields["shareholders"] = [{"name": _text(_first(_dict(x).get("name"), _dict(x).get("shareholder_name")))} for x in shareholders if _text(_first(_dict(x).get("name"), _dict(x).get("shareholder_name")))]
                tags = _list(source_fields.get("technology_enterprise_tags"))
                if tags:
                    fields["technology_enterprise_tags"] = [str(tag)[:80] for tag in tags if _text(tag)]
    conflicts = []
    for key, conflict_type in (("enterprise_name", "enterprise_subject"), ("legal_representative", "legal_representative"), ("actual_controller", "actual_controller")):
        if len(candidates[key]) > 1:
            fields[key] = None
            conflicts.append({"type": conflict_type, "status": "needs_review", "message": f"{key}存在多个结构化资料值，需核验主体关系"})
        elif candidates[key]:
            fields[key] = candidates[key][0]
    if not fields["legal_representative"] and _text(person.get("name")):
        # An identity card alone does not prove the legal-representative role.
        pass
    kyc_items = [item for item in extractions if _kind(item) in {"business_license", "company_articles", "id_card", "shareholder_id_card"}]
    has_confirmed_identity = any(_kind(item) == "business_license" and bool(_candidate_fields(item)[0]) for item in kyc_items)
    status = "needs_review" if conflicts else "confirmed" if has_confirmed_identity and identity.get("company_name") else "available" if identity.get("company_name") else "partial" if kyc_items or any(candidates.values()) else "missing"
    return _section(status, "kyc_profile", _latest_date(kyc_items), **fields), conflicts


def _credit(extractions: list[dict[str, Any]], enterprise: bool, people: dict[str, list[str]]) -> dict[str, Any]:
    kinds = ENTERPRISE_CREDIT_TYPES if enterprise else PERSONAL_CREDIT_TYPES
    items = sorted((item for item in extractions if _kind(item) in kinds), key=lambda x: _latest_date([x]) or "", reverse=True)
    if not items:
        return _section(**({"people": []} if not enterprise else {}))
    if enterprise:
        item = items[0]
        payload = _payload(item)
        diagnostic = build_enterprise_credit_diagnostic_from_payload(payload)
        debt = _dict(diagnostic.get("debt_summary"))
        loans = _list(_first(payload.get("loans"), payload.get("loan_accounts"), _dict(payload.get("credit_details")).get("loans")))
        guarantees = _list(_first(payload.get("guarantees"), payload.get("external_guarantees")))
        query_records = _list(payload.get("query_records"))
        info = _dict(_first(payload.get("basic_info"), payload.get("report_info"), payload.get("company_info"), payload.get("report_basic")))
        subject_name = _text(_first(info.get("company_name"), info.get("enterprise_name"), payload.get("company_name")))
        as_of = _source_date(payload, item)
        return _section("available", "enterprise_credit_extraction", as_of,
            subject_name=subject_name, outstanding_loan_balance=_number(debt.get("total_unsettled_balance")),
            outstanding_loan_institution_count=len({_text(_first(_dict(x).get("institution"), _dict(x).get("lender"))) for x in loans if _text(_first(_dict(x).get("institution"), _dict(x).get("lender")))}) if loans else None,
            outstanding_loans=[{"institution": _text(_first(x.get("institution"), x.get("institution_name"))),
                                "balance": _number(x.get("balance")), "due_date": _date(x.get("due_date")),
                                "classification": _text(_first(x.get("classification"), x.get("five_category")))} for x in loans[:30] if isinstance(x, dict)],
            overdue_summary={"count": len(_list(_dict(diagnostic.get("loan_summary")).get("overdue_loans")))},
            nonperforming_summary={"count": len(_list(_dict(diagnostic.get("loan_summary")).get("abnormal_classification_loans")))},
            five_classification=sorted({_text(_first(_dict(x).get("five_category"), _dict(x).get("classification"))) for x in loans if _text(_first(_dict(x).get("five_category"), _dict(x).get("classification")))}),
            guarantee_balance=_number(_dict(diagnostic.get("guarantee_summary")).get("external_guarantee_balance")),
            guarantee_records=[{"beneficiary": _text(x.get("beneficiary")), "balance": _number(_first(x.get("balance"), x.get("amount")))} for x in guarantees[:20] if isinstance(x, dict)],
            upcoming_or_past_due_records=[{"institution": _text(_dict(x).get("institution")), "balance": _number(_dict(x).get("balance")), "due_date": _date(_dict(x).get("due_date"))} for x in (_list(_dict(diagnostic.get("loan_summary")).get("upcoming_due_loans")) + _list(_dict(diagnostic.get("loan_summary")).get("overdue_loans")))[:20]],
            query_summary={"count": len(query_records) if "query_records" in payload else None},
            source_report_date=as_of, unit=_text(_first(payload.get("unit"), info.get("unit"), info.get("currency_unit"))))
    persons = []
    matched = set()
    unmatched = 0
    for item in items:
        payload = _payload(item)
        info = _dict(_first(payload.get("basic_info"), payload.get("report_info"), payload.get("identity")))
        name = _text(_first(info.get("name"), info.get("person_name"), payload.get("name")))
        if not name or name not in people:
            unmatched += 1
            continue
        if name in matched:
            continue
        matched.add(name)
        diagnostic = build_personal_credit_diagnostic_from_payload(payload)
        summary = _dict(_first(payload.get("credit_summary"), payload.get("summary")))
        debt = _dict(diagnostic.get("debt_summary"))
        cards = _list(payload.get("credit_card_accounts"))
        limit = _number(_first(summary.get("credit_card_limit"), summary.get("total_credit_limit")))
        if limit is None:
            amounts = [_number(_first(_dict(x).get("credit_limit"), _dict(x).get("limit"))) for x in cards]
            limit = round(sum(x for x in amounts if x is not None), 2) if any(x is not None for x in amounts) else None
        used = _number(debt.get("credit_card_used_amount"))
        related = _list(payload.get("related_repayment_responsibilities"))
        related_amounts = [_number(_first(_dict(x).get("balance"), _dict(x).get("guarantee_balance"), _dict(x).get("amount"))) for x in related]
        related_balance = _number(summary.get("related_repayment_balance"))
        if related_balance is None and any(amount is not None for amount in related_amounts):
            related_balance = round(sum(amount for amount in related_amounts if amount is not None), 2)
        as_of = _source_date(payload, item)
        persons.append({"name": name, "roles": people[name],
            "loan_balance": _number(debt.get("loan_balance")),
            "loan_account_count": len(_list(payload.get("loan_accounts"))),
            "credit_card_limit": limit, "credit_card_used": used,
            "credit_card_utilization": round(used / limit, 4) if used is not None and limit and limit > 0 else None,
            "overdue_summary": {"count": len(_list(_dict(diagnostic.get("overdue_summary")).get("overdue_records")))},
            "query_summary": {"last_3_months": _dict(diagnostic.get("query_summary")).get("last_3_months_query_count"), "last_6_months": _dict(diagnostic.get("query_summary")).get("last_6_months_query_count")},
            "related_repayment_balance": related_balance,
            "related_repayment_records": [{"institution": _text(x.get("institution")), "balance": _number(_first(x.get("balance"), x.get("amount"))), "responsibility_type": _text(x.get("responsibility_type"))} for x in related[:20] if isinstance(x, dict)],
            "source_report_date": as_of, "data_status": "available"})
    report_dates = [person.get("source_report_date") for person in persons if person.get("source_report_date")]
    return _section("needs_review" if unmatched else "available" if persons else "needs_review", "personal_credit_extraction", max(report_dates) if report_dates else _latest_date(items), people=persons, unmatched_report_count=unmatched)


def _read_only_override_rows(storage: Any, customer_id: str, kind: str) -> list[dict[str, Any]]:
    """Read existing review rows; never use helpers that call create_all()."""
    method = getattr(storage, f"list_{kind}", None)
    if callable(method):
        rows = method(customer_id)
        return rows if isinstance(rows, list) else []
    factory = getattr(storage, "_session_factory", None)
    if not callable(factory):
        return []
    try:
        from sqlalchemy import select
        from backend.db_models import CustomerFlowRule, IncomeConfirmationOverride
        with factory() as db:
            if kind == "income_confirmations":
                rows = db.execute(select(IncomeConfirmationOverride).where(
                    IncomeConfirmationOverride.customer_id == customer_id,
                    IncomeConfirmationOverride.source_type == "personal_flow",
                )).scalars().all()
                return [{"document_id": row.document_id, "counterparty_name": row.counterparty_name,
                         "income_type": row.income_type, "manual_status": row.manual_status,
                         "target_type": row.target_type, "confirmed_at": row.confirmed_at.isoformat() if row.confirmed_at else ""} for row in rows]
            row = db.execute(select(CustomerFlowRule).where(CustomerFlowRule.customer_id == customer_id)).scalar_one_or_none()
            if not row:
                return []
            import json
            def load(value: Any, fallback: Any) -> Any:
                try:
                    return json.loads(value) if value else fallback
                except (TypeError, ValueError):
                    return fallback
            return [{"customer_id": customer_id,
                     "related_company_names": load(row.related_company_names_json, []),
                     "self_account_numbers": load(row.self_account_numbers_json, []),
                     "internal_transfer_keywords": load(row.internal_transfer_keywords_json, []),
                     "operating_counterparty_whitelist": load(row.operating_counterparty_whitelist_json, []),
                     "internal_counterparty_blacklist": load(row.internal_counterparty_blacklist_json, []),
                     "personal_counterparty_names": load(row.personal_counterparty_names_json, []),
                     "manual_overrides": load(row.manual_overrides_json, {})}]
    except Exception:
        return []


def _enterprise_cashflow(extractions: list[dict[str, Any]], rules: dict[str, Any]) -> dict[str, Any]:
    items = [item for item in extractions if _kind(item) in ENTERPRISE_FLOW_TYPES]
    if not items:
        return _section()
    aggregated = aggregate_customer_enterprise_flows(items, rules=rules)
    if not aggregated.get("source_files"):
        return _legacy_enterprise_cashflow(items, rules)
    summary = _dict(aggregated.get("summary"))
    period = _dict(aggregated.get("statement_period"))
    diagnostic = build_enterprise_bank_flow_diagnostic_from_aggregated(aggregated)
    operating = _number(summary.get("operating_inflow"))
    excluded = _number(summary.get("excluded_inflow_total"))
    months = _number(period.get("months_count"))
    cp = _dict(aggregated.get("counterparty_summary"))
    monthly = _list(_first(aggregated.get("monthly_summary"), aggregated.get("monthly_stats")))
    return _section("available" if operating is not None else "partial", "enterprise_flow_aggregator", _date(period.get("end_date")) or _latest_date(items),
        account_count=len(_list(aggregated.get("accounts"))),
        statement_period={"start": _date(period.get("start_date")), "end": _date(period.get("end_date")), "months": int(months) if months else None},
        total_inflow=_number(summary.get("raw_total_inflow")), total_outflow=_number(summary.get("raw_total_outflow")),
        net_inflow=_number(summary.get("raw_net_cashflow")), operating_inflow=operating,
        operating_outflow=_number(summary.get("operating_outflow")),
        internal_transfer_inflow=_number(summary.get("internal_transfer_inflow")),
        internal_transfer_outflow=_number(summary.get("internal_transfer_outflow")),
        related_party_inflow=_number(summary.get("related_party_inflow")),
        related_party_outflow=_number(summary.get("related_party_outflow")),
        non_operating_inflow=excluded,
        monthly_average_operating_inflow=round(operating / months, 2) if operating is not None and months else None,
        monthly_trend=[{"month": _text(_first(_dict(x).get("month"), _dict(x).get("period"))),
                        "inflow": _number(_first(_dict(x).get("operating_inflow"), _dict(x).get("total_inflow")))} for x in monthly[:36] if isinstance(x, dict)],
        top_counterparties=[{"name": _text(_first(_dict(x).get("counterparty"), _dict(x).get("counterparty_name"))),
                             "inflow": _number(_dict(x).get("inflow")), "outflow": _number(_dict(x).get("outflow"))} for x in _list(cp.get("top_inflow_counterparties"))[:10] if isinstance(x, dict)],
        concentration_metrics={"top5_inflow_ratio": _number(cp.get("customer_concentration_top5_ratio"))},
        abnormal_large_transaction_count=_dict(diagnostic.get("quality_metrics")).get("large_in_out_count"),
        data_quality={"status": diagnostic.get("flow_status"), "unreviewed_suspicious_count": summary.get("unreviewed_suspicious_count")})


def _legacy_enterprise_cashflow(items: list[dict[str, Any]], rules: dict[str, Any]) -> dict[str, Any]:
    """Summarize saved bank_statement classifications; never reclassify or parse files."""
    customer_name = _text(rules.get("customer_name"))
    related_names = {str(name).strip() for key in ("related_company_names", "personal_counterparty_names", "internal_counterparty_blacklist")
                     for name in _list(rules.get(key)) if _text(name)}
    accounts: set[str] = set()
    units: set[str] = set()
    starts: list[str] = []
    ends: list[str] = []
    totals = {key: 0.0 for key in ("total_inflow", "total_outflow", "operating_inflow", "operating_outflow",
                                      "internal_transfer_inflow", "internal_transfer_outflow", "related_party_inflow",
                                      "related_party_outflow", "non_operating_inflow")}
    months: dict[str, float] = {}
    matched = 0
    for item in items:
        if _kind(item) != "bank_statement":
            continue
        payload = _payload(item)
        if not customer_name or _text(payload.get("account_name")) != customer_name:
            continue
        matched += 1
        account = _text(payload.get("account_no"))
        if account:
            accounts.add(account)
        if _text(payload.get("unit")):
            units.add(_text(payload.get("unit")))
        start, end = _date(payload.get("period_start")), _date(payload.get("period_end"))
        if start:
            starts.append(start)
        if end:
            ends.append(end)
        for transaction in _list(payload.get("transactions")):
            tx = _dict(transaction)
            if tx.get("is_valid_transaction") is False or tx.get("is_page_block") or tx.get("is_ocr_anomaly"):
                continue
            direction = tx.get("direction")
            if direction not in {"入账", "出账"}:
                continue
            amount = _number(tx.get("amount"))
            if amount is None:
                continue
            amount = abs(amount)
            suffix = "inflow" if direction == "入账" else "outflow"
            totals[f"total_{suffix}"] += amount
            counterparty = _text(tx.get("counterparty_name"))
            internal = bool(tx.get("is_self_transfer")) or bool(counterparty and counterparty == customer_name)
            related = bool(tx.get("is_related_person_transfer")) or bool(counterparty and counterparty in related_names)
            category = _text(tx.get("category"))
            operating = category == ("经营入账" if direction == "入账" else "经营出账") and not internal and not related and not tx.get("exclude_from_effective_flow")
            if internal:
                totals[f"internal_transfer_{suffix}"] += amount
            elif related:
                totals[f"related_party_{suffix}"] += amount
            elif operating:
                totals[f"operating_{suffix}"] += amount
                if direction == "入账":
                    date_key = _date(tx.get("transaction_time"))
                    if date_key:
                        months[date_key[:7]] = months.get(date_key[:7], 0.0) + amount
            elif direction == "入账":
                totals["non_operating_inflow"] += amount
    if not matched:
        return _section("partial", "bank_statement_extraction", _latest_date(items), account_count=None)
    start, end = min(starts) if starts else None, max(ends) if ends else None
    month_count = (int(end[:4]) - int(start[:4])) * 12 + int(end[5:7]) - int(start[5:7]) + 1 if start and end else None
    operating = round(totals["operating_inflow"], 2)
    related_reliably_classified = bool(related_names or totals["related_party_inflow"] or totals["related_party_outflow"])
    return _section("partial", "saved_bank_statement_classification", end or _latest_date(items),
        account_count=len(accounts) if accounts else None,
        unit=next(iter(units)) if len(units) == 1 else None,
        statement_period={"start": start, "end": end, "months": month_count},
        **{key: (None if key.startswith("related_party_") and not related_reliably_classified else round(value, 2)) for key, value in totals.items()},
        net_inflow=round(totals["total_inflow"] - totals["total_outflow"], 2),
        monthly_average_operating_inflow=round(operating / month_count, 2) if month_count else None,
        monthly_trend=[{"month": key, "inflow": round(value, 2)} for key, value in sorted(months.items())],
        top_counterparties=[], concentration_metrics={}, abnormal_large_transaction_count=None,
        data_quality={"status": "partial", "notes": "基于已保存的经营入账分类；关联关系和未识别交易需复核"})


def _personal_cashflow(extractions: list[dict[str, Any]], confirmations: list[dict[str, Any]]) -> dict[str, Any]:
    items = [item for item in extractions if _kind(item) in PERSONAL_FLOW_TYPES]
    if not items:
        return _section()
    aggregated = aggregate_customer_personal_flows(items, income_confirmations=confirmations)
    if not aggregated.get("source_document_count"):
        return _section("partial", "personal_flow_extraction", _latest_date(items))
    summary = _dict(aggregated.get("customer_level_summary"))
    confirmed = _number(summary.get("confirmed_salary_income"))
    manual = _number(summary.get("manual_confirmed_salary_income"))
    usable = round((confirmed or 0) + (manual or 0), 2) if confirmed is not None or manual is not None else None
    return _section("available" if usable is not None and usable > 0 else "partial", "personal_flow_aggregator", _date(summary.get("period_end")) or _latest_date(items),
        account_count=summary.get("account_count"),
        statement_period={"start": _date(summary.get("period_start")), "end": _date(summary.get("period_end"))},
        confirmed_salary_income=confirmed, manually_confirmed_salary_income=manual,
        suspected_salary_income=_number(summary.get("suspected_salary_income")),
        usable_salary_income=usable, other_income=_number(summary.get("verified_other_stable_income")),
        fixed_expense=_number(summary.get("fixed_expense")), debt_repayment=_number(summary.get("loan_repayment_expense")))


FINANCIAL_FIELDS = {
    "revenue": ("income_statement", "revenue"), "operating_cost": ("income_statement", "operating_cost"),
    "gross_profit": ("income_statement", "gross_profit"), "net_profit": ("income_statement", "net_profit"),
    "total_assets": ("balance_sheet", "total_assets"), "total_liabilities": ("balance_sheet", "total_liabilities"),
    "net_assets": ("balance_sheet", "total_equity"), "accounts_receivable": ("balance_sheet", "accounts_receivable"),
    "inventory": ("balance_sheet", "inventory"), "short_term_borrowings": ("balance_sheet", "short_term_loans"),
    "long_term_borrowings": ("balance_sheet", "long_term_loans"),
    "operating_cashflow": ("cash_flow_statement", "net_operating_cash_flow"),
}


def _financials(extractions: list[dict[str, Any]]) -> dict[str, Any]:
    items = [item for item in extractions if _kind(item) in {"financial_report", "financial_data", "财务报表", "财务数据"}]
    aggregate = aggregate_customer_financial_reports(items)
    all_reports = _list(aggregate.get("reports"))
    reports = all_reports[-3:]
    if not reports:
        return _section(periods=[], period_count=0)
    periods = []
    for report in reports:
        info = _dict(report.get("company_info"))
        row = {key: _number(_dict(report.get(group)).get(field)) for key, (group, field) in FINANCIAL_FIELDS.items()}
        row["period"] = _date(_first(info.get("report_period_end"), info.get("report_date")))
        row["period_type"] = _text(info.get("report_type"))
        row["source_date"] = _date(info.get("report_date"))
        row["unit"] = _text(info.get("unit"))
        if row["net_assets"] is None and row["total_assets"] is not None and row["total_liabilities"] is not None:
            row["net_assets"] = round(row["total_assets"] - row["total_liabilities"], 2)
        if row["gross_profit"] is None and row["revenue"] is not None and row["operating_cost"] is not None:
            row["gross_profit"] = round(row["revenue"] - row["operating_cost"], 2)
        row["debt_asset_ratio"] = round(row["total_liabilities"] / row["total_assets"], 4) if row["total_liabilities"] is not None and row["total_assets"] and row["total_assets"] > 0 else None
        periods.append(row)
    latest = periods[-1]
    def growth(key: str) -> float | None:
        if len(periods) < 2 or periods[-2][key] in (None, 0) or latest[key] is None:
            return None
        return round((latest[key] - periods[-2][key]) / abs(periods[-2][key]), 4)
    return _section("available" if latest.get("revenue") is not None or latest.get("total_assets") is not None else "partial",
        "financial_report_aggregator", latest.get("period") or _latest_date(items), periods=periods,
        period_count=len(periods), available_period_count=len(all_reports),
        latest_period=latest.get("period"), latest=latest,
        trends={"revenue_growth": growth("revenue"), "profit_growth": growth("net_profit"),
                "receivable_growth": growth("accounts_receivable"),
                "operating_cashflow_change": (round(latest["operating_cashflow"] - periods[-2]["operating_cashflow"], 2)
                                              if len(periods) > 1 and latest["operating_cashflow"] is not None and periods[-2]["operating_cashflow"] is not None else None)},
        diagnostic_status=build_financial_statement_diagnostic_from_report(reports[-1]).get("financial_status"))


def _assets(profile: dict[str, Any], extractions: list[dict[str, Any]]) -> dict[str, Any]:
    saved = _dict(profile.get("assets"))
    categories = {"property": _list(saved.get("properties")), "vehicle": _list(saved.get("vehicles")),
                  "equipment": [], "intellectual_property": [], "equity": [], "deposit": [], "other_collateral": []}
    asset_types = {"property_cert": "property", "real_estate_cert": "property", "real_estate_query": "property",
                   "vehicle_license": "vehicle", "equipment": "equipment", "intellectual_property": "intellectual_property",
                   "equity": "equity", "deposit": "deposit", "collateral": "other_collateral"}
    for item in extractions:
        group = asset_types.get(_kind(item))
        if group:
            confirmed, extracted = _candidate_fields(item)
            fields = {**extracted, **confirmed}
            categories[group].append({"name": _text(_first(fields.get("name"), fields.get("asset_name"), fields.get("property_address"), fields.get("address"))),
                                      "type": group, "source_document_id": item.get("doc_id"),
                                      "valuation_basis": _text(fields.get("valuation_basis")),
                                      "market_value": fields.get("market_value")})
    result = {}
    for group, items in categories.items():
        result[group] = []
        for item in items[:30]:
            data = _dict(item)
            basis = _text(_first(data.get("valuation_basis"), data.get("appraisal_source")))
            value = _number(_first(data.get("market_value"), data.get("appraised_value"))) if basis else None
            result[group].append({"name": _text(_first(data.get("name"), data.get("property_address"), data.get("address"))),
                                  "market_value": value, "value_basis": basis if value is not None else None,
                                  "source_document_id": _text(data.get("source_document_id"))})
    asset_items = [item for item in extractions if _kind(item) in {"property_cert", "real_estate_cert", "real_estate_query", "vehicle_license", *asset_types}]
    return _section("available" if any(result.values()) else "missing", "kyc_asset_profile" if any(result.values()) else None,
                    _latest_date(asset_items), **result)


REQUIREMENT_KEYS = {
    "financing_subject": ("financing_subject", "borrower", "申请主体"),
    "amount": ("financing_amount", "loan_amount", "amount", "融资金额", "申请金额"),
    "currency": ("currency", "币种"), "amount_unit": ("amount_unit", "unit", "金额单位"),
    "purpose": ("financing_purpose", "loan_purpose", "purpose", "贷款用途"),
    "term": ("financing_term", "loan_term", "term", "贷款期限"),
    "expected_use_date": ("expected_use_date", "use_date", "用款时间"),
    "repayment_preference": ("repayment_preference", "还款偏好"),
    "guarantee_preference": ("guarantee_preference", "担保偏好"),
    "collateral": ("collateral", "抵押物"),
    "existing_banks": ("existing_banks", "现有银行"),
    "preferred_banks": ("preferred_banks", "偏好银行"),
    "excluded_banks": ("excluded_banks", "排除银行"),
    "registered_location": ("registered_location", "注册地"),
    "operating_location": ("operating_location", "经营地"),
}


def _flatten_explicit(data: dict[str, Any]) -> list[dict[str, Any]]:
    sections = [data]
    for value in data.values():
        if isinstance(value, dict):
            sections.extend(_flatten_explicit(value))
    return sections


def _requirement(applications: list[dict[str, Any]], diagnostic_snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    for application in sorted(applications, key=lambda x: str(x.get("savedAt") or ""), reverse=True):
        if application.get("stale"):
            continue
        sections = _flatten_explicit(_dict(application.get("applicationData")))
        facts = {}
        for key, aliases in REQUIREMENT_KEYS.items():
            found = _first(*(section.get(alias) for section in sections for alias in aliases))
            facts[key] = _number(found) if key == "amount" else _text(found)
        if any(facts.get(key) is not None for key in ("amount", "purpose", "term", "expected_use_date")):
            return _section("available" if facts.get("amount") is not None and facts.get("amount_unit") and facts.get("purpose") else "partial",
                            "saved_application", _date(application.get("savedAt")), **facts)
    for snapshot in sorted(diagnostic_snapshots, key=lambda x: str(x.get("generated_at") or ""), reverse=True):
        explicit = _dict(_dict(snapshot.get("report_json")).get("financing_requirement"))
        if not explicit:
            continue
        facts = {key: (_number(_first(*(explicit.get(alias) for alias in aliases))) if key == "amount"
                       else _text(_first(*(explicit.get(alias) for alias in aliases)))) for key, aliases in REQUIREMENT_KEYS.items()}
        if any(facts.get(key) is not None for key in ("amount", "purpose", "term")):
            return _section("available" if facts.get("amount") is not None and facts.get("amount_unit") and facts.get("purpose") else "partial",
                            "financing_diagnostic_snapshot", _date(snapshot.get("generated_at")), **facts)
    return _section("missing", None, None, **{key: None for key in REQUIREMENT_KEYS})


def _risk_context(risk: dict[str, Any], profile: dict[str, Any], latest_material_date: str | None) -> dict[str, Any]:
    if not risk:
        return _section()
    generated = _date(risk.get("generated_at"))
    profile_updated = _date(profile.get("updated_at"))
    stale = bool((generated and latest_material_date and latest_material_date > generated)
                 or (generated and profile_updated and profile_updated > generated)
                 or (profile.get("version") and risk.get("profile_version") and int(profile["version"]) > int(risk["profile_version"])))
    report = _dict(risk.get("report_json"))
    overall = _dict(report.get("overall_assessment"))
    return _section("needs_review" if stale else "available", "customer_risk_report", generated,
                    source_type="customer_risk_report", source_id=_text(risk.get("report_id")),
                    generated_at=generated, stale=stale,
                    risk_level=_text(overall.get("risk_level")), total_score=_number(overall.get("total_score")))


def _plan_context(scheme: dict[str, Any]) -> dict[str, Any]:
    if not scheme:
        return _section()
    # Stored scheme results are reference material, never recomputed here.
    return _section("available", "scheme_snapshot", _date(_first(scheme.get("updated_at"), scheme.get("created_at"))),
                    source_type="scheme_snapshot", source_id=_text(scheme.get("snapshot_id")),
                    has_saved_result=bool(scheme.get("summary_markdown") or scheme.get("raw_result")))


def _material(section_type: str, section: dict[str, Any], note: str = "") -> MaterialRecord:
    status = section.get("status") or "missing"
    return MaterialRecord(type=section_type, status=status, latest_date=section.get("as_of"),
                          usable=status in {"confirmed", "available", "partial"}, notes=note)


def _conflict(kind: str, message: str) -> dict[str, str]:
    return {"type": kind, "status": "needs_review", "message": message}


def _cross_source_conflicts(subject: dict[str, Any], enterprise_credit: dict[str, Any], personal_credit: dict[str, Any],
                            financials: dict[str, Any], cashflow: dict[str, Any], source_dates: dict[str, Any]) -> list[dict[str, str]]:
    conflicts = []
    kyc_name = _text(subject.get("enterprise_name"))
    credit_name = _text(enterprise_credit.get("subject_name"))
    if kyc_name and credit_name and kyc_name != credit_name:
        conflicts.append(_conflict("enterprise_subject", "企业征信主体与KYC主体名称不同，需核验资料归属"))
    for person in _list(personal_credit.get("people")):
        if _dict(person).get("name") not in {subject.get("legal_representative"), subject.get("actual_controller")}:
            conflicts.append(_conflict("personal_credit_subject", "个人征信主体与已确认的法定代表人、实际控制人关系不一致"))
            break
    latest = _dict(financials.get("latest"))
    financial_borrowing = None
    if latest.get("short_term_borrowings") is not None and latest.get("long_term_borrowings") is not None:
        financial_borrowing = latest["short_term_borrowings"] + latest["long_term_borrowings"]
    credit_balance = _number(enterprise_credit.get("outstanding_loan_balance"))
    credit_date = _date(enterprise_credit.get("source_report_date"))
    financial_date = _date(latest.get("period"))
    near_dates = bool(credit_date and financial_date and abs((date.fromisoformat(credit_date) - date.fromisoformat(financial_date)).days) <= 90)
    if (financial_borrowing is not None and credit_balance is not None and near_dates
            and latest.get("unit") and latest.get("unit") == enterprise_credit.get("unit")
            and abs(financial_borrowing - credit_balance) > max(1.0, abs(financial_borrowing) * 0.05)):
        conflicts.append(_conflict("credit_vs_financial_debt", "企业征信融资余额与财务报表借款余额口径或时点存在差异，需核验"))
    revenue = _number(latest.get("revenue"))
    operating = _number(cashflow.get("operating_inflow"))
    flow_period = _dict(cashflow.get("statement_period"))
    flow_start, flow_end = _date(flow_period.get("start")), _date(flow_period.get("end"))
    annual_coverage = bool(flow_start and flow_end and (date.fromisoformat(flow_end) - date.fromisoformat(flow_start)).days >= 330)
    if (revenue is not None and operating is not None and latest.get("unit") == "元"
            and latest.get("period_type") == "annual" and annual_coverage
            and latest.get("period") and flow_end
            and latest["period"][:4] == flow_end[:4]
            and abs(revenue - operating) > max(1.0, abs(revenue) * 0.1)):
        conflicts.append(_conflict("financial_vs_cashflow_income", "财务收入与银行流水经营入账口径存在差异，需结合期间与回款方式核验"))
    dates = [_date(value) for key, value in source_dates.items() if key in {"enterprise_credit", "personal_credit", "financial_latest_period", "kyc_updated_at", "enterprise_cashflow_end", "personal_cashflow_end"}]
    dates = [date.fromisoformat(value) for value in dates if value]
    if len(dates) >= 2 and (max(dates) - min(dates)).days > 365:
        conflicts.append(_conflict("source_date_gap", "关键资料时点跨度超过一年，综合分析时需核验期间可比性"))
    return conflicts


async def _optional(storage: Any, method_name: str, *args: Any, default: Any = None) -> Any:
    method = getattr(storage, method_name, None)
    if not callable(method):
        return default
    import inspect
    result = method(*args)
    return await result if inspect.isawaitable(result) else result


async def build_comprehensive_financing_report_context(
    storage_service: Any, customer_id: str,
) -> ComprehensiveFinancingReportModel:
    """Build one isolated fact model from already persisted customer data."""
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise ValueError("未找到该客户记录")
    raw_extractions = await storage_service.get_extractions_by_customer(customer_id)
    documents = _list(await _optional(storage_service, "list_documents", customer_id, default=[]))
    active_documents = {str(row.get("doc_id")): row.get("is_active") is not False
                        for row in documents if isinstance(row, dict) and row.get("doc_id")}
    extractions = [item for item in _list(raw_extractions) if isinstance(item, dict)
                   and str(item.get("extraction_status") or "success").lower() in {"success", "completed", "partial", "partial_success", "成功", "部分成功"}
                   and item.get("is_active") is not False
                   and active_documents.get(str(item.get("doc_id")), True)]
    kyc = await build_customer_kyc_profile(storage_service, customer_id)
    subject, subject_conflicts = _subject(_dict(customer), kyc, extractions)
    roles: dict[str, list[str]] = {}
    for key, role in (("legal_representative", "法定代表人"), ("actual_controller", "实际控制人")):
        name = _text(subject.get(key))
        if name:
            roles.setdefault(name, []).append(role)
    enterprise_credit = _credit(extractions, True, roles)
    personal_credit = _credit(extractions, False, roles)
    rule_rows = _read_only_override_rows(storage_service, customer_id, "enterprise_flow_rules")
    rules = _dict(rule_rows[0]) if rule_rows else {"customer_name": customer.get("name"), "customer_id": customer_id}
    rules.setdefault("customer_name", customer.get("name"))
    enterprise_cashflow = _enterprise_cashflow(extractions, rules)
    personal_cashflow = _personal_cashflow(extractions, _read_only_override_rows(storage_service, customer_id, "income_confirmations"))
    financials = _financials(extractions)
    assets = _assets(kyc, extractions)
    applications = await _optional(storage_service, "list_saved_applications", customer_id, default=[])
    diagnostic_snapshots = await _optional(storage_service, "list_financing_diagnostic_report_snapshots", customer_id, default=[])
    requirement = _requirement(_list(applications), _list(diagnostic_snapshots))
    risk = _dict(await _optional(storage_service, "get_latest_customer_risk_report", customer_id, default={}))
    profile = _dict(await _optional(storage_service, "get_customer_profile", customer_id, default={}))
    scheme = _dict(await _optional(storage_service, "get_latest_scheme_snapshot", customer_id, default={}))
    source_dates = {
        "enterprise_credit": enterprise_credit.get("as_of"), "personal_credit": personal_credit.get("as_of"),
        "enterprise_cashflow_start": _dict(enterprise_cashflow.get("statement_period")).get("start"),
        "enterprise_cashflow_end": _dict(enterprise_cashflow.get("statement_period")).get("end"),
        "personal_cashflow_start": _dict(personal_cashflow.get("statement_period")).get("start"),
        "personal_cashflow_end": _dict(personal_cashflow.get("statement_period")).get("end"),
        "financial_latest_period": financials.get("latest_period"), "kyc_updated_at": subject.get("as_of"),
        "risk_assessment_generated_at": _date(risk.get("generated_at")),
    }
    material_dates = [_date(value) for value in source_dates.values()]
    risk_context = _risk_context(risk, profile, max((x for x in material_dates if x), default=None))
    plan_context = _plan_context(scheme)
    sections = {
        "enterprise_kyc": subject, "enterprise_credit": enterprise_credit, "personal_credit": personal_credit,
        "enterprise_cashflow": enterprise_cashflow, "personal_cashflow": personal_cashflow,
        "financial_statements": financials, "assets": assets, "financing_requirement": requirement,
        "risk_assessment": risk_context, "financing_plan": plan_context,
    }
    materials = [_material(kind, sections[kind]) for kind in MATERIAL_TYPES]
    people = _list(personal_credit.get("people"))
    personal_balances = [_number(_dict(person).get("loan_balance")) for person in people]
    related_balances = [_number(_dict(person).get("related_repayment_balance")) for person in people]
    derived = {
        "total_enterprise_credit_balance": enterprise_credit.get("outstanding_loan_balance"),
        "total_personal_credit_balance": round(sum(x for x in personal_balances if x is not None), 2) if any(x is not None for x in personal_balances) else None,
        "total_related_repayment_balance": round(sum(x for x in related_balances if x is not None), 2) if any(x is not None for x in related_balances) else None,
        "enterprise_operating_inflow": enterprise_cashflow.get("operating_inflow"),
        "enterprise_total_inflow": enterprise_cashflow.get("total_inflow"),
        "monthly_average_operating_inflow": enterprise_cashflow.get("monthly_average_operating_inflow"),
        "debt_asset_ratio": _dict(financials.get("latest")).get("debt_asset_ratio"),
        "credit_card_utilization": {str(_dict(person).get("name")): _dict(person).get("credit_card_utilization") for person in people},
        "enterprise_overdue_count": _dict(enterprise_credit.get("overdue_summary")).get("count"),
        "enterprise_query_count": _dict(enterprise_credit.get("query_summary")).get("count"),
        "personal_overdue_count": sum(int(_dict(_dict(person).get("overdue_summary")).get("count") or 0) for person in people) if people else None,
        "personal_query_counts": {str(_dict(person).get("name")): _dict(person).get("query_summary") for person in people},
        "financial_period_count": financials.get("period_count"),
    }
    conflicts = subject_conflicts + _cross_source_conflicts(subject, enterprise_credit, personal_credit, financials, enterprise_cashflow, source_dates)
    if personal_credit.get("unmatched_report_count") and roles:
        conflicts.append(_conflict("personal_credit_subject", "个人征信主体未能与已知法定代表人或实际控制人对应，需核验资料归属"))
    if conflicts and subject.get("status") != "missing":
        subject["status"] = "needs_review" if any(x["type"] in {"enterprise_subject", "legal_representative", "actual_controller"} for x in conflicts) else subject["status"]
        materials[0] = _material("enterprise_kyc", subject)
    available_count = sum(item.usable for item in materials)
    missing_count = sum(item.status == "missing" for item in materials)
    derived.update({
        "material_completeness_ratio": round(available_count / len(materials), 4),
        "available_material_count": available_count,
        "missing_material_count": missing_count,
    })
    return ComprehensiveFinancingReportModel(
        customer={"id": customer_id, "name": _text(customer.get("name")), "type": _text(customer.get("customer_type"))},
        data_scope={"materials": [item.model_dump() for item in materials]},
        subject_profile=subject, financing_requirement=requirement, enterprise_credit=enterprise_credit,
        personal_credit=personal_credit, enterprise_cashflow=enterprise_cashflow, personal_cashflow=personal_cashflow,
        financials=financials, assets=assets, risk_context=risk_context, existing_financing_plan=plan_context,
        derived_metrics=derived, conflicts=conflicts,
        data_quality={"available_material_count": available_count, "missing_material_count": missing_count,
                      "status": "needs_review" if conflicts else "missing" if available_count == 0 else "partial" if missing_count else "available"}, source_dates=source_dates,
    )
