from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.services.customer_matching_facts_service import FactSource, FactValue, MatchingFactsAccessor
from backend.services.product_rule_engine import ProductRuleEngine


ENGINE = ProductRuleEngine()


def facts(path: str, value=None, status="known", *, unit=None, source=True):
    node = {}
    current = node
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    fact_source = FactSource(
        source_type="regression_fixture", source_id="sh-yichuan", source_date="2026-03-31",
        evidence_refs=["report:fact:1"],
    ) if source else None
    current[parts[-1]] = FactValue(
        value=value if status == "known" else None, status=status, unit=unit, source=fact_source,
        candidate_values=[] if status != "conflict" else [],
    )
    return node


def rule(field, operator, expected, *, severity="hard", action="exclude", rule_id="rule-1"):
    return {
        "rule_id": rule_id, "version_id": "version-1", "rule_group": "eligibility",
        "field_name": field, "operator": operator, "expected_value": expected,
        "severity": severity, "failure_action": action, "message": "", "source_text": "测试规则",
    }


def product_input(rules, fact_tree, *, status="published"):
    return ENGINE.evaluate_product(
        fact_tree,
        {"product_id": "product-1", "external_product_code": "TEST-001", "institution_name": "测试银行", "product_name": "测试产品"},
        {"product_id": "product-1", "version_id": "version-1", "status": status, "institution_name": "测试银行", "product_name": "测试产品"},
        rules,
    )


def result(path, actual, operator, expected, **kwargs):
    return ENGINE.evaluate_rule(MatchingFactsAccessor(facts(path, actual)), rule(path, operator, expected, **kwargs))


def test_eq_pass():
    assert result("credit.personal.current_overdue_count", 0, "eq", 0).result == "passed"


def test_eq_fail():
    assert result("credit.personal.current_overdue_count", 1, "eq", 0).result == "failed"


def test_unknown_fact_returns_unknown():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor(facts("asset.has_real_estate", status="unknown")), rule("asset.has_real_estate", "eq", True))
    assert value.result == "unknown"


def test_conflict_fact_returns_review():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor(facts("financial.revenue", status="conflict")), rule("financial.revenue", "gte", 1))
    assert value.result == "review"


def test_not_applicable_fact_returns_review_consistently():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor(facts("asset.has_vehicle", status="not_applicable")), rule("asset.has_vehicle", "not_exists", None))
    assert value.result == "review"


def test_zero_is_known_value():
    value = result("credit.personal.current_overdue_count", 0, "exists", None)
    assert value.result == "passed" and value.actual_value == 0


def test_decimal_compare():
    assert result("financial.debt_asset_ratio", Decimal("0.9953"), "lte", "0.70").result == "failed"
    assert result("financial.debt_asset_ratio", Decimal("0.70"), "eq", "0.700").result == "passed"


def test_between():
    assert result("requirement.term_months", 12, "between", [6, 12]).result == "passed"


def test_between_rejects_reversed_range():
    value = result("requirement.term_months", 12, "between", [24, 6])
    assert value.configuration_error.error_code == "invalid_expected_type"


def test_in_operator():
    assert result("customer.industry", "制造业", "in", ["制造业", "建筑业"]).result == "passed"


def test_not_in_operator():
    assert result("customer.industry", "建筑业", "not_in", ["房地产", "金融业"]).result == "passed"


def test_contains_string():
    assert result("customer.industry", "专用设备制造业", "contains", "制造业").result == "passed"


def test_contains_list():
    assert result("requirement.preferred_banks", ["建设银行", "中国银行"], "contains", "建设银行").result == "passed"


def test_contains_type_mismatch_is_configuration_error():
    value = result("customer.industry", "制造业", "contains", ["制造业"])
    assert value.configuration_error.error_code == "invalid_expected_type"


def test_exists_unknown_not_pass():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor(facts("asset.has_equipment", status="unknown")), rule("asset.has_equipment", "exists", None))
    assert value.result == "unknown"


def test_not_exists_unknown_not_pass():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor(facts("asset.has_equipment", status="unknown")), rule("asset.has_equipment", "not_exists", None))
    assert value.result == "unknown"


def test_known_value_not_exists_fails():
    assert result("asset.has_equipment", False, "not_exists", None).result == "failed"


def test_invalid_operator_configuration_error():
    value = result("requirement.term_months", 12, "matches", 12)
    assert value.result is None and value.configuration_error.error_code == "invalid_operator"


def test_invalid_expected_type_configuration_error():
    value = result("requirement.term_months", 12, "in", "12")
    assert value.result is None and value.configuration_error.error_code == "invalid_expected_type"


def test_field_outside_whitelist_configuration_error():
    value = ENGINE.evaluate_rule(MatchingFactsAccessor({}), rule("customer.secret", "eq", "x"))
    assert value.configuration_error.error_code == "field_not_whitelisted"


def test_unpublished_version_is_configuration_error():
    value = product_input([], {}, status="draft")
    assert value.evaluation_status == "product_configuration_error" and value.overall_status is None


def test_hard_fail_excludes_product():
    value = product_input([rule("requirement.amount", "lte", 5_000_000)], facts("requirement.amount", Decimal("8000000")))
    assert value.overall_status == "ineligible" and value.hard_fail_count == 1


def test_hard_unknown_makes_conditional():
    value = product_input(
        [rule("asset.has_confirmed_collateral", "eq", True, action="conditional")],
        facts("asset.has_confirmed_collateral", status="unknown"),
    )
    assert value.overall_status == "conditional" and value.hard_unknown_count == 1


def test_hard_unknown_with_exclude_still_does_not_exclude():
    value = product_input([rule("asset.has_real_estate", "eq", True)], facts("asset.has_real_estate", status="unknown"))
    assert value.overall_status == "conditional"


def test_soft_fail_does_not_exclude():
    value = product_input(
        [rule("financial.debt_asset_ratio", "lte", Decimal("0.70"), severity="soft")],
        facts("financial.debt_asset_ratio", Decimal("0.9953")),
    )
    assert value.overall_status == "eligible" and value.soft_fail_count == 1 and value.soft_gaps


def test_info_rule_does_not_affect_status():
    value = product_input(
        [rule("financial.debt_asset_ratio", "lte", Decimal("0.70"), severity="info")],
        facts("financial.debt_asset_ratio", Decimal("0.9953")),
    )
    assert value.overall_status == "eligible"


def test_review_action_makes_manual_review():
    value = product_input(
        [rule("financial.revenue", "gte", 1, action="review")], facts("financial.revenue", status="conflict"),
    )
    assert value.overall_status == "manual_review" and value.review_reasons


def test_amount_rule_800w_vs_500w_fails():
    value = product_input([rule("requirement.amount", "lte", 5_000_000)], facts("requirement.amount", Decimal("8000000"), unit="CNY"))
    assert value.failed_rules[0].result == "failed"
    assert "800万元" in value.failed_rules[0].explanation and "500万元" in value.failed_rules[0].explanation


def test_asset_unknown_not_false():
    value = product_input(
        [rule("asset.has_confirmed_collateral", "eq", True, action="conditional")],
        facts("asset.has_confirmed_collateral", status="unknown"),
    )
    assert not value.failed_rules and value.unknown_rules and value.overall_status == "conditional"


def test_evidence_propagated():
    value = result("credit.personal.current_overdue_count", 0, "eq", 0)
    assert value.fact_source.source_id == "sh-yichuan" and value.evidence_refs == ["report:fact:1"]


def test_catalog_evaluation():
    active = [{
        "product": {"product_id": "product-1", "external_product_code": "TEST-001", "institution_name": "测试银行", "product_name": "测试产品"},
        "version": {"product_id": "product-1", "version_id": "version-1", "status": "published"},
        "rules": [rule("credit.personal.current_overdue_count", "eq", 0)],
    }]
    values = ENGINE.evaluate_catalog(facts("credit.personal.current_overdue_count", 0), active)
    assert len(values) == 1 and values[0].overall_status == "eligible"


@pytest.mark.parametrize(
    ("field", "actual", "operator", "expected", "expected_result"),
    [
        ("credit.personal.current_overdue_count", 0, "eq", 0, "passed"),
        ("financial.debt_asset_ratio", Decimal("0.9953"), "lte", Decimal("0.70"), "failed"),
    ],
)
def test_shanghai_yichuan_known_regression_cases(field, actual, operator, expected, expected_result):
    assert result(field, actual, operator, expected).result == expected_result


def test_all_comparison_operators_are_executable():
    cases = [("ne", 6, 12, True), ("gt", 12, 6, True), ("gte", 12, 12, True), ("lt", 6, 12, True), ("lte", 12, 12, True)]
    for operator, actual, expected, passes in cases:
        assert (result("requirement.term_months", actual, operator, expected).result == "passed") is passes


def test_product_configuration_error_is_not_customer_ineligibility():
    value = product_input([rule("requirement.amount", "between", [10, 1])], facts("requirement.amount", 8))
    assert value.evaluation_status == "product_configuration_error"
    assert value.overall_status is None and value.configuration_errors

