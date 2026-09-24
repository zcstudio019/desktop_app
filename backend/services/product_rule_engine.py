"""Deterministic execution of published product rules against matching facts.

The engine is deliberately isolated from persistence and source documents.  It
only accepts a whitelist enforcing ``MatchingFactsAccessor``, a published
product version, and explicit ``FinancingProductRule`` values.
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.services.customer_matching_facts_service import (
    FactSource,
    FactValue,
    MATCHING_FACT_FIELD_TYPES,
    MatchingFactsAccessor,
)
from backend.services.product_catalog_service import FAILURE_ACTIONS, OPERATORS, SEVERITIES


RuleResult = Literal["passed", "failed", "unknown", "review"]
ProductStatus = Literal["eligible", "conditional", "ineligible", "manual_review"]


class RuleConfigurationError(BaseModel):
    product_id: str = ""
    version_id: str = ""
    rule_id: str = ""
    error_code: str
    message: str


class RuleEvaluationResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    rule_id: str
    rule_source: str = "product_rule"
    field_name: str
    operator: str
    expected_value: Any = None
    actual_value: Any = None
    actual_status: str = "unknown"
    result: RuleResult | None = None
    severity: str
    failure_action: str
    message: str = ""
    explanation: str = ""
    source_text: str = ""
    fact_source: FactSource | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    configuration_error: RuleConfigurationError | None = None


class ProductEvaluationResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    product_id: str
    version_id: str
    external_product_code: str = ""
    institution_name: str = ""
    product_name: str = ""
    evaluation_status: Literal["success", "product_configuration_error"]
    overall_status: ProductStatus | None = None
    rule_results: list[RuleEvaluationResult] = Field(default_factory=list)
    passed_rules: list[RuleEvaluationResult] = Field(default_factory=list)
    failed_rules: list[RuleEvaluationResult] = Field(default_factory=list)
    unknown_rules: list[RuleEvaluationResult] = Field(default_factory=list)
    review_rules: list[RuleEvaluationResult] = Field(default_factory=list)
    hard_fail_count: int = 0
    hard_unknown_count: int = 0
    soft_fail_count: int = 0
    blocking_reasons: list[str] = Field(default_factory=list)
    missing_information: list[str] = Field(default_factory=list)
    review_reasons: list[str] = Field(default_factory=list)
    soft_gaps: list[str] = Field(default_factory=list)
    configuration_errors: list[RuleConfigurationError] = Field(default_factory=list)


FIELD_LABELS = {
    "requirement.amount": "本次融资金额",
    "requirement.term_months": "本次融资期限",
    "credit.personal.current_overdue_count": "个人征信当前逾期次数",
    "financial.debt_asset_ratio": "资产负债率",
    "asset.has_confirmed_collateral": "已确认抵押物",
}


def _row_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, BaseModel):
        return value.model_dump()
    table = getattr(value, "__table__", None)
    if table is not None:
        return {column.name: getattr(value, column.name) for column in table.columns}
    raise TypeError("产品、版本和规则必须是结构化对象")


def _decimal(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("不是有效数值")
    try:
        result = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("不是有效数值") from exc
    if not result.is_finite():
        raise ValueError("不是有限数值")
    return result


def _parse_expected(raw: Any) -> Any:
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("expected_value_json 不是合法 JSON") from exc


def _configuration_error(
    rule: dict[str, Any], product_id: str, version_id: str, code: str, message: str,
    *, expected: Any = None, fact: FactValue | None = None,
) -> RuleEvaluationResult:
    error = RuleConfigurationError(
        product_id=product_id, version_id=version_id, rule_id=str(rule.get("rule_id") or ""),
        error_code=code, message=message,
    )
    source = fact.source if fact else None
    return RuleEvaluationResult(
        rule_id=error.rule_id, field_name=str(rule.get("field_name") or ""),
        operator=str(rule.get("operator") or ""), expected_value=expected,
        actual_value=fact.value if fact else None, actual_status=fact.status if fact else "unknown",
        severity=str(rule.get("severity") or ""), failure_action=str(rule.get("failure_action") or ""),
        message=str(rule.get("message") or ""), source_text=str(rule.get("source_text") or ""),
        fact_source=source, evidence_refs=list(source.evidence_refs) if source else [],
        configuration_error=error, explanation=f"产品规则配置错误：{message}",
    )


def _validate_expected(field_kind: str, operator: str, expected: Any) -> Any:
    if operator in {"exists", "not_exists"}:
        if expected is not None:
            raise ValueError("存在性规则的预期值必须为空")
        return None
    if operator in {"in", "not_in"}:
        if not isinstance(expected, list):
            raise ValueError("in/not_in 的预期值必须是数组")
        return [_normalize(item, field_kind if field_kind != "list" else None) for item in expected]
    if operator == "between":
        if not isinstance(expected, list) or len(expected) != 2:
            raise ValueError("between 的预期值必须是两个元素的数组")
        if field_kind != "number":
            raise ValueError("between 只支持数值字段")
        values = [_decimal(item) for item in expected]
        if values[0] > values[1]:
            raise ValueError("between 区间下界不能大于上界")
        return values
    if operator in {"gt", "gte", "lt", "lte"}:
        if field_kind != "number":
            raise ValueError("大小比较只支持数值字段")
        return _decimal(expected)
    if operator == "contains":
        if field_kind == "string":
            if not isinstance(expected, str):
                raise ValueError("文本 contains 的预期值必须是文本")
        elif field_kind == "list":
            if isinstance(expected, (list, dict)) or expected is None:
                raise ValueError("列表 contains 的预期值必须是单个元素")
        else:
            raise ValueError("contains 只支持文本或列表字段")
        return expected
    return _normalize(expected, field_kind)


def _normalize(value: Any, kind: str | None) -> Any:
    if kind == "number":
        return _decimal(value)
    if kind == "boolean":
        if not isinstance(value, bool):
            raise ValueError("布尔字段的值必须为 true 或 false")
    elif kind in {"string", "date"}:
        if not isinstance(value, str):
            raise ValueError("文本或日期字段的值必须为文本")
    elif kind == "list":
        if not isinstance(value, list):
            raise ValueError("列表字段的值必须为数组")
    return value


def _evaluate_known(actual: Any, expected: Any, operator: str, kind: str) -> bool:
    normalized_actual = _normalize(actual, kind)
    if operator == "eq":
        return normalized_actual == expected
    if operator == "ne":
        return normalized_actual != expected
    if operator == "in":
        return normalized_actual in expected
    if operator == "not_in":
        return normalized_actual not in expected
    if operator == "gt":
        return normalized_actual > expected
    if operator == "gte":
        return normalized_actual >= expected
    if operator == "lt":
        return normalized_actual < expected
    if operator == "lte":
        return normalized_actual <= expected
    if operator == "between":
        return expected[0] <= normalized_actual <= expected[1]
    if operator == "contains":
        return expected in normalized_actual
    if operator == "exists":
        return True
    if operator == "not_exists":
        return False
    raise ValueError("不支持的操作符")


def _display(value: Any, field_name: str) -> str:
    if isinstance(value, Decimal):
        if field_name == "financial.debt_asset_ratio":
            return f"{value * 100:.2f}%"
        if field_name == "requirement.amount":
            wan = value / Decimal("10000")
            return f"{format(wan.normalize(), 'f')}万元"
        return format(value, "f")
    if isinstance(value, bool):
        return "是" if value else "否"
    return str(value)


def _explanation(field_name: str, result: RuleResult, actual: Any, expected: Any, operator: str) -> str:
    label = FIELD_LABELS.get(field_name, field_name)
    if result == "unknown":
        if field_name == "asset.has_confirmed_collateral":
            return "产品要求确认抵押物，但当前资产资料尚未提供。"
        return f"{label}资料不足，暂无法自动判断。"
    if result == "review":
        return f"{label}存在多来源冲突或不适用状态，需要人工核验。"
    if field_name == "requirement.amount" and operator == "lte" and result == "failed":
        return f"客户本次融资需求{_display(actual, field_name)}，高于产品规则允许的{_display(expected, field_name)}。"
    if field_name == "financial.debt_asset_ratio" and operator == "lte" and result == "failed":
        return f"当前资产负债率{_display(actual, field_name)}，高于产品要求{_display(expected, field_name)}。"
    if field_name == "credit.personal.current_overdue_count" and operator == "eq" and expected == Decimal("0"):
        return "个人征信当前逾期次数为0，满足产品要求。" if result == "passed" else "个人征信存在当前逾期，不满足产品要求。"
    outcome = "满足" if result == "passed" else "不满足"
    return f"{label}实际值为{_display(actual, field_name)}，{outcome}产品要求。"


class ProductRuleEngine:
    """Evaluate explicit product rules without database, source document, or AI access."""

    def evaluate_rule(
        self, accessor: MatchingFactsAccessor, rule_value: Any, *, product_id: str = "", version_id: str = "",
    ) -> RuleEvaluationResult:
        rule = _row_dict(rule_value)
        field_name = str(rule.get("field_name") or "")
        operator = str(rule.get("operator") or "")
        severity = str(rule.get("severity") or "")
        action = str(rule.get("failure_action") or "")
        has_direct_expected = "expected_value" in rule
        raw_expected = rule.get("expected_value") if has_direct_expected else rule.get("expected_value_json")
        try:
            expected = raw_expected if has_direct_expected else _parse_expected(raw_expected)
        except ValueError as exc:
            return _configuration_error(rule, product_id, version_id, "invalid_expected_json", str(exc))
        if field_name not in MATCHING_FACT_FIELD_TYPES:
            return _configuration_error(rule, product_id, version_id, "field_not_whitelisted", "规则字段不在事实白名单", expected=expected)
        if operator not in OPERATORS:
            return _configuration_error(rule, product_id, version_id, "invalid_operator", "规则操作符不合法", expected=expected)
        if severity not in SEVERITIES:
            return _configuration_error(rule, product_id, version_id, "invalid_severity", "规则严重程度不合法", expected=expected)
        if action not in FAILURE_ACTIONS:
            return _configuration_error(rule, product_id, version_id, "invalid_failure_action", "规则失败动作不合法", expected=expected)
        fact = accessor.get_fact(field_name)
        try:
            normalized_expected = _validate_expected(MATCHING_FACT_FIELD_TYPES[field_name], operator, expected)
        except ValueError as exc:
            return _configuration_error(rule, product_id, version_id, "invalid_expected_type", str(exc), expected=expected, fact=fact)

        source = fact.source
        common = dict(
            rule_id=str(rule.get("rule_id") or ""), rule_source=str(rule.get("rule_source") or "product_rule"),
            field_name=field_name, operator=operator,
            expected_value=normalized_expected, actual_value=fact.value, actual_status=fact.status,
            severity=severity, failure_action=action, message=str(rule.get("message") or ""),
            source_text=str(rule.get("source_text") or ""), fact_source=source,
            evidence_refs=list(source.evidence_refs) if source else [],
        )
        if fact.status == "unknown":
            return RuleEvaluationResult(**common, result="unknown", explanation=_explanation(field_name, "unknown", None, normalized_expected, operator))
        if fact.status in {"conflict", "not_applicable"}:
            # not_applicable is not evidence that a requirement is absent.  A human
            # must decide whether the rule itself applies to this product/customer.
            return RuleEvaluationResult(**common, result="review", explanation=_explanation(field_name, "review", None, normalized_expected, operator))
        try:
            passed = _evaluate_known(fact.value, normalized_expected, operator, MATCHING_FACT_FIELD_TYPES[field_name])
        except (TypeError, ValueError) as exc:
            return _configuration_error(rule, product_id, version_id, "fact_type_mismatch", f"事实值类型与规则字段不匹配：{exc}", expected=expected, fact=fact)
        result: RuleResult = "passed" if passed else "failed"
        return RuleEvaluationResult(**common, result=result, explanation=_explanation(field_name, result, fact.value, normalized_expected, operator))

    def evaluate_product(
        self, facts: Any, product_value: Any, version_value: Any, rules: list[Any],
    ) -> ProductEvaluationResult:
        product, version = _row_dict(product_value), _row_dict(version_value)
        product_id = str(product.get("product_id") or version.get("product_id") or "")
        version_id = str(version.get("version_id") or "")
        base = dict(
            product_id=product_id, version_id=version_id,
            external_product_code=str(product.get("external_product_code") or ""),
            institution_name=str(version.get("institution_name") or product.get("institution_name") or ""),
            product_name=str(version.get("product_name") or product.get("product_name") or ""),
        )
        accessor = facts if isinstance(facts, MatchingFactsAccessor) else MatchingFactsAccessor(facts)
        results: list[RuleEvaluationResult] = []
        if version.get("status") != "published":
            fake_rule = {"rule_id": "", "field_name": "", "operator": "", "severity": "", "failure_action": ""}
            results.append(_configuration_error(fake_rule, product_id, version_id, "version_not_published", "ProductRuleEngine 只能执行已发布产品版本"))
        for rule_value in rules:
            rule = _row_dict(rule_value)
            if str(rule.get("version_id") or version_id) != version_id:
                results.append(_configuration_error(rule, product_id, version_id, "rule_version_mismatch", "规则不属于当前产品版本"))
                continue
            results.append(self.evaluate_rule(accessor, rule, product_id=product_id, version_id=version_id))
        errors = [item.configuration_error for item in results if item.configuration_error]
        grouped = {state: [item for item in results if item.result == state] for state in ("passed", "failed", "unknown", "review")}
        if errors:
            return ProductEvaluationResult(
                **base, evaluation_status="product_configuration_error", rule_results=results,
                passed_rules=grouped["passed"], failed_rules=grouped["failed"], unknown_rules=grouped["unknown"],
                review_rules=grouped["review"], configuration_errors=errors,
            )

        material = [item for item in results if item.severity != "info" and item.result != "passed"]
        manual = [item for item in material if item.result == "review" or item.failure_action == "review"]
        excluded = [item for item in material if item.severity == "hard" and item.result == "failed" and item.failure_action == "exclude"]
        conditional = [item for item in material if item.severity == "hard" and (
            item.result == "unknown" or (item.result == "failed" and item.failure_action == "conditional")
        )]
        if manual:
            overall: ProductStatus = "manual_review"
        elif excluded:
            overall = "ineligible"
        elif conditional:
            overall = "conditional"
        else:
            overall = "eligible"
        hard_failed = [item for item in grouped["failed"] if item.severity == "hard"]
        hard_unknown = [item for item in grouped["unknown"] if item.severity == "hard"]
        soft_failed = [item for item in grouped["failed"] if item.severity == "soft"]
        return ProductEvaluationResult(
            **base, evaluation_status="success", overall_status=overall, rule_results=results,
            passed_rules=grouped["passed"], failed_rules=grouped["failed"], unknown_rules=grouped["unknown"],
            review_rules=grouped["review"], hard_fail_count=len(hard_failed),
            hard_unknown_count=len(hard_unknown), soft_fail_count=len(soft_failed),
            blocking_reasons=[item.explanation for item in excluded],
            missing_information=[item.explanation for item in hard_unknown],
            review_reasons=[item.explanation for item in manual],
            soft_gaps=[item.explanation for item in soft_failed],
        )

    def evaluate_catalog(self, facts: Any, active_products: list[dict[str, Any]]) -> list[ProductEvaluationResult]:
        return [
            self.evaluate_product(facts, entry["product"], entry["version"], list(entry.get("rules") or []))
            for entry in active_products
        ]


__all__ = [
    "ProductRuleEngine", "ProductEvaluationResult", "ProductStatus", "RuleConfigurationError",
    "RuleEvaluationResult", "RuleResult",
]
