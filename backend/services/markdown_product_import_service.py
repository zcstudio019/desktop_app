"""Parse the six approved local Markdown catalogs without guessing missing rules."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any


SOURCE_DIR = Path(__file__).resolve().parents[1] / "data" / "product_catalog_sources"
SOURCE_FILES = {
    "guarantee_fund": "担保基金产品库.md",
    "personal_mortgage": "个人抵押类产品库.md",
    "personal_credit": "个人信用类产品库.md",
    "technology_enterprise": "科技企业产品库.md",
    "enterprise_mortgage": "企业抵押类产品库.md",
    "enterprise_credit": "企业信用类产品库.md",
}
SOURCE_LABELS = {
    "guarantee_fund": "担保基金", "personal_mortgage": "个人抵押", "personal_credit": "个人信用",
    "technology_enterprise": "科技企业", "enterprise_mortgage": "企业抵押", "enterprise_credit": "企业信用",
}
_HEADING = re.compile(r"(?m)^##\s*(\d+)\s*[.、．]\s*【([A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*-\d+)】\s*(.+?)\s*$")
_CODE = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*-\d+$")
_TABLE_DELIMITER = re.compile(r"^:?-{3,}:?$")
_AMBIGUOUS = re.compile(r"征信良好|经营稳定|资质较好|最好有房|可沟通|系统判定|个案审批|原则上|建议|视情况|以审批为准")


@dataclass
class MarkdownProduct:
    external_product_code: str
    product_name: str
    institution_name: str
    product_category: str
    source_file: str
    source_update_date: date | None
    source_snapshot: str
    source_snapshot_hash: str
    raw_fields: dict[str, Any]
    fields: dict[str, Any] = field(default_factory=dict)
    review: dict[str, str] = field(default_factory=dict)
    rules: list[dict[str, Any]] = field(default_factory=list)
    needs_review: bool = True


@dataclass
class ParsedSource:
    category: str
    source_file: str
    file_updated_at: str | None
    declared_count: int | None
    products: list[MarkdownProduct]
    missing: bool = False


def _split_table_row(line: str) -> list[str]:
    text = line.strip().strip("|")
    return [value.replace(r"\|", "|").strip() for value in re.split(r"(?<!\\)\|", text)]


def _tables(snapshot: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    lines = snapshot.splitlines()
    for i in range(len(lines) - 1):
        headers = _split_table_row(lines[i])
        delimiter = _split_table_row(lines[i + 1])
        if len(headers) < 2 or len(delimiter) < 2 or not all(_TABLE_DELIMITER.fullmatch(x) for x in delimiter[:2]):
            continue
        if not (any("字段" in x or "项目" in x for x in headers[:1]) and any("内容" in x or "说明" in x or "要求" in x for x in headers[1:2])):
            continue
        for row in lines[i + 2:]:
            if not row.lstrip().startswith("|"):
                break
            cells = _split_table_row(row)
            if len(cells) < 2 or not cells[0]:
                continue
            key, value = cells[0], cells[1]
            if key in result:
                if not isinstance(result[key], list):
                    result[key] = [result[key]]
                result[key].append(value)
            else:
                result[key] = value
    return result


def _first(raw: dict[str, Any], *aliases: str) -> str:
    for alias in aliases:
        for key, value in raw.items():
            if alias == key.replace(" ", ""):
                return str(value[0] if isinstance(value, list) else value).strip()
    return ""


def _amount_bounds(text: str, *, maximum_label: bool = False) -> tuple[Decimal | None, Decimal | None]:
    if not text or _AMBIGUOUS.search(text):
        return None, None
    unit = {"元": 1, "万": 10_000, "万元": 10_000, "亿": 100_000_000, "亿元": 100_000_000}
    range_match = re.search(r"(\d+(?:\.\d+)?)\s*(万|亿|元)?\s*[-~～至到]\s*(\d+(?:\.\d+)?)\s*(万元|亿元|万|亿|元)", text)
    if range_match:
        multiplier = unit[range_match.group(4)]
        first_multiplier = unit.get(range_match.group(2) or range_match.group(4), multiplier)
        return Decimal(range_match.group(1)) * first_multiplier, Decimal(range_match.group(3)) * multiplier
    max_match = re.search(r"(?:最高|不超过|上限|不高于)\s*(\d+(?:\.\d+)?)\s*(万元|亿元|万|亿|元)", text)
    if not max_match:
        max_match = re.search(r"(\d+(?:\.\d+)?)\s*(万元|亿元|万|亿|元)\s*(?:以内|以下|封顶)", text)
    min_match = re.search(r"(?:最低|不少于|下限|不低于)\s*(\d+(?:\.\d+)?)\s*(万元|亿元|万|亿|元)", text)
    minimum = Decimal(min_match.group(1)) * unit[min_match.group(2)] if min_match else None
    maximum = Decimal(max_match.group(1)) * unit[max_match.group(2)] if max_match else None
    if maximum is None and maximum_label:
        amounts = re.findall(r"(\d+(?:\.\d+)?)\s*(万元|亿元|万|亿|元)", text)
        if len(amounts) == 1:
            maximum = Decimal(amounts[0][0]) * unit[amounts[0][1]]
    return minimum, maximum


def _term_bounds(text: str, *, maximum_label: bool = False) -> tuple[int | None, int | None]:
    if not text or _AMBIGUOUS.search(text):
        return None, None
    range_match = re.search(r"(\d+)\s*(年|个月|月)?\s*[-~～至到]\s*(\d+)\s*(年|个月|月)", text)
    if range_match:
        unit = range_match.group(4)
        first_unit = range_match.group(2) or unit
        return int(range_match.group(1)) * (12 if first_unit == "年" else 1), int(range_match.group(3)) * (12 if unit == "年" else 1)
    max_match = re.search(r"(?:最长|最高|不超过|以内)\s*(\d+)\s*(年|个月|月)", text)
    if not max_match:
        max_match = re.search(r"(\d+)\s*(年|个月|月)\s*(?:以内|以下|封顶)", text)
    if not max_match and maximum_label:
        matches = re.findall(r"(\d+)\s*(年|个月|月)", text)
        if len(matches) == 1:
            value, unit = matches[0]
            return None, int(value) * (12 if unit == "年" else 1)
    return None, int(max_match.group(1)) * (12 if max_match.group(2) == "年" else 1) if max_match else None


def _source_date(content: str) -> date | None:
    match = re.search(r"(?:更新日期|更新时间|修订日期|资料日期)\s*[：:]\s*(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})", content[:3000])
    if not match:
        return None
    try:
        return date(*map(int, match.groups()))
    except ValueError:
        return None


def _rules(raw: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    rules: list[dict[str, Any]] = []
    review: dict[str, str] = {}
    for key, value in raw.items():
        text = "；".join(value) if isinstance(value, list) else str(value)
        if _AMBIGUOUS.search(text):
            review[key] = "needs_review"
            continue
        age = re.fullmatch(r"(?:企业|公司)成立(?:满|至少|不低于|≥)\s*(\d+)\s*年", text.strip())
        ratio = re.fullmatch(r"资产负债率\s*(?:≤|<=|不超过|不高于)\s*(\d+(?:\.\d+)?)\s*%", text.strip())
        if age:
            rules.append({"rule_group": "eligibility", "field_name": "customer.company_age_months", "operator": "gte",
                          "expected_value": int(age.group(1)) * 12, "severity": "hard", "failure_action": "exclude",
                          "message": "企业成立年限", "source_text": text})
        elif ratio:
            rules.append({"rule_group": "eligibility", "field_name": "financial.debt_asset_ratio", "operator": "lte",
                          "expected_value": str(Decimal(ratio.group(1)) / 100), "severity": "hard", "failure_action": "exclude",
                          "message": "资产负债率上限", "source_text": text})
        elif re.search(r"查询|逾期|征信|准入|要求", key + text):
            review[key] = "needs_review"
    return rules, review


def _parse_product(code: str, name: str, snapshot: str, category: str, source_file: str,
                   source_date: date | None) -> MarkdownProduct:
    raw = _tables(snapshot)
    institution = _first(raw, "机构名称", "银行名称", "贷款银行", "合作银行", "机构", "银行")
    if not institution:
        match = re.search(r"(?m)^\s*[-*]\s*机构\s*[：:]\s*(.+?)\s*$", snapshot)
        if match:
            institution = match.group(1).strip()
    if institution:
        raw.setdefault("机构", institution)
    fields: dict[str, Any] = {"summary": f"{name}（本地 Markdown 导入草稿）", "raw_fields_json": raw}
    review: dict[str, str] = {"external_product_code": "extracted_review", "product_name": "extracted_review",
                               "product_category": "extracted_review", "institution_name": "extracted_review" if institution else "insufficient_data"}
    aliases = {
        "loan_type": ("贷款类型", "产品类型", "融资类型"),
        "guarantee_type": ("担保方式", "担保类型"),
        "rate_text": ("年化利率", "贷款利率", "参考利率", "利率"),
        "tax_grade": ("纳税等级", "税务等级"),
        "revenue_requirement": ("营收要求", "收入要求", "营业收入要求"),
        "tax_requirement": ("纳税要求", "税收要求"),
        "invoice_requirement": ("开票要求", "发票要求"),
        "credit_overdue_requirement": ("征信逾期要求", "逾期要求"),
        "credit_query_requirement": ("征信查询要求", "查询次数要求"),
        "debt_requirement": ("负债要求", "负债"),
        "collateral_requirement": ("抵押物要求", "抵押要求", "房屋要求"),
        "suitable_customer_text": ("适用客户", "适合客户", "适用对象"),
        "notes": ("备注", "特别说明", "补充说明", "特殊优势及备注"),
    }
    for target, labels in aliases.items():
        value = _first(raw, *labels)
        if value:
            fields[target] = value[:255] if target in {"loan_type", "guarantee_type", "rate_text", "tax_grade"} else value
            review[target] = "extracted_review"
        else:
            review[target] = "insufficient_data"
    maximum_label = bool(_first(raw, "最高额度"))
    amount_text = _first(raw, "最高额度", "最低额度", "贷款额度", "授信额度", "融资额度", "额度")
    minimum, maximum = _amount_bounds(amount_text, maximum_label=maximum_label)
    if minimum is not None:
        fields["min_amount"] = minimum
    if maximum is not None:
        fields["max_amount"] = maximum
    review["amount"] = "extracted_review" if minimum is not None or maximum is not None else ("needs_review" if amount_text else "insufficient_data")
    maximum_term_label = bool(_first(raw, "贷款授信最长期限"))
    term_text = _first(raw, "贷款授信最长期限", "贷款期限", "授信期限", "融资期限", "借款期限", "期限")
    min_term, max_term = _term_bounds(term_text, maximum_label=maximum_term_label)
    if min_term is not None:
        fields["min_term_months"] = min_term
    if max_term is not None:
        fields["max_term_months"] = max_term
    review["term"] = "extracted_review" if min_term is not None or max_term is not None else ("needs_review" if term_text else "insufficient_data")
    repayment = _first(raw, "还款方式", "还本付息方式")
    if repayment:
        fields["repayment_methods_json"] = [repayment]
    review["repayment_methods"] = "extracted_review" if repayment else "insufficient_data"
    material = _first(raw, "申请材料", "进件材料", "所需材料", "所需资料", "材料清单", "准备材料")
    if material:
        fields["materials_json"] = [part.strip() for part in re.split(r"[；;\n]", material) if part.strip()]
    review["materials"] = "extracted_review" if material else "insufficient_data"
    region = _first(raw, "适用地区", "业务地区", "地区范围", "地域范围", "服务区域", "准入区域", "准入地区")
    if region:
        exact = re.fullmatch(r"(上海|北京|天津|重庆|广东|江苏|浙江)(?:市|省)?(?:地区)?", region)
        if exact:
            fields["region_scope_json"] = [exact.group(1)]
            review["region_scope"] = "extracted_review"
        else:
            review["region_scope"] = "needs_review"
    else:
        review["region_scope"] = "insufficient_data"
    age = _first(raw, "企业成立年限", "成立年限", "成立时间")
    age_match = re.fullmatch(r"(?:企业|公司)?成立(?:满|至少|不低于|≥)\s*(\d+)\s*年", age)
    if age_match:
        fields["company_age_months"] = int(age_match.group(1)) * 12
    review["company_age_months"] = "extracted_review" if age_match else ("needs_review" if age else "insufficient_data")
    borrower_age = _first(raw, "借款人年龄", "申请人年龄", "贷款人年龄", "年龄要求", "年龄")
    age_range = re.fullmatch(r"(\d+)\s*[-~～至到]\s*(\d+)\s*岁", borrower_age)
    if age_range:
        fields["borrower_age_min"] = int(age_range.group(1))
        fields["borrower_age_max"] = int(age_range.group(2))
    review["borrower_age"] = "extracted_review" if age_range else ("needs_review" if borrower_age else "insufficient_data")
    rules, rule_review = _rules(raw)
    review.update(rule_review)
    review.setdefault("guarantee_modes", "needs_review" if fields.get("guarantee_type") else "insufficient_data")
    review.setdefault("collateral_types", "needs_review" if fields.get("collateral_requirement") else "insufficient_data")
    review.setdefault("company_age_rule", "extracted_review" if any(r["field_name"] == "customer.company_age_months" for r in rules) else "insufficient_data")
    review.setdefault("max_amount", review["amount"])
    review.setdefault("max_term_months", review["term"])
    digest = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
    clean_name = re.sub(r"\s*[（(]20\d{2}年\d{1,2}月\d{1,2}日(?:更新|录入)[）)]\s*$", "", name).strip()
    return MarkdownProduct(code, clean_name, institution, category, source_file, source_date,
                           snapshot, digest, raw, fields, review, rules, True)


def parse_markdown_source(category: str, content: str, *, file_updated_at: str | None = None) -> ParsedSource:
    if category not in SOURCE_FILES:
        raise ValueError("不支持的本地产品分类")
    declared_match = re.search(r"产品数量\s*[：:]\s*(\d+)\s*款?", content[:3000])
    declared = int(declared_match.group(1)) if declared_match else None
    matches = list(_HEADING.finditer(content))
    products = []
    source_date = _source_date(content)
    for i, match in enumerate(matches):
        code = match.group(2).upper()
        if not _CODE.fullmatch(code):
            continue
        snapshot = content[match.start():(matches[i + 1].start() if i + 1 < len(matches) else len(content))].strip()
        products.append(_parse_product(code, match.group(3), snapshot, category, SOURCE_FILES[category], source_date))
    return ParsedSource(category, SOURCE_FILES[category], file_updated_at, declared, products)


def read_markdown_source(category: str, directory: Path = SOURCE_DIR) -> ParsedSource:
    if category not in SOURCE_FILES:
        raise ValueError("不支持的本地产品分类")
    path = directory / SOURCE_FILES[category]
    if not path.is_file():
        return ParsedSource(category, path.name, None, None, [], missing=True)
    content = path.read_text(encoding="utf-8-sig")
    return parse_markdown_source(category, content, file_updated_at=date.fromtimestamp(path.stat().st_mtime).isoformat())


def scan_sources(directory: Path = SOURCE_DIR) -> dict[str, Any]:
    sources = [read_markdown_source(category, directory) for category in SOURCE_FILES]
    by_code: dict[str, list[MarkdownProduct]] = {}
    for source in sources:
        for item in source.products:
            by_code.setdefault(item.external_product_code, []).append(item)
    conflicts = []
    deduplicated = []
    for code, items in by_code.items():
        first = items[0]
        if any(x.product_name != first.product_name or x.source_snapshot_hash != first.source_snapshot_hash for x in items[1:]):
            conflicts.append({"external_product_code": code, "status": "duplicate_conflict", "needs_review": True,
                              "sides": [{"source_file": x.source_file, "product_name": x.product_name,
                                         "snapshot_hash": x.source_snapshot_hash} for x in items]})
        else:
            deduplicated.extend(items[1:])
    conflicting_codes = {row["external_product_code"] for row in conflicts}
    summaries = []
    for source in sources:
        codes = {p.external_product_code for p in source.products}
        summaries.append({"category": source.category, "label": SOURCE_LABELS[source.category], "source_file": source.source_file,
                          "missing": source.missing, "file_updated_at": source.file_updated_at,
                          "declared_count": source.declared_count, "parsed_count": len(source.products),
                          "unique_count": len(codes), "duplicate_count": len(source.products) - len(codes),
                          "conflict_count": len(codes & conflicting_codes),
                          "needs_review_count": sum(p.needs_review for p in source.products)})
    return {"sources": sources, "summaries": summaries, "conflicts": conflicts,
            "deduplicated_count": len(deduplicated),
            "duplicate_code_count": sum(1 for items in by_code.values() if len(items) > 1),
            "duplicate_occurrence_count": sum(len(items) - 1 for items in by_code.values()),
            "parsed_count": sum(len(s.products) for s in sources),
            "unique_count": len(by_code), "conflict_count": len(conflicts),
            "conflicting_codes": conflicting_codes, "by_code": by_code}
