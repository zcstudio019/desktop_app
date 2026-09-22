"""Conservative, read-only Feishu document import for product drafts."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal

from services.wiki_service import WikiService


@dataclass
class ParsedProduct:
    institution_name: str
    product_name: str
    snapshot: str
    fields: dict = field(default_factory=dict)
    review: dict = field(default_factory=dict)
    rules: list[dict] = field(default_factory=list)


_STANDARD_TITLE = re.compile(
    r"(?m)^(?P<bank>[^\n]{2,35}?银行)\s*[-—–]{1,2}\s*(?P<name>[^\n]{2,80}?)(?=\s+(?:可贷额度|贷款额度|最高额度|额度|参考利率|年化利率|利率|贷款期限|期限|还款方式)\s*[：:]|$)"
)
_MORTGAGE_TITLE = re.compile(r"(?m)^(?P<bank>[^\n]{2,30}?银行)(?P<name>[^\n]{2,40}?)(?:方案内容)\s*$")


def _label_value(block: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        match = re.search(rf"(?m)^\s*{re.escape(label)}\s*[：:]?\s*(.*?)\s*$", block)
        if match:
            value = match.group(1).strip()
            if value:
                return value
            tail = block[match.end():].splitlines()
            for line in tail:
                if line.strip():
                    return line.strip()
    return ""


def _max_amount(text: str) -> Decimal | None:
    # Only explicit maximums become executable bounds. "额度较高" stays unknown.
    match = re.search(r"(?:最高|不超过|以内|上限|可贷额度|额度)\s*[：:]?\s*(\d+(?:\.\d+)?)\s*(亿|万|元)", text)
    if not match:
        match = re.search(r"(\d+(?:\.\d+)?)\s*(亿|万|元)\s*(?:以内|以下|封顶|内)", text)
    if not match:
        return None
    multiplier = {"亿": 100_000_000, "万": 10_000, "元": 1}[match.group(2)]
    return Decimal(match.group(1)) * multiplier


def _parse_block(bank: str, name: str, block: str, category: str) -> ParsedProduct:
    fields: dict = {}
    review: dict = {}
    amount_text = _label_value(block, ("可贷额度", "贷款额度", "最高额度", "额度"))
    amount = _max_amount(amount_text)
    if amount is not None:
        fields["max_amount"] = amount
        review["max_amount"] = "extracted_review"
    else:
        review["max_amount"] = "insufficient_data" if not amount_text else "needs_review"

    rate = _label_value(block, ("参考利率", "年化利率", "贷款利率", "利率%", "利率"))
    if rate:
        fields["rate_text"] = rate[:255]
        review["rate_text"] = "extracted_review"
    else:
        review["rate_text"] = "insufficient_data"

    term = _label_value(block, ("贷款授信最长期限", "贷款期限", "授信期限", "期限"))
    term_match = re.fullmatch(r"(?:最长)?\s*(\d+)\s*(年|个月|月)", term)
    if term_match:
        fields["max_term_months"] = int(term_match.group(1)) * (12 if term_match.group(2) == "年" else 1)
        review["max_term_months"] = "extracted_review"
    else:
        review["max_term_months"] = "needs_review" if term else "insufficient_data"

    region = re.search(r"(?:适用|限定|仅限)?\s*(上海|北京|广东|江苏|浙江)(?:市|省)?\s*(?:地区|区域|企业|客户)", block)
    if region:
        fields["region_scope_json"] = [region.group(1)]
        review["region_scope"] = "extracted_review"
    else:
        review["region_scope"] = "insufficient_data"

    repayment = _label_value(block, ("还款方式",))
    if repayment:
        fields["repayment_methods_json"] = [repayment]
        review["repayment_methods"] = "extracted_review"
    else:
        review["repayment_methods"] = "insufficient_data"

    material_match = re.search(r"材料准备清单\s*\n(.*?)(?=\n(?:审批流程|申请流程|经验总结|[^\n]{2,35}银行\s*[-—–])|\Z)", block, re.S)
    if material_match:
        materials = [re.sub(r"^\s*[\d.)、*•-]+\s*", "", line).strip() for line in material_match.group(1).splitlines()]
        fields["materials_json"] = [line for line in materials if line][:40]
        review["materials"] = "extracted_review"
    else:
        review["materials"] = "insufficient_data"

    # No vague phrase is turned into a rule. Only an explicit year threshold is proposed.
    rules: list[dict] = []
    age_text = _label_value(block, ("成立时间",))
    age_match = re.search(r"(?:企业|公司)?\s*成立\s*(?:满|至少|不低于|≥)\s*(\d+)\s*年", age_text)
    if age_match:
        rules.append({"rule_group": "eligibility", "field_name": "customer.company_age_months", "operator": "gte",
                      "expected_value": int(age_match.group(1)) * 12, "severity": "hard", "failure_action": "review",
                      "message": "企业成立年限要求", "source_text": age_text, "sort_order": 0})
        review["company_age_rule"] = "extracted_review"
    elif age_text:
        review["company_age_rule"] = "needs_review"
    else:
        review["company_age_rule"] = "insufficient_data"

    for key in ("institution_name", "product_name", "product_category"):
        review[key] = "extracted_review"
    for key in ("effective_from", "guarantee_modes", "collateral_types", "min_amount", "min_term_months"):
        review.setdefault(key, "insufficient_data")
    fields["summary"] = f"{bank} · {name}（飞书导入草稿，待管理员复核）"
    return ParsedProduct(bank.strip(), name.strip(), block.strip(), fields, review, rules)


def parse_product_document(category: str, content: str) -> list[ParsedProduct]:
    if category not in {"enterprise_credit", "enterprise_mortgage", "personal"}:
        raise ValueError("不支持的飞书产品类别")
    pattern = _MORTGAGE_TITLE if category == "enterprise_mortgage" else _STANDARD_TITLE
    matches = list(pattern.finditer(content))
    parsed: list[ParsedProduct] = []
    seen: set[tuple[str, str]] = set()
    for index, match in enumerate(matches):
        bank = match.group("bank").strip()
        name = match.group("name").strip()
        if not bank or not name or len(name) > 60 or (bank, name) in seen:
            continue
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        block = content[match.start():end]
        if len(block.strip()) < 80:
            continue
        seen.add((bank, name))
        parsed.append(_parse_block(bank, name, block, category))
    return parsed


class FeishuProductImportService:
    def __init__(self, wiki_service: WikiService | None = None):
        self.wiki_service = wiki_service or WikiService()

    def fetch(self, category: str, node_token: str) -> tuple[str, str, str | None]:
        expected_url = self.wiki_service.PRODUCT_DOCS.get(category)
        if not expected_url or self.wiki_service._extract_node_token(expected_url) != node_token:
            raise ValueError("产品类别与已批准的飞书节点不匹配")
        node = self.wiki_service._get_node_info(node_token)
        if node.get("obj_type") != "docx" or not node.get("obj_token"):
            raise ValueError("飞书产品来源不是可读取的 docx 节点")
        content = self.wiki_service._get_document_raw_content(node["obj_token"])
        if not content.strip():
            raise ValueError("飞书产品文档为空")
        return content, node["obj_token"], node.get("obj_edit_time") or node.get("updated_at")
