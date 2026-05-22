from __future__ import annotations

from typing import Any

from ..normalizer import normalize_text


CATEGORY_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("salary_income", ("工资", "薪资", "奖金", "绩效", "劳务报酬", "代发")),
    ("operating_income", ("经营回款", "货款", "服务费", "客户付款", "销售款", "营业款", "收款")),
    ("other_stable_income", ("租金", "分红", "固定收入")),
    ("loan_inflow", ("贷款发放", "借款", "网贷", "小贷", "消费贷", "备用金")),
    ("refund", ("退款", "退货", "冲正", "撤销")),
    ("investment_transfer", ("理财", "基金", "证券", "股票", "银证转账", "赎回")),
    ("credit_card_repayment", ("信用卡还款", "卡中心", "信用卡")),
    ("loan_repayment", ("贷款还款", "按揭", "房贷", "车贷", "小贷还款", "还贷")),
    ("living_expense", ("餐饮", "购物", "交通", "物业", "水电", "通信", "日常消费", "超市")),
    ("operating_expense", ("采购", "进货", "房租", "员工工资", "运费", "材料款")),
    ("related_party_transfer", ("亲属", "关联人", "配偶", "父亲", "母亲")),
)


def _keyword_category(text: str) -> str:
    for category, keywords in CATEGORY_KEYWORDS:
        if any(word in text for word in keywords):
            return category
    return "other"


def classify_transactions(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for tx in transactions:
        if tx.get("is_internal_transfer"):
            tx["category"] = "internal_transfer"
            continue
        text = normalize_text(f"{tx.get('summary')} {tx.get('counterparty_name')}")
        category = _keyword_category(text)
        amount = float(tx.get("debit_amount") or tx.get("credit_amount") or 0)
        if category == "other" and tx.get("direction") == "expense" and amount >= 100000:
            category = "abnormal_large_expense"
        tx["category"] = category
        tx["is_related_party"] = category == "related_party_transfer"
        tx["is_loan_inflow"] = category == "loan_inflow" and tx.get("direction") == "income"
        tx["is_salary"] = category == "salary_income" and tx.get("direction") == "income"
        tx["is_operating_income"] = category == "operating_income" and tx.get("direction") == "income"
        tx["is_credit_card_repayment"] = category == "credit_card_repayment" and tx.get("direction") == "expense"
        if category == "abnormal_large_expense":
            tx.setdefault("risk_tags", []).append("abnormal_large_expense")
        if tx.get("is_loan_inflow"):
            tx.setdefault("risk_tags", []).append("loan_inflow_as_income")
        tx["evidence"] = tx.get("evidence") or f"按摘要/对手方关键词归类为 {category}"
    return transactions
