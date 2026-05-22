from __future__ import annotations

from typing import Any

from ..normalizer import normalize_text


SALARY_KEYWORDS = ("工资", "薪资", "代发工资", "代发薪", "奖金", "绩效", "劳务报酬")
OPERATING_KEYWORDS = ("货款", "服务费", "销售款", "客户付款", "经营回款", "工程款", "材料款", "结算款")
OPERATING_EXCLUDE_KEYWORDS = ("借款", "贷款", "还款", "转存", "理财赎回", "赎回")
UNKNOWN_INFLOW_KEYWORDS = ("汇款汇入", "转账收入", "跨行汇入", "他行汇入")
INTEREST_KEYWORDS = ("存款利息", "结息")
LOAN_INFLOW_KEYWORDS = ("贷款发放", "借款", "网贷", "小贷", "消费贷", "备用金")
REFUND_KEYWORDS = ("退款", "退货", "冲正", "撤销")
INVESTMENT_KEYWORDS = ("理财", "基金", "证券", "股票", "银证转账", "赎回")
LOAN_REPAYMENT_KEYWORDS = ("个贷还款", "贷款回收", "贷款扣款", "贷款还款", "按揭", "房贷", "车贷", "小贷", "消费贷", "还本", "还息", "还贷")
CREDIT_CARD_KEYWORDS = ("信用卡还款", "卡中心", "信用卡")
QUICK_PAYMENT_KEYWORDS = ("快捷支付", "支付宝", "微信支付", "POS", "消费")
LIVING_KEYWORDS = ("餐饮", "购物", "交通", "物业", "水电", "通信", "日常消费", "超市")
OPERATING_EXPENSE_KEYWORDS = ("采购", "进货", "房租", "员工工资", "运费")
RELATED_PARTY_KEYWORDS = ("亲属", "关联人", "配偶", "父亲", "母亲")


def _has_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _has_counterparty(tx: dict[str, Any]) -> bool:
    return bool(normalize_text(tx.get("counterparty_name")) or normalize_text(tx.get("counterparty_account")))


def classify_transactions(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for tx in transactions:
        debit = float(tx.get("debit_amount") or 0)
        credit = float(tx.get("credit_amount") or 0)
        tx["amount"] = round(credit if credit > 0 else debit, 2)
        tx["transaction_time"] = tx.get("transaction_time") or tx.get("transaction_date") or ""
        tx["accounting_date"] = tx.get("accounting_date") or tx.get("transaction_date") or ""
        tx["transaction_place"] = tx.get("transaction_place") or ""
        tx["is_verified_income"] = False
        tx["is_stable_income"] = False
        tx["is_unknown_inflow"] = False
        tx["is_salary"] = False
        tx["is_operating_income"] = False
        tx["is_loan_repayment"] = False
        tx["is_fast_in_fast_out_related"] = False
        tx.setdefault("risk_tags", [])

        if tx.get("is_internal_transfer"):
            tx["category"] = "internal_transfer"
            tx["evidence"] = tx.get("evidence") or "本人账户互转，不计入稳定收入"
            continue

        text = normalize_text(f"{tx.get('summary')} {tx.get('counterparty_name')}")
        if tx.get("direction") == "income" and credit > 0:
            if _has_any(text, SALARY_KEYWORDS):
                tx["category"] = "salary_income"
                tx["is_salary"] = True
                tx["is_verified_income"] = True
                tx["is_stable_income"] = True
                tx["evidence"] = "摘要命中工资/薪资类关键词，计入可采信工资收入"
            elif _has_any(text, OPERATING_KEYWORDS) and not _has_any(text, OPERATING_EXCLUDE_KEYWORDS):
                tx["category"] = "operating_income"
                tx["is_operating_income"] = True
                tx["is_verified_income"] = True
                tx["is_stable_income"] = True
                tx["evidence"] = "摘要命中货款/服务费/销售款等经营收入关键词"
            elif _has_any(text, INTEREST_KEYWORDS):
                tx["category"] = "interest_income"
                tx["evidence"] = "存款利息/结息收入，统计但不作为主要稳定收入"
            elif _has_any(text, LOAN_INFLOW_KEYWORDS):
                tx["category"] = "loan_inflow"
                tx["is_loan_inflow"] = True
                tx["risk_tags"].append("loan_inflow_as_income")
                tx["evidence"] = "贷款/借款类流入，不计入稳定收入"
            elif _has_any(text, REFUND_KEYWORDS):
                tx["category"] = "refund"
                tx["evidence"] = "退款/冲正类收入，不计入稳定收入"
            elif _has_any(text, INVESTMENT_KEYWORDS):
                tx["category"] = "investment_redeem_income"
                tx["evidence"] = "理财/证券赎回类收入，不计入稳定收入"
            elif _has_any(text, RELATED_PARTY_KEYWORDS):
                tx["category"] = "related_party_transfer"
                tx["is_related_party"] = True
                tx["evidence"] = "关联方/亲属往来，暂不计入稳定收入"
            elif _has_any(text, UNKNOWN_INFLOW_KEYWORDS):
                tx["category"] = "unknown_inflow"
                tx["is_unknown_inflow"] = True
                tx["risk_tags"].append("income_source_unclear")
                tx["evidence"] = "汇款汇入/转账收入缺少明确工资或经营用途，不计入稳定收入"
            elif not _has_counterparty(tx):
                tx["category"] = "unknown_inflow"
                tx["is_unknown_inflow"] = True
                tx["risk_tags"].append("income_source_unclear")
                tx["evidence"] = "收入缺少对手方和用途备注，不计入稳定收入"
            else:
                tx["category"] = "other_inflow"
                tx["evidence"] = "未命中可采信收入关键词，暂不计入稳定收入"
        elif tx.get("direction") == "expense" and debit > 0:
            if _has_any(text, LOAN_REPAYMENT_KEYWORDS):
                tx["category"] = "loan_repayment_expense"
                tx["is_loan_repayment"] = True
                tx["evidence"] = "摘要命中个贷还款/贷款回收等贷款还款关键词"
            elif _has_any(text, CREDIT_CARD_KEYWORDS):
                tx["category"] = "credit_card_repayment_expense"
                tx["is_credit_card_repayment"] = True
                tx["evidence"] = "摘要命中信用卡还款关键词"
            elif _has_any(text, QUICK_PAYMENT_KEYWORDS):
                tx["category"] = "quick_payment_expense"
                tx["evidence"] = "快捷支付/POS/消费类支出"
            elif _has_any(text, INVESTMENT_KEYWORDS):
                tx["category"] = "investment_expense"
                tx["evidence"] = "理财/基金/证券类支出"
            elif _has_any(text, LIVING_KEYWORDS):
                tx["category"] = "living_expense"
                tx["evidence"] = "日常生活消费类支出"
            elif _has_any(text, OPERATING_EXPENSE_KEYWORDS):
                tx["category"] = "operating_expense"
                tx["evidence"] = "采购/运费/房租等经营相关支出"
            elif debit >= 100000:
                tx["category"] = "other_large_expense"
                tx["risk_tags"].append("abnormal_large_expense")
                tx["evidence"] = "单笔大额支出，需人工核验用途"
            else:
                tx["category"] = "other_expense"
                tx["evidence"] = "未命中明确支出分类关键词"
        else:
            tx["category"] = "other"
            tx["evidence"] = "交易方向无法唯一判断"
    return transactions
