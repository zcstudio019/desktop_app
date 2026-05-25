from __future__ import annotations

from typing import Any

from ..normalizer import normalize_text
from .salary_income_detection_skill import detect_salary_income


SALARY_KEYWORDS = ("工资", "薪资", "代发工资", "代发薪", "奖金", "绩效", "劳务报酬")
OPERATING_KEYWORDS = ("货款", "服务费", "销售款", "客户付款", "经营回款", "工程款", "材料款", "结算款")
OPERATING_EXCLUDE_KEYWORDS = ("借款", "贷款", "还款", "转存", "理财赎回", "赎回")
UNKNOWN_INFLOW_KEYWORDS = ("汇入汇款", "汇款汇入", "转账收入", "跨行汇入", "他行汇入")
COMPANY_INFLOW_KEYWORDS = ("汇入汇款", "汇款汇入", "跨行汇入", "他行汇入", "普通汇款", "电子汇入")
PLATFORM_COLLECTION_KEYWORDS = ("银联代付", "网联收款")
INTEREST_KEYWORDS = ("存款利息", "结息", "账户结息")
LOAN_INFLOW_KEYWORDS = ("个贷放款", "贷款发放", "借款入账", "贷款入账", "网贷放款", "小贷放款", "消费贷放款")
REFUND_KEYWORDS = ("退款", "退货", "冲正", "撤销", "快捷退款", "转账退款")
INVESTMENT_KEYWORDS = ("理财", "基金", "证券", "股票", "银证转账", "赎回")
LOAN_REPAYMENT_KEYWORDS = ("个贷交易", "个贷还款", "贷款回收", "贷款扣款", "贷款还款", "按揭", "房贷", "车贷", "消费贷", "还本", "还息", "还贷")
ONLINE_LOAN_REPAYMENT_KEYWORDS = ("中融小贷", "支付宝信贷业务待还款账户", "美团月付还款", "小贷还款", "网贷还款")
CREDIT_CARD_KEYWORDS = ("信用卡还款", "卡中心", "信用卡")
QUICK_PAYMENT_KEYWORDS = ("快捷支付", "支付宝", "微信支付", "财付通", "美团支付", "POS", "消费")
LIVING_KEYWORDS = ("餐饮", "购物", "交通", "物业", "水电", "通信", "日常消费", "超市")
OPERATING_EXPENSE_KEYWORDS = ("采购", "进货", "房租", "员工工资", "运费")
RELATED_PARTY_KEYWORDS = ("亲属", "关联人", "配偶", "父亲", "母亲")
CASH_WITHDRAWAL_KEYWORDS = ("银联ATM取款", "ATM取款", "取款")
FEE_EXPENSE_KEYWORDS = ("手续费", "工本费", "服务费")
TRANSFER_OUT_KEYWORDS = ("转账汇款", "转支", "转出", "汇款")
EMPLOYER_COUNTERPARTY_KEYWORDS = ("有限公司", "有限责任公司", "股份有限公司", "集团", "公司", "科技", "软件", "信息", "网络", "工程", "建筑", "实业", "商贸", "贸易", "人力资源", "劳务", "工厂", "厂", "银行代发", "代发工资专户")


def _has_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _has_counterparty(tx: dict[str, Any]) -> bool:
    return bool(normalize_text(tx.get("counterparty_name")) or normalize_text(tx.get("counterparty_account")))


def _employer_like(name: Any) -> bool:
    return _has_any(normalize_text(name), EMPLOYER_COUNTERPARTY_KEYWORDS)


def _is_self_counterparty(tx: dict[str, Any], account_name: str) -> bool:
    return bool(normalize_text(account_name) and normalize_text(tx.get("counterparty_name")) == normalize_text(account_name))


def _is_personal_counterparty(tx: dict[str, Any]) -> bool:
    name = normalize_text(tx.get("counterparty_name"))
    return bool(name and 2 <= len(name) <= 4 and all("\u4e00" <= char <= "\u9fff" for char in name) and not _employer_like(name))


def classify_transactions(transactions: list[dict[str, Any]], account_name: str = "") -> list[dict[str, Any]]:
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

    salary_summary = detect_salary_income(transactions, account_name=account_name)
    for tx in transactions:
        debit = float(tx.get("debit_amount") or 0)
        credit = float(tx.get("credit_amount") or 0)
        if tx.get("is_internal_transfer"):
            tx["category"] = "internal_transfer"
            tx["evidence"] = tx.get("evidence") or "本人账户互转，不计入稳定收入"
            continue

        text = normalize_text(f"{tx.get('summary')} {tx.get('counterparty_name')}")
        if tx.get("direction") == "income" and credit > 0:
            salary_detection = tx.get("salary_detection") or {}
            salary_type = salary_detection.get("salary_type")
            income_nature = salary_detection.get("income_nature") or tx.get("income_nature")
            exclusion_category = str(tx.get("salary_exclusion_category") or "")
            if income_nature == "self_transfer_income":
                tx["category"] = "self_transfer_income"
                tx["is_internal_transfer"] = True
                tx["evidence"] = salary_detection.get("evidence") or "对手方与户名一致，识别为本人账户转入"
            elif income_nature == "personal_transfer_income":
                tx["category"] = "personal_transfer_income"
                tx["evidence"] = salary_detection.get("evidence") or "个人付款方转入，不计入工资收入"
            elif salary_type == "confirmed_salary":
                tx["category"] = "salary_income"
                tx["is_salary"] = True
                tx["is_verified_income"] = True
                tx["is_stable_income"] = True
                tx["evidence"] = salary_detection.get("evidence") or "工资收入识别器判定为明确工资收入"
            elif salary_type == "suspected_salary":
                tx["category"] = "suspected_salary_income"
                tx["is_salary"] = True
                tx["is_verified_income"] = False
                tx["is_stable_income"] = False
                tx["need_manual_review"] = True
                tx["evidence"] = salary_detection.get("evidence") or "疑似工资收入，需人工核实，不计入默认可采信工资"
            elif salary_type == "low_confidence_suspected_salary":
                tx["category"] = "low_confidence_suspected_salary_income"
                tx["need_manual_review"] = True
                tx["evidence"] = salary_detection.get("evidence") or "代发/平台收款存在周期特征，但付款方不足以确认工资"
            elif exclusion_category == "reimbursement_or_advance_income":
                tx["category"] = "reimbursement_or_advance_income"
                tx["evidence"] = salary_detection.get("evidence") or "报销/差旅费/备用金类入账，不计入工资"
            elif exclusion_category == "borrowing_or_transfer_income":
                tx["category"] = "borrowing_or_transfer_income"
                tx["evidence"] = salary_detection.get("evidence") or "借款/往来款/还款类入账，不计入工资"
            elif exclusion_category == "investment_income":
                tx["category"] = "investment_income"
                tx["evidence"] = salary_detection.get("evidence") or "分红/投资收益类入账，不计入工资"
            elif _has_any(text, ("劳务费", "劳务报酬")):
                tx["category"] = "labor_income"
                tx["evidence"] = "劳务费/劳务报酬不直接认定为工资，需人工核实"
            elif _has_any(text, OPERATING_KEYWORDS) and not _has_any(text, OPERATING_EXCLUDE_KEYWORDS):
                tx["category"] = "operating_income"
                tx["is_operating_income"] = True
                tx["is_verified_income"] = True
                tx["is_stable_income"] = True
                tx["evidence"] = "摘要命中货款/服务费/销售款等经营收入关键词"
            elif _has_any(text, LOAN_INFLOW_KEYWORDS):
                tx["category"] = "loan_inflow"
                tx["is_loan_inflow"] = True
                tx["risk_tags"].append("loan_inflow_as_income")
                tx["evidence"] = "贷款/借款类流入，不计入稳定收入"
            elif _has_any(text, REFUND_KEYWORDS):
                tx["category"] = "refund_income"
                tx["evidence"] = "退款/冲正类收入，不计入稳定收入"
            elif _has_any(text, INVESTMENT_KEYWORDS):
                tx["category"] = "investment_income"
                tx["evidence"] = "理财/证券赎回类收入，不计入稳定收入"
            elif _has_any(text, INTEREST_KEYWORDS):
                tx["category"] = "interest_income"
                tx["evidence"] = "存款利息/结息收入，统计但不作为主要稳定收入"
            elif _is_self_counterparty(tx, account_name):
                tx["category"] = "internal_transfer_income"
                tx["is_internal_transfer"] = True
                tx["evidence"] = "对手方与户名一致，按本人账户转入处理"
            elif _is_personal_counterparty(tx):
                tx["category"] = "personal_transfer_income"
                tx["evidence"] = "对手方为自然人，不作为工资或经营收入采信"
            elif _has_any(text, PLATFORM_COLLECTION_KEYWORDS) and _employer_like(tx.get("counterparty_name")):
                tx["category"] = "platform_collection_income"
                tx["evidence"] = "平台/网联收款且付款方为单位主体，需人工核实真实业务来源"
            elif _has_any(text, COMPANY_INFLOW_KEYWORDS) and _employer_like(tx.get("counterparty_name")):
                tx["category"] = "company_business_inflow"
                tx["evidence"] = "公司主体汇入，但无工资或明确经营用途，不直接作为工资采信"
            elif _has_any(text, RELATED_PARTY_KEYWORDS):
                tx["category"] = "related_party_income"
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
            if _has_any(text, ONLINE_LOAN_REPAYMENT_KEYWORDS):
                tx["category"] = "online_loan_repayment_expense"
                tx["is_loan_repayment"] = True
                tx["evidence"] = "小贷/线上信贷还款支出"
            elif _has_any(text, CREDIT_CARD_KEYWORDS):
                tx["category"] = "credit_card_repayment_expense"
                tx["is_credit_card_repayment"] = True
                tx["evidence"] = "摘要命中信用卡还款关键词"
            elif _has_any(text, LOAN_REPAYMENT_KEYWORDS):
                tx["category"] = "loan_repayment_expense"
                tx["is_loan_repayment"] = True
                tx["evidence"] = "摘要命中个贷还款/贷款回收等贷款还款关键词"
            elif _has_any(text, INVESTMENT_KEYWORDS):
                tx["category"] = "investment_expense"
                tx["evidence"] = "理财/基金/银证转账类支出"
            elif _has_any(text, CASH_WITHDRAWAL_KEYWORDS):
                tx["category"] = "cash_withdrawal"
                tx["evidence"] = "ATM/现金取款支出"
            elif _has_any(text, FEE_EXPENSE_KEYWORDS):
                tx["category"] = "fee_expense"
                tx["evidence"] = "手续费或服务费用支出"
            elif _has_any(text, TRANSFER_OUT_KEYWORDS) and _is_self_counterparty(tx, account_name):
                tx["category"] = "internal_transfer_expense"
                tx["is_internal_transfer"] = True
                tx["evidence"] = "转账支出对手方与户名一致，按本人账户互转处理"
            elif _has_any(text, TRANSFER_OUT_KEYWORDS) and _is_personal_counterparty(tx):
                tx["category"] = "related_party_transfer_expense"
                tx["is_related_party"] = True
                tx["evidence"] = "向个人对手方转账，按个人往来支出处理"
            elif _has_any(text, TRANSFER_OUT_KEYWORDS) and _employer_like(tx.get("counterparty_name")):
                tx["category"] = "business_or_company_outflow"
                tx["evidence"] = "向公司主体转账，按公司往来支出处理"
            elif _has_any(text, QUICK_PAYMENT_KEYWORDS):
                tx["category"] = "platform_payment_expense"
                tx["evidence"] = "快捷支付/POS/消费类支出"
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
    for tx in transactions:
        if tx.get("direction") == "income":
            tx.setdefault("salary_detection", {"salary_type": "unknown", "confidence": 0.0})
    setattr(classify_transactions, "last_salary_summary", salary_summary)
    return transactions
