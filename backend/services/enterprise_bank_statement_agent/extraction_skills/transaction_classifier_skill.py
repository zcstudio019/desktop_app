from __future__ import annotations

import logging
import re
from collections import Counter
from statistics import quantiles
from typing import Any

from ..normalizer import is_probable_person_name, normalize_account_number, normalize_text

logger = logging.getLogger(__name__)

ORG_WORDS = ("公司", "有限公司", "银行", "合作社", "中心", "集团", "工程", "建材", "供应链", "材料", "矿业")
TRANSFER_WORDS = ("转账", "内部转账", "户间转账", "往来款", "账户互转", "备用金", "资金归集", "本系统转帐", "网银转账", "调拨")
REGION_WORDS = (
    "上海",
    "江苏",
    "苏州",
    "昆山",
    "北京",
    "浙江",
    "杭州",
    "南京",
    "无锡",
    "常州",
    "南通",
    "深圳",
    "广州",
)
COMPANY_SUFFIX_RE = re.compile(r"(有限责任公司|股份有限公司|有限公司|集团股份公司|集团有限公司|集团|公司)$")


def _blob(tx: dict[str, Any]) -> str:
    return " ".join(
        normalize_text(tx.get(field))
        for field in ("summary", "purpose", "remark", "counterparty_name", "counterparty_bank", "payee_name")
    )


def normalize_company_name(value: Any) -> str:
    text = normalize_text(value)
    text = re.sub(r"[()（）\[\]【】]", "", text)
    text = re.sub(r"\s+", "", text)
    return text


def company_core(value: Any) -> str:
    text = normalize_company_name(value)
    text = re.sub(r"[（(][^）)]*[）)]", "", text)
    for region in REGION_WORDS:
        text = text.replace(region, "")
    text = COMPANY_SUFFIX_RE.sub("", text)
    for suffix in ("机电设备", "设备", "科技", "贸易", "工程", "建设", "材料", "实业"):
        if len(text) > len(suffix) + 2:
            text = text.replace(suffix, "机电" if suffix == "机电设备" else "")
    return text[:8]


def _build_customer_aliases(company_name: str | None, metadata: dict[str, Any], transactions: list[dict[str, Any]]) -> set[str]:
    aliases: set[str] = set()
    for value in (
        company_name,
        metadata.get("customer_name"),
        metadata.get("customerName"),
        metadata.get("company_name"),
        metadata.get("companyName"),
    ):
        if value:
            aliases.add(normalize_company_name(value))
    for key in ("related_company_names", "relatedCompanyNames", "customer_aliases", "customerAliases", "related_party_keywords"):
        value = metadata.get(key)
        if isinstance(value, str):
            aliases.add(normalize_company_name(value))
        elif isinstance(value, (list, tuple, set)):
            aliases.update(normalize_company_name(item) for item in value if item)

    # 高频同字号公司可作为候选关联主体。这里只生成别名，不会把普通交易对手直接当成本方账号。
    base_cores = {company_core(item) for item in aliases if company_core(item)}
    counter = Counter(
        normalize_company_name(tx.get("counterparty_name") or tx.get("payee_name"))
        for tx in transactions
        if tx.get("counterparty_name") or tx.get("payee_name")
    )
    for name, count in counter.items():
        if count < 2:
            continue
        core = company_core(name)
        if core and core in base_cores:
            aliases.add(name)
    return {item for item in aliases if item}


def _known_accounts(metadata: dict[str, Any], transactions: list[dict[str, Any]]) -> set[str]:
    accounts: set[str] = set()
    for key in ("known_account_numbers", "knownAccounts", "account_numbers", "accountNumbers"):
        value = metadata.get(key)
        if isinstance(value, str):
            normalized = normalize_account_number(value)
            if normalized:
                accounts.add(normalized)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                normalized = normalize_account_number(item)
                if normalized:
                    accounts.add(normalized)
    for tx in transactions:
        normalized = normalize_account_number(tx.get("account_number"))
        if normalized:
            accounts.add(normalized)
    return accounts


def _matches_alias(name: str, aliases: set[str]) -> bool:
    normalized = normalize_company_name(name)
    if not normalized:
        return False
    if normalized in aliases:
        return True
    core = company_core(normalized)
    return bool(core and any(core == company_core(alias) or (len(core) >= 4 and core in company_core(alias)) for alias in aliases))


def classify_transaction_nature(
    tx: dict[str, Any],
    *,
    aliases: set[str],
    known_accounts: set[str],
) -> dict[str, Any]:
    text = _blob(tx)
    direction = tx.get("direction")
    counterparty_name = normalize_text(tx.get("counterparty_name"))
    payee_name = normalize_text(tx.get("payee_name"))
    counterparty_account = normalize_account_number(tx.get("counterparty_account"))
    payee_account = normalize_account_number(tx.get("payee_account"))

    names = [name for name in (counterparty_name, payee_name) if name]

    # In Beijing Bank exports, incoming receipts often have payee_name/payee_account
    # equal to the current company. That is normal receipt metadata, not proof of
    # internal transfer. For inflow, only the payer/counterparty side can prove
    # internal transfer; for outflow, the payee side can.
    account_hit = bool(
        (counterparty_account and counterparty_account in known_accounts)
        or (direction == "outflow" and payee_account and payee_account in known_accounts)
    )
    alias_hit = bool(
        (counterparty_name and _matches_alias(counterparty_name, aliases))
        or (direction == "outflow" and payee_name and _matches_alias(payee_name, aliases))
    )
    transfer_hint = any(word in text for word in TRANSFER_WORDS)
    # Internal transfer is intentionally strict: a transfer keyword alone is
    # not enough, and a similar name alone is not enough. Normal customer
    # receipts often contain "转账" or "货款", so require self-account/name
    # evidence plus a transfer/collection hint.
    if (account_hit and transfer_hint) or (alias_hit and transfer_hint):
        return {
            "nature": "internal_transfer",
            "exclude_from_operating": True,
            "reason": "对手方/收款人或账号命中本方及同字号关联主体，按内部往来剔除",
            "confidence": 0.92 if account_hit else 0.82,
        }
    if alias_hit:
        return {
            "nature": "related_party",
            "exclude_from_operating": bool(False),
            "reason": "对手方名称与客户核心字号相同，先按关联方往来列示，未强制剔除经营流水",
            "confidence": 0.75,
        }
    if any(is_probable_person_name(name) for name in names):
        return {
            "nature": "personal_transfer",
            "exclude_from_operating": bool(False),
            "reason": "对手方疑似个人账户往来，先标记为需补充用途说明，未默认全额剔除",
            "confidence": 0.7,
        }
    if any(word in text for word in ("工资", "薪资", "奖金", "社保", "公积金", "税务", "税款", "缴税", "国库", "手续费")):
        return {
            "nature": "fee_tax_salary",
            "exclude_from_operating": False,
            "reason": "税费、工资或手续费类经营相关支出",
            "confidence": 0.65,
        }
    return {
        "nature": "operating",
        "exclude_from_operating": False,
        "reason": "未命中内部往来、关联方或个人往来规则",
        "confidence": 0.55,
    }


def classify_transactions(transactions: list[dict[str, Any]], company_name: str | None = None, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    metadata = metadata or {}
    aliases = _build_customer_aliases(company_name, metadata, transactions)
    known_accounts = _known_accounts(metadata, transactions)
    amounts = sorted(float(tx.get("normalized_amount") or 0) for tx in transactions if float(tx.get("normalized_amount") or 0) > 0)
    p90 = quantiles(amounts, n=10)[-1] if len(amounts) >= 10 else 100000.0
    large_threshold = max(100000.0, p90)
    nature_counts: Counter[str] = Counter()
    internal_counterparties: Counter[str] = Counter()

    for tx in transactions:
        text = _blob(tx)
        counterparty = normalize_text(tx.get("counterparty_name") or tx.get("payee_name"))
        tags = list(tx.get("tags") or [])
        nature = classify_transaction_nature(tx, aliases=aliases, known_accounts=known_accounts)
        tx["nature"] = nature["nature"]
        tx["exclude_from_operating"] = bool(nature["exclude_from_operating"])
        tx["nature_reason"] = nature["reason"]
        tx["nature_confidence"] = nature["confidence"]
        nature_counts[tx["nature"]] += 1

        if tx["nature"] == "internal_transfer":
            tx["is_internal_transfer"] = True
            tags.append("internal_transfer")
            internal_counterparties[counterparty or "未知对手方"] += 1
        elif tx["nature"] == "related_party":
            tx["is_related_party"] = True
            tags.append("related_party")
        elif tx["nature"] == "personal_transfer":
            tx["is_personal_counterparty"] = True
            tags.append("personal_counterparty")

        if any(word in text for word in ("工资", "薪资", "奖金", "社保", "公积金")):
            category = "工资薪酬"
        elif any(word in text for word in ("税务", "税款", "缴税", "国库")):
            category = "税费"
        elif any(word in text for word in ("贷款", "还款", "利息", "贴现", "承兑", "保证金", "担保费", "扣息", "手续费")):
            category = "贷款还款" if "还款" in text or "贷款" in text else "利息"
        elif tx["is_internal_transfer"]:
            category = "内部往来"
        elif tx["is_related_party"]:
            category = "关联方往来"
        elif tx["is_personal_counterparty"]:
            category = "个人往来"
        elif tx.get("direction") == "inflow":
            category = "客户回款" if any(word in counterparty for word in ORG_WORDS) else "经营收入"
        elif tx.get("direction") == "outflow":
            category = "供应商付款" if any(word in counterparty for word in ORG_WORDS) else "经营支出"
        else:
            category = "其他"

        tx["category"] = category
        tx["is_large_amount"] = float(tx.get("normalized_amount") or 0) >= large_threshold
        if tx["is_large_amount"]:
            tags.append("large_amount")
        tx["tags"] = sorted(set(tags))

    internal_amount = sum(float(tx.get("normalized_amount") or 0) for tx in transactions if tx.get("is_internal_transfer"))
    logger.info(
        "[EnterpriseFlow][NatureClassify] total=%s operating=%s internal_transfer=%s related_party=%s personal=%s unknown=%s",
        len(transactions),
        nature_counts.get("operating", 0),
        nature_counts.get("internal_transfer", 0),
        nature_counts.get("related_party", 0),
        nature_counts.get("personal_transfer", 0),
        nature_counts.get("unknown", 0),
    )
    logger.info(
        "[EnterpriseFlow][InternalTransfer] amount=%s count=%s top_counterparties=%s",
        round(internal_amount, 2),
        nature_counts.get("internal_transfer", 0),
        internal_counterparties.most_common(5),
    )
    return transactions
