from __future__ import annotations

import logging
import re
from typing import Any

from .evidence import clean_amount, clean_value, first_match, split_numbered_blocks, value_after_label
from .schema import LOAN_ACCOUNT_FIELDS, ensure_record_fields

logger = logging.getLogger(__name__)

LOAN_TYPES = ("购房贷款", "住房贷款", "经营性贷款", "经营贷款", "其他个人消费贷款", "个人消费贷款", "消费贷款", "汽车贷款", "其他贷款", "贷款")
STATUS_WORDS = ("未结清", "已结清", "正常", "逾期", "结清", "关闭", "销户")
FIVE_CATEGORY_WORDS = ("正常", "关注", "次级", "可疑", "损失")
CLOSED_STATUS_WORDS = ("已结清", "结清", "已关闭", "关闭")
ABNORMAL_WORDS = ("逾期", "呆账", "代偿", "核销", "强制执行", "90天以上逾期")
ABNORMAL_FIVE_CATEGORY_WORDS = ("关注", "次级", "可疑", "损失")
ZERO_LIKE_STATUS_WORDS = ("未识别", "未知", "不详")
STOP_SECTION_KEYWORDS = (
    "相关还款责任信息",
    "相关还款责任",
    "担保信息",
    "保证合同编号",
    "查询记录",
    "查询记录明细",
    "机构查询记录明细",
    "本人查询记录明细",
    "公共记录",
    "公共信息",
    "说明",
    "本人声明",
    "异议标注",
)
POLLUTED_INSTITUTION_KEYWORDS = (
    "查询记录",
    "查询记录明细",
    "机构查询",
    "本人查询",
    "相关还款责任",
    "担保信息",
    "公共记录",
)
POLLUTED_EVIDENCE_KEYWORDS = (
    "为企业相关还款责任",
    "为个人相关还款责任",
    "相关还款责任信息",
    "承担相关还款责任",
    "责任人类型",
    "保证合同编号",
    "保证人",
    "共同借款人",
)
QUERY_RECORD_KEYWORDS = ("查询记录明细", "查询日期", "查询机构", "查询原因", "贷款审批", "信用卡审批", "贷后管理")
OWN_LOAN_HINTS = ("发放的", "发放贷款", "贷款授信", "为其他个人消费贷款授信", "余额为", "当前无逾期", "五级分类")
NEGATIVE_ABNORMAL_PHRASES = ("当前无逾期", "无逾期", "未发生逾期", "没有逾期")
PERSONAL_LOAN_TYPE_PATTERN = r"(?:个人经营性贷款|其他个人消费贷款|个人消费贷款|个人住房商业贷款|个人住房贷款|个人商用房贷款|住房公积金贷款|经营性贷款|消费贷款|汽车贷款|其他贷款)"


def _normalize_block(block: str) -> str:
    text = str(block or "").replace("\r", "\n")
    text = re.sub(r"第\s*\d+\s*页\s*[，,]\s*共\s*\d+\s*页", " ", text)
    text = re.sub(r"[ \t\u3000]+", " ", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"截至\s*((?:19|20)\d{2})年\s*(\d{1,2})月", lambda m: f"截至{m.group(1)}年{int(m.group(2)):02d}月", text)
    text = re.sub(r"信用\s*额度", "信用额度", text)
    text = re.sub(r"额度\s*有效期", "额度有效期", text)
    return clean_value(text)


def _normalize_date(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    match = re.search(r"((?:19|20)\d{2})[-./](\d{1,2})[-./](\d{1,2})", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return clean_value(value)


def _normalize_year_month(value: str) -> str:
    match = re.search(r"((?:19|20)\d{2})年(\d{1,2})月", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    match = re.search(r"((?:19|20)\d{2})[-./](\d{1,2})", str(value or ""))
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    return clean_value(value)


def _money_with_yuan(value: str) -> str:
    text = clean_amount(value)
    if not text or text in {"--", "-"}:
        return text
    return text if any(unit in text for unit in ("元", "万元")) else f"{text}元"


def _looks_like_loan(block: str) -> bool:
    return any(keyword in block for keyword in LOAN_TYPES)


def _strip_stop_sections(text: str) -> str:
    source = str(text or "")
    stops = [source.find(keyword) for keyword in STOP_SECTION_KEYWORDS if keyword in source]
    stops = [index for index in stops if index >= 0]
    if not stops:
        return source
    return source[:min(stops)]


def _extract_loan_window(text: str) -> str:
    source = str(text or "")
    if not source.strip():
        return ""
    start_positions = []
    for anchor in ("从未发生过逾期的账户明细如下", "发生过逾期的账户明细如下", "贷款账户明细", "\n贷款\n", "\r\n贷款\r\n"):
        index = source.find(anchor)
        if index >= 0:
            start_positions.append(index)
    start = min(start_positions) if start_positions else 0
    tail = source[start:]
    stop_positions: list[int] = []
    for heading in ("信用卡", "相关还款责任信息", "相关还款责任", "非信贷交易记录", "公共记录", "公共信息", "查询记录", "查询记录明细"):
        match = re.search(rf"(?m)^\s*{re.escape(heading)}\s*[:：]?\s*$", tail)
        if match and match.start() > 0:
            stop_positions.append(match.start())
    return tail[:min(stop_positions)] if stop_positions else tail


def _extract_label(block: str, labels: tuple[str, ...], *, max_chars: int = 120) -> str:
    value = value_after_label(block, labels, max_chars=max_chars)
    if value:
        return value
    compact = _normalize_block(block)
    for label in labels:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*([^,，;；。|｜]{{1,{max_chars}}})"
        match = re.search(pattern, compact)
        if match:
            return clean_value(match.group(1))
    return ""


def _extract_institution(block: str) -> str:
    value = (
        _extract_label(block, ("机构", "贷款机构", "发放机构", "授信机构", "管理机构"), max_chars=80)
        or first_match(block, (r"([\u4e00-\u9fffA-Za-z0-9（）()·]{2,50}(?:银行|小额贷款|消费金融|汽车金融|信托|财务公司|信用社|金融公司)[\u4e00-\u9fffA-Za-z0-9（）()·]{0,30})",))
    )
    value = re.sub(r"^(?:19|20)\d{2}年\d{1,2}月\d{1,2}日", "", value or "")
    value = re.split(r"(?:发放|为其他个人消费贷款授信|为个人消费贷款授信|为消费贷款授信)", value, maxsplit=1)[0]
    return clean_value(value)


def _extract_business_type(block: str) -> str:
    direct = _extract_label(block, ("业务类型", "贷款类型", "业务种类", "账户类型"), max_chars=60)
    if direct:
        for item in LOAN_TYPES:
            if item in direct:
                return item
        return direct
    for item in LOAN_TYPES:
        if item != "贷款" and item in block:
            return item
    return first_match(block, (r"(购房贷款|住房贷款|经营性贷款|经营贷款|其他个人消费贷款|个人消费贷款|消费贷款|汽车贷款|其他贷款|贷款)",))


def _extract_date(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=40)
    match = re.search(r"(?:19|20)\d{2}[-./年]\d{1,2}[-./月]\d{1,2}日?", labeled or "")
    if match:
        return clean_value(match.group(0))
    return ""


def parse_personal_loan_sentence(sentence: str) -> dict[str, Any]:
    source = _normalize_block(sentence)
    base: dict[str, Any] = {
        "account_no": "",
        "start_date": "",
        "institution": "",
        "loan_type": "",
        "business_type": "",
        "open_date": "",
        "due_date": "",
        "cutoff_date": "",
        "amount": "",
        "loan_amount": "",
        "issued_amount": "",
        "balance": "",
        "overdue_status": "",
        "account_status": "",
        "five_category": "",
        "overdue_amount": "",
        "overdue_months": "",
        "latest_repayment_date": "",
        "latest_repayment_amount": "",
        "overdue_info": "",
        "last_repayment": "",
        "history_performance": "",
        "information_report_date": "",
        "evidence": source[:1000],
        "evidence_text": source[:1000],
    }
    direct = re.search(
        rf"(?P<start>(?:19|20)\d{{2}}年\d{{1,2}}月\d{{1,2}}日)\s*"
        rf"(?P<institution>.+?)发放的\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*元\s*[（(]\s*人民币\s*[）)]\s*"
        rf"(?P<loan_type>{PERSONAL_LOAN_TYPE_PATTERN})\s*[，,]\s*"
        rf"(?P<due>(?:19|20)\d{{2}}年\d{{1,2}}月\d{{1,2}}日)\s*到期\s*[。.]?\s*"
        rf"截至\s*(?P<cutoff>(?:19|20)\d{{2}}年\d{{1,2}}月)\s*[，,]\s*余额\s*(?P<balance>[0-9][0-9,]*(?:\.\d+)?)",
        source,
        flags=re.S,
    )
    if direct:
        amount = _money_with_yuan(direct.group("amount"))
        balance = _money_with_yuan(direct.group("balance"))
        base.update({
            "start_date": _normalize_date(direct.group("start")),
            "open_date": _normalize_date(direct.group("start")),
            "institution": clean_value(direct.group("institution")),
            "loan_type": clean_value(direct.group("loan_type")),
            "business_type": clean_value(direct.group("loan_type")),
            "due_date": _normalize_date(direct.group("due")),
            "cutoff_date": _normalize_year_month(direct.group("cutoff")),
            "amount": amount,
            "loan_amount": amount,
            "issued_amount": amount,
            "balance": balance,
            "overdue_status": "无 / 当前无逾期",
            "account_status": "未结清",
        })
        return base

    revolving = re.search(
        rf"(?P<start>(?:19|20)\d{{2}}年\d{{1,2}}月\d{{1,2}}日)\s*"
        rf"(?P<institution>.+?)(?:为)?(?P<loan_type>{PERSONAL_LOAN_TYPE_PATTERN})授信\s*[，,]\s*"
        rf"额度有效期至\s*(?P<due>(?:19|20)\d{{2}}年\d{{1,2}}月\d{{1,2}}日)\s*[，,]\s*可循环使用\s*[。.]?\s*"
        rf"截至\s*(?P<cutoff>(?:19|20)\d{{2}}年\d{{1,2}}月)\s*[，,]\s*"
        rf"信用额度\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*元\s*(?:[（(]\s*人民币\s*[）)])?\s*[，,]\s*"
        rf"余额为?\s*(?P<balance>[0-9][0-9,]*(?:\.\d+)?)\s*[，,]?\s*(?P<overdue>当前无逾期|无逾期|当前有逾期|逾期[^。.]*)?",
        source,
        flags=re.S,
    )
    if revolving:
        amount = _money_with_yuan(revolving.group("amount"))
        balance = _money_with_yuan(revolving.group("balance"))
        overdue = clean_value(revolving.group("overdue") or "当前无逾期")
        base.update({
            "start_date": _normalize_date(revolving.group("start")),
            "open_date": _normalize_date(revolving.group("start")),
            "institution": clean_value(revolving.group("institution")),
            "loan_type": clean_value(revolving.group("loan_type")),
            "business_type": clean_value(revolving.group("loan_type")),
            "due_date": _normalize_date(revolving.group("due")),
            "cutoff_date": _normalize_year_month(revolving.group("cutoff")),
            "amount": amount,
            "loan_amount": amount,
            "issued_amount": amount,
            "balance": balance,
            "overdue_status": overdue,
            "account_status": "当前有效",
        })
        return base

    return base


def _extract_money(block: str, labels: tuple[str, ...]) -> str:
    labeled = _extract_label(block, labels, max_chars=80)
    if labeled:
        money = first_match(labeled, (r"((?:人民币)?\s*[0-9][0-9,]*(?:\.\d+)?\s*(?:万?元|万元)?)",))
        value = clean_amount(money or labeled)
        if _looks_like_date_pollution(value, block):
            return ""
        return value
    return ""


def _looks_like_date_pollution(value: Any, block: str) -> bool:
    text = re.sub(r"\s+", "", str(value or ""))
    if not re.fullmatch(r"20[2-3]\d(?:年)?", text):
        return False
    return any(keyword in block for keyword in ("报告日期", "信息报告日期", "报告时间", "查询时间"))


def _extract_status(block: str) -> str:
    labeled = _extract_label(block, ("账户状态", "状态", "当前状态"), max_chars=40)
    labeled_for_status = labeled
    block_for_status = block
    for phrase in NEGATIVE_ABNORMAL_PHRASES:
        labeled_for_status = labeled_for_status.replace(phrase, "")
        block_for_status = block_for_status.replace(phrase, "")
    for word in STATUS_WORDS:
        if word in labeled_for_status:
            return word
    for word in STATUS_WORDS:
        if word in block_for_status:
            return word
    return clean_value(labeled)


def _extract_five_category(block: str) -> str:
    labeled = _extract_label(block, ("五级分类", "分类"), max_chars=40)
    for word in FIVE_CATEGORY_WORDS:
        if word in labeled:
            return word
    for word in FIVE_CATEGORY_WORDS:
        if f"五级分类{word}" in block or f"分类{word}" in block:
            return word
    return clean_value(labeled)


def _extract_overdue_months(block: str) -> str:
    labeled = _extract_label(block, ("逾期月份", "逾期月数", "逾期期数", "逾期信息"), max_chars=80)
    source = labeled or block
    match = re.search(r"逾期\s*(\d+)\s*(?:个)?月", source)
    if match:
        return match.group(1)
    match = re.search(r"(\d+)\s*(?:个)?月", labeled)
    return match.group(1) if match else ""


def _extract_account_no(block: str) -> str:
    value = _extract_label(block, ("账户编号", "账户号", "账号", "合同编号"), max_chars=80)
    if value:
        match = re.search(r"([A-Za-z0-9\-*]{4,40})", value)
        return match.group(1) if match else value
    return first_match(block, (r"(?:账户编号|账户号|账号|合同编号)\s*[:：]?\s*([A-Za-z0-9\-*]{4,40})",))


def _amount_to_number(value: Any) -> float:
    text = re.sub(r"[,\s，人民币元]", "", str(value or ""))
    match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    if not match:
        return 0.0
    number = float(match.group(1))
    if "万" in str(value or ""):
        number *= 10000
    return number


def _is_abnormal_account(block: str, record: dict[str, Any]) -> bool:
    combined = " ".join(str(item or "") for item in (
        record.get("account_status"),
        record.get("five_category"),
        record.get("overdue_amount"),
        record.get("overdue_months"),
        record.get("overdue_info"),
        record.get("history_performance"),
    ))
    if "信贷记录概要" not in block and "信用卡账户数" not in block and "贷款账户数" not in block:
        combined = f"{combined} {block}"
    for phrase in NEGATIVE_ABNORMAL_PHRASES:
        combined = combined.replace(phrase, "")
    if any(word in combined for word in ABNORMAL_WORDS):
        return True
    if any(word in str(record.get("five_category") or "") for word in ABNORMAL_FIVE_CATEGORY_WORDS):
        return True
    return _amount_to_number(record.get("overdue_amount")) > 0


def is_polluted_loan_account(record: dict[str, Any], evidence_text: str) -> bool:
    institution = str(record.get("institution") or "").strip()
    evidence = str(evidence_text or record.get("evidence") or record.get("evidence_text") or "")
    account_no = str(record.get("account_no") or "").strip()
    if any(keyword in institution for keyword in POLLUTED_INSTITUTION_KEYWORDS):
        return True
    if any(keyword in evidence for keyword in POLLUTED_EVIDENCE_KEYWORDS):
        return True
    if account_no.upper().startswith("D") and "保证合同编号" in evidence:
        return True
    if "相关还款责任金额" in evidence or "承担相关还款责任" in evidence:
        return True
    if institution == "查询记录明细":
        return True
    if (not institution or institution == "查询记录明细") and any(keyword in evidence for keyword in QUERY_RECORD_KEYWORDS):
        return True
    meaningful_keys = ("account_no", "institution", "business_type", "open_date", "due_date", "amount", "balance", "account_status", "five_category", "overdue_amount")
    meaningful_count = sum(1 for key in meaningful_keys if str(record.get(key) or "").strip() and str(record.get(key) or "").strip() != "未识别")
    has_only_money = bool(record.get("balance") or record.get("amount")) and meaningful_count <= 4
    if has_only_money and not any(keyword in evidence for keyword in OWN_LOAN_HINTS):
        return True
    return False


def _should_skip_closed_loan(block: str, record: dict[str, Any]) -> bool:
    status_text = " ".join(str(item or "") for item in (record.get("account_status"), block))
    if _is_abnormal_account(block, record):
        return False
    if "未结清" not in status_text and any(word in status_text for word in CLOSED_STATUS_WORDS):
        return True
    if "授信" in str(record.get("evidence") or record.get("evidence_text") or block) and record.get("due_date"):
        return False
    balance_value = _amount_to_number(record.get("balance"))
    if record.get("balance") and balance_value <= 0:
        return True
    if not record.get("balance") and any(word in str(record.get("account_status") or "") for word in ZERO_LIKE_STATUS_WORDS):
        return True
    return False


def _candidate_blocks(text: str) -> list[str]:
    explicit = str(text or "")
    explicit = _normalize_block(explicit)
    date_blocks = re.split(
        r"(?=(?:\d+[\.、)]\s*)?(?:19|20)\d{2}年\d{1,2}月\d{1,2}日[^。；;]{0,120}(?:发放的|授信))",
        explicit,
    )
    blocks = [block for block in date_blocks if block.strip()]
    if not blocks:
        blocks = split_numbered_blocks(explicit)
    if not blocks:
        blocks = re.split(r"(?=(?:\d+[\.、)]\s*)?(?:[\u4e00-\u9fffA-Za-z0-9（）()·]{2,50})?(?:购房贷款|住房贷款|经营性贷款|经营贷款|消费贷款|汽车贷款|其他贷款))", explicit)
    return [block.strip() for block in blocks if block and _looks_like_loan(block)]


def extract_loan_accounts(sections: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        text = _extract_loan_window("\n".join(
            _strip_stop_sections(str(sections.get(key) or ""))
            for key in ("loan_accounts", "credit_transaction_details", "full_text")
            if sections.get(key)
        ))
        logger.info("[PersonalCredit][Loan][SECTION_LEN]=%s", len(text))
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, ...]] = set()
        for index, block in enumerate(_candidate_blocks(text), start=1):
            logger.info("[PersonalCredit][Loan][CANDIDATE] index=%s raw_start=%s", index, _normalize_block(block)[:300])
            normalized_block = _normalize_block(block)
            parsed = parse_personal_loan_sentence(normalized_block)
            if parsed.get("institution") and parsed.get("balance"):
                record = parsed
            else:
                amount = _extract_money(block, ("发放金额", "借款金额", "贷款金额", "授信金额"))
                latest_date = _extract_date(block, ("最近一次还款日期", "最近还款日期", "最近一次还款", "最近还款"))
                latest_amount = _extract_money(block, ("最近一次还款金额", "最近还款金额"))
                overdue_amount = _extract_money(block, ("当前逾期金额", "逾期金额"))
                record = {
                    "account_no": _extract_account_no(block),
                    "start_date": _extract_date(block, ("起始日期", "发放日期", "开户日期", "开立日期")),
                    "institution": _extract_institution(block),
                    "loan_type": _extract_business_type(block),
                    "business_type": _extract_business_type(block),
                    "open_date": _extract_date(block, ("发放日期", "开户日期", "开立日期", "起始日期")),
                    "due_date": _extract_date(block, ("到期日期", "结清日期", "结束日期")),
                    "cutoff_date": _extract_date(block, ("截止日期", "截至日期")),
                    "amount": amount,
                    "loan_amount": amount,
                    "issued_amount": amount,
                    "balance": _extract_money(block, ("余额", "本金余额", "贷款余额")),
                    "overdue_status": _extract_label(block, ("逾期", "逾期状态"), max_chars=80),
                    "account_status": _extract_status(block),
                    "five_category": _extract_five_category(block),
                    "overdue_amount": overdue_amount,
                    "overdue_months": _extract_overdue_months(block),
                    "latest_repayment_date": latest_date,
                    "latest_repayment_amount": latest_amount,
                    "overdue_info": _extract_label(block, ("逾期信息", "逾期记录"), max_chars=120),
                    "last_repayment": " ".join(item for item in (latest_date, latest_amount) if item),
                    "history_performance": _extract_label(block, ("历史表现", "还款表现", "还款记录"), max_chars=160),
                    "information_report_date": _extract_date(block, ("信息报告日期", "报送日期")),
                    "evidence": normalized_block[:1000],
                    "evidence_text": normalized_block[:1000],
                }
            if not record.get("institution") or not record.get("balance"):
                logger.info("[PersonalCredit][Loan][PARSE_FAIL] index=%s reason=missing_core_fields raw_start=%s", index, normalized_block[:300])
                continue
            logger.info(
                "[PersonalCredit][Loan][PARSE_OK] index=%s start_date=%s institution=%s amount=%s balance=%s",
                index,
                record.get("start_date") or record.get("open_date"),
                record.get("institution"),
                record.get("amount"),
                record.get("balance"),
            )
            if is_polluted_loan_account(record, normalized_block):
                logger.info("[PersonalCredit][Loan][FILTER_DROP] index=%s reason=polluted", index)
                continue
            if _should_skip_closed_loan(block, record):
                logger.info("[PersonalCredit][Loan][FILTER_DROP] index=%s reason=settled", index)
                continue
            if any(value for key, value in record.items() if key not in {"evidence", "evidence_text"}):
                signature = tuple(str(record.get(key) or "") for key in ("start_date", "institution", "business_type", "amount", "balance", "due_date", "cutoff_date"))
                if signature in seen:
                    continue
                seen.add(signature)
                records.append(ensure_record_fields(record, LOAN_ACCOUNT_FIELDS))
        logger.info("[PersonalCredit][Loan][FINAL_COUNT]=%s", len(records))
        return records
    except Exception:
        return []
