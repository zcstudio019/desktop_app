from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .schemas import BusinessRecord, CreditLineRecord, CreditSummary, LoanRecord, ReportMeta, RevolvingOverdraftRecord
from .segmenter import compact_text, normalize_text


INSTITUTION_PATTERN = (
    r"[\u4e00-\u9fa5A-Za-z0-9（）()]{2,100}?"
    r"(?:村镇银行股份有限公司|银行股份有限公司|银行|信用社|小额贷款|消费金融|财务公司|信托|"
    r"金融租赁有限公司|融资租赁[\u4e00-\u9fa5A-Za-z0-9（）()]*有限公司|租赁[\u4e00-\u9fa5A-Za-z0-9（）()]*有限公司)"
    r"[\u4e00-\u9fa5A-Za-z0-9（）()]{0,50}?"
)
BUSINESS_TYPES = (
    "有追索权的国内卖方保理融资|无追索权的国内卖方保理融资|国内保理融资|"
    "供应链保理融资|应收账款保理|保理融资|"
    "融资型租赁|中长期流动资金贷款|固定资产贷款|项目贷款|"
    "流动资金贷款|贸易融资贷款|融资租赁|循环透支|经营贷|周转贷|贷款"
)
GUARANTEE_TYPES = "信用/无担保|保证/保证金|保证|组合|抵押|质押|信用|无担保|其他"
CREDIT_TYPES = (
    "银行承兑汇票额度|商业承兑汇票额度|承兑汇票额度|信用证额度|"
    "保函额度|贸易融资额度|票据池额度|其他授信额度|"
    "综合授信|贷款|贸易融资|银行承兑汇票|信用证|保函|其他|保理|循环额度"
)
FIVE_CATEGORIES = "正常|关注|次级|可疑|损失|违约|未分类"
INVALID_INSTITUTION_FRAGMENTS = {
    "", "公司", "有限", "有限公司", "股份有限公司", "银行", "分行", "支行",
    "内卖方", "国内卖方", "保理融资", "有追索权", "无追索权", "流动资金", "贷款",
}
NON_LOAN_KEYWORDS = ["银行承兑汇票", "商业承兑汇票", "信用证", "保函", "银行保函", "保证金"]
MEDIUM_KEYWORDS = ["融资型租赁", "融资租赁", "售后回租", "长期借款", "固定资产贷款", "项目贷款", "中长期流动资金贷款"]
SHORT_KEYWORDS = [
    "有追索权的国内卖方保理融资", "无追索权的国内卖方保理融资", "国内卖方保理",
    "国内保理融资", "保理融资", "应收账款保理", "供应链保理融资",
    "流动资金贷款", "短期借款", "经营贷", "周转贷", "贸易融资贷款",
]


def to_amount(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if text in {"", "--", "未识别"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def format_date(value: str) -> str:
    if not value:
        return ""
    m = re.search(r"\d{4}-\d{2}-\d{2}", str(value))
    if m:
        return m.group(0)
    return "长期" if "长期" in str(value) else ""


def classify_credit_business(record: dict[str, Any]) -> str:
    biz = str(record.get("business_type") or record.get("biz_type") or "")
    start_date = str(record.get("start_date") or record.get("open_date") or "")
    end_date = str(record.get("end_date") or record.get("due_date") or "")
    if any(keyword in biz for keyword in NON_LOAN_KEYWORDS):
        return "non_loan"
    if any(keyword in biz for keyword in MEDIUM_KEYWORDS):
        return "medium_long_term_loan"
    days = _duration_days(start_date, end_date)
    if days is not None and days > 366:
        return "medium_long_term_loan"
    if any(keyword in biz for keyword in SHORT_KEYWORDS):
        return "short_term_loan"
    return "unknown"


def normalize_institution_name(raw: str, context: str = "") -> str:
    s = re.sub(r"\s+", "", str(raw or ""))
    ctx = re.sub(r"\s+", "", str(context or ""))
    for keyword in [
        "账户编号", "账户编", "授信机构", "业务种类", "开立日期", "到期日", "币种",
        "借款金额", "信用额度", "发放形式", "担保方式", "余额", "五级分类",
        "逾期总额", "逾期本金", "逾期月数", "最近一次还款日期",
        "最近一次还款总额", "最近一次还款形式", "特定交易提示",
        "授信协议编号", "历史表现", "信息报告日期",
    ]:
        s = s.replace(keyword, "")
    s = re.sub(r"^[A-Z0-9_-]{6,}", "", s)
    s = re.sub(r"^\d{1,12}", "", s)
    if "股份有限" in s and "股份有限公司" not in s:
        s = s.replace("股份有限", "股份有限公司")
    start_markers = [
        "远东", "永赢金融租赁", "长江联合金融租赁", "平安国际融资租赁", "海通恒信国际融资租赁",
        "浙江网商银行", "温州银行", "江苏银行", "南京银行", "上海松江民生村镇银行",
        "浙商银行",
        "中国银行", "中国工商银行", "中国建设银行", "中国农业银行", "交通银行",
    ]
    marker_positions = [s.find(marker) for marker in start_markers if s.find(marker) != -1]
    if marker_positions:
        s = s[min(marker_positions):]
    candidates = _institution_candidates(s)
    if not candidates and ctx:
        candidates = _institution_candidates(ctx)
    if candidates:
        name = max(candidates, key=len)
        for biz in [
            "有追索权的国内卖方保理融资",
            "无追索权的国内卖方保理融资",
            "国内保理融资",
            "供应链保理融资",
            "应收账款保理",
            "保理融资",
            "融资型租赁",
            "中长期流动资金贷款",
            "固定资产贷款",
            "项目贷款",
            "流动资金贷款",
            "循环透支",
            "贸易融资贷款",
            "贷款",
        ]:
            idx = name.find(biz)
            if idx != -1:
                name = name[:idx]
                break
        return "" if name in INVALID_INSTITUTION_FRAGMENTS else name
    return "" if s in INVALID_INSTITUTION_FRAGMENTS else s


def clean_institution(name: str) -> str:
    return normalize_institution_name(name, name)


def evidence(text: str, start: int, end: int, radius: int = 80) -> str:
    return normalize_text(text[max(0, start - radius): min(len(text), end + radius)])


def extract_basic_info(sections: dict[str, Any]) -> ReportMeta:
    text = normalize_text(str(sections.get("basic_info") or "") + "\n" + str(sections.get("full_text") or "")[:3000])
    compact = compact_text(text)

    def value_after(label: str, max_len: int = 80) -> str:
        m = re.search(rf"{re.escape(label)}[:：]?\s*([^\n]+)", text)
        if m:
            return re.split(r"\s{2,}|中征码|统一社会信用代码|组织机构代码|工商注册号|纳税人识别号|报告时间|查询机构", m.group(1))[0].strip()
        idx = compact.find(label)
        if idx == -1:
            return ""
        return compact[idx + len(label): idx + len(label) + max_len]

    company = value_after("企业名称")
    company_match = re.search(r"[\u4e00-\u9fa5A-Za-z0-9（）()]{4,100}有限公司", company or compact)
    usc_match = re.search(r"[A-Z0-9]{18}", compact)
    query = value_after("查询机构")
    query_name = normalize_institution_name(query)
    report_match = re.search(r"报告时间[:：]?\s*(\d{4}-\d{2}-\d{2})(?:T\d{2}:\d{2}:\d{2})?", text)
    if not report_match:
        report_match = re.search(r"报告日期[:：]?\s*(\d{4}-\d{2}-\d{2})", text)

    return ReportMeta(
        query_org=query_name or query,
        report_time=report_match.group(1) if report_match else "",
        customer_name=company_match.group(0) if company_match else company,
        unified_social_credit_code=usc_match.group(0) if usc_match else "",
    )


def extract_credit_summary(sections: dict[str, Any]) -> CreditSummary:
    text = normalize_text(str(sections.get("credit_summary") or ""))
    summary = CreditSummary()
    m = re.search(r"借贷交易\s*担保交易.*?余额\s*(\d+(?:\.\d+)?)\s*余额\s*(\d+(?:\.\d+)?)", text, re.S)
    if m:
        summary.unsettled_credit_balance = to_amount(m.group(1))
        summary.external_guarantee_balance = to_amount(m.group(2))
    m = re.search(r"中长期借款\s+(\d+)\s+(\d+(?:\.\d+)?)", text)
    if m:
        summary.unsettled_credit_institution_count = (summary.unsettled_credit_institution_count or 0) + to_int(m.group(1))
        summary.medium_long_term_loan_balance = to_amount(m.group(2))
    m = re.search(r"短期借款\s+(\d+)\s+(\d+(?:\.\d+)?)", text)
    if m:
        summary.unsettled_credit_institution_count = (summary.unsettled_credit_institution_count or 0) + to_int(m.group(1))
        summary.short_term_loan_balance = to_amount(m.group(2))
    m = re.search(r"循环透支\s+(\d+)\s+(\d+(?:\.\d+)?)", text)
    if m:
        summary.unsettled_credit_institution_count = (summary.unsettled_credit_institution_count or 0) + to_int(m.group(1))
    return summary


def parse_loan_rows(section: str, *, term_type: str, source_section: str) -> list[LoanRecord]:
    text = normalize_text(section)
    compact = compact_text(text)
    if not compact:
        return []
    pattern = re.compile(
        rf"(?P<account_no>[A-Z]\d{{4,}}[A-Z0-9_-]*)?"
        rf"(?P<institution>{INSTITUTION_PATTERN})"
        rf"(?P<biz>{BUSINESS_TYPES})"
        rf"(?P<start>\d{{4}}-\d{{2}}-\d{{2}})"
        rf"(?P<end>\d{{4}}-\d{{2}}-\d{{2}}|长期)"
        rf"人民币元"
        rf"(?P<amount>\d+(?:\.\d+)?)"
        rf"(?:新增|无还本续贷|其他)?"
        rf".{{0,40}}?"
        rf"(?P<guarantee>{GUARANTEE_TYPES})"
        rf"(?P<balance>\d+(?:\.\d+)?)"
        rf"(?P<five>{FIVE_CATEGORIES})"
        rf"(?P<overdue_total>\d+(?:\.\d+)?)"
        rf"(?P<overdue_principal>\d+(?:\.\d+)?)"
        rf"(?P<overdue_months>\d+)"
        rf"(?P<last_repay_date>\d{{4}}-\d{{2}}-\d{{2}})",
        re.S,
    )
    records: list[LoanRecord] = []
    for match in pattern.finditer(compact):
        biz = match.group("biz")
        raw_evidence = evidence(compact, match.start(), match.end(), 80)
        if any(keyword in biz or keyword in raw_evidence for keyword in NON_LOAN_KEYWORDS):
            continue
        bank = normalize_institution_name(match.group("institution"), raw_evidence)
        record_type = classify_credit_business({
            "business_type": biz,
            "start_date": match.group("start"),
            "end_date": match.group("end"),
        })
        if term_type == "short_term_loan" and record_type != "short_term_loan":
            continue
        if term_type == "medium_long_term_loan" and record_type != "medium_long_term_loan":
            continue
        records.append(
            LoanRecord(
                institution_name=bank,
                business_type=biz,
                guarantee_type=match.group("guarantee"),
                loan_amount=to_amount(match.group("amount")),
                balance=to_amount(match.group("balance")),
                start_date=format_date(match.group("start")),
                end_date=format_date(match.group("end")),
                five_category=match.group("five"),
                overdue_months=to_int(match.group("overdue_months")),
                status=match.group("five"),
                evidence_text=raw_evidence,
                source_section=source_section,
                confidence=0.84,
            )
        )
    return _dedupe_loans(records)


def extract_short_term_loans(sections: dict[str, Any]) -> list[LoanRecord]:
    return parse_loan_rows(str(sections.get("short_term_loans") or ""), term_type="short_term_loan", source_section="short_term_loans")


def extract_medium_long_term_loans(sections: dict[str, Any]) -> list[LoanRecord]:
    primary = parse_loan_rows(str(sections.get("medium_long_term_loans") or ""), term_type="medium_long_term_loan", source_section="medium_long_term_loans")
    # 兼容报告/分段异常：如果融资租赁被落入短期 section，强制归入中长期，不进入短期。
    reclassified = parse_loan_rows(str(sections.get("short_term_loans") or ""), term_type="medium_long_term_loan", source_section="short_term_loans_reclassified")
    return _dedupe_loans([*primary, *reclassified])


def extract_credit_lines(sections: dict[str, Any]) -> list[CreditLineRecord]:
    text = normalize_text(str(sections.get("credit_lines") or ""))
    compact = compact_text(text)
    if not compact:
        return []
    pattern = re.compile(
        rf"(?P<institution>{INSTITUTION_PATTERN})"
        rf"(?P<credit_type>{CREDIT_TYPES})"
        rf"(?P<revolving>是|否)"
        rf"(?P<effective>\d{{4}}-\d{{2}}-\d{{2}})"
        rf"(?P<expiry>\d{{4}}-\d{{2}}-\d{{2}}|长期)"
        rf"(?P<report>\d{{4}}-\d{{2}}-\d{{2}})?"
        rf"人民币元"
        rf"(?P<amount_blob>\d+(?:\.\d+)?\d*(?:\.\d+)?)",
        re.S,
    )
    records: list[CreditLineRecord] = []
    for match in pattern.finditer(compact):
        credit_amount, used_amount = _split_credit_line_amount_blob(match.group("amount_blob"))
        records.append(
            CreditLineRecord(
                institution_name=normalize_institution_name(match.group("institution"), evidence(compact, match.start(), match.end(), 60)),
                credit_type=match.group("credit_type"),
                credit_revolving=match.group("revolving") == "是",
                credit_amount=credit_amount,
                used_amount=used_amount,
                effective_date=format_date(match.group("effective")),
                expiry_date=format_date(match.group("expiry")),
                report_date=format_date(match.group("report") or ""),
                currency="人民币",
                status="",
                evidence_text=evidence(compact, match.start(), match.end(), 60),
                source_section="credit_lines",
                confidence=0.78,
            )
        )
    return records


def _split_credit_line_amount_blob(blob: str) -> tuple[float | None, float | None]:
    text = str(blob or "").replace(",", "").strip()
    if not text:
        return None, None
    if "." in text:
        nums = re.findall(r"\d+(?:\.\d+)?", text)
        if len(nums) >= 2:
            return to_amount(nums[0]), to_amount(nums[1])
    if len(text) % 2 == 0:
        half = len(text) // 2
        left, right = text[:half], text[half:]
        if left == right:
            return to_amount(left), to_amount(right)
    for split_at in range(len(text) - 1, 0, -1):
        left, right = text[:split_at], text[split_at:]
        if right == "0" or (right.isdigit() and left.isdigit() and float(right) <= float(left)):
            return to_amount(left), to_amount(right)
    return to_amount(text), None


def parse_business_summary(section: str, business_terms: str, source_section: str) -> list[BusinessRecord]:
    text = normalize_text(section)
    compact = compact_text(text)
    if not compact:
        return []
    pattern = re.compile(
        rf"(?P<institution>[\u4e00-\u9fa5A-Za-z0-9（）()]{{2,90}}(?:银行|信用社|财务公司|保险股份有限公司|保险)[\u4e00-\u9fa5A-Za-z0-9（）()]{{0,50}})"
        rf"(?P<business>{business_terms})"
        rf"(?P<five>{FIVE_CATEGORIES})"
        rf"(?P<count>\d+)"
        rf"(?P<balance>\d+(?:\.\d+)?)",
        re.S,
    )
    records: list[BusinessRecord] = []
    for match in pattern.finditer(compact):
        institution = normalize_institution_name(match.group("institution"), evidence(compact, match.start(), match.end(), 60))
        if any(x in institution for x in ["授信机构", "业务种类", "五级分类", "账户数"]):
            continue
        records.append(
            BusinessRecord(
                institution_name=institution,
                business_type=match.group("business"),
                five_category=match.group("five"),
                account_count=to_int(match.group("count")),
                balance=to_amount(match.group("balance")),
                evidence_text=evidence(compact, match.start(), match.end(), 60),
                source_section=source_section,
                confidence=0.8,
            )
        )
    return records


def extract_bills(sections: dict[str, Any]) -> list[BusinessRecord]:
    section = str(sections.get("bills") or "")
    if not re.search(r"银行承兑汇票和信用证\s*共\s*\d+\s*笔", section):
        return []
    return parse_business_summary(section, "银行承兑汇票", "bills")


def extract_letters_of_credit(sections: dict[str, Any]) -> list[BusinessRecord]:
    section = str(sections.get("letters_of_credit") or "")
    if not re.search(r"银行承兑汇票和信用证\s*共\s*\d+\s*笔", section):
        return []
    return parse_business_summary(section, "信用证", "letters_of_credit")


def extract_guarantees(sections: dict[str, Any]) -> list[BusinessRecord]:
    section = str(sections.get("guarantees") or "")
    if not re.search(r"银行保函及其他业务\s*共\s*\d+\s*笔", section):
        return []
    return parse_business_summary(section, "非融资类银行保函|贷款保证保险|银行保函|保函", "guarantees")


def extract_external_guarantees(sections: dict[str, Any]) -> list[BusinessRecord]:
    return []


def _duration_days(start_date: str, end_date: str) -> int | None:
    try:
        if not start_date or not end_date or end_date == "长期":
            return None
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        return (d2 - d1).days
    except Exception:
        return None


def _institution_candidates(text: str) -> list[str]:
    patterns = [
        r"[\u4e00-\u9fa5]{2,30}村镇银行股份有限公司",
        r"[\u4e00-\u9fa5]{2,50}银行股份有限公司[\u4e00-\u9fa5]{0,30}(?:分行|支行|营业部)",
        r"[\u4e00-\u9fa5]{2,50}银行股份有限公司",
        r"[\u4e00-\u9fa5A-Za-z0-9（）()]{2,60}融资租赁[\u4e00-\u9fa5A-Za-z0-9（）()]*有限公司",
        r"[\u4e00-\u9fa5]{2,50}金融租赁有限公司",
        r"[\u4e00-\u9fa5]{2,50}租赁有限公司",
        r"[\u4e00-\u9fa5]{2,50}(?:小额贷款|消费金融|财务公司|信托)[\u4e00-\u9fa5]{0,20}",
    ]
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(match.group(0) for match in re.finditer(pattern, text))
    return candidates


def _dedupe_loans(loans: list[LoanRecord]) -> list[LoanRecord]:
    result: list[LoanRecord] = []
    seen: set[tuple[Any, ...]] = set()
    for loan in loans:
        key = (
            loan.institution_name,
            loan.business_type,
            loan.start_date,
            loan.end_date,
            loan.loan_amount,
            loan.balance,
            loan.guarantee_type,
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(loan)
    return result


# ---------------------------------------------------------------------------
# UTF-8 safe overrides added after the legacy mojibake-compatible extractors.
# The module keeps the old names for compatibility; Python binds these later
# definitions, so callers automatically use the hardened implementations below.

COMPANY_SUFFIX_PATTERN_UTF8 = r"(?:股份有限公司|有限公司|合伙企业|分公司|个体工商户|鏈夐檺鍏徃|鑲′唤鏈夐檺鍏徃)"
COMPANY_STOP_LABELS_UTF8 = [
    "中征码",
    "统一社会信用代码",
    "组织机构代码",
    "工商注册号",
    "纳税人识别号",
    "报告时间",
    "查询机构",
    "涓緛鐮?",
    "缁熶竴绀句細淇＄敤浠ｇ爜",
    "缁勭粐鏈烘瀯浠ｇ爜",
    "宸ュ晢娉ㄥ唽鍙?",
    "绾崇◣浜鸿瘑鍒彿",
    "鎶ュ憡鏃堕棿",
    "鏌ヨ鏈烘瀯",
]


def normalize_company_name(raw: str) -> str:
    value = re.sub(r"\s+", "", str(raw or ""))
    value = value.replace("(", "（").replace(")", "）")
    for label in COMPANY_STOP_LABELS_UTF8:
        idx = value.find(label)
        if idx != -1:
            value = value[:idx]
    match = re.search(rf"[\u4e00-\u9fa5A-Za-z0-9（）()·]+?{COMPANY_SUFFIX_PATTERN_UTF8}", value)
    return match.group(0) if match else value


def _extract_company_name_from_lines_utf8(text: str) -> str:
    labels = ("企业名称", "浼佷笟鍚嶇О")
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        if not any(label in line for label in labels):
            continue
        current = line
        for label in labels:
            current = re.sub(rf".*?{re.escape(label)}\s*[:：锛歖]?\s*", "", current)
        parts = [current]
        for next_line in lines[idx + 1: idx + 4]:
            if any(stop in next_line for stop in COMPANY_STOP_LABELS_UTF8):
                break
            parts.append(next_line)
            if re.search(COMPANY_SUFFIX_PATTERN_UTF8, normalize_company_name("".join(parts))):
                break
        merged = normalize_company_name("".join(parts))
        if merged:
            return merged
    compact = compact_text(text)
    match = re.search(
        rf"(?:企业名称|浼佷笟鍚嶇О)[:：锛歖]?([\u4e00-\u9fa5A-Za-z0-9（）()·]{{4,140}}?{COMPANY_SUFFIX_PATTERN_UTF8})",
        compact,
    )
    return normalize_company_name(match.group(1)) if match else ""


def extract_basic_info(sections: dict[str, Any]) -> ReportMeta:
    text = normalize_text(str(sections.get("basic_info") or "") + "\n" + str(sections.get("full_text") or "")[:5000])
    compact = compact_text(text)

    def value_after(label: str, max_len: int = 120) -> str:
        m = re.search(rf"{re.escape(label)}[:：锛歖]?\s*([^\n]+)", text)
        if m:
            return re.split(
                r"\s{2,}|中征码|统一社会信用代码|组织机构代码|工商注册号|纳税人识别号|报告时间|查询机构|涓緛鐮?|缁熶竴绀句細淇＄敤浠ｇ爜|缁勭粐鏈烘瀯浠ｇ爜|宸ュ晢娉ㄥ唽鍙?|绾崇◣浜鸿瘑鍒彿|鎶ュ憡鏃堕棿|鏌ヨ鏈烘瀯",
                m.group(1),
            )[0].strip()
        idx = compact.find(label)
        if idx == -1:
            return ""
        return compact[idx + len(label): idx + len(label) + max_len]

    company = _extract_company_name_from_lines_utf8(text) or value_after("企业名称") or value_after("浼佷笟鍚嶇О")
    usc_match = re.search(r"[A-Z0-9]{18}", compact)
    query = value_after("查询机构") or value_after("鏌ヨ鏈烘瀯")
    query_name = normalize_institution_name(query)
    report_match = re.search(r"(?:报告时间|报告日期|鎶ュ憡鏃堕棿|鎶ュ憡鏃ユ湡)[:：锛歖]?\s*(\d{4}-\d{2}-\d{2})(?:T\d{2}:\d{2}:\d{2})?", text)

    return ReportMeta(
        query_org=query_name or query,
        report_time=report_match.group(1) if report_match else "",
        customer_name=normalize_company_name(company),
        unified_social_credit_code=usc_match.group(0) if usc_match else "",
    )


def extract_credit_summary(sections: dict[str, Any]) -> CreditSummary:
    text = normalize_text(str(sections.get("credit_summary") or ""))
    summary = CreditSummary()
    patterns = [
        ("中长期借款", "medium_long_term_loan_balance"),
        ("涓暱鏈熷€熸", "medium_long_term_loan_balance"),
        ("短期借款", "short_term_loan_balance"),
        ("鐭湡鍊熸", "short_term_loan_balance"),
        ("循环透支", "revolving_overdraft_balance"),
        ("寰幆閫忔敮", "revolving_overdraft_balance"),
    ]
    count_total = 0
    for label, attr in patterns:
        m = re.search(rf"{re.escape(label)}\s+(\d+)\s+(\d+(?:\.\d+)?)", text)
        if not m:
            continue
        count_total += to_int(m.group(1))
        setattr(summary, attr, to_amount(m.group(2)))
    if count_total:
        summary.unsettled_credit_institution_count = count_total
    m = re.search(r"(?:借贷交易|鍊熻捶浜ゆ槗).*?(?:余额|浣欓)\s*(\d+(?:\.\d+)?)", text, re.S)
    if m:
        summary.unsettled_credit_balance = to_amount(m.group(1))
    m = re.search(r"(?:对外担保|瀵瑰鎷呬繚).*?(?:余额|浣欓)?\s*(\d+(?:\.\d+)?)", text, re.S)
    if m:
        summary.external_guarantee_balance = to_amount(m.group(1))
    return summary


def _extract_revolving_section_text(sections_or_text: dict[str, Any] | str) -> str:
    if isinstance(sections_or_text, dict):
        value = sections_or_text.get("revolving_overdrafts") or sections_or_text.get("revolving_overdraft") or ""
        return str(value or "")
    return str(sections_or_text or "")


def extract_revolving_summary_fallback_detail(
    section_text: str | dict[str, Any],
    balance: float | int | str | None,
) -> list[RevolvingOverdraftRecord]:
    text = normalize_text(_extract_revolving_section_text(section_text))
    if balance is None or not text:
        return []
    compact = compact_text(text)
    if not any(keyword in compact for keyword in ["循环透支", "循环贷款", "循环额度", "透支余额", "寰幆"]):
        return []
    amount = to_amount(str(balance))
    if amount is None or amount <= 0:
        return []
    # Do not emit a summary-balance-only shell detail. It renders as
    # institution=未识别 / guarantee=信用 and hides the real parsing failure.
    # Validators will keep revolving_balance_without_details when no complete
    # detail can be recovered from the revolving section.
    return []
    institution = ""
    inst_match = re.search(INSTITUTION_PATTERN, compact)
    if inst_match:
        institution = normalize_institution_name(inst_match.group(0), text)
    return [
        RevolvingOverdraftRecord(
            institution_name=institution,
            business_type="循环透支",
            used_amount=amount,
            balance=amount,
            evidence_text=text[:1000],
            source_section="revolving_overdraft",
            confidence=0.35,
            warning="summary_balance_fallback_detail",
        )
    ]


def extract_revolving_overdrafts(section_text: str | dict[str, Any]) -> list[RevolvingOverdraftRecord]:
    text = normalize_text(_extract_revolving_section_text(section_text))
    compact = compact_text(text)
    if not compact:
        return []
    pattern = re.compile(
        rf"(?P<institution>{INSTITUTION_PATTERN}|[\u4e00-\u9fa5A-Za-z0-9（）()]+?银行股份有限公司[\u4e00-\u9fa5]{{0,30}}(?:分行|支行)?|[\u4e00-\u9fa5A-Za-z0-9（）()]+?银行股份有限公司)"
        rf"(?P<biz>循环透支|循环贷款|循环额度|循环授信透支|寰幆閫忔敮|流动资金贷款|娴佸姩璧勯噾璐锋|贷款|璐锋)"
        rf"(?P<start>\d{{4}}-\d{{2}}-\d{{2}})"
        rf"(?P<end>\d{{4}}-\d{{2}}-\d{{2}}|长期|闀挎湡)"
        rf"(?:人民币元|浜烘皯甯佸厓)"
        rf"(?P<amount>\d+(?:\.\d+)?)"
        rf"(?:新增|鏂板|无还本续贷|鏃犺繕鏈画璐?|其他|鍏朵粬)?"
        rf".{{0,80}}?"
        rf"(?P<guarantee>信用/无担保|淇＄敤/鏃犳媴淇?|保证/保证金|淇濊瘉/淇濊瘉閲?|保证|淇濊瘉|组合|缁勫悎|抵押|鎶垫娂|质押|璐ㄦ娂|信用|淇＄敤|其他|鍏朵粬)?"
        rf"(?P<balance>\d+(?:\.\d+)?)"
        rf"(?P<five>正常|姝ｅ父|关注|鍏虫敞|次级|娆＄骇|可疑|鍙枒|损失|鎹熷け|违约|杩濈害|未分类|鏈垎绫?)"
        rf"(?P<overdue_total>\d+(?:\.\d+)?)"
        rf"(?P<overdue_principal>\d+(?:\.\d+)?)"
        rf"(?P<overdue_months>\d+)",
        re.S,
    )
    records: list[RevolvingOverdraftRecord] = []
    for match in pattern.finditer(compact):
        raw_evidence = evidence(compact, match.start(), match.end(), 80)
        institution = normalize_institution_name(match.group("institution"), raw_evidence)
        records.append(
            RevolvingOverdraftRecord(
                institution_name=institution,
                business_type="循环透支" if "循环" in match.group("biz") else match.group("biz"),
                credit_amount=to_amount(match.group("amount")),
                used_amount=to_amount(match.group("balance")),
                balance=to_amount(match.group("balance")),
                start_date=format_date(match.group("start")),
                end_date=format_date(match.group("end")),
                currency="人民币",
                guarantee_type=match.group("guarantee") or "",
                five_category=match.group("five"),
                overdue_months=to_int(match.group("overdue_months")),
                evidence_text=raw_evidence,
                source_section="revolving_overdraft",
                confidence=0.78,
            )
        )
    return records


# Final UTF-8 implementation: revolving-overdraft rows often span 2-3 physical
# lines, so parse from a local section window and merge the header/status/tail.
def extract_revolving_overdrafts(section_text: str | dict[str, Any]) -> list[RevolvingOverdraftRecord]:
    text = normalize_text(_extract_revolving_section_text(section_text))
    if not text:
        return []
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    records: list[RevolvingOverdraftRecord] = []
    start_re = re.compile(
        r"(?P<account>[A-Z]\d{5,}[A-Z0-9_-]*)?\s*"
        r"(?P<institution>[\u4e00-\u9fa5A-Za-z0-9（）()]{2,100}?(?:银行股份有限公司|村镇银行股份有限公司|银行)[\u4e00-\u9fa5A-Za-z0-9（）()]{0,40}?(?:分行|支行|营业部)?)\s*"
        r"(?P<biz>循环透支|循环贷款|循环额度|循环授信透支|流动资金贷款|贷款)\s*"
        r"(?P<start>\d{4}-\d{2}-\d{2})\s*"
        r"(?P<end>\d{4}-\d{2}-\d{2}|长期)\s*"
        r"(?P<currency>人民币元|人民币)\s*"
        r"(?P<amount>\d+(?:\.\d+)?)\s*"
        r"(?P<issue>新增|无还本续贷|其他)?"
    )
    status_re = re.compile(
        r"(?P<guarantee>信用/无担保|保证/保证金|保证金|保证|组合|抵押|质押|信用|其他)\s*"
        r"(?P<balance>\d+(?:\.\d+)?)\s*"
        r"(?P<five>正常|关注|次级|可疑|损失|违约|未分类)\s*"
        r"(?P<overdue_total>\d+(?:\.\d+)?)\s*"
        r"(?P<overdue_principal>\d+(?:\.\d+)?)\s*"
        r"(?P<overdue_months>\d+)\s*"
        r"(?P<last_date>\d{4}-\d{2}-\d{2})?"
    )
    tail_re = re.compile(
        r"(?P<last_amount>\d+(?:\.\d+)?)\s*"
        r"(?P<last_type>正常还款|逾期还款|提前还款|--)?\s*"
        r"(?P<remain>\d+)?\s*"
        r"(?P<agreement>[A-Z]\d{5,}[A-Z0-9_-]*)?\s*"
        r"(?P<history>见附件|--)?\s*"
        r"(?P<report>\d{4}-\d{2}-\d{2})?"
    )

    for idx, line in enumerate(lines):
        compact_line = re.sub(r"\s+", "", line)
        first = start_re.search(compact_line)
        if not first:
            continue
        window_lines = lines[idx: idx + 6]
        evidence_text = "\n".join(window_lines)
        status = None
        status_idx = -1
        status_line = ""
        for rel_idx, candidate in enumerate(window_lines[1:], start=1):
            status = status_re.search(re.sub(r"\s+", " ", candidate).strip())
            if status:
                status_idx = rel_idx
                status_line = candidate
                break
        if not status:
            joined = re.sub(r"\s+", "", evidence_text)
            status = status_re.search(joined[first.end():])
        if not status:
            continue
        tail_source = "\n".join(window_lines[status_idx + 1:]) if status_idx != -1 else "\n".join(window_lines[2:])
        tail = tail_re.search(re.sub(r"\s+", " ", tail_source).strip())
        institution = first.group("institution")
        normalized = normalize_institution_name(institution, evidence_text)
        if normalized and len(normalized) > len(institution):
            institution = normalized
        records.append(
            RevolvingOverdraftRecord(
                account_no=first.group("account") or "",
                institution_name=institution,
                business_type=first.group("biz"),
                credit_amount=to_amount(first.group("amount")),
                used_amount=to_amount(status.group("balance")),
                balance=to_amount(status.group("balance")),
                start_date=format_date(first.group("start")),
                end_date=format_date(first.group("end")),
                currency="人民币",
                issue_type=first.group("issue") or "",
                guarantee_type=status.group("guarantee") or "",
                five_category=status.group("five") or "",
                overdue_months=to_int(status.group("overdue_months")),
                last_repayment_date=status.group("last_date") or "",
                last_repayment_amount=to_amount(tail.group("last_amount")) if tail else None,
                last_repayment_type=tail.group("last_type") if tail else "",
                remaining_repayment_months=to_int(tail.group("remain"), default=0) if tail and tail.group("remain") else None,
                credit_agreement_no=tail.group("agreement") if tail else "",
                history_performance=tail.group("history") if tail else "",
                report_date=tail.group("report") if tail else "",
                evidence_text=evidence_text,
                source_section="revolving_overdraft",
                confidence=0.88,
            )
        )

    if records:
        return records

    compact = compact_text(text)
    pattern = re.compile(
        r"(?P<account>[A-Z]\d{5,}[A-Z0-9_-]*)?"
        r"(?P<institution>[\u4e00-\u9fa5A-Za-z0-9（）()]{2,100}?(?:银行股份有限公司|村镇银行股份有限公司|银行)[\u4e00-\u9fa5A-Za-z0-9（）()]{0,40}?(?:分行|支行|营业部)?)"
        r"(?P<biz>循环透支|循环贷款|循环额度|循环授信透支|流动资金贷款|贷款)"
        r"(?P<start>\d{4}-\d{2}-\d{2})(?P<end>\d{4}-\d{2}-\d{2}|长期)"
        r"(?:人民币元|人民币)(?P<amount>\d+(?:\.\d+)?)(?:新增|无还本续贷|其他)?"
        r".{0,120}?"
        r"(?P<guarantee>信用/无担保|保证/保证金|保证金|保证|组合|抵押|质押|信用|其他)"
        r"(?P<balance>\d+(?:\.\d+)?)(?P<five>正常|关注|次级|可疑|损失|违约|未分类)"
        r"(?P<overdue_total>\d+(?:\.\d+)?)(?P<overdue_principal>\d+(?:\.\d+)?)(?P<overdue_months>\d+)"
    )
    for match in pattern.finditer(compact):
        raw = match.group(0)
        institution = normalize_institution_name(match.group("institution"), raw)
        records.append(
            RevolvingOverdraftRecord(
                account_no=match.group("account") or "",
                institution_name=institution,
                business_type=match.group("biz"),
                credit_amount=to_amount(match.group("amount")),
                used_amount=to_amount(match.group("balance")),
                balance=to_amount(match.group("balance")),
                start_date=format_date(match.group("start")),
                end_date=format_date(match.group("end")),
                currency="人民币",
                guarantee_type=match.group("guarantee"),
                five_category=match.group("five"),
                overdue_months=to_int(match.group("overdue_months")),
                evidence_text=raw,
                source_section="revolving_overdraft",
                confidence=0.86,
            )
        )
    return records
