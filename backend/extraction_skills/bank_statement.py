"""Deterministic extraction skill for official bank statement PDFs."""

from __future__ import annotations

import re
import logging
import calendar
import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult

logger = logging.getLogger(__name__)


HEADERS = (
    "凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号",
    "用途", "摘要", "备注", "金额", "交易金额", "发生额", "借方发生额", "贷方发生额", "收入", "支出", "余额", "回单个性化信息",
)
BANK_FORMAT_ICBC = "icbc"
BANK_FORMAT_SHANGHAI = "shanghai_bank"
BANK_FORMAT_BOCM = "bocm_statement"
BANK_FORMAT_GENERIC = "generic_bank_statement"
DATE_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})[年/.-]?(\d{1,2})[月/.-]?(\d{1,2})(?:日)?(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?")
PERIOD_RE = re.compile(r"(?<!\d)((?:19|20)\d{6})\s*(?:-|—|~|至|到)\s*((?:19|20)\d{6})(?!\d)")
LABELED_PERIOD_RE = re.compile(r"时间范围\s*[:：]\s*((?:19|20)\d{6})\s*[-－—]\s*((?:19|20)\d{6})")
ACCOUNT_NO_RE = re.compile(r"(?:本方)?账号\s*[:：]\s*([0-9]{8,32})")
TRANSACTION_ANCHOR_RE = re.compile(
    r"(?P<voucher_no>\d{6,})\s+"
    r"(?P<counterparty_account>\d{5,32})\s+"
    r"(?P<trade_date>(?:19|20)\d{2}-\d{2}-\d{2})\s+"
    r"(?P<trade_time>\d{2}:\d{2}:\d{2})"
    r"(?P<rest>.*?)"
    r"(?=(?:\d{6,}\s+\d{5,32}\s+(?:19|20)\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})|\Z)",
    re.S,
)
AMOUNT_LABEL_RE = re.compile(
    r"(?P<label>实收金额|应收金额|交易金额|发生额|借方发生额|贷方发生额|收入|支出|贷款金额|归还金额|还款金额|本金|利息|金额)\s*[:：]?\s*"
    r"(?P<amount>[+-]?(?:人民币|￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?)"
)

OPERATING_KEYWORDS = ("货款", "工程款", "工程款安装", "项目款", "材料款", "劳务费", "劳务款", "电缆款", "桥架款", "灯具款", "风管", "风管材料款", "防火包裹材料款", "扶持资金", "房租", "服务费", "咨询费", "快递费", "水费", "餐费", "报销")
CURRENT_ACCOUNT_KEYWORDS = ("往来款", "转账", "普通汇兑", "汇兑业务")
BANK_FEE_KEYWORDS = ("手续费", "年费", "工本费", "协议费", "半年费", "跨行快汇", "企业短信业务服务费", "账户管理费", "短信费", "网银费", "ETC业务扣款")
LOAN_DISBURSEMENT_KEYWORDS = ("贷款发放", "对公贷款记账")
LOAN_REPAYMENT_KEYWORDS = ("贷款归还", "对公贷款批量正常分期", "还款", "还贷款及利息", "融资还款", "担保费", "融资租赁")
INTEREST_EXPENSE_KEYWORDS = ("对公贷款利息支付", "利息支出", "息余积数")
SENSITIVE_DISPLAY_MARKERS = (
    "指令编号", "支付交易序号", "报文种类", "提交人", "起息日期", "止息日期", "止息日",
    "利息期间", "贷款账号", "贷款帐号", "借据编号", "HQP928", "w191001",
)
GARBAGE_COUNTERPARTY_MARKERS = (
    "HQP928", "w191001", "期:", "期：", "起息日期", "止息日", "支付交易序号", "指令编号", "提交人",
    "若与实际交易不符", "文件下载后", "重要提示",
)
GENERIC_TABLE_HEADER_KEYWORDS = (
    "交易日期", "交易时间", "记账日期", "发生日期", "入账日期", "对方户名", "对手名称",
    "对方名称", "收款方", "付款方", "交易对方", "摘要", "用途", "借方发生额",
    "贷方发生额", "交易金额", "发生额", "余额",
)
BOCM_TABLE_HEADER_KEYWORDS = (
    "交通银行上海市分行明细对账单", "明细对账单", "开户机构", "账号", "户名", "会计日期",
    "交易日期", "交易名称", "借方发生额", "贷方发生额", "余额", "对方账号", "对方户名",
    "对方行名", "摘要", "流水号",
)
BOCM_HEADER_LABELS = ("开户机构", "账号", "币种", "年份", "月份", "页码", "户名")
BOCM_HEADER_BOUNDARIES = BOCM_HEADER_LABELS + ("序号", "会计日期", "交易日期", "交易名称")
BOCM_INVALID_HEADER_VALUES = {
    "", "页码", "页码：", "年份", "年份：", "月份", "月份：", "币种", "币种：",
    "账号", "账号：", "开户机构", "开户机构：", "户名", "户名：", "序号", "会计日期", "交易日期",
}
GENERIC_BANK_NAMES = (
    "中国工商银行", "上海银行", "中国建设银行", "中国农业银行", "中国银行", "招商银行", "交通银行",
    "浦发银行", "中信银行", "民生银行", "平安银行", "兴业银行", "光大银行", "广发银行",
    "江苏银行", "南京银行", "浙商银行", "泰隆银行", "温州银行", "宁波银行",
)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" |\t\r\n")


def _normalize_company_spacing(value: Any) -> str:
    text = re.sub(r"[\s\u3000]+", "", str(value or "")).strip("，,。；;：:")
    text = text.replace("有限 公司", "有限公司").replace("有限责任 公司", "有限责任公司")
    return text


def clean_account_name(value: Any) -> str:
    text = _normalize_company_spacing(value)
    text = re.split(r"(?:借方总金额|贷方总金额|总笔数)\s*[:：]?", text, maxsplit=1)[0]
    return text.strip("，,。；;：:")


def validate_account_name(name: Any) -> tuple[bool, str]:
    text = _clean(str(name or ""))
    compact = re.sub(r"[\s\u3000]+", "", text)
    if not compact:
        return False, "empty"
    if len(compact) > 80:
        return False, "too_long"
    if len(re.findall(r"有限公司|公司", compact)) >= 2:
        return False, "multiple_counterparty_names"
    forbidden = ("对手名称", "对方户名", "对方名称", "收款方", "付款方", "交易用途", "摘要", "余额", "单位国内汇款", "交易对方")
    if any(marker in compact for marker in forbidden):
        return False, "contains_transaction_table_field"
    bocm_counterparty_fragments = (
        "百威（中国）销", "百威(中国)销", "上海汇付支付有", "上海顺衡物流有", "上海基连网络科",
        "支付宝（中国）", "支付宝(中国)", "华润守正招标有",
    )
    if any(fragment in compact for fragment in bocm_counterparty_fragments):
        return False, "counterparty_name_pollution"
    return True, ""


def _generic_header_account_name(header_text: str) -> tuple[str, list[dict[str, str]], list[dict[str, str]]]:
    candidates: list[dict[str, str]] = []
    rejected: list[dict[str, str]] = []
    pattern = r"(?:户名|账户名称|客户名称|单位名称|存款人名称|账户户名|本方户名)\s*[:：\s]*([^\n\r|]{2,120})"
    for match in re.finditer(pattern, header_text):
        value = _clean(match.group(1))
        ok, reason = validate_account_name(value)
        item = {"value": value, "source": "header"}
        if ok:
            candidates.append(item)
        else:
            rejected.append({**item, "reason": reason})
    return (candidates[0]["value"] if candidates else ""), candidates, rejected


def _generic_header_bank_name(header_text: str) -> str:
    return next((name for name in GENERIC_BANK_NAMES if name in str(header_text or "")), "")


def detect_statement_subtype(bank_format: str, source: str) -> str:
    text = str(source or "")
    if bank_format == BANK_FORMAT_BOCM:
        return "account_statement"
    if bank_format == BANK_FORMAT_ICBC and "中国工商银行账户明细清单" in text:
        return "account_statement"
    if bank_format == BANK_FORMAT_ICBC and any(marker in text for marker in ("单位国内汇款", "电子回单", "回单编号", "回单批量", "网银回单")):
        return "receipt_bundle"
    if any(marker in text for marker in ("账户明细查询", "账户明细清单", "交易流水号")):
        return "account_statement"
    return "unknown_bank_statement"


def clean_opening_bank(value: Any, *, account_no: str = "", account_name: str = "") -> str:
    text = re.sub(r"[\s\u3000\t\r\n]+", "", str(value or "")).strip("，,。；;：:")
    if not text:
        return ""
    branch_match = re.search(r"(上海银行[\u4e00-\u9fffA-Za-z0-9（）()]*?(?:支行|营业部|分行|网点)(?:营业部)?)", text)
    if branch_match:
        text = branch_match.group(1)
    if account_no:
        text = text.replace(str(account_no), "")
    if "开户行" in text:
        text = re.split(r"开户行[:：]?", text, maxsplit=1)[-1]
    for marker in ("币种", "人民币", "美元", "欧元", "港币", "日元", "选择账号", "借方总金额", "贷方总金额", "总笔数"):
        if marker in text:
            text = text.split(marker, 1)[0]
    clean_name = clean_account_name(account_name)
    if clean_name and clean_name in text:
        text = text.split(clean_name, 1)[0]
    return text.strip("，,。；;：:")


def _date(value: str) -> str:
    match = DATE_RE.search(str(value or ""))
    if not match:
        return ""
    base = f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return f"{base} {match.group(4)}" if match.group(4) else base


def _decimal(value: Any) -> Decimal | None:
    cleaned = re.sub(r"[^\d.\-]", "", str(value or "").replace(",", ""))
    if not cleaned or cleaned in {"-", "."}:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _money(value: Decimal | None) -> str:
    return f"{value:,.2f}" if value is not None else "未识别"


def _find_labeled(source: str, labels: Iterable[str], stop: Iterable[str] = ()) -> str:
    label_expr = "|".join(map(re.escape, labels))
    stop_expr = "|".join(map(re.escape, stop))
    suffix = rf"(?=\s*(?:{stop_expr})\s*[:：]?|$)" if stop_expr else r"(?=\n|$)"
    match = re.search(rf"(?:{label_expr})\s*[:：]?\s*(.{{1,80}}?){suffix}", source, re.I)
    return _clean(match.group(1)) if match else ""


def _load_native_pdf_pages(file_path: str) -> list[dict[str, Any]]:
    """Recover native page text when the upload adapter did not pass raw_pages."""
    path = Path(str(file_path or ""))
    if not path.is_file() or path.suffix.lower() != ".pdf":
        return []
    try:
        import fitz  # type: ignore

        pages: list[dict[str, Any]] = []
        with fitz.open(str(path)) as document:
            for page_no, page in enumerate(document, start=1):
                pages.append({"page": page_no, "text": page.get_text("text") or "", "source": "bank_statement_skill_pdf_native"})
        return pages
    except Exception as exc:  # pragma: no cover - upload raw_pages remains primary
        logger.warning("[BankStatementSkill] native_pdf_recovery_failed file=%s error=%s", path.name, exc)
        return []


def detect_bank_format(raw_text: str, ocr_text: str = "") -> str:
    raw_source = f"{raw_text}\n{ocr_text}"
    source = raw_source.lower()
    header_source = raw_source[:5000]
    if (
        "交通银行上海市分行明细对账单" in header_source
        or (
            "明细对账单" in header_source
            and "开户机构" in header_source
            and "会计日期" in raw_source
            and "借方发生额" in raw_source
            and "贷方发生额" in raw_source
            and "对方户名" in raw_source
        )
    ):
        counterparty_banks = sorted({name for name in ("中国工商银行", "中国建设银行", "中国农业银行", "华夏银行", "招商银行", "中国银行") if name in raw_source})
        logger.info("[BankFormatDetector] title_bank_name=交通银行")
        logger.info("[BankFormatDetector] counterparty_bank_names_ignored=%s", counterparty_banks)
        logger.info("[BankFormatDetector] final_bank_name=交通银行")
        return BANK_FORMAT_BOCM
    if any(keyword.lower() in source for keyword in ("中国工商银行账户明细清单", "中国工商银行", "工商银行", "工行")):
        return BANK_FORMAT_ICBC
    if any(keyword.lower() in source for keyword in ("上海银行股份有限公司", "上海银行对账单", "上海银行账户明细", "上海银行交易明细", "上海银行", "shanghai bank")):
        return BANK_FORMAT_SHANGHAI
    return BANK_FORMAT_GENERIC


def parse_valid_date(value: Any) -> date | None:
    text = str(value or "").strip()
    match = re.fullmatch(r"((?:19|20)\d{2})[年./-]?(\d{1,2})[月./-]?(\d{1,2})日?", text)
    if not match:
        return None
    try:
        parsed = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None
    if parsed < date(2000, 1, 1) or parsed.year > date.today().year + 1:
        return None
    return parsed


def _period_range_candidates(text: str) -> list[tuple[date, date, str]]:
    candidates: list[tuple[date, date, str]] = []
    date_token = r"((?:20)\d{2}(?:年|[./-])?\d{1,2}(?:月|[./-])?\d{1,2}日?)"
    patterns = (
        rf"(?:时间范围|对账期间|查询日期|起止日期|记账日期)\s*[:：]?\s*{date_token}\s*(?:至|到|[-－—~]{{1,3}})\s*{date_token}",
        rf"起始日期\s*[:：]?\s*{date_token}.*?结束日期\s*[:：]?\s*{date_token}",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.S | re.I):
            start = parse_valid_date(match.group(1))
            end = parse_valid_date(match.group(2))
            if start and end and start <= end:
                candidates.append((start, end, _clean(match.group(0))))
    return candidates


def _filename_period(filename: str) -> tuple[date, date] | None:
    match = re.search(r"(?<!\d)((?:20)\d{4})[-_至~]*((?:20)\d{4})(?!\d)", str(filename or ""))
    if not match:
        return None
    start_month, end_month = match.groups()
    start_year, start_mon = int(start_month[:4]), int(start_month[4:])
    end_year, end_mon = int(end_month[:4]), int(end_month[4:])
    if not (1 <= start_mon <= 12 and 1 <= end_mon <= 12):
        return None
    start = date(start_year, start_mon, 1)
    end = date(end_year, end_mon, calendar.monthrange(end_year, end_mon)[1])
    return (start, end) if start <= end else None


def _periods(raw_pages: list[dict[str, Any]], text: str, filename: str = "") -> tuple[list[dict[str, Any]], str, str]:
    evidence: list[dict[str, Any]] = []
    values: list[tuple[date, date, int, str]] = []
    pages = raw_pages or [{"page": 1, "text": text}]
    for item in pages:
        page_text = str(item.get("text") or "")
        page_no = int(item.get("page") or 0)
        for start, end, raw_value in _period_range_candidates(page_text[:4000]):
            values.append((start, end, page_no, raw_value))
            evidence.append({
                "field": "时间范围", "page": page_no, "source": "header",
                "raw_value": raw_value, "value": f"{start.isoformat()} 至 {end.isoformat()}",
            })
    if values:
        start = min(item[0] for item in values)
        end = max(item[1] for item in values)
        return evidence, start.isoformat(), end.isoformat()
    filename_period = _filename_period(filename)
    if filename_period:
        start, end = filename_period
        evidence.append({"field": "时间范围", "page": 0, "source": "filename", "raw_value": filename, "value": f"{start.isoformat()} 至 {end.isoformat()}"})
        return evidence, start.isoformat(), end.isoformat()
    return evidence, "", ""


def _account_header_text(raw_pages: list[dict[str, Any]], source: str) -> str:
    first_pages = sorted((item for item in raw_pages if isinstance(item, dict)), key=lambda item: int(item.get("page") or 0))[:2]
    if first_pages and first_pages[0].get("text_boxes"):
        first_page = first_pages[0]
        page_height = float(first_page.get("page_height") or max((float(box.get("y1") or 0) for box in first_page.get("text_boxes") or []), default=1))
        for ratio in (0.25, 0.40):
            selected = [box for box in first_page.get("text_boxes") or [] if float(box.get("y1") or 0) <= page_height * ratio]
            if selected:
                synthetic_page = {**first_page, "text_boxes": selected}
                _boxes, lines = _page_lines_from_boxes([synthetic_page])
                header_line_index = next((index for index, line in enumerate(lines) if len(_header_matches(line)) >= 3), None)
                if header_line_index is not None:
                    lines = lines[:header_line_index]
                header_text = "\n".join(line["text"] for line in lines)
                if ratio == 0.40 or re.search(r"(?:账号|账户号|客户名称|户名|开户行|开户网点)\s*[:：]", header_text):
                    return header_text
    text = "\n".join(str(item.get("text") or "")[:5000] for item in first_pages) or str(source or "")[:8000]
    return _text_before_generic_table_header(text)


def _text_before_generic_table_header(text: str) -> str:
    positions = [match.start() for keyword in GENERIC_TABLE_HEADER_KEYWORDS for match in re.finditer(re.escape(keyword), str(text or ""))]
    if positions:
        return str(text or "")[:min(positions)]
    table_header = re.search(r"(?:交易日期|记账日期|交易时间).{0,120}(?:摘要|借方发生额|贷方发生额|收入|支出)", str(text or ""), re.S)
    return str(text or "")[:table_header.start()] if table_header else str(text or "")


def _bocm_header_text(raw_pages: list[dict[str, Any]], source: str) -> str:
    text = "\n".join(str(item.get("text") or "")[:6000] for item in raw_pages[:2]) or str(source or "")[:10000]
    positions = [
        text.find(keyword)
        for keyword in ("序号", "会计日期", "借方发生额", "贷方发生额", "对方账号", "对方户名", "对方行名", "摘要", "流水号")
        if text.find(keyword) >= 0
    ]
    return text[:min(positions)] if positions else text[:4000]


def _month_end(year: int, month: int) -> str:
    last = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-{last:02d}"


def clean_bocm_header_field(value: Any, *, field: str = "") -> str:
    text = _normalize_company_spacing(_clean(value).replace("\u3000", " "))
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"^[：:]+", "", text).strip()
    for label in BOCM_HEADER_LABELS:
        text = re.sub(rf"^{re.escape(label)}[：:]?", "", text)
    if text in BOCM_INVALID_HEADER_VALUES:
        return ""
    if any(keyword in text for keyword in ("序号", "会计日期", "交易日期", "交易名称")):
        text = re.split(r"序号|会计日期|交易日期|交易名称", text, maxsplit=1)[0]
    if field == "account_name":
        if text in BOCM_INVALID_HEADER_VALUES or re.fullmatch(r"\d+-\d+", text or ""):
            return ""
        if any(keyword in text for keyword in ("页码", "年份", "月份", "币种", "开户机构", "账号：", "户名：")):
            logger.info("[BOCMHeaderParser] rejected_account_name=%s", text)
            logger.info("[BOCMHeaderParser] reject_reason=label_value_pollution")
            return ""
    if field == "opening_bank":
        text = re.split(r"账号|户名|币种|年份|月份|页码", text, maxsplit=1)[0]
    if field == "currency":
        match = re.search(r"人民币|美元|欧元|港币|日元", text)
        return match.group(0) if match else ""
    if field == "year":
        match = re.search(r"20\d{2}", text)
        return match.group(0) if match else ""
    if field == "month":
        match = re.search(r"(?<!\d)(0?[1-9]|1[0-2])(?!\d)", text)
        return f"{int(match.group(1)):02d}" if match else ""
    if field == "account_no":
        match = re.search(r"(?<!\d)(\d{8,32})(?!\d)", text)
        return match.group(1) if match else ""
    return text


def _bocm_labeled_value(header: str, label: str, *, field: str = "") -> str:
    boundary = "|".join(re.escape(item) for item in BOCM_HEADER_BOUNDARIES if item != label)
    match = re.search(rf"{re.escape(label)}\s*[:：]?\s*(.*?)(?=\s*(?:{boundary})\s*[:：]?|\Z)", header, re.S)
    if not match:
        return ""
    return clean_bocm_header_field(match.group(1), field=field)


def _bocm_parse_label_value_lines(header: str) -> dict[str, str]:
    values: dict[str, str] = {}
    lines = [line.strip() for line in str(header or "").splitlines() if line.strip()]
    pending: list[str] = []
    label_expr = "|".join(re.escape(label) for label in BOCM_HEADER_LABELS)
    for raw_line in lines:
        line = _clean(raw_line)
        if "交通银行上海市分行明细对账单" in line:
            continue
        labels = [match.group(1) for match in re.finditer(rf"({label_expr})\s*[:：]?", line)]
        if labels:
            for index, label in enumerate(labels):
                next_label = labels[index + 1] if index + 1 < len(labels) else ""
                pattern = rf"{re.escape(label)}\s*[:：]?\s*(.*?)(?=\s*{re.escape(next_label)}\s*[:：]?|\Z)" if next_label else rf"{re.escape(label)}\s*[:：]?\s*(.*)"
                value_match = re.search(pattern, line)
                value = clean_bocm_header_field(value_match.group(1) if value_match else "", field=_bocm_field_for_label(label))
                if value:
                    values[_bocm_field_for_label(label)] = value
                else:
                    pending.append(label)
            continue
        if pending:
            tokens = [_clean(token) for token in re.split(r"\s{2,}|\t+| {1,}", line) if _clean(token)]
            if len(tokens) < len(pending):
                tokens = re.findall(r"交通银行[^ ]+支行|人民币|20\d{2}|0?[1-9]|1[0-2]|\d+-\d+|\d{8,32}|[\u4e00-\u9fff（）()A-Za-z]+有限公司", line)
            for label, token in zip(pending, tokens):
                field = _bocm_field_for_label(label)
                value = clean_bocm_header_field(token, field=field)
                if value:
                    values[field] = value
            pending = pending[len(tokens):] if len(tokens) < len(pending) else []
    return values


def _bocm_field_for_label(label: str) -> str:
    return {
        "开户机构": "opening_bank",
        "账号": "account_no",
        "币种": "currency",
        "年份": "year",
        "月份": "month",
        "页码": "page_no",
        "户名": "account_name",
    }.get(label, label)


def parse_bocm_header_by_layout(raw_pages: list[dict[str, Any]]) -> dict[str, str]:
    first_page = next((item for item in sorted((page for page in raw_pages if isinstance(page, dict)), key=lambda item: int(item.get("page") or 0)) if item.get("text_boxes")), None)
    if not first_page:
        return {}
    boxes = []
    raw_boxes = first_page.get("text_boxes") or []
    page_height = float(first_page.get("page_height") or max((float(box.get("y1") or 0) for box in raw_boxes), default=1))
    for box in raw_boxes:
        text = _clean(box.get("text"))
        if not text:
            continue
        y1 = float(box.get("y1") or 0)
        if y1 > page_height * 0.30:
            continue
        x0 = float(box.get("x0") or 0)
        x1 = float(box.get("x1") or x0)
        y0 = float(box.get("y0") or y1)
        boxes.append({**box, "text": text, "x0": x0, "x1": x1, "y0": y0, "y1": y1, "yc": (y0 + y1) / 2})
    if not boxes:
        return {}
    values: dict[str, str] = {}
    for label in BOCM_HEADER_LABELS:
        label_box = next((
            box for box in boxes
            if label in str(box.get("text") or "")
            and not any(marker in str(box.get("text") or "") for marker in ("对方", "收款", "付款", "交易"))
        ), None)
        if not label_box:
            continue
        inline_match = re.search(rf"{re.escape(label)}\s*[:：]\s*(.+)", str(label_box.get("text") or ""))
        if inline_match:
            field = _bocm_field_for_label(label)
            inline_value = clean_bocm_header_field(inline_match.group(1), field=field)
            if inline_value:
                values[field] = inline_value
                continue
        same_line_labels = [
            box for box in boxes
            if box is not label_box and any(other in str(box.get("text") or "") for other in BOCM_HEADER_LABELS)
            and not any(marker in str(box.get("text") or "") for marker in ("对方", "收款", "付款", "交易"))
            and abs(float(box.get("yc") or 0) - float(label_box.get("yc") or 0)) <= 10
            and float(box.get("x0") or 0) > float(label_box.get("x0") or 0)
        ]
        right_limit = min((float(box.get("x0") or 0) for box in same_line_labels), default=float("inf"))
        candidates = [
            box for box in boxes
            if box is not label_box
            and float(box.get("x0") or 0) >= float(label_box.get("x1") or 0) - 2
            and float(box.get("x0") or 0) < right_limit
            and abs(float(box.get("yc") or 0) - float(label_box.get("yc") or 0)) <= 10
            and not any(other in str(box.get("text") or "") for other in BOCM_HEADER_LABELS)
        ]
        if not candidates:
            candidates = [
                box for box in boxes
                if box is not label_box
                and float(box.get("y0") or 0) >= float(label_box.get("y1") or 0) - 2
                and float(box.get("y0") or 0) <= float(label_box.get("y1") or 0) + 45
                and abs(float(box.get("x0") or 0) - float(label_box.get("x0") or 0)) <= 80
                and not any(other in str(box.get("text") or "") for other in BOCM_HEADER_LABELS)
            ]
        if candidates:
            candidates.sort(key=lambda box: (abs(float(box.get("yc") or 0) - float(label_box.get("yc") or 0)), float(box.get("x0") or 0)))
            field = _bocm_field_for_label(label)
            value = clean_bocm_header_field(candidates[0].get("text"), field=field)
            if value:
                values[field] = value
    return values


def parse_bocm_statement_header(raw_pages: list[dict[str, Any]], source: str) -> dict[str, Any]:
    header = _bocm_header_text(raw_pages, source)
    full = "\n".join(str(item.get("text") or "")[:8000] for item in raw_pages[:2]) or str(source or "")[:12000]
    title = "交通银行上海市分行明细对账单" if "交通银行上海市分行明细对账单" in full else ("明细对账单" if "明细对账单" in full else "")
    layout_values = parse_bocm_header_by_layout(raw_pages)
    line_values = _bocm_parse_label_value_lines(header)
    opening_bank = layout_values.get("opening_bank") or line_values.get("opening_bank") or _bocm_labeled_value(header, "开户机构", field="opening_bank")
    account_no = layout_values.get("account_no") or line_values.get("account_no") or _bocm_labeled_value(header, "账号", field="account_no")
    account_name_raw = layout_values.get("account_name") or line_values.get("account_name") or _bocm_labeled_value(header, "户名", field="account_name")
    account_name = clean_bocm_header_field(account_name_raw, field="account_name")
    year_text = layout_values.get("year") or line_values.get("year") or _bocm_labeled_value(header, "年份", field="year")
    month_text = layout_values.get("month") or line_values.get("month") or _bocm_labeled_value(header, "月份", field="month")
    currency = layout_values.get("currency") or line_values.get("currency") or _bocm_labeled_value(header, "币种", field="currency") or ("人民币" if "人民币" in header else "")
    page_no = layout_values.get("page_no") or line_values.get("page_no") or _bocm_labeled_value(header, "页码", field="page_no")
    year_match = re.search(r"(20\d{2})", year_text or header)
    month_match = re.search(r"(?<!\d)(0?[1-9]|1[0-2])(?!\d)", month_text)
    period_start = ""
    period_end = ""
    if year_match and month_match:
        year = int(year_match.group(1))
        month = int(month_match.group(1))
        if 1 <= month <= 12:
            period_start = f"{year:04d}-{month:02d}-01"
            period_end = _month_end(year, month)
    cutoff = re.search(r"出单截至日期\s*[:：]?\s*((?:20\d{2})[-/.年]?\d{1,2}[-/.月]?\d{1,2})", full)
    if cutoff:
        cutoff_date = _date(cutoff.group(1))
        if cutoff_date:
            period_end = cutoff_date
    result = {
        "title": title,
        "bank_name": "交通银行" if title or "交通银行" in header[:500] else "",
        "opening_bank": clean_bocm_header_field(opening_bank, field="opening_bank"),
        "account_no": account_no,
        "account_name": account_name,
        "currency": currency,
        "year": year_match.group(1) if year_match else "",
        "month": f"{int(month_match.group(1)):02d}" if month_match else "",
        "page_no": page_no,
        "period_start": period_start,
        "period_end": period_end,
        "header_preview": _clean(header[:1000]),
    }
    logger.info("[BOCMHeaderParser] activated=true")
    logger.info("[BOCMHeaderParser] header_text_preview=%s", _clean(header[:1000]))
    logger.info("[BOCMHeaderParser] layout_enabled=%s", str(bool(layout_values)).lower())
    logger.info(
        "[BOCMHeaderParser] label_value_pairs=%s",
        {
            "开户机构": result["opening_bank"],
            "账号": result["account_no"],
            "币种": result["currency"],
            "年份": result["year"],
            "月份": result["month"],
            "页码": result["page_no"],
            "户名": result["account_name"],
        },
    )
    logger.info("[BOCMHeaderParser] title=%s", result["title"] or "未识别")
    logger.info("[BOCMHeaderParser] opening_bank_raw=%s", opening_bank or "未识别")
    logger.info("[BOCMHeaderParser] opening_bank_clean=%s", result["opening_bank"] or "未识别")
    logger.info("[BOCMHeaderParser] account_no=%s", result["account_no"] or "未识别")
    logger.info("[BOCMHeaderParser] account_name_raw=%s", account_name_raw or "未识别")
    logger.info("[BOCMHeaderParser] account_name_clean=%s", result["account_name"] or "未识别")
    logger.info("[BOCMHeaderParser] currency=%s", result["currency"] or "未识别")
    logger.info("[BOCMHeaderParser] year=%s", result["year"] or "未识别")
    logger.info("[BOCMHeaderParser] month=%s", result["month"] or "未识别")
    logger.info("[BOCMHeaderParser] page_no=%s", result["page_no"] or "未识别")
    return result


def _header_labeled_value(header: str, labels: tuple[str, ...]) -> str:
    expression = "|".join(re.escape(label) for label in sorted(labels, key=len, reverse=True))
    match = re.search(rf"(?:{expression})\s*[:：]\s*([^\n\r|]{{1,100}})", header, re.I)
    if match:
        value = match.group(1)
    else:
        loose = re.search(rf"(?:{expression})\s*[:：]?\s+(?![:：])([^\n\r|]{{1,100}})", header, re.I)
        if not loose:
            return ""
        value = loose.group(1)
    value = re.split(r"(?=(?:本方账号开户行|开户行名称|开户机构|开户网点|开户银行|开户行|本方账号户名|账号户名|账户名称|客户名称|单位名称|存款人名称|企业名称|户名|币种|单位|时间范围|对账期间)\s*[:：])", value, maxsplit=1)[0]
    return _clean(value)


def _extract_account_info(raw_pages: list[dict[str, Any]], source: str, bank_format: str) -> tuple[dict[str, str], list[dict[str, Any]]]:
    header = _account_header_text(raw_pages, source)
    shanghai_native = _parse_shanghai_native_header(raw_pages, source) if bank_format == BANK_FORMAT_SHANGHAI else {}
    bocm_header = parse_bocm_statement_header(raw_pages, source) if bank_format == BANK_FORMAT_BOCM else {}
    account_labels = ("本方账号", "银行账号", "对账账号", "结算账号", "户名账号", "账号/卡号", "账户号", "账号", "账户")
    name_labels = ("本方账号户名", "账号户名", "账户名称", "客户名称", "单位名称", "存款人名称", "企业名称", "户名")
    branch_labels = ("本方账号开户行", "开户行名称", "开户机构", "开户网点", "开户银行", "开户行")
    account_value = _header_labeled_value(header, account_labels)
    account_match = re.search(r"(?<!\d)(\d{8,32})(?!\d)", account_value)
    account_no = str(bocm_header.get("account_no") or shanghai_native.get("account_no") or (account_match.group(1) if account_match else ""))
    if bank_format == BANK_FORMAT_GENERIC:
        generic_account_name, account_name_candidates, rejected_account_name_candidates = _generic_header_account_name(header)
        account_name = generic_account_name
        logger.info("[GenericBankStatementParser] header_text_preview=%s", _clean(header[:1000]))
        source_for_pos = "\n".join(str(item.get("text") or "")[:5000] for item in raw_pages[:2]) or str(source or "")[:8000]
        table_positions = [source_for_pos.find(keyword) for keyword in GENERIC_TABLE_HEADER_KEYWORDS if source_for_pos.find(keyword) >= 0]
        logger.info("[GenericBankStatementParser] table_header_pos=%s", min(table_positions) if table_positions else -1)
        logger.info("[GenericBankStatementParser] account_name_candidates=%s", account_name_candidates)
        logger.info("[GenericBankStatementParser] rejected_account_name_candidates=%s", rejected_account_name_candidates)
        logger.info("[GenericBankStatementParser] reject_reason=%s", rejected_account_name_candidates[0]["reason"] if rejected_account_name_candidates else "")
        logger.info("[GenericBankStatementParser] final_account_name=%s", account_name or "未识别")
    elif bank_format == BANK_FORMAT_BOCM:
        account_name = str(bocm_header.get("account_name") or "")
    else:
        account_name = str(shanghai_native.get("account_name") or _header_labeled_value(header, name_labels))
    opening_bank = str(bocm_header.get("opening_bank") or shanghai_native.get("opening_bank") or _header_labeled_value(header, branch_labels))
    evidence: list[dict[str, Any]] = []
    for field, value in (("account_no", account_no), ("account_name", account_name), ("opening_bank", opening_bank)):
        if value:
            evidence.append({"field": field, "page": 1, "source": "header", "value": value})
    return {
        "account_no": account_no,
        "account_name": account_name,
        "opening_bank": opening_bank,
        "header_preview": _clean((bocm_header.get("header_preview") or header)[:1000]),
        "bank_format": bank_format,
    }, evidence


def _parse_shanghai_native_header(raw_pages: list[dict[str, Any]], source: str = "") -> dict[str, Any]:
    """Parse Shanghai Bank native text header/totals before any OCR/table fallback."""
    first_pages = sorted((item for item in raw_pages if isinstance(item, dict)), key=lambda item: int(item.get("page") or 0))[:2]
    text = str(source or "")[:10000] or "\n".join(str(item.get("text") or "")[:6000] for item in first_pages)
    if first_pages:
        text = f"{text}\n" + "\n".join(str(item.get("text") or "")[:6000] for item in first_pages)
    compact = _clean(text.replace("\u3000", " "))
    result: dict[str, Any] = {}
    header_match = re.search(
        r"选择账号\s*[:：]\s*(?P<account_no>\d{5,32})\s+"
        r"开户行\s*[:：]\s*(?P<opening_bank>.*?)\s+"
        r"币种\s*[:：]\s*(?P<currency>人民币|美元|欧元|港币|日元)\s+"
        r"(?P<account_name>.*?)(?=\s*借方总金额\s*[:：])",
        compact,
        re.S,
    )
    if header_match:
        result["account_no"] = header_match.group("account_no")
        opening_bank_raw = _clean(header_match.group("opening_bank"))
        account_name_clean = clean_account_name(header_match.group("account_name"))
        result["opening_bank"] = clean_opening_bank(opening_bank_raw, account_no=result["account_no"], account_name=account_name_clean)
        result["currency"] = header_match.group("currency")
        result["account_name"] = account_name_clean
        logger.info("[ShanghaiBankHeaderParser] raw_header_line=%s", _clean(header_match.group(0)))
        logger.info("[ShanghaiBankHeaderParser] account_no=%s", result["account_no"])
        logger.info("[ShanghaiBankHeaderParser] opening_bank_raw=%s", opening_bank_raw)
        logger.info("[ShanghaiBankHeaderParser] opening_bank_clean=%s", result["opening_bank"])
        logger.info("[ShanghaiBankHeaderParser] currency=%s", result["currency"])
        logger.info("[ShanghaiBankHeaderParser] account_name=%s", result["account_name"])
    account_match = re.search(r"选择账号\s*[:：]\s*([0-9]{5,32})", compact)
    if account_match:
        result.setdefault("account_no", account_match.group(1))
    branch_match = re.search(r"开户行\s*[:：]\s*(.+?)(?=\s*(?:币种|客户名称|借方总金额|总笔数)\s*[:：]|$)", compact)
    if branch_match and "opening_bank" not in result:
        result["opening_bank"] = clean_opening_bank(branch_match.group(1), account_no=str(result.get("account_no") or ""), account_name=str(result.get("account_name") or ""))
    currency_match = re.search(r"币种\s*[:：]\s*(人民币|美元|欧元|港币|日元)", compact)
    if currency_match and "currency" not in result:
        result["currency"] = currency_match.group(1)
    name_match = re.search(r"币种\s*[:：]\s*(?:人民币|美元|欧元|港币|日元)\s*(?P<name>[\u4e00-\u9fff\s（）()]{4,100}?)(?=\s*借方总金额\s*[:：])", compact)
    if name_match and "account_name" not in result:
        result["account_name"] = clean_account_name(name_match.group("name"))
    period_match = re.search(r"记账日期\s*[:：]\s*(\d{4}-\d{2}-\d{2})\s*[-—－]{2,3}\s*(\d{4}-\d{2}-\d{2})", compact)
    if period_match:
        result["period_start"], result["period_end"] = period_match.groups()
    summary_patterns = {
        "debit_total_amount": r"借方总金额\s*[:：]\s*([\d,]+\.\d{2})",
        "credit_total_amount": r"贷方总金额\s*[:：]\s*([\d,]+\.\d{2})",
        "transaction_count": r"总笔数\s*[:：]\s*(\d+)",
        "debit_count": r"借方总笔数\s*[:：]\s*(\d+)",
        "credit_count": r"贷方总笔数\s*[:：]\s*(\d+)",
    }
    for key, pattern in summary_patterns.items():
        match = re.search(pattern, compact)
        if not match:
            continue
        result[key] = int(match.group(1)) if key.endswith("_count") or key == "transaction_count" else _decimal(match.group(1))
    result["header_preview"] = compact[:1000]
    return result


def _header_mapping(cells: list[str]) -> dict[int, str]:
    mapping: dict[int, str] = {}
    aliases = {
        "本方借贷标志": "借贷标志", "借/贷": "借贷标志", "借贷方向": "借贷标志",
        "交易日期": "交易时间", "记账日期": "交易时间", "入账日期": "交易时间", "日期": "交易时间",
        "对方户名": "对方单位", "对方名称": "对方单位", "交易对手": "对方单位",
        "借方金额": "借方发生额", "付款金额": "借方发生额", "支出金额": "借方发生额", "支出": "借方发生额",
        "贷方金额": "贷方发生额", "收款金额": "贷方发生额", "收入金额": "贷方发生额", "收入": "贷方发生额",
        "交易摘要": "摘要", "附言": "备注", "流水号": "凭证号",
    }
    for index, cell in enumerate(cells):
        compact = _clean(cell).replace(" ", "")
        for alias, canonical in aliases.items():
            if alias in compact:
                mapping[index] = canonical
                break
        else:
            for header in sorted(HEADERS, key=len, reverse=True):
                if header in compact:
                    mapping[index] = header
                    break
    return mapping


def _positive_amount(value: Any) -> Decimal | None:
    amount = _decimal(value)
    return amount if amount is not None and amount != 0 else None


def _normalize_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"((?:20)\d{2})[年./-](\d{1,2})[月./-](\d{1,2})日?(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?", text)
    if not match:
        match = re.search(r"(?<!\d)((?:20)\d{6})(?!\d)", text)
        if not match:
            return ""
        parsed = parse_valid_date(match.group(1))
        return parsed.isoformat() if parsed else ""
    parsed = parse_valid_date(f"{match.group(1)}-{match.group(2)}-{match.group(3)}")
    if not parsed:
        return ""
    time_value = match.group(4)
    if time_value and len(time_value) == 5:
        time_value += ":00"
    return f"{parsed.isoformat()} {time_value}" if time_value else parsed.isoformat()


SHANGHAI_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "transaction_date": ("交易日期", "记账日期", "账务日期", "日期"),
    "transaction_time": ("交易时间", "时间"),
    "purpose": ("交易用途", "用途"),
    "summary": ("交易摘要", "摘要", "备注"),
    "debit_amount": ("借方发生额", "借方金额", "支出金额", "付款金额", "支出", "借方"),
    "credit_amount": ("贷方发生额", "贷方金额", "收入金额", "收款金额", "收入", "贷方"),
    "balance": ("账户余额", "余额"),
    "counterparty_account": ("对方账号", "对手账号", "对方账户", "对方帐号"),
    "counterparty_name": ("对方账户名称", "对方户名", "对方名称", "对方单位", "对手户名", "对手名称"),
    "counterparty_bank": ("对方开户行", "对方行名", "对方银行", "开户行"),
}
SHANGHAI_FOOTER_MARKERS = ("本明细仅限", "重要提示", "若与实际交易不符", "文件下载后", "打印时间", "操作员", "复核", "合计", "小计")
SHANGHAI_PAGE_BLOCK_HEADERS = (
    "交易用途", "摘要", "对手名称", "对方名称", "对手账号", "对方账号", "余额",
    "记账日期", "交易日期", "交易时间", "借方发生额", "贷方发生额",
)


def _page_lines_from_boxes(raw_pages: list[dict[str, Any]], tolerance: float = 8.0) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    all_boxes: list[dict[str, Any]] = []
    page_lines: list[dict[str, Any]] = []
    for page_item in raw_pages:
        page_no = int(page_item.get("page") or 0)
        boxes = []
        for raw_box in page_item.get("text_boxes") or []:
            text = _clean(raw_box.get("text"))
            if not text:
                continue
            box = {
                "page": page_no, "x0": float(raw_box.get("x0") or 0), "y0": float(raw_box.get("y0") or 0),
                "x1": float(raw_box.get("x1") or 0), "y1": float(raw_box.get("y1") or 0),
                "text": text, "confidence": float(raw_box.get("confidence") or 0),
            }
            box["y_center"] = (box["y0"] + box["y1"]) / 2
            boxes.append(box)
            all_boxes.append(box)
        lines: list[dict[str, Any]] = []
        for box in sorted(boxes, key=lambda item: (item["y_center"], item["x0"])):
            line = next((item for item in reversed(lines[-3:]) if abs(item["y_center"] - box["y_center"]) <= tolerance), None)
            if line is None:
                line = {"page": page_no, "y_center": box["y_center"], "boxes": []}
                lines.append(line)
            line["boxes"].append(box)
            count = len(line["boxes"])
            line["y_center"] = ((line["y_center"] * (count - 1)) + box["y_center"]) / count
        for line_no, line in enumerate(lines, start=1):
            line["boxes"].sort(key=lambda item: item["x0"])
            line["line_no"] = line_no
            line["text"] = " ".join(item["text"] for item in line["boxes"])
            page_lines.append(line)
    return all_boxes, page_lines


def _header_matches(line: dict[str, Any]) -> dict[str, dict[str, float]]:
    boxes = line.get("boxes") or []
    matches: dict[str, dict[str, float]] = {}
    for field, aliases in SHANGHAI_HEADER_ALIASES.items():
        for width in (1, 2, 3):
            for start in range(len(boxes)):
                selected = boxes[start:start + width]
                if not selected:
                    continue
                compact = re.sub(r"\s+", "", "".join(item["text"] for item in selected))
                if any(alias in compact for alias in aliases):
                    matches[field] = {
                        "center": (selected[0]["x0"] + selected[-1]["x1"]) / 2,
                        "x0": selected[0]["x0"], "x1": selected[-1]["x1"],
                    }
                    break
            if field in matches:
                break
    return matches


def _column_ranges(matches: dict[str, dict[str, float]], page_width: float) -> list[dict[str, Any]]:
    ordered = sorted(((field, value["center"]) for field, value in matches.items()), key=lambda item: item[1])
    columns: list[dict[str, Any]] = []
    for index, (field, center) in enumerate(ordered):
        left = 0.0 if index == 0 else (ordered[index - 1][1] + center) / 2
        right = page_width if index == len(ordered) - 1 else (center + ordered[index + 1][1]) / 2
        columns.append({"field": field, "x0": left, "x1": right, "center": center})
    return columns


def _cells_from_line(line: dict[str, Any], columns: list[dict[str, Any]]) -> dict[str, str]:
    cells: dict[str, list[str]] = {column["field"]: [] for column in columns}
    for box in line.get("boxes") or []:
        center = (box["x0"] + box["x1"]) / 2
        column = next((item for item in columns if item["x0"] <= center < item["x1"]), None)
        if column:
            cells[column["field"]].append(box["text"])
    return {field: _clean(" ".join(parts)) for field, parts in cells.items()}


def _short_date_with_period(value: Any, period_start: str, period_end: str, last_date: date | None = None) -> date | None:
    parsed = parse_valid_date(str(value or "").strip())
    if parsed:
        return parsed
    match = re.search(r"(?<!\d)(\d{1,2})(?:[./-](\d{1,2})|月(\d{1,2})日)(?!\d)", str(value or ""))
    start, end = parse_valid_date(period_start), parse_valid_date(period_end)
    if not match or not start or not end:
        return None
    month, day = int(match.group(1)), int(match.group(2) or match.group(3))
    candidate_years = list(range(start.year, end.year + 1))
    candidates: list[date] = []
    for year in candidate_years:
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue
        if start <= candidate <= end:
            candidates.append(candidate)
    if not candidates:
        return None
    if last_date:
        forward = [item for item in candidates if item >= last_date]
        if forward:
            return min(forward)
    return candidates[0]


def _date_anchor_in_text(text: str, period_start: str, period_end: str, last_date: date | None = None) -> tuple[date | None, str]:
    patterns = (
        r"(?<!\d)(20\d{2}[./-]\d{1,2}[./-]\d{1,2})(?!\d)",
        r"(?<!\d)(20\d{6})(?!\d)",
        r"(?<!\d)(\d{1,2}[./-]\d{1,2})(?!\d)",
        r"(?<!\d)(\d{1,2}月\d{1,2}日)(?!\d)",
    )
    for pattern in patterns:
        match = re.search(pattern, str(text or ""))
        if not match:
            continue
        parsed = _short_date_with_period(match.group(1), period_start, period_end, last_date)
        if parsed:
            return parsed, match.group(1)
    return None, ""


def _amount_tokens(text: str) -> list[tuple[str, Decimal, int]]:
    tokens: list[tuple[str, Decimal, int]] = []
    source = str(text or "")
    for match in re.finditer(r"(?<![\d-])(?:￥|¥)?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})(?:\s*元)?|(?<![\d-])(?:￥|¥)?\s*\d+\.\d{2}(?:\s*元)?", source):
        amount = _decimal(match.group(0))
        if amount is None:
            continue
        raw = match.group(0).strip()
        context = source[max(0, match.start() - 12):match.end() + 12]
        plain = raw.replace("￥", "").replace("¥", "").replace("元", "").replace(" ", "").replace(",", "")
        month_day_like = bool(re.fullmatch(r"\d{1,2}\.\d{1,2}", plain)) and 1 <= int(plain.split(".")[0]) <= 12 and 1 <= int(plain.split(".")[1]) <= 31
        date_context = bool(re.search(r"(?:月|日|租房|公寓|日期|20\d{2}[./年-])", context))
        # Text fallback is deliberately conservative: M.DD fragments are dates/remarks,
        # never authoritative transaction amounts. Explicit debit/credit columns bypass this.
        if month_day_like or date_context:
            continue
        tokens.append((match.group(0), amount, match.start()))
    return tokens


def _column_block_field(line: str) -> str:
    compact = re.sub(r"[\s:：|]+", "", str(line or ""))
    for field, aliases in SHANGHAI_HEADER_ALIASES.items():
        for alias in sorted(aliases, key=len, reverse=True):
            if compact == re.sub(r"\s+", "", alias):
                return field
    return ""


def parse_shanghai_bank_column_blocks(
    page_text: str, page_no: int, period_start: str, period_end: str, own_account: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Restore pages whose PDF extraction serializes each table column top-to-bottom."""
    lines = [_clean(line) for line in str(page_text or "").splitlines() if _clean(line)]
    header_positions = [(index, _column_block_field(line)) for index, line in enumerate(lines)]
    header_positions = [(index, field) for index, field in header_positions if field]
    fields_found = {field for _index, field in header_positions}
    if "transaction_date" not in fields_found or len(fields_found) < 3:
        return [], [], 0

    columns: dict[str, list[str]] = defaultdict(list)
    for pos, (start, field) in enumerate(header_positions):
        end = header_positions[pos + 1][0] if pos + 1 < len(header_positions) else len(lines)
        values = []
        for value in lines[start + 1:end]:
            if _column_block_field(value) or any(marker in value for marker in SHANGHAI_FOOTER_MARKERS):
                continue
            if re.search(r"第\s*\d+\s*页|共\s*\d+\s*页", value):
                continue
            values.append(value)
        if len(values) > len(columns[field]):
            columns[field] = values

    date_values = columns.get("transaction_date") or []
    transactions: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    invalid = 0
    last_date: date | None = None
    for index, date_value in enumerate(date_values):
        trade_date = _short_date_with_period(date_value, period_start, period_end, last_date)
        row = {field: values[index] if index < len(values) else "" for field, values in columns.items()}
        artifact = {"page": page_no, "row_index": index + 1, "cells": row, "status": "candidate"}
        if not trade_date:
            artifact["status"] = "invalid_date"
            artifacts.append(artifact)
            invalid += 1
            continue
        last_date = trade_date
        debit = _positive_amount(row.get("debit_amount"))
        credit = _positive_amount(row.get("credit_amount"))
        balance = _positive_amount(row.get("balance"))
        if debit is not None and credit is None:
            direction, flag, amount = "出账", "借", debit
        elif credit is not None and debit is None:
            direction, flag, amount = "入账", "贷", credit
        elif debit is not None and credit is not None:
            direction, flag, amount = "未识别", "未识别", None
            artifact["manual_review"] = "借贷金额列同时非零"
        else:
            direction, flag, amount = "未识别", "未识别", None
        support = sum(bool(row.get(field)) for field in ("purpose", "summary", "counterparty_account", "counterparty_name")) + int(amount is not None) + int(balance is not None)
        if support < 1:
            artifact["status"] = "invalid_no_transaction_fields"
            artifacts.append(artifact)
            invalid += 1
            continue
        raw_row = " ".join(str(row.get(field) or "") for field in (
            "transaction_date", "transaction_time", "purpose", "summary", "counterparty_account", "counterparty_name",
            "debit_amount", "credit_amount", "balance",
        ))
        tx = _new_tx(page_no)
        tx.update({
            "交易时间": trade_date.isoformat(), "借贷标志": flag, "收支方向": direction,
            "用途": row.get("purpose") or "", "摘要": row.get("summary") or "",
            "对方账号": row.get("counterparty_account") or "", "对方单位": row.get("counterparty_name") or "",
            "金额": amount, "余额": balance,
            "金额来源": "上海银行列块借贷金额列" if amount is not None else "",
            "raw_line_text": _clean(raw_row), "parser_source": "shanghai_column_block",
        })
        tx["交易分类"] = classify_transaction(tx)
        transactions.append(tx)
        artifact["status"] = "valid"
        artifacts.append(artifact)
    return transactions, artifacts, invalid


def _text_fallback_fields(text: str, date_token: str, columns: list[dict[str, Any]] | None = None, own_account: str = "") -> dict[str, Any]:
    source = _clean(text)
    amounts = _amount_tokens(source)
    nonzero = [(raw, value, pos) for raw, value, pos in amounts if value != 0]
    account_candidates = []
    scrubbed_for_accounts = source.replace(date_token, " ")
    for raw, _value, _pos in amounts:
        scrubbed_for_accounts = scrubbed_for_accounts.replace(raw, " ")
    for match in re.finditer(r"(?<!\d)(\d{8,32})(?!\d)", scrubbed_for_accounts):
        candidate = match.group(1)
        if candidate != str(own_account or "") and candidate not in account_candidates:
            account_candidates.append(candidate)
    company_matches = re.findall(r"[\u4e00-\u9fffA-Za-z0-9（）()]{1,80}(?:有限责任公司|有限公司|公司|商行|个体工商户|银行|支行|分行|营业部|合作社|经营部|中心|店)", source)
    counterparty_name = max(company_matches, key=len) if company_matches else ""
    amount = nonzero[0][1] if nonzero else None
    direction = "未识别"
    flag = "未识别"
    # With the usual Shanghai-bank order, three amounts represent debit, credit and balance.
    if len(amounts) >= 3:
        debit, credit = amounts[0][1], amounts[1][1]
        if debit != 0 and credit == 0:
            amount, direction, flag = debit, "出账", "借"
        elif credit != 0 and debit == 0:
            amount, direction, flag = credit, "入账", "贷"
    if direction == "未识别":
        if re.search(r"(?:贷方|收入|收款)", source) and not re.search(r"(?:借方|支出|付款)", source):
            direction, flag = "入账", "贷"
        elif re.search(r"(?:借方|支出|付款)", source) and not re.search(r"(?:贷方|收入|收款)", source):
            direction, flag = "出账", "借"
    cleaned = source.replace(date_token, " ")
    for raw, _value, _pos in amounts:
        cleaned = cleaned.replace(raw, " ")
    for account in account_candidates:
        cleaned = cleaned.replace(account, " ")
    if counterparty_name:
        cleaned = cleaned.replace(counterparty_name, " ")
    summary = _clean(re.sub(r"\s+", " ", cleaned)).strip("|：:；;,，-")
    return {
        "amount": amount, "direction": direction, "flag": flag,
        "amount_candidates": [value for _raw, value, _pos in nonzero],
        "counterparty_account": account_candidates[0] if account_candidates else "",
        "counterparty_name": counterparty_name, "summary": summary,
        "balance": amounts[-1][1] if len(amounts) >= 2 else None,
    }


def _write_shanghai_debug_artifacts(
    raw_pages: list[dict[str, Any]], boxes: list[dict[str, Any]], page_lines: list[dict[str, Any]],
    detected_headers: list[dict[str, Any]], candidate_rows: list[dict[str, Any]],
) -> None:
    try:
        debug_dir = Path(__file__).resolve().parents[2] / "logs" / "bank_statement_debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        raw_parts = [f"--- page {int(item.get('page') or 0)} ---\n{str(item.get('text') or '')[:2000]}" for item in raw_pages]
        (debug_dir / "shanghai_bank_raw_text.txt").write_text("\n\n".join(raw_parts), encoding="utf-8")
        with (debug_dir / "shanghai_bank_ocr_boxes.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=("page", "x0", "y0", "x1", "y1", "text", "confidence"))
            writer.writeheader()
            writer.writerows({key: item.get(key, "") for key in writer.fieldnames} for item in boxes)
        line_text = "\n".join(f"{item['page']}\t{item['line_no']}\t{item['y_center']:.2f}\t{item['text']}" for item in page_lines)
        (debug_dir / "shanghai_bank_page_lines.txt").write_text(line_text, encoding="utf-8")
        (debug_dir / "shanghai_bank_detected_headers.json").write_text(json.dumps(detected_headers, ensure_ascii=False, indent=2), encoding="utf-8")
        (debug_dir / "shanghai_bank_candidate_rows.json").write_text(json.dumps(candidate_rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception as exc:  # pragma: no cover - debug output must not break parsing
        logger.warning("[ShanghaiBankAdapter] debug_artifact_write_failed error=%s", exc)


def _parse_shanghai_coordinate_table(
    raw_pages: list[dict[str, Any]], period_start: str, period_end: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], int]:
    boxes, page_lines = _page_lines_from_boxes(raw_pages)
    transactions: list[dict[str, Any]] = []
    detected_headers: list[dict[str, Any]] = []
    candidate_artifacts: list[dict[str, Any]] = []
    invalid_rows = 0
    lines_by_page: dict[int, list[dict[str, Any]]] = defaultdict(list)
    pages_by_no = {int(item.get("page") or 0): item for item in raw_pages}
    for line in page_lines:
        lines_by_page[int(line["page"])].append(line)
    last_date: date | None = None
    for page_no, lines in sorted(lines_by_page.items()):
        active_columns: list[dict[str, Any]] = []
        last_tx: dict[str, Any] | None = None
        last_tx_y = 0.0
        page_width = float(pages_by_no.get(page_no, {}).get("page_width") or max((box["x1"] for line in lines for box in line.get("boxes") or []), default=1000))
        for line_index, line in enumerate(lines):
            matches = _header_matches(line)
            if len(matches) >= 3 and "transaction_date" in matches:
                active_columns = _column_ranges(matches, page_width)
                header_artifact = {
                    "page": page_no, "header_line": line["text"], "matched_headers": sorted(matches),
                    "header_y": line["y_center"], "header_columns": active_columns,
                    "header_boxes": [{key: box.get(key) for key in ("x0", "x1", "y0", "y1", "text")} for box in line.get("boxes") or []],
                }
                detected_headers.append(header_artifact)
                logger.info("[ShanghaiBankAdapter] header_found page=%s y=%.2f text=%s", page_no, line["y_center"], line["text"])
                logger.info("[ShanghaiBankAdapter] header_boxes=%s", header_artifact["header_boxes"])
                for below in lines[line_index + 1:line_index + 31]:
                    below_boxes = [{"x0": box.get("x0"), "x1": box.get("x1"), "text": box.get("text")} for box in below.get("boxes") or []]
                    logger.info("[ShanghaiBankAdapter] below_header_line page=%s line=%s y=%.2f text=%s", page_no, below["line_no"], below["y_center"], below["text"])
                    logger.info("[ShanghaiBankAdapter] below_header_boxes=%s", below_boxes)
                last_tx = None
                continue
            if not active_columns or any(marker in line["text"] for marker in SHANGHAI_FOOTER_MARKERS) or re.search(r"第\s*\d+\s*页|共\s*\d+\s*页", line["text"]):
                continue
            cells = _cells_from_line(line, active_columns)
            trade_date = _short_date_with_period(cells.get("transaction_date"), period_start, period_end, last_date)
            date_token = str(cells.get("transaction_date") or "")
            if not trade_date:
                trade_date, date_token = _date_anchor_in_text(line["text"], period_start, period_end, last_date)
            if not trade_date:
                has_continuation = any(cells.get(field) for field in ("counterparty_account", "counterparty_name", "summary"))
                has_amount = _positive_amount(cells.get("debit_amount")) is not None or _positive_amount(cells.get("credit_amount")) is not None
                if last_tx and has_continuation and not has_amount and line["y_center"] - last_tx_y <= 35:
                    if not last_tx.get("对方账号") and cells.get("counterparty_account"):
                        last_tx["对方账号"] = cells["counterparty_account"]
                    if not last_tx.get("对方单位") and cells.get("counterparty_name"):
                        last_tx["对方单位"] = cells["counterparty_name"]
                    if cells.get("summary"):
                        last_tx["备注"] = _clean(" ".join(filter(None, (last_tx.get("备注"), cells["summary"]))))
                    logger.info("[ShanghaiBankAdapter] line_merged page=%s line=%s reason=continuation_without_date", page_no, line["line_no"])
                else:
                    logger.info("[ShanghaiBankAdapter] line_rejected page=%s line=%s reason=no_valid_transaction_date text=%s", page_no, line["line_no"], line["text"])
                continue
            last_date = trade_date
            block_lines = [line["text"]]
            for following in lines[line_index + 1:line_index + 4]:
                next_date, _next_token = _date_anchor_in_text(following["text"], period_start, period_end, last_date)
                if next_date or any(marker in following["text"] for marker in SHANGHAI_FOOTER_MARKERS):
                    break
                block_lines.append(following["text"])
            fallback = _text_fallback_fields(" ".join(block_lines), date_token, active_columns)
            cells["summary"] = cells.get("summary") or fallback["summary"]
            cells["counterparty_account"] = cells.get("counterparty_account") or fallback["counterparty_account"]
            cells["counterparty_name"] = cells.get("counterparty_name") or fallback["counterparty_name"]
            debit = _positive_amount(cells.get("debit_amount"))
            credit = _positive_amount(cells.get("credit_amount"))
            support = sum(bool(cells.get(field)) for field in ("summary", "counterparty_account", "counterparty_name")) + int(debit is not None) + int(credit is not None) + int(_positive_amount(cells.get("balance")) is not None)
            candidate = {"page": page_no, "line_no": line["line_no"], "y_center": line["y_center"], "text": line["text"], "cells": cells, "status": "candidate"}
            if support < 1 and fallback.get("amount") is None and fallback.get("balance") is None:
                candidate["status"] = "invalid_support_fields"
                invalid_rows += 1
                candidate_artifacts.append(candidate)
                logger.info("[ShanghaiBankAdapter] line_rejected page=%s line=%s reason=no_transaction_fields text=%s", page_no, line["line_no"], line["text"])
                continue
            if debit is not None and credit is None:
                direction, flag, amount = "出账", "借", debit
            elif credit is not None and debit is None:
                direction, flag, amount = "入账", "贷", credit
            elif debit is not None and credit is not None:
                direction, flag, amount = ("出账", "借", debit) if debit >= credit else ("入账", "贷", credit)
                candidate["manual_review"] = "借贷金额列同时非零"
            else:
                amount = fallback.get("amount")
                direction = fallback.get("direction") or "未识别"
                flag = fallback.get("flag") or "未识别"
            tx = _new_tx(page_no)
            tx.update({
                "交易时间": trade_date.isoformat(), "借贷标志": flag, "收支方向": direction,
                "对方账号": cells.get("counterparty_account") or "", "对方单位": cells.get("counterparty_name") or "",
                "对方行号": cells.get("counterparty_bank") or "", "摘要": cells.get("summary") or "",
                "金额": amount, "余额": _positive_amount(cells.get("balance")) or fallback.get("balance"),
                "金额来源": "上海银行坐标借贷金额列" if debit is not None or credit is not None else ("上海银行日期锚点文本" if amount is not None else ""),
            })
            tx["交易分类"] = classify_transaction(tx)
            transactions.append(tx)
            candidate["status"] = "valid"
            candidate_artifacts.append(candidate)
            last_tx, last_tx_y = tx, line["y_center"]
    return transactions, boxes, page_lines, detected_headers, candidate_artifacts, invalid_rows


def parse_shanghai_bank_by_date_anchor(
    page_lines: list[dict[str, Any]],
    period_start: str,
    period_end: str,
    detected_headers: list[dict[str, Any]] | None = None,
    own_account: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Second-stage parser that cuts transaction blocks by valid dates, independent of columns."""
    header_y_by_page = {
        int(item.get("page") or 0): min(
            float(item.get("header_y") or 0),
            float(next((current.get("header_y") for current in detected_headers or [] if int(current.get("page") or 0) == int(item.get("page") or 0)), item.get("header_y") or 0)),
        )
        for item in detected_headers or []
    }
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for line in page_lines:
        page = int(line.get("page") or 0)
        if page in header_y_by_page and float(line.get("y_center") or 0) <= header_y_by_page[page]:
            continue
        if any(marker in str(line.get("text") or "") for marker in SHANGHAI_FOOTER_MARKERS):
            continue
        if re.search(r"第\s*\d+\s*页|共\s*\d+\s*页", str(line.get("text") or "")):
            continue
        grouped[page].append(line)
    transactions: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    invalid = 0
    last_date: date | None = None
    for page, lines in sorted(grouped.items()):
        lines.sort(key=lambda item: (float(item.get("y_center") or 0), int(item.get("line_no") or 0)))
        index = 0
        while index < len(lines):
            line = lines[index]
            trade_date, date_token = _date_anchor_in_text(str(line.get("text") or ""), period_start, period_end, last_date)
            if not trade_date:
                index += 1
                continue
            last_date = trade_date
            block = [str(line.get("text") or "")]
            consumed = 0
            for following in lines[index + 1:index + 6]:
                next_date, _next_token = _date_anchor_in_text(str(following.get("text") or ""), period_start, period_end, last_date)
                if next_date:
                    break
                if any(marker in str(following.get("text") or "") for marker in SHANGHAI_FOOTER_MARKERS):
                    break
                block.append(str(following.get("text") or ""))
                consumed += 1
            raw_line_text = _clean(" ".join(block))
            fields = _text_fallback_fields(raw_line_text, date_token, own_account=own_account)
            support = sum(bool(fields.get(key)) for key in ("amount", "summary", "counterparty_account", "counterparty_name", "balance"))
            artifact = {
                "page": page, "line_no": line.get("line_no"), "transaction_date": trade_date.isoformat(),
                "raw_line_text": raw_line_text, "amount_candidates": fields.get("amount_candidates") or [],
                "counterparty_account": fields.get("counterparty_account") or "",
                "counterparty_name": fields.get("counterparty_name") or "", "summary": fields.get("summary") or "",
                "direction": fields.get("direction") or "未识别",
            }
            if support < 1:
                artifact["status"] = "invalid_no_transaction_fields"
                invalid += 1
                candidates.append(artifact)
                index += max(1, consumed + 1)
                continue
            tx = _new_tx(page)
            tx.update({
                "交易时间": trade_date.isoformat(), "借贷标志": fields.get("flag") or "未识别",
                "收支方向": fields.get("direction") or "未识别", "对方账号": fields.get("counterparty_account") or "",
                "对方单位": fields.get("counterparty_name") or "", "摘要": fields.get("summary") or "",
                "金额": fields.get("amount"), "余额": fields.get("balance"),
                "金额来源": "上海银行日期锚点文本" if fields.get("amount") is not None else "",
                "raw_line_text": raw_line_text, "amount_candidates": fields.get("amount_candidates") or [],
            })
            tx["交易分类"] = classify_transaction(tx)
            transactions.append(tx)
            artifact["status"] = "valid"
            candidates.append(artifact)
            index += max(1, consumed + 1)
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for tx in transactions:
        key = (tx.get("交易时间"), tx.get("对方账号"), tx.get("对方单位"), tx.get("金额"), tx.get("摘要"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(tx)
    return unique, candidates, invalid


SHANGHAI_SERIAL_RE = re.compile(r"(?:FT\d{8,}|BEA\d{8,}|V\d{8,}|G\d{8,}|ONLINE\.AC\.CLOSURE\S*|\d{3}\.\d{8,}\.\d+|\d{8,32}-\d{8})")
SHANGHAI_NATIVE_ROW_RE = re.compile(
    rf"(?P<serial_no>{SHANGHAI_SERIAL_RE.pattern})\s+"
    r"(?P<trade_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<book_date>\d{4}-\d{2}-\d{2})\s+"
    r"(?P<direction_raw>入账\(贷方\)|出账\(借方\))\s+"
    r"(?P<amount>[\d,]+\.\d{2})\s+"
    r"(?P<balance>[\d,]+\.\d{2})\s*"
    rf"(?P<rest>.*?)(?=\n?\s*{SHANGHAI_SERIAL_RE.pattern}\s+\d{{4}}-\d{{2}}-\d{{2}}\s+\d{{2}}:\d{{2}}:\d{{2}}|\Z)",
    re.S,
)
SHANGHAI_SUMMARY_KEYWORDS = (
    "企业电子银行行内及跨行同城转账手续费收费", "企业电子银行跨行异地转账手续费收费",
    "企业网上银行跨行同城转账", "企业网上银行跨行异地转账", "企业短信业务服务费收费",
    "单位活期存款利息收入", "帐户结息（贷记）", "扣款（缴税）", "企业短信业务服务费",
    "跨行转账", "ETC业务扣款", "企业网上银行", "缴税", "帐户结息", "账户结息",
    "转账", "手续费", "银行手续费", "代发专用账户", "单位活期存款",
)


def _shanghai_native_text(raw_pages: list[dict[str, Any]], source_text: str = "") -> str:
    if source_text and _is_shanghai_native_statement_text(source_text):
        return re.sub(r"(BEA\d{8,})\s*\n\s*(\d)\s+(?=\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", r"\1\2 ", str(source_text))
    parts = [str(source_text or "")]
    parts.extend(str(item.get("text") or "") for item in raw_pages if isinstance(item, dict))
    text = "\n".join(part for part in parts if part)
    text = re.sub(r"(BEA\d{8,})\s*\n\s*(\d)\s+(?=\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", r"\1\2 ", text)
    return text


def _is_shanghai_native_statement_text(text: str) -> bool:
    compact = str(text or "")
    required = ("账户明细查询", "交易流水号", "交易时间", "记账日期", "交易方向", "交易金额", "余额")
    return all(item in compact for item in required)


def _normalize_shanghai_native_lines(raw_pages: list[dict[str, Any]], source_text: str = "") -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    pages = [{"page": 1, "text": source_text}] if source_text else []
    pages.extend(raw_pages)
    for page_item in pages:
        page_no = int(page_item.get("page") or 0)
        lines = [_clean(line) for line in str(page_item.get("text") or "").splitlines()]
        lines = [line for line in lines if line]
        index = 0
        while index < len(lines):
            line = lines[index]
            # BEA25107140212966000 / 1 2025-... is a common native-text wrap.
            if re.fullmatch(SHANGHAI_SERIAL_RE, line) and index + 1 < len(lines):
                next_line = lines[index + 1]
                merge_match = re.match(r"^(\d?)\s*((?:20)\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b.*)$", next_line)
                if merge_match:
                    line = f"{line}{merge_match.group(1)} {merge_match.group(2)}".strip()
                    index += 1
            normalized.append({"page": page_no, "line_no": index + 1, "text": line})
            index += 1
    return normalized


def _parse_shanghai_rest(rest: str, own_account: str = "") -> dict[str, str]:
    text = _clean(rest)
    text = re.sub(r"有限\s+公司", "有限公司", text)
    text = re.sub(r"公\s+司", "公司", text)
    text = text.replace("跨行同城 转账", "跨行同城转账").replace("跨行异地 转账", "跨行异地转账")
    result = {"counterparty_account": "", "counterparty_name": "", "summary": "", "purpose": "", "remark": ""}
    account_match = re.match(r"(?P<account>\d{8,32})\s*(?P<remain>.*)$", text)
    if account_match:
        result["counterparty_account"] = account_match.group("account")
        text = _clean(account_match.group("remain"))
    keyword_positions = [(text.find(keyword), keyword) for keyword in sorted(SHANGHAI_SUMMARY_KEYWORDS, key=len, reverse=True) if text.find(keyword) >= 0]
    keyword_positions.sort(key=lambda item: item[0])
    if keyword_positions:
        pos, keyword = keyword_positions[0]
        before = _normalize_company_spacing(text[:pos])
        after = _clean(text[pos + len(keyword):])
        result["counterparty_name"] = "" if _normalize_entity_name(before) in {_normalize_entity_name("企业网上银行"), _normalize_entity_name("企业短信业务服务费"), _normalize_entity_name("ETC业务扣款"), _normalize_entity_name("缴税"), _normalize_entity_name("帐户结息"), _normalize_entity_name("单位活期存款")} else before
        result["summary"] = keyword
        result["purpose"] = after[:120]
    else:
        name_match = re.search(r"([\u4e00-\u9fff（）()]{2,60}(?:有限公司|公司|商行|个体工商户|银行|支行|合作社|经营部|店|中心|专户))", text)
        if name_match:
            result["counterparty_name"] = _normalize_company_spacing(name_match.group(1))
            result["purpose"] = _clean(text.replace(name_match.group(1), "", 1))[:120]
        elif re.fullmatch(r"[\u4e00-\u9fff]{2,4}", text):
            result["counterparty_name"] = text
        else:
            result["summary"] = text[:60]
    if own_account and result["counterparty_account"] == own_account:
        result["counterparty_account"] = ""
    return result


def parse_shanghai_bank_native_text_rows(
    raw_pages: list[dict[str, Any]],
    period_start: str,
    period_end: str,
    own_account: str = "",
    source_text: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    native_text = _shanghai_native_text(raw_pages, source_text)
    page_lines = _normalize_shanghai_native_lines(raw_pages, source_text)
    transactions: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    invalid = 0
    start = parse_valid_date(period_start)
    end = parse_valid_date(period_end)
    matches = list(SHANGHAI_NATIVE_ROW_RE.finditer(native_text))
    if not matches:
        for line in page_lines:
            text = str(line.get("text") or "")
            match = SHANGHAI_NATIVE_ROW_RE.match(text)
            if not match:
                continue
            matches.append(match)
    for offset, match in enumerate(matches, start=1):
        data = match.groupdict()
        trade_date = parse_valid_date(data["trade_time"][:10])
        artifact = {
            "page": 1, "line_no": offset, "serial_no": data["serial_no"],
            "transaction_date": data["trade_time"], "raw_line_text": _clean(match.group(0)), "status": "candidate",
        }
        candidates.append(artifact)
        if not trade_date or (start and trade_date < start) or (end and trade_date > end):
            artifact["status"] = "invalid_date_out_of_period"
            invalid += 1
            continue
        amount = _positive_amount(data["amount"])
        balance = _positive_amount(data["balance"])
        direction = "出账" if "出账" in data["direction_raw"] else "入账"
        flag = "借" if direction == "出账" else "贷"
        rest_fields = _parse_shanghai_rest(data.get("rest") or "", own_account)
        support = int(amount is not None) + int(balance is not None) + sum(bool(rest_fields.get(key)) for key in ("counterparty_account", "counterparty_name", "summary", "purpose"))
        if support < 1:
            artifact["status"] = "invalid_no_transaction_fields"
            invalid += 1
            continue
        tx = _new_tx(1)
        tx.update({
            "凭证号": data["serial_no"],
            "交易流水号": data["serial_no"],
            "交易时间": data["trade_time"],
            "记账日期": data["book_date"],
            "借贷标志": flag,
            "收支方向": direction,
            "金额": amount,
            "余额": balance,
            "金额来源": "上海银行交易金额列" if amount is not None else "",
            "对方账号": rest_fields.get("counterparty_account") or "",
            "对方单位": rest_fields.get("counterparty_name") or "",
            "摘要": rest_fields.get("summary") or "",
            "用途": rest_fields.get("purpose") or "",
            "备注": rest_fields.get("remark") or "",
            "raw_line_text": _clean(match.group(0)),
            "parser_source": "shanghai_native_text_row",
        })
        tx["交易分类"] = classify_transaction(tx)
        transactions.append(tx)
        artifact.update({
            "status": "valid", "amount": amount, "balance": balance, "direction": direction,
            "counterparty_account": tx["对方账号"], "counterparty_name": tx["对方单位"],
            "summary": tx["摘要"], "purpose": tx["用途"], "category": tx["交易分类"],
        })
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for tx in transactions:
        key = (tx.get("凭证号"), tx.get("交易时间"), tx.get("金额"), tx.get("余额"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(tx)
    unique.sort(key=lambda tx: str(tx.get("交易时间") or ""))
    return unique, candidates, invalid


def parse_shanghai_bank_statement(
    raw_pages: list[dict[str, Any]],
    period_start: str,
    period_end: str,
    own_account: str = "",
    source_text: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, dict[str, Any]]:
    logger.info("[ShanghaiBankAdapter] detected_bank_format=shanghai_bank")
    logger.info("[ShanghaiBankAdapter] pages=%s", len({int(item.get('page') or index) for index, item in enumerate(raw_pages, 1)}))
    logger.info("[ShanghaiBankAdapter] raw_text_len=%s", sum(len(str(item.get("text") or "")) for item in raw_pages))
    combined_native_text = _shanghai_native_text(raw_pages, source_text)
    native_header = _parse_shanghai_native_header(raw_pages, source_text)
    native_activated = _is_shanghai_native_statement_text(combined_native_text)
    logger.info("[ShanghaiBankNativeParser] activated=%s", str(native_activated).lower())
    native_transactions: list[dict[str, Any]] = []
    native_candidates: list[dict[str, Any]] = []
    native_invalid = 0
    if native_activated:
        native_transactions, native_candidates, native_invalid = parse_shanghai_bank_native_text_rows(raw_pages, period_start, period_end, own_account, source_text)
    else:
        missing = [item for item in ("账户明细查询", "交易流水号", "交易时间", "记账日期", "交易方向", "交易金额", "余额") if item not in combined_native_text]
        logger.info("[ShanghaiBankNativeParser] activated=false missing_markers=%s", missing)
    logger.info("[ShanghaiBankNativeParser] account_no=%s", native_header.get("account_no") or "未识别")
    logger.info("[ShanghaiBankNativeParser] opening_bank=%s", native_header.get("opening_bank") or "未识别")
    logger.info("[ShanghaiBankNativeParser] account_name=%s", native_header.get("account_name") or "未识别")
    logger.info("[ShanghaiBankNativeParser] total_count_from_header=%s", native_header.get("transaction_count") or "未识别")
    logger.info("[ShanghaiBankNativeParser] debit_total_amount=%s", native_header.get("debit_total_amount") or "未识别")
    logger.info("[ShanghaiBankNativeParser] credit_total_amount=%s", native_header.get("credit_total_amount") or "未识别")
    logger.info("[ShanghaiBankNativeParser] native_matches_count=%s", len(native_transactions))
    logger.info("[ShanghaiBankNativeParser] first_5_transactions=%s", native_candidates[:5])
    if native_activated and not native_candidates:
        anchor = combined_native_text.find("交易流水号")
        logger.info("[ShanghaiBankNativeParser] native_match_failed_preview=%s", _clean(combined_native_text[anchor:anchor + 3000] if anchor >= 0 else combined_native_text[:3000]))
    if native_transactions:
        evidence: list[dict[str, Any]] = []
        for index, tx in enumerate(native_transactions, start=1):
            tx["序号"] = index
            tx.update({
                "voucher_no": tx.get("凭证号") or "", "counterparty_account": tx.get("对方账号") or "",
                "transaction_time": tx.get("交易时间") or "", "debit_credit_flag": tx.get("借贷标志") or "未识别",
                "direction": tx.get("收支方向") or "未识别", "counterparty_name": tx.get("对方单位") or "",
                "counterparty_bank_no": tx.get("对方行号") or "", "purpose": tx.get("用途") or "",
                "summary": tx.get("摘要") or "", "remark": tx.get("备注") or "", "amount": tx.get("金额"),
                "balance": tx.get("余额"), "receipt_info": "", "category": tx.get("交易分类") or "其他", "source_page": tx.get("来源页码") or 0,
            })
            evidence.append({"field": "交易明细", "page": tx["来源页码"], "record": index, "source": "shanghai_bank_native_text_row"})
        diagnostics = {
            "table_headers_detected": ["交易流水号", "交易时间", "记账日期", "交易方向", "交易金额", "余额", "对手账号", "对手名称", "摘要", "交易用途", "备注"],
            "candidate_transaction_rows": int(native_header.get("transaction_count") or len(native_candidates)),
            "parsed_transaction_count": len(native_transactions),
            "candidate_rows_preview": native_candidates[:10],
            "invalid_candidate_rows": native_invalid,
            "raw_text_blocks_count": 0,
            "page_lines_count": sum(len(str(item.get("text") or "").splitlines()) for item in raw_pages),
            "parser_path": "native_text_row",
            "header_summary": native_header,
            "coordinate_parse_valid_count": 0,
            "fallback_date_anchor_candidates": 0,
            "fallback_valid_transactions": 0,
            "first_10_fallback_transactions": [],
        }
        logger.info("[ShanghaiBankAdapter] candidate_transaction_rows=%s", diagnostics["candidate_transaction_rows"])
        logger.info("[ShanghaiBankAdapter] valid_transaction_count=%s", len(native_transactions))
        logger.info("[ShanghaiBankAdapter] invalid_candidate_rows=%s", native_invalid)
        return native_transactions, evidence, True, diagnostics
    coordinate_transactions, boxes, page_lines, detected_header_artifacts, candidate_artifacts, coordinate_invalid = _parse_shanghai_coordinate_table(raw_pages, period_start, period_end)
    if not page_lines:
        for page_item in raw_pages:
            page_no = int(page_item.get("page") or 0)
            for line_no, text_line in enumerate(str(page_item.get("text") or "").splitlines(), start=1):
                if _clean(text_line):
                    page_lines.append({"page": page_no, "line_no": line_no, "y_center": float(line_no * 20), "text": _clean(text_line), "boxes": []})
    logger.info("[ShanghaiBankAdapter] ocr_boxes_count=%s", len(boxes))
    for page_no in sorted({int(item.get("page") or 0) for item in raw_pages}):
        logger.info("[ShanghaiBankAdapter] page=%s lines_count=%s", page_no, sum(int(line.get("page") or 0) == page_no for line in page_lines))
    logger.info("[ShanghaiBankAdapter] detected_headers=%s", detected_header_artifacts)
    logger.info("[ShanghaiBankAdapter] detected_headers_count=%s", len(detected_header_artifacts))
    logger.info("[ShanghaiBankAdapter] coordinate_parse_valid_count=%s", len(coordinate_transactions))
    column_transactions: list[dict[str, Any]] = []
    column_candidates: list[dict[str, Any]] = []
    column_invalid = 0
    for page_item in raw_pages:
        page_transactions, page_candidates, page_invalid = parse_shanghai_bank_column_blocks(
            str(page_item.get("text") or ""), int(page_item.get("page") or 0), period_start, period_end, own_account,
        )
        column_transactions.extend(page_transactions)
        column_candidates.extend(page_candidates)
        column_invalid += page_invalid
    logger.info("[ShanghaiBankAdapter] column_block_candidates=%s", len(column_candidates))
    logger.info("[ShanghaiBankAdapter] column_block_valid_count=%s", len(column_transactions))
    parser_path = "coordinate_table"
    # A recovered column block is structurally safer than rows assembled from a
    # page-wide date anchor, so prefer it whenever it produced real rows.
    if column_transactions:
        coordinate_transactions = column_transactions
        candidate_artifacts = column_candidates
        coordinate_invalid = column_invalid
        parser_path = "column_block"
    fallback_transactions: list[dict[str, Any]] = []
    fallback_candidates: list[dict[str, Any]] = []
    fallback_invalid = 0
    if not coordinate_transactions:
        fallback_transactions, fallback_candidates, fallback_invalid = parse_shanghai_bank_by_date_anchor(
            page_lines, period_start, period_end, detected_header_artifacts, own_account,
        )
        logger.info("[ShanghaiBankAdapter] fallback_date_anchor_candidates=%s", len(fallback_candidates))
        logger.info("[ShanghaiBankAdapter] fallback_valid_transactions=%s", len(fallback_transactions))
        logger.info("[ShanghaiBankAdapter] first_10_fallback_transactions=%s", fallback_candidates[:10])
        if not fallback_candidates:
            for line in page_lines[:50]:
                logger.info("[ShanghaiBankAdapter] fallback_below_header_line page=%s line=%s text=%s", line.get("page"), line.get("line_no"), line.get("text"))
        if fallback_transactions:
            coordinate_transactions = fallback_transactions
            candidate_artifacts = fallback_candidates
            coordinate_invalid = fallback_invalid
            parser_path = "date_anchor_text_fallback"
    if detected_header_artifacts or coordinate_transactions:
        deduplicated_transactions: list[dict[str, Any]] = []
        seen_transactions: set[tuple[Any, ...]] = set()
        for tx in coordinate_transactions:
            key = (tx.get("交易时间"), tx.get("对方账号"), tx.get("对方单位"), tx.get("金额"), tx.get("收支方向"))
            if key in seen_transactions:
                continue
            seen_transactions.add(key)
            deduplicated_transactions.append(tx)
        coordinate_transactions = sorted(deduplicated_transactions, key=lambda tx: str(tx.get("交易时间") or ""))
        unique_candidates: list[dict[str, Any]] = []
        seen_candidates: set[tuple[Any, ...]] = set()
        for item in candidate_artifacts:
            cells = item.get("cells") or {}
            if cells:
                key = (
                    item.get("page"), cells.get("transaction_date"), cells.get("debit_amount"), cells.get("credit_amount"),
                    cells.get("counterparty_account"), cells.get("counterparty_name"), cells.get("summary"), item.get("status"),
                )
            else:
                key = (item.get("page"), item.get("transaction_date"), item.get("raw_line_text"), item.get("status"))
            if key in seen_candidates:
                continue
            seen_candidates.add(key)
            unique_candidates.append(item)
        candidate_artifacts = unique_candidates
        coordinate_invalid = sum(item.get("status") != "valid" for item in candidate_artifacts)
        evidence: list[dict[str, Any]] = []
        for index, tx in enumerate(coordinate_transactions, start=1):
            tx["序号"] = index
            tx.update({
                "voucher_no": tx.get("凭证号") or "", "counterparty_account": tx.get("对方账号") or "",
                "transaction_time": tx.get("交易时间") or "", "debit_credit_flag": tx.get("借贷标志") or "未识别",
                "direction": tx.get("收支方向") or "未识别", "counterparty_name": tx.get("对方单位") or "",
                "counterparty_bank_no": tx.get("对方行号") or "", "purpose": tx.get("用途") or "",
                "summary": tx.get("摘要") or "", "remark": tx.get("备注") or "", "amount": tx.get("金额"),
                "balance": tx.get("余额"), "receipt_info": "", "category": tx.get("交易分类") or "其他", "source_page": tx.get("来源页码") or 0,
            })
            evidence.append({"field": "交易明细", "page": tx["来源页码"], "record": index, "source": "shanghai_bank_coordinate_table"})
        diagnostics = {
            "table_headers_detected": sorted({field for item in detected_header_artifacts for field in item.get("matched_headers") or []}),
            "detected_header_artifacts": detected_header_artifacts,
            "candidate_transaction_rows": len(candidate_artifacts),
            "candidate_rows_preview": candidate_artifacts[:10],
            "invalid_candidate_rows": coordinate_invalid,
            "raw_text_blocks_count": len(boxes),
            "page_lines_count": len(page_lines),
            "parser_path": parser_path,
            "column_block_candidates": len(column_candidates),
            "column_block_valid_count": len(column_transactions),
            "coordinate_parse_valid_count": 0 if fallback_transactions else len(coordinate_transactions),
            "fallback_date_anchor_candidates": len(fallback_candidates),
            "fallback_valid_transactions": len(fallback_transactions),
            "first_10_fallback_transactions": fallback_candidates[:10],
        }
        _write_shanghai_debug_artifacts(raw_pages, boxes, page_lines, detected_header_artifacts, candidate_artifacts)
        logger.info("[ShanghaiBankAdapter] candidate_transaction_rows=%s", len(candidate_artifacts))
        logger.info("[ShanghaiBankAdapter] valid_transaction_count=%s", len(coordinate_transactions))
        logger.info("[ShanghaiBankAdapter] invalid_candidate_rows=%s", coordinate_invalid)
        return coordinate_transactions, evidence, bool(any(tx.get("金额") is not None for tx in coordinate_transactions)), diagnostics
    transactions: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    header_names: set[str] = set()
    candidate_rows = 0
    invalid_rows = 0
    amount_column_detected = False
    for page_item in raw_pages:
        page_no = int(page_item.get("page") or 0)
        source_rows = page_item.get("table_rows") if isinstance(page_item.get("table_rows"), list) else []
        rows = [[_clean(cell) for cell in row] for row in source_rows if isinstance(row, (list, tuple))]
        rows.extend([[_clean(cell) for cell in re.split(r"\s*\|\s*|\t+", line)] for line in str(page_item.get("text") or "").splitlines() if "|" in line or "\t" in line])
        if not source_rows and not boxes:
            rows.extend([
                [_clean(cell) for cell in re.split(r"\s{2,}", line) if _clean(cell)]
                for line in str(page_item.get("text") or "").splitlines()
                if re.search(r"\s{2,}", line)
            ])
        mapping: dict[int, str] = {}
        for cells in rows:
            detected = _header_mapping(cells)
            detected_values = set(detected.values())
            if "交易时间" in detected_values and detected_values.intersection({"借方发生额", "贷方发生额", "金额", "余额", "对方账号", "对方单位", "摘要"}) and len(detected) >= 3:
                mapping = detected
                header_names.update(detected.values())
                amount_column_detected = amount_column_detected or bool(detected_values.intersection({"借方发生额", "贷方发生额", "金额"}))
                continue
            if not mapping:
                continue
            values = {field: cells[index] if index < len(cells) else "" for index, field in mapping.items()}
            trade_time = _normalize_trade_date(values.get("交易时间"))
            if not trade_time:
                short_date = _short_date_with_period(values.get("交易时间"), period_start, period_end)
                trade_time = short_date.isoformat() if short_date else ""
            if not trade_time:
                continue
            candidate_rows += 1
            trade_date = parse_valid_date(trade_time[:10])
            start = parse_valid_date(period_start)
            end = parse_valid_date(period_end)
            if not trade_date or (start and trade_date < start) or (end and trade_date > end):
                invalid_rows += 1
                continue
            debit = _positive_amount(values.get("借方发生额"))
            credit = _positive_amount(values.get("贷方发生额"))
            flag = _clean(values.get("借贷标志"))[:1]
            if debit is not None and credit is None:
                direction, flag, amount = "出账", "借", debit
            elif credit is not None and debit is None:
                direction, flag, amount = "入账", "贷", credit
            elif flag in {"借", "贷"}:
                direction = "出账" if flag == "借" else "入账"
                amount = _positive_amount(values.get("金额"))
            else:
                direction, flag = "未识别", "未识别"
                amount = _positive_amount(values.get("金额"))
            support = sum(bool(_clean(values.get(field))) for field in ("对方账号", "对方单位", "摘要", "用途")) + int(amount is not None)
            support += int(_positive_amount(values.get("余额")) is not None)
            if support < 1:
                invalid_rows += 1
                continue
            tx = _new_tx(page_no)
            tx.update({
                "凭证号": _clean(values.get("凭证号")), "对方账号": _clean(values.get("对方账号")),
                "交易时间": trade_time, "借贷标志": flag, "收支方向": direction,
                "对方单位": _clean(values.get("对方单位")), "对方行号": _clean(values.get("对方行号")),
                "用途": _clean(values.get("用途")), "摘要": _clean(values.get("摘要")),
                "备注": _clean(values.get("备注")), "金额": amount,
                "余额": _positive_amount(values.get("余额")), "金额来源": "上海银行借贷金额列" if amount is not None else "",
            })
            tx["交易分类"] = classify_transaction(tx)
            transactions.append(tx)
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for tx in transactions:
        key = (tx.get("交易时间"), tx.get("对方账号"), tx.get("对方单位"), tx.get("金额"), tx.get("收支方向"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(tx)
    unique.sort(key=lambda tx: str(tx.get("交易时间") or ""))
    for index, tx in enumerate(unique, start=1):
        tx["序号"] = index
        tx.update({
            "voucher_no": tx.get("凭证号") or "", "counterparty_account": tx.get("对方账号") or "",
            "transaction_time": tx.get("交易时间") or "", "debit_credit_flag": tx.get("借贷标志") or "未识别",
            "direction": tx.get("收支方向") or "未识别", "counterparty_name": tx.get("对方单位") or "",
            "counterparty_bank_no": tx.get("对方行号") or "", "purpose": tx.get("用途") or "",
            "summary": tx.get("摘要") or "", "remark": tx.get("备注") or "", "amount": tx.get("金额"),
            "balance": tx.get("余额"), "receipt_info": "", "category": tx.get("交易分类") or "其他", "source_page": tx.get("来源页码") or 0,
        })
        evidence.append({"field": "交易明细", "page": tx["来源页码"], "record": index, "source": "shanghai_bank_table"})
    diagnostics = {
        "table_headers_detected": sorted(header_names),
        "candidate_transaction_rows": candidate_rows,
        "invalid_candidate_rows": invalid_rows,
        "raw_text_blocks_count": len(boxes) or sum(len(item.get("table_rows") or []) for item in raw_pages),
        "page_lines_count": len(page_lines),
        "candidate_rows_preview": [],
        "parser_path": "table_rows_or_text_fallback",
    }
    _write_shanghai_debug_artifacts(raw_pages, boxes, page_lines, detected_header_artifacts, candidate_artifacts)
    logger.info("[ShanghaiBankAdapter] candidate_transaction_rows=%s", candidate_rows)
    logger.info("[ShanghaiBankAdapter] valid_transaction_count=%s", len(unique))
    logger.info("[ShanghaiBankAdapter] invalid_candidate_rows=%s", invalid_rows)
    return unique, evidence, amount_column_detected, diagnostics


def _extract_amount(tx: dict[str, Any], explicit_amount: str = "", explicit_balance: str = "") -> None:
    if explicit_balance:
        tx["余额"] = _decimal(explicit_balance)
    if explicit_amount:
        tx["金额"] = _decimal(explicit_amount)
        tx["金额来源"] = "主表金额列" if tx["金额"] is not None else ""
        return
    info = str(tx.get("回单个性化信息") or "")
    matches = [(m.group("label"), _decimal(m.group("amount"))) for m in AMOUNT_LABEL_RE.finditer(info)]
    matches = [(label, amount) for label, amount in matches if amount is not None]
    category = str(tx.get("交易分类") or "")
    priorities = (
        ("利息",) if "利息" in category else
        (("实收金额", "应收金额") if category == "银行费用" else
         ("交易金额", "发生额", "贷款金额", "归还金额", "还款金额", "本金", "实收金额", "应收金额", "利息", "金额"))
    )
    for wanted in priorities:
        found = next(((label, amount) for label, amount in matches if label == wanted), None)
        if found:
            tx["金额"], tx["金额来源"] = found[1], f"回单个性化信息.{found[0]}"
            return


def classify_transaction(tx: dict[str, Any]) -> str:
    text = " ".join(str(tx.get(key) or "") for key in ("用途", "摘要", "备注", "回单个性化信息"))
    direction = tx.get("收支方向")
    if any(keyword in text for keyword in BANK_FEE_KEYWORDS):
        return "银行费用"
    if any(keyword in text for keyword in LOAN_REPAYMENT_KEYWORDS):
        return "贷款归还"
    if any(keyword in text for keyword in LOAN_DISBURSEMENT_KEYWORDS):
        return "贷款发放"
    if any(keyword in text for keyword in INTEREST_EXPENSE_KEYWORDS):
        return "利息支出"
    if "利息" in text and direction == "入账" and "利息支付" not in text:
        return "利息收入"
    if any(keyword in text for keyword in OPERATING_KEYWORDS):
        return "经营入账" if direction == "入账" else ("经营出账" if direction == "出账" else "其他")
    if any(keyword in text for keyword in CURRENT_ACCOUNT_KEYWORDS):
        return "往来入账" if direction == "入账" else ("往来出账" if direction == "出账" else "其他")
    if "借款" in text:
        return "资金拆借"
    return "其他"


def _new_tx(page: int) -> dict[str, Any]:
    return {
        "序号": 0, "凭证号": "", "对方账号": "", "交易时间": "", "借贷标志": "",
        "收支方向": "", "对方单位": "", "对方行号": "", "用途": "", "摘要": "",
        "备注": "", "金额": None, "余额": None, "回单个性化信息": "", "交易分类": "其他",
        "来源页码": page, "金额来源": "",
    }


def _append_info(tx: dict[str, Any], value: str) -> None:
    value = _clean(value)
    if value and value not in str(tx.get("回单个性化信息") or ""):
        tx["回单个性化信息"] = "；".join(filter(None, (str(tx.get("回单个性化信息") or ""), value)))


def _normalize_entity_name(value: Any) -> str:
    text = str(value or "").replace("（", "(").replace("）", ")").replace("　", "")
    text = re.sub(r"\s+", "", text)
    text = text.replace("有限 公司", "有限公司")
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff()]", "", text).lower()


def normalize_person_name(value: Any) -> str:
    text = re.sub(r"^\s*\d{8,32}\s*", "", str(value or ""))
    text = re.sub(r"[\s\u3000\r\n\t]+", "", text)
    text = re.sub(r"(先生|女士|小姐|总|经理|法人|法定代表人|实控人|实际控制人|股东|高管)$", "", text)
    chinese = "".join(re.findall(r"[\u4e00-\u9fff]", text))
    if 2 <= len(chinese) <= 6:
        return chinese
    return ""


def _iter_name_values(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in re.split(r"[,，、;/；\s]+", value) if item]
    if isinstance(value, dict):
        name = value.get("name") or value.get("姓名") or value.get("person_name") or value.get("personName")
        return [str(name)] if name else []
    if isinstance(value, (list, tuple, set)):
        names: list[str] = []
        for item in value:
            names.extend(_iter_name_values(item))
        return names
    return [str(value)]


def _related_person_map_from_metadata(metadata: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    source = metadata or {}
    containers = [
        source,
        source.get("customer_profile") or {},
        source.get("customer") or {},
        source.get("profile") or {},
        source.get("company_profile") or {},
    ]
    roles: dict[str, dict[str, str]] = {}

    def add(value: Any, role: str) -> None:
        for name in _iter_name_values(value):
            normalized = normalize_person_name(name)
            if normalized:
                roles[normalized] = {"name": normalized, "role": role}

    for container in containers:
        if not isinstance(container, dict):
            continue
        add(container.get("legal_representative_name") or container.get("legalRepresentativeName") or container.get("法定代表人"), "法定代表人")
        add(container.get("actual_controller_name") or container.get("actualControllerName") or container.get("实际控制人") or container.get("实控人"), "实际控制人")
        add(container.get("shareholder_names") or container.get("shareholderNames") or container.get("shareholders") or container.get("股东"), "股东")
        add(container.get("executive_names") or container.get("executiveNames") or container.get("executives") or container.get("高管"), "高管")
        contact_role = str(container.get("contact_person_role") or container.get("contactPersonRole") or container.get("联系人角色") or "")
        if any(marker in contact_role for marker in ("法人", "法定代表人", "实控", "实际控制", "股东", "高管", "董事", "监事", "经理")):
            add(container.get("contact_person_name") or container.get("contactPersonName") or container.get("联系人"), contact_role or "关联联系人")
    manual_roles = source.get("related_person_roles") or source.get("relatedPersonRoles") or {}
    if isinstance(manual_roles, dict):
        for name, role in manual_roles.items():
            add(name, str(role or "人工维护关联人"))
    for name in _iter_name_values(source.get("related_person_names") or source.get("relatedPersonNames")):
        normalized = normalize_person_name(name)
        if normalized and normalized not in roles:
            roles[normalized] = {"name": normalized, "role": "人工维护关联人"}
    return roles


def normalize_opening_bank_name(value: Any, bank_name: str = "") -> str:
    """Normalize branch whitespace without dropping institution suffixes."""
    text = re.sub(r"[\s　]+", "", str(value or "")).strip("：:；;,，")
    if not text:
        return ""
    if text.endswith(("支行", "分行", "营业部", "分理处")):
        return text
    bank_context = f"{bank_name}{text}"
    if text.endswith("路") and ("工行" in bank_context or "中国工商银行" in bank_context):
        return f"{text}支行"
    return text


def sanitize_opening_bank_for_display(value: Any, *, bank_format: str = "", account_no: str = "", account_name: str = "") -> str:
    if bank_format == BANK_FORMAT_SHANGHAI:
        cleaned = clean_opening_bank(value, account_no=account_no, account_name=account_name)
        forbidden = ["币种", "人民币", "选择账号", "借方总金额", "总笔数"]
        if account_no:
            forbidden.append(account_no)
        if account_name:
            forbidden.append(account_name)
        if any(marker and marker in cleaned for marker in forbidden):
            cleaned = clean_opening_bank(cleaned, account_no=account_no, account_name=account_name)
        return cleaned if not any(marker and marker in cleaned for marker in forbidden) else ""
    return normalize_opening_bank_name(value, "")


def _clean_display_text(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    marker_pattern = "|".join(re.escape(marker) for marker in SENSITIVE_DISPLAY_MARKERS)
    text = re.split(marker_pattern, text, maxsplit=1, flags=re.I)[0]
    text = re.sub(r"(?:附言|处理种类|用途)\s*[:：]\s*", "", text)
    return _clean(text).strip("：:；;,，")


def _invalid_counterparty_reason(value: Any) -> str:
    text = _clean(value)
    compact = _normalize_entity_name(text)
    invalid_exact = {
        "企业网上银行", "有限公司", "公司", "运营有限公司", "）有限公司", "对手名称", "对手账号",
        "对方名称", "对方账号", "余额", "代发专用账户", "单位活期存款",
        "企业短信业务服务费", "ETC业务扣款", "缴税", "帐户结息", "账户结息",
    }
    if not text or text == "—":
        return "对方单位为空"
    if compact in {_normalize_entity_name(item) for item in invalid_exact}:
        return "对方单位为渠道、表头或不完整公司名称"
    if len(compact) > 60:
        return "对方单位疑似整页列块或多个名称拼接"
    if len(re.findall(r"(?:有限责任公司|有限公司|公司)", compact)) > 1:
        return "对方单位包含多个公司名称"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "对方单位为日期"
    if text.startswith((":", "：")) or any(marker.lower() in text.lower() for marker in GARBAGE_COUNTERPARTY_MARKERS):
        return "对方单位疑似回单说明或 OCR 垃圾字段"
    if compact.isdigit():
        return "对方单位为纯数字"
    if len(compact) < 4 and not re.fullmatch(r"[\u4e00-\u9fff]{2,3}", text):
        return "对方单位过短"
    return ""


def _split_account_prefixed_counterparty(tx: dict[str, Any]) -> None:
    value = _clean(tx.get("对方单位") or tx.get("counterparty_name"))
    match = re.fullmatch(r"\s*(\d{8,32})\s*([\u4e00-\u9fff].{1,60})\s*", value)
    if match:
        if not (tx.get("对方账号") or tx.get("counterparty_account")):
            tx["对方账号"] = match.group(1)
            tx["counterparty_account"] = match.group(1)
        tx["对方单位"] = _clean(match.group(2))
        tx["counterparty_name"] = tx["对方单位"]
    if _normalize_entity_name(tx.get("对方单位")) == _normalize_entity_name("企业网上银行"):
        tx["channel"] = "企业网上银行"
        tx["对方单位"] = ""
        tx["counterparty_name"] = ""


def _is_page_block_transaction(tx: dict[str, Any]) -> bool:
    raw = " ".join(str(tx.get(key) or "") for key in ("raw_line_text", "用途", "摘要", "备注"))
    header_hits = sum(marker in raw for marker in SHANGHAI_PAGE_BLOCK_HEADERS)
    purpose = _clean(tx.get("用途") or tx.get("purpose"))
    summary = _clean(tx.get("摘要") or tx.get("summary"))
    counterparty = _clean(tx.get("对方单位") or tx.get("counterparty_name"))
    return header_hits >= 2 or len(purpose) > 80 or len(summary) > 80 or len(counterparty) > 60


def _infer_account_name_from_counterparties(result: dict[str, Any]) -> None:
    if result.get("account_name") or result.get("bank_format") != BANK_FORMAT_SHANGHAI:
        return
    stats: dict[str, dict[str, Any]] = {}
    for tx in result.get("transactions") or []:
        _split_account_prefixed_counterparty(tx)
        name = _clean(tx.get("对方单位") or tx.get("counterparty_name"))
        if _invalid_counterparty_reason(name) or _is_page_block_transaction(tx):
            continue
        normalized = _normalize_entity_name(name)
        if not normalized or not re.search(r"(?:有限责任公司|有限公司|公司)$", normalized):
            continue
        item = stats.setdefault(normalized, {"name": re.sub(r"[\s　]+", "", name), "count": 0, "directions": set()})
        item["count"] += 1
        direction = tx.get("收支方向") or tx.get("direction")
        if direction in {"入账", "出账"}:
            item["directions"].add(direction)
    eligible = [item for item in stats.values() if item["count"] >= 2 and item["directions"] == {"入账", "出账"}]
    if not eligible:
        return
    selected = max(eligible, key=lambda item: (item["count"], len(item["name"])))
    result["account_name"] = selected["name"]
    result["account_name_source"] = "high_frequency_counterparty_fallback"
    result["account_name_needs_review"] = True


def _is_bank_internal_counterparty(value: Any) -> bool:
    compact = _normalize_entity_name(value)
    return any(keyword in compact for keyword in ("中国工商银行", "工商银行", "银行系统", "银行内部"))


def _valid_transaction_datetime(value: Any, period_start: str, period_end: str, *, allow_date_only: bool = False) -> bool:
    text = str(value or "").strip()
    expected = r"\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?" if allow_date_only else r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
    if not re.fullmatch(expected, text):
        return False
    try:
        trade_time = datetime.strptime(text, "%Y-%m-%d %H:%M:%S" if " " in text else "%Y-%m-%d")
        start = datetime.strptime(period_start, "%Y-%m-%d") if period_start else None
        end = datetime.strptime(period_end, "%Y-%m-%d") if period_end else None
    except ValueError:
        return False
    if start and trade_time < start:
        return False
    if end and trade_time.date() > end.date():
        return False
    return True


def _clean_and_mark_transactions(result: dict[str, Any]) -> None:
    account_name_normalized = _normalize_entity_name(result.get("account_name") or result.get("customer_name"))
    related_roles = result.get("related_person_roles") if isinstance(result.get("related_person_roles"), dict) else {}
    period_start = str(result.get("period_start") or "")
    period_end = str(result.get("period_end") or "")
    for tx in result.get("transactions") or []:
        _split_account_prefixed_counterparty(tx)
        counterparty = tx.get("对方单位") or tx.get("counterparty_name") or ""
        normalized_counterparty = _normalize_entity_name(counterparty)
        normalized_person = normalize_person_name(counterparty)
        related_person = related_roles.get(normalized_person) if normalized_person else None
        is_related_person = bool(related_person)
        invalid_counterparty = _invalid_counterparty_reason(counterparty)
        is_self = bool(
            account_name_normalized and (
                normalized_counterparty == account_name_normalized or
                (normalized_counterparty.startswith(account_name_normalized) and any(marker in normalized_counterparty for marker in ("专户", "工资", "农民工")))
            )
        )
        is_valid_time = _valid_transaction_datetime(
            tx.get("交易时间") or tx.get("transaction_time"), period_start, period_end,
            allow_date_only=result.get("bank_format") in {BANK_FORMAT_SHANGHAI, BANK_FORMAT_BOCM},
        )
        category = classify_transaction(tx)
        raw_text = " ".join(str(tx.get(key) or "") for key in ("用途", "摘要", "备注", "回单个性化信息"))
        is_bank_fee = category == "银行费用"
        is_loan = category in {"贷款发放", "贷款归还", "资金拆借"}
        is_interest = category in {"利息收入", "利息支出"} or "利息" in raw_text
        is_tax = any(marker in raw_text for marker in ("缴税", "扣款（缴税）"))
        is_payroll = "代发专用账户" in counterparty or any(marker in raw_text for marker in ("工资", "年终奖"))
        is_bank_internal = _is_bank_internal_counterparty(counterparty)
        is_page_block = result.get("bank_format") == BANK_FORMAT_SHANGHAI and _is_page_block_transaction(tx)
        is_ocr_anomaly = not is_valid_time or is_page_block or bool(invalid_counterparty and invalid_counterparty != "对方单位为空")

        reasons: list[str] = []
        if is_self:
            reasons.append("本方户名与对方单位一致或本方关联账户划转")
        if is_related_person:
            reasons.append("公司账户与法人/关联人之间转账，已从有效经营流水中剔除")
        if is_bank_fee:
            reasons.append("银行手续费或账户服务费用")
        if is_loan or is_interest:
            reasons.append("贷款及利息相关交易")
        if is_tax:
            reasons.append("税费交易")
        if is_payroll:
            reasons.append("工资代发交易")
        if is_bank_internal:
            reasons.append("银行系统内部交易对手")
        if not is_valid_time:
            reasons.append("交易时间无效或超出对账单时间范围")
        elif is_page_block:
            reasons.append("整页列块误合并为单条交易")
        elif invalid_counterparty:
            reasons.append(invalid_counterparty)

        tx["交易分类"] = category
        tx["is_valid_transaction"] = is_valid_time and not is_page_block
        tx["is_page_block"] = is_page_block
        tx["invalid_reason"] = "整页列块误合并为单条交易" if is_page_block else ""
        tx["is_self_transfer"] = is_self
        tx["is_related_person_transfer"] = is_related_person
        tx["related_person_name"] = (related_person or {}).get("name", "") if isinstance(related_person, dict) else ""
        tx["related_person_role"] = (related_person or {}).get("role", "") if isinstance(related_person, dict) else ""
        tx["is_bank_fee"] = is_bank_fee
        tx["is_tax_payment"] = is_tax
        tx["is_salary_payment"] = is_payroll
        tx["is_loan_related"] = is_loan
        tx["is_interest_related"] = is_interest
        tx["is_ocr_anomaly"] = is_ocr_anomaly
        tx["is_bank_internal_counterparty"] = is_bank_internal
        tx["exclude_from_effective_flow"] = bool(reasons)
        tx["exclude_reason"] = "；".join(dict.fromkeys(reasons))
        tx["clean_counterparty_name"] = "" if invalid_counterparty or is_page_block else _clean(counterparty)[:40]
        tx["clean_summary"] = "" if is_page_block else _clean_display_text(tx.get("摘要") or tx.get("summary"))[:40]
        tx["clean_purpose"] = "" if is_page_block else _clean_display_text(tx.get("用途") or tx.get("purpose"))[:40]
        tx["display_remark"] = _clean_display_text(tx.get("备注") or tx.get("remark"))
        tx.update({
            "category": category, "is_self_transfer": is_self, "is_related_person_transfer": is_related_person,
            "related_person_name": tx["related_person_name"], "related_person_role": tx["related_person_role"],
            "is_tax_payment": is_tax, "is_salary_payment": is_payroll,
            "exclude_from_effective_flow": bool(reasons), "exclude_reason": tx["exclude_reason"],
            "clean_counterparty_name": tx["clean_counterparty_name"], "clean_summary": tx["clean_summary"],
            "clean_purpose": tx["clean_purpose"], "display_remark": tx["display_remark"],
        })


def _transactions_from_pages(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    transactions: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    explicit_amount_column = False
    pages = raw_pages or [{"page": 1, "text": text, "table_rows": []}]
    continuation_markers = ("附言", "指令编号", "支付交易序号", "报文种类", "提交人", "产品名称", "费用名称", "应收金额", "实收金额", "起息日期", "止息日期", "利率", "利息", "贷款账号", "贷款帐号", "借据编号", "处理种类")
    for page_item in pages:
        page = int(page_item.get("page") or 0)
        rows = page_item.get("table_rows") if isinstance(page_item.get("table_rows"), list) else []
        normalized_rows = [[_clean(cell) for cell in row] for row in rows if isinstance(row, (list, tuple))]
        # Prefer true table rows, then visually delimited native/OCR lines.
        lines = str(page_item.get("text") or "").splitlines()
        normalized_rows.extend([[_clean(cell) for cell in re.split(r"\s*\|\s*|\t+", line)] for line in lines if "|" in line or "\t" in line])
        mapping: dict[int, str] = {}
        page_before = len(transactions)
        for cells in normalized_rows:
            candidate_mapping = _header_mapping(cells)
            if len(candidate_mapping) >= 3 and "交易时间" in candidate_mapping.values():
                mapping = candidate_mapping
                explicit_amount_column = explicit_amount_column or any(field in mapping.values() for field in ("金额", "交易金额", "发生额", "借方发生额", "贷方发生额", "收入", "支出"))
                continue
            if not mapping:
                continue
            row_values = {field: cells[index] if index < len(cells) else "" for index, field in mapping.items()}
            tx_date = _date(row_values.get("交易时间", ""))
            debit_credit = _clean(row_values.get("借贷标志", ""))[:1]
            if tx_date:
                tx = _new_tx(page)
                tx.update({key: _clean(value) for key, value in row_values.items() if key in tx and key not in {"金额", "余额"}})
                tx["交易时间"] = tx_date
                tx["借贷标志"] = debit_credit if debit_credit in {"借", "贷"} else ""
                tx["收支方向"] = "入账" if debit_credit == "贷" else ("出账" if debit_credit == "借" else "未识别")
                tx["交易分类"] = classify_transaction(tx)
                explicit_amount = row_values.get("金额") or row_values.get("交易金额") or row_values.get("发生额")
                if not explicit_amount:
                    explicit_amount = row_values.get("贷方发生额") or row_values.get("收入") if debit_credit == "贷" else row_values.get("借方发生额") or row_values.get("支出")
                _extract_amount(tx, explicit_amount or "", row_values.get("余额", ""))
                transactions.append(tx)
            elif transactions and any(marker in " ".join(cells) for marker in continuation_markers):
                _append_info(transactions[-1], " ".join(filter(None, cells)))
                transactions[-1]["交易分类"] = classify_transaction(transactions[-1])
                _extract_amount(transactions[-1])
        # Native-text fallback. Borrow/lend columns are optional: the stable anchor is
        # voucher + counterparty account + date + time, followed by continuation text.
        page_text = str(page_item.get("text") or "")
        for match in TRANSACTION_ANCHOR_RE.finditer(page_text):
            rest = _clean(match.group("rest"))
            tx = _new_tx(page)
            tx["凭证号"] = match.group("voucher_no")
            tx["对方账号"] = match.group("counterparty_account")
            tx["交易时间"] = f"{match.group('trade_date')} {match.group('trade_time')}"
            side_match = re.search(r"(?:^|\s)(借|贷)(?:\s|$)", rest)
            tx["借贷标志"] = side_match.group(1) if side_match else "未识别"
            tx["收支方向"] = "入账" if tx["借贷标志"] == "贷" else ("出账" if tx["借贷标志"] == "借" else "未识别")
            tx["回单个性化信息"] = rest
            process_match = re.search(r"处理种类\s*[:：]\s*([^:：；;]{1,80})", rest)
            appendix_match = re.search(r"附言\s*[:：]\s*([^:：；;]{1,120})", rest)
            purpose_match = re.search(r"用途\s*[:：]\s*([^:：；;]{1,120})", rest)
            tx["摘要"] = _clean(process_match.group(1)) if process_match else ""
            tx["备注"] = _clean(appendix_match.group(1)) if appendix_match else ""
            tx["用途"] = _clean(purpose_match.group(1)) if purpose_match else ""
            tx["交易分类"] = classify_transaction(tx)
            _extract_amount(tx)
            transactions.append(tx)
    # Deduplicate coordinate/table representations of the same row.
    unique: list[dict[str, Any]] = []
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    for tx in transactions:
        if tx.get("凭证号") and tx.get("对方账号") and tx.get("交易时间"):
            key = (tx.get("凭证号"), tx.get("对方账号"), tx.get("交易时间"), tx.get("来源页码"))
        else:
            key = (tx.get("凭证号"), tx.get("对方账号"), tx.get("交易时间"), tx.get("借贷标志"), tx.get("对方单位"), tx.get("摘要"), tx.get("来源页码"))
        if key in seen:
            existing = seen[key]
            for field in ("借贷标志", "收支方向", "对方单位", "对方行号", "用途", "摘要", "备注", "金额", "余额", "金额来源"):
                if existing.get(field) in (None, "", "未识别") and tx.get(field) not in (None, "", "未识别"):
                    existing[field] = tx[field]
            _append_info(existing, str(tx.get("回单个性化信息") or ""))
            existing["交易分类"] = classify_transaction(existing)
            _extract_amount(existing)
            continue
        seen[key] = tx
        unique.append(tx)
    unique.sort(key=lambda item: (str(item.get("交易时间") or ""), int(item.get("来源页码") or 0)))
    for index, tx in enumerate(unique, start=1):
        tx["序号"] = index
        tx.update({
            "voucher_no": tx.get("凭证号") or "",
            "counterparty_account": tx.get("对方账号") or "",
            "transaction_time": tx.get("交易时间") or "",
            "debit_credit_flag": tx.get("借贷标志") or "未识别",
            "direction": tx.get("收支方向") or "未识别",
            "counterparty_name": tx.get("对方单位") or "",
            "counterparty_bank_no": tx.get("对方行号") or "",
            "purpose": tx.get("用途") or "",
            "summary": tx.get("摘要") or "",
            "remark": tx.get("备注") or "",
            "amount": tx.get("金额"),
            "balance": tx.get("余额"),
            "receipt_info": tx.get("回单个性化信息") or "",
            "category": tx.get("交易分类") or "其他",
            "source_page": tx.get("来源页码") or 0,
        })
        evidence.append({"field": "交易明细", "page": tx["来源页码"], "record": index, "locator": f"凭证号={tx.get('凭证号') or '未识别'};交易时间={tx.get('交易时间')}"})
    return unique, evidence, explicit_amount_column


def parse_icbc_statement(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, dict[str, Any]]:
    transactions, evidence, amount_column = _transactions_from_pages(raw_pages, text)
    return transactions, evidence, amount_column, {
        "table_headers_detected": [], "candidate_transaction_rows": len(transactions),
        "invalid_candidate_rows": 0, "raw_text_blocks_count": sum(len(item.get("table_rows") or []) for item in raw_pages),
    }


def parse_generic_bank_statement(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, dict[str, Any]]:
    transactions, evidence, amount_column = _transactions_from_pages(raw_pages, text)
    return transactions, evidence, amount_column, {
        "table_headers_detected": [], "candidate_transaction_rows": len(transactions),
        "invalid_candidate_rows": 0, "raw_text_blocks_count": sum(len(item.get("table_rows") or []) for item in raw_pages),
    }


def _bocm_date(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    if len(text) >= 8:
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8])).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return _date(str(value or ""))


def _bocm_header_mapping(cells: list[str]) -> dict[int, str]:
    fields = {
        "序号": "transaction_index",
        "会计日期": "accounting_date",
        "交易日期": "trade_date",
        "交易名称": "transaction_name",
        "凭证种类": "voucher_type",
        "凭证号码": "voucher_no",
        "借方发生额": "debit_amount",
        "贷方发生额": "credit_amount",
        "余额": "balance",
        "卡号": "card_no",
        "交易地点": "transaction_location",
        "对方账号": "counterparty_account",
        "对方户名": "counterparty_name",
        "对方行名": "counterparty_bank",
        "摘要": "summary",
        "流水号": "serial_no",
    }
    mapping: dict[int, str] = {}
    for index, cell in enumerate(cells):
        compact = _clean(cell).replace(" ", "")
        for label, field in fields.items():
            if label in compact:
                mapping[index] = field
                break
    return mapping


def _bocm_tx_from_fields(fields: dict[str, Any], page: int, previous_balance: Decimal | None = None) -> tuple[dict[str, Any] | None, str]:
    index_text = str(fields.get("transaction_index") or "").strip()
    if not re.fullmatch(r"\d{1,6}", index_text):
        return None, "missing_index"
    trade_date = _bocm_date(fields.get("trade_date") or fields.get("accounting_date"))
    accounting_date = _bocm_date(fields.get("accounting_date") or fields.get("trade_date"))
    if not (trade_date or accounting_date):
        return None, "missing_date"
    debit = _positive_amount(fields.get("debit_amount"))
    credit = _positive_amount(fields.get("credit_amount"))
    balance = _decimal(fields.get("balance"))
    amount = debit or credit
    direction = "出账" if debit is not None else ("入账" if credit is not None else "未识别")
    if amount is None and previous_balance is not None and balance is not None:
        delta = balance - previous_balance
        if delta != 0:
            amount = abs(delta)
            direction = "入账" if delta > 0 else "出账"
            if delta > 0:
                credit = amount
            else:
                debit = amount
    support = any(_clean(fields.get(key)) for key in ("transaction_name", "counterparty_account", "counterparty_name", "counterparty_bank", "summary", "serial_no"))
    if amount is None:
        return None, "missing_amount"
    if not support:
        return None, "missing_transaction_fields"
    tx = _new_tx(page)
    tx["序号"] = int(index_text)
    tx["凭证号"] = _clean(fields.get("voucher_no") or fields.get("serial_no") or "")
    tx["对方账号"] = _clean(fields.get("counterparty_account") or "")
    tx["对方单位"] = _clean(fields.get("counterparty_name") or "")
    tx["对方行号"] = _clean(fields.get("counterparty_bank") or "")
    tx["交易时间"] = trade_date or accounting_date
    tx["借贷标志"] = "借" if direction == "出账" else ("贷" if direction == "入账" else "未识别")
    tx["收支方向"] = direction
    tx["用途"] = _clean(fields.get("transaction_name") or "")
    tx["摘要"] = _clean(fields.get("summary") or "")
    tx["备注"] = _clean(fields.get("transaction_location") or fields.get("voucher_type") or "")
    tx["金额"] = amount
    tx["余额"] = balance
    tx["金额来源"] = "借方发生额" if debit is not None else ("贷方发生额" if credit is not None else "余额变化")
    tx["交易分类"] = classify_transaction(tx)
    tx["accounting_date"] = accounting_date
    tx["trade_date"] = trade_date
    tx["transaction_name"] = _clean(fields.get("transaction_name") or "")
    tx["debit_amount"] = debit
    tx["credit_amount"] = credit
    tx["counterparty_bank"] = tx["对方行号"]
    tx["serial_no"] = _clean(fields.get("serial_no") or "")
    tx["is_valid_transaction"] = True
    return tx, ""


def _parse_bocm_text_rows(page_text: str, page: int, previous_balance: Decimal | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Decimal | None]:
    transactions: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    footer_keywords = ("承前", "本月累计", "当前账单", "出单截至日期", "以下此页无其他交易信息", "页码", "对账专用章")
    lines = [_clean(line) for line in str(page_text or "").splitlines() if _clean(line)]
    if previous_balance is None:
        for line in lines:
            if "承前" not in line:
                continue
            balance_candidates = re.findall(r"(?<!\d)(?:\d{1,3}(?:,\d{3})+|\d+)\.\d{2}(?!\d)", line)
            if balance_candidates:
                previous_balance = _decimal(balance_candidates[-1])
                break
    blocks: list[str] = []
    current = ""
    anchor = re.compile(r"^\s*(\d{1,6})\s+(20\d{6})\s+(20\d{6})\b")
    for line in lines:
        if any(keyword in line for keyword in footer_keywords):
            continue
        if anchor.search(line):
            if current:
                blocks.append(current)
            current = line
        elif current:
            current = f"{current} {line}"
    if current:
        blocks.append(current)
    for block in blocks:
        match = anchor.search(block)
        if not match:
            continue
        rest = block[match.end():].strip()
        amounts = [(m.group(0), m.start()) for m in re.finditer(r"(?<!\d)(?:\d{1,3}(?:,\d{3})+|\d+)\.\d{2}(?!\d)", rest)]
        if len(amounts) < 2:
            rejected.append({"page": page, "reason": "missing_amount_columns", "text": block[:300]})
            continue
        balance_value = _decimal(amounts[-1][0])
        amount_value: Decimal | None = None
        direction = "未识别"
        if previous_balance is not None and balance_value is not None:
            for amount_text, _pos in amounts[:-1]:
                candidate = _decimal(amount_text)
                if candidate is None:
                    continue
                if abs((balance_value - previous_balance) - candidate) <= Decimal("0.01"):
                    amount_value = candidate
                    direction = "入账"
                    break
                if abs((previous_balance - balance_value) - candidate) <= Decimal("0.01"):
                    amount_value = candidate
                    direction = "出账"
                    break
        if amount_value is None:
            amount_value = _decimal(amounts[0][0])
            if any(keyword in block for keyword in ("手续费", "服务费", "扣费", "扣款", "支付", "支出")):
                direction = "出账"
            elif any(keyword in block for keyword in ("收款", "入账", "收入", "货款")):
                direction = "入账"
        first_amount_pos = amounts[0][1]
        last_amount_end = amounts[-1][1] + len(amounts[-1][0])
        before_amount = rest[:first_amount_pos].strip()
        after_balance = rest[last_amount_end:].strip()
        account_match = re.search(r"(?<!\d)(\d{8,32})(?!\d)", after_balance)
        counterparty_account = account_match.group(1) if account_match else ""
        after_account = after_balance[account_match.end():].strip() if account_match else after_balance
        serial_match = re.search(r"([A-Za-z0-9._-]{8,})\s*$", after_account)
        serial_no = serial_match.group(1) if serial_match else ""
        if serial_no:
            after_account = after_account[:serial_match.start()].strip()
        counterparty_bank = ""
        bank_match = re.search(
            r"((?:中国工商|中国建设|中国农业|中国银行|交通|招商|华夏|浦发|中信|民生|平安|兴业|光大|广发|江苏|南京|上海)?银行[^ ]{0,60}(?:支行|分行|营业部)?)",
            after_account,
        )
        if bank_match:
            counterparty_bank = bank_match.group(1)
            counterparty_name = after_account[:bank_match.start()].strip()
            summary = after_account[bank_match.end():].strip()
        else:
            parts = after_account.split()
            counterparty_name = parts[0] if parts else ""
            summary = " ".join(parts[1:]) if len(parts) > 1 else ""
        fields = {
            "transaction_index": match.group(1),
            "accounting_date": match.group(2),
            "trade_date": match.group(3),
            "transaction_name": before_amount.split()[0] if before_amount else "",
            "debit_amount": str(amount_value) if direction == "出账" else "",
            "credit_amount": str(amount_value) if direction == "入账" else "",
            "balance": amounts[-1][0],
            "counterparty_account": counterparty_account,
            "counterparty_name": counterparty_name,
            "counterparty_bank": counterparty_bank,
            "summary": summary,
            "serial_no": serial_no,
        }
        tx, reason = _bocm_tx_from_fields(fields, page, previous_balance)
        if tx:
            if direction == "未识别":
                tx["收支方向"] = "未识别"
                tx["借贷标志"] = "未识别"
            transactions.append(tx)
            previous_balance = tx.get("余额") if tx.get("余额") is not None else previous_balance
        else:
            rejected.append({"page": page, "reason": reason, "text": block[:300]})
    return transactions, rejected, previous_balance


def parse_bocm_statement_transactions(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool, dict[str, Any]]:
    transactions: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    pages = raw_pages or [{"page": 1, "text": text, "table_rows": []}]
    explicit_amount_column = False
    previous_balance: Decimal | None = None
    for page_item in pages:
        page = int(page_item.get("page") or 1)
        rows = page_item.get("table_rows") if isinstance(page_item.get("table_rows"), list) else []
        mapping: dict[int, str] = {}
        for raw_row in rows:
            if not isinstance(raw_row, (list, tuple)):
                continue
            cells = [_clean(cell) for cell in raw_row]
            candidate = _bocm_header_mapping(cells)
            if len(candidate) >= 6 and {"accounting_date", "trade_date", "debit_amount", "credit_amount", "balance"} & set(candidate.values()):
                mapping = candidate
                explicit_amount_column = True
                continue
            if not mapping:
                continue
            fields = {field: cells[index] if index < len(cells) else "" for index, field in mapping.items()}
            tx, reason = _bocm_tx_from_fields(fields, page, previous_balance)
            if tx:
                transactions.append(tx)
                previous_balance = tx.get("余额") if tx.get("余额") is not None else previous_balance
            elif any(fields.values()):
                rejected.append({"page": page, "reason": reason, "fields": fields})
        text_txs, text_rejected, previous_balance = _parse_bocm_text_rows(str(page_item.get("text") or ""), page, previous_balance)
        transactions.extend(text_txs)
        rejected.extend(text_rejected)
    unique: list[dict[str, Any]] = []
    seen = set()
    for tx in transactions:
        key = (tx.get("序号"), tx.get("交易时间"), tx.get("金额"), tx.get("余额"), tx.get("对方账号"), tx.get("来源页码"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(tx)
        evidence.append({"type": "bocm_transaction", "page": tx.get("来源页码"), "index": tx.get("序号")})
    diagnostics = {
        "parser_path": "bocm_statement",
        "table_headers_detected": ["序号", "会计日期", "交易日期", "借方发生额", "贷方发生额", "余额", "对方账号", "对方户名", "对方行名", "摘要", "流水号"],
        "candidate_transaction_rows": len(unique) + len(rejected),
        "invalid_candidate_rows": len(rejected),
        "raw_text_blocks_count": sum(len(str(item.get("text") or "").splitlines()) for item in pages),
        "reject_reasons": dict(Counter(str(item.get("reason") or "unknown") for item in rejected)),
    }
    logger.info("[BOCMStatementParser] parsed_transactions_count=%s", len(unique))
    logger.info("[BOCMStatementParser] first_10_transactions=%s", _json_safe(unique[:10]))
    if not unique:
        combined = "\n".join(str(item.get("text") or "") for item in pages)
        header_pos = min([pos for pos in (combined.find("序号"), combined.find("会计日期"), combined.find("借方发生额")) if pos >= 0] or [0])
        logger.warning("[BOCMStatementParser] header_near_text=%s", combined[header_pos:header_pos + 1500])
        logger.warning("[BOCMStatementParser] first_page_3000=%s", str(pages[0].get("text") or "")[:3000] if pages else "")
        logger.warning("[BOCMStatementParser] candidate_rows=%s reject_reasons=%s", diagnostics["candidate_transaction_rows"], diagnostics["reject_reasons"])
    return unique, evidence, True, diagnostics


def _summary(result: dict[str, Any]) -> None:
    txs = result["transactions"]
    valid = [tx for tx in txs if tx.get("is_valid_transaction")]
    effective = [tx for tx in valid if not tx.get("exclude_from_effective_flow")]
    recognized = [tx for tx in valid if tx.get("金额") is not None]
    amount_ratio = (len(recognized) / len(valid)) if valid else 0.0
    result["amount_recognition_status"] = "完整识别" if amount_ratio >= 0.8 else ("部分识别" if recognized else "未识别")
    result["amount_recognition_ratio"] = round(amount_ratio, 4)
    result["transaction_count"] = len(txs)
    result["raw_transaction_count"] = len(txs)
    result["valid_transaction_count"] = len(valid)
    result["effective_transaction_count"] = len(effective)
    result["self_transfer_count"] = sum(bool(tx.get("is_self_transfer")) for tx in valid)
    result["related_person_transfer_count"] = sum(bool(tx.get("is_related_person_transfer")) for tx in valid)
    result["bank_fee_count"] = sum(bool(tx.get("is_bank_fee")) for tx in valid)
    result["tax_payment_count"] = sum(bool(tx.get("is_tax_payment")) for tx in valid)
    result["salary_payment_count"] = sum(bool(tx.get("is_salary_payment")) for tx in valid)
    result["loan_interest_count"] = sum(bool(tx.get("is_loan_related") or tx.get("is_interest_related")) for tx in valid)
    result["ocr_anomaly_count"] = sum(bool(tx.get("is_ocr_anomaly")) for tx in txs)
    result["inflow_count"] = sum(tx.get("收支方向") == "入账" for tx in valid)
    result["outflow_count"] = sum(tx.get("收支方向") == "出账" for tx in valid)
    result["effective_inflow_count"] = sum(tx.get("收支方向") == "入账" for tx in effective)
    result["effective_outflow_count"] = sum(tx.get("收支方向") == "出账" for tx in effective)
    result["recognizable_inflow"] = sum((tx["金额"] for tx in recognized if tx.get("收支方向") == "入账"), Decimal("0"))
    result["recognizable_outflow"] = sum((tx["金额"] for tx in recognized if tx.get("收支方向") == "出账"), Decimal("0"))
    transaction_dates = [str(tx.get("交易时间") or "")[:10] for tx in valid]
    has_direction = any(tx.get("借贷标志") in {"借", "贷"} for tx in valid)
    result["debit_count"] = result["outflow_count"] if has_direction else None
    result["credit_count"] = result["inflow_count"] if has_direction else None
    result["debit_total_amount"] = result["recognizable_outflow"] if valid and len(recognized) == len(valid) and has_direction else None
    result["credit_total_amount"] = result["recognizable_inflow"] if valid and len(recognized) == len(valid) and has_direction else None
    result["first_transaction_date"] = min(transaction_dates) if transaction_dates else ""
    result["last_transaction_date"] = max(transaction_dates) if transaction_dates else ""
    inflow_amounts = [tx["金额"] for tx in recognized if tx.get("收支方向") == "入账"]
    outflow_amounts = [tx["金额"] for tx in recognized if tx.get("收支方向") == "出账"]
    result["max_in_amount"] = max(inflow_amounts) if inflow_amounts else None
    result["max_out_amount"] = max(outflow_amounts) if outflow_amounts else None

    def aggregate(items: list[dict[str, Any]], direction: str) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        for tx in items:
            if tx.get("收支方向") != direction:
                continue
            name = str(tx.get("clean_counterparty_name") or "").strip()
            if not name:
                continue
            item = groups.setdefault(name, {"counterparty": name, "count": 0, "recognizable_amount": Decimal("0"), "recognized_amount_count": 0, "descriptions": Counter()})
            item["count"] += 1
            if tx.get("金额") is not None:
                item["recognizable_amount"] += tx["金额"]
                item["recognized_amount_count"] += 1
            desc = tx.get("clean_purpose") or tx.get("clean_summary")
            if desc:
                item["descriptions"][desc] += 1
        ranked = sorted(groups.values(), key=lambda item: (-item["count"], -item["recognizable_amount"], item["counterparty"]))
        return [{**item, "main_description": "、".join(name for name, _ in item.pop("descriptions").most_common(3))} for item in ranked]

    result["effective_transactions"] = effective
    operating_inflow = [tx for tx in effective if tx.get("交易分类") == "经营入账" and tx.get("收支方向") == "入账"]
    operating_outflow = [tx for tx in effective if tx.get("交易分类") == "经营出账" and tx.get("收支方向") == "出账"]
    result["effective_operating_inflow_transactions"] = operating_inflow
    result["effective_operating_outflow_transactions"] = operating_outflow
    result["effective_operating_inflow_count"] = len(operating_inflow)
    result["effective_operating_outflow_count"] = len(operating_outflow)
    result["effective_inflow_counterparties"] = aggregate(operating_inflow, "入账")
    result["effective_outflow_counterparties"] = aggregate(operating_outflow, "出账")
    result["effective_operating_inflow_counterparty_count"] = len(result["effective_inflow_counterparties"])
    result["effective_operating_outflow_counterparty_count"] = len(result["effective_outflow_counterparties"])
    result["counterparty_summary"] = result["effective_inflow_counterparties"] + result["effective_outflow_counterparties"]
    category_names = ("经营入账", "经营出账", "往来入账", "往来出账", "贷款发放", "贷款归还", "利息收入", "利息支出", "银行费用", "资金拆借", "其他")
    result["category_summary"] = [
        {"category": name, "count": sum(tx.get("交易分类") == name for tx in valid), "recognizable_amount": sum((tx["金额"] for tx in recognized if tx.get("交易分类") == name), Decimal("0"))}
        for name in category_names
    ]
    result["loan_related_transactions"] = [tx for tx in valid if tx.get("is_loan_related") or tx.get("is_interest_related")]
    result["fee_interest_transactions"] = [tx for tx in valid if tx.get("is_bank_fee") or tx.get("is_interest_related")]
    result["related_internal_transactions"] = [tx for tx in valid if tx.get("is_self_transfer") or tx.get("is_related_person_transfer")]
    result["focus_transactions"] = sorted(effective + result["loan_related_transactions"], key=lambda tx: str(tx.get("交易时间") or ""))
    result["exclusion_summary"] = [
        {"type": "本方同名划转", "count": result["self_transfer_count"], "description": "本方户名与对方单位一致，疑似同主体账户划转"},
        {"type": "关联人转账", "count": result["related_person_transfer_count"], "description": "公司账户与法人/实控人/股东/高管等关联个人之间的转账"},
        {"type": "银行手续费", "count": result["bank_fee_count"], "description": "手续费、年费、短信费、ETC 等"},
        {"type": "税费", "count": result["tax_payment_count"], "description": "缴税、扣款缴税"},
        {"type": "工资代发", "count": result["salary_payment_count"], "description": "代发工资、年终奖等"},
        {"type": "贷款及利息", "count": result["loan_interest_count"], "description": "贷款发放、贷款归还、贷款利息、融资租赁"},
        {"type": "OCR异常行", "count": result["ocr_anomaly_count"], "description": "无效日期或说明字段被误识别为交易记录、交易对手"},
    ]


def _apply_header_summary(result: dict[str, Any]) -> None:
    header = (result.get("parse_diagnostics") or {}).get("header_summary") or {}
    if result.get("bank_format") != BANK_FORMAT_SHANGHAI or not header:
        return
    parsed_count = len(result.get("transactions") or [])
    if header.get("transaction_count"):
        result["transaction_count"] = int(header["transaction_count"])
        result["raw_transaction_count"] = int(header["transaction_count"])
        result["header_transaction_count"] = int(header["transaction_count"])
        result["parsed_transaction_count"] = parsed_count
        if parsed_count and parsed_count != int(header["transaction_count"]):
            result["parse_completeness"] = f"{parsed_count}/{int(header['transaction_count'])}"
    if header.get("debit_count") is not None:
        result["debit_count"] = int(header["debit_count"])
    if header.get("credit_count") is not None:
        result["credit_count"] = int(header["credit_count"])
    if header.get("debit_total_amount") is not None:
        result["debit_total_amount"] = header["debit_total_amount"]
    if header.get("credit_total_amount") is not None:
        result["credit_total_amount"] = header["credit_total_amount"]
    if header.get("currency"):
        result["currency"] = header["currency"]
    if header.get("period_start") and not result.get("period_start"):
        result["period_start"] = header["period_start"]
    if header.get("period_end") and not result.get("period_end"):
        result["period_end"] = header["period_end"]
    if result.get("period_start") and result.get("period_end"):
        result["period_text"] = f"{result['period_start']} 至 {result['period_end']}"
    if header.get("debit_total_amount") is not None and header.get("credit_total_amount") is not None:
        result["amount_recognition_status"] = "完整识别"


def _apply_parse_quality(result: dict[str, Any], *, customer_profile_name: str = "", amount_column_detected: bool = False) -> None:
    ok_name, _name_reason = validate_account_name(result.get("account_name"))
    account_name_valid = ok_name or bool(customer_profile_name)
    account_info_valid = bool(account_name_valid and result.get("account_no") and result.get("bank_name"))
    valid_transactions = [tx for tx in result.get("transactions") or [] if tx.get("is_valid_transaction") and not tx.get("is_page_block")]
    transaction_support = [
        tx for tx in valid_transactions
        if (tx.get("交易时间") or tx.get("transaction_time")) and (tx.get("金额") is not None or tx.get("收支方向") in {"入账", "出账"})
    ]
    transactions_valid = bool(transaction_support)
    amounts_valid = bool(amount_column_detected and any(tx.get("金额") is not None for tx in transaction_support))
    if result.get("amount_recognition_status") == "完整识别" and any(tx.get("金额") is not None for tx in transaction_support):
        amounts_valid = True
    subtype = result.get("statement_subtype") or "unknown_bank_statement"
    score = 0
    score += 35 if account_info_valid else (10 if account_name_valid or result.get("account_no") or result.get("bank_name") else 0)
    score += 40 if transactions_valid else 0
    score += 25 if amounts_valid else 0
    if account_info_valid and transactions_valid and amounts_valid:
        status = "success"
    elif not account_info_valid and not transactions_valid:
        status = "invalid_account_info" if subtype != "receipt_bundle" else "partial"
    elif not transactions_valid:
        status = "invalid_transaction_structure"
    else:
        status = "partial"
    result.update({
        "parse_quality_status": status,
        "parse_quality_score": score,
        "account_info_valid": account_info_valid,
        "transactions_valid": transactions_valid,
        "amounts_valid": amounts_valid,
        "can_join_aggregate": not (not account_info_valid and not transactions_valid),
        "can_join_amount_statistics": bool(transactions_valid and amounts_valid),
        "can_join_effective_flow_statistics": bool(transactions_valid and amounts_valid),
        "parsed_transactions_count": len(transaction_support),
    })
    if subtype == "receipt_bundle":
        result["can_join_effective_flow_statistics"] = False
        result["can_join_amount_statistics"] = False
        result["parse_quality_status"] = "partial"


def validate_bank_account_header(result: dict[str, Any]) -> None:
    if result.get("bank_format") != BANK_FORMAT_BOCM:
        return
    rejected: list[str] = []
    account_name = clean_bocm_header_field(result.get("account_name"), field="account_name")
    account_no = clean_bocm_header_field(result.get("account_no"), field="account_no")
    opening_bank = clean_bocm_header_field(result.get("opening_bank"), field="opening_bank")
    year = clean_bocm_header_field(result.get("statement_year"), field="year")
    month = clean_bocm_header_field(result.get("statement_month"), field="month")
    counterparty_names = {
        re.sub(r"[\s\u3000]+", "", str(tx.get("对方单位") or tx.get("counterparty_name") or ""))
        for tx in result.get("transactions") or []
        if str(tx.get("对方单位") or tx.get("counterparty_name") or "").strip()
    }
    compact_name = re.sub(r"[\s\u3000]+", "", account_name)
    ok_name, name_reason = validate_account_name(account_name)
    looks_like_counterparty = False
    if compact_name:
        for counterparty in counterparty_names:
            if not counterparty:
                continue
            if compact_name == counterparty:
                looks_like_counterparty = True
                break
            if len(compact_name) <= 12 and (counterparty.startswith(compact_name) or compact_name.startswith(counterparty[:len(compact_name)])):
                looks_like_counterparty = True
                break
    if not ok_name or looks_like_counterparty:
        if account_name:
            rejected.append(account_name)
        account_name = ""
        result.setdefault("manual_review_items", []).append("疑似将交易对手名称误识别为户名，已拦截，需人工复核本方户名。")
        logger.info("[BOCMHeaderParser] rejected_account_name_candidates=%s", rejected)
        logger.info("[BOCMHeaderParser] reject_reason=%s", "counterparty_name_pollution" if looks_like_counterparty else name_reason)
    if account_no and not re.fullmatch(r"\d{8,32}", account_no):
        account_no = ""
    if opening_bank and not any(token in opening_bank for token in ("银行", "支行", "分行", "营业部", "开户机构")):
        opening_bank = ""
    if year:
        current_year = date.today().year
        if not (2000 <= int(year) <= current_year + 1):
            year = ""
    if month and not re.fullmatch(r"0[1-9]|1[0-2]", month):
        month = ""
    result["account_name"] = account_name
    result["account_no"] = account_no
    result["opening_bank"] = opening_bank
    result["statement_year"] = year
    result["statement_month"] = month
    if not (result.get("period_start") and result.get("period_end")) and year and month:
        result["period_start"] = f"{year}-{month}-01"
        result["period_end"] = _month_end(int(year), int(month))
        result["period_text"] = f"{result['period_start']} 至 {result['period_end']}"
    if not (result.get("period_start") and result.get("period_end")):
        dates = sorted({str(tx.get("交易时间") or "")[:10] for tx in result.get("transactions") or [] if parse_valid_date(str(tx.get("交易时间") or "")[:10])})
        if dates:
            result["period_start"] = dates[0]
            result["period_end"] = dates[-1]
            result["period_text"] = f"{dates[0]} 至 {dates[-1]}"
    logger.info("[BOCMHeaderParser] final_account_name=%s", result.get("account_name") or "未识别")
    logger.info("[BOCMHeaderParser] final_account_no=%s", result.get("account_no") or "未识别")
    logger.info("[BOCMHeaderParser] final_opening_institution=%s", result.get("opening_bank") or "未识别")
    logger.info("[BOCMHeaderParser] final_year=%s", result.get("statement_year") or "未识别")
    logger.info("[BOCMHeaderParser] final_month=%s", result.get("statement_month") or "未识别")


def _cell(value: Any) -> str:
    return _clean(value).replace("|", "\\|") or "—"


def _display(value: Any) -> str:
    return _clean(value) if value not in (None, "") else "未识别"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def render_bank_statement_markdown(result: dict[str, Any]) -> str:
    if result.get("statement_subtype") == "receipt_bundle":
        lines = [
            "## 银行回单集合",
            "",
            "- 资料类型：银行回单集合",
            f"- 来源文件：{_cell(result.get('source_file'))}",
            "- 原件状态：可查看",
            "- 提取状态：部分成功",
            "- 说明：已识别为银行回单集合，但未形成标准账户流水明细，暂不纳入银行流水聚合分析。",
            "",
            "### 解析结果",
            "- 文件类型：疑似银行回单集合",
            f"- 识别银行：{_display(result.get('bank_name'))}",
            "- 是否形成账户流水明细：否",
            "- 是否纳入经营流水聚合：否",
            "- 原因：文件未包含可稳定识别的本方账号、账户户名、交易时间范围和标准流水表格。",
            "",
            "### 后续处理建议",
            "该文件更适合使用“银行回单集合 Agent”提取回单日期、收付款方、账号、金额、用途、摘要和回单编号。",
            "",
            "### 需人工复核",
            "- 当前文件疑似为银行回单集合，不是标准银行账户明细。",
            "- 未识别稳定的本方账号、本方户名和交易时间范围。",
            "- 暂未纳入客户级经营流水统计。",
            "- 建议上传银行账户明细/账户流水 PDF 或 Excel。",
        ]
        return "\n".join(lines).replace("None", "").replace("null", "").replace("undefined", "")

    nonstandard_low_quality = (
        result.get("statement_subtype") == "unknown_bank_statement"
        and (
            result.get("account_info_valid") is False
            or result.get("transactions_valid") is False
            or result.get("valid_transaction_count", 0) == 0
            or result.get("can_join_effective_flow_statistics") is False
            or (not result.get("account_no") and result.get("valid_transaction_count", 0) == 0)
        )
    )
    if nonstandard_low_quality:
        return "\n".join([
            "## 银行流水文件",
            "",
            "- 资料类型：银行流水文件",
            f"- 来源文件：{_cell(result.get('source_file'))}",
            "- 原件状态：可查看",
            "- 提取状态：部分成功",
            "- 说明：已识别为银行流水相关文件，但未形成标准账户流水明细，暂不纳入银行流水聚合分析。",
            "",
            "### 解析结果",
            "- 文件类型：未知银行对账单 / 非标准银行流水文件",
            f"- 识别银行：{_display(result.get('bank_name'))}",
            "- 是否形成账户流水明细：否",
            "- 是否纳入经营流水聚合：否",
            "- 原因：未识别本方账号、账户户名、交易时间范围和标准流水表格。",
            "",
            "### 后续处理建议",
            "建议人工确认该文件是否为标准银行账户流水。如为回单、凭证或截图类文件，应走对应的回单/凭证解析链路；如需用于经营流水分析，请上传银行账户明细/账户流水 PDF 或 Excel。",
            "",
            "### 需人工复核",
            "- 当前文件未形成标准银行账户流水明细。",
            "- 建议上传银行账户明细、账户流水 PDF 或 Excel。",
        ]).replace("None", "").replace("null", "").replace("undefined", "")

    lines = [
        "## 银行对账单", "", "- 资料类型：银行对账单",
        f"- 来源文件：{_cell(result.get('source_file'))}", "- 原件状态：可查看",
        f"- 提取状态：{result.get('extraction_status', '成功')}", "", "### 账户信息",
    ]
    if result.get("bank_format") == BANK_FORMAT_BOCM:
        lines.extend([
            f"- 户名：{_display(result.get('account_name'))}",
            f"- 开户机构：{_display(result.get('opening_bank'))}",
        ])
    else:
        lines.extend([
            f"- 客户名称：{_display(result.get('account_name'))}",
            f"- 开户行：{_display(result.get('opening_bank'))}",
        ])
    lines += [
        f"- 账号：{_display(result.get('account_no'))}", f"- 银行名称：{_display(result.get('bank_name'))}",
        f"- 对账单标题：{_display(result.get('statement_title'))}", f"- 币种：{_display(result.get('currency'))}",
        f"- 单位：{_display(result.get('unit'))}",
    ]
    if result.get("bank_format") == BANK_FORMAT_BOCM:
        lines.extend([
            f"- 年份：{_display(result.get('statement_year'))}",
            f"- 月份：{_display(result.get('statement_month'))}",
        ])
    lines += [
        f"- 时间范围：{_display(result.get('period_text'))}",
        f"- 页数：{result.get('page_count', 0)}页", f"- 金额识别状态：{_display(result.get('amount_recognition_status'))}",
    ]
    if result.get("bank_format") == BANK_FORMAT_SHANGHAI and (result.get("debit_total_amount") is not None or result.get("credit_total_amount") is not None or result.get("header_transaction_count")):
        parsed_count = result.get("parsed_transaction_count", len(result.get("transactions") or []))
        total_count = result.get("header_transaction_count") or result.get("transaction_count") or 0
        lines += [
            "", "### 汇总信息",
            f"- 借方总金额：{_money(result.get('debit_total_amount'))}",
            f"- 贷方总金额：{_money(result.get('credit_total_amount'))}",
            f"- 借方总笔数：{_display(result.get('debit_count'))}",
            f"- 贷方总笔数：{_display(result.get('credit_count'))}",
            f"- 总笔数：{_display(total_count)}",
            f"- 已解析交易笔数：{parsed_count}",
            f"- 解析完整率：{parsed_count}/{total_count}" if total_count else f"- 解析完整率：{parsed_count}/未识别",
        ]
    lines += [
        "", "### 流水分析摘要",
        f"- 原始交易笔数：{result.get('raw_transaction_count', 0)}",
        f"- 有效交易笔数：{result.get('effective_transaction_count', 0)}",
        f"- 本方同名划转笔数：{result.get('self_transfer_count', 0)}",
        "- 本方同名划转说明：本方户名与对方单位一致的交易已从经营流水分析中剔除",
        f"- 关联人转账笔数：{result.get('related_person_transfer_count', 0)}",
        "- 关联人转账说明：公司账户与法人/实控人/股东/高管等关联个人之间的转账，已从有效经营流水中剔除",
        f"- 有效入账笔数：{result.get('effective_inflow_count', 0)}",
        f"- 有效出账笔数：{result.get('effective_outflow_count', 0)}",
        f"- 有效经营入账方数量：{result.get('effective_operating_inflow_counterparty_count', 0)}",
        f"- 有效经营出账方数量：{result.get('effective_operating_outflow_counterparty_count', 0)}",
        f"- 银行费用笔数：{result.get('bank_fee_count', 0)}",
        f"- 贷款/利息相关笔数：{result.get('loan_interest_count', 0)}",
    ]
    if result.get("amount_recognition_status") != "完整识别":
        lines.append("- 金额识别说明：当前 PDF 主表金额列未完整识别，仅保留明确识别金额，不进行完整收入、支出和净流入测算")
    if result.get("valid_transaction_count", 0) == 0 and result.get("text_recognized"):
        bank_format_name = {BANK_FORMAT_ICBC: "工商银行", BANK_FORMAT_SHANGHAI: "上海银行", BANK_FORMAT_BOCM: "交通银行", BANK_FORMAT_GENERIC: "通用银行对账单"}.get(result.get("bank_format"), "通用银行对账单")
        headers = set((result.get("parse_diagnostics") or {}).get("table_headers_detected") or [])
        if headers and "transaction_date" not in headers:
            failure_reason = "已识别表头，但日期列识别失败"
        elif headers and not headers.intersection({"debit_amount", "credit_amount"}):
            failure_reason = "已识别表头，但借方/贷方金额列识别失败"
        elif headers and "counterparty_name" not in headers:
            failure_reason = "已识别表头，但对方户名列识别失败或坐标列切分失败"
        elif headers:
            failure_reason = "已识别表头，但交易行坐标列切分或行合并失败"
        else:
            failure_reason = f"未能识别{bank_format_name}交易明细表格结构"
        lines += [
            "", "### 解析诊断", "- 已识别文本：是", f"- 银行格式：{bank_format_name}",
            f"- 失败原因：{failure_reason}",
            "- 建议处理：请检查表头字段映射或 OCR 表格恢复",
        ]
        return "\n".join(lines)
    if result.get("bank_format") == BANK_FORMAT_SHANGHAI and (result.get("parse_diagnostics") or {}).get("parser_path") == "column_block":
        lines += [
            "", "### 解析质量提示",
            "- 上海银行对账单采用列块恢复方式解析。",
            "- 若单页交易被识别为整页块，已从有效流水中剔除。",
            "- 客户名称、账号或开户行如仍未识别，建议结合原件首页复核。",
        ]
    lines += [
        "", "### 有效经营入账方汇总",
        "- 说明：本表按入账方名称汇总展示，不是逐笔交易明细。",
        f"- 有效经营入账方数量：{result.get('effective_operating_inflow_counterparty_count', 0)} 个",
        f"- 有效经营入账笔数：{result.get('effective_operating_inflow_count', 0)} 笔", "",
        "| 排名 | 入账方名称 | 入账笔数 | 可识别金额 | 主要用途/摘要 |", "|---:|---|---:|---:|---|",
    ]
    for index, item in enumerate(result.get("effective_inflow_counterparties") or [], start=1):
        amount = _money(item.get("recognizable_amount")) if item.get("recognized_amount_count") else "未识别"
        lines.append(f"| {index} | {_cell(item.get('counterparty'))} | {item.get('count', 0)} | {amount} | {_cell(item.get('main_description'))} |")
    lines += ["", "### 有效经营入账明细", "| 序号 | 交易时间 | 入账方名称 | 用途 | 摘要 | 金额 |", "|---:|---|---|---|---|---:|"]
    for index, tx in enumerate(result.get("effective_operating_inflow_transactions") or [], start=1):
        lines.append(f"| {index} | {_cell(tx.get('交易时间'))} | {_cell(tx.get('clean_counterparty_name'))} | {_cell(tx.get('clean_purpose'))} | {_cell(tx.get('clean_summary'))} | {_money(tx.get('金额'))} |")
    lines += [
        "", "### 有效经营出账方汇总",
        "- 说明：本表按出账方名称汇总展示，不是逐笔交易明细。",
        f"- 有效经营出账方数量：{result.get('effective_operating_outflow_counterparty_count', 0)} 个",
        f"- 有效经营出账笔数：{result.get('effective_operating_outflow_count', 0)} 笔", "",
        "| 排名 | 出账方名称 | 出账笔数 | 可识别金额 | 主要用途/摘要 |", "|---:|---|---:|---:|---|",
    ]
    for index, item in enumerate(result.get("effective_outflow_counterparties") or [], start=1):
        amount = _money(item.get("recognizable_amount")) if item.get("recognized_amount_count") else "未识别"
        lines.append(f"| {index} | {_cell(item.get('counterparty'))} | {item.get('count', 0)} | {amount} | {_cell(item.get('main_description'))} |")
    lines += ["", "### 有效经营出账明细", "| 序号 | 交易时间 | 出账方名称 | 用途 | 摘要 | 金额 |", "|---:|---|---|---|---|---:|"]
    for index, tx in enumerate(result.get("effective_operating_outflow_transactions") or [], start=1):
        lines.append(f"| {index} | {_cell(tx.get('交易时间'))} | {_cell(tx.get('clean_counterparty_name'))} | {_cell(tx.get('clean_purpose'))} | {_cell(tx.get('clean_summary'))} | {_money(tx.get('金额'))} |")
    lines += ["", "### 关联人及内部往来", "| 类型 | 交易时间 | 收支方向 | 对方名称 | 关联关系 | 摘要 | 用途 | 金额 | 剔除说明 |", "|---|---|---|---|---|---|---|---:|---|"]
    for tx in result.get("related_internal_transactions") or []:
        tx_type = "关联人转账" if tx.get("is_related_person_transfer") else "本方同名/关联账户"
        relation = tx.get("related_person_role") or ("本方同名或关联账户" if tx.get("is_self_transfer") else "—")
        lines.append(
            f"| {_cell(tx_type)} | {_cell(tx.get('交易时间'))} | {_cell(tx.get('收支方向'))} | {_cell(tx.get('clean_counterparty_name') or tx.get('对方单位'))} | "
            f"{_cell(relation)} | {_cell(tx.get('clean_summary'))} | {_cell(tx.get('clean_purpose'))} | {_money(tx.get('金额'))} | {_cell(tx.get('exclude_reason'))} |"
        )
    lines += ["", "### 剔除项汇总", "| 剔除类型 | 笔数 | 说明 |", "|---|---:|---|"]
    for item in result.get("exclusion_summary") or []:
        lines.append(f"| {_cell(item.get('type'))} | {item.get('count', 0)} | {_cell(item.get('description'))} |")
    lines += ["", "### 贷款及融资相关交易", "| 交易时间 | 收支方向 | 对方单位 | 摘要 | 金额 | 说明 |", "|---|---|---|---|---:|---|"]
    for tx in result.get("loan_related_transactions") or []:
        lines.append(f"| {_cell(tx.get('交易时间'))} | {_cell(tx.get('收支方向'))} | {_cell(tx.get('clean_counterparty_name'))} | {_cell(tx.get('clean_summary') or tx.get('clean_purpose'))} | {_money(tx.get('金额'))} | {_cell(tx.get('交易分类'))} |")
    lines += ["", "### 银行费用及利息", "| 交易时间 | 类型 | 收支方向 | 金额 | 摘要 |", "|---|---|---|---:|---|"]
    for tx in result.get("fee_interest_transactions") or []:
        lines.append(f"| {_cell(tx.get('交易时间'))} | {_cell(tx.get('交易分类'))} | {_cell(tx.get('收支方向'))} | {_money(tx.get('金额'))} | {_cell(tx.get('clean_summary') or tx.get('clean_purpose') or tx.get('display_remark'))} |")
    lines += ["", "### 重点交易明细", "| 序号 | 交易时间 | 收支方向 | 对方单位 | 用途 | 摘要 | 金额 | 分类 | 是否剔除 |", "|---:|---|---|---|---|---|---:|---|---|"]
    for index, tx in enumerate(result.get("focus_transactions") or [], start=1):
        exclusion = f"是，{tx.get('exclude_reason')}" if tx.get("exclude_from_effective_flow") else "否"
        lines.append(f"| {index} | {_cell(tx.get('交易时间'))} | {_cell(tx.get('收支方向'))} | {_cell(tx.get('clean_counterparty_name'))} | {_cell(tx.get('clean_purpose'))} | {_cell(tx.get('clean_summary'))} | {_money(tx.get('金额'))} | {_cell(tx.get('交易分类'))} | {_cell(exclusion)} |")
    lines += ["", "### 风险提示"] + [f"- {item}" for item in result.get("risk_tips") or []]
    if result.get("manual_review_items"):
        lines += ["", "### 需人工复核"] + [f"- {item}" for item in result["manual_review_items"]]
    markdown = "\n".join(lines).replace("None", "").replace("null", "").replace("undefined", "")
    # Defense in depth: internal receipt/evidence labels must never leak to display Markdown.
    for marker in ("回单个性化信息", "指令编号", "支付交易序号", "报文种类", "提交人", "HQP928", "w191001"):
        markdown = markdown.replace(marker, "")
    return markdown


class BankStatementSkill(BaseExtractionSkill):
    document_type = "bank_statement"
    supported_extensions = {".pdf", ".xlsx"}
    skill_name = "bank_statement_skill"
    skill_version = "v1"

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        metadata = input_data.metadata or {}
        related_person_roles = _related_person_map_from_metadata(metadata)
        raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else []
        if not raw_pages:
            raw_pages = _load_native_pdf_pages(input_data.file_path)
        text = str(input_data.raw_text or "")
        page_text = "\n".join(str(item.get("text") or "") for item in raw_pages)
        source = f"{text}\n{page_text}"
        logger.info("[BankStatementSkill] file=%s pages=%s", input_data.file_name, len({int(item.get('page') or index) for index, item in enumerate(raw_pages, 1)}))
        for index, page in enumerate(raw_pages, start=1):
            value = str(page.get("text") or "")
            logger.info("[BankStatementSkill] page=%s text_len=%s preview=%s", page.get("page") or index, len(value), _clean(value[:500]))
        logger.info(
            "[BankStatementSkill] raw_text_len=%s preview=%s title_hit=%s account_label_hit=%s period_label_hit=%s",
            len(source), _clean(source[:500]), "中国工商银行账户明细清单" in source,
            bool(re.search(r"(?:本方)?账号\s*[:：]", source)), bool(re.search(r"时间范围\s*[:：]", source)),
        )
        bank_format = detect_bank_format(text, page_text)
        statement_subtype = detect_statement_subtype(bank_format, source)
        logger.info("[BankStatementSkill] detected_bank_format=%s", bank_format)
        account_info, account_evidence = _extract_account_info(raw_pages, source, bank_format)
        logger.info("[BankStatementSkill] account_info_candidates=%s", account_info)
        if bank_format == BANK_FORMAT_SHANGHAI:
            logger.info("[ShanghaiBankAdapter] first_page_header_text=%s", account_info.get("header_preview") or "未识别")
        period_evidence, period_start, period_end = _periods(raw_pages, source, input_data.file_name)
        bocm_header = parse_bocm_statement_header(raw_pages, source) if bank_format == BANK_FORMAT_BOCM else {}
        if bank_format == BANK_FORMAT_BOCM:
            period_start = bocm_header.get("period_start") or period_start
            period_end = bocm_header.get("period_end") or period_end
        logger.info("[BankStatementSkill] period_candidates=%s", period_evidence)
        selected_period_source = str(period_evidence[0].get("source") or "unknown") if period_evidence else "missing"
        logger.info("[BankStatementSkill] selected_period=%s~%s source=%s", period_start or "未识别", period_end or "未识别", selected_period_source)
        if bank_format == BANK_FORMAT_SHANGHAI:
            transactions, tx_evidence, explicit_amount_column, parse_diagnostics = parse_shanghai_bank_statement(
                raw_pages, period_start, period_end, account_info.get("account_no") or "", text,
            )
        elif bank_format == BANK_FORMAT_BOCM:
            transactions, tx_evidence, explicit_amount_column, parse_diagnostics = parse_bocm_statement_transactions(raw_pages, source)
        elif bank_format == BANK_FORMAT_ICBC:
            transactions, tx_evidence, explicit_amount_column, parse_diagnostics = parse_icbc_statement(raw_pages, source)
        else:
            transactions, tx_evidence, explicit_amount_column, parse_diagnostics = parse_generic_bank_statement(raw_pages, source)
        logger.info("[BankStatementSkill] table_headers_detected=%s", ",".join(parse_diagnostics.get("table_headers_detected") or []))
        logger.info("[BankStatementSkill] candidate_transaction_rows=%s", parse_diagnostics.get("candidate_transaction_rows", 0))
        logger.info("[BankStatementSkill] invalid_candidate_rows=%s", parse_diagnostics.get("invalid_candidate_rows", 0))
        if bank_format == BANK_FORMAT_ICBC:
            title = "中国工商银行账户明细清单" if "中国工商银行账户明细清单" in source else _find_labeled(source, ("对账单标题", "标题"))
            bank_name = "中国工商银行"
        elif bank_format == BANK_FORMAT_SHANGHAI:
            title = next((item for item in ("账户明细查询", "上海银行对账单", "上海银行账户明细", "上海银行交易明细") if item in source), "银行对账单")
            bank_name = "上海银行"
        elif bank_format == BANK_FORMAT_BOCM:
            title = bocm_header.get("title") or "交通银行明细对账单"
            bank_name = "交通银行"
        else:
            title = _find_labeled(source, ("对账单标题", "标题")) or "银行对账单"
            bank_name = _find_labeled(source, ("银行名称",))
        account_no = account_info.get("account_no") or ""
        is_icbc_statement = bank_format == BANK_FORMAT_ICBC
        period_text = f"{period_start} 至 {period_end}" if period_start and period_end else ""
        period_ranges = [str(item.get("raw_value") or "").replace(" ", "") for item in period_evidence]
        logger.info("[BankStatementSkill] account_no=%s", account_no or "未识别")
        logger.info("[BankStatementSkill] period_ranges=%s", period_ranges)
        logger.info("[BankStatementSkill] transactions_count=%s", len(transactions))
        opening_bank_raw = account_info.get("opening_bank") or ""
        account_name_clean = clean_account_name(account_info.get("account_name") or "") if bank_format == BANK_FORMAT_SHANGHAI else (account_info.get("account_name") or "")
        opening_bank = (
            sanitize_opening_bank_for_display(opening_bank_raw, bank_format=bank_format, account_no=account_no, account_name=account_name_clean)
            if bank_format == BANK_FORMAT_SHANGHAI else normalize_opening_bank_name(opening_bank_raw, bank_name)
        )
        logger.info("[BankStatementSkill] opening_bank_raw=%s opening_bank=%s", opening_bank_raw or "未识别", opening_bank or "未识别")
        result: dict[str, Any] = {
            "doc_type": "bank_statement", "doc_type_name": "银行对账单", "agent_type": "bank_statement_agent", "bank_format": bank_format,
            "statement_subtype": statement_subtype,
            "source_file": input_data.file_name or (Path(input_data.file_path).name if input_data.file_path else ""),
            "original_status": "可查看", "extract_status": "成功", "extraction_status": "成功", "bank_name": bank_name,
            "statement_title": title or ("账户明细清单" if "账户明细清单" in source else "银行对账单"),
            "account_no": account_no,
            "account_name": account_name_clean,
            "opening_bank": opening_bank,
            "currency": (bocm_header.get("currency") if bank_format == BANK_FORMAT_BOCM else "") or _find_labeled(source, ("币种",), ("单位", "本方账号开户行", "记账时间范围", "时间范围")) or ("人民币" if bank_format in {BANK_FORMAT_ICBC, BANK_FORMAT_SHANGHAI, BANK_FORMAT_BOCM} or "人民币" in source else ""),
            "unit": _find_labeled(source, ("单位",), ("本方账号开户行", "本方开户行", "开户行", "币种", "记账时间范围", "时间范围", "交易时间")) or ("元" if bank_format in {BANK_FORMAT_ICBC, BANK_FORMAT_SHANGHAI, BANK_FORMAT_BOCM} or re.search(r"单位\s*[:：]?\s*元", source) else ""),
            "statement_year": bocm_header.get("year") if bank_format == BANK_FORMAT_BOCM else "",
            "statement_month": bocm_header.get("month") if bank_format == BANK_FORMAT_BOCM else "",
            "statement_page_no": bocm_header.get("page_no") if bank_format == BANK_FORMAT_BOCM else "",
            "period_start": period_start, "period_end": period_end, "period_text": period_text,
            "page_count": len({int(item.get("page") or index) for index, item in enumerate(raw_pages, 1)}) or int(metadata.get("page_count") or 0),
            "transactions": transactions, "evidence": account_evidence + period_evidence + tx_evidence,
            "parse_diagnostics": parse_diagnostics, "text_recognized": bool(source.strip()),
            "related_person_roles": related_person_roles,
        }
        pending_manual_review_items: list[str] = []
        ok_name, name_reject_reason = validate_account_name(result.get("account_name"))
        if not ok_name and result.get("account_name"):
            logger.info("[BankStatementSkill] final_account_name_rejected=%s reason=%s", result.get("account_name"), name_reject_reason)
            result["account_name"] = ""
            pending_manual_review_items.append("疑似将交易对手名称误识别为客户名称，已拦截，需人工复核本方户名。")
        if bank_format == BANK_FORMAT_GENERIC:
            if not result.get("bank_name"):
                result["bank_name"] = _generic_header_bank_name(account_info.get("header_preview") or "")
        _infer_account_name_from_counterparties(result)
        _clean_and_mark_transactions(result)
        _summary(result)
        if bank_format == BANK_FORMAT_SHANGHAI:
            result["raw_transaction_count"] = int(parse_diagnostics.get("candidate_transaction_rows") or len(transactions))
            result["ocr_anomaly_count"] = int(parse_diagnostics.get("invalid_candidate_rows") or 0)
            _apply_header_summary(result)
        if bank_format == BANK_FORMAT_BOCM:
            validate_bank_account_header(result)
        result["raw_text_blocks_count"] = int(parse_diagnostics.get("raw_text_blocks_count") or 0)
        result["candidate_transaction_rows"] = int(parse_diagnostics.get("candidate_transaction_rows") or len(transactions))
        result["ocr_abnormal_rows"] = int(result.get("ocr_anomaly_count") or 0)
        result["amount_column_detected"] = explicit_amount_column
        customer_profile = metadata.get("customer_profile") if isinstance(metadata.get("customer_profile"), dict) else {}
        customer_profile_name = str(
            metadata.get("customer_name")
            or customer_profile.get("customer_name")
            or customer_profile.get("name")
            or ""
        )
        _apply_parse_quality(result, customer_profile_name=customer_profile_name, amount_column_detected=explicit_amount_column)
        logger.info("[BankStatementSkill] valid_transaction_count=%s", result.get("valid_transaction_count", 0))
        # Compatibility aliases for existing storage/profile readers. The canonical
        # fields above remain the single source of truth.
        result.update({
            "customer_name": result["account_name"], "bank_branch": result["opening_bank"],
            "account_number": result["account_no"], "date_range": result["period_text"],
            "start_date": result["period_start"], "end_date": result["period_end"],
            "debit_transaction_count": result["debit_count"], "credit_transaction_count": result["credit_count"],
            "total_transaction_count": result["transaction_count"], "transaction_detail_count": len(result["transactions"]),
            "max_credit_amount": result["max_in_amount"], "max_debit_amount": result["max_out_amount"],
        })
        result["risk_tips"] = []
        if result["self_transfer_count"] >= 2:
            result["risk_tips"].append("存在多笔本方同名账户划转，已从有效经营流水中剔除，建议结合其他账户流水核验资金闭环。")
        if result.get("related_person_transfer_count", 0) > 0:
            result["risk_tips"].append("存在公司账户与法人/关联人之间的资金往来，已从有效经营流水中剔除，建议结合借款协议、报销单据、备用金台账核验资金性质。")
        if any(tx["交易分类"] in {"贷款归还", "利息支出"} for tx in result["loan_related_transactions"]):
            result["risk_tips"].append("存在贷款归还或贷款利息支出记录。")
        if result["amount_recognition_status"] != "完整识别":
            result["risk_tips"].append("当前 PDF 主表金额列未完整识别，不建议直接用于完整流水测算。")
        if result["effective_inflow_count"] < 3:
            result["risk_tips"].append("有效经营入账需结合其他银行账户流水进一步核验。")
        if any(tx["交易分类"] == "资金拆借" for tx in result["loan_related_transactions"]):
            result["risk_tips"].append("存在资金拆借相关交易，建议进一步核验借款主体和用途。")
        if not result["risk_tips"]: result["risk_tips"].append("未从已识别内容中发现需要特别提示的事项。")
        existing_manual_review_items = list(result.get("manual_review_items") or [])
        result["manual_review_items"] = []
        result["manual_review_items"].extend(existing_manual_review_items)
        result["manual_review_items"].extend(pending_manual_review_items)
        if result["amount_recognition_status"] != "完整识别": result["manual_review_items"].append("金额列缺失或金额识别不完整。")
        if transactions and sum(not tx.get("clean_counterparty_name") for tx in transactions) / len(transactions) >= 0.3: result["manual_review_items"].append("对方单位为空或被判定为无效值的交易较多。")
        if len(period_evidence) > 1: result["manual_review_items"].append("文件包含多个时间区间，已按同一账户合并，建议核对区间连续性。")
        if any(tx["交易分类"] in {"贷款发放", "贷款归还", "资金拆借", "往来入账", "往来出账"} for tx in transactions): result["manual_review_items"].append("存在贷款、借款或往来款交易，建议人工核验资金性质。")
        if result.get("account_name_needs_review"):
            result["manual_review_items"].append("客户名称由高频同名交易对手兜底识别，建议人工复核。")
        if result.get("parse_completeness"):
            result["manual_review_items"].append(f"上海银行页头总笔数与已解析交易笔数存在差异，解析完整率：{result['parse_completeness']}。")
        if result.get("statement_subtype") == "receipt_bundle":
            result["manual_review_items"].append("当前文件可能不是标准银行对账单，而是银行回单集合。")
        if not result.get("transactions_valid"):
            result["manual_review_items"].append("交易明细结构未恢复，暂不纳入客户级有效经营流水统计。")
        if not result.get("account_info_valid"):
            result["manual_review_items"].append("账户信息未达聚合质量要求，需人工复核本方户名、账号和银行名称。")
        markdown = render_bank_statement_markdown(result)
        warnings = list(result["manual_review_items"])
        confidence = min(0.98, 0.45 + (0.1 if account_no else 0) + (0.1 if period_start else 0) + (0.2 if transactions else 0) + (0.05 if result["page_count"] else 0))
        return ExtractionResult(document_type="bank_statement", schema_version="bank_statement.agent.v1", extracted_json=_json_safe(result), markdown_summary=markdown, confidence=confidence, warnings=warnings, skill_name=self.skill_name, skill_version=self.skill_version)
