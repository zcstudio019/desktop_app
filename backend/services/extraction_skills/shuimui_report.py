"""Structured extraction and Chinese Markdown rendering for Shuimui reports."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from utils.json_parser import parse_json

logger = logging.getLogger(__name__)

DOC_TYPE = "shuimui_report"
DOC_TYPE_NAME = "水母报告"
UNKNOWN = "未识别"
CAPTURE_JSON_START = "__SHUIMUI_REPORT_CAPTURE_JSON__"
CAPTURE_JSON_END = "__END_SHUIMUI_REPORT_CAPTURE_JSON__"
EMPTY_VALUES = {"", "--", "-", "无数据", "暂无", UNKNOWN}
INVALID_DISPLAY_VALUES = {"None", "none", "null", "undefined", "未明确", UNKNOWN}
INTERNAL_MARKERS = (
    "tables_text",
    "raw_text",
    "raw_html",
    "page_text",
    "debug_text",
    "structured json",
    "structured_json",
    "report markdown",
    "report_markdown",
    "fields",
    "data",
    "confidence",
    "evidence",
    "metadata",
    "document type",
    "doc type",
    "owner type",
    CAPTURE_JSON_START,
    CAPTURE_JSON_END,
)
SUPPRESSED_SECTIONS = {"经营与流水概况", "上下游交易", "融资参考结论"}
AUTH_CUTOFF_MARKERS = (
    "tables_text",
    "社保人数",
    "股东名称",
    "变更类型",
    "滞纳金时间",
    "登记日期",
    "纳税信息",
    "发票信息",
    "供应商信息",
)
HEADER_VALUES = {
    "社保人数",
    "最近一次社保缴费记录",
    "应缴费额",
    "应缴费额(元)",
    "股东名称",
    "参股比例",
    "变更类型",
    "变更时间",
    "变更前",
    "变更后",
    "变更时间 变更前 变更后",
}

TAX_FIELDS = [
    "纳税信用等级",
    "纳税人种类",
    "近12月欠税记录次数",
    "当前欠税余额（元）",
    "近3个月滞纳金金额(元)",
    "近12个月滞纳金金额(元)",
    "近12月滞纳金次数",
    "近12月增税销售额（元）",
    "近24月增税销售额（元）",
    "近12月完税总额(元)",
    "近24月完税总额(元)",
    "近12月增税应纳额(元)",
    "近12月0申报月数(月)",
    "近12月最长连续0纳税申报月数",
]

FINANCIAL_FIELDS = [
    "资产金额（去年年报）",
    "营业利润额（去年年报）",
    "负债率（去年年报）",
    "营业净利率（去年年报）",
]

INVOICE_AMOUNT_FIELDS = [
    "近1个月开票金额(元)",
    "近3个月开票金额(元)",
    "近6月开票金额(元)",
    "近12个月开票金额(元)",
    "近24个月开票金额(元)",
]

INVOICE_GROWTH_FIELDS = [
    "近3月开票环比增长率",
    "近6月开票环比增长率",
    "近12月开票环比增长率",
]

INVOICE_FIELD_ALIASES = {
    "近6开票环比增长率": "近6月开票环比增长率",
    "近6月开票环比增长率": "近6月开票环比增长率",
    "近6个月开票环比增长率": "近6月开票环比增长率",
    "近6月开票环比增长率(不含本月)": "近6月开票环比增长率",
    "近6个月开票环比增长率(不含本月)": "近6月开票环比增长率",
}

INVOICE_ACTIVITY_FIELDS = [
    "近45日是否有开票记录",
    "近3个月下游客户统计",
    "近12月下游客户数量(家)",
    "近12个月下游开票张数",
    "近12个月作废发票数量占比",
    "近12个月最大连续未开票间隔天数（销项）",
    "近12月断票月数(不含2月)",
    "近12月最长连续断票月数",
    "近12个月红冲金额占比",
    "近12月红冲发票张数占比",
]

INVOICE_SUMMARY_FIELDS = INVOICE_AMOUNT_FIELDS + INVOICE_GROWTH_FIELDS + INVOICE_ACTIVITY_FIELDS

TAX_SECTION_TITLES = (
    "滞纳金情况",
    "税务处罚",
    "近三年纳税信息完税表(元)",
    "近三年纳税信息",
    "近三年完税信息",
    "近三年完税表",
    "完税表",
    "完税信息",
    "完税情况",
    "纳税明细",
)

INVOICE_SECTION_TITLES = (
    "开票金额汇总（不含本月）",
    "开票金额环比增长率（不含本月）",
    "开票活跃度与客户情况",
    "近三年开票信息报表（元）",
)

REPORT_TABLE_SECTION_TITLES = TAX_SECTION_TITLES + INVOICE_SECTION_TITLES

TABLE_CLASSIFIER_COLUMNS = {
    "three_year_tax_payment_table": ["月份", "2024", "2025", "2026"],
    "three_year_invoice_table": ["月份", "2024", "2025", "2026"],
    "late_fee_table": ["滞纳金时间", "滞纳金金额(元)", "状态"],
    "tax_penalty_table": ["登记日期", "违法违章信息", "违法违章状态"],
    "top_suppliers_table": ["排名", "供应商名称", "采购额(元)", "金额占比(%)", "是否关联方"],
    "top_customers_table": ["排名", "客户名称", "销售额(元)", "金额占比(%)", "是否关联方"],
}

SECTION_FIELDS: list[tuple[str, list[str]]] = [
    ("企业基本信息", ["企业名称", "统一社会信用代码", "法定代表人", "法人占股比例", "成立日期", "注册资本", "注册类型", "注册地址", "行业分类"]),
    ("报告基础信息", ["报告编号", "报告创建时间", "查询时间", "报告生成时间", "数据更新时间", "授权状态"]),
    ("社保信息", ["社保人数", "应缴费额"]),
    ("股东信息", ["股东名称", "参股比例"]),
    ("法人/股东变更", ["变更类型", "变更时间", "变更前", "变更后"]),
    ("银税互动授权记录", ["授权记录"]),
    ("纳税信息", ["纳税信用等级", "纳税状态", "纳税金额", "税种", "税款所属期", "申报状态", "欠税信息", "税务异常"]),
    ("财报信息", []),
    ("发票信息", ["开票总金额", "开票月份", "销项发票金额", "进项发票金额", "发票张数", "作废发票", "红冲发票", "主要开票品类", "发票异常提示"]),
    ("前十供应商", []),
    ("前十销售客户", []),
    ("经营与流水概况", ["经营稳定性", "近期开票趋势", "主要收入来源", "主要支出方向", "上下游集中度", "经营异常提示"]),
    ("上下游交易", ["主要上游客户", "主要下游客户", "关联交易提示", "内部转账/疑似异常交易"]),
    ("司法与风险信息", ["被执行信息", "失信信息", "裁判文书", "行政处罚", "经营异常", "股权冻结", "其他风险"]),
    ("融资参考结论", ["可采信经营情况", "主要优势", "主要风险", "需要补充资料", "建议授信关注点"]),
]

ALL_FIELDS = [field for _, fields in SECTION_FIELDS for field in fields]
ALL_FIELDS.extend(TAX_FIELDS)
ALL_FIELDS.extend(FINANCIAL_FIELDS)
ALL_FIELDS.extend(INVOICE_SUMMARY_FIELDS)

LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "企业名称": ("企业名称", "公司名称", "主体名称", "被查询企业"),
    "统一社会信用代码": ("统一社会信用代码", "统一信用代码", "社会信用代码", "信用代码"),
    "法定代表人": ("法定代表人", "法人代表", "当前法人姓名", "法人"),
    "法人占股比例": ("法人占股比例",),
    "成立日期": ("成立日期", "成立时间"),
    "注册资本": ("注册资本", "注册资金"),
    "注册类型": ("注册类型",),
    "注册地址": ("注册地址", "注册区域", "住所", "企业地址"),
    "行业分类": ("行业分类",),
    "报告编号": ("报告编号", "报告号", "水母报告编号", "sn"),
    "报告创建时间": ("报告创建时间",),
    "查询时间": ("查询时间",),
    "报告生成时间": ("报告生成时间", "生成时间"),
    "数据更新时间": ("数据更新时间", "更新时间"),
    "授权状态": ("授权状态",),
    "纳税人识别号": ("纳税人识别号", "税号"),
    "纳税信用等级": ("纳税信用等级", "纳税等级"),
    "近期开票情况": ("近期开票情况", "开票情况"),
    "销项发票金额": ("销项发票金额", "销项金额"),
    "进项发票金额": ("进项发票金额", "进项金额"),
    "发票稳定性": ("发票稳定性",),
    "发票异常提示": ("发票异常提示", "发票异常"),
    "最近一次社保缴费记录": ("最近一次社保缴费记录",),
    "社保人数": ("社保人数",),
    "应缴费额": ("应缴费额",),
    "股东名称": ("股东名称", "股东姓名", "股东"),
    "参股比例": ("参股比例", "持股比例"),
    "变更类型": ("变更类型",),
    "变更时间": ("变更时间",),
    "变更前": ("变更前",),
    "变更后": ("变更后",),
    "授权记录": ("银税互动授权记录", "授权记录"),
    "纳税信用等级": ("纳税信用等级", "纳税等级", "信用等级"),
    "纳税状态": ("纳税状态", "税务状态"),
    "纳税金额": ("纳税金额", "缴税金额", "实缴税额", "税额"),
    "税种": ("税种",),
    "税款所属期": ("税款所属期", "所属期"),
    "申报状态": ("申报状态",),
    "欠税信息": ("欠税信息", "欠税"),
    "税务异常": ("税务异常", "纳税异常"),
    "开票总金额": ("开票总金额", "开票金额", "发票总金额"),
    "开票月份": ("开票月份", "月份"),
    "销项发票金额": ("销项发票金额", "销项金额"),
    "进项发票金额": ("进项发票金额", "进项金额"),
    "发票张数": ("发票张数", "张数"),
    "作废发票": ("作废发票", "作废张数"),
    "红冲发票": ("红冲发票", "红冲张数"),
    "主要开票品类": ("主要开票品类", "开票品类", "商品品类"),
    "发票异常提示": ("发票异常提示", "发票异常"),
    "供应商名称": ("供应商名称", "供应商", "客户名称"),
    "交易金额": ("交易金额", "金额"),
    "交易次数": ("交易次数", "次数", "笔数"),
    "占比": ("占比", "比例"),
    "最近交易时间": ("最近交易时间", "最近交易日期", "交易时间"),
    "集中度提示": ("集中度提示", "集中度"),
}

TAB_SECTION_TITLES = {
    "basic_info": "基本信息",
    "tax_info": "纳税信息",
    "invoice_info": "发票信息",
    "supplier_info": "供应商信息",
}


def _clean(value: Any, max_len: int = 500) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        return ""
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:\t\r\n")
    text = re.sub(r"\s*复制\s*$", "", text).strip()
    lower_text = text.lower()
    if text in EMPTY_VALUES or text in HEADER_VALUES or text in INVALID_DISPLAY_VALUES:
        return ""
    if any(marker in lower_text for marker in INTERNAL_MARKERS):
        return ""
    if re.search(r"^\s*[\[{].*[\]}]\s*$", text):
        return ""
    if not text:
        return ""
    return text[:max_len].strip()


def is_valid_report_value(value: Any) -> bool:
    text = re.sub(r"\s+", "", str(value or ""))
    text = re.sub(r"复制$", "", text).strip()
    return text not in {"", "--", "-", "无", "暂无", "无数据", "null", "undefined", "None", "none", UNKNOWN}


def _clean_label(value: Any, max_len: int = 80) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:\t\r\n")
    lower_text = text.lower()
    if not text or text in EMPTY_VALUES or text in INVALID_DISPLAY_VALUES:
        return ""
    if any(marker in lower_text for marker in INTERNAL_MARKERS):
        return ""
    if re.search(r"^\s*[\[{].*[\]}]\s*$", text):
        return ""
    return text[:max_len].strip()


def _clean_auth_record(value: Any) -> str:
    text = str(value or "")
    for marker in AUTH_CUTOFF_MARKERS:
        index = text.find(marker)
        if index > 0:
            text = text[:index]
    text = text.strip(" \t\r\n,，'\"：:")
    if text.startswith("无"):
        return "无"
    if text.startswith("有"):
        return "有"
    return _clean(text, 120)


def _parse_number(value: Any) -> float | None:
    text = str(value or "").replace(",", "").replace("，", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _normalize_label(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    return text.replace("（", "(").replace("）", ")").replace("：", ":").strip(":")


def _format_integer_amount(value: Any) -> str:
    text = str(value or "").strip()
    if text == "无":
        return "无"
    number = _parse_number(text)
    if number is None:
        return _clean(text, 80)
    return f"{number:,.0f}"


def _format_decimal_amount(value: Any) -> str:
    text = str(value or "").strip()
    if text == "无":
        return "无"
    number = _parse_number(text)
    if number is None:
        return _clean(text, 80)
    return f"{number:,.2f}"


def _format_tax_indicator_value(field: str, value: Any) -> str:
    clean_value = _clean(value, 80)
    if not clean_value:
        return ""
    if clean_value == "无":
        return "无"
    if "销售额" in field or "完税总额" in field or "应纳额" in field or "余额" in field or "金额" in field:
        return _format_integer_amount(clean_value)
    if "次数" in field or "月数" in field:
        number = _parse_number(clean_value)
        return str(int(number)) if number is not None else clean_value
    return clean_value


def _format_financial_value(field: str, value: Any) -> str:
    clean_value = _clean(value, 80)
    if not clean_value:
        return ""
    if "率" in field:
        return _format_percent(clean_value)
    return _format_decimal_amount(clean_value)


def _clean_invoice_value(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:\t\r\n")
    text = re.sub(r"\s*复制\s*$", "", text).strip()
    lower_text = text.lower()
    if not text or text in {"None", "none", "null", "undefined", "未明确", UNKNOWN}:
        return ""
    if any(marker in lower_text for marker in INTERNAL_MARKERS):
        return ""
    if re.search(r"^\s*[\[{].*[\]}]\s*$", text):
        return ""
    return text[:120].strip()


def _format_invoice_summary_value(field: str, value: Any) -> str:
    clean_value = _clean_invoice_value(value)
    if not clean_value:
        return ""
    if clean_value in {"--", "--%"}:
        return clean_value
    if "占比" in field or "增长率" in field:
        return clean_value if clean_value.endswith("%") else _format_percent(clean_value)
    if "金额" in field:
        return _format_integer_amount(clean_value)
    return clean_value


def _format_money(value: Any, *, allow_small: bool = False) -> str:
    text = str(value or "").strip()
    number = _parse_number(text)
    if number is None:
        return ""
    if number <= 100 and "%" in text:
        return ""
    if not allow_small and number <= 100 and "元" not in text and "," not in text and "，" not in text:
        return ""
    return f"{number:,.2f} 元"


def _format_percent(value: Any) -> str:
    number = _parse_number(value)
    if number is None:
        return ""
    return f"{number:.2f}%"


def _record_money_value(record: dict[str, str]) -> str:
    candidate = _format_money(_first_record_value(record, ("交易金额", "采购额", "销售额", "金额"), exclude=("占比", "比例")))
    if candidate:
        return candidate
    for key, value in record.items():
        if any(token in key for token in ("占比", "比例", "次数", "排名")):
            continue
        candidate = _format_money(value)
        if candidate:
            return candidate
    return ""


def _record_percent_value(record: dict[str, str]) -> str:
    candidate = _format_percent(_first_record_value(record, ("占比", "比例")))
    if candidate:
        return candidate
    for key, value in record.items():
        if any(token in key for token in ("金额", "次数", "排名")):
            continue
        number = _parse_number(value)
        if number is not None and 0 <= number <= 100:
            return _format_percent(value)
    return ""


def _first_record_value(record: dict[str, str], keywords: tuple[str, ...], *, exclude: tuple[str, ...] = ()) -> str:
    for key, value in record.items():
        if any(token in key for token in keywords) and not any(token in key for token in exclude):
            cleaned = _clean(value, 160)
            if cleaned:
                return cleaned
    return ""


def _extract_next_line_value(text: str, label: str, max_len: int = 220) -> str:
    lines = [line.strip() for line in (text or "").splitlines()]
    known_labels = set(ALL_FIELDS)
    known_labels.update(HEADER_VALUES)
    for aliases in LABEL_ALIASES.values():
        known_labels.update(aliases)
    for index, line in enumerate(lines):
        if line != label:
            continue
        for candidate in lines[index + 1 : index + 5]:
            if not candidate:
                continue
            if candidate in known_labels:
                return ""
            cleaned = _clean(candidate, max_len)
            return cleaned if cleaned != label else ""
    return ""


def _extract_after_label(text: str, aliases: tuple[str, ...], max_len: int = 220) -> str:
    for label in aliases:
        pattern = re.compile(rf"{re.escape(label)}\s*[：:]\s*([^\n\r]+)")
        match = pattern.search(text or "")
        if match:
            return _clean(match.group(1), max_len)
        next_line = _extract_next_line_value(text, label, max_len)
        if next_line:
            return next_line
        loose = re.compile(rf"{re.escape(label)}\s+([^\n\r：:]+)")
        match = loose.search(text or "")
        if match:
            return _clean(match.group(1), max_len)
    return ""


def _extract_capture_payload(raw_text: str) -> dict[str, Any]:
    if CAPTURE_JSON_START not in (raw_text or ""):
        return {}
    try:
        payload_text = raw_text.split(CAPTURE_JSON_START, 1)[1].split(CAPTURE_JSON_END, 1)[0].strip()
        payload = json.loads(payload_text)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        logger.info("[ShuimuiExtract] capture json parse failed error=%s", str(exc)[:160])
        return {}


def _extract_tab_text(raw_text: str, tab_title: str, capture_payload: dict[str, Any] | None = None) -> str:
    capture_payload = capture_payload or {}
    sections = capture_payload.get("sections")
    if isinstance(sections, dict):
        for section in sections.values():
            if not isinstance(section, dict) or section.get("label") != tab_title:
                continue
            parts = [str(section.get("text") or ""), str(section.get("tables_text") or "")]
            return "\n".join(part for part in parts if part).strip()

    marker = f"### 页签：{tab_title}"
    text = raw_text or ""
    start = text.find(marker)
    if start < 0:
        return ""
    start += len(marker)
    while start < len(text) and text[start] in "\r\n\t ":
        start += 1
    next_marker = text.find("\n### 页签：", start)
    end = next_marker if next_marker >= 0 else len(text)
    return text[start:end].strip()


def _split_row(line: str) -> list[str]:
    if "\t" in line:
        return [item.strip() for item in line.split("\t") if item.strip()]
    return [item.strip() for item in re.split(r"\s{2,}", line.strip()) if item.strip()]


def _extract_table_records(section_text: str) -> list[dict[str, str]]:
    rows = [_split_row(line) for line in (section_text or "").splitlines()]
    rows = [row for row in rows if len(row) >= 2]
    records: list[dict[str, str]] = []
    for index, row in enumerate(rows[:-1]):
        if len(row) < 2:
            continue
        header_score = sum(1 for item in row if item in ALL_FIELDS or item in HEADER_VALUES or any(token in item for token in ("金额", "名称", "月份", "状态", "税种", "所属期", "占比", "次数", "张数", "时间")))
        if header_score < 1:
            continue
        for next_row in rows[index + 1 : index + 21]:
            next_header_score = sum(1 for item in next_row if item in ALL_FIELDS or item in HEADER_VALUES or any(token in item for token in ("金额", "名称", "月份", "状态", "税种", "所属期", "占比", "次数", "张数", "时间")))
            if next_header_score >= header_score and next_row != row:
                break
            if len(next_row) < min(len(row), 2):
                continue
            record = {
                header: _clean(next_row[pos], 160)
                for pos, header in enumerate(row)
                if pos < len(next_row) and _clean(header, 80) and _clean(next_row[pos], 160)
            }
            if record:
                records.append(record)
    return records


def _extract_named_block(section_text: str, titles: tuple[str, ...], stop_titles: tuple[str, ...] = REPORT_TABLE_SECTION_TITLES) -> str:
    text = section_text or ""
    compact_candidates: list[tuple[int, str]] = []
    for title in titles:
        match = re.search(rf"(?m)^\s*{re.escape(title)}\s*$", text)
        if match:
            compact_candidates.append((match.start(), title))
        else:
            index = text.find(title)
            if index >= 0:
                compact_candidates.append((index, title))
    if not compact_candidates:
        return ""
    start, matched_title = min(compact_candidates, key=lambda item: item[0])
    content_start = start + len(matched_title)
    end = len(text)
    for title in stop_titles:
        if title == matched_title:
            continue
        match = re.search(rf"(?m)^\s*{re.escape(title)}\s*$", text[content_start:])
        if match:
            end = min(end, content_start + match.start())
    return text[content_start:end].strip()


def _filter_table_record(record: dict[str, str]) -> dict[str, str]:
    filtered: dict[str, str] = {}
    for key, value in record.items():
        clean_key = _clean_label(key, 80)
        clean_value = _clean(value, 180)
        if not clean_key or not clean_value:
            continue
        if clean_value in {"展开", "查看", "查看详细路径", "详情", "收起"}:
            continue
        if clean_key in {"详细信息"} and clean_value in {"查看", "详情"}:
            continue
        filtered[clean_key] = clean_value
    return filtered


def _records_to_markdown_table(records: list[dict[str, str]], max_rows: int = 30) -> str:
    filtered_records = [_filter_table_record(record) for record in records]
    filtered_records = [record for record in filtered_records if record]
    if not filtered_records:
        return ""
    headers: list[str] = []
    for record in filtered_records:
        for key in record:
            if key not in headers:
                headers.append(key)
    if not headers:
        return ""
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for record in filtered_records[:max_rows]:
        lines.append("| " + " | ".join(record.get(header, "") for header in headers) + " |")
    return "\n".join(lines)


def _extract_block_from_header(section_text: str, required_tokens: tuple[str, ...]) -> str:
    lines = (section_text or "").splitlines()
    for index, line in enumerate(lines):
        if not all(token in line for token in required_tokens):
            continue
        collected = [line]
        for next_line in lines[index + 1 : index + 80]:
            clean_line = _clean(next_line, 220)
            if not clean_line:
                continue
            if clean_line in REPORT_TABLE_SECTION_TITLES:
                break
            collected.append(next_line)
        return "\n".join(collected).strip()
    return ""


def _extract_records_after_header_tokens(section_text: str, required_tokens: tuple[str, ...]) -> list[dict[str, str]]:
    rows = [_split_row(line) for line in (section_text or "").splitlines()]
    rows = [row for row in rows if len(row) >= 2]
    for index, header in enumerate(rows[:-1]):
        if not all(any(token in cell for cell in header) for token in required_tokens):
            continue
        records: list[dict[str, str]] = []
        for row in rows[index + 1 : index + 80]:
            if any(cell in REPORT_TABLE_SECTION_TITLES for cell in row):
                break
            if len(row) < min(2, len(header)):
                continue
            if all(any(token in cell for cell in row) for token in required_tokens):
                break
            record = {
                key: _clean(row[pos], 180)
                for pos, key in enumerate(header)
                if pos < len(row) and _clean_label(key, 80) and _clean(row[pos], 180)
            }
            if record:
                records.append(record)
        return records
    return []


def _normalize_table_cell(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("（", "(").replace("）", ")")


def _classify_table_header(header: list[str]) -> str:
    normalized = {_normalize_table_cell(cell) for cell in header}
    if {"月份", "2024", "2025", "2026"} <= normalized:
        return "three_year_tax_payment_table"
    if "滞纳金时间" in normalized and ("滞纳金金额(元)" in normalized or "滞纳金金额" in normalized) and "状态" in normalized:
        return "late_fee_table"
    if {"登记日期", "违法违章信息", "违法违章状态"} <= normalized:
        return "tax_penalty_table"
    if {"排名", "供应商名称", "采购额(元)"} <= normalized:
        return "top_suppliers_table"
    if {"排名", "客户名称", "销售额(元)"} <= normalized:
        return "top_customers_table"
    return ""


def _canonical_header_map(header: list[str], table_type: str) -> dict[int, str]:
    allowed = TABLE_CLASSIFIER_COLUMNS.get(table_type, [])
    normalized_allowed = {_normalize_table_cell(item): item for item in allowed}
    normalized_allowed.update(
        {
            "滞纳金金额": "滞纳金金额(元)",
            "采购额": "采购额(元)",
            "销售额": "销售额(元)",
            "金额占比": "金额占比(%)",
        }
    )
    mapping: dict[int, str] = {}
    for index, cell in enumerate(header):
        canonical = normalized_allowed.get(_normalize_table_cell(cell))
        if canonical:
            mapping[index] = canonical
    return mapping


def _extract_classified_tables(section_text: str) -> dict[str, list[dict[str, str]]]:
    blocks = _extract_classified_table_blocks(section_text)
    tables: dict[str, list[dict[str, str]]] = {}
    for block in blocks:
        table_type = str(block.get("type") or "")
        records = block.get("records")
        if table_type and isinstance(records, list):
            tables.setdefault(table_type, []).extend(record for record in records if isinstance(record, dict))
    return tables


def _extract_classified_table_blocks(section_text: str) -> list[dict[str, Any]]:
    rows = [_split_row(line) for line in (section_text or "").splitlines()]
    skipped_union_table_count = 0
    classifier_results: list[str] = []
    blocks: list[dict[str, Any]] = []
    current_section_title = ""

    index = 0
    while index < len(rows):
        if len(rows[index]) == 1 and rows[index][0] in REPORT_TABLE_SECTION_TITLES:
            current_section_title = rows[index][0]
            index += 1
            continue
        header = rows[index]
        if len(header) < 2:
            index += 1
            continue
        table_type = _classify_table_header(header)
        if not table_type:
            index += 1
            continue
        if table_type == "three_year_tax_payment_table" and current_section_title == "近三年开票信息报表（元）":
            table_type = "three_year_invoice_table"
        classifier_results.append(table_type)
        mapping = _canonical_header_map(header, table_type)
        if len(mapping) != len(TABLE_CLASSIFIER_COLUMNS.get(table_type, [])):
            skipped_union_table_count += 1
            index += 1
            continue
        records: list[dict[str, str]] = []
        index += 1
        while index < len(rows):
            row = rows[index]
            if len(row) == 1 and row[0] in REPORT_TABLE_SECTION_TITLES:
                break
            if _classify_table_header(row):
                break
            record: dict[str, str] = {}
            for pos, canonical in mapping.items():
                if pos < len(row):
                    value = _clean(row[pos], 180)
                    if value:
                        record[canonical] = value
            if record and len(record) >= 2:
                records.append(record)
            index += 1
        if records:
            blocks.append(
                {
                    "section_title": current_section_title,
                    "type": table_type,
                    "headers": TABLE_CLASSIFIER_COLUMNS.get(table_type, []),
                    "records": records,
                }
            )

    logger.info(
        "[ShuimuiExtract] tax_tables_detected_count=%s table_classifier_result=%s skipped_union_table_count=%s",
        len(blocks),
        ",".join(classifier_results),
        skipped_union_table_count,
    )
    return blocks


def _format_year_amount_cell(value: Any) -> str:
    number = _parse_number(value)
    if number is None:
        return _clean(value, 80)
    return f"{number:,.0f}"


def _trim_three_year_payment_records(records: list[dict[str, str]]) -> tuple[list[dict[str, str]], int, bool]:
    clean_records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    duplicate_month_block_removed = False
    annual_summary_seen = False
    months_seen: set[str] = set()
    for record in records:
        month = _clean(record.get("月份"), 40)
        if not month or not (re.fullmatch(r"\d{1,2}月", month) or month == "年度汇总"):
            continue
        if month == "1月" and (annual_summary_seen or month in months_seen):
            duplicate_month_block_removed = True
            break
        if month == "年度汇总" and annual_summary_seen:
            duplicate_month_block_removed = True
            break
        row = {
            "月份": month,
            "2024": _format_year_amount_cell(record.get("2024")),
            "2025": _format_year_amount_cell(record.get("2025")),
            "2026": _format_year_amount_cell(record.get("2026")),
        }
        key = (row["月份"], row["2024"], row["2025"], row["2026"])
        if key in seen:
            continue
        seen.add(key)
        clean_records.append(row)
        if month == "年度汇总":
            annual_summary_seen = True
        else:
            months_seen.add(month)
        if len(clean_records) >= 13:
            duplicate_month_block_removed = len(records) > len(clean_records)
            break
    return clean_records, len(records), duplicate_month_block_removed


def _three_year_payment_table_rows_from_blocks(blocks: list[dict[str, Any]]) -> tuple[list[tuple[str, str]], int]:
    candidates = [block for block in blocks if block.get("type") == "three_year_tax_payment_table"]
    preferred = [block for block in candidates if block.get("section_title") == "近三年纳税信息完税表(元)"]
    selected = (preferred or candidates or [None])[0]
    if not isinstance(selected, dict):
        return [], 0
    records = selected.get("records")
    if not isinstance(records, list):
        return [], 0
    clean_records, before_trim_count, duplicate_removed = _trim_three_year_payment_records([record for record in records if isinstance(record, dict)])
    table_markdown = _records_to_markdown_table(clean_records) if clean_records else ""
    if not table_markdown:
        return [], 0
    logger.info(
        "[ShuimuiExtract] three_year_tax_table_candidates_count=%s selected_three_year_tax_table_section_title=%s selected_three_year_tax_table_rows_before_trim=%s selected_three_year_tax_table_rows_after_trim=%s duplicate_month_block_removed=%s skipped_same_header_tables_count=%s",
        len(candidates),
        selected.get("section_title") or "",
        before_trim_count,
        len(clean_records),
        duplicate_removed,
        max(len(candidates) - 1, 0),
    )
    return [("__SUBSECTION__", "近三年纳税信息完税表(元)"), ("__TABLE__", table_markdown)], len(clean_records)


def _three_year_invoice_table_rows_from_blocks(blocks: list[dict[str, Any]]) -> tuple[list[tuple[str, str]], int]:
    candidates = [block for block in blocks if block.get("type") == "three_year_invoice_table"]
    preferred = [block for block in candidates if block.get("section_title") == "近三年开票信息报表（元）"]
    selected = (preferred or candidates or [None])[0]
    if not isinstance(selected, dict):
        return [], 0
    records = selected.get("records")
    if not isinstance(records, list):
        return [], 0
    clean_records, before_trim_count, duplicate_removed = _trim_three_year_payment_records([record for record in records if isinstance(record, dict)])
    table_markdown = _records_to_markdown_table(clean_records) if clean_records else ""
    if not table_markdown:
        return [], 0
    logger.info(
        "[ShuimuiExtract] invoice_table_candidates_count=%s selected_invoice_table_section_title=%s selected_invoice_table_rows_before_trim=%s selected_invoice_table_rows_after_trim=%s tax_table_and_invoice_table_separated=%s skipped_same_header_cross_tab_tables_count=%s duplicate_month_block_removed=%s",
        len(candidates),
        selected.get("section_title") or "",
        before_trim_count,
        len(clean_records),
        True,
        max(len(candidates) - 1, 0),
        duplicate_removed,
    )
    return [("__SUBSECTION__", "近三年开票信息报表（元）"), ("__TABLE__", table_markdown)], len(clean_records)


def _add_dynamic_field(rows: list[tuple[str, str]], label: str, value: Any) -> None:
    clean_label = _clean(label, 80)
    clean_value = _clean(value, 220)
    if not clean_label or not clean_value or clean_label == clean_value:
        return
    if not clean_label.startswith("供应商 "):
        header_hits = sum(1 for field in ALL_FIELDS if field != clean_label and field in clean_value)
        if header_hits >= 1 and not re.search(r"\d", clean_value):
            return
        if header_hits >= 2:
            return
    if clean_value in HEADER_VALUES or clean_label in {"基本信息", "纳税信息", "发票信息", "供应商信息"}:
        return
    if (clean_label, clean_value) not in rows:
        rows.append((clean_label, clean_value))


def _extract_dynamic_section_fields(section_text: str, target_fields: list[str]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for field in target_fields:
        value = _extract_after_label(section_text, LABEL_ALIASES.get(field, (field,)), max_len=220)
        if value:
            _add_dynamic_field(rows, field, value)

    table_records = _extract_table_records(section_text)
    if table_records:
        first = table_records[0]
        for key, value in first.items():
            if any(token in key for token in ("名称", "金额", "月份", "状态", "税种", "所属期", "占比", "次数", "张数", "时间", "异常", "品类")):
                _add_dynamic_field(rows, key, value)
    return rows


def _is_noise_value(value: str) -> bool:
    text = _clean(value, 120)
    if not text:
        return True
    if text in HEADER_VALUES:
        return True
    if text in {"展开", "收起", "详情", "查看", "年度汇总"}:
        return True
    if re.fullmatch(r"20\d{2}", text):
        return True
    if re.fullmatch(r"\d{1,2}月", text):
        return True
    return False


def _extract_whitelisted_label_values(section_text: str, fields: list[str], formatter: Any) -> list[tuple[str, str]]:
    lines = [_clean(line, 120) for line in (section_text or "").splitlines()]
    lines = [line for line in lines if line]
    normalized_to_field = {_normalize_label(field): field for field in fields}
    normalized_labels = set(normalized_to_field)
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()

    for line in lines:
        if "：" in line or ":" in line:
            label, raw_value = re.split(r"[：:]", line, maxsplit=1)
            field = normalized_to_field.get(_normalize_label(label))
            value = formatter(field, raw_value) if field else ""
            if field and value and field not in seen:
                rows.append((field, value))
                seen.add(field)

    for index, line in enumerate(lines):
        field = normalized_to_field.get(_normalize_label(line))
        if not field or field in seen:
            continue
        for candidate in lines[index + 1 : index + 6]:
            if _normalize_label(candidate) in normalized_labels:
                break
            if _is_noise_value(candidate):
                continue
            value = formatter(field, candidate)
            if value:
                rows.append((field, value))
                seen.add(field)
                break
    return rows


def _extract_tax_indicator_rows(section_text: str) -> list[tuple[str, str]]:
    return _extract_whitelisted_label_values(section_text, TAX_FIELDS, _format_tax_indicator_value)


def _extract_financial_rows(section_text: str) -> list[tuple[str, str]]:
    return _extract_whitelisted_label_values(section_text, FINANCIAL_FIELDS, _format_financial_value)


def _extract_invoice_summary_rows(section_text: str, fields: list[str]) -> list[tuple[str, str]]:
    lines = [re.sub(r"\s+", " ", str(line or "")).strip(" ：:\t\r\n") for line in (section_text or "").splitlines()]
    lines = [line for line in lines if line]
    normalized_to_field = {_normalize_label(field): field for field in fields}
    for alias, field in INVOICE_FIELD_ALIASES.items():
        if field in fields:
            normalized_to_field[_normalize_label(alias)] = field
    normalized_labels = set(normalized_to_field)
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()
    growth_labels_seen: list[str] = []

    for line in lines:
        if "：" in line or ":" in line:
            label, raw_value = re.split(r"[：:]", line, maxsplit=1)
            if "开票环比增长率" in label and label not in growth_labels_seen:
                growth_labels_seen.append(label)
            field = normalized_to_field.get(_normalize_label(label))
            value = _format_invoice_summary_value(field, raw_value) if field else ""
            if field and value and field not in seen:
                rows.append((field, value))
                seen.add(field)

    for index, line in enumerate(lines):
        if "开票环比增长率" in line and line not in growth_labels_seen:
            growth_labels_seen.append(line)
        field = normalized_to_field.get(_normalize_label(line))
        if not field or field in seen:
            continue
        for candidate in lines[index + 1 : index + 6]:
            if _normalize_label(candidate) in normalized_labels:
                break
            if candidate in {"展开", "收起", "详情", "查看", "查看详细路径"}:
                continue
            value = _format_invoice_summary_value(field, candidate)
            if value:
                rows.append((field, value))
                seen.add(field)
                break
    if any(field in INVOICE_GROWTH_FIELDS for field in fields):
        logger.info(
            "[ShuimuiExtract] invoice_growth_labels_seen=%s matched_invoice_growth_fields=%s invoice_growth_6m_value=%s",
            ",".join(growth_labels_seen),
            ",".join(field for field, _ in rows if field in INVOICE_GROWTH_FIELDS),
            next((value for field, value in rows if field == "近6月开票环比增长率"), ""),
        )
    return rows


def _is_penalty_status(value: str) -> bool:
    text = _clean(value, 80)
    return bool(text and re.search(r"(已缴清|未缴清|已处理|未处理|已缴|未缴)", text))


def _late_fee_rows(section_text: str, classified_tables: dict[str, list[dict[str, str]]]) -> tuple[list[tuple[str, str]], int, int]:
    rows: list[tuple[str, str]] = []
    seen_records: set[tuple[str, str, str]] = set()
    skipped_rows = 0
    for record in classified_tables.get("late_fee_table", []):
        date = _clean(record.get("滞纳金时间"), 80)
        if not re.fullmatch(r"(?:19|20)\d{2}-\d{1,2}-\d{1,2}", date or ""):
            skipped_rows += 1
            continue
        amount = _format_money(record.get("滞纳金金额(元)"), allow_small=True)
        status = _clean(record.get("状态"), 80)
        if not amount or not _is_penalty_status(status):
            skipped_rows += 1
            continue
        key = (date, amount, status)
        if key in seen_records:
            continue
        seen_records.add(key)
        rows.append((f"记录 {len(rows) + 1}", "，".join([date, f"滞纳金金额：{amount}", f"状态：{status}"])))
    return rows, len(rows), skipped_rows


def _tax_penalty_rows(classified_tables: dict[str, list[dict[str, str]]]) -> tuple[list[tuple[str, str]], int, int]:
    rows: list[tuple[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    before_count = 0
    for record in classified_tables.get("tax_penalty_table", []):
        before_count += 1
        date = _clean(record.get("登记日期"), 80)
        info = _clean(record.get("违法违章信息"), 180)
        status = _clean(record.get("违法违章状态"), 120)
        if not date or not (info or status):
            continue
        key = (date, info, status)
        if key in seen:
            continue
        seen.add(key)
        parts = [f"登记日期：{date}"]
        if info:
            parts.append(f"违法违章信息：{info}")
        if status:
            parts.append(f"违法违章状态：{status}")
        rows.append((f"记录 {len(rows) + 1}", "，".join(parts)))
    return rows, before_count, len(rows)


def _section_table_rows(section_text: str, titles: tuple[str, ...]) -> tuple[list[tuple[str, str]], int]:
    block = _extract_named_block(section_text, titles)
    if not block and "近三年纳税信息" in titles:
        block = _extract_block_from_header(section_text, ("年份/期间", "增值税销售额"))
    if not block and any(title in titles for title in ("完税表", "完税信息", "近三年完税表")):
        block = _extract_block_from_header(section_text, ("税款所属期", "税种"))
    if not block:
        return [], 0
    table_markdown = _records_to_markdown_table(_extract_table_records(block))
    if not table_markdown and "近三年纳税信息" in titles:
        table_markdown = _records_to_markdown_table(_extract_table_records(_extract_block_from_header(section_text, ("年份/期间", "增值税销售额"))))
    if not table_markdown and "近三年纳税信息" in titles:
        table_markdown = _records_to_markdown_table(_extract_records_after_header_tokens(section_text, ("年份/期间", "增值税销售额")))
    if not table_markdown and any(title in titles for title in ("完税表", "完税信息", "近三年完税表")):
        table_markdown = _records_to_markdown_table(_extract_table_records(_extract_block_from_header(section_text, ("税款所属期", "税种"))))
    if not table_markdown and any(title in titles for title in ("完税表", "完税信息", "近三年完税表")):
        table_markdown = _records_to_markdown_table(_extract_records_after_header_tokens(section_text, ("税款所属期", "税种")))
    if not table_markdown:
        return [], 0
    row_count = max(len(table_markdown.splitlines()) - 2, 0)
    return [("__TABLE__", table_markdown)], row_count


def _tax_display_rows(section_text: str) -> list[tuple[str, str]]:
    rows = _extract_tax_indicator_rows(section_text)
    classified_table_blocks = _extract_classified_table_blocks(section_text)
    classified_tables: dict[str, list[dict[str, str]]] = {}
    for block in classified_table_blocks:
        table_type = str(block.get("type") or "")
        records = block.get("records")
        if table_type and isinstance(records, list):
            classified_tables.setdefault(table_type, []).extend(record for record in records if isinstance(record, dict))
    late_fee_rows, late_fee_count, skipped_non_penalty_rows = _late_fee_rows(section_text, classified_tables)
    tax_penalty_rows, tax_penalty_before_count, tax_penalty_count = _tax_penalty_rows(classified_tables)
    three_year_rows, three_year_count = _three_year_payment_table_rows_from_blocks(classified_table_blocks)
    if late_fee_rows:
        rows.append(("__SUBSECTION__", "滞纳金情况"))
        rows.extend(late_fee_rows[:80])
    if tax_penalty_rows:
        rows.append(("__SUBSECTION__", "税务处罚"))
        rows.extend(tax_penalty_rows[:50])
    if three_year_rows:
        rows.extend(three_year_rows)
    logger.info(
        "[ShuimuiExtract] tax_tab_text_length=%s matched_tax_fields=%s three_year_tax_payment_table_columns=%s three_year_tax_payment_table_rows=%s late_fee_table_rows=%s tax_penalty_rows_before_dedupe=%s tax_penalty_rows_after_dedupe=%s skipped_rows_count=%s final_tax_markdown_lines_count=%s",
        len(section_text or ""),
        ",".join(field for field, _ in rows if field != "__SUBSECTION__" and not field.startswith("记录 ")),
        ",".join(TABLE_CLASSIFIER_COLUMNS["three_year_tax_payment_table"] if three_year_count else []),
        three_year_count,
        late_fee_count,
        tax_penalty_before_count,
        tax_penalty_count,
        skipped_non_penalty_rows,
        len(rows),
    )
    return rows


def _invoice_display_rows(section_text: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    amount_rows = _extract_invoice_summary_rows(section_text, INVOICE_AMOUNT_FIELDS)
    growth_rows = _extract_invoice_summary_rows(section_text, INVOICE_GROWTH_FIELDS)
    activity_rows = _extract_invoice_summary_rows(section_text, INVOICE_ACTIVITY_FIELDS)
    classified_blocks = _extract_classified_table_blocks(section_text)
    invoice_table_rows, invoice_table_count = _three_year_invoice_table_rows_from_blocks(classified_blocks)
    if amount_rows:
        rows.append(("__SUBSECTION__", "开票金额汇总（不含本月）"))
        rows.extend(amount_rows)
    if growth_rows:
        rows.append(("__SUBSECTION__", "开票金额环比增长率（不含本月）"))
        rows.extend(growth_rows)
    if activity_rows:
        rows.append(("__SUBSECTION__", "开票活跃度与客户情况"))
        rows.extend(activity_rows)
    rows.extend(invoice_table_rows)
    logger.info(
        "[ShuimuiExtract] invoice_tab_opened=%s invoice_summary_fields_matched=%s invoice_table_rows=%s",
        bool(section_text),
        ",".join(field for field, _ in amount_rows + growth_rows + activity_rows),
        invoice_table_count,
    )
    return rows


def _split_supplier_customer_records(section_text: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    records = _extract_table_records(section_text)
    supplier_records: list[dict[str, str]] = []
    customer_records: list[dict[str, str]] = []
    for record in records:
        supplier_key = next((key for key in record if "供应商" in key), "")
        customer_key = next((key for key in record if any(token in key for token in ("客户", "购买方", "销售"))), "")
        generic_name_key = next((key for key in record if any(token in key for token in ("企业名称", "名称"))), "")
        if customer_key and _clean(record.get(customer_key)):
            customer_records.append(record)
        elif supplier_key and _clean(record.get(supplier_key)):
            supplier_records.append(record)
        elif generic_name_key and _clean(record.get(generic_name_key)):
            supplier_records.append(record)

    if not customer_records and len(supplier_records) > 10:
        customer_records = supplier_records[10:20]
        supplier_records = supplier_records[:10]
    return supplier_records[:10], customer_records[:10]


def _supplier_display_rows(section_text: str) -> list[tuple[str, str]]:
    supplier_records, _ = _split_supplier_customer_records(section_text)

    if supplier_records:
        rows: list[tuple[str, str]] = []
        for index, record in enumerate(supplier_records, 1):
            name = _first_record_value(record, ("供应商", "企业名称", "名称"), exclude=("金额", "占比", "比例", "次数", "时间", "日期"))
            amount = _record_money_value(record)
            ratio = _record_percent_value(record)
            related = _first_record_value(record, ("是否关联方", "关联方"))
            parts = [name]
            if amount:
                parts.append(f"交易金额：{amount}")
            if ratio:
                parts.append(f"占比：{ratio}")
            if related:
                parts.append(f"是否关联方：{related}")
            _add_dynamic_field(rows, f"供应商 {index}", "，".join(part for part in parts if part))
        return rows
    return _extract_dynamic_section_fields(section_text, ["供应商名称", "交易金额", "交易次数", "占比", "最近交易时间", "集中度提示"])


def _customer_display_rows(section_text: str) -> list[tuple[str, str]]:
    _, customer_records = _split_supplier_customer_records(section_text)
    rows: list[tuple[str, str]] = []
    for index, record in enumerate(customer_records, 1):
        name = _first_record_value(record, ("客户", "购买方", "企业名称", "名称"), exclude=("金额", "占比", "比例", "次数", "时间", "日期"))
        amount = _record_money_value(record)
        ratio = _record_percent_value(record)
        related = _first_record_value(record, ("是否关联方", "关联方"))
        parts = [name]
        if amount:
            parts.append(f"交易金额：{amount}")
        if ratio:
            parts.append(f"占比：{ratio}")
        if related:
            parts.append(f"是否关联方：{related}")
        _add_dynamic_field(rows, f"客户 {index}", "，".join(part for part in parts if part))
    return rows


def _flatten_api_payload_for_sections(capture_payload: dict[str, Any]) -> dict[str, str]:
    result = {"纳税信息": "", "发票信息": "", "供应商信息": ""}
    api_items = capture_payload.get("api_json")
    if not isinstance(api_items, list):
        return result

    def stringify_relevant(value: Any, keywords: tuple[str, ...]) -> list[str]:
        lines: list[str] = []
        if isinstance(value, dict):
            for key, nested in value.items():
                key_text = str(key)
                if any(token.lower() in key_text.lower() for token in keywords):
                    if isinstance(nested, (str, int, float)):
                        lines.append(f"{key_text}：{nested}")
                    elif isinstance(nested, (dict, list)):
                        lines.append(json.dumps(nested, ensure_ascii=False, default=str)[:3000])
                lines.extend(stringify_relevant(nested, keywords))
        elif isinstance(value, list):
            for item in value[:20]:
                lines.extend(stringify_relevant(item, keywords))
        return lines

    for item in api_items:
        if not isinstance(item, dict):
            continue
        payload = item.get("payload")
        result["纳税信息"] += "\n".join(stringify_relevant(payload, ("tax", "纳税", "税务", "税款", "申报", "欠税"))) + "\n"
        result["发票信息"] += "\n".join(stringify_relevant(payload, ("invoice", "发票", "开票", "销项", "进项", "红冲", "作废"))) + "\n"
        result["供应商信息"] += "\n".join(stringify_relevant(payload, ("supplier", "vendor", "供应商", "上游", "客户", "交易"))) + "\n"
    return {key: value.strip() for key, value in result.items()}


def _extract_dynamic_sections(raw_text: str, capture_payload: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    api_text = _flatten_api_payload_for_sections(capture_payload)
    tax_text = "\n".join(part for part in (api_text.get("纳税信息"), _extract_tab_text(raw_text, "纳税信息", capture_payload)) if part)
    invoice_text = "\n".join(part for part in (api_text.get("发票信息"), _extract_tab_text(raw_text, "发票信息", capture_payload)) if part)
    supplier_text = "\n".join(part for part in (api_text.get("供应商信息"), _extract_tab_text(raw_text, "供应商信息", capture_payload)) if part)
    financial_rows = _extract_financial_rows(tax_text)
    if financial_rows:
        logger.info(
            "[ShuimuiExtract] tax_tab_text_length=%s matched_financial_fields=%s",
            len(tax_text or ""),
            ",".join(field for field, _ in financial_rows),
        )
    dynamic = {
        "纳税信息": _tax_display_rows(tax_text),
        "财报信息": financial_rows,
        "发票信息": _invoice_display_rows(invoice_text),
        "前十供应商": _supplier_display_rows(supplier_text),
        "前十销售客户": _customer_display_rows(supplier_text),
    }
    return {section: rows for section, rows in dynamic.items() if rows}


def _extract_row_tables(raw_text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    text = raw_text or ""

    social_security = re.search(
        r"社保人数\s+应缴费额(?:\(元\))?\s*\n\s*(\d+)\s+([0-9,.]+)",
        text,
    )
    if social_security:
        data["社保人数"] = social_security.group(1)
        data["应缴费额"] = f"{social_security.group(2)} 元"

    shareholder = re.search(
        r"股东名称\s+参股比例\s*\n\s*([^\s\n]+)\s+([0-9.]+%)",
        text,
    )
    if shareholder:
        data["股东名称"] = _clean(shareholder.group(1), 80)
        data["参股比例"] = _clean(shareholder.group(2), 80)

    change = re.search(
        r"变更类型\s+变更时间\s+变更前\s+变更后\s*\n\s*([^\n\t]+?)\s+(20\d{2}-\d{1,2}-\d{1,2}|19\d{2}-\d{1,2}-\d{1,2})\s+([^\n\t]+?)\s+([^\n\t]+)",
        text,
    )
    if not change:
        change = re.search(
            r"变更类型\s+变更时间\s+变更前\s+变更后\s*\n\s*([^\n]+?)\s+(20\d{2}-\d{1,2}-\d{1,2}|19\d{2}-\d{1,2}-\d{1,2})\s*\n\s*([^\n]+)\s*\n\s*([^\n]+)",
            text,
        )
    if change:
        data["变更类型"] = _clean(change.group(1), 80)
        data["变更时间"] = _clean(change.group(2), 80)
        data["变更前"] = _clean(change.group(3), 80)
        data["变更后"] = _clean(change.group(4), 80)

    return {key: value for key, value in data.items() if value}


def _rule_extract(raw_text: str, sn: str) -> dict[str, str]:
    data: dict[str, str] = _extract_row_tables(raw_text)
    for field, aliases in LABEL_ALIASES.items():
        if field in {"供应商名称", "交易金额", "交易次数", "占比", "最近交易时间", "集中度提示"}:
            continue
        if data.get(field):
            continue
        value = _extract_after_label(raw_text, aliases)
        if value:
            data[field] = value

    code_match = re.search(r"\b([0-9A-Z]{18})\b", raw_text or "")
    if code_match:
        data.setdefault("统一社会信用代码", code_match.group(1))

    date_match = re.search(r"报告创建时间\s*(?:[:：]|\n+)\s*((?:19|20)\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)", raw_text or "")
    if date_match:
        data.setdefault("报告创建时间", date_match.group(1))

    data.setdefault("报告编号", sn)
    if "银税互动授权记录" in raw_text and not data.get("授权记录"):
        data["授权记录"] = _extract_after_label(raw_text, ("银税互动授权记录",), max_len=80) or "无"
    if not data.get("社保人数"):
        match = re.search(r"社保人数\s*(?:[:：]|\n+)\s*(\d+)", raw_text or "")
        if match:
            data["社保人数"] = match.group(1)
    if not data.get("应缴费额"):
        match = re.search(r"应缴费额\s*(?:[:：]|\n+)\s*([0-9,.]+\s*元?)", raw_text or "")
        if match:
            data["应缴费额"] = _clean(match.group(1), 80)
    if not data.get("股东名称"):
        match = re.search(r"股东(?:名称|姓名)?\s*(?:[:：]|\n+)\s*([\u4e00-\u9fa5·]{2,20})", raw_text or "")
        if match:
            data["股东名称"] = _clean(match.group(1), 80)
    if not data.get("参股比例"):
        match = re.search(r"(?:参股比例|持股比例)\s*(?:[:：]|\n+)\s*([0-9.]+%)", raw_text or "")
        if match:
            data["参股比例"] = _clean(match.group(1), 80)
    if "法定代表人变更" in raw_text:
        data.setdefault("变更类型", "法定代表人变更")
    if not data.get("变更时间"):
        match = re.search(r"(20\d{2}-\d{1,2}-\d{1,2}|19\d{2}-\d{1,2}-\d{1,2})", raw_text or "")
        if match and "法人/股东变更" in raw_text:
            data["变更时间"] = match.group(1)
    if not data.get("变更前"):
        match = re.search(r"变更前\s*(?:[:：]|\n+)\s*([\u4e00-\u9fa5·]{2,20})", raw_text or "")
        if match:
            data["变更前"] = _clean(match.group(1), 80)
    if not data.get("变更后"):
        match = re.search(r"变更后\s*(?:[:：]|\n+)\s*([\u4e00-\u9fa5·]{2,20})", raw_text or "")
        if match:
            data["变更后"] = _clean(match.group(1), 80)
    return data


def _chunk_text(text: str, chunk_size: int = 6000, overlap: int = 300) -> list[str]:
    clean_text = str(text or "").strip()
    if len(clean_text) <= chunk_size:
        return [clean_text] if clean_text else []
    chunks: list[str] = []
    start = 0
    while start < len(clean_text):
        chunks.append(clean_text[start : start + chunk_size])
        start += max(chunk_size - overlap, 1)
    return chunks


def _build_llm_prompt() -> str:
    fields = "、".join(ALL_FIELDS)
    return (
        "请从用户主动提供且已经可访问的水母报告页面文本中提取结构化信息。"
        "只返回一个 JSON 对象，JSON 的 key 必须使用中文字段名，不要返回英文 key，不要编造。"
        f"字段范围：{fields}。"
        "没有识别到的字段不要填。"
    )


def _llm_extract(raw_text: str, ai_service: Any | None) -> dict[str, str]:
    if ai_service is None:
        return {}
    merged: dict[str, str] = {}
    for chunk in _chunk_text(raw_text):
        try:
            result = ai_service.extract(_build_llm_prompt(), chunk, max_tokens=4096)
        except Exception as exc:
            logger.info("[ShuimuiExtract] llm chunk failed error=%s", str(exc)[:160])
            continue
        parsed = parse_json(result)
        if not isinstance(parsed, dict):
            with_json = parse_json(str(result))
            parsed = with_json if isinstance(with_json, dict) else {}
        for key, value in parsed.items():
            clean_key = _clean(key, 80)
            if clean_key in ALL_FIELDS:
                clean_value = _clean(value)
                if clean_value:
                    merged[clean_key] = clean_value
    return merged


def _drop_unverified_page_fields(fields: dict[str, Any], rule_fields: dict[str, str], raw_text: str) -> None:
    if "注册资本" in fields and "注册资本" not in rule_fields:
        fields.pop("注册资本", None)
        logger.info("[ShuimuiExtract] dropped_unverified_field=注册资本 reason=not_present_as_valid_page_value")


def render_shuimui_report_markdown(
    fields: dict[str, Any],
    *,
    source_url: str,
    sn: str,
    extraction_status: str = "成功",
    original_status: str = "可查看",
) -> str:
    def value(field: str) -> str:
        if field == "授权记录":
            return _clean_auth_record(fields.get(field))
        return _clean(fields.get(field))

    lines = [
        "## 水母报告",
        "",
        "* 资料类型：水母报告",
        f"* 来源链接：{source_url or ''}",
        f"* 提取状态：{extraction_status or ''}",
        f"* 报告编号：{sn or value('报告编号')}",
        f"* 原件状态：{original_status or ''}",
        "",
    ]
    for section, section_fields in SECTION_FIELDS:
        if section in SUPPRESSED_SECTIONS:
            continue
        rows: list[tuple[str, str]] = []
        dynamic_sections = fields.get("_dynamic_sections") if isinstance(fields.get("_dynamic_sections"), dict) else {}
        if section in dynamic_sections:
            dynamic_rows = dynamic_sections.get(section)
            if isinstance(dynamic_rows, list):
                rows = []
                for item in dynamic_rows:
                    if not isinstance(item, (list, tuple)) or len(item) < 2:
                        continue
                    marker = str(item[0])
                    if marker in {"__SUBSECTION__", "__TABLE__"}:
                        field = marker
                        field_value = str(item[1] or "").strip()
                    else:
                        field = _clean_label(item[0], 80)
                        raw_field_value = str(item[1] or "").strip()
                        field_value = raw_field_value if raw_field_value in {"--", "--%"} else _clean(item[1], 240)
                    if field_value and (field or marker in {"", "__SUBSECTION__", "__TABLE__"}):
                        rows.append((field, field_value))
                if not rows:
                    continue
                lines.append(f"### {section}")
                lines.append("")
                for field, field_value in rows:
                    if field == "__SUBSECTION__":
                        lines.extend(["", f"#### {field_value}", ""])
                    elif field == "__TABLE__":
                        lines.extend([field_value, ""])
                    elif field:
                        lines.append(f"* {field}：{field_value}")
                    else:
                        lines.append(f"* {field_value}")
                lines.append("")
                continue
        for field in section_fields:
            if field == "报告编号":
                continue
            field_value = value(field)
            if field_value:
                rows.append((field, field_value))
        if section == "社保信息" and rows:
            existing_labels = {field for field, _ in rows}
            if {"社保人数", "应缴费额"} & existing_labels:
                rows.append(("说明", "社保人数和应缴费额取自职工基本养老保险单位缴纳人数及金额"))
        if not rows:
            continue
        lines.append(f"### {section}")
        lines.append("")
        for field, field_value in rows:
            field = _clean_label(field, 80)
            field_value = _clean(field_value, 240)
            if field and field_value:
                lines.append(f"* {field}：{field_value}")
        lines.append("")
    markdown = "\n".join(lines).strip() + "\n"
    safe_lines = []
    for line in markdown.splitlines():
        lower_line = line.lower()
        if any(marker in lower_line for marker in INTERNAL_MARKERS):
            continue
        if any(token in lower_line for token in ("none", "null", "undefined")) or any(token in line for token in ("未明确", "未识别")):
            continue
        if re.search(r"[\[{]['\"]?.+['\"]?\s*:", line):
            continue
        safe_lines.append(line)
    return "\n".join(safe_lines).strip() + "\n"


def extract_shuimui_report(
    raw_text: str,
    *,
    source_url: str,
    sn: str,
    ai_service: Any | None = None,
) -> dict[str, Any]:
    capture_payload = _extract_capture_payload(raw_text)
    rule_fields = _rule_extract(raw_text, sn)
    llm_fields = _llm_extract(raw_text, ai_service)
    fields = {**llm_fields, **rule_fields}
    _drop_unverified_page_fields(fields, rule_fields, raw_text)
    dynamic_sections = _extract_dynamic_sections(raw_text, capture_payload)
    if dynamic_sections:
        fields["_dynamic_sections"] = dynamic_sections
    fields["报告编号"] = _clean(fields.get("报告编号")) or sn

    markdown = render_shuimui_report_markdown(
        fields,
        source_url=source_url,
        sn=sn,
        extraction_status="成功",
        original_status="可查看",
    )
    final_sections = re.findall(r"^###\s+(.+)$", markdown, flags=re.MULTILINE)
    logger.info("[ShuimuiExtract] parsed_sn=%s final_markdown_sections=%s", sn, ",".join(final_sections))
    summary_text = re.sub(r"\n{3,}", "\n\n", markdown)
    return {
        "document_type": DOC_TYPE,
        "document_type_code": DOC_TYPE,
        "document_type_name": DOC_TYPE_NAME,
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "owner_type": "company",
        "source_type": "url",
        "source_url": source_url,
        "report_sn": sn,
        "report_markdown": markdown,
        "markdown_summary": markdown,
        "summary": summary_text,
        "structured_json": fields,
        "extracted_json": fields,
        "data": fields,
        "extraction_status": "success",
        "skill_name": "shuimui_report",
        "skill_version": "1.0",
        "schema_version": "shuimui_report.v1",
    }


def build_failed_shuimui_report_content(*, source_url: str, sn: str, error_message: str, original_status: str) -> dict[str, Any]:
    fields = {field: UNKNOWN for field in ALL_FIELDS}
    fields["报告编号"] = sn or UNKNOWN
    markdown = render_shuimui_report_markdown(
        fields,
        source_url=source_url,
        sn=sn,
        extraction_status="失败",
        original_status=original_status or error_message or "链接不可访问",
    )
    return {
        "document_type": DOC_TYPE,
        "document_type_code": DOC_TYPE,
        "document_type_name": DOC_TYPE_NAME,
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "owner_type": "company",
        "source_type": "url",
        "source_url": source_url,
        "report_sn": sn,
        "report_markdown": markdown,
        "markdown_summary": markdown,
        "summary": error_message,
        "structured_json": fields,
        "extracted_json": fields,
        "data": fields,
        "extraction_status": "failed",
        "extraction_error": error_message,
        "skill_name": "shuimui_report",
        "skill_version": "1.0",
        "schema_version": "shuimui_report.v1",
    }


def to_safe_json_for_debug(content: dict[str, Any]) -> str:
    safe = {
        "document_type": content.get("document_type"),
        "report_sn": content.get("report_sn"),
        "extraction_status": content.get("extraction_status"),
    }
    return json.dumps(safe, ensure_ascii=False)
