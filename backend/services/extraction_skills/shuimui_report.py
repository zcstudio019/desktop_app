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

SECTION_FIELDS: list[tuple[str, list[str]]] = [
    ("企业基本信息", ["企业名称", "统一社会信用代码", "法定代表人", "法人占股比例", "成立日期", "注册资本", "注册类型", "注册地址", "行业分类"]),
    ("报告基础信息", ["报告编号", "报告创建时间", "查询时间", "报告生成时间", "数据更新时间", "授权状态"]),
    ("社保信息", ["社保人数", "应缴费额"]),
    ("股东信息", ["股东名称", "参股比例"]),
    ("法人/股东变更", ["变更类型", "变更时间", "变更前", "变更后"]),
    ("银税互动授权记录", ["授权记录"]),
    ("纳税信息", ["纳税信用等级", "纳税状态", "纳税金额", "税种", "税款所属期", "申报状态", "欠税信息", "税务异常"]),
    ("发票信息", ["开票总金额", "开票月份", "销项发票金额", "进项发票金额", "发票张数", "作废发票", "红冲发票", "主要开票品类", "发票异常提示"]),
    ("供应商信息", ["供应商名称", "交易金额", "交易次数", "占比", "最近交易时间", "集中度提示"]),
    ("经营与流水概况", ["经营稳定性", "近期开票趋势", "主要收入来源", "主要支出方向", "上下游集中度", "经营异常提示"]),
    ("上下游交易", ["主要上游客户", "主要下游客户", "关联交易提示", "内部转账/疑似异常交易"]),
    ("司法与风险信息", ["被执行信息", "失信信息", "裁判文书", "行政处罚", "经营异常", "股权冻结", "其他风险"]),
    ("融资参考结论", ["可采信经营情况", "主要优势", "主要风险", "需要补充资料", "建议授信关注点"]),
]

ALL_FIELDS = [field for _, fields in SECTION_FIELDS for field in fields]

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
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:\t\r\n")
    text = re.sub(r"\s*复制\s*$", "", text).strip()
    if text in EMPTY_VALUES or text in HEADER_VALUES:
        return ""
    if not text:
        return ""
    return text[:max_len].strip()


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


def _supplier_display_rows(section_text: str) -> list[tuple[str, str]]:
    records = _extract_table_records(section_text)
    supplier_records: list[dict[str, str]] = []
    for record in records:
        name_key = next((key for key in record if any(token in key for token in ("供应商", "客户名称", "企业名称", "名称"))), "")
        if name_key and _clean(record.get(name_key)):
            supplier_records.append(record)

    if supplier_records:
        rows: list[tuple[str, str]] = []
        for index, record in enumerate(supplier_records[:20], 1):
            name_key = next((key for key in record if any(token in key for token in ("供应商", "客户名称", "企业名称", "名称"))), "")
            amount_key = next((key for key in record if "金额" in key), "")
            count_key = next((key for key in record if any(token in key for token in ("次数", "笔数"))), "")
            ratio_key = next((key for key in record if any(token in key for token in ("占比", "比例"))), "")
            time_key = next((key for key in record if "时间" in key or "日期" in key), "")
            parts = [_clean(record.get(name_key), 120)]
            if amount_key and _clean(record.get(amount_key)):
                parts.append(f"交易金额 {_clean(record.get(amount_key), 80)}")
            if count_key and _clean(record.get(count_key)):
                parts.append(f"交易次数 {_clean(record.get(count_key), 80)}")
            if ratio_key and _clean(record.get(ratio_key)):
                parts.append(f"占比 {_clean(record.get(ratio_key), 80)}")
            if time_key and _clean(record.get(time_key)):
                parts.append(f"最近交易时间 {_clean(record.get(time_key), 80)}")
            _add_dynamic_field(rows, f"供应商 {index}", "，".join(part for part in parts if part))
        return rows
    return _extract_dynamic_section_fields(section_text, ["供应商名称", "交易金额", "交易次数", "占比", "最近交易时间", "集中度提示"])


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
    dynamic = {
        "纳税信息": _extract_dynamic_section_fields(tax_text, ["纳税信用等级", "纳税状态", "纳税金额", "税种", "税款所属期", "申报状态", "欠税信息", "税务异常"]),
        "发票信息": _extract_dynamic_section_fields(invoice_text, ["开票总金额", "开票月份", "销项发票金额", "进项发票金额", "发票张数", "作废发票", "红冲发票", "主要开票品类", "发票异常提示"]),
        "供应商信息": _supplier_display_rows(supplier_text),
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


def render_shuimui_report_markdown(
    fields: dict[str, Any],
    *,
    source_url: str,
    sn: str,
    extraction_status: str = "成功",
    original_status: str = "可查看",
) -> str:
    def value(field: str) -> str:
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
        rows: list[tuple[str, str]] = []
        dynamic_sections = fields.get("_dynamic_sections") if isinstance(fields.get("_dynamic_sections"), dict) else {}
        if section in dynamic_sections:
            dynamic_rows = dynamic_sections.get(section)
            if isinstance(dynamic_rows, list):
                rows = [
                    (str(item[0]), str(item[1]))
                    for item in dynamic_rows
                    if isinstance(item, (list, tuple)) and len(item) >= 2 and _clean(item[0], 80) and _clean(item[1], 220)
                ]
                if not rows:
                    continue
                lines.append(f"### {section}")
                lines.append("")
                for field, field_value in rows:
                    lines.append(f"* {field}：{field_value}")
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
            lines.append(f"* {field}：{field_value}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


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
