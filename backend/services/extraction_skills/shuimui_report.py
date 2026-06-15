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

SECTION_FIELDS: list[tuple[str, list[str]]] = [
    ("企业基本信息", ["企业名称", "统一社会信用代码", "法定代表人", "成立日期", "注册资本", "企业状态", "注册地址", "经营范围"]),
    ("报告基础信息", ["报告编号", "查询时间", "报告生成时间", "数据更新时间", "授权状态"]),
    ("税务/发票信息", ["纳税人识别号", "纳税信用等级", "近期开票情况", "销项发票金额", "进项发票金额", "发票稳定性", "发票异常提示"]),
    ("经营与流水概况", ["经营稳定性", "近期开票趋势", "主要收入来源", "主要支出方向", "上下游集中度", "经营异常提示"]),
    ("上下游交易", ["主要上游客户", "主要下游客户", "关联交易提示", "内部转账/疑似异常交易"]),
    ("司法与风险信息", ["被执行信息", "失信信息", "裁判文书", "行政处罚", "经营异常", "股权冻结", "其他风险"]),
    ("融资参考结论", ["可采信经营情况", "主要优势", "主要风险", "需要补充资料", "建议授信关注点"]),
]

ALL_FIELDS = [field for _, fields in SECTION_FIELDS for field in fields]

LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "企业名称": ("企业名称", "公司名称", "主体名称", "被查询企业"),
    "统一社会信用代码": ("统一社会信用代码", "社会信用代码", "信用代码"),
    "法定代表人": ("法定代表人", "法人代表", "法人"),
    "成立日期": ("成立日期", "成立时间"),
    "注册资本": ("注册资本", "注册资金"),
    "企业状态": ("企业状态", "登记状态", "经营状态"),
    "注册地址": ("注册地址", "住所", "企业地址"),
    "经营范围": ("经营范围",),
    "报告编号": ("报告编号", "报告号", "水母报告编号", "sn"),
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
}


def _clean(value: Any, max_len: int = 500) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" ：:\t\r\n")
    if not text:
        return ""
    return text[:max_len].strip()


def _extract_after_label(text: str, aliases: tuple[str, ...], max_len: int = 220) -> str:
    for label in aliases:
        pattern = re.compile(rf"{re.escape(label)}\s*[：:]\s*([^\n\r]+)")
        match = pattern.search(text or "")
        if match:
            return _clean(match.group(1), max_len)
        loose = re.compile(rf"{re.escape(label)}\s+([^\n\r：:]+)")
        match = loose.search(text or "")
        if match:
            return _clean(match.group(1), max_len)
    return ""


def _rule_extract(raw_text: str, sn: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for field, aliases in LABEL_ALIASES.items():
        value = _extract_after_label(raw_text, aliases)
        if value:
            data[field] = value

    code_match = re.search(r"\b([0-9A-Z]{18})\b", raw_text or "")
    if code_match:
        data.setdefault("统一社会信用代码", code_match.group(1))
        data.setdefault("纳税人识别号", code_match.group(1))

    date_match = re.search(r"((?:19|20)\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)", raw_text or "")
    if date_match:
        data.setdefault("查询时间", date_match.group(1))

    data.setdefault("报告编号", sn)
    if "授权" in raw_text:
        data.setdefault("授权状态", "页面可访问，未识别到额外授权限制")
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
        return _clean(fields.get(field)) or UNKNOWN

    lines = [
        "## 水母报告",
        "",
        "* 资料类型：水母报告",
        f"* 来源链接：{source_url or UNKNOWN}",
        f"* 提取状态：{extraction_status or UNKNOWN}",
        f"* 报告编号：{sn or value('报告编号')}",
        f"* 原件状态：{original_status or UNKNOWN}",
        "",
    ]
    for section, section_fields in SECTION_FIELDS:
        lines.append(f"### {section}")
        lines.append("")
        for field in section_fields:
            lines.append(f"* {field}：{value(field)}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def extract_shuimui_report(
    raw_text: str,
    *,
    source_url: str,
    sn: str,
    ai_service: Any | None = None,
) -> dict[str, Any]:
    rule_fields = _rule_extract(raw_text, sn)
    llm_fields = _llm_extract(raw_text, ai_service)
    fields = {**llm_fields, **rule_fields}
    for field in ALL_FIELDS:
        fields.setdefault(field, UNKNOWN)
    fields["报告编号"] = _clean(fields.get("报告编号")) or sn

    markdown = render_shuimui_report_markdown(
        fields,
        source_url=source_url,
        sn=sn,
        extraction_status="成功",
        original_status="可查看",
    )
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
