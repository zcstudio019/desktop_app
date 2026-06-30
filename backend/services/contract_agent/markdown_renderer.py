from __future__ import annotations

import re
from typing import Any

from .schema import ContractResult


MISSING = "未识别"
ID_CARD_RE = re.compile(r"(?<!\d)([1-9]\d{5})(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}([\dXx])(?!\d)")


def mask_sensitive_text(value: Any) -> str:
    text = str(value or "").strip()
    text = ID_CARD_RE.sub(lambda m: f"{m.group(1)[:4]}********{m.group(2)}", text)
    return text


def value(value: Any) -> str:
    text = mask_sensitive_text(value)
    if not text or text.lower() in {"none", "null", "undefined", "nan"}:
        return MISSING
    return text


def evidence_suffix(result: ContractResult, key: str) -> str:
    evidence = result.evidence.get(key) if isinstance(result.evidence, dict) else None
    if not isinstance(evidence, dict):
        return ""
    page = evidence.get("source_page")
    if not page:
        return ""
    return f"（来源页：第 {page} 页）"


def _row(cells: list[Any]) -> str:
    return "| " + " | ".join(value(cell).replace("\n", " ") for cell in cells) + " |"


def render_contract_markdown(result: ContractResult) -> str:
    amount = result.amount or {}
    project = result.project or {}
    duration = result.duration or {}
    settlement = result.settlement or {}
    clauses = result.clauses or {}
    signature = result.signature or {}
    quality = result.quality or {}
    validation = result.validation or {}
    parties = result.parties[:2]
    while len(parties) < 2:
        parties.append(type(result.parties[0])() if result.parties else None)  # type: ignore[arg-type]

    party_rows = []
    for party in parties:
        party_rows.append(_row([
            getattr(party, "role", ""),
            getattr(party, "name", ""),
            getattr(party, "unified_social_credit_code", ""),
            getattr(party, "legal_representative", ""),
            getattr(party, "contact", ""),
            getattr(party, "phone", ""),
            getattr(party, "address", ""),
        ]))

    payment_rows = [
        _row([item.get("node"), item.get("condition"), item.get("amount_or_ratio"), item.get("remark")])
        for item in (result.payment_nodes or [])[:20]
        if isinstance(item, dict)
    ] or [_row(["", "", "", ""])]

    item_count = len(result.line_items or [])
    item_rows = [
        _row([
            item.get("index"),
            item.get("name") or item.get("service_content"),
            item.get("spec"),
            item.get("unit"),
            item.get("quantity"),
            item.get("unit_price"),
            item.get("total_price") or item.get("amount"),
            item.get("remark"),
        ])
        for item in (result.line_items or [])[:20]
        if isinstance(item, dict)
    ] or [_row(["", "", "", "", "", "", "", ""])]
    item_notice = ""
    if item_count > 20:
        item_notice = f"\n\n- 清单展示：共识别 {item_count} 条，页面仅展示前 20 条，可展开查看全部。"

    warnings = result.warnings or validation.get("warnings") or []
    review_items = "；".join(str(item) for item in warnings if item) or "无"
    completeness = validation.get("completeness") or quality.get("field_completeness") or MISSING

    return "\n".join([
        "## 合同",
        "",
        "- 资料类型：合同",
        f"- 合同类型：{value(result.contract_category_name)}",
        f"- 来源文件：{value(result.source_file)}",
        f"- 原件状态：{value(result.original_status)}",
        f"- 提取状态：{value(result.extraction_status)}",
        "",
        "### 合同基本信息",
        "",
        f"- 合同名称：{value(result.title)}",
        f"- 项目名称：{value(result.project_name or project.get('project_name'))}",
        f"- 合同编号：{value(result.contract_no)}",
        f"- 签订日期：{value(result.signing_date)}{evidence_suffix(result, 'signing_date')}",
        f"- 签订地点：{value(result.signing_place)}",
        f"- 合同页数：{value(result.page_count)}",
        f"- 合同生效条件：{value(result.effective_condition)}",
        f"- 合同份数：{value(result.copies)}",
        "",
        "### 合同主体",
        "",
        "| 角色 | 名称 | 统一社会信用代码 | 法定代表人/授权代表 | 联系人 | 电话 | 地址 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        *party_rows,
        "",
        "### 项目/服务内容",
        "",
        f"- 工程或项目名称：{value(project.get('project_name') or result.project_name)}",
        f"- 工程或服务地点：{value(project.get('location'))}",
        f"- 合同范围：{value(project.get('scope'))}",
        f"- 承包/采购/服务方式：{value(project.get('method'))}",
        f"- 质量标准：{value(project.get('quality_standard') or clauses.get('quality_acceptance'))}",
        "",
        "### 合同金额",
        "",
        f"- 合同金额：{value(amount.get('contract_amount'))}{evidence_suffix(result, 'contract_amount')}",
        f"- 大写金额：{value(amount.get('amount_upper'))}",
        f"- 小写金额：{value(amount.get('amount_lower'))}",
        f"- 含税金额：{value(amount.get('tax_included_amount'))}",
        f"- 不含税金额：{value(amount.get('tax_excluded_amount'))}",
        f"- 税率：{value(amount.get('tax_rate'))}",
        f"- 税额：{value(amount.get('tax_amount'))}",
        f"- 金额校验：{value(amount.get('amount_check'))}",
        f"- 金额识别状态：{value(amount.get('recognition_status'))}",
        "",
        "### 工期/交付/服务期限",
        "",
        f"- 开始日期：{value(duration.get('start_date'))}",
        f"- 结束日期：{value(duration.get('end_date'))}",
        f"- 合同工期/服务期限：{value(duration.get('period'))}",
        f"- 交付地点：{value(duration.get('delivery_place'))}",
        f"- 交付方式：{value(duration.get('delivery_method'))}",
        f"- 验收期限：{value(duration.get('acceptance_period'))}",
        "",
        "### 付款与结算",
        "",
        "| 节点 | 触发条件 | 支付比例/金额 | 备注 |",
        "| --- | --- | --- | --- |",
        *payment_rows,
        "",
        f"- 结算方式：{value(settlement.get('settlement_method'))}",
        f"- 发票要求：{value(settlement.get('invoice_requirement'))}",
        f"- 收款账户：{value(settlement.get('receiving_account'))}",
        "",
        "### 清单明细",
        "",
        "| 序号 | 名称/服务内容 | 型号规格 | 单位 | 数量 | 单价 | 合价 | 备注 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
        *item_rows,
        item_notice,
        f"- 合计金额：{value(result.line_item_summary.get('total_amount') if isinstance(result.line_item_summary, dict) else '')}",
        f"- 清单识别状态：{value(result.line_item_summary.get('recognition_status') if isinstance(result.line_item_summary, dict) else '')}",
        "",
        "### 重要条款摘要",
        "",
        f"- 质量与验收：{value(clauses.get('quality_acceptance'))}",
        f"- 保修/质保：{value(clauses.get('warranty'))}",
        f"- 违约责任：{value(clauses.get('breach_liability'))}",
        f"- 争议解决：{value(clauses.get('dispute_resolution'))}",
        f"- 发票要求：{value(settlement.get('invoice_requirement') or clauses.get('invoice_requirement'))}",
        f"- 禁止转包/分包：{value(clauses.get('no_subcontract'))}",
        f"- 安全文明施工：{value(clauses.get('safety_civilization'))}",
        f"- 其他重要条款：{value(clauses.get('other'))}",
        "",
        "### 签章信息",
        "",
        f"- 甲方签章：{value(signature.get('party_a_stamp'))}",
        f"- 乙方签章：{value(signature.get('party_b_stamp'))}",
        f"- 签字人：{value(signature.get('signers'))}",
        f"- 签章页：{value(signature.get('signature_page'))}",
        f"- 签订日期：{value(signature.get('signing_date') or result.signing_date)}",
        f"- 附件情况：{value(signature.get('attachments'))}",
        "",
        "### 解析质量提示",
        "",
        f"- OCR质量：{value(quality.get('ocr_quality'))}",
        f"- 关键字段完整度：{value(completeness)}",
        f"- 需人工复核事项：{value(review_items)}",
    ]).replace("\n\n\n", "\n\n").strip()
