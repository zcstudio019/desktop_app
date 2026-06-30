from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from .schema import CONTRACT_CATEGORY_NAMES, ContractParty


CONTRACT_KEYWORDS = (
    "建设工程专业分包合同", "机电安装专业分包合同", "机电安装工程专业分包合同", "物资采购合同", "材料采购合同",
    "BIM 深化咨询服务合同", "BIM深化咨询服务合同", "咨询服务合同", "分包人", "承包人", "发包人",
    "甲方", "乙方", "合同价款", "合同工期", "付款方式", "采购清单", "结算方式", "签订日期", "签订地点",
)

MONEY_RE = re.compile(r"(?:人民币)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:元|圆)")
DATE_RE = re.compile(r"((?:19|20)\d{2}\s*[年./-]\s*\d{1,2}\s*[月./-]\s*\d{1,2}\s*(?:日)?)")
USCC_RE = re.compile(r"\b([0-9A-Z]{18})\b")
PHONE_RE = re.compile(r"(?<!\d)(1[3-9]\d{9}|0\d{2,3}[- ]?\d{7,8})(?!\d)")
ID_CARD_RE = re.compile(r"(?<!\d)[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)")


def is_contract_like(text: str, filename: str = "") -> bool:
    source = f"{filename}\n{text}"
    lowered_name = str(filename or "").lower()
    if any(token in lowered_name for token in ("合同", "contract", "专业分包", "物资采购", "材料采购", "咨询服务", "bim")):
        return True
    return sum(1 for keyword in CONTRACT_KEYWORDS if keyword in source) >= 2


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" ：:;；，,。")


def _pages(text: str, pages: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    result = []
    for idx, page in enumerate(pages or [], start=1):
        if isinstance(page, dict):
            result.append({"page": int(page.get("page") or idx), "text": str(page.get("text") or "")})
    if not result and str(text or "").strip():
        result.append({"page": 1, "text": str(text or "")})
    return result


def _joined(pages: list[dict[str, Any]]) -> str:
    return "\n\n".join(f"--- 第 {page['page']} 页 ---\n{page['text']}" for page in pages if str(page.get("text") or "").strip())


def _source_page(pages: list[dict[str, Any]], value: str) -> int | None:
    if not value:
        return None
    for page in pages:
        if value in str(page.get("text") or ""):
            return int(page.get("page") or 0) or None
    return None


def _after_label(text: str, labels: tuple[str, ...], max_len: int = 120) -> str:
    for label in labels:
        pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([^\n\r]{{1,{max_len}}})")
        match = pattern.search(text)
        if match:
            return _clean(match.group(1))
    return ""


def _line_with(text: str, keywords: tuple[str, ...]) -> str:
    for line in str(text or "").splitlines():
        if any(keyword in line for keyword in keywords):
            return _clean(line)
    return ""


def _category(text: str, filename: str = "") -> str:
    source = f"{filename}\n{text}"
    if any(token in source for token in ("建设工程专业分包合同", "机电安装工程专业分包合同", "机电安装专业分包合同", "分包工程")) or ("承包人" in source and "分包人" in source):
        return "construction_subcontract"
    if any(token in source for token in ("物资采购合同", "材料采购合同", "货物名称", "计量单位", "含税单价", "合价")):
        return "material_purchase"
    if any(token in source for token in ("BIM 深化咨询服务合同", "BIM深化咨询服务合同", "咨询服务", "服务期限", "咨询费")):
        return "consulting_service"
    return "unknown_contract"


def _title(text: str, filename: str = "") -> str:
    for line in str(text or "").splitlines()[:80]:
        cleaned = _clean(line)
        if 4 <= len(cleaned) <= 80 and "合同" in cleaned and not any(x in cleaned for x in ("目录", "编号", "签订")):
            return cleaned
    return re.sub(r"\.pdf$", "", filename, flags=re.I) if filename else ""


def _amounts(text: str) -> tuple[dict[str, Any], dict[str, Any]]:
    evidence: dict[str, Any] = {}
    amount_text = _line_with(text, ("合同价款", "合同金额", "合同总金额", "价税合计", "咨询服务费", "暂定金额"))
    money_candidates = MONEY_RE.findall(amount_text) or MONEY_RE.findall(text)
    normalized = ""
    if money_candidates:
        normalized = money_candidates[0].replace(",", "")
    upper = _after_label(text, ("大写金额", "人民币大写", "金额大写", "大写"))
    tax_rate = ""
    tax_match = re.search(r"税率\s*[:：]?\s*(\d+(?:\.\d+)?%)", text)
    if tax_match:
        tax_rate = tax_match.group(1)
    status = "成功" if normalized or upper else "需人工复核"
    amount_check = "未校验"
    if upper and normalized:
        amount_check = "已同时识别大写和小写金额，需人工复核一致性"
    elif upper or normalized:
        amount_check = "仅识别到单侧金额，需人工复核"
    return {
        "contract_amount": f"人民币 {Decimal(normalized):,.2f} 元" if normalized else amount_text,
        "amount_upper": upper,
        "amount_lower": f"{Decimal(normalized):,.2f} 元" if normalized else "",
        "tax_included_amount": _line_with(text, ("含税金额", "价税合计", "含税合价")),
        "tax_excluded_amount": _line_with(text, ("不含税金额",)),
        "tax_rate": tax_rate,
        "tax_amount": _line_with(text, ("税额",)),
        "provisional_amount": _line_with(text, ("暂定金额",)),
        "currency": "元",
        "amount_check": amount_check,
        "recognition_status": status,
        "raw_amount_evidence": amount_text,
    }, evidence


def _party_name(text: str, labels: tuple[str, ...]) -> str:
    for label in labels:
        for pattern in (
            rf"{re.escape(label)}\s*[:：]\s*([^\n\r]+)",
            rf"{re.escape(label)}\s*[（(][^）)]*[）)]\s*[:：]?\s*([^\n\r]+)",
        ):
            match = re.search(pattern, text)
            if match:
                candidate = _clean(match.group(1))
                if candidate and not re.search(r"(地址|账号|电话|联系人|开户)", candidate):
                    return candidate[:80]
    return ""


def _extract_parties(text: str) -> list[ContractParty]:
    roles = [
        ("甲方/承包人/发包人", ("甲方", "发包人", "承包人", "买方", "需方", "委托方", "发包单位")),
        ("乙方/分包人/供方/受托方", ("乙方", "分包人", "供方", "卖方", "受托方", "分包单位", "咨询单位")),
    ]
    usccs = USCC_RE.findall(text)
    phones = PHONE_RE.findall(text)
    parties: list[ContractParty] = []
    for index, (role, labels) in enumerate(roles):
        name = _party_name(text, labels)
        block = _near_block(text, labels)
        parties.append(ContractParty(
            role=role,
            name=name,
            unified_social_credit_code=USCC_RE.search(block).group(1) if USCC_RE.search(block) else (usccs[index] if index < len(usccs) else ""),
            legal_representative=_after_label(block, ("法定代表人", "授权代表", "代表人")),
            contact=_after_label(block, ("联系人",)),
            phone=PHONE_RE.search(block).group(1) if PHONE_RE.search(block) else (phones[index] if index < len(phones) else ""),
            address=_after_label(block, ("地址", "住所", "通讯地址")),
            bank_name=_after_label(block, ("开户银行", "开户行")),
            bank_account=_after_label(block, ("银行账号", "账号", "收款账号")),
            taxpayer_id=_after_label(block, ("纳税人识别号", "税号")),
            stamp_status="疑似已盖章" if any(token in block for token in ("盖章", "公章", "合同专用章")) else "",
        ))
    return parties


def _near_block(text: str, labels: tuple[str, ...], window: int = 800) -> str:
    positions = [text.find(label) for label in labels if text.find(label) >= 0]
    if not positions:
        return ""
    start = min(positions)
    return text[start:start + window]


def _duration(text: str, category: str) -> dict[str, Any]:
    start = _after_label(text, ("计划开工日期", "开工日期", "服务开始时间", "开始日期", "交货时间"))
    end = _after_label(text, ("计划竣工日期", "竣工日期", "服务结束时间", "结束日期"))
    if not start:
        dates = DATE_RE.findall(text)
        start = _clean(dates[0]) if dates else ""
        end = _clean(dates[1]) if len(dates) > 1 else end
    period = _after_label(text, ("合同工期", "服务期限", "合同期限", "工期总日历天数"))
    return {
        "start_date": start,
        "end_date": end,
        "period": period,
        "extension_condition": _line_with(text, ("工期顺延", "顺延")),
        "delivery_place": _after_label(text, ("交货地点", "交付地点", "服务地点", "工程地点")),
        "delivery_method": _after_label(text, ("交付方式", "运输方式", "供货方式")),
        "acceptance_period": _after_label(text, ("验收期限", "验收时间")),
        "category": category,
    }


def _payment_nodes(text: str) -> list[dict[str, Any]]:
    nodes = []
    for line in str(text or "").splitlines():
        cleaned = _clean(line)
        if not cleaned:
            continue
        if any(token in cleaned for token in ("付款", "支付", "进度款", "预付款", "验收款", "质保金", "结算")):
            ratio = re.search(r"(\d+(?:\.\d+)?%|[0-9][0-9,]*(?:\.[0-9]{1,2})?\s*元)", cleaned)
            nodes.append({
                "node": f"节点{len(nodes) + 1}",
                "condition": cleaned[:120],
                "amount_or_ratio": ratio.group(1) if ratio else "",
                "remark": "",
            })
        if len(nodes) >= 10:
            break
    return nodes


def _line_items(text: str, category: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in str(text or "").splitlines():
        cleaned = _clean(line)
        if not re.match(r"^\d{1,3}[、.\s]", cleaned):
            continue
        if not any(token in cleaned for token in ("元", "米", "套", "项", "台", "kg", "m", "BIM", "电缆")):
            continue
        parts = re.split(r"\s{2,}|\t|[|｜]", cleaned)
        rows.append({
            "index": re.match(r"^(\d{1,3})", cleaned).group(1) if re.match(r"^(\d{1,3})", cleaned) else str(len(rows) + 1),
            "name": parts[1] if len(parts) > 1 else cleaned,
            "spec": parts[2] if len(parts) > 2 else "",
            "unit": parts[3] if len(parts) > 3 else "",
            "quantity": parts[4] if len(parts) > 4 else "",
            "unit_price": parts[5] if len(parts) > 5 else "",
            "total_price": parts[6] if len(parts) > 6 else "",
            "remark": parts[7] if len(parts) > 7 else "",
        })
        if len(rows) >= 50:
            break
    total = ""
    try:
        total_value = sum(Decimal(str(item.get("total_price") or "0").replace(",", "")) for item in rows if re.match(r"^[0-9,.]+$", str(item.get("total_price") or "")))
        if total_value:
            total = f"{total_value:,.2f} 元"
    except (InvalidOperation, ValueError):
        total = ""
    return rows, {
        "total_count": len(rows),
        "total_amount": total,
        "recognition_status": "成功" if rows else ("清单识别不完整，需人工复核" if category in {"material_purchase", "consulting_service"} else "未识别"),
    }


def _validation(result: dict[str, Any], text: str) -> dict[str, Any]:
    warnings: list[str] = []
    parties = result.get("parties") or []
    if len([p for p in parties if getattr(p, "name", "")]) < 2:
        warnings.append("至少两个合同主体未完整识别")
    if ID_CARD_RE.search(text):
        warnings.append("识别到身份证号码，展示时已脱敏，请人工确认附件用途")
    amount_status = (result.get("amount") or {}).get("recognition_status")
    if amount_status != "成功":
        warnings.append("合同金额需人工复核")
    recognized_keys = 0
    for key in ("title", "project_name", "contract_no", "signing_date"):
        recognized_keys += 1 if result.get(key) else 0
    recognized_keys += sum(1 for p in parties if getattr(p, "name", ""))
    completeness = f"{recognized_keys}/6"
    return {"is_valid": not warnings, "warnings": warnings, "completeness": completeness}


class ContractSkill:
    skill_name = "contract_skill"

    def extract(self, *, text: str, pages: list[dict[str, Any]] | None = None, filename: str = "") -> dict[str, Any]:
        page_items = _pages(text, pages)
        full_text = _joined(page_items) or str(text or "")
        category = _category(full_text, filename)
        amount, _ = _amounts(full_text)
        title = _title(full_text, filename)
        project_name = _after_label(full_text, ("工程名称", "项目名称", "工程项目名称", "采购项目", "服务项目"))
        parties = _extract_parties(full_text)
        line_items, line_summary = _line_items(full_text, category)
        result: dict[str, Any] = {
            "contract_category": category,
            "contract_category_name": CONTRACT_CATEGORY_NAMES[category],
            "title": title,
            "project_name": project_name,
            "contract_no": _after_label(full_text, ("合同编号", "合同号", "编号")),
            "signing_date": _after_label(full_text, ("签订日期", "签约日期")) or (_clean(DATE_RE.findall(full_text)[-1]) if DATE_RE.findall(full_text) else ""),
            "signing_place": _after_label(full_text, ("签订地点", "签约地点")),
            "effective_condition": _line_with(full_text, ("生效", "合同生效")),
            "copies": _line_with(full_text, ("合同份数", "一式")),
            "parties": parties,
            "project": {
                "project_name": project_name,
                "location": _after_label(full_text, ("工程地点", "项目地点", "交货地点", "服务地点")),
                "scope": _line_with(full_text, ("工程范围", "分包范围", "采购范围", "服务范围", "承包范围")),
                "method": _after_label(full_text, ("承包方式", "供货方式", "服务方式", "运输方式")),
                "quality_standard": _line_with(full_text, ("质量标准", "质量要求", "验收标准")),
                "safety_requirement": _line_with(full_text, ("安全文明施工", "安全施工")),
                "standards": _line_with(full_text, ("适用标准", "规范")),
            },
            "amount": amount,
            "duration": _duration(full_text, category),
            "payment_nodes": _payment_nodes(full_text),
            "settlement": {
                "settlement_method": _line_with(full_text, ("结算方式", "结算")),
                "invoice_requirement": _line_with(full_text, ("发票", "增值税专用发票", "开票")),
                "receiving_account": _line_with(full_text, ("收款账户", "开户银行", "银行账号")),
            },
            "line_items": line_items,
            "line_item_summary": line_summary,
            "clauses": {
                "quality_acceptance": _line_with(full_text, ("质量与验收", "验收标准", "质量标准")),
                "warranty": _line_with(full_text, ("保修期", "质保期", "质量保证金")),
                "breach_liability": _line_with(full_text, ("违约责任", "违约")),
                "dispute_resolution": _line_with(full_text, ("争议解决", "仲裁", "管辖法院")),
                "no_subcontract": _line_with(full_text, ("禁止转包", "不得转包", "不得分包")),
                "safety_civilization": _line_with(full_text, ("安全文明施工", "安全施工")),
                "confidentiality": _line_with(full_text, ("保密",)),
                "insurance": _line_with(full_text, ("保险",)),
                "intellectual_property": _line_with(full_text, ("知识产权", "成果归属")),
                "other": "",
            },
            "signature": {
                "party_a_stamp": "疑似有" if any(k in full_text for k in ("甲方盖章", "发包人盖章", "承包人盖章", "公章")) else "未识别",
                "party_b_stamp": "疑似有" if any(k in full_text for k in ("乙方盖章", "分包人盖章", "供方盖章", "合同专用章")) else "未识别",
                "signers": _line_with(full_text, ("签字", "签署", "授权代表")),
                "signature_page": _signature_page(page_items),
                "signing_date": "",
                "attachments": _line_with(full_text, ("附件", "授权委托书", "身份证复印件")),
            },
            "quality": {
                "ocr_quality": "可用" if len(full_text.strip()) >= 100 else "文本较少，可能需要重新OCR",
            },
            "evidence": {},
        }
        result["signature"]["signing_date"] = result["signing_date"]
        result["validation"] = _validation(result, full_text)
        result["warnings"] = list(result["validation"].get("warnings") or [])
        for key, value in {
            "contract_amount": amount.get("contract_amount"),
            "signing_date": result["signing_date"],
            "project_name": project_name,
        }.items():
            page = _source_page(page_items, str(value or ""))
            if page:
                result["evidence"][key] = {"value": value, "source_page": page, "raw_text": "", "confidence": 0.7}
        result["page_count"] = len(page_items)
        result["extraction_status"] = "success" if not result["warnings"] else "partial"
        return result


def _signature_page(pages: list[dict[str, Any]]) -> str:
    for page in reversed(pages):
        text = str(page.get("text") or "")
        if any(token in text for token in ("签字", "盖章", "公章", "合同专用章", "签订日期")):
            return f"第 {page.get('page')} 页"
    return ""
