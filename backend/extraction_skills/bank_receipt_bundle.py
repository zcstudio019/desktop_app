from __future__ import annotations

import re
import json
import logging
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult


DOC_TYPE = "bank_receipt_bundle"
DOC_TYPE_NAME = "银行回单集合"
logger = logging.getLogger(__name__)


def _clean(value: Any) -> str:
    text = re.sub(r"[\t\r ]+", " ", str(value or ""))
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = text.replace("有限 公司", "有限公司").replace("有 限公司", "有限公司").replace("公 司", "公司")
    return text.strip(" \n\t:：,，。；;")


def _clean_party_name(value: Any) -> str:
    text = _clean(value).replace("\n", "")
    text = re.sub(r"(付款人名称?|付款单位|付款户名|汇款人|付款方|付款账户名称|收款人名称?|收款单位|收款户名|收款方|收款账户名称|账号|账户|金额|用途|摘要|附言|备注)\s*[:：]?", "", text)
    text = _clean(text)
    if len(re.findall(r"(?:有限)?公司", text)) >= 2:
        return text[:80]
    return text[:80]


def _cell(value: Any) -> str:
    return _clean(value).replace("|", "\\|") or "—"


def _money_value(value: Any) -> Decimal | None:
    if value in (None, "", "—", "未识别"):
        return None
    text = str(value)
    text = re.sub(r"[￥¥元人民币,\s]", "", text)
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _money(value: Any) -> str:
    amount = _money_value(value)
    return f"{amount:,.2f}" if amount is not None else "未识别"


def _cn_number_to_decimal(value: str) -> Decimal | None:
    text = _clean(value)
    if not text:
        return None
    digits = {
        "零": 0, "〇": 0, "一": 1, "壹": 1, "二": 2, "贰": 2, "两": 2, "三": 3, "叁": 3,
        "四": 4, "肆": 4, "五": 5, "伍": 5, "六": 6, "陆": 6, "七": 7, "柒": 7,
        "八": 8, "捌": 8, "九": 9, "玖": 9,
    }
    small_units = {"十": 10, "拾": 10, "百": 100, "佰": 100, "千": 1000, "仟": 1000}
    big_units = {"万": 10000, "亿": 100000000}

    def parse_integer(part: str) -> int:
        total = 0
        section = 0
        number = 0
        for char in part:
            if char in digits:
                number = digits[char]
            elif char in small_units:
                unit = small_units[char]
                section += (number or 1) * unit
                number = 0
            elif char in big_units:
                section += number
                total += section * big_units[char]
                section = 0
                number = 0
        return total + section + number

    integer_text = text.split("元", 1)[0].replace("人民币", "").replace("整", "")
    if not integer_text:
        return None
    amount = Decimal(parse_integer(integer_text))
    jiao = re.search(r"([零〇一壹二贰两三叁四肆五伍六陆七柒八捌九玖])角", text)
    fen = re.search(r"([零〇一壹二贰两三叁四肆五伍六陆七柒八捌九玖])分", text)
    if jiao:
        amount += Decimal(digits[jiao.group(1)]) / Decimal("10")
    if fen:
        amount += Decimal(digits[fen.group(1)]) / Decimal("100")
    return amount


def _bank_name(text: str) -> str:
    if "中国工商银行" in text or "工商银行" in text or "工行" in text:
        return "中国工商银行"
    if "中国银行" in text:
        return "中国银行"
    for name in ("建设银行", "农业银行", "招商银行", "交通银行", "浦发银行", "上海银行", "中信银行", "民生银行", "平安银行", "兴业银行", "光大银行"):
        if name in text:
            return name if name.startswith("中国") else name
    return ""


def _label_value(text: str, labels: tuple[str, ...], *, max_chars: int = 120) -> str:
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_labels = (
        "付款人", "付款人名称", "付款单位", "付款户名", "汇款人", "付款方", "付款账户名称", "付款账号", "付款账户",
        "收款人", "收款人名称", "收款单位", "收款户名", "收款方", "收款账户名称", "收款账号", "收款账户",
        "付款方", "收款方", "金额", "交易金额", "汇款金额", "人民币金额", "小写金额", "大写金额", "用途", "汇款用途", "交易用途", "附言", "备注", "摘要",
        "回单编号", "业务编号", "流水号", "交易流水号", "指令编号", "日期", "交易日期", "回单日期",
        "付款银行", "收款银行", "开户行", "状态", "渠道",
    )
    stop_pattern = "|".join(re.escape(item) for item in stop_labels if item not in labels)
    pattern = rf"(?:{label_pattern})\s*[:：]?\s*([\s\S]{{0,{max_chars}}}?)(?=\s*(?:{stop_pattern})\s*[:：]?|\n|$)"
    match = re.search(pattern, text)
    if not match:
        return ""
    value = _clean(match.group(1))
    value = re.sub(rf"^(?:{label_pattern})\s*[:：]?", "", value).strip()
    return value[:max_chars].strip()


def _extract_account(text: str, labels: tuple[str, ...]) -> str:
    labeled = _label_value(text, labels, max_chars=80)
    match = re.search(r"\b(\d{8,32})\b", labeled)
    if match:
        return match.group(1)
    label_pattern = "|".join(re.escape(label) for label in labels)
    match = re.search(rf"(?:{label_pattern})\s*[:：]?\s*(\d{{8,32}})", text)
    return match.group(1) if match else ""


def _unknown_accounts(text: str, known: set[str]) -> list[str]:
    accounts = []
    for match in re.finditer(r"\b\d{8,32}\b", text):
        value = match.group(0)
        if value in known:
            continue
        around = text[max(0, match.start() - 12): match.end() + 12]
        if any(keyword in around for keyword in ("回单编号", "业务编号", "流水号", "交易流水号", "指令编号", "金额", "日期", "电话", "页")):
            continue
        accounts.append(value)
    return accounts[:5]


def _extract_amount(text: str) -> Decimal | None:
    amount_labels = ("交易金额", "汇款金额", "人民币金额", "小写金额", "金额")
    for label in amount_labels:
        match = re.search(rf"{re.escape(label)}\s*[:：]?\s*(?:人民币|￥|¥)?\s*([0-9][\d,]*\.\d{{2}})", text)
        if match:
            return _money_value(match.group(1))
    cn_match = re.search(r"(?:大写金额|金额大写|人民币大写)\s*[:：]?\s*(人民币?[零〇一壹二贰两三叁四肆五伍六陆七柒八捌九玖十拾百佰千仟万亿]+元(?:[零〇一壹二贰两三叁四肆五伍六陆七柒八捌九玖]角)?(?:[零〇一壹二贰两三叁四肆五伍六陆七柒八捌九玖]分)?整?)", text)
    if cn_match:
        return _cn_number_to_decimal(cn_match.group(1))
    return None


def _fallback_amount_candidate(text: str) -> Decimal | None:
    candidates: list[Decimal] = []
    for match in re.finditer(r"(?<!\d)(?:￥|¥|CNY\s*)?([1-9]\d{0,2}(?:,\d{3})*\.\d{2}|[1-9]\d+\.\d{2})(?:元)?(?!\d)", text, re.I):
        around = text[max(0, match.start() - 20): match.end() + 20]
        if any(keyword in around for keyword in ("日期", "账号", "账户", "编号", "流水号", "指令", "页码", "电话")):
            continue
        amount = _money_value(match.group(1))
        if amount is not None:
            candidates.append(amount)
    return candidates[0] if len(candidates) == 1 else None


def _extract_date(text: str) -> tuple[str, str]:
    label = r"(?:回单日期|交易日期|业务日期|日期|付款日期|汇款日期|记账日期)"
    match = re.search(rf"{label}\s*[:：]?\s*((?:20\d{{2}})[-/年.](?:0?\d|1[0-2])[-/月.](?:[0-3]?\d)日?)(?:\s+(\d{{2}}:\d{{2}}:\d{{2}}))?", text)
    if not match:
        match = re.search(r"((?:20\d{2})[-/年.](?:0?\d|1[0-2])[-/月.](?:[0-3]?\d)日?)(?:\s+(\d{2}:\d{2}:\d{2}))?", text)
    if not match:
        return "", ""
    raw = match.group(1)
    parts = re.findall(r"\d+", raw)
    if len(parts) >= 3:
        date = f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    else:
        date = ""
    return date, match.group(2) or ""


def _receipt_no(text: str) -> tuple[str, str]:
    receipt_no = _label_value(text, ("回单编号", "回单号"), max_chars=80)
    business_no = _label_value(text, ("业务编号", "流水号", "交易流水号", "指令编号"), max_chars=80)
    if not receipt_no:
        match = re.search(r"(?:回单编号|回单号)\s*[:：]?\s*([A-Za-z0-9._-]{6,80})", text)
        receipt_no = match.group(1) if match else ""
    if not business_no:
        match = re.search(r"(?:业务编号|流水号|交易流水号|指令编号)\s*[:：]?\s*([A-Za-z0-9._-]{6,80})", text)
        business_no = match.group(1) if match else ""
    return receipt_no, business_no


def _split_blocks(pages: list[dict[str, Any]], source_text: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    primary_markers = ("中国工商银行电子回单", "网上银行电子回单", "电子回单", "交易回单")
    secondary_markers = ("单位国内汇款手续费", "单位国内汇款手", "单位国内汇款", "回单编号", "业务编号", "交易流水号", "指令序号", "付款人", "收款人")
    if pages:
        for index, page in enumerate(pages, start=1):
            text = str(page.get("text") or "")
            if not text.strip():
                continue
            starts = sorted({m.start() for marker in primary_markers for m in re.finditer(re.escape(marker), text)})
            if not starts:
                starts = sorted({
                    m.start()
                    for marker in secondary_markers
                    for m in re.finditer(rf"(?m)^\s*{re.escape(marker)}(?:\s*[:：]?\s*\S*)?\s*$", text)
                })
            if len(starts) <= 1:
                blocks.append({"page": int(page.get("page") or index), "text": text})
                continue
            for pos, start in enumerate(starts):
                end = starts[pos + 1] if pos + 1 < len(starts) else len(text)
                chunk = text[start:end]
                if len(chunk.strip()) >= 20:
                    blocks.append({"page": int(page.get("page") or index), "text": chunk})
    if not blocks and source_text.strip():
        blocks.append({"page": 1, "text": source_text})
    return blocks


def split_receipt_blocks(raw_text: str, ocr_pages: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    return _split_blocks(ocr_pages or [], raw_text)


def _parse_receipt(block: dict[str, Any], fallback_bank: str) -> dict[str, Any]:
    text = _clean(block.get("text"))
    date, time = _extract_date(text)
    receipt_no, business_no = _receipt_no(text)
    payer_name = _clean_party_name(_label_value(text, ("付款人名称", "付款人", "付款单位", "付款户名", "汇款人", "付款方", "付款账户名称"), max_chars=100))
    payee_name = _clean_party_name(_label_value(text, ("收款人名称", "收款人", "收款单位", "收款户名", "收款方", "收款账户名称"), max_chars=100))
    payer_account = _extract_account(text, ("付款账号", "付款人账号", "付款账户", "付款人账户"))
    payee_account = _extract_account(text, ("收款账号", "收款人账号", "收款账户", "收款人账户"))
    amount = _extract_amount(text)
    amount_source = "labeled_amount" if amount is not None else ""
    if amount is None:
        amount = _fallback_amount_candidate(text)
        amount_source = "single_amount_candidate" if amount is not None else ""
    purpose = _label_value(text, ("汇款用途", "交易用途", "用途", "附言", "备注"), max_chars=120)
    summary = _label_value(text, ("摘要",), max_chars=120)
    remark = _label_value(text, ("备注",), max_chars=120)
    known_accounts = {item for item in (payer_account, payee_account) if item}
    unknown_accounts = _unknown_accounts(text, known_accounts)
    receipt = {
        "receipt_date": date,
        "receipt_time": time,
        "bank_name": _bank_name(text) or fallback_bank,
        "receipt_no": receipt_no,
        "business_no": business_no,
        "payer_name": payer_name,
        "payer_account": payer_account,
        "payer_bank": _label_value(text, ("付款银行", "付款行", "付款开户行"), max_chars=80),
        "payee_name": payee_name,
        "payee_account": payee_account,
        "payee_bank": _label_value(text, ("收款银行", "收款行", "收款开户行"), max_chars=80),
        "amount": amount,
        "amount_source": amount_source,
        "currency": "人民币" if "人民币" in text or amount is not None else "",
        "summary": summary,
        "purpose": purpose,
        "remark": remark,
        "channel": _label_value(text, ("渠道", "交易渠道"), max_chars=60),
        "status": _label_value(text, ("状态", "交易状态"), max_chars=60),
        "source_page": int(block.get("page") or 1),
        "confidence": 0.0,
        "unknown_accounts": unknown_accounts,
        "raw_text": text[:1000],
    }
    criteria = {
        "date": bool(date),
        "amount": amount is not None,
        "party": bool(payer_name or payee_name),
        "account": bool(payer_account or payee_account or unknown_accounts),
        "purpose_or_summary": bool(purpose or summary or remark),
        "receipt_no": bool(receipt_no or business_no),
    }
    score = 0
    score += 0.22 if criteria["date"] else 0
    score += 0.24 if criteria["amount"] else 0
    score += 0.22 if criteria["party"] else 0
    score += 0.14 if criteria["account"] else 0
    score += 0.10 if criteria["purpose_or_summary"] else 0
    score += 0.08 if criteria["receipt_no"] else 0
    receipt["confidence"] = round(score, 2)
    receipt["matched_criteria"] = [name for name, matched in criteria.items() if matched]
    receipt["is_valid"] = sum(1 for matched in criteria.values() if matched) >= 2
    if not receipt["is_valid"]:
        receipt["reject_reason"] = "有效字段不足，未满足回单候选最小条件"
    elif not (payer_name or payee_name):
        receipt["reject_reason"] = "缺少收付款方，需人工复核"
    elif amount is None:
        receipt["reject_reason"] = "缺少金额，需人工复核"
    else:
        receipt["reject_reason"] = ""
    return receipt


def _aggregate(receipts: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for receipt in receipts:
        name = _clean(receipt.get(key))
        if not name:
            continue
        item = groups.setdefault(name, {"name": name, "count": 0, "amount": Decimal("0"), "purposes": Counter()})
        item["count"] += 1
        item["amount"] += _money_value(receipt.get("amount")) or Decimal("0")
        purpose = _clean(receipt.get("purpose") or receipt.get("summary") or receipt.get("remark"))
        if purpose:
            item["purposes"][purpose] += 1
    rows = []
    for index, item in enumerate(sorted(groups.values(), key=lambda x: (-x["amount"], -x["count"], x["name"])), start=1):
        rows.append({
            "rank": index,
            "name": item["name"],
            "count": item["count"],
            "amount": item["amount"],
            "main_purpose": "、".join(name for name, _ in item["purposes"].most_common(3)),
        })
    return rows


def render_bank_receipt_bundle_markdown(result: dict[str, Any]) -> str:
    lines = [
        "## 银行回单集合",
        "",
        "- 资料类型：银行回单集合",
        f"- 来源文件：{_cell(result.get('source_file'))}",
        "- 原件状态：可查看",
        f"- 提取状态：{_cell(result.get('extract_status'))}",
        f"- 识别银行：{_cell(result.get('bank_name'))}",
        f"- 回单数量：{result.get('receipt_count', 0)} 张",
        f"- 有效回单数量：{result.get('valid_receipt_count', 0)} 张",
        f"- 可识别金额合计：{_money(result.get('recognizable_amount_total'))}",
        "- 是否纳入银行流水聚合：否",
        "- 说明：银行回单集合仅作为交易凭证提取，不作为标准账户流水参与经营流水统计。",
        "",
        "### 回单明细",
        "| 序号 | 回单日期 | 付款方 | 付款账号 | 收款方 | 收款账号 | 金额 | 用途 | 摘要 | 回单编号 |",
        "|---:|---|---|---|---|---|---:|---|---|---|",
    ]
    receipts = result.get("receipts") or []
    if not receipts:
        lines.append("| — | 暂未形成可用回单明细。 | — | — | — | — | 未识别 | — | — | — |")
    for index, receipt in enumerate(receipts, start=1):
        lines.append(
            f"| {index} | {_cell(receipt.get('receipt_date'))} | {_cell(receipt.get('payer_name'))} | {_cell(receipt.get('payer_account'))} | "
            f"{_cell(receipt.get('payee_name'))} | {_cell(receipt.get('payee_account'))} | {_money(receipt.get('amount'))} | "
            f"{_cell(receipt.get('purpose'))} | {_cell(receipt.get('summary'))} | {_cell(receipt.get('receipt_no') or receipt.get('business_no'))} |"
        )
    lines += ["", "### 按收款方汇总", "| 排名 | 收款方 | 笔数 | 金额合计 | 主要用途 |", "|---:|---|---:|---:|---|"]
    for item in result.get("payee_summary") or []:
        lines.append(f"| {item.get('rank')} | {_cell(item.get('name'))} | {item.get('count', 0)} | {_money(item.get('amount'))} | {_cell(item.get('main_purpose'))} |")
    lines += ["", "### 按付款方汇总", "| 排名 | 付款方 | 笔数 | 金额合计 | 主要用途 |", "|---:|---|---:|---:|---|"]
    for item in result.get("payer_summary") or []:
        lines.append(f"| {item.get('rank')} | {_cell(item.get('name'))} | {item.get('count', 0)} | {_money(item.get('amount'))} | {_cell(item.get('main_purpose'))} |")
    lines += [
        "",
        "### 需人工复核",
        "- 当前文件为银行回单集合，不是标准银行账户流水。",
        "- 回单可作为交易凭证辅助核验，但不能直接替代银行流水。",
        "- 如需经营流水分析，请上传银行账户明细/账户流水 PDF 或 Excel。",
    ]
    for item in result.get("manual_review_items") or []:
        if item not in "\n".join(lines):
            lines.append(f"- {item}")
    return "\n".join(lines).replace("None", "").replace("null", "").replace("undefined", "")


class BankReceiptBundleSkill(BaseExtractionSkill):
    document_type = DOC_TYPE
    supported_extensions = {".pdf", ".jpg", ".jpeg", ".png"}
    skill_name = "bank_receipt_bundle_skill"
    skill_version = "v1"

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        metadata = input_data.metadata or {}
        raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else []
        source_text = "\n".join([str(input_data.raw_text or ""), *[str(page.get("text") or "") for page in raw_pages]])
        source_file = input_data.file_name or (Path(input_data.file_path).name if input_data.file_path else "")
        receipt_keywords = ("中国工商银行电子回单", "电子回单", "网上银行电子回单", "单位国内汇款", "单位国内汇款手续费", "单位国内汇款手", "回单编号", "业务编号", "交易流水号", "指令序号", "付款人", "收款人", "付款账号", "收款账号", "汇款金额", "交易金额")
        keyword_hits = [keyword for keyword in receipt_keywords if keyword in source_text]
        bank_name = _bank_name(source_text)
        blocks = split_receipt_blocks(source_text, raw_pages)
        receipts = [_parse_receipt(block, bank_name) for block in blocks]
        valid = [item for item in receipts if item.get("is_valid")]
        total = sum((_money_value(item.get("amount")) or Decimal("0") for item in valid), Decimal("0"))
        logger.info("[BankReceiptBundleAgent] activated=true")
        logger.info("[BankReceiptBundleAgent] source_file=%s", source_file)
        logger.info("[BankReceiptBundleAgent] raw_text_len=%s", len(source_text or ""))
        logger.info("[BankReceiptBundleAgent] receipt_keyword_hits=%s", keyword_hits)
        logger.info("[BankReceiptBundleAgent] receipt_blocks_count=%s", len(blocks))
        logger.info("[BankReceiptBundleAgent] candidate_receipts_count=%s", len(receipts))
        logger.info("[BankReceiptBundleAgent] valid_receipts_count=%s", len(valid))
        logger.info(
            "[BankReceiptBundleAgent] first_5_receipts=%s",
            json.dumps([
                {
                    "receipt_date": item.get("receipt_date"),
                    "payer_name": item.get("payer_name"),
                    "payee_name": item.get("payee_name"),
                    "amount": str(item.get("amount") or ""),
                    "receipt_no": item.get("receipt_no") or item.get("business_no"),
                    "confidence": item.get("confidence"),
                    "reject_reason": item.get("reject_reason"),
                }
                for item in receipts[:5]
            ], ensure_ascii=False),
        )
        if not valid:
            logger.warning("[BankReceiptBundleAgent] first_3000_raw_text=%s", source_text[:3000])
            logger.warning("[BankReceiptBundleAgent] first_3_receipt_blocks=%s", json.dumps([{"page": block.get("page"), "text": str(block.get("text") or "")[:1000]} for block in blocks[:3]], ensure_ascii=False))
            logger.warning("[BankReceiptBundleAgent] reject_reasons=%s", Counter(str(item.get("reject_reason") or "字段不足") for item in receipts))
        result = {
            "doc_type": DOC_TYPE,
            "doc_type_name": DOC_TYPE_NAME,
            "agent_type": "bank_receipt_bundle_agent",
            "source_file": source_file,
            "original_status": "可查看",
            "extract_status": "成功" if valid else "部分成功",
            "extraction_status": "成功" if valid else "部分成功",
            "bank_name": bank_name or "未识别",
            "receipt_count": len(valid),
            "valid_receipt_count": len(valid),
            "candidate_receipts_count": len(receipts),
            "receipt_blocks_count": len(blocks),
            "recognizable_amount_total": total if receipts else None,
            "amount_recognition_status": "完整识别" if valid and all(item.get("amount") is not None for item in valid) else ("部分识别" if any(item.get("amount") is not None for item in valid) else "未识别"),
            "parse_quality_status": "success" if valid else "partial",
            "can_join_bank_statement_aggregate": False,
            "receipts": receipts if valid else [],
            "payee_summary": _aggregate(valid, "payee_name"),
            "payer_summary": _aggregate(valid, "payer_name"),
            "manual_review_items": [],
            "evidence": [{"type": "receipt_block", "page": item.get("source_page"), "confidence": item.get("confidence")} for item in receipts],
        }
        if not valid:
            result["manual_review_items"].append("未形成可稳定识别的回单明细，建议人工复核原件或补充 OCR。")
        markdown = render_bank_receipt_bundle_markdown(result)
        confidence = min(0.98, 0.45 + 0.12 * min(len(valid), 3) + (0.08 if bank_name else 0))
        return ExtractionResult(
            document_type=DOC_TYPE,
            schema_version="bank_receipt_bundle.agent.v1",
            extracted_json=result,
            markdown_summary=markdown,
            confidence=confidence,
            warnings=list(result["manual_review_items"]),
            skill_name=self.skill_name,
            skill_version=self.skill_version,
        )
