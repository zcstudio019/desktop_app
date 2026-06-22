"""Deterministic extraction skill for official bank statement PDFs."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult


HEADERS = (
    "凭证号", "对方账号", "交易时间", "借贷标志", "对方单位", "对方行号",
    "用途", "摘要", "备注", "金额", "交易金额", "发生额", "借方发生额", "贷方发生额", "收入", "支出", "余额", "回单个性化信息",
)
DATE_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})[年/.-]?(\d{1,2})[月/.-]?(\d{1,2})(?:日)?(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?")
PERIOD_RE = re.compile(r"(?<!\d)((?:19|20)\d{6})\s*(?:-|—|~|至|到)\s*((?:19|20)\d{6})(?!\d)")
AMOUNT_LABEL_RE = re.compile(
    r"(?P<label>实收金额|应收金额|交易金额|发生额|借方发生额|贷方发生额|收入|支出|贷款金额|归还金额|还款金额|本金|利息|金额)\s*[:：]?\s*"
    r"(?P<amount>[+-]?(?:人民币|￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?)"
)

CATEGORY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("贷款发放", ("贷款发放", "对公贷款记账", "贷款账号", "借据编号")),
    ("贷款归还", ("贷款归还", "对公贷款批量正常分期", "还款", "贷款帐号")),
    ("利息支出", ("对公贷款利息支付", "利息支出", "利率", "息余积数")),
    ("银行手续费", ("跨行汇款手续费", "对公跨行汇款手续费", "对公跨行快汇手续费", "企业网银证书年费", "USBKey证书工本费", "财智账户卡年费", "到账伴侣协议费", "企业网银账户半年费")),
    ("经营收入", ("货款", "工程款", "项目款", "材料款", "劳务费", "电缆款", "灯具款", "风管材料款", "防火包裹材料款")),
    ("往来款", ("往来款", "转账", "普通汇兑", "汇兑业务")),
    ("资金拆借", ("借款",)),
)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" |\t\r\n")


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


def _periods(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], str, str]:
    evidence: list[dict[str, Any]] = []
    values: list[tuple[str, str]] = []
    pages = raw_pages or [{"page": 1, "text": text}]
    for item in pages:
        page_text = str(item.get("text") or "")
        found = PERIOD_RE.findall(page_text)
        for start, end in found:
            start_fmt, end_fmt = _date(start), _date(end)
            values.append((start_fmt, end_fmt))
            evidence.append({
                "field": "时间范围", "page": int(item.get("page") or 0),
                "raw_value": f"{start} - {end}", "value": f"{start_fmt} 至 {end_fmt}",
            })
    if not values:
        dates = [_date("".join(groups[:3])) for groups in DATE_RE.findall(text)]
        dates = [item[:10] for item in dates if item]
        return evidence, (min(dates) if dates else ""), (max(dates) if dates else "")
    return evidence, min(item[0] for item in values), max(item[1] for item in values)


def _header_mapping(cells: list[str]) -> dict[int, str]:
    mapping: dict[int, str] = {}
    aliases = {"本方借贷标志": "借贷标志", "借/贷": "借贷标志", "对方户名": "对方单位", "附言": "备注"}
    for index, cell in enumerate(cells):
        compact = _clean(cell).replace(" ", "")
        for alias, canonical in aliases.items():
            if alias in compact:
                mapping[index] = canonical
                break
        else:
            for header in HEADERS:
                if header in compact:
                    mapping[index] = header
                    break
    return mapping


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
        (("实收金额", "应收金额") if category == "银行手续费" else
         ("交易金额", "发生额", "贷款金额", "归还金额", "还款金额", "本金", "实收金额", "应收金额", "利息", "金额"))
    )
    for wanted in priorities:
        found = next(((label, amount) for label, amount in matches if label == wanted), None)
        if found:
            tx["金额"], tx["金额来源"] = found[1], f"回单个性化信息.{found[0]}"
            return


def classify_transaction(tx: dict[str, Any]) -> str:
    text = " ".join(str(tx.get(key) or "") for key in ("用途", "摘要", "备注", "回单个性化信息"))
    if "利息" in text and tx.get("借贷标志") == "贷" and "利息支付" not in text:
        return "利息收入"
    for category, keywords in CATEGORY_RULES:
        if any(keyword.lower() in text.lower() for keyword in keywords):
            return category
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


def _transactions_from_pages(raw_pages: list[dict[str, Any]], text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    transactions: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    explicit_amount_column = False
    pages = raw_pages or [{"page": 1, "text": text, "table_rows": []}]
    continuation_markers = ("附言", "指令编号", "支付交易序号", "报文种类", "提交人", "产品名称", "费用名称", "应收金额", "实收金额", "起息日期", "止息日期", "利率", "利息", "贷款账号", "贷款帐号", "借据编号")
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
        # Text-only fallback: keep transactions conservative; a date and 借/贷 are mandatory.
        if len(transactions) == page_before:
            for line in lines:
                date_value = _date(line)
                side_match = re.search(r"(?:^|\s)(借|贷)(?:\s|$)", line)
                if date_value and side_match:
                    tx = _new_tx(page)
                    tx["交易时间"] = date_value
                    tx["借贷标志"] = side_match.group(1)
                    tx["收支方向"] = "入账" if side_match.group(1) == "贷" else "出账"
                    tx["摘要"] = _clean(line)
                    tx["交易分类"] = classify_transaction(tx)
                    _extract_amount(tx)
                    transactions.append(tx)
                elif transactions and any(marker in line for marker in continuation_markers):
                    _append_info(transactions[-1], line)
                    transactions[-1]["交易分类"] = classify_transaction(transactions[-1])
                    _extract_amount(transactions[-1])
    # Deduplicate coordinate/table representations of the same row.
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for tx in transactions:
        key = (tx.get("凭证号"), tx.get("对方账号"), tx.get("交易时间"), tx.get("借贷标志"), tx.get("对方单位"), tx.get("摘要"), tx.get("来源页码"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(tx)
    unique.sort(key=lambda item: (str(item.get("交易时间") or ""), int(item.get("来源页码") or 0)))
    for index, tx in enumerate(unique, start=1):
        tx["序号"] = index
        evidence.append({"field": "交易明细", "page": tx["来源页码"], "record": index, "locator": f"凭证号={tx.get('凭证号') or '未识别'};交易时间={tx.get('交易时间')}"})
    return unique, evidence, explicit_amount_column


def _summary(result: dict[str, Any]) -> None:
    txs = result["transactions"]
    recognized = [tx for tx in txs if tx.get("金额") is not None]
    result["amount_recognition_status"] = "完整识别" if txs and len(recognized) == len(txs) else ("部分识别" if recognized else "未识别")
    result["transaction_count"] = len(txs)
    result["inflow_count"] = sum(tx.get("收支方向") == "入账" for tx in txs)
    result["outflow_count"] = sum(tx.get("收支方向") == "出账" for tx in txs)
    result["recognizable_inflow"] = sum((tx["金额"] for tx in recognized if tx.get("收支方向") == "入账"), Decimal("0"))
    result["recognizable_outflow"] = sum((tx["金额"] for tx in recognized if tx.get("收支方向") == "出账"), Decimal("0"))
    monthly: dict[str, dict[str, Any]] = {}
    if result.get("period_start") and result.get("period_end"):
        cursor = datetime.strptime(result["period_start"][:7] + "-01", "%Y-%m-%d")
        end = datetime.strptime(result["period_end"][:7] + "-01", "%Y-%m-%d")
        while cursor <= end:
            monthly[cursor.strftime("%Y-%m")] = {"month": cursor.strftime("%Y-%m"), "inflow_count": 0, "outflow_count": 0, "recognizable_inflow": Decimal("0"), "recognizable_outflow": Decimal("0"), "categories": Counter()}
            cursor = datetime(cursor.year + (cursor.month == 12), 1 if cursor.month == 12 else cursor.month + 1, 1)
    for tx in txs:
        month = str(tx.get("交易时间") or "")[:7]
        item = monthly.setdefault(month, {"month": month, "inflow_count": 0, "outflow_count": 0, "recognizable_inflow": Decimal("0"), "recognizable_outflow": Decimal("0"), "categories": Counter()})
        direction = tx.get("收支方向")
        if direction == "入账": item["inflow_count"] += 1
        if direction == "出账": item["outflow_count"] += 1
        if tx.get("金额") is not None and direction in {"入账", "出账"}:
            item["recognizable_inflow" if direction == "入账" else "recognizable_outflow"] += tx["金额"]
        item["categories"][tx.get("交易分类") or "其他"] += 1
    result["monthly_summary"] = [{**item, "main_description": "、".join(name for name, _ in item.pop("categories").most_common(3))} for _, item in sorted(monthly.items()) if item["month"]]
    categories = {name: {"category": name, "count": 0, "recognizable_amount": Decimal("0")} for name in ("经营收入", "往来款", "贷款发放", "贷款归还", "利息支出", "利息收入", "银行手续费", "资金拆借", "其他")}
    counterparties: dict[tuple[str, str], dict[str, Any]] = {}
    for tx in txs:
        category = tx.get("交易分类") or "其他"
        categories[category]["count"] += 1
        if tx.get("金额") is not None:
            categories[category]["recognizable_amount"] += tx["金额"]
        name = _clean(tx.get("对方单位"))
        if name:
            key = (name, tx.get("收支方向") or "未识别")
            item = counterparties.setdefault(key, {"counterparty": name, "direction": key[1], "count": 0, "recognizable_amount": Decimal("0"), "descriptions": Counter()})
            item["count"] += 1
            if tx.get("金额") is not None:
                item["recognizable_amount"] += tx["金额"]
            desc = _clean(tx.get("摘要") or tx.get("用途"))
            if desc:
                item["descriptions"][desc] += 1
    result["category_summary"] = list(categories.values())
    ranked = sorted(counterparties.values(), key=lambda item: (-item["count"], item["counterparty"]))
    result["counterparty_summary"] = [{**item, "main_description": "、".join(name for name, _ in item.pop("descriptions").most_common(3))} for item in ranked]
    result["loan_related_transactions"] = [tx for tx in txs if tx.get("交易分类") in {"贷款发放", "贷款归还", "利息支出"} or any(key in " ".join(str(tx.get(field) or "") for field in ("摘要", "备注", "回单个性化信息")) for key in ("借据编号", "贷款账号", "贷款帐号"))]
    result["fee_interest_transactions"] = [tx for tx in txs if tx.get("交易分类") in {"银行手续费", "利息支出", "利息收入"}]


def _cell(value: Any) -> str:
    return _clean(value).replace("|", "\\|") or "—"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def render_bank_statement_markdown(result: dict[str, Any]) -> str:
    lines = [
        "## 银行对账单", "",
        "- 资料类型：银行对账单", f"- 来源文件：{_cell(result.get('source_file'))}",
        "- 原件状态：可查看", f"- 提取状态：{result.get('extraction_status', '成功')}", "",
        "### 基本信息",
        f"- 银行名称：{_cell(result.get('bank_name'))}", f"- 对账单标题：{_cell(result.get('statement_title'))}",
        f"- 本方账号：{_cell(result.get('account_no'))}", f"- 本方账户户名：{_cell(result.get('account_name'))}",
        f"- 本方开户行：{_cell(result.get('opening_bank'))}", f"- 币种：{_cell(result.get('currency'))}",
        f"- 单位：{_cell(result.get('unit'))}", f"- 时间范围：{_cell(result.get('period_start'))} 至 {_cell(result.get('period_end'))}",
        f"- 页数：{result.get('page_count', 0)}页", f"- 交易记录数：{result.get('transaction_count', 0)}",
        f"- 金额识别状态：{result.get('amount_recognition_status')}", "", "### 账户流水概览",
        f"- 入账笔数：{result.get('inflow_count', 0)}", f"- 出账笔数：{result.get('outflow_count', 0)}",
    ]
    if result.get("amount_recognition_status") == "完整识别":
        inflow, outflow = result["recognizable_inflow"], result["recognizable_outflow"]
        lines += [f"- 总入账金额：{_money(inflow)}", f"- 总出账金额：{_money(outflow)}", f"- 净流入：{_money(inflow - outflow)}"]
    else:
        lines.append("- 金额识别说明：本对账单主表未稳定识别到完整交易金额列，仅从明确金额字段或回单个性化信息中提取部分金额，因此不生成完整收入、支出和净流入统计。")
    inflow_categories = [item["category"] for item in result["category_summary"] if item["count"] and item["category"] in {"经营收入", "往来款", "贷款发放", "利息收入", "资金拆借", "其他"}]
    outflow_categories = [item["category"] for item in result["category_summary"] if item["count"] and item["category"] in {"往来款", "贷款归还", "利息支出", "银行手续费", "资金拆借", "其他"}]
    lines += [f"- 主要入账类型：{'、'.join(inflow_categories) or '未识别'}", f"- 主要出账类型：{'、'.join(outflow_categories) or '未识别'}", "", "### 月度汇总", "| 月份 | 入账笔数 | 出账笔数 | 可识别入账金额 | 可识别出账金额 | 主要交易说明 |", "|---|---:|---:|---:|---:|---|"]
    for item in result["monthly_summary"]:
        lines.append(f"| {_cell(item['month'])} | {item['inflow_count']} | {item['outflow_count']} | {_money(item['recognizable_inflow'])} | {_money(item['recognizable_outflow'])} | {_cell(item['main_description'])} |")
    lines += ["", "### 交易分类汇总", "| 分类 | 笔数 | 可识别金额 | 说明 |", "|---|---:|---:|---|"]
    for item in result["category_summary"]:
        lines.append(f"| {item['category']} | {item['count']} | {_money(item['recognizable_amount'])} | 仅汇总明确识别金额 |")
    lines += ["", "### 主要交易对手", "| 排名 | 对方单位 | 交易方向 | 笔数 | 可识别金额 | 主要摘要/用途 |", "|---:|---|---|---:|---:|---|"]
    for index, item in enumerate(result["counterparty_summary"], start=1):
        lines.append(f"| {index} | {_cell(item['counterparty'])} | {_cell(item['direction'])} | {item['count']} | {_money(item['recognizable_amount'])} | {_cell(item['main_description'])} |")
    lines += ["", "### 交易明细", "| 序号 | 交易时间 | 借贷标志 | 收支方向 | 对方账号 | 对方单位 | 用途 | 摘要 | 备注 | 金额 | 分类 |", "|---:|---|---|---|---|---|---|---|---|---:|---|"]
    for tx in result["transactions"]:
        lines.append("| " + " | ".join((_cell(tx.get("序号")), _cell(tx.get("交易时间")), _cell(tx.get("借贷标志")), _cell(tx.get("收支方向")), _cell(tx.get("对方账号")), _cell(tx.get("对方单位")), _cell(tx.get("用途")), _cell(tx.get("摘要")), _cell(tx.get("备注")), _money(tx.get("金额")), _cell(tx.get("交易分类")))) + " |")
    lines += ["", "### 贷款及融资相关交易", "| 交易时间 | 收支方向 | 摘要 | 备注 | 回单个性化信息 | 金额 |", "|---|---|---|---|---|---:|"]
    for tx in result["loan_related_transactions"]:
        lines.append(f"| {_cell(tx.get('交易时间'))} | {_cell(tx.get('收支方向'))} | {_cell(tx.get('摘要'))} | {_cell(tx.get('备注'))} | {_cell(tx.get('回单个性化信息'))} | {_money(tx.get('金额'))} |")
    lines += ["", "### 银行费用及利息", "| 交易时间 | 类型 | 摘要 | 实收金额 | 回单个性化信息 |", "|---|---|---|---:|---|"]
    for tx in result["fee_interest_transactions"]:
        lines.append(f"| {_cell(tx.get('交易时间'))} | {_cell(tx.get('交易分类'))} | {_cell(tx.get('摘要'))} | {_money(tx.get('金额'))} | {_cell(tx.get('回单个性化信息'))} |")
    lines += ["", "### 风险提示"] + [f"- {item}" for item in result["risk_tips"]]
    if result["manual_review_items"]:
        lines += ["", "### 需人工复核"] + [f"- {item}" for item in result["manual_review_items"]]
    return "\n".join(lines).replace("None", "").replace("null", "").replace("undefined", "")


class BankStatementSkill(BaseExtractionSkill):
    document_type = "bank_statement"
    supported_extensions = {".pdf", ".xlsx"}
    skill_name = "bank_statement_skill"
    skill_version = "v1"

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        metadata = input_data.metadata or {}
        raw_pages = metadata.get("raw_pages") if isinstance(metadata.get("raw_pages"), list) else []
        text = str(input_data.raw_text or "")
        page_text = "\n".join(str(item.get("text") or "") for item in raw_pages)
        source = f"{text}\n{page_text}"
        period_evidence, period_start, period_end = _periods(raw_pages, source)
        transactions, tx_evidence, explicit_amount_column = _transactions_from_pages(raw_pages, source)
        title = "中国工商银行账户明细清单" if "中国工商银行账户明细清单" in source else _find_labeled(source, ("对账单标题", "标题"))
        bank_name = "中国工商银行" if ("中国工商银行" in source or "工商银行" in input_data.file_name) else _find_labeled(source, ("银行名称",))
        account_no = _find_labeled(source, ("本方账号", "账号"), ("本方账号户名", "本方账户户名", "户名", "币种", "本方账号开户行"))
        account_no_match = re.search(r"(?<!\d)\d{12,30}(?!\d)", account_no)
        account_no = account_no_match.group(0) if account_no_match else ""
        result: dict[str, Any] = {
            "doc_type": "bank_statement", "doc_type_name": "银行对账单", "agent_type": "bank_statement_agent",
            "source_file": input_data.file_name or (Path(input_data.file_path).name if input_data.file_path else ""),
            "original_status": "可查看", "extraction_status": "成功", "bank_name": bank_name,
            "statement_title": title or ("账户明细清单" if "账户明细清单" in source else "银行对账单"),
            "account_no": account_no,
            "account_name": _find_labeled(source, ("本方账号户名", "本方账户户名", "本方户名", "账户户名"), ("币种", "本方账号开户行", "开户行", "单位", "记账时间范围")),
            "opening_bank": _find_labeled(source, ("本方账号开户行", "本方开户行", "开户行"), ("记账时间范围", "时间范围", "币种", "单位")),
            "currency": _find_labeled(source, ("币种",), ("单位", "本方账号开户行", "记账时间范围")) or ("人民币" if "人民币" in source else ""),
            "unit": _find_labeled(source, ("单位",), ("本方账号开户行", "本方开户行", "开户行", "币种", "记账时间范围", "交易时间")) or ("元" if re.search(r"单位\s*[:：]?\s*元", source) else ""),
            "period_start": period_start, "period_end": period_end,
            "page_count": len({int(item.get("page") or index) for index, item in enumerate(raw_pages, 1)}) or int(metadata.get("page_count") or 0),
            "transactions": transactions, "evidence": period_evidence + tx_evidence,
        }
        _summary(result)
        result["risk_tips"] = []
        if any(tx["交易分类"] == "贷款发放" for tx in transactions): result["risk_tips"].append("存在银行贷款发放记录。")
        if any(tx["交易分类"] == "贷款归还" for tx in transactions): result["risk_tips"].append("存在银行贷款归还记录。")
        if sum(tx["交易分类"] == "往来款" for tx in transactions) > 1: result["risk_tips"].append("存在多笔往来款，建议结合交易对手和合同核验资金性质。")
        if result["amount_recognition_status"] != "完整识别": result["risk_tips"].append("本文件金额字段识别不完整，暂不建议直接用于完整流水测算。")
        if result["fee_interest_transactions"]: result["risk_tips"].append("存在银行手续费或贷款利息收支记录。")
        if any(tx["交易分类"] == "资金拆借" for tx in transactions): result["risk_tips"].append("存在资金拆借相关交易，建议进一步核验借款主体和用途。")
        if not result["risk_tips"]: result["risk_tips"].append("未从已识别内容中发现需要特别提示的事项。")
        result["manual_review_items"] = []
        if result["amount_recognition_status"] != "完整识别": result["manual_review_items"].append("金额列缺失或金额识别不完整。")
        if transactions and sum(not tx.get("对方单位") for tx in transactions) / len(transactions) >= 0.3: result["manual_review_items"].append("对方单位为空较多。")
        if len(period_evidence) > 1: result["manual_review_items"].append("文件包含多个时间区间，已按同一账户合并，建议核对区间连续性。")
        if any(tx["交易分类"] in {"贷款发放", "贷款归还", "资金拆借", "往来款"} for tx in transactions): result["manual_review_items"].append("存在贷款、借款或往来款交易，建议人工核验资金性质。")
        result["amount_column_detected"] = explicit_amount_column
        markdown = render_bank_statement_markdown(result)
        warnings = list(result["manual_review_items"])
        confidence = min(0.98, 0.45 + (0.1 if account_no else 0) + (0.1 if period_start else 0) + (0.2 if transactions else 0) + (0.05 if result["page_count"] else 0))
        return ExtractionResult(document_type="bank_statement", schema_version="bank_statement.agent.v1", extracted_json=_json_safe(result), markdown_summary=markdown, confidence=confidence, warnings=warnings, skill_name=self.skill_name, skill_version=self.skill_version)
