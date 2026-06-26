from __future__ import annotations

import csv
import hashlib
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from .base import BaseExtractionSkill, ExtractionInput, ExtractionResult

logger = logging.getLogger(__name__)

DOC_TYPE = "bank_reconciliation_detail"
DOC_TYPE_NAME = "银行对账明细"
AGENT_TYPE = "bank_reconciliation_detail_agent"
SKILL_NAME = "bank_reconciliation_detail_skill"
SCHEMA_VERSION = "bank_reconciliation_detail.agent.v1"

PLACEHOLDER_VALUES = {"17"}
UNKNOWN = "未识别"

OPERATING_IN_KEYWORDS = ("工程款", "项目款", "货款", "材料款", "服务费", "劳务款", "咨询费", "结算款", "回款", "合同款", "进度款", "施工款", "设计费", "监理费")
OPERATING_OUT_KEYWORDS = ("材料款", "工程款", "项目款", "货款", "劳务费", "服务费", "采购款", "设备款", "租赁费", "安装费", "施工费", "分包款", "电缆款", "风管材料款", "灯具款")
NON_OPERATING_KEYWORDS = ("借款", "还款", "退款", "保证金", "押金", "备用金", "代垫款", "内部款")
LOAN_KEYWORDS = ("融资租赁", "普惠融资", "贷款", "放款", "还款", "贷款利息", "利息支付", "对公贷款记账", "租赁", "担保", "小企业贷款", "小企业其他短期贷款利息收入")
FEE_KEYWORDS = ("手续费", "短信业务服务费", "工本费", "年费", "证书收入", "证书工本费", "账户服务费", "普通卡年费", "跨行汇款手续费", "到账伴侣")
SALARY_KEYWORDS = ("工资", "代发", "薪酬", "社保", "公积金")
TAX_KEYWORDS = ("税", "税款", "税务", "国库", "社保", "公积金")
DEPOSIT_KEYWORDS = ("押金", "保证金", "退回")
INTEREST_KEYWORDS = ("利息", "结息")
UNKNOWN_COUNTERPARTY_NAMES = {"", "未识别", "未知", "空", "none", "null", "None", "NULL"}
ORGANIZATION_KEYWORDS = ("公司", "银行", "账户", "中心", "集团", "有限", "合作社", "商行", "个体工商户", "厂", "店", "部", "局", "院", "所")
NOISE_COUNTERPARTY_KEYWORDS = (
    "代发专用账户",
    "贷款利息收入",
    "小企业其他短期贷款利息收入",
    "对公工行证书收入",
    "对公客户证书工本费收入",
    "网银注册账户服务费",
    "财智账户卡普通卡年费",
    "跨行汇款手续费",
    "手续费",
    "利息",
    "到账伴侣",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return "" if text in PLACEHOLDER_VALUES else text


def _compact(value: Any) -> str:
    return re.sub(r"\s+", "", _text(value))


def _normalize_party_name(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    text = (
        text.replace("\u3000", "")
        .replace("\ufeff", "")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("（", "(")
        .replace("）", ")")
    )
    return re.sub(r"[\s\r\n\t]+", "", text)


SELF_PARTY_KEYS = {
    "customer_name",
    "customerName",
    "company_name",
    "companyName",
    "name",
    "account_name",
    "accountName",
    "account_holder",
    "accountHolder",
    "客户名称",
    "企业名称",
    "公司名称",
    "户名",
}
CUSTOMER_CONTAINER_KEYS = {"customer", "customer_info", "customerInfo", "customer_profile", "customerProfile", "客户档案", "客户信息"}
LEGAL_REP_KEYS = {"legal_representative", "legalRepresentative", "legal_representative_name", "legalRepresentativeName", "legal_person", "legalPerson", "legal_person_name", "legalPersonName", "法人", "法人姓名", "法定代表人", "法定代表人姓名"}
SHAREHOLDER_KEYS = {"shareholder", "shareholder_name", "shareholderName", "shareholder_names", "shareholderNames", "shareholders", "股东", "股东姓名", "股东名称", "股东信息", "股东列表"}
RELATED_PARTY_KEYS = {
    "actual_controller",
    "actualController",
    "actual_controller_name",
    "actualControllerName",
    "controller",
    "controller_name",
    "controllerName",
    "beneficial_owner",
    "beneficialOwner",
    "beneficial_owner_name",
    "beneficialOwnerName",
    "ultimate_beneficial_owner",
    "ultimateBeneficialOwner",
    "ultimate_beneficial_owner_name",
    "ultimateBeneficialOwnerName",
    "spouse",
    "related_party",
    "relatedParty",
    "related_parties",
    "relatedParties",
    "related_party_names",
    "relatedPartyNames",
    "实控人",
    "实际控制人",
    "受益人",
    "最终受益人",
    "配偶",
    "关联方",
    "关联方名称",
    "关联方信息",
}
PARTY_NAME_KEYS = {"name", "company_name", "companyName", "person_name", "personName", "shareholder_name", "shareholderName", "姓名", "名称", "企业名称", "公司名称", "股东名称"}


def _add_excluded_party(excluded_parties: dict[str, str], value: Any, reason: str) -> None:
    name = _normalize_party_name(value)
    if name:
        excluded_parties.setdefault(name, reason)


def _collect_party_names(value: Any, reason: str, excluded_parties: dict[str, str]) -> None:
    if value is None:
        return
    if isinstance(value, (str, int, float, Decimal)):
        _add_excluded_party(excluded_parties, value, reason)
        return
    if isinstance(value, dict):
        matched = False
        for key, item in value.items():
            key_text = str(key)
            if key_text in PARTY_NAME_KEYS:
                _add_excluded_party(excluded_parties, item, reason)
                matched = True
            elif isinstance(item, (dict, list, tuple, set)):
                _collect_party_names(item, reason, excluded_parties)
        if not matched and len(value) == 1:
            _collect_party_names(next(iter(value.values())), reason, excluded_parties)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_party_names(item, reason, excluded_parties)


def _collect_excluded_parties_from_source(source: Any, excluded_parties: dict[str, str]) -> None:
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        key_text = str(key)
        if key_text in SELF_PARTY_KEYS:
            _collect_party_names(value, "本方同名划转", excluded_parties)
        elif key_text in CUSTOMER_CONTAINER_KEYS:
            _collect_party_names(value, "本方同名划转", excluded_parties)
        elif key_text in LEGAL_REP_KEYS:
            _collect_party_names(value, "法定代表人往来", excluded_parties)
        elif key_text in SHAREHOLDER_KEYS:
            _collect_party_names(value, "股东往来", excluded_parties)
        elif key_text in RELATED_PARTY_KEYS:
            _collect_party_names(value, "关联方往来", excluded_parties)
        elif isinstance(value, dict):
            _collect_excluded_parties_from_source(value, excluded_parties)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                _collect_excluded_parties_from_source(item, excluded_parties)


def _is_placeholder(value: Any) -> bool:
    return str(value).strip() in PLACEHOLDER_VALUES


def _money(value: Any) -> Decimal | None:
    if value is None or _is_placeholder(value):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = _text(value).replace(",", "").replace("￥", "").replace("¥", "").strip()
    if not text or text in {"-", "--"}:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def _int(value: Any) -> int | None:
    amount = _money(value)
    return int(amount) if amount is not None else None


def _fmt_money(value: Any) -> str:
    amount = _money(value)
    return f"{amount:,.2f}" if amount is not None else "0.00"


def _fmt_count(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _column_letter(index: int) -> str:
    if index <= 0:
        return ""
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _display(value: Any, default: str = UNKNOWN) -> str:
    text = _text(value)
    if not text or text in {"null", "None", "undefined", "{}", "[]"}:
        return default
    return text.replace("|", "｜")


def _date_text(value: Any, with_time: bool = False) -> str:
    if value is None or _is_placeholder(value):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S" if with_time else "%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = _text(value)
    if not text:
        return ""
    match = re.search(r"((?:19|20)\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:日)?(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?", text)
    if match:
        base = f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        time_part = match.group(4) or ""
        if with_time and time_part:
            if len(time_part.split(":")) == 2:
                time_part += ":00"
            return f"{base} {time_part}"
        return base
    match = re.search(r"\b((?:19|20)\d{2})(\d{2})(\d{2})\b", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return ""


def _date_range(value: Any) -> tuple[str, str]:
    text = _text(value)
    dates = re.findall(r"(?:19|20)\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}", text)
    if len(dates) >= 2:
        return _date_text(dates[0]), _date_text(dates[1])
    compact = re.findall(r"\b(?:19|20)\d{6}\b", text)
    if len(compact) >= 2:
        return _date_text(compact[0]), _date_text(compact[1])
    return "", ""


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _is_unknown_counterparty(value: Any) -> bool:
    text = _display(value, "")
    normalized = _normalize_party_name(text)
    normalized_unknowns = {_normalize_party_name(item) for item in UNKNOWN_COUNTERPARTY_NAMES}
    return not normalized or text in UNKNOWN_COUNTERPARTY_NAMES or normalized in normalized_unknowns


def _is_personal_name(value: Any) -> bool:
    text = _normalize_party_name(value)
    if not re.fullmatch(r"[\u4e00-\u9fff]{2,4}", text):
        return False
    return not any(keyword in text for keyword in ORGANIZATION_KEYWORDS)


def _is_noise_counterparty(value: Any, joined: str = "") -> bool:
    text = _display(value, "")
    if _is_unknown_counterparty(text):
        return True
    combined = f"{text} {joined}"
    return _contains_any(combined, NOISE_COUNTERPARTY_KEYWORDS)


def _is_organization_counterparty(value: Any) -> bool:
    text = _display(value, "")
    if _is_unknown_counterparty(text) or _is_personal_name(text):
        return False
    return bool(text) and not _is_noise_counterparty(text)


def _is_top_eligible_operating_tx(tx: dict[str, Any], direction: str) -> bool:
    if tx.get("direction") != direction:
        return False
    if direction == "in" and not tx.get("is_operating_inflow"):
        return False
    if direction == "out" and not tx.get("is_operating_outflow"):
        return False
    blocked_flags = (
        "is_excluded_related_party",
        "is_self_transfer",
        "is_related_party_transfer",
        "is_loan_related",
        "is_fee",
        "is_interest",
        "is_noise",
        "is_personal_counterparty",
    )
    if any(tx.get(flag) for flag in blocked_flags):
        return False
    if direction == "out" and any(tx.get(flag) for flag in ("is_salary", "is_tax")):
        return False
    return not _is_unknown_counterparty(tx.get("counterparty_name"))


def _bank_from_filename(filename: str) -> str:
    if "工商银行" in filename or "工行" in filename:
        return "工商银行"
    if "上海银行" in filename:
        return "上海银行"
    return ""


@dataclass
class AccountInfo:
    bank_name: str = ""
    account_name: str = ""
    account_no: str = ""
    branch_name: str = ""
    currency: str = "人民币"
    date_start: str = ""
    date_end: str = ""
    source_file: str = ""
    sheet_name: str = ""
    account_confidence: str = "medium"
    parse_status: str = "success"
    parse_warnings: list[str] = field(default_factory=list)


@dataclass
class FileParseResult:
    source_file: str
    sheet_name: str
    bank_name: str
    header_row_no: int
    header_col_start: int
    account: AccountInfo
    transactions: list[dict[str, Any]]
    raw_summary: dict[str, Any]
    placeholder_cleaned_count: int = 0
    status: str = "成功"
    warnings: list[str] = field(default_factory=list)


def _read_workbook(file_path: str, filename: str) -> list[tuple[str, list[list[Any]]]]:
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        rows: list[list[Any]] = []
        raw = path.read_bytes()
        text = ""
        for encoding in ("utf-8-sig", "gb18030", "gbk"):
            try:
                text = raw.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if not text:
            text = raw.decode("utf-8", errors="ignore")
        for row in csv.reader(text.splitlines()):
            rows.append(row)
        return [(Path(filename).stem or "CSV", rows)]
    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        last_error: Exception | None = None
        for read_only in (False, True):
            try:
                wb = load_workbook(path, read_only=read_only, data_only=True)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "[BankReconciliationDetail] workbook_open_failed filename=%s path=%s read_only=%s error=%s",
                    filename,
                    file_path,
                    read_only,
                    exc,
                )
                continue
            try:
                logger.info(
                    "[BankReconciliationDetail] workbook_opened filename=%s path=%s read_only=%s active_sheet=%s sheets=%s",
                    filename,
                    file_path,
                    read_only,
                    wb.active.title if wb.active else "",
                    wb.sheetnames,
                )
                sheets: list[tuple[str, list[list[Any]]]] = []
                for ws in wb.worksheets:
                    try:
                        dimension = ws.calculate_dimension()
                    except Exception as exc:
                        dimension = f"calculate_dimension_failed:{exc}"
                    logger.info(
                        "[BankReconciliationDetail] sheet_dimension file=%s sheet=%s read_only=%s max_row=%s max_col=%s dimension=%s",
                        file_path,
                        ws.title,
                        read_only,
                        getattr(ws, "max_row", ""),
                        getattr(ws, "max_column", ""),
                        dimension,
                    )
                    if read_only and dimension == "A1:A1" and hasattr(ws, "reset_dimensions"):
                        logger.warning(
                            "[BankReconciliationDetail] worksheet dimension is A1:A1, reset_dimensions file=%s sheet=%s",
                            file_path,
                            ws.title,
                        )
                        ws.reset_dimensions()
                        try:
                            dimension = ws.calculate_dimension(force=True)
                        except Exception as exc:
                            dimension = f"force_calculate_dimension_failed:{exc}"
                        logger.info(
                            "[BankReconciliationDetail] sheet_dimension_after_reset file=%s sheet=%s max_row=%s max_col=%s dimension=%s",
                            file_path,
                            ws.title,
                            getattr(ws, "max_row", ""),
                            getattr(ws, "max_column", ""),
                            dimension,
                        )
                    rows = [[cell for cell in row] for row in ws.iter_rows(values_only=True)]
                    sheets.append((ws.title, rows))
                return sheets
            finally:
                wb.close()
        raise RuntimeError(f"workbook 读取失败：{last_error}") if last_error else RuntimeError("workbook 读取失败")
    if suffix == ".xls":
        try:
            import pandas as pd  # type: ignore

            sheets = pd.read_excel(path, sheet_name=None, header=None, dtype=object)
            return [(str(name), frame.where(frame.notna(), None).values.tolist()) for name, frame in sheets.items()]
        except Exception as exc:
            raise RuntimeError(f"暂不支持读取该 .xls 文件，请转换为 xlsx 后重试：{exc}") from exc
    raise RuntimeError(f"不支持的文件格式：{suffix or '未知'}")


def _row_text(row: list[Any]) -> str:
    return " ".join(_text(cell) for cell in row if _text(cell))


def _detect_bank_format(rows: list[list[Any]], bank_hint: str, mapping: dict[str, int]) -> tuple[str, str]:
    if bank_hint == "上海银行":
        return "shanghai", "filename"
    if bank_hint == "工商银行":
        return "icbc", "filename"
    scanned_cells = [_text(cell) for row in rows[:30] for cell in row[:30] if _text(cell)]
    joined = " ".join(scanned_cells)
    header_fields = set(mapping)
    if "账户明细查询" in joined or "选择账号" in joined:
        return "shanghai", "sheet_marker"
    if "开户行" in joined and "上海银行" in joined:
        return "shanghai", "branch_marker"
    if {"transaction_id", "trade_time", "direction", "amount", "counterparty_name"} <= header_fields:
        return "shanghai", "header_fields"
    if {"voucher_no", "in_amount", "out_amount"} & header_fields:
        return "icbc", "header_fields"
    return ("shanghai", "amount_header") if "amount" in header_fields else ("generic", "unknown")


def _find_header(rows: list[list[Any]], bank_hint: str) -> tuple[int, dict[str, int], str, int]:
    aliases = {
        "transaction_id": ("交易流水号",),
        "trade_time": ("交易时间",),
        "accounting_date": ("记账日期",),
        "direction": ("交易方向", "借贷标志"),
        "amount": ("交易金额",),
        "balance": ("余额",),
        "counterparty_account": ("对手账号", "对方账号"),
        "counterparty_name": ("对手名称", "对方单位"),
        "counterparty_bank_no": ("对方行号",),
        "summary": ("摘要",),
        "purpose": ("交易用途", "用途"),
        "remark": ("备注", "附言"),
        "voucher_no": ("凭证号",),
        "in_amount": ("转入金额",),
        "out_amount": ("转出金额",),
    }
    best_idx = -1
    best_map: dict[str, int] = {}
    best_score = 0
    shanghai_required = {
        "交易流水号",
        "交易时间",
        "记账日期",
        "交易方向",
        "交易金额",
        "余额",
        "对手账号",
        "对手名称",
        "摘要",
        "交易用途",
        "备注",
    }
    for idx, row in enumerate(rows[:50]):
        mapping: dict[str, int] = {}
        shanghai_hits = 0
        for col, cell in enumerate(row[:50]):
            header = _compact(cell)
            if not header:
                continue
            if any(_compact(name) == header or _compact(name) in header for name in shanghai_required):
                shanghai_hits += 1
            for field, names in aliases.items():
                if field in mapping:
                    continue
                if any(_compact(name) == header or _compact(name) in header for name in names):
                    mapping[field] = col
        fields = set(mapping)
        score = len(fields) + (3 if "trade_time" in fields or "accounting_date" in fields else 0) + (3 if {"in_amount", "out_amount"} & fields or "amount" in fields else 0)
        if shanghai_hits >= 5:
            score += 20
        if score > best_score:
            best_idx, best_map, best_score = idx, mapping, score
    if best_score < 5:
        return -1, {}, "generic", 0
    bank_format, _ = _detect_bank_format(rows, bank_hint, best_map)
    header_col_start = min(best_map.values()) + 1 if best_map else 0
    return best_idx, best_map, bank_format, header_col_start


def _value_after_label(row: list[Any], label: str) -> str:
    compact_label = _compact(label).rstrip(":：")
    for idx, cell in enumerate(row):
        text = _text(cell)
        compact = _compact(text)
        if not compact:
            continue
        if compact.startswith(compact_label):
            for sep in (":", "："):
                if sep in text:
                    right = text.split(sep, 1)[1].strip()
                    if right:
                        return _text(right)
            if idx + 1 < len(row):
                return _text(row[idx + 1])
    return ""


def _find_label_index(row: list[Any], label: str) -> int:
    compact_label = _compact(label).rstrip(":：")
    for idx, cell in enumerate(row):
        compact = _compact(cell).rstrip(":：")
        if compact.startswith(compact_label):
            return idx
    return -1


def _next_value_after_index(row: list[Any], start_idx: int, stop_labels: tuple[str, ...] = ()) -> str:
    stop_compacts = tuple(_compact(label).rstrip(":：") for label in stop_labels)
    for idx in range(start_idx + 1, len(row)):
        value = _text(row[idx])
        compact = _compact(value).rstrip(":：")
        if not compact:
            continue
        if any(compact.startswith(label) for label in stop_compacts):
            return ""
        return value
    return ""


def _parse_meta(rows: list[list[Any]], sheet_name: str, source_file: str, bank_name: str) -> tuple[AccountInfo, dict[str, Any]]:
    account = AccountInfo(bank_name=bank_name or _bank_from_filename(source_file), source_file=source_file, sheet_name=sheet_name)
    summary: dict[str, Any] = {}
    for row in rows[:10]:
        row_joined = _row_text(row)
        if "记账日期" in row_joined:
            start, end = _date_range(row_joined)
            account.date_start = account.date_start or start
            account.date_end = account.date_end or end
        selected = _value_after_label(row, "选择账号")
        if selected:
            account.account_no = re.sub(r"\D", "", selected) or account.account_no
            selected_idx = _find_label_index(row, "选择账号")
            if selected_idx >= 0 and not account.account_name:
                possible_name = _next_value_after_index(row, selected_idx + 1, ("开户行", "币种"))
                if possible_name and not re.fullmatch(r"\d+", possible_name):
                    account.account_name = possible_name
        for label, field_name in (
            ("户名", "account_name"),
            ("开户行", "branch_name"),
            ("币种", "currency"),
        ):
            value = _value_after_label(row, label)
            if value:
                setattr(account, field_name, value)
        for label, key in (
            ("总笔数", "raw_transaction_count"),
            ("借方总笔数", "debit_count"),
            ("借方总金额", "debit_amount"),
            ("贷方总笔数", "credit_count"),
            ("贷方总金额", "credit_amount"),
        ):
            value = _value_after_label(row, label)
            if value:
                summary[key] = _money(value) if "金额" in label else _int(value)
    if not account.bank_name:
        account.bank_name = _bank_from_filename(source_file) or UNKNOWN
    if not account.currency:
        account.currency = "人民币"
    if not account.account_name and bank_name == "工商银行":
        account.account_confidence = "missing"
    return account, summary


def _row_get(row: list[Any], mapping: dict[str, int], key: str) -> Any:
    idx = mapping.get(key)
    return row[idx] if idx is not None and idx < len(row) else None


def _classify(tx: dict[str, Any], excluded_parties: dict[str, str]) -> None:
    joined = " ".join(_text(tx.get(key)) for key in ("counterparty_name", "summary", "purpose", "remark"))
    own_name = _normalize_party_name(tx.get("account_name"))
    counterparty = _normalize_party_name(tx.get("counterparty_name"))
    excluded_reason = excluded_parties.get(counterparty, "") if counterparty else ""
    if counterparty and own_name and counterparty == own_name:
        excluded_reason = "本方同名划转"
    tx["is_excluded_related_party"] = bool(excluded_reason)
    tx["excluded_reason"] = excluded_reason
    tx["related_party_type"] = excluded_reason.replace("往来", "") if excluded_reason and excluded_reason != "本方同名划转" else ("本方同名账户" if excluded_reason else "")
    tx["is_self_transfer"] = excluded_reason == "本方同名划转"
    tx["is_related_party_transfer"] = bool(excluded_reason)
    tx["is_noise"] = _is_noise_counterparty(tx.get("counterparty_name"), joined)
    tx["is_personal_counterparty"] = _is_personal_name(tx.get("counterparty_name"))
    tx["is_loan_related"] = _contains_any(joined, LOAN_KEYWORDS)
    tx["is_fee"] = _contains_any(joined, FEE_KEYWORDS)
    tx["is_salary"] = _contains_any(joined, SALARY_KEYWORDS)
    tx["is_tax"] = _contains_any(joined, TAX_KEYWORDS)
    tx["is_interest"] = _contains_any(joined, INTEREST_KEYWORDS)
    is_deposit = _contains_any(joined, DEPOSIT_KEYWORDS)
    has_operating_in = _contains_any(joined, OPERATING_IN_KEYWORDS)
    has_operating_out = _contains_any(joined, OPERATING_OUT_KEYWORDS)
    has_non_operating = _contains_any(joined, NON_OPERATING_KEYWORDS)
    is_org = _is_organization_counterparty(tx.get("counterparty_name"))
    if _is_unknown_counterparty(tx.get("counterparty_name")):
        tx["category"] = "未识别对手方"
        tx["is_noise"] = True
    elif tx["is_excluded_related_party"]:
        tx["category"] = "内部/关联方往来"
        tx["is_noise"] = False
    elif tx["is_personal_counterparty"]:
        tx["category"] = "个人往来"
    elif tx["is_loan_related"]:
        tx["category"] = "融资/贷款相关"
    elif tx["is_tax"]:
        tx["category"] = "税费/社保"
    elif tx["is_salary"]:
        tx["category"] = "人工成本/代发"
    elif tx["is_fee"] or tx["is_interest"]:
        tx["category"] = "手续费/利息"
    elif is_deposit:
        tx["category"] = "押金/保证金/退款"
    elif tx["is_noise"]:
        tx["category"] = "未识别对手方"
    elif tx.get("direction") == "in" and is_org and has_operating_in and not has_non_operating:
        tx["category"] = "经营性入账"
    elif tx.get("direction") == "out" and is_org and has_operating_out and not has_non_operating:
        tx["category"] = "经营性出账"
    else:
        tx["category"] = "非经营性往来"
    tx["is_operating_inflow"] = tx["category"] == "经营性入账"
    tx["is_operating_outflow"] = tx["category"] == "经营性出账"
    tx["confidence"] = "high" if tx["category"] in {"经营性入账", "经营性出账", "内部/关联方往来", "贷款相关"} else "medium"


def _parse_sheet(rows: list[list[Any]], sheet_name: str, source_file: str) -> FileParseResult | None:
    bank_hint = _bank_from_filename(source_file)
    max_cols = max((len(row) for row in rows), default=0)
    header_idx, mapping, bank_format, header_col_start = _find_header(rows, bank_hint)
    bank_format, bank_reason = _detect_bank_format(rows, bank_hint, mapping)
    field_map_for_log = {key: value + 1 for key, value in mapping.items()}
    logger.info(
        "[BankReconciliationDetail] scan file=%s sheet=%s range=%sx%s bank_hint=%s bank_format=%s bank_reason=%s header_row_no=%s header_col_start=%s header_col_start_letter=%s field_map=%s",
        source_file,
        sheet_name,
        len(rows),
        max_cols,
        bank_hint or UNKNOWN,
        bank_format,
        bank_reason,
        header_idx + 1 if header_idx >= 0 else 0,
        header_col_start,
        _column_letter(header_col_start),
        field_map_for_log,
    )
    if header_idx < 0 or not mapping:
        logger.warning(
            "[BankReconciliationDetail] parse_skip file=%s sheet=%s reason=header_not_found range=%sx%s",
            source_file,
            sheet_name,
            len(rows),
            max_cols,
        )
        return None
    bank_name = bank_hint or ("上海银行" if bank_format == "shanghai" else "工商银行" if bank_format == "icbc" else UNKNOWN)
    account, raw_summary = _parse_meta(rows, sheet_name, source_file, bank_name)
    placeholder_cleaned = 0
    transactions: list[dict[str, Any]] = []
    excluded_parties: dict[str, str] = {}
    _add_excluded_party(excluded_parties, account.account_name, "本方同名划转")
    empty_streak = 0
    for row_no, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2):
        if not any(_text(cell) for cell in row):
            empty_streak += 1
            if empty_streak >= 8:
                break
            continue
        empty_streak = 0
        placeholder_cleaned += sum(1 for cell in row if _is_placeholder(cell))
        trade_time = _date_text(_row_get(row, mapping, "trade_time"), with_time=True)
        accounting_date = _date_text(_row_get(row, mapping, "accounting_date"))
        if not trade_time and accounting_date:
            trade_time = accounting_date
        direction_raw = _text(_row_get(row, mapping, "direction"))
        in_amount = _money(_row_get(row, mapping, "in_amount"))
        out_amount = _money(_row_get(row, mapping, "out_amount"))
        amount = _money(_row_get(row, mapping, "amount"))
        direction = ""
        if direction_raw in {"贷", "贷方"} or "入账" in direction_raw or "贷方" in direction_raw:
            direction = "in"
        elif direction_raw in {"借", "借方"} or "出账" in direction_raw or "借方" in direction_raw:
            direction = "out"
        elif in_amount is not None:
            direction = "in"
        elif out_amount is not None:
            direction = "out"
        final_amount = amount if amount is not None else (in_amount if direction == "in" else out_amount)
        if final_amount is None and not trade_time and not accounting_date:
            continue
        tx = {
            "transaction_id": _text(_row_get(row, mapping, "transaction_id")) or _text(_row_get(row, mapping, "voucher_no")),
            "source_file": source_file,
            "bank_name": bank_name,
            "account_name": account.account_name,
            "account_no": account.account_no,
            "trade_time": trade_time,
            "accounting_date": accounting_date or (trade_time[:10] if trade_time else ""),
            "direction": direction,
            "direction_name": "入账" if direction == "in" else "出账" if direction == "out" else UNKNOWN,
            "amount": final_amount,
            "balance": _money(_row_get(row, mapping, "balance")),
            "counterparty_name": _text(_row_get(row, mapping, "counterparty_name")),
            "counterparty_account": _text(_row_get(row, mapping, "counterparty_account")),
            "counterparty_bank_no": _text(_row_get(row, mapping, "counterparty_bank_no")),
            "summary": _text(_row_get(row, mapping, "summary")),
            "purpose": _text(_row_get(row, mapping, "purpose")),
            "remark": _text(_row_get(row, mapping, "remark")),
            "voucher_no": _text(_row_get(row, mapping, "voucher_no")),
            "raw_row_no": row_no,
            "warning": "" if final_amount is not None else "金额未识别",
        }
        _classify(tx, excluded_parties)
        transactions.append(tx)
    dates = sorted(tx["accounting_date"] for tx in transactions if tx.get("accounting_date"))
    if dates:
        account.date_start = account.date_start or dates[0]
        account.date_end = account.date_end or dates[-1]
    if not raw_summary.get("raw_transaction_count"):
        raw_summary["raw_transaction_count"] = len(transactions)
    logger.info(
        "[BankReconciliationDetail] parsed file=%s sheet=%s bank=%s header_row_no=%s header_col_start=%s header_col_start_letter=%s read_rows=%s parsed_transaction_count=%s amount_status=%s",
        source_file,
        sheet_name,
        bank_name,
        header_idx + 1,
        header_col_start,
        _column_letter(header_col_start),
        max(0, len(rows) - header_idx - 1),
        len(transactions),
        "完整" if transactions and all(tx.get("amount") is not None for tx in transactions) else "部分缺失" if transactions else "未识别",
    )
    return FileParseResult(
        source_file=source_file,
        sheet_name=sheet_name,
        bank_name=bank_name,
        header_row_no=header_idx + 1,
        header_col_start=header_col_start,
        account=account,
        transactions=transactions,
        raw_summary=raw_summary,
        placeholder_cleaned_count=placeholder_cleaned,
        warnings=["已清理占位值 17"] if placeholder_cleaned else [],
    )


def _dedupe(transactions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for tx in transactions:
        signature = "|".join(
            _text(tx.get(key))
            for key in (
                "bank_name",
                "account_no",
                "trade_time",
                "accounting_date",
                "direction",
                "counterparty_account",
                "counterparty_name",
                "summary",
                "purpose",
                "remark",
            )
        )
        signature += f"|{_fmt_money(tx.get('amount'))}|{_fmt_money(tx.get('balance'))}"
        digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        unique.append(tx)
    return unique, len(transactions) - len(unique)


def _sum(transactions: list[dict[str, Any]], predicate: Any) -> Decimal:
    total = Decimal("0")
    for tx in transactions:
        if predicate(tx):
            amount = _money(tx.get("amount"))
            if amount is not None:
                total += amount
    return total


def _period(transactions: list[dict[str, Any]], accounts: list[dict[str, Any]]) -> tuple[str, str]:
    dates = [tx.get("accounting_date") for tx in transactions if tx.get("accounting_date")]
    dates.extend(account.get("date_start") for account in accounts if account.get("date_start"))
    dates.extend(account.get("date_end") for account in accounts if account.get("date_end"))
    dates = sorted(str(item) for item in dates if item)
    return (dates[0], dates[-1]) if dates else ("", "")


def _account_key(account: dict[str, Any]) -> str:
    return "|".join([str(account.get("bank_name") or ""), str(account.get("account_no") or ""), str(account.get("source_file") or ""), str(account.get("sheet_name") or "")])


def _aggregate(file_results: list[FileParseResult], excluded_parties: dict[str, str] | None = None) -> dict[str, Any]:
    all_excluded_parties = dict(excluded_parties or {})
    for result in file_results:
        normalized_name = _normalize_party_name(result.account.account_name)
        if normalized_name:
            all_excluded_parties.setdefault(normalized_name, "本方同名划转")
    raw_transactions = [tx for result in file_results for tx in result.transactions]
    for tx in raw_transactions:
        _classify(tx, all_excluded_parties)
    transactions, duplicate_count = _dedupe(raw_transactions)
    account_map: dict[str, dict[str, Any]] = {}
    for result in file_results:
        account = result.account.__dict__.copy()
        key = _account_key(account)
        account_map.setdefault(key, {**account, "raw_count": 0, "deduped_count": 0})
        account_map[key]["raw_count"] += len(result.transactions)
    for tx in transactions:
        key = "|".join([str(tx.get("bank_name") or ""), str(tx.get("account_no") or ""), str(tx.get("source_file") or ""), str(next((r.sheet_name for r in file_results if r.source_file == tx.get("source_file")), ""))])
        if key in account_map:
            account_map[key]["deduped_count"] += 1
    accounts = list(account_map.values())
    start, end = _period(transactions, accounts)
    monthly: dict[str, dict[str, Any]] = defaultdict(lambda: {"in": Decimal("0"), "out": Decimal("0"), "op_in": Decimal("0"), "op_out": Decimal("0"), "count": 0, "op_count": 0})
    in_counter: dict[str, dict[str, Any]] = defaultdict(lambda: {"amount": Decimal("0"), "count": 0, "operating": 0})
    out_counter: dict[str, dict[str, Any]] = defaultdict(lambda: {"amount": Decimal("0"), "count": 0, "operating": 0})
    for tx in transactions:
        month = str(tx.get("accounting_date") or "")[:7] or UNKNOWN
        amount = _money(tx.get("amount")) or Decimal("0")
        monthly[month]["count"] += 1
        if tx.get("direction") == "in":
            monthly[month]["in"] += amount
            if tx.get("is_operating_inflow"):
                monthly[month]["op_in"] += amount
                monthly[month]["op_count"] += 1
            if _is_top_eligible_operating_tx(tx, "in"):
                item = in_counter[_display(tx.get("counterparty_name"))]
                item["amount"] += amount
                item["count"] += 1
                item["operating"] += 1
        elif tx.get("direction") == "out":
            monthly[month]["out"] += amount
            if tx.get("is_operating_outflow"):
                monthly[month]["op_out"] += amount
                monthly[month]["op_count"] += 1
            if _is_top_eligible_operating_tx(tx, "out"):
                item = out_counter[_display(tx.get("counterparty_name"))]
                item["amount"] += amount
                item["count"] += 1
                item["operating"] += 1
    return {
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "document_type": DOC_TYPE,
        "document_type_code": DOC_TYPE,
        "document_type_name": DOC_TYPE_NAME,
        "agent_type": AGENT_TYPE,
        "skill_name": SKILL_NAME,
        "schema_version": SCHEMA_VERSION,
        "extraction_status": "success" if transactions else "failed",
        "files": [
            {
                "source_file": r.source_file,
                "sheet_name": r.sheet_name,
                "bank_name": r.bank_name,
                "header_row_no": r.header_row_no,
                "header_col_start": r.header_col_start,
                "transaction_count": len(r.transactions),
                "amount_status": "完整" if all(tx.get("amount") is not None for tx in r.transactions) else "部分缺失",
                "date_start": r.account.date_start,
                "date_end": r.account.date_end,
                "status": r.status,
                "warnings": r.warnings,
                "placeholder_cleaned_count": r.placeholder_cleaned_count,
                "raw_summary": r.raw_summary,
            }
            for r in file_results
        ],
        "accounts": accounts,
        "summary": {
            "file_count": len({r.source_file for r in file_results}),
            "account_count": len(accounts),
            "date_start": start,
            "date_end": end,
            "raw_transaction_count": len(raw_transactions),
            "deduped_transaction_count": len(transactions),
            "duplicate_transaction_count": duplicate_count,
            "in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in"),
            "out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out"),
            "self_transfer_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and tx.get("is_self_transfer")),
            "self_transfer_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and tx.get("is_self_transfer")),
            "in_amount_excluding_self_transfer": _sum(transactions, lambda tx: tx.get("direction") == "in" and not tx.get("is_self_transfer")),
            "out_amount_excluding_self_transfer": _sum(transactions, lambda tx: tx.get("direction") == "out" and not tx.get("is_self_transfer")),
            "excluded_related_transaction_count": sum(1 for tx in transactions if tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer")),
            "excluded_related_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and (tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer"))),
            "excluded_related_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and (tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer"))),
            "in_amount_excluding_excluded_related": _sum(transactions, lambda tx: tx.get("direction") == "in" and not (tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer"))),
            "out_amount_excluding_excluded_related": _sum(transactions, lambda tx: tx.get("direction") == "out" and not (tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer"))),
            "personal_transaction_count": sum(1 for tx in transactions if tx.get("category") == "个人往来"),
            "personal_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and tx.get("category") == "个人往来"),
            "personal_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and tx.get("category") == "个人往来"),
            "loan_interest_transaction_count": sum(1 for tx in transactions if tx.get("category") == "融资/贷款相关" or (tx.get("category") == "手续费/利息" and tx.get("is_interest") and not tx.get("is_fee"))),
            "loan_interest_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and (tx.get("category") == "融资/贷款相关" or (tx.get("category") == "手续费/利息" and tx.get("is_interest") and not tx.get("is_fee")))),
            "loan_interest_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and (tx.get("category") == "融资/贷款相关" or (tx.get("category") == "手续费/利息" and tx.get("is_interest") and not tx.get("is_fee")))),
            "salary_tax_fee_transaction_count": sum(1 for tx in transactions if tx.get("category") in {"税费/社保", "人工成本/代发"} or (tx.get("category") == "手续费/利息" and tx.get("is_fee"))),
            "salary_tax_fee_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and (tx.get("category") in {"税费/社保", "人工成本/代发"} or (tx.get("category") == "手续费/利息" and tx.get("is_fee")))),
            "salary_tax_fee_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and (tx.get("category") in {"税费/社保", "人工成本/代发"} or (tx.get("category") == "手续费/利息" and tx.get("is_fee")))),
            "noise_transaction_count": sum(1 for tx in transactions if tx.get("category") == "未识别对手方"),
            "noise_in_amount": _sum(transactions, lambda tx: tx.get("direction") == "in" and tx.get("category") == "未识别对手方"),
            "noise_out_amount": _sum(transactions, lambda tx: tx.get("direction") == "out" and tx.get("category") == "未识别对手方"),
            "operating_in_amount": _sum(transactions, lambda tx: tx.get("is_operating_inflow")),
            "operating_out_amount": _sum(transactions, lambda tx: tx.get("is_operating_outflow")),
            "amount_completeness": "完整" if transactions and all(tx.get("amount") is not None for tx in transactions) else "部分缺失" if transactions else "未识别",
            "aggregation_status": "可用" if transactions else "不可用",
        },
        "monthly": dict(sorted(monthly.items())),
        "top_in": sorted(in_counter.items(), key=lambda item: item[1]["amount"], reverse=True)[:10],
        "top_out": sorted(out_counter.items(), key=lambda item: item[1]["amount"], reverse=True)[:10],
        "transactions": transactions,
        "warnings": list(dict.fromkeys(w for r in file_results for w in r.warnings)),
    }


def _render_markdown(data: dict[str, Any]) -> str:
    summary = data.get("summary") or {}
    transactions = data.get("transactions") or []
    lines: list[str] = [
        "## 银行对账明细",
        "",
        f"- 资料类型：{DOC_TYPE_NAME}",
        f"- 提取状态：{'成功' if summary.get('deduped_transaction_count') else '失败'}",
        f"- 覆盖文件数：{_fmt_count(summary.get('file_count'))} 份",
        f"- 已识别银行账户数：{_fmt_count(summary.get('account_count'))} 个",
        f"- 覆盖时间范围：{_display(summary.get('date_start'))} 至 {_display(summary.get('date_end'))}",
        f"- 原始交易笔数：{_fmt_count(summary.get('raw_transaction_count'))}",
        f"- 去重后交易笔数：{_fmt_count(summary.get('deduped_transaction_count'))}",
        f"- 重复交易笔数：{_fmt_count(summary.get('duplicate_transaction_count'))}",
        f"- 金额识别完整度：{_display(summary.get('amount_completeness'), '未识别')}",
        f"- 聚合状态：{_display(summary.get('aggregation_status'), '不可用')}",
        "",
        "### 文件解析质量清单",
        "",
        "| 序号 | 来源文件 | 银行 | 工作表 | 表头识别 | 明细笔数 | 金额识别 | 时间范围 | 状态 | 提示 |",
        "|---|---|---|---|---|---:|---|---|---|---|",
    ]
    for idx, item in enumerate(data.get("files") or [], start=1):
        tip = "；".join(item.get("warnings") or []) or "-"
        lines.append(f"| {idx} | {_display(item.get('source_file'))} | {_display(item.get('bank_name'))} | {_display(item.get('sheet_name'))} | {'已识别' if item.get('header_row_no') else '未识别'} | {_fmt_count(item.get('transaction_count'))} | {_display(item.get('amount_status'), '未识别')} | {_display(item.get('date_start'))} 至 {_display(item.get('date_end'))} | {_display(item.get('status'), '失败')} | {_display(tip, '-')} |")
    lines += ["", "### 银行账户汇总", "", "| 序号 | 银行 | 户名 | 账号 | 开户行 | 币种 | 时间范围 | 交易笔数 |", "|---|---|---|---|---|---|---|---:|"]
    for idx, account in enumerate(data.get("accounts") or [], start=1):
        lines.append(f"| {idx} | {_display(account.get('bank_name'))} | {_display(account.get('account_name'))} | {_display(account.get('account_no'))} | {_display(account.get('branch_name'))} | {_display(account.get('currency'), '人民币')} | {_display(account.get('date_start'))} 至 {_display(account.get('date_end'))} | {_fmt_count(account.get('deduped_count') or account.get('raw_count'))} |")
    net = (_money(summary.get("in_amount")) or Decimal("0")) - (_money(summary.get("out_amount")) or Decimal("0"))
    op_net = (_money(summary.get("operating_in_amount")) or Decimal("0")) - (_money(summary.get("operating_out_amount")) or Decimal("0"))
    lines += [
        "",
        "### 资金总览",
        "",
        "| 项目 | 金额/数量 |",
        "|---|---:|",
        f"| 原始入账金额 | {_fmt_money(summary.get('in_amount'))} |",
        f"| 原始出账金额 | {_fmt_money(summary.get('out_amount'))} |",
        f"| 资金净流入 | {_fmt_money(net)} |",
        f"| 本方同名划转入账 | {_fmt_money(summary.get('self_transfer_in_amount'))} |",
        f"| 本方同名划转出账 | {_fmt_money(summary.get('self_transfer_out_amount'))} |",
        f"| 有效经营入账 | {_fmt_money(summary.get('operating_in_amount'))} |",
        f"| 有效经营出账 | {_fmt_money(summary.get('operating_out_amount'))} |",
        f"| 经营净流入 | {_fmt_money(op_net)} |",
        "",
        "### 月度汇总",
        "",
        "| 月份 | 入账金额 | 出账金额 | 净流入 | 经营性入账 | 经营性出账 | 经营净流入 | 交易笔数 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for month, item in (data.get("monthly") or {}).items():
        net_month = item["in"] - item["out"]
        op_net_month = item["op_in"] - item["op_out"]
        lines.append(f"| {month} | {_fmt_money(item['in'])} | {_fmt_money(item['out'])} | {_fmt_money(net_month)} | {_fmt_money(item['op_in'])} | {_fmt_money(item['op_out'])} | {_fmt_money(op_net_month)} | {_fmt_count(item['count'])} |")
    for title, key, direction in (("主要入账对象 TOP10", "top_in", "入账"), ("主要出账对象 TOP10", "top_out", "出账")):
        lines += ["", f"### {title}", "", f"| 排名 | 对方户名 | {direction}金额 | {direction}笔数 | 是否经营性 | 备注 |", "|---|---|---:|---:|---|---|"]
        for idx, (name, item) in enumerate(data.get(key) or [], start=1):
            operating = "是" if item.get("operating") else "否"
            lines.append(f"| {idx} | {_display(name)} | {_fmt_money(item.get('amount'))} | {_fmt_count(item.get('count'))} | {operating} | - |")
        if not data.get(key):
            lines.append("| - | 无 | 0.00 | 0 | 否 | 无 |")
    self_transfers = [tx for tx in transactions if tx.get("is_self_transfer")]
    lines += ["", "### 本方同名划转识别", ""]
    if self_transfers:
        lines += ["| 序号 | 日期 | 银行 | 对方户名 | 方向 | 金额 | 摘要/用途 | 处理结果 |", "|---|---|---|---|---|---:|---|---|"]
        for idx, tx in enumerate(self_transfers[:20], start=1):
            result = "已剔除经营收入" if tx.get("direction") == "in" else "已剔除经营支出"
            lines.append(f"| {idx} | {_display(tx.get('accounting_date'))} | {_display(tx.get('bank_name'))} | {_display(tx.get('counterparty_name'))} | {_display(tx.get('direction_name'))} | {_fmt_money(tx.get('amount'))} | {_display(tx.get('summary') or tx.get('purpose'), '无')} | {result} |")
    else:
        lines.append("- 本方同名划转：未发现")
    placeholder_count = sum(int(item.get("placeholder_cleaned_count") or 0) for item in data.get("files") or [])
    amount_missing = any(tx.get("amount") is None for tx in transactions)
    date_missing = any(not tx.get("accounting_date") for tx in transactions)
    large_in = any((_money(tx.get("amount")) or Decimal("0")) >= Decimal("1000000") and tx.get("direction") == "in" for tx in transactions)
    lines += [
        "",
        "### 异常和提示",
        "",
        f"- 是否存在金额缺失：{'是' if amount_missing else '否'}",
        f"- 是否存在日期缺失：{'是' if date_missing else '否'}",
        f"- 是否存在占位值清理：{'是' if placeholder_count else '否'}",
        f"- 是否存在大额集中收款：{'是' if large_in else '否'}",
        f"- 是否存在本方同名大额往来：{'是' if self_transfers else '否'}",
        f"- 是否存在贷款/利息/手续费类交易：{'是' if any(tx.get('is_loan_related') or tx.get('is_interest') or tx.get('is_fee') for tx in transactions) else '否'}",
        "",
        "### 交易明细样例",
        "",
        "| 日期 | 银行 | 方向 | 金额 | 对方户名 | 摘要 | 用途 | 分类 |",
        "|---|---|---|---:|---|---|---|---|",
    ]
    for tx in transactions[:50]:
        lines.append(f"| {_display(tx.get('accounting_date'))} | {_display(tx.get('bank_name'))} | {_display(tx.get('direction_name'))} | {_fmt_money(tx.get('amount'))} | {_display(tx.get('counterparty_name'))} | {_display(tx.get('summary'), '无')} | {_display(tx.get('purpose'), '无')} | {_display(tx.get('category'), '未分类')} |")
    if not transactions:
        lines.append("| 无 | 无 | 无 | 0.00 | 无 | 无 | 无 | 无 |")
    markdown = "\n".join(lines)
    for forbidden in ("raw_result", "normalized_data", "fields:", "transactions:", "null", "undefined", "None", "{}", "[]", "| 17 |", "> 17 <"):
        markdown = markdown.replace(forbidden, "")
    return markdown


def _render_compact_display_markdown(data: dict[str, Any]) -> str:
    summary = data.get("summary") or {}
    files = data.get("files") or []
    accounts = data.get("accounts") or []
    primary_account = accounts[0] if accounts else {}
    transactions = data.get("transactions") or []
    file_count = summary.get("file_count") or len(files)
    source_file = _display(files[0].get("source_file")) if len(files) == 1 else f"{_fmt_count(file_count)} 份文件"
    banks = sorted({_display(item.get("bank_name"), "") for item in files if _display(item.get("bank_name"), "")})
    bank_name = banks[0] if len(banks) == 1 else ("多家银行" if banks else UNKNOWN)
    status = "成功" if summary.get("deduped_transaction_count") else "失败"
    in_amount = _money(summary.get("in_amount")) or Decimal("0")
    out_amount = _money(summary.get("out_amount")) or Decimal("0")
    excluded_related_in = _money(summary.get("excluded_related_in_amount")) or _money(summary.get("self_transfer_in_amount")) or Decimal("0")
    excluded_related_out = _money(summary.get("excluded_related_out_amount")) or _money(summary.get("self_transfer_out_amount")) or Decimal("0")
    personal_in = _money(summary.get("personal_in_amount")) or Decimal("0")
    personal_out = _money(summary.get("personal_out_amount")) or Decimal("0")
    loan_interest_in = _money(summary.get("loan_interest_in_amount")) or Decimal("0")
    loan_interest_out = _money(summary.get("loan_interest_out_amount")) or Decimal("0")
    salary_tax_fee_in = _money(summary.get("salary_tax_fee_in_amount")) or Decimal("0")
    salary_tax_fee_out = _money(summary.get("salary_tax_fee_out_amount")) or Decimal("0")
    noise_in = _money(summary.get("noise_in_amount")) or Decimal("0")
    noise_out = _money(summary.get("noise_out_amount")) or Decimal("0")
    in_excluding_related = in_amount - excluded_related_in - personal_in - loan_interest_in - noise_in
    out_excluding_related = out_amount - excluded_related_out - personal_out - loan_interest_out - salary_tax_fee_out - noise_out
    excluded_related_count = int(summary.get("excluded_related_transaction_count") or 0)
    personal_count = int(summary.get("personal_transaction_count") or 0)
    loan_interest_count = int(summary.get("loan_interest_transaction_count") or 0)
    salary_tax_fee_count = int(summary.get("salary_tax_fee_transaction_count") or 0)
    noise_count = int(summary.get("noise_transaction_count") or 0)
    operating_in = _money(summary.get("operating_in_amount")) or Decimal("0")
    operating_out = _money(summary.get("operating_out_amount")) or Decimal("0")
    net = in_amount - out_amount
    op_net = operating_in - operating_out
    base_amount = max(in_amount, out_amount, Decimal("1"))
    excluded_related_rows = [tx for tx in transactions if tx.get("is_excluded_related_party") or tx.get("is_self_transfer") or tx.get("is_related_party_transfer")]
    loan_like = [tx for tx in transactions if tx.get("is_loan_related") or tx.get("is_interest")]
    fee_like = [tx for tx in transactions if tx.get("is_fee")]
    personal_like = [tx for tx in transactions if tx.get("is_personal_counterparty") and not (tx.get("is_excluded_related_party") or tx.get("is_related_party_transfer"))]
    noise_like = [tx for tx in transactions if tx.get("is_noise")]
    operating_out_rows = [tx for tx in transactions if tx.get("is_operating_outflow")]

    lines: list[str] = [
        "## 银行对账明细",
        "",
        f"- 资料类型：{DOC_TYPE_NAME}",
        f"- 来源文件：{source_file}",
        f"- 提取状态：{status}",
        f"- 银行名称：{bank_name}",
        f"- 户名：{_display(primary_account.get('account_name'))}",
        f"- 账号：{_display(primary_account.get('account_no'))}",
        f"- 开户行：{_display(primary_account.get('branch_name'))}",
        f"- 覆盖时间：{_display(summary.get('date_start'))} 至 {_display(summary.get('date_end'))}",
        f"- 交易笔数：{_fmt_count(summary.get('deduped_transaction_count'))} 笔",
        f"- 金额识别：{_display(summary.get('amount_completeness'), UNKNOWN)}",
        "",
        "### 核心资金概览",
        "",
        "| 项目 | 金额/数量 |",
        "|---|---:|",
        f"| 原始入账总额 | {_fmt_money(in_amount)} |",
        f"| 原始出账总额 | {_fmt_money(out_amount)} |",
        f"| 已剔除内部/关联方入账 | {_fmt_money(excluded_related_in)} |",
        f"| 已剔除内部/关联方出账 | {_fmt_money(excluded_related_out)} |",
        f"| 已剔除个人往来入账 | {_fmt_money(personal_in)} |",
        f"| 已剔除个人往来出账 | {_fmt_money(personal_out)} |",
        f"| 已剔除贷款/融资/利息类入账 | {_fmt_money(loan_interest_in)} |",
        f"| 已剔除贷款/融资/利息类出账 | {_fmt_money(loan_interest_out)} |",
        f"| 已剔除工资/代发/税费/手续费类入账 | {_fmt_money(salary_tax_fee_in)} |",
        f"| 已剔除工资/代发/税费/手续费类出账 | {_fmt_money(salary_tax_fee_out)} |",
        f"| 已剔除未识别及噪音入账 | {_fmt_money(noise_in)} |",
        f"| 已剔除未识别及噪音出账 | {_fmt_money(noise_out)} |",
        f"| 剔除后入账 | {_fmt_money(in_excluding_related)} |",
        f"| 剔除后出账 | {_fmt_money(out_excluding_related)} |",
        f"| 有效经营入账 | {_fmt_money(operating_in)} |",
        f"| 有效经营出账 | {_fmt_money(operating_out)} |",
        f"| 经营净流入 | {_fmt_money(op_net)} |",
        "",
        "### 经营判断",
        "",
    ]
    if abs(net) <= base_amount * Decimal("0.02"):
        lines.append("- 整体资金净流入较小，入账和出账基本持平。")
    elif net > 0:
        lines.append("- 整体资金呈净流入，需要结合交易对手和用途判断回款质量。")
    else:
        lines.append("- 整体资金呈净流出，需关注持续支出对现金流的压力。")
    if op_net < 0:
        lines.append("- 有效经营入账低于有效经营出账，经营现金流为负。")
    elif op_net > 0:
        lines.append("- 有效经营入账高于有效经营出账，经营现金流为正。")
    else:
        lines.append("- 有效经营入账与经营出账基本持平。")
    if loan_like:
        lines.append("- 入账或出账中存在贷款、利息相关交易，不能全部视为经营收入。")
    if operating_out_rows:
        lines.append("- 出账中存在材料款、劳务费、项目款等经营性支出。")
    if excluded_related_rows:
        lines.append("- 已识别内部/关联方往来，相关入账和出账已从经营性现金流中剔除。")
        lines.append("- 内部或关联方之间的资金往来只能反映内部资金调拨，不能作为销售回款或经营采购支出。")
        lines.append("- 剔除内部/关联方往来后，再判断企业真实经营回款和经营支出。")
    if not loan_like and not operating_out_rows and not excluded_related_rows:
        lines.append("- 当前报告已剔除明显内部往来、贷款、手续费等非经营性交易后计算经营性资金表现。")

    if excluded_related_rows or personal_like or loan_like or fee_like or noise_like:
        lines += [
            "",
            "### 非经营性及噪音剔除说明",
            "",
            f"- 已剔除内部/关联方往来：{_fmt_count(excluded_related_count or len(excluded_related_rows))} 笔，金额 {_fmt_money(excluded_related_in + excluded_related_out)}。",
            f"- 已剔除个人往来：{_fmt_count(personal_count or len(personal_like))} 笔，金额 {_fmt_money(personal_in + personal_out)}。",
            f"- 已剔除贷款/融资/利息类：{_fmt_count(loan_interest_count or len(loan_like))} 笔，金额 {_fmt_money(loan_interest_in + loan_interest_out)}。",
            f"- 已剔除工资/代发/税费/手续费类：{_fmt_count(salary_tax_fee_count)} 笔，金额 {_fmt_money(salary_tax_fee_in + salary_tax_fee_out)}。",
            f"- 已剔除未识别对手方及噪音账户：{_fmt_count(noise_count or len(noise_like))} 笔，金额 {_fmt_money(noise_in + noise_out)}。",
            "- 上述交易不纳入有效经营入账、有效经营出账和经营净流入，默认不展示具体对手方名称。",
        ]

    lines += [
        "",
        "### 月度经营资金变化",
        "",
        "| 月份 | 有效经营入账 | 有效经营出账 | 经营净流入 | 经营交易笔数 |",
        "|---|---:|---:|---:|---:|",
    ]
    for month, item in (data.get("monthly") or {}).items():
        net_month = item["op_in"] - item["op_out"]
        lines.append(f"| {month} | {_fmt_money(item['op_in'])} | {_fmt_money(item['op_out'])} | {_fmt_money(net_month)} | {_fmt_count(item.get('op_count'))} |")
    if not data.get("monthly"):
        lines.append("| 未识别 | 0.00 | 0.00 | 0.00 | 0 |")

    def judgment(name: str, item: dict[str, Any], direction: str) -> str:
        return "经营性入账" if direction == "in" else "经营性出账"

    lines += [
        "",
        "### 主要经营入账来源",
        "",
        "| 排名 | 对方户名 | 入账金额 | 笔数 | 判断 |",
        "|---|---|---:|---:|---|",
    ]
    for idx, (name, item) in enumerate(data.get("top_in") or [], start=1):
        lines.append(f"| {idx} | {_display(name)} | {_fmt_money(item.get('amount'))} | {_fmt_count(item.get('count'))} | {judgment(name, item, 'in')} |")
    if not data.get("top_in"):
        lines.append("| - | 无 | 0.00 | 0 | 无 |")

    lines += [
        "",
        "### 主要经营出账对象",
        "",
        "| 排名 | 对方户名 | 出账金额 | 笔数 | 判断 |",
        "|---|---|---:|---:|---|",
    ]
    for idx, (name, item) in enumerate(data.get("top_out") or [], start=1):
        lines.append(f"| {idx} | {_display(name)} | {_fmt_money(item.get('amount'))} | {_fmt_count(item.get('count'))} | {judgment(name, item, 'out')} |")
    if not data.get("top_out"):
        lines.append("| - | 无 | 0.00 | 0 | 无 |")

    lines += ["", "### 风险提示", ""]
    risks: list[str] = []
    if abs(net) <= base_amount * Decimal("0.02"):
        risks.append("入账和出账金额高度接近，真实经营沉淀资金较少。")
    if op_net < 0:
        risks.append("经营性现金流为负，需要结合发票、合同、应收账款进一步判断回款质量。")
    if loan_like or fee_like:
        risks.append("存在贷款、利息、手续费等非经营性交易，不能直接作为销售回款。")
    if excluded_related_rows:
        risks.append("已识别内部/关联方往来，需要在融资分析中单独剔除，不作为经营回款或经营采购支出。")
    if not risks:
        risks.append("未识别到明显集中风险，仍建议结合合同、发票和回款周期复核。")
    lines.extend(f"- {risk}" for risk in risks)
    markdown = "\n".join(lines)
    for forbidden in ("raw_result", "normalized_data", "fields:", "transactions:", "null", "undefined", "None", "{}", "[]", "| 17 |", "> 17 <"):
        markdown = markdown.replace(forbidden, "")
    return markdown


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _failure_result(reason: str, suggestion: str = "请检查上传文件是否成功保存，或检查调度层是否将文件路径传入银行对账明细 Agent。") -> dict[str, Any]:
    markdown = "\n".join(
        [
            "## 银行对账明细",
            "",
            f"- 资料类型：{DOC_TYPE_NAME}",
            "- 提取状态：失败",
            f"- 失败原因：{reason}",
            f"- 处理建议：{suggestion}",
        ]
    )
    return {
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "document_type": DOC_TYPE,
        "document_type_code": DOC_TYPE,
        "document_type_name": DOC_TYPE_NAME,
        "agent_type": AGENT_TYPE,
        "skill_name": SKILL_NAME,
        "schema_version": SCHEMA_VERSION,
        "extraction_status": "failed",
        "failure_reason": reason,
        "warnings": [reason],
        "files": [],
        "accounts": [],
        "summary": {
            "file_count": 0,
            "account_count": 0,
            "raw_transaction_count": 0,
            "deduped_transaction_count": 0,
            "duplicate_transaction_count": 0,
            "amount_completeness": "未识别",
            "aggregation_status": "不可用",
        },
        "monthly": {},
        "top_in": [],
        "top_out": [],
        "transactions": [],
        "display_markdown": markdown,
        "markdown": markdown,
        "markdown_summary": markdown,
        "report_markdown": markdown,
    }


def _normalize_input_files(files: Any) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    if not isinstance(files, list):
        return normalized
    for item in files:
        if not isinstance(item, dict):
            continue
        file_path = str(item.get("file_path") or item.get("path") or item.get("filePath") or "").strip()
        file_name = str(item.get("file_name") or item.get("filename") or item.get("fileName") or Path(file_path).name).strip()
        if file_path or file_name:
            normalized.append({"file_path": file_path, "file_name": file_name})
    return normalized


def parse_bank_reconciliation_files(files: list[dict[str, str]], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    file_results: list[FileParseResult] = []
    warnings: list[str] = []
    excluded_parties: dict[str, str] = {}
    meta = metadata or {}
    _collect_excluded_parties_from_source(meta, excluded_parties)
    files = _normalize_input_files(files)
    logger.info("[BankReconciliationDetail] received_files=%s", len(files or []))
    if not files:
        logger.error("[BankReconciliationDetail] failed reason=no_files_received metadata_keys=%s", sorted(meta.keys()))
        return _failure_result("未收到可解析的银行对账明细文件")
    for item in files:
        file_path = item.get("file_path") or ""
        filename = item.get("file_name") or Path(file_path).name
        path_obj = Path(file_path)
        logger.info(
            "[BankReconciliationDetail] file_start filename=%s file_path=%s exists=%s suffix=%s",
            filename,
            file_path,
            path_obj.exists() if file_path else False,
            path_obj.suffix.lower() if file_path else "",
        )
        if not file_path:
            reason = f"{filename or '上传文件'} 未提供文件路径"
            logger.error("[BankReconciliationDetail] failed reason=file_path_missing filename=%s", filename)
            warnings.append(reason)
            continue
        if not path_obj.exists():
            reason = f"xlsx 文件路径不存在：{file_path}"
            logger.error("[BankReconciliationDetail] failed reason=file_path_not_exists filename=%s path=%s", filename, file_path)
            warnings.append(reason)
            continue
        _collect_excluded_parties_from_source(item, excluded_parties)
        try:
            sheets = _read_workbook(file_path, filename)
            logger.info(
                "[BankReconciliationDetail] workbook file=%s path=%s sheets=%s",
                filename,
                file_path,
                [(sheet_name, len(rows), max((len(row) for row in rows), default=0)) for sheet_name, rows in sheets],
            )
            matched_file = False
            for sheet_name, rows in sheets:
                parsed = _parse_sheet(rows, sheet_name, filename)
                if parsed:
                    matched_file = True
                    file_results.append(parsed)
                    logger.info(
                        "[BankReconciliationDetail] file=%s sheet=%s bank=%s header_row=%s header_col_start=%s detail_rows=%s amount_status=%s placeholders=%s",
                        filename,
                        sheet_name,
                        parsed.bank_name,
                        parsed.header_row_no,
                        parsed.header_col_start,
                        len(parsed.transactions),
                        "完整" if all(tx.get("amount") is not None for tx in parsed.transactions) else "部分缺失",
                        parsed.placeholder_cleaned_count,
                    )
            if not matched_file:
                logger.warning("[BankReconciliationDetail] file_no_parsed_sheet file=%s path=%s", filename, file_path)
                warnings.append(f"{filename} 表头扫描失败，已扫描工作表前 50 行 50 列，未找到银行对账明细交易表头")
        except Exception as exc:
            logger.exception("[BankReconciliationDetail] parse_failed file=%s", filename)
            warnings.append(f"{filename} 解析失败：{exc}")
    if not file_results:
        reason = warnings[0] if warnings else "未找到交易表头或未读取到有效交易明细"
        logger.error("[BankReconciliationDetail] failed parsed_files=0 reason=%s warnings=%s", reason, warnings)
        data = _failure_result(reason, "请检查文件是否为上海银行/工商银行对账明细，或查看日志中的 sheet、表头定位和文件路径信息。")
        data["warnings"] = list(dict.fromkeys([*data.get("warnings", []), *warnings]))
        return _json_safe(data)

    data = _aggregate(file_results, excluded_parties=excluded_parties)
    data["warnings"] = list(dict.fromkeys([*data.get("warnings", []), *warnings]))
    data["display_markdown"] = _render_compact_display_markdown(data)
    data["markdown"] = data["display_markdown"]
    data["markdown_summary"] = data["display_markdown"]
    logger.info(
        "[BankReconciliationDetail] aggregate raw=%s deduped=%s duplicate=%s",
        (data.get("summary") or {}).get("raw_transaction_count"),
        (data.get("summary") or {}).get("deduped_transaction_count"),
        (data.get("summary") or {}).get("duplicate_transaction_count"),
    )
    return _json_safe(data)


class BankReconciliationDetailSkill(BaseExtractionSkill):
    document_type = DOC_TYPE
    supported_extensions = {".xlsx", ".xls", ".csv"}
    skill_name = SKILL_NAME
    skill_version = "v1"

    def extract(self, input_data: ExtractionInput) -> ExtractionResult:
        files = _normalize_input_files(input_data.metadata.get("files")) if isinstance(input_data.metadata.get("files"), list) else []
        if not files:
            if input_data.file_path:
                files = [{"file_path": input_data.file_path, "file_name": input_data.file_name}]
        logger.info(
            "[BankReconciliationDetailSkill] start received_file_count=%s received_file_paths=%s source_file_names=%s",
            len(files),
            [item.get("file_path") for item in files],
            [item.get("file_name") for item in files],
        )
        data = parse_bank_reconciliation_files(files, metadata=input_data.metadata)
        markdown = data.get("display_markdown") or ""
        success = data.get("extraction_status") != "failed" and bool((data.get("summary") or {}).get("deduped_transaction_count"))
        return ExtractionResult(
            document_type=DOC_TYPE,
            schema_version=SCHEMA_VERSION,
            extracted_json=data,
            markdown_summary=markdown,
            confidence=0.92 if success else 0.2,
            warnings=list(data.get("warnings") or []),
            errors=[] if success else [str(data.get("failure_reason") or "未识别到有效银行对账明细")],
            skill_name=SKILL_NAME,
            skill_version=self.skill_version,
        )


def build_bank_reconciliation_detail_content(
    *,
    file_path: str,
    file_name: str = "",
    customer_id: str = "",
    document_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    skill = BankReconciliationDetailSkill()
    result = skill.extract(
        ExtractionInput(
            customer_id=customer_id,
            document_id=document_id,
            document_type=DOC_TYPE,
            file_name=file_name or Path(file_path).name,
            file_path=file_path,
            mime_type=None,
            raw_text="",
            metadata=metadata or {},
        )
    )
    content = {
        "type": DOC_TYPE,
        "name": DOC_TYPE_NAME,
        "title": DOC_TYPE_NAME,
        "doc_type": DOC_TYPE,
        "doc_type_name": DOC_TYPE_NAME,
        "document_type": DOC_TYPE,
        "document_type_code": DOC_TYPE,
        "document_type_name": DOC_TYPE_NAME,
        "storage_label": DOC_TYPE_NAME,
        "extraction_status": "success" if not result.errors else "failed",
        "extraction_error": "；".join(result.errors),
        "display_markdown": result.markdown_summary,
        "markdown": result.markdown_summary,
        "markdown_summary": result.markdown_summary,
        "report_markdown": result.markdown_summary,
        "structured_data": result.extracted_json,
        "data": {"display_markdown": result.markdown_summary},
    }
    return content
