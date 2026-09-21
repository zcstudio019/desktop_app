"""Presentation-only wording shared by the credit overview Markdown and HTML renderers."""

from __future__ import annotations

from typing import Any


VERIFICATION_CHECKLIST: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("优先核验", (
        "核对源征信报告及未展示账户状态",
        "核验法人相关还款责任的责任类型、当前余额及关联主体",
    )),
    ("补充资料", (
        "补充可核验收入及月还款数据",
        "补充授信及到期明细",
    )),
    ("后续评估", (
        "按拟申请银行或产品的正式规则人工评估",
        "人工核验贷款机构类别",
    )),
)


def display_rule_checks(items: list[dict[str, Any]], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Return display copies; keep rule results and credit facts untouched."""
    displayed: list[dict[str, Any]] = []
    for source in items:
        item = dict(source)
        name = item.get("item")
        if name in {"逾期记录", "90天以上逾期"} and item.get("status") == "达标":
            item["status"] = "未见异常"
        elif name == "企业对外担保" and item.get("status") == "达标":
            item["status"] = "当前为0"
        elif name == "信用卡账户数量一致性" and item.get("status") == "达标":
            item["status"] = "数量一致"
        elif item.get("status") == "达标":
            item["status"] = "已记录"

        if name == "信用卡数量":
            item["item"] = "征信概要信用账户数"
            count = metrics.get("credit_card_account_count")
            detail_count = metrics.get("credit_card_detail_count")
            if count is not None:
                item["current"] = f"概要账户数：{count}"
                item["basis"] = (
                    f"征信概要记录账户数为{count}，但当前可稳定提取信用卡明细数为{detail_count}"
                    if detail_count is not None and count != detail_count
                    else f"征信概要记录账户数为{count}"
                )
                item["direction"] = "核对源征信报告及未展示账户状态"
        displayed.append(item)
    return displayed


_ABSENCE_TEXT = {
    "public_records": {
        "无", "系统明确记载无公共记录", "系统中没有您最近5年内的公共信息记录",
    },
    "non_credit_transactions": {
        "无", "系统明确记载无非信贷交易记录", "系统中没有您最近5年内的非信贷交易记录",
    },
}


def absence_statement(records: list[dict[str, Any]], kind: str) -> str | None:
    """Summarize explicit absence once; do not infer absence from an empty list."""
    allowed = _ABSENCE_TEXT.get(kind)
    if not allowed or not records:
        return None
    values = [
        str(item[field]).strip().rstrip("。；; ")
        for item in records
        for field in ("record_type", "content", "status")
        if item.get(field) not in (None, "")
    ]
    if not values or any(value not in allowed for value in values):
        return None
    label = "公共记录" if kind == "public_records" else "非信贷交易记录"
    period = "近5年" if any("最近5年内" in value for value in values) else ""
    return f"{period}未发现{label}（源资料明确记载）"
