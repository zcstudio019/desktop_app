from __future__ import annotations

import re
from typing import Any


ROLE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "cover_page": (
        "根据《中华人民共和国物权法》",
        "为保护不动产权利人合法权益",
        "经审查核实",
        "准予登记",
        "颁发此证",
        "登记机构",
        "国土资源部监制",
        "编号",
    ),
    "detail_page": (
        "权利人",
        "共有情况",
        "坐落",
        "不动产单元号",
        "权利类型",
        "权利性质",
        "用途",
        "面积",
        "使用期限",
        "房屋状况",
        "土地状况",
        "建筑面积",
        "总层数",
        "竣工日期",
    ),
    "old_property_detail_page": (
        "上海市房地产权证",
        "房地坐落",
        "权属性质",
        "使用权取得方式",
        "宗地号",
        "土地使用期限",
        "室号或部位",
        "建筑类型",
    ),
    "attachment_page": ("附记", "其他权利状况", "变更登记", "抵押", "查封"),
    "mortgage_page": ("抵押权人", "被担保债权数额", "债务履行期限", "抵押登记"),
}


def _score(text: str, keywords: tuple[str, ...]) -> int:
    compact = re.sub(r"\s+", "", str(text or ""))
    return sum(1 for keyword in keywords if keyword in compact)


def detect_page_role(text: str, image_metadata: dict[str, Any] | None = None) -> str:
    scores = {role: _score(text, keywords) for role, keywords in ROLE_KEYWORDS.items()}
    compact = re.sub(r"\s+", "", str(text or ""))
    if scores["mortgage_page"] >= 2:
        return "mortgage_page"
    if "上海市房地产权证" in compact or scores["old_property_detail_page"] >= 3:
        return "old_property_detail_page"
    if scores["detail_page"] >= 3:
        return "detail_page"
    if scores["cover_page"] >= 2:
        return "cover_page"
    if scores["attachment_page"] >= 2:
        return "attachment_page"
    if scores["detail_page"] > 0:
        return "detail_page"
    if scores["cover_page"] > 0:
        return "cover_page"
    return "unknown"
