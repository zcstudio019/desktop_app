from __future__ import annotations

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


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

NEW_REAL_ESTATE_STRONG_KEYWORDS = (
    "不动产权第",
    "不动产单元号",
    "共有情况",
    "权利类型",
    "权利性质",
    "国有建设用地使用权/房屋所有权",
    "国有建设用地使用权房屋所有权",
    "土地用途",
    "房屋用途",
)

OLD_SHANGHAI_STRONG_KEYWORDS = (
    "房地产权证",
    "沪房地",
    "房地坐落",
    "权属性质",
    "使用权取得方式",
    "宗地号",
    "土地使用期限",
    "宗地(丘)面积",
    "宗地丘面积",
)

ATTACHMENT_TABLE_KEYWORDS = (
    "不动产单元号",
    "土地状况",
    "房屋状况",
    "室号或部位",
    "室号部位",
    "建筑面积",
    "房屋用途",
    "土地用途",
    "总层数",
    "竣工日期",
    "类型",
    "合计",
)


def _score(text: str, keywords: tuple[str, ...]) -> int:
    compact = re.sub(r"\s+", "", str(text or ""))
    return sum(1 for keyword in keywords if keyword in compact)


def _has_attachment_title(text: str) -> bool:
    lines = [re.sub(r"[\s:：,，;；。]+", "", line) for line in str(text or "").splitlines() if line.strip()]
    return any(line == "附记" for line in lines[:5])


def detect_page_role(text: str, image_metadata: dict[str, Any] | None = None) -> str:
    scores = {role: _score(text, keywords) for role, keywords in ROLE_KEYWORDS.items()}
    compact = re.sub(r"\s+", "", str(text or ""))
    new_hits = sum(1 for keyword in NEW_REAL_ESTATE_STRONG_KEYWORDS if keyword in compact)
    old_hits = sum(1 for keyword in OLD_SHANGHAI_STRONG_KEYWORDS if keyword in compact)
    attachment_hits = sum(1 for keyword in ATTACHMENT_TABLE_KEYWORDS if keyword in compact)
    if _has_attachment_title(text) and attachment_hits >= 2:
        page_no = (image_metadata or {}).get("page_no") or (image_metadata or {}).get("page")
        logger.info("[AttachmentPageRole] detected=true page=%s hits=%s", page_no or "", attachment_hits)
        logger.info("[PropertyPageRole] detected_role=attachment_page")
        return "attachment_page"
    if "不动产权第" in compact or "不动产单元号" in compact:
        logger.info("[PropertyPageRole] detected_role=new_real_estate_detail_page new_hits=%s old_hits=%s", new_hits, old_hits)
        return "new_real_estate_detail_page"
    if new_hits >= 2:
        logger.info("[PropertyPageRole] detected_role=new_real_estate_detail_page new_hits=%s old_hits=%s", new_hits, old_hits)
        return "new_real_estate_detail_page"
    if scores["mortgage_page"] >= 2:
        logger.info("[PropertyPageRole] detected_role=mortgage_page")
        return "mortgage_page"
    if old_hits >= 2 or "上海市房地产权证" in compact or "沪房地" in compact:
        logger.info("[PropertyPageRole] detected_role=old_property_detail_page new_hits=%s old_hits=%s", new_hits, old_hits)
        return "old_property_detail_page"
    if scores["detail_page"] >= 3:
        logger.info("[PropertyPageRole] detected_role=new_real_estate_detail_page new_hits=%s old_hits=%s", new_hits, old_hits)
        return "new_real_estate_detail_page"
    if scores["cover_page"] >= 2:
        logger.info("[PropertyPageRole] detected_role=cover_page")
        return "cover_page"
    if scores["attachment_page"] >= 2:
        logger.info("[PropertyPageRole] detected_role=attachment_page")
        return "attachment_page"
    if scores["detail_page"] > 0:
        logger.info("[PropertyPageRole] detected_role=new_real_estate_detail_page new_hits=%s old_hits=%s", new_hits, old_hits)
        return "new_real_estate_detail_page"
    if scores["cover_page"] > 0:
        return "cover_page"
    return "unknown"
