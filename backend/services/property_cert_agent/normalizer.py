from __future__ import annotations

import re
import logging
from typing import Any

from .skills.attachment_page_skill import is_valid_building_type, is_valid_unit_number_value

logger = logging.getLogger(__name__)

NEW_FIELD_ORDER = [
    "权利人",
    "共有情况",
    "权证编号",
    "封面编号",
    "坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "土地用途",
    "房屋用途",
    "地号",
    "宗地面积",
    "建筑面积",
    "使用期限",
    "室号或部位",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记机构",
]

OLD_FIELD_ORDER = [
    "权利人",
    "权证编号",
    "房地坐落",
    "权属性质",
    "使用权取得方式",
    "土地用途",
    "房屋用途",
    "宗地号",
    "宗地面积",
    "土地使用期限",
    "室号或部位",
    "建筑面积",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日",
    "登记日期",
    "填证单位",
]

SYNONYM_GROUPS = (
    ("权利性质", "权属性质"),
    ("地号", "宗地号"),
    ("使用期限", "土地使用期限"),
    ("登记日期", "登记日"),
)

OLD_ADDRESS_KEYS = ("房地坐落", "坐落", "property_address", "address")
NEW_ADDRESS_KEYS = ("坐落", "房地坐落", "property_address", "address")
FIELD_ALIASES = {
    "building_type": "建筑类型",
    "house_type": "建筑类型",
    "property_address": "坐落",
    "address": "坐落",
    "property_unit_number": "不动产单元号",
    "real_estate_unit_no": "不动产单元号",
    "real_estate_unit_number": "不动产单元号",
    "certificate_number": "权证编号",
    "cover_certificate_number": "封面编号",
    "owner": "权利人",
    "co_ownership": "共有情况",
    "shared_status": "共有情况",
    "right_type": "权利类型",
    "right_nature": "权利性质",
    "land_use": "土地用途",
    "house_use": "房屋用途",
    "building_use": "房屋用途",
    "use_type": "房屋用途",
    "parcel_number": "地号",
    "land_area": "宗地面积",
    "building_area": "建筑面积",
    "land_use_term": "使用期限",
    "use_term": "使用期限",
    "room_number": "室号或部位",
    "total_floors": "总层数",
    "completion_date": "竣工日期",
    "registration_date": "登记日期",
    "registration_authority": "登记机构",
}

DIRTY_STOP_LABELS = (
    "房屋用途",
    "土地用途",
    "宗地面积",
    "建筑面积",
    "地号",
    "使用权面积",
    "独用面积",
    "分摊面积",
    "房屋状况",
    "土地状况",
    "室号部位",
    "室号或部位",
    "权利其他状况",
    "类型",
    "建筑类型",
    "总层数",
    "竣工日期",
    "使用期限",
    "登记日期",
    "登记机构",
)
FIELD_STOP_LABELS = tuple(dict.fromkeys((*DIRTY_STOP_LABELS, *NEW_FIELD_ORDER, *OLD_FIELD_ORDER, "不动产单元号", "共有情况", "权利类型", "权利性质", "封面编号")))
INVALID_OWNER_KEYWORDS = (
    "合法权益",
    "申请登记",
    "经审查核实",
    "准予登记",
    "颁发此证",
    "不动产权利人申请",
    "根据《中华人民共和国物权法》",
    "登记机构",
    "国土资源部监制",
    "权利人合法权益",
    "对不动产权利人",
)
CO_OWNER_VALUES = ("单独所有", "共同共有", "按份共有", "共有", "单独所有/共同共有")

LAND_USE_VALUES = ("其它商服用地", "其他商服用地", "城镇住宅用地", "住宅用地", "商业用地", "工业用地", "办公用地", "仓储用地", "住宅", "商业", "办公", "工业", "仓储")
HOUSE_USE_VALUES = ("办公", "居住", "住宅", "商业", "工业", "仓储", "车库", "公寓", "商铺", "非居住")
BUILDING_TYPE_VALUES = ("办公楼", "公寓", "住宅", "商业", "工业", "厂房", "车库", "仓库", "商铺", "别墅", "非居住", "居住")
ATTACHMENT_PLACEHOLDERS = ("详见附记", "详见附页", "见附记", "详见附表", "详见附记页", "详见附件")


def clean_value(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\\n", "\n")
    text = re.sub(r"\s+", " ", text).strip(" :：,，;；。")
    return text


def is_attachment_placeholder(value: Any) -> bool:
    text = clean_value(value)
    return bool(text and any(placeholder in text for placeholder in ATTACHMENT_PLACEHOLDERS))


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").replace("\\n", "\n"))


def _lines(text: str) -> list[str]:
    return [re.sub(r"\s+", " ", line).strip(" :：,，;；。") for line in str(text or "").replace("\\n", "\n").splitlines() if line.strip()]


def _is_new_version(fields: dict[str, Any], raw_text: str = "", page_role: str = "", cert_version: str = "") -> bool:
    compact = _compact(raw_text)
    cert_number = str(fields.get("权证编号") or fields.get("certificate_number") or "")
    return (
        cert_version in {"new_real_estate_cert", "new_real_estate_detail_page"}
        or page_role in {"new_real_estate_detail_page", "detail_page"}
        or "不动产权第" in compact
        or "不动产单元号" in compact
        or any(key in fields for key in ("不动产单元号", "共有情况", "权利类型", "权利性质"))
        or "不动产权第" in cert_number
    )


def _truncate_before_labels(value: str, labels: tuple[str, ...] = DIRTY_STOP_LABELS) -> str:
    text = clean_value(value)
    best = len(text)
    for label in labels:
        idx = text.find(label)
        if idx > 0:
            best = min(best, idx)
    text = text[:best]
    text = re.split(r"[/／]", text, maxsplit=1)[0] if any(label in value for label in ("房屋用途", "土地用途")) else text
    return clean_value(text)


def _pick_known_value(value: str, candidates: tuple[str, ...]) -> str:
    text = clean_value(value)
    for candidate in candidates:
        if candidate in text:
            return candidate
    return _truncate_before_labels(text)


def _normalize_area(value: str) -> str:
    text = _truncate_before_labels(value, ("建筑面积", "宗地面积", "使用期限", "地号", "房屋状况", "土地状况", "室号部位", "室号或部位", "类型", "总层数", "竣工日期"))
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return ""
    number = match.group(0)
    return f"{number} 平方米"


def _label_value(raw_text: str, labels: tuple[str, ...], *, max_next_lines: int = 2) -> str:
    split_lines = _lines(raw_text)
    for index, line in enumerate(split_lines):
        for label in labels:
            if label not in line:
                continue
            after = clean_value(line.split(label, 1)[-1])
            if after:
                return _truncate_before_labels(after)
            values: list[str] = []
            for next_line in split_lines[index + 1 : index + 1 + max_next_lines]:
                if any(stop in next_line for stop in FIELD_STOP_LABELS if stop not in labels):
                    break
                values.append(next_line)
            return _truncate_before_labels(" ".join(values))
    return ""


def is_invalid_property_owner(value: Any) -> bool:
    text = clean_value(value)
    return bool(text and any(keyword in text for keyword in INVALID_OWNER_KEYWORDS))


def _is_field_label(value: str) -> bool:
    compact = re.sub(r"[\s:：,，;；。]+", "", str(value or ""))
    return any(compact == re.sub(r"[\s:：,，;；。]+", "", label) for label in FIELD_STOP_LABELS)


def _recover_owner_and_co_owner(raw_text: str) -> tuple[str, str]:
    split_lines = _lines(raw_text)
    for index, line in enumerate(split_lines):
        if re.sub(r"[\s:：,，;；。]+", "", line) != "权利人":
            continue
        owner = ""
        for candidate_line in split_lines[index + 1 : index + 5]:
            candidate = clean_value(candidate_line)
            if not candidate or _is_field_label(candidate):
                break
            if is_invalid_property_owner(candidate):
                owner = ""
                break
            owner = candidate
            break
        if not owner:
            continue
        co_owner = ""
        for offset, candidate_line in enumerate(split_lines[index + 1 : index + 9], start=index + 1):
            compact = re.sub(r"[\s:：,，;；。]+", "", candidate_line)
            if compact != "共有情况" and not compact.startswith("共有情况"):
                continue
            after = clean_value(candidate_line.replace("共有情况", ""))
            values = [after, *split_lines[offset + 1 : offset + 3]]
            for value in values:
                value = clean_value(value)
                if not value or _is_field_label(value):
                    continue
                co_owner = value
                break
            break
        if co_owner:
            for value in CO_OWNER_VALUES:
                if value in co_owner:
                    co_owner = value
                    break
        return owner, co_owner
    return "", ""


def _recover_new_cert_number(raw_text: str) -> str:
    compact = _compact(raw_text)
    match = re.search(r"([沪京津渝苏浙粤鲁豫川湘鄂闽皖赣辽吉黑冀晋陕甘青桂琼贵云藏宁新内][（(]?\d{4}[）)]?[^第号]{0,8}字?不动产权第\d{4,8}号)", compact)
    if match:
        value = match.group(1).replace("（", "(").replace("）", ")")
        return value
    return ""


def _recover_old_cert_number(raw_text: str) -> str:
    compact = _compact(raw_text)
    match = re.search(r"(沪房地[\u4e00-\u9fa5]{1,4}字[（(]?\d{4}[）)]?第\d{4,8}号)", compact)
    if match:
        return match.group(1).replace("（", "(").replace("）", ")")
    return ""


def _recover_use_term(raw_text: str) -> str:
    compact = _compact(raw_text)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日起(?:19|20)\d{2})使用期限?年?(\d{1,2}月\d{1,2}日止?)", compact)
    if match:
        return f"{match.group(1)}年{match.group(2)}"
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日起(?:19|20)\d{2}年?\d{1,2}月\d{1,2}日止?)", compact)
    if match:
        value = re.sub(r"(?<=\d{4})(?=\d{1,2}月)", "年", match.group(1))
        return value if value.endswith("止") else f"{value}止"
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日至(?:19|20)\d{2}年?\d{1,2}月\d{1,2}日止?)", compact)
    if match:
        value = re.sub(r"(?<=\d{4})(?=\d{1,2}月)", "年", match.group(1))
        return value if value.endswith("止") else f"{value}止"
    return ""


def _recover_usage(raw_text: str) -> tuple[str, str]:
    compact = _compact(raw_text)
    match = re.search(r"土地用途[:：]?([^/／；;]+)[/／；;]?房屋用途[:：]?([^；;宗建地使房室类总竣]+)", compact)
    if match:
        return _pick_known_value(match.group(1), LAND_USE_VALUES), _pick_known_value(match.group(2), HOUSE_USE_VALUES)
    return "", ""


def _recover_building_status(raw_text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    compact = _compact(raw_text)
    room = re.search(r"(?:室号部位|室号或部位)[:：]?(\d+[A-Za-z]?)", compact)
    if room:
        result["室号或部位"] = room.group(1)
    building_type = re.search(r"(?:房屋状况)?.{0,40}(?:建筑类型|房屋类型|类型)[:：]?([^；;总竣建]+)", compact)
    if building_type:
        value = _pick_known_value(building_type.group(1), BUILDING_TYPE_VALUES)
        if value:
            result["建筑类型"] = value
    floors = re.search(r"总层数[:：]?(\d+)", compact)
    if floors:
        result["总层数"] = floors.group(1)
    completion = re.search(r"竣工日期[:：]?((?:19|20)\d{2}年)", compact)
    if completion:
        result["竣工日期"] = completion.group(1)
    return result


def _dirty(value: str) -> bool:
    return "\\n" in str(value) or "\n" in str(value) or any(label in str(value) for label in DIRTY_STOP_LABELS)


def normalize_property_cert_fields(fields: dict[str, Any], raw_text: str = "", page_role: str = "", cert_version: str = "") -> dict[str, Any]:
    logger.info("[PropertyNormalizer] before_fields=%s", fields)
    source = dict(fields or {})
    for alias, target in FIELD_ALIASES.items():
        if source.get(alias) and not source.get(target):
            source[target] = source.get(alias)
        source.pop(alias, None)

    old_version = is_old_version(page_role, source) and not _is_new_version(source, raw_text, page_role, cert_version)
    new_version = not old_version
    before_owner = source.get("权利人")
    invalid_owner = is_invalid_property_owner(before_owner)
    logger.info("[PropertyNormalizer][OWNER] before_权利人=%s", before_owner or "")
    logger.info("[PropertyNormalizer][OWNER] invalid_owner_removed=%s", str(invalid_owner).lower())
    if invalid_owner:
        source.pop("权利人", None)
    for key, value in list(source.items()):
        if is_attachment_placeholder(value):
            logger.info("[PropertyNormalizer] placeholder_field_removed field=%s value=%s", key, value)
            source.pop(key, None)
    if source.get("不动产单元号") and not is_valid_unit_number_value(source.get("不动产单元号")):
        logger.info("[PropertyNormalizer] invalid_unit_number_removed=%s", source.get("不动产单元号"))
        source.pop("不动产单元号", None)
    if source.get("建筑类型") and not is_valid_building_type(source.get("建筑类型")):
        logger.info("[PropertyNormalizer] invalid_building_type_removed=%s", source.get("建筑类型"))
        source.pop("建筑类型", None)

    if new_version:
        address = _first_non_empty(source, NEW_ADDRESS_KEYS)
        if address:
            source["坐落"] = address
        source.pop("房地坐落", None)
        if source.get("权属性质") and not source.get("权利性质"):
            source["权利性质"] = source.pop("权属性质")
        else:
            source.pop("权属性质", None)
        if source.get("宗地号") and not source.get("地号"):
            source["地号"] = source.pop("宗地号")
        else:
            source.pop("宗地号", None)
        if source.get("土地使用期限") and not source.get("使用期限"):
            source["使用期限"] = source.pop("土地使用期限")
        else:
            source.pop("土地使用期限", None)
    else:
        address = _first_non_empty(source, OLD_ADDRESS_KEYS)
        if address:
            source["房地坐落"] = address
        source.pop("坐落", None)
        if source.get("权利性质") and not source.get("权属性质"):
            source["权属性质"] = source.pop("权利性质")
        else:
            source.pop("权利性质", None)
        if source.get("地号") and not source.get("宗地号"):
            source["宗地号"] = source.pop("地号")
        else:
            source.pop("地号", None)
        if source.get("使用期限") and not source.get("土地使用期限"):
            source["土地使用期限"] = source.pop("使用期限")
        else:
            source.pop("使用期限", None)

    if source.get("权证编号") and re.fullmatch(r"D\d{8,}", clean_value(source["权证编号"])):
        if not source.get("封面编号"):
            source["封面编号"] = source["权证编号"]
        logger.info("[PropertyNormalizer] dirty_field_removed field=权证编号")
        source.pop("权证编号", None)

    cert = _recover_new_cert_number(raw_text) if new_version else _recover_old_cert_number(raw_text)
    if cert and (not source.get("权证编号") or re.fullmatch(r"D\d{8,}", clean_value(source.get("权证编号")))):
        source["权证编号"] = cert
        logger.info("[PropertyNormalizer] recovered_field field=权证编号 value=%s", cert)

    recovered_owner, recovered_co_owner = _recover_owner_and_co_owner(raw_text)
    if not source.get("权利人"):
        owner = recovered_owner or _label_value(raw_text, ("权利人",), max_next_lines=2)
        if is_invalid_property_owner(owner):
            owner = ""
        if owner:
            source["权利人"] = owner
            logger.info("[PropertyNormalizer] recovered_field field=权利人 value=%s", owner)
            logger.info("[PropertyNormalizer][OWNER] recovered_权利人=%s", owner)
    if new_version and (not source.get("共有情况") or _dirty(str(source.get("共有情况")))):
        if recovered_co_owner:
            source["共有情况"] = recovered_co_owner
            logger.info("[PropertyNormalizer][CO_OWNER] recovered_共有情况=%s", recovered_co_owner)

    if new_version:
        for key, labels in (
            ("坐落", ("坐落",)),
            ("不动产单元号", ("不动产单元号",)),
            ("地号", ("地号",)),
        ):
            if not source.get(key) or _dirty(str(source.get(key))):
                value = _label_value(raw_text, labels, max_next_lines=2)
                if value:
                    source[key] = value
                    if key == "地号":
                        logger.info("[PropertyNormalizer] recovered_field field=地号 value=%s", value)

    land_use, house_use = _recover_usage(raw_text)
    if land_use:
        source["土地用途"] = land_use
    if house_use:
        source["房屋用途"] = house_use

    for key, candidates in (("土地用途", LAND_USE_VALUES), ("房屋用途", HOUSE_USE_VALUES)):
        if source.get(key):
            source[key] = _pick_known_value(str(source[key]), candidates)
    if source.get("地号"):
        source["地号"] = _truncate_before_labels(str(source["地号"]), ("使用权面积", "独用面积", "分摊面积", "房屋状况", "室号部位", "室号或部位", "权利其他状况", "类型", "建筑类型", "总层数", "竣工日期"))
    if source.get("宗地号"):
        source["宗地号"] = _truncate_before_labels(str(source["宗地号"]), ("使用权面积", "独用面积", "分摊面积", "房屋状况", "室号部位", "室号或部位", "权利其他状况", "类型", "建筑类型", "总层数", "竣工日期"))

    for area_key in ("宗地面积", "建筑面积"):
        if source.get(area_key):
            source[area_key] = _normalize_area(str(source[area_key]))

    term_key = "土地使用期限" if old_version else "使用期限"
    if source.get(term_key):
        term = clean_value(source[term_key])
        if not re.search(r"(?:19|20)\d{2}年\d{1,2}月\d{1,2}日(?:起|至)", term):
            recovered = _recover_use_term(raw_text)
            if recovered:
                source[term_key] = recovered
        else:
            source[term_key] = term
    else:
        recovered = _recover_use_term(raw_text)
        if recovered:
            source[term_key] = recovered

    for key, value in _recover_building_status(raw_text).items():
        if not source.get(key) or _dirty(str(source.get(key))):
            source[key] = value

    allowed = OLD_FIELD_ORDER if old_version else NEW_FIELD_ORDER
    normalized: dict[str, str] = {}
    for key in allowed:
        value = clean_value(source.get(key))
        if not value:
            continue
        if "\\n" in value or "\n" in value:
            logger.info("[PropertyNormalizer] dirty_field_removed field=%s", key)
            continue
        if is_attachment_placeholder(value):
            logger.info("[PropertyNormalizer] placeholder_field_removed field=%s value=%s", key, value)
            continue
        if key == "不动产单元号" and not is_valid_unit_number_value(value):
            logger.info("[PropertyNormalizer] invalid_unit_number_removed=%s", value)
            continue
        if key == "建筑类型" and not is_valid_building_type(value):
            logger.info("[PropertyNormalizer] invalid_building_type_removed=%s", value)
            continue
        normalized[key] = value
    logger.info("[PropertyNormalizer] after_fields=%s", normalized)
    return normalized


def is_old_version(page_role: str, fields: dict[str, Any]) -> bool:
    cert_number = str(fields.get("权证编号") or fields.get("certificate_number") or "")
    return (
        page_role == "old_property_detail_page"
        or any(key in fields for key in ("房地坐落", "权属性质", "使用权取得方式", "宗地号", "土地使用期限"))
        or "沪房地" in cert_number
    )


def _first_non_empty(fields: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = fields.get(key, "")
        if value:
            return value
    return ""


def _normalize_address_aliases(fields: dict[str, str], *, old_version: bool) -> None:
    if old_version:
        address = _first_non_empty(fields, OLD_ADDRESS_KEYS)
        if address:
            fields["房地坐落"] = address
        fields.pop("坐落", None)
    else:
        address = _first_non_empty(fields, NEW_ADDRESS_KEYS)
        if address:
            fields["坐落"] = address
        fields.pop("房地坐落", None)
    fields.pop("property_address", None)
    fields.pop("address", None)


def normalize_fields(fields: dict[str, Any], *, old_version: bool = False) -> dict[str, Any]:
    if not old_version:
        return normalize_property_cert_fields(fields, page_role="")
    old_version = old_version or is_old_version("", fields or {})
    cleaned = {
        key: clean_value(value)
        for key, value in (fields or {}).items()
        if clean_value(value) and not is_attachment_placeholder(value)
    }
    if cleaned.get("不动产单元号") and not is_valid_unit_number_value(cleaned.get("不动产单元号")):
        logger.info("[PropertyNormalizer] invalid_unit_number_removed=%s", cleaned.get("不动产单元号"))
        cleaned.pop("不动产单元号", None)
    if cleaned.get("建筑类型") and not is_valid_building_type(cleaned.get("建筑类型")):
        logger.info("[PropertyNormalizer] invalid_building_type_removed=%s", cleaned.get("建筑类型"))
        cleaned.pop("建筑类型", None)
    for alias, target in FIELD_ALIASES.items():
        if cleaned.get(alias) and not cleaned.get(target):
            cleaned[target] = cleaned.pop(alias)
        else:
            cleaned.pop(alias, None)
    if old_version:
        logger.info("[PropertyNormalizer][ADDRESS] input_房地坐落=%s", cleaned.get("房地坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] input_坐落=%s", cleaned.get("坐落"))
    _normalize_address_aliases(cleaned, old_version=old_version)
    for new_key, old_key in SYNONYM_GROUPS:
        if old_version:
            if new_key in cleaned and old_key in cleaned:
                cleaned.pop(new_key, None)
            elif new_key in cleaned and old_key not in cleaned:
                cleaned[old_key] = cleaned.pop(new_key)
        else:
            if old_key in cleaned and new_key in cleaned:
                cleaned.pop(old_key, None)
            elif old_key in cleaned and new_key not in cleaned:
                cleaned[new_key] = cleaned.pop(old_key)
    order = OLD_FIELD_ORDER if old_version else NEW_FIELD_ORDER
    ordered = {key: cleaned[key] for key in order if cleaned.get(key)}
    for key, value in cleaned.items():
        if key not in ordered:
            ordered[key] = value
    if old_version:
        logger.info("[PropertyNormalizer][ADDRESS] output_房地坐落=%s", ordered.get("房地坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] output_坐落=%s", ordered.get("坐落"))
        logger.info("[PropertyNormalizer][ADDRESS] output_keys=%s", list(ordered.keys()))
    return ordered


def field_confidence(fields: dict[str, Any]) -> dict[str, float]:
    return {key: 0.86 for key, value in fields.items() if value}
