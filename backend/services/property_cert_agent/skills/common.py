from __future__ import annotations

import re


LABELS = (
    "权利人",
    "共有情况",
    "权证编号",
    "房地坐落",
    "坐落",
    "不动产单元号",
    "权利类型",
    "权利性质",
    "权属性质",
    "使用权取得方式",
    "土地用途",
    "房屋用途",
    "用途",
    "地号",
    "宗地号",
    "宗地面积",
    "建筑面积",
    "使用期限",
    "土地使用期限",
    "室号或部位",
    "室号部位",
    "建筑类型",
    "总层数",
    "竣工日期",
    "登记日期",
    "登记日",
    "填证单位",
)


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or ""))


def lines(text: str) -> list[str]:
    return [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines() if line.strip()]


def clean(value: str) -> str:
    value = re.sub(r"\s+", " ", str(value or ""))
    return value.strip(" :：,，;；")


def strip_trailing_labels(value: str) -> str:
    value = clean(value)
    for label in LABELS:
        idx = value.find(label)
        if idx > 0:
            return clean(value[:idx])
    return value


def label_value(text: str, labels: tuple[str, ...], *, max_next_lines: int = 2) -> str:
    label_pattern = "|".join(re.escape(label) for label in labels)
    stop_pattern = "|".join(re.escape(label) for label in LABELS if label not in labels)
    joined = "\n".join(lines(text))
    match = re.search(rf"(?:{label_pattern})\s*[:：]?\s*([^\n]+)", joined)
    if match:
        return strip_trailing_labels(match.group(1))
    split_lines = lines(text)
    for index, line in enumerate(split_lines):
        if any(label == line or label in line for label in labels):
            after = line
            for label in labels:
                after = after.replace(label, "")
            after = clean(after)
            if after:
                return strip_trailing_labels(after)
            candidates = []
            for next_line in split_lines[index + 1 : index + 1 + max_next_lines]:
                if re.search(stop_pattern, next_line):
                    break
                candidates.append(next_line)
            return strip_trailing_labels(" ".join(candidates))
    return ""


def certificate_number(text: str) -> str:
    compact = compact_text(text)
    patterns = (
        r"([沪京津渝苏浙粤鲁豫川湘鄂闽皖赣辽吉黑冀晋陕甘青桂琼贵云藏宁新内]\(?\d{4}\)?[^第]{0,8}不动产权第\d+号)",
        r"((?:沪|上海)[^号]{0,20}房地[^号]{0,20}第\d+号)",
        r"(沪房权证[^号]{0,20}第\d+号)",
    )
    for pattern in patterns:
        match = re.search(pattern, compact)
        if match:
            value = match.group(1)
            if not re.fullmatch(r"D\d{8,}", value):
                return value
    return ""


def normalize_use_term(text: str) -> str:
    compact = compact_text(text)
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日起(?:19|20)\d{2}年?\d{1,2}月\d{1,2}日止?)", compact)
    if match:
        value = match.group(1)
        value = re.sub(r"(?<=\d{4})(?=\d{1,2}月)", "年", value)
        if not value.endswith("止"):
            value += "止"
        return value
    match = re.search(r"((?:19|20)\d{2}年\d{1,2}月\d{1,2}日?起(?:19|20)\d{2}).{0,8}?年?(\d{1,2}月\d{1,2}日止?)", compact)
    if match:
        value = f"{match.group(1)}年{match.group(2)}"
        value = value.replace("日日", "日")
        if not value.endswith("止"):
            value += "止"
        return value
    return ""


def split_usage(text: str) -> tuple[str, str]:
    land = label_value(text, ("土地用途",))
    house = label_value(text, ("房屋用途",))
    if land and house:
        return land, house
    compact = compact_text(text)
    match = re.search(r"用途[:：]?([^/／\s]+)[/／]([^面积期限]+)", compact)
    if match:
        return clean(match.group(1)), clean(match.group(2))
    return land, house
