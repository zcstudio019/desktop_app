from __future__ import annotations

import re
from typing import Iterable


def clean_value(value: object) -> str:
    text = str(value or "")
    text = re.sub(r"[ \t\u3000]+", " ", text)
    return text.strip(" \t\r\n:：,，;；。")


def clean_amount(value: object) -> str:
    text = clean_value(value)
    text = re.sub(r"\s+", "", text)
    return text


def lines(text: str) -> list[str]:
    return [clean_value(line) for line in str(text or "").splitlines() if clean_value(line)]


def first_match(text: str, patterns: Iterable[str]) -> str:
    source = str(text or "")
    for pattern in patterns:
        match = re.search(pattern, source, flags=re.I | re.S)
        if match:
            return clean_value(match.group(1))
    return ""


def value_after_label(text: str, labels: tuple[str, ...], *, max_chars: int = 100) -> str:
    source = str(text or "")
    for label in labels:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*([^\n\r]{{1,{max_chars}}})"
        match = re.search(pattern, source, flags=re.I)
        if match:
            return clean_value(match.group(1))
    return ""


def split_numbered_blocks(text: str) -> list[str]:
    source = str(text or "")
    matches = list(re.finditer(r"(?m)^\s*(?:\d+[\.、)]|账户\s*\d+|业务\s*\d+)", source))
    if not matches:
        chunks = re.split(r"\n\s*\n+", source)
        return [chunk.strip() for chunk in chunks if chunk.strip()]
    blocks: list[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(source)
        block = source[match.start():end].strip()
        if block:
            blocks.append(block)
    return blocks
