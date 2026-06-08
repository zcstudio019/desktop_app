from __future__ import annotations

from io import BytesIO

from PIL import Image

from backend.services.property_cert_agent import ocr
from backend.services.property_cert_agent.ocr import (
    run_property_cert_ocr_plan,
    score_property_ocr_text,
)


def _image_bytes() -> bytes:
    image = Image.new("RGB", (1200, 1600), "white")
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def test_score_enough_stops_followup_ocr(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ocr, "OCR_CACHE_DIR", tmp_path)
    calls: list[int] = []

    def ocr_func(_payload: bytes) -> str:
        calls.append(1)
        return """
不动产权证书
沪(2022)宝字不动产权第011468号
权利人 沃志方
坐落 上海市宝山区示例路1号
不动产单元号 310113999999GB00001F00010001
建筑面积 88.88平方米
使用期限 2015年10月16日起2076年12月28日止
土地用途 住宅 房屋用途 居住
"""

    text, pages, summary = run_property_cert_ocr_plan(
        file_bytes=_image_bytes(),
        file_type="image",
        filename="房产正面.pdf",
        pdf_to_images=lambda _data: [],
        ocr_func=ocr_func,
    )

    assert score_property_ocr_text(text) >= 60
    assert pages
    assert summary["calls"] == 1
    assert len(calls) == 1


def test_use_term_region_runs_only_when_use_term_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ocr, "OCR_CACHE_DIR", tmp_path)
    seen_payloads: list[int] = []

    def ocr_func(_payload: bytes) -> str:
        seen_payloads.append(1)
        if len(seen_payloads) == 1:
            return "不动产权证书\n沪(2022)宝字不动产权第011468号\n权利人 沃志方\n坐落 上海市宝山区示例路1号\n建筑面积 88.88平方米"
        if len(seen_payloads) == 2:
            return ""
        return "使用期限 2015年10月16日起2076年12月28日止"

    text, _pages, summary = run_property_cert_ocr_plan(
        file_bytes=_image_bytes(),
        file_type="image",
        filename="房产正面.pdf",
        pdf_to_images=lambda _data: [],
        ocr_func=ocr_func,
        max_calls=5,
    )

    assert "使用期限" in text
    assert summary["calls"] <= 5


def test_image_size_error_retries_once_without_blocking(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ocr, "OCR_CACHE_DIR", tmp_path)
    calls = 0

    def ocr_func(_payload: bytes) -> str:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise RuntimeError("OCR API 错误 image size error")
        return "不动产权证书\n权利人 沃志方"

    _text, _pages, summary = run_property_cert_ocr_plan(
        file_bytes=_image_bytes(),
        file_type="image",
        filename="房产正面.pdf",
        pdf_to_images=lambda _data: [],
        ocr_func=ocr_func,
        max_calls=3,
    )

    assert calls == 3
    assert summary["skipped"] == 1
