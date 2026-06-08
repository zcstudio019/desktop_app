from __future__ import annotations

from io import BytesIO

from PIL import Image

from backend.services.property_cert_agent import ocr


def _image_bytes() -> bytes:
    image = Image.new("RGB", (800, 1000), "white")
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def test_ocr_cache_hit_skips_api_call(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(ocr, "OCR_CACHE_DIR", tmp_path)
    image_bytes = _image_bytes()
    calls = 0

    def ocr_func(_payload: bytes) -> str:
        nonlocal calls
        calls += 1
        return "不动产权证书\n权利人 沃志方\n坐落 上海市宝山区示例路1号\n建筑面积 88.88平方米\n使用期限 2015年10月16日起2076年12月28日止\n土地用途 住宅 房屋用途 居住"

    first = ocr.run_property_cert_ocr_plan(
        file_bytes=image_bytes,
        file_type="image",
        filename="房产正面.pdf",
        pdf_to_images=lambda _data: [],
        ocr_func=ocr_func,
    )
    second = ocr.run_property_cert_ocr_plan(
        file_bytes=image_bytes,
        file_type="image",
        filename="房产正面.pdf",
        pdf_to_images=lambda _data: [],
        ocr_func=ocr_func,
    )

    assert first[2]["calls"] == 1
    assert second[2]["calls"] == 0
    assert second[2]["cache_hits"] >= 1
    assert calls == 1
