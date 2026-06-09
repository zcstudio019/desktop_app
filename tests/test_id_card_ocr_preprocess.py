from io import BytesIO
from pathlib import Path

from PIL import Image

from backend.services.id_card_ocr_preprocess_service import (
    ocr_id_card_with_variants,
    preprocess_id_card_image,
    score_id_card_ocr_text,
)


def _sample_image_bytes() -> bytes:
    image = Image.new("RGB", (320, 200), "white")
    for x in range(60, 260):
        for y in range(55, 145):
            image.putpixel((x, y), (210, 225, 245))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def test_score_id_card_ocr_text_normal_text_high_score():
    text = """
姓名 林勇
性别 男 民族 汉
出生 1979年3月16日
住址 上海市奉贤区
公民身份号码 330303197903161234
签发机关 上海市公安局奉贤分局
有效期限 2025.02.26-2045.02.26
"""
    assert score_id_card_ocr_text(text) >= 120


def test_score_id_card_ocr_text_garbled_text_low_score():
    text = """9:30:910193090具
021091206761222022
一0一
中9:66州田
"""
    assert score_id_card_ocr_text(text) < 20


def test_preprocess_id_card_image_returns_rotation_variants():
    variants = preprocess_id_card_image(_sample_image_bytes())
    names = {item["variant"] for item in variants}
    assert {"original", "rotate_90", "rotate_180", "rotate_270"}.issubset(names)
    assert len(variants) <= 12


def test_ocr_id_card_with_variants_selects_highest_scored_candidate(monkeypatch):
    import backend.services.id_card_ocr_preprocess_service as service

    monkeypatch.setattr(
        service,
        "preprocess_id_card_image",
        lambda _image: [
            {"variant": "bad", "image_bytes": b"bad", "rotation": 0},
            {"variant": "good", "image_bytes": b"good", "rotation": 90},
        ],
    )

    def fake_ocr(image_bytes):
        if image_bytes == b"good":
            return "姓名 林勇\n公民身份号码 330303197903161234"
        return "9:30:910193090具"

    result = ocr_id_card_with_variants(b"image", fake_ocr)
    assert result["best_variant"] == "good"
    assert "姓名 林勇" in result["text"]


def test_ocr_id_card_with_variants_low_quality(monkeypatch):
    import backend.services.id_card_ocr_preprocess_service as service

    monkeypatch.setattr(
        service,
        "preprocess_id_card_image",
        lambda _image: [{"variant": "bad", "image_bytes": b"bad", "rotation": 0}],
    )

    result = ocr_id_card_with_variants(b"image", lambda _image: "9:30:910193090具")
    assert result["ocr_quality"]["status"] == "low_quality"


def test_file_router_id_card_branch_uses_specialized_ocr():
    source = Path("backend/routers/file.py").read_text(encoding="utf-8")
    assert "_should_use_id_card_ocr(explicit_document_type, filename)" in source
    assert "ocr_id_card_with_variants" in source
    assert '"source": "id_card_ocr_variants"' in source


def test_file_router_non_id_card_keeps_regular_ocr_branch():
    source = Path("backend/routers/file.py").read_text(encoding="utf-8")
    assert 'if _should_use_id_card_ocr(explicit_document_type, filename):' in source
    assert "compressed = file_service.compress_image(file_bytes)" in source
    assert "text_content = ocr_service.recognize_image(compressed)" in source
