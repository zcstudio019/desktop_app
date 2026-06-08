from __future__ import annotations

from PIL import Image

from backend.services.property_cert_agent.ocr import DEFAULT_MAX_OCR_BYTES, prepare_image_for_ocr


def test_prepare_image_for_ocr_compresses_large_image_under_limit() -> None:
    image = Image.effect_noise((3600, 3600), 96).convert("RGB")

    prepared = prepare_image_for_ocr(image)

    assert len(prepared) <= DEFAULT_MAX_OCR_BYTES


def test_prepare_image_for_ocr_keeps_payload_below_ocr_hard_limit() -> None:
    image = Image.effect_noise((4200, 3200), 120).convert("RGB")

    prepared = prepare_image_for_ocr(image)

    assert len(prepared) < 4_000_000
