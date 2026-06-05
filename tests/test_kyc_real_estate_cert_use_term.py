from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


def _extract(text: str) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "房产正面.pdf", "declared_doc_type": "property_cert"},
        }
    )


def test_real_estate_cert_extracts_complete_use_term():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086 "
        "使用期限 国有建设用地使用权使用期限：2015年10月16日起2076年12月28日止 "
        "建筑面积：62.40平方米 竣工日期：2011年"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日止"
    assert result["fields"]["土地使用期限"] == "2015年10月16日起2076年12月28日止"
    assert result["fields"]["使用期限"] != "2015年10月16日起2076"


def test_real_estate_cert_extracts_cross_line_use_term():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086\n"
        "使用期限：2015年10月16日起2076\n年12月28日止\n"
        "建筑面积：62.40平方米\n竣工日期：2011年"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日止"


def test_real_estate_cert_keeps_term_without_stop_when_ocr_misses_stop():
    result = _extract(
        "不动产权证书 权利人 沃志方 不动产单元号 310104019001GB00045F00430086 "
        "使用期限：2015年10月16日起2076年12月28日 "
        "建筑面积：62.40平方米"
    )

    assert result["fields"]["使用期限"] == "2015年10月16日起2076年12月28日"
