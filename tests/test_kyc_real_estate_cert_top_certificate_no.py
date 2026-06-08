from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent
from backend.services.kyc_document_agent.skills.property_cert_skill import _extract_cert_number


def test_top_certificate_number_has_priority_over_cover_number():
    text = """
不动产权证书
沪（2022）宝字 不动产权第011468号
权利人 智先生数字科技（上海）有限公司
不动产单元号 310113015003GB00011F00020088
编号 D31003610514
"""

    result = run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "产权证-306(1).pdf", "declared_doc_type": "property_cert"},
        }
    )

    assert result["fields"]["权证编号"] == "沪(2022)宝字不动产权第011468号"
    assert result["fields"]["权证编号"] != "D31003610514"


def test_extract_cert_number_assembles_split_top_number():
    value, _ = _extract_cert_number("沪（2022）宝字  不动产权第011468号 编号 D31003610514")

    assert value == "沪(2022)宝字不动产权第011468号"


def test_cover_like_d_number_is_not_used_as_certificate_number():
    value, _ = _extract_cert_number("编号 D31003610514")

    assert value == ""
