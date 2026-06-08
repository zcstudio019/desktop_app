from __future__ import annotations

from backend.services.property_cert_agent.skills.new_real_estate_cert_skill import extract
from backend.services.property_cert_agent.skills.common import normalize_use_term


def test_use_term_cross_line_can_be_joined() -> None:
    text = """
使用期限
2015年10月16日起2076
年12月28日止
"""
    assert normalize_use_term(text) == "2015年10月16日起2076年12月28日止"


def test_use_term_complete_date_is_preserved() -> None:
    text = "使用期限 2018年08月28日起2046年08月20日止"
    assert extract({"text": text})["fields"]["使用期限"] == "2018年08月28日起2046年08月20日止"
