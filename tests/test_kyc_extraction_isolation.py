from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.services.document_extractor_service import build_structured_extraction
from backend.services.kyc_document_agent.renderer import render_markdown


FORBIDDEN_KEYS = {
    "historical_financial_reports",
    "financial_reports",
    "enterprise_credit_reports",
    "personal_credit_reports",
    "enterprise_flows",
    "bank_flows",
    "financial_statement_diagnostic",
    "financing_diagnostic_report",
    "comprehensive_financing_advice",
    "customer_profile_markdown",
    "customer_context",
}


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, dict):
        return any(key in FORBIDDEN_KEYS or _contains_forbidden_key(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def test_kyc_extraction_does_not_include_historical_financial_reports():
    result = build_structured_extraction(
        "上海市 房地产权证\n权利人 张三\n房地坐落 测试路1号",
        "property_cert",
        filename="房产.pdf",
        customer_id="customer-1",
        customer_name="测试客户",
        historical_financial_reports=[{
            "document_type": "financial_report",
            "source_file": "2022财务报表.pdf",
        }],
    )

    assert result["agent_type"] == "kyc_document_agent"
    assert not _contains_forbidden_key(result)
    assert "historical_financial_reports" not in json.dumps(result, ensure_ascii=False)
    assert "financial_report" not in result.get("markdown", "")


def test_kyc_metadata_is_minimal_and_context_free():
    result = build_structured_extraction(
        "上海市 房地产权证\n权利人 张三\n房地坐落 测试路1号",
        "property_cert",
        filename="房产.pdf",
        customer_id="customer-1",
        customer_name="测试客户",
        historical_financial_reports=[{"document_type": "financial_report"}],
    )

    metadata = result.get("metadata") or {}
    assert metadata["filename"] == "房产.pdf"
    assert metadata["customer_id"] == "customer-1"
    assert metadata["declared_doc_type"] == "property_cert"
    assert not _contains_forbidden_key(metadata)


def test_kyc_markdown_filters_polluted_field_payload():
    markdown = render_markdown({
        "doc_type": "property_cert",
        "doc_type_name": "房产证/房地产权证",
        "fields": {
            "权证编号": "沪房地奉字(2014)第004478号",
            "建筑面积": "148.08 平方米",
            "historical_financial_reports": [{"document_type": "financial_report"}],
        },
        "validation": {"warnings": [], "errors": []},
        "missing_fields": [],
        "confidence": {"overall": 0.7},
        "evidence": {},
    })

    assert "权证编号: 沪房地奉字(2014)第004478号" in markdown
    assert "historical_financial_reports" not in markdown
    assert "financial_report" not in markdown


def test_frontend_kyc_display_filters_pollution_and_aliases():
    util_source = Path("src/utils/kycDisplayFields.ts").read_text(encoding="utf-8")
    component_source = Path("src/components/KycExtractionResult.tsx").read_text(encoding="utf-8")

    assert "FORBIDDEN_KYC_DISPLAY_KEYS" in util_source
    assert "historical_financial_reports" in util_source
    assert "house_use: '房屋用途'" in util_source
    assert "building_use: '房屋用途'" in util_source
    assert "use_type: '房屋用途'" in util_source
    assert "JSON.stringify(result.fields" not in component_source


def test_file_router_does_not_append_raw_text_to_kyc_content():
    source = Path("backend/routers/file.py").read_text(encoding="utf-8")

    assert "KYC_EXTRACTION_TYPES" in source
    assert "is_kyc_content" in source
    assert "raw_pages and not is_kyc_content" in source
    assert "and not is_kyc_content" in source


def test_storage_services_sanitize_kyc_extracted_data_helpers():
    from backend.services.local_storage_service import sanitize_kyc_extracted_data as sanitize_local
    from backend.services.sqlalchemy_storage_service import sanitize_kyc_extracted_data as sanitize_sql

    polluted = {
        "agent_type": "kyc_document_agent",
        "doc_type": "property_cert",
        "fields": {"权证编号": "沪房地奉字(2014)第004478号"},
        "metadata": {
            "filename": "房产.pdf",
            "declared_doc_type": "property_cert",
            "historical_financial_reports": [{"document_type": "financial_report"}],
        },
        "historical_financial_reports": [{"document_type": "financial_report"}],
        "balance_sheet": {"货币资金": 1},
    }

    for sanitizer in (sanitize_local, sanitize_sql):
        cleaned = sanitizer(polluted)
        assert cleaned["metadata"]["filename"] == "房产.pdf"
        assert cleaned["metadata"]["declared_doc_type"] == "property_cert"
        assert "historical_financial_reports" not in cleaned
        assert "historical_financial_reports" not in cleaned["metadata"]
        assert "balance_sheet" not in cleaned


def test_financial_report_branch_keeps_historical_context_support_in_service_source():
    source = Path("backend/services/document_extractor_service.py").read_text(encoding="utf-8")
    agent_branch = source[source.index("if normalized_code in DOCUMENT_AGENT_DISPATCH_TYPES"):source.index("elif normalized_code == \"business_license\"")]

    assert "historical_financial_reports" in agent_branch
    assert "run_document_extraction_agent" in agent_branch
