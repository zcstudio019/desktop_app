from __future__ import annotations

import asyncio

from backend.services.kyc_document_agent.classifier import classify
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent
from backend.services.kyc_document_agent.renderer import render_markdown
from backend.services.kyc_profile_sync_service import build_customer_kyc_profile


SAMPLE_TEXT = """账号：03005029359
开户银行：上海银行股份有限公司浦西支行
法定代表人：（单位负责人）梁云
基本存款账户编号：J2900405373694
2022 年 08 月 10 日
"""


class FakeStorage:
    def __init__(self, extractions: list[dict] | None = None) -> None:
        self.extractions = extractions or []

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        return self.extractions

    async def list_documents(self, customer_id: str) -> list[dict]:
        return []


def extract(text: str, declared_doc_type: str | None = None) -> dict:
    metadata = {"declared_doc_type": declared_doc_type} if declared_doc_type else {}
    return KycDocumentAgent().extract({"text": text, "metadata": metadata})


def kyc_extraction(fields: dict, doc_type: str = "account_permit", confirmed_fields: dict | None = None) -> dict:
    extraction = {
        "doc_id": "doc-bank",
        "created_at": "2026-06-11T10:00:00",
        "extraction_type": doc_type,
        "extracted_data": {
            "agent_type": "kyc_document_agent",
            "doc_type": doc_type,
            "doc_type_name": "开户许可证" if doc_type == "account_permit" else "基本存款账户信息",
            "fields": fields,
        },
    }
    if confirmed_fields is not None:
        extraction["confirmed_data"] = {
            "confirmed_fields": confirmed_fields,
            "confirm_status": "partial",
        }
    return extraction


def build_profile(extractions: list[dict]) -> dict:
    return asyncio.run(build_customer_kyc_profile(FakeStorage(extractions), "customer-1"))


def test_sample_account_permit_extracts_core_fields() -> None:
    result = extract(SAMPLE_TEXT, declared_doc_type="account_license")
    fields = result["fields"]

    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] == "account_permit"
    assert fields["bank_account_number"] == "03005029359"
    assert fields["opening_bank"] == "上海银行股份有限公司浦西支行"
    assert fields["legal_representative"] == "梁云"
    assert fields["basic_account_number"] == "J2900405373694"
    assert fields["issue_date"] == "2022-08-10"
    assert fields["account_type"] == "基本存款账户"
    assert result["extraction_status"] in {"partial", "success"}


def test_basic_account_info_extracts_doc_type() -> None:
    result = extract("""基本存款账户信息
存款人名称 上海示例科技有限公司
账号 123456789012
开户银行 上海银行股份有限公司浦西支行
""")

    assert result["doc_type"] == "basic_account_info"
    assert result["fields"]["company_name"] == "上海示例科技有限公司"
    assert result["fields"]["bank_account_number"] == "123456789012"


def test_account_permit_extracts_doc_type() -> None:
    result = extract("""开户许可证
核准号 J123456789
开户银行 中国工商银行上海分行营业部
账号 123456789012345678
""")

    assert result["doc_type"] == "account_permit"
    assert result["fields"]["opening_bank"] == "中国工商银行上海分行营业部"
    assert result["fields"]["approval_number"] == "J123456789"


def test_account_date_formats_normalize() -> None:
    for value in ("2022年08月10日", "2022 年 08 月 10 日", "2022.08.10", "2022/08/10"):
        result = extract(f"开户许可证\n账号 03005029359\n开户银行 上海银行股份有限公司浦西支行\n日期 {value}")
        assert result["fields"]["issue_date"] == "2022-08-10"


def test_basic_account_number_not_used_as_bank_account_number() -> None:
    result = extract("""开户许可证
基本存款账户编号：J2900405373694
开户银行：上海银行股份有限公司浦西支行
""")

    assert result["fields"].get("basic_account_number") == "J2900405373694"
    assert result["fields"].get("bank_account_number") in (None, "")


def test_missing_company_name_is_not_fabricated() -> None:
    result = extract(SAMPLE_TEXT, declared_doc_type="account_permit")

    assert result["fields"].get("company_name") in (None, "")
    assert "company_name" in result["missing_fields"]
    assert "bank_account_name" in result["missing_fields"]


def test_legal_representative_same_line_extracts() -> None:
    result = extract("开户许可证\n法定代表人：梁云", declared_doc_type="account_permit")

    assert result["fields"]["legal_representative"] == "梁云"


def test_unit_responsible_person_same_line_extracts() -> None:
    result = extract("开户许可证\n单位负责人：梁云", declared_doc_type="account_permit")

    assert result["fields"]["legal_representative"] == "梁云"


def test_legal_representative_next_line_extracts() -> None:
    result = extract("""开户许可证
法定代表人：
梁云
""", declared_doc_type="account_permit")

    assert result["fields"]["legal_representative"] == "梁云"


def test_legal_representative_two_label_lines_extracts() -> None:
    result = extract("""开户许可证
法定代表人：
（单位负责人）
梁云
""", declared_doc_type="account_permit")

    assert result["fields"]["legal_representative"] == "梁云"


def test_legal_representative_layout_sample_extracts() -> None:
    result = extract("""账号：03005029359
开户银行：上海银行股份有限公司浦西支行
法定代表人：
（单位负责人）
梁云
基本存款账户编号：J2900405373694
2022 年 08 月 10 日
""", declared_doc_type="account_permit")
    fields = result["fields"]

    assert fields["bank_account_number"] == "03005029359"
    assert fields["opening_bank"] == "上海银行股份有限公司浦西支行"
    assert fields["legal_representative"] == "梁云"
    assert fields["basic_account_number"] == "J2900405373694"
    assert fields["issue_date"] == "2022-08-10"


def test_legal_representative_does_not_use_bank_name() -> None:
    result = extract("""开户许可证
法定代表人：
（单位负责人）
开户银行：上海银行股份有限公司浦西支行
基本存款账户编号：J2900405373694
""", declared_doc_type="account_permit")

    assert result["fields"].get("legal_representative") in (None, "")
    assert "legal_representative" in result["missing_fields"]


def test_account_renderer_legal_representative_not_missing_when_extracted() -> None:
    result = extract("""开户许可证
法定代表人：
（单位负责人）
梁云
账号：03005029359
开户银行：上海银行股份有限公司浦西支行
基本存款账户编号：J2900405373694
""", declared_doc_type="account_permit")
    markdown = result["markdown"]

    assert "法定代表人/单位负责人：梁云" in markdown
    missing_section = markdown.split("### 缺失字段", 1)[1] if "### 缺失字段" in markdown else ""
    assert "法定代表人/单位负责人" not in missing_section


def test_classifier_recognizes_account_permit() -> None:
    assert classify("基本账户开户许可证\n开户银行 上海银行\n账号 123456789\n基本存款账户编号 J123456789") == "account_permit"


def test_classifier_recognizes_basic_account_info() -> None:
    assert classify("基本存款账户信息\n存款人名称 上海示例科技有限公司\n账号 123456789\n开户银行 上海银行") == "basic_account_info"


def test_account_profile_enters_bank_account() -> None:
    profile = build_profile([kyc_extraction({
        "bank_account_name": "上海示例科技有限公司",
        "bank_account_number": "03005029359",
        "opening_bank": "上海银行股份有限公司浦西支行",
        "account_type": "基本存款账户",
        "basic_account_number": "J2900405373694",
        "legal_representative": "梁云",
        "issue_date": "2022-08-10",
    })])

    bank = profile["bank_account"]
    assert bank["account_name"] == "上海示例科技有限公司"
    assert bank["account_number"] == "03005029359"
    assert bank["opening_bank"] == "上海银行股份有限公司浦西支行"
    assert bank["basic_account_number"] == "J2900405373694"
    assert bank["legal_representative"] == "梁云"


def test_account_renderer_markdown_is_chinese_not_json() -> None:
    result = extract(SAMPLE_TEXT, declared_doc_type="account_permit")
    markdown = render_markdown(result)

    assert "## 开户许可证/基本存款账户信息" in markdown
    assert "账号：03005029359" in markdown
    assert "开户银行：上海银行股份有限公司浦西支行" in markdown
    assert "法定代表人/单位负责人：梁云" in markdown
    assert "基本存款账户编号：J2900405373694" in markdown
    assert "```json" not in markdown
    assert "bank_account_number" not in markdown
