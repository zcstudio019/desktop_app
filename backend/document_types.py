"""Canonical document-type registry used by upload/extraction/storage flows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentTypeDefinition:
    code: str
    name: str
    storage_label: str
    formats: tuple[str, ...]
    aliases: tuple[str, ...]
    customer_scope: str = "enterprise"
    store_original: bool = True
    store_markdown: bool = True


DOCUMENT_TYPE_DEFINITIONS: tuple[DocumentTypeDefinition, ...] = (
    DocumentTypeDefinition(
        code="enterprise_credit_report",
        name="企业征信",
        storage_label="企业征信提取",
        formats=("pdf", "image"),
        aliases=("企业征信", "企业征信提取", "企业信用报告", "企业征信报告", "enterprise_credit", "enterprise_credit_report"),
    ),
    DocumentTypeDefinition(
        code="personal_credit_report",
        name="个人征信",
        storage_label="个人征信提取",
        formats=("pdf", "image"),
        aliases=("个人征信", "个人征信提取", "个人信用报告", "个人征信报告", "personal_credit_report"),
        customer_scope="personal",
    ),
    DocumentTypeDefinition(
        code="enterprise_bank_statement",
        name="企业银行流水",
        storage_label="企业银行流水解析",
        formats=("pdf", "xlsx", "image"),
        aliases=("企业银行流水", "企业流水", "对公流水", "enterprise_bank_statement", "bank_statement_enterprise", "company_bank_statement"),
    ),
    DocumentTypeDefinition(
        code="enterprise_flow",
        name="企业流水",
        storage_label="企业流水提取",
        formats=("pdf", "xlsx", "image"),
        aliases=("企业流水", "企业流水提取", "对公流水"),
    ),
    DocumentTypeDefinition(
        code="personal_flow",
        name="个人流水",
        storage_label="个人流水提取",
        formats=("pdf", "xlsx", "image"),
        aliases=("个人流水", "个人流水提取", "personal_flow", "personal_bank_statement", "bank_statement_personal", "individual_bank_statement", "个人银行流水"),
        customer_scope="personal",
    ),
    DocumentTypeDefinition(
        code="financial_report",
        name="财务报表",
        storage_label="财务报表提取",
        formats=("pdf", "xlsx", "image"),
        aliases=("财务报表", "财务报表提取", "财务数据", "财务数据提取", "financial_data", "financial_report"),
    ),
    DocumentTypeDefinition(
        code="collateral",
        name="房产证",
        storage_label="房产证",
        formats=("pdf", "image"),
        aliases=("房产证", "不动产权证", "抵押物信息"),
    ),
    DocumentTypeDefinition(
        code="jellyfish_report",
        name="水母报告",
        storage_label="水母报告提取",
        formats=("pdf", "image"),
        aliases=("水母报告", "水母报告提取"),
    ),
    DocumentTypeDefinition(
        code="shuimui_report",
        name="水母报告",
        storage_label="水母报告",
        formats=("url",),
        aliases=("水母报告", "水母报告提取", "shuimui_report"),
        customer_scope="enterprise",
        store_original=False,
        store_markdown=True,
    ),
    DocumentTypeDefinition(
        code="personal_tax",
        name="个人纳税/公积金",
        storage_label="个人纳税/公积金提取",
        formats=("pdf", "xlsx", "image"),
        aliases=("个人纳税", "公积金", "个人纳税/公积金"),
        customer_scope="personal",
    ),
    DocumentTypeDefinition(
        code="contract",
        name="合同",
        storage_label="合同",
        formats=("pdf", "docx"),
        aliases=("合同", "借款合同", "采购合同", "销售合同"),
    ),
    DocumentTypeDefinition(
        code="id_card",
        name="身份证",
        storage_label="身份证",
        formats=("pdf", "docx", "image"),
        aliases=("身份证", "居民身份证"),
        customer_scope="personal",
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="marriage_cert",
        name="结婚证",
        storage_label="结婚证",
        formats=("pdf", "docx", "image"),
        aliases=("结婚证", "婚姻登记证", "marriage_certificate"),
        customer_scope="personal",
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="marriage_certificate",
        name="结婚证",
        storage_label="结婚证",
        formats=("pdf", "docx", "image"),
        aliases=("结婚证", "婚姻登记证", "marriage_cert"),
        customer_scope="personal",
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="hukou",
        name="户口本",
        storage_label="户口本",
        formats=("pdf", "docx", "image"),
        aliases=("户口本", "户籍证明"),
        customer_scope="personal",
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="property_report",
        name="房产证",
        storage_label="房产证",
        formats=("pdf", "docx", "image"),
        aliases=("房产证", "不动产权证", "产调", "不动产登记信息", "不动产产调", "房产调查"),
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="vehicle_license",
        name="行驶证",
        storage_label="行驶证",
        formats=("pdf", "docx", "image"),
        aliases=("行驶证", "机动车行驶证"),
        customer_scope="personal",
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="business_license",
        name="营业执照正副本",
        storage_label="营业执照",
        formats=("pdf", "docx", "image"),
        aliases=("营业执照", "营业执照正副本", "营业执照副本", "licence", "license", "company_license"),
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="account_license",
        name="开户许可证",
        storage_label="开户许可证",
        formats=("pdf", "docx", "image"),
        aliases=("开户许可证", "开户许可证书"),
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="special_license",
        name="特殊许可证",
        storage_label="特殊许可证",
        formats=("pdf", "docx", "image"),
        aliases=("特殊许可证", "专项许可证", "经营许可证", "行业许可证"),
        store_original=True,
    ),
    DocumentTypeDefinition(
        code="company_articles",
        name="公司章程",
        storage_label="公司章程",
        formats=("pdf", "docx"),
        aliases=("公司章程", "章程"),
        store_original=True,
        store_markdown=True,
    ),
    DocumentTypeDefinition(
        code="bank_statement",
        name="银行对账单",
        storage_label="银行对账单",
        formats=("pdf", "xlsx"),
        aliases=("银行对账单", "银行账户明细", "账户明细清单", "银行流水明细", "中国工商银行账户明细清单", "对账单", "银行账单", "statement", "bank statement"),
        store_original=True,
        store_markdown=True,
    ),
    DocumentTypeDefinition(
        code="bank_receipt_bundle",
        name="银行回单集合",
        storage_label="银行回单集合",
        formats=("pdf", "image"),
        aliases=("银行回单集合", "银行回单", "电子回单", "汇款回单", "转账回单", "付款凭证", "收款凭证", "bank_receipt_bundle"),
        store_original=True,
        store_markdown=True,
    ),
    DocumentTypeDefinition(
        code="bank_statement_detail",
        name="银行对账明细",
        storage_label="银行对账明细",
        formats=("pdf", "xlsx"),
        aliases=("银行对账明细", "对账明细", "银行明细"),
        store_original=True,
        store_markdown=True,
    ),
)

DOCUMENT_TYPES_BY_CODE = {item.code: item for item in DOCUMENT_TYPE_DEFINITIONS}

DOCUMENT_TYPE_CANONICAL_ALIASES = {
    "bank_statement": "bank_statement",
    "bank_receipt_bundle": "bank_receipt_bundle",
    "银行回单集合": "bank_receipt_bundle",
    "银行回单": "bank_receipt_bundle",
    "电子回单": "bank_receipt_bundle",
    "汇款回单": "bank_receipt_bundle",
    "转账回单": "bank_receipt_bundle",
    "付款凭证": "bank_receipt_bundle",
    "收款凭证": "bank_receipt_bundle",
    "bank statement": "bank_statement",
    "statement": "bank_statement",
    "银行对账单": "bank_statement",
    "银行账户明细": "bank_statement",
    "账户明细清单": "bank_statement",
    "银行流水明细": "bank_statement",
    "中国工商银行账户明细清单": "bank_statement",
    "对账单": "bank_statement",
    "financial_data": "financial_report",
    "financial_report": "financial_report",
    "财务数据": "financial_report",
    "财务数据提取": "financial_report",
    "财务报表": "financial_report",
    "财务报表提取": "financial_report",
    "personal_credit": "personal_credit_report",
    "personal_credit_report": "personal_credit_report",
    "个人征信": "personal_credit_report",
    "个人征信报告": "personal_credit_report",
    "个人信用报告": "personal_credit_report",
    "个人征信提取": "personal_credit_report",
    "enterprise_credit": "enterprise_credit_report",
    "enterprise_credit_report": "enterprise_credit_report",
    "企业征信": "enterprise_credit_report",
    "企业征信报告": "enterprise_credit_report",
    "企业信用报告": "enterprise_credit_report",
    "企业征信提取": "enterprise_credit_report",
    "enterprise_flow": "enterprise_flow",
    "company_articles": "company_articles",
    "articles": "company_articles",
    "articles of association": "company_articles",
    "公司章程": "company_articles",
    "章程": "company_articles",
    "shuimui_report": "shuimui_report",
    "水母报告": "shuimui_report",
    "水母报告提取": "shuimui_report",
    "enterprise_bank_statement": "enterprise_bank_statement",
    "bank_statement_enterprise": "enterprise_flow",
    "company_bank_statement": "enterprise_flow",
    "企业银行流水": "enterprise_flow",
    "企业流水": "enterprise_flow",
    "银行流水": "enterprise_flow",
    "对公流水": "enterprise_flow",
    "personal_flow": "personal_flow",
    "personal_bank_statement": "personal_flow",
    "bank_statement_personal": "personal_flow",
    "individual_bank_statement": "personal_flow",
    "个人流水": "personal_flow",
    "个人银行流水": "personal_flow",
}

_ALIASES_TO_CODE: dict[str, str] = {}
for item in DOCUMENT_TYPE_DEFINITIONS:
    _ALIASES_TO_CODE[item.code.lower()] = item.code
    _ALIASES_TO_CODE[item.name.lower()] = item.code
    _ALIASES_TO_CODE[item.storage_label.lower()] = item.code
    for alias in item.aliases:
        _ALIASES_TO_CODE[alias.lower()] = item.code


def get_document_type_definition(code: str | None) -> DocumentTypeDefinition | None:
    if not code:
        return None
    normalized = normalize_document_type_code(code)
    if not normalized:
        return None
    return DOCUMENT_TYPES_BY_CODE.get(normalized)


def normalize_document_type_code(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if raw in DOCUMENT_TYPE_CANONICAL_ALIASES:
        return DOCUMENT_TYPE_CANONICAL_ALIASES[raw]
    lowered = raw.lower()
    if lowered in DOCUMENT_TYPE_CANONICAL_ALIASES:
        return DOCUMENT_TYPE_CANONICAL_ALIASES[lowered]
    return _ALIASES_TO_CODE.get(lowered) or raw


def get_document_storage_label(value: str | None) -> str:
    definition = get_document_type_definition(value)
    if not definition:
        return str(value or "").strip()
    return definition.storage_label


def get_document_display_name(value: str | None) -> str:
    definition = get_document_type_definition(value)
    if not definition:
        return str(value or "").strip()
    return definition.name


def should_store_original(value: str | None) -> bool:
    definition = get_document_type_definition(value)
    if not definition:
        return True
    return definition.store_original


def should_store_markdown(value: str | None) -> bool:
    definition = get_document_type_definition(value)
    if not definition:
        return True
    return definition.store_markdown


def should_append_same_type_document(value: str | None) -> bool:
    """Return True for document types that can have multiple active uploads per customer."""
    normalized = normalize_document_type_code(value) or str(value or "").strip()
    return normalized in {
        "id_card",
        "enterprise_flow",
        "personal_flow",
        "personal_bank_statement",
        "bank_statement_personal",
        "individual_bank_statement",
        "个人流水",
        "个人银行流水",
        "bank_statement",
        "bank_receipt_bundle",
        "enterprise_bank_statement",
        "bank_statement_enterprise",
        "company_bank_statement",
        "企业流水",
        "银行流水",
        "enterprise_credit_report",
        "shuimui_report",
        "financial_report",
        "company_articles",
        "property_cert",
        "real_estate_cert",
        "collateral",
        "property_report",
        "mortgage_info",
    }
