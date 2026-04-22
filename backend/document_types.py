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
        code="enterprise_credit",
        name="企业征信",
        storage_label="企业征信提取",
        formats=("pdf", "image"),
        aliases=("企业征信", "企业征信提取", "企业信用报告"),
    ),
    DocumentTypeDefinition(
        code="personal_credit",
        name="个人征信",
        storage_label="个人征信提取",
        formats=("pdf", "image"),
        aliases=("个人征信", "个人征信提取", "个人信用报告"),
        customer_scope="personal",
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
        aliases=("个人流水", "个人流水提取"),
        customer_scope="personal",
    ),
    DocumentTypeDefinition(
        code="financial_data",
        name="财务数据",
        storage_label="财务数据提取",
        formats=("pdf", "xlsx", "image"),
        aliases=("财务数据", "财务数据提取", "财务报表"),
    ),
    DocumentTypeDefinition(
        code="collateral",
        name="抵押物信息",
        storage_label="抵押物信息提取",
        formats=("pdf", "image"),
        aliases=("抵押物信息", "抵押物信息提取"),
    ),
    DocumentTypeDefinition(
        code="jellyfish_report",
        name="水母报告",
        storage_label="水母报告提取",
        formats=("pdf", "image"),
        aliases=("水母报告", "水母报告提取"),
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
        aliases=("结婚证", "婚姻登记证"),
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
        name="产调",
        storage_label="产调",
        formats=("pdf", "docx", "image"),
        aliases=("产调", "不动产登记信息", "不动产产调", "房产调查"),
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
        aliases=("营业执照", "营业执照正副本", "营业执照副本"),
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
        aliases=("银行对账单", "对账单", "银行账单"),
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
    return _ALIASES_TO_CODE.get(str(value).strip().lower())


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
