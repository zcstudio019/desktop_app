"""SQLAlchemy table definitions for deployment and database initialization."""

from __future__ import annotations

from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, event, func, inspect, select
from sqlalchemy.dialects.mysql import LONGTEXT

from .database import Base


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(64), unique=True, nullable=False, index=True)
    name = Column(String(255))
    phone = Column(String(50))
    id_card = Column(String(100))
    loan_amount = Column(Float)
    loan_purpose = Column(String(255))
    income_source = Column(String(255))
    monthly_income = Column(Float)
    credit_score = Column(Integer)
    status = Column(String(50), default="new", index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    uploader = Column(String(255), default="")
    upload_time = Column(String(50), default="")
    customer_type = Column(String(20), default="enterprise")


class FinancingRequirement(Base):
    __tablename__ = "financing_requirements"
    __table_args__ = (UniqueConstraint("customer_id", "version", name="uq_financing_requirement_customer_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    requirement_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, index=True)
    borrower_entity = Column(String(255))
    requested_amount = Column(Numeric(18, 2))
    currency = Column(String(8), default="CNY")
    amount_confirmed = Column(Integer, default=0)
    financing_purpose = Column(String(64))
    purpose_detail = Column(Text)
    term_value = Column(Integer)
    term_unit = Column(String(16))
    term_confirmed = Column(Integer, default=0)
    term_original = Column(String(100))
    expected_funding_date = Column(String(50))
    repayment_preference = Column(String(100))
    guarantee_preference_json = Column(Text, default="[]")
    collateral_available_json = Column(Text, default="[]")
    registered_region = Column(String(255))
    operating_region = Column(String(255))
    existing_banks_json = Column(Text, default="[]")
    preferred_banks_json = Column(Text, default="[]")
    excluded_banks_json = Column(Text, default="[]")
    accept_additional_guarantee = Column(Integer)
    accept_mortgage = Column(Integer)
    accept_refinancing = Column(Integer)
    field_sources_json = Column(Text, default="{}")
    draft_source = Column(String(32), nullable=True, index=True)
    created_by = Column(String(128))
    confirmed_by = Column(String(128))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    confirmed_at = Column(DateTime)


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    file_name = Column(String(255))
    file_path = Column(String(512))
    file_type = Column(String(50))
    file_size = Column(Integer)
    upload_time = Column(DateTime, server_default=func.now(), nullable=False)
    feishu_file_id = Column(String(255))
    uploader = Column(String(255), default="")
    file_hash = Column(String(128), default="")
    is_active = Column(Integer, default=1)
    archived_at = Column(DateTime, nullable=True)
    replaced_by_document_id = Column(String(64), default="")
    version_policy = Column(String(50), default="")
    report_date = Column(String(64), default="")
    valid_until = Column(String(64), default="")


class Extraction(Base):
    __tablename__ = "extractions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_id = Column(String(64), unique=True, nullable=False, index=True)
    doc_id = Column(String(64), nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    extraction_type = Column(String(50))
    extracted_data = Column(LONGTEXT().with_variant(Text(), "sqlite"))
    confidence = Column(Float)
    extraction_status = Column(String(32), default="success")
    extraction_error = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    skill_name = Column(String(100), default="")
    skill_version = Column(String(50), default="")
    schema_version = Column(String(100), default="")
    confirmed_data = Column(LONGTEXT().with_variant(Text(), "sqlite"), default="{}")
    confirm_status = Column(String(32), default="unconfirmed")
    confirmed_by = Column(String(128), default="")
    confirmed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class CustomerFlowRule(Base):
    __tablename__ = "customer_flow_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(64), unique=True, nullable=False, index=True)
    related_company_names_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    self_account_numbers_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    internal_transfer_keywords_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    operating_counterparty_whitelist_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    internal_counterparty_blacklist_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    personal_counterparty_names_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    manual_overrides_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    updated_by = Column(String(128), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class IncomeConfirmationOverride(Base):
    __tablename__ = "income_confirmation_overrides"
    __table_args__ = (
        UniqueConstraint("document_id", "counterparty_name", "income_type", name="uq_income_confirmation_source"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(64), nullable=False, index=True)
    document_id = Column(String(64), nullable=False, index=True)
    source_type = Column(String(50), nullable=False, default="personal_flow")
    income_type = Column(String(50), nullable=False, default="suspected_salary")
    target_type = Column(String(50), nullable=False, default="confirmed_salary")
    counterparty_name = Column(String(255), nullable=False)
    amount = Column(Float, default=0.0)
    months_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    transaction_ids_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    manual_status = Column(String(32), nullable=False, default="pending")
    reason = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    confirmed_by = Column(String(128), default="")
    confirmed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(64), unique=True, nullable=False, index=True)
    title = Column(String(255), default="")
    markdown_content = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    source_mode = Column(String(20), default="auto")
    source_snapshot_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    rag_source_priority_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    risk_report_schema_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    version = Column(Integer, default=1)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerSchemeSnapshot(Base):
    __tablename__ = "customer_scheme_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    customer_name = Column(String(255), default="")
    summary_markdown = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    raw_result = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    source = Column(String(50), default="manual")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerDocumentChunk(Base):
    __tablename__ = "customer_document_chunks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chunk_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    source_type = Column(String(50), nullable=False)
    source_id = Column(String(64), default="")
    chunk_index = Column(Integer, default=0)
    chunk_text = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    embedding_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    metadata_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerRiskReport(Base):
    __tablename__ = "customer_risk_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    profile_version = Column(Integer, default=1)
    profile_updated_at = Column(String(64), default="")
    generated_at = Column(String(64), nullable=False, index=True)
    report_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    report_markdown = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class CustomerFinancingDiagnosticReportSnapshot(Base):
    __tablename__ = "customer_financing_diagnostic_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    report_version = Column(String(32), nullable=False, index=True)
    report_status = Column(String(32), default="draft")
    report_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    report_markdown = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    source_summary = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    generated_by = Column(String(128), default="")
    generated_at = Column(String(64), nullable=False, index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(64), unique=True, nullable=False, index=True)
    username = Column(String(128), default="", nullable=False, index=True)
    customer_id = Column(String(64), default="", index=True)
    customer_name = Column(String(255), default="")
    title = Column(String(255), default="")
    last_message_preview = Column(String(512), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class ChatMessageRecord(Base):
    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(64), unique=True, nullable=False, index=True)
    session_id = Column(String(64), nullable=False, index=True)
    role = Column(String(32), nullable=False, index=True)
    content = Column(Text().with_variant(LONGTEXT(), "mysql"), default="", nullable=False)
    sequence = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ProductCacheEntry(Base):
    __tablename__ = "product_cache_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cache_key = Column(String(64), unique=True, nullable=False, index=True)
    content = Column(Text().with_variant(LONGTEXT(), "mysql"), default="", nullable=False)
    last_updated = Column(String(64), default="", nullable=False, index=True)
    source = Column(String(64), default="wiki", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingProduct(Base):
    __tablename__ = "financing_products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(64), unique=True, nullable=False, index=True)
    identity_key = Column(String(64), unique=True, nullable=False, index=True)
    external_product_code = Column(String(64), unique=True, nullable=True, index=True)
    institution_name = Column(String(255), nullable=False)
    product_name = Column(String(255), nullable=False)
    product_category = Column(String(32), nullable=False, index=True)
    region_key = Column(String(255), default="", nullable=False)
    source_type = Column(String(32), nullable=False, default="feishu_wiki")
    source_ref = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingProductVersion(Base):
    __tablename__ = "financing_product_versions"
    __table_args__ = (UniqueConstraint("product_id", "version_number", name="uq_financing_product_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(String(64), unique=True, nullable=False, index=True)
    product_id = Column(String(64), ForeignKey("financing_products.product_id"), nullable=False, index=True)
    version_number = Column(Integer, nullable=False)
    institution_name = Column(String(255), default="", server_default="", nullable=False)
    product_name = Column(String(255), default="", server_default="", nullable=False)
    status = Column(String(32), nullable=False, default="draft", index=True)
    effective_from = Column(Date)
    effective_to = Column(Date)
    source_snapshot = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    source_snapshot_hash = Column(String(64), nullable=False, index=True)
    source_type = Column(String(32), nullable=False, default="feishu_wiki", server_default="feishu_wiki")
    source_file = Column(String(255), default="", nullable=False, server_default="")
    source_refs_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="[]")
    conflict_code = Column(String(64), nullable=True, index=True)
    conflict_resolution_hash = Column(String(64), nullable=True)
    source_update_date = Column(Date)
    source_node_token = Column(String(128), nullable=False)
    source_document_token = Column(String(128), nullable=False)
    source_imported_at = Column(DateTime, nullable=False)
    source_updated_at = Column(DateTime)
    summary = Column(Text, default="")
    raw_fields_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    needs_review = Column(Integer, default=1, server_default="1", nullable=False)
    review_status = Column(String(16), default="unreviewed", server_default="unreviewed", nullable=False)
    review_reasons_json = Column(Text, default="[]")
    loan_type = Column(String(255), default="")
    guarantee_type = Column(String(255), default="")
    region_scope_json = Column(Text, default="[]")
    currency = Column(String(8), default="CNY")
    min_amount = Column(Numeric(18, 2))
    max_amount = Column(Numeric(18, 2))
    min_term_months = Column(Integer)
    max_term_months = Column(Integer)
    company_age_months = Column(Integer)
    borrower_age_min = Column(Integer)
    borrower_age_max = Column(Integer)
    tax_grade = Column(String(255), default="")
    revenue_requirement = Column(Text, default="")
    tax_requirement = Column(Text, default="")
    invoice_requirement = Column(Text, default="")
    credit_overdue_requirement = Column(Text, default="")
    credit_query_requirement = Column(Text, default="")
    debt_requirement = Column(Text, default="")
    collateral_requirement = Column(Text, default="")
    suitable_customer_text = Column(Text, default="")
    repayment_methods_json = Column(Text, default="[]")
    guarantee_modes_json = Column(Text, default="[]")
    collateral_types_json = Column(Text, default="[]")
    materials_json = Column(Text, default="[]")
    rate_text = Column(String(255), default="")
    notes = Column(Text, default="")
    field_review_json = Column(Text, default="{}")
    created_by = Column(String(128), nullable=False)
    published_by = Column(String(128), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    published_at = Column(DateTime)


class FinancingProductRule(Base):
    __tablename__ = "financing_product_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(String(64), unique=True, nullable=False, index=True)
    version_id = Column(String(64), ForeignKey("financing_product_versions.version_id"), nullable=False, index=True)
    rule_group = Column(String(64), nullable=False, default="eligibility")
    field_name = Column(String(128), nullable=False)
    operator = Column(String(16), nullable=False)
    expected_value_json = Column(Text, nullable=False)
    severity = Column(String(16), nullable=False)
    failure_action = Column(String(16), nullable=False)
    message = Column(Text, default="")
    source_text = Column(Text, default="")
    sort_order = Column(Integer, default=0)


class ProductMatchSnapshot(Base):
    __tablename__ = "product_match_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(64), unique=True, nullable=False, index=True)
    context_hash = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    requirement_version = Column(Integer, nullable=False)
    facts_snapshot_id = Column(String(64), nullable=True)
    facts_hash = Column(String(64), nullable=False, index=True)
    facts_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    catalog_as_of_date = Column(Date, nullable=False)
    catalog_version_hash = Column(String(64), nullable=False, index=True)
    generated_at = Column(DateTime, nullable=False)
    generated_by = Column(String(128), nullable=False, default="")
    eligible_count = Column(Integer, nullable=False, default=0)
    conditional_count = Column(Integer, nullable=False, default=0)
    ineligible_count = Column(Integer, nullable=False, default=0)
    manual_review_count = Column(Integer, nullable=False, default=0)
    configuration_error_count = Column(Integer, nullable=False, default=0)
    summary_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ProductMatchItem(Base):
    __tablename__ = "product_match_items"
    __table_args__ = (UniqueConstraint("snapshot_id", "version_id", name="uq_product_match_snapshot_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(64), ForeignKey("product_match_snapshots.snapshot_id"), nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    version_id = Column(String(64), nullable=False, index=True)
    external_product_code = Column(String(64), nullable=False, default="")
    institution_name = Column(String(255), nullable=False, default="")
    product_name = Column(String(255), nullable=False, default="")
    product_category = Column(String(32), nullable=False, default="")
    max_amount = Column(Numeric(18, 2), nullable=True)
    max_term_months = Column(Integer, nullable=True)
    overall_status = Column(String(40), nullable=False, index=True)
    hard_fail_count = Column(Integer, nullable=False, default=0)
    hard_unknown_count = Column(Integer, nullable=False, default=0)
    soft_fail_count = Column(Integer, nullable=False, default=0)
    review_count = Column(Integer, nullable=False, default=0)
    blocking_reasons_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    missing_information_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    review_reasons_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    soft_gaps_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    rule_results_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingPlan(Base):
    __tablename__ = "financing_plans"

    id = Column(Integer, primary_key=True, autoincrement=True)
    financing_plan_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    requirement_version = Column(Integer, nullable=False)
    source_match_snapshot_id = Column(String(64), ForeignKey("product_match_snapshots.snapshot_id"), nullable=False, index=True)
    current_version_id = Column(String(64), nullable=True, index=True)
    status = Column(String(32), nullable=False, default="draft", index=True)
    created_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingPlanVersion(Base):
    __tablename__ = "financing_plan_versions"
    __table_args__ = (UniqueConstraint("financing_plan_id", "version_no", name="uq_financing_plan_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_version_id = Column(String(64), unique=True, nullable=False, index=True)
    financing_plan_id = Column(String(64), ForeignKey("financing_plans.financing_plan_id"), nullable=False, index=True)
    version_no = Column(Integer, nullable=False)
    plan_type = Column(String(24), nullable=False)
    target_amount = Column(Numeric(18, 2), nullable=False)
    covered_amount = Column(Numeric(18, 2), nullable=False)
    funding_gap = Column(Numeric(18, 2), nullable=False)
    currency = Column(String(8), nullable=False, default="CNY")
    summary = Column(Text, nullable=False, default="")
    rationale = Column(Text, nullable=False, default="")
    status = Column(String(32), nullable=False, default="draft", index=True)
    generation_status = Column(String(40), nullable=False, index=True)
    required_actions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    missing_information_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    conditions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    source_requirement_version = Column(Integer, nullable=False)
    source_facts_hash = Column(String(64), nullable=False, index=True)
    source_match_snapshot_id = Column(String(64), ForeignKey("product_match_snapshots.snapshot_id"), nullable=False, index=True)
    source_catalog_hash = Column(String(64), nullable=False, index=True)
    created_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    confirmed_by = Column(String(128), nullable=True)
    confirmed_at = Column(DateTime, nullable=True)


class FinancingPlanItem(Base):
    __tablename__ = "financing_plan_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_item_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    product_version_id = Column(String(64), nullable=False, index=True)
    product_match_item_id = Column(Integer, ForeignKey("product_match_items.id"), nullable=False, index=True)
    external_product_code = Column(String(64), nullable=False, default="")
    institution_name = Column(String(255), nullable=False, default="")
    product_name = Column(String(255), nullable=False, default="")
    proposed_amount = Column(Numeric(18, 2), nullable=False)
    proposed_term_months = Column(Integer, nullable=False)
    match_status = Column(String(40), nullable=False)
    item_role = Column(String(24), nullable=False)
    sequence_no = Column(Integer, nullable=False, default=0)
    reason = Column(Text, nullable=False, default="")
    notes = Column(Text, nullable=False, default="")
    conditions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    risks_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    manual_approved_by = Column(String(128), nullable=True)
    manual_approved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingPlanGap(Base):
    __tablename__ = "financing_plan_gaps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gap_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=False, index=True)
    gap_type = Column(String(32), nullable=False)
    description = Column(Text, nullable=False)
    related_product_id = Column(String(64), nullable=True)
    related_fact_field = Column(String(128), nullable=True)
    severity = Column(String(16), nullable=False, default="info")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ManualCandidateOverride(Base):
    __tablename__ = "manual_candidate_overrides"

    id = Column(Integer, primary_key=True, autoincrement=True)
    override_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    match_snapshot_id = Column(String(64), ForeignKey("product_match_snapshots.snapshot_id"), nullable=False, index=True)
    product_match_item_id = Column(Integer, ForeignKey("product_match_items.id"), nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    product_version_id = Column(String(64), nullable=False, index=True)
    previous_status = Column(String(32), nullable=False)
    new_status = Column(String(32), nullable=False)
    operator_id = Column(String(128), nullable=False)
    operator_name = Column(String(255), nullable=False)
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingPlanCondition(Base):
    __tablename__ = "financing_plan_conditions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    condition_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=False, index=True)
    plan_item_id = Column(String(64), ForeignKey("financing_plan_items.plan_item_id"), nullable=True, index=True)
    condition_type = Column(String(32), nullable=False, default="other")
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False, default="")
    source_type = Column(String(64), nullable=False)
    source_rule_id = Column(String(64), nullable=True)
    source_fact_field = Column(String(128), nullable=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    required = Column(Integer, nullable=False, default=1)
    sort_order = Column(Integer, nullable=False, default=0)
    updated_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingPlanMaterial(Base):
    __tablename__ = "financing_plan_materials"
    __table_args__ = (UniqueConstraint("plan_version_id", "material_code", name="uq_plan_version_material"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    material_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=False, index=True)
    plan_item_id = Column(String(64), ForeignKey("financing_plan_items.plan_item_id"), nullable=True, index=True)
    material_code = Column(String(128), nullable=False)
    material_name = Column(String(255), nullable=False)
    material_category = Column(String(64), nullable=False, default="other")
    required = Column(Integer, nullable=False, default=1)
    status = Column(String(24), nullable=False, default="missing", index=True)
    source_type = Column(String(64), nullable=False)
    source_product_rule_id = Column(String(64), nullable=True)
    notes = Column(Text, nullable=False, default="")
    required_by_products_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    updated_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingPlanExplanation(Base):
    __tablename__ = "financing_plan_explanations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    explanation_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), unique=True, nullable=False, index=True)
    plan_summary = Column(Text, nullable=False, default="")
    coverage_summary = Column(Text, nullable=False, default="")
    product_structure_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    key_conditions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    funding_gap_summary = Column(Text, nullable=False, default="")
    risk_notes_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    next_actions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    generated_by = Column(String(32), nullable=False, default="template")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingPlanSelection(Base):
    __tablename__ = "financing_plan_selections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    selection_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    primary_plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=True, index=True)
    backup_plan_version_ids_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    conditional_plan_version_ids_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    status = Column(String(24), nullable=False, default="draft", index=True)
    selected_by = Column(String(128), nullable=False, default="")
    selected_at = Column(DateTime, nullable=False)
    finalized_by = Column(String(128), nullable=True)
    finalized_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingPlanReportSnapshot(Base):
    __tablename__ = "financing_plan_report_snapshots"
    __table_args__ = (UniqueConstraint("plan_selection_id", "report_type", "report_version", name="uq_plan_report_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(String(64), unique=True, nullable=False, index=True)
    plan_selection_id = Column(String(64), ForeignKey("financing_plan_selections.selection_id"), nullable=False, index=True)
    primary_plan_version_id = Column(String(64), nullable=True, index=True)
    report_type = Column(String(16), nullable=False, index=True)
    report_version = Column(Integer, nullable=False)
    structured_payload_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    rendered_html = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    source_requirement_version = Column(Integer, nullable=False)
    source_match_snapshot_id = Column(String(64), nullable=False, index=True)
    source_facts_hash = Column(String(64), nullable=False, index=True)
    source_catalog_hash = Column(String(64), nullable=False, index=True)
    generated_by = Column(String(128), nullable=False, default="")
    generated_at = Column(DateTime, nullable=False)


class FinancingApplication(Base):
    __tablename__ = "financing_applications"
    # MySQL must see the referenced business key as unique while the table is
    # being created; an index emitted after CREATE TABLE is too late for the
    # self-referencing parent_application_id foreign key.
    __table_args__ = (
        UniqueConstraint("application_id", name="uq_financing_application_application_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    application_id = Column(String(64), nullable=False)
    parent_application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=True, index=True)
    attempt_no = Column(Integer, nullable=False, default=1)
    customer_id = Column(String(128), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    requirement_version = Column(Integer, nullable=False)
    plan_id = Column(String(64), ForeignKey("financing_plans.financing_plan_id"), nullable=False, index=True)
    plan_version_id = Column(String(64), ForeignKey("financing_plan_versions.plan_version_id"), nullable=False, index=True)
    plan_item_id = Column(String(64), ForeignKey("financing_plan_items.plan_item_id"), nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    product_version_id = Column(String(64), nullable=False, index=True)
    external_product_code = Column(String(64), nullable=False, default="")
    institution_name = Column(String(255), nullable=False, default="")
    product_name = Column(String(255), nullable=False, default="")
    application_no = Column(String(64), unique=True, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="draft", index=True)
    target_amount = Column(Numeric(18, 2), nullable=False)
    submitted_amount = Column(Numeric(18, 2), nullable=True)
    approved_amount = Column(Numeric(18, 2), nullable=True)
    disbursed_amount = Column(Numeric(18, 2), nullable=True)
    target_term_months = Column(Integer, nullable=False)
    approved_term_months = Column(Integer, nullable=True)
    target_interest_rate = Column(Numeric(10, 6), nullable=True)
    approved_interest_rate = Column(Numeric(10, 6), nullable=True)
    responsible_user_id = Column(String(128), nullable=True)
    responsible_user_name = Column(String(255), nullable=True)
    current_stage_code = Column(String(40), nullable=False, default="01_material_preparation")
    submission_channel = Column(String(64), nullable=True)
    submission_reference = Column(String(128), nullable=True)
    submission_notes = Column(Text, nullable=False, default="")
    approval_reference = Column(String(128), nullable=True)
    rejection_reason = Column(Text, nullable=False, default="")
    rejection_code = Column(String(64), nullable=True)
    disbursement_reference = Column(String(128), nullable=True)
    final_result = Column(String(64), nullable=True)
    submitted_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    rejected_at = Column(DateTime, nullable=True)
    disbursed_at = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)
    created_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationStage(Base):
    __tablename__ = "financing_application_stages"
    __table_args__ = (UniqueConstraint("application_id", "stage_code", name="uq_application_stage_code"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    stage_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    stage_code = Column(String(40), nullable=False)
    stage_name = Column(String(64), nullable=False)
    status = Column(String(24), nullable=False, default="pending", index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    entered_by = Column(String(128), nullable=True)
    completed_by = Column(String(128), nullable=True)
    notes = Column(Text, nullable=False, default="")
    sequence_no = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingApplicationTask(Base):
    __tablename__ = "financing_application_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    stage_id = Column(String(64), ForeignKey("financing_application_stages.stage_id"), nullable=False, index=True)
    task_type = Column(String(40), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False, default="")
    status = Column(String(24), nullable=False, default="todo", index=True)
    priority = Column(String(16), nullable=False, default="normal", index=True)
    assignee_user_id = Column(String(128), nullable=True)
    assignee_user_name = Column(String(255), nullable=True)
    due_date = Column(Date, nullable=True)
    source_type = Column(String(40), nullable=False)
    source_ref = Column(String(64), nullable=True, index=True)
    related_material_id = Column(String(64), nullable=True, index=True)
    related_condition_id = Column(String(64), nullable=True, index=True)
    required = Column(Integer, nullable=False, default=1)
    completed_at = Column(DateTime, nullable=True)
    completed_by = Column(String(128), nullable=True)
    notes = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationEvent(Base):
    __tablename__ = "financing_application_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    event_type = Column(String(40), nullable=False, index=True)
    from_status = Column(String(32), nullable=True)
    to_status = Column(String(32), nullable=True)
    operator_id = Column(String(128), nullable=False)
    operator_name = Column(String(255), nullable=False)
    payload_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingSupplementRequest(Base):
    __tablename__ = "financing_supplement_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    supplement_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    request_no = Column(String(64), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=False)
    requested_by_bank = Column(String(255), nullable=True)
    requested_at = Column(DateTime, nullable=False)
    due_date = Column(Date, nullable=True, index=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    completed_at = Column(DateTime, nullable=True)
    completed_by = Column(String(128), nullable=True)
    notes = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationMaterial(Base):
    __tablename__ = "financing_application_materials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    application_material_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    supplement_request_id = Column(String(64), ForeignKey("financing_supplement_requests.supplement_id"), nullable=True, index=True)
    material_type = Column(String(64), nullable=False, default="other", index=True)
    material_name = Column(String(255), nullable=False)
    material_category = Column(String(64), nullable=False, default="other")
    owner_type = Column(String(32), nullable=False, default="enterprise", index=True)
    owner_id = Column(String(128), nullable=True, index=True)
    owner_name = Column(String(255), nullable=True)
    required = Column(Integer, nullable=False, default=1)
    required_verified = Column(Integer, nullable=False, default=0)
    status = Column(String(24), nullable=False, default="required_missing", index=True)
    source_type = Column(String(40), nullable=False)
    source_id = Column(String(64), nullable=True, index=True)
    source_document_id = Column(String(64), nullable=True, index=True)
    customer_material_id = Column(String(64), nullable=True, index=True)
    source_file_id = Column(String(64), nullable=True, index=True)
    file_reference = Column(String(512), nullable=True)
    valid_from = Column(Date, nullable=True)
    valid_to = Column(Date, nullable=True)
    coverage_start = Column(Date, nullable=True)
    coverage_end = Column(Date, nullable=True)
    version_no = Column(Integer, nullable=False, default=1)
    replaces_material_id = Column(String(64), nullable=True, index=True)
    rejection_reason = Column(Text, nullable=False, default="")
    verified_by = Column(String(128), nullable=True)
    verified_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationPackage(Base):
    __tablename__ = "financing_application_packages"
    __table_args__ = (UniqueConstraint("application_id", "package_version", name="uq_application_package_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    package_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    package_version = Column(Integer, nullable=False)
    status = Column(String(24), nullable=False, default="draft", index=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationPackageItem(Base):
    __tablename__ = "financing_application_package_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    package_item_id = Column(String(64), unique=True, nullable=False, index=True)
    package_id = Column(String(64), ForeignKey("financing_application_packages.package_id"), nullable=False, index=True)
    application_material_id = Column(String(64), ForeignKey("financing_application_materials.application_material_id"), nullable=False, index=True)
    customer_material_id = Column(String(64), nullable=True, index=True)
    source_file_id = Column(String(64), nullable=True, index=True)
    material_type = Column(String(64), nullable=False)
    material_name = Column(String(255), nullable=False)
    owner_type = Column(String(32), nullable=False, default="enterprise")
    owner_name = Column(String(255), nullable=True)
    file_name = Column(String(255), nullable=True)
    file_hash = Column(String(128), nullable=False, default="")
    source_file_path = Column(String(512), nullable=True)
    display_name = Column(String(255), nullable=True)
    package_file_name = Column(String(255), nullable=True)
    sort_order = Column(Integer, nullable=False, default=0)
    required = Column(Integer, nullable=False, default=1)
    included = Column(Integer, nullable=False, default=1)
    notes = Column(Text, nullable=False, default="")


class FinancingSubmissionPackage(Base):
    __tablename__ = "financing_submission_packages"
    __table_args__ = (UniqueConstraint("application_id", "submission_version", name="uq_submission_package_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    submission_package_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    application_package_id = Column(String(64), ForeignKey("financing_application_packages.package_id"), nullable=False, index=True)
    submission_version = Column(Integer, nullable=False)
    institution_name = Column(String(255), nullable=False)
    product_name = Column(String(255), nullable=False)
    status = Column(String(24), nullable=False, default="draft", index=True)
    manifest_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    package_hash = Column(String(64), nullable=False, index=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    submitted_at = Column(DateTime, nullable=True)


class FinancingSupplementPackage(Base):
    __tablename__ = "financing_supplement_packages"
    __table_args__ = (UniqueConstraint("supplement_request_id", "package_version", name="uq_supplement_package_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    supplement_package_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    supplement_request_id = Column(String(64), ForeignKey("financing_supplement_requests.supplement_id"), nullable=False, index=True)
    package_version = Column(Integer, nullable=False)
    status = Column(String(24), nullable=False, default="draft", index=True)
    manifest_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    package_hash = Column(String(64), nullable=False, default="")
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    submitted_at = Column(DateTime, nullable=True)
    submitted_by = Column(String(128), nullable=True)
    submission_reference = Column(String(128), nullable=True)


class FinancingSupplementPackageItem(Base):
    __tablename__ = "financing_supplement_package_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    package_item_id = Column(String(64), unique=True, nullable=False, index=True)
    supplement_package_id = Column(String(64), ForeignKey("financing_supplement_packages.supplement_package_id"), nullable=False, index=True)
    application_material_id = Column(String(64), ForeignKey("financing_application_materials.application_material_id"), nullable=False, index=True)
    source_file_id = Column(String(64), nullable=True, index=True)
    material_type = Column(String(64), nullable=False)
    material_name = Column(String(255), nullable=False)
    file_name = Column(String(255), nullable=True)
    file_hash = Column(String(128), nullable=False, default="")
    source_file_path = Column(String(512), nullable=True)
    required = Column(Integer, nullable=False, default=1)


class FinancingMaterialEvent(Base):
    __tablename__ = "financing_material_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    material_event_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    application_material_id = Column(String(64), nullable=True, index=True)
    package_id = Column(String(64), nullable=True, index=True)
    event_type = Column(String(40), nullable=False, index=True)
    operator_id = Column(String(128), nullable=False)
    operator_name = Column(String(255), nullable=False)
    payload_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingContact(Base):
    __tablename__ = "financing_contacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contact_id = Column(String(64), unique=True, nullable=False, index=True)
    contact_type = Column(String(24), nullable=False, index=True)
    customer_id = Column(String(128), nullable=True, index=True)
    institution_name = Column(String(255), nullable=True, index=True)
    branch_name = Column(String(255), nullable=True)
    name = Column(String(255), nullable=False, index=True)
    title = Column(String(128), nullable=True)
    department = Column(String(255), nullable=True)
    mobile = Column(String(64), nullable=True)
    phone = Column(String(64), nullable=True)
    email = Column(String(255), nullable=True)
    wechat = Column(String(128), nullable=True)
    is_primary = Column(Integer, nullable=False, default=0)
    related_person_id = Column(String(128), nullable=True, index=True)
    status = Column(String(24), nullable=False, default="active", index=True)
    notes = Column(Text, nullable=False, default="")
    created_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingApplicationContact(Base):
    __tablename__ = "financing_application_contacts"
    __table_args__ = (UniqueConstraint("application_id", "contact_id", name="uq_financing_application_contact"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    application_contact_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    contact_id = Column(String(64), ForeignKey("financing_contacts.contact_id"), nullable=False, index=True)
    role = Column(String(40), nullable=False, default="handler")
    is_primary = Column(Integer, nullable=False, default=0)
    created_by = Column(String(128), nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingCommunicationRecord(Base):
    __tablename__ = "financing_communication_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    communication_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    contact_id = Column(String(64), ForeignKey("financing_contacts.contact_id"), nullable=True, index=True)
    communication_side = Column(String(24), nullable=False, index=True)
    channel = Column(String(24), nullable=False, index=True)
    direction = Column(String(24), nullable=False)
    feedback_tag = Column(String(40), nullable=True, index=True)
    subject = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    occurred_at = Column(DateTime, nullable=False, index=True)
    operator_id = Column(String(128), nullable=False)
    operator_name = Column(String(255), nullable=False)
    related_task_id = Column(String(64), nullable=True, index=True)
    related_supplement_id = Column(String(64), nullable=True, index=True)
    related_review_feedback_id = Column(String(64), nullable=True, index=True)
    related_approval_record_id = Column(String(64), nullable=True, index=True)
    follow_up_required = Column(Integer, nullable=False, default=0)
    next_follow_up_at = Column(DateTime, nullable=True)
    outcome = Column(String(32), nullable=False, default="info_only", index=True)
    internal_note = Column(Text, nullable=False, default="")
    status = Column(String(24), nullable=False, default="active", index=True)
    voided_by = Column(String(128), nullable=True)
    voided_at = Column(DateTime, nullable=True)
    void_reason = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingFollowUp(Base):
    __tablename__ = "financing_followups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    follow_up_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    communication_record_id = Column(String(64), ForeignKey("financing_communication_records.communication_id"), nullable=True, index=True)
    related_task_id = Column(String(64), nullable=True, index=True)
    follow_up_type = Column(String(24), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False, default="")
    assignee_user_id = Column(String(128), nullable=True, index=True)
    assignee_user_name = Column(String(255), nullable=True)
    due_at = Column(DateTime, nullable=False, index=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    priority = Column(String(16), nullable=False, default="normal")
    completed_at = Column(DateTime, nullable=True)
    completed_by = Column(String(128), nullable=True)
    result = Column(Text, nullable=False, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingReviewFeedback(Base):
    __tablename__ = "financing_review_feedback"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feedback_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    feedback_type = Column(String(32), nullable=False, index=True)
    feedback_date = Column(DateTime, nullable=False)
    institution_contact = Column(String(255), nullable=True)
    content = Column(Text, nullable=False)
    related_stage = Column(String(40), nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingApprovalRecord(Base):
    __tablename__ = "financing_approval_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    approval_record_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    approval_status = Column(String(24), nullable=False, index=True)
    submitted_amount = Column(Numeric(18, 2), nullable=False)
    approved_amount = Column(Numeric(18, 2), nullable=True)
    approved_term_months = Column(Integer, nullable=True)
    approved_interest_rate = Column(Numeric(10, 6), nullable=True)
    guarantee_method = Column(String(255), nullable=True)
    repayment_method = Column(String(255), nullable=True)
    approval_reference = Column(String(128), nullable=True)
    approval_date = Column(DateTime, nullable=False)
    approval_expiry_date = Column(Date, nullable=True)
    conditions_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    notes = Column(Text, nullable=False, default="")
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingApprovalCondition(Base):
    __tablename__ = "financing_approval_conditions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    approval_condition_id = Column(String(64), unique=True, nullable=False, index=True)
    approval_record_id = Column(String(64), ForeignKey("financing_approval_records.approval_record_id"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False, default="")
    status = Column(String(24), nullable=False, default="pending", index=True)
    required = Column(Integer, nullable=False, default=1)
    updated_by = Column(String(128), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class FinancingDisbursementRecord(Base):
    __tablename__ = "financing_disbursement_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    disbursement_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    disbursement_no = Column(String(64), unique=True, nullable=False, index=True)
    amount = Column(Numeric(18, 2), nullable=False)
    disbursed_at = Column(DateTime, nullable=False)
    bank_reference = Column(String(128), nullable=True)
    recipient_name = Column(String(255), nullable=True)
    recipient_account_masked = Column(String(128), nullable=True)
    purpose = Column(String(255), nullable=True)
    notes = Column(Text, nullable=False, default="")
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingApplicationOutcome(Base):
    __tablename__ = "financing_application_outcomes"
    __table_args__ = (UniqueConstraint("application_id", "outcome_version", name="uq_application_outcome_version"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    outcome_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    customer_id = Column(String(128), nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    product_version_id = Column(String(64), nullable=False, index=True)
    requirement_id = Column(String(64), nullable=False, index=True)
    plan_version_id = Column(String(64), nullable=False, index=True)
    outcome_version = Column(Integer, nullable=False, default=1)
    supersedes_outcome_id = Column(String(64), nullable=True, index=True)
    final_status = Column(String(48), nullable=False, index=True)
    submitted_amount = Column(Numeric(18, 2), nullable=True)
    approved_amount = Column(Numeric(18, 2), nullable=True)
    disbursed_amount = Column(Numeric(18, 2), nullable=True)
    submitted_term_months = Column(Integer, nullable=True)
    approved_term_months = Column(Integer, nullable=True)
    submitted_interest_rate = Column(Numeric(10, 6), nullable=True)
    approved_interest_rate = Column(Numeric(10, 6), nullable=True)
    approval_date = Column(DateTime, nullable=True)
    disbursement_date = Column(DateTime, nullable=True)
    rejection_code = Column(String(64), nullable=True)
    rejection_reason = Column(Text, nullable=False, default="")
    disbursement_variance_reason = Column(String(64), nullable=True)
    final_notes = Column(Text, nullable=False, default="")
    source_type = Column(String(40), nullable=False, default="execution")
    status = Column(String(24), nullable=False, default="finalized", index=True)
    closed_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class FinancingApplicationRejectionReason(Base):
    __tablename__ = "financing_application_rejection_reasons"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rejection_reason_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    outcome_id = Column(String(64), ForeignKey("financing_application_outcomes.outcome_id"), nullable=False, index=True)
    reason_code = Column(String(48), nullable=False, index=True)
    description = Column(Text, nullable=False, default="")
    source_type = Column(String(32), nullable=False)
    confirmed_by = Column(String(128), nullable=False)
    confirmed_at = Column(DateTime, nullable=False)


class FinancingSupplementOutcome(Base):
    __tablename__ = "financing_supplement_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    supplement_outcome_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, unique=True, index=True)
    supplement_count = Column(Integer, nullable=False, default=0)
    supplement_material_count = Column(Integer, nullable=False, default=0)
    supplement_rounds = Column(Integer, nullable=False, default=0)
    categories_json = Column(Text, nullable=False, default="[]")
    generated_at = Column(DateTime, nullable=False)


class ApplicationBottleneck(Base):
    __tablename__ = "financing_application_bottlenecks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bottleneck_id = Column(String(64), unique=True, nullable=False, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    bottleneck_type = Column(String(40), nullable=False, index=True)
    source_id = Column(String(64), nullable=True, index=True)
    description = Column(Text, nullable=False)
    started_at = Column(DateTime, nullable=False)
    resolved_at = Column(DateTime, nullable=True)
    duration_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ProductRuleFeedback(Base):
    __tablename__ = "product_rule_feedback"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feedback_id = Column(String(64), unique=True, nullable=False, index=True)
    product_id = Column(String(64), nullable=False, index=True)
    product_version_id = Column(String(64), nullable=False, index=True)
    rule_id = Column(String(64), nullable=True, index=True)
    application_id = Column(String(64), ForeignKey("financing_applications.application_id"), nullable=False, index=True)
    feedback_type = Column(String(40), nullable=False, index=True)
    expected_result = Column(Text, nullable=False, default="")
    actual_bank_feedback = Column(Text, nullable=False)
    evidence_source = Column(String(40), nullable=False)
    evidence_reference = Column(String(128), nullable=True)
    status = Column(String(24), nullable=False, default="pending", index=True)
    reviewed_by = Column(String(128), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


@event.listens_for(FinancingApplicationOutcome, "before_update")
def _guard_finalized_application_outcome_update(_mapper, _connection, row):
    state = inspect(row)
    old_status = state.attrs.status.history.deleted[0] if state.attrs.status.history.deleted else row.status
    if old_status == "finalized":
        raise ValueError("已定稿的融资申请结果不可修改，请创建修正版本")


@event.listens_for(FinancingApplicationOutcome, "before_delete")
def _guard_application_outcome_delete(_mapper, _connection, _row):
    raise ValueError("融资申请结果不可删除")


class FinancingProductConflict(Base):
    __tablename__ = "financing_product_conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_product_code = Column(String(64), unique=True, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="unresolved", server_default="unresolved")
    source_hashes_json = Column(Text, nullable=False, default="[]")
    source_sides_json = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False, default="[]")
    decision_json = Column(Text, nullable=False, default="{}")
    decision_hash = Column(String(64), nullable=True)
    resolved_by = Column(String(128), nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


@event.listens_for(FinancingPlanSelection, "before_update")
def _guard_finalized_plan_selection_update(_mapper, _connection, row):
    state = inspect(row)
    history = state.attrs.status.history
    old_status = history.deleted[0] if history.deleted else row.status
    if old_status == "finalized":
        raise ValueError("已定稿的方案选择不可修改")


@event.listens_for(FinancingPlanReportSnapshot, "before_update")
def _guard_plan_report_snapshot_update(_mapper, _connection, _row):
    raise ValueError("融资方案报告快照不可修改")


@event.listens_for(FinancingPlanReportSnapshot, "before_delete")
def _guard_plan_report_snapshot_delete(_mapper, _connection, _row):
    raise ValueError("融资方案报告快照不可删除")


@event.listens_for(FinancingProductVersion, "before_update")
def _guard_published_version_update(_mapper, _connection, row):
    state = inspect(row)
    history = state.attrs.status.history
    old_status = history.deleted[0] if history.deleted else row.status
    if old_status not in {"published", "expired", "superseded", "disabled"}:
        return
    changed = {attr.key for attr in state.attrs if attr.history.has_changes()}
    if old_status == "published" and changed == {"status"} and row.status in {"expired", "superseded", "disabled"}:
        return
    raise ValueError("已发布版本内容不可修改")


@event.listens_for(FinancingProductVersion, "before_delete")
def _guard_published_version_delete(_mapper, _connection, row):
    if row.status not in {"draft", "needs_review"}:
        raise ValueError("已发布版本不可删除")


@event.listens_for(FinancingProductRule, "before_update")
@event.listens_for(FinancingProductRule, "before_delete")
def _guard_published_rule(_mapper, connection, row):
    status = connection.execute(select(FinancingProductVersion.status).where(FinancingProductVersion.version_id == row.version_id)).scalar_one_or_none()
    if status not in {"draft", "needs_review"}:
        raise ValueError("已发布版本的规则不可修改")


@event.listens_for(FinancingProduct, "before_update")
def _guard_published_product_identity(_mapper, connection, row):
    exists = connection.execute(select(FinancingProductVersion.version_id).where(
        FinancingProductVersion.product_id == row.product_id,
        FinancingProductVersion.status.in_(("published", "expired", "superseded", "disabled")),
    ).limit(1)).first()
    if exists:
        raise ValueError("已有已发布版本时不能修改产品身份")


@event.listens_for(FinancingPlanVersion, "before_update")
def _guard_confirmed_plan_version_update(_mapper, _connection, row):
    state = inspect(row)
    history = state.attrs.status.history
    old_status = history.deleted[0] if history.deleted else row.status
    if old_status != "confirmed":
        return
    changed = {attr.key for attr in state.attrs if attr.history.has_changes()}
    if changed == {"status"} and row.status == "superseded":
        return
    raise ValueError("已确认方案版本不可修改")


@event.listens_for(FinancingPlanItem, "before_update")
@event.listens_for(FinancingPlanItem, "before_delete")
def _guard_confirmed_plan_item(_mapper, connection, row):
    status = connection.execute(select(FinancingPlanVersion.status).where(
        FinancingPlanVersion.plan_version_id == row.plan_version_id,
    )).scalar_one_or_none()
    if status == "confirmed":
        raise ValueError("已确认方案版本的产品项不可修改")


@event.listens_for(FinancingPlanGap, "before_update")
@event.listens_for(FinancingPlanGap, "before_delete")
def _guard_confirmed_plan_gap(_mapper, connection, row):
    status = connection.execute(select(FinancingPlanVersion.status).where(
        FinancingPlanVersion.plan_version_id == row.plan_version_id,
    )).scalar_one_or_none()
    if status == "confirmed":
        raise ValueError("已确认方案版本的缺口不可修改")


@event.listens_for(FinancingPlanCondition, "before_update")
@event.listens_for(FinancingPlanCondition, "before_delete")
@event.listens_for(FinancingPlanMaterial, "before_update")
@event.listens_for(FinancingPlanMaterial, "before_delete")
@event.listens_for(FinancingPlanExplanation, "before_update")
@event.listens_for(FinancingPlanExplanation, "before_delete")
def _guard_confirmed_plan_artifact(_mapper, connection, row):
    status = connection.execute(select(FinancingPlanVersion.status).where(
        FinancingPlanVersion.plan_version_id == row.plan_version_id,
    )).scalar_one_or_none()
    if status == "confirmed":
        raise ValueError("已确认方案版本的条件、材料及说明不可修改")


class AsyncJobRecord(Base):
    __tablename__ = "async_jobs"
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(64), unique=True, nullable=False, index=True)
    job_type = Column(String(64), nullable=False, index=True)
    customer_id = Column(String(64), default="", index=True)
    username = Column(String(128), default="", nullable=False, index=True)
    status = Column(String(32), default="pending", nullable=False, index=True)
    progress_message = Column(String(255), default="")
    request_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    execution_payload_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    result_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    error_message = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    celery_task_id = Column(String(255), default="", index=True)
    worker_name = Column(String(255), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    started_at = Column(String(64), default="")
    finished_at = Column(String(64), default="")


class SavedApplicationRecord(Base):
    __tablename__ = "saved_applications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    application_id = Column(String(64), unique=True, nullable=False, index=True)
    version_group_id = Column(String(64), default="", index=True)
    previous_application_id = Column(String(64), default="", index=True)
    version_no = Column(Integer, default=1, nullable=False)
    customer_name = Column(String(255), nullable=False, default="")
    customer_id = Column(String(64), default="", index=True)
    loan_type = Column(String(50), default="enterprise")
    application_data = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    saved_at = Column(String(64), nullable=False, index=True)
    owner_username = Column(String(255), default="")
    source = Column(String(50), default="manual")
    stale = Column(Integer, default=0)
    stale_reason = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    stale_at = Column(String(64), default="")
    profile_version = Column(Integer, default=1)
    profile_updated_at = Column(String(64), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class UserAccount(Base):
    __tablename__ = "user_accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(128), unique=True, nullable=False, index=True)
    role = Column(String(32), default="user", nullable=False, index=True)
    password_hash = Column(String(255), default="", nullable=False)
    salt = Column(String(128), default="", nullable=False)
    password_algo = Column(String(64), default="pbkdf2_sha256", nullable=False)
    password_iterations = Column(Integer, default=600000, nullable=False)
    security_question = Column(String(255), default="")
    security_answer_hash = Column(String(255), default="")
    security_answer_salt = Column(String(128), default="")
    security_answer_algo = Column(String(64), default="pbkdf2_sha256")
    security_answer_iterations = Column(Integer, default=600000)
    display_name = Column(String(255), default="")
    phone = Column(String(64), default="")
    created_at = Column(String(64), default="")
    last_login_at = Column(String(64), default="")
    updated_at = Column(String(64), default="")


class ActivityLogEntry(Base):
    __tablename__ = "activity_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    activity_id = Column(String(64), unique=True, nullable=False, index=True)
    activity_type = Column(String(64), nullable=False, index=True)
    activity_time = Column(String(64), nullable=False, index=True)
    status = Column(String(32), default="completed", index=True)
    title = Column(String(255), default="")
    description = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    customer_name = Column(String(255), default="", index=True)
    customer_id = Column(String(64), default="", index=True)
    username = Column(String(128), default="", index=True)
    file_name = Column(String(255), default="")
    file_type = Column(String(100), default="")
    metadata_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerActivityState(Base):
    __tablename__ = "customer_activity_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String(255), unique=True, nullable=False, index=True)
    has_application = Column(Integer, default=0, nullable=False)
    has_matching = Column(Integer, default=0, nullable=False)
    last_update = Column(String(64), default="", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class TableField(Base):
    __tablename__ = "table_fields"
    __table_args__ = (UniqueConstraint("field_id", name="uq_table_fields_field_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    field_id = Column(String(64), nullable=False, index=True)
    field_name = Column(String(255), nullable=False)
    field_key = Column(String(100), nullable=False, index=True)
    doc_type = Column(String(100), default="")
    field_order = Column(Integer, default=0)
    editable = Column(Integer, default=1)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
