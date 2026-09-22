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
