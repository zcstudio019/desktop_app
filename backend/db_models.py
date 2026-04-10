"""SQLAlchemy table definitions for deployment and database initialization."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, UniqueConstraint, func
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


class Extraction(Base):
    __tablename__ = "extractions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    extraction_id = Column(String(64), unique=True, nullable=False, index=True)
    doc_id = Column(String(64), nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    extraction_type = Column(String(50))
    extracted_data = Column(Text)
    confidence = Column(Float)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(64), unique=True, nullable=False, index=True)
    title = Column(String(255), default="")
    markdown_content = Column(Text, default="")
    source_mode = Column(String(20), default="auto")
    source_snapshot_json = Column(Text, default="{}")
    rag_source_priority_json = Column(Text, default="[]")
    risk_report_schema_json = Column(Text, default="{}")
    version = Column(Integer, default=1)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class CustomerSchemeSnapshot(Base):
    __tablename__ = "customer_scheme_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_id = Column(String(64), nullable=False, index=True)
    customer_name = Column(String(255), default="")
    summary_markdown = Column(Text, default="")
    raw_result = Column(Text, default="")
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
    chunk_text = Column(Text, nullable=False)
    embedding_json = Column(Text, default="[]")
    metadata_json = Column(Text, default="{}")
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
    report_json = Column(Text, default="{}")
    report_markdown = Column(Text, default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


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
    content = Column(Text, default="", nullable=False)
    sequence = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class ProductCacheEntry(Base):
    __tablename__ = "product_cache_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cache_key = Column(String(64), unique=True, nullable=False, index=True)
    content = Column(Text, default="", nullable=False)
    last_updated = Column(String(64), default="", nullable=False, index=True)
    source = Column(String(64), default="wiki", nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class AsyncJobRecord(Base):
    __tablename__ = "async_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(64), unique=True, nullable=False, index=True)
    job_type = Column(String(64), nullable=False, index=True)
    customer_id = Column(String(64), default="", index=True)
    username = Column(String(128), default="", nullable=False, index=True)
    status = Column(String(32), default="pending", nullable=False, index=True)
    progress_message = Column(String(255), default="")
    request_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    result_json = Column(Text().with_variant(LONGTEXT(), "mysql"), default="{}")
    error_message = Column(Text().with_variant(LONGTEXT(), "mysql"), default="")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    started_at = Column(String(64), default="")
    finished_at = Column(String(64), default="")


class SavedApplicationRecord(Base):
    __tablename__ = "saved_applications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    application_id = Column(String(64), unique=True, nullable=False, index=True)
    customer_name = Column(String(255), nullable=False, default="")
    customer_id = Column(String(64), default="", index=True)
    loan_type = Column(String(50), default="enterprise")
    application_data = Column(Text, default="{}")
    saved_at = Column(String(64), nullable=False, index=True)
    owner_username = Column(String(255), default="")
    source = Column(String(50), default="manual")
    stale = Column(Integer, default=0)
    stale_reason = Column(Text, default="")
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
    description = Column(Text, default="")
    customer_name = Column(String(255), default="", index=True)
    customer_id = Column(String(64), default="", index=True)
    username = Column(String(128), default="", index=True)
    file_name = Column(String(255), default="")
    file_type = Column(String(100), default="")
    metadata_json = Column(Text, default="{}")
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
