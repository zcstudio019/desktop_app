"""Initialize database schema with SQLAlchemy."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import inspect, text

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from backend.database import Base, engine
    from backend import db_models  # noqa: F401  # Import models so metadata is populated.
    from backend.routers.auth import ensure_default_admin_exists_only_for_empty_db
else:
    from .database import Base, engine
    from . import db_models  # noqa: F401  # Import models so metadata is populated.
    from .routers.auth import ensure_default_admin_exists_only_for_empty_db

logger = logging.getLogger(__name__)


def _mysql_column_exists(connection, table_name: str, column_name: str) -> bool:
    inspector = inspect(connection)
    try:
        columns = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(column.get("name") == column_name for column in columns)


def table_exists(connection, table_name: str) -> bool:
    try:
        return inspect(connection).has_table(table_name)
    except Exception as exc:
        logger.info("[DB Migration] skip table check %s: %s", table_name, exc)
        return False


def column_exists(connection, table_name: str, column_name: str) -> bool:
    return _mysql_column_exists(connection, table_name, column_name)


def _mysql_column_type(connection, table_name: str, column_name: str) -> str:
    inspector = inspect(connection)
    try:
        columns = inspector.get_columns(table_name)
    except Exception:
        return ""
    for column in columns:
        if column.get("name") == column_name:
            return str(column.get("type") or "").lower()
    return ""


def get_column_info(connection, table_name: str, column_name: str) -> dict[str, str] | None:
    database_name = (getattr(engine.url, "database", None) or "").strip()
    if not database_name:
        return None
    row = connection.execute(
        text(
            """
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
                   CHARACTER_SET_NAME, COLLATION_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :database_name
              AND TABLE_NAME = :table_name
              AND COLUMN_NAME = :column_name
            LIMIT 1
            """
        ),
        {"database_name": database_name, "table_name": table_name, "column_name": column_name},
    ).mappings().first()
    return dict(row) if row else None


def _execute_mysql_ddl(connection, ddl: str, *, label: str) -> None:
    try:
        connection.execute(text(ddl))
    except Exception as exc:
        logger.warning("[DB Migration] %s failed: %s", label, exc)


def alter_longtext_utf8mb4_if_exists(connection, table_name: str, column_name: str) -> None:
    try:
        if not table_exists(connection, table_name):
            logger.info("[DB Migration] skip missing table %s", table_name)
            return
        if not column_exists(connection, table_name, column_name):
            logger.info("[DB Migration] skip missing column %s.%s", table_name, column_name)
            return
        column_info = get_column_info(connection, table_name, column_name)
        if not column_info:
            logger.info("[DB Migration] skip unavailable column info %s.%s", table_name, column_name)
            return
        column_type = str(column_info.get("COLUMN_TYPE") or "").lower()
        charset = str(column_info.get("CHARACTER_SET_NAME") or "").lower()
        collation = str(column_info.get("COLLATION_NAME") or "").lower()
        if "longtext" in column_type and charset == "utf8mb4" and collation == "utf8mb4_unicode_ci":
            logger.info("[DB Migration] skip unchanged %s.%s", table_name, column_name)
            return
        null_sql = "NULL" if str(column_info.get("IS_NULLABLE") or "").upper() == "YES" else "NOT NULL"
        connection.execute(
            text(
                f"""
            ALTER TABLE `{table_name}`
            MODIFY COLUMN `{column_name}` LONGTEXT
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci
            {null_sql}
            """,
            )
        )
        logger.info("[DB Migration] altered %s.%s to LONGTEXT utf8mb4", table_name, column_name)
    except Exception as exc:
        logger.warning("[DB Migration] failed to alter %s.%s: %s", table_name, column_name, exc)


def _repair_mysql_charset_and_text_columns(connection) -> None:
    database_name = (getattr(engine.url, "database", None) or "").strip()
    if database_name:
        _execute_mysql_ddl(
            connection,
            f"ALTER DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            label="database utf8mb4 conversion",
        )
    large_text_columns = {
        "async_jobs": ["request_json", "execution_payload_json", "result_json", "error_message"],
        "documents": ["metadata_json", "raw_text", "content_text"],
        "extractions": ["extracted_data", "extracted_json", "result_json", "markdown_summary", "raw_text", "extraction_error"],
        "customer_profiles": ["markdown_content", "source_snapshot_json", "rag_source_priority_json", "risk_report_schema_json"],
        "customer_flow_rules": [
            "related_company_names_json",
            "self_account_numbers_json",
            "internal_transfer_keywords_json",
            "operating_counterparty_whitelist_json",
            "internal_counterparty_blacklist_json",
            "personal_counterparty_names_json",
            "manual_overrides_json",
        ],
        "income_confirmation_overrides": ["months_json", "transaction_ids_json", "reason"],
        "customer_document_chunks": ["chunk_text", "embedding_json", "metadata_json"],
        "risk_reports": ["risk_json", "report_markdown"],
        "customer_risk_reports": ["report_json", "report_markdown"],
        "customer_financing_diagnostic_reports": ["report_json", "report_markdown", "source_summary"],
        "application_records": ["application_json", "report_markdown"],
        "saved_applications": ["application_data", "stale_reason"],
        "activity_logs": ["description", "metadata_json"],
        "chat_messages": ["content"],
        "product_cache_entries": ["content"],
    }
    for table_name, column_names in large_text_columns.items():
        for column_name in column_names:
            alter_longtext_utf8mb4_if_exists(connection, table_name, column_name)


def init_database() -> None:
    Base.metadata.create_all(bind=engine, checkfirst=True)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
        dialect = engine.dialect.name.lower()
        if dialect == "mysql":
            _repair_mysql_charset_and_text_columns(connection)
            if not _mysql_column_exists(connection, "async_jobs", "execution_payload_json"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE async_jobs
                        ADD COLUMN execution_payload_json LONGTEXT
                        CHARACTER SET utf8mb4
                        COLLATE utf8mb4_unicode_ci
                        NULL
                    """,
                    label="async_jobs.execution_payload_json add",
                )
            if not _mysql_column_exists(connection, "async_jobs", "celery_task_id"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE async_jobs
                        ADD COLUMN celery_task_id VARCHAR(255) NULL
                    """,
                    label="async_jobs.celery_task_id add",
                )
            if not _mysql_column_exists(connection, "async_jobs", "worker_name"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE async_jobs
                        ADD COLUMN worker_name VARCHAR(255) NULL
                    """,
                    label="async_jobs.worker_name add",
                )
            if not _mysql_column_exists(connection, "saved_applications", "version_group_id"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE saved_applications
                        ADD COLUMN version_group_id VARCHAR(64) NULL
                    """,
                    label="saved_applications.version_group_id add",
                )
            if not _mysql_column_exists(connection, "saved_applications", "previous_application_id"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE saved_applications
                        ADD COLUMN previous_application_id VARCHAR(64) NULL
                    """,
                    label="saved_applications.previous_application_id add",
                )
            if not _mysql_column_exists(connection, "saved_applications", "version_no"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE saved_applications
                        ADD COLUMN version_no INT NOT NULL DEFAULT 1
                    """,
                    label="saved_applications.version_no add",
                )
            _repair_mysql_charset_and_text_columns(connection)
            if "longtext" not in _mysql_column_type(connection, "extractions", "extracted_data"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE extractions
                        MODIFY COLUMN extracted_data LONGTEXT
                        CHARACTER SET utf8mb4
                        COLLATE utf8mb4_unicode_ci
                        NULL
                    """,
                    label="extractions.extracted_data LONGTEXT",
                )
                logger.info("[DB Migration] extractions.extracted_data upgraded to LONGTEXT")
            if not _mysql_column_exists(connection, "documents", "file_hash"):
                _execute_mysql_ddl(
                    connection,
                    """
                        ALTER TABLE documents
                        ADD COLUMN file_hash VARCHAR(128) DEFAULT ''
                    """,
                    label="documents.file_hash add",
                )
            if not _mysql_column_exists(connection, "documents", "is_active"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN is_active TINYINT(1) DEFAULT 1", label="documents.is_active add")
            if not _mysql_column_exists(connection, "documents", "archived_at"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN archived_at DATETIME NULL", label="documents.archived_at add")
            if not _mysql_column_exists(connection, "documents", "replaced_by_document_id"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN replaced_by_document_id VARCHAR(64) DEFAULT ''", label="documents.replaced_by_document_id add")
            if not _mysql_column_exists(connection, "documents", "version_policy"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN version_policy VARCHAR(50) DEFAULT ''", label="documents.version_policy add")
            if not _mysql_column_exists(connection, "documents", "report_date"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN report_date VARCHAR(64) DEFAULT ''", label="documents.report_date add")
            if not _mysql_column_exists(connection, "documents", "valid_until"):
                _execute_mysql_ddl(connection, "ALTER TABLE documents ADD COLUMN valid_until VARCHAR(64) DEFAULT ''", label="documents.valid_until add")
            if not _mysql_column_exists(connection, "extractions", "extraction_status"):
                _execute_mysql_ddl(connection, "ALTER TABLE extractions ADD COLUMN extraction_status VARCHAR(32) DEFAULT 'success'", label="extractions.extraction_status add")
            if not _mysql_column_exists(connection, "extractions", "extraction_error"):
                _execute_mysql_ddl(connection, "ALTER TABLE extractions ADD COLUMN extraction_error TEXT NULL", label="extractions.extraction_error add")
            if not _mysql_column_exists(connection, "extractions", "skill_name"):
                _execute_mysql_ddl(connection, "ALTER TABLE extractions ADD COLUMN skill_name VARCHAR(100) DEFAULT ''", label="extractions.skill_name add")
            if not _mysql_column_exists(connection, "extractions", "skill_version"):
                _execute_mysql_ddl(connection, "ALTER TABLE extractions ADD COLUMN skill_version VARCHAR(50) DEFAULT ''", label="extractions.skill_version add")
            if not _mysql_column_exists(connection, "extractions", "schema_version"):
                _execute_mysql_ddl(connection, "ALTER TABLE extractions ADD COLUMN schema_version VARCHAR(100) DEFAULT ''", label="extractions.schema_version add")
            connection.commit()
    ensure_default_admin_exists_only_for_empty_db()


if __name__ == "__main__":
    init_database()
    print("Database initialized successfully.")
