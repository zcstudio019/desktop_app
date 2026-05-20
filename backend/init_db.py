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


def _execute_mysql_ddl(connection, ddl: str, *, label: str) -> None:
    try:
        connection.execute(text(ddl))
    except Exception as exc:
        logger.warning("[DB Migration] %s failed: %s", label, exc, exc_info=True)


def _ensure_mysql_longtext(connection, table_name: str, column_name: str) -> None:
    try:
        column_type = _mysql_column_type(connection, table_name, column_name)
        if not column_type or "longtext" in column_type:
            return
        _execute_mysql_ddl(
            connection,
            f"""
            ALTER TABLE `{table_name}`
            MODIFY COLUMN `{column_name}` LONGTEXT
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci
            NULL
            """,
            label=f"{table_name}.{column_name} LONGTEXT",
        )
    except Exception as exc:
        logger.warning("[DB Migration] failed to inspect %s.%s: %s", table_name, column_name, exc, exc_info=True)


def _repair_mysql_charset_and_text_columns(connection) -> None:
    database_name = (getattr(engine.url, "database", None) or "").strip()
    if database_name:
        _execute_mysql_ddl(
            connection,
            f"ALTER DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            label="database utf8mb4 conversion",
        )
    for table_name in ("async_jobs", "documents", "extractions", "customer_profiles", "customer_document_chunks", "product_cache_entries"):
        _execute_mysql_ddl(
            connection,
            f"ALTER TABLE `{table_name}` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            label=f"{table_name} utf8mb4 conversion",
        )
    large_text_columns = {
        "async_jobs": ["request_json", "execution_payload_json", "result_json", "error_message"],
        "extractions": ["extracted_data", "extraction_error"],
        "customer_profiles": ["markdown_content", "source_snapshot_json", "rag_source_priority_json", "risk_report_schema_json"],
        "customer_document_chunks": ["chunk_text", "embedding_json", "metadata_json"],
        "customer_risk_reports": ["report_json", "report_markdown"],
        "saved_applications": ["application_data", "stale_reason"],
        "activity_logs": ["description", "metadata_json"],
        "chat_messages": ["content"],
        "product_cache_entries": ["content"],
    }
    for table_name, column_names in large_text_columns.items():
        for column_name in column_names:
            _ensure_mysql_longtext(connection, table_name, column_name)


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
