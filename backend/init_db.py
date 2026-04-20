"""Initialize database schema with SQLAlchemy."""

from __future__ import annotations

from sqlalchemy import inspect, text

from .database import Base, engine
from . import db_models  # noqa: F401  # Import models so metadata is populated.
from .routers.auth import ensure_default_admin_exists_only_for_empty_db


def _mysql_column_exists(connection, table_name: str, column_name: str) -> bool:
    inspector = inspect(connection)
    try:
        columns = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(column.get("name") == column_name for column in columns)


def init_database() -> None:
    Base.metadata.create_all(bind=engine, checkfirst=True)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
        dialect = engine.dialect.name.lower()
        if dialect == "mysql":
            database_name = (getattr(engine.url, "database", None) or "").strip()
            if database_name:
                connection.execute(
                    text(
                        f"ALTER DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                )
            if not _mysql_column_exists(connection, "async_jobs", "execution_payload_json"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE async_jobs
                        ADD COLUMN execution_payload_json LONGTEXT
                        CHARACTER SET utf8mb4
                        COLLATE utf8mb4_unicode_ci
                        NULL
                        """
                    )
                )
            if not _mysql_column_exists(connection, "async_jobs", "celery_task_id"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE async_jobs
                        ADD COLUMN celery_task_id VARCHAR(255) NULL
                        """
                    )
                )
            if not _mysql_column_exists(connection, "async_jobs", "worker_name"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE async_jobs
                        ADD COLUMN worker_name VARCHAR(255) NULL
                        """
                    )
                )
            if not _mysql_column_exists(connection, "saved_applications", "version_group_id"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE saved_applications
                        ADD COLUMN version_group_id VARCHAR(64) NULL
                        """
                    )
                )
            if not _mysql_column_exists(connection, "saved_applications", "previous_application_id"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE saved_applications
                        ADD COLUMN previous_application_id VARCHAR(64) NULL
                        """
                    )
                )
            if not _mysql_column_exists(connection, "saved_applications", "version_no"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE saved_applications
                        ADD COLUMN version_no INT NOT NULL DEFAULT 1
                        """
                    )
                )
            connection.execute(
                text(
                    """
                    ALTER TABLE async_jobs
                    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
            )
            connection.execute(
                text(
                    """
                    ALTER TABLE async_jobs
                    MODIFY COLUMN request_json LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                    MODIFY COLUMN execution_payload_json LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                    MODIFY COLUMN result_json LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                    MODIFY COLUMN error_message LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL
                    """
                )
            )
            connection.commit()
    ensure_default_admin_exists_only_for_empty_db()


if __name__ == "__main__":
    init_database()
    print("Database initialized successfully.")
