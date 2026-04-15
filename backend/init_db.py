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
            if not _mysql_column_exists(connection, "async_jobs", "execution_payload_json"):
                connection.execute(
                    text(
                        """
                        ALTER TABLE async_jobs
                        ADD COLUMN execution_payload_json LONGTEXT NULL
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
            connection.execute(
                text(
                    """
                    ALTER TABLE async_jobs
                    MODIFY COLUMN request_json LONGTEXT NULL,
                    MODIFY COLUMN execution_payload_json LONGTEXT NULL,
                    MODIFY COLUMN result_json LONGTEXT NULL,
                    MODIFY COLUMN error_message LONGTEXT NULL
                    """
                )
            )
            connection.commit()
    ensure_default_admin_exists_only_for_empty_db()


if __name__ == "__main__":
    init_database()
    print("Database initialized successfully.")
