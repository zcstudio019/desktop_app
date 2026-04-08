"""Initialize database schema with SQLAlchemy."""

from __future__ import annotations

from sqlalchemy import text

from .database import Base, engine
from . import db_models  # noqa: F401  # Import models so metadata is populated.


def init_database() -> None:
    Base.metadata.create_all(bind=engine, checkfirst=True)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


if __name__ == "__main__":
    init_database()
    print("Database initialized successfully.")
