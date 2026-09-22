"""Add Step 7A.2 columns to existing Step 7A catalog tables."""

from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateColumn

from backend.db_models import FinancingProduct, FinancingProductVersion


def ensure_markdown_catalog_schema(engine) -> None:
    inspector = inspect(engine)
    for table in (FinancingProduct.__table__, FinancingProductVersion.__table__):
        existing = {column["name"] for column in inspector.get_columns(table.name)}
        missing = [column for column in table.columns if column.name not in existing]
        if not missing:
            continue
        with engine.begin() as connection:
            for column in missing:
                ddl = str(CreateColumn(column).compile(dialect=engine.dialect))
                connection.execute(text(f"ALTER TABLE {table.name} ADD COLUMN {ddl}"))
        inspector = inspect(engine)
    for index in FinancingProduct.__table__.indexes:
        if "external_product_code" in {column.name for column in index.columns}:
            index.create(bind=engine, checkfirst=True)
