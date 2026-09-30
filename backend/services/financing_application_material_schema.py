"""Backward compatible schema additions for Step 9.3 application materials."""
from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateColumn

from backend.db_models import FinancingApplicationMaterial


def ensure_application_material_schema(engine) -> None:
    inspector = inspect(engine)
    table = FinancingApplicationMaterial.__table__
    if not inspector.has_table(table.name):
        table.create(bind=engine, checkfirst=True)
        return
    existing = {column["name"] for column in inspector.get_columns(table.name)}
    missing = [column for column in table.columns if column.name not in existing]
    if not missing:
        return
    with engine.begin() as connection:
        for column in missing:
            ddl = str(CreateColumn(column).compile(dialect=engine.dialect))
            connection.execute(text(f"ALTER TABLE {table.name} ADD COLUMN {ddl}"))
