from __future__ import annotations

from typing import Any

from backend.services.enterprise_bank_statement_agent.excel_reader import read_excel_workbook


def read_personal_bank_statement_workbook(
    file_path: str | None = None,
    rows: list[dict[str, Any]] | None = None,
    filename: str | None = None,
) -> dict[str, Any]:
    """Reuse the hardened bank-statement workbook reader; personal rules run later."""
    return read_excel_workbook(file_path=file_path, rows=rows, filename=filename)
