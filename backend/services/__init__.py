"""Backend storage service factory."""

from __future__ import annotations

import sys
from pathlib import Path

desktop_app_path = Path(__file__).parent.parent.parent
if str(desktop_app_path) not in sys.path:
    sys.path.insert(0, str(desktop_app_path))

from config import LOCAL_DB_PATH, USE_LOCAL_STORAGE

from backend.database import DB_BACKEND

from .local_storage_service import LocalStorageService
from .sqlalchemy_storage_service import SQLAlchemyStorageService

try:
    from services.feishu_service import FeishuService
except ImportError:
    class FeishuService:  # type: ignore[override]
        def __init__(self):
            raise NotImplementedError("FeishuService not available")


def get_storage_service() -> LocalStorageService | SQLAlchemyStorageService | FeishuService:
    """Return the most appropriate storage service for the current environment."""
    if DB_BACKEND == "mysql":
        return SQLAlchemyStorageService()

    if USE_LOCAL_STORAGE:
        # Local SQLite fallback is for development-only use.
        return LocalStorageService(db_path=LOCAL_DB_PATH)

    from services.feishu_service import FeishuService as RealFeishuService

    return RealFeishuService()


def supports_structured_storage(service: object) -> bool:
    """Return True when the storage backend supports customer-scoped DB operations."""
    required_methods = (
        "get_customer",
        "list_customers",
        "save_document",
        "save_extraction",
        "get_customer_profile",
        "upsert_customer_profile",
    )
    return all(hasattr(service, method_name) for method_name in required_methods)


__all__ = [
    "FeishuService",
    "LocalStorageService",
    "SQLAlchemyStorageService",
    "get_storage_service",
    "supports_structured_storage",
]
