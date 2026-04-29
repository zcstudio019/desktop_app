"""SQLAlchemy-backed business storage service."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, desc, inspect, select, text, update
from sqlalchemy.exc import DataError, SQLAlchemyError

from backend.database import Base, SessionLocal, engine
from backend.db_models import (
    AsyncJobRecord,
    ChatMessageRecord,
    ChatSession,
    Customer,
    CustomerDocumentChunk,
    CustomerProfile,
    CustomerRiskReport,
    CustomerSchemeSnapshot,
    Document,
    Extraction,
    ProductCacheEntry,
    SavedApplicationRecord,
    TableField,
)

logger = logging.getLogger(__name__)
JOB_PAYLOAD_PREVIEW_LIMIT = max(120, int(os.getenv("JOB_PAYLOAD_PREVIEW_LIMIT", "300")))
JOB_STALE_TIMEOUT_SECONDS = max(300, int(os.getenv("JOB_STALE_TIMEOUT_SECONDS", "900")))


def sanitize_async_job_error_message(error_message: str | None) -> str:
    raw_message = (error_message or "").strip()
    if not raw_message:
        return ""

    lowered = raw_message.lower()

    if "incorrect string value" in lowered or "pymysql.err.dataerror" in lowered:
        return "任务结果保存失败，可能包含当前数据库暂不支持的特殊字符。请联系管理员检查数据库字符集后重试。"

    if "duplicate entry" in lowered:
        return "任务保存失败，检测到重复记录，请刷新后重试。"

    if "lock wait timeout" in lowered or "deadlock" in lowered:
        return "任务保存失败，数据库当前较忙，请稍后重试。"

    if "redis" in lowered or "broker" in lowered or "connection refused" in lowered:
        return "后台任务服务暂时不可用，请稍后重试。"

    if "invalid token" in lowered or "unauthorized" in lowered:
        return "当前登录状态已失效，请重新登录后重试。"

    if "timeout" in lowered:
        return "任务执行超时，请稍后重试。"

    if "interrupted" in lowered or "stale" in lowered:
        return "任务可能已中断，请重新提交。"

    return raw_message


def normalize_async_job_error_message(error_message: str | None) -> str:
    raw_message = (error_message or "").strip()
    if not raw_message:
        return ""

    lowered = raw_message.lower()

    if "incorrect string value" in lowered or "pymysql.err.dataerror" in lowered:
        return "任务结果保存失败，可能包含当前数据库暂不支持的特殊字符。请联系管理员检查数据库字符集后重试。"

    if "duplicate entry" in lowered:
        return "任务保存失败，检测到重复记录，请刷新后重试。"

    if "lock wait timeout" in lowered or "deadlock" in lowered:
        return "任务保存失败，数据库当前较忙，请稍后重试。"

    if "redis" in lowered or "broker" in lowered or "connection refused" in lowered:
        return "后台任务服务暂时不可用，请稍后重试。"

    if "invalid token" in lowered or "unauthorized" in lowered:
        return "当前登录状态已失效，请重新登录后重试。"

    if "timeout" in lowered:
        return "任务执行超时，请稍后重试。"

    if "interrupted" in lowered or "stale" in lowered:
        return "任务可能已中断，请重新提交。"

    return raw_message


class SQLAlchemyStorageService:
    """Storage service backed by SQLAlchemy models for MySQL/RDS usage."""

    def __init__(self) -> None:
        self._session_factory = SessionLocal
        Base.metadata.create_all(
            bind=engine,
            tables=[
                ChatSession.__table__,
                ChatMessageRecord.__table__,
                ProductCacheEntry.__table__,
                AsyncJobRecord.__table__,
            ],
            checkfirst=True,
        )
        self._ensure_document_owner_columns()
        self._ensure_extraction_data_longtext()

    def _ensure_document_owner_columns(self) -> None:
        try:
            inspector = inspect(engine)
            document_columns = {column["name"] for column in inspector.get_columns("documents")}
            if "uploader" in document_columns:
                return
            ddl = "ALTER TABLE documents ADD COLUMN uploader VARCHAR(255) DEFAULT ''"
            with engine.begin() as conn:
                conn.execute(text(ddl))
            logger.info("[Migration] Added column uploader to documents")
        except Exception as exc:
            logger.warning("[Migration] Failed to ensure documents.uploader column: %s", exc)

    def _ensure_extraction_data_longtext(self) -> None:
        if engine.dialect.name.lower() != "mysql":
            return
        try:
            inspector = inspect(engine)
            if not inspector.has_table("extractions"):
                return
            columns = {
                column["name"]: str(column.get("type") or "").lower()
                for column in inspector.get_columns("extractions")
            }
            column_type = columns.get("extracted_data", "")
            if "longtext" in column_type:
                return
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        ALTER TABLE extractions
                        MODIFY COLUMN extracted_data LONGTEXT
                        CHARACTER SET utf8mb4
                        COLLATE utf8mb4_unicode_ci
                        NULL
                        """
                    )
                )
            logger.info("[DB Migration] extractions.extracted_data upgraded to LONGTEXT")
        except Exception as exc:
            logger.warning(
                "[DB Migration] Failed to ensure extractions.extracted_data LONGTEXT: %s",
                exc,
                exc_info=True,
            )

    def _sanitize_text_for_mysql(
        self,
        text: str,
        *,
        strip_non_bmp: bool = False,
    ) -> tuple[str, bool]:
        if not text:
            return text, False

        replaced = False
        cleaned_chars: list[str] = []
        for char in text:
            code_point = ord(char)
            if char == "\x00" or 0xD800 <= code_point <= 0xDFFF:
                cleaned_chars.append("\uFFFD")
                replaced = True
                continue
            if strip_non_bmp and code_point > 0xFFFF:
                cleaned_chars.append("\uFFFD")
                replaced = True
                continue
            cleaned_chars.append(char)
        return "".join(cleaned_chars), replaced

    def _sanitize_value_for_mysql(
        self,
        value: Any,
        *,
        strip_non_bmp: bool = False,
    ) -> tuple[Any, bool]:
        replaced = False

        if isinstance(value, str):
            cleaned, replaced = self._sanitize_text_for_mysql(value, strip_non_bmp=strip_non_bmp)
            return cleaned, replaced

        if isinstance(value, list):
            items: list[Any] = []
            for item in value:
                cleaned_item, item_replaced = self._sanitize_value_for_mysql(item, strip_non_bmp=strip_non_bmp)
                replaced = replaced or item_replaced
                items.append(cleaned_item)
            return items, replaced

        if isinstance(value, dict):
            sanitized: dict[str, Any] = {}
            for key, item in value.items():
                cleaned_key, key_replaced = self._sanitize_text_for_mysql(str(key), strip_non_bmp=strip_non_bmp)
                cleaned_item, item_replaced = self._sanitize_value_for_mysql(item, strip_non_bmp=strip_non_bmp)
                replaced = replaced or key_replaced or item_replaced
                sanitized[cleaned_key] = cleaned_item
            return sanitized, replaced

        return value, False

    def _dump_json_for_async_job(
        self,
        field_name: str,
        value: Any,
        default: str = "{}",
        *,
        strip_non_bmp: bool = False,
    ) -> str:
        if value is None:
            return default

        sanitized_value, replaced = self._sanitize_value_for_mysql(value, strip_non_bmp=strip_non_bmp)
        if isinstance(sanitized_value, str):
            dumped = sanitized_value
            original_length = len(str(value))
        else:
            dumped = json.dumps(sanitized_value, ensure_ascii=False)
            original_length = len(json.dumps(value, ensure_ascii=False, default=str))

        if replaced or len(dumped) != original_length:
            logger.warning(
                "[SQLAlchemyStorage] sanitized async job json field=%s original_len=%s sanitized_len=%s replaced=%s strip_non_bmp=%s",
                field_name,
                original_length,
                len(dumped),
                replaced,
                strip_non_bmp,
            )
        return dumped

    def _dumps(self, value: Any, default: str = "{}") -> str:
        if value is None:
            return default
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

    def _loads(self, value: str | None, fallback: Any) -> Any:
        if not value:
            return fallback
        try:
            return json.loads(value)
        except Exception:
            return fallback

    def _truncate_text(self, value: Any, limit: int) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if len(text) <= limit:
            return text
        return f"{text[:limit]}..."

    def _build_async_job_request_snapshot(self, job_data: dict[str, Any]) -> dict[str, Any]:
        request_payload = job_data.get("request_json")
        if not isinstance(request_payload, dict):
            request_payload = {}

        try:
            messages = request_payload.get("messages")
            files = request_payload.get("files")

            messages_preview: list[dict[str, str]] = []
            if isinstance(messages, list):
                for message in messages[:5]:
                    if not isinstance(message, dict):
                        continue
                    messages_preview.append(
                        {
                            "role": self._truncate_text(message.get("role") or "", 20),
                            "content": self._truncate_text(message.get("content") or "", JOB_PAYLOAD_PREVIEW_LIMIT),
                        }
                    )

            file_names: list[str] = []
            if isinstance(files, list):
                for file_item in files[:10]:
                    if isinstance(file_item, dict):
                        file_names.append(
                            self._truncate_text(
                                file_item.get("fileName")
                                or file_item.get("name")
                                or file_item.get("filename")
                                or "未命名文件",
                                120,
                            )
                        )

            snapshot = {
                "jobType": self._truncate_text(job_data.get("job_type") or request_payload.get("jobType") or "", 64),
                "customerId": self._truncate_text(job_data.get("customer_id") or request_payload.get("customerId") or request_payload.get("customer_id") or "", 64),
                "customerName": self._truncate_text(request_payload.get("customerName") or request_payload.get("customer_name") or "", 255),
                "username": self._truncate_text(job_data.get("username") or request_payload.get("username") or "", 128),
                "fileNames": file_names,
                "fileCount": len(files) if isinstance(files, list) else 0,
                "messagesPreview": messages_preview,
                "messageCount": len(messages) if isinstance(messages, list) else 0,
                "createdFrom": self._truncate_text(request_payload.get("createdFrom") or request_payload.get("source") or "async_jobs", 64),
            }
            return snapshot
        except Exception as exc:
            logger.warning("[SQLAlchemyStorage] Failed to sanitize async job request payload: %s", exc)
            return {
                "jobType": self._truncate_text(job_data.get("job_type") or "", 64),
                "customerId": self._truncate_text(job_data.get("customer_id") or "", 64),
                "customerName": self._truncate_text((request_payload or {}).get("customerName") or "", 255),
                "username": self._truncate_text(job_data.get("username") or "", 128),
                "fileNames": [],
                "fileCount": 0,
                "messagesPreview": [],
                "messageCount": 0,
                "createdFrom": "async_jobs",
            }

    def _resolve_async_job_customer_name(self, row: AsyncJobRecord) -> str:
        request_payload = self._loads(row.request_json, {}) or {}
        if isinstance(request_payload, dict):
            customer_name = (
                request_payload.get("customerName")
                or request_payload.get("customer_name")
                or request_payload.get("title")
                or ""
            )
            if isinstance(customer_name, str) and customer_name.strip():
                return customer_name.strip()

        result_payload = self._loads(row.result_json, {}) or {}
        if isinstance(result_payload, dict):
            customer_name = (
                result_payload.get("customerName")
                or result_payload.get("customer_name")
                or ""
            )
            if isinstance(customer_name, str) and customer_name.strip():
                return customer_name.strip()

        customer_id = row.customer_id or ""
        if customer_id:
            with self._session_factory() as lookup_db:
                customer_row = self._select_first_with_warning(
                    lookup_db,
                    select(Customer).where(Customer.customer_id == customer_id),
                    table_name="customers",
                    customer_id=customer_id,
                )
                if customer_row and customer_row.name:
                    return customer_row.name

        return ""

    def _parse_job_time(self, value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        try:
            if normalized.endswith("Z"):
                normalized = normalized.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    def _mark_stale_async_job_row(self, row: AsyncJobRecord, reason: str) -> bool:
        if row.status not in {"running", "retrying"}:
            return False
        if row.finished_at:
            return False
        row.status = "failed"
        row.progress_message = "任务执行超时"
        row.error_message = reason
        row.finished_at = datetime.now(timezone.utc).isoformat()
        return True

    def _reconcile_async_job_row(self, row: AsyncJobRecord) -> bool:
        if row.status not in {"running", "retrying"}:
            return False
        started_at = self._parse_job_time(row.started_at) or (
            row.created_at.replace(tzinfo=timezone.utc) if row.created_at else None
        )
        if started_at is None:
            return False
        elapsed_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        if elapsed_seconds <= JOB_STALE_TIMEOUT_SECONDS:
            return False
        reason = (
            f"任务执行超时，已运行超过 {JOB_STALE_TIMEOUT_SECONDS} 秒，请稍后重试。"
            if row.status == "running"
            else f"任务重试超时，已运行超过 {JOB_STALE_TIMEOUT_SECONDS} 秒，请稍后重试。"
        )
        logger.warning(
            "[SQLAlchemyStorage] reconcile stale async job job_id=%s status=%s started_at=%s timeout_seconds=%s",
            row.job_id,
            row.status,
            row.started_at or (row.created_at.isoformat() if row.created_at else ""),
            JOB_STALE_TIMEOUT_SECONDS,
        )
        return self._mark_stale_async_job_row(row, reason)

    def _select_first_with_warning(
        self,
        db: Any,
        stmt: Any,
        *,
        table_name: str,
        customer_id: str = "",
    ) -> Any:
        rows = db.execute(stmt.limit(2)).scalars().all()
        if len(rows) > 1:
            logger.warning(
                "[SQLAlchemyStorage] duplicate rows detected table=%s customer_id=%s row_count=%s",
                table_name,
                customer_id or "",
                len(rows),
            )
        return rows[0] if rows else None

    def _row_to_customer(self, row: Customer) -> dict[str, Any]:
        return {
            "id": row.id,
            "customer_id": row.customer_id,
            "name": row.name or "",
            "phone": row.phone or "",
            "id_card": row.id_card or "",
            "loan_amount": row.loan_amount,
            "loan_purpose": row.loan_purpose or "",
            "income_source": row.income_source or "",
            "monthly_income": row.monthly_income,
            "credit_score": row.credit_score,
            "status": row.status or "new",
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
            "uploader": row.uploader or "",
            "owner": row.uploader or "",
            "created_by": row.uploader or "",
            "username": row.uploader or "",
            "upload_time": row.upload_time or "",
            "customer_type": row.customer_type or "enterprise",
        }

    def _row_to_document(self, row: Document) -> dict[str, Any]:
        return {
            "doc_id": row.doc_id,
            "customer_id": row.customer_id,
            "file_name": row.file_name or "",
            "file_path": row.file_path or "",
            "file_type": row.file_type or "",
            "file_size": row.file_size or 0,
            "upload_time": row.upload_time.isoformat() if row.upload_time else "",
            "feishu_file_id": row.feishu_file_id or "",
            "uploader": row.uploader or "",
        }

    def _row_to_extraction(self, row: Extraction) -> dict[str, Any]:
        return {
            "extraction_id": row.extraction_id,
            "doc_id": row.doc_id,
            "customer_id": row.customer_id,
            "extraction_type": row.extraction_type or "",
            "extracted_data": self._loads(row.extracted_data, {}),
            "confidence": row.confidence or 0.0,
            "created_at": row.created_at.isoformat() if row.created_at else "",
        }

    def _row_to_profile(self, row: CustomerProfile) -> dict[str, Any]:
        return {
            "customer_id": row.customer_id,
            "title": row.title or "",
            "markdown_content": row.markdown_content or "",
            "source_mode": row.source_mode or "auto",
            "source_snapshot": self._loads(row.source_snapshot_json, {}),
            "rag_source_priority": self._loads(row.rag_source_priority_json, []),
            "risk_report_schema": self._loads(row.risk_report_schema_json, {}),
            "version": row.version or 1,
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def _row_to_scheme_snapshot(self, row: CustomerSchemeSnapshot) -> dict[str, Any]:
        return {
            "snapshot_id": row.snapshot_id,
            "customer_id": row.customer_id,
            "customer_name": row.customer_name or "",
            "summary_markdown": row.summary_markdown or "",
            "raw_result": row.raw_result or "",
            "source": row.source or "",
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def _row_to_chunk(self, row: CustomerDocumentChunk) -> dict[str, Any]:
        return {
            "chunk_id": row.chunk_id,
            "customer_id": row.customer_id,
            "source_type": row.source_type or "",
            "source_id": row.source_id or "",
            "chunk_index": row.chunk_index or 0,
            "chunk_text": row.chunk_text or "",
            "embedding": self._loads(row.embedding_json, []),
            "metadata": self._loads(row.metadata_json, {}),
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def _row_to_risk_report(self, row: CustomerRiskReport) -> dict[str, Any]:
        return {
            "report_id": row.report_id,
            "customer_id": row.customer_id,
            "profile_version": row.profile_version or 1,
            "profile_updated_at": row.profile_updated_at or "",
            "generated_at": row.generated_at or "",
            "report_json": self._loads(row.report_json, {}),
            "report_markdown": row.report_markdown or "",
            "created_at": row.created_at.isoformat() if row.created_at else "",
        }

    def _row_to_application(self, row: SavedApplicationRecord) -> dict[str, Any]:
        return {
            "id": row.application_id,
            "versionGroupId": row.version_group_id or "",
            "previousApplicationId": row.previous_application_id or "",
            "versionNo": row.version_no or 1,
            "customerName": row.customer_name or "",
            "customerId": row.customer_id or "",
            "loanType": row.loan_type or "enterprise",
            "applicationData": self._loads(row.application_data, {}),
            "savedAt": row.saved_at or "",
            "ownerUsername": row.owner_username or "",
            "source": row.source or "manual",
            "stale": bool(row.stale),
            "stale_reason": row.stale_reason or "",
            "stale_at": row.stale_at or "",
            "profile_version": row.profile_version or 1,
            "profile_updated_at": row.profile_updated_at or "",
        }

    def _row_to_chat_session(self, row: ChatSession) -> dict[str, Any]:
        return {
            "session_id": row.session_id,
            "username": row.username or "",
            "customer_id": row.customer_id or "",
            "customer_name": row.customer_name or "",
            "title": row.title or "",
            "last_message_preview": row.last_message_preview or "",
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def _row_to_chat_message(self, row: ChatMessageRecord) -> dict[str, Any]:
        return {
            "message_id": row.message_id,
            "session_id": row.session_id,
            "role": row.role or "user",
            "content": row.content or "",
            "sequence": row.sequence or 0,
            "created_at": row.created_at.isoformat() if row.created_at else "",
        }

    def _row_to_product_cache(self, row: ProductCacheEntry) -> dict[str, Any]:
        return {
            "cache_key": row.cache_key,
            "content": row.content or "",
            "last_updated": row.last_updated or "",
            "source": row.source or "wiki",
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def _row_to_async_job(self, row: AsyncJobRecord) -> dict[str, Any]:
        return {
            "job_id": row.job_id,
            "job_type": row.job_type or "",
            "customer_id": row.customer_id or "",
            "customer_name": self._resolve_async_job_customer_name(row),
            "username": row.username or "",
            "status": row.status or "pending",
            "progress_message": row.progress_message or "",
            "request_json": self._loads(row.request_json, {}),
            "execution_payload_json": self._loads(row.execution_payload_json, {}),
            "result_json": self._loads(row.result_json, {}),
            "error_message": normalize_async_job_error_message(row.error_message),
            "celery_task_id": row.celery_task_id or "",
            "worker_name": row.worker_name or "",
            "created_at": row.created_at.isoformat() if row.created_at else "",
            "started_at": row.started_at or "",
            "finished_at": row.finished_at or "",
        }

    async def create_customer(self, customer_data: dict) -> dict:
        with self._session_factory() as db:
            row = Customer(**{k: v for k, v in customer_data.items() if hasattr(Customer, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_customer(row)

    async def get_customer(self, customer_id: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(Customer).where(Customer.customer_id == customer_id),
                table_name="customers",
                customer_id=customer_id,
            )
            return self._row_to_customer(row) if row else None

    async def update_customer(self, customer_id: str, updates: dict[str, Any]) -> bool:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(Customer).where(Customer.customer_id == customer_id),
                table_name="customers",
                customer_id=customer_id,
            )
            if not row:
                return False
            for key, value in updates.items():
                if hasattr(row, key):
                    setattr(row, key, value)
            db.commit()
            return True

    async def list_customers(self) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(select(Customer).order_by(desc(Customer.updated_at), desc(Customer.id))).scalars().all()
            return [self._row_to_customer(row) for row in rows]

    async def customer_has_document_uploader(self, customer_id: str, username: str) -> bool:
        if not customer_id or not username:
            return False
        with self._session_factory() as db:
            row = db.execute(
                select(Document.id)
                .where(Document.customer_id == customer_id)
                .where(Document.uploader == username)
                .limit(1)
            ).first()
            return row is not None

    async def delete_customer(self, customer_id: str) -> bool:
        with self._session_factory() as db:
            try:
                row = self._select_first_with_warning(
                    db,
                    select(Customer).where(Customer.customer_id == customer_id),
                    table_name="customers",
                    customer_id=customer_id,
                )
                if not row:
                    return False

                document_ids = db.execute(
                    select(Document.doc_id).where(Document.customer_id == customer_id)
                ).scalars().all()

                chat_session_ids = db.execute(
                    select(ChatSession.session_id).where(ChatSession.customer_id == customer_id)
                ).scalars().all()

                deleted_counts: dict[str, int] = {
                    "chat_messages": 0,
                    "chat_sessions": 0,
                    "saved_applications": 0,
                    "customer_risk_reports": 0,
                    "customer_scheme_snapshots": 0,
                    "customer_profile": 0,
                    "customer_document_chunks": 0,
                    "extractions": 0,
                    "documents": 0,
                    "customer": 0,
                }

                if chat_session_ids:
                    message_result = db.execute(
                        delete(ChatMessageRecord).where(ChatMessageRecord.session_id.in_(chat_session_ids))
                    )
                    deleted_counts["chat_messages"] = message_result.rowcount or 0
                    db.flush()

                session_result = db.execute(
                    delete(ChatSession).where(ChatSession.customer_id == customer_id)
                )
                deleted_counts["chat_sessions"] = session_result.rowcount or 0
                db.flush()

                saved_applications_result = db.execute(
                    delete(SavedApplicationRecord).where(SavedApplicationRecord.customer_id == customer_id)
                )
                deleted_counts["saved_applications"] = saved_applications_result.rowcount or 0
                db.flush()

                risk_reports_result = db.execute(
                    delete(CustomerRiskReport).where(CustomerRiskReport.customer_id == customer_id)
                )
                deleted_counts["customer_risk_reports"] = risk_reports_result.rowcount or 0
                db.flush()

                scheme_snapshots_result = db.execute(
                    delete(CustomerSchemeSnapshot).where(CustomerSchemeSnapshot.customer_id == customer_id)
                )
                deleted_counts["customer_scheme_snapshots"] = scheme_snapshots_result.rowcount or 0
                db.flush()

                profile_result = db.execute(
                    delete(CustomerProfile).where(CustomerProfile.customer_id == customer_id)
                )
                deleted_counts["customer_profile"] = profile_result.rowcount or 0
                db.flush()

                chunks_result = db.execute(
                    delete(CustomerDocumentChunk).where(CustomerDocumentChunk.customer_id == customer_id)
                )
                deleted_counts["customer_document_chunks"] = chunks_result.rowcount or 0
                db.flush()

                if document_ids:
                    extraction_result = db.execute(
                        delete(Extraction).where(Extraction.doc_id.in_(document_ids))
                    )
                    deleted_counts["extractions"] += extraction_result.rowcount or 0
                    db.flush()

                extraction_by_customer_result = db.execute(
                    delete(Extraction).where(Extraction.customer_id == customer_id)
                )
                deleted_counts["extractions"] += extraction_by_customer_result.rowcount or 0
                db.flush()

                document_result = db.execute(
                    delete(Document).where(Document.customer_id == customer_id)
                )
                deleted_counts["documents"] = document_result.rowcount or 0
                db.flush()

                db.delete(row)
                deleted_counts["customer"] = 1
                db.flush()

                db.commit()
                logger.info(
                    "[SQLAlchemyStorage] Deleted customer customer_id=%s deleted=%s",
                    customer_id,
                    deleted_counts,
                )
                return True
            except SQLAlchemyError as exc:
                db.rollback()
                logger.error(
                    "[SQLAlchemyStorage] Failed to delete customer customer_id=%s error=%s",
                    customer_id,
                    exc,
                    exc_info=True,
                )
                raise

    async def save_document(self, doc_data: dict) -> dict:
        with self._session_factory() as db:
            row = Document(**{k: v for k, v in doc_data.items() if hasattr(Document, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_document(row)

    async def get_document(self, doc_id: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(Document).where(Document.doc_id == doc_id),
                table_name="documents",
            )
            return self._row_to_document(row) if row else None

    async def list_documents(self, customer_id: str) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(
                select(Document).where(Document.customer_id == customer_id).order_by(desc(Document.upload_time), desc(Document.id))
            ).scalars().all()
            return [self._row_to_document(row) for row in rows]

    async def delete_document(self, doc_id: str) -> bool:
        with self._session_factory() as db:
            try:
                row = self._select_first_with_warning(
                    db,
                    select(Document).where(Document.doc_id == doc_id),
                    table_name="documents",
                )
                if not row:
                    return False

                extraction_delete_result = db.execute(
                    delete(Extraction).where(Extraction.doc_id == doc_id)
                )
                deleted_extractions = extraction_delete_result.rowcount or 0
                db.flush()

                db.delete(row)
                db.flush()
                db.commit()

                logger.info(
                    "[SQLAlchemyStorage] Deleted document doc_id=%s extractions_deleted=%s document_deleted=true",
                    doc_id,
                    deleted_extractions,
                )
                return True
            except SQLAlchemyError as exc:
                db.rollback()
                logger.error(
                    "[SQLAlchemyStorage] Failed to delete document doc_id=%s during replacement extractions_deleted_before_failure=%s error=%s",
                    doc_id,
                    locals().get("deleted_extractions", 0),
                    exc,
                    exc_info=True,
                )
                raise

    async def save_extraction(self, extraction_data: dict) -> dict:
        with self._session_factory() as db:
            payload = extraction_data.copy()
            payload["extracted_data"] = self._dumps(payload.get("extracted_data"), "{}")
            logger.info(
                "[Local Save] extracted_data length=%s document_type=%s",
                len(payload["extracted_data"]),
                payload.get("extraction_type") or "",
            )
            row = Extraction(**{k: v for k, v in payload.items() if hasattr(Extraction, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_extraction(row)

    async def get_extraction(self, extraction_id: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(Extraction).where(Extraction.extraction_id == extraction_id),
                table_name="extractions",
            )
            return self._row_to_extraction(row) if row else None

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(
                select(Extraction).where(Extraction.customer_id == customer_id).order_by(desc(Extraction.created_at), desc(Extraction.id))
            ).scalars().all()
            return [self._row_to_extraction(row) for row in rows]

    async def get_extractions_by_doc(self, doc_id: str) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(select(Extraction).where(Extraction.doc_id == doc_id).order_by(desc(Extraction.created_at))).scalars().all()
            return [self._row_to_extraction(row) for row in rows]

    async def update_extraction(self, extraction_id: str, field: str, value: str) -> bool:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(Extraction).where(Extraction.extraction_id == extraction_id),
                table_name="extractions",
            )
            if not row:
                return False
            data = self._loads(row.extracted_data, {})
            data[field] = value
            row.extracted_data = self._dumps(data, "{}")
            db.commit()
            return True

    async def get_customer_profile(self, customer_id: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(CustomerProfile).where(CustomerProfile.customer_id == customer_id),
                table_name="customer_profiles",
                customer_id=customer_id,
            )
            return self._row_to_profile(row) if row else None

    async def upsert_customer_profile(self, profile_data: dict) -> dict:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(CustomerProfile).where(CustomerProfile.customer_id == profile_data["customer_id"]),
                table_name="customer_profiles",
                customer_id=profile_data["customer_id"],
            )
            if row:
                row.title = profile_data.get("title") or row.title
                row.markdown_content = profile_data.get("markdown_content") or ""
                row.source_mode = profile_data.get("source_mode") or row.source_mode
                row.source_snapshot_json = self._dumps(profile_data.get("source_snapshot"), "{}")
                row.rag_source_priority_json = self._dumps(profile_data.get("rag_source_priority"), "[]")
                row.risk_report_schema_json = self._dumps(profile_data.get("risk_report_schema"), "{}")
                row.version = int(profile_data.get("version") or (row.version or 1))
            else:
                row = CustomerProfile(
                    customer_id=profile_data["customer_id"],
                    title=profile_data.get("title") or "",
                    markdown_content=profile_data.get("markdown_content") or "",
                    source_mode=profile_data.get("source_mode") or "auto",
                    source_snapshot_json=self._dumps(profile_data.get("source_snapshot"), "{}"),
                    rag_source_priority_json=self._dumps(profile_data.get("rag_source_priority"), "[]"),
                    risk_report_schema_json=self._dumps(profile_data.get("risk_report_schema"), "{}"),
                    version=int(profile_data.get("version") or 1),
                )
                db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_profile(row)

    async def delete_customer_profile(self, customer_id: str) -> bool:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(CustomerProfile).where(CustomerProfile.customer_id == customer_id),
                table_name="customer_profiles",
                customer_id=customer_id,
            )
            if not row:
                return False
            db.delete(row)
            db.commit()
            return True

    async def save_scheme_snapshot(self, snapshot_data: dict) -> dict:
        with self._session_factory() as db:
            row = CustomerSchemeSnapshot(
                snapshot_id=snapshot_data["snapshot_id"],
                customer_id=snapshot_data["customer_id"],
                customer_name=snapshot_data.get("customer_name") or "",
                summary_markdown=snapshot_data.get("summary_markdown") or "",
                raw_result=snapshot_data.get("raw_result") or "",
                source=snapshot_data.get("source") or "manual",
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_scheme_snapshot(row)

    async def get_latest_scheme_snapshot(self, customer_id: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(CustomerSchemeSnapshot)
                .where(CustomerSchemeSnapshot.customer_id == customer_id)
                .order_by(desc(CustomerSchemeSnapshot.updated_at), desc(CustomerSchemeSnapshot.id)),
                table_name="customer_scheme_snapshots",
                customer_id=customer_id,
            )
            return self._row_to_scheme_snapshot(row) if row else None

    async def replace_customer_chunks(self, customer_id: str, chunks: list[dict]) -> None:
        with self._session_factory() as db:
            old_rows = db.execute(select(CustomerDocumentChunk).where(CustomerDocumentChunk.customer_id == customer_id)).scalars().all()
            for row in old_rows:
                db.delete(row)
            for chunk in chunks:
                db.add(
                    CustomerDocumentChunk(
                        chunk_id=chunk["chunk_id"],
                        customer_id=chunk["customer_id"],
                        source_type=chunk.get("source_type") or "",
                        source_id=chunk.get("source_id") or "",
                        chunk_index=int(chunk.get("chunk_index") or 0),
                        chunk_text=chunk.get("chunk_text") or "",
                        embedding_json=self._dumps(chunk.get("embedding"), "[]"),
                        metadata_json=self._dumps(chunk.get("metadata"), "{}"),
                    )
                )
            db.commit()

    async def get_customer_chunks(self, customer_id: str) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(
                select(CustomerDocumentChunk)
                .where(CustomerDocumentChunk.customer_id == customer_id)
                .order_by(CustomerDocumentChunk.source_type, CustomerDocumentChunk.chunk_index)
            ).scalars().all()
            return [self._row_to_chunk(row) for row in rows]

    async def save_customer_risk_report(self, report_data: dict) -> dict:
        with self._session_factory() as db:
            report_id = report_data.get("report_id") or uuid.uuid4().hex
            logger.info(
                "[SQLAlchemyStorage] saving customer risk report report_id=%s customer_id=%s generated_at=%s",
                report_id,
                report_data.get("customer_id") or "",
                report_data.get("generated_at") or "",
            )
            row = CustomerRiskReport(
                report_id=report_id,
                customer_id=report_data["customer_id"],
                profile_version=int(report_data.get("profile_version") or 1),
                profile_updated_at=report_data.get("profile_updated_at") or "",
                generated_at=report_data.get("generated_at") or "",
                report_json=self._dumps(report_data.get("report_json"), "{}"),
                report_markdown=report_data.get("report_markdown") or "",
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_risk_report(row)

    async def list_customer_risk_reports(self, customer_id: str, limit: int = 5) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(
                select(CustomerRiskReport)
                .where(CustomerRiskReport.customer_id == customer_id)
                .order_by(desc(CustomerRiskReport.generated_at), desc(CustomerRiskReport.id))
                .limit(limit)
            ).scalars().all()
            return [self._row_to_risk_report(row) for row in rows]

    async def get_latest_customer_risk_report(self, customer_id: str) -> dict | None:
        reports = await self.list_customer_risk_reports(customer_id, limit=1)
        return reports[0] if reports else None

    async def save_application_record(self, application_data: dict[str, Any]) -> dict[str, Any]:
        with self._session_factory() as db:
            row = SavedApplicationRecord(
                application_id=application_data["id"],
                version_group_id=application_data.get("versionGroupId") or "",
                previous_application_id=application_data.get("previousApplicationId") or "",
                version_no=int(application_data.get("versionNo") or 1),
                customer_name=application_data.get("customerName") or "",
                customer_id=application_data.get("customerId") or "",
                loan_type=application_data.get("loanType") or "enterprise",
                application_data=self._dumps(application_data.get("applicationData"), "{}"),
                saved_at=application_data.get("savedAt") or "",
                owner_username=application_data.get("ownerUsername") or "",
                source=application_data.get("source") or "manual",
                stale=1 if application_data.get("stale") else 0,
                stale_reason=application_data.get("stale_reason") or "",
                stale_at=application_data.get("stale_at") or "",
                profile_version=int(application_data.get("profile_version") or 1),
                profile_updated_at=application_data.get("profile_updated_at") or "",
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_application(row)

    async def list_saved_applications(self, customer_id: str | None = None) -> list[dict[str, Any]]:
        with self._session_factory() as db:
            stmt = select(SavedApplicationRecord).order_by(desc(SavedApplicationRecord.saved_at), desc(SavedApplicationRecord.id))
            if customer_id:
                stmt = stmt.where(SavedApplicationRecord.customer_id == customer_id)
            rows = db.execute(stmt).scalars().all()
            return [self._row_to_application(row) for row in rows]

    async def get_saved_application(self, application_id: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(SavedApplicationRecord).where(SavedApplicationRecord.application_id == application_id),
                table_name="saved_application_records",
            )
            return self._row_to_application(row) if row else None

    async def delete_saved_application(self, application_id: str) -> bool:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(SavedApplicationRecord).where(SavedApplicationRecord.application_id == application_id),
                table_name="saved_application_records",
            )
            if not row:
                return False
            db.delete(row)
            db.commit()
            return True

    async def mark_customer_applications_stale(
        self,
        customer_id: str,
        reason: str,
        stale_at: str,
        profile_version: int,
        profile_updated_at: str,
    ) -> int:
        with self._session_factory() as db:
            rows = db.execute(
                select(SavedApplicationRecord).where(
                    SavedApplicationRecord.customer_id == customer_id,
                    SavedApplicationRecord.stale == 0,
                )
            ).scalars().all()
            updated = 0
            for row in rows:
                row.stale = 1
                row.stale_reason = reason
                row.stale_at = stale_at
                row.profile_version = profile_version
                row.profile_updated_at = profile_updated_at or ""
                updated += 1
            db.commit()
            return updated

    async def create_chat_session(self, session_data: dict[str, Any]) -> dict[str, Any]:
        with self._session_factory() as db:
            row = ChatSession(
                session_id=session_data["session_id"],
                username=session_data.get("username") or "",
                customer_id=session_data.get("customer_id") or "",
                customer_name=session_data.get("customer_name") or "",
                title=session_data.get("title") or "",
                last_message_preview=session_data.get("last_message_preview") or "",
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_chat_session(row)

    async def get_chat_session(self, session_id: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(ChatSession).where(ChatSession.session_id == session_id),
                table_name="chat_sessions",
            )
            return self._row_to_chat_session(row) if row else None

    async def list_chat_sessions(
        self, username: str | None = None, customer_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        with self._session_factory() as db:
            stmt = select(ChatSession).order_by(desc(ChatSession.updated_at), desc(ChatSession.id)).limit(limit)
            if username:
                stmt = stmt.where(ChatSession.username == username)
            if customer_id:
                stmt = stmt.where(ChatSession.customer_id == customer_id)
            rows = db.execute(stmt).scalars().all()
            return [self._row_to_chat_session(row) for row in rows]

    async def save_chat_message(self, message_data: dict[str, Any]) -> dict[str, Any]:
        with self._session_factory() as db:
            row = ChatMessageRecord(
                message_id=message_data["message_id"],
                session_id=message_data["session_id"],
                role=message_data.get("role") or "user",
                content=message_data.get("content") or "",
                sequence=int(message_data.get("sequence") or 0),
            )
            db.add(row)

            session_row = self._select_first_with_warning(
                db,
                select(ChatSession).where(ChatSession.session_id == message_data["session_id"]),
                table_name="chat_sessions",
            )
            if session_row:
                preview = (message_data.get("content") or "").strip()
                session_row.last_message_preview = preview[:500]
            db.commit()
            db.refresh(row)
            return self._row_to_chat_message(row)

    async def get_chat_messages(self, session_id: str) -> list[dict[str, Any]]:
        with self._session_factory() as db:
            rows = db.execute(
                select(ChatMessageRecord)
                .where(ChatMessageRecord.session_id == session_id)
                .order_by(ChatMessageRecord.sequence, ChatMessageRecord.id)
            ).scalars().all()
            return [self._row_to_chat_message(row) for row in rows]

    async def upsert_product_cache_entry(self, cache_data: dict[str, Any]) -> dict[str, Any]:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_data["cache_key"]),
                table_name="product_cache_entries",
            )
            if row:
                row.content = cache_data.get("content") or ""
                row.last_updated = cache_data.get("last_updated") or row.last_updated or ""
                row.source = cache_data.get("source") or row.source or "wiki"
            else:
                row = ProductCacheEntry(
                    cache_key=cache_data["cache_key"],
                    content=cache_data.get("content") or "",
                    last_updated=cache_data.get("last_updated") or "",
                    source=cache_data.get("source") or "wiki",
                )
                db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_product_cache(row)

    async def create_async_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        with self._session_factory() as db:
            request_snapshot = self._build_async_job_request_snapshot(job_data)
            row = AsyncJobRecord(
                job_id=job_data["job_id"],
                job_type=job_data.get("job_type") or "chat_extract",
                customer_id=job_data.get("customer_id") or "",
                username=job_data.get("username") or "",
                status=job_data.get("status") or "pending",
                progress_message=job_data.get("progress_message") or "",
                request_json=self._dump_json_for_async_job("request_json", request_snapshot, "{}"),
                execution_payload_json=self._dump_json_for_async_job("execution_payload_json", job_data.get("execution_payload_json"), "{}"),
                result_json=self._dump_json_for_async_job("result_json", job_data.get("result_json"), "{}"),
                error_message=normalize_async_job_error_message(job_data.get("error_message") or ""),
                celery_task_id=job_data.get("celery_task_id") or "",
                worker_name=job_data.get("worker_name") or "",
                started_at=job_data.get("started_at") or "",
                finished_at=job_data.get("finished_at") or "",
            )
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_async_job(row)

    async def get_async_job(self, job_id: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id),
                table_name="async_jobs",
            )
            if row and self._reconcile_async_job_row(row):
                db.commit()
                db.refresh(row)
            return self._row_to_async_job(row) if row else None

    async def list_async_jobs(self, username: str, limit: int = 10) -> list[dict[str, Any]]:
        with self._session_factory() as db:
            rows = db.execute(
                select(AsyncJobRecord)
                .where(AsyncJobRecord.username == username)
                .order_by(desc(AsyncJobRecord.created_at))
                .limit(limit)
            ).scalars().all()
            stale_updated = False
            for row in rows:
                stale_updated = self._reconcile_async_job_row(row) or stale_updated
            if stale_updated:
                db.commit()
                for row in rows:
                    db.refresh(row)
            return [self._row_to_async_job(row) for row in rows]

    async def update_async_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id),
                table_name="async_jobs",
            )
            if not row:
                return None

            if "job_type" in updates:
                row.job_type = updates["job_type"] or row.job_type
            if "customer_id" in updates:
                row.customer_id = updates["customer_id"] or ""
            if "username" in updates:
                row.username = updates["username"] or row.username
            if "status" in updates:
                row.status = updates["status"] or row.status
            if "progress_message" in updates:
                row.progress_message = updates["progress_message"] or ""
            if "request_json" in updates:
                request_snapshot = self._build_async_job_request_snapshot(
                    {
                        "request_json": updates.get("request_json"),
                        "job_type": row.job_type,
                        "customer_id": row.customer_id,
                        "username": row.username,
                    }
                )
                row.request_json = self._dump_json_for_async_job("request_json", request_snapshot, "{}")
            if "execution_payload_json" in updates:
                row.execution_payload_json = self._dump_json_for_async_job(
                    "execution_payload_json",
                    updates.get("execution_payload_json"),
                    "{}",
                )
            if "result_json" in updates:
                row.result_json = self._dump_json_for_async_job("result_json", updates.get("result_json"), "{}")
            if "error_message" in updates:
                row.error_message = normalize_async_job_error_message(updates["error_message"])
            if "celery_task_id" in updates:
                row.celery_task_id = updates["celery_task_id"] or ""
            if "worker_name" in updates:
                row.worker_name = updates["worker_name"] or ""
            if "started_at" in updates:
                row.started_at = updates["started_at"] or ""
            if "finished_at" in updates:
                row.finished_at = updates["finished_at"] or ""

            try:
                db.commit()
            except DataError as exc:
                db.rollback()
                logger.exception(
                    "[SQLAlchemyStorage] async job update failed job_id=%s status=%s has_result_json=%s error=%s",
                    job_id,
                    updates.get("status") or row.status,
                    "result_json" in updates,
                    exc,
                )
                if "result_json" in updates:
                    fallback_error = normalize_async_job_error_message(str(exc))
                    row = self._select_first_with_warning(
                        db,
                        select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id),
                        table_name="async_jobs",
                    )
                    if not row:
                        raise

                    row.status = "failed"
                    row.progress_message = "任务结果保存失败"
                    row.error_message = fallback_error
                    row.finished_at = updates.get("finished_at") or datetime.now(timezone.utc).isoformat()
                    row.result_json = self._dump_json_for_async_job(
                        "result_json",
                        updates.get("result_json"),
                        "{}",
                        strip_non_bmp=True,
                    )
                    try:
                        db.commit()
                    except Exception:
                        db.rollback()
                        row = self._select_first_with_warning(
                            db,
                            select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id),
                            table_name="async_jobs",
                        )
                        if row:
                            row.status = "failed"
                            row.progress_message = "任务结果保存失败"
                            row.error_message = fallback_error
                            row.finished_at = datetime.now(timezone.utc).isoformat()
                            row.result_json = "{}"
                            db.commit()
                            db.refresh(row)
                            return self._row_to_async_job(row)
                        raise
                else:
                    raise

            db.refresh(row)
            return self._row_to_async_job(row)

    async def get_async_job_execution_payload(self, job_id: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id),
                table_name="async_jobs",
            )
            if not row:
                return None
            payload = self._loads(row.execution_payload_json, {})
            if not isinstance(payload, dict) or not payload:
                logger.warning(
                    "[SQLAlchemyStorage] async job execution payload missing or empty job_id=%s",
                    job_id,
                )
                return None
            return payload

    async def set_async_job_execution_payload(
        self,
        job_id: str,
        execution_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        return await self.update_async_job(
            job_id,
            {
                "execution_payload_json": execution_payload,
            },
        )

    async def mark_async_job_dispatched(
        self,
        job_id: str,
        celery_task_id: str,
        worker_name: str = "",
    ) -> dict[str, Any] | None:
        return await self.update_async_job(
            job_id,
            {
                "celery_task_id": celery_task_id,
                "worker_name": worker_name,
            },
        )

    async def delete_async_job(self, job_id: str, username: str | None = None) -> bool:
        with self._session_factory() as db:
            stmt = select(AsyncJobRecord).where(AsyncJobRecord.job_id == job_id)
            if username:
                stmt = stmt.where(AsyncJobRecord.username == username)
            row = self._select_first_with_warning(
                db,
                stmt,
                table_name="async_jobs",
            )
            if not row:
                return False
            db.delete(row)
            db.commit()
            return True

    async def get_product_cache_entry(self, cache_key: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_key),
                table_name="product_cache_entries",
            )
            return self._row_to_product_cache(row) if row else None

    async def list_product_cache_entries(self) -> list[dict[str, Any]]:
        with self._session_factory() as db:
            rows = db.execute(
                select(ProductCacheEntry).order_by(ProductCacheEntry.cache_key, desc(ProductCacheEntry.updated_at))
            ).scalars().all()
            return [self._row_to_product_cache(row) for row in rows]

    async def get_table_fields(self) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(select(TableField).order_by(TableField.field_order, TableField.id)).scalars().all()
            return [
                {
                    "field_id": row.field_id,
                    "field_name": row.field_name,
                    "field_key": row.field_key,
                    "doc_type": row.doc_type or "",
                    "field_order": row.field_order or 0,
                    "editable": bool(row.editable),
                    "created_at": row.created_at.isoformat() if row.created_at else "",
                }
                for row in rows
            ]

    async def update_table_field(self, field_id: str, field_name: str) -> bool:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(TableField).where(TableField.field_id == field_id),
                table_name="table_fields",
            )
            if not row:
                return False
            row.field_name = field_name
            db.commit()
            return True

    async def get_field_by_doc_type(self, doc_type: str) -> dict | None:
        with self._session_factory() as db:
            row = self._select_first_with_warning(
                db,
                select(TableField).where(TableField.doc_type == doc_type),
                table_name="table_fields",
            )
            if not row:
                return None
            return {
                "field_id": row.field_id,
                "field_name": row.field_name,
                "field_key": row.field_key,
                "doc_type": row.doc_type or "",
                "field_order": row.field_order or 0,
                "editable": bool(row.editable),
            }

    async def get_customer_field_data(self, customer_id: str) -> dict[str, Any]:
        customer = await self.get_customer(customer_id)
        if not customer:
            return {}
        extractions = await self.get_extractions_by_customer(customer_id)
        return {
            "customer": customer,
            "extractions": extractions,
        }
