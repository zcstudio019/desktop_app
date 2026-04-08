"""SQLAlchemy-backed business storage service."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import desc, select, update

from backend.database import Base, SessionLocal, engine
from backend.db_models import (
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


class SQLAlchemyStorageService:
    """Storage service backed by SQLAlchemy models for MySQL/RDS usage."""

    def __init__(self) -> None:
        self._session_factory = SessionLocal
        Base.metadata.create_all(
            bind=engine,
            tables=[ChatSession.__table__, ChatMessageRecord.__table__, ProductCacheEntry.__table__],
            checkfirst=True,
        )

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

    def _row_to_customer(self, row: Customer) -> dict[str, Any]:
        return {
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

    async def create_customer(self, customer_data: dict) -> dict:
        with self._session_factory() as db:
            row = Customer(**{k: v for k, v in customer_data.items() if hasattr(Customer, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_customer(row)

    async def get_customer(self, customer_id: str) -> dict | None:
        with self._session_factory() as db:
            row = db.execute(select(Customer).where(Customer.customer_id == customer_id)).scalar_one_or_none()
            return self._row_to_customer(row) if row else None

    async def update_customer(self, customer_id: str, updates: dict[str, Any]) -> bool:
        with self._session_factory() as db:
            row = db.execute(select(Customer).where(Customer.customer_id == customer_id)).scalar_one_or_none()
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

    async def delete_customer(self, customer_id: str) -> bool:
        with self._session_factory() as db:
            row = db.execute(select(Customer).where(Customer.customer_id == customer_id)).scalar_one_or_none()
            if not row:
                return False
            db.execute(update(Document).where(Document.customer_id == customer_id).values(customer_id=customer_id))
            db.delete(row)
            db.commit()
            return True

    async def save_document(self, doc_data: dict) -> dict:
        with self._session_factory() as db:
            row = Document(**{k: v for k, v in doc_data.items() if hasattr(Document, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_document(row)

    async def get_document(self, doc_id: str) -> dict | None:
        with self._session_factory() as db:
            row = db.execute(select(Document).where(Document.doc_id == doc_id)).scalar_one_or_none()
            return self._row_to_document(row) if row else None

    async def list_documents(self, customer_id: str) -> list[dict]:
        with self._session_factory() as db:
            rows = db.execute(
                select(Document).where(Document.customer_id == customer_id).order_by(desc(Document.upload_time), desc(Document.id))
            ).scalars().all()
            return [self._row_to_document(row) for row in rows]

    async def delete_document(self, doc_id: str) -> bool:
        with self._session_factory() as db:
            row = db.execute(select(Document).where(Document.doc_id == doc_id)).scalar_one_or_none()
            if not row:
                return False
            extractions = db.execute(select(Extraction).where(Extraction.doc_id == doc_id)).scalars().all()
            for extraction in extractions:
                db.delete(extraction)
            db.delete(row)
            db.commit()
            return True

    async def save_extraction(self, extraction_data: dict) -> dict:
        with self._session_factory() as db:
            payload = extraction_data.copy()
            payload["extracted_data"] = self._dumps(payload.get("extracted_data"), "{}")
            row = Extraction(**{k: v for k, v in payload.items() if hasattr(Extraction, k)})
            db.add(row)
            db.commit()
            db.refresh(row)
            return self._row_to_extraction(row)

    async def get_extraction(self, extraction_id: str) -> dict | None:
        with self._session_factory() as db:
            row = db.execute(select(Extraction).where(Extraction.extraction_id == extraction_id)).scalar_one_or_none()
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
            row = db.execute(select(Extraction).where(Extraction.extraction_id == extraction_id)).scalar_one_or_none()
            if not row:
                return False
            data = self._loads(row.extracted_data, {})
            data[field] = value
            row.extracted_data = self._dumps(data, "{}")
            db.commit()
            return True

    async def get_customer_profile(self, customer_id: str) -> dict | None:
        with self._session_factory() as db:
            row = db.execute(select(CustomerProfile).where(CustomerProfile.customer_id == customer_id)).scalar_one_or_none()
            return self._row_to_profile(row) if row else None

    async def upsert_customer_profile(self, profile_data: dict) -> dict:
        with self._session_factory() as db:
            row = db.execute(select(CustomerProfile).where(CustomerProfile.customer_id == profile_data["customer_id"])).scalar_one_or_none()
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
            row = db.execute(select(CustomerProfile).where(CustomerProfile.customer_id == customer_id)).scalar_one_or_none()
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
            row = db.execute(
                select(CustomerSchemeSnapshot)
                .where(CustomerSchemeSnapshot.customer_id == customer_id)
                .order_by(desc(CustomerSchemeSnapshot.updated_at), desc(CustomerSchemeSnapshot.id))
            ).scalar_one_or_none()
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
            row = CustomerRiskReport(
                report_id=report_data["report_id"],
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
            row = db.execute(
                select(SavedApplicationRecord).where(SavedApplicationRecord.application_id == application_id)
            ).scalar_one_or_none()
            return self._row_to_application(row) if row else None

    async def delete_saved_application(self, application_id: str) -> bool:
        with self._session_factory() as db:
            row = db.execute(
                select(SavedApplicationRecord).where(SavedApplicationRecord.application_id == application_id)
            ).scalar_one_or_none()
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
            row = db.execute(select(ChatSession).where(ChatSession.session_id == session_id)).scalar_one_or_none()
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

            session_row = db.execute(
                select(ChatSession).where(ChatSession.session_id == message_data["session_id"])
            ).scalar_one_or_none()
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
            row = db.execute(
                select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_data["cache_key"])
            ).scalar_one_or_none()
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

    async def get_product_cache_entry(self, cache_key: str) -> dict[str, Any] | None:
        with self._session_factory() as db:
            row = db.execute(
                select(ProductCacheEntry).where(ProductCacheEntry.cache_key == cache_key)
            ).scalar_one_or_none()
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
            row = db.execute(select(TableField).where(TableField.field_id == field_id)).scalar_one_or_none()
            if not row:
                return False
            row.field_name = field_name
            db.commit()
            return True

    async def get_field_by_doc_type(self, doc_type: str) -> dict | None:
        with self._session_factory() as db:
            row = db.execute(select(TableField).where(TableField.doc_type == doc_type).limit(1)).scalar_one_or_none()
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
