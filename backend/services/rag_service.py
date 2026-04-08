"""Customer-scoped RAG MVP service."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from services.ai_service import AIService
from utils.json_parser import parse_json

from .embedding_service import EmbeddingService
from .markdown_profile_service import get_or_create_customer_profile

logger = logging.getLogger(__name__)

CHUNK_SIZE = 500
CHUNK_OVERLAP = 80
TOP_K = 5

SOURCE_PRIORITY = {
    "customer_profile_markdown": 4,
    "parsed_document_text": 3,
    "scheme_match_summary": 2,
    "application_summary": 1,
}


class RagService:
    def __init__(self, ai_service: AIService | None = None, embedding_service: EmbeddingService | None = None):
        self.ai_service = ai_service or AIService()
        self.embedding_service = embedding_service or EmbeddingService()

    def _split_text(self, text: str) -> list[str]:
        normalized = (text or "").strip()
        if not normalized:
            return []
        chunks: list[str] = []
        start = 0
        while start < len(normalized):
            end = min(len(normalized), start + CHUNK_SIZE)
            chunks.append(normalized[start:end])
            if end >= len(normalized):
                break
            start = max(0, end - CHUNK_OVERLAP)
        return chunks

    def _serialize_data(self, data: Any) -> str:
        if data is None:
            return ""
        if isinstance(data, str):
            return data
        return json.dumps(data, ensure_ascii=False, indent=2)

    async def _build_source_documents(self, storage_service: Any, customer_id: str) -> list[dict[str, Any]]:
        customer = await storage_service.get_customer(customer_id)
        if not customer:
            raise ValueError("customer not found")

        profile, _ = await get_or_create_customer_profile(storage_service, customer_id)
        documents: list[dict[str, Any]] = [
            {"source_type": "customer_profile_markdown", "source_id": customer_id, "text": profile.get("markdown_content") or ""}
        ]

        extractions = await storage_service.get_extractions_by_customer(customer_id)
        for extraction in extractions:
            documents.append(
                {
                    "source_type": "parsed_document_text",
                    "source_id": extraction.get("extraction_id") or "",
                    "text": self._serialize_data(extraction.get("extracted_data") or {}),
                }
            )

        scheme_snapshot = await storage_service.get_latest_scheme_snapshot(customer_id)
        if scheme_snapshot:
            documents.append(
                {
                    "source_type": "scheme_match_summary",
                    "source_id": scheme_snapshot.get("snapshot_id") or customer_id,
                    "text": scheme_snapshot.get("summary_markdown") or scheme_snapshot.get("raw_result") or "",
                }
            )

        applications = await storage_service.list_saved_applications(customer_id=customer_id)
        active_apps = [item for item in applications if not item.get("stale")]
        if active_apps:
            latest_app = active_apps[0]
            documents.append(
                {
                    "source_type": "application_summary",
                    "source_id": latest_app.get("id") or "",
                    "text": self._serialize_data(latest_app.get("applicationData") or {}),
                }
            )
        return documents

    async def rebuild_customer_index(self, storage_service: Any, customer_id: str) -> int:
        source_documents = await self._build_source_documents(storage_service, customer_id)
        stored_chunks: list[dict[str, Any]] = []
        for document in source_documents:
            for chunk_index, chunk_text in enumerate(self._split_text(document["text"])):
                stored_chunks.append(
                    {
                        "chunk_id": str(uuid.uuid4()),
                        "customer_id": customer_id,
                        "source_type": document["source_type"],
                        "source_id": document["source_id"],
                        "chunk_index": chunk_index,
                        "chunk_text": chunk_text,
                        "embedding": self.embedding_service.embed_text(chunk_text),
                        "metadata": {"source_priority": SOURCE_PRIORITY.get(document["source_type"], 0)},
                    }
                )
        await storage_service.replace_customer_chunks(customer_id, stored_chunks)
        return len(stored_chunks)

    async def _ensure_customer_index(self, storage_service: Any, customer_id: str) -> list[dict]:
        chunks = await storage_service.get_customer_chunks(customer_id)
        if chunks:
            return chunks
        await self.rebuild_customer_index(storage_service, customer_id)
        return await storage_service.get_customer_chunks(customer_id)

    def _compute_missing_info(self, question: str, chunks: list[dict]) -> list[str]:
        corpus = "\n".join(chunk.get("chunk_text") or "" for chunk in chunks)
        required_checks = [
            ("flow", "bank flow", "近6个月银行流水"),
            ("流水", "bank flow", "近6个月银行流水"),
            ("征信", "credit report", "企业征信报告"),
            ("credit", "credit report", "企业征信报告"),
            ("tax", "tax", "纳税或经营数据"),
            ("纳税", "tax", "纳税或经营数据"),
            ("scheme", "scheme", "最新方案匹配结果"),
            ("方案", "scheme", "最新方案匹配结果"),
        ]
        missing: list[str] = []
        lower_question = question.lower()
        lower_corpus = corpus.lower()
        for keyword, corpus_keyword, label in required_checks:
            if (keyword in question or keyword in lower_question) and corpus_keyword not in lower_corpus and keyword not in corpus:
                if label not in missing:
                    missing.append(label)
        source_types = {chunk.get("source_type") for chunk in chunks}
        if "parsed_document_text" not in source_types and "已解析资料文本" not in missing:
            missing.append("已解析资料文本")
        return missing

    def _build_prompt(self, question: str, evidence: list[dict], missing_info: list[str]) -> str:
        evidence_text = "\n\n".join(
            f"[{item['source_type']} | score={item['score']}]\n{item['text']}" for item in evidence
        ) or "无可用证据。"
        missing_text = "\n".join(f"- {item}" for item in missing_info) if missing_info else "- 当前未识别到强制缺失项"
        return (
            "你是贷款助手的客户资料问答模块。\n"
            "你只能基于下方检索证据回答，不允许编造未提供的信息。\n"
            "如果证据不足，请明确说明资料不足，并列出缺失项。\n"
            "回答风格要求：专业、简洁、可执行。\n\n"
            f"用户问题：{question}\n\n"
            f"检索证据：\n{evidence_text}\n\n"
            f"缺失项：\n{missing_text}\n\n"
            "请输出 JSON：{\"answer\": \"...\", \"missing_info\": [\"...\"]}"
        )

    async def chat(self, storage_service: Any, customer_id: str, question: str) -> dict[str, Any]:
        chunks = await self._ensure_customer_index(storage_service, customer_id)
        if not chunks:
            return {"answer": "当前客户暂无可检索资料，暂时无法回答。", "evidence": [], "missing_info": ["资料汇总", "已解析资料文本"]}

        query_embedding = self.embedding_service.embed_text(question)
        ranked = sorted(
            [
                {
                    "source_type": chunk.get("source_type") or "",
                    "text": chunk.get("chunk_text") or "",
                    "score": round(
                        self.embedding_service.cosine_similarity(query_embedding, chunk.get("embedding") or []),
                        4,
                    ),
                }
                for chunk in chunks
            ],
            key=lambda item: (SOURCE_PRIORITY.get(item["source_type"], 0), item["score"]),
            reverse=True,
        )
        evidence = ranked[:TOP_K]
        missing_info = self._compute_missing_info(question, evidence)

        prompt = self._build_prompt(question, evidence, missing_info)
        try:
            response = self.ai_service.client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": prompt}],
                temperature=0.1,
                max_tokens=1024,
            )
            content = response.choices[0].message.content or ""
            parsed = parse_json(content) if content else {}
            answer = parsed.get("answer") or "根据当前资料，暂时无法给出更明确结论。"
            model_missing = parsed.get("missing_info") if isinstance(parsed.get("missing_info"), list) else []
            final_missing = list(dict.fromkeys([*missing_info, *model_missing]))
        except Exception as exc:
            logger.warning("RAG answer generation failed for %s: %s", customer_id, exc)
            answer = "当前资料检索已完成，但模型回答服务暂时不可用。请先参考证据片段并稍后重试。"
            final_missing = missing_info

        return {"answer": answer, "evidence": evidence, "missing_info": final_missing}
