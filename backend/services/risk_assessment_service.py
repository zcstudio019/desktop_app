"""Customer-scoped risk assessment service."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from services.ai_service import AIService

from .embedding_service import EmbeddingService
from .markdown_profile_service import get_or_create_customer_profile
from .rag_service import RagService

logger = logging.getLogger(__name__)

RISK_DIMENSIONS = [
    "subject_qualification",
    "credit_and_debt",
    "business_stability",
    "repayment_source",
    "data_completeness",
]

DIMENSION_LABELS = {
    "subject_qualification": "主体资质",
    "credit_and_debt": "征信与负债",
    "business_stability": "经营稳定性",
    "repayment_source": "还款来源",
    "data_completeness": "资料完整性",
}

KEY_MATERIAL_RULES = [
    ("营业执照", ["营业执照", "统一社会信用代码"]),
    ("银行对账单", ["对账单", "对账明细", "bank statement"]),
    ("企业征信报告", ["征信", "credit"]),
    ("近6个月银行流水", ["流水", "bank flow", "交易流水"]),
    ("经营/纳税数据", ["纳税", "营收", "开票", "财务"]),
    ("贷款申请表", ["申请表", "application"]),
    ("方案匹配结果", ["方案", "匹配", "scheme"]),
]


class RiskAssessmentService:
    def __init__(
        self,
        ai_service: AIService | None = None,
        embedding_service: EmbeddingService | None = None,
        rag_service: RagService | None = None,
    ) -> None:
        self.ai_service = ai_service or AIService()
        self.embedding_service = embedding_service or EmbeddingService()
        self.rag_service = rag_service or RagService(ai_service=self.ai_service, embedding_service=self.embedding_service)

    def _serialize(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, indent=2)

    def _collect_missing_items(self, corpus: str, has_application: bool, has_scheme: bool) -> list[str]:
        lower_corpus = corpus.lower()
        missing: list[str] = []
        for label, keywords in KEY_MATERIAL_RULES:
            if label == "贷款申请表" and has_application:
                continue
            if label == "方案匹配结果" and has_scheme:
                continue
            if not any(keyword.lower() in lower_corpus for keyword in keywords):
                missing.append(label)
        return missing

    async def summarize_customer_materials(self, storage_service: Any, customer_id: str) -> dict[str, Any]:
        customer = await storage_service.get_customer(customer_id)
        if not customer:
            return {"missing_items": [], "has_application": False, "has_scheme": False, "profile_version": None}

        profile, _ = await get_or_create_customer_profile(storage_service, customer_id)
        get_business_extractions = getattr(storage_service, "get_business_extractions_by_customer", None)
        if callable(get_business_extractions):
            extractions = await get_business_extractions(customer_id)
        else:
            extractions = await storage_service.get_extractions_by_customer(customer_id)
        scheme_snapshot = await storage_service.get_latest_scheme_snapshot(customer_id)
        applications = await storage_service.list_saved_applications(customer_id=customer_id)
        active_apps = [item for item in applications if not item.get("stale")]
        application_summary = active_apps[0] if active_apps else None

        profile_text = profile.get("markdown_content") or ""
        extraction_text = "\n\n".join(self._serialize(item.get("extracted_data") or {}) for item in extractions)
        scheme_text = (scheme_snapshot or {}).get("summary_markdown") or ""
        application_text = self._serialize((application_summary or {}).get("applicationData") or {})
        corpus = "\n\n".join(part for part in [profile_text, extraction_text, scheme_text, application_text] if part)

        missing_items = self._collect_missing_items(corpus, bool(application_summary), bool(scheme_snapshot))
        return {
            "missing_items": missing_items,
            "has_application": bool(application_summary),
            "has_scheme": bool(scheme_snapshot),
            "profile_version": profile.get("version") if profile else None,
        }

    async def _retrieve_basis(self, storage_service: Any, customer_id: str, query: str, top_k: int = 3) -> list[dict[str, Any]]:
        chunks = await storage_service.get_customer_chunks(customer_id)
        if not chunks:
            try:
                await self.rag_service.rebuild_customer_index(storage_service, customer_id)
            except Exception as exc:
                logger.warning("Failed to build chunks for risk report %s: %s", customer_id, exc)
            chunks = await storage_service.get_customer_chunks(customer_id)
        if not chunks:
            return []
        query_embedding = self.embedding_service.embed_text(query)
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
            key=lambda item: item["score"],
            reverse=True,
        )
        return ranked[:top_k]

    def _score_dimension(self, label: str, corpus: str, missing_items: list[str]) -> tuple[int, list[str]]:
        reasons: list[str] = []
        text = corpus.lower()
        score = 8

        if label == "主体资质":
            if any(word in text for word in ["公司", "企业", "营业", "成立", "法人"]):
                score += 8
                reasons.append("已识别企业主体与基本身份信息。")
            if any(word in text for word in ["在业", "正常", "存续"]):
                score += 4
                reasons.append("主体状态偏稳定。")
        elif label == "征信与负债":
            if any(word in text for word in ["征信", "信用", "授信"]):
                score += 6
                reasons.append("已有征信或授信信息。")
            if any(word in text for word in ["逾期", "不良", "失信"]):
                score -= 6
                reasons.append("存在潜在征信负面表述。")
            if "企业征信报告" in missing_items:
                score -= 4
                reasons.append("缺少完整企业征信报告。")
        elif label == "经营稳定性":
            if any(word in text for word in ["营收", "纳税", "开票", "流水", "财务"]):
                score += 8
                reasons.append("已有经营或财务数据支撑。")
            if "经营/纳税数据" in missing_items:
                score -= 4
                reasons.append("经营与纳税数据仍不完整。")
        elif label == "还款来源":
            if any(word in text for word in ["收入", "回款", "流水", "还款来源", "现金流"]):
                score += 8
                reasons.append("已有还款来源相关信息。")
            if "近6个月银行流水" in missing_items:
                score -= 4
                reasons.append("缺少近6个月银行流水。")
        elif label == "资料完整性":
            score += max(0, 10 - len(missing_items) * 2)
            if missing_items:
                reasons.append(f"当前仍缺少 {len(missing_items)} 项关键资料。")
            else:
                reasons.append("关键资料较完整。")

        return max(0, min(score, 20)), reasons

    def _risk_level_from_score(self, score: int) -> str:
        if score >= 75:
            return "low"
        if score >= 50:
            return "medium"
        return "high"

    def _render_markdown(self, report_json: dict[str, Any]) -> str:
        overall = report_json["overall_assessment"]
        customer_summary = report_json["customer_summary"]
        lines = [
            "# 风险评估报告",
            "",
            "## 客户概况",
            f"- 客户名称：{customer_summary.get('customer_name') or '暂无'}",
            f"- 客户类型：{customer_summary.get('customer_type') or '暂无'}",
            f"- 所属行业：{customer_summary.get('industry') or '暂无'}",
            f"- 融资需求：{customer_summary.get('financing_need') or '暂无'}",
            "",
            "## 综合风险结论",
            f"- 综合评分：{overall.get('total_score')}",
            f"- 风险等级：{overall.get('risk_level')}",
            f"- 核心结论：{overall.get('conclusion') or '暂无'}",
            "",
            "## 风险维度评估",
        ]
        for item in report_json["risk_dimensions"]:
            lines.extend(
                [
                    f"### {item.get('dimension')}",
                    f"- 评分：{item.get('score')}/20",
                    f"- 等级：{item.get('risk_level')}",
                    f"- 说明：{item.get('summary') or '暂无'}",
                ]
            )
        lines.extend(
            [
                "",
                "## 优化建议",
                *(f"- {item}" for item in report_json["optimization_suggestions"]["short_term"]),
                *(f"- {item}" for item in report_json["optimization_suggestions"]["mid_term"]),
                "",
                "## 融资规划",
                f"- 当前阶段：{report_json['financing_plan'].get('current_stage') or '暂无'}",
                *(f"- {item}" for item in report_json["financing_plan"]["one_to_three_months"]),
                *(f"- {item}" for item in report_json["financing_plan"]["three_to_six_months"]),
                "",
                "## 最终建议",
                f"- 建议动作：{report_json['final_recommendation'].get('action') or '暂无'}",
                *(f"- {item}" for item in report_json["final_recommendation"]["next_steps"]),
            ]
        )
        return "\n".join(lines).strip()

    async def generate_report(self, storage_service: Any, customer_id: str) -> dict[str, Any]:
        customer = await storage_service.get_customer(customer_id)
        if not customer:
            raise ValueError("customer not found")

        profile, _ = await get_or_create_customer_profile(storage_service, customer_id)
        get_business_extractions = getattr(storage_service, "get_business_extractions_by_customer", None)
        if callable(get_business_extractions):
            extractions = await get_business_extractions(customer_id)
        else:
            extractions = await storage_service.get_extractions_by_customer(customer_id)
        scheme_snapshot = await storage_service.get_latest_scheme_snapshot(customer_id)
        applications = await storage_service.list_saved_applications(customer_id=customer_id)
        active_apps = [item for item in applications if not item.get("stale")]
        application_summary = active_apps[0] if active_apps else None

        corpus = "\n\n".join(
            part
            for part in [
                profile.get("markdown_content") or "",
                "\n\n".join(self._serialize(item.get("extracted_data") or {}) for item in extractions),
                (scheme_snapshot or {}).get("summary_markdown") or "",
                self._serialize((application_summary or {}).get("applicationData") or {}),
            ]
            if part
        )
        missing_items = self._collect_missing_items(corpus, bool(application_summary), bool(scheme_snapshot))

        risk_dimensions = []
        total_score = 0
        for key in RISK_DIMENSIONS:
            label = DIMENSION_LABELS[key]
            score, reasons = self._score_dimension(label, corpus, missing_items)
            total_score += score
            basis = await self._retrieve_basis(storage_service, customer_id, label)
            risk_dimensions.append(
                {
                    "dimension": label,
                    "score": score,
                    "risk_level": self._risk_level_from_score(score * 5),
                    "summary": "；".join(reasons) or "暂无说明",
                    "basis": basis,
                    "missing_info": missing_items,
                }
            )

        total_score = max(0, min(total_score, 100))
        overall_risk_level = self._risk_level_from_score(total_score)

        report_json = {
            "customer_summary": {
                "customer_id": customer_id,
                "customer_name": customer.get("name") or "",
                "customer_type": "个人" if (customer.get("customer_type") or "") == "personal" else "企业",
                "industry": "",
                "financing_need": (application_summary or {}).get("loanType") or "",
                "data_completeness": {
                    "status": "部分完整" if missing_items else "较完整",
                    "score": max(0, 20 - len(missing_items) * 2),
                    "missing_items": missing_items,
                },
            },
            "overall_assessment": {
                "total_score": total_score,
                "risk_level": overall_risk_level,
                "conclusion": "当前报告基于已归档客户资料生成，建议结合缺失项补充后再次评估。",
                "immediate_application_recommended": total_score >= 75 and not missing_items,
                "basis": await self._retrieve_basis(storage_service, customer_id, "综合风险"),
            },
            "risk_dimensions": risk_dimensions,
            "matched_schemes": {
                "has_match": bool(scheme_snapshot),
                "items": [] if not scheme_snapshot else [
                    {
                        "product_name": "已匹配方案",
                        "estimated_limit": "",
                        "estimated_rate": "",
                        "match_reason": "系统已存在最近一次方案匹配结果。",
                        "constraints": [],
                        "basis": await self._retrieve_basis(storage_service, customer_id, "方案匹配"),
                    }
                ],
            },
            "no_match_analysis": {
                "has_no_match_issue": not bool(scheme_snapshot),
                "reasons": [] if scheme_snapshot else ["当前尚无已保存的方案匹配结果。"],
                "core_shortboards": [] if scheme_snapshot else missing_items[:3],
                "basis": await self._retrieve_basis(storage_service, customer_id, "未匹配原因"),
            },
            "optimization_suggestions": {
                "short_term": ["优先补齐关键缺失资料。"] + [f"补充：{item}" for item in missing_items[:2]],
                "mid_term": ["在补充资料后重新生成申请表与方案匹配。"],
                "document_supplement": missing_items,
                "credit_optimization": ["如存在征信疑点，建议先复核近两年逾期与授信结构。"],
                "debt_optimization": ["结合负债结构优化现有授信与担保占用。"],
            },
            "financing_plan": {
                "current_stage": "先完成资料校准与结果重跑。",
                "one_to_three_months": ["补齐资料后重新生成风险报告。"],
                "three_to_six_months": ["根据更新后的评估结果选择申请路径。"],
                "alternative_paths": ["如暂未命中方案，可考虑抵押类、担保类或供应链融资。"],
            },
            "final_recommendation": {
                "action": "optimize_then_apply" if missing_items or overall_risk_level != "low" else "apply_now",
                "priority_product_types": ["抵押类融资", "担保类融资", "供应链融资"],
                "next_steps": ["确认补件清单", "重新生成申请表", "重新匹配融资方案"],
                "basis": await self._retrieve_basis(storage_service, customer_id, "最终建议"),
            },
        }

        # LLM only refines summary text and must not change scores or levels.
        try:
            prompt = (
                "你是贷款风险报告润色助手。只能在不改动评分、风险等级和结构字段的前提下，"
                "优化 overall_assessment.conclusion 以及各 risk_dimensions.summary 的表达。"
                "输出 JSON，且仅返回 conclusion 和 summaries。\n\n"
                f"{json.dumps(report_json, ensure_ascii=False)}"
            )
            response = self.ai_service.client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": prompt}],
                temperature=0.1,
                max_tokens=1200,
            )
            content = response.choices[0].message.content or ""
            refined = json.loads(content) if content.strip().startswith("{") else {}
            if isinstance(refined, dict):
                conclusion = refined.get("conclusion")
                if isinstance(conclusion, str) and conclusion.strip():
                    report_json["overall_assessment"]["conclusion"] = conclusion.strip()
                summaries = refined.get("summaries")
                if isinstance(summaries, list):
                    for idx, summary in enumerate(summaries[: len(report_json["risk_dimensions"])]):
                        if isinstance(summary, str) and summary.strip():
                            report_json["risk_dimensions"][idx]["summary"] = summary.strip()
        except Exception as exc:
            logger.warning("Risk report refinement skipped for %s: %s", customer_id, exc)

        generated_at = datetime.now(timezone.utc).isoformat()
        report_markdown = self._render_markdown(report_json)
        return {
            "report_json": report_json,
            "report_markdown": report_markdown,
            "generated_at": generated_at,
            "profile_version": profile.get("version") or 1,
            "profile_updated_at": profile.get("updated_at") or "",
        }
