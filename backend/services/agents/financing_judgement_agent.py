from __future__ import annotations

from typing import Any

from .base import BaseAgent
from .compliance_guard import DISCLAIMER, ensure_disclaimer
from .llm_runner import agent_llm_enabled, run_agent_llm_json
from .prompts import FINANCING_JUDGEMENT_AGENT_SYSTEM_PROMPT


JUDGEMENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["agent_name", "status", "judgement", "compliance_disclaimer", "warnings"],
    "properties": {
        "agent_name": {"type": "string"},
        "status": {"type": "string", "enum": ["success", "failed"]},
        "judgement": {
            "type": "object",
            "required": [
                "overall_opinion",
                "possible_products",
                "estimated_amount_range",
                "strengths",
                "weaknesses",
                "next_actions",
                "customer_talking_points",
            ],
            "properties": {
                "overall_opinion": {"type": "string"},
                "possible_products": {"type": "array", "items": {"type": "string"}},
                "estimated_amount_range": {"type": "string"},
                "strengths": {"type": "array", "items": {"type": "string"}},
                "weaknesses": {"type": "array", "items": {"type": "string"}},
                "next_actions": {"type": "array", "items": {"type": "string"}},
                "customer_talking_points": {"type": "array", "items": {"type": "string"}},
            },
        },
        "compliance_disclaimer": {"type": "string"},
        "warnings": {"type": "array", "items": {"type": "string"}},
    },
}


class FinancingJudgementAgent(BaseAgent):
    agent_name = "FinancingJudgementAgent"

    async def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        rule_output = self._run_rules(context)
        if not agent_llm_enabled():
            return {
                **rule_output,
                "llm_used": False,
                "fallback_used": False,
                "retry_count": 0,
                "validation_errors": [],
                "compliance_warnings": [],
                "rule_baseline": rule_output,
            }

        payload = {
            "customer_profile": context.get("customer") or {},
            "documents": {
                "count": len(context.get("documents") or []),
                "items": context.get("documents") or [],
            },
            "credit_analysis": (context.get("steps") or {}).get("CreditAnalysisAgent") or {},
            "risk_analysis": (context.get("steps") or {}).get("RiskAgent") or {},
            "missing_materials": (context.get("steps") or {}).get("MissingMaterialAgent") or {},
            "rule_judgement": rule_output,
        }
        llm_output = await run_agent_llm_json(
            agent_name=self.agent_name,
            system_prompt=FINANCING_JUDGEMENT_AGENT_SYSTEM_PROMPT,
            user_payload=payload,
            json_schema=JUDGEMENT_SCHEMA,
            fallback_output=rule_output,
            debug_context=context.get("_debug") or {},
        )
        if llm_output.get("llm_used"):
            llm_output = ensure_disclaimer(llm_output)
            amount_range = ((llm_output.get("judgement") or {}).get("estimated_amount_range") or "")
            if "粗略估算" not in amount_range and "需结合银行政策复核" not in amount_range:
                fallback = {
                    **rule_output,
                    "llm_used": False,
                    "fallback_used": True,
                    "fallback_reason": "amount range missing compliance qualifier",
                    "validation_errors": [],
                    "compliance_warnings": ["estimated_amount_range 缺少合规限定"],
                    "retry_count": llm_output.get("retry_count", 0),
                }
                return fallback
            llm_output["rule_baseline"] = rule_output
        return llm_output

    def _run_rules(self, context: dict[str, Any]) -> dict[str, Any]:
        steps = context.get("steps") or {}
        credit_profile = (steps.get("CreditAnalysisAgent") or {}).get("credit_profile") or {}
        risk_output = steps.get("RiskAgent") or {}
        missing_output = steps.get("MissingMaterialAgent") or {}
        risk_level = risk_output.get("risk_level") or "unknown"
        total_balance = credit_profile.get("total_unsettled_balance")
        credit_line_total = credit_profile.get("credit_line_total")
        possible_products = ["企业信用贷资料预审", "流动资金周转贷资料预审"]
        if risk_level == "high":
            opinion = "当前存在较明显风险点，建议先补齐资料并解释风险后再推进银行准入。"
            amount_range = "暂不建议给出额度，需结合银行政策复核"
        elif credit_line_total:
            opinion = "资料具备初步评估基础，可围绕存量授信、流水和纳税进一步判断可做方向。"
            amount_range = "粗略估算：需结合授信余量、流水和银行政策复核"
        else:
            opinion = "当前资料不足，无法准确判断，建议先补齐征信、流水、开票、纳税和财务资料。"
            amount_range = "unknown，需结合银行政策复核"
        strengths = []
        if credit_line_total:
            strengths.append("已有授信信息，可用于判断银行合作基础")
        if total_balance:
            strengths.append("已有负债结构信息，可辅助测算偿债压力")
        weaknesses = []
        if risk_level in {"medium", "high"}:
            weaknesses.append("存在需要解释的风险项")
        if missing_output.get("required_materials"):
            weaknesses.append("关键资料尚未齐全")
        next_actions = [
            "补齐缺失资料清单中的关键材料",
            "核对征信中未结清贷款、授信和担保是否分类准确",
            "准备经营流水、开票、纳税与主要合同用于交叉验证",
        ]
        talking_points = [
            "我们先按现有资料做预审，不代表银行最终审批结论。",
            "请优先补充流水、开票和纳税，这会直接影响额度判断。",
            "如征信存在多头授信或集中到期，需要准备用途和还款来源说明。",
        ]
        return {
            "agent_name": self.agent_name,
            "status": "success",
            "judgement": {
                "overall_opinion": opinion,
                "possible_products": possible_products,
                "estimated_amount_range": amount_range,
                "strengths": strengths,
                "weaknesses": weaknesses,
                "next_actions": next_actions,
                "customer_talking_points": talking_points,
            },
            "compliance_disclaimer": DISCLAIMER,
            "warnings": [],
        }
