from __future__ import annotations

from datetime import datetime
from typing import Any

from .base import BaseAgent
from .llm_runner import agent_llm_enabled, run_agent_llm_json
from .prompts import RISK_AGENT_SYSTEM_PROMPT


RISK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["agent_name", "status", "risk_level", "risks", "summary", "warnings"],
    "properties": {
        "agent_name": {"type": "string"},
        "status": {"type": "string", "enum": ["success", "failed"]},
        "risk_level": {"type": "string", "enum": ["low", "medium", "high", "unknown"]},
        "risks": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["risk_type", "risk_level", "description", "evidence", "suggestion", "source"],
                "properties": {
                    "risk_type": {"type": "string"},
                    "risk_level": {"type": "string", "enum": ["low", "medium", "high", "unknown"]},
                    "description": {"type": "string"},
                    "evidence": {"type": "string"},
                    "suggestion": {"type": "string"},
                    "source": {"type": "string"},
                },
            },
        },
        "summary": {"type": "string"},
        "warnings": {"type": "array", "items": {"type": "string"}},
    },
}


def _num(value: Any) -> float:
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return 0.0


def _risk_key(item: dict[str, Any]) -> str:
    return str(item.get("risk_type") or item.get("description") or "").strip().lower()


def _merge_rule_and_llm_risks(rule_output: dict[str, Any], llm_output: dict[str, Any]) -> dict[str, Any]:
    rule_risks = list(rule_output.get("risks") or [])
    llm_risks = list(llm_output.get("risks") or [])
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    warnings = list(rule_output.get("warnings") or []) + list(llm_output.get("warnings") or [])

    for item in rule_risks:
        item = {**item, "source": item.get("source") or "rule"}
        key = _risk_key(item)
        if key:
            seen.add(key)
        merged.append(item)

    for item in llm_risks:
        if not item.get("evidence"):
            warnings.append(f"LLM 风险缺少 evidence，已降级为 warning：{item.get('description') or item.get('risk_type')}")
            continue
        key = _risk_key(item)
        if key in seen:
            continue
        item["source"] = item.get("source") or "llm"
        merged.append(item)
        if key:
            seen.add(key)

    level_order = {"unknown": 0, "low": 1, "medium": 2, "high": 3}
    risk_level = "low"
    for item in merged:
        level = item.get("risk_level") or "unknown"
        if level_order.get(level, 0) > level_order.get(risk_level, 0):
            risk_level = level

    return {
        **llm_output,
        "agent_name": "RiskAgent",
        "status": "success",
        "risk_level": risk_level,
        "risks": merged,
        "summary": llm_output.get("summary") or rule_output.get("summary") or "",
        "warnings": warnings,
        "rule_baseline": rule_output,
    }


class RiskAgent(BaseAgent):
    agent_name = "RiskAgent"

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
            "credit_summary": ((context.get("steps") or {}).get("CreditAnalysisAgent") or {}).get("credit_profile") or {},
            "parsed_credit_fields": context.get("enterprise_credit") or {},
            "rule_risks": rule_output.get("risks") or [],
        }
        llm_output = await run_agent_llm_json(
            agent_name=self.agent_name,
            system_prompt=RISK_AGENT_SYSTEM_PROMPT,
            user_payload=payload,
            json_schema=RISK_SCHEMA,
            fallback_output=rule_output,
            debug_context=context.get("_debug") or {},
        )
        if not llm_output.get("llm_used"):
            return llm_output
        return _merge_rule_and_llm_risks(rule_output, llm_output)

    def _run_rules(self, context: dict[str, Any]) -> dict[str, Any]:
        credit_profile = ((context.get("steps") or {}).get("CreditAnalysisAgent") or {}).get("credit_profile") or {}
        credit = context.get("enterprise_credit") or {}
        active_loans = credit.get("active_loans") or []
        risks: list[dict[str, Any]] = []

        credit_line_total = _num(credit_profile.get("credit_line_total"))
        used_credit_line = _num(credit_profile.get("used_credit_line"))
        if credit_line_total and used_credit_line / credit_line_total >= 0.9:
            risks.append({
                "risk_type": "授信使用率过高",
                "risk_level": "medium",
                "description": "授信使用率较高",
                "evidence": f"已用授信 {used_credit_line} / 授信总额 {credit_line_total}",
                "suggestion": "补充还款来源、经营流水和新增订单证明，说明授信使用合理性。",
                "source": "rule",
            })

        institution_count = int(_num(credit_profile.get("institution_count")))
        if institution_count >= 6:
            risks.append({
                "risk_type": "多头授信",
                "risk_level": "high",
                "description": "存在多头授信或多机构融资特征",
                "evidence": f"未结清信贷机构/账户数约 {institution_count}",
                "suggestion": "整理各银行授信用途和到期安排，避免被认定为过度融资。",
                "source": "rule",
            })

        external_guarantee = _num(credit_profile.get("external_guarantee_balance"))
        if external_guarantee > 0:
            risks.append({
                "risk_type": "对外担保风险",
                "risk_level": "medium",
                "description": "存在对外担保余额",
                "evidence": f"对外担保余额 {external_guarantee} 万元",
                "suggestion": "补充被担保方经营情况、反担保安排和代偿风险说明。",
                "source": "rule",
            })

        abnormal = []
        for loan in active_loans:
            if _num(loan.get("overdue_months")) > 0 or _num(loan.get("overdue_amount") or loan.get("overdue_total")) > 0:
                abnormal.append(loan)
            if (loan.get("five_classification") or loan.get("five_category")) in {"关注", "次级", "可疑", "损失"}:
                abnormal.append(loan)
        if abnormal:
            risks.append({
                "risk_type": "逾期或分类异常",
                "risk_level": "high",
                "description": "存在逾期或五级分类异常记录",
                "evidence": f"异常记录数量 {len(abnormal)}",
                "suggestion": "先核实征信异常原因，准备结清证明或银行说明。",
                "source": "rule",
            })

        near_due_count = 0
        now = datetime.now()
        for loan in active_loans:
            end_date = loan.get("due_date") or loan.get("end_date")
            try:
                days = (datetime.strptime(str(end_date), "%Y-%m-%d") - now).days
                if 0 <= days <= 120:
                    near_due_count += 1
            except Exception:
                continue
        if near_due_count >= 3:
            risks.append({
                "risk_type": "短期借款集中到期",
                "risk_level": "medium",
                "description": "短期内到期贷款较集中",
                "evidence": f"未来 120 天内约 {near_due_count} 笔贷款到期",
                "suggestion": "准备续贷计划、现金流覆盖说明和替代还款来源。",
                "source": "rule",
            })

        level_order = {"low": 1, "medium": 2, "high": 3}
        risk_level = "low"
        for item in risks:
            if level_order[item["risk_level"]] > level_order[risk_level]:
                risk_level = item["risk_level"]
        return {
            "agent_name": self.agent_name,
            "status": "success",
            "risk_level": risk_level if risks else "low",
            "risks": risks,
            "summary": "规则风险识别完成" if risks else "暂未识别到明显风险",
            "warnings": [],
        }
