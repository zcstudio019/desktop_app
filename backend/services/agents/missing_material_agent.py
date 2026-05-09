from __future__ import annotations

from typing import Any

from .base import BaseAgent


class MissingMaterialAgent(BaseAgent):
    agent_name = "MissingMaterialAgent"

    async def _run(self, context: dict[str, Any]) -> dict[str, Any]:
        document_step = (context.get("steps") or {}).get("DocumentAgent") or {}
        uploaded = document_step.get("uploaded_documents") or []
        parsed = document_step.get("parsed_documents") or []
        text = " ".join(
            str(x.get("document_type") or x.get("extraction_type") or x.get("file_name") or "")
            for x in [*uploaded, *parsed]
            if isinstance(x, dict)
        )
        required_catalog = [
            ("business_license", "营业执照", "确认企业主体和经营状态"),
            ("enterprise_credit", "企业征信", "判断负债、授信、逾期和担保"),
            ("id_card", "法人身份证", "核验实控/法人身份"),
            ("bank_statement", "近12个月银行流水", "验证经营现金流和还款来源"),
            ("invoice", "近12个月开票", "验证收入规模和交易稳定性"),
            ("tax", "近12个月纳税", "验证纳税和经营真实性"),
            ("financial_report", "财务报表", "评估资产负债和盈利能力"),
        ]
        optional_catalog = [
            {"material": "主要合同", "reason": "证明订单和未来收入"},
            {"material": "房产/资产证明", "reason": "用于抵押或增信"},
            {"material": "经营场地证明", "reason": "证明经营稳定性"},
        ]
        required = [
            {"document_type": code, "material": name, "reason": reason}
            for code, name, reason in required_catalog
            if code not in text and name not in text
        ]
        return {
            "agent_name": self.agent_name,
            "status": "success",
            "required_materials": required,
            "optional_materials": optional_catalog,
            "reason": [item["reason"] for item in required],
            "warnings": [],
        }
