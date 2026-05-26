from __future__ import annotations

import asyncio
import tempfile
from typing import Any

from backend.services.agents.financing_judgement_agent import FinancingJudgementAgent
from backend.services.agents.orchestrator import _normalize_workflow_context
from backend.services.local_storage_service import LocalStorageService
from backend.services.markdown_profile_service import build_auto_profile_payload
from backend.services.rag_service import RagService


def _amount(value: float) -> dict[str, float]:
    return {"normalized_value": value}


def _financial_report(
    year: int,
    assets: float,
    liabilities: float,
    revenue: float,
    net_profit: float,
    operating_cash_flow: float,
) -> dict[str, Any]:
    quarterly = year == 2024
    period_start = "2024-10-01" if quarterly else f"{year}-01-01"
    period_end = f"{year}-12-31"
    report = {
        "document_type": "financial_report",
        "source_file": f"{year}财务报表.pdf",
        "company_info": {
            "company_name": "测试有限公司",
            "taxpayer_id": "913201055804841947",
            "accounting_standard": "enterprise_accounting_standard",
            "report_type": "quarterly" if quarterly else "annual",
            "report_period_start": period_start,
            "report_period_end": period_end,
            "report_date": period_end,
            "currency": "CNY",
            "unit": "元",
        },
        "balance_sheet": {
            "cash_and_equivalents": _amount(150161.66),
            "short_term_loans": _amount(25020000.00),
            "total_assets": _amount(assets),
            "total_liabilities": _amount(liabilities),
            "total_equity": _amount(assets - liabilities),
        },
        "income_statement": {
            "revenue": _amount(revenue),
            "operating_cost": _amount(revenue * 0.8),
            "net_profit": _amount(net_profit),
        },
        "cash_flow_statement": {
            "net_operating_cash_flow": _amount(operating_cash_flow),
        },
        "financial_ratios": {
            "asset_liability_ratio": liabilities / assets,
            "current_ratio": 1.1,
            "quick_ratio": 0.8,
            "cash_ratio": 0.05,
            "gross_margin": 0.2,
            "net_margin": net_profit / revenue,
            "operating_cash_flow_to_revenue": operating_cash_flow / revenue,
            "financing_dependence": 0.3,
        },
    }
    return {
        "extraction_id": f"financial-{year}",
        "doc_id": f"doc-financial-{year}",
        "customer_id": "customer-coexist",
        "extraction_type": "financial_report",
        "file_name": report["source_file"],
        "extracted_data": {"structured_json": report},
    }


def _enterprise_flow(index: int, inflow: float, outflow: float) -> dict[str, Any]:
    file_name = f"企业流水-{index}.xlsx"
    return {
        "extraction_id": f"flow-{index}",
        "doc_id": f"doc-flow-{index}",
        "customer_id": "customer-coexist",
        "extraction_type": "enterprise_flow",
        "file_name": file_name,
        "extracted_data": {
            "extracted_json": {
                "document_type": "enterprise_flow",
                "source_file": file_name,
                "statement_period": {"start_date": "2024-01-01", "end_date": "2024-12-31"},
                "summary": {
                    "total_inflow": inflow,
                    "total_outflow": outflow,
                    "net_cashflow": inflow - outflow,
                    "operating_inflow": inflow,
                    "operating_outflow": outflow,
                    "transaction_count": 2,
                },
                "accounts": [
                    {
                        "bank_name": "测试银行",
                        "account_number": f"6222{index}",
                        "total_inflow": inflow,
                        "total_outflow": outflow,
                        "net_cashflow": inflow - outflow,
                        "transaction_count": 2,
                    }
                ],
                "transactions": [],
            }
        },
    }


class _Storage:
    def __init__(self, extractions: list[dict[str, Any]]):
        self.extractions = extractions
        self.chunks: list[dict[str, Any]] = []

    async def get_customer(self, customer_id: str) -> dict[str, Any]:
        return {"customer_id": customer_id, "name": "测试有限公司", "customer_type": "enterprise"}

    async def get_business_extractions_by_customer(self, customer_id: str) -> list[dict[str, Any]]:
        return self.extractions

    async def get_extractions_by_customer(self, customer_id: str) -> list[dict[str, Any]]:
        return self.extractions

    async def get_document(self, doc_id: str) -> dict[str, Any]:
        item = next(extraction for extraction in self.extractions if extraction["doc_id"] == doc_id)
        return {
            "doc_id": doc_id,
            "file_name": item["file_name"],
            "file_path": f"/test/{item['file_name']}",
            "upload_time": "2024-12-31",
        }

    async def list_saved_applications(self, customer_id: str) -> list[dict[str, Any]]:
        return []

    async def get_latest_scheme_snapshot(self, customer_id: str) -> None:
        return None

    async def get_customer_profile(self, customer_id: str) -> dict[str, Any]:
        return {"markdown_content": "# 已保存资料汇总"}

    async def replace_customer_chunks(self, customer_id: str, chunks: list[dict[str, Any]]) -> None:
        self.chunks = chunks


FINANCIAL_REPORTS = [
    _financial_report(2022, 84697985.94, 78474828.15, 140360769.35, 429625.06, -15841870.74),
    _financial_report(2023, 69320214.02, 56276448.92, 100012470.73, 6690607.31, -8438844.57),
    _financial_report(2024, 54688482.62, 41636748.83, 60376572.48, 7968.69, -1989500.82),
]
ENTERPRISE_FLOWS = [_enterprise_flow(1, 5000000.00, 4200000.00), _enterprise_flow(2, 7000000.00, 6000000.00)]


def test_profile_keeps_financial_report_and_enterprise_flow_in_both_upload_orders() -> None:
    for extractions in (
        [FINANCIAL_REPORTS[-1], ENTERPRISE_FLOWS[0]],
        [ENTERPRISE_FLOWS[0], FINANCIAL_REPORTS[-1]],
    ):
        payload = asyncio.run(build_auto_profile_payload(_Storage(extractions), "customer-coexist"))
        markdown = payload["markdown_content"]
        source_types = {
            item.get("source_type") for item in payload["source_snapshot"]["source_documents"]
        }
        assert "## 财务报表" in markdown
        assert "### 财务数据总览" in markdown
        assert "### 资产负债表摘要" in markdown
        assert "### 利润表摘要" in markdown
        assert "### 现金流量表摘要" in markdown
        assert "### 企业信息" in markdown
        assert "### 银行授信核心指标表" in markdown
        assert "### 偿债能力分析" in markdown
        assert "### 盈利能力分析" in markdown
        assert "### 现金流质量分析" in markdown
        assert "### 异常科目分析" in markdown
        assert "### 银行贷款审核关注点" in markdown
        assert "### 缺失材料清单" in markdown
        assert "### 综合授信建议" in markdown
        assert "## 企业流水" in markdown
        assert "### 企业流水分析" in markdown
        assert "### 财务风险摘要" in markdown
        assert "### 流水风险摘要" in markdown
        assert {"financial_report", "enterprise_flow"} <= source_types


def test_profile_aggregates_multiple_financial_reports_and_enterprise_flows_separately() -> None:
    storage = _Storage([*FINANCIAL_REPORTS, *ENTERPRISE_FLOWS])
    payload = asyncio.run(build_auto_profile_payload(storage, "customer-coexist"))
    markdown = payload["markdown_content"]
    lines = markdown.splitlines()
    assert lines.count("## 财务报表") == 1
    assert lines.count("## 企业流水") == 1
    assert "已识别报表份数：3份" in markdown
    assert "2022年报、2023年报、2024季报" in markdown
    assert "| 企业名称 | 测试有限公司 |" in markdown
    assert "| 纳税人识别号 | 913201055804841947 |" in markdown
    assert "| 报表类型 | 多期（最新为季报） |" in markdown
    assert "| 币种 | 人民币 |" in markdown
    assert "| 资产总计 | 54,688,482.62 元 | 69,320,214.02 元 | -14,631,731.40 元 | -21.11% |" in markdown
    assert "| 2022年报 | 140,360,769.35 元 |" in markdown
    assert "| 2024季报 | -1,989,500.82 元 |" in markdown
    assert "| 资产负债率 | 76.13% |" in markdown
    assert "本分析基于客户名下全部财务报表资料自动汇总生成。" in markdown
    assert "来源文件数：2" in markdown


def test_rag_index_keeps_financial_report_and_enterprise_flow_source_types() -> None:
    storage = _Storage([FINANCIAL_REPORTS[-1], ENTERPRISE_FLOWS[0]])
    service = RagService(ai_service=object())
    asyncio.run(service.rebuild_customer_index(storage, "customer-coexist"))
    typed_chunks = {
        chunk["source_type"]: chunk
        for chunk in storage.chunks
        if chunk["source_type"] in {"financial_report", "enterprise_flow"}
    }
    assert set(typed_chunks) == {"financial_report", "enterprise_flow"}
    for document_type, chunk in typed_chunks.items():
        metadata = chunk["metadata"]
        assert metadata["customer_id"] == "customer-coexist"
        assert metadata["document_id"]
        assert metadata["document_type"] == document_type
        assert metadata["source_type"] == document_type
        assert metadata["source_file"]
        assert metadata["section_title"]


def test_financing_context_contains_both_summary_buckets() -> None:
    context = _normalize_workflow_context(
        {
            "customer": {"customer_id": "customer-coexist", "name": "测试有限公司"},
            "extractions": [*FINANCIAL_REPORTS, *ENTERPRISE_FLOWS],
        }
    )
    summaries = context["analysis_summaries_by_type"]
    assert len(summaries["financial_report"]["reports"]) == 3
    assert summaries["enterprise_flow"]["source_document_count"] == 2

    context["steps"] = {
        "CreditAnalysisAgent": {"credit_profile": {}},
        "RiskAgent": {"risk_level": "low"},
        "MissingMaterialAgent": {"required_materials": []},
    }
    judgement = FinancingJudgementAgent()._run_rules(context)["judgement"]
    assert any("财务报表汇总" in item for item in judgement["strengths"])
    assert any("企业流水汇总" in item for item in judgement["strengths"])


def test_local_storage_keeps_financial_and_flow_extractions_as_distinct_records() -> None:
    with tempfile.TemporaryDirectory() as temporary_directory:
        storage = LocalStorageService(f"{temporary_directory}/coexistence.db")
        asyncio.run(storage.create_customer({"customer_id": "customer-coexist", "name": "测试有限公司"}))
        for doc_id, file_type in (("doc-financial", "financial_report"), ("doc-flow", "enterprise_flow")):
            asyncio.run(
                storage.save_document(
                    {
                        "doc_id": doc_id,
                        "customer_id": "customer-coexist",
                        "file_name": f"{doc_id}.pdf",
                        "file_path": f"/test/{doc_id}.pdf",
                        "file_type": file_type,
                    }
                )
            )
        asyncio.run(
            storage.save_extraction(
                {
                    "extraction_id": "extraction-financial",
                    "doc_id": "doc-financial",
                    "customer_id": "customer-coexist",
                    "extraction_type": "financial_data",
                    "extracted_data": {"structured_json": {"document_type": "financial_report"}},
                }
            )
        )
        asyncio.run(
            storage.save_extraction(
                {
                    "extraction_id": "extraction-flow",
                    "doc_id": "doc-flow",
                    "customer_id": "customer-coexist",
                    "extraction_type": "enterprise_flow",
                    "extracted_data": {"extracted_json": {"document_type": "enterprise_flow"}},
                }
            )
        )
        results = asyncio.run(storage.get_extractions_by_customer("customer-coexist"))
        types = {item["extraction_type"] for item in results}
        assert len(results) == 2
        assert types == {"financial_report", "enterprise_flow"}
