from decimal import Decimal

from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code
from backend.services.bank_statement_agent.aggregator import (
    aggregate_customer_bank_statements,
    render_customer_bank_flow_aggregate_markdown,
)


def receipt_pages():
    first = """中国工商银行电子回单
单位国内汇款
回单日期：2024-05-31 10:20:30
付款人名称：上海乐芙兰电子商务有限公司 付款账号：310066629013003064589
收款人名称：上海顺衡物流有限公司 收款账号：6222000011112222333
交易金额：1,234.56
用途：物流费
摘要：单位国内汇款
回单编号：ICBC202405310001
"""
    second = """中国工商银行电子回单
单位国内汇款
交易日期：2024-05-31 11:22:33
付款人：上海乐芙兰电子商务有限公司
付款账号：310066629013003064589
收款人：福建律动信息技术有限公司
收款账号：6222000044445555666
汇款金额：2,000.00
汇款用途：服务费
业务编号：BIZ202405310002
"""
    return [
        {"page": 1, "text": first, "table_rows": [], "source": "pdf_native"},
        {"page": 2, "text": second, "table_rows": [], "source": "pdf_native"},
    ]


def test_bank_receipt_bundle_type_detection_and_agent_registry():
    text = "\n".join(page["text"] for page in receipt_pages())
    assert detect_document_type_code(text, filename="工商银行电子回单.pdf") == "bank_receipt_bundle"
    assert detect_document_type_code(text, explicit_type="bank_statement", filename="工商银行电子回单.pdf") == "bank_receipt_bundle"

    result = run_document_extraction_agent(
        "bank_receipt_bundle",
        text,
        "工商银行电子回单.pdf",
        metadata={"raw_pages": receipt_pages()},
    )
    assert result.agent_name == "bank_receipt_bundle_agent"
    assert result.document_type == "bank_receipt_bundle"
    assert result.debug["skill_name"] == "bank_receipt_bundle_skill"


def test_bank_receipt_bundle_extracts_receipt_rows_and_markdown():
    text = "\n".join(page["text"] for page in receipt_pages())
    result = run_document_extraction_agent(
        "bank_receipt_bundle",
        text,
        "工商银行电子回单.pdf",
        metadata={"raw_pages": receipt_pages()},
    )
    data = result.extracted_json
    assert data["doc_type"] == "bank_receipt_bundle"
    assert data["doc_type_name"] == "银行回单集合"
    assert data["bank_name"] == "中国工商银行"
    assert data["receipt_count"] == 2
    assert data["valid_receipt_count"] == 2
    assert data["can_join_bank_statement_aggregate"] is False
    assert data["recognizable_amount_total"] == Decimal("3234.56")
    assert data["receipts"][0]["payer_name"] == "上海乐芙兰电子商务有限公司"
    assert data["receipts"][0]["payee_name"] == "上海顺衡物流有限公司"
    assert data["receipts"][0]["payer_account"] == "310066629013003064589"
    assert data["receipts"][0]["payee_account"] == "6222000011112222333"
    assert data["receipts"][0]["amount"] == Decimal("1234.56")
    assert (data["receipts"][1]["receipt_no"] or data["receipts"][1]["business_no"]) == "BIZ202405310002"

    markdown = result.markdown_summary
    assert "## 银行回单集合" in markdown
    assert "资料类型：银行回单集合" in markdown
    assert "是否纳入银行流水聚合：否" in markdown
    assert "### 回单明细" in markdown
    assert "上海顺衡物流有限公司" in markdown
    assert "福建律动信息技术有限公司" in markdown
    for forbidden in ('"doc_type"', "raw_text", "undefined", "null", "None"):
        assert forbidden not in markdown


def test_bank_statement_build_is_rerouted_to_receipt_bundle_agent():
    text = "\n".join(page["text"] for page in receipt_pages())
    content = build_structured_extraction(
        text,
        "bank_statement",
        raw_pages=receipt_pages(),
        filename="31006662901300306458920240531_4.pdf",
    )
    data = content["extracted_json"]
    assert content["document_type_code"] == "bank_receipt_bundle"
    assert content["agent_type"] == "bank_receipt_bundle_agent"
    assert data["doc_type"] == "bank_receipt_bundle"
    assert data["receipt_count"] == 2
    assert "## 银行回单集合" in content["display_markdown"]
    assert "建议使用“银行回单集合 Agent”" not in content["display_markdown"]


def test_bank_receipt_bundle_splits_multiple_receipts_on_one_page_and_chinese_amount():
    page_text = """中国工商银行电子回单
单位国内汇款
业务日期：2024-05-20
付款方：上海乐芙兰电子商务有限公司
付款账号：310066629013003064589
收款方：南京华韵商务服务有限公司
收款账号：6222000099998888777
大写金额：人民币壹仟贰佰叁拾肆元伍角陆分
交易用途：服务费
交易流水号：FLOW202405200001
中国工商银行电子回单
单位国内汇款
回单日期：2024-05-20
付款方：上海乐芙兰电子商务有限公司
付款账号：310066629013003064589
收款方：江苏苏泰华庆贸易有限公司
收款账号：6222000066667777888
小写金额：￥3,000.00
用途：材料款
回单编号：FLOW202405200002
"""
    result = run_document_extraction_agent(
        "bank_receipt_bundle",
        page_text,
        "31006662901300306458920240520_2.pdf",
        metadata={"raw_pages": [{"page": 1, "text": page_text, "table_rows": [], "source": "pdf_native"}]},
    )
    data = result.extracted_json
    assert data["receipt_count"] == 2
    assert data["valid_receipt_count"] == 2
    assert data["receipts"][0]["amount"] == Decimal("1234.56")
    assert data["receipts"][0]["payee_name"] == "南京华韵商务服务有限公司"
    assert data["receipts"][1]["amount"] == Decimal("3000.00")
    assert data["receipts"][1]["payee_name"] == "江苏苏泰华庆贸易有限公司"


def test_receipt_bundle_does_not_override_standard_bank_statement_detection():
    text = """中国工商银行账户明细清单
账号：1001068319100134987
时间范围：20250401 - 20260331
交易日期 余额 借方发生额 贷方发生额 对方户名 摘要
2025-04-01 100.00 0.00 100.00 上海客户有限公司 项目款
"""
    assert detect_document_type_code(text, filename="工商银行账户明细.pdf") == "bank_statement"


def test_receipt_bundle_is_only_auxiliary_material_in_bank_statement_aggregate():
    text = "\n".join(page["text"] for page in receipt_pages())
    result = run_document_extraction_agent(
        "bank_receipt_bundle",
        text,
        "工商银行电子回单.pdf",
        metadata={"raw_pages": receipt_pages()},
    )
    aggregate = aggregate_customer_bank_statements(
        [{
            "extraction_type": "bank_receipt_bundle",
            "file_name": "工商银行电子回单.pdf",
            "extracted_data": {"extracted_json": result.extracted_json},
        }],
        customer_profile={"name": "上海乐芙兰电子商务有限公司"},
    )
    assert aggregate["file_count"] == 1
    assert aggregate["included_files_count"] == 0
    assert aggregate["account_count"] == 0
    assert aggregate["raw_transaction_count"] == 0
    assert aggregate["deduplicated_transaction_count"] == 0
    assert aggregate["aggregate_status"] == "未达标"
    assert aggregate["standard_account_statement_file_count"] == 0
    assert aggregate["receipt_bundle_file_count"] == 1
    assert aggregate["receipt_bundle_with_details_count"] == 1
    assert aggregate["receipt_detail_count"] == 2
    assert aggregate["file_quality"][0]["statement_subtype"] == "receipt_bundle"
    assert aggregate["file_quality"][0]["included"] is False
    assert "已提取 2 条回单明细" in aggregate["file_quality"][0]["problem"]

    markdown = render_customer_bank_flow_aggregate_markdown(aggregate)
    assert "银行流水聚合分析" in markdown
    assert "标准账户流水文件数：0 份" in markdown
    assert "银行回单集合文件数：1 份" in markdown
    assert "辅助回单材料：已识别 1 份银行回单集合，合计提取 2 条回单明细" in markdown
    assert "疑似银行回单集合" in markdown
    assert "暂无法生成的统计" in markdown
    assert "客户级流水摘要" not in markdown
