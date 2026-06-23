from backend.services.bank_statement_agent.aggregator import (
    aggregate_customer_bank_statements,
    render_customer_bank_flow_aggregate_markdown,
)


def statement(file_name, bank, account_no, account_name, transactions, start="2025-04-01", end="2025-04-30"):
    return {
        "extraction_type": "bank_statement",
        "file_name": file_name,
        "extracted_data": {
            "extracted_json": {
                "doc_type": "bank_statement",
                "source_file": file_name,
                "bank_name": bank,
                "account_name": account_name,
                "account_no": account_no,
                "opening_bank": f"{bank}营业部",
                "currency": "人民币",
                "period_start": start,
                "period_end": end,
                "transaction_count": len(transactions),
                "amount_recognition_status": "完整识别",
                "transactions": transactions,
            }
        },
    }


def tx(time, direction, amount, counterparty, *, account="", summary="跨行转账", purpose="工程款", category="", balance="1000.00"):
    return {
        "transaction_time": time,
        "direction": direction,
        "amount": amount,
        "balance": balance,
        "counterparty_account": account,
        "counterparty_name": counterparty,
        "summary": summary,
        "purpose": purpose,
        "category": category or ("经营入账" if direction == "入账" else "经营出账"),
    }


def test_single_bank_statement_can_generate_customer_aggregate_report():
    data = aggregate_customer_bank_statements([
        statement("工商银行4月.pdf", "中国工商银行", "1001", "上海测试有限公司", [
            tx("2025-04-10 10:00:00", "入账", "10000.00", "客户甲有限公司", purpose="项目款"),
        ])
    ], customer_id="c1")
    assert data["file_count"] == 1
    assert data["account_count"] == 1
    assert data["effective_in_amount"] == 10000
    markdown = render_customer_bank_flow_aggregate_markdown(data)
    assert "## 银行流水聚合分析" in markdown
    assert "当前仅基于 1 个银行账户/1 份对账单进行聚合分析" in markdown


def test_multi_statement_duplicate_transactions_are_deduplicated():
    duplicate = tx("2025-04-10 10:00:00", "入账", "10000.00", "客户甲有限公司", purpose="项目款")
    data = aggregate_customer_bank_statements([
        statement("上海银行4月.pdf", "上海银行", "2001", "上海测试有限公司", [duplicate]),
        statement("上海银行4月重复.pdf", "上海银行", "2001", "上海测试有限公司", [duplicate]),
    ])
    assert data["raw_transaction_count"] == 2
    assert data["deduplicated_transaction_count"] == 1
    assert data["duplicate_transaction_count"] == 1
    assert data["effective_in_amount"] == 10000


def test_multi_bank_cross_account_transfer_is_excluded():
    data = aggregate_customer_bank_statements([
        statement("工商银行.pdf", "中国工商银行", "1001", "上海测试有限公司", [
            tx("2025-04-10 10:00:00", "出账", "50000.00", "上海测试有限公司", account="2001", purpose="内部划转", category="往来出账"),
            tx("2025-04-11 10:00:00", "出账", "8000.00", "供应商乙有限公司", purpose="材料款"),
        ]),
        statement("上海银行.pdf", "上海银行", "2001", "上海测试有限公司", [
            tx("2025-04-10 10:01:00", "入账", "50000.00", "上海测试有限公司", account="1001", purpose="内部划转", category="往来入账"),
        ]),
    ])
    assert data["account_count"] == 2
    assert data["internal_transfer_count"] == 2
    assert data["effective_out_amount"] == 8000
    assert all(item["name"] != "上海测试有限公司" for item in data["supplier_outflow_summary"])
    markdown = render_customer_bank_flow_aggregate_markdown(data)
    assert "内部划转及关联人往来" in markdown
    assert "客户名下账户之间内部划转" in markdown


def test_related_person_transfer_excluded_with_profile_but_personal_name_not_guessed_without_profile():
    extraction = statement("上海银行.pdf", "上海银行", "2001", "上海测试有限公司", [
        tx("2025-04-10 10:00:00", "出账", "30000.00", "张三", purpose="材料款"),
        tx("2025-04-11 10:00:00", "出账", "7000.00", "李四", purpose="材料款"),
    ])
    with_profile = aggregate_customer_bank_statements([extraction], customer_profile={"legal_representative_name": "张三"})
    assert with_profile["related_person_transfer_count"] == 1
    assert with_profile["effective_out_amount"] == 7000
    assert with_profile["supplier_outflow_summary"][0]["name"] == "李四"
    markdown = render_customer_bank_flow_aggregate_markdown(with_profile)
    assert "法人/关联人转账" in markdown
    assert "法定代表人" in markdown

    without_profile = aggregate_customer_bank_statements([extraction])
    assert without_profile["related_person_transfer_count"] == 0
    assert without_profile["effective_out_amount"] == 37000
    assert "关联人名单缺失" in " ".join(without_profile["manual_review_items"])
