from backend.services.bank_statement_agent.aggregator import (
    aggregate_customer_bank_statements,
    render_customer_bank_flow_aggregate_markdown,
)
from backend.extraction_skills.bank_statement import render_bank_statement_markdown


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


def test_low_quality_receipt_bundle_files_do_not_pollute_customer_aggregate():
    bad_name = "上海顺衡物流有限公司 单位国内汇款手续费 上海汇付支付有限公司 薯一薯二文化传媒有限公司 支付宝 华润守正招标有限公司"
    files = []
    for index in range(1, 5):
        files.append({
            "extraction_type": "bank_statement",
            "file_name": f"3100666290130030645892024053{index}_{index}.pdf",
            "extracted_data": {
                "extracted_json": {
                    "doc_type": "bank_statement",
                    "source_file": f"3100666290130030645892024053{index}_{index}.pdf",
                    "bank_name": "中国工商银行",
                    "statement_subtype": "receipt_bundle",
                    "account_name": bad_name,
                    "account_no": "",
                    "opening_bank": "",
                    "period_start": "",
                    "period_end": "",
                    "amount_recognition_status": "未识别",
                    "parse_quality_status": "partial",
                    "account_info_valid": False,
                    "transactions_valid": False,
                    "amounts_valid": False,
                    "can_join_effective_flow_statistics": False,
                    "candidate_transaction_rows": 30,
                    "transactions": [
                        {"transaction_time": "2000-00", "counterparty_name": "上海汇付支付有限公司", "summary": "单位国内汇款手续费"}
                    ],
                }
            },
        })

    data = aggregate_customer_bank_statements(
        files,
        customer_profile={"name": "上海乐芙兰电子商务有限公司"},
    )
    assert data["customer_name"] == "上海乐芙兰电子商务有限公司"
    assert data["file_count"] == 4
    assert data["account_count"] == 0
    assert data["unrecognized_account_file_count"] == 4
    assert data["included_files_count"] == 0
    assert data["raw_transaction_count"] == 0
    assert data["deduplicated_transaction_count"] == 0
    assert data["aggregate_status"] == "未达标"
    assert data["period_start"] == ""
    assert data["monthly_summary"] == []

    markdown = render_customer_bank_flow_aggregate_markdown(data)
    assert "聚合状态：未达标" in markdown
    assert "金额识别完整度：不可评估" in markdown
    assert "聚合说明：当前 4 份文件均未形成标准账户流水明细" in markdown
    assert "当前交易统计仅统计已形成标准流水明细的交易" in markdown
    assert "文件解析质量清单" in markdown
    assert "疑似银行回单集合" in markdown
    assert "暂无已识别银行账户" in markdown
    assert bad_name not in markdown
    assert "上海乐芙兰电子商务有限公司" in markdown
    assert "无法计算" in markdown
    assert "### 客户级流水摘要" not in markdown
    assert "### 主要入账客户" not in markdown
    assert "### 主要出账供应商" not in markdown
    assert "### 内部划转及关联人往来" not in markdown
    assert "暂无可统计的有效交易明细，无法计算剔除项" in markdown
    assert "金额完整识别文件数" not in markdown
    assert "金额未识别文件数" not in markdown
    assert "### 风险提示" not in markdown
    assert "### 分析限制" in markdown
    assert "0/0" not in markdown
    assert "2000-00" not in markdown


def test_receipt_bundle_single_file_markdown_uses_receipt_bundle_language():
    markdown = render_bank_statement_markdown({
        "source_file": "31006662901300306458920240531_4.pdf",
        "statement_subtype": "receipt_bundle",
        "bank_name": "中国工商银行",
        "manual_review_items": ["交易明细结构未恢复，暂不纳入客户级有效经营流水统计。"],
    })
    assert "## 银行回单集合" in markdown
    assert "资料类型：银行回单集合" in markdown
    assert "提取状态：部分成功" in markdown
    assert "是否形成账户流水明细：否" in markdown
    assert "是否纳入经营流水聚合：否" in markdown
    assert "后续处理建议" in markdown
    assert "回单日期、收付款方、账号、金额、用途、摘要和回单编号" in markdown
    assert "有效入账笔数" not in markdown
    assert "有效出账笔数" not in markdown
    assert "金额列缺失" not in markdown


def test_bocm_multiple_files_same_account_aggregate_as_one_account():
    tx1 = tx("2024-05-13", "出账", "766.14", "上海顺衡物流有限公司", account="622200001", summary="物流费", purpose="单位国内汇款手续费", category="经营出账", balance="145209.67")
    tx2 = tx("2024-05-24", "入账", "206360.00", "上海汇付支付有限公司", account="622200002", summary="货款", purpose="单位国内汇款", category="经营入账", balance="351569.67")
    tx3 = tx("2024-05-31", "出账", "3000.00", "江苏苏泰华庆贸易有限公司", account="622200003", summary="材料款", purpose="单位国内汇款", category="经营出账", balance="348569.67")
    files = [
        statement("31006662901300306458920240520_2.pdf", "交通银行", "310066629013003064589", "上海乐芙兰电子商务有限公司", [tx1], start="2024-05-01", end="2024-05-20"),
        statement("31006662901300306458920240524_3.pdf", "交通银行", "310066629013003064589", "上海乐芙兰电子商务有限公司", [tx2], start="2024-05-01", end="2024-05-24"),
        statement("31006662901300306458920240531_4.pdf", "交通银行", "310066629013003064589", "上海乐芙兰电子商务有限公司", [tx3], start="2024-05-01", end="2024-05-31"),
    ]
    for item in files:
        payload = item["extracted_data"]["extracted_json"]
        payload["bank_format"] = "bocm_statement"
        payload["statement_subtype"] = "account_statement"
        payload["opening_bank"] = "交通银行上海长宁支行"
        payload["statement_year"] = "2024"
        payload["statement_month"] = "05"
        payload["parse_quality_status"] = "success"
        payload["account_info_valid"] = True
        payload["transactions_valid"] = True
        payload["amounts_valid"] = True
        payload["can_join_effective_flow_statistics"] = True
    data = aggregate_customer_bank_statements(files, customer_profile={"name": "上海乐芙兰电子商务有限公司"})
    assert data["file_count"] == 3
    assert data["included_files_count"] == 3
    assert data["receipt_bundle_file_count"] == 0
    assert data["account_count"] == 1
    assert data["bank_accounts"][0]["bank_name"] == "交通银行"
    assert data["bank_accounts"][0]["opening_bank"] == "交通银行上海长宁支行"
    assert data["bank_accounts"][0]["account_no"] == "310066629013003064589"
    assert data["bank_accounts"][0]["account_name"] == "上海乐芙兰电子商务有限公司"
    assert data["bank_accounts"][0]["file_count"] == 3
    assert len(data["monthly_file_groups"]) == 1
    assert data["monthly_file_groups"][0]["month"] == "2024-05"
    assert data["monthly_file_groups"][0]["file_count"] == 3
    assert data["monthly_file_groups"][0]["transaction_count"] == 3
    markdown = render_customer_bank_flow_aggregate_markdown(data)
    assert "已识别银行账户数：1 个" in markdown
    assert "银行回单集合文件数：0 份" not in markdown
    assert "辅助回单明细数量：0 条" not in markdown
    assert "| 序号 | 银行名称 | 开户机构 | 账号 | 户名 | 时间范围 | 文件数 | 交易笔数 |" in markdown
    assert "### 月度文件清单" in markdown
    assert "| 2024-05 | 交通银行 | 310066629013003064589 | 上海乐芙兰电子商务有限公司 | 3 |" in markdown
    assert "### 来源文件" in markdown
    assert "| 序号 | 来源文件 | 识别银行 | 文件类型 | 日期范围 | 交易笔数 | 是否纳入经营流水聚合 | 问题说明 |" in markdown
    assert "交通银行上海长宁支行" in markdown
