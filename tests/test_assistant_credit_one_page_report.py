import asyncio
import json
from decimal import Decimal

from backend.services.assistant_credit_report_service import (
    _REQUIRED_HEADINGS,
    build_credit_report_model,
    build_report_context,
    generate_credit_one_page_report,
    get_customer_materials,
    is_credit_report_request,
)
from backend.services.credit_report_markdown_renderer import (
    TABLE_HEADERS,
    format_number,
    render_credit_card_table,
    render_core_metrics_table,
    render_loan_table,
    render_query_table,
    render_related_liability_table,
    render_rule_check_table,
    render_money,
)


def run(coro):
    return asyncio.run(coro)


class FakeStorage:
    def __init__(self, customers=None, extractions=None, profile=None, documents=None):
        self.customers = customers or []
        self.extractions = extractions or {}
        self.profile = profile or {}
        self.documents = documents or {}

    async def get_customer(self, customer_id):
        return next((item for item in self.customers if item["customer_id"] == customer_id), None)

    async def list_customers(self):
        return self.customers

    async def get_extractions_by_customer(self, customer_id):
        return self.extractions.get(customer_id, [])

    async def get_customer_profile(self, customer_id):
        markdown = self.profile.get(customer_id)
        return {"markdown_content": markdown} if markdown is not None else None

    async def list_documents(self, customer_id):
        return self.documents.get(customer_id, [])


class FakeAI:
    def __init__(self, response=None):
        self.prompt = ""
        self.content = ""
        self.response = response or {
            "emergency_attention": [],
            "query_frequency_analysis": "按已保存查询记录展示，准入结论待具体银行规则评估。",
            "historical_credit_features": ["企业与个人信贷记录已按主体拆分。"],
            "optimization_urgent": [],
            "optimization_medium": [],
            "optimization_long": [],
            "advantages": [],
            "risks": [],
            "comprehensive_summary": "企业征信与法人个人征信已分口径展示。",
            "one_sentence_conclusion": "需结合具体银行正式准入规则进一步评估。",
        }

    def extract(self, prompt, content, model="deepseek-chat", timeout=None, max_tokens=8192):
        self.prompt = prompt
        self.content = content
        return json.dumps(self.response, ensure_ascii=False)


def company_customer(customer_id="enterprise_上海意川建筑科技有限公司", name="上海意川建筑科技有限公司", **overrides):
    row = {
        "customer_id": customer_id,
        "name": name,
        "customer_type": "enterprise",
        "id_card": "",
        "phone": "02100000000",
        "updated_at": "2026-09-18T09:00:00",
    }
    row.update(overrides)
    return row


def extraction(extraction_type, structured, markdown="", status="success"):
    data = dict(structured)
    if markdown:
        data["markdown"] = markdown
    return {
        "extraction_type": extraction_type,
        "extraction_status": status,
        "extracted_data": data,
        "created_at": "2026-09-18T08:00:00",
    }


def enterprise_credit_payload():
    return {
        "extracted_json": {
            "report_meta": {
                "customer_name": "上海意川建筑科技有限公司",
                "unified_social_credit_code": "91310118MA1JP7UB2B",
                "report_time": "2026-03-20",
            },
            "credit_summary": {
                "unsettled_credit_balance": 5_000_000,
                "unsettled_credit_institution_count": 1,
                "external_guarantee_balance": 0,
            },
            "short_term_loans": [{
                "institution_name": "示例企业银行",
                "business_type": "流动资金贷款",
                "loan_amount": 6_000_000,
                "balance": 5_000_000,
                "start_date": "2025-06-01",
                "end_date": "2026-06-01",
                "status": "正常",
            }],
            "external_guarantees": [],
        }
    }


def personal_credit_payload():
    return {
        "report_json": {
            "basic_info": {
                "name": "黎云",
                "id_number": "310101199001011234",
                "report_number": "PC-20260320",
                "report_time": "2026-03-20",
                "marital_status": "已婚",
            },
            "credit_summary": {
                "loan_account_count": 2,
                "outstanding_loan_account_count": 1,
                "credit_card_account_count": 1,
                "loan_overdue_account_count": 0,
                "credit_card_overdue_account_count": 0,
                "loan_90d_overdue_account_count": 0,
                "credit_card_90d_overdue_account_count": 0,
            },
            "loan_accounts": [{
                "institution": "示例个人银行",
                "loan_type": "个人消费贷款",
                "loan_amount": "300,000",
                "balance": "180,000",
                "start_date": "2025-01-01",
                "due_date": "2028-01-01",
                "account_status": "正常",
            }],
            "credit_card_accounts": [{
                "issuer": "示例发卡行",
                "currency": "人民币",
                "credit_limit": "100,000",
                "used_amount": "20,000",
                "account_status": "正常",
                "overdue_description": "无逾期",
            }],
            "related_repayment_responsibilities": [{
                "related_party": "上海意川建筑科技有限公司",
                "institution": "远东宏信普惠融资租赁（天津）有限公司",
                "responsibility_amount": "20,000,000",
                "loan_balance": "18,739,532",
                "responsibility_type": "保证人",
                "business_type": "融资租赁",
                "as_of_date": "2026-03-12",
                "evidence": "截至2026年03月12日，融资租赁余额18,739,532（人民币元）。",
            }],
            "query_records": [
                {"query_date": "2026-03-10", "query_institution": "A银行", "query_reason": "贷款审批"},
                {"query_date": "2026-02-10", "query_institution": "B银行", "query_reason": "信用卡审批"},
            ],
            "overdue_records": [],
            "public_records": [{"record_type": "系统明确记载无公共记录", "content": "无"}],
            "non_credit_transactions": [],
        }
    }


def complete_storage(include_finance=False):
    row = company_customer()
    items = [
        extraction("enterprise_credit_report", enterprise_credit_payload(), "## 企业征信\n- 企业征信主体：上海意川建筑科技有限公司"),
        extraction("personal_credit_report", personal_credit_payload(), "## 个人征信\n- 姓名：黎云"),
        extraction(
            "business_license",
            {"company_name": row["name"], "legal_representative": "黎云", "actual_controller": "黎云"},
            "## 营业执照\n- 法定代表人：黎云\n- 实际控制人：黎云",
        ),
    ]
    if include_finance:
        items.extend([
            extraction("financial_report", {"asset_liability_ratio": "99.92%", "revenue_trend": "连续下滑"}, "## 财务报表\n- 资产负债率：99.92%"),
            extraction("enterprise_flow", {"operating_cashflow": "负"}, "## 企业流水\n- 经营现金流为负"),
        ])
    return FakeStorage(
        customers=[row],
        extractions={row["customer_id"]: items},
        profile={row["customer_id"]: "# 资料汇总\n\n## 企业征信\n企业征信资料\n\n## 财务报表\n资产负债率99.92%\n\n## 企业流水\n经营现金流为负"},
        documents={row["customer_id"]: [{"is_active": True}]},
    )


def generated(storage=None, ai=None):
    storage = storage or complete_storage()
    row = storage.customers[0]
    return run(generate_credit_one_page_report(storage, ai or FakeAI(), "根据上海意川建筑科技有限公司的资料生成征信一页纸", row["customer_id"]))


def test_customer_and_credit_subjects_remain_separate():
    storage = complete_storage()
    materials = run(get_customer_materials(storage, storage.customers[0]["customer_id"]))
    model = build_credit_report_model(materials)
    assert model["subjects"]["customer_subject"] == "上海意川建筑科技有限公司"
    assert model["subjects"]["enterprise_credit_subject"] == "上海意川建筑科技有限公司"
    assert model["subjects"]["personal_credit_subject"] == "黎云"
    assert model["subjects"]["personal_credit_subject_role"] == "法定代表人 / 实际控制人"


def test_personal_loan_and_card_are_not_rendered_as_enterprise_accounts():
    report = generated()["message"]
    enterprise_section = report.split("## 企业贷款", 1)[1].split("## 个人贷款", 1)[0]
    personal_section = report.split("## 个人贷款", 1)[1].split("# 三、信用卡", 1)[0]
    card_section = report.split("# 三、信用卡", 1)[1].split("# 四、", 1)[0]
    assert "示例个人银行" not in enterprise_section
    assert "示例个人银行" in personal_section
    assert "个人征信主体“黎云”" in card_section
    assert "不属于企业信用卡" in card_section


def test_enterprise_guarantee_and_personal_related_liability_coexist():
    report = generated()["message"]
    enterprise_guarantee = report.split("## 4.1 企业对外担保", 1)[1].split("## 4.2", 1)[0]
    personal_related = report.split("## 4.2 法人相关还款责任", 1)[1].split("# 五、", 1)[0]
    assert "明确记录为0" in enterprise_guarantee
    assert "18,739,532" not in enterprise_guarantee
    assert "18,739,532" in personal_related
    assert "18,739,532元" in personal_related
    assert "责任主体" in personal_related and "黎云" in personal_related
    assert "不等同于企业对外担保" in personal_related


def test_credit_report_context_excludes_finance_flow_and_property():
    storage = complete_storage(include_finance=True)
    materials = run(get_customer_materials(storage, storage.customers[0]["customer_id"]))
    build_credit_report_model(materials)
    context = build_report_context(materials)
    assert "99.92%" not in context
    assert "资产负债率" not in context
    assert "经营现金流" not in context
    assert "企业流水" not in context


def test_llm_receives_only_credit_facts_not_financial_metrics():
    ai = FakeAI()
    result = generated(complete_storage(include_finance=True), ai)
    assert result["data"]["reportStatus"] == "completed"
    assert "资产负债率" not in ai.content
    assert "99.92%" not in ai.content
    assert "经营现金流" not in result["message"]


def test_query_frequency_rule_is_pending_evaluation_not_model_threshold():
    storage = complete_storage()
    payload = personal_credit_payload()["report_json"]
    payload["query_records"] = [
        {"query_date": "2026-03-10", "query_institution": f"银行{i}", "query_reason": "贷款审批"}
        for i in range(21)
    ]
    customer_id = storage.customers[0]["customer_id"]
    storage.extractions[customer_id] = [
        extraction("enterprise_credit_report", enterprise_credit_payload()),
        extraction("personal_credit_report", {"report_json": payload}),
        extraction("business_license", {"legal_representative": "黎云", "actual_controller": "黎云"}),
    ]
    materials = run(get_customer_materials(storage, customer_id))
    model = build_credit_report_model(materials)
    rule = next(item for item in model["rule_checks"] if item["item"] == "查询频率")
    assert rule["status"] == "待评估"
    assert "尚未配置正式硬查询类型范围" in rule["basis"]


def test_missing_dti_is_not_calculated():
    storage = complete_storage()
    materials = run(get_customer_materials(storage, storage.customers[0]["customer_id"]))
    model = build_credit_report_model(materials)
    rule = next(item for item in model["rule_checks"] if item["item"] == "负债收入比 DTI")
    assert rule["status"] == "资料不足"
    assert model["metrics"]["dti"] is None


def test_metric_names_keep_account_semantics_not_overdue_times():
    report = generated()["message"]
    assert "贷款逾期账户数" in report
    assert "信用卡逾期账户数" in report
    assert "贷款逾期次数" not in report
    assert "相关还款责任余额" in report


def test_all_required_report_headings_are_present():
    report = generated()["message"]
    assert all(heading in report for heading in _REQUIRED_HEADINGS)


def test_all_deterministic_markdown_tables_have_consistent_columns():
    samples = [
        render_core_metrics_table({}),
        render_loan_table([{"index": 1, "institution": "A/B"}]),
        render_credit_card_table([]),
        render_related_liability_table([{"responsible_subject": "黎云"}]),
        render_query_table([{"window": "近1月", "loan_approval": 1}]),
        render_rule_check_table([{"item": "硬查询", "status": "待评估"}]),
    ]
    for table in samples:
        rows = [line for line in table.splitlines() if line.startswith("|")]
        expected = rows[0].count("|")
        assert expected >= 3
        assert all(row.count("|") == expected for row in rows)


def test_missing_values_are_not_defaulted_to_zero():
    storage = complete_storage()
    personal = personal_credit_payload()["report_json"]
    personal["credit_summary"].pop("credit_card_account_count")
    personal["credit_card_accounts"] = []
    customer_id = storage.customers[0]["customer_id"]
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    materials = run(get_customer_materials(storage, customer_id))
    model = build_credit_report_model(materials)
    assert model["metrics"]["credit_card_account_count"] is None
    assert next(item for item in model["rule_checks"] if item["item"] == "信用卡数量")["status"] == "资料不足"
    assert model["metrics"]["credit_card_usage_rate"] is None


def test_missing_query_records_are_insufficient_not_pending_evaluation():
    storage = complete_storage()
    personal = personal_credit_payload()["report_json"]
    personal["query_records"] = []
    customer_id = storage.customers[0]["customer_id"]
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    materials = run(get_customer_materials(storage, customer_id))
    model = build_credit_report_model(materials)
    assert model["metrics"]["institution_query_6m_count"] is None
    assert next(item for item in model["rule_checks"] if item["item"] == "查询频率")["status"] == "资料不足"


def test_current_customer_and_explicit_name_both_generate():
    storage = complete_storage()
    row = storage.customers[0]
    current = run(generate_credit_one_page_report(storage, FakeAI(), "生成征信一页纸", row["customer_id"]))
    named = run(generate_credit_one_page_report(storage, FakeAI(), "根据上海意川建筑科技有限公司的资料生成征信一页纸"))
    assert current["data"]["reportStatus"] == "completed"
    assert named["data"]["reportStatus"] == "completed"


def test_duplicate_customer_name_waits_for_input_without_internal_id():
    rows = [company_customer("enterprise_1"), company_customer("enterprise_2")]
    result = run(generate_credit_one_page_report(FakeStorage(customers=rows), FakeAI(), "根据上海意川建筑科技有限公司的资料生成征信一页纸"))
    assert result["data"]["reportStatus"] == "waiting_for_input"
    assert "找到多个同名客户，请选择" in result["message"]
    assert "enterprise_1" not in result["message"]


def test_customer_not_found_is_business_message():
    result = run(generate_credit_one_page_report(FakeStorage(), FakeAI(), "根据不存在客户的资料生成征信一页纸"))
    assert result["data"]["reportStatus"] == "customer_not_found"
    assert "未找到" in result["message"]


def test_no_personal_credit_keeps_personal_sections_unknown():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    storage.extractions[customer_id] = storage.extractions[customer_id][:1]
    result = generated(storage)
    assert result["data"]["reportStatus"] == "completed"
    assert "个人征信主体：暂未获取" in result["message"]
    personal_section = result["message"].split("## 个人贷款", 1)[1].split("# 三、", 1)[0]
    assert "资料不足" in personal_section


def test_identifier_is_masked_and_internal_ids_are_absent():
    report = generated()["message"]
    assert "91310118MA1JP7UB2B" not in report
    assert "310101199001011234" not in report
    assert "customer_id" not in report
    assert "extraction_id" not in report


def test_untrusted_llm_financial_commentary_is_rejected():
    bad = FakeAI(response={
        "emergency_attention": ["资产负债率99.92%需要关注"],
        "query_frequency_analysis": "资料不足",
        "historical_credit_features": [],
        "optimization_urgent": [],
        "optimization_medium": [],
        "optimization_long": [],
        "advantages": [],
        "risks": [],
        "comprehensive_summary": "经营现金流为负",
        "one_sentence_conclusion": "资料不足",
    })
    report = generated(ai=bad)["message"]
    assert "99.92%" not in report
    assert "经营现金流" not in report


def test_ordinary_chat_does_not_trigger_report_intent():
    assert not is_credit_report_request("你好，今天怎么样？")
    assert not is_credit_report_request("征信报告一般包含哪些内容？")
    assert is_credit_report_request("分析一下这个客户的征信")


def test_no_materials_and_processing_are_business_safe():
    row = company_customer()
    no_materials = run(generate_credit_one_page_report(FakeStorage(customers=[row]), FakeAI(), "生成征信一页纸", row["customer_id"]))
    assert no_materials["data"]["reportStatus"] == "no_materials"
    processing_storage = FakeStorage(customers=[row], extractions={row["customer_id"]: [extraction("personal_credit_report", {}, status="running")]})
    processing = run(generate_credit_one_page_report(processing_storage, FakeAI(), "生成征信一页纸", row["customer_id"]))
    assert processing["data"]["reportStatus"] == "materials_processing"


def test_report_does_not_expose_raw_ocr_text():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    leaked = "个人信用报告\n信息概要\n" + "从未逾期过的贷记卡账户。" * 80
    personal["raw_text"] = leaked
    personal["public_records"] = [{"record_type": "公共记录", "content": leaked}]
    storage.extractions[customer_id][1] = extraction(
        "personal_credit_report", {"report_json": personal, "raw_markdown": leaked}
    )
    ai = FakeAI()
    report = generated(storage, ai)["message"]
    assert "从未逾期过的贷记卡账户" not in report
    assert "从未逾期过的贷记卡账户" not in ai.content
    assert "个人信用报告\n信息概要" not in report
    assert "资料异常，需核验" in report


def test_report_does_not_contain_html_br():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["public_records"] = [{"record_type": "公共记录", "content": "第一段<br>第二段<br/>第三段"}]
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    report = generated(storage)["message"]
    assert "<br" not in report.lower()
    assert "资料异常，需核验" in report


def test_report_table_headers_are_separate_columns():
    report = generated()["message"]
    assert "| 主体信息 | 内容 |" in report
    assert "| 指标 | 当前情况 | 口径/来源 |" in report
    assert "| 序号 | 贷款机构 | 机构类别 | 贷款类型 | 合同金额 | 当前余额 | 发放日期 | 到期日期 | 状态/备注 |" in report
    assert "| 责任主体 | 被担保/关联主体 | 贷款机构 | 责任金额 | 当前余额 | 责任类型 | 业务类型 | 截至日期 |" in report
    assert "**主体信息内容**" not in report


def test_all_credit_report_tables_have_expected_headers():
    expected = {
        "subject": ("主体信息", "内容"),
        "core_metrics": ("指标", "当前情况", "口径/来源"),
        "loan": ("序号", "贷款机构", "机构类别", "贷款类型", "合同金额", "当前余额", "发放日期", "到期日期", "状态/备注"),
        "credit_card": ("发卡行", "币种", "信用额度", "已用额度", "使用率", "逾期", "备注"),
        "enterprise_guarantee": ("被担保主体", "贷款机构", "担保金额", "当前余额", "担保日期", "状态"),
        "related_liability": ("责任主体", "被担保/关联主体", "贷款机构", "责任金额", "当前余额", "责任类型", "业务类型", "截至日期"),
        "query": ("时间范围", "贷款审批", "信用卡审批", "担保资格审查", "法人资信审查"),
        "rule_check": ("检查项", "状态", "当前情况", "判断依据", "优化方向"),
    }
    assert TABLE_HEADERS == expected
    report = generated()["message"]
    for headers in expected.values():
        assert "| " + " | ".join(headers) + " |" in report


def test_person_enterprise_relation_from_structured_kyc():
    storage = complete_storage()
    materials = run(get_customer_materials(storage, storage.customers[0]["customer_id"]))
    model = build_credit_report_model(materials)
    assert model["subjects"]["personal_credit_subject_role"] == "法定代表人 / 实际控制人"
    assert "个人与企业关系：法定代表人 / 实际控制人" in generated(storage)["message"]


def test_person_enterprise_relation_supports_nested_structured_kyc():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    storage.extractions[customer_id][2] = extraction(
        "business_license",
        {"legalRepresentative": {"name": "黎云"}, "actualController": {"name": "黎云"}},
    )
    materials = run(get_customer_materials(storage, customer_id))
    assert build_credit_report_model(materials)["subjects"]["personal_credit_subject_role"] == "法定代表人 / 实际控制人"


def test_credit_card_rmb_amount_uses_yuan():
    report = generated()["message"]
    assert "100,000元" in report
    assert "20,000元" in report


def test_related_liability_rmb_amount_uses_yuan():
    report = generated()["message"]
    # This fixture declares yuan only for the balance, not responsibility_amount.
    assert "20,000,000（单位待核验）" in report
    assert "18,739,532元" in report


def test_unknown_unit_remains_needs_review():
    report = generated()["message"]
    assert "300,000（单位待核验）" in report


def test_zero_overdue_does_not_render_fake_overdue_detail():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["overdue_records"] = [{"account_type": "逾期", "current_status": "逾期"}]
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    section = generated(storage)["message"].split("## 个人征信逾期", 1)[1].split("## 公共记录", 1)[0]
    assert "贷款逾期账户数：0" in section
    assert "信用卡逾期账户数：0" in section
    assert "90天以上逾期账户数：0" in section
    assert "账户/业务类型" not in section
    assert "稳定结构化逾期明细" not in section


def test_overdue_section_uses_structured_fields_only():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["raw_text"] = "个人信用报告 信息概要 账户/业务类型：逾期；逾期" * 30
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    section = generated(storage)["message"].split("## 个人征信逾期", 1)[1].split("## 公共记录", 1)[0]
    assert "账户/业务类型：逾期" not in section
    assert "个人信用报告" not in section


def test_query_total_not_called_hard_query_without_rule():
    report = generated()["message"]
    assert "近6个月征信机构查询次数" in report
    assert "硬查询次数" not in report
    assert "| 查询频率 | 待评估 |" in report


def test_past_due_date_uses_generated_at_and_does_not_claim_future_due():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["loan_accounts"][0].update({"institution": "中国建设银行", "due_date": "2026-04-23"})
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    bad_ai = FakeAI()
    bad_ai.response["historical_credit_features"] = ["中国建设银行贷款将于2026-04-23到期，距报告时间较近。"]
    report = generated(storage, bad_ai)["message"]
    assert "原到期日为2026-04-23，早于本报告生成日；当前是否已结清、续贷或展期资料不足，需核实" in report
    assert "将于2026-04-23到期" not in report
    assert "距报告时间较近" not in report


def test_query_windows_are_explicitly_based_on_source_report_date():
    report = generated()["message"]
    assert "均以个人征信源报告日期 2026-03-20 为基准，不以本报告生成时间为基准" in report


def test_zero_overdue_removes_unreliable_overdue_items_from_all_sections():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["overdue_records"] = [{"account_type": "逾期"}, {"current_status": "逾期"}]
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    bad_ai = FakeAI()
    bad_ai.response["historical_credit_features"] = ["存在2条要素缺失的逾期条目。"]
    bad_ai.response["optimization_urgent"] = ["核验2条要素缺失的逾期条目。"]
    bad_ai.response["risks"] = ["2条要素缺失的逾期条目构成风险。"]
    bad_ai.response["comprehensive_summary"] = "存在2条要素缺失的逾期条目。"
    result = generated(storage, bad_ai)
    assert result["data"]["reportStatus"] == "completed"
    assert "2条要素缺失的逾期条目" not in result["message"]
    assert "2条要素缺失的逾期条目" not in bad_ai.content


def test_final_markdown_does_not_expose_internal_statuses():
    ai = FakeAI()
    ai.response["historical_credit_features"] = ["needs_review raw_text ocr_text normalized internal_status"]
    result = generated(ai=ai)
    forbidden = ("needs_review", "raw_text", "ocr_text", "normalized", "internal_status")
    assert all(token not in result["message"] for token in forbidden)
    assert all(token not in ai.content for token in forbidden)


def test_report_table_column_counts_match():
    report = generated()["message"]
    tables = []
    current = []
    for line in report.splitlines():
        if line.startswith("|"):
            current.append(line)
        elif current:
            tables.append(current)
            current = []
    if current:
        tables.append(current)
    assert tables
    for table in tables:
        width = table[0].count("|")
        assert width >= 3
        assert all(row.count("|") == width for row in table)


def test_conflicting_overdue_data_becomes_needs_review():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["credit_summary"]["loan_overdue_account_count"] = 0
    personal["loan_accounts"][0]["overdue_status"] = "当前逾期"
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    materials = run(get_customer_materials(storage, customer_id))
    model = build_credit_report_model(materials)
    rule = next(item for item in model["rule_checks"] if item["item"] == "逾期记录")
    assert rule["status"] == "待核验"
    assert rule["current"] == "资料存在冲突"
    assert "贷款逾期账户数=0" in rule["basis"]
    report = generated(storage)["message"]
    assert "| 逾期记录 | 待核验 | 资料存在冲突 |" in report
    urgent = report.split("## 优先核验", 1)[1].split("## 补充资料", 1)[0]
    assert "核对源征信报告" in urgent


def test_money_without_unit_marked_needs_review():
    report = generated()["message"]
    assert "300,000（单位待核验）" in report
    assert "180,000（单位待核验）" in report


def test_institution_count_not_renamed_to_loan_count():
    report = generated()["message"]
    assert "企业未结清贷款机构数" in report
    assert "企业未结清贷款笔数" not in report


def test_overdue_account_count_not_renamed_to_overdue_times():
    report = generated()["message"]
    assert "贷款逾期账户数" in report
    assert "信用卡逾期账户数" in report
    assert "逾期次数" not in report


def test_related_repayment_not_equal_enterprise_guarantee():
    report = generated()["message"]
    enterprise = report.split("## 4.1 企业对外担保", 1)[1].split("## 4.2", 1)[0]
    personal = report.split("## 4.2 法人相关还款责任", 1)[1].split("# 五、", 1)[0]
    assert "18,739,532" not in enterprise
    assert "18,739,532元" in personal
    assert "不等同于企业对外担保" in personal


def test_missing_data_does_not_fallback_to_raw_text():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    raw_only = {
        "report_json": {
            "basic_info": {"name": "黎云"},
            "credit_summary": {},
            "raw_text": "个人信用报告 信息概要 当前逾期账户1个" * 50,
        }
    }
    storage.extractions[customer_id][1] = extraction("personal_credit_report", raw_only)
    report = generated(storage)["message"]
    assert "当前逾期账户1个" not in report
    assert "贷款逾期账户数 | 资料不足" in report


def test_credit_card_summary_detail_count_mismatch_is_explained():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["credit_summary"]["credit_card_account_count"] = 11
    storage.extractions[customer_id][1] = extraction("personal_credit_report", {"report_json": personal})
    report = generated(storage)["message"]
    assert "征信概要账户数为11，本节可稳定提取明细数为1，两者不一致，需核验" in report
    assert "| 信用卡账户数量一致性 | 待核验 |" in report


def test_format_number_int():
    assert format_number(500) == "500"


def test_format_number_float_integer():
    assert format_number(500.0) == "500"


def test_format_number_float_decimal():
    assert format_number(500.25) == "500.25"


def test_format_number_decimal_integer():
    assert format_number(Decimal("500")) == "500"


def test_format_number_decimal_fraction():
    assert format_number(Decimal("500.2500")) == "500.25"


def test_format_number_none():
    assert format_number(None) == "资料不足"


def test_render_money_int():
    assert render_money(500, "元") == "500元"
    assert render_money(500) == "500（单位待核验）"


def test_credit_one_page_report_with_integer_amounts_does_not_crash():
    storage = complete_storage()
    customer_id = storage.customers[0]["customer_id"]
    enterprise = enterprise_credit_payload()["extracted_json"]
    enterprise["short_term_loans"][0]["loan_amount"] = 500
    enterprise["short_term_loans"][0]["balance"] = 500
    enterprise["credit_summary"]["unsettled_credit_balance"] = 500
    personal = personal_credit_payload()["report_json"]
    personal["loan_accounts"][0]["loan_amount"] = 500
    personal["loan_accounts"][0]["balance"] = 500
    personal["credit_summary"]["credit_card_account_count"] = 7
    personal["credit_card_accounts"] = personal["credit_card_accounts"] * 7
    personal["query_records"] = [
        {"query_date": "2026-03-10", "query_institution": f"银行{i}", "query_reason": "贷款审批"}
        for i in range(8)
    ]
    personal["related_repayment_responsibilities"] = [
        {"related_party": "上海意川建筑科技有限公司", "loan_balance": 0, "unit": "元"},
    ]
    storage.extractions[customer_id] = [
        extraction("enterprise_credit_report", {"extracted_json": enterprise}),
        extraction("personal_credit_report", {"report_json": personal}),
        extraction("business_license", {"legal_representative": "黎云", "actual_controller": "黎云"}),
    ]
    result = generated(storage)
    assert result["data"]["reportStatus"] == "completed"
    assert "500（单位待核验）" in result["message"]
    assert "账户数：7" in result["message"]
    assert "近6个月征信机构查询次数：8" in result["message"]


def test_chat_post_credit_one_page_report_returns_200(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import backend.services as services_package
    import backend.services.sqlalchemy_storage_service as sqlalchemy_storage_module

    storage = complete_storage()
    monkeypatch.setattr(services_package, "get_storage_service", lambda: storage)
    monkeypatch.setattr(sqlalchemy_storage_module, "SQLAlchemyStorageService", lambda: storage)
    from backend.routers import chat as chat_router

    app = FastAPI()
    app.include_router(chat_router.router, prefix="/api")
    monkeypatch.setattr(chat_router, "storage_service", storage)
    monkeypatch.setattr(chat_router, "ai_service", FakeAI())
    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={"messages": [{"role": "user", "content": "根据上海意川建筑科技有限公司的资料生成征信一页纸"}]},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "credit_one_page_report"
    assert payload["data"]["reportStatus"] == "completed"
    assert payload["message"].startswith("# 征信速览报告")
    assert "| 主体信息 | 内容 |" in payload["message"]
    assert "| 指标 | 当前情况 | 口径/来源 |" in payload["message"]
    assert "| 序号 | 贷款机构 | 机构类别 | 贷款类型 | 合同金额 | 当前余额 | 发放日期 | 到期日期 | 状态/备注 |" in payload["message"]
    assert "| 发卡行 | 币种 | 信用额度 | 已用额度 | 使用率 | 逾期 | 备注 |" in payload["message"]
    assert "| 时间范围 | 贷款审批 | 信用卡审批 | 担保资格审查 | 法人资信审查 |" in payload["message"]
    assert "| 检查项 | 状态 | 当前情况 | 判断依据 | 优化方向 |" in payload["message"]
    assert payload["message"].splitlines()[0] == "# 征信速览报告"


def test_generic_kyc_document_preserves_both_roles_in_final_report():
    storage = complete_storage()
    cid = storage.customers[0]["customer_id"]
    enterprise = enterprise_credit_payload()
    enterprise["extracted_json"]["actual_controller"] = {"name": "黎云"}
    storage.extractions[cid][0] = extraction("enterprise_credit_report", enterprise)
    storage.extractions[cid][2] = extraction("kyc_document_agent", {
        "doc_type": "business_license", "fields": {"legal_representative": "黎　云"},
    })
    report = generated(storage)["message"]
    assert "个人与企业关系：法定代表人 / 实际控制人" in report


def test_single_confirmed_role_is_preserved():
    for key, expected in (("legal_representative", "法定代表人"), ("actual_controller", "实际控制人")):
        storage = complete_storage()
        cid = storage.customers[0]["customer_id"]
        storage.extractions[cid][2] = extraction("business_license", {key: "黎云"})
        assert f"个人与企业关系：{expected}\n" in generated(storage)["message"]


def test_related_liability_section_currency_reaches_details_and_total():
    storage = complete_storage()
    cid = storage.customers[0]["customer_id"]
    personal = personal_credit_payload()["report_json"]
    personal["related_repayment_responsibilities"] = {
        "currency": "人民币",
        "records": [
            {"responsibility_amount": 4000000, "loan_balance": 3424532},
            {"loan_balance": 15315000},
        ],
    }
    storage.extractions[cid][1] = extraction("personal_credit_report", {"report_json": personal})
    report = generated(storage)["message"]
    for amount in ("4,000,000元", "3,424,532元", "18,739,532元"):
        assert amount in report


def test_document_currency_and_money_field_unit_priority():
    from backend.services.assistant_credit_report_service import _money, _personal_credit_model
    assert _money({"amount": {"value": 4, "unit": "万元"}, "currency": "人民币"}, "amount")["unit"] == "万元"
    model = _personal_credit_model({"currency": "人民币", "related_repayment_responsibilities": [{"loan_balance": 18739532}]}, "测试主体")
    assert model["related_repayment_responsibilities"][0]["balance"]["unit"] == "元"


def test_past_due_unknown_survives_empty_llm_emergency_attention():
    report = generated()["message"]
    emergency = report.split("## 优先核验事项", 1)[1].split("# 一、", 1)[0]
    assert "原到期日早于本报告生成日" in emergency
    assert "建议优先核实当前状态" in emergency
    assert "逾期" not in emergency
    assert "暂无需要立即处理" not in emergency


def test_runtime_renderer_and_final_markdown_reject_joined_headers():
    import pytest
    from backend.services.credit_report_markdown_renderer import render_markdown_table, has_valid_final_report_tables
    with pytest.raises(ValueError):
        render_markdown_table(["指标当前情况口径/来源"], [])
    report = generated()["message"]
    assert has_valid_final_report_tables(report)
    assert not has_valid_final_report_tables(report.replace("| 主体信息 | 内容 |", "| **主体信息内容** | |"))
    assert report.splitlines()[0] == "# 征信速览报告"
