import asyncio

from backend.services.assistant_credit_report_service import (
    CREDIT_ONE_PAGE_REPORT_TEMPLATE,
    build_report_context,
    generate_credit_one_page_report,
    get_customer_materials,
    is_credit_report_request,
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
    def __init__(self):
        self.prompt = ""
        self.content = ""

    def extract(self, prompt, content, model="deepseek-chat", timeout=None, max_tokens=8192):
        self.prompt = prompt
        self.content = content
        return CREDIT_ONE_PAGE_REPORT_TEMPLATE.format(
            customer_name="测试客户",
            masked_identifier="3101**********1234",
            generated_at="2026-09-18 10:00:00",
        )


def customer(customer_id="personal_刘聪", name="刘聪", **overrides):
    row = {
        "customer_id": customer_id,
        "name": name,
        "customer_type": "personal",
        "id_card": "310101199001011234",
        "phone": "13800000000",
        "updated_at": "2026-09-18T09:00:00",
    }
    row.update(overrides)
    return row


def extraction(extraction_type, content, status="success"):
    return {
        "extraction_type": extraction_type,
        "extraction_status": status,
        "extracted_data": {"markdown": content},
        "created_at": "2026-09-18T08:00:00",
    }


def complete_storage(markdown="## 个人征信\n- 未结清贷款余额：120000元"):
    row = customer()
    return FakeStorage(
        customers=[row],
        extractions={row["customer_id"]: [extraction("personal_credit_report", markdown)]},
        profile={row["customer_id"]: f"# 资料汇总\n\n{markdown}"},
        documents={row["customer_id"]: [{"is_active": True}]},
    )


def test_current_customer_generates_credit_one_page_report():
    ai = FakeAI()
    result = run(generate_credit_one_page_report(complete_storage(), ai, "生成征信一页纸", "personal_刘聪"))
    assert result["data"]["reportStatus"] == "completed"
    assert result["message"].startswith("# 征信速览报告")
    assert "# 十、综合判断" in result["message"]


def test_explicit_customer_name_overrides_missing_current_context():
    ai = FakeAI()
    result = run(generate_credit_one_page_report(complete_storage(), ai, "根据刘聪的资料生成征信一页纸"))
    assert result["data"]["reportStatus"] == "completed"
    assert "未结清贷款余额：120000元" in ai.content


def test_duplicate_customer_name_waits_for_input_without_exposing_customer_id():
    rows = [customer("personal_1"), customer("personal_2", phone="13900000000")]
    result = run(generate_credit_one_page_report(FakeStorage(customers=rows), FakeAI(), "根据刘聪的资料生成征信一页纸"))
    assert result["data"]["reportStatus"] == "waiting_for_input"
    assert "找到多个同名客户，请选择" in result["message"]
    assert "personal_1" not in result["message"]


def test_customer_not_found_is_business_message():
    result = run(generate_credit_one_page_report(FakeStorage(), FakeAI(), "根据不存在客户的资料生成征信一页纸"))
    assert result["data"]["reportStatus"] == "customer_not_found"
    assert "未找到" in result["message"]


def test_missing_personal_credit_is_marked_unprovided_in_context():
    storage = complete_storage("## 企业征信\n- 当前贷款余额：20万元")
    storage.extractions["personal_刘聪"] = [extraction("enterprise_credit_report", "## 企业征信\n- 当前贷款余额：20万元")]
    result = run(generate_credit_one_page_report(storage, FakeAI(), "生成征信一页纸", "personal_刘聪"))
    assert result["data"]["reportStatus"] == "completed"
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "## 个人征信\n未提供或未解析" in context


def test_partial_credit_material_is_preserved():
    storage = complete_storage("## 个人征信\n- 仅识别到贷款账户数量：2\n- 查询记录：资料不足")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "贷款账户数量：2" in context
    assert "查询记录：资料不足" in context


def test_complete_personal_credit_material_enters_llm_context():
    markdown = "## 个人征信\n- 未结清贷款余额：120000元\n- 信用卡已用额度：30000元\n- 近6月贷款审批查询：3次"
    ai = FakeAI()
    run(generate_credit_one_page_report(complete_storage(markdown), ai, "生成标准征信报告", "personal_刘聪"))
    assert "信用卡已用额度：30000元" in ai.content
    assert "近6月贷款审批查询：3次" in ai.content


def test_enterprise_relationship_evidence_enters_context():
    storage = complete_storage("## 企业征信\n- 关联企业：上海示例有限公司（法定代表人关系）")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "上海示例有限公司" in context
    assert "法定代表人关系" in context


def test_guarantee_evidence_enters_context():
    storage = complete_storage("## 个人征信\n- 对外担保余额：500000元\n- 被担保主体：示例贸易公司")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "对外担保余额：500000元" in context


def test_loan_overdue_evidence_enters_context():
    storage = complete_storage("## 个人征信\n- 贷款逾期次数：2次\n- 最长逾期：30天")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "贷款逾期次数：2次" in context


def test_explicit_no_overdue_is_preserved_as_evidence():
    storage = complete_storage("## 个人征信\n- 报告明确记载：贷款逾期记录为无")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "贷款逾期记录为无" in context


def test_dti_remains_insufficient_when_income_missing():
    storage = complete_storage("## 个人征信\n- 月供合计：8000元")
    materials = run(get_customer_materials(storage, "personal_刘聪"))
    assert "\"月收入\": \"资料不足\"" in build_report_context(materials)


def test_missing_query_records_are_not_changed_to_zero():
    context = build_report_context(run(get_customer_materials(complete_storage("## 个人征信\n- 查询记录：资料不足"), "personal_刘聪")))
    assert "查询记录：资料不足" in context
    assert "查询记录：0" not in context


def test_missing_values_are_never_defaulted_to_zero():
    storage = complete_storage("## 个人征信\n- 信用卡信息：暂未获取")
    context = build_report_context(run(get_customer_materials(storage, "personal_刘聪")))
    assert "信用卡信息：暂未获取" in context
    assert "信用卡信息：0" not in context


def test_only_saved_material_is_sent_to_model_and_identifier_is_masked():
    storage = complete_storage("## 个人征信\n- 身份证：310101199001011234\n- 已保存事实：贷款余额1万元")
    ai = FakeAI()
    run(generate_credit_one_page_report(storage, ai, "生成征信报告", "personal_刘聪"))
    assert "已保存事实：贷款余额1万元" in ai.content
    assert "310101199001011234" not in ai.content
    assert "未经保存的虚构事实" not in ai.content


def test_ordinary_chat_does_not_trigger_report_intent():
    assert not is_credit_report_request("你好，今天怎么样？")
    assert not is_credit_report_request("征信报告一般包含哪些内容？")
    assert is_credit_report_request("分析一下这个客户的征信")


def test_no_materials_and_processing_are_business_safe():
    row = customer()
    storage = FakeStorage(customers=[row])
    no_materials = run(generate_credit_one_page_report(storage, FakeAI(), "生成征信一页纸", row["customer_id"]))
    assert no_materials["data"]["reportStatus"] == "no_materials"

    processing_storage = FakeStorage(
        customers=[row],
        extractions={row["customer_id"]: [extraction("personal_credit_report", "", status="running")]},
    )
    processing = run(generate_credit_one_page_report(processing_storage, FakeAI(), "生成征信一页纸", row["customer_id"]))
    assert processing["data"]["reportStatus"] == "materials_processing"

