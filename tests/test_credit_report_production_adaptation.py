"""Report-context regression fixtures; no production records are modified."""
import json
import pytest

from test_assistant_credit_one_page_report import (
    complete_storage, extraction, generated, run,
)
from backend.services.assistant_credit_report_service import (
    get_customer_materials, _personal_credit_model, _money_sum_objects,
)


def relation_storage(other_name="/非法人组织负责"):
    storage = complete_storage()
    rows = storage.extractions[storage.customers[0]["customer_id"]]
    rows[:] = rows[:2]
    enterprise = rows[0]["extracted_data"]["extracted_json"]
    enterprise["registration_info"] = {"legal_representative": other_name}
    enterprise["actual_controller"] = {"name": "黎云"}
    rows.append(extraction("account_permit", {"doc_type": "account_permit", "fields": {
        "legal_representative": "黎云", "bank_account_number": "DO_NOT_INCLUDE_ACCOUNT",
        "opening_bank": "DO_NOT_INCLUDE_BANK",
    }}))
    return storage


def liability(record):
    return _personal_credit_model({"related_repayment_responsibilities": [record]}, "测试主体")["related_repayment_responsibilities"][0]


def test_account_permit_legal_representative_enters_report_context():
    storage = relation_storage()
    materials = run(get_customer_materials(storage, storage.customers[0]["customer_id"]))
    item = next(x for x in materials["materials"] if x["type"] == "account_permit")
    assert item["structured_data"] == {"fields": {"legal_representative": "黎云"}}
    assert "DO_NOT_INCLUDE" not in json.dumps(materials)


@pytest.mark.parametrize("fragment", ["/非法人组织负责", "法定代表人", "非法人组织负责人", "###\ufffd123"])
def test_invalid_enterprise_credit_legal_representative_fragment_is_filtered(fragment):
    assert "个人与企业关系：法定代表人 / 实际控制人" in generated(relation_storage(fragment))["message"]


def test_valid_conflicting_legal_representatives_still_require_review():
    assert "个人与企业关系：需人工核实" in generated(relation_storage("张三"))["message"]


def test_person_relation_combines_legal_representative_and_actual_controller():
    storage = relation_storage()
    rows = storage.extractions[storage.customers[0]["customer_id"]]
    rows[1]["extracted_data"]["report_json"]["basic_info"]["name"] = "王明"
    rows[0]["extracted_data"]["extracted_json"]["actual_controller"]["name"] = "王明"
    rows[2]["extracted_data"]["fields"]["legal_representative"] = "王明"
    assert "个人与企业关系：法定代表人 / 实际控制人" in generated(storage)["message"]


@pytest.mark.parametrize("evidence", [
    "无关段落" * 300 + "余额5,000,000（人民币元）",
    "3,424,532。下一节 人民币元", "13,424,532（人民币元）",
    "3,424,532<br>人民币元", "3,424,532\n人民币元",
])
def test_long_evidence_does_not_global_inherit_unit(evidence):
    assert liability({"balance": 3424532, "evidence": evidence})["balance"]["unit"] is None


@pytest.mark.parametrize("token", ["3,424,532（人民币元）", "3424532人民币元", "3,424,532（人 民币元）", "3424532元"])
def test_exact_balance_evidence_can_resolve_yuan(token):
    assert liability({"balance": 3424532, "evidence": "前文" * 400 + token + "后文" * 400})["balance"]["unit"] == "元"


@pytest.mark.parametrize("prefix", ["", "前文" * 400])
def test_responsibility_amount_without_own_unit_stays_needs_review(prefix):
    row = liability({"responsibility_amount": 4000000, "balance": 3424532,
                     "evidence": prefix + "责任金额4,000,000，融资租赁余额3,424,532（人民币元）"})
    assert row["responsibility_amount"] == {"value": 4000000, "unit": None}
    assert row["balance"] == {"value": 3424532, "unit": "元"}


def test_sum_unit_only_resolved_when_all_balance_units_match():
    balances = [5000000, 2240000, 75000, 3200000, 1800000, 3000000, 3424532]
    monies = [liability({"balance": n, "evidence": f"余额{n:,}（人民币元）"})["balance"] for n in balances]
    assert _money_sum_objects(monies) == {"value": 18739532, "unit": "元"}
    for unit in (None, "万元"):
        monies[-1]["unit"] = unit
        assert _money_sum_objects(monies)["unit"] is None


def test_related_liability_evidence_not_exposed_in_markdown():
    storage = relation_storage()
    personal = storage.extractions[storage.customers[0]["customer_id"]][1]["extracted_data"]["report_json"]
    personal["related_repayment_responsibilities"] = [{
        "institution": "远东宏信", "responsibility_amount": 4000000, "balance": 3424532,
        "evidence": "PRIVATE_EVIDENCE<br>" * 100 + "余额3,424,532（人民币元）",
    }]
    markdown = generated(storage)["message"]
    assert "4,000,000（单位待核验）" in markdown
    assert "3,424,532元" in markdown
    assert "PRIVATE_EVIDENCE" not in markdown
    assert "<br>" not in markdown
    assert markdown.startswith("# 征信速览报告\n")
    assert "| 主体信息 | 内容 |" in markdown
