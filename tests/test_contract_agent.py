from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from backend.document_types import get_document_display_name, normalize_document_type_code
from backend.services.contract_agent import ContractAgent, ContractSkill, is_contract_like
from backend.services.contract_agent.markdown_renderer import final_sanitize_contract_markdown, sanitize_contract_result_payload
from backend.services.contract_agent.markdown_renderer import format_extract_status
from backend.services.contract_agent.skill import (
    extract_clause_by_keywords,
    extract_contract_party_blocks,
    extract_contract_tax_amounts_from_amount_page,
    extract_signature_page_two_columns,
    is_valid_bank_account,
    second_pass_extract_contract_clauses,
)
from backend.services.local_storage_service import LocalStorageService
from backend.services.markdown_profile_service import _build_single_document_section
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_agents.registry import DOCUMENT_AGENT_REGISTRY, get_document_agent
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code


CONSTRUCTION_TEXT = """
建设工程专业分包合同
合同编号：A04C-JD-001
工程名称：张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）机电安装专业分包工程
工程地点：上海市浦东新区
承包人：上海总承包有限公司
统一社会信用代码：91310000MA1TEST001
分包人：上海机电安装有限公司
统一社会信用代码：91310000MA1TEST002
合同价款：人民币 498,000.00 元
大写金额：肆拾玖万捌仟元整
合同工期：120日历天
付款方式：进度款按已完工程量的70%支付，验收后支付至95%，质保金5%。
质量标准：一次验收合格
安全文明施工：按总包现场管理要求执行
违约责任：违约方承担相应损失
争议解决：提交项目所在地人民法院管辖
签订地点：上海
签订日期：2025年6月18日
承包人盖章 分包人盖章 授权代表签字
"""

PURCHASE_TEXT = """
物资采购合同
项目名称：临空12号地块国际商务花园四期项目
甲方：上海建设有限公司
乙方：江苏吉达电缆有限公司
合同总金额：人民币 1,280,000.00 元
采购清单
1 电缆 WDZ-YJY-4*95+1*50 米 1000 980.00 980000.00
2 电缆 WDZ-YJY-5*16 米 500 600.00 300000.00
交货地点：项目现场
运输方式：乙方负责运输
验收标准：按国家标准及甲方要求验收
付款方式：货到验收合格并开具增值税专用发票后支付95%，质保金5%。
签订日期：2025年5月20日
"""

CONSULTING_TEXT = """
BIM深化咨询服务合同
工程名称：张江项目施工阶段BIM深化咨询服务
发包单位：上海工程管理有限公司
分包单位：上海数字建造咨询有限公司
咨询服务费：人民币 320,000.00 元
服务范围：机电安装BIM深化、碰撞检查、综合支吊架深化、竣工模型交付
服务期限：2025年1月1日至2025年12月31日
付款方式：合同签订后支付30%，阶段成果确认后支付50%，最终交付后支付20%。
知识产权：成果归属甲方，乙方保留署名权。
开户银行：中国银行上海分行 银行账号：123456789012345678
签订日期：2025年1月1日
"""

CONTRACT_002_POLLUTED_TEXT = """
机电安装工程专业分包合同（南区）
工程名称：临空12号地块国际商务花园四期项目（除桩基）
承包人：上海建工集团股份有限公司
统一社会信用代码：91310000631189305E
分包人：上海意川建筑科技有限公司
统一社会信用代码：91310118MA1JP7UB2B
目录
一、分包工程概况.........................-7-
三、质量标准.........................-7-
十二、合同生效...............................-10-
十三、合同份数.......................-10-
12.合同价格、计量与支付.....................-39-
2.2承包人项目经理.........................-17-
合同价款：人民币 188,491,296.13 元 大写金额：壹亿捌仟捌佰肆拾玖万壹仟贰佰玖拾陆元
增值税税额=不含税价×9%
税率：9%
工程地点：长宁区基地东至协和路，西至广顺北路，南至北翟路绿化带
承包方式：包工包料、包工期、包质量、包安全、包文明施工
开户银行：建行上海第二支行；账号：03005029359
一类内容的文件，应以最新签署的为准
签订日期：2024年6月30日
承包人盖章 分包人盖章
"""


def test_contract_registry_and_aliases() -> None:
    assert "contract" in DOCUMENT_AGENT_REGISTRY
    assert get_document_agent("contract") is not None
    assert normalize_document_type_code("合同") == "contract"
    assert get_document_display_name("contract") == "合同"


def test_detect_contract_filename_priority() -> None:
    assert detect_document_type_code("", filename="contract-001.pdf") == "contract"
    assert is_contract_like("", filename="material-purchase-contract.pdf")


def test_contract_agent_construction_markdown_no_json() -> None:
    result = run_document_extraction_agent(
        document_type="contract",
        raw_text=CONSTRUCTION_TEXT,
        filename="合同001：张江机电安装专业分包工程.pdf",
        metadata={"raw_pages": [{"page": 1, "text": CONSTRUCTION_TEXT}]},
    )
    payload = result.raw_agent_result or {}
    markdown = payload["display_markdown"]
    assert result.document_type == "contract"
    assert result.debug["selected_agent"] == "contract_agent"
    assert payload["contract_category"] == "construction_subcontract"
    assert "## 合同" in markdown
    assert "建设工程专业分包合同" in markdown
    assert "合同金额：人民币 498,000.00 元（来源页：第 1 页）" in markdown
    assert "structured_data" not in markdown
    assert "raw_json" not in markdown
    assert "None" not in markdown
    assert "null" not in markdown


def test_contract_skill_categories() -> None:
    assert ContractSkill().extract(text=PURCHASE_TEXT, pages=[{"page": 1, "text": PURCHASE_TEXT}], filename="发物资采购合同（电缆）江苏吉达合同.pdf")["contract_category"] == "material_purchase"
    assert ContractSkill().extract(text=CONSULTING_TEXT, pages=[{"page": 1, "text": CONSULTING_TEXT}], filename="张江项目-施工阶段BIM深化咨询服务合同.pdf")["contract_category"] == "consulting_service"


def test_build_structured_extraction_contract_uses_agent() -> None:
    content = build_structured_extraction(
        PURCHASE_TEXT,
        "contract",
        raw_pages=[{"page": 1, "text": PURCHASE_TEXT}],
        filename="发物资采购合同（电缆）江苏吉达合同.pdf",
    )
    assert content["doc_type"] == "contract"
    assert content["agent_type"] == "contract_agent"
    assert content["contract_category"] == "material_purchase"
    assert content["markdown_result"].startswith("## 合同")
    assert "采购清单" in content["markdown_result"] or "清单明细" in content["markdown_result"]


def test_contract_agent_masks_id_card_in_markdown() -> None:
    text = CONSTRUCTION_TEXT + "\n附件：授权代表身份证复印件 330203199001012199"
    result = ContractAgent().run({"text": text, "raw_pages": [{"page": 2, "text": text}], "filename": "合同.pdf"})
    assert "330203199001012199" not in result.display_markdown
    assert "3302********9" in result.display_markdown


def test_contract_002_toc_pollution_and_display_regression() -> None:
    result = ContractAgent().run({
        "text": CONTRACT_002_POLLUTED_TEXT,
        "raw_pages": [
            {"page": 1, "text": CONTRACT_002_POLLUTED_TEXT},
            {"page": 10, "text": "签订日期：2024年6月30日\n承包人盖章 分包人盖章"},
        ],
        "filename": "合同002：临空12号地块国际商务花园四期项目（除桩基）-机电安装工程（南区）.pdf",
    })
    markdown = result.display_markdown
    assert "owner type" not in markdown.lower()
    assert "contract category" not in markdown.lower()
    assert "markdown result" not in markdown.lower()
    assert "evidence" not in markdown.lower()
    assert "................" not in markdown
    assert "12.合同价格、计量与支付" not in markdown
    assert "2.2承包人项目经理" not in markdown
    assert "签订地点：未识别" in markdown
    assert "合同生效条件：未识别" in markdown
    assert "合同份数：未识别" in markdown
    assert "清单明细：未识别到独立清单明细" in markdown
    assert "| 节点 | 触发条件 | 支付比例/金额 | 备注 |" not in markdown
    assert "| 甲方/承包人/发包人 | 上海建工集团股份有限公司 | 91310000631189305E | 未识别 | 未识别 | 未识别 |" in markdown
    assert "电话 |" in markdown
    assert "03005029359 |" not in markdown
    assert "收款账户：未识别" in markdown
    assert "签字人：未识别" in markdown
    assert "税率：9%" in markdown
    assert "税额：未识别" in markdown
    assert "金额校验：大写金额疑似不完整，需人工复核" in markdown
    assert "提取状态：部分成功" in markdown
    assert "提取状态：partial" not in markdown


def test_contract_002_rejects_start_date_and_unowned_account() -> None:
    page_one = """
机电安装工程专业分包合同（南区）
工程名称：临空12号地块国际商务花园四期项目（除桩基）
承包人：上海建工集团股份有限公司
统一社会信用代码：91310000631189305E
分包人：上海意川建筑科技有限公司
统一社会信用代码：91310118MA1JP7UB2B
合同价款：人民币 188,491,296.13 元
签订地点：本合同在上海市长宁区签订
计划开工日期：2022年10月1日
合同工期：638天
安全文明施工费（含税）：大写：零元（￥0元）
账号：31001502500055390033
"""
    signature_page = """
承包人盖章 分包人盖章
本合同一式_捌_份，均具有同等法律效力，承包人执_肆_份，分包人执肆_份
"""
    result = ContractAgent().run({
        "text": f"{page_one}\n{signature_page}",
        "raw_pages": [{"page": 1, "text": page_one}, {"page": 10, "text": signature_page}],
        "filename": "合同002.pdf",
    })
    markdown = result.display_markdown
    assert "签订日期：2022年10月1日" not in markdown
    assert markdown.count("签订日期：未识别") == 2
    assert "合同份数：本合同一式捌份，均具有同等法律效力，承包人执肆份，分包人执肆份" in markdown
    assert "收款账户：未识别" in markdown
    assert "31001502500055390033" not in markdown
    assert "安全文明施工：安全文明施工费为 0 元" in markdown
    assert "关键字段完整度：部分完整" in markdown
    assert "收款账户归属需人工复核" in markdown


def test_construction_payment_account_prefers_party_b_context() -> None:
    text = CONSTRUCTION_TEXT + """
分包人：上海机电安装有限公司
开户银行：中国建设银行上海第二支行
账号：03005029359
"""
    result = ContractAgent().run({
        "text": text,
        "raw_pages": [{"page": 1, "text": text}],
        "filename": "合同001.pdf",
    })
    assert "收款账户：开户银行：中国建设银行上海第二支行；账号：03005029359" in result.display_markdown


def test_contract_markdown_final_sanitizer_removes_outer_fields_and_evidence() -> None:
    dirty = """- owner type：company
- markdown result：## 合同
## 合同
- 资料类型：合同
- evidence：{
  "signing_date": {"value": "2022年10月1日", "source_page": 7}
}
"""
    cleaned = final_sanitize_contract_markdown(dirty)
    assert cleaned == "## 合同\n- 资料类型：合同"


def _dirty_contract_payload() -> dict:
    return {
        "doc_type": "contract",
        "agent_type": "contract_agent",
        "owner_type": "company",
        "contract_category": "construction_subcontract",
        "contract_category_name": "建设工程专业分包合同",
        "markdown_result": """- owner type：company
- markdown result：## 合同
- 资料类型：合同
- 合同类型：建设工程专业分包合同

### 合同基本信息
- 合同名称：机电安装工程专业分包合同（南区）

### 合同主体
| 角色 | 名称 |
| --- | --- |
| 甲方 | 上海建工集团股份有限公司 |

### 解析质量提示
- 关键字段完整度：部分完整
- evidence：{"project_name":{"value":"测试项目","source_page":1,"confidence":0.7}}
""",
        "evidence": {"project_name": {"value": "测试项目", "source_page": 1, "confidence": 0.7}},
        "structured_data": {"project_name": "测试项目"},
    }


def _assert_contract_markdown_is_display_only(markdown: str) -> None:
    assert markdown.startswith("## 合同")
    for forbidden in (
        "owner type", "contract category", "contract category name", "markdown result",
        "evidence", "source_page", "confidence", '"value"', "raw_text",
    ):
        assert forbidden not in markdown.lower()
    for required in ("资料类型：合同", "合同类型：建设工程专业分包合同", "合同基本信息", "合同主体"):
        assert required in markdown


def test_contract_profile_export_short_circuits_generic_field_rendering() -> None:
    class Storage:
        async def get_document(self, _doc_id: str) -> dict:
            return {"file_name": "合同002.pdf", "file_path": "D:/stored/合同002.pdf"}

    markdown, source = asyncio.run(_build_single_document_section(
        Storage(),
        "customer-contract-002",
        {"extraction_type": "contract", "doc_id": "doc-contract-002", "extracted_data": _dirty_contract_payload()},
    ))
    _assert_contract_markdown_is_display_only(markdown)
    assert source["source_type"] == "contract"


def test_contract_storage_sanitizes_markdown_before_database_write() -> None:
    with tempfile.TemporaryDirectory() as directory:
        storage = LocalStorageService(str(Path(directory) / "contract-save.db"))
        asyncio.run(storage.create_customer({"customer_id": "customer-contract-002", "name": "测试企业"}))
        asyncio.run(storage.save_document({
            "doc_id": "doc-contract-002",
            "customer_id": "customer-contract-002",
            "file_name": "合同002.pdf",
            "file_path": "D:/stored/合同002.pdf",
            "file_type": "contract",
        }))
        asyncio.run(storage.save_extraction({
            "extraction_id": "extraction-contract-002",
            "doc_id": "doc-contract-002",
            "customer_id": "customer-contract-002",
            "extraction_type": "contract",
            "extracted_data": _dirty_contract_payload(),
        }))
        saved = asyncio.run(storage.get_extractions_by_doc("doc-contract-002"))[0]["extracted_data"]
        _assert_contract_markdown_is_display_only(saved["markdown_result"])
        assert saved["evidence"]["project_name"]["source_page"] == 1


def test_contract_payload_sanitizer_keeps_evidence_separate() -> None:
    payload = sanitize_contract_result_payload(_dirty_contract_payload(), force=True)
    _assert_contract_markdown_is_display_only(payload["markdown_result"])
    assert payload["evidence"]["project_name"]["confidence"] == 0.7


def test_customer_detail_api_has_contract_short_circuit_before_generic_fields() -> None:
    source = Path("backend/routers/customer.py").read_text(encoding="utf-8")
    contract_branch = source.index('all_fields["合同"] = {')
    generic_loop = source.index("for key, value in extracted_data.items()", contract_branch)
    assert contract_branch < generic_loop
    branch_source = source[contract_branch:generic_loop]
    assert '"markdown_result": contract_payload.get("markdown_result") or ""' in branch_source
    assert '"evidence"' not in branch_source
    assert '"structured_data"' not in branch_source


def test_contract_extract_status_is_localized() -> None:
    assert format_extract_status("success") == "成功"
    assert format_extract_status("partial") == "部分成功"
    assert format_extract_status("failed") == "失败"
    assert format_extract_status("pending") == "解析中"
    assert format_extract_status("unknown") == "未识别"
    assert format_extract_status("") == "未识别"


def test_second_pass_ignores_toc_only_clause_hits() -> None:
    toc_page = {
        "page": 2,
        "text": """目录
12.3工程款支付................-40-
16.结算........................-46-
18.违约........................-49-
22.争议解决....................-58-
十二、合同生效................-10-
""",
    }
    assert extract_clause_by_keywords([toc_page], ("工程款支付", "进度款")) == ""
    structured = {
        "parties": [],
        "settlement": {"payment_method": "", "settlement_method": "", "invoice_requirement": "", "receiving_account": ""},
        "clauses": {"warranty": "", "breach_liability": "", "dispute_resolution": "", "no_subcontract": ""},
        "signature": {"attachments": ""},
        "effective_condition": "",
    }
    second_pass_extract_contract_clauses([toc_page], "construction_subcontract", structured)
    assert structured["settlement"]["payment_method"] == ""
    assert structured["settlement"]["settlement_method"] == ""
    assert structured["clauses"]["breach_liability"] == ""
    assert structured["clauses"]["dispute_resolution"] == ""
    assert structured["effective_condition"] == ""


def test_second_pass_supplements_only_body_backed_clauses() -> None:
    body_pages = [{
        "page": 40,
        "text": """12.3 工程款支付
分包人按月提交已完工程量，承包人审核后支付进度款。
工程验收合格并完成结算后，按合同约定支付结算款。
分包人应在付款前提供合法有效的增值税专用发票。
16. 竣工结算
分包人提交结算申请，承包人完成结算审核后办理最终结算支付。
18. 违约责任
任何一方违反合同约定，应承担违约责任并赔偿对方损失。
19. 质量保修
质量保证金、缺陷责任期及保修期按照本合同约定执行。
20. 禁止转包
分包人不得转包或违法分包本工程。
22. 争议解决
双方应先协商解决，协商不成的，向项目所在地人民法院提起诉讼。
24. 合同生效
本合同自双方签字盖章之日起生效。
附件一 工程量清单
附件内容及工程量以合同附件页记载为准。
""",
    }]
    structured = {
        "parties": [],
        "settlement": {"payment_method": "", "settlement_method": "", "invoice_requirement": "", "receiving_account": ""},
        "clauses": {"warranty": "", "breach_liability": "", "dispute_resolution": "", "no_subcontract": ""},
        "signature": {"attachments": ""},
        "effective_condition": "",
    }
    second_pass_extract_contract_clauses(body_pages, "construction_subcontract", structured)
    assert structured["settlement"]["payment_method"] == "按合同约定的工程款支付节点执行，具体以正文条款为准"
    assert structured["settlement"]["settlement_method"] == "按合同结算申请、审核及结算支付条款执行"
    assert structured["settlement"]["invoice_requirement"] == "按合同发票条款执行"
    assert structured["settlement"]["receiving_account"] == ""
    assert structured["clauses"]["warranty"] == "按合同质量保证金、缺陷责任期及保修期条款执行"
    assert structured["clauses"]["breach_liability"] == "按合同违约责任条款执行"
    assert structured["clauses"]["dispute_resolution"] == "按合同争议解决条款执行"
    assert structured["clauses"]["no_subcontract"] == "分包人不得转包或违法分包"
    assert structured["effective_condition"] == "本合同自双方签字盖章之日起生效"
    assert structured["signature"]["attachments"] == "识别到合同附件，具体以合同附件页为准"


def test_contract_agent_renders_second_pass_clause_summaries() -> None:
    base_text = """建设工程专业分包合同
工程名称：测试机电安装工程
承包人：上海测试总承包有限公司
分包人：上海测试机电有限公司
合同价款：人民币 1,000,000.00 元
"""
    clause_text = """12.3 工程款支付
分包人按月提交已完工程量，承包人审核后办理工程款支付。
16. 竣工结算
分包人提交结算申请，承包人完成结算审核后办理最终结算支付。
17. 发票
分包人应在付款前提供合法有效的增值税专用发票。
18. 违约责任
任何一方违反合同约定，应承担违约责任并赔偿对方损失。
19. 质量保修
质量保证金、缺陷责任期及保修期按照本合同约定执行。
20. 禁止转包
分包人不得转包或违法分包本工程。
22. 争议解决
双方应先协商解决，协商不成的，向项目所在地人民法院提起诉讼。
24. 合同生效
本合同自双方签字盖章之日起生效。
附件一 工程量清单
附件内容及工程量以合同附件页记载为准。
"""
    result = ContractAgent().run({
        "text": f"{base_text}\n{clause_text}",
        "raw_pages": [{"page": 1, "text": base_text}, {"page": 40, "text": clause_text}],
        "filename": "正文条款合同.pdf",
    })
    markdown = result.display_markdown
    assert "付款方式：按合同约定的工程款支付节点执行，具体以正文条款为准" in markdown
    assert "结算方式：按合同结算申请、审核及结算支付条款执行" in markdown
    assert "发票要求：未识别" not in markdown
    assert "保修/质保：未识别" not in markdown
    assert "违约责任：未识别" not in markdown
    assert "争议解决：未识别" not in markdown
    assert "禁止转包/分包：未识别" not in markdown
    assert "合同生效条件：本合同自双方签字盖章之日起生效" in markdown
    assert "附件情况：识别到合同附件，具体以合同附件页为准" in markdown
    assert "收款账户：未识别" in markdown


def test_contract_002_agreement_only_pdf_extracts_pages_7_to_10_and_flags_missing_body() -> None:
    toc = """目录
通用合同条款....................-11-
12.3工程款支付................-40-
16.结算........................-46-
18.违约责任....................-49-
22.争议解决....................-57-
专用合同条款....................-77-
"""
    page7 = """机电安装工程专业分包合同（南区）
承包人：上海建工集团股份有限公司
分包人：上海意川建筑科技有限公司
总包工程名称：临空12号地块国际商务花园四期项目（除桩基）
分包工程名称：临空12号地块国际商务花园四期项目（除桩基）-机电安装工程（南区）
分包工程地点：长宁区基地东至协和路，西至广顺北路，南至北翟路绿化带，北至通协路
分包工程承包范围和内容：包括但不限于5#~9#楼地上、地下室、室外总体的电气工程、给排水工程、人防工程（水、电、风）、雨水回收工程、通风防排烟工程、
预埋套管工程、弱电及消防报警预埋工程、防火封堵及抗震支架（地下室除电缆按末端设备服务功能区域或号房划分外，其余按物理位置划分）等机电安装工程施工及相关图纸深化、相关方案编制、评审等一切与机电安装相关的工作。
承包方式：包工包料（除甲供外）、包工期、包质量、包安全、包文明施工、包工程一切保险费、包环境保护、
包工程整体协调配合管理、包监测检测、包验收、包竣工验收备案、包维修保修的施工专业分包方式。
计划开工日期：2022年10月1日，具体开工日期以承包人书面通知为准。计划完工日期：2024年6月30日
合同工期：638天
质量标准：符合总包合同约定的分包工程质量标准，并达到一次性验收合格；施工期间无死亡事故、无重大伤残事故，达到上海市文明工地标准。
"""
    page8 = """签约合同价暂定为含税：人民币188,491,296.13元
大写：壹亿捌仟捌佰肆拾玖万壹仟贰佰玖拾陆元壹角叁分
不含增值税签约合同价：人民币172927794.6元
增值税税率：9%
增值税税额：人民币15,563,501.52元
安全文明施工费（含税）：大写：零元（￥0元）
合同价格形式：固定总价
分包合同文件构成：合同协议书；中标通知书（如有）；专用合同条款及其附件；通用合同条款；技术标准和要求；图纸目录；
"""
    page9 = """已标价工程量清单或预算书；招标文件（如有）；投标函及其附录（如有）；其他分包合同文件。
分包人承诺确保工程质量和安全，不进行转包及违法分包。
分包人承诺在缺陷责任期及保修期内承担相应工程维修责任。
本合同于2022年10月__日签订
本合同在上海市长宁区签订
"""
    page10 = """承包人（盖章）：上海建工集团股份有限公司    分包人（盖章）：上海意川建筑科技有限公司
地址：东大名路666号    地址：上海市松江区佘山镇沈砖公路3129弄1
号1幢3楼A区213室
邮政编码：200080    邮政编码：201600
统一社会信用代码：91310000631189305E    统一社会信用代码：91310118MA1JP7UB2B
开户银行：建行上海第二支行    开户银行：上海银行股份有限公司浦西支行
账号：31001502500055390033    账号：03005029359
纳税人性质：一般纳税人    纳税人性质：一般纳税人
本合同自双方加盖公章或合同专用章并经法定代表人或其委托代理人签字（章）后生效。
本合同一式_捌_份，均具有同等法律效力，承包人执_肆_份，分包人执肆_份
"""
    pages = [{"page": number, "text": ""} for number in range(1, 11)]
    pages[1]["text"] = toc
    pages[6]["text"] = page7
    pages[7]["text"] = page8
    pages[8]["text"] = page9
    pages[9]["text"] = page10
    text = "\n".join(page["text"] for page in pages)
    result = ContractAgent().run({
        "text": text,
        "raw_pages": pages,
        "filename": "合同002：临空12号地块国际商务花园四期项目（除桩基）-机电安装工程（南区） (1).pdf",
    })
    markdown = result.display_markdown
    assert "提取状态：部分成功" in markdown
    assert "文件完整性：当前PDF疑似仅包含合同协议书、目录及签章页，通用/专用条款正文未包含在本文件中" in markdown
    assert "签订日期：2022年10月（具体日期未填写，需人工复核）" in markdown
    assert "签订地点：本合同在上海市长宁区签订" in markdown
    assert "合同生效条件：双方加盖公章或合同专用章，并经法定代表人或其委托代理人签字（章）后生效" in markdown
    assert "合同份数：本合同一式捌份，均具有同等法律效力，承包人执肆份，分包人执肆份" in markdown
    assert "东大名路666号" in markdown
    assert "| 甲方/承包人/发包人 | 上海建工集团股份有限公司 | 91310000631189305E | 未识别 | 未识别 | 未识别 | 东大名路666号 |" in markdown
    assert "| 甲方/承包人/发包人 | 上海银行股份有限公司" not in markdown
    assert "上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室" in markdown
    assert "| 乙方/分包人 | 上海意川建筑科技有限公司 | 91310118MA1JP7UB2B | 未识别 | 未识别 | 未识别 | 东大名路666号 |" not in markdown
    assert "| 乙方/分包人 | 上海意川建筑科技有限公司 | 91310118MA1JP7UB2B | 未识别 | 未识别 | 未识别 | 上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室 |" in markdown
    assert "工程或服务地点：长宁区基地东至协和路，西至广顺北路，南至北翟路绿化带，北至通协路" in markdown
    assert "绿化带北至通协路" not in markdown
    assert "防火封堵及抗震支架（地下室除电缆按末端设备服务功能区域或号房划分外，其余按物理位置划分）等机电安装工程施工及相关图纸深化、相关方案编制、评审等一切与机电安装相关的工作。" in markdown
    assert "包工程整体协调配合管理、包监测检测、包验收、包竣工验收备案、包维修保修的施工专业分包方式。" in markdown
    assert "质量标准：符合总包合同约定的分包工程质量标准，并达到一次性验收合格；施工期间无死亡事故、无重大伤残事故，达到上海市文明工地标准" in markdown
    assert "质量标准：分包工程质量：应符合总包合同约定的分包工程的质量标准，并同时达到工程质量符合一次性验收合格。安全文明标准：施工期间无死亡事故，无重大伤残事故，达。" not in markdown
    assert "质量标准：" in markdown and "达。\n" not in markdown
    assert "合同金额：人民币 188,491,296.13 元" in markdown
    assert "大写金额：壹亿捌仟捌佰肆拾玖万壹仟贰佰玖拾陆元壹角叁分" in markdown
    assert "不含税金额：172,927,794.60 元" in markdown
    assert "税率：9%" in markdown
    assert "税额：15,563,501.52 元" in markdown
    assert "安全文明施工费：0 元" in markdown
    assert "合同价格形式：固定总价" in markdown
    assert "大写金额与小写金额基本一致" in markdown
    assert "税额与不含税金额存在小额四舍五入差异，需人工复核" in markdown
    assert "税额或不含税金额未识别" not in markdown
    assert "开始日期：2022年10月1日，具体开工日期以承包人书面通知为准" in markdown
    assert "开始日期：2022年10月1日，具体开工日期以承包人书面通知为准。计划完工日期" not in markdown
    assert "结束日期：2024年6月30日" in markdown
    assert "合同工期/服务期限：638天" in markdown
    assert "付款方式：未识别（当前PDF未包含工程款支付正文条款）" in markdown
    assert "结算方式：未识别（当前PDF未包含结算正文条款）" in markdown
    assert "发票要求：未识别（当前PDF未包含发票正文条款）" in markdown
    assert "收款账户：开户银行：上海银行股份有限公司浦西支行；账号：03005029359" in markdown
    assert "收款账户：开户银行：上海银行股份有限公司浦西支行；账号：91310118" not in markdown
    assert "收款账户：开户银行：上海银行股份有限公司浦西支行；账号：91310118MA1JP7UB2B" not in markdown
    assert "收款账户：未识别" not in markdown
    assert "大写金额疑似不完整" not in markdown
    assert "收款账户归属需人工复核" not in markdown
    assert "税额与不含税金额存在小额四舍五入差异需复核" in markdown
    assert "保修/质保：分包人承诺在缺陷责任期及保修期内承担相应工程维修责任。" in markdown
    assert "违约责任：未识别（当前PDF未包含违约责任正文条款）" in markdown
    assert "争议解决：未识别（当前PDF未包含争议解决正文条款）" in markdown
    assert "禁止转包/分包：分包人承诺不进行转包及违法分包。" in markdown
    assert "附件情况：合同文件包括合同协议书、中标通知书、专用合同条款及附件、通用合同条款、技术标准和要求、图纸目录、已标价工程量清单或预算书、招标文件、投标函及附录、其他分包合同文件。" in markdown
    assert "按合同约定的" not in markdown
    assert "按合同违约责任条款执行" not in markdown
    assert "清单明细：未识别到独立清单明细" in markdown


def test_contract_tax_amount_page_extracts_ocr_spaced_values() -> None:
    pages = [
        {
            "page": 8,
            "text": """签约合同价暂定为（含税）：人民币 188,491,296.13 元
不 含 增 值 税 签 约 合 同 价 ：人民币 172 927 794 . 60 元
增值税税额=不含税价×9%
增 值 税 税 率 为 9 %
增 值 税 税 额 ：人民币 15 563 501.52 元
价 格 形 式 ：固定总价
""",
        }
    ]

    amount_data = extract_contract_tax_amounts_from_amount_page(pages)

    assert amount_data["tax_excluded_amount"] == "172,927,794.60 元"
    assert amount_data["tax_rate"] == "9%"
    assert amount_data["tax_amount"] == "15,563,501.52 元"
    assert amount_data["price_form"] == "固定总价"


def test_signature_page_two_columns_uses_coordinates_and_rejects_credit_code_as_account() -> None:
    page = {
        "width": 1000,
        "lines": [
            {"text": "承包人（盖章）：上海建工集团股份有限公司", "x_center": 240, "y": 10},
            {"text": "分包人（盖章）：上海意川建筑科技有限公司", "x_center": 740, "y": 10},
            {"text": "地址：东大名路666号", "x_center": 240, "y": 30},
            {"text": "地址：上海市松江区佘山镇沈砖公路3129弄1", "x_center": 740, "y": 30},
            {"text": "号1幢3楼A区213室", "x_center": 740, "y": 42},
            {"text": "统一社会信用代码：91310000631189305E", "x_center": 240, "y": 55},
            {"text": "统一社会信用代码：91310118MA1JP7UB2B", "x_center": 740, "y": 55},
            {"text": "开户银行：建行上海第二支行", "x_center": 240, "y": 75},
            {"text": "开户银行：上海银行股份有限公司浦西支行", "x_center": 740, "y": 75},
            {"text": "账号：31001502500055390033", "x_center": 240, "y": 95},
            {"text": "账号：03005029359", "x_center": 740, "y": 95},
        ],
    }
    blocks = extract_signature_page_two_columns(page)
    assert blocks["contractor"]["name"] == "上海建工集团股份有限公司"
    assert "银行" not in blocks["contractor"]["name"]
    assert blocks["contractor"]["address"] == "东大名路666号"
    assert blocks["subcontractor"]["address"] == "上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室"
    assert blocks["subcontractor"]["bank"] == "上海银行股份有限公司浦西支行"
    assert blocks["subcontractor"]["account"] == "03005029359"
    assert blocks["subcontractor"]["account"] not in {"91310118", "91310118MA1JP7UB2B"}


def test_signature_page_mixed_text_does_not_leak_contractor_fields_to_subcontractor() -> None:
    page_text = """承包人（盖章）：上海建工集团股份有限公司    分包人（盖章）：上海意川建筑科技有限公司
地址：东大名路666号    地址：上海市松江区佘山镇沈砖公路3129弄1
号1幢3楼A区213室
统一社会信用代码：91310000631189305E    统一社会信用代码：91310118MA1JP7UB2B
开户银行：建行上海第二支行    开户银行：上海银行股份有限公司浦西支行
账号：31001502500055390033    账号：03005029359
纳税人性质：一般纳税人    纳税人性质：一般纳税人
"""
    blocks = extract_contract_party_blocks([{"page": 10, "text": page_text}], "construction_subcontract")

    assert blocks["contractor"]["name"] == "上海建工集团股份有限公司"
    assert blocks["contractor"]["name"] != "上海银行股份有限公司"
    assert blocks["contractor"]["credit_code"] == "91310000631189305E"
    assert blocks["contractor"]["address"] == "东大名路666号"
    assert blocks["subcontractor"]["name"] == "上海意川建筑科技有限公司"
    assert blocks["subcontractor"]["credit_code"] == "91310118MA1JP7UB2B"
    assert blocks["subcontractor"]["credit_code"] != "91310000631189305E"
    assert "松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室" in blocks["subcontractor"]["address"]
    assert blocks["subcontractor"]["address"] != "东大名路666号"
    assert blocks["subcontractor"]["bank"] == "上海银行股份有限公司浦西支行"
    assert blocks["subcontractor"]["account"] == "03005029359"
    assert blocks["subcontractor"]["account"] not in {"91310118", "91310118MA1JP7UB2B", "31001502500055390033"}
    assert not is_valid_bank_account("91310118MA1JP7UB2B", "统一社会信用代码：91310118MA1JP7UB2B")
