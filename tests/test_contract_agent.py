from __future__ import annotations

from backend.document_types import get_document_display_name, normalize_document_type_code
from backend.services.contract_agent import ContractAgent, ContractSkill, is_contract_like
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
