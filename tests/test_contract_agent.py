from __future__ import annotations

from backend.document_types import get_document_display_name, normalize_document_type_code
from backend.services.contract_agent import ContractAgent, ContractSkill, is_contract_like
from backend.services.contract_agent.markdown_renderer import final_sanitize_contract_markdown
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
