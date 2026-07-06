from __future__ import annotations

import re

from backend.services.contract_agent import ContractAgent
from backend.services.contract_agent.schema import ContractParty
from backend.services.contract_agent.skill import apply_long_construction_contract_safeguards


FILENAME = "合同001：张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）机电安装专业分包工程.pdf"


def _pages() -> list[dict[str, str | int]]:
    pages: list[dict[str, str | int]] = [
        {"page": page, "text": f"建设工程专业分包合同 第{page}页"}
        for page in range(1, 240)
    ]
    pages[0]["text"] = """建设工程专业分包合同
合同编号：专业-07/2025
第一部分 合同协议书
"""
    pages[1]["text"] = """合同协议书
承包人：【上海建工智慧营造】有限公司（盖章）
分包人：上海意川建筑科技有限公司
分包工程承包范围：防排烟通风工程、通风工程（人防）、给排水工程、给排水工程（人防）、消火栓工程、喷淋工程、气体灭火工程、电气工程、电气工程（人防）、应急照明及疏散指示系统、火灾报警工程预留预埋、余压监控系统、电气火灾监控系统、电气综合监控系统、防火门监控系统、消防广播系统预留预埋不包含内容:弱电工程、电梯工程、4#楼精装修水电安装、气体灭火系统、火灾报警工程设备安装、消防广播设备安装
"""
    pages[3]["text"] = "合同价款目录 附件单价 4.00元，本页为章节索引。"
    pages[4]["text"] = "目录 第一部分合同协议书 第二部分通用合同条款 第三部分专用合同条款"
    pages[9]["text"] = "工程概况 工程地点：上海市浦东新区新场镇，东至康新公路，南至古翠路，西至良耀路，北至美济路 质量标准：一次性验收合格。"
    pages[19]["text"] = "工期条款 计划开工日期待书面通知，计划竣工日期按总包进度执行。"
    pages[29]["text"] = "合同价款 不含税金额：46,485,219.74元；税率：9%；税额：-46,485,215.57元；安全文明施工费：1.10元。"
    pages[39]["text"] = "工程款支付 进度款及结算款按专用合同条款执行，具体付款节点见原件。"
    pages[49]["text"] = "竣工结算 本工程采用固定单价，最终结算及结算总价按双方确认的工程量执行。"
    pages[59]["text"] = "发票条款 分包人应开具合法有效的增值税专用发票，具体税率见开票信息。"
    pages[69]["text"] = "账户信息 开户银行、银行账号及收款账户详见双方确认资料。"
    pages[79]["text"] = "禁止转包 未经承包人书面同意，分包人不得转包或违法分包。"
    pages[89]["text"] = "争议解决 双方发生争议应协商解决，具体诉讼管辖法院见专用合同条款。"
    pages[99]["text"] = "本合同自双方签字盖章后生效，一式肆份，承包人执贰份，分包人执贰份，具有同等法律效力。"
    pages[220]["text"] = "附件：保密协议"
    pages[229]["text"] = """保密协议
8.2本协议自双方法定代表人签字并加盖公章之日起生效，一式贰份，双方各执壹份。
"""
    pages[230]["text"] = """保密协议
7.2.2在诉讼、仲裁或者配合政府行政执法等活动中依法知悉或者披露的。
7.3任何一方违反本条约定的保密义务，应承担责任。
"""
    pages[235]["text"] = """主合同签章页
承包人（盖章）：【上海建工智慧营造】有限公司
分包人（盖章）：上海意川建筑科技有限公司
法定代表人或委托代理人：签字并加
"""
    return pages


def _run():
    pages = _pages()
    return ContractAgent().run({
        "text": "\n".join(str(page["text"]) for page in pages),
        "raw_pages": pages,
        "filename": FILENAME,
    })


def _run_selective_ocr_shape():
    full_pages = _pages()
    selected_numbers = (1, 2, 5, 10, 20, 40, 50, 60, 70, 80, 90, 100, 221, 230, 231, 236, 239)
    selected = [dict(full_pages[page_no - 1]) for page_no in selected_numbers]
    selected[0]["pdf_page_count"] = 239
    selected[0]["contract_ocr_meta"] = {
        "pdf_page_count": 239,
        "ocr_pages_count": len(selected),
        "text_pages_count": len(selected),
        "scanned_page_indices": list(selected_numbers),
        "skipped_page_indices_count": 239 - len(selected),
        "has_full_page_text": False,
    }
    return ContractAgent().run({
        "text": "\n".join(str(page["text"]) for page in selected),
        "raw_pages": selected,
        "filename": FILENAME,
    })


def _run_with_explicit_missing_field_evidence():
    pages = _pages()
    pages[0]["text"] += "\n签订日期：2025年7月1日"
    pages[19]["text"] = "计划开工日期：2025年7月15日；计划竣工日期：2026年11月26日；工期总日历天数：500天。"
    pages[29]["text"] = """合同价款
人民币
不含税金额：46,485,219.74元
税率：9%
大写金额：伍仟零陆拾陆万捌仟捌佰捌拾玖元伍角贰分
小写金额：待原件复核
安全文明施工费：1,234,567.89元
"""
    pages[39]["text"] = """工程款支付
预付款按签约合同价的10%支付。
月进度款按当月已完工程量的70%支付。
竣工结算完成后支付至结算总价的97%。
扣留结算总价的3%作为质量保证金。
"""
    pages[69]["text"] = "分包人账户 户名：上海意川建筑科技有限公司；开户银行：上海银行浦西支行；账号：03005029359"
    for page_index in range(7, 13):
        pages[page_index]["text"] = "已标价工程量清单 预算书 分部分项工程 措施项目费 清单合计，完整明细见原件。"
    pages[8]["text"] += "\n承包人（盖章）：上海建工智慧营造有限公司 分包人（盖章）：上海意川建筑科技有限公司"
    pages[235]["text"] = """主合同签章页
承包人（盖章）：【上海建工智慧营造】有限公司
分包人（盖章）：上海意川建筑科技有限公司
委托代理人：张三
"""
    return ContractAgent().run({
        "text": "\n".join(str(page["text"]) for page in pages),
        "raw_pages": pages,
        "filename": FILENAME,
    })


def test_contract_001_long_contract_minimum_structured_baseline() -> None:
    data = _run().structured_data_dict()
    assert data["doc_type"] == "contract"
    assert data["contract_category"] == "construction_subcontract"
    assert data["title"] == "建设工程专业分包合同"
    assert "张江创新药基地A04C-01地块" in data["project_name"]
    assert "专业化标准厂房四期项目" in data["project_name"]
    assert data["contract_no"] == "专业-07/2025"
    assert data["page_count"] == 239
    buyer_name = data["parties"][0]["name"]
    assert "上海建工智慧营造" in buyer_name
    assert "【" not in buyer_name and "盖章" not in buyer_name
    assert data["parties"][1]["name"] == "上海意川建筑科技有限公司"
    assert "4.00" not in data["amount"].get("contract_amount", "")
    assert not data["amount"].get("contract_amount")
    assert "8.2" not in data["copies"] and "保密协议" not in data["copies"]
    dispute = data["clauses"]["dispute_resolution"]
    assert "保密义务" not in dispute and "行政执法" not in dispute
    assert data["signature"]["signers"] != "签字并加"
    assert data["quality"]["body_missing_note"]
    assert data["quality"]["body_missing_note"] != "未识别"
    assert all("付款节点已提取" not in warning for warning in data["warnings"])
    assert "上海市浦东新区新场镇" in data["project"]["location"]
    assert data["project"]["excluded_scope"].startswith("弱电工程、电梯工程")
    assert data["amount"]["price_form"] == "固定单价（已定位主合同结算条款，需按原件复核）"
    assert "上海市浦东新区新场镇" in data["duration"]["construction_place"]
    assert not data["duration"]["delivery_place"]
    assert data["settlement"]["payment_method"].startswith("已定位主合同工程款支付条款")
    assert "主合同结算条款" in data["settlement"]["settlement_method"]
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert data["settlement"]["receiving_account"] == "未稳定识别"
    assert "识别到主合同争议解决条款" in data["clauses"]["dispute_resolution"]


def test_contract_001_long_contract_markdown_forbidden_regressions() -> None:
    markdown = _run().markdown
    assert "项目名称：张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）" in markdown
    assert "合同金额：人民币 4.00 元" not in markdown
    assert "合同份数：8.2" not in markdown
    assert "争议解决：7.2.2" not in markdown
    assert "签字人：签字并加" not in markdown
    assert "文件完整性：未识别" not in markdown
    assert "付款节点已提取" not in markdown
    assert "- 合同价格形式：固定单价（已定位主合同结算条款，需按原件复核）" in markdown
    assert "- 不包含内容：弱电工程、电梯工程、4#楼精装修水电安装" in markdown
    assert "- 施工地点：上海市浦东新区新场镇" in markdown
    assert "- 交付地点：上海市浦东新区新场镇" not in markdown


def test_contract_001_selective_ocr_metadata_triggers_long_contract_extraction() -> None:
    result = _run_selective_ocr_shape()
    data = result.structured_data_dict()
    assert data["page_count"] == 239
    assert data["quality"]["long_contract"] is True
    assert data["quality"]["contract_ocr_meta"]["ocr_pages_count"] < 239
    assert data["quality"]["contract_ocr_meta"]["has_full_page_text"] is False
    assert data["settlement"]["payment_method"].startswith("已定位主合同工程款支付条款")
    assert "主合同结算条款" in data["settlement"]["settlement_method"]
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert data["settlement"]["receiving_account"] == "未稳定识别"
    assert "付款方式：未识别（未稳定定位到主合同付款条款）" not in result.markdown


def test_contract_001_long_contract_field_safety_guards() -> None:
    pages = _pages()
    pages[1]["text"] = """合同协议书
承包人：上海建工集团股份有限公司
统一社会信用代码：91310000MA1H3XJJ78
地址：中国（上海）自由贸易试验区临港新片区环湖西二路888号C楼
分包人：上海意川建筑科技有限公司
"""
    pages[9]["text"] = """工程概况
工程地点：上海市浦东新区新场镇，东至康新公路，南至古翠路，西至良耀路，北
质量标准：结合施工总承包工程的合同要求：分包人应配合承包人获得奖项
"""
    pages[29]["text"] = """合同价款
不含税金额：46,485,219.74元
税率：9%
税额：-46,485,215.57元
安全文明施工费：1.10元
"""
    pages[39]["text"] = """工程款支付
预付款按签约合同价的10%支付。
月进度款按当月已完工程量的70%支付。
竣工结算完成后支付至结算总价的97%。
扣留结算总价的3%作为质量保证金。
"""
    pages[59]["text"] = "发票条款 的增值税专用发票作为收取合同价款的前提条件"
    pages[69]["text"] = "账户信息 开户银行：中国银行上海分行；账号：1234567890123456"
    pages[79]["text"] = "及违法分包，服从承包人对现场管理的要求，并在缺陷责任期承担维修责"

    result = {
        "contract_category": "construction_subcontract",
        "project_name": "【张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）】",
        "project": {
            "project_name": "【张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）】",
            "location": "上海市浦东新区新场镇，东至康新公路，南至古翠路，西至良耀路，北",
        },
        "parties": [
            ContractParty(role="甲方/承包人/发包人", name="【上海建工智慧营造】有限公司（盖章）", unified_social_credit_code="91310000WRONG00000", address="错误地址"),
            ContractParty(role="乙方/分包人", name="上海意川建筑科技有限公司"),
        ],
        "amount": {
            "contract_amount": "",
            "tax_included_amount": "",
            "tax_excluded_amount": "46,485,219.74 元",
            "tax_rate": "9%",
            "tax_amount": "-46,485,215.57 元",
            "tax_amount_source": "derived",
            "safety_civilization_fee": "1.10 元",
        },
        "duration": {
            "delivery_place": "上海市浦东新区新场镇，东至康新公路，南至古翠路，西至良耀路，北至美济路",
        },
        "settlement": {},
        "clauses": {
            "breach_liability": "支付人民币20万元/项作为违约金应符合现行国家、行业标准及承包人要求",
        },
        "signature": {"signers": "资格证明"},
        "quality": {},
        "payment_nodes": [],
    }
    pages[0]["pdf_page_count"] = 239
    apply_long_construction_contract_safeguards(pages, result, filename=FILENAME)

    assert result["project_name"] == "张江创新药基地A04C-01地块专业化标准厂房四期项目（除桩基）"
    assert result["project"]["location"].endswith("北至美济路")
    assert result["parties"][0].name == "上海建工智慧营造有限公司"
    assert result["parties"][0].unified_social_credit_code == "91310000MA1H3XJJ78"
    assert result["parties"][0].address.startswith("中国（上海）自由贸易试验区")
    assert result["amount"]["tax_amount"] == ""
    assert result["amount"]["safety_civilization_fee"] == ""
    assert result["amount"]["tax_excluded_amount"] == "46,485,219.74 元（已识别，需结合含税金额复核）"
    assert result["amount"]["recognition_status"] == "部分成功"
    assert result["amount"]["amount_check"] == "识别到不含税金额和税率，可推算含税金额候选，但原文含税金额未稳定识别，需人工复核"
    assert result["quality"]["amount_evidence"]["calculated_tax_amount_candidate"] == "4,183,669.78"
    assert result["quality"]["amount_evidence"]["calculated_tax_included_candidate"] == "50,668,889.52"
    assert [node["node"] for node in result["payment_nodes"]] == ["预付款", "进度款", "结算款", "质量保证金"]
    assert [node["amount_or_ratio"] for node in result["payment_nodes"]] == ["10%", "70%", "97%", "3%"]
    assert "作为收取合同价款的前提条件" in result["settlement"]["invoice_requirement"]
    assert result["settlement"]["invoice_details"]["发票类型"] == "增值税专用发票"
    assert result["settlement"]["invoice_details"]["税率"] == "9%"
    assert result["clauses"]["invoice_requirement_summary"] == "涉及增值税专用发票，详见“付款与结算”中的发票要求"
    assert result["settlement"]["receiving_account"] == "识别到账户信息，但账户名、账号或归属不完整，需人工复核"
    assert result["settlement"]["account_details"]["账户结构化状态"] == "partial"
    assert "现行国家、行业标准" not in result["clauses"]["breach_liability"]
    assert result["clauses"]["no_subcontract"].startswith("识别到禁止转包及违法分包")
    assert result["signature"]["signers"] == ""


def test_contract_001_long_contract_multiline_display_and_calculated_candidates() -> None:
    result = _run_selective_ocr_shape()
    markdown = result.markdown
    assert "- 付款方式：已定位主合同工程款支付条款，具体付款节点需按原件复核" in markdown
    assert "  - 付款条款类型：进度款/结算款已定位，具体比例需复核" in markdown
    assert "  - 付款识别状态：已定位付款条款页，付款节点尚未完全结构化" in markdown
    assert "- 结算方式：已定位主合同结算条款，具体结算口径需按原件复核" in markdown
    assert "  - 结算识别状态：已定位结算条款页，部分结构化" in markdown
    assert "  - 发票类型：增值税专用发票" in markdown
    assert "  - 发票识别状态：已定位发票条款页，部分结构化" in markdown
    assert "  - 账户识别状态：部分识别" in markdown
    assert "  - 缺失原因：账户名称未识别；银行账号未识别；账户归属未稳定确认" in markdown
    assert "- 发票要求：涉及增值税专用发票，详见“付款与结算”中的发票要求" in markdown
    assert "。；" not in markdown
    assert "上海建工集团股份有限公司" not in markdown
    for technical_status in ("partial", "success", "missing", "failed", "unknown", "pending"):
        assert not re.search(rf"(?<![A-Za-z_]){technical_status}(?![A-Za-z_])", markdown, flags=re.I)


def test_contract_001_locked_partial_success_baseline() -> None:
    result = _run()
    data = result.structured_data_dict()
    markdown = result.markdown

    assert "张江创新药基地A04C-01地块" in data["project_name"]
    assert data["contract_no"] == "专业-07/2025"
    assert data["page_count"] == 239
    assert data["parties"][0]["name"] == "上海建工智慧营造有限公司"
    assert data["parties"][1]["name"] == "上海意川建筑科技有限公司"
    assert "上海市浦东新区新场镇" in data["project"]["location"]
    assert data["amount"]["tax_excluded_amount"] == "46,485,219.74 元（已识别，需结合含税金额复核）"
    assert data["amount"]["tax_rate"] == "9%"
    assert data["amount"]["price_form"] == "固定单价（已定位主合同结算条款，需按原件复核）"
    assert data["amount"]["contract_amount"] == ""
    assert data["amount"]["tax_included_amount"] == ""
    assert data["amount"]["tax_amount"] == ""
    assert data["amount"]["safety_civilization_fee"] == ""
    assert "50,668,889.52 元" in data["amount"]["calculated_tax_included_candidate"]
    assert "4,183,669.78 元" in data["amount"]["calculated_tax_amount_candidate"]

    assert "- 合同范围：" in markdown
    assert "- 不包含内容：" in markdown
    assert "- 施工地点：" in markdown
    assert "- 交付地点：上海市浦东新区" not in markdown

    forbidden = (
        "合同金额：人民币 4.00 元",
        "税额：-",
        "安全文明施工费：1.10 元",
        "合同份数：8.2",
        "项目名称：【",
        "上海建工集团股份有限公司",
        "签字人：资格证明",
        "签字人：签字并加",
        "的增值税专用发票作为收取合同价款的前提条件",
    )
    assert all(item not in markdown for item in forbidden)
    assert "- 合同金额：未识别" in markdown
    assert "- 含税金额：未识别" in markdown
    assert "- 税额：未识别" in markdown


def test_contract_001_missing_field_debug_logs_include_candidates_and_text(caplog) -> None:
    caplog.set_level("INFO", logger="backend.services.contract_agent.skill")
    _run()
    log_text = caplog.text
    for label in ("amount", "date", "duration", "payment", "account", "signature", "bill"):
        assert f"[ContractMissingFieldDebug] {label}_pages=" in log_text
        assert f"[ContractMissingFieldDebug] {label}_page_text=" in log_text
    assert "page_no" in log_text
    assert "keywords" in log_text
    assert "snippet" in log_text


def test_contract_001_explicit_missing_fields_are_extracted_only_with_original_evidence() -> None:
    result = _run_with_explicit_missing_field_evidence()
    data = result.structured_data_dict()

    assert data["amount"]["contract_amount"] == "人民币 50,668,889.52 元"
    assert data["amount"]["tax_included_amount"] == "50,668,889.52 元"
    assert data["amount"]["tax_amount"] == "4,183,669.78 元"
    assert data["amount"]["amount_upper"] == "伍仟零陆拾陆万捌仟捌佰捌拾玖元伍角贰分"
    assert data["amount"]["safety_civilization_fee"] == "1,234,567.89 元"
    assert data["quality"]["amount_close_loop"]["context_valid"] is True
    assert data["quality"]["amount_close_loop"]["fill_official_amount"] is True
    assert data["signing_date"] == "2025-07-01"
    assert data["duration"]["start_date"] == "2025年7月15日"
    assert data["duration"]["end_date"] == "2026年11月26日"
    assert "500天" in data["duration"]["period"]
    assert [node["node"] for node in data["payment_nodes"]] == ["预付款", "进度款", "结算款", "质量保证金"]
    assert data["settlement"]["receiving_account"] == "账户名：上海意川建筑科技有限公司；开户银行：上海银行浦西支行；账号：03005029359；归属需人工复核"
    assert data["signature"]["signers"] == "张三"
    assert data["line_item_summary"]["message"] == "识别到已标价工程量清单/预算书，疑似位于附件清单页，完整明细需按原件复核"
    assert data["line_item_summary"]["page_range_conflicts_with_signature"] is True
    assert data["line_item_summary"]["recognition_status"] == "已定位清单页，未完全结构化"
