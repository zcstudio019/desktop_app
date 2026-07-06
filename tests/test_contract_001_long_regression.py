from __future__ import annotations

from backend.services.contract_agent import ContractAgent


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
分包工程承包范围：机电安装专业分包工程
"""
    pages[3]["text"] = "合同价款目录 附件单价 4.00元，本页为章节索引。"
    pages[4]["text"] = "目录 第一部分合同协议书 第二部分通用合同条款 第三部分专用合同条款"
    pages[9]["text"] = "工程概况 工程地点：上海市浦东新区张江科学城 质量标准：一次性验收合格。"
    pages[19]["text"] = "工期条款 计划开工日期待书面通知，计划竣工日期按总包进度执行。"
    pages[39]["text"] = "工程款支付 进度款及结算款按专用合同条款执行，具体付款节点见原件。"
    pages[49]["text"] = "竣工结算 最终结算及结算总价按双方确认的工程量执行。"
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
    assert data["project"]["location"] == "上海市浦东新区张江科学城"
    assert data["settlement"]["payment_method"].startswith("识别到主合同工程款支付条款")
    assert "主合同结算条款" in data["settlement"]["settlement_method"]
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert data["settlement"]["receiving_account"] == "识别到账户信息，归属需人工复核"
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


def test_contract_001_selective_ocr_metadata_triggers_long_contract_extraction() -> None:
    result = _run_selective_ocr_shape()
    data = result.structured_data_dict()
    assert data["page_count"] == 239
    assert data["quality"]["long_contract"] is True
    assert data["quality"]["contract_ocr_meta"]["ocr_pages_count"] < 239
    assert data["quality"]["contract_ocr_meta"]["has_full_page_text"] is False
    assert data["settlement"]["payment_method"].startswith("识别到主合同工程款支付条款")
    assert "主合同结算条款" in data["settlement"]["settlement_method"]
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert data["settlement"]["receiving_account"] == "识别到账户信息，归属需人工复核"
    assert "付款方式：未识别（未稳定定位到主合同付款条款）" not in result.markdown
