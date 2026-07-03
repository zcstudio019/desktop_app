from __future__ import annotations

from backend.services.contract_agent import ContractAgent


FILENAME = "张江项目-施工阶段BIM深化咨询服务合同.pdf"


def _pages() -> list[dict[str, str | int]]:
    pages = [{"page": page, "text": f"施工阶段BIM深化咨询服务合同 第{page}页 共10页"} for page in range(1, 13)]
    pages[0]["text"] = """施工阶段BIM深化咨询服务合同
工程名称：张江创新药基地A04C-01地块专业化标准厂房四期项目
发包单位：上海意川建筑科技有限公司
分包单位：上海驿桐驿景建筑科技有限公司
签订日期：2025年9月22日
本合同自双方签字盖章（含电子签章）后生效
双方盖章
"""
    pages[1]["text"] = """工程概况和服务内容、服务期限
工程地点：上海市浦东新区张江科学城
施工阶段BIM咨询服务，包括BIM建模、碰撞检查、净高分析、管线综合调整、竣工模型和BIM轻量化。
服务期限：自合同签订之日起至整体机电BIM深化工作交付完成。
"""
    pages[3]["text"] = """合同价格及支付方式
BIM图纸会审 0.5
合同价款总计人民币：498000.00元 大写：肆拾玖万捌仟元整
其中不含税价款为493020元，增值税4980元，税率1%。总价包干。
"""
    pages[4]["text"] = """付款方式
合同签订后，乙方提交本项目地下室图纸模型后，甲方支付签约合同价的10%深化咨询服务费。
现场土建地下室结构封顶后十个工作日内，甲方支付签约合同价15%深化咨询服务费。
乙方完成竣工BIM模型移交后十个工作日内付清剩余款项。
"""
    pages[5]["text"] = """甲方发票信息
上海意川建筑科技有限公司 税号91310118MA1JP7UB2B 电话13761162886
地址上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室
乙方账户信息
上海驿桐驿景建筑科技有限公司 税号91310117MABN6G8FBD 电话18512114992
地址上海市松江区泗泾镇明源路255号1幢一层A区
开户行：中国民生银行股份有限公司上海莘庄支行 账号：635427216
"""
    pages[6]["text"] = "违约责任：违约金20%，延期每日0.1%，累计不超过3%。"
    pages[8]["text"] = "合同签章页 甲方盖章 乙方盖章 日期2025年9月22日"
    pages[9]["text"] = "第十九条 本合同一式【肆】份，甲方执【贰】份，乙方执【贰】份，具有同等法律效力。"
    pages[10]["text"] = "附件：项目实施进度计划"
    pages[11]["text"] = "附件：上海驿桐驿景建筑科技有限公司营业执照"
    return pages


def _run():
    pages = _pages()
    return ContractAgent().run({"text": "\n".join(str(p["text"]) for p in pages), "raw_pages": pages, "filename": FILENAME})


def test_consulting_service_structured_fields() -> None:
    data = _run().structured_data_dict()
    assert data["contract_category"] == "consulting_service"
    assert data["contract_category_name"] == "咨询服务合同"
    assert data["title"] == "施工阶段BIM深化咨询服务合同"
    assert data["project_name"] == "张江创新药基地A04C-01地块专业化标准厂房四期项目"
    assert data["signing_date"] == "2025年9月22日"
    assert data["page_count"] == 12
    assert data["effective_condition"] == "本合同自双方签字盖章（含电子签章）后生效"
    assert data["copies"] == "一式肆份，甲方执贰份，乙方执贰份"
    assert data["parties"][0]["name"] == "上海意川建筑科技有限公司"
    assert "乙方：" not in data["parties"][0]["name"]
    assert data["parties"][0]["unified_social_credit_code"] == "91310118MA1JP7UB2B"
    assert data["parties"][0]["phone"] == "13761162886"
    assert data["parties"][0]["address"] == "上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室"
    assert data["parties"][1]["name"] == "上海驿桐驿景建筑科技有限公司"
    assert data["parties"][1]["unified_social_credit_code"] == "91310117MABN6G8FBD"
    assert data["parties"][1]["phone"] == "18512114992"
    assert data["parties"][1]["address"] == "上海市松江区泗泾镇明源路255号1幢一层A区"


def test_consulting_service_scope_amount_and_duration() -> None:
    data = _run().structured_data_dict()
    project = data["project"]
    amount = data["amount"]
    duration = data["duration"]
    assert project["project_name"] == "张江创新药基地A04C-01地块专业化标准厂房四期项目"
    assert project["location"] == "上海市浦东新区张江科学城"
    for token in ("施工阶段BIM深化咨询服务", "BIM建模", "碰撞检查", "净高分析", "管线综合调整", "竣工模型"):
        assert token in project["scope"]
    assert "按项目进度提交相应BIM成果文件" in project["method"]
    assert "BIM成果应满足上海市BIM技术应用监管要点" in project["quality_standard"]
    assert data["amount"]["contract_amount"] == "人民币 498,000.00 元"
    assert amount["amount_upper"] == "肆拾玖万捌仟元整"
    assert amount["amount_lower"] == "498,000.00 元"
    assert amount["tax_included_amount"] == "498,000.00 元"
    assert amount["tax_excluded_amount"] == "493,020.00 元"
    assert amount["tax_rate"] == "1%"
    assert amount["tax_amount"] == "4,980.00 元"
    assert not amount["tax_amount"].startswith("-")
    assert amount["safety_civilization_fee"] == "不适用"
    assert amount["price_form"] == "总价包干"
    assert amount["recognition_status"] == "成功"
    assert duration["start_date"] == "2025年9月22日"
    assert duration["end_date"] == ""
    assert duration["period"] == "自合同签订之日起至整体机电BIM深化工作交付完成"
    for token in ("BIM模型", "深化图纸", "碰撞检查", "净高分析", "管线综合调整", "竣工模型"):
        assert token in duration["delivery_method"]


def test_consulting_service_payment_clauses_signature_and_quality() -> None:
    data = _run().structured_data_dict()
    payment = str(data["payment_nodes"])
    for token in ("地下室模型提交款", "签约合同价的10%", "地下室结构封顶款", "签约合同价的15%", "竣工模型移交款"):
        assert token in payment
    assert "20%" not in payment and "0.1%" not in payment and "3%" not in payment
    assert data["settlement"]["settlement_method"] == "总价包干，按合同约定节点支付。"
    assert "中国民生银行股份有限公司上海莘庄支行" in data["settlement"]["receiving_account"]
    assert "635427216" in data["settlement"]["receiving_account"]
    clauses = data["clauses"]
    assert "BIM成果应满足合同约定、项目需求及相关BIM技术标准" in clauses["quality_acceptance"]
    assert "乙方未按合同约定完成成果或成果不符合验收标准" in clauses["breach_liability"]
    assert "友好协商解决" in clauses["dispute_resolution"]
    assert "有管辖权的人民法院" in clauses["dispute_resolution"]
    assert "合法有效发票" in data["settlement"]["invoice_requirement"]
    assert "未经甲方书面同意" in clauses["no_subcontract"]
    assert clauses["safety_civilization"] == "不适用"
    assert "BIM成果文件、知识产权、保密义务、人员配置、成果交付及附件进度计划" in clauses["other"]
    assert data["signature"]["party_a_stamp"] == "有"
    assert data["signature"]["party_b_stamp"] == "有"
    assert data["signature"]["signers"] == ""
    assert data["signature"]["signature_page"] == "第1页、第9页"
    assert data["signature"]["signing_date"] == "2025年9月22日"
    assert "项目实施进度计划" in data["signature"]["attachments"]
    assert "营业执照" in data["signature"]["attachments"]
    assert data["quality"]["ocr_quality"] == "可用"
    assert data["validation"]["completeness"] == "部分完整"
    assert "咨询服务合同正文、项目实施进度计划、签章页及营业执照附件" in data["quality"]["body_missing_note"]
    assert "文件结构较完整" in data["quality"]["body_missing_note"]
    assert "竣工模型移交款具体比例需按原件复核" in data["warnings"]
    assert "发票具体类型及要求需按原件复核" in data["warnings"]


def test_consulting_service_markdown_regression() -> None:
    markdown = _run().markdown
    assert "合同金额：人民币 498,000.00 元" in markdown
    assert "合同金额：人民币 0.50 元" not in markdown
    assert "小写金额：0.50 元" not in markdown
    assert "含税金额：0.50 元" not in markdown
    assert "| 地下室模型提交款 |" in markdown
    assert "| 地下室结构封顶款 |" in markdown
    assert "签约合同价的10%" in markdown
    assert "签约合同价的15%" in markdown
    assert "20%违约金" not in markdown
    assert "0.1%" not in markdown
    assert "税额：-" not in markdown
    assert "甲方/委托方/发包方 | 乙方：" not in markdown
    assert "文件完整性：未识别" not in markdown
    for forbidden in ("owner type", "contract category", "evidence", "raw_text", "source_page", "confidence"):
        assert forbidden not in markdown.lower()
    assert "第1页、第9页" in markdown
