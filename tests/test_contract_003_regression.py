from __future__ import annotations

from backend.services.contract_agent import ContractAgent


CONTRACT_003_FILENAME = "合同003：青浦区徐泾镇张广泾南侧01-49地块(一期)项目-机电安装工程.pdf"


def _contract_003_pages() -> list[dict[str, str | int]]:
    pages: list[dict[str, str | int]] = [{"page": page, "text": ""} for page in range(1, 35)]
    pages[0]["text"] = "机电安装工程专业分包合同\n项目名称：青浦区徐泾镇张广泾南侧01-49地块项目一期机电安装工程"
    pages[1]["text"] = """合同协议书
承包人：上海华建工程建设咨询有限公司
分包人：上海意川建筑科技有限公司
工程名称：青浦区徐泾镇张广泾南侧01-49地块项目一期机电安装工程
工程地点：青浦区徐泾镇张广泾南侧01-49地块
本分包工程计划于2024年6月26日开工
本分包工程计划于2025年11月23日竣工
暂定合同工期：总日历天数516天
合同价款：人民币 60,305,209.07 元
大写金额：陆仟零叁拾万伍仟贰佰零玖元零柒分
其中安全文明措施费除税金额为 1809156.27 元
质量标准：符合总包合同约定的分包工程质量标准，并达到一次性验收合格；施工期间无死亡事故、无重大伤残事故，达到上海市文明工地标准。
"""
    pages[2]["text"] = """合同的生效
本协议经立协议双方签字、盖章有效，一式陆份，承包人执叁份，分包人执叁份。
OCR误识别噪声：签订日期：2020年6月9日
签订日期：2024年6月__日
签订地点：上海
"""
    pages[3]["text"] = """承包人（盖章）：上海华建工程建设咨询有限公司
地址：上海徐汇区龙吴路888号
开户银行：交通银行上海分行
账号：216200100110778688
分包人（盖章）：上海意川建筑科技有限公司
地址：上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室
联系人及联系电话：021-69611755
开户银行：上海银行浦西支行
账号：03005029359
签字人：盖章
"""
    pages[10]["text"] = """合同价款及支付
本工程工程量按实结算，固定单价。
"""
    pages[11]["text"] = """合同价款的支付
预付款：未约定预付款。
安全文明措施费：合同约定安全文明措施费 1,809,156.27 元，第一次进度款含安全文明措施费。
进度款：合同签订后按月进度付款，按每月完成工作量支付65%。
结算款：承包人总承包项目结算完成并本工程结算完成后，支付至本工程结算总价的97%。
质量保证金：扣留结算总价的3%作为质量保证金，保修期满2年后15日内无息返还。
发票要求：每次付款前，分包人必须提供一般纳税人增值税专用发票，税率9%，并对发票真实性、合法性负责。
若本分包工程因分包人责任最终不能通过验收，则分包人承担质量违约责任。
"""
    for page in (22, 25, 27, 29, 31, 33):
        pages[page - 1]["text"] = f"附件协议签章页\n承包人（盖章）\n分包人（盖章）\n第{page}页"
    pages[33]["text"] = """合同总价明细表
汇总表
序号 项目 金额
1 除税预算造价 55325879.87 元
2 税金9% 4979329.2 元
3 合计 60305209.07 元
"""
    return pages


def _run_contract_003():
    pages = _contract_003_pages()
    return ContractAgent().run(
        {
            "text": "\n".join(str(page["text"]) for page in pages),
            "raw_pages": pages,
            "filename": CONTRACT_003_FILENAME,
        }
    )


def test_contract_003_construction_subcontract_structured_fields() -> None:
    data = _run_contract_003().structured_data_dict()
    parties = data["parties"]
    amount = data["amount"]
    duration = data["duration"]

    assert data["doc_type"] == "contract"
    assert data["doc_type_name"] == "合同"
    assert data["contract_category"] == "construction_subcontract"
    assert data["contract_category_name"] == "建设工程专业分包合同"
    assert data["extraction_status"] == "partial"
    assert data["title"] == "机电安装工程专业分包合同"
    assert data["project_name"] == "青浦区徐泾镇张广泾南侧01-49地块项目一期机电安装工程"
    assert data["signing_date"] == "2024年6月（具体日期需人工复核）"
    assert data["signing_date"] != "2020年6月9日"
    assert data["signing_place"] == "上海"
    assert data["page_count"] == 34
    assert data["effective_condition"] == "本协议经立协议双方签字、盖章后有效"
    assert data["copies"] == "一式陆份，承包人执叁份，分包人执叁份"
    assert "工资专用账户" not in data["copies"]

    assert parties[0]["name"] == "上海华建工程建设咨询有限公司"
    assert parties[0]["unified_social_credit_code"] == ""
    assert parties[0]["unified_social_credit_code"] != "216200100110778688"
    assert parties[0]["address"] == "上海徐汇区龙吴路888号"
    assert parties[1]["name"] == "上海意川建筑科技有限公司"
    assert parties[1]["unified_social_credit_code"] == ""
    assert parties[1]["unified_social_credit_code"] != "216200100107958688"
    assert parties[1]["phone"] == "021-69611755"
    assert parties[1]["address"] == "上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室"
    assert parties[1]["bank_account"] == "03005029359"

    assert duration["start_date"] == "2024年6月26日"
    assert duration["end_date"] == "2025年11月23日"
    assert duration["period"] == "516天"
    assert duration["period"] != "3天"
    assert data["project"]["location"] == "青浦区徐泾镇张广泾南侧01-49地块"

    assert amount["contract_amount"] == "人民币 60,305,209.07 元"
    assert "陆仟零叁拾万伍仟贰佰零玖元零柒分" in amount["amount_upper"]
    assert amount["amount_lower"] == "60,305,209.07 元"
    assert amount["tax_included_amount"] == "60,305,209.07 元"
    assert amount["tax_excluded_amount"] == "55,325,879.87 元"
    assert amount["tax_rate"] == "9%"
    assert amount["tax_amount"] == "4,979,329.20 元"
    assert amount["safety_civilization_fee"] == "1,809,156.27 元（除税金额）"
    assert amount["price_form"] == "固定单价"
    assert amount["amount_check"] == "大写金额与小写金额基本一致；含税金额、不含税金额与税额基本一致"
    assert amount["recognition_status"] == "成功"
    assert data["settlement"]["settlement_method"] == "工程量按实结算，固定单价"

    assert data["signature"]["signers"] == ""
    assert data["signature"]["signature_page"] == "第4页及附件签章页"
    assert "第1页" not in data["signature"]["signature_page"]
    assert "第3页" not in data["signature"]["signature_page"]
    assert data["validation"]["completeness"] == "部分完整"
    assert data["quality"]["ocr_quality"] == "可用"
    assert "签订日期具体日期需人工复核" in data["validation"]["warnings"]
    assert "统一社会信用代码未识别" in data["validation"]["warnings"]
    assert "争议解决方式需人工复核" in data["validation"]["warnings"]
    assert data["quality"]["body_missing_note"] == "当前PDF包含合同协议书、合同条款、附件、签章页及合同总价明细表，文件结构较完整"


def test_contract_003_payment_invoice_and_markdown_regression() -> None:
    result = _run_contract_003()
    data = result.structured_data_dict()
    payload = result.to_dict()
    markdown = result.display_markdown
    markdown_result = payload["markdown_result"]

    payment_nodes = data["payment_nodes"]
    assert len(payment_nodes) >= 5
    assert len(data["settlement"]["payment_schedule"]) >= 5
    assert [node["node"] for node in payment_nodes] == ["预付款", "安全文明措施费", "进度款", "结算款", "质量保证金"]
    assert all("节点1" not in str(node) for node in payment_nodes)
    assert all("桩基工程" not in str(node) for node in payment_nodes)
    assert "65%" in markdown
    assert "97%" in markdown
    assert "3%" in markdown
    assert "安全文明施工费：1,809,156.27 元（除税金额）" in markdown
    assert "安全文明施工费：1,809,156.27 元（除税金额）" in markdown_result
    assert "合同价格形式：固定单价" in markdown
    assert "合同价格形式：固定单价" in markdown_result
    assert "| 节点 | 触发条件 | 支付比例/金额 | 备注 |" in markdown
    assert "| 预付款 |" in markdown_result
    assert "| 安全文明措施费 |" in markdown_result
    assert "结算方式：工程量按实结算，固定单价" in markdown
    assert "结算方式：工程量按实结算，固定单价" in markdown_result
    assert "发票要求：每次付款前，分包人必须提供一般纳税人增值税专用发票，税率9%，并对发票真实性、合法性负责。" in markdown_result
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert "税率9%" in data["settlement"]["invoice_requirement"]
    assert data["settlement"]["receiving_account"] == "开户银行：上海银行浦西支行；账号：03005029359"
    assert "扣留结算总价的3%作为质量保证金" in markdown_result
    assert "保修期满2年后15日内无息返还" in markdown_result
    assert "增值税专用发票，税率9%" in markdown_result
    assert "安全文明措施费除税金额为1,809,156.27元" in markdown_result
    assert "签章页：第31页及附件签章页" in markdown_result
    assert "签章页：第 31 页" not in markdown_result
    assert "文件结构较完整" in markdown
    assert "关键字段完整度：部分完整" in markdown
    assert "签订日期具体日期需人工复核" in markdown_result
    assert "统一社会信用代码未识别" in markdown_result
    assert "争议解决方式需人工复核" in markdown_result
    assert "付款节点已提取，建议按原件复核" in markdown_result
    assert "付款条款需人工复核" not in markdown_result
    assert "签字人：盖章" not in markdown
    assert "2020年6月9日" not in markdown
    assert "算时一并扣除" not in markdown
    assert "算时一并扣除" not in markdown_result
    assert "甲方对此代发总额" not in markdown_result
    assert "1.5工程承包方式" not in markdown
    assert "付款方式：未识别" not in markdown_result
    assert "结算方式：未识别" not in markdown_result
    assert "安全文明施工费：未识别" not in markdown_result
    assert "合同价格形式：未识别" not in markdown_result
    assert "算时一并扣除" not in markdown
    assert "代发总额" not in markdown


def test_contract_003_forbidden_regressions() -> None:
    markdown = _run_contract_003().display_markdown
    lower_markdown = markdown.lower()
    for forbidden in ("owner type", "contract category", "evidence", "raw_text", "source_page", "confidence", "markdown result"):
        assert forbidden not in lower_markdown
    for forbidden in (
        "统一社会信用代码：216200100110778688",
        "统一社会信用代码：216200100107958688",
        "合同份数：方开设的工资专用账户",
        "合同工期/服务期限：3天",
        "签字人：盖章",
        "合同金额：人民币 60,305,209.00 元",
        "税额：4,979,329.00 元",
    ):
        assert forbidden not in markdown
