from __future__ import annotations

from backend.services.contract_agent import ContractAgent


FILENAME = "临空项目：发物资采购合同（电缆）江苏吉达合同.pdf"


def _pages() -> list[dict[str, str | int]]:
    pages: list[dict[str, str | int]] = [
        {"page": page, "text": f"物资采购合同 {page}/28"} for page in range(1, 24)
    ]
    pages[0]["text"] = """物资采购合同 1/28
电缆采购合同
合同编号：YC202410003-L048
项目名称：临空12号地块国际商务花园四期项目（除桩基）机电安装工程
甲方：上海意川建筑科技有限公司
乙方：江苏吉达电缆有限公司
第二条 货物名称、计量单位、数量、价款
序号 名称 型号/规格 单位 数量 含税单价（元） 含税合价（元）
1 矿物绝缘电缆 RTTMY-3x1+E1 米 490 13.71 6722.60
2 矿物绝缘电缆 RTTMY-3x6+E6 米 462 19.61 9059.82
3 矿物绝缘电缆 RTTMY-3x10+E10 米 207 31.25 6468.75
"""
    pages[1]["text"] = """交货期限及地点 2/28
交货地点：临空12号地块国际商务花园四期项目现场
乙方根据甲方传真、邮件、电话或微信等指示分批交货。
货到现场后按合同验收标准及方法进行验收。
质量和技术标准要求：货物应符合国家、行业、地方质量技术标准及合同约定。
乙方应提供送货清单、产品合格证、质量保证书、检测报告等资料。
"""
    pages[10]["text"] = """货物清单汇总 11/28
合同暂定总金额（含税）小写：35011142.68 元
合同暂定总金额（含税）大写：叁仟伍佰零壹万壹仟壹佰肆拾贰元陆角捌分
合同暂定增值税税额（税率13%）小写：4027861.64 元
合同暂定总金额（不含税）小写：30983281.04 元
"""
    pages[11]["text"] = """收货及供货约定 12/28
甲方收货联系人：项目材料员 联系电话：021-55556666
乙方联系人：张经理 联系电话：0510-88889999
"""
    pages[15]["text"] = """税务及增值税约定 16/28
甲方：上海意川建筑科技有限公司 纳税人识别号：91310118MA1JP7UB2B
乙方：江苏吉达电缆有限公司 纳税人识别号：91320200TESTCABLE1
乙方应按付款金额向甲方开具合法有效的增值税专用发票，税率13%。
发票应符合合同税务及增值税约定。
"""
    pages[18]["text"] = """第九条 付款约定 19/28
本合同采用第3种支付方式，按订货批次付款。
每批订货单确认后，预付款按该批订货单金额的20%支付。
货到现场支付该批订货单金额的50%。
货到现场60天内支付该批订货单金额的20%。
货到现场90天内支付该批订货单金额的10%。
乙方逾期交货应按暂定总价20%向甲方支付违约金。
第八条 结算方式：本合同从开始供货后每满1个月进行进度对账，最终以双方确认的结算单为准。
"""
    pages[19]["text"] = """质量保证 20/28
电缆质保期限与本工程整体工程缺陷责任期一致，期限为2年。
质保期内出现质量问题，乙方承担更换、修理及相关责任。
"""
    pages[21]["text"] = """联系信息及附件清单 22/28
乙方收件地址：江苏省无锡市宜兴市电缆产业园
附件一：货物采购清单
本合同自双方签字并盖章后生效，一式伍份，甲方执叁份，乙方执贰份。
"""
    pages[22]["text"] = """合同签章页 23/28
甲方（盖章）：上海意川建筑科技有限公司
乙方（盖章）：江苏吉达电缆有限公司
法定代表人或授权代表：
附件清单详见合同约定
"""
    return pages


def _run():
    pages = _pages()
    return ContractAgent().run({
        "text": "\n".join(str(page["text"]) for page in pages),
        "raw_pages": pages,
        "filename": FILENAME,
    })


def test_material_purchase_structured_fields() -> None:
    data = _run().structured_data_dict()
    amount = data["amount"]
    assert data["contract_category"] == "material_purchase"
    assert data["contract_category_name"] == "物资采购合同"
    assert data["title"] == "电缆采购合同"
    assert data["contract_no"] == "YC202410003-L048"
    assert amount["contract_amount"] == "人民币 35,011,142.68 元"
    assert amount["amount_upper"] == "叁仟伍佰零壹万壹仟壹佰肆拾贰元陆角捌分"
    assert amount["tax_included_amount"] == "35,011,142.68 元"
    assert amount["tax_excluded_amount"] == "30,983,281.04 元"
    assert amount["tax_rate"] == "13%"
    assert amount["tax_amount"] == "4,027,861.64 元"
    assert amount["safety_civilization_fee"] == "不适用"
    assert "暂定总价" in amount["price_form"]
    assert amount["recognition_status"] == "成功"
    assert data["effective_condition"] == "本合同自双方签字并盖章后生效"
    assert data["copies"] == "一式伍份，甲方执叁份，乙方执贰份"
    assert data["signature"]["signers"] == ""
    assert data["signature"]["signature_page"] == "第23页"
    assert "页脚显示共28页但当前PDF仅23页" in data["quality"]["body_missing_note"]


def test_material_purchase_payment_items_and_markdown() -> None:
    result = _run()
    data = result.structured_data_dict()
    markdown = result.display_markdown
    nodes = data["payment_nodes"]
    assert [node["node"] for node in nodes] == ["预付款", "到货款", "货到60天付款", "货到90天付款"]
    assert [node["amount_or_ratio"] for node in nodes] == ["该批订货单金额的20%", "50%", "20%", "10%"]
    assert all("违约金" not in str(node) and "暂定总价20%" not in str(node) for node in nodes)
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert "税率13%" in data["settlement"]["invoice_requirement"]
    assert data["line_item_summary"]["total_amount"] == "35,011,142.68 元"
    assert len(data["line_items"]) == 3
    assert all(item["name"] not in {"序号", "含税单价"} for item in data["line_items"])
    assert all(not any(token in str(item) for token in ("违约金", "付款条款", "发票条款")) for item in data["line_items"])
    assert "签字人：均对甲方" not in markdown
    assert "签章页：第23页" in markdown
    assert "安全文明施工费：不适用" in markdown
    assert "合计金额：35,011,142.68 元" in markdown
    assert "清单识别状态：部分成功（已识别清单及合计金额，完整明细建议按原件复核）" in markdown

