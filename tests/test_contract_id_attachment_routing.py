from __future__ import annotations

from backend.services.document_extractor_service import (
    classify_main_document_container,
    detect_document_type_code,
    run_document_extraction,
)


FILENAME = "青浦项目：物资材料采购合同(博汇盛）.pdf"


def _pages() -> list[dict[str, str | int]]:
    pages: list[dict[str, str | int]] = [{"page": page, "text": f"{page}/17"} for page in range(1, 15)]
    pages[0]["text"] = """物资材料采购合同（通用版） 1/17
合同编号：
工程名称：青浦区徐泾镇张广泾南侧01-49地块项目一期工程
货物供应工程概况
甲方（需方）：上海意川建筑科技有限公司
乙方（供方）：上海博汇盛建筑安装工程有限公司
工程地点：上海市青浦区沪青平公路蟠龙路交界路口
"""
    pages[1]["text"] = """第二条 货物名称、计量单位、数量、价款
序号 名称 型号规格 单位 数量 含税单价 含税合价
1 热镀锌钢管 DN25 米 100 20.00 2000.00
2 管件 DN25 个 50 8.00 400.00
3 电缆 WDZ-YJY 米 200 30.00 6000.00
"""
    pages[2]["text"] = """交货期限及地点 3/17
交货地点：上海青浦区沪青平公路谢家角交叉路口
乙方根据甲方传真、邮件、电话或微信等指示交货。
质量和技术标准要求
验收标准及方法
"""
    pages[5]["text"] = """双方法务信息及增值税约定 6/17
名称 上海意川建筑科技有限公司 上海博汇盛建筑安装工程有限公司
纳税人识别号 91310118MA1JP7UB2B 91310120MABYXGEHXK
地址 上海市松江区佘山镇沈砖公路3129弄1号1幢3楼A区213室 | 上海市松江区泗泾镇沪松公路5599号7幢2楼102室
开户行及账号 上海银行浦西支行 03005029359 | 浙江泰隆商业银行上海松江支行 31010030201000091777
乙方应按照付款金额向甲方开具合法有效的增值税专用发票，税率13%。
"""
    pages[6]["text"] = """合同金额汇总 7/17
合同暂定总金额（含税）小写：32055959.16
合同暂定增值税税额（税率13%）小写：3687853.71
合同暂定总金额（不含税）小写：28368105.45
"""
    pages[7]["text"] = """第九条付款约定 8/17
选择第3种其他支付方式：每月20日为对账日，按月进行对账。
每次进度对账后90天支付该对账单货物金额的70%。
全部货物供货完毕后6个月内支付至已供货物金额的80%。
本工程竣工验收合格且最终结算完成后3个月内结清余款。
以上任何时间段的款项支付都不计任何利息。
"""
    pages[8]["text"] = """违约责任 9/17
合同解除
廉政规定的直接责任人员支付合同价款5%的违约金。
解除合同并要求乙方支付合同结算价10%的违约金。
"""
    pages[9]["text"] = """第十二条争议解决方式 10/17
选择第（2）种：向本合同签订地人民法院提起诉讼。
送达信息 甲方联系人：费慧 电话：18621877799
乙方联系人：朱海波 电话：13586577884
"""
    pages[10]["text"] = """合同签章页 11/17
甲方（盖章）：上海意川建筑科技有限公司
乙方（盖章）：上海博汇盛建筑安装工程有限公司
2、本合同自双方签字并盖章后生效，一式肆份，甲方执贰份，乙方执贰份。
"""
    pages[11]["text"] = """附件一 授权委托书
授权代表处理本合同相关事项。
"""
    pages[12]["text"] = """附件二 廉洁协议
双方遵守廉洁合作约定。
"""
    pages[13]["text"] = """廉洁协议签章页 14/17
甲方（盖章）：上海意川建筑科技有限公司
乙方（盖章）：上海博汇盛建筑安装工程有限公司
附件三 身份证复印件
中华人民共和国居民身份证
姓名 顾某 性别 男 民族 汉
公民身份号码 320000199001011234
签发机关 宜兴市公安局
有效期限 2020.01.01-2040.01.01
"""
    return pages


def _text() -> str:
    return "\n".join(str(page["text"]) for page in _pages())


def test_contract_container_beats_id_card_attachment() -> None:
    decision = classify_main_document_container(_text(), raw_pages=_pages(), filename=FILENAME)
    assert decision["page_count"] == 14
    assert decision["main_doc_type"] == "contract"
    assert decision["route_agent"] == "ContractAgent"
    assert decision["reason"] == "contract_keywords_in_front_pages_and_filename"
    assert "id_card" in decision["attachment_doc_types"]
    assert decision["contract_score"] > decision["id_card_score"]
    assert detect_document_type_code(_text(), explicit_type="id_card", filename=FILENAME) == "contract"


def test_main_dispatch_routes_mixed_pdf_to_contract_agent() -> None:
    content = run_document_extraction(
        _text(),
        _pages(),
        FILENAME,
        customer_id="customer-qingpu",
        declared_doc_type="id_card",
        metadata={"raw_pages": _pages()},
    )
    markdown = str(content.get("markdown_result") or content.get("display_markdown") or "")
    assert content["doc_type"] == "contract"
    assert content["contract_category"] == "material_purchase"
    assert content["agent_type"] == "contract_agent"
    assert content["structured_data"]["title"] == "物资材料采购合同（通用版）"
    assert markdown.startswith("## 合同")
    assert "合同类型：物资采购合同" in markdown
    assert "青浦区徐泾镇张广泾南侧01-49地块" in markdown
    assert "上海意川建筑科技有限公司" in markdown
    assert "上海博汇盛建筑安装工程有限公司" in markdown
    assert "身份证复印件" in markdown
    assert "授权委托书" in markdown
    assert "廉洁协议" in markdown
    assert "## 居民身份证" not in markdown
    assert "资料类型：居民身份证" not in markdown


def test_bohui_material_purchase_final_fields() -> None:
    content = run_document_extraction(
        _text(), _pages(), FILENAME, customer_id="customer-qingpu",
        declared_doc_type="id_card", metadata={"raw_pages": _pages()},
    )
    data = content["structured_data"]
    markdown = content["markdown_result"]
    buyer, seller = data["parties"][:2]
    assert data["doc_type"] == "contract"
    assert data["contract_category"] == "material_purchase"
    assert data["contract_no"] == ""
    assert data["copies"] == "一式肆份，甲方执贰份，乙方执贰份"
    assert buyer["unified_social_credit_code"] == "91310118MA1JP7UB2B"
    assert buyer["contact"] == "费慧"
    assert buyer["phone"] == "18621877799"
    assert seller["unified_social_credit_code"] == "91310120MABYXGEHXK"
    assert seller["contact"] == "朱海波"
    assert seller["phone"] == "13586577884"
    assert seller["address"] == "上海市松江区泗泾镇沪松公路5599号7幢2楼102室"
    assert "电缆采购" not in data["project"]["scope"]
    assert "传真、邮件、电话或微信" in data["duration"]["delivery_method"]
    nodes = data["payment_nodes"]
    assert "70%" in str(nodes) and "80%" in str(nodes)
    assert "廉政规定" not in str(nodes)
    assert "合同价款5%的违约金" not in str(nodes)
    assert "合同结算价10%的违约金" not in str(nodes)
    assert "增值税专用发票" in data["settlement"]["invoice_requirement"]
    assert "税率13%" in data["settlement"]["invoice_requirement"]
    assert "浙江泰隆商业银行上海松江支行" in data["settlement"]["receiving_account"]
    assert "31010030201000091777" in data["settlement"]["receiving_account"]
    assert "向本合同签订地人民法院提起诉讼" in data["clauses"]["dispute_resolution"]
    assert data["amount"]["tax_included_amount"] == "32,055,959.16 元"
    assert data["amount"]["tax_excluded_amount"] == "28,368,105.45 元"
    assert data["amount"]["tax_rate"] == "13%"
    assert data["amount"]["tax_amount"] == "3,687,853.71 元"
    assert data["amount"]["price_form"] == "暂定总价，按实际供货数量及合同单价结算"
    assert data["amount"]["amount_check"] == "大写金额与小写金额基本一致；含税金额、不含税金额与税额基本一致"
    assert data["line_item_summary"]["total_amount"] == "32,055,959.16 元"
    assert "电缆质保期限" not in data["clauses"]["warranty"]
    assert any(token in data["clauses"]["warranty"] for token in ("货物", "材料", "采购货物"))
    assert "期限为2年" in data["clauses"]["warranty"]
    assert "收款账户未识别" not in data["warnings"]
    assert "收款账户未识别" not in data["validation"]["warnings"]
    assert "收款账户建议按原件复核" in data["warnings"]
    assert "收款账户建议按原件复核" in data["validation"]["warnings"]
    assert "授权委托书" in data["signature"]["attachments"]
    assert "身份证复印件" in data["signature"]["attachments"]
    assert "廉洁协议" in data["signature"]["attachments"]
    assert "页脚显示共17页但当前PDF仅14页" in data["quality"]["body_missing_note"]
    assert "合同编号：甲方（需方）" not in markdown
    assert "合同份数：2、本合同" not in markdown
    assert "70%" in markdown and "80%" in markdown
    assert "合同价款5%的违约金" not in markdown
    assert "电缆质保期限" not in markdown
    assert "采购货物质保期限" in markdown
    assert "收款账户未识别" not in markdown
    assert "收款账户建议按原件复核" in markdown
    assert "大写金额与小写金额基本一致；含税金额、不含税金额与税额基本一致" in markdown
    assert "## 居民身份证" not in markdown
