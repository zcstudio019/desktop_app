from __future__ import annotations

from backend.services.document_extractor_service import (
    classify_main_document_container,
    detect_document_type_code,
    run_document_extraction,
)


FILENAME = "青浦项目：物资材料采购合同(博汇盛）.pdf"


def _pages() -> list[dict[str, str | int]]:
    pages: list[dict[str, str | int]] = [{"page": page, "text": ""} for page in range(1, 15)]
    pages[0]["text"] = """物资材料采购合同
工程名称：青浦区徐泾镇张广泾南侧01-49地块项目一期工程
货物供应工程概况
甲方：上海意川建筑科技有限公司
乙方：上海博汇盛建筑安装工程有限公司
工程地点：上海市青浦区沪青平公路蟠龙路交界路口
"""
    pages[1]["text"] = """第二条 货物名称、计量单位、数量、价款
序号 名称 型号规格 单位 数量 含税单价 含税合价
1 热镀锌钢管 DN25 米 100 20.00 2000.00
2 管件 DN25 个 50 8.00 400.00
3 电缆 WDZ-YJY 米 200 30.00 6000.00
"""
    pages[2]["text"] = """交货期限及地点
质量和技术标准要求
验收标准及方法
乙方按甲方通知分批供货。
"""
    pages[5]["text"] = """双方税务信息及增值税约定
甲方 上海意川建筑科技有限公司
乙方 上海博汇盛建筑安装工程有限公司
乙方应开具合法有效的增值税专用发票。
"""
    pages[7]["text"] = """付款约定
按订货批次及到货验收情况支付货款。
"""
    pages[8]["text"] = """违约责任
合同解除
争议解决方式
"""
    pages[10]["text"] = """合同签章页
甲方（盖章）：上海意川建筑科技有限公司
乙方（盖章）：上海博汇盛建筑安装工程有限公司
"""
    pages[11]["text"] = """附件一 授权委托书
授权代表处理本合同相关事项。
"""
    pages[12]["text"] = """附件二 廉洁协议
双方遵守廉洁合作约定。
"""
    pages[13]["text"] = """附件三 身份证复印件
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
    assert content["structured_data"]["title"] == "物资材料采购合同"
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
