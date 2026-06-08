from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


def test_company_owner_is_not_filtered_by_length() -> None:
    result = run_property_cert_agent(
        {
            "text": """
不动产权证书
沪(2022)宝字不动产权第011468号
权利人 智先生数字科技（上海）有限公司
坐落 上海市宝山区示例路306号
不动产单元号 310113999999GB00001F00010001
权利类型 国有建设用地使用权/房屋所有权
权利性质 出让
土地用途 其它商服用地
房屋用途 办公
建筑面积 306.00平方米
""",
            "metadata": {"filename": "产权证-306(1).pdf", "declared_doc_type": "property_cert"},
        }
    )

    fields = result["fields"]
    assert fields["权利人"] == "智先生数字科技（上海）有限公司"
    assert fields["土地用途"] == "其它商服用地"
    assert fields["房屋用途"] == "办公"
