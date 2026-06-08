from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


def test_cover_page_extracts_cover_number_authority_and_date_only() -> None:
    result = run_property_cert_agent(
        {
            "text": """
根据《中华人民共和国物权法》
为保护不动产权利人合法权益
经审查核实，准予登记，颁发此证
登记机构 上海市不动产登记专用章
2018年10月23日
国土资源部监制
编号№D31001337469
""",
            "metadata": {"filename": "房产.pdf", "declared_doc_type": "property_cert"},
        }
    )

    fields = result["fields"]
    assert fields["封面编号"] == "D31001337469"
    assert fields["登记日期"] == "2018年10月23日"
    assert fields["登记机构"] == "登记机构 上海市不动产登记专用章"
    assert "权利人" not in fields
    assert "仅识别到封面页" in result["markdown"]
