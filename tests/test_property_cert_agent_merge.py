from __future__ import annotations

from backend.services.property_cert_agent import run_property_cert_agent


def test_detail_page_and_cover_page_merge_without_cover_overriding_cert_no() -> None:
    result = run_property_cert_agent(
        {
            "text": "",
            "metadata": {
                "filename": "房产合并.pdf",
                "declared_doc_type": "property_cert",
                "raw_pages": [
                    {
                        "page": 1,
                        "filename": "房产正面.pdf",
                        "text": "不动产权证书\n沪(2018)徐字不动产权第015979号\n权利人 沃志方\n坐落 上海市徐汇区示例路1号\n建筑面积 88.88平方米\n使用期限 2018年08月28日起2046年08月20日止",
                    },
                    {
                        "page": 2,
                        "filename": "房产.pdf",
                        "text": "根据《中华人民共和国物权法》\n登记机构 上海市不动产登记专用章\n2018年10月23日\n编号 No D31001337469",
                    },
                ],
            },
        }
    )

    fields = result["fields"]
    assert fields["权证编号"] == "沪(2018)徐字不动产权第015979号"
    assert fields["封面编号"] == "D31001337469"
    assert fields["登记日期"] == "2018年10月23日"
    assert "房产.pdf" in result["supplemental_files"]
