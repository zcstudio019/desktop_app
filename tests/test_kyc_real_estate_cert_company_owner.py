from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import run_kyc_document_agent


COMPANY_CERT_TEXT = """
不动产权证书
沪（2022）宝字 不动产权第011468号
权利人 智先生数字科技（上海）有限公司
共有情况 单独所有
坐落 殷高西路101号
不动产单元号 310113015003GB00011F00020088
权利类型 国有建设用地使用权/房屋所有权
权利性质 土地权利性质：出让
用途 土地用途：其它商服用地 / 房屋用途：办公
面积 宗地面积：8615.00平方米 / 建筑面积：800.35平方米
国有建设用地使用权使用期限：2018年08月28日起2046年08月20日止
土地状况：地号：宝山区高境镇9街坊73/7丘；使用权面积：相应的土地面积；独用面积：；分摊面积：
房屋状况：室号部位：306；类型：办公楼；总层数：17；竣工日期：2007年
编号 D31003610514
"""


def _extract(text: str = COMPANY_CERT_TEXT) -> dict:
    return run_kyc_document_agent(
        {
            "text": text,
            "pages": [],
            "metadata": {"filename": "产权证-306(1).pdf", "declared_doc_type": "property_cert"},
        }
    )


def test_company_owner_can_be_extracted_from_real_estate_cert():
    result = _extract()

    assert result["fields"]["权利人"] == "智先生数字科技（上海）有限公司"
    assert result["fields"]["owner"] == "智先生数字科技（上海）有限公司"


def test_company_real_estate_cert_extracts_address_and_unit_number():
    result = _extract()

    assert result["fields"]["坐落"] == "殷高西路101号"
    assert result["fields"]["不动产单元号"] == "310113015003GB00011F00020088"
