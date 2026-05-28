from __future__ import annotations

from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent, run_kyc_document_agent


def payload(text: str) -> dict:
    return {
        "text": text,
        "pages": [],
        "metadata": {"filename": "sample.txt", "customer_id": "C001", "source": "unit_test"},
    }


def test_id_card_classification_and_extraction() -> None:
    text = """
居民身份证
姓名 张三
性别 男
民族 汉
出生 1949年12月31日
住址 北京市朝阳区幸福路1号
公民身份号码 11010519491231002X
签发机关 北京市公安局朝阳分局
有效期限 2010年01月01日-2030年01月01日
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "id_card"
    assert result["extraction_status"] == "success"
    assert result["fields"]["name"] == "张三"
    assert result["fields"]["id_number"] == "11010519491231002X"
    assert result["fields"]["birth_date"] == "1949-12-31"
    assert result["evidence"]["id_number"]["evidence_text"]


def test_business_license_classification_and_extraction() -> None:
    text = """
营业执照
统一社会信用代码 91310115MA1K3ABCDE
名称 上海示例科技有限公司
类型 有限责任公司
法定代表人 李四
注册资本 人民币1000万元
成立日期 2020年05月20日
营业期限 2020年05月20日至长期
住所 上海市浦东新区世纪大道100号
经营范围 技术开发、技术服务。
登记机关 上海市市场监督管理局
发照日期 2020年05月21日
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "business_license"
    assert result["fields"]["company_name"] == "上海示例科技有限公司"
    assert result["fields"]["legal_representative"] == "李四"
    assert result["fields"]["registered_capital"] == {"amount": 1000.0, "unit": "万元"}
    assert result["validation"]["is_valid"] is True


def test_account_permit_classification_and_extraction() -> None:
    text = """
开户许可证
存款人名称 上海示例科技有限公司
账户名称 上海示例科技有限公司
账号 123456789012345678
开户银行 中国工商银行上海分行营业部
账户性质 基本存款账户
核准号 J100000000001
法定代表人 李四
发证日期 2021年06月01日
账户状态 正常
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "account_permit"
    assert result["fields"]["company_name"] == "上海示例科技有限公司"
    assert result["fields"]["bank_account_number"] == "123456789012345678"
    assert result["fields"]["opening_bank"] == "中国工商银行上海分行营业部"


def test_property_cert_classification_and_extraction() -> None:
    text = """
房屋所有权证
房屋所有权人 王五
证号 沪房权证浦字第123456号
房屋坐落 上海市浦东新区花园路88号1幢101室
规划用途 住宅
建筑面积 89.50平方米
抵押情况 无
查封情况 无
填发日期 2018年08月18日
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "property_cert"
    assert result["fields"]["owner"] == "王五"
    assert result["fields"]["property_address"] == "上海市浦东新区花园路88号1幢101室"
    assert result["fields"]["building_area"] == {"value": 89.5, "unit": "平方米"}


def test_vehicle_license_classification_and_extraction() -> None:
    text = """
机动车行驶证
号牌号码 沪A12345
车辆类型 小型轿车
所有人 上海示例科技有限公司
住址 上海市浦东新区世纪大道100号
使用性质 非营运
品牌型号 大众牌SVW1234
车辆识别代号 LSVAC6187N2187654
发动机号码 EA88812345
注册日期 2022年03月01日
发证日期 2022年03月02日
核定载人数 5人
总质量 1800kg
整备质量 1450kg
检验有效期至 2026年03月
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "vehicle_license"
    assert result["fields"]["plate_number"] == "沪A12345"
    assert result["fields"]["vehicle_identification_number"] == "LSVAC6187N2187654"
    assert result["fields"]["vehicle_owner"] == "上海示例科技有限公司"


def test_marriage_cert_classification_and_extraction() -> None:
    text = """
结婚证
持证人 张三
配偶 李四
11010519491231002X
110105198806153520
登记日期 2020年10月10日
登记机关 北京市朝阳区民政局
结婚证字号 J110105-2020-000001
婚姻登记员 赵老师
"""
    result = run_kyc_document_agent(payload(text))

    assert result["doc_type"] == "marriage_cert"
    assert result["fields"]["holder_name"] == "张三"
    assert result["fields"]["spouse_name"] == "李四"
    assert result["fields"]["registration_date"] == "2020-10-10"


def test_unknown_document_type_returns_unknown_without_crashing() -> None:
    result = KycDocumentAgent().extract(payload("这是一段没有证件关键词的普通文本"))

    assert result["doc_type"] == "unknown"
    assert result["extraction_status"] == "failed"
    assert result["validation"]["warnings"]
