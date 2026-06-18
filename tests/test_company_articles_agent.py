from __future__ import annotations

from backend.services.company_articles_agent import CompanyArticlesAgent, CompanyArticlesSkill, detect_company_articles
from backend.services.company_articles_agent.company_articles_locator import locate_articles_block
from backend.services.company_articles_agent.extractor import (
    clean_articles_title,
    clean_company_address,
    extract_external_shareholder_names,
    extract_fields,
    repair_shareholder_dates_by_majority,
    repair_duplicate_shareholder_names_by_external_names,
    repair_shareholder_names_by_external_names,
)
from backend.services.company_articles_agent.markdown_renderer import render_company_articles_markdown
from backend.services.company_articles_agent.page_classifier import classify_company_articles_pages
from backend.services.company_articles_agent.schema import CompanyArticlesResult, Shareholder
from backend.services.document_agents.orchestrator import run_document_extraction_agent
from backend.services.document_agents.registry import DOCUMENT_AGENT_REGISTRY, get_document_agent
from backend.services.document_extractor_service import build_structured_extraction, detect_document_type_code
from backend.services.kyc_document_agent.orchestrator import KycDocumentAgent


SAMPLE_PAGES = [
    {
        "page": 1,
        "source": "ocr",
        "text": """
上海乐芙兰电子商务有限公司章程
第一章 公司的名称和住所
第一条 公司名称：上海乐芙兰电子商务有限公司
第二条 公司住所：上海市长宁区广顺路33号3幢6层672室
第二章 公司经营范围
第三条 公司经营范围：许可项目：食品经营；食品互联网销售。一般项目：电子商务（不得从事增值电信、金融业务），日用百货销售，化妆品零售，化妆品批发，互联网销售（除销售需要许可的商品），企业管理咨询，信息咨询服务（不含许可类信息咨询服务）。
第三章 公司注册资本
第四条 公司注册资本：人民币 500 万元；
""",
    },
    {
        "page": 2,
        "source": "ocr",
        "text": """
股东的姓名或者名称、出资方式、出资额和出资时间
沃志方 495万元 货币 2030.12.31
李倩 5万元 货币 2030.12.31
股东会会议由股东按照出资比例行使表决权。
股东会会议作出修改公司章程、增加或者减少注册资本的决议，以及公司合并、分立、解散或者变更公司形式的决议，必须经代表全体股东三分之二以上表决权的股东通过。
股东会会议作出除前款以外事项的决议，须经代表全体股东三分之二以上表决权通过。
""",
    },
    {
        "page": 3,
        "source": "ocr",
        "text": """
股权转让
股东之间可以相互转让全部或者部分股权；向股东以外的人转让股权，应经其他股东过半数同意；其他股东自接到书面通知之日起满三十日未答复的，视为同意转让；同等条件下其他股东有优先购买权。
""",
    },
    {
        "page": 4,
        "source": "ocr",
        "text": """
公司治理
首次股东会会议由出资最多的股东召集和主持，依照公司法规定行使职权。
公司不设董事会，设执行董事一名，任期三年，由股东会选举产生。
公司设经理，由股东会决定聘任或者解聘，任期三年，可以连任。
公司不设监事会，设监事一人，任期三年，可以连任。
公司的法定代表人由执行董事担任。
""",
    },
    {
        "page": 5,
        "source": "ocr",
        "text": """
财务、会计与利润分配
公司依照法律、行政法规和国务院财政主管部门规定建立财务会计制度；会计年度终了编制财务会计报告；股东按照出资比例分取红利；聘用或解聘会计师事务所由股东会决定。
解散与清算：营业期限为长期；股东会决议可以解散；公司合并或者分立需要解散；依法被吊销营业执照、责令关闭或者被撤销；人民法院依法予以解散；清算组由股东组成。
高级管理人员包括经理、副经理、财务负责人；不得侵占公司财产；不得挪用公司资金；不得未经同意订立合同或者交易；不得泄露公司秘密。
""",
    },
    {
        "page": 6,
        "source": "ocr",
        "text": """
本章程自全体股东盖章、签字之日起生效。
股东（签字、盖章）：沃志方 李倩
红色印章 手写签名
年 月 日
""",
    },
]


SAMPLE_TEXT = "\n\n".join(page["text"] for page in SAMPLE_PAGES)
FILENAME = "乐芙兰章程(新 沃志方).pdf"


def test_detect_company_articles_strong_rules() -> None:
    assert detect_company_articles(SAMPLE_TEXT, filename=FILENAME)
    assert detect_document_type_code(SAMPLE_TEXT, filename=FILENAME) == "company_articles"


def test_registry_dispatches_company_articles_agent() -> None:
    assert "company_articles" in DOCUMENT_AGENT_REGISTRY
    assert get_document_agent("company_articles").agent_name == "company_articles_agent"
    result = run_document_extraction_agent(
        document_type="company_articles",
        raw_text=SAMPLE_TEXT,
        filename=FILENAME,
        metadata={"raw_pages": SAMPLE_PAGES},
    )
    assert result.document_type == "company_articles"
    assert result.agent_name == "company_articles_agent"
    assert result.debug["selected_agent"] == "company_articles_agent"
    assert result.debug["skill_name"] == "company_articles_skill"


def test_company_articles_agent_extracts_sample_fields_and_markdown() -> None:
    result = CompanyArticlesAgent().run({"text": "", "raw_pages": SAMPLE_PAGES, "filename": FILENAME})
    data = result.to_dict()
    structured = data["structured_data"]
    markdown = data["markdown"]
    assert structured["title"] == "上海乐芙兰电子商务有限公司章程"
    assert structured["company_name"] == "上海乐芙兰电子商务有限公司"
    assert structured["company_address"] == "上海市长宁区广顺路33号3幢6层672室"
    assert "第二章" not in structured["company_address"]
    assert structured["registered_capital"] == "人民币500万元"
    assert structured["registered_capital_amount"] == 500
    assert len(structured["shareholders"]) == 2
    assert structured["shareholders"][0]["name"] == "沃志方"
    assert structured["shareholders"][0]["contribution_ratio"] == "99.00%"
    assert structured["shareholders"][1]["name"] == "李倩"
    assert structured["shareholders"][1]["contribution_ratio"] == "1.00%"
    assert structured["capital_check"]["is_consistent"] is True
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert structured["governance"]["legal_representative"] == "由执行董事担任"
    assert structured["governance"]["first_shareholders_meeting"] == "首次股东会会议由出资最多的股东召集和主持，依照公司法规定行使职权"
    assert structured["major_resolution_rules"]["amendment_rule"] != "未识别"
    assert structured["major_resolution_rules"]["capital_change_rule"] != "未识别"
    assert structured["major_resolution_rules"]["merger_split_dissolution_rule"] != "未识别"
    assert "法定代表人：由执行董事担任" in markdown
    assert "沃志方 | 495万元 | 货币 | 2030.12.31 | 99.00%" in markdown
    assert "李倩 | 5万元 | 货币 | 2030.12.31 | 1.00%" in markdown
    assert "签章页：第6页" in markdown
    assert "签署日期：未填写/未识别" in markdown
    assert "需人工复核：无" in markdown


def test_company_articles_markdown_does_not_render_raw_keys_or_json() -> None:
    data = CompanyArticlesAgent().run({"text": "", "raw_pages": SAMPLE_PAGES, "filename": FILENAME}).to_dict()
    markdown = data["markdown"]
    forbidden = [
        "doc type",
        "doc_type",
        "doc type name",
        "agent type",
        "company address",
        "registered capital amount",
        "capital check",
        "governance",
        "signature info",
        "evidence",
        "metadata",
        "raw text preview",
        "report markdown",
        "raw_fields",
        "fields",
        "{",
        "}",
        "undefined",
        "null",
        "None",
    ]
    lower = markdown.lower()
    for item in forbidden:
        assert item not in lower


def test_address_boundary_before_second_chapter() -> None:
    text = "第二条公司住所：上海市长宁区广顺路33号3幢6层672室\n第二章公司经营范围"
    extracted = CompanyArticlesSkill().extract(text=text, pages=[{"page": 1, "text": text}], filename=FILENAME)
    assert extracted["company_address"] == "上海市长宁区广顺路33号3幢6层672室"


def test_registered_capital_ocr_variants() -> None:
    text = "第三章 公司注册资本\n第四条公司注册资本：人民币_500_万元；"
    extracted = CompanyArticlesSkill().extract(text=text, pages=[{"page": 1, "text": text}], filename=FILENAME)
    assert extracted["registered_capital"] == "人民币500万元"
    assert extracted["registered_capital_amount"] == 500


def test_build_structured_extraction_uses_document_agent_not_legacy_or_kyc() -> None:
    content = build_structured_extraction(
        SAMPLE_TEXT,
        "company_articles",
        raw_pages=SAMPLE_PAGES,
        filename=FILENAME,
    )
    assert content["document_type_code"] == "company_articles"
    assert content["doc_type_name"] == "公司章程"
    assert content["markdown"].startswith("## 公司章程")
    assert content["display_markdown"] == content["markdown"]
    assert content["report_markdown"] == content["markdown"]
    assert content["extraction_version"] == "company_articles_v6_external_name_repair"
    assert content["display_markdown"] == content["report_markdown"] == content["markdown"]
    assert "agent_type" not in content
    assert "title" not in content
    assert "company_address" not in content
    assert "registered_capital_amount" not in content
    assert "capital_check" not in content
    assert "governance" not in content
    assert "major_resolution_rules" not in content
    assert "raw_text_preview" not in content
    assert "evidence" not in content
    assert "metadata" not in content
    assert "raw_text_preview" not in content["structured_data"]
    assert "evidence" not in content["structured_data"]
    assert "metadata" not in content["structured_data"]
    assert "markdown" not in content["structured_data"]
    assert "display_markdown" not in content["structured_data"]
    assert "report_markdown" not in content["structured_data"]


def test_company_articles_is_excluded_from_lightweight_kyc() -> None:
    result = KycDocumentAgent().extract({"text": SAMPLE_TEXT, "metadata": {"filename": FILENAME, "declared_doc_type": "company_articles"}})
    assert result["agent_type"] == "kyc_document_agent"
    assert result["doc_type"] != "company_articles"
    assert result["doc_type"] != "articles_keypage"


def test_company_articles_skill_supports_multi_page_ocr_merge() -> None:
    extracted = CompanyArticlesSkill().extract(text=SAMPLE_TEXT, pages=SAMPLE_PAGES, filename=FILENAME)
    assert extracted["page_count"] == 6
    assert extracted["company_name"] == "上海乐芙兰电子商务有限公司"
    assert len(extracted["shareholders"]) == 2
    assert extracted["signature_info"]["signature_page"] == "第6页"


def test_company_articles_extracts_all_shareholder_rows_and_rechecks_capital() -> None:
    text = """
上海测试贸易有限公司章程
第一章 公司的名称和住所
第一条 公司名称：上海测试贸易有限公司
第二条 公司住所：上海市浦东新区测试路1号
第二章 公司经营范围
第三条 公司经营范围：一般项目：日用百货销售。
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元

第四章 股东的姓名或者名称、出资方式、出资额和出资日期
第五条 股东的姓名或者名称、出资方式、出资额和出资日期如下：
股东的姓名或者名称 出资额 出资方式 出资日期
钟璟 1400万元 货币 2029年12月15日
黎云 600万元 货币 2029年12月15日

第六条 公司成立后，应向股东签发出资证明书。
第五章 公司机构
股东会会议由股东按照出资比例行使表决权。
公司不设董事会，设执行董事一名，任期三年，由股东会选举产生。
公司的法定代表人由执行董事担任。
本章程自全体股东盖章、签字之日起生效。
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "钟璟黎云公司章程.pdf",
        }
    )
    data = result.to_dict()
    structured = data["structured_data"]
    shareholders = structured["shareholders"]
    markdown = data["display_markdown"]

    assert len(shareholders) == 2
    assert shareholders[0]["name"] == "钟璟"
    assert shareholders[0]["subscribed_amount"] == "1400万元"
    assert shareholders[0]["subscribed_amount_number"] == 1400
    assert shareholders[0]["contribution_method"] == "货币"
    assert shareholders[0]["contribution_deadline"] == "2029.12.15"
    assert shareholders[0]["contribution_ratio"] == "70.00%"
    assert shareholders[1]["name"] == "黎云"
    assert shareholders[1]["subscribed_amount"] == "600万元"
    assert shareholders[1]["subscribed_amount_number"] == 600
    assert shareholders[1]["contribution_method"] == "货币"
    assert shareholders[1]["contribution_deadline"] == "2029.12.15"
    assert shareholders[1]["contribution_ratio"] == "30.00%"
    assert structured["registered_capital_amount"] == 2000
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["is_consistent"] is True
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert "| 钟璟 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" in markdown
    assert "| 黎云 | 600万元 | 货币 | 2029.12.15 | 30.00% |" in markdown
    assert "股东出资额合计与注册资本不一致，请人工复核" not in markdown
    assert "股东信息：未识别" not in markdown


def test_company_articles_shareholder_parser_supports_no_space_ocr_rows() -> None:
    text = """
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元

第四章 股东的姓名或者名称、出资方式、出资额和出资日期
股东的姓名或者名称 出资额 出资方式 出资日期
钟璟1400万元货币2029年12月15日黎云600万元货币2029年12月15日
第六条 公司成立后，应向股东签发出资证明书并置备股东名册。
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "无空格股东表章程.pdf",
        }
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]
    markdown = result["display_markdown"]

    assert len(shareholders) == 2
    assert shareholders[0]["name"] == "钟璟"
    assert shareholders[0]["subscribed_amount"] == "1400万元"
    assert shareholders[0]["contribution_method"] == "货币"
    assert shareholders[0]["contribution_deadline"] == "2029.12.15"
    assert shareholders[0]["contribution_ratio"] == "70.00%"
    assert shareholders[1]["name"] == "黎云"
    assert shareholders[1]["subscribed_amount"] == "600万元"
    assert shareholders[1]["contribution_method"] == "货币"
    assert shareholders[1]["contribution_deadline"] == "2029.12.15"
    assert shareholders[1]["contribution_ratio"] == "30.00%"
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert "| 未识别 | 未识别 | 未识别 | 未识别 | 未识别 |" not in markdown
    assert "| 钟璟 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" in markdown
    assert "| 黎云 | 600万元 | 货币 | 2029.12.15 | 30.00% |" in markdown


def test_company_articles_shareholder_parser_supports_misaligned_newline_ocr_rows() -> None:
    text = """
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元
股东的姓名或者名称
出资额
出资方式
出资日期
钟璟
1400万元
货币
2029年12月15日
黎云
600万元
货币
2029年12月15日
第六条 公司成立后
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "换行错位股东表章程.pdf",
        }
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]
    markdown = result["display_markdown"]

    assert len(shareholders) == 2
    assert shareholders[0]["name"] == "钟璟"
    assert shareholders[0]["subscribed_amount"] == "1400万元"
    assert shareholders[0]["contribution_method"] == "货币"
    assert shareholders[0]["contribution_deadline"] == "2029.12.15"
    assert shareholders[0]["contribution_ratio"] == "70.00%"
    assert shareholders[1]["name"] == "黎云"
    assert shareholders[1]["subscribed_amount"] == "600万元"
    assert shareholders[1]["contribution_method"] == "货币"
    assert shareholders[1]["contribution_deadline"] == "2029.12.15"
    assert shareholders[1]["contribution_ratio"] == "30.00%"
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert "| 钟璟 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" in markdown
    assert "| 黎云 | 600万元 | 货币 | 2029.12.15 | 30.00% |" in markdown
    assert "股东信息：未识别" not in markdown
    assert "股东出资额合计与注册资本不一致，请人工复核" not in markdown


def test_company_articles_shareholder_parser_ignores_equity_transfer_phrase() -> None:
    text = """
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元

第四章 股东的姓名或者名称、出资方式、出资额和出资日期
第五条 股东的姓名或者名称、出资方式、出资额和出资日期如下：
股东的姓名或者名称 出资额 出资方式 出资日期
钟璟 1400万元 货币 2029年12月15日
黎云 600万元 货币 2029年12月15日
第六条 公司成立后，应向股东签发出资证明书并置备股东名册。

第七章 股权转让
股权转让后 1400万元 货币 2029年12月15日
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "股权转让误识别章程.pdf",
        }
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]
    markdown = result["display_markdown"]

    assert len(shareholders) == 2
    assert [item["name"] for item in shareholders] == ["钟璟", "黎云"]
    assert "股权转让后" not in [item["name"] for item in shareholders]
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert "| 股权转让后 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" not in markdown
    assert "| 钟璟 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" in markdown
    assert "| 黎云 | 600万元 | 货币 | 2029.12.15 | 30.00% |" in markdown


def test_company_articles_shareholder_parser_stops_fallback_when_capital_matches() -> None:
    text = """
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元

第四章 股东的姓名或者名称、出资方式、出资额和出资日期
第五条 股东的姓名或者名称、出资方式、出资额和出资日期如下：
股东的姓名或者名称 出资额 出资方式 出资日期
钟璟 1400万元 货币 2029年12月15日
黎云 600万元 货币 2029年12月15日
第六条 公司成立后，应向股东签发出资证明书并置备股东名册。

第八章 财务、会计与利润分配
其他正文 600万元 货币 2029年12月15日
高级管理人员 1400万元 货币 2029年12月15日
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "后文金额不追加章程.pdf",
        }
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]

    assert len(shareholders) == 2
    assert [item["name"] for item in shareholders] == ["钟璟", "黎云"]
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"


def test_company_articles_shareholder_parser_filters_same_amount_fake_shareholder() -> None:
    text = """
第三章 公司注册资本
第四条 公司注册资本：人民币2000万元

第四章 股东的姓名或者名称、出资方式、出资额和出资日期
第五条 股东的姓名或者名称、出资方式、出资额和出资日期如下：
股东的姓名或者名称 出资额 出资方式 出资日期
钟璟 1400万元 货币 2029年12月15日
黎云 600万元 货币 2029年12月15日
第六条 公司成立后，应向股东签发出资证明书并置备股东名册。

正文噪声：
上海意川建筑科科 1400万元 货币 2029年12月15日
"""
    result = CompanyArticlesAgent().run(
        {
            "text": text,
            "raw_pages": [{"page": 1, "text": text}],
            "filename": "同金额假股东章程.pdf",
        }
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]
    markdown = result["display_markdown"]

    assert len(shareholders) == 2
    assert [item["name"] for item in shareholders] == ["钟璟", "黎云"]
    assert "上海意川建筑科科" not in [item["name"] for item in shareholders]
    assert structured["capital_check"]["shareholder_total_amount"] == 2000
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert "| 上海意川建筑科科 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" not in markdown
    assert "股东出资额合计：3400万元" not in markdown
    assert "股东出资额合计与注册资本不一致，请人工复核" not in markdown
    assert "| 钟璟 | 1400万元 | 货币 | 2029.12.15 | 70.00% |" in markdown
    assert "| 黎云 | 600万元 | 货币 | 2029.12.15 | 30.00% |" in markdown


def test_company_articles_markdown_does_not_create_fake_unknown_shareholder_row() -> None:
    result = CompanyArticlesResult(
        doc_type_name="公司章程",
        registered_capital="人民币2000万元",
        registered_capital_amount=2000,
        capital_check={
            "shareholder_total_amount_text": "未识别",
            "message": "股东出资额合计与注册资本不一致，请人工复核",
        },
    )
    markdown = render_company_articles_markdown(result, filename="空股东章程.pdf")
    assert "| 未识别 | 未识别 | 未识别 | 未识别 | 未识别 |" not in markdown
    assert "- 股东信息：未识别" in markdown


def test_company_articles_locates_articles_inside_registration_archive_bundle() -> None:
    pages = [
        {"page": 1, "text": "准予变更登记通知书\n经审查，准予变更登记\n登记机关"},
        {"page": 2, "text": "准予变更登记通知书\n变更事项"},
        {"page": 3, "text": "公司登记（备案）申请书\n基本信息\n申请人声明"},
        {"page": 4, "text": "公司登记备案申请书\n变更信息\n申请人声明"},
        {
            "page": 5,
            "text": (
                "股东（发起人）出资情况\n证件号码\n认缴出资额\n"
                "李亚光 20万\n梁啸民 20万\n徐绚纹 40万\n王毅 20万"
            ),
        },
        {"page": 6, "text": "法定代表人信息\n移动电话\n电子邮箱\n身份证件号码"},
        {"page": 7, "text": "董事、监事、经理信息\n身份证件号码"},
        {"page": 8, "text": "承诺书\n申请人承诺"},
        {"page": 9, "text": "财务负责人信息\n移动电话\n电子邮箱"},
        {"page": 10, "text": "联络员信息\n移动电话\n电子邮箱"},
        {"page": 11, "text": "指定代表或者共同委托代理人授权委托书"},
        {
            "page": 12,
            "text": (
                "股东会决议\n同意变更后的经营范围\n通过公司新的章程\n"
                "股东签字：徐绚纹 李亚光 王毅 梁啸民"
            ),
        },
        {
            "page": 13,
            "text": """
上海崇璟项目管理有限公司章程
依据《中华人民共和国公司法》制定本章程。
第一章 公司的名称和住所
第一条 公司名称：上海崇璟项目管理有限公司
第二条 公司住所：上海市普陀区武威路88弄21号3层97室
第二章 公司经营范围
第三条 公司经营范围：建筑项目管理，建设工程监理服务，建筑装修装饰工程专业施工，物业管理，从事信息科技专业领域内的技术咨询、技术服务，企业管理咨询。【依法须经批准的项目，经相关部门批准后方可开展经营活动】
第三章 公司注册资本
第四条 公司注册资本：人民币100.0000万元
第四章 股东的姓名或者名称、出资方式、出资额和出资时间
第五条 股东的姓名或者名称 出资额 出资方式 出资时间
李亚光 20万 货币 2048.4.2
梁啸民 20万 货币 2048.4.2
徐绚纹 40万 货币 2048.4.2
李亚光 20万 货币 2048.4.21
第六条 公司成立后，应向股东签发出资证明书。
""",
        },
        {
            "page": 14,
            "text": """
第五章 公司的机构及其产生办法、职权、议事规则
股东会是公司的权力机构。
股东会会议由股东按照出资比例行使表决权。
修改公司章程、增加或者减少注册资本以及公司合并、分立、解散或者变更公司形式，
须经代表全体股东三分之二以上表决权的股东通过。
公司不设董事会，设执行董事一名，由股东会选举产生。
""",
        },
        {
            "page": 15,
            "text": """
第六章 经理、监事和公司的法定代表人
经理由股东会决定聘任或者解聘。
公司不设监事会，设监事一人。
公司的法定代表人由执行董事担任。
第七章 股权转让
股东之间可以相互转让全部或者部分股权，其他股东在同等条件下有优先购买权。
""",
        },
        {
            "page": 16,
            "text": """
第八章 财务、会计、利润分配
公司依照法律、行政法规建立财务会计制度，股东按照出资比例分取红利。
第九章 公司的解散事由与清算办法
公司解散时，清算组由股东组成。
第十章 高级管理人员义务
高级管理人员不得侵占公司财产，不得挪用公司资金，不得泄露公司秘密。
""",
        },
        {
            "page": 17,
            "text": """
本章程自全体股东盖章、签字之日起生效。
股东签字：李亚光 梁啸民 徐绚纹 王毅
公司印章
签署日期：
""",
        },
        {"page": 18, "text": "营业执照\n统一社会信用代码\n名称 上海崇璟项目管理有限公司\n登记机关\n成立日期"},
        {"page": 19, "text": "营业执照\n统一社会信用代码\n住所 上海市普陀区武威路88弄21号3层97室\n登记机关"},
    ]

    classes = classify_company_articles_pages(pages)
    block = locate_articles_block(pages)
    assert classes[0].page_type == "change_registration_notice"
    assert classes[11].page_type == "shareholder_resolution"
    assert classes[17].page_type == "business_license"
    assert classes[18].page_type == "business_license"
    assert block is not None
    assert block.page_numbers == [13, 14, 15, 16, 17]
    external_names = extract_external_shareholder_names(pages, classes)
    assert set(external_names) == {"李亚光", "梁啸民", "徐绚纹", "王毅"}

    result = CompanyArticlesAgent().run(
        {"text": "", "raw_pages": pages, "filename": "崇景公司章程.pdf"}
    ).to_dict()
    structured = result["structured_data"]
    shareholders = structured["shareholders"]
    markdown = result["display_markdown"]

    assert result["doc_type"] == "company_articles"
    assert structured["title"] == "上海崇璟项目管理有限公司章程"
    assert structured["company_name"] == "上海崇璟项目管理有限公司"
    assert structured["company_address"] == "上海市普陀区武威路88弄21号3层97室"
    assert structured["registered_capital"] == "人民币100万元"
    assert structured["registered_capital_amount"] == 100
    assert len(shareholders) == 4
    assert [(item["name"], item["subscribed_amount"], item["contribution_deadline"], item["contribution_ratio"]) for item in shareholders] == [
        ("李亚光", "20万元", "2048.04.02", "20.00%"),
        ("梁啸民", "20万元", "2048.04.02", "20.00%"),
        ("徐绚纹", "40万元", "2048.04.02", "40.00%"),
        ("王毅", "20万元", "2048.04.02", "20.00%"),
    ]
    assert structured["capital_check"]["shareholder_total_amount"] == 100
    assert structured["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert structured["signature_info"]["signature_page"] == "第17页"
    assert structured["signature_info"]["signature_detection_summary"] == "识别到股东签字和公司印章"
    assert "章程正文页：第13-17页" in markdown
    assert "章程标题：未识别" not in markdown
    assert "公司名称：未识别" not in markdown
    assert "股东信息：未识别" not in markdown
    assert "股东出资额合计与注册资本不一致，请人工复核" not in markdown


def test_company_articles_title_ignores_body_phrase() -> None:
    text = """
上海崇璟项目管理有限公司章程
依据《中华人民共和国公司法》及本公司章程，经全体股东讨论制定。
第一章 公司的名称和住所
第一条 公司名称：上海崇璟项目管理有限公司
"""
    extracted = extract_fields(text, pages=[{"page": 13, "text": text}], filename="崇景公司章程.pdf")
    assert extracted["title"] == "上海崇璟项目管理有限公司章程"
    assert clean_articles_title("及本公司章程", "上海崇璟项目管理有限公司") == "上海崇璟项目管理有限公司章程"


def test_company_articles_address_stops_before_company_name_and_chapter() -> None:
    address = clean_company_address(
        "上海市普陀区武威路88弄21号3层97室；上海崇璟项目管理有限公司 第二章 公司经营范围",
        "上海崇璟项目管理有限公司",
    )
    assert address == "上海市普陀区武威路88弄21号3层97室"
    extracted = extract_fields(
        "第一条 公司名称：上海崇璟项目管理有限公司\n"
        "第二条 公司住所：上海市普陀区武威路88弄21号3层97室；上海崇璟项目管理有限公司 第二章 公司经营范围\n"
        "第三条 公司经营范围：建筑项目管理\n第三章 公司注册资本\n"
        "第四条 公司注册资本：人民币100万元",
        pages=[{"page": 13, "text": "章程正文"}],
    )
    assert extracted["company_address"] == "上海市普陀区武威路88弄21号3层97室"


def test_company_articles_repairs_duplicate_name_and_outlier_date() -> None:
    shareholders = [
        Shareholder("李亚光", "20万元", 20, "货币", "2048.04.02", "20.00%"),
        Shareholder("梁啸民", "20万元", 20, "货币", "2048.04.02", "20.00%"),
        Shareholder("徐绚纹", "40万元", 40, "货币", "2048.04.02", "40.00%"),
        Shareholder("李亚光", "20万元", 20, "货币", "2048.04.21", "20.00%"),
    ]
    repaired = repair_shareholder_dates_by_majority(
        repair_shareholder_names_by_external_names(
            shareholders,
            ["李亚光", "梁啸民", "徐绚纹", "王毅"],
        )
    )
    assert [(item.name, item.contribution_deadline) for item in repaired] == [
        ("李亚光", "2048.04.02"),
        ("梁啸民", "2048.04.02"),
        ("徐绚纹", "2048.04.02"),
        ("王毅", "2048.04.02"),
    ]


def test_company_articles_repairs_duplicate_name_even_when_capital_already_matches() -> None:
    shareholders = [
        Shareholder("李亚光", "20万元", 20, "货币", "2048.04.02", "20.00%"),
        Shareholder("梁啸民", "20万元", 20, "货币", "2048.04.02", "20.00%"),
        Shareholder("徐绚纹", "40万元", 40, "货币", "2048.04.02", "40.00%"),
        Shareholder("李亚光", "20万元", 20, "货币", "2048.04.02", "20.00%"),
    ]
    repaired = repair_duplicate_shareholder_names_by_external_names(
        shareholders,
        ["李亚光", "梁啸民", "徐绚纹", "王毅"],
        100,
    )
    assert [item.name for item in repaired] == ["李亚光", "梁啸民", "徐绚纹", "王毅"]
    assert sum(float(item.subscribed_amount_number or 0) for item in repaired) == 100
    assert [item.name for item in repaired].count("李亚光") == 1
    assert [item.name for item in repaired].count("王毅") == 1


def test_company_articles_classifier_uses_merged_image_and_crop_ocr_text() -> None:
    page_13 = {
        "page": 13,
        "pdf_text": "上海崇璟项目管理有限公司 2025-01-01 验证码",
        "image_ocr_text": (
            "上海崇璟项目管理有限公司章程\n第一章 公司的名称和住所\n"
            "第一条 公司名称：上海崇璟项目管理有限公司\n"
            "第二条 公司住所：上海市普陀区武威路88弄21号3层97室\n"
            "第四条 公司注册资本：人民币100万元"
        ),
        "crop_ocr_text": (
            "股东的姓名 出资额 出资方式 出资时间\n"
            "李亚光 20万 货币 2048.4.2\n梁啸民 20万 货币 2048.4.2\n"
            "徐绚纹 40万 货币 2048.4.2\n王毅 20万 货币 2048.4.2"
        ),
    }
    page_13["text"] = "\n".join(
        [page_13["pdf_text"], page_13["image_ocr_text"], page_13["crop_ocr_text"]]
    )
    classified = classify_company_articles_pages([page_13])
    assert classified[0].page_type == "company_articles_page"
    assert "上海崇璟项目管理有限公司章程" in classified[0].text
    assert "公司注册资本" in classified[0].text
    assert "李亚光 20万 货币 2048.4.2" in classified[0].text


def test_company_articles_upload_path_uses_all_page_high_dpi_ocr() -> None:
    source = (
        __import__("pathlib").Path(__file__).parents[1]
        / "backend"
        / "routers"
        / "file.py"
    ).read_text(encoding="utf-8")
    assert "def _ocr_company_articles_pdf_pages(" in source
    assert "file_service.pdf_to_images(file_bytes, dpi=400)" in source
    assert "text_content, raw_pages = _ocr_company_articles_pdf_pages(" in source
    assert '"shareholder_table"' in source
    assert "max(0, int(height * 0.45))" in source
    assert "min(height, int(height * 0.75))" in source
