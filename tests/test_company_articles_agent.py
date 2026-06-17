from __future__ import annotations

from backend.services.company_articles_agent import CompanyArticlesAgent, CompanyArticlesSkill, detect_company_articles
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
    markdown = data["markdown"]
    assert data["title"] == "上海乐芙兰电子商务有限公司章程"
    assert data["company_name"] == "上海乐芙兰电子商务有限公司"
    assert data["company_address"] == "上海市长宁区广顺路33号3幢6层672室"
    assert "第二章" not in data["company_address"]
    assert data["registered_capital"] == "人民币500万元"
    assert data["registered_capital_amount"] == 500
    assert len(data["shareholders"]) == 2
    assert data["shareholders"][0]["name"] == "沃志方"
    assert data["shareholders"][0]["contribution_ratio"] == "99.00%"
    assert data["shareholders"][1]["name"] == "李倩"
    assert data["shareholders"][1]["contribution_ratio"] == "1.00%"
    assert data["capital_check"]["is_consistent"] is True
    assert data["capital_check"]["message"] == "出资额合计与注册资本一致"
    assert data["governance"]["legal_representative"] == "由执行董事担任"
    assert data["governance"]["first_shareholders_meeting"] == "首次股东会会议由出资最多的股东召集和主持，依照公司法规定行使职权"
    assert data["major_resolution_rules"]["amendment_rule"] != "未识别"
    assert data["major_resolution_rules"]["capital_change_rule"] != "未识别"
    assert data["major_resolution_rules"]["merger_split_dissolution_rule"] != "未识别"
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
    assert content["extraction_version"] == "company_articles_v2_display_only"
    assert "agent_type" not in content
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
