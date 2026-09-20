import asyncio
import copy
import importlib.util
import re
from html import unescape

import pytest
from test_assistant_credit_one_page_report import complete_storage, run
from backend.services.assistant_credit_report_service import (
    get_customer_materials, build_credit_report_model, _default_narrative,
)
from backend.services.credit_report_markdown_renderer import render_credit_one_page_report
from backend.services.credit_report_html_renderer import render_credit_report_html
from backend.services.credit_report_export_service import (
    freeze_credit_report, snapshot_html, pdf_filename, render_html_pdf, SNAPSHOT_VERSION,
)

GENERATED_AT = '2026-09-18 17:07:19'


@pytest.fixture
def report():
    storage=complete_storage()
    cid=storage.customers[0]['customer_id']
    personal=storage.extractions[cid][1]['extracted_data']['report_json']
    balances=[5000000,2240000,75000,3200000,1800000,3000000,3424532]
    personal['related_repayment_responsibilities']=[{
        'related_party':'上海意川建筑科技有限公司',
        'institution':'远东宏信普惠融资租赁（天津）有限公司' if i==6 else f'测试银行{i+1}',
        'responsibility_amount':4000000 if i==6 else n+100,
        'balance':n,'responsibility_type':'保证人','business_type':'融资租赁','as_of_date':'2026-03-12',
        'evidence':'PRIVATE_EVIDENCE'*80+f'余额{n:,}（人民币元）',
    } for i,n in enumerate(balances)]
    material=run(get_customer_materials(storage,cid))
    model=build_credit_report_model(material,GENERATED_AT)
    narrative=_default_narrative(model)
    return model,narrative


@pytest.fixture
def html(report):
    return render_credit_report_html(*report,GENERATED_AT)


def test_credit_report_html_has_five_main_pages(html):
    assert html.count('class="page main-page')==5


def test_credit_report_html_contains_title(html):
    assert '<h1>征信速览报告</h1>' in html


def test_credit_report_html_contains_customer_subjects(html):
    for value in ('上海意川建筑科技有限公司','黎云','法定代表人 / 实际控制人'):
        assert value in html


def test_credit_report_html_escapes_user_text(report):
    report[0]['subjects']['customer_subject']='<script>alert("x")</script>'
    result=render_credit_report_html(*report,GENERATED_AT)
    assert '<script>' not in result
    assert '&lt;script&gt;' in result


def test_credit_report_html_does_not_contain_raw_evidence(report):
    report[0]['evidence']='DO_NOT_RENDER'
    report[0]['personal_credit']['raw_text']='DO_NOT_RENDER'
    assert 'DO_NOT_RENDER' not in render_credit_report_html(*report,GENERATED_AT)


def test_credit_report_html_does_not_contain_internal_fields(html):
    for value in ('customer_id','extraction_id','document_id','raw_text','ocr_text','needs_review','PRIVATE_EVIDENCE','internal_status'):
        assert value not in html


def test_credit_report_html_uses_same_model_as_markdown(report,html):
    before=copy.deepcopy(report)
    markdown=render_credit_one_page_report(*report,GENERATED_AT)
    for value in ('上海意川建筑科技有限公司','黎云','法定代表人 / 实际控制人','18,739,532元','4,000,000（单位待核验）','3,424,532元'):
        assert value in markdown and value in html
    assert report==before


@pytest.mark.parametrize('page,labels',[
    (1,['紧急关注','主体基本信息','核心指标']),
    (2,['企业贷款','个人贷款','三、信用卡']),
    (3,['企业对外担保','法人相关还款责任','逾期与公共记录']),
    (4,['征信查询记录','历史信贷特征','征信指标检查']),
    (5,['征信优化路线图','综合说明','一句话结论']),
],ids=['page1_contains_summary_and_core_metrics','page2_contains_loans_and_credit_cards','page3_contains_guarantees_and_overdue','page4_contains_queries_and_rule_checks','page5_contains_roadmap_and_summary'])
def test_page_structure(html,page,labels):
    content=html.split(f'data-page="{page}">',1)[1].split('</section>',1)[0]
    assert all(label in content for label in labels)


def test_credit_report_pdf_filename_is_safe(report):
    report[0]['subjects']['customer_subject']='公司/\\:*?"<>|\r\n'
    result=pdf_filename({'report_json':{'model':report[0],'generated_at':GENERATED_AT}})
    assert not re.search(r'[\\/:*?"<>|\r\n]',result)
    assert result.endswith('_20260918.pdf')


def test_explicit_zero_guarantee_is_not_displayed_as_missing(report):
    html=render_credit_report_html(*report,GENERATED_AT)
    assert '企业征信明确记录为0' in html


def test_missing_public_records_are_not_displayed_as_no_records(report):
    report[0]['personal_credit']['public_records']=[]
    report[0]['personal_credit']['non_credit_transactions']=[]
    html=render_credit_report_html(*report,GENERATED_AT)
    assert '未发现记录' not in html


def test_long_tables_preserve_all_rows_after_five_main_pages(report):
    report[0]['enterprise_credit']['loans']=[{'index':i,'institution':f'唯一机构{i:03}'} for i in range(50)]
    result=render_credit_report_html(*report,GENERATED_AT)
    assert result.count('class="page main-page')==5
    assert result.count('class="page appendix')>0
    for i in range(50):assert result.count(f'唯一机构{i:03}')==1
    assert result.index('data-page="5"')<result.index('data-page="6"')
    assert 'overflow: hidden' not in result


class SnapshotStorage:
    def __init__(self):self.rows={}
    async def create_financing_diagnostic_report_snapshot(self,row):
        self.rows[row['report_id']]=copy.deepcopy(row);return row
    async def get_credit_report_snapshot(self,cid,rid):
        row=self.rows.get(rid)
        return row if row and row['customer_id']==cid else None


def test_snapshot_is_frozen_and_export_never_reanalyzes(report):
    storage=SnapshotStorage()
    links=run(freeze_credit_report(storage,'c1',*report,GENERATED_AT,'saved markdown'))
    frozen=storage.rows[links['reportId']]
    before=snapshot_html(frozen)
    report[0]['subjects']['customer_subject']='后续修改的客户'
    assert snapshot_html(frozen)==before
    assert run(storage.get_credit_report_snapshot('other',links['reportId'])) is None


def test_credit_snapshot_is_isolated_from_financing_history(report):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from backend.db_models import CustomerFinancingDiagnosticReportSnapshot
    from backend.services.sqlalchemy_storage_service import SQLAlchemyStorageService
    engine=create_engine('sqlite:///:memory:')
    CustomerFinancingDiagnosticReportSnapshot.__table__.create(engine)
    storage=SQLAlchemyStorageService.__new__(SQLAlchemyStorageService)
    storage._session_factory=sessionmaker(bind=engine)
    links=run(freeze_credit_report(storage,'c1',*report,GENERATED_AT,'markdown'))
    assert run(storage.get_credit_report_snapshot('c1',links['reportId']))
    assert run(storage.get_credit_report_snapshot('c2',links['reportId'])) is None
    assert run(storage.list_financing_diagnostic_report_snapshots('c1'))==[]
    assert run(storage.get_financing_diagnostic_report_snapshot('c1',links['reportId'])) is None
    engine.dispose()


@pytest.fixture(scope='module')
def pdf_engine():
    if not importlib.util.find_spec('playwright'):
        pytest.skip('Local Playwright unavailable; real Chromium tests must run on production')


@pytest.fixture
def pdf(pdf_engine,html):
    return run(render_html_pdf(html))


def test_credit_report_pdf_is_non_empty(pdf):
    assert len(pdf)>10000


def test_credit_report_pdf_starts_with_pdf_signature(pdf):
    assert pdf.startswith(b'%PDF-')


def test_credit_report_pdf_has_five_pages_and_consistent_facts(pdf):
    import fitz
    doc=fitz.open(stream=pdf,filetype='pdf')
    assert len(doc)==5
    content=''.join(page.get_text() for page in doc)
    for value in ('上海意川建筑科技有限公司','18,739,532','3,424,532','法定代表人'):
        assert value in content
    assert 'PRIVATE_EVIDENCE' not in content


def test_realistic_record_volume_keeps_five_pdf_pages(pdf_engine,report):
    model,narrative=report
    for owner,count in [('enterprise_credit',7),('personal_credit',10)]:
        seed=model[owner]['loans'][0]
        model[owner]['loans']=[{**seed,'index':i+1,'institution':'中国建设银行股份有限公司上海闵行支行',
            'due_date_assessment':'原到期日为2026-04-23，早于本报告生成日；当前是否已结清、续贷或展期资料不足，需核实',
            'contract_amount':{'value':500,'unit':None},'balance':{'value':500,'unit':None}} for i in range(count)]
    model['personal_credit']['credit_cards']*=3
    narrative['emergency_attention']=['核验多笔贷款原到期日早于报告生成日的当前状态，确认是否已结清、续贷或展期。']*4
    for key in ('optimization_urgent','optimization_medium','optimization_long','advantages','risks'):
        narrative[key]=['现有资料需结合源征信报告逐项核验，补充相关材料并确认记录的当前状态。']*4
    import fitz
    doc=fitz.open(stream=run(render_html_pdf(render_credit_report_html(model,narrative,GENERATED_AT))),filetype='pdf')
    assert len(doc)==5
    headings=['主体与核心指标','贷款及信用卡','担保、逾期与公共记录','查询记录与征信指标','优化路线图与综合结论']
    for page,heading in zip(doc,headings):assert heading in page.get_text()


def test_credit_report_pdf_returns_pdf_content_type(monkeypatch,report):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import backend.services as services
    import backend.services.sqlalchemy_storage_service as sql_storage
    storage=SnapshotStorage()
    async def customer(cid):return {'customer_id':cid,'name':'测试','uploader':'tester'}
    storage.get_customer=customer
    monkeypatch.setattr(services,'get_storage_service',lambda:storage)
    monkeypatch.setattr(sql_storage,'SQLAlchemyStorageService',lambda:storage)
    from backend.routers import customer as routes
    from backend.middleware.auth import get_current_user
    import backend.services.credit_report_export_service as exports
    monkeypatch.setattr(routes,'storage_service',storage)
    async def authorized(*args):return None
    monkeypatch.setattr(routes,'_ensure_local_customer_access',authorized)
    async def fake_pdf(html):return b'%PDF-1.7\nmock route transport test'
    monkeypatch.setattr(exports,'render_html_pdf',fake_pdf)
    links=run(freeze_credit_report(storage,'c1',*report,GENERATED_AT,'markdown'))
    app=FastAPI();app.include_router(routes.router,prefix='/api')
    with TestClient(app) as client:
        assert client.get(links['pdfUrl']).status_code==401
        app.dependency_overrides[get_current_user]=lambda:{'username':'tester','role':'viewer'}
        response=client.get(links['pdfUrl'])
        assert response.status_code==200
        assert response.headers['content-type']=='application/pdf'
        assert 'filename*=UTF-8' in response.headers['content-disposition']
        preview=client.get(links['previewUrl'])
        assert preview.status_code==200 and preview.headers['content-type'].startswith('text/html')
        assert preview.headers['cache-control']=='private, no-store'
        assert client.get(links['pdfUrl'].replace('/c1/','/c2/')).status_code==404
        async def forbidden(*args):
            from fastapi import HTTPException
            raise HTTPException(403,'无权查看该客户记录')
        monkeypatch.setattr(routes,'_ensure_local_customer_access',forbidden)
        assert client.get(links['previewUrl']).status_code==403
