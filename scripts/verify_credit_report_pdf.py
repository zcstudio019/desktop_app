"""Production HTTP/PDF acceptance using an explicitly supplied login session.

Creates one normal chat report unless --report-id reuses a previously frozen one.
Does not write customer/extraction data or rerun any extraction pipeline.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.request
from urllib.parse import quote

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--customer-record-id',type=int,default=135)
    parser.add_argument('--report-id')
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    from sqlalchemy import text
    from backend.database import engine
    token=os.environ.get('CREDIT_REPORT_QA_TOKEN','')
    if not token:
        raise RuntimeError('请通过正常登录取得授权会话，在进程环境 CREDIT_REPORT_QA_TOKEN 中提供；不要将令牌写入文件或日志。')
    import fitz
    with engine.connect() as c:
        c.exec_driver_sql('SET TRANSACTION READ ONLY')
        customer=dict(c.execute(text('SELECT * FROM customers WHERE id=:id'),{'id':args.customer_record_id}).mappings().one())
        c.rollback()
    cid=customer['customer_id']
    def source_digest():
        with engine.connect() as c:
            c.exec_driver_sql('SET TRANSACTION READ ONLY')
            rows=[dict(r) for r in c.execute(text('SELECT * FROM extractions WHERE customer_id=:cid ORDER BY extraction_id'),{'cid':cid}).mappings()]
            cust=dict(c.execute(text('SELECT * FROM customers WHERE id=:id'),{'id':args.customer_record_id}).mappings().one())
            c.rollback()
        return hashlib.sha256(json.dumps([cust,rows],sort_keys=True,default=str).encode()).hexdigest()
    before=source_digest()
    def request(path,body=None):
        data=json.dumps(body,ensure_ascii=False).encode() if body else None
        req=urllib.request.Request('http://127.0.0.1:8000'+path,data=data,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=180) as res:return res.status,res.headers,res.read()
    if args.report_id:
        rid=args.report_id
        chat_markdown=None
    else:
        status,_,body=request('/api/chat',{'messages':[{'role':'user','content':f'根据{customer["name"]}的资料生成征信一页纸'}],'customerId':cid})
        response=json.loads(body)
        assert status==200 and response['data']['reportStatus']=='completed'
        rid=response['data']['reportId'];chat_markdown=response['message']
    with engine.connect() as c:
        c.exec_driver_sql('SET TRANSACTION READ ONLY')
        row=dict(c.execute(text('SELECT report_json,report_markdown FROM customer_financing_diagnostic_reports WHERE customer_id=:cid AND report_id=:rid'),{'cid':cid,'rid':rid}).mappings().one())
        c.rollback()
    frozen=json.loads(row['report_json']) if isinstance(row['report_json'],str) else row['report_json']
    markdown=row['report_markdown']
    if chat_markdown is not None:assert markdown==chat_markdown
    from backend.services.credit_report_markdown_renderer import render_credit_one_page_report
    from backend.services.assistant_credit_report_service import _strip_internal_output
    assert _strip_internal_output(render_credit_one_page_report(frozen['model'],frozen['narrative'],frozen['generated_at']))==markdown
    base=f'/api/customers/{quote(cid,safe="")}/credit-report/snapshots/{rid}'
    hs,hh,hb=request(base+'/preview');ps,ph,pb=request(base+'/export/pdf')
    assert hs==ps==200 and 'text/html' in hh['Content-Type'] and ph['Content-Type']=='application/pdf'
    assert pb.startswith(b'%PDF-') and len(pb)>10000
    html=hb.decode()
    doc=fitz.open(stream=pb,filetype='pdf')
    content=''.join(page.get_text() for page in doc)
    expected=['法定代表人 / 实际控制人','18,739,532元','4,000,000（单位待核验）','3,424,532元']
    for value in expected:
        assert value in markdown and value in html
        assert re.sub(r'\s','',value) in re.sub(r'\s','',content)
    for forbidden in ('raw_text','ocr_text','needs_review','internal_status','PRIVATE_EVIDENCE','<br>'):
        assert forbidden not in html and forbidden not in content
    output=Path(args.output_dir);output.mkdir(parents=True,exist_ok=True,mode=0o700)
    (output/'report.html').write_bytes(hb)
    (output/'report.md').write_text(markdown,encoding='utf-8')
    (output/'report.pdf').write_bytes(pb)
    for i,page in enumerate(doc):page.get_pixmap(matrix=fitz.Matrix(1.5,1.5)).save(output/f'page-{i+1}.png')
    assert source_digest()==before,'Source customer/extraction data changed'
    result={'report_id':rid,'preview_http':hs,'pdf_http':ps,'pages':len(doc),'pdf_bytes':len(pb),'markdown_matches_frozen_model':True,'source_data_unchanged':True,'expected_values':expected}
    (output/'verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False))
    assert len(doc)==5, f'Expected normal five-page report; got {len(doc)} pages'


if __name__=='__main__':main()
