"""Offline, escaped presentation of the frozen credit report DTO (no analysis)."""
from html import escape
import re
from typing import Any

from backend.services.credit_report_markdown_renderer import render_money
from backend.services.credit_report_v11_display import VERIFICATION_CHECKLIST, absence_statement, display_rule_checks


CSS = """
@page { size: A4; margin: 12mm; }
* { box-sizing: border-box; }
body { margin:0; color:#243447; background:#e9edf2; font:9pt/1.5 'Microsoft YaHei','PingFang SC','Noto Sans CJK SC','Source Han Sans SC','WenQuanYi Zen Hei',sans-serif; }
.page { width:186mm; min-height:270mm; margin:8mm auto; padding:0 0 6mm; background:white; display:flex; flex-direction:column; break-after:page; page-break-after:always; }
.page:last-child { break-after:auto; page-break-after:auto; }
.masthead { display:flex; justify-content:space-between; color:#64748b; border-bottom:2px solid #173c64; padding:3mm 0; font-size:8pt; }
h1 { font-size:25pt; color:#173c64; letter-spacing:1mm; margin:7mm 0 5mm; }
h2 { font-size:13pt; color:#173c64; margin:5mm 0 2mm; border-bottom:1px solid #dbe3eb; padding-bottom:1mm; break-after:avoid; }
h3 { font-size:10pt; margin:3mm 0 1.5mm; break-after:avoid; }
p { margin:1.5mm 0; } ul { padding-left:5mm; margin:2mm 0; } li { margin:1mm 0; }
.grid { display:grid; grid-template-columns:1fr 1fr; gap:3mm; }
.card { border:1px solid #dbe3eb; border-radius:2mm; padding:3mm; break-inside:avoid; }
.metrics { display:grid; grid-template-columns:repeat(4,1fr); gap:2mm; margin-top:3mm; }
.metric { border:1px solid #dbe3eb; border-top:2px solid #173c64; padding:3mm 2mm; break-inside:avoid; }
.metric span { display:block; color:#64748b; font-size:8pt; }
.metric strong { display:block; font-size:12pt; margin-top:2mm; overflow-wrap:anywhere; }
.alert { border-left:3px solid #a95739; background:#fbf4ef; padding:3mm; margin:3mm 0; break-inside:avoid; }
.good { background:#f2f7f4; border-color:#bad6c4; } .warn { background:#fcf8ee; border-color:#e0d0a3; }
.muted,.note { color:#64748b; font-size:8pt; } .note { margin:2mm 0; }
table { border-collapse:collapse; width:100%; table-layout:fixed; font-size:7.3pt; line-height:1.35; margin:1.5mm 0; }
thead { display:table-header-group; } th { color:#173c64; background:#edf2f7; font-weight:600; }
td,th { border:1px solid #d9e1e8; padding:1.4mm 1mm; overflow-wrap:anywhere; vertical-align:top; }
tr { break-inside:avoid; page-break-inside:avoid; } td.money { text-align:right; font-variant-numeric:tabular-nums; } td.date { text-align:center; }
.badge { display:inline-block; padding:.3mm 1mm; border-radius:1mm; background:#edf0f3; color:#586675; }
.badge.green { background:#e9f4ed; color:#286344; } .badge.yellow { background:#fbf0d7; color:#806323; } .badge.red { background:#f9e9e7; color:#993f36; }
.footer { margin-top:auto; padding-top:4mm; font-size:7pt; color:#758394; display:flex; justify-content:space-between; }
.conclusion { border-top:2px solid #173c64; padding:4mm; background:#f1f5f9; font-size:11pt; margin-top:5mm; break-inside:avoid; }
.compact { font-size:8pt; } .compact h2 { margin-top:3mm; } .compact h3 { margin-top:2mm; }
.metadata { display:grid; grid-template-columns:1fr 1fr; column-gap:4mm; font-size:8pt; }
.metadata p { margin:1mm 0; }
.page[data-page="1"] .alert { font-size:8pt; }
.page[data-page="1"] h1 { margin:5mm 0 4mm; }
.compact table { font-size:7pt; line-height:1.25; }
.compact td,.compact th { padding:1mm .8mm; }
.roadmap { display:grid; grid-template-columns:repeat(3,1fr); gap:2mm; }
.summary-grid { display:grid; grid-template-columns:1fr 1fr; gap:3mm; }
.page[data-page="5"] { font-size:8.3pt; line-height:1.4; }
.page[data-page="5"] li { margin:.7mm 0; }
.page[data-page="5"] .conclusion { font-size:9pt; }
.page[data-page="2"] table { font-size:6.7pt; line-height:1.15; }
.page[data-page="2"] td,.page[data-page="2"] th { padding:.7mm .6mm; }
.page[data-page="2"] .note { font-size:7pt; line-height:1.3; margin:1mm 0; }
.page[data-page="2"] h2 { margin:2mm 0 1mm; }
.page[data-page="2"] h3 { margin:1.5mm 0 1mm; }
.page[data-page="5"] { font-size:8pt; }
.page[data-page="5"] h2 { margin-top:3mm; }
.page[data-page="5"] .conclusion { margin-top:3mm; padding:3mm; font-size:8.5pt; }
@media screen { .page { padding:8mm; width:210mm; box-shadow:0 2px 12px #0001; } }
@media print { body { background:white; } .page { margin:0; } * { print-color-adjust:exact; -webkit-print-color-adjust:exact; } }
"""


def text(value: Any) -> str:
    if isinstance(value, dict):
        value = (value.get('message') or '资料不足') if value.get('value') in (None,'') else render_money(value.get('value'), value.get('unit'))
    if value is None or value == '':
        value = '资料不足'
    if isinstance(value, bool):
        value = '是' if value else '否'
    value = str(value)
    if re.search(r'\b(?:raw_text|ocr_text|internal_status|normalized|customer_id|extraction_id|document_id)\b|<br\s*/?>|个人信用报告|信息概要', value, re.I):
        value = '资料异常，需核验'
    value = re.sub(r'\bneeds_review\b', '待核验', value)
    value = re.sub(r'\b(?:missing|unknown)\b', '资料不足', value)
    value = re.sub(r'\bpending\b', '待评估', value)
    return escape(value, quote=True)


def bullets(values, empty='资料不足'):
    return '<ul>' + ''.join('<li>'+text(v)+'</li>' for v in (values or [empty])) + '</ul>'


def badge(value):
    color = {'达标':'green','关注':'yellow','待核验':'yellow','风险':'red','超标':'red'}.get(value, '')
    return f'<span class="badge {color}">{text(value)}</span>'


def table(headers, rows, fields, widths=None):
    cols = '<colgroup>'+''.join(f'<col style="width:{w}%">' for w in widths)+'</colgroup>' if widths else ''
    result = '<table>'+cols+'<thead><tr>'+''.join('<th>'+text(h)+'</th>' for h in headers)+'</tr></thead><tbody>'
    for row in rows or [{}]:
        result += '<tr>'
        for field in fields:
            value = row.get(field)
            cls = 'money' if isinstance(value,dict) and 'value' in value else ('date' if 'date' in field else '')
            result += f'<td class="{cls}">'+(badge(value) if field=='status' else text(value))+'</td>'
        result += '</tr>'
    return result+'</tbody></table>'


def records(items, fields):
    if not items:
        return '<p class="muted">资料不足</p>'
    return ''.join('<p>'+ '；'.join(text(label)+'：'+text(item[key]) for label,key in fields if item.get(key) not in (None,''))+'</p>' for item in items)


def render_credit_report_html(report_model: dict, narrative: dict, generated_at: str) -> str:
    s=report_model.get('subjects') or {}; e=report_model.get('enterprise_credit') or {}
    p=report_model.get('personal_credit') or {}; m=report_model.get('metrics') or {}
    dates=report_model.get('dates') or {}; basic=p.get('basic_info') or {}
    appendices=[]

    def section_table(title, headers, rows, fields, limit=8, widths=None):
        # Bound the main-page row budget; preserve every remaining record on
        # appendices AFTER the five main sections, never clip or discard rows.
        visible=rows[:limit]
        remainder=rows[limit:]
        for start in range(0,len(remainder),12):
            appendices.append((title+'（续表）', table(headers,remainder[start:start+12],fields,widths)))
        notice=f'<p class="note">共 {len(rows)} 条，本页展示 {len(visible)} 条，其余见附页「{text(title)}」。</p>' if remainder else ''
        return '<h3>'+text(title)+'</h3>'+table(headers,visible,fields,widths)+notice

    def page(number, title, content, compact=False):
        kind='main-page' if number<=5 else 'appendix'
        return f'<section class="page {kind} {"compact" if compact else ""}" data-page="{number}"><header class="masthead"><span>征信速览报告</span><span>{text(title)}</span></header>{content}<footer class="footer"><span>基于已保存资料 · 不构成授信承诺</span><span>{number:02d}</span></footer></section>'

    info=[('企业客户',s.get('customer_subject')),('企业征信主体',s.get('enterprise_credit_subject')),('个人征信主体',s.get('personal_credit_subject')),('个人与企业关系',s.get('personal_credit_subject_role')),('报告生成时间',generated_at),('企业征信源报告日期',dates.get('enterprise_source_report_date')),('个人征信源报告日期',dates.get('personal_source_report_date'))]
    first='<h1>征信速览报告</h1><div class="metadata">'+''.join('<p><b>'+label+'：</b>'+text(v)+'</p>' for label,v in info)+'</div>'
    first+='<div class="alert"><h3>优先核验事项</h3>'+bullets(narrative.get('emergency_attention'),'暂无需要立即处理的明确风险事项。')+'</div>'
    first+='<h2>一、主体基本信息</h2><div class="grid"><div class="card"><h3>企业主体</h3><p>'+text(s.get('enterprise_credit_subject'))+'</p><p>统一社会信用代码：'+text(s.get('enterprise_identifier_masked'))+'</p></div><div class="card"><h3>个人主体</h3><p>'+text(s.get('personal_credit_subject'))+' · '+text(s.get('personal_credit_subject_role'))+'</p><p>证件号码：'+text(s.get('personal_identifier_masked'))+'</p><p>婚姻状况：'+text(basic.get('marital_status'))+'</p><p>征信报告编号：'+text(basic.get('report_number'))+'</p><p>征信报告时间：'+text(basic.get('report_time'))+'</p></div></div>'
    metrics=[('企业未结清贷款余额','enterprise_unsettled_loan_balance'),('个人未结清贷款余额','personal_unsettled_loan_balance'),('个人未结清贷款账户数','personal_unsettled_loan_account_count'),('个人信用卡使用率','credit_card_usage_rate'),('贷款逾期账户数','loan_overdue_account_count'),('90天以上逾期账户数','overdue_90d_account_count'),('企业对外担保余额','enterprise_external_guarantee_balance'),('法人相关还款责任余额','personal_related_repayment_balance')]
    first+='<h2>核心指标</h2><div class="metrics">'+''.join('<div class="metric"><span>'+label+'</span><strong>'+text(m.get(key))+'</strong></div>' for label,key in metrics)+'</div>'
    # All additional Markdown metrics are retained, outside the eight headline cards.
    first+='<p class="note">企业未结清贷款机构数：'+text(m.get('enterprise_unsettled_loan_institution_count'))+'；信用卡逾期账户数：'+text(m.get('credit_card_overdue_account_count'))+'；近6个月征信机构查询次数：'+text(m.get('institution_query_6m_count'))+'</p>'

    second='<h2>二、未结清贷款</h2>'
    for title,subject,loans,limit in [('企业贷款',s.get('enterprise_credit_subject'),e.get('loans') or [],7),('个人贷款',s.get('personal_credit_subject'),p.get('loans') or [],10)]:
        display=[]
        details=[]
        for row in loans:
            assessment=str(row.get('due_date_assessment') or '')
            status='当前状态需核实' if '早于本报告生成日' in assessment else ('尚未到期' if '尚未到期' in assessment else row.get('status'))
            display.append({**row,'status':status})
            if assessment: details.append({'index':row.get('index'),'institution':row.get('institution'),'status':row.get('status'),'assessment':assessment,'start_date':row.get('start_date')})
        second+='<p class="note">'+text(title)+'主体：'+text(subject)+'</p>'
        second+=section_table(title,['序号','贷款机构','贷款类型','合同金额','当前余额','发放日期','到期日期','状态'],display,['index','institution','loan_type','contract_amount','balance','start_date','due_date','status'],limit,[4,21,10,13,13,12,12,15])
        # Preserve full source-status/date explanations once, not in narrow cells.
        if details:
            source_statuses={}
            for row in loans:
                source_statuses.setdefault(str(row.get('status') or '资料不足'),[]).append(str(row.get('index') or ''))
            source_note='；'.join(status+'（序号'+','.join(indices)+'）' for status,indices in source_statuses.items())
            second+='<p class="note">源记录状态：'+text(source_note)+'。到期状态说明：“当前状态需核实”指原到期日早于报告生成日；是否已结清、续贷或展期资料不足，并不代表当前逾期。</p>'
    second+='<h2>三、信用卡</h2><p class="note">仅个人征信主体信用卡，不属于企业信用卡。</p>'
    second+=section_table('信用卡明细',['发卡行','币种','信用额度','已用额度','使用率','逾期','状态'],p.get('credit_cards') or [],['issuer','currency','credit_limit','used_amount','usage_rate','overdue','remark'],3,[20,8,17,17,10,13,15])
    second+='<p class="note">账户数量核对：概要账户数 '+text(m.get('credit_card_account_count'))+'；可稳定提取明细数 '+text(m.get('credit_card_detail_count'))+('；两者不一致，需核验。' if m.get('credit_card_count_mismatch') else '。')+'</p>'

    third='<h2>四、担保及相关还款责任</h2><p class="note">企业对外担保仅采用企业征信口径；法人相关还款责任仅采用个人征信口径，两者不等同。</p><p>企业对外担保余额：'+text(m.get('enterprise_external_guarantee_balance'))+'</p>'
    guarantees=e.get('external_guarantees') or []
    guarantee_balance=m.get('enterprise_external_guarantee_balance') or {}
    if not guarantees and m.get('enterprise_external_guarantee_explicit') and isinstance(guarantee_balance,dict) and guarantee_balance.get('value')==0:
        guarantees=[{'guaranteed_subject':'企业征信明确记录为0','institution':'-','balance':guarantee_balance,'guarantee_date':'-','status':'无余额'}]
    third+=section_table('企业对外担保',['被担保主体','贷款机构','担保金额','当前余额','担保日期','状态'],guarantees,['guaranteed_subject','institution','guarantee_amount','balance','guarantee_date','status'],3)
    third+='<p>责任主体：'+text(s.get('personal_credit_subject'))+'；法人相关还款责任余额：'+text(m.get('personal_related_repayment_balance'))+'</p>'
    third+=section_table('法人相关还款责任',['被担保/关联主体','贷款机构','责任金额','当前余额','责任类型','业务类型','截至日期'],p.get('related_repayment_responsibilities') or [],['related_party','institution','responsibility_amount','balance','responsibility_type','business_type','as_of_date'],7,[17,24,16,14,8,8,13])
    third+='<h2>五、逾期与公共记录</h2><div class="grid"><div class="card"><h3>企业征信逾期</h3>'+records(e.get('overdue_records'),[('机构','institution'),('业务类型','account_type'),('状态','current_status'),('逾期月数','overdue_months')])+'</div><div class="card"><h3>个人征信逾期</h3>'
    third+=''.join('<p>'+label+'：'+text(m.get(key))+'</p>' for label,key in [('贷款逾期账户数','loan_overdue_account_count'),('信用卡逾期账户数','credit_card_overdue_account_count'),('90天以上逾期账户数','overdue_90d_account_count')])
    third+=bullets(m.get('overdue_data_conflicts')) if m.get('overdue_data_conflicts') else ''
    third+='</div></div><div class="card"><h3>公共记录 / 非信贷交易记录</h3>'
    for label,key in [('公共记录','public_records'),('非信贷交易记录','non_credit_transactions')]:
        items=p.get(key) or []
        absence=absence_statement(items,key)
        third+=('<p>'+text(absence)+'</p>' if absence else '<b>'+label+'</b>'+records(items,[('记录类型','record_type'),('状态','status'),('日期','date'),('金额','amount'),('内容','content')]))
    third+='</div>'
    if p.get('overdue_records'):
        appendices.append(('个人征信逾期明细',table(['机构','业务类型','状态','逾期金额','逾期月数'],p['overdue_records'],['institution','account_type','current_status','overdue_amount','overdue_months'])))

    fourth='<h2>六、征信查询记录</h2><p class="note">所有统计窗口均以个人征信源报告日期 '+text(dates.get('personal_source_report_date'))+' 为基准，不以本报告生成时间为基准。</p>'
    fourth+=table(['时间范围','贷款审批','信用卡审批','担保资格审查','法人资信审查'],p.get('query_matrix') or [],['window','loan_approval','credit_card_approval','guarantee_review','legal_person_review'])
    fourth+='<p class="note">查询频率说明：'+text(narrative.get('query_frequency_analysis'))+'</p><p class="note">近期贷款审批查询明细：'+text(p.get('recent_loan_approval_queries'))+'</p>'
    fourth+='<h2>七、历史信贷特征</h2>'+bullets(narrative.get('historical_credit_features'))
    fourth+='<h2>八、征信指标检查</h2>'+table(['检查项','状态','当前情况','判断依据','优化方向'],display_rule_checks(report_model.get('rule_checks') or [],m),['item','status','current','basis','direction'],[17,10,22,28,23])
    fifth='<h2>九、征信核验与补充清单</h2><div class="roadmap">'
    for label,items in VERIFICATION_CHECKLIST:
        fifth+='<div class="card"><h3>'+text(label)+'</h3>'+bullets(items)+'</div>'
    fifth+='</div><h2>十、综合说明</h2><div class="summary-grid"><div class="card good"><h3>征信优势</h3>'+bullets(narrative.get('advantages'))+'</div><div class="card warn"><h3>征信风险</h3>'+bullets(narrative.get('risks'))+'</div></div><h3>综合说明</h3><p>'+text(narrative.get('comprehensive_summary'))+'</p><div class="conclusion"><b>一句话结论</b><p>'+text(narrative.get('one_sentence_conclusion'))+'</p></div>'
    pages=[page(1,'主体与核心指标',first),page(2,'贷款及信用卡',second,True),page(3,'担保、逾期与公共记录',third,True),page(4,'查询记录与征信指标',fourth,True),page(5,'核验与补充清单及综合结论',fifth)]
    pages += [page(i+6,'附页', '<h2>'+text(title)+'</h2>'+content,True) for i,(title,content) in enumerate(appendices)]
    return '<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>征信速览报告</title><style>'+CSS+'</style></head><body>'+''.join(pages)+'</body></html>'
