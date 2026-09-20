"""Offline A4 HTML for the frozen comprehensive financing report."""

from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any, Iterable

from backend.services.comprehensive_financing_analysis_service import ComprehensiveFinancingAnalysisResult
from backend.services.comprehensive_financing_report_model import CUSTOMER_MATERIAL_TYPES, ComprehensiveFinancingReportModel
from backend.services.comprehensive_financing_report_markdown_renderer import format_money, format_ratio


STATUS = {"confirmed": "已确认", "available": "已获取", "partial": "部分资料", "missing": "资料不足", "needs_review": "待核验"}
PATH_STATUS = {"potential": "可进一步评估", "conditional": "有条件", "insufficient_data": "资料不足"}
MATERIAL = {"enterprise_kyc": "KYC主体资料", "enterprise_credit": "企业征信", "personal_credit": "个人征信",
            "enterprise_cashflow": "企业流水", "personal_cashflow": "个人流水", "financial_statements": "财务报表",
            "assets": "资产资料", "financing_requirement": "融资需求", "enterprise_cashflow_classification": "企业流水分类",
            "financial_cashflow_period_mismatch": "财务与流水期间", "source_date_comparability": "资料时点可比性"}
SOURCE = {"subject_profile": "主体资料", "financing_requirement": "融资需求", "enterprise_credit": "企业征信",
          "personal_credit": "个人征信", "enterprise_cashflow": "企业流水", "personal_cashflow": "个人流水",
          "financials": "财务报表", "assets": "资产资料", "derived_metrics": "程序计算指标",
          "conflicts": "资料冲突核验", "data_scope": "数据范围", "data_quality": "资料完整度", "source_dates": "资料日期"}
READINESS = {"ready_for_further_evaluation": "可进入下一步评估", "conditionally_ready": "具备一定基础但存在前置条件",
             "needs_data_completion": "需补充资料", "needs_issue_resolution": "需先解决关键问题"}
ASSET = {"property": "房产", "vehicle": "车辆", "equipment": "设备", "intellectual_property": "知识产权",
         "equity": "股权", "deposit": "存单", "other_collateral": "其他抵质押物"}


def _e(value: Any) -> str:
    if value is None or value == "" or value == [] or value == {}:
        return "资料不足"
    return escape(str(value), quote=True).replace("\n", "<br>")


def _join(values: Iterable[Any], empty: str = "资料不足") -> str:
    items = [_e(value) for value in values if value not in (None, "", [], {})]
    return "、".join(items) if items else empty


def _sources(sections: list[str]) -> str:
    return _join((SOURCE[item] for item in sections if item in SOURCE), "资料来源待核验")


def _table(headers: list[str], rows: list[list[Any]], *, class_name: str = "") -> str:
    head = "".join(f"<th scope='col'>{_e(item)}</th>" for item in headers)
    body = "".join("<tr>" + "".join(f"<td>{_e(cell)}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table class='{class_name}'><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _list(items: Iterable[Any]) -> str:
    values = list(items)
    return "<ul>" + "".join(f"<li>{_e(item)}</li>" for item in values) + "</ul>" if values else "<p class='muted'>资料不足</p>"


def _analysis(title: str, section: Any) -> str:
    return f"<div class='analysis'><h4>{_e(title)}</h4><p>{_e(section.summary)}</p><small>数据来源：{_sources(section.source_sections)}</small></div>"


def _page(title: str, number: str, body: str) -> str:
    return f"<section class='report-page'><div class='section-index'>{_e(number)} · 综合融资分析</div><h2>{_e(title)}</h2>{body}</section>"


def _period(model: ComprehensiveFinancingReportModel, kind: str, material: dict[str, Any]) -> str:
    if kind in {"enterprise_cashflow", "personal_cashflow"}:
        section = model.enterprise_cashflow if kind == "enterprise_cashflow" else model.personal_cashflow
        period = section.get("statement_period") or {}
        if period.get("start") or period.get("end"):
            return f"{period.get('start') or '资料不足'} 至 {period.get('end') or '资料不足'}"
    if kind == "financial_statements" and model.financials.get("latest_period"):
        return f"最新 {model.financials['latest_period']}"
    return material.get("latest_date") or "资料不足"


def _scope(model: ComprehensiveFinancingReportModel) -> str:
    rows = [[MATERIAL.get(row.get("type"), "其他资料"), STATUS.get(row.get("status"), "待核验"),
             _period(model, row.get("type"), row)] for row in model.data_scope.get("materials", [])
            if row.get("type") in CUSTOMER_MATERIAL_TYPES]
    return _table(["资料类型", "状态", "数据期间/日期"], rows, class_name="compact")


def _core_metrics(model: ComprehensiveFinancingReportModel) -> str:
    credit, cash, financial = model.enterprise_credit, model.enterprise_cashflow, model.financials.get("latest") or {}
    people = model.personal_credit.get("people") or []
    personal_money = model.derived_metrics.get("total_personal_credit_balance_money")
    related_money = model.derived_metrics.get("total_related_repayment_balance_money")
    personal = format_money(personal_money or model.derived_metrics.get("total_personal_credit_balance"), model.personal_credit.get("unit"))
    related = format_money(related_money or model.derived_metrics.get("total_related_repayment_balance"), model.personal_credit.get("unit"))
    if len(people) == 1:
        personal = format_money(people[0].get("loan_balance_money") or people[0].get("loan_balance"), people[0].get("unit") or model.personal_credit.get("unit"))
        related = format_money(people[0].get("related_repayment_balance_money") or people[0].get("related_repayment_balance"), people[0].get("unit") or model.personal_credit.get("unit"))
    unit = financial.get("unit")
    entries = [
        ("企业征信融资余额", format_money(credit.get("outstanding_loan_balance_money") or credit.get("outstanding_loan_balance"), credit.get("unit"))),
        ("个人征信贷款余额", personal), ("法人相关还款责任", related),
        ("企业流水总流入", format_money(cash.get("total_inflow"), cash.get("unit"))),
        ("初步经营入账", format_money(cash.get("operating_inflow"), cash.get("unit"))),
        ("月均经营入账", format_money(cash.get("monthly_average_operating_inflow"), cash.get("unit"))),
        ("最新总资产", format_money(financial.get("total_assets"), unit)),
        ("最新总负债", format_money(financial.get("total_liabilities"), unit)),
        ("最新净资产", format_money(financial.get("net_assets"), unit)),
        ("资产负债率", format_ratio(financial.get("debt_asset_ratio"))),
        ("最新净利润", format_money(financial.get("net_profit"), unit)),
        ("经营现金流", format_money(financial.get("operating_cashflow"), unit)),
    ]
    return "<div class='metric-grid'>" + "".join(
        f"<div class='metric'><span>{_e(label)}</span><strong>{_e(value)}</strong></div>" for label, value in entries
    ) + "</div>"


def _subject(model: ComprehensiveFinancingReportModel) -> str:
    subject = model.subject_profile
    rows = [("企业名称", subject.get("enterprise_name")), ("统一社会信用代码", subject.get("unified_social_credit_code")),
            ("法定代表人", subject.get("legal_representative")), ("实际控制人", subject.get("actual_controller")),
            ("注册资本", subject.get("registered_capital")), ("成立时间", subject.get("established_date")),
            ("企业类型", subject.get("enterprise_type")), ("注册地址", subject.get("registered_address")),
            ("经营地址", subject.get("operating_address")), ("主营/经营范围", subject.get("business_scope"))]
    return "<div class='facts-grid'>" + "".join(f"<div><span>{_e(k)}</span><strong>{_e(v)}</strong></div>" for k, v in rows) + "</div>"


def _requirement(model: ComprehensiveFinancingReportModel) -> str:
    req = model.financing_requirement
    if req.get("status") == "missing":
        return "<p class='note'>当前尚未确认明确融资金额、用途、期限及担保偏好，本报告暂不对具体融资金额、期限或产品作确定性建议。</p>"
    rows = [[label, format_money(req.get(key), req.get("amount_unit")) if key == "amount" else req.get(key)]
            for label, key in (("融资主体", "financing_subject"), ("融资金额", "amount"), ("用途", "purpose"),
                               ("期限", "term"), ("用款时间", "expected_use_date"), ("担保偏好", "guarantee_preference"))]
    return _table(["项目", "已保存需求"], rows)


def _cashflow(model: ComprehensiveFinancingReportModel) -> str:
    cash = model.enterprise_cashflow
    period = cash.get("statement_period") or {}
    span = f"{period.get('start') or '资料不足'} 至 {period.get('end') or '资料不足'}"
    unit = cash.get("unit")
    rows = [["流水覆盖期间", span], ["账户数", cash.get("account_count")],
            ["总流入", format_money(cash.get("total_inflow"), unit)],
            ["初步经营入账", format_money(cash.get("operating_inflow"), unit)],
            ["内部互转流入", format_money(cash.get("internal_transfer_inflow"), unit)],
            ["关联方流入", format_money(cash.get("related_party_inflow"), unit)],
            ["其他非经营/未识别流入", format_money(cash.get("non_operating_inflow"), unit)],
            ["月均经营入账", format_money(cash.get("monthly_average_operating_inflow"), unit)]]
    note = "<p class='note'>当前企业流水可用于初步分析；经营入账属于已保存分类下的初步统计，关联方分类仍需核验，不代表最终核定真实经营收入。</p>" if cash.get("status") == "partial" else ""
    return _table(["指标", "当前情况"], rows) + note


def _financials(model: ComprehensiveFinancingReportModel) -> str:
    latest = model.financials.get("latest") or {}
    unit = latest.get("unit")
    keys = [("财务期间", "period"), ("报表口径", "period_type"), ("营业收入", "revenue"), ("营业成本", "operating_cost"),
            ("净利润", "net_profit"), ("总资产", "total_assets"), ("总负债", "total_liabilities"),
            ("净资产", "net_assets"), ("应收账款", "accounts_receivable"), ("存货", "inventory"),
            ("短期借款", "short_term_borrowings"), ("长期借款", "long_term_borrowings"),
            ("经营现金流", "operating_cashflow"), ("资产负债率", "debt_asset_ratio")]
    rows = []
    for label, key in keys:
        value = latest.get(key)
        if key == "period_type":
            value = {"monthly": "月度", "annual": "年度", "quarterly": "季度"}.get(value, "资料不足")
        elif key == "debt_asset_ratio":
            value = format_ratio(value)
        elif key not in {"period", "period_type"}:
            value = format_money(value, unit)
        rows.append([label, value])
    periods = model.financials.get("periods") or []
    trend = ""
    if periods:
        trend_rows = [[label] + [format_money(p.get(key), p.get("unit")) for p in periods]
                      for label, key in (("营业收入", "revenue"), ("净利润", "net_profit"), ("总资产", "total_assets"),
                                         ("总负债", "total_liabilities"), ("净资产", "net_assets"))]
        trend = "<h3>财务趋势</h3>" + _table(["指标"] + [str(p.get("period") or "资料不足") for p in periods], trend_rows, class_name="compact")
        trend += "<p class='footnote'>各期数据按原报表口径列示；月度与年度期间不直接作同比判断。</p>"
    return "<h3>核心财务指标</h3>" + _table(["指标", "当前值"], rows, class_name="two-column") + trend


def _credit(model: ComprehensiveFinancingReportModel, analysis: ComprehensiveFinancingAnalysisResult) -> str:
    credit = model.enterprise_credit
    rows = [["未结清融资余额", format_money(credit.get("outstanding_loan_balance_money") or credit.get("outstanding_loan_balance"), credit.get("unit"))],
            ["融资机构数", credit.get("outstanding_loan_institution_count")],
            ["逾期记录数", (credit.get("overdue_summary") or {}).get("count")],
            ["异常分类记录数", (credit.get("nonperforming_summary") or {}).get("count")],
            ["企业对外担保余额", format_money(credit.get("guarantee_balance_money") or credit.get("guarantee_balance"), credit.get("unit"))],
            ["企业贷款记录数", credit.get("loan_record_count")], ["报告日期", credit.get("source_report_date")]]
    content = "<h3>企业征信摘要</h3>" + _table(["项目", "当前情况"], rows)
    people_rows, queries = [], []
    for person in model.personal_credit.get("people") or []:
        unit = person.get("unit") or model.personal_credit.get("unit")
        overdue = person.get("overdue_summary") or {}
        overdue_text = f"贷款 {overdue.get('loan_overdue_account_count') if overdue.get('loan_overdue_account_count') is not None else '资料不足'}；信用卡 {overdue.get('credit_card_overdue_account_count') if overdue.get('credit_card_overdue_account_count') is not None else '资料不足'}；90天以上 {overdue.get('overdue_90d_account_count') if overdue.get('overdue_90d_account_count') is not None else '资料不足'}"
        people_rows.append([person.get("name"), _join(person.get("roles") or []),
                            format_money(person.get("loan_balance_money") or person.get("loan_balance"), unit),
                            format_money(person.get("credit_card_limit_money") or person.get("credit_card_limit"), unit),
                            format_money(person.get("credit_card_used_money") or person.get("credit_card_used"), unit),
                            overdue_text,
                            format_money(person.get("related_repayment_balance_money") or person.get("related_repayment_balance"), unit),
                            person.get("source_report_date")])
        for window in (person.get("query_summary") or {}).get("windows") or []:
            queries.append([person.get("name"), window.get("window"), window.get("loan_approval"),
                            window.get("credit_card_approval"), window.get("guarantee_review"), window.get("legal_person_review")])
    content += "<h3>法人 / 实际控制人征信摘要</h3>" + _table(
        ["姓名", "角色", "贷款余额", "信用卡额度", "信用卡已用", "逾期账户", "相关还款责任", "报告日期"], people_rows or [["资料不足"] * 8], class_name="compact")
    if queries:
        content += "<h3>征信查询窗口</h3>" + _table(["姓名", "窗口", "贷款审批", "信用卡审批", "担保资格", "法人资信"], queries, class_name="compact")
    content += _analysis("企业与个人负债联动", analysis.enterprise_person_linkage)
    content += _analysis("征信综合分析", analysis.credit_analysis)
    return content


def _assets(model: ComprehensiveFinancingReportModel, analysis: ComprehensiveFinancingAnalysisResult) -> str:
    if model.assets.get("status") == "missing":
        content = "<p class='note'>当前资料中未找到可用于本次分析的稳定结构化资产资料，暂无法判断抵押或其他资产增信能力。</p>"
    else:
        rows = [[label, item.get("name"), format_money(item.get("market_value"), item.get("unit")), item.get("value_basis")]
                for key, label in ASSET.items() for item in model.assets.get(key) or []]
        content = _table(["资产类型", "资产名称", "参考价值", "价值依据"], rows) if rows else "<p class='muted'>资料不足</p>"
    return content + _analysis("资产与增信分析", analysis.asset_and_enhancement_analysis)


def _strengths_constraints(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    strengths = "".join(f"<article class='item-card strength'><h4>{_e(item.title)}</h4><p><b>事实依据：</b>{_e(item.fact)}</p><p><b>融资意义：</b>{_e(item.impact)}</p><small>数据来源：{_sources(item.source_sections)}</small></article>" for item in analysis.financing_strengths)
    constraints = "".join(f"<article class='item-card constraint'><h4>{_e(item.title)}</h4><p><b>当前事实：</b>{_e(item.fact)}</p><p><b>融资影响：</b>{_e(item.impact)}</p><p><b>建议动作：</b>{_e(item.required_action)}</p><small>数据来源：{_sources(item.source_sections)}</small></article>" for item in analysis.financing_constraints)
    issues = "".join(f"<article class='item-card issue'><h4><span class='issue-no'>{i:02d}</span>{_e(item.issue)}</h4><p><b>事实：</b>{_join(item.facts)}</p><p><b>融资影响：</b>{_e(item.financing_impact)}</p><p><b>下一步：</b>{_e(item.next_action)}</p></article>" for i, item in enumerate(analysis.core_issues[:5], 1))
    empty = "<p class='muted'>资料不足</p>"
    return f"<h3>五、融资优势</h3>{strengths or empty}<h3>六、融资障碍</h3>{constraints or empty}<h3>七、当前核心问题</h3>{issues or empty}"


def _paths_actions(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    path_tone = {"potential": "path-green", "conditional": "path-amber", "insufficient_data": "path-gray"}
    rows = "".join(
        f"<tr><td>{_e(item.path)}</td><td><span class='path-status {path_tone.get(item.status, 'path-gray')}'>{_e(PATH_STATUS.get(item.status, '待核验'))}</span></td>"
        f"<td>{_join(item.basis)}</td><td>{_join(item.missing_conditions)}</td></tr>"
        for item in analysis.financing_paths
    )
    content = "<h3>八、融资路径方向</h3><table><thead><tr><th>融资路径</th><th>当前状态</th><th>依据</th><th>当前缺口</th></tr></thead><tbody>" + rows + "</tbody></table>"
    content += "<h3>九、行动计划</h3>"
    for title, actions in (("立即处理", analysis.action_plan.immediate), ("短期处理", analysis.action_plan.short_term),
                           ("中期优化", analysis.action_plan.medium_term)):
        if actions:
            content += f"<h4 class='action-heading'>{_e(title)}</h4>"
            content += "".join(f"<div class='action'><strong>{_e(item.action)}</strong><span>依据：{_e(item.basis)}</span><small>数据来源：{_sources(item.source_sections)}</small></div>" for item in actions)
    return content


def _limitations_conclusion(analysis: ComprehensiveFinancingAnalysisResult) -> str:
    allowed = set(CUSTOMER_MATERIAL_TYPES) | {"financial_cashflow_period_mismatch", "enterprise_cashflow_classification", "source_date_comparability"}
    rows = [[MATERIAL.get(item.material_type, "其他资料"), item.limitation, item.impact, item.required_data]
            for item in analysis.data_limitations if item.material_type in allowed]
    content = "<h3>十、资料缺口与分析限制</h3>" + (_table(["资料类型", "当前限制", "分析影响", "需要补充/核验"], rows) if rows else "<p class='muted'>未列出额外资料限制。</p>")
    conclusion = analysis.conclusion
    content += "<h3>十一、综合结论</h3>"
    content += f"<div class='analysis'><h4>综合判断</h4><p>{_e(conclusion.overall)}</p></div>"
    content += f"<div class='analysis'><h4>当前融资方向</h4><p>{_e(conclusion.financing_direction)}</p></div>"
    content += "<h4>前置条件</h4>" + _list(conclusion.prerequisites)
    content += f"<div class='final-line'><span>一句话结论</span><strong>{_e(conclusion.one_sentence)}</strong></div>"
    return content


CSS = """
@page { size: A4; margin: 12mm; }
* { box-sizing: border-box; }
html { color: #253444; background: #fff; }
body { margin: 0; font-family: 'Microsoft YaHei','PingFang SC','Noto Sans CJK SC','Source Han Sans SC',sans-serif; font-size: 11.5px; line-height: 1.58; }
.report { max-width: 186mm; margin: 0 auto; }
.report-page { break-before: page; }
.report-page:first-child { break-before: auto; }
.eyebrow,.section-index { color: #66809a; font-size: 9px; font-weight: 700; letter-spacing: .12em; text-transform: uppercase; }
.cover-head { border-top: 5px solid #183650; padding-top: 17px; margin-bottom: 18px; }
.cover-page .cover-head { padding-top: 10px; margin-bottom: 9px; }
.cover-page h3 { margin: 10px 0 5px; }
.cover-page .summary-grid > div { padding: 6px 8px; }
.cover-page .summary-grid ul { margin-bottom: 4px; }
.cover-page .summary-grid li { margin: 1px 0; }
.cover-page .metric-grid { gap: 4px; }
.cover-page .metric { padding: 4px 7px; min-height: 35px; }
.cover-page .metric strong { font-size: 12.5px; }
h1 { color:#183650; font-size: 27px; line-height: 1.3; margin: 8px 0 11px; }
h2 { color:#183650; font-size: 19px; line-height: 1.35; margin: 7px 0 14px; padding-bottom: 7px; border-bottom: 1px solid #b9c9d5; break-after: avoid; }
h3 { color:#29465f; font-size: 14px; margin: 20px 0 8px; break-after: avoid; }
h4 { color:#29465f; font-size: 12px; margin: 10px 0 5px; break-after: avoid; }
p { margin: 4px 0 8px; }
small,.muted,.footnote { color:#67798b; font-size: 10px; }
.meta { color:#526c83; display:flex; gap:20px; }
.meta strong { color:#183650; }
.note { padding: 9px 11px; background:#fbf6e9; border-left: 3px solid #b7954b; color:#5b503b; break-inside: avoid; }
.summary-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
.summary-grid > div { background:#f2f6f9; padding:9px 11px; break-inside:avoid; }
.summary-grid strong { display:block; color:#29465f; margin-bottom:3px; }
.metric-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:6px; }
.metric { background:#f4f7f9; border:1px solid #dbe4eb; padding:8px 10px; min-height:49px; break-inside:avoid; }
.metric span { display:block; color:#65798b; font-size:10px; }
.metric strong { display:block; color:#183650; font-size:14px; font-weight:650; overflow-wrap:anywhere; }
.facts-grid { display:grid; grid-template-columns:1fr 1fr; gap:0 14px; border-top:1px solid #d8e2e9; }
.facts-grid > div { display:flex; gap:8px; padding:5px 0; border-bottom:1px solid #e5ebef; break-inside:avoid; }
.facts-grid span { width:95px; flex:none; color:#65798b; }
.facts-grid strong { font-weight:500; overflow-wrap:anywhere; }
table { width:100%; border-collapse:collapse; table-layout:fixed; margin:5px 0 12px; font-size:10px; }
thead { display:table-header-group; }
tr { break-inside:avoid; }
th,td { border-bottom:1px solid #dce5eb; padding:5px 6px; vertical-align:top; text-align:left; overflow-wrap:anywhere; word-break:break-word; }
th { background:#eaf0f4; color:#29465f; font-weight:700; }
tbody tr:nth-child(even) { background:#f9fbfc; }
.compact { font-size:9px; }
.compact th,.compact td { padding:4px 5px; }
.two-column { width:76%; }
.two-column td:last-child { text-align:right; }
.analysis { border-left:3px solid #a9bfce; padding:6px 10px; margin:9px 0; background:#f7f9fb; break-inside:avoid; }
.analysis h4 { margin-top:0; }
.item-card { border:1px solid #dce5eb; border-left:3px solid #8ca6ba; padding:8px 11px; margin:7px 0; break-inside:avoid; }
.item-card p { margin:3px 0; }
.strength { border-left-color:#65937e; }
.constraint { border-left-color:#b79a5c; }
.issue-no { color:#718b9e; margin-right:7px; }
.path-status { display:inline-block; padding:1px 6px; border-radius:2px; white-space:nowrap; }
.path-green { color:#386950; background:#e7f0e9; }
.path-amber { color:#79622c; background:#f7f0dd; }
.path-gray { color:#66717c; background:#edf0f2; }
.action-heading { border-bottom:1px solid #dbe4eb; padding-bottom:4px; }
.action { display:flex; flex-direction:column; border-left:2px solid #b7c8d4; padding:4px 10px; margin:6px 0; break-inside:avoid; }
.final-line { background:#ecf3f6; border-left:4px solid #294e6a; padding:12px 14px; margin-top:14px; break-inside:avoid; }
.final-line span { display:block; color:#65798b; font-size:9px; }
.final-line strong { display:block; color:#183650; font-size:12px; }
ul { margin:4px 0 10px; padding-left:18px; }
li { margin:3px 0; }
@media screen { body { background:#e8edf1; padding:18px; } .report-page { background:#fff; padding:15mm 12mm; margin:0 auto 14px; box-shadow:0 4px 18px #1d344022; width:210mm; } .report { max-width:none; } }
"""


def render_comprehensive_financing_report_html(
    report_model: ComprehensiveFinancingReportModel,
    analysis_result: ComprehensiveFinancingAnalysisResult,
    generated_at: str | datetime,
) -> str:
    """Deterministically render the two frozen structured objects; never fetch data or call an LLM."""
    date_text = generated_at.isoformat(sep=" ", timespec="seconds") if isinstance(generated_at, datetime) else str(generated_at)
    customer = report_model.subject_profile.get("enterprise_name") or report_model.customer.get("name")
    summary = analysis_result.executive_summary
    intro = f"<div class='cover-head'><div class='eyebrow'>FINANCING ANALYSIS REPORT</div><h1>客户综合融资分析报告</h1><div class='meta'><div>企业客户：<strong>{_e(customer)}</strong></div><div>报告生成时间：<strong>{_e(date_text)}</strong></div></div></div>"
    if analysis_result.validation_fallback_used:
        intro += "<p class='note'>本次部分综合判断采用保守口径，建议结合补充资料进一步核验。</p>"
    intro += "<h3>数据范围</h3>" + _scope(report_model)
    intro += "<h3>综合摘要</h3>" + f"<p>{_e(summary.overall_observation)}</p><p><b>当前融资准备状态：</b>{_e(READINESS.get(summary.current_financing_readiness, '待核验'))}</p>"
    intro += f"<div class='summary-grid'><div><strong>融资优势摘要</strong>{_list(summary.main_strengths)}</div><div><strong>主要融资约束</strong>{_list(summary.main_constraints)}</div></div>"
    intro += "<h3>核心融资指标</h3>" + _core_metrics(report_model)
    pages = [f"<section class='report-page cover-page'>{intro}</section>"]
    pages.append(_page("客户主体与企业流水", "01", "<h3>一、客户融资画像</h3><h4>主体基本信息</h4>" + _subject(report_model)
                       + "<h4>当前融资需求</h4>" + _requirement(report_model)
                       + "<h3>二、企业经营与流水分析</h3>" + _cashflow(report_model)
                       + _analysis("企业经营分析", analysis_result.business_analysis)
                       + _analysis("企业流水分析", analysis_result.cashflow_analysis)))
    pages.append(_page("财务分析", "02", "<h3>三、财务分析</h3>" + _financials(report_model) + _analysis("财务分析结论", analysis_result.financial_analysis)))
    pages.append(_page("征信与负债", "03", "<h3>四、征信与负债分析</h3>" + _credit(report_model, analysis_result)
                       + "<h3>五、资产与增信条件</h3>" + _assets(report_model, analysis_result)))
    pages.append(_page("融资优势、障碍与核心问题", "04", _strengths_constraints(analysis_result)))
    pages.append(_page("融资路径与行动计划", "05", _paths_actions(analysis_result)))
    pages.append(_page("资料限制与综合结论", "06", _limitations_conclusion(analysis_result)))
    return "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>客户综合融资分析报告</title><style>" + CSS + "</style></head><body><main class='report'>" + "".join(pages) + "</main></body></html>"
