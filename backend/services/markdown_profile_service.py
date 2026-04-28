"""Customer markdown profile generation and persistence helpers."""

from __future__ import annotations

import json
import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.document_types import get_document_display_name, should_store_original
from .local_storage_service import DEFAULT_RAG_SOURCE_PRIORITY

logger = logging.getLogger(__name__)

RISK_REPORT_SCHEMA_TEMPLATE: dict[str, Any] = {
    "customer_summary": {
        "customer_id": "",
        "customer_name": "",
        "customer_type": "",
        "industry": "",
        "financing_need": "",
        "data_completeness": {"status": "", "score": 0, "missing_items": []},
    },
    "overall_assessment": {
        "total_score": 0,
        "risk_level": "high",
        "conclusion": "",
        "immediate_application_recommended": False,
        "basis": [],
    },
    "risk_dimensions": [],
    "matched_schemes": {"has_match": False, "items": []},
    "no_match_analysis": {"has_no_match_issue": False, "reasons": [], "core_shortboards": [], "basis": []},
    "optimization_suggestions": {
        "short_term": [],
        "mid_term": [],
        "document_supplement": [],
        "credit_optimization": [],
        "debt_optimization": [],
    },
    "financing_plan": {"current_stage": "", "one_to_three_months": [], "three_to_six_months": [], "alternative_paths": []},
    "final_recommendation": {"action": "", "priority_product_types": [], "next_steps": [], "basis": []},
}

AMOUNT_FIELDS = {
    'total_income', 'total_expense', 'monthly_avg_income', 'monthly_avg_expense',
    'opening_balance', 'closing_balance',
}

COUNT_FIELDS = {'transaction_count'}

STRUCTURED_FIELD_LABELS: dict[str, str] = {
    'name': '\u59d3\u540d',
    'gender': '\u6027\u522b',
    'ethnicity': '\u6c11\u65cf',
    'birth_date': '\u51fa\u751f\u65e5\u671f',
    'id_number': '\u8eab\u4efd\u8bc1\u53f7\u7801',
    'issuing_authority': '\u7b7e\u53d1\u673a\u5173',
    'valid_period': '\u6709\u6548\u671f\u9650',
    'side': '\u8bc6\u522b\u9762',
    'completeness_hint': '\u5b8c\u6574\u6027\u63d0\u793a',
    'household_head_name': '户主姓名',
    'household_number': '户号',
    'household_type': '户别',
    'household_address': '户籍地址',
    'members': '家庭成员',
    'completeness_note': '完整性提示',
    'account_name': '\u8d26\u6237\u540d\u79f0',
    'account_number': '\u8d26\u53f7',
    'bank_name': '\u94f6\u884c\u540d\u79f0',
    'bank_branch': '\u5f00\u6237\u652f\u884c',
    'license_number': '\u6838\u51c6\u53f7',
    'basic_deposit_account_number': '\u57fa\u672c\u5b58\u6b3e\u8d26\u6237\u7f16\u53f7',
    'account_type': '\u8d26\u6237\u6027\u8d28',
    'open_date': '\u5f00\u6237\u65e5\u671f',
    'company_name': '\u516c\u53f8\u540d\u79f0',
    'credit_code': '\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801',
    'certificate_number': '\u8bc1\u7167\u7f16\u53f7',
    'real_estate_certificate_no': '\u4e0d\u52a8\u4ea7\u6743\u53f7',
    'right_holder': '\u6743\u5229\u4eba',
    'ownership_status': '\u5171\u6709\u60c5\u51b5',
    'property_location': '\u5750\u843d',
    'real_estate_unit_no': '\u4e0d\u52a8\u4ea7\u5355\u5143\u53f7',
    'right_type': '\u6743\u5229\u7c7b\u578b',
    'right_nature': '\u6743\u5229\u6027\u8d28',
    'usage': '\u7528\u9014',
    'land_area': '\u571f\u5730\u9762\u79ef',
    'building_area': '\u5efa\u7b51\u9762\u79ef',
    'land_use_term': '\u4f7f\u7528\u671f\u9650',
    'other_rights_info': '\u6743\u5229\u5176\u4ed6\u72b6\u51b5',
    'legal_person': '\u6cd5\u5b9a\u4ee3\u8868\u4eba',
    'executive_director': '\u6267\u884c\u8463\u4e8b',
    'chairman': '\u8463\u4e8b\u957f',
    'manager': '\u7ecf\u7406',
    'supervisor': '\u76d1\u4e8b',
    'registered_capital': '\u6ce8\u518c\u8d44\u672c',
    'establish_date': '\u6210\u7acb\u65e5\u671f',
    'business_scope': '\u7ecf\u8425\u8303\u56f4',
    'address': '\u5730\u5740',
    'company_type': '\u7c7b\u578b',
    'equity_structure_summary': '\u80a1\u6743\u7ed3\u6784',
    'equity_ratios': '\u80a1\u6743\u5360\u6bd4',
    'voting_rights_basis': '\u8868\u51b3\u6743\u4f9d\u636e',
    'financing_approval_rule': '\u878d\u8d44/\u8d37\u6b3e\u5ba1\u6279\u89c4\u5219',
    'financing_approval_threshold': '\u878d\u8d44\u8868\u51b3\u95e8\u69db',
    'major_decision_rules': '\u91cd\u5927\u4e8b\u9879\u89c4\u5219',
    'control_analysis': '\u63a7\u5236\u6743\u5206\u6790',
    'registration_authority': '\u767b\u8bb0\u673a\u5173',
    'registration_date': '\u767b\u8bb0\u673a\u5173\u65e5\u671f',
    'document_type_name': '\u8d44\u6599\u7c7b\u578b',
    'storage_label': '\u8d44\u6599\u5f52\u7c7b',
    'currency': '\u5e01\u79cd',
    'start_date': '\u5f00\u59cb\u65e5\u671f',
    'end_date': '\u7ed3\u675f\u65e5\u671f',
    'opening_balance': '\u671f\u521d\u4f59\u989d',
    'closing_balance': '\u671f\u672b\u4f59\u989d',
    'total_income': '\u603b\u6536\u5165',
    'total_expense': '\u603b\u652f\u51fa',
    'transaction_count': '\u4ea4\u6613\u7b14\u6570',
    'monthly_avg_income': '\u6708\u5747\u6536\u5165',
    'monthly_avg_expense': '\u6708\u5747\u652f\u51fa',
    'top_inflows': '\u5927\u989d\u6d41\u5165',
    'top_outflows': '\u5927\u989d\u6d41\u51fa',
    'top_transactions': '\u5927\u989d\u4ea4\u6613',
    'frequent_counterparties': '\u9ad8\u9891\u5bf9\u624b\u65b9',
    'abnormal_summary': '\u5f02\u5e38\u6458\u8981',
    'summary': '\u6458\u8981',
    'shareholders': '\u80a1\u4e1c\u4fe1\u606f',
    'shareholder_count': '\u80a1\u4e1c\u6570\u91cf',
    'management_structure': '\u6cbb\u7406\u7ed3\u6784',
    'management_roles_summary': '\u4efb\u804c\u4fe1\u606f\u6458\u8981',
    'major_decision_rule_details': '\u91cd\u5927\u4e8b\u9879\u89c4\u5219\u660e\u7ec6',
    'source_type': '\u6765\u6e90\u7c7b\u578b',
}

HIDDEN_STRUCTURED_FIELDS = {
    'document_type_code',
    'document_type_name',
    'storage_label',
    'source_type',
    'source_type_name',
    'management_role_evidence_lines',
    'raw_text',
    'raw_pages',
}
OPTIONAL_COMPANY_ARTICLES_FIELDS = {
    'executive_director',
    'chairman',
    'manager',
    'supervisor',
    'management_roles_summary',
}


def get_risk_report_schema_template() -> dict[str, Any]:
    return json.loads(json.dumps(RISK_REPORT_SCHEMA_TEMPLATE, ensure_ascii=False))


def get_rag_source_priority() -> list[str]:
    return list(DEFAULT_RAG_SOURCE_PRIORITY)


def _format_customer_type(customer_type: Any) -> str:
    value = str(customer_type or '').strip().lower()
    if value == 'personal':
        return '\u4e2a\u4eba'
    return '\u4f01\u4e1a'


def _markdown_section(title: str, lines: list[str]) -> str:
    body = '\n'.join(line for line in lines if line.strip()) or '- \u6682\u65e0\u6570\u636e'
    return f'## {title}\n{body}'


def _format_amount_for_markdown(value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        return '\u6682\u65e0'
    raw = text.replace(',', '').replace('\u5143', '').strip()
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError):
        return text if text.endswith('\u5143') else f'{text} \u5143'
    return f'{amount:,.2f} \u5143'


def _format_list_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\u65e0'
    return json.dumps(value, ensure_ascii=False, indent=2)


def _format_company_articles_shareholders_v2(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    lines: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            text = str(item).strip()
            if text:
                lines.append(f"  - {text}")
            continue
        parts = [
            str(item.get('name') or '').strip(),
            str(item.get('capital_contribution') or '').strip(),
            str(item.get('contribution_method') or '').strip(),
            str(item.get('contribution_date') or '').strip(),
            str(item.get('equity_ratio') or '').strip(),
            str(item.get('voting_ratio') or '').strip(),
        ]
        parts = [part for part in parts if part]
        if parts:
            lines.append(f"  - {' | '.join(parts)}")
    return '\n' + '\n'.join(lines) if lines else '\u6682\u65e0'

def _format_company_articles_rule_list_v2(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            matter = str(item.get('matter') or item.get('topic') or '').strip()
            approval_rule = str(item.get('approval_rule') or item.get('rule') or '').strip()
            threshold = str(item.get('threshold') or '').strip()
            text = f"{matter}: {approval_rule}" if matter and approval_rule else matter or approval_rule
            if threshold:
                text = f"{text} ({threshold})" if text else threshold
            if text:
                lines.append(f"  - {text}")
            continue
        text = str(item).strip()
        if text:
            lines.append(f"  - {text}")
    return '\n' + '\n'.join(lines) if lines else '\u6682\u65e0'


def _format_hukou_members_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\n- 暂未识别到成员信息'
    header = [
        '| 姓名 | 与户主关系 | 性别 | 民族 | 出生日期 | 身份证号码 | 婚姻状况 |',
        '|---|---|---|---|---|---|---|',
    ]
    rows: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        rows.append(
            '| '
            + ' | '.join(
                [
                    str(item.get('name') or '').strip() or '暂无',
                    str(item.get('relationship_to_head') or '').strip() or '暂无',
                    str(item.get('gender') or '').strip() or '暂无',
                    str(item.get('ethnicity') or '').strip() or '暂无',
                    str(item.get('birth_date') or '').strip() or '暂无',
                    str(item.get('id_number') or '').strip() or '暂无',
                    str(item.get('marital_status') or '').strip() or '暂无',
                ]
            )
            + ' |'
        )
    if not rows:
        return '\n- 暂未识别到成员信息'
    return '\n### 家庭成员\n' + '\n'.join(header + rows)

def _format_raw_pages_for_markdown(value: list[Any]) -> str:
    if not value:
        return ''
    lines = ['### PDF原文识别内容']
    has_page = False
    for item in value:
        if not isinstance(item, dict):
            continue
        page_number = item.get('page')
        page_text = str(item.get('text') or '').strip()
        if not page_text:
            continue
        has_page = True
        lines.append(f'#### 第 {page_number} 页')
        lines.append('```text')
        lines.append(page_text)
        lines.append('```')
    return '\n' + '\n'.join(lines) if has_page else ''


def _format_hukou_members_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\n- 暂未识别到成员信息'
    header = [
        '| 姓名 | 与户主关系 | 性别 | 民族 | 出生日期 | 身份证号码 | 婚姻状况 |',
        '|---|---|---|---|---|---|---|',
    ]
    rows: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or '').strip()
        id_number = str(item.get('id_number') or '').strip()
        if not name and not id_number:
            continue
        rows.append(
            '| '
            + ' | '.join(
                [
                    name,
                    str(item.get('relationship_to_head') or '').strip(),
                    str(item.get('gender') or '').strip(),
                    str(item.get('ethnicity') or '').strip(),
                    str(item.get('birth_date') or '').strip(),
                    id_number,
                    str(item.get('marital_status') or '').strip(),
                ]
            )
            + ' |'
        )
    if not rows:
        return '\n- 暂未识别到成员信息'
    return '\n### 家庭成员\n' + '\n'.join(header + rows)


def _format_shareholders_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    lines: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            lines.append(f"  - {item}")
            continue
        parts = [
            str(item.get('name') or '').strip(),
            str(item.get('capital_contribution') or '').strip(),
            str(item.get('contribution_method') or '').strip(),
            str(item.get('contribution_date') or '').strip(),
            str(item.get('equity_ratio') or '').strip(),
        ]
        parts = [part for part in parts if part]
        if parts:
            lines.append(f"  - {' | '.join(parts)}")
    return '\n' + '\n'.join(lines) if lines else '\u6682\u65e0'

def _format_equity_ratios_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    parts: list[str] = []
    for item in value:
        if isinstance(item, dict):
            name = str(item.get('name') or '').strip()
            ratio = str(item.get('equity_ratio') or '').strip()
            if name and ratio:
                parts.append(f"{name} {ratio}")
        elif item:
            parts.append(str(item))
    return '; '.join(parts) if parts else '\u6682\u65e0'


def _format_hukou_members_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\n### 家庭成员\n- 暂未识别到成员信息'
    header = [
        '### 家庭成员',
        '',
        '| 序号 | 姓名 | 与户主关系 | 性别 | 民族 | 出生日期 | 身份证号码 | 婚姻状况 |',
        '|---:|---|---|---|---|---|---|---|',
    ]
    rows: list[str] = []
    index = 0
    for item in value:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or '').strip()
        relation = str(item.get('relationship_to_head') or '').strip()
        gender = str(item.get('gender') or '').strip()
        ethnicity = str(item.get('ethnicity') or '').strip()
        birth_date = str(item.get('birth_date') or '').strip()
        id_number = str(item.get('id_number') or '').strip()
        marital_status = str(item.get('marital_status') or '').strip()
        if not name and not id_number:
            continue
        index += 1
        cells = [
            str(index),
            name or '-',
            relation or '-',
            gender or '-',
            ethnicity or '-',
            birth_date or '-',
            id_number or '-',
            marital_status or '-',
        ]
        rows.append(f"| {' | '.join(cells)} |")
    if not rows:
        return '\n### 家庭成员\n- 暂未识别到成员信息'
    return '\n' + '\n'.join(header + rows)

def _format_rule_list_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    lines = [f"  - {str(item).strip()}" for item in value if str(item).strip()]
    return '\n' + '\n'.join(lines) if lines else '\u6682\u65e0'


def _format_rule_detail_list_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\u6682\u65e0'
    lines: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            text = str(item).strip()
            if text:
                lines.append(f"  - {text}")
            continue
        topic = str(item.get('topic') or '').strip()
        rule = str(item.get('rule') or '').strip()
        threshold = str(item.get('threshold') or '').strip()
        parts = [part for part in (topic, rule) if part]
        text = '｜'.join(parts)
        if threshold:
            text = f"{text}｜门槛：{threshold}" if text else f"门槛：{threshold}"
        if text:
            lines.append(f"  - {text}")
    return '\n' + '\n'.join(lines) if lines else '\u6682\u65e0'


def _format_hukou_members_for_markdown(value: list[Any]) -> str:
    if not value:
        return '\n### 家庭成员\n- 暂未识别到成员信息'
    header = [
        '### 家庭成员',
        '',
        '| 序号 | 姓名 | 与户主关系 | 性别 | 民族 | 出生日期 | 身份证号码 | 婚姻状况 |',
        '|---:|---|---|---|---|---|---|---|',
    ]
    rows: list[str] = []
    for index, item in enumerate([item for item in value if isinstance(item, dict)], start=1):
        name = str(item.get('name') or '').strip()
        relation = str(item.get('relationship_to_head') or '').strip()
        gender = str(item.get('gender') or '').strip()
        ethnicity = str(item.get('ethnicity') or '').strip()
        birth_date = str(item.get('birth_date') or '').strip()
        id_number = str(item.get('id_number') or '').strip()
        marital_status = str(item.get('marital_status') or '').strip()
        if not name and not id_number:
            continue
        cells = [
            str(len(rows) + 1),
            name or '-',
            relation or '-',
            gender or '-',
            ethnicity or '-',
            birth_date or '-',
            id_number or '-',
            marital_status or '-',
        ]
        rows.append(f"| {' | '.join(cells)} |")
    if not rows:
        return '\n### 家庭成员\n- 暂未识别到成员信息'
    return '\n' + '\n'.join(header + rows)


def _format_count_for_markdown(value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        return '\u6682\u65e0'
    digits = ''.join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return f'{digits} \u7b14'


def _is_invalid_legal_person_value(value: Any) -> bool:
    text = str(value or '').strip()
    if not text:
        return True
    if text == '公司法':
        return True
    invalid_values = {
        '姓名或者名称',
        '姓名或名称',
        '姓名名称',
        '信息',
        '资料',
        '说明',
        '无',
        '暂无',
        '待定',
        '空白',
        '填写',
        '填报',
        '填入',
        '未填写',
        '未填报',
        '未填入',
          '职务',
          '董事',
          '报酬',
          '及其报酬',
          '其报酬',
          '公司类型',
          '公司股东',
          '决定聘任',
          '印章',
          '用章',
          '动用',
          '使用',
          '制度',
          '印鉴',
          '利润',
          '分配',
          '亏损',
          '利润分配',
          '弥补亏损',
          '委托',
          '受托',
          '国家',
          '机关',
          '授权',
          '报告',
          '通知',
          '通知书',
          '材料',
          '文件',
          '目录',
          '附件',
          '法规',
          '法律',
          '条例',
          '立本',
          '签字',
        '签章',
        '盖章',
        '姓名',
        '名称',
        '股东',
        '法定代表人',
        '的法定代表人',
        '执行董事',
        '的执行董事',
        '董事长',
        '的董事长',
        '负责人',
        '的负责人',
        '事)担任。',
    }
    if text in invalid_values:
        return True
    if any(title_fragment in text for title_fragment in ('法定代表', '执行董事', '董事长', '负责人')):
        return True
    if any(fragment in text for fragment in ('职务', '报酬', '董事', '监事会', '制度', '印章', '用章', '动用', '使用', '印鉴', '利润', '分配', '亏损', '收益', '财务', '会计', '清算', '章程', '事项', '委托', '受托', '国家', '机关', '授权', '报告', '通知', '材料', '文件', '目录', '附件', '立本', '法规', '法律', '条例')):
        return True
    if text.startswith('的') and any(title in text for title in ('法定代表人', '执行董事', '董事长', '负责人')):
        return True
    invalid_fragments = ('担任', '组成', '任命', '选举', '产生', '负责', '行使', '职权', '为公司')
    if any(fragment in text for fragment in invalid_fragments):
        return True
    if any(keyword in text for keyword in ('姓名或者名称', '姓名或名称', '股东姓名', '股东名称', '出资方式', '出资额', '出资日期')):
        return True
    return False


def _is_invalid_management_role_value(value: Any) -> bool:
    return _is_invalid_legal_person_value(value)


def _format_value(key: str, value: Any) -> str:
    if value is None or value == '':
        return '\u6682\u65e0'
    if isinstance(value, list):
        if key == 'members':
            return _format_hukou_members_for_markdown(value)
        if key == 'shareholders':
            return _format_company_articles_shareholders_v2(value)
        if key == 'equity_ratios':
            return _format_equity_ratios_for_markdown(value)
        if key == 'major_decision_rules':
            return _format_company_articles_rule_list_v2(value)
        if key == 'major_decision_rule_details':
            return _format_rule_detail_list_for_markdown(value)
        return _format_list_for_markdown(value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    if key in AMOUNT_FIELDS:
        return _format_amount_for_markdown(value)
    if key in COUNT_FIELDS:
        return _format_count_for_markdown(value)
    if key == 'side':
        side_map = {
            'front': '\u6b63\u9762',
            'back': '\u53cd\u9762',
            'both': '\u6b63\u53cd\u9762',
            'unknown': '\u672a\u77e5',
        }
        return side_map.get(str(value or '').strip().lower(), str(value))
    if key in {'legal_person', 'executive_director', 'chairman', 'manager', 'supervisor'} and _is_invalid_management_role_value(value):
        return '\u6682\u65e0'
    return str(value)


def _format_field_label(key: str) -> str:
    normalized = str(key or '').strip()
    if not normalized:
        return '\u672a\u547d\u540d\u5b57\u6bb5'
    return STRUCTURED_FIELD_LABELS.get(normalized, normalized.replace('_', ' ').strip())


async def _build_document_sections(storage_service: Any, customer_id: str) -> tuple[list[str], list[dict[str, Any]]]:
    extractions = await storage_service.get_extractions_by_customer(customer_id)
    sections: list[str] = []
    source_documents: list[dict[str, Any]] = []
    id_card_extractions: list[dict[str, Any]] = []
    other_extractions = extractions

    if id_card_extractions:
        try:
            merged_data: dict[str, Any] = {}
            merged_file_names: list[str] = []
            has_front = False
            has_back = False
            any_original_available = False

            for extraction in id_card_extractions:
                extracted_data = (extraction.get('extracted_data') or {}) if isinstance(extraction.get('extracted_data'), dict) else {}
                for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address', 'issuing_authority', 'valid_period'):
                    if not merged_data.get(key):
                        merged_data[key] = extracted_data.get(key) or ''
                if any(extracted_data.get(key) for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address')):
                    has_front = True
                if any(extracted_data.get(key) for key in ('issuing_authority', 'valid_period')):
                    has_back = True

                document = None
                doc_id = extraction.get('doc_id')
                if doc_id:
                    try:
                        document = await storage_service.get_document(doc_id)
                    except Exception as exc:
                        logger.warning("profile_markdown id_card_document_meta_failed customer_id=%s doc_id=%s error=%s", customer_id, doc_id, exc)
                file_name = (document or {}).get('file_name') or '\u6682\u65e0'
                file_path = (document or {}).get('file_path') or ''
                merged_file_names.append(file_name)
                any_original_available = any_original_available or bool(file_path)
                source_documents.append({
                    'source_type': 'id_card',
                    'source_type_name': get_document_display_name('id_card'),
                    'extraction_id': extraction.get('extraction_id'),
                    'doc_id': doc_id,
                    'file_name': file_name,
                    'original_status': '\u53ef\u67e5\u770b' if file_path else '\u539f\u4ef6\u6587\u4ef6\u4e0d\u5b58\u5728\u6216\u5df2\u4e0d\u53ef\u7528',
                    'original_available': bool(file_path),
                })

            if has_front and has_back:
                merged_data['side'] = 'both'
                merged_data['completeness_hint'] = '\u5df2\u8bc6\u522b\u6b63\u53cd\u9762'
            elif has_front:
                merged_data['side'] = 'front'
                merged_data['completeness_hint'] = '\u5df2\u8bc6\u522b\u6b63\u9762\uff0c\u7f3a\u5c11\u53cd\u9762\u4fe1\u606f\uff08\u7b7e\u53d1\u673a\u5173\u3001\u6709\u6548\u671f\u9650\uff09'
            elif has_back:
                merged_data['side'] = 'back'
                merged_data['completeness_hint'] = '\u5df2\u8bc6\u522b\u53cd\u9762\uff0c\u7f3a\u5c11\u6b63\u9762\u4fe1\u606f\uff08\u59d3\u540d\u3001\u8eab\u4efd\u8bc1\u53f7\u7801\u3001\u4f4f\u5740\uff09'
            else:
                merged_data['side'] = 'unknown'
                merged_data['completeness_hint'] = '\u672a\u8bc6\u522b\u5230\u8eab\u4efd\u8bc1\u6b63\u53cd\u9762\u5173\u952e\u4fe1\u606f'

            merged_file_name_text = '、'.join(dict.fromkeys(merged_file_names)) if merged_file_names else '暂无'
            merged_original_status = '可查看' if any_original_available else '原件文件不存在或已不可用'
            id_card_lines = [
                f"- \u8d44\u6599\u7c7b\u578b\uff1a{get_document_display_name('id_card')}",
                f"- \u6765\u6e90\u6587\u4ef6\uff1a{merged_file_name_text}",
                f"- \u539f\u4ef6\u72b6\u6001\uff1a{merged_original_status}",
            ]
            for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address', 'issuing_authority', 'valid_period', 'side', 'completeness_hint'):
                id_card_lines.append(f"- {_format_field_label(key)}\uff1a{_format_value(key, merged_data.get(key))}")
            id_card_name = str(merged_data.get('name') or '').strip()
            id_card_title = f"{get_document_display_name('id_card')}（{id_card_name}）" if id_card_name else get_document_display_name('id_card')
            sections.append(_markdown_section(id_card_title, id_card_lines))
        except Exception as exc:
            logger.warning("profile_markdown id_card_section_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
            sections.append(
                _markdown_section(
                    get_document_display_name('id_card'),
                    [
                        f"- \u8d44\u6599\u7c7b\u578b\uff1a{get_document_display_name('id_card')}",
                        '- \u63d0\u793a\uff1a\u8eab\u4efd\u8bc1\u8d44\u6599\u6574\u7406\u5931\u8d25\uff0c\u8bf7\u91cd\u65b0\u4e0a\u4f20\u6216\u68c0\u67e5\u539f\u4ef6\u3002',
                    ],
                )
            )

    for extraction in other_extractions:
        extraction_id = extraction.get('extraction_id') or ''
        extraction_type = extraction.get('extraction_type') or '\u672a\u547d\u540d\u8d44\u6599'
        try:
            section, source_document = await _build_single_document_section(storage_service, customer_id, extraction)
            sections.append(section)
            source_documents.append(source_document)
        except Exception as exc:
            logger.warning(
                "profile_markdown extraction_section_failed customer_id=%s extraction_id=%s document_type=%s error=%s",
                customer_id,
                extraction_id,
                extraction_type,
                exc,
                exc_info=True,
            )
            sections.append(
                _markdown_section(
                    get_document_display_name(extraction_type),
                    [
                        f'- \u8d44\u6599\u7c7b\u578b\uff1a{get_document_display_name(extraction_type)}',
                        f'- \u63d0\u793a\uff1a\u8be5\u8d44\u6599\u90e8\u5206\u5b57\u6bb5\u6574\u7406\u5931\u8d25\uff0c\u5176\u4ed6\u8d44\u6599\u5df2\u7ee7\u7eed\u751f\u6210\u3002',
                    ],
                )
            )
    return sections, source_documents


def _marriage_display(value: Any) -> str:
    text = str(value or '').strip()
    return text if text else '\u672a\u8bc6\u522b'


def _format_marriage_persons_for_markdown(value: Any, extracted_data: dict[str, Any]) -> str:
    persons = value if isinstance(value, list) else []
    normalized: list[dict[str, Any]] = [item for item in persons if isinstance(item, dict)]
    if not normalized:
        husband_name = str(extracted_data.get('husband_name') or '').strip()
        wife_name = str(extracted_data.get('wife_name') or '').strip()
        if husband_name or extracted_data.get('husband_id_number'):
            normalized.append({
                'name': husband_name,
                'gender': '\u7537',
                'nationality': extracted_data.get('husband_nationality') or '',
                'birth_date': extracted_data.get('husband_birth_date') or '',
                'id_number': extracted_data.get('husband_id_number') or '',
            })
        if wife_name or extracted_data.get('wife_id_number'):
            normalized.append({
                'name': wife_name,
                'gender': '\u5973',
                'nationality': extracted_data.get('wife_nationality') or '',
                'birth_date': extracted_data.get('wife_birth_date') or '',
                'id_number': extracted_data.get('wife_id_number') or '',
            })
    if not normalized:
        return '### \u767b\u8bb0\u53cc\u65b9\n- \u672a\u8bc6\u522b\u5230\u767b\u8bb0\u53cc\u65b9\u4fe1\u606f'

    lines = [
        '### \u767b\u8bb0\u53cc\u65b9',
        '',
        '| \u59d3\u540d | \u6027\u522b | \u56fd\u7c4d | \u51fa\u751f\u65e5\u671f | \u8eab\u4efd\u8bc1\u53f7\u7801 |',
        '|---|---|---|---|---|',
    ]
    for person in normalized:
        lines.append(
            '| '
            + ' | '.join(
                [
                    _marriage_display(person.get('name')),
                    _marriage_display(person.get('gender')),
                    _marriage_display(person.get('nationality')),
                    _marriage_display(person.get('birth_date')),
                    _marriage_display(person.get('id_number')),
                ]
            )
            + ' |'
        )
    return '\n'.join(lines)


MARRIAGE_RAW_NOISE_KEYWORDS = (
    '\u626b\u63cf\u5168\u80fd\u738b',
    '\u626b\u63cfApp',
    '3\u4ebf\u4eba',
    'CamScanner',
    'Adobe Scan',
)
MARRIAGE_RAW_FIELD_LABELS = (
    '\u8eab\u4efd\u8bc1\u4ef6\u53f7',
    '\u7ed3\u5a5a\u8bc1\u5b57\u53f7',
    '\u51fa\u751f\u65e5\u671f',
    '\u767b\u8bb0\u65e5\u671f',
    '\u767b\u8bb0\u673a\u5173',
    '\u8eab\u4efd\u8bc1\u53f7',
    '\u6301\u8bc1\u4eba',
    '\u59d3\u540d',
    '\u6027\u522b',
    '\u56fd\u7c4d',
)


def _format_marriage_raw_line(line: str) -> str:
    text = str(line or '').strip().strip('|').strip()
    if not text:
        return ''
    if any(keyword in text for keyword in MARRIAGE_RAW_NOISE_KEYWORDS):
        return ''
    if text in {'```text', '```'}:
        return ''
    if re.fullmatch(r'[\d\s./_-]+', text) and len(text.replace(' ', '')) <= 3:
        return ''
    if re.fullmatch(r'[\W_]+', text, flags=re.UNICODE):
        return ''

    text = re.sub(r'\s+', '', text)
    for label in MARRIAGE_RAW_FIELD_LABELS:
        if text.startswith(label) and not text.startswith(f'{label}\uff1a') and not text.startswith(f'{label}:'):
            value = text[len(label):].strip()
            if value:
                text = f'{label}\uff1a{value}'
            break
    text = text.replace('\u8eab\u4efd\u8bc1\u4ef6\u53f7\uff1a', '\u8eab\u4efd\u8bc1\u53f7\uff1a')
    return text


def _format_marriage_readable_raw_block(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in str(text or '').replace('\r', '\n').split('\n'):
        formatted = _format_marriage_raw_line(raw_line)
        if formatted:
            lines.append(f'{formatted}  ')
    return lines


def _format_marriage_raw_text_for_markdown(extracted_data: dict[str, Any]) -> str:
    raw_pages = extracted_data.get('raw_pages')
    lines = ['### \u539f\u6587\u8bc6\u522b\u5185\u5bb9']
    has_content = False
    if isinstance(raw_pages, list):
        for item in raw_pages:
            if not isinstance(item, dict):
                continue
            page_text = str(item.get('text') or '').strip()
            if not page_text:
                continue
            page_lines = _format_marriage_readable_raw_block(page_text)
            if not page_lines:
                continue
            has_content = True
            lines.append(f"#### \u7b2c {item.get('page') or len(lines)} \u9875")
            lines.extend(page_lines)
    if not has_content:
        raw_text = str(extracted_data.get('raw_text') or '').strip()
        if raw_text:
            raw_lines = _format_marriage_readable_raw_block(raw_text)
            if raw_lines:
                has_content = True
                lines.extend(raw_lines)
    return '\n'.join(lines) if has_content else ''


PROPERTY_RAW_NOISE_KEYWORDS = (
    '\u626b\u63cf\u5168\u80fd\u738b',
    '\u626b\u63cfApp',
    '3\u4ebf\u4eba',
    'CamScanner',
    'Adobe Scan',
)


def _format_property_raw_line(line: str) -> str:
    text = str(line or '').strip().strip('|').strip()
    if not text:
        return ''
    if any(keyword in text for keyword in PROPERTY_RAW_NOISE_KEYWORDS):
        return ''
    if re.fullmatch(r'[\d\s./_-]+', text) and len(text.replace(' ', '')) <= 3:
        return ''
    if re.fullmatch(r'[\W_]+', text, flags=re.UNICODE):
        return ''
    text = re.sub(r'\s+', ' ', text).strip()
    field_labels = (
        '\u7f16\u53f7', '\u4e0d\u52a8\u4ea7\u6743\u53f7', '\u6743\u5229\u4eba', '\u5171\u6709\u60c5\u51b5',
        '\u5750\u843d', '\u4e0d\u52a8\u4ea7\u5355\u5143\u53f7', '\u6743\u5229\u7c7b\u578b', '\u6743\u5229\u6027\u8d28',
        '\u7528\u9014', '\u571f\u5730\u9762\u79ef', '\u5efa\u7b51\u9762\u79ef', '\u4f7f\u7528\u671f\u9650',
        '\u767b\u8bb0\u673a\u6784', '\u767b\u8bb0\u673a\u5173', '\u767b\u8bb0\u65e5\u671f', '\u53d1\u8bc1\u65e5\u671f',
    )
    compact = re.sub(r'\s+', '', text)
    for label in field_labels:
        if compact.startswith(label) and not compact.startswith(f'{label}\uff1a') and not compact.startswith(f'{label}:'):
            value = compact[len(label):].strip()
            if value:
                return f'{label}\uff1a{value}  '
    return f'{text}  '


def _format_property_raw_text_for_markdown(extracted_data: dict[str, Any]) -> str:
    raw_pages = extracted_data.get('raw_pages')
    lines = ['### \u539f\u6587\u8bc6\u522b\u5185\u5bb9']
    has_content = False
    if isinstance(raw_pages, list):
        for item in raw_pages:
            if not isinstance(item, dict):
                continue
            page_text = str(item.get('text') or '').strip()
            if not page_text:
                continue
            page_lines = [_format_property_raw_line(raw_line) for raw_line in page_text.replace('\r', '\n').split('\n')]
            page_lines = [line for line in page_lines if line]
            if not page_lines:
                continue
            has_content = True
            page_no = item.get('page') or len(lines)
            lines.append(f'#### \u7b2c {page_no} \u9875')
            lines.extend(page_lines)
    if not has_content:
        raw_text = str(extracted_data.get('raw_text') or '').strip()
        raw_lines = [_format_property_raw_line(raw_line) for raw_line in raw_text.replace('\r', '\n').split('\n')]
        raw_lines = [line for line in raw_lines if line]
        if raw_lines:
            has_content = True
            lines.extend(raw_lines)
    return '\n'.join(lines) if has_content else ''


PROPERTY_DOCUMENT_TYPES = {
    'property_report',
    'collateral',
    'mortgage_info',
    'property_certificate',
    '房产证',
    '房产证 / 产调',
    '房产证/产调',
    '抵押物信息',
}
PROPERTY_MERGE_FIELDS = (
    'certificate_number',
    'real_estate_certificate_no',
    'right_holder',
    'ownership_status',
    'property_location',
    'real_estate_unit_no',
    'right_type',
    'right_nature',
    'usage',
    'land_area',
    'building_area',
    'land_use_term',
    'registration_authority',
    'registration_date',
    'other_rights_info',
    'land_status',
    'house_status',
    'room_no',
    'building_type',
    'total_floors',
    'completion_date',
)


def _is_empty_property_value(value: Any) -> bool:
    text = str(value or '').strip()
    return not text or text in {'未识别', '暂无', '-', 'null', 'None'}


def _merge_property_extracted_data(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in PROPERTY_MERGE_FIELDS:
        value = source.get(key)
        if _is_empty_property_value(value):
            continue
        if _is_empty_property_value(target.get(key)):
            target[key] = value


def merge_property_certificate_contents(contents: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for content in contents:
        if isinstance(content, dict):
            _merge_property_extracted_data(merged, content)
    return merged


def _property_nested_value(data: Any, key: str) -> str:
    if isinstance(data, dict):
        return str(data.get(key) or '').strip() or '未识别'
    return '未识别'


def _build_property_section_lines(file_names: list[str], original_available: bool, extracted_data: dict[str, Any]) -> list[str]:
    property_title = '\u623f\u4ea7\u8bc1'
    file_name_text = '、'.join(dict.fromkeys([name for name in file_names if name])) or '\u6682\u65e0'
    original_status = '\u53ef\u67e5\u770b' if original_available else '\u539f\u4ef6\u6587\u4ef6\u4e0d\u5b58\u5728\u6216\u5df2\u4e0d\u53ef\u7528'
    lines = [
        f'- \u8d44\u6599\u7c7b\u578b\uff1a{property_title}',
        f'- \u6765\u6e90\u6587\u4ef6\uff1a{file_name_text}',
        f'- \u539f\u4ef6\u72b6\u6001\uff1a{original_status}',
        '',
        '### \u7ed3\u6784\u5316\u63d0\u53d6\u7ed3\u679c',
    ]
    property_fields = [
        ('certificate_number', '\u7f16\u53f7'),
        ('real_estate_certificate_no', '\u4e0d\u52a8\u4ea7\u6743\u53f7'),
        ('right_holder', '\u6743\u5229\u4eba'),
        ('ownership_status', '\u5171\u6709\u60c5\u51b5'),
        ('property_location', '\u5750\u843d'),
        ('real_estate_unit_no', '\u4e0d\u52a8\u4ea7\u5355\u5143\u53f7'),
        ('right_type', '\u6743\u5229\u7c7b\u578b'),
        ('right_nature', '\u6743\u5229\u6027\u8d28'),
        ('usage', '\u7528\u9014'),
        ('land_area', '\u571f\u5730\u9762\u79ef'),
        ('building_area', '\u5efa\u7b51\u9762\u79ef'),
        ('land_use_term', '\u4f7f\u7528\u671f\u9650'),
        ('registration_authority', '\u767b\u8bb0\u673a\u6784'),
        ('registration_date', '\u767b\u8bb0\u65e5\u671f'),
    ]
    for key, label in property_fields:
        lines.append(f"- {label}\uff1a{_marriage_display(extracted_data.get(key))}")
    other_rights_info = str(extracted_data.get('other_rights_info') or '').strip()
    land_status = extracted_data.get('land_status')
    house_status = extracted_data.get('house_status')
    if isinstance(land_status, dict) or isinstance(house_status, dict):
        lines.append("- \u6743\u5229\u5176\u4ed6\u72b6\u51b5\uff1a")
        lines.append("  - \u571f\u5730\u72b6\u51b5\uff1a")
        lines.append(f"    - \u5730\u53f7\uff1a{_property_nested_value(land_status, 'parcel_no')}")
        lines.append(f"    - \u4f7f\u7528\u6743\u9762\u79ef\uff1a{_property_nested_value(land_status, 'land_use_area')}")
        lines.append(f"    - \u72ec\u7528\u9762\u79ef\uff1a{_property_nested_value(land_status, 'exclusive_area')}")
        lines.append(f"    - \u5206\u644a\u9762\u79ef\uff1a{_property_nested_value(land_status, 'shared_area')}")
        lines.append("  - \u623f\u5c4b\u72b6\u51b5\uff1a")
        lines.append(f"    - \u5ba4\u53f7\u90e8\u4f4d\uff1a{_property_nested_value(house_status, 'room_no')}")
        lines.append(f"    - \u7c7b\u578b\uff1a{_property_nested_value(house_status, 'building_type')}")
        lines.append(f"    - \u603b\u5c42\u6570\uff1a{_property_nested_value(house_status, 'total_floors')}")
        lines.append(f"    - \u7ae3\u5de5\u65e5\u671f\uff1a{_property_nested_value(house_status, 'completion_date')}")
    elif other_rights_info and other_rights_info not in {'未识别', '暂无', '-'}:
        lines.append("- \u6743\u5229\u5176\u4ed6\u72b6\u51b5\uff1a")
        parts = [part.strip(" \uff1b;") for part in re.split(r"[；;]\s*", other_rights_info) if part.strip(" \uff1b;")]
        for part in parts:
            lines.append(f"  - {part}")
    else:
        lines.append("- \u6743\u5229\u5176\u4ed6\u72b6\u51b5\uff1a\u672a\u8bc6\u522b")
    return lines


async def _build_single_document_section(
    storage_service: Any,
    customer_id: str,
    extraction: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    extraction_type = extraction.get('extraction_type') or '\u672a\u547d\u540d\u8d44\u6599'
    extracted_data = extraction.get('extracted_data') or {}
    type_name = get_document_display_name(extraction_type)
    document = None
    doc_id = extraction.get('doc_id')
    if doc_id:
        try:
            document = await storage_service.get_document(doc_id)
        except Exception as exc:
            logger.warning("profile_markdown document_meta_failed customer_id=%s doc_id=%s error=%s", customer_id, doc_id, exc)
    file_name = (document or {}).get('file_name') or '\u6682\u65e0'
    file_path = (document or {}).get('file_path') or ''
    store_original = should_store_original(extraction_type)
    if store_original:
        original_status = '\u53ef\u67e5\u770b' if file_path else '\u539f\u4ef6\u6587\u4ef6\u4e0d\u5b58\u5728\u6216\u5df2\u4e0d\u53ef\u7528'
    else:
        original_status = '\u672a\u4fdd\u7559\u539f\u4ef6\uff0c\u4ec5\u4fdd\u7559\u63d0\u53d6\u7ed3\u679c'
    source_document = {
        'source_type': extraction_type,
        'source_type_name': type_name,
        'extraction_id': extraction.get('extraction_id'),
        'doc_id': doc_id,
        'file_name': file_name,
        'original_status': original_status,
        'original_available': bool(store_original and file_path),
    }
    if extraction_type in {'property_report', 'collateral', 'mortgage_info'} and isinstance(extracted_data, dict):
        property_title = '\u623f\u4ea7\u8bc1'
        lines = _build_property_section_lines([file_name], bool(store_original and file_path), extracted_data)
        source_document['source_type_name'] = property_title
        return _markdown_section(property_title, lines), source_document
    if extraction_type == 'marriage_cert' and isinstance(extracted_data, dict):
        lines = [
            f'- \u8d44\u6599\u7c7b\u578b\uff1a{type_name}',
            f'- \u6765\u6e90\u6587\u4ef6\uff1a{file_name}',
            f'- \u539f\u4ef6\u72b6\u6001\uff1a{original_status}',
            '',
            '### \u7ed3\u6784\u5316\u63d0\u53d6\u7ed3\u679c',
            f"- \u5a5a\u59fb\u72b6\u6001\uff1a{_marriage_display(extracted_data.get('marital_status'))}",
            f"- \u767b\u8bb0\u65e5\u671f\uff1a{_marriage_display(extracted_data.get('registration_date'))}",
            f"- \u7ed3\u5a5a\u8bc1\u5b57\u53f7\uff1a{_marriage_display(extracted_data.get('certificate_number'))}",
            f"- \u767b\u8bb0\u673a\u5173\uff1a{_marriage_display(extracted_data.get('registration_authority'))}",
            f"- \u5b8c\u6574\u6027\u63d0\u793a\uff1a{_marriage_display(extracted_data.get('completeness_note'))}",
            '',
            _format_marriage_persons_for_markdown(extracted_data.get('persons'), extracted_data),
        ]
        raw_markdown = _format_marriage_raw_text_for_markdown(extracted_data)
        if raw_markdown:
            lines.extend(['', raw_markdown])
        return _markdown_section(type_name, lines), source_document
    if extraction_type == 'hukou' and isinstance(extracted_data, dict):
        lines = [
            f'- \u8d44\u6599\u7c7b\u578b\uff1a{type_name}',
            f'- \u6765\u6e90\u6587\u4ef6\uff1a{file_name}',
            f'- \u539f\u4ef6\u72b6\u6001\uff1a{original_status}',
            '',
            '### 结构化提取结果',
            f"- {_format_field_label('household_head_name')}\uff1a{_format_value('household_head_name', extracted_data.get('household_head_name'))}",
            f"- {_format_field_label('household_number')}\uff1a{_format_value('household_number', extracted_data.get('household_number'))}",
            f"- {_format_field_label('household_type')}\uff1a{_format_value('household_type', extracted_data.get('household_type'))}",
            f"- {_format_field_label('household_address')}\uff1a{_format_value('household_address', extracted_data.get('household_address'))}",
            f"- {_format_field_label('registration_authority')}\uff1a{_format_value('registration_authority', extracted_data.get('registration_authority'))}",
            f"- {_format_field_label('completeness_note')}\uff1a{_format_value('completeness_note', extracted_data.get('completeness_note'))}",
            _format_value('members', extracted_data.get('members') or []),
        ]
        return _markdown_section(type_name, lines), source_document
    lines = [f'- \u8d44\u6599\u7c7b\u578b\uff1a{type_name}']
    lines.append(f'- \u6765\u6e90\u6587\u4ef6\uff1a{file_name}')
    lines.append(f'- \u539f\u4ef6\u72b6\u6001\uff1a{original_status}')
    if isinstance(extracted_data, dict):
        for key, value in extracted_data.items():
            try:
                if key in HIDDEN_STRUCTURED_FIELDS:
                    continue
                formatted_value = _format_value(key, value)
                if extraction_type == 'company_articles' and key in OPTIONAL_COMPANY_ARTICLES_FIELDS and formatted_value == '\u6682\u65e0':
                    continue
                if key == 'members':
                    lines.append(formatted_value)
                    continue
                lines.append(f'- {_format_field_label(key)}\uff1a{formatted_value}')
            except Exception as exc:
                logger.warning(
                    "profile_markdown field_failed customer_id=%s document_type=%s field=%s error=%s",
                    customer_id,
                    extraction_type,
                    key,
                    exc,
                )
    return _markdown_section(type_name, lines), source_document


async def _build_application_section(storage_service: Any, customer_id: str) -> tuple[str, dict[str, Any]]:
    applications = await storage_service.list_saved_applications(customer_id=customer_id)
    active = [item for item in applications if not item.get('stale')]

    if not active:
        if applications:
            latest_stale = applications[0]
            stale_reason = latest_stale.get('stale_reason') or '\u5ba2\u6237\u8d44\u6599\u5df2\u66f4\u65b0\uff0c\u8bf7\u91cd\u65b0\u751f\u6210\u7533\u8bf7\u8868'
            stale_at = latest_stale.get('stale_at') or '\u6682\u65e0\u8bb0\u5f55'
            lines = [
                '- \u5f53\u524d\u5df2\u4fdd\u5b58\u7533\u8bf7\u8868\u56e0\u8d44\u6599\u66f4\u65b0\u800c\u5931\u6548\u3002',
                f"- \u5931\u6548\u539f\u56e0\uff1a{stale_reason}",
                f"- \u5931\u6548\u65f6\u95f4\uff1a{stale_at}",
            ]
            return _markdown_section('\u7533\u8bf7\u8868\u6458\u8981', lines), {'count': len(applications), 'stale': True}
        return _markdown_section('\u7533\u8bf7\u8868\u6458\u8981', ['- \u6682\u65e0\u5df2\u4fdd\u5b58\u7533\u8bf7\u8868']), {'count': 0}

    latest = active[0]
    loan_type = latest.get('loanType') or '\u6682\u65e0'
    saved_at = latest.get('savedAt') or '\u6682\u65e0'
    lines = [
        f"- \u8d37\u6b3e\u7c7b\u578b\uff1a{loan_type}",
        f"- \u4fdd\u5b58\u65f6\u95f4\uff1a{saved_at}",
        f"- \u7ed3\u6784\u5316\u6570\u636e\uff1a{_format_value('applicationData', latest.get('applicationData') or {})}",
    ]
    return _markdown_section('\u7533\u8bf7\u8868\u6458\u8981', lines), {'count': len(active), 'latest_saved_at': latest.get('savedAt')}


def _build_scheme_section(snapshot: dict[str, Any] | None) -> tuple[str, dict[str, Any]]:
    if not snapshot:
        return _markdown_section('\u65b9\u6848\u5339\u914d\u6458\u8981', ['- \u5f53\u524d\u6682\u65e0\u5df2\u4fdd\u5b58\u5339\u914d\u65b9\u6848\u3002']), {'matched': False}

    updated_at = snapshot.get('updated_at') or snapshot.get('created_at') or '\u6682\u65e0'
    summary_text = snapshot.get('summary_markdown') or snapshot.get('raw_result') or '\u6682\u65e0'
    lines = [
        f"- \u6765\u6e90\uff1a{snapshot.get('source') or 'manual'}",
        f"- \u66f4\u65b0\u65f6\u95f4\uff1a{updated_at}",
        f"- \u5185\u5bb9\u6458\u8981\uff1a{summary_text}",
    ]
    return _markdown_section('\u65b9\u6848\u5339\u914d\u6458\u8981', lines), {'matched': True}


async def build_auto_profile_payload(storage_service: Any, customer_id: str) -> dict[str, Any]:
    customer = await storage_service.get_customer(customer_id)
    if not customer:
        raise ValueError('customer not found')

    customer_name = customer.get('name') or ''
    try:
        doc_sections, source_documents = await _build_document_sections(storage_service, customer_id)
    except Exception as exc:
        logger.warning("profile_markdown documents_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
        doc_sections, source_documents = [
            _markdown_section('\u5df2\u89e3\u6790\u8d44\u6599', ['- \u8d44\u6599\u6bb5\u843d\u751f\u6210\u5931\u8d25\uff0c\u8bf7\u67e5\u770b\u6765\u6e90\u6587\u6863\u5217\u8868\u6216\u91cd\u65b0\u4e0a\u4f20\u3002'])
        ], []
    try:
        application_section, application_snapshot = await _build_application_section(storage_service, customer_id)
    except Exception as exc:
        logger.warning("profile_markdown application_section_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
        application_section, application_snapshot = _markdown_section('\u7533\u8bf7\u8868\u6458\u8981', ['- \u7533\u8bf7\u8868\u6458\u8981\u6682\u65f6\u65e0\u6cd5\u751f\u6210']), {'error': str(exc)}
    try:
        scheme_snapshot = await storage_service.get_latest_scheme_snapshot(customer_id)
        scheme_section, scheme_meta = _build_scheme_section(scheme_snapshot)
    except Exception as exc:
        logger.warning("profile_markdown scheme_section_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
        scheme_section, scheme_meta = _markdown_section('\u65b9\u6848\u5339\u914d\u6458\u8981', ['- \u65b9\u6848\u5339\u914d\u6458\u8981\u6682\u65f6\u65e0\u6cd5\u751f\u6210']), {'error': str(exc)}

    customer_display_name = customer_name or '\u6682\u65e0'
    uploader = customer.get('uploader') or '\u6682\u65e0'
    upload_time = customer.get('upload_time') or customer.get('updated_at') or '\u6682\u65e0'
    overview_lines = [
        f"- \u5ba2\u6237\u540d\u79f0\uff1a{customer_display_name}",
        f"- \u5ba2\u6237\u7c7b\u578b\uff1a{_format_customer_type(customer.get('customer_type'))}",
        f"- \u4e0a\u4f20\u8d26\u53f7\uff1a{uploader}",
        f"- \u6700\u8fd1\u4e0a\u4f20\u65f6\u95f4\uff1a{upload_time}",
    ]

    markdown_parts = [
        '# \u8d44\u6599\u6c47\u603b',
        _markdown_section('\u5ba2\u6237\u57fa\u7840\u4fe1\u606f', overview_lines),
        _markdown_section(
            '\u4f7f\u7528\u8bf4\u660e',
            [
                '- \u8be5\u5185\u5bb9\u53ef\u7531\u7cfb\u7edf\u81ea\u52a8\u6574\u7406\uff0c\u4e5f\u53ef\u624b\u52a8\u8865\u5145\u4fee\u8ba2\u3002',
                '- \u624b\u52a8\u4fdd\u5b58\u540e\u4f1a\u4f5c\u4e3a\u5f53\u524d\u4f7f\u7528\u7248\u672c\u3002',
                '- RAG \u68c0\u7d22\u4f18\u5148\u7ea7\uff1a\u8d44\u6599\u6c47\u603b > \u5df2\u89e3\u6790\u8d44\u6599\u6587\u672c > \u65b9\u6848\u5339\u914d\u6458\u8981 > \u7533\u8bf7\u8868\u6458\u8981\u3002',
            ],
        ),
        _markdown_section('\u5df2\u89e3\u6790\u8d44\u6599\u7d22\u5f15', [f'- \u5171 {len(source_documents)} \u4efd\u8d44\u6599'] if source_documents else ['- \u6682\u65e0\u5df2\u89e3\u6790\u8d44\u6599']),
        *doc_sections,
        application_section,
        scheme_section,
    ]

    return {
        'customer_id': customer_id,
        'customer_name': customer_name,
        'title': f"{customer_name or customer_id}\u8d44\u6599\u6c47\u603b",
        'markdown_content': '\n\n'.join(markdown_parts).strip(),
        'source_mode': 'auto',
        'source_snapshot': {
            'customer_name': customer_name,
            'source_documents': source_documents,
            'application_summary': application_snapshot,
            'scheme_summary': scheme_meta,
        },
        'rag_source_priority': get_rag_source_priority(),
        'risk_report_schema': get_risk_report_schema_template(),
    }


async def get_or_create_customer_profile(storage_service: Any, customer_id: str) -> tuple[dict[str, Any], bool]:
    existing = await storage_service.get_customer_profile(customer_id)
    if existing:
        return existing, False

    generated = await build_auto_profile_payload(storage_service, customer_id)
    saved = await storage_service.upsert_customer_profile(generated)
    return saved, True


async def regenerate_customer_profile(storage_service: Any, customer_id: str) -> dict[str, Any]:
    generated = await build_auto_profile_payload(storage_service, customer_id)
    return await storage_service.upsert_customer_profile(generated)


def _normalize_id_card_identity_piece(value: Any) -> str:
    return ''.join(ch for ch in str(value or '').strip().upper() if ch.isalnum())


def _resolve_id_card_group_key(extracted_data: dict[str, Any], file_name: str, index: int) -> str:
    id_number = _normalize_id_card_identity_piece(extracted_data.get('id_number'))
    if id_number:
        return f"id_number:{id_number}"
    name = str(extracted_data.get('name') or '').strip()
    birth_date = _normalize_id_card_identity_piece(extracted_data.get('birth_date'))
    if name and birth_date:
        return f"name_birth:{name}|{birth_date}"
    if name:
        return f"name:{name}"
    return f"document:{file_name or index}"


async def _build_document_sections(storage_service: Any, customer_id: str) -> tuple[list[str], list[dict[str, Any]]]:
    extractions = await storage_service.get_extractions_by_customer(customer_id)
    sections: list[str] = []
    source_documents: list[dict[str, Any]] = []
    id_card_extractions = [item for item in extractions if (item.get('extraction_type') or '') == 'id_card']
    property_extractions = [item for item in extractions if (item.get('extraction_type') or '') in PROPERTY_DOCUMENT_TYPES]
    other_extractions = [
        item for item in extractions
        if (item.get('extraction_type') or '') != 'id_card'
        and (item.get('extraction_type') or '') not in PROPERTY_DOCUMENT_TYPES
    ]

    if id_card_extractions:
        try:
            grouped_id_cards: dict[str, dict[str, Any]] = {}

            for index, extraction in enumerate(id_card_extractions, start=1):
                extracted_data = (extraction.get('extracted_data') or {}) if isinstance(extraction.get('extracted_data'), dict) else {}
                document = None
                doc_id = extraction.get('doc_id')
                if doc_id:
                    try:
                        document = await storage_service.get_document(doc_id)
                    except Exception as exc:
                        logger.warning("profile_markdown id_card_document_meta_failed customer_id=%s doc_id=%s error=%s", customer_id, doc_id, exc)

                file_name = (document or {}).get('file_name') or '暂无'
                file_path = (document or {}).get('file_path') or ''
                source_documents.append({
                    'source_type': 'id_card',
                    'source_type_name': get_document_display_name('id_card'),
                    'extraction_id': extraction.get('extraction_id'),
                    'doc_id': doc_id,
                    'file_name': file_name,
                    'original_status': '可查看' if file_path else '原件文件不存在或已不可用',
                    'original_available': bool(file_path),
                })

                group_key = _resolve_id_card_group_key(extracted_data, file_name, index)
                group = grouped_id_cards.setdefault(
                    group_key,
                    {
                        'data': {},
                        'file_names': [],
                        'has_front': False,
                        'has_back': False,
                        'any_original_available': False,
                    },
                )

                for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address', 'issuing_authority', 'valid_period'):
                    if not group['data'].get(key):
                        group['data'][key] = extracted_data.get(key) or ''

                if any(extracted_data.get(key) for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address')):
                    group['has_front'] = True
                if any(extracted_data.get(key) for key in ('issuing_authority', 'valid_period')):
                    group['has_back'] = True

                group['file_names'].append(file_name)
                group['any_original_available'] = group['any_original_available'] or bool(file_path)

            for group in grouped_id_cards.values():
                merged_data = group['data']
                if group['has_front'] and group['has_back']:
                    merged_data['side'] = 'both'
                    merged_data['completeness_hint'] = '已识别正反面'
                elif group['has_front']:
                    merged_data['side'] = 'front'
                    merged_data['completeness_hint'] = '已识别正面，缺少反面信息（签发机关、有效期限）'
                elif group['has_back']:
                    merged_data['side'] = 'back'
                    merged_data['completeness_hint'] = '已识别反面，缺少正面信息（姓名、身份证号码、住址）'
                else:
                    merged_data['side'] = 'unknown'
                    merged_data['completeness_hint'] = '未识别到身份证正反面关键信息'

                merged_file_name_text = '、'.join(dict.fromkeys(group['file_names'])) if group['file_names'] else '暂无'
                merged_original_status = '可查看' if group['any_original_available'] else '原件文件不存在或已不可用'
                id_card_lines = [
                    f"- 资料类型：{get_document_display_name('id_card')}",
                    f"- 来源文件：{merged_file_name_text}",
                    f"- 原件状态：{merged_original_status}",
                ]
                for key in ('name', 'gender', 'ethnicity', 'birth_date', 'id_number', 'address', 'issuing_authority', 'valid_period', 'side', 'completeness_hint'):
                    id_card_lines.append(f"- {_format_field_label(key)}：{_format_value(key, merged_data.get(key))}")
                id_card_name = str(merged_data.get('name') or '').strip()
                id_card_title = f"{get_document_display_name('id_card')}（{id_card_name}）" if id_card_name else get_document_display_name('id_card')
                sections.append(_markdown_section(id_card_title, id_card_lines))
        except Exception as exc:
            logger.warning("profile_markdown id_card_section_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
            sections.append(
                _markdown_section(
                    get_document_display_name('id_card'),
                    [
                        f"- 资料类型：{get_document_display_name('id_card')}",
                        '- 提示：身份证资料整理失败，请重新上传或检查原件。',
                    ],
                )
            )

    if property_extractions:
        try:
            merged_property_data: dict[str, Any] = {}
            property_file_names: list[str] = []
            property_original_available = False
            logger.info("[property merge] found property docs count=%s", len(property_extractions))
            for extraction in property_extractions:
                extraction_type = extraction.get('extraction_type') or ''
                extracted_data = (extraction.get('extracted_data') or {}) if isinstance(extraction.get('extracted_data'), dict) else {}
                document = None
                doc_id = extraction.get('doc_id')
                if doc_id:
                    try:
                        document = await storage_service.get_document(doc_id)
                    except Exception as exc:
                        logger.warning("profile_markdown property_document_meta_failed customer_id=%s doc_id=%s error=%s", customer_id, doc_id, exc)
                file_name = (document or {}).get('file_name') or '暂无'
                file_path = (document or {}).get('file_path') or ''
                logger.info("[property merge] source=%s content_keys=%s", file_name, list(extracted_data.keys()))
                logger.info("[property merge] before=%s", merged_property_data)
                _merge_property_extracted_data(merged_property_data, extracted_data)
                logger.info("[property merge] after merge source=%s merged=%s", file_name, merged_property_data)
                property_file_names.append(file_name)
                property_original_available = property_original_available or bool(file_path)
                source_documents.append({
                    'source_type': extraction_type,
                    'source_type_name': '房产证',
                    'extraction_id': extraction.get('extraction_id'),
                    'doc_id': doc_id,
                    'file_name': file_name,
                    'original_status': '可查看' if file_path else '原件文件不存在或已不可用',
                    'original_available': bool(file_path),
                })
            logger.info("[property merge] final merged=%s", merged_property_data)
            sections.append(_markdown_section('房产证', _build_property_section_lines(property_file_names, property_original_available, merged_property_data)))
        except Exception as exc:
            logger.warning("profile_markdown property_section_failed customer_id=%s error=%s", customer_id, exc, exc_info=True)
            sections.append(_markdown_section('房产证', ['- 提示：房产证资料整理失败，请查看来源文档列表或重新上传。']))

    for extraction in other_extractions:
        extraction_id = extraction.get('extraction_id') or ''
        extraction_type = extraction.get('extraction_type') or '未命名资料'
        try:
            section, source_document = await _build_single_document_section(storage_service, customer_id, extraction)
            sections.append(section)
            source_documents.append(source_document)
        except Exception as exc:
            logger.warning(
                "profile_markdown extraction_section_failed customer_id=%s extraction_id=%s document_type=%s error=%s",
                customer_id,
                extraction_id,
                extraction_type,
                exc,
                exc_info=True,
            )
            sections.append(
                _markdown_section(
                    get_document_display_name(extraction_type),
                    [
                        f"- 资料类型：{get_document_display_name(extraction_type)}",
                        '- 提示：该资料整理失败，请查看来源文档列表或重新上传。',
                    ],
                )
            )

    return sections, source_documents
