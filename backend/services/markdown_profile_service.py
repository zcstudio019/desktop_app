"""Customer markdown profile generation and persistence helpers."""

from __future__ import annotations

import json
import logging
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
    'financing_approval_rule': '\u878d\u8d44/\u8d37\u6b3e\u5ba1\u6279\u89c4\u5219',
    'financing_approval_threshold': '\u878d\u8d44\u8868\u51b3\u95e8\u69db',
    'major_decision_rules': '\u91cd\u5927\u4e8b\u9879\u89c4\u5219',
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
            lines.append(f"  - {'｜'.join(parts)}")
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
    return '；'.join(parts) if parts else '\u6682\u65e0'


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
        if key == 'shareholders':
            return _format_shareholders_for_markdown(value)
        if key == 'equity_ratios':
            return _format_equity_ratios_for_markdown(value)
        if key == 'major_decision_rules':
            return _format_rule_list_for_markdown(value)
        if key == 'major_decision_rule_details':
            return _format_rule_detail_list_for_markdown(value)
        return _format_list_for_markdown(value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    if key in AMOUNT_FIELDS:
        return _format_amount_for_markdown(value)
    if key in COUNT_FIELDS:
        return _format_count_for_markdown(value)
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
    for extraction in extractions:
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
