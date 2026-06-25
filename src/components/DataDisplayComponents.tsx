/* eslint-disable react-refresh/only-export-components -- Exports utility functions alongside display components for cohesion */
/**
 * Shared Data Display Components
 * 
 * Reusable components for rendering structured data in card/table format.
 * Used by both ChatPage (extraction results) and CustomerListPage (detail modal).
 */

import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  FileText, User, Building2, CreditCard, Banknote, AlertCircle,
  FileCheck, Percent, Calendar, DollarSign, Building, BadgeCheck,
  FileSpreadsheet
} from 'lucide-react';

// ============================================
// Icon Helpers
// ============================================

export function getFieldIcon(fieldName: string): React.ReactNode {
  const fieldIcons: Record<string, React.ReactNode> = {
    '姓名': <User className="w-3.5 h-3.5" />,
    '企业名称': <Building2 className="w-3.5 h-3.5" />,
    '公司名称': <Building2 className="w-3.5 h-3.5" />,
    '身份证号': <CreditCard className="w-3.5 h-3.5" />,
    '统一社会信用代码': <BadgeCheck className="w-3.5 h-3.5" />,
    '贷款金额': <DollarSign className="w-3.5 h-3.5" />,
    '贷款余额': <DollarSign className="w-3.5 h-3.5" />,
    '利率': <Percent className="w-3.5 h-3.5" />,
    '逾期': <AlertCircle className="w-3.5 h-3.5" />,
    '逾期提醒': <AlertCircle className="w-3.5 h-3.5" />,
    '查询日期': <Calendar className="w-3.5 h-3.5" />,
    '报告日期': <Calendar className="w-3.5 h-3.5" />,
  };

  for (const [key, icon] of Object.entries(fieldIcons)) {
    if (fieldName.includes(key)) {
      return icon;
    }
  }
  return <FileText className="w-3.5 h-3.5" />;
}


export function getSectionIcon(sectionName: string): React.ReactNode {
  const iconMap: Record<string, React.ReactNode> = {
    '报告基础信息': <FileText className="w-4 h-4" />,
    '报告信息': <FileText className="w-4 h-4" />,
    '基础信息': <FileText className="w-4 h-4" />,
    '基本信息': <FileText className="w-4 h-4" />,
    '查询信息': <FileText className="w-4 h-4" />,
    '企业身份信息': <Building2 className="w-4 h-4" />,
    '企业信息': <Building2 className="w-4 h-4" />,
    '企业基本信息': <Building2 className="w-4 h-4" />,
    '公司信息': <Building2 className="w-4 h-4" />,
    '账户信息': <Building2 className="w-4 h-4" />,
    '基础账户信息': <Building2 className="w-4 h-4" />,
    '个人信息': <User className="w-4 h-4" />,
    '个人基本信息': <User className="w-4 h-4" />,
    '法定代表人信息': <User className="w-4 h-4" />,
    '法定代表人': <User className="w-4 h-4" />,
    '主要出资人信息': <User className="w-4 h-4" />,
    '注册资本': <DollarSign className="w-4 h-4" />,
    '财务信息': <DollarSign className="w-4 h-4" />,
    '财务数据': <DollarSign className="w-4 h-4" />,
    '资产负债': <DollarSign className="w-4 h-4" />,
    '收入支出': <Banknote className="w-4 h-4" />,
    '流水统计': <Banknote className="w-4 h-4" />,
    '交易统计': <Banknote className="w-4 h-4" />,
    '月度统计': <Banknote className="w-4 h-4" />,
    '信贷信息': <CreditCard className="w-4 h-4" />,
    '贷款信息': <CreditCard className="w-4 h-4" />,
    '信用卡信息': <CreditCard className="w-4 h-4" />,
    '授信信息': <CreditCard className="w-4 h-4" />,
    '担保信息': <CreditCard className="w-4 h-4" />,
    '逾期信息': <AlertCircle className="w-4 h-4" />,
    '逾期提醒': <AlertCircle className="w-4 h-4" />,
    '风险提示': <AlertCircle className="w-4 h-4" />,
    '异常信息': <AlertCircle className="w-4 h-4" />,
    '查询记录': <Calendar className="w-4 h-4" />,
    '历史查询': <Calendar className="w-4 h-4" />,
    '交易对手结构': <Building className="w-4 h-4" />,
    '关联方交易': <Building className="w-4 h-4" />,
  };

  if (iconMap[sectionName]) return iconMap[sectionName];

  for (const [key, icon] of Object.entries(iconMap)) {
    if (sectionName.includes(key) || key.includes(sectionName)) {
      return icon;
    }
  }

  return <FileCheck className="w-4 h-4" />;
}

// ============================================
// Value Formatting
// ============================================

const HIDDEN_DISPLAY_KEYS = new Set([
  'title',
  'type',
  'data',
  'transactions',
  'transactionid',
  'transaction_id',
  'counterpartyaccount',
  'counterparty_account',
  'counterpartybankno',
  'counterparty_bank_no',
  'rawrowno',
  'raw_row_no',
  'isselftransfer',
  'is_self_transfer',
  'isloanrelated',
  'is_loan_related',
  'isfee',
  'is_fee',
  'issalary',
  'is_salary',
  'istax',
  'is_tax',
  'isinterest',
  'is_interest',
  'isoperatinginflow',
  'is_operating_inflow',
  'isoperatingoutflow',
  'is_operating_outflow',
  'confidence',
  'doc_type',
  'doctype',
  'doc_type_name',
  'doctypename',
  'agent_type',
  'agenttype',
  'structured_data',
  'structureddata',
  'raw_fields',
  'rawfields',
  'fields',
  'capital_check',
  'capitalcheck',
  'governance',
  'major_resolution_rules',
  'majorresolutionrules',
  'signature_info',
  'signatureinfo',
  'registered_capital_amount',
  'registeredcapitalamount',
  'shareholder_total_amount',
  'shareholdertotalamount',
  'legal_representative',
  'legalrepresentative',
  'source_pages',
  'sourcepages',
  'text_length',
  'textlength',
  'customer_id',
  'customerid',
  'currency',
  'metadata',
  'evidence',
  'raw_text',
  'rawtext',
  'raw_text_preview',
  'rawtextpreview',
  'markdown',
  'display_markdown',
  'displaymarkdown',
  'report_markdown',
  'reportmarkdown',
  'markdown_summary',
  'markdownsummary',
  'summary_markdown',
  'summarymarkdown',
  'page_count',
  'pagecount',
  'document_type_code',
  'documenttypecode',
  'document_type_name',
  'documenttypename',
  'storage_label',
  'storagelabel',
  'schema_version',
  'schemaversion',
  'extraction_version',
  'extractionversion',
]);

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>;
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return asRecord(parsed);
    } catch {
      return {};
    }
  }
  return {};
}

function collectDisplayRecords(value: unknown): Record<string, unknown>[] {
  const root = asRecord(value);
  if (!Object.keys(root).length) return [];
  return [
    root,
    asRecord(root.content),
    asRecord(root.result),
    asRecord(root.extracted_json),
    asRecord(root.extractedJson),
    asRecord(root.extracted_data),
    asRecord(root.extractedData),
    asRecord(root.data),
    asRecord(root.structured_data),
    asRecord(root.structuredData),
  ].filter((record) => Object.keys(record).length > 0);
}

function getDocTypeFromRecords(records: Record<string, unknown>[]): string {
  return String(records.flatMap((record) => [
    record.doc_type,
    record.docType,
    record.document_type,
    record.documentType,
    record.document_type_code,
    record.documentTypeCode,
    record.type,
    record.extraction_type,
  ]).find((value) => String(value || '').trim()) || '').trim();
}

function monthlyCountsFromRecords(records: Record<string, unknown>[]): Record<string, string> {
  const counts: Record<string, string> = {};
  records.forEach((record) => {
    const monthly = asRecord(record.monthly);
    Object.entries(monthly).forEach(([month, item]) => {
      const row = asRecord(item);
      const count = row.count ?? row.transaction_count ?? row.transactionCount;
      if (String(month || '').match(/^\d{4}-\d{2}$/) && count !== undefined && count !== null && count !== '') {
        counts[month] = String(count);
      }
    });
  });
  return counts;
}

export function cleanupBankReconciliationMarkdown(markdown: string, payload?: unknown): string {
  const records = collectDisplayRecords(payload);
  const monthCounts = monthlyCountsFromRecords(records);
  const forbiddenLine = /^\s*[-*]?\s*(title|type|document\s*type|doc\s*type|doc\s*type\s*name|data|markdown|display\s*markdown|report\s*markdown|structured\s*data|structured_data|display_markdown|report_markdown|transactions)\s*[:：]/i;
  return markdown
    .split(/\r?\n/)
    .filter((line) => !forbiddenLine.test(line))
    .map((line) => {
      if (/^\|\s*\d{4}-\d{2}\s*\|/.test(line)) {
        const month = line.match(/^\|\s*(\d{4}-\d{2})\s*\|/)?.[1] || '';
        const trimmed = line.trim();
        const closed = trimmed.endsWith('|') ? trimmed : `${trimmed} |`;
        const cells = closed.split('|').slice(1, -1).map((cell) => cell.trim());
        if (cells.length === 4) return `${closed} ${monthCounts[month] || '0'} |`;
        return closed;
      }
      return line.replace(/[{}[\]]/g, '');
    })
    .join('\n')
    .replace(/\b(null|None|undefined|true|false)\b/g, '')
    .trim();
}

export function getBankReconciliationDisplayMarkdown(value: unknown): string | null {
  const records = collectDisplayRecords(value);
  if (getDocTypeFromRecords(records) !== 'bank_reconciliation_detail') return null;
  const markdownKeys = [
    'display_markdown',
    'displayMarkdown',
    'markdown',
    'report_markdown',
    'reportMarkdown',
    'markdown_result',
    'markdownResult',
  ];
  for (const key of markdownKeys) {
    for (const record of records) {
      const markdown = typeof record[key] === 'string' ? String(record[key]).trim() : '';
      if (markdown) return cleanupBankReconciliationMarkdown(markdown, value);
    }
  }
  return '## 银行对账明细\n\n- 提取状态：成功\n- 展示结果：暂无可展示内容';
}

export function isHiddenDisplayKey(key: string): boolean {
  const normalized = String(key || '').replace(/[\s_-]+/g, '').toLowerCase();
  return HIDDEN_DISPLAY_KEYS.has(key) || HIDDEN_DISPLAY_KEYS.has(normalized);
}

function formatScientificNotationString(value: string): string {
  const trimmed = value.trim();
  const match = trimmed.match(/^(-?\d+(?:\.\d+)?e[+-]?\d+)(.*)$/i);
  if (!match) return value;

  const numericValue = Number(match[1]);
  if (!Number.isFinite(numericValue)) return value;

  const unitSuffix = match[2] || '';
  const formatted = Number.isInteger(numericValue)
    ? new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 }).format(numericValue)
    : new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 }).format(numericValue);

  return `${formatted}${unitSuffix}`;
}

export function formatTableValue(value: unknown): string {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'string') {
    if (!value) return '-';
    return formatScientificNotationString(value);
  }
  if (typeof value === 'number') return value.toLocaleString();
  if (typeof value === 'boolean') return value ? '是' : '否';
  if (Array.isArray(value)) {
    if (value.length === 0) return '-';
    if (value.every(v => typeof v === 'string' || typeof v === 'number')) {
      return value.join('、');
    }
    return `共 ${value.length} 项`;
  }
  if (typeof value === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 0) return '-';
    return `共 ${keys.length} 项`;
  }
  return String(value);
}

export function isNestedObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) && Object.keys(value).length > 0;
}

export function isArrayOfObjects(value: unknown): value is Array<Record<string, unknown>> {
  return Array.isArray(value) && value.length > 0 && typeof value[0] === 'object' && value[0] !== null;
}


// ============================================
// Components
// ============================================

interface DataTableProps {
  data: Record<string, unknown>;
  level?: number;
}

export const DataTable: React.FC<DataTableProps> = ({ data, level = 0 }) => {
  const bankMarkdown = getBankReconciliationDisplayMarkdown(data);
  if (bankMarkdown) {
    return (
      <article className="prose prose-slate max-w-none rounded-lg border border-slate-200 bg-white p-4">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>
          {bankMarkdown}
        </ReactMarkdown>
      </article>
    );
  }
  const entries = Object.entries(data).filter(([key]) => !isHiddenDisplayKey(key));

  const simpleEntries = entries.filter(([, value]) => !isNestedObject(value) && !isArrayOfObjects(value));
  const nestedEntries = entries.filter(([, value]) => isNestedObject(value));
  const arrayEntries = entries.filter(([, value]) => isArrayOfObjects(value));

  return (
    <div className="space-y-3">
      {simpleEntries.length > 0 && (
        <div className="overflow-hidden rounded-lg border border-gray-200">
          <table className="w-full text-sm">
            <tbody>
              {simpleEntries.map(([key, value], idx) => (
                <tr key={key} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  <td className="px-3 py-2 text-gray-500 font-medium w-1/3 border-r border-gray-100">
                    <div className="flex items-center gap-2">
                      <span className="text-gray-400">{getFieldIcon(key)}</span>
                      <span className="truncate">{key}</span>
                    </div>
                  </td>
                  <td className="px-3 py-2 text-gray-800">
                    <span className="break-words" title={formatTableValue(value)}>
                      {formatTableValue(value)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {nestedEntries.map(([key, value]) => (
        <DataSectionCard key={key} title={key} data={value as Record<string, unknown>} level={level + 1} />
      ))}

      {arrayEntries.map(([key, value]) => (
        <ArrayDataCard key={key} title={key} data={value as Array<Record<string, unknown>>} />
      ))}
    </div>
  );
};

interface DataSectionCardProps {
  title: string;
  data: Record<string, unknown>;
  level?: number;
}

export const DataSectionCard: React.FC<DataSectionCardProps> = ({ title, data, level = 0 }) => {
  const bankMarkdown = getBankReconciliationDisplayMarkdown(data);
  if (bankMarkdown) {
    return (
      <article className="prose prose-slate max-w-none rounded-lg border border-slate-200 bg-white p-4">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>
          {bankMarkdown}
        </ReactMarkdown>
      </article>
    );
  }
  if (isHiddenDisplayKey(title)) return null;
  const visibleData = Object.fromEntries(Object.entries(data).filter(([key]) => !isHiddenDisplayKey(key)));
  if (Object.keys(visibleData).length === 0) return null;
  const bgGradient = level === 0
    ? 'bg-gradient-to-r from-slate-50 to-gray-50'
    : 'bg-gray-50';
  const iconBg = level === 0 ? 'bg-blue-100 text-blue-600' : 'bg-gray-200 text-gray-600';

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div className={`px-3 py-2 ${bgGradient} border-b border-gray-100`}>
        <div className="flex items-center gap-2">
          <div className={`w-7 h-7 rounded-md flex items-center justify-center ${iconBg}`}>
            {getSectionIcon(title)}
          </div>
          <span className="font-medium text-gray-700 text-sm">{title}</span>
        </div>
      </div>
      <div className="p-3">
        <DataTable data={visibleData} level={level} />
      </div>
    </div>
  );
};

interface ArrayDataCardProps {
  title: string;
  data: Array<Record<string, unknown>>;
}

export const ArrayDataCard: React.FC<ArrayDataCardProps> = ({ title, data }) => {
  if (isHiddenDisplayKey(title)) return null;
  if (data.length === 0) return null;

  const columns = Array.from(new Set(data.flatMap(item => Object.keys(item).filter((key) => !isHiddenDisplayKey(key)))));
  if (columns.length === 0) return null;

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div className="px-3 py-2 bg-gradient-to-r from-slate-50 to-gray-50 border-b border-gray-100">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-md flex items-center justify-center bg-purple-100 text-purple-600">
            <FileSpreadsheet className="w-4 h-4" />
          </div>
          <span className="font-medium text-gray-700 text-sm">{title}</span>
          <span className="text-xs text-gray-400">({data.length} 条记录)</span>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              {columns.map(col => (
                <th key={col} className="px-3 py-2 text-left text-gray-600 font-medium whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIdx) => (
              <tr key={rowIdx} className={rowIdx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                {columns.map(col => (
                  <td key={col} className="px-3 py-2 text-gray-800 whitespace-nowrap">
                    {formatTableValue(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
