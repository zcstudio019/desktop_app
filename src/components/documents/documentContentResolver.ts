type JsonRecord = Record<string, unknown>;

export type DocumentContentSource =
  | 'document_report_markdown'
  | 'extraction_report_markdown'
  | 'extracted_json_report_markdown'
  | 'extracted_json_markdown_report'
  | 'structured_json_report_markdown'
  | 'generated_financial_report_markdown'
  | 'extracted_text'
  | 'parsed_text'
  | 'empty';

export type DocumentContentResolution = {
  content: string;
  source: DocumentContentSource;
};

function asRecord(value: unknown): JsonRecord {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as JsonRecord;
  if (typeof value === 'string') {
    try {
      return asRecord(JSON.parse(value));
    } catch {
      return {};
    }
  }
  return {};
}

function nonEmpty(value: unknown): string {
  return typeof value === 'string' && value.trim() ? value.trim() : '';
}

function monetaryField(section: JsonRecord, key: string): JsonRecord {
  return asRecord(section[key]);
}

function money(value: unknown): string {
  const parsed = typeof value === 'number' ? value : Number(String(value ?? '').replace(/,/g, ''));
  if (!Number.isFinite(parsed)) return '-';
  return new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(parsed);
}

function statementTable(
  title: string,
  section: JsonRecord,
  fields: Array<[string, string]>,
  currentColumn: string,
  previousColumn: string,
): string {
  const rows = fields.map(([key, label]) => {
    const field = monetaryField(section, key);
    return `| ${label} | ${money(field.normalized_value)} | ${money(field.previous_normalized_value)} | ${String(field.source_page ?? '-')} | ${String(field.confidence ?? '-')} |`;
  });
  return [
    `### ${title}`,
    `| 项目 | ${currentColumn} | ${previousColumn} | 来源页码 | 置信度 |`,
    '|---|---:|---:|---:|---:|',
    ...rows,
  ].join('\n');
}

export function renderFinancialReportMarkdownFromStructuredData(structuredValue: unknown): string {
  const structured = asRecord(structuredValue);
  const companyInfo = asRecord(structured.company_info);
  const balanceSheet = asRecord(structured.balance_sheet);
  const incomeStatement = asRecord(structured.income_statement);
  const cashFlowStatement = asRecord(structured.cash_flow_statement);
  const reportType = { annual: '年报', quarterly: '季报', monthly: '月报' }[String(companyInfo.report_type || '')] || '-';
  const currency = String(companyInfo.currency || '') === 'CNY' ? '人民币' : String(companyInfo.currency || '-');
  return [
    '## 财务报表',
    '',
    '### 企业信息',
    '| 字段 | 内容 |',
    '|---|---|',
    `| 企业名称 | ${String(companyInfo.company_name || '-')} |`,
    `| 报表类型 | ${reportType} |`,
    `| 所属期开始日期 | ${String(companyInfo.report_period_start || '-')} |`,
    `| 所属期结束日期 | ${String(companyInfo.report_period_end || '-')} |`,
    `| 报送日期/报表日 | ${String(companyInfo.report_date || '-')} |`,
    `| 币种 | ${currency} |`,
    `| 金额单位 | ${String(companyInfo.unit || '元')} |`,
    '',
    statementTable('资产负债表摘要', balanceSheet, [
      ['cash_and_equivalents', '货币资金'],
      ['accounts_receivable', '应收账款'],
      ['current_assets_total', '流动资产合计'],
      ['short_term_loans', '短期借款'],
      ['total_liabilities', '负债合计'],
      ['total_equity', '所有者权益合计'],
      ['total_assets', '资产总计'],
    ], '期末余额', '上年年末余额'),
    '',
    statementTable('利润表摘要', incomeStatement, [
      ['revenue', '营业收入'],
      ['operating_cost', '营业成本'],
      ['operating_profit', '营业利润'],
      ['total_profit', '利润总额'],
      ['income_tax_expense', '所得税费用'],
      ['net_profit', '净利润'],
    ], '本期金额', '上期金额'),
    '',
    statementTable('现金流量表摘要', cashFlowStatement, [
      ['net_operating_cash_flow', '经营活动产生的现金流量净额'],
      ['net_investing_cash_flow', '投资活动产生的现金流量净额'],
      ['net_financing_cash_flow', '筹资活动产生的现金流量净额'],
      ['net_cash_increase', '现金及现金等价物净增加额'],
      ['ending_cash_balance', '期末现金及现金等价物余额'],
    ], '本期金额', '上期金额'),
  ].join('\n');
}

export function resolveDocumentContent(detailValue: unknown): DocumentContentResolution {
  const detail = asRecord(detailValue);
  const extraction = asRecord(detail.extraction);
  const extractedData = asRecord(extraction.extracted_data);
  const extractedJson = asRecord(detail.extracted_json ?? extractedData.extracted_json ?? extractedData.data ?? extractedData);
  const structuredJson = asRecord(detail.structured_json ?? extractedData.structured_json ?? extractedJson.structured_json);
  const documentType = String(detail.document_type ?? detail.file_type ?? structuredJson.document_type ?? '');

  const candidates: Array<[DocumentContentSource, string]> = [
    ['document_report_markdown', nonEmpty(detail.report_markdown)],
    ['extraction_report_markdown', nonEmpty(extraction.report_markdown ?? extractedData.report_markdown)],
    ['extracted_json_report_markdown', nonEmpty(extractedJson.report_markdown)],
    ['extracted_json_markdown_report', nonEmpty(extractedJson.markdown_report)],
    ['structured_json_report_markdown', nonEmpty(structuredJson.report_markdown)],
  ];
  for (const [source, content] of candidates) {
    if (content) return { content, source };
  }
  if (documentType === 'financial_report' && Object.keys(structuredJson).length > 0) {
    return {
      content: renderFinancialReportMarkdownFromStructuredData(structuredJson),
      source: 'generated_financial_report_markdown',
    };
  }
  const extractedText = nonEmpty(detail.extracted_text ?? extractedJson.extracted_text ?? extractedData.extracted_text);
  if (extractedText) return { content: extractedText, source: 'extracted_text' };
  const parsedText = nonEmpty(detail.parsed_text ?? extractedJson.parsed_text ?? extractedData.parsed_text);
  if (parsedText) return { content: parsedText, source: 'parsed_text' };
  return { content: '暂无分析报告', source: 'empty' };
}
