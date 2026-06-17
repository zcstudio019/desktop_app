type JsonRecord = Record<string, unknown>;

export type DocumentContentSource =
  | 'selectedDocument.report_markdown'
  | 'selectedDocument.reportMarkdown'
  | 'selectedDocument.latest_extraction.report_markdown'
  | 'selectedDocument.latestExtraction.reportMarkdown'
  | 'selectedDocument.extraction.report_markdown'
  | 'selectedDocument.extracted_json.display_markdown'
  | 'selectedDocument.extracted_json.report_markdown'
  | 'selectedDocument.extracted_json.markdown'
  | 'selectedDocument.extracted_json.markdown_report'
  | 'selectedDocument.structured_json.report_markdown'
  | 'selectedDocument.structured_json.markdown_report'
  | 'generatedFinancialReportMarkdown'
  | 'selectedDocument.extracted_text'
  | 'selectedDocument.parsed_text'
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

function firstMarkdownFromRecord(...records: JsonRecord[]): string {
  const version = records
    .map((record) => nonEmpty(record.extraction_version ?? record.extractionVersion ?? record.schema_version ?? record.schemaVersion))
    .find(Boolean);
  const priorities = [
    'display_markdown',
    'displayMarkdown',
    'report_markdown',
    'reportMarkdown',
    'markdown',
    'markdown_summary',
    'markdownSummary',
    'summary_markdown',
    'summaryMarkdown',
  ];
  const candidates: string[] = [];
  for (const key of priorities) {
    for (const record of records) {
      const markdown = nonEmpty(record[key]);
      if (markdown) candidates.push(markdown);
    }
  }
  if (version === 'company_articles_v3_canonical_markdown_only') return candidates[0] || '';
  return (
    candidates.find((item) => item.includes('## 公司章程') && item.includes('经营范围') && item.includes('法定代表人：由执行董事担任')) ||
    candidates.find((item) => item.includes('## 公司章程') && !item.includes('法定代表人：暂无')) ||
    candidates[0] ||
    ''
  );
}

function isShuimuiReportType(value: unknown): boolean {
  return String(value || '').trim() === 'shuimui_report';
}

function isCompanyArticlesType(value: unknown): boolean {
  return String(value || '').trim() === 'company_articles';
}

function dedupeShuimuiMarkdown(markdown: string): string {
  const text = markdown.trim();
  const marker = '## 水母报告';
  const first = text.indexOf(marker);
  if (first < 0) return text;
  const second = text.indexOf(marker, first + marker.length);
  if (second < 0) return text;
  return text.slice(0, second).trim();
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
  optionalNonZeroKeys: Set<string> = new Set(),
): string {
  const rows = fields.flatMap(([key, label]) => {
    const field = monetaryField(section, key);
    const hasCurrent = field.normalized_value !== null && field.normalized_value !== undefined && field.normalized_value !== '';
    const hasPrevious = field.previous_normalized_value !== null && field.previous_normalized_value !== undefined && field.previous_normalized_value !== '';
    if (!hasCurrent && !hasPrevious) return [];
    const current = hasCurrent ? Number(field.normalized_value) : 0;
    const previous = hasPrevious ? Number(field.previous_normalized_value) : 0;
    if (optionalNonZeroKeys.has(key) && current === 0 && previous === 0) return [];
    return [`| ${label} | ${money(field.normalized_value)} | ${money(field.previous_normalized_value)} | ${String(field.source_page ?? '-')} | ${String(field.confidence ?? '-')} |`];
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
  const financialRatios = asRecord(structured.financial_ratios);
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
      ['short_term_investments', '短期投资'],
      ['accounts_receivable', '应收账款'],
      ['prepayments', '预付款项'],
      ['other_receivables', '其他应收款'],
      ['inventory', '存货'],
      ['current_assets_total', '流动资产合计'],
      ['long_term_equity_investment', '长期股权投资'],
      ['fixed_assets_original_cost', '固定资产原价'],
      ['fixed_assets_net_value', '固定资产账面价值'],
      ['fixed_assets', '固定资产'],
      ['intangible_assets', '无形资产'],
      ['non_current_assets_total', '非流动资产合计'],
      ['total_assets', '资产总计'],
      ['short_term_loans', '短期借款'],
      ['accounts_payable', '应付账款'],
      ['other_payables', '其他应付款'],
      ['current_liabilities_total', '流动负债合计'],
      ['long_term_loans', '长期借款'],
      ['long_term_payables', '长期应付款'],
      ['non_current_liabilities_total', '非流动负债合计'],
      ['total_liabilities', '负债合计'],
      ['paid_in_capital', '实收资本'],
      ['undistributed_profit', '未分配利润'],
      ['total_equity', '所有者权益合计'],
      ['total_liabilities_and_equity', '负债和所有者权益总计'],
    ], '期末余额', '上年年末余额', new Set([
      'short_term_investments', 'long_term_equity_investment',
      'fixed_assets_original_cost', 'fixed_assets_net_value', 'long_term_payables',
    ])),
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
    '',
    '### 银行授信核心指标表',
    '| 指标 | 数值 |',
    '|---|---:|',
    `| 资产负债率 | ${money(financialRatios.asset_liability_ratio)} |`,
    `| 流动比率 | ${money(financialRatios.current_ratio)} |`,
    `| 速动比率 | ${money(financialRatios.quick_ratio)} |`,
    `| 现金比率 | ${money(financialRatios.cash_ratio)} |`,
    `| 毛利率 | ${money(financialRatios.gross_margin)} |`,
    `| 净利率 | ${money(financialRatios.net_margin)} |`,
  ].join('\n');
}

export function resolveDocumentContent(detailValue: unknown): DocumentContentResolution {
  const detail = asRecord(detailValue);
  const extraction = asRecord(detail.extraction);
  const latestExtraction = asRecord(detail.latest_extraction ?? detail.latestExtraction);
  const extractedData = asRecord(extraction.extracted_data);
  const latestExtractedData = asRecord(latestExtraction.extracted_data);
  const extractedJson = asRecord(
    detail.extracted_json
    ?? latestExtractedData.extracted_json
    ?? latestExtractedData.data
    ?? extractedData.extracted_json
    ?? extractedData.data
    ?? extractedData,
  );
  const structuredJson = asRecord(
    detail.structured_json
    ?? latestExtractedData.structured_json
    ?? extractedData.structured_json
    ?? extractedJson.structured_json,
  );
  const documentType = String(
    detail.document_type
    ?? detail.file_type
    ?? latestExtraction.extraction_type
    ?? latestExtraction.document_type
    ?? latestExtraction.document_type_code
    ?? latestExtractedData.document_type
    ?? latestExtractedData.document_type_code
    ?? latestExtractedData.doc_type
    ?? extractedData.document_type
    ?? extractedData.document_type_code
    ?? extractedData.doc_type
    ?? extractedJson.document_type
    ?? extractedJson.document_type_code
    ?? extractedJson.doc_type
    ?? structuredJson.document_type
    ?? structuredJson.document_type_code
    ?? structuredJson.doc_type
    ?? ''
  );

  if (isCompanyArticlesType(documentType)) {
    const markdown = firstMarkdownFromRecord(
      detail,
      latestExtraction,
      latestExtractedData,
      extraction,
      extractedData,
      extractedJson,
      structuredJson,
    );
    return { content: markdown || '暂无公司章程解析结果', source: markdown ? 'selectedDocument.extracted_json.display_markdown' : 'empty' };
  }

  const candidates: Array<[DocumentContentSource, string]> = [
    ['selectedDocument.report_markdown', nonEmpty(detail.report_markdown)],
    ['selectedDocument.reportMarkdown', nonEmpty(detail.reportMarkdown)],
    ['selectedDocument.latest_extraction.report_markdown', nonEmpty(latestExtraction.report_markdown ?? latestExtractedData.display_markdown ?? latestExtractedData.report_markdown ?? latestExtractedData.markdown ?? latestExtractedData.markdown_report ?? latestExtractedData.markdown_summary)],
    ['selectedDocument.latestExtraction.reportMarkdown', nonEmpty(latestExtraction.reportMarkdown)],
    ['selectedDocument.extraction.report_markdown', nonEmpty(extraction.report_markdown ?? extractedData.display_markdown ?? extractedData.report_markdown ?? extractedData.markdown ?? extractedData.markdown_report ?? extractedData.markdown_summary)],
    ['selectedDocument.extracted_json.display_markdown', nonEmpty(extractedJson.display_markdown)],
    ['selectedDocument.extracted_json.report_markdown', nonEmpty(extractedJson.report_markdown)],
    ['selectedDocument.extracted_json.markdown', nonEmpty(extractedJson.markdown)],
    ['selectedDocument.extracted_json.markdown_report', nonEmpty(extractedJson.markdown_report)],
    ['selectedDocument.structured_json.report_markdown', nonEmpty(structuredJson.report_markdown)],
    ['selectedDocument.structured_json.markdown_report', nonEmpty(structuredJson.markdown_report)],
  ];
  for (const [source, content] of candidates) {
    if (content) {
      const finalContent = isShuimuiReportType(documentType) ? dedupeShuimuiMarkdown(content) : content;
      return { content: finalContent, source };
    }
  }
  if (isCompanyArticlesType(documentType)) {
    return { content: '暂无公司章程解析结果', source: 'empty' };
  }
  if (isShuimuiReportType(documentType)) {
    return { content: '暂无水母报告解析结果', source: 'empty' };
  }
  if (documentType === 'financial_report' && Object.keys(structuredJson).length > 0) {
    return {
      content: renderFinancialReportMarkdownFromStructuredData(structuredJson),
      source: 'generatedFinancialReportMarkdown',
    };
  }
  const extractedText = nonEmpty(detail.extracted_text ?? extractedJson.extracted_text ?? extractedData.extracted_text);
  if (extractedText) return { content: extractedText, source: 'selectedDocument.extracted_text' };
  const parsedText = nonEmpty(detail.parsed_text ?? extractedJson.parsed_text ?? extractedData.parsed_text);
  if (parsedText) return { content: parsedText, source: 'selectedDocument.parsed_text' };
  return { content: '暂无分析报告', source: 'empty' };
}
