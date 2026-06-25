import { describe, expect, it } from 'vitest';
import { resolveDocumentContent } from './documentContentResolver';

describe('resolveDocumentContent', () => {
  it('prefers document report markdown over other sources', () => {
    const result = resolveDocumentContent({
      document_type: 'financial_report',
      report_markdown: '# 单文档完整分析',
      extraction: { report_markdown: '# 提取结果报告' },
      extracted_json: { markdown_report: '# 备用报告' },
    });
    expect(result.source).toBe('selectedDocument.report_markdown');
    expect(result.content).toBe('# 单文档完整分析');
  });

  it('prefers latest extraction report markdown before an older extraction payload', () => {
    const result = resolveDocumentContent({
      document_type: 'financial_report',
      latest_extraction: { report_markdown: '# 最新单文档报告' },
      extraction: { report_markdown: '# 旧提取报告' },
    });
    expect(result.source).toBe('selectedDocument.latest_extraction.report_markdown');
    expect(result.content).toBe('# 最新单文档报告');
  });

  it('supports camel-case latest extraction report markdown', () => {
    const result = resolveDocumentContent({
      document_type: 'financial_report',
      latestExtraction: { reportMarkdown: '# 驼峰单文档报告' },
    });
    expect(result.source).toBe('selectedDocument.latestExtraction.reportMarkdown');
    expect(result.content).toBe('# 驼峰单文档报告');
  });

  it('generates a financial report from structured data without using profile markdown', () => {
    const result = resolveDocumentContent({
      document_type: 'financial_report',
      profile_markdown: '## 财务数据总览',
      structured_json: {
        document_type: 'financial_report',
        company_info: { company_name: '测试企业', report_type: 'annual', currency: 'CNY', unit: '元' },
        balance_sheet: { total_assets: { normalized_value: 1000 } },
        income_statement: { revenue: { normalized_value: 500 } },
        cash_flow_statement: { net_operating_cash_flow: { normalized_value: -10 } },
      },
    });
    expect(result.source).toBe('generatedFinancialReportMarkdown');
    expect(result.content).toContain('资产负债表摘要');
    expect(result.content).toContain('利润表摘要');
    expect(result.content).toContain('现金流量表摘要');
    expect(result.content).toContain('银行授信核心指标表');
    expect(result.content).not.toContain('财务数据总览');
  });
  it('uses only final markdown for shuimui reports', () => {
    const result = resolveDocumentContent({
      document_type: 'shuimui_report',
      latest_extraction: {
        extracted_data: {
          doc_type: 'shuimui_report',
          report_markdown: '## 水母报告\n\n* 资料类型：水母报告\n* 提取状态：成功',
          structured_json: { 企业名称: '上海测试有限公司' },
          data: { 企业名称: '上海测试有限公司' },
        },
      },
    });

    expect(result.content).toContain('## 水母报告');
    expect(result.content).not.toContain('structured_json');
    expect(result.content).not.toContain('doc_type');
  });

  it('uses only final markdown for company articles', () => {
    const result = resolveDocumentContent({
      document_type: 'company_articles',
      latest_extraction: {
        extracted_data: {
          doc_type: 'company_articles',
          display_markdown: '## 公司章程\n- 资料类型：公司章程\n- 公司住所：上海市长宁区广顺路33号3幢6层672室',
          governance: { legal_representative: '由执行董事担任' },
          capital_check: { message: '出资额合计与注册资本一致' },
          metadata: { filename: '乐芙兰章程(新 沃志方).pdf' },
          raw_text_preview: 'raw',
        },
      },
    });

    expect(result.content).toContain('## 公司章程');
    expect(result.content).toContain('公司住所：上海市长宁区广顺路33号3幢6层672室');
    expect(result.content).not.toContain('doc_type');
    expect(result.content).not.toContain('governance');
    expect(result.content).not.toContain('capital_check');
    expect(result.content).not.toContain('metadata');
    expect(result.content).not.toContain('{');
    expect(result.content).not.toContain('}');
  });

  it('short-circuits nested cached company articles payloads to display markdown', () => {
    const result = resolveDocumentContent({
      latest_extraction: {
        extracted_data: {
          markdown: '## 公司章程\n- 法定代表人：暂无',
          extracted_json: {
            doc_type: 'company_articles',
            display_markdown: [
              '## 公司章程',
              '- 章程标题：上海乐芙兰电子商务有限公司章程',
              '- 公司住所：上海市长宁区广顺路33号3幢6层672室',
              '- 注册资本：人民币500万元',
              '- 法定代表人：由执行董事担任',
              '- 需人工复核：无',
            ].join('\n'),
            structured_data: {
              registered_capital_amount: 500,
              governance: { legal_representative: '由执行董事担任' },
              metadata: { source: 'old-cache' },
            },
            evidence: { source_pages: [1, 2, 3, 4, 5, 6] },
            raw_text_preview: 'raw text preview',
          },
        },
      },
    });

    expect(result.content).toContain('## 公司章程');
    expect(result.content).toContain('法定代表人：由执行董事担任');
    [
      'doc type',
      'agent type',
      'company address',
      'registered capital amount',
      'capital check',
      'governance',
      'major resolution rules',
      'signature info',
      'page count',
      'markdown：',
      'display markdown',
      'report markdown',
      'raw text preview',
      'evidence',
      'metadata',
      'registered_capital_amount',
      'legal_representative',
      '{',
      '}',
      '法定代表人：暂无',
    ].forEach((item) => expect(result.content.toLowerCase()).not.toContain(item.toLowerCase()));
  });

  it('renders bank reconciliation detail from display markdown only', () => {
    const result = resolveDocumentContent({
      document_type: 'bank_reconciliation_detail',
      extracted_json: {
        doc_type: 'bank_reconciliation_detail',
        display_markdown: '## 银行对账明细\n- 资料类型：银行对账明细\n\n### 核心资金概览',
        structured_data: {
          transactions: [{ amount: 17, is_fee: false }],
          summary: { in_amount: 100 },
        },
        data: { transactions: [{ amount: 17 }] },
      },
    });

    expect(result.source).toBe('selectedDocument.bank_reconciliation_detail.display_markdown');
    expect(result.content).toContain('## 银行对账明细');
    expect(result.content).not.toContain('transactions');
    expect(result.content).not.toContain('{');
    expect(result.content).not.toContain('}');
  });

  it('does not fall back to raw JSON for bank reconciliation detail', () => {
    const result = resolveDocumentContent({
      document_type: 'bank_reconciliation_detail',
      extracted_json: {
        doc_type: 'bank_reconciliation_detail',
        transactions: [{ amount: 17 }],
      },
    });

    expect(result.source).toBe('empty');
    expect(result.content).toBe('暂无可展示的银行对账明细结果');
    expect(result.content).not.toContain('transactions');
    expect(result.content).not.toContain('{');
  });
});
