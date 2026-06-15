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
});
