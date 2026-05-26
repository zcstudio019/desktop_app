import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import FinancialReportView from './FinancialReportView';

const amount = (value: number) => ({ normalized_value: value });

const payload = {
  structured_json: {
    document_type: 'financial_report',
    company_info: {
      company_name: '测试企业有限公司',
      report_type: 'annual',
      report_period_start: '2024-01-01',
      report_period_end: '2024-12-31',
      report_date: '2024-12-31',
      accounting_standard: 'enterprise_accounting_standard',
      currency: 'CNY',
      unit: '元',
    },
    balance_sheet: {
      cash_and_equivalents: amount(100),
      total_assets: amount(1_000),
      short_term_loans: amount(700),
      total_liabilities: amount(800),
      total_equity: amount(200),
    },
    income_statement: {
      revenue: amount(500),
      operating_cost: amount(400),
      net_profit: amount(10),
      interest_expense: amount(0),
    },
    cash_flow_statement: {
      net_operating_cash_flow: amount(-20),
      ending_cash_balance: amount(100),
    },
    financial_ratios: {
      asset_liability_ratio: 0.8,
      short_debt_cash_coverage: 100 / 700,
      interest_bearing_debt: 700,
    },
    bank_credit_analysis: {
      overall_risk_level: 'high',
      credit_view: '现金流承压。',
      risk_findings: [],
      missing_materials: [],
      suggested_credit_strategy: '审慎授信。',
    },
  },
  markdown_report: '# 财务报表授信分析报告',
  display_json: { 资料类型: '财务报表', 文档类型: '财务报表' },
};

describe('FinancialReportView', () => {
  it('renders financial advice, risk signals and collapsed raw sections in order', () => {
    const { container } = render(<FinancialReportView data={payload} profileMarkdown="# 客户资料汇总" />);
    const content = container.textContent || '';
    const orderedTitles = [
      '综合授信分析',
      '融资建议',
      '风险信号',
      '查看原始分析报告',
      '查看原始结构化数据',
      '查看原始资料汇总 Markdown',
    ];

    orderedTitles.forEach((title) => expect(screen.getByText(title)).toBeInTheDocument());
    orderedTitles.reduce((position, title) => {
      const next = content.indexOf(title);
      expect(next).toBeGreaterThan(position);
      return next;
    }, -1);

    const details = Array.from(container.querySelectorAll('details'));
    expect(details).toHaveLength(3);
    details.forEach((section) => expect(section).not.toHaveAttribute('open'));
    expect(details[1].textContent).toContain('资料类型');
    expect(details[1].textContent).not.toContain('document_type');
  });

  it('keeps ratio judgement labels horizontal when the panel is narrow', () => {
    const { container } = render(<FinancialReportView data={payload} />);
    const ratioSection = screen.getByText('银行授信核心指标表').closest('section');
    const table = ratioSection?.querySelector('table');
    const judgementHeader = screen.getByText('判断');
    const judgmentPills = Array.from(ratioSection?.querySelectorAll('tbody span') || []);

    expect(table).toHaveClass('min-w-[620px]');
    expect(judgementHeader).toHaveClass('w-20', 'min-w-20', 'whitespace-nowrap');
    expect(judgmentPills.length).toBeGreaterThan(0);
    judgmentPills.forEach((pill) => {
      expect(pill).toHaveClass('inline-flex', 'min-w-[44px]', 'whitespace-nowrap', 'break-keep');
    });
  });
});
