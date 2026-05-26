import { describe, expect, it } from 'vitest';
import { buildFinancialReportCustomerSummary } from './financialReportRightPanelBuilder';

const amount = (value: number) => ({ normalized_value: value });

function report(
  year: string,
  reportType: 'annual' | 'quarterly',
  periodStart: string,
  assets: number,
  liabilities: number,
  equity: number,
  revenue: number,
  profit: number,
  operatingCashFlow: number,
  cash: number,
  shortLoans: number,
) {
  return {
    document_type: 'financial_report',
    source_file: `${year}.pdf`,
    company_info: {
      company_name: '上海乐芙兰电子商务有限公司',
      report_type: reportType,
      report_period_start: periodStart,
      report_period_end: `${year}-12-31`,
      report_date: `${year}-12-31`,
      accounting_standard: 'enterprise_accounting_standard',
      currency: 'CNY',
      unit: '元',
    },
    balance_sheet: {
      cash_and_equivalents: amount(cash),
      accounts_receivable: amount(1),
      prepayments: amount(2),
      other_receivables: amount(3),
      inventory: amount(4),
      current_assets_total: amount(5),
      total_assets: amount(assets),
      short_term_loans: amount(shortLoans),
      long_term_loans: amount(0),
      accounts_payable: amount(6),
      current_liabilities_total: amount(7),
      total_liabilities: amount(liabilities),
      total_equity: amount(equity),
    },
    income_statement: {
      revenue: amount(revenue),
      operating_cost: amount(revenue * 0.8),
      net_profit: amount(profit),
    },
    cash_flow_statement: {
      net_operating_cash_flow: amount(operatingCashFlow),
      net_financing_cash_flow: amount(3_000_000),
      ending_cash_balance: amount(cash),
    },
    financial_ratios: {
      asset_liability_ratio: liabilities / assets,
      current_ratio: 1,
      quick_ratio: 0.5,
      cash_ratio: 0.1,
      gross_margin: 0.2,
      net_margin: profit / revenue,
      short_debt_cash_coverage: cash / shortLoans,
    },
    bank_credit_analysis: {
      overall_risk_level: 'medium',
      credit_view: '关注现金流。',
      risk_findings: [],
      positive_factors: [],
      negative_factors: [],
      missing_materials: [],
      suggested_credit_strategy: '审慎授信。',
    },
  };
}

const reports = [
  report('2022', 'annual', '2022-01-01', 84_697_985.94, 78_474_828.15, 6_223_157.79, 140_360_769.35, 429_625.06, -15_841_870.74, 3_507_503.11, 26_500_000),
  report('2023', 'annual', '2023-01-01', 69_320_214.02, 56_276_448.92, 13_043_765.10, 100_012_470.73, 6_690_607.31, -8_438_844.57, 2_000_000, 26_000_000),
  report('2024', 'quarterly', '2024-10-01', 54_688_482.62, 41_636_748.83, 13_051_733.79, 60_376_572.48, 7_968.69, -1_989_500.82, 150_161.66, 25_020_000),
];

describe('buildFinancialReportCustomerSummary', () => {
  it('aggregates all financial reports using latest balance and cumulative period metrics', () => {
    const summary = buildFinancialReportCustomerSummary(reports);
    const cards = Object.fromEntries(summary.topMetrics.map((item) => [item.label, item.value]));
    const totalAssets = summary.latestBalanceSheet.find((item) => item.label === '资产总计');

    expect(summary.title).toBe('财务数据总览');
    expect(summary.reportCount).toBe(3);
    expect(summary.baseInfo).toContainEqual(['已识别报表份数', '3份']);
    expect(summary.baseInfo).toContainEqual(['已识别报表期间', '2022年报、2023年报、2024季报']);
    expect(summary.baseInfo).toContainEqual(['覆盖期间', '2022-01-01 至 2024-12-31']);
    expect(summary.baseInfo).toContainEqual(['最新报表期', '2024-10-01 至 2024-12-31']);
    expect(cards['最新资产总计']).toBe(54_688_482.62);
    expect(cards['最新负债合计']).toBe(41_636_748.83);
    expect(cards['近三期累计营业收入']).toBe(300_749_812.56);
    expect(cards['近三期累计净利润']).toBe(7_128_201.06);
    expect(totalAssets?.latest).toBe(54_688_482.62);
    expect(totalAssets?.previous).toBe(69_320_214.02);
    expect(summary.incomeTrendRows.map((item) => item.period)).toEqual(['2022年报', '2023年报', '2024季报']);
    expect(summary.cashFlowTrendRows).toHaveLength(3);
    expect(summary.riskFlags.map((item) => item.title)).toContain('经营现金流连续为负');
    expect(summary.creditConclusion.conclusion).toContain('本分析基于客户名下全部财务报表资料自动汇总生成。');
  });

  it('marks a single available report as single-period analysis', () => {
    const summary = buildFinancialReportCustomerSummary([reports[2]]);

    expect(summary.title).toBe('单期财务报表分析');
    expect(summary.isSinglePeriod).toBe(true);
    expect(summary.reportCount).toBe(1);
  });
});
