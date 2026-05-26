export type DisplayRow = {
  label: string;
  value: number | null;
};

export type RatioRisk = 'normal' | 'weak' | 'risk';

export type RatioRow = {
  label: string;
  value: number | null;
  format: 'amount' | 'ratio' | 'multiple';
  judgment: RatioRisk;
  explanation: string;
};

export type RiskFlag = {
  level: string;
  title: string;
  evidence: string[];
  bankAttention: string;
};

export type TrendRow = {
  period: string;
  revenue: number | null;
  netProfit: number | null;
  operatingCashFlow: number | null;
  totalAssets: number | null;
  totalLiabilities: number | null;
};

export type FinancialReportRightPanel = {
  available: boolean;
  baseInfo: Array<[string, string]>;
  balanceSheetSummary: DisplayRow[];
  incomeStatementSummary: DisplayRow[];
  cashFlowSummary: DisplayRow[];
  coreRatios: RatioRow[];
  riskFlags: RiskFlag[];
  trendRows: TrendRow[];
  creditConclusion: {
    riskLevel: string;
    conclusion: string;
    positiveFactors: string[];
    negativeFactors: string[];
    missingMaterials: string[];
    strategy: string;
  };
};

type JsonRecord = Record<string, unknown>;

const EMPTY = '-';

function asRecord(value: unknown): JsonRecord {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as JsonRecord;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return asRecord(parsed);
    } catch {
      return {};
    }
  }
  return {};
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function text(value: unknown): string {
  const result = String(value ?? '').trim();
  return result || EMPTY;
}

function numberValue(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const parsed = typeof value === 'number' ? value : Number(String(value).replace(/,/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function amount(section: JsonRecord, key: string): number | null {
  const value = section[key];
  const item = asRecord(value);
  if (Object.keys(item).length > 0) {
    return numberValue(item.normalized_value ?? item['标准化数值'] ?? item.value);
  }
  return numberValue(value);
}

function mapValue(value: unknown): string {
  const values: Record<string, string> = {
    financial_report: '财务报表',
    financial_data: '财务报表',
    annual: '年报',
    quarterly: '季报',
    monthly: '月报',
    unknown: '未知',
    CNY: '人民币',
    enterprise_accounting_standard: '企业会计准则一般企业',
    small_enterprise_accounting_standard: '小企业会计准则',
  };
  const original = String(value ?? '').trim();
  return values[original] || original || EMPTY;
}

export function findFinancialReportStructuredData(raw: unknown, depth = 0): JsonRecord | null {
  if (depth > 6) return null;
  const data = asRecord(raw);
  if (!Object.keys(data).length) return null;
  const documentType = String(data.document_type ?? data.document_type_code ?? data.type ?? '').trim();
  if (
    (documentType === 'financial_report' || documentType === 'financial_data' || data.company_info) &&
    (data.balance_sheet || data.income_statement || data.financial_ratios)
  ) {
    return data;
  }
  for (const candidate of [
    data.structured_json,
    data.extracted_json,
    data.extracted_data,
    data.data,
    data.result,
    data.payload,
  ]) {
    const result = findFinancialReportStructuredData(candidate, depth + 1);
    if (result) return result;
  }
  return null;
}

export function hasFinancialReportStructuredData(raw: unknown): boolean {
  return Boolean(findFinancialReportStructuredData(raw));
}

function assessment(
  label: string,
  value: number | null,
  format: RatioRow['format'],
  judgment: RatioRisk,
  explanation: string,
): RatioRow {
  return { label, value, format, judgment, explanation };
}

function lowerIsRisk(value: number | null, risk: number, weak: number): RatioRisk {
  if (value === null) return 'weak';
  if (value < risk) return 'risk';
  if (value < weak) return 'weak';
  return 'normal';
}

function upperIsRisk(value: number | null, risk: number, weak: number): RatioRisk {
  if (value === null) return 'weak';
  if (value > risk) return 'risk';
  if (value > weak) return 'weak';
  return 'normal';
}

function reportPeriod(data: JsonRecord): string {
  const info = asRecord(data.company_info);
  return text(info.report_period_end || info.report_period_start);
}

function trendFromReport(data: JsonRecord): TrendRow {
  return {
    period: reportPeriod(data),
    revenue: amount(asRecord(data.income_statement), 'revenue'),
    netProfit: amount(asRecord(data.income_statement), 'net_profit'),
    operatingCashFlow: amount(asRecord(data.cash_flow_statement), 'net_operating_cash_flow'),
    totalAssets: amount(asRecord(data.balance_sheet), 'total_assets'),
    totalLiabilities: amount(asRecord(data.balance_sheet), 'total_liabilities'),
  };
}

function uniqueReports(inputs: unknown[]): JsonRecord[] {
  const reports: JsonRecord[] = [];
  const seen = new Set<string>();
  for (const input of inputs) {
    const data = findFinancialReportStructuredData(input);
    if (!data) continue;
    const info = asRecord(data.company_info);
    const key = [
      text(info.company_name),
      text(info.report_period_end),
      text(info.report_type),
      text(data.source_file),
    ].join('|');
    if (seen.has(key)) continue;
    seen.add(key);
    reports.push(data);
  }
  return reports.sort((a, b) => reportPeriod(a).localeCompare(reportPeriod(b)));
}

export function buildFinancialReportRightPanel(data: unknown, reports: unknown[] = []): FinancialReportRightPanel {
  const current = findFinancialReportStructuredData(data);
  const collected = uniqueReports([...reports, data]);
  const latest = current || collected[collected.length - 1] || null;
  if (!latest) {
    return {
      available: false,
      baseInfo: [],
      balanceSheetSummary: [],
      incomeStatementSummary: [],
      cashFlowSummary: [],
      coreRatios: [],
      riskFlags: [],
      trendRows: [],
      creditConclusion: {
        riskLevel: 'unknown',
        conclusion: '',
        positiveFactors: [],
        negativeFactors: [],
        missingMaterials: [],
        strategy: '',
      },
    };
  }

  const info = asRecord(latest.company_info);
  const balance = asRecord(latest.balance_sheet);
  const income = asRecord(latest.income_statement);
  const cash = asRecord(latest.cash_flow_statement);
  const ratios = asRecord(latest.financial_ratios);
  const analysis = asRecord(latest.bank_credit_analysis);
  const ratio = (key: string) => numberValue(ratios[key]);
  const interestDebt = ratio('interest_bearing_debt');

  const balanceSheetSummary: DisplayRow[] = [
    ['货币资金', 'cash_and_equivalents'],
    ['应收账款', 'accounts_receivable'],
    ['预付款项', 'prepayments'],
    ['其他应收款', 'other_receivables'],
    ['存货', 'inventory'],
    ['流动资产合计', 'current_assets_total'],
    ['短期借款', 'short_term_loans'],
    ['应付账款', 'accounts_payable'],
    ['流动负债合计', 'current_liabilities_total'],
    ['负债合计', 'total_liabilities'],
    ['所有者权益合计', 'total_equity'],
    ['资产总计', 'total_assets'],
  ].map(([label, key]) => ({ label, value: amount(balance, key) }));

  const incomeStatementSummary: DisplayRow[] = [
    ['营业收入', 'revenue'],
    ['营业成本', 'operating_cost'],
    ['毛利', 'gross_profit'],
    ['销售费用', 'selling_expenses'],
    ['管理费用', 'admin_expenses'],
    ['研发费用', 'rd_expenses'],
    ['财务费用', 'finance_expenses'],
    ['营业利润', 'operating_profit'],
    ['利润总额', 'total_profit'],
    ['净利润', 'net_profit'],
  ].map(([label, key]) => ({
    label,
    value: key === 'gross_profit' ? ratio(key) : amount(income, key),
  }));

  const cashFlowSummary: DisplayRow[] = [
    ['经营活动产生的现金流量净额', 'net_operating_cash_flow'],
    ['投资活动产生的现金流量净额', 'net_investing_cash_flow'],
    ['筹资活动产生的现金流量净额', 'net_financing_cash_flow'],
    ['现金及现金等价物净增加额', 'net_cash_increase'],
    ['期末现金及现金等价物余额', 'ending_cash_balance'],
  ].map(([label, key]) => ({ label, value: amount(cash, key) }));

  const coreRatios: RatioRow[] = [
    assessment('资产负债率', ratio('asset_liability_ratio'), 'ratio', upperIsRisk(ratio('asset_liability_ratio'), 0.7, 0.6), '负债占总资产比例，越高偿债压力越大。'),
    assessment('流动比率', ratio('current_ratio'), 'multiple', lowerIsRisk(ratio('current_ratio'), 1, 1.5), '流动资产对短期负债的覆盖程度。'),
    assessment('速动比率', ratio('quick_ratio'), 'multiple', lowerIsRisk(ratio('quick_ratio'), 0.7, 1), '剔除存货后短期偿债覆盖程度。'),
    assessment('现金比率', ratio('cash_ratio'), 'multiple', lowerIsRisk(ratio('cash_ratio'), 0.1, 0.2), '可立即动用现金对流动负债的覆盖程度。'),
    assessment('毛利率', ratio('gross_margin'), 'ratio', lowerIsRisk(ratio('gross_margin'), 0, 0.1), '主营业务毛利空间。'),
    assessment('净利率', ratio('net_margin'), 'ratio', lowerIsRisk(ratio('net_margin'), 0, 0.03), '收入转化为净利润的能力。'),
    assessment('经营现金流收入比', ratio('operating_cash_flow_to_revenue'), 'ratio', lowerIsRisk(ratio('operating_cash_flow_to_revenue'), 0, 0.05), '经营现金流与收入的匹配程度。'),
    assessment('销售收现比', ratio('sales_cash_collection_ratio'), 'ratio', lowerIsRisk(ratio('sales_cash_collection_ratio'), 0.8, 1), '销售收入实现现金回款的比例。'),
    assessment('短期借款现金覆盖率', ratio('short_debt_cash_coverage'), 'multiple', lowerIsRisk(ratio('short_debt_cash_coverage'), 0.1, 0.3), '货币资金对短期借款的覆盖程度。'),
    assessment('有息负债', interestDebt, 'amount', interestDebt === null ? 'weak' : 'normal', '短期借款、长期借款及一年内到期债务合计。'),
    assessment('总资产周转率', ratio('total_asset_turnover'), 'multiple', lowerIsRisk(ratio('total_asset_turnover'), 0.3, 0.5), '总资产形成营业收入的效率。'),
  ];

  const questions = asArray(analysis.key_bank_questions).map((item) => text(item)).filter((item) => item !== EMPTY);
  const riskFlags = asArray(analysis.risk_findings).map((item, index) => {
    const finding = asRecord(item);
    return {
      level: String(finding.risk_level || 'medium'),
      title: text(finding.title || `风险提示 ${index + 1}`),
      evidence: asArray(finding.evidence).map((line) => text(line)).filter((line) => line !== EMPTY),
      bankAttention: text(finding.suggestion || questions[index] || questions[0]),
    };
  });
  const missingMaterials = asArray(analysis.missing_materials).map((item) => {
    const material = asRecord(item);
    return text(material.material || item);
  }).filter((item) => item !== EMPTY);

  return {
    available: true,
    baseInfo: [
      ['企业名称', text(info.company_name)],
      ['资料类型', mapValue(latest.document_type || 'financial_report')],
      ['报表类型', mapValue(info.report_type)],
      ['会计准则', mapValue(info.accounting_standard)],
      ['所属期开始日期', text(info.report_period_start)],
      ['所属期结束日期', text(info.report_period_end)],
      ['报送日期/报表日', text(info.report_date)],
      ['币种', mapValue(info.currency)],
      ['金额单位', text(info.unit)],
    ],
    balanceSheetSummary,
    incomeStatementSummary,
    cashFlowSummary,
    coreRatios,
    riskFlags,
    trendRows: uniqueReports([...collected, latest]).map(trendFromReport),
    creditConclusion: {
      riskLevel: String(analysis.overall_risk_level || 'low'),
      conclusion: text(analysis.credit_view),
      positiveFactors: asArray(analysis.positive_factors).map((item) => text(item)).filter((item) => item !== EMPTY),
      negativeFactors: asArray(analysis.negative_factors).map((item) => text(item)).filter((item) => item !== EMPTY),
      missingMaterials,
      strategy: text(analysis.suggested_credit_strategy),
    },
  };
}
