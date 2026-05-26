export type RatioRisk = 'normal' | 'weak' | 'risk';

export type SummaryMetric = {
  label: string;
  value: number | null;
};

export type BalanceChangeRow = {
  label: string;
  latest: number | null;
  previous: number | null;
  change: number | null;
  changeRate: number | null;
};

export type IncomeTrendRow = {
  period: string;
  revenue: number | null;
  operatingCost: number | null;
  grossProfit: number | null;
  netProfit: number | null;
  grossMargin: number | null;
  netMargin: number | null;
};

export type CashFlowTrendRow = {
  period: string;
  operatingCashFlow: number | null;
  investingCashFlow: number | null;
  financingCashFlow: number | null;
  endingCashBalance: number | null;
};

export type RatioRow = {
  label: string;
  value: number | string | null;
  format: 'amount' | 'ratio' | 'multiple' | 'text';
  judgment: RatioRisk;
  explanation: string;
};

export type RiskFlag = {
  level: string;
  title: string;
  evidence: string[];
  bankAttention: string;
};

export type FinancialReportCustomerSummary = {
  available: boolean;
  isSinglePeriod: boolean;
  reportCount: number;
  title: string;
  subtitle: string;
  topMetrics: SummaryMetric[];
  baseInfo: Array<[string, string]>;
  latestBalanceSheet: BalanceChangeRow[];
  incomeTrendRows: IncomeTrendRow[];
  cashFlowTrendRows: CashFlowTrendRow[];
  coreRatios: RatioRow[];
  riskFlags: RiskFlag[];
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
      return asRecord(JSON.parse(value));
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

function previousAmount(section: JsonRecord, key: string): number | null {
  const value = asRecord(section[key]);
  return numberValue(value.previous_normalized_value ?? value.compare_value ?? value['对比列标准化数值']);
}

function balanceAmount(section: JsonRecord, key: string): number | null {
  if (key !== 'total_equity') return amount(section, key);
  const directCandidates = [
    amount(section, 'total_equity'),
    amount(asRecord(section.equity), 'total_equity'),
    amount(section, 'total_owners_equity'),
    amount(section, 'owners_equity_total'),
  ];
  for (const value of directCandidates) {
    if (value !== null) return value;
  }
  const assets = amount(section, 'total_assets');
  const liabilities = amount(section, 'total_liabilities');
  return assets !== null && liabilities !== null ? Math.round((assets - liabilities) * 100) / 100 : null;
}

function previousBalanceAmount(section: JsonRecord, key: string): number | null {
  if (key !== 'total_equity') return previousAmount(section, key);
  const directCandidates = [
    previousAmount(section, 'total_equity'),
    previousAmount(asRecord(section.equity), 'total_equity'),
    previousAmount(section, 'total_owners_equity'),
    previousAmount(section, 'owners_equity_total'),
  ];
  for (const value of directCandidates) {
    if (value !== null) return value;
  }
  const assets = previousAmount(section, 'total_assets');
  const liabilities = previousAmount(section, 'total_liabilities');
  return assets !== null && liabilities !== null ? Math.round((assets - liabilities) * 100) / 100 : null;
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
    general_enterprise: '企业会计准则一般企业',
    small_enterprise_accounting_standard: '小企业会计准则',
  };
  const original = String(value ?? '').trim();
  return values[original] || original || EMPTY;
}

function info(data: JsonRecord): JsonRecord {
  return asRecord(data.company_info);
}

function reportSortKey(data: JsonRecord): string {
  const company = info(data);
  return String(company.report_period_end || company.report_date || company.report_period_start || '');
}

function periodRange(data: JsonRecord): string {
  const company = info(data);
  const start = text(company.report_period_start);
  const end = text(company.report_period_end);
  return start === EMPTY && end === EMPTY ? EMPTY : `${start} 至 ${end}`;
}

function periodLabel(data: JsonRecord): string {
  const company = info(data);
  const end = String(company.report_period_end || company.report_date || '').trim();
  const year = end.slice(0, 4);
  const type = mapValue(company.report_type);
  return year && type !== EMPTY && type !== '未知' ? `${year}${type}` : periodRange(data);
}

function change(latest: number | null, previous: number | null): number | null {
  return latest === null || previous === null ? null : latest - previous;
}

function changeRate(latest: number | null, previous: number | null): number | null {
  const delta = change(latest, previous);
  return delta === null || previous === null || previous === 0 ? null : delta / Math.abs(previous);
}

function sum(values: Array<number | null>): number | null {
  const present = values.filter((value): value is number => value !== null);
  return present.length ? Math.round(present.reduce((total, value) => total + value, 0) * 100) / 100 : null;
}

function divide(numerator: number | null, denominator: number | null): number | null {
  return numerator === null || denominator === null || denominator === 0 ? null : numerator / denominator;
}

function assessment(
  label: string,
  value: number | string | null,
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

function uniqueReports(inputs: unknown[]): JsonRecord[] {
  const reports: JsonRecord[] = [];
  const seen = new Set<string>();
  for (const input of inputs) {
    const data = findFinancialReportStructuredData(input);
    if (!data) continue;
    const company = info(data);
    const key = [
      text(company.company_name),
      text(company.report_period_start),
      text(company.report_period_end),
      text(company.report_type),
      text(data.source_file),
    ].join('|');
    if (seen.has(key)) continue;
    seen.add(key);
    reports.push(data);
  }
  return reports.sort((a, b) => reportSortKey(a).localeCompare(reportSortKey(b)));
}

function addRisk(flags: RiskFlag[], flag: RiskFlag): void {
  if (!flags.some((item) => item.title === flag.title)) flags.push(flag);
}

function riskFlag(level: string, title: string, evidence: string[], bankAttention: string): RiskFlag {
  return { level, title, evidence, bankAttention };
}

export function buildFinancialReportCustomerSummary(inputs: unknown[]): FinancialReportCustomerSummary {
  const reports = uniqueReports(inputs);
  const latest = reports[reports.length - 1] || null;
  const previous = reports[reports.length - 2] || null;
  if (!latest) {
    return {
      available: false,
      isSinglePeriod: true,
      reportCount: 0,
      title: '财务数据总览',
      subtitle: '基于客户名下全部财务报表自动汇总',
      topMetrics: [],
      baseInfo: [],
      latestBalanceSheet: [],
      incomeTrendRows: [],
      cashFlowTrendRows: [],
      coreRatios: [],
      riskFlags: [],
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

  const latestInfo = info(latest);
  const latestBalance = asRecord(latest.balance_sheet);
  const previousBalance = asRecord(previous?.balance_sheet);
  const latestRatios = asRecord(latest.financial_ratios);
  const latestAnalysis = asRecord(latest.bank_credit_analysis);
  const latestValue = (section: JsonRecord, key: string) => balanceAmount(section, key);
  const ratio = (key: string) => numberValue(latestRatios[key]);
  const latestThree = reports.slice(-3);

  const cumulativeRevenue = sum(latestThree.map((report) => amount(asRecord(report.income_statement), 'revenue')));
  const cumulativeNetProfit = sum(latestThree.map((report) => amount(asRecord(report.income_statement), 'net_profit')));
  const cumulativeOperatingCashFlow = sum(latestThree.map((report) => amount(asRecord(report.cash_flow_statement), 'net_operating_cash_flow')));

  const balanceFields: Array<[string, string]> = [
    ['货币资金', 'cash_and_equivalents'],
    ['应收账款', 'accounts_receivable'],
    ['预付款项', 'prepayments'],
    ['其他应收款', 'other_receivables'],
    ['存货', 'inventory'],
    ['流动资产合计', 'current_assets_total'],
    ['资产总计', 'total_assets'],
    ['短期借款', 'short_term_loans'],
    ['长期借款', 'long_term_loans'],
    ['应付账款', 'accounts_payable'],
    ['流动负债合计', 'current_liabilities_total'],
    ['负债合计', 'total_liabilities'],
    ['所有者权益合计', 'total_equity'],
  ];
  const latestBalanceSheet = balanceFields.map(([label, key]) => {
    const latestAmount = latestValue(latestBalance, key);
    const priorAmount = previousBalanceAmount(latestBalance, key) ?? latestValue(previousBalance, key);
    return {
      label,
      latest: latestAmount,
      previous: priorAmount,
      change: change(latestAmount, priorAmount),
      changeRate: changeRate(latestAmount, priorAmount),
    };
  });

  const incomeTrendRows = reports.map((report) => {
    const statement = asRecord(report.income_statement);
    const revenue = amount(statement, 'revenue');
    const cost = amount(statement, 'operating_cost');
    const grossProfit = revenue !== null && cost !== null ? revenue - cost : null;
    const netProfit = amount(statement, 'net_profit');
    return {
      period: periodLabel(report),
      revenue,
      operatingCost: cost,
      grossProfit,
      netProfit,
      grossMargin: divide(grossProfit, revenue),
      netMargin: divide(netProfit, revenue),
    };
  });
  const cashFlowTrendRows = reports.map((report) => {
    const statement = asRecord(report.cash_flow_statement);
    return {
      period: periodLabel(report),
      operatingCashFlow: amount(statement, 'net_operating_cash_flow'),
      investingCashFlow: amount(statement, 'net_investing_cash_flow'),
      financingCashFlow: amount(statement, 'net_financing_cash_flow'),
      endingCashBalance: amount(statement, 'ending_cash_balance'),
    };
  });

  const ocfValues = cashFlowTrendRows.map((row) => row.operatingCashFlow).filter((value): value is number => value !== null);
  const cashValues = reports.map((report) => amount(asRecord(report.balance_sheet), 'cash_and_equivalents')).filter((value): value is number => value !== null);
  const revenueValues = incomeTrendRows.map((row) => row.revenue).filter((value): value is number => value !== null);
  const netProfitValues = incomeTrendRows.map((row) => row.netProfit).filter((value): value is number => value !== null);
  const assetValues = reports.map((report) => amount(asRecord(report.balance_sheet), 'total_assets')).filter((value): value is number => value !== null);
  const financingValues = cashFlowTrendRows.map((row) => row.financingCashFlow);
  const consecutiveNegativeOperatingCash = ocfValues.length >= 2 && ocfValues.every((value) => value < 0);
  const decliningCash = cashValues.length >= 2 && cashValues.every((value, index) => index === 0 || value < cashValues[index - 1]);
  const decliningRevenue = revenueValues.length >= 2 && revenueValues.every((value, index) => index === 0 || value < revenueValues[index - 1]);
  const decliningAssets = assetValues.length >= 2 && assetValues.every((value, index) => index === 0 || value < assetValues[index - 1]);
  const financingSupportsOperations = cashFlowTrendRows.some((row, index) => (
    row.operatingCashFlow !== null && row.operatingCashFlow < 0 &&
    financingValues[index] !== null && (financingValues[index] as number) > 0
  ));
  const latestAssets = amount(latestBalance, 'total_assets');
  const latestCash = amount(latestBalance, 'cash_and_equivalents');
  const latestShortLoans = amount(latestBalance, 'short_term_loans');
  const otherReceivableRatio = divide(amount(latestBalance, 'other_receivables'), latestAssets);
  const prepaymentRatio = divide(amount(latestBalance, 'prepayments'), latestAssets);
  const shortDebtCoverage = ratio('short_debt_cash_coverage') ?? divide(latestCash, latestShortLoans);
  const latestDebtRatio = ratio('asset_liability_ratio');

  const riskFlags: RiskFlag[] = [];
  for (const raw of asArray(latestAnalysis.risk_findings)) {
    const finding = asRecord(raw);
    addRisk(riskFlags, riskFlag(
      String(finding.risk_level || 'medium'),
      text(finding.title),
      asArray(finding.evidence).map((item) => text(item)).filter((item) => item !== EMPTY),
      text(finding.suggestion),
    ));
  }
  if (consecutiveNegativeOperatingCash) {
    addRisk(riskFlags, riskFlag('high', '经营现金流连续为负', [`连续 ${ocfValues.length} 期经营现金流均为负。`], '关注经营回款真实性和后续现金流改善安排。'));
  }
  if (decliningCash) {
    addRisk(riskFlags, riskFlag('medium_high', '货币资金持续下降', ['各期货币资金呈持续下降趋势。'], '核查可动用现金、受限资金及短期偿债来源。'));
  }
  if (latestShortLoans !== null && latestAssets !== null && latestShortLoans / latestAssets > 0.3) {
    addRisk(riskFlags, riskFlag('medium_high', '最新期短期借款较高', [`短期借款占最新资产总额 ${(latestShortLoans / latestAssets * 100).toFixed(2)}%。`], '关注借款到期分布及续贷依赖。'));
  }
  if (otherReceivableRatio !== null && otherReceivableRatio > 0.2) {
    addRisk(riskFlags, riskFlag('medium_high', '其他应收款占比偏高', [`其他应收款占最新资产总额 ${(otherReceivableRatio * 100).toFixed(2)}%。`], '核查对手方、账龄和关联方资金占用。'));
  }
  if (prepaymentRatio !== null && prepaymentRatio > 0.2) {
    addRisk(riskFlags, riskFlag('medium_high', '预付款项占比偏高', [`预付款项占最新资产总额 ${(prepaymentRatio * 100).toFixed(2)}%。`], '核验合同、付款依据和对应货物交付情况。'));
  }
  if (netProfitValues.length >= 2 && Math.abs(netProfitValues[netProfitValues.length - 1]) < Math.abs(netProfitValues[netProfitValues.length - 2]) * 0.5) {
    addRisk(riskFlags, riskFlag('medium_high', '净利润波动较大', ['最新期净利润较上一期显著下降。'], '说明盈利波动原因及后续盈利可持续性。'));
  }
  if (decliningRevenue) {
    addRisk(riskFlags, riskFlag('medium_high', '收入规模下降', ['营业收入在已识别期间连续下降。'], '核查订单、客户稳定性及收入下降原因。'));
  }
  if (financingSupportsOperations) {
    addRisk(riskFlags, riskFlag('high', '筹资现金流补经营现金流缺口', ['存在经营现金流为负且筹资现金流为正的期间。'], '关注外部融资依赖及还款资金闭环。'));
  }
  if (decliningAssets) {
    addRisk(riskFlags, riskFlag('medium_high', '资产总额下降', ['资产总额在已识别期间连续下降。'], '了解资产收缩原因及可抵质押资产变化。'));
  }
  if (shortDebtCoverage !== null && shortDebtCoverage < 0.2) {
    addRisk(riskFlags, riskFlag('high', '现金余额对短期借款覆盖不足', [`最新短期借款现金覆盖率 ${(shortDebtCoverage * 100).toFixed(2)}%。`], '核查短期借款还款安排及备用流动性。'));
  }

  const coreRatios: RatioRow[] = [
    assessment('最新资产负债率', latestDebtRatio, 'ratio', upperIsRisk(latestDebtRatio, 0.7, 0.6), '以最新一期资产负债表为准，反映杠杆水平。'),
    assessment('最新流动比率', ratio('current_ratio'), 'multiple', lowerIsRisk(ratio('current_ratio'), 1, 1.5), '以最新一期为准，反映短期偿债能力。'),
    assessment('最新速动比率', ratio('quick_ratio'), 'multiple', lowerIsRisk(ratio('quick_ratio'), 0.7, 1), '以最新一期为准，剔除存货后的短债覆盖。'),
    assessment('最新现金比率', ratio('cash_ratio'), 'multiple', lowerIsRisk(ratio('cash_ratio'), 0.1, 0.2), '以最新一期为准，反映即时偿债能力。'),
    assessment('最新毛利率', ratio('gross_margin'), 'ratio', lowerIsRisk(ratio('gross_margin'), 0, 0.1), '以最新一期利润表为准，关注盈利空间。'),
    assessment('最新净利率', ratio('net_margin'), 'ratio', lowerIsRisk(ratio('net_margin'), 0, 0.03), '以最新一期利润表为准，关注盈利质量。'),
    assessment('经营现金流连续性', consecutiveNegativeOperatingCash ? '连续为负' : '未见连续为负', 'text', consecutiveNegativeOperatingCash ? 'risk' : 'normal', `已检查 ${ocfValues.length} 期经营现金流。`),
    assessment('近三期累计经营现金流', cumulativeOperatingCashFlow, 'amount', cumulativeOperatingCashFlow !== null && cumulativeOperatingCashFlow < 0 ? 'risk' : 'normal', '期间指标按最近三份已识别报表累计。'),
    assessment('短期借款现金覆盖率', shortDebtCoverage, 'multiple', lowerIsRisk(shortDebtCoverage, 0.1, 0.3), '最新期货币资金对短期借款的覆盖水平。'),
    assessment('其他应收款占总资产比例', otherReceivableRatio, 'ratio', upperIsRisk(otherReceivableRatio, 0.2, 0.1), '以最新一期资产余额测算。'),
    assessment('预付款项占总资产比例', prepaymentRatio, 'ratio', upperIsRisk(prepaymentRatio, 0.2, 0.1), '以最新一期资产余额测算。'),
  ];

  const inheritedPositive = asArray(latestAnalysis.positive_factors).map((item) => text(item)).filter((item) => item !== EMPTY);
  const inheritedNegative = asArray(latestAnalysis.negative_factors).map((item) => text(item)).filter((item) => item !== EMPTY);
  const negativeFactors = [...new Set([...inheritedNegative, ...riskFlags.map((item) => item.title)])];
  const missingMaterials = asArray(latestAnalysis.missing_materials).map((item) => {
    const material = asRecord(item);
    return text(material.material || item);
  }).filter((item) => item !== EMPTY);
  const riskLevel = riskFlags.some((item) => item.level === 'high') ? 'high'
    : riskFlags.length > 0 ? 'medium_high'
    : String(latestAnalysis.overall_risk_level || 'low');
  const aggregationNotice = '本分析基于客户名下全部财务报表资料自动汇总生成。';

  return {
    available: true,
    isSinglePeriod: reports.length === 1,
    reportCount: reports.length,
    title: reports.length === 1 ? '单期财务报表分析' : '财务数据总览',
    subtitle: reports.length === 1 ? '当前客户仅识别到 1 份财务报表' : '基于客户名下全部财务报表自动汇总',
    topMetrics: [
      { label: '最新资产总计', value: amount(latestBalance, 'total_assets') },
      { label: '最新负债合计', value: amount(latestBalance, 'total_liabilities') },
      { label: '近三期累计营业收入', value: cumulativeRevenue },
      { label: '近三期累计净利润', value: cumulativeNetProfit },
    ],
    baseInfo: [
      ['企业名称', text(latestInfo.company_name)],
      ['资料类型', '财务报表'],
      ['已识别报表份数', `${reports.length}份`],
      ['已识别报表期间', reports.map(periodLabel).join('、')],
      ['覆盖期间', `${text(info(reports[0]).report_period_start)} 至 ${text(latestInfo.report_period_end)}`],
      ['最新报表期', periodRange(latest)],
      ['最新报表日', text(latestInfo.report_date)],
      ['会计准则', mapValue(latestInfo.accounting_standard)],
      ['币种', mapValue(latestInfo.currency)],
      ['金额单位', text(latestInfo.unit)],
      ['综合风险', riskLevel === 'high' ? '高' : riskLevel === 'medium_high' || riskLevel === 'medium' ? '中' : '低'],
    ],
    latestBalanceSheet,
    incomeTrendRows,
    cashFlowTrendRows,
    coreRatios,
    riskFlags,
    creditConclusion: {
      riskLevel,
      conclusion: `${aggregationNotice}${text(latestAnalysis.credit_view) === EMPTY ? '' : ` ${text(latestAnalysis.credit_view)}`}`,
      positiveFactors: inheritedPositive,
      negativeFactors,
      missingMaterials,
      strategy: text(latestAnalysis.suggested_credit_strategy),
    },
  };
}

export function buildFinancialReportRightPanel(data: unknown, reports: unknown[] = []): FinancialReportCustomerSummary {
  return buildFinancialReportCustomerSummary([...reports, data]);
}
