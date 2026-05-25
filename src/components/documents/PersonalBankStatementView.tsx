import React from 'react';
import {
  BadgeCheck,
  Banknote,
  CircleDollarSign,
  FileWarning,
  Landmark,
  ReceiptText,
  TrendingDown,
  TrendingUp,
  WalletCards,
} from 'lucide-react';

type PersonalBankStatementViewProps = {
  data?: Record<string, unknown> | null;
  summary?: Record<string, unknown> | null;
  selectedDoc?: Record<string, unknown> | null;
  markdownText?: string;
  customerId?: string | null;
  markdown?: string;
  loading?: boolean;
  error?: string | null;
  showHeader?: boolean;
};

const EMPTY = '--';

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function asArray<T = Record<string, unknown>>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function normalizeWarnings(value: unknown): string[] {
  return asArray<unknown>(value)
    .map((item) => {
      if (typeof item === 'string') return item;
      const record = asRecord(item);
      return String(record.message || record.evidence || record.code || '').trim();
    })
    .filter(Boolean);
}

function parseMaybeJson(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function firstValue(...values: unknown[]): unknown {
  return values.find((value) => value !== undefined && value !== null && value !== '');
}

function toNumber(value: unknown): number {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (typeof value === 'string') {
    const cleaned = value
      .replace(/,/g, '')
      .replace(/¥/g, '')
      .replace(/￥/g, '')
      .trim();
    const number = Number(cleaned);
    return Number.isFinite(number) ? number : 0;
  }
  return 0;
}

function absAmount(value: unknown): number {
  return Math.abs(toNumber(value));
}

function numberValue(...values: unknown[]): number {
  return toNumber(firstValue(...values));
}

function hasValue(value: unknown): boolean {
  return value !== undefined && value !== null && value !== '';
}

function pickNumber(...values: unknown[]): number {
  const value = values.find(hasValue);
  return toNumber(value);
}

function pickAbsNumber(...values: unknown[]): number {
  const value = values.find(hasValue);
  return absAmount(value);
}

function pickMeaningfulNumber(...values: unknown[]): number {
  const value = values.find((item) => hasValue(item) && toNumber(item) !== 0);
  return value === undefined ? pickNumber(...values) : toNumber(value);
}

function pickMeaningfulAbsNumber(...values: unknown[]): number {
  const value = values.find((item) => hasValue(item) && absAmount(item) !== 0);
  return value === undefined ? pickAbsNumber(...values) : absAmount(value);
}

function display(value: unknown): string {
  const text = String(value ?? '').trim();
  return text || EMPTY;
}

function money(...values: unknown[]): string {
  const number = numberValue(...values);
  return new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency: 'CNY',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(number);
}

function percent(...values: unknown[]): string {
  const number = numberValue(...values);
  return `${(number * 100).toFixed(2)}%`;
}

function riskMeta(level?: unknown) {
  const value = String(level || '').toLowerCase();
  if (value === 'high') return { label: '高风险', className: 'border-rose-200 bg-rose-50 text-rose-700' };
  if (value === 'medium') return { label: '中风险', className: 'border-amber-200 bg-amber-50 text-amber-700' };
  return { label: '低风险', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
}

const FLOW_NATURE_LABELS: Record<string, string> = {
  salary_flow: '工资流水',
  operating_flow: '经营流水',
  repayment_account_flow: '还款账户流水',
  mixed_flow: '混合流水',
  unknown: '无法判断',
};

const RETENTION_LABELS: Record<string, string> = {
  strong: '较强',
  medium: '一般',
  weak: '较弱',
  unknown: '无法判断',
};

const RISK_CODE_LABELS: Record<string, string> = {
  income_source_unclear: '收入来源不明',
  weak_verified_income: '可采信收入弱',
  repayment_account_flow: '还款账户流水',
  high_loan_repayment_ratio: '贷款还款占比高',
  fast_in_fast_out: '快进快出',
  weak_cash_retention: '账户沉淀弱',
  income_expense_highly_matched: '收入支出高度匹配',
  cannot_use_as_primary_income_proof: '不建议作为主收入证明',
};

const Section: React.FC<{ title: string; children: React.ReactNode; action?: React.ReactNode }> = ({ title, children, action }) => (
  <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
    <div className="mb-3 flex items-center justify-between gap-3">
      <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
      {action}
    </div>
    {children}
  </section>
);

const Metric: React.FC<{ label: string; help?: string; value: string; icon: React.ReactNode; tone: string }> = ({ label, help, value, icon, tone }) => (
  <div className={`rounded-xl border p-4 ${tone}`}>
    <div className="flex items-center gap-2 text-xs font-medium">{icon}{label}</div>
    <div className="mt-2 text-xl font-semibold tracking-normal">{value}</div>
    {help ? <div className="mt-1 text-xs opacity-80">{help}</div> : null}
  </div>
);

function InfoGrid({ rows }: { rows: Array<[string, string]> }) {
  return (
    <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
      {rows.map(([label, value]) => (
        <div key={label} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
          <div className="text-xs text-slate-500">{label}</div>
          <div className="mt-1 break-words font-semibold text-slate-800">{value}</div>
        </div>
      ))}
    </div>
  );
}

function AlertBox({ tone = 'amber', children }: { tone?: 'amber' | 'rose' | 'blue'; children: React.ReactNode }) {
  const className = tone === 'rose'
    ? 'border-rose-200 bg-rose-50 text-rose-800'
    : tone === 'blue'
      ? 'border-blue-200 bg-blue-50 text-blue-800'
      : 'border-amber-200 bg-amber-50 text-amber-800';
  return <div className={`rounded-lg border px-3 py-2 text-sm ${className}`}>{children}</div>;
}

type NormalizedTx = {
  summary: string;
  direction: string;
  amount: number;
  debitAmount: number;
  creditAmount: number;
  transactionDate: string;
  counterpartyName: string;
};

type NormalizedPersonalFlowSummary = {
  totalIncome: number;
  totalExpense: number;
  netCashFlow: number;
  incomeCount: number;
  expenseCount: number;
  avgMonthlyIncome: number;
  avgMonthlyExpense: number;
  verifiedIncome: number;
  avgMonthlyVerifiedIncome: number;
  confirmedSalaryIncome: number;
  suspectedSalaryIncome: number;
  lowConfidenceSuspectedSalaryIncome: number;
  verifiedSalaryIncome: number;
  verifiedOperatingIncome: number;
  verifiedOtherStableIncome: number;
  salaryMonths: number;
  salaryAvgMonthlyAmount: number;
  salaryConfidence: number;
  salarySources: Record<string, unknown>[];
  salaryDetectionNotes: string[];
  unknownInflow: number;
  interestIncome: number;
  loanRepaymentExpense: number;
  quickPaymentExpense: number;
  creditCardRepaymentExpense: number;
  transactions: NormalizedTx[];
};

const LOAN_REPAYMENT_KEYWORDS = ['个贷还款', '贷款回收', '贷款还款', '贷款扣款', '按揭', '房贷', '车贷', '小贷', '消费贷', '网贷还款', '还本', '还息'];
const QUICK_PAYMENT_KEYWORDS = ['快捷支付', '消费', 'POS', '支付宝', '微信支付'];
const UNKNOWN_INFLOW_KEYWORDS = ['汇款汇入', '转账收入', '跨行汇入', '他行汇入'];
const INTEREST_INCOME_KEYWORDS = ['存款利息', '结息'];
const SUSPECTED_SALARY_KEYWORDS = ['代发款项', '代发', '批量代发', '企业代发', '单位代发', '代发入账', '代发业务', '对公代发', '银联代付', '网联收款', '代付入账', '批量入账'];
const EMPLOYER_COUNTERPARTY_KEYWORDS = ['有限公司', '有限责任公司', '股份有限公司', '集团', '公司', '科技', '软件', '信息', '网络', '工程', '建筑', '实业', '商贸', '贸易', '人力资源', '劳务', '工厂', '厂', '银行股份有限公司', '代发工资专户'];

function containsAny(text: string, keywords: string[]): boolean {
  return keywords.some((keyword) => text.includes(keyword));
}

function getScaleSummary(raw: Record<string, unknown>): Record<string, unknown> {
  const summary = asRecord(raw.summary);
  const extractedJson = asRecord(raw.extracted_json);
  return asRecord(
    raw['收支规模汇总'] ||
    summary['收支规模汇总'] ||
    extractedJson['收支规模汇总']
  );
}

function extractPersonalFlowFromMarkdown(markdownText = ''): Record<string, unknown> {
  if (!markdownText || !markdownText.includes('收支规模汇总')) {
    return {};
  }
  const fields = [
    '总收入金额',
    '总收入笔数',
    '总支出金额',
    '总支出笔数',
    '净现金流',
    '月均收入',
    '月均支出',
    '最大单笔收入金额',
    '最大单笔支出金额',
  ];
  const scaleSummary: Record<string, string> = {};
  for (const field of fields) {
    const pattern = new RegExp(`["“]?${field}["”]?\\s*[:：]\\s*["“]?([-+]?\\d[\\d,]*(?:\\.\\d+)?)`, 'i');
    const match = markdownText.match(pattern);
    if (match?.[1]) {
      scaleSummary[field] = match[1];
    }
  }
  return Object.keys(scaleSummary).length ? { 收支规模汇总: scaleSummary } : {};
}

function getTransactionList(raw: Record<string, unknown>): Record<string, unknown>[] {
  const summary = asRecord(raw.summary);
  const extractedJson = asRecord(raw.extracted_json);
  return asArray<Record<string, unknown>>(
    raw['交易明细列表'] ||
    raw['三、交易明细列表'] ||
    raw.transactions ||
    summary['交易明细列表'] ||
    summary['三、交易明细列表'] ||
    extractedJson['交易明细列表'] ||
    extractedJson['三、交易明细列表'] ||
    extractedJson.transactions ||
    asRecord(raw.analysis_result)['交易明细列表'] ||
    asRecord(raw.result_json)['交易明细列表']
  );
}

function normalizeTransaction(tx: Record<string, unknown>): NormalizedTx {
  const summary = String(tx.summary || tx['摘要'] || tx['交易摘要'] || tx.description || '').trim();
  const counterpartyName = String(tx.counterparty_name || tx.counterpartyName || tx.counterparty || tx['对手信息'] || tx['对方户名'] || tx['对方名称'] || tx['交易对手'] || tx['对手方'] || tx['Counter Party'] || tx.Counterparty || '').trim();
  const transactionDate = String(tx.transaction_date || tx.transactionDate || tx['交易日期'] || tx.date || '').slice(0, 10);
  const rawDirection = String(tx.direction || tx['收支'] || tx.transaction_direction || '').trim();
  const debit = absAmount(tx.debit_amount ?? tx.debitAmount);
  const credit = absAmount(tx.credit_amount ?? tx.creditAmount);
  const signedAmount = toNumber(tx.amount ?? tx['金额'] ?? tx.transaction_amount ?? tx['交易金额']);
  const amount = absAmount(tx.amount ?? tx['金额'] ?? tx.transaction_amount ?? tx['交易金额'] ?? (credit || debit));
  const direction = rawDirection || (credit > 0 || signedAmount > 0 ? '收' : debit > 0 || signedAmount < 0 ? '支' : '');
  const isIncome = direction === '收' || direction === 'income' || direction === 'credit' || direction === 'inflow';
  const isExpense = direction === '支' || direction === 'expense' || direction === 'debit' || direction === 'outflow';
  return {
    summary,
    direction,
    amount,
    debitAmount: debit || (isExpense ? amount : 0),
    creditAmount: credit || (isIncome ? amount : 0),
    transactionDate,
    counterpartyName,
  };
}

function sumTransactions(transactions: NormalizedTx[], predicate: (tx: NormalizedTx) => boolean, amountSelector: (tx: NormalizedTx) => number): number {
  return transactions.reduce((sum, tx) => predicate(tx) ? sum + amountSelector(tx) : sum, 0);
}

function employerLikeCounterparty(name: string): boolean {
  return containsAny(name, EMPLOYER_COUNTERPARTY_KEYWORDS);
}

function deriveSuspectedSalary(transactions: NormalizedTx[]): {
  amount: number;
  count: number;
  months: number;
  confidence: number;
  sources: Record<string, unknown>[];
  notes: string[];
} {
  const matched = transactions.filter((tx) => (
    (tx.creditAmount > 0 || tx.direction === '收') &&
    containsAny(tx.summary, SUSPECTED_SALARY_KEYWORDS) &&
    employerLikeCounterparty(tx.counterpartyName)
  ));
  const amount = matched.reduce((sum, tx) => sum + (tx.creditAmount || tx.amount), 0);
  const months = new Set(matched.map((tx) => tx.transactionDate.slice(0, 7)).filter(Boolean));
  const bySource = new Map<string, { counterparty_name: string; amount: number; count: number; months: Set<string>; salary_type: string }>();
  for (const tx of matched) {
    const key = tx.counterpartyName || '未知付款方';
    const item = bySource.get(key) || { counterparty_name: key, amount: 0, count: 0, months: new Set<string>(), salary_type: 'suspected_salary' };
    item.amount += tx.creditAmount || tx.amount;
    item.count += 1;
    if (tx.transactionDate.slice(0, 7)) item.months.add(tx.transactionDate.slice(0, 7));
    bySource.set(key, item);
  }
  const sources = Array.from(bySource.values())
    .map((item) => ({ ...item, months: Array.from(item.months).sort(), amount: Math.round(item.amount * 100) / 100 }))
    .sort((a, b) => Number(b.amount) - Number(a.amount));
  const monthCount = months.size;
  const confidence = monthCount >= 6 ? 0.85 : monthCount >= 3 ? 0.75 : monthCount >= 2 ? 0.65 : matched.length ? 0.6 : 0;
  const topSource = sources[0]?.counterparty_name ? String(sources[0].counterparty_name) : '';
  return {
    amount: Math.round(amount * 100) / 100,
    count: matched.length,
    months: monthCount,
    confidence,
    sources,
    notes: matched.length ? [`摘要为代发款项/代发类收入，付款方为${topSource || '公司主体'}，连续多月出现，识别为疑似工资收入，需人工核实是否为工资。`] : [],
  };
}

function normalizePersonalFlowSummary(raw: Record<string, unknown>): NormalizedPersonalFlowSummary {
  const rawSummary = asRecord(raw.raw_summary);
  const customerSummary = asRecord(raw.customer_level_summary);
  const income = asRecord(raw.income_verification);
  const expense = asRecord(raw.expense_analysis);
  const cash = asRecord(raw.cash_retention_analysis);
  const repayment = asRecord(raw.repayment_analysis);
  const deterministic = asRecord(raw.deterministic_summary);
  const scaleSummary = getScaleSummary(raw);
  const transactions = getTransactionList(raw).map(normalizeTransaction);
  const isIncome = (tx: NormalizedTx) => tx.creditAmount > 0 || tx.direction === '收';
  const isExpense = (tx: NormalizedTx) => tx.debitAmount > 0 || tx.direction === '支';
  const derivedLoanRepayment = sumTransactions(
    transactions,
    (tx) => isExpense(tx) && containsAny(tx.summary, LOAN_REPAYMENT_KEYWORDS),
    (tx) => tx.debitAmount || tx.amount
  );
  const derivedQuickPayment = sumTransactions(
    transactions,
    (tx) => isExpense(tx) && containsAny(tx.summary, QUICK_PAYMENT_KEYWORDS),
    (tx) => tx.debitAmount || tx.amount
  );
  const derivedUnknownInflow = sumTransactions(
    transactions,
    (tx) => isIncome(tx) && containsAny(tx.summary, UNKNOWN_INFLOW_KEYWORDS),
    (tx) => tx.creditAmount || tx.amount
  );
  const derivedInterestIncome = sumTransactions(
    transactions,
    (tx) => isIncome(tx) && containsAny(tx.summary, INTEREST_INCOME_KEYWORDS),
    (tx) => tx.creditAmount || tx.amount
  );
  const derivedSuspectedSalary = deriveSuspectedSalary(transactions);
  const totalIncome = pickMeaningfulNumber(
    deterministic.total_income,
    income.raw_total_income,
    rawSummary.total_income,
    customerSummary.raw_total_income,
    customerSummary.customer_raw_total_income,
    raw['收支规模汇总'] && asRecord(raw['收支规模汇总'])['总收入金额'],
    scaleSummary['总收入金额']
  );
  const totalExpense = pickMeaningfulAbsNumber(
    deterministic.total_expense,
    expense.raw_total_expense,
    rawSummary.total_expense,
    customerSummary.raw_total_expense,
    customerSummary.customer_raw_total_expense,
    raw['收支规模汇总'] && asRecord(raw['收支规模汇总'])['总支出金额'],
    scaleSummary['总支出金额']
  );
  const netCashFlow = pickMeaningfulNumber(
    deterministic.net_cash_flow,
    cash.net_cash_flow,
    rawSummary.net_cash_flow,
    customerSummary.net_cash_flow,
    customerSummary.raw_total_income && customerSummary.raw_total_expense ? toNumber(customerSummary.raw_total_income) - absAmount(customerSummary.raw_total_expense) : undefined,
    scaleSummary['净现金流'],
    totalIncome - totalExpense
  );
  const loanRepaymentExpense = pickMeaningfulAbsNumber(expense.loan_repayment_expense, repayment.repayment_related_expense, customerSummary.loan_repayment_expense, customerSummary.customer_loan_repayment_expense, derivedLoanRepayment);
  const quickPaymentExpense = hasValue(expense.quick_payment_expense)
    ? pickMeaningfulAbsNumber(expense.quick_payment_expense, derivedQuickPayment)
    : derivedQuickPayment;
  const unknownInflow = hasValue(income.unknown_inflow) || hasValue(customerSummary.unknown_inflow) || hasValue(customerSummary.customer_unknown_inflow)
    ? pickMeaningfulNumber(income.unknown_inflow, customerSummary.unknown_inflow, customerSummary.customer_unknown_inflow, derivedUnknownInflow)
    : derivedUnknownInflow;
  const interestIncome = hasValue(income.interest_income)
    ? pickMeaningfulNumber(income.interest_income, derivedInterestIncome)
    : derivedInterestIncome;
  const suspectedSalaryIncome = pickMeaningfulNumber(income.suspected_salary_income, customerSummary.suspected_salary_income, derivedSuspectedSalary.amount);
  const lowConfidenceSuspectedSalaryIncome = pickNumber(income.low_confidence_suspected_salary_income, income.suspected_salary_income_low_confidence, customerSummary.low_confidence_suspected_salary_income, customerSummary.suspected_salary_income_low_confidence);
  const existingSalaryNotes = asArray<string>(income.salary_detection_notes);
  const salaryDetectionNotes = suspectedSalaryIncome > 0 && (
    !existingSalaryNotes.length ||
    (existingSalaryNotes.length === 1 && existingSalaryNotes[0] === '未识别到明确工资收入')
  )
    ? ['未识别到明确工资收入，但存在疑似单位代发收入，需人工核实。']
    : lowConfidenceSuspectedSalaryIncome > 0 && (
      !existingSalaryNotes.length ||
      (existingSalaryNotes.length === 1 && existingSalaryNotes[0] === '未识别到明确工资收入')
    )
      ? ['存在疑似代发收入，但缺少付款方，无法确认工资性质，需补充完整流水或修复对手信息提取。']
    : existingSalaryNotes.length ? existingSalaryNotes : derivedSuspectedSalary.notes;

  return {
    totalIncome,
    totalExpense,
    netCashFlow,
    incomeCount: pickMeaningfulNumber(deterministic.income_count, rawSummary.income_count, scaleSummary['总收入笔数']),
    expenseCount: pickMeaningfulNumber(deterministic.expense_count, rawSummary.expense_count, scaleSummary['总支出笔数']),
    avgMonthlyIncome: pickMeaningfulNumber(deterministic.avg_monthly_income, income.avg_monthly_raw_income, customerSummary.avg_monthly_income, scaleSummary['月均收入']),
    avgMonthlyExpense: pickMeaningfulAbsNumber(deterministic.avg_monthly_expense, expense.avg_monthly_expense, scaleSummary['月均支出']),
    verifiedIncome: pickNumber(income.verified_income, income.stable_income, customerSummary.verified_income, customerSummary.stable_income, customerSummary.customer_verified_income, customerSummary.customer_stable_income),
    avgMonthlyVerifiedIncome: pickNumber(income.avg_monthly_verified_income, income.avg_monthly_stable_income, customerSummary.avg_monthly_verified_income, customerSummary.avg_monthly_stable_income, customerSummary.customer_avg_monthly_verified_income),
    confirmedSalaryIncome: pickNumber(income.confirmed_salary_income, income.verified_salary_income, customerSummary.salary_income),
    suspectedSalaryIncome,
    lowConfidenceSuspectedSalaryIncome,
    verifiedSalaryIncome: pickNumber(income.verified_salary_income, income.salary_income, customerSummary.salary_income),
    verifiedOperatingIncome: pickNumber(income.verified_operating_income, income.operating_income, customerSummary.operating_income),
    verifiedOtherStableIncome: pickNumber(income.verified_other_stable_income, income.other_stable_income),
    salaryMonths: pickMeaningfulNumber(income.salary_months, customerSummary.salary_months, derivedSuspectedSalary.months),
    salaryAvgMonthlyAmount: pickNumber(income.salary_avg_monthly_amount),
    salaryConfidence: pickMeaningfulNumber(income.salary_confidence, customerSummary.salary_confidence, derivedSuspectedSalary.confidence),
    salarySources: asArray<Record<string, unknown>>(income.salary_sources).length ? asArray<Record<string, unknown>>(income.salary_sources) : derivedSuspectedSalary.sources,
    salaryDetectionNotes,
    unknownInflow,
    interestIncome,
    loanRepaymentExpense,
    quickPaymentExpense,
    creditCardRepaymentExpense: pickAbsNumber(expense.credit_card_repayment_expense),
    transactions,
  };
}

function normalizePersonalFlowData(raw: Record<string, unknown>) {
  const rawSummary = asRecord(raw.raw_summary);
  const customerSummary = asRecord(raw.customer_level_summary);
  const income = asRecord(raw.income_verification);
  const expense = asRecord(raw.expense_analysis);
  const cash = asRecord(raw.cash_retention_analysis);
  const repayment = asRecord(raw.repayment_analysis);
  const fast = asRecord(raw.fast_in_fast_out_analysis);
  const nature = asRecord(raw.flow_nature);
  const judgement = asRecord(raw.financing_judgement);
  const accounts = asArray<Record<string, unknown>>(raw.accounts);
  const firstAccount = accounts[0] || {};
  const baseInfo = asRecord(raw.base_info || raw.account_info || raw['账户基础信息']);
  const normalizedSummary = normalizePersonalFlowSummary(raw);

  const rawIncome = normalizedSummary.totalIncome;
  const rawExpense = normalizedSummary.totalExpense;
  const netCashFlow = normalizedSummary.netCashFlow;
  const verifiedIncome = normalizedSummary.verifiedIncome;
  const loanRepaymentExpense = normalizedSummary.loanRepaymentExpense;
  const monthlyTrend = asArray<Record<string, unknown>>(raw.monthly_trend).length
    ? asArray<Record<string, unknown>>(raw.monthly_trend)
    : asArray<Record<string, unknown>>(firstAccount.monthly_trend);

  return {
    rawSummary,
    customerSummary,
    income,
    expense,
    cash,
    repayment,
    fast,
    nature,
    judgement,
    accounts,
    firstAccount,
    bankName: String(raw.bank_name || baseInfo.bank_name || firstAccount.bank_name || ''),
    accountName: String(raw.account_name || firstAccount.account_name || ''),
    accountNo: String(raw.account_no || firstAccount.account_no || ''),
    currency: String(raw.currency || firstAccount.currency || '人民币'),
    accountType: String(raw.account_type || firstAccount.account_type || ''),
    statementPeriod: asRecord(raw.statement_period || firstAccount.statement_period),
    normalizedSummary,
    rawIncome,
    rawExpense,
    netCashFlow,
    verifiedIncome,
    loanRepaymentExpense,
    monthlyTrend,
    topIncome: asArray<Record<string, unknown>>(raw.top_income_counterparties || firstAccount.top_income_counterparties),
    topExpense: asArray<Record<string, unknown>>(raw.top_expense_counterparties || firstAccount.top_expense_counterparties),
    riskSignals: asArray<Record<string, unknown>>(raw.risk_signals),
    warnings: normalizeWarnings(raw.warnings || raw.summary_warnings),
    documents: asArray<Record<string, unknown>>(raw.documents || raw.source_files),
    documentCount: numberValue(raw.document_count, raw.source_document_count, asArray(raw.documents || raw.source_files).length),
    accountCount: numberValue(raw.account_count, customerSummary.account_count, accounts.length),
  };
}

function buildPersonalFlowRawInput(props: PersonalBankStatementViewProps): Record<string, unknown> {
  const summary = asRecord(props.summary);
  if (Object.keys(summary).length) {
    return summary;
  }
  const selectedDoc = asRecord(props.selectedDoc);
  const selectedDocExtracted = asRecord(parseMaybeJson(
    selectedDoc.extracted_json ??
    selectedDoc.extractedJson ??
    selectedDoc.analysis_result ??
    selectedDoc.result_json ??
    selectedDoc.extracted_data ??
    selectedDoc.extractedData
  ));
  const markdownFallback = extractPersonalFlowFromMarkdown(props.markdownText || props.markdown || '');
  return {
    ...markdownFallback,
    ...selectedDocExtracted,
    ...selectedDoc,
    ...asRecord(props.data),
    ...asRecord(props.summary),
  };
}

function FastMatchesTable({ matches }: { matches: Record<string, unknown>[] }) {
  if (!matches.length) {
    return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到快进快出匹配明细</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>{['收入日期', '收入金额', '支出日期', '支出金额', '间隔天数', '匹配比例', '原因'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
        </thead>
        <tbody>
          {matches.slice(0, 20).map((item, index) => (
            <tr key={`${item.income_transaction_id || 'match'}-${index}`} className="odd:bg-white even:bg-slate-50/60">
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{display(item.income_date)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(item.income_amount)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{display(item.expense_date)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(item.expense_amount)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{display(item.days_between)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{percent(item.match_ratio)}</td>
              <td className="min-w-[220px] border-b border-slate-100 px-3 py-2 text-slate-700">{display(item.reason)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MonthlyTrendTable({ items }: { items: Record<string, unknown>[] }) {
  if (!items.length) {
    return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂无月度趋势数据</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>{['月份', '总收入', '总支出', '可采信收入', '贷款还款支出', '净流入'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
        </thead>
        <tbody>
          {items.map((item) => {
            const income = numberValue(item.raw_income, item.income, item.total_income);
            const expense = numberValue(item.raw_expense, item.expense, item.total_expense);
            const verified = numberValue(item.verified_income, item.stable_income, item.salary_income, item.operating_income);
            const loan = numberValue(item.loan_repayment_expense, item.loan_repayment);
            return (
              <tr key={String(item.month || Math.random())} className="odd:bg-white even:bg-slate-50/60">
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{display(item.month)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(income)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(expense)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right font-semibold">{money(verified)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(loan)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right">{money(income - expense)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const PersonalBankStatementView: React.FC<PersonalBankStatementViewProps> = (props) => {
  const { markdown, markdownText, loading = false, error = null, showHeader = false } = props;
  if (loading) {
    return <div className="rounded-xl border border-slate-200 bg-white p-5 text-sm text-slate-500">正在加载个人流水结构化分析...</div>;
  }
  const statement = buildPersonalFlowRawInput(props);
  const normalized = normalizePersonalFlowData(statement);
  const normalizedSummary = normalized.normalizedSummary;
  if (import.meta.env.DEV) {
    console.debug('[PersonalBankStatementView] summary source =', Object.keys(asRecord(props.summary)).length ? 'personalFlowSummary' : 'fallback');
    console.debug('[PersonalBankStatementView] raw_summary =', statement.raw_summary || statement.deterministic_summary);
    console.debug('[PersonalBankStatementView] warnings =', statement.warnings || statement.summary_warnings || []);
    console.debug('[PersonalBankStatementView] raw summary:', statement);
    console.debug('[PersonalBankStatementView] normalized summary:', normalizedSummary);
  }
  const hasData = Boolean(
    normalized.documentCount > 0 ||
    normalized.accountCount > 0 ||
    normalized.rawIncome > 0 ||
    normalized.rawExpense > 0 ||
    Object.keys(normalized.income).length > 0
  );

  if (error && !hasData) {
    return <AlertBox tone="amber">{error}</AlertBox>;
  }

  if (!hasData) {
    return (
      <div className="space-y-3">
        <div className="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-6 text-sm text-slate-500">暂无个人流水结构化数据</div>
        {markdown || markdownText ? <pre className="whitespace-pre-wrap rounded-xl border border-slate-200 bg-white p-4 text-sm leading-6 text-slate-700">{markdown || markdownText}</pre> : null}
      </div>
    );
  }

  const unknownInflow = normalizedSummary.unknownInflow;
  const unknownRatio = normalized.rawIncome ? unknownInflow / normalized.rawIncome : 0;
  const loanRepaymentRatio = numberValue(normalized.expense.loan_repayment_ratio, normalized.loanRepaymentExpense / Math.max(normalized.rawExpense, 1));
  const flowType = String(normalized.nature.primary_type || 'unknown');
  const retentionLevel = String(normalized.cash.retention_level || 'unknown');
  const fastMatches = asArray<Record<string, unknown>>(normalized.fast.matches);

  return (
    <div className="space-y-4">
      {error ? <AlertBox tone="amber">{error}</AlertBox> : null}
      {showHeader ? (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-emerald-100 bg-white p-4 shadow-sm">
          <div>
            <h3 className="text-base font-semibold text-slate-900">个人流水结构化分析</h3>
            <p className="mt-1 text-xs text-slate-500">客户级个人流水汇总</p>
          </div>
          <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs font-medium text-emerald-700">个人流水</span>
        </div>
      ) : null}

      <Section title="基础信息">
        <InfoGrid rows={[
          ['银行', display(normalized.bankName)],
          ['户名', display(normalized.accountName)],
          ['账号', display(normalized.accountNo)],
          ['账户类型', display(normalized.accountType)],
          ['流水期间', `${display(normalized.statementPeriod.start_date)} 至 ${display(normalized.statementPeriod.end_date)}`],
          ['币种', display(normalized.currency)],
        ]} />
      </Section>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        <Metric label="总收入" help="原始进账，不等于可用收入" value={money(normalized.rawIncome)} icon={<TrendingUp className="h-4 w-4" />} tone="border-emerald-100 bg-emerald-50 text-emerald-700" />
        <Metric label="总支出" value={money(normalized.rawExpense)} icon={<TrendingDown className="h-4 w-4" />} tone="border-rose-100 bg-rose-50 text-rose-700" />
        <Metric label="净流入" value={money(normalized.netCashFlow)} icon={<WalletCards className="h-4 w-4" />} tone="border-blue-100 bg-blue-50 text-blue-700" />
        <Metric label="可采信收入" help="工资/经营/其他稳定收入" value={money(normalized.verifiedIncome)} icon={<BadgeCheck className="h-4 w-4" />} tone="border-violet-100 bg-violet-50 text-violet-700" />
        <Metric label="月均可采信收入" value={money(normalizedSummary.avgMonthlyVerifiedIncome)} icon={<Banknote className="h-4 w-4" />} tone="border-indigo-100 bg-indigo-50 text-indigo-700" />
        <Metric label="贷款还款支出" value={money(normalized.loanRepaymentExpense)} icon={<Landmark className="h-4 w-4" />} tone="border-orange-100 bg-orange-50 text-orange-700" />
      </div>

      <Section title="收入采信分析">
        <InfoGrid rows={[
          ['原始收入', money(normalizedSummary.totalIncome)],
          ['明确工资收入', money(normalizedSummary.confirmedSalaryIncome)],
          ['疑似工资收入', money(normalizedSummary.suspectedSalaryIncome)],
          ['低置信疑似工资收入', money(normalizedSummary.lowConfidenceSuspectedSalaryIncome)],
          ['工资覆盖月份', display(normalizedSummary.salaryMonths)],
          ['工资月均金额', money(normalizedSummary.salaryAvgMonthlyAmount)],
          ['工资识别置信度', percent(normalizedSummary.salaryConfidence)],
          ['主要发薪单位', normalizedSummary.salarySources.length ? normalizedSummary.salarySources.slice(0, 3).map((item) => display(item.counterparty_name)).join('、') : EMPTY],
          ['工资识别说明', normalizedSummary.salaryDetectionNotes.length ? normalizedSummary.salaryDetectionNotes.join('；') : EMPTY],
          ['工资收入', money(normalizedSummary.verifiedSalaryIncome)],
          ['经营收入', money(normalizedSummary.verifiedOperatingIncome)],
          ['其他稳定收入', money(normalizedSummary.verifiedOtherStableIncome)],
          ['来源不明汇入', money(unknownInflow)],
          ['利息收入', money(normalizedSummary.interestIncome)],
          ['可采信收入', money(normalized.verifiedIncome)],
          ['月均可采信收入', money(normalizedSummary.avgMonthlyVerifiedIncome)],
        ]} />
        {unknownRatio >= 0.5 ? <div className="mt-3"><AlertBox>存在大量来源不明汇入，不建议直接作为稳定收入采信。</AlertBox></div> : null}
      </Section>

      <Section title="支出与还款分析">
        <InfoGrid rows={[
          ['总支出', money(normalizedSummary.totalExpense)],
          ['贷款还款支出', money(normalized.loanRepaymentExpense)],
          ['信用卡还款支出', money(normalizedSummary.creditCardRepaymentExpense)],
          ['线上贷款/小贷还款', money(normalized.expense.online_loan_repayment_expense)],
          ['快捷支付/消费', money(normalizedSummary.quickPaymentExpense)],
          ['投资证券转账', money(normalized.expense.investment_expense)],
          ['本人账户转出', money(normalized.expense.internal_transfer_expense)],
          ['个人往来转出', money(normalized.expense.related_party_transfer_expense)],
          ['现金取款', money(normalized.expense.cash_withdrawal)],
          ['月均贷款还款', money(normalized.expense.avg_monthly_loan_repayment, normalized.repayment.monthly_repayment_estimate)],
          ['贷款还款占比', percent(loanRepaymentRatio)],
        ]} />
        {loanRepaymentRatio >= 0.6 ? <div className="mt-3"><AlertBox>贷款相关支出占比较高，该账户可能主要用于贷款还款。</AlertBox></div> : null}
      </Section>

      <Section title="流水性质判断" action={<span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-600">置信度 {percent(normalized.nature.confidence)}</span>}>
        <InfoGrid rows={[
          ['流水性质', FLOW_NATURE_LABELS[flowType] || FLOW_NATURE_LABELS.unknown],
          ['是否工资流水', flowType === 'salary_flow' ? '是' : '否'],
          ['是否经营流水', flowType === 'operating_flow' ? '是' : '否'],
          ['是否还款账户流水', flowType === 'repayment_account_flow' || normalized.repayment.is_repayment_account_flow ? '是' : '否'],
        ]} />
        {flowType === 'repayment_account_flow' || normalized.repayment.is_repayment_account_flow ? (
          <div className="mt-3"><AlertBox tone="rose">该流水更像还款账户流水，可证明持续还款行为，但不宜单独作为主收入证明。</AlertBox></div>
        ) : null}
        <div className="mt-3 space-y-2">
          {asArray<string>(normalized.nature.reasons).length ? asArray<string>(normalized.nature.reasons).map((reason, index) => (
            <div key={`${reason}-${index}`} className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">{reason}</div>
          )) : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂无判断依据</div>}
        </div>
      </Section>

      <Section title="账户沉淀分析">
        <InfoGrid rows={[
          ['净流入', money(normalized.cash.net_cash_flow, normalized.netCashFlow)],
          ['沉淀率', percent(normalized.cash.retention_ratio)],
          ['沉淀等级', RETENTION_LABELS[retentionLevel] || RETENTION_LABELS.unknown],
          ['说明', display(normalized.cash.message)],
        ]} />
      </Section>

      <Section title="快进快出分析" action={<span className={`rounded-full border px-2.5 py-1 text-xs ${normalized.fast.has_fast_in_fast_out ? 'border-rose-200 bg-rose-50 text-rose-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'}`}>{normalized.fast.has_fast_in_fast_out ? '已识别' : '未识别'}</span>}>
        <InfoGrid rows={[
          ['是否存在快进快出', normalized.fast.has_fast_in_fast_out ? '是' : '否'],
          ['匹配笔数', display(normalized.fast.matched_count)],
          ['匹配金额', money(normalized.fast.matched_amount)],
          ['匹配金额占比', percent(normalized.fast.matched_amount_ratio)],
        ]} />
        <div className="mt-3">
          <FastMatchesTable matches={fastMatches} />
        </div>
      </Section>

      <Section title="月度趋势">
        <MonthlyTrendTable items={normalized.monthlyTrend} />
      </Section>

      <Section title="风险信号" action={<FileWarning className="h-4 w-4 text-amber-500" />}>
        {normalized.riskSignals.length ? (
          <div className="space-y-3">
            {normalized.riskSignals.map((signal, index) => {
              const meta = riskMeta(signal.level);
              const code = String(signal.code || '');
              return (
                <div key={`${code || 'risk'}-${index}`} className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${meta.className}`}>{meta.label}</span>
                    <span className="text-sm font-semibold text-slate-800">{RISK_CODE_LABELS[code] || display(signal.message || code)}</span>
                  </div>
                  <div className="mt-2 text-sm leading-6 text-slate-600">{display(signal.message)}</div>
                  {signal.evidence ? <div className="mt-1 text-xs leading-5 text-slate-500">依据：{display(signal.evidence)}</div> : null}
                </div>
              );
            })}
          </div>
        ) : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确风险信号</div>}
      </Section>

      <Section title="融资判断" action={<CircleDollarSign className="h-4 w-4 text-indigo-500" />}>
        <InfoGrid rows={[
          ['收入质量', display(normalized.judgement.income_quality)],
          ['还款能力', display(normalized.judgement.repayment_capacity)],
          ['可疑流水风险', display(normalized.judgement.suspicious_flow_risk)],
          ['建议用途', display(normalized.judgement.recommended_usage)],
        ]} />
        <div className="mt-3 rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm leading-6 text-indigo-800">{display(normalized.judgement.final_summary)}</div>
        <div className="mt-3">
          <div className="mb-2 text-xs font-medium text-slate-500">建议补充材料</div>
          <div className="flex flex-wrap gap-2">
            {asArray<string>(normalized.judgement.missing_materials).length ? asArray<string>(normalized.judgement.missing_materials).map((item) => (
              <span key={item} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-700">{item}</span>
            )) : <span className="text-sm text-slate-500">暂无</span>}
          </div>
        </div>
      </Section>

      {normalized.warnings.length ? (
        <Section title="解析提示" action={<ReceiptText className="h-4 w-4 text-slate-400" />}>
          <div className="space-y-2">
            {normalized.warnings.map((item, index) => <div key={`${item}-${index}`} className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">{item}</div>)}
          </div>
        </Section>
      ) : null}
    </div>
  );
};

export default PersonalBankStatementView;
