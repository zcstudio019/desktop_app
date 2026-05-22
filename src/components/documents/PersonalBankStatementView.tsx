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

function firstValue(...values: unknown[]): unknown {
  return values.find((value) => value !== undefined && value !== null && value !== '');
}

function numberValue(...values: unknown[]): number {
  const value = firstValue(...values);
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
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

  const rawIncome = numberValue(income.raw_total_income, rawSummary.total_income, customerSummary.raw_total_income, customerSummary.customer_raw_total_income);
  const rawExpense = numberValue(expense.raw_total_expense, rawSummary.total_expense, customerSummary.raw_total_expense);
  const netCashFlow = numberValue(cash.net_cash_flow, rawSummary.net_cash_flow, rawIncome - rawExpense);
  const verifiedIncome = numberValue(income.verified_income, income.stable_income, customerSummary.verified_income, customerSummary.customer_verified_income, customerSummary.stable_income);
  const loanRepaymentExpense = numberValue(expense.loan_repayment_expense, repayment.repayment_related_expense, customerSummary.loan_repayment_expense, customerSummary.customer_loan_repayment_expense);
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
    rawIncome,
    rawExpense,
    netCashFlow,
    verifiedIncome,
    loanRepaymentExpense,
    monthlyTrend,
    topIncome: asArray<Record<string, unknown>>(raw.top_income_counterparties || firstAccount.top_income_counterparties),
    topExpense: asArray<Record<string, unknown>>(raw.top_expense_counterparties || firstAccount.top_expense_counterparties),
    riskSignals: asArray<Record<string, unknown>>(raw.risk_signals),
    warnings: asArray<string>(raw.warnings),
    documents: asArray<Record<string, unknown>>(raw.documents || raw.source_files),
    documentCount: numberValue(raw.document_count, raw.source_document_count, asArray(raw.documents || raw.source_files).length),
    accountCount: numberValue(raw.account_count, customerSummary.account_count, accounts.length),
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

const PersonalBankStatementView: React.FC<PersonalBankStatementViewProps> = ({ data, markdown, loading = false, error = null, showHeader = false }) => {
  if (loading) {
    return <div className="rounded-xl border border-slate-200 bg-white p-5 text-sm text-slate-500">正在加载个人流水结构化分析...</div>;
  }
  if (error) {
    return <AlertBox tone="amber">{error}</AlertBox>;
  }
  const statement = asRecord(data);
  const normalized = normalizePersonalFlowData(statement);
  const hasData = Boolean(
    normalized.documentCount > 0 ||
    normalized.accountCount > 0 ||
    normalized.rawIncome > 0 ||
    normalized.rawExpense > 0 ||
    Object.keys(normalized.income).length > 0
  );

  if (!hasData) {
    return (
      <div className="space-y-3">
        <div className="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-6 text-sm text-slate-500">暂无个人流水结构化数据</div>
        {markdown ? <pre className="whitespace-pre-wrap rounded-xl border border-slate-200 bg-white p-4 text-sm leading-6 text-slate-700">{markdown}</pre> : null}
      </div>
    );
  }

  const unknownInflow = numberValue(normalized.income.unknown_inflow);
  const unknownRatio = normalized.rawIncome ? unknownInflow / normalized.rawIncome : 0;
  const loanRepaymentRatio = numberValue(normalized.expense.loan_repayment_ratio, normalized.loanRepaymentExpense / Math.max(normalized.rawExpense, 1));
  const flowType = String(normalized.nature.primary_type || 'unknown');
  const retentionLevel = String(normalized.cash.retention_level || 'unknown');
  const fastMatches = asArray<Record<string, unknown>>(normalized.fast.matches);

  return (
    <div className="space-y-4">
      {showHeader ? (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-emerald-100 bg-white p-4 shadow-sm">
          <div>
            <h3 className="text-base font-semibold text-slate-900">个人流水结构化分析</h3>
            <p className="mt-1 text-xs text-slate-500">客户级个人流水汇总</p>
          </div>
          <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs font-medium text-emerald-700">个人流水</span>
        </div>
      ) : null}

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        <Metric label="总收入" help="原始进账，不等于可用收入" value={money(normalized.rawIncome)} icon={<TrendingUp className="h-4 w-4" />} tone="border-emerald-100 bg-emerald-50 text-emerald-700" />
        <Metric label="总支出" value={money(normalized.rawExpense)} icon={<TrendingDown className="h-4 w-4" />} tone="border-rose-100 bg-rose-50 text-rose-700" />
        <Metric label="净流入" value={money(normalized.netCashFlow)} icon={<WalletCards className="h-4 w-4" />} tone="border-blue-100 bg-blue-50 text-blue-700" />
        <Metric label="可采信收入" help="工资/经营/其他稳定收入" value={money(normalized.verifiedIncome)} icon={<BadgeCheck className="h-4 w-4" />} tone="border-violet-100 bg-violet-50 text-violet-700" />
        <Metric label="月均可采信收入" value={money(normalized.income.avg_monthly_verified_income, normalized.income.avg_monthly_stable_income)} icon={<Banknote className="h-4 w-4" />} tone="border-indigo-100 bg-indigo-50 text-indigo-700" />
        <Metric label="贷款还款支出" value={money(normalized.loanRepaymentExpense)} icon={<Landmark className="h-4 w-4" />} tone="border-orange-100 bg-orange-50 text-orange-700" />
      </div>

      <Section title="收入采信分析">
        <InfoGrid rows={[
          ['原始收入', money(normalized.income.raw_total_income, normalized.rawIncome)],
          ['工资收入', money(normalized.income.verified_salary_income)],
          ['经营收入', money(normalized.income.verified_operating_income)],
          ['其他稳定收入', money(normalized.income.verified_other_stable_income)],
          ['来源不明汇入', money(unknownInflow)],
          ['利息收入', money(normalized.income.interest_income)],
          ['可采信收入', money(normalized.verifiedIncome)],
          ['月均可采信收入', money(normalized.income.avg_monthly_verified_income, normalized.income.avg_monthly_stable_income)],
        ]} />
        {unknownRatio >= 0.5 ? <div className="mt-3"><AlertBox>存在大量来源不明汇入，不建议直接作为稳定收入采信。</AlertBox></div> : null}
      </Section>

      <Section title="支出与还款分析">
        <InfoGrid rows={[
          ['总支出', money(normalized.expense.raw_total_expense, normalized.rawExpense)],
          ['贷款还款支出', money(normalized.loanRepaymentExpense)],
          ['信用卡还款支出', money(normalized.expense.credit_card_repayment_expense)],
          ['快捷支付/消费', money(normalized.expense.quick_payment_expense, normalized.expense.living_expense)],
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
