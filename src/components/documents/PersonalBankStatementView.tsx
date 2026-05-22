import React from 'react';
import { AlertTriangle, Banknote, TrendingDown, TrendingUp, WalletCards } from 'lucide-react';

type Props = {
  data?: Record<string, unknown> | null;
  markdown?: string;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? value as Record<string, unknown> : {};
}

function asArray<T = Record<string, unknown>>(value: unknown): T[] {
  return Array.isArray(value) ? value as T[] : [];
}

function money(value: unknown): string {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return '-';
  return new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'CNY', maximumFractionDigits: 2 }).format(number);
}

function text(value: unknown): string {
  const s = String(value ?? '').trim();
  return s || '-';
}

const Section: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
    <h4 className="mb-3 text-sm font-semibold text-slate-800">{title}</h4>
    {children}
  </section>
);

const Metric: React.FC<{ label: string; value: unknown; icon: React.ReactNode; tone: string }> = ({ label, value, icon, tone }) => (
  <div className={`rounded-xl border p-4 ${tone}`}>
    <div className="flex items-center gap-2 text-xs font-medium">{icon}{label}</div>
    <div className="mt-2 text-xl font-semibold tracking-normal">{money(value)}</div>
  </div>
);

function CounterpartyTable({ items }: { items: Record<string, unknown>[] }) {
  if (!items.length) return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">暂无数据</div>;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>{['对手方', '金额', '笔数'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
        </thead>
        <tbody>
          {items.slice(0, 10).map((item, index) => (
            <tr key={`${item.name || 'counterparty'}-${index}`} className="odd:bg-white even:bg-slate-50/60">
              <td className="max-w-[260px] whitespace-normal break-words border-b border-slate-100 px-3 py-2 text-slate-800">{text(item.name)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-700">{money(item.amount)}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-700">{text(item.count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const PersonalBankStatementView: React.FC<Props> = ({ data, markdown }) => {
  const statement = asRecord(data);
  const summary = asRecord(statement.customer_level_summary);
  const monthly = asArray<Record<string, unknown>>(statement.monthly_trend);
  const accounts = asArray<Record<string, unknown>>(statement.accounts);
  const firstAccount = accounts[0] || {};
  const judgement = asRecord(statement.financing_judgement);
  const riskSignals = asArray<Record<string, unknown>>(statement.risk_signals);
  const topIncome = asArray<Record<string, unknown>>(statement.top_income_counterparties || firstAccount.top_income_counterparties);
  const topExpense = asArray<Record<string, unknown>>(statement.top_expense_counterparties || firstAccount.top_expense_counterparties);

  if (!Object.keys(summary).length) {
    return <pre className="whitespace-pre-wrap rounded-xl border border-slate-200 bg-white p-4 text-sm leading-6 text-slate-700">{markdown || '暂无个人流水结构化数据'}</pre>;
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <Metric label="原始收入" value={summary.raw_total_income} icon={<TrendingUp className="h-4 w-4" />} tone="border-emerald-100 bg-emerald-50 text-emerald-700" />
        <Metric label="原始支出" value={summary.raw_total_expense} icon={<TrendingDown className="h-4 w-4" />} tone="border-orange-100 bg-orange-50 text-orange-700" />
        <Metric label="稳定收入" value={summary.stable_income} icon={<Banknote className="h-4 w-4" />} tone="border-sky-100 bg-sky-50 text-sky-700" />
        <Metric label="月均稳定收入" value={summary.avg_monthly_stable_income} icon={<WalletCards className="h-4 w-4" />} tone="border-indigo-100 bg-indigo-50 text-indigo-700" />
      </div>

      <Section title="收入与净化">
        <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
          <div><div className="text-xs text-slate-500">工资收入</div><div className="mt-1 font-semibold text-slate-800">{money(summary.salary_income)}</div></div>
          <div><div className="text-xs text-slate-500">经营收入</div><div className="mt-1 font-semibold text-slate-800">{money(summary.operating_income)}</div></div>
          <div><div className="text-xs text-slate-500">贷款流入剔除</div><div className="mt-1 font-semibold text-slate-800">{money(summary.loan_inflow)}</div></div>
          <div><div className="text-xs text-slate-500">内部转账收入剔除</div><div className="mt-1 font-semibold text-slate-800">{money(summary.internal_transfer_income)}</div></div>
        </div>
      </Section>

      <Section title="月度趋势">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>{['月份', '收入', '支出', '工资', '经营', '稳定收入'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
            </thead>
            <tbody>
              {monthly.map((item) => (
                <tr key={String(item.month)} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2">{text(item.month)}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{money(item.raw_income)}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{money(item.raw_expense)}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{money(item.salary_income)}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{money(item.operating_income)}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right font-semibold">{money(item.stable_income)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <div className="grid gap-4 xl:grid-cols-2">
        <Section title="主要收入来源"><CounterpartyTable items={topIncome} /></Section>
        <Section title="主要支出去向"><CounterpartyTable items={topExpense} /></Section>
      </div>

      <Section title="风险信号">
        {riskSignals.length ? (
          <div className="space-y-2">
            {riskSignals.map((signal, index) => (
              <div key={`${signal.code || 'risk'}-${index}`} className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
                <AlertTriangle className="mr-2 inline h-4 w-4" />[{text(signal.level)}] {text(signal.message)}：{text(signal.evidence)}
              </div>
            ))}
          </div>
        ) : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确风险信号</div>}
      </Section>

      <Section title="融资判断">
        <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
          <div><div className="text-xs text-slate-500">收入质量</div><div className="mt-1 font-semibold text-slate-800">{text(judgement.income_quality)}</div></div>
          <div><div className="text-xs text-slate-500">还款能力</div><div className="mt-1 font-semibold text-slate-800">{text(judgement.repayment_capacity)}</div></div>
          <div><div className="text-xs text-slate-500">疑似刷流水风险</div><div className="mt-1 font-semibold text-slate-800">{text(judgement.suspicious_flow_risk)}</div></div>
          <div><div className="text-xs text-slate-500">建议用途</div><div className="mt-1 font-semibold text-slate-800">{text(judgement.recommended_usage)}</div></div>
        </div>
      </Section>
    </div>
  );
};

export default PersonalBankStatementView;
