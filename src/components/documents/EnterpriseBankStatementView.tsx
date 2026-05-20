import React from 'react';
import { AlertTriangle, Banknote, Building2, FileText, TrendingDown, TrendingUp } from 'lucide-react';
import type {
  EnterpriseBankAccountStatement,
  EnterpriseBankStatementExtraction,
  EnterpriseCounterpartyStat,
  EnterpriseMonthlyCashflowSummary,
  EnterpriseRiskSignal,
} from '../../services/types';

type EnterpriseBankStatementViewProps = {
  data?: EnterpriseBankStatementExtraction | Record<string, unknown> | null;
  markdown?: string;
};

const EMPTY = '-';
const ENTERPRISE_BANK_STATEMENT_TYPES = new Set([
  'enterprise_flow',
  'enterprise_bank_statement',
  'bank_statement_enterprise',
  'company_bank_statement',
  '企业流水',
  '银行流水',
]);

export function isEnterpriseBankStatementType(documentType?: unknown): boolean {
  const value = String(documentType || '').trim();
  return ENTERPRISE_BANK_STATEMENT_TYPES.has(value);
}

export function parseMaybeJson(value: unknown): Record<string, unknown> | null {
  if (!value) return null;
  if (typeof value === 'object') return value as Record<string, unknown>;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null;
    } catch (error) {
      if (import.meta.env.DEV) {
        console.debug('[EnterpriseBankStatementView] JSON.parse failed', error, value.slice(0, 200));
      }
      return null;
    }
  }
  return null;
}

export function hasEnterpriseFlowShape(value: unknown): boolean {
  if (!value || typeof value !== 'object') return false;
  const obj = value as Record<string, unknown>;
  return Boolean(
    obj.summary ||
    obj.accounts ||
    obj.monthly_summary ||
    obj.monthlySummary ||
    obj.counterparty_summary ||
    obj.counterpartySummary ||
    obj.risk_analysis ||
    obj.riskAnalysis ||
    obj.financing_view ||
    obj.financingView ||
    obj.transactions
  );
}

export function normalizeEnterpriseFlowFieldNames(data: unknown): EnterpriseBankStatementExtraction | null {
  if (!data || typeof data !== 'object') return null;
  const obj = data as Record<string, unknown>;
  return {
    ...obj,
    summary: obj.summary ?? obj.statement_summary ?? obj.statementSummary ?? {},
    accounts: obj.accounts ?? obj.account_statements ?? obj.accountStatements ?? [],
    monthly_summary: obj.monthly_summary ?? obj.monthlySummary ?? obj.monthly_trends ?? obj.monthlyTrends ?? [],
    counterparty_summary: obj.counterparty_summary ?? obj.counterpartySummary ?? {},
    risk_analysis: obj.risk_analysis ?? obj.riskAnalysis ?? {},
    financing_view: obj.financing_view ?? obj.financingView ?? {},
    transactions: obj.transactions ?? [],
    warnings: obj.warnings ?? [],
  } as EnterpriseBankStatementExtraction;
}

export function normalizeEnterpriseFlowData(raw: unknown, depth = 0): EnterpriseBankStatementExtraction | null {
  if (!raw || depth > 5) return null;
  const parsed = parseMaybeJson(raw);
  if (!parsed || typeof parsed !== 'object') return null;
  if (hasEnterpriseFlowShape(parsed)) {
    return normalizeEnterpriseFlowFieldNames(parsed);
  }
  const nestedCandidates = [
    parsed.extracted_json,
    parsed.extractedJson,
    parsed.extracted_data,
    parsed.extractedData,
    parsed.structured_data,
    parsed.structuredData,
    parsed.data,
    parsed.result,
    parsed.payload,
  ];
  for (const candidate of nestedCandidates) {
    const normalized = normalizeEnterpriseFlowData(candidate, depth + 1);
    if (normalized && hasEnterpriseFlowShape(normalized)) {
      return normalized;
    }
  }
  return parsed as EnterpriseBankStatementExtraction;
}

export function looksLikeEnterpriseBankStatementData(value: unknown): value is EnterpriseBankStatementExtraction {
  const data = normalizeEnterpriseFlowData(value);
  if (!data) return false;
  return data.normalized_document_type === 'enterprise_bank_statement' || data.document_type === 'enterprise_flow' || !!data.summary?.total_inflow;
}

function asArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function display(value: unknown): string {
  if (value === null || value === undefined || value === '') return EMPTY;
  return String(value);
}

function formatMoney(value: unknown): string {
  const number = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(number)) return EMPTY;
  const abs = Math.abs(number);
  const formatted = new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency: 'CNY',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(abs);
  return number < 0 ? `-${formatted}` : formatted;
}

function formatRatio(value: unknown): string {
  const number = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(number)) return EMPTY;
  return `${(number * 100).toFixed(2)}%`;
}

function riskMeta(level?: string) {
  if (level === 'high') return { label: '高风险', className: 'border-rose-200 bg-rose-50 text-rose-700' };
  if (level === 'medium') return { label: '中风险', className: 'border-amber-200 bg-amber-50 text-amber-700' };
  return { label: '低风险', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
}

const Section: React.FC<{ title: string; children: React.ReactNode; action?: React.ReactNode }> = ({ title, children, action }) => (
  <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
    <div className="mb-3 flex items-center justify-between gap-3">
      <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
      {action}
    </div>
    {children}
  </section>
);

const MoneyCell: React.FC<{ value: unknown; strong?: boolean }> = ({ value, strong }) => (
  <td className={`whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right ${strong ? 'font-semibold text-slate-800' : 'text-slate-700'}`}>
    {formatMoney(value)}
  </td>
);

function CounterpartyTable({ items }: { items: EnterpriseCounterpartyStat[] }) {
  if (items.length === 0) {
    return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">暂无数据</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>
            {['对手方', '收入金额', '支出金额', '净额', '笔数', '分类判断', '关联方', '个人', '风险备注'].map((label) => (
              <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.slice(0, 10).map((item, index) => (
            <tr key={`${item.name || 'counterparty'}-${index}`} className="odd:bg-white even:bg-slate-50/60">
              <td className="max-w-[220px] whitespace-normal break-words border-b border-slate-100 px-3 py-2 text-slate-800">{display(item.name)}</td>
              <MoneyCell value={item.inflow} />
              <MoneyCell value={item.outflow} />
              <MoneyCell value={item.net} strong />
              <td className="border-b border-slate-100 px-3 py-2 text-right text-slate-700">{item.transaction_count ?? 0}</td>
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{display(item.category_guess)}</td>
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.is_related_party ? '是' : '否'}</td>
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.is_personal_counterparty ? '是' : '否'}</td>
              <td className="min-w-[160px] whitespace-normal border-b border-slate-100 px-3 py-2 text-slate-600">{display(item.risk_note)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RelatedPartyTable({ title, items, emptyText }: { title: string; items: EnterpriseCounterpartyStat[]; emptyText: string }) {
  return (
    <div>
      <div className="mb-2 text-xs font-medium text-slate-500">{title}</div>
      {items.length > 0 ? <CounterpartyTable items={items} /> : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">{emptyText}</div>}
    </div>
  );
}

export const EnterpriseBankStatementView: React.FC<EnterpriseBankStatementViewProps> = ({ data, markdown }) => {
  const statement = (normalizeEnterpriseFlowData(data) || {}) as EnterpriseBankStatementExtraction;
  const summary = statement.summary;

  if (!summary) {
    return (
      <div className="space-y-3">
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          当前资料暂无结构化流水数据，以下为原始分析报告。
        </div>
        <pre className="whitespace-pre-wrap rounded-xl border border-slate-200 bg-white p-4 text-sm leading-6 text-slate-700">{markdown || '暂无内容'}</pre>
      </div>
    );
  }

  const accounts = asArray<EnterpriseBankAccountStatement>(statement.accounts);
  const monthly = asArray<EnterpriseMonthlyCashflowSummary>(statement.monthly_summary);
  const counterparty = statement.counterparty_summary || {};
  const risk = statement.risk_analysis || {};
  const financing = statement.financing_view || {};
  const riskBadge = riskMeta(risk.overall_level);
  const metricCards = [
    { label: '总收入', value: summary.total_inflow, icon: <TrendingUp className="h-4 w-4" />, tone: 'text-emerald-700 bg-emerald-50 border-emerald-100' },
    { label: '总支出', value: summary.total_outflow, icon: <TrendingDown className="h-4 w-4" />, tone: 'text-rose-700 bg-rose-50 border-rose-100' },
    { label: '净流入', value: summary.net_cashflow, icon: <Banknote className="h-4 w-4" />, tone: Number(summary.net_cashflow || 0) < 0 ? 'text-rose-700 bg-rose-50 border-rose-100' : 'text-blue-700 bg-blue-50 border-blue-100' },
    { label: '月均收入', value: summary.average_monthly_inflow, icon: <TrendingUp className="h-4 w-4" />, tone: 'text-sky-700 bg-sky-50 border-sky-100' },
    { label: '月均支出', value: summary.average_monthly_outflow, icon: <TrendingDown className="h-4 w-4" />, tone: 'text-orange-700 bg-orange-50 border-orange-100' },
    { label: '银行认可经营性回款估算', value: summary.estimated_operating_inflow, icon: <Building2 className="h-4 w-4" />, tone: 'text-indigo-700 bg-indigo-50 border-indigo-100' },
  ];

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {metricCards.map((item) => (
          <div key={item.label} className={`rounded-xl border p-4 ${item.tone}`}>
            <div className="flex items-center gap-2 text-xs font-medium">{item.icon}{item.label}</div>
            <div className="mt-2 text-xl font-semibold tracking-normal">{formatMoney(item.value)}</div>
          </div>
        ))}
      </div>

      <Section title="基础信息" action={<span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${riskBadge.className}`}>{riskBadge.label} {risk.overall_score ?? 0}分</span>}>
        <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
          <div><div className="text-xs text-slate-500">客户名称</div><div className="mt-1 text-slate-800">{display(statement.company_name)}</div></div>
          <div><div className="text-xs text-slate-500">资料来源文件</div><div className="mt-1 break-words text-slate-800">{display(statement.source_file)}</div></div>
          <div><div className="text-xs text-slate-500">流水期间</div><div className="mt-1 text-slate-800">{display(statement.statement_period?.start_date)} 至 {display(statement.statement_period?.end_date)}</div></div>
          <div><div className="text-xs text-slate-500">月份/交易/账户/银行</div><div className="mt-1 text-slate-800">{statement.statement_period?.months_count ?? 0} 月 · {summary.transaction_count ?? 0} 笔 · {summary.account_count ?? 0} 户 · {summary.bank_count ?? 0} 家</div></div>
        </div>
      </Section>

      <Section title="总体流水汇总">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <tbody>
              {[
                ['总收入', summary.total_inflow],
                ['总支出', summary.total_outflow],
                ['净流入', summary.net_cashflow],
                ['月均收入', summary.average_monthly_inflow],
                ['月均支出', summary.average_monthly_outflow],
                ['月均净流入', summary.average_monthly_net_cashflow],
                ['银行可能认可经营性回款估算', summary.estimated_operating_inflow],
                ['剔除内部转账金额', summary.excluded_internal_transfer_amount],
                ['剔除关联方收入', summary.excluded_related_party_inflow],
                ['剔除个人往来收入', summary.excluded_personal_inflow],
              ].map(([label, value]) => (
                <tr key={String(label)} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2 text-slate-500">{label}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right font-medium text-slate-800">{formatMoney(value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="各银行账户汇总">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>{['银行', '户名', '账号', '币种', '收入', '支出', '净流入', '笔数', '期初余额', '期末余额', 'sheet_name'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
            </thead>
            <tbody>
              {accounts.map((item, index) => (
                <tr key={item.account_id || index} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2">{display(item.bank_name)}</td>
                  <td className="max-w-[220px] whitespace-normal break-words border-b border-slate-100 px-3 py-2">{display(item.account_name)}</td>
                  <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 font-mono">{display(item.account_number)}</td>
                  <td className="border-b border-slate-100 px-3 py-2">{display(item.currency)}</td>
                  <MoneyCell value={item.total_inflow} />
                  <MoneyCell value={item.total_outflow} />
                  <MoneyCell value={item.net_cashflow} strong />
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{item.transaction_count ?? 0}</td>
                  <MoneyCell value={item.opening_balance} />
                  <MoneyCell value={item.ending_balance} />
                  <td className="border-b border-slate-100 px-3 py-2">{display(item.sheet_name)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="月度趋势分析">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>{['月份', '收入', '支出', '净流入', '收入笔数', '支出笔数', '月末余额'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
            </thead>
            <tbody>
              {monthly.map((item) => (
                <tr key={item.month} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2">{display(item.month)}</td>
                  <MoneyCell value={item.inflow} />
                  <MoneyCell value={item.outflow} />
                  <MoneyCell value={item.net_cashflow} strong />
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{item.inflow_count ?? 0}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right">{item.outflow_count ?? 0}</td>
                  <MoneyCell value={item.ending_balance} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <div className="grid gap-4 xl:grid-cols-2">
        <Section title="主要收入来源"><CounterpartyTable items={asArray<EnterpriseCounterpartyStat>(counterparty.top_inflow_counterparties)} /></Section>
        <Section title="主要支出对象"><CounterpartyTable items={asArray<EnterpriseCounterpartyStat>(counterparty.top_outflow_counterparties)} /></Section>
      </div>

      <Section title="关联方与个人往来">
        <div className="grid gap-4 xl:grid-cols-2">
          <RelatedPartyTable title="关联方往来" items={asArray<EnterpriseCounterpartyStat>(counterparty.related_party_counterparties)} emptyText="暂未识别到明显关联方往来" />
          <RelatedPartyTable title="个人账户往来" items={asArray<EnterpriseCounterpartyStat>(counterparty.personal_counterparties)} emptyText="暂未识别到明显个人账户往来" />
        </div>
      </Section>

      <Section title="风险信号">
        <div className="mb-3 flex flex-wrap gap-2">
          {asArray<string>(risk.strengths).map((item) => <span key={item} className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs text-emerald-700">{item}</span>)}
          {asArray<string>(risk.weaknesses).map((item) => <span key={item} className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-xs text-amber-700">{item}</span>)}
        </div>
        <div className="space-y-3">
          {asArray<EnterpriseRiskSignal>(risk.signals).length > 0 ? asArray<EnterpriseRiskSignal>(risk.signals).map((signal, index) => {
            const meta = riskMeta(signal.level);
            return (
              <div key={signal.code || index} className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${meta.className}`}>{meta.label}</span>
                  <span className="text-sm font-semibold text-slate-800">{display(signal.title)}</span>
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-600">{display(signal.description)}</div>
                <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500">
                  <span>金额：{formatMoney(signal.amount)}</span>
                  <span>占比：{formatRatio(signal.ratio)}</span>
                </div>
                {signal.suggestion ? <div className="mt-2 text-sm text-slate-700">建议：{signal.suggestion}</div> : null}
              </div>
            );
          }) : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确风险信号</div>}
        </div>
      </Section>

      <Section title="融资建议">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">银行可能认可流水口径</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(financing.bank_recognizable_inflow)}</div></div>
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">调整后经营性进账</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(financing.adjusted_operating_inflow)}</div></div>
        </div>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议产品</div><div className="flex flex-wrap gap-2">{asArray<string>(financing.suggested_credit_products).map((item) => <span key={item} className="rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs text-blue-700">{item}</span>)}</div></div>
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议补充材料</div><div className="flex flex-wrap gap-2">{asArray<string>(financing.material_checklist).map((item) => <span key={item} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-700">{item}</span>)}</div></div>
        </div>
        <div className="mt-3 rounded-lg border border-slate-200 bg-white p-3">
          <div className="mb-2 text-xs font-medium text-slate-500">客户经理说明话术</div>
          <ul className="space-y-1 text-sm leading-6 text-slate-700">{asArray<string>(financing.bank_explanation).map((item) => <li key={item}>- {item}</li>)}</ul>
        </div>
        <div className="mt-3 rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm leading-6 text-indigo-800">{display(financing.conclusion)}</div>
      </Section>

      <details className="rounded-xl border border-slate-200 bg-white p-4">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800"><FileText className="mr-2 inline h-4 w-4" />查看原始分析报告</summary>
        <pre className="mt-3 whitespace-pre-wrap text-sm leading-6 text-slate-700">{markdown || '暂无内容'}</pre>
      </details>

      <details className="rounded-xl border border-slate-200 bg-white p-4">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800"><AlertTriangle className="mr-2 inline h-4 w-4" />查看原始结构化数据</summary>
        <pre className="mt-3 max-h-[420px] overflow-auto rounded-lg bg-slate-950 p-3 text-xs leading-5 text-slate-100">{JSON.stringify(statement, null, 2)}</pre>
      </details>
    </div>
  );
};

export default EnterpriseBankStatementView;
