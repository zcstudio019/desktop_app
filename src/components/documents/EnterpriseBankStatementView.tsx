import React, { useState } from 'react';
import { AlertTriangle, Banknote, Building2, FileText, Settings, TrendingDown, TrendingUp } from 'lucide-react';
import {
  getEnterpriseFlowRules,
  reviewEnterpriseFlowTransaction,
  saveEnterpriseFlowRules,
} from '../../services/api';
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
  customerId?: string;
  onRulesSaved?: () => void | Promise<void>;
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

const DOCUMENT_TYPE_LABELS: Record<string, string> = {
  enterprise_flow: '企业流水',
  enterprise_bank_statement: '企业流水',
  bank_statement_enterprise: '企业流水',
  company_bank_statement: '企业流水',
  enterprise_credit: '企业征信',
  enterprise_credit_report: '企业征信',
  personal_credit: '个人征信',
  personal_credit_report: '个人征信',
  personal_flow: '个人流水',
  unknown: '未知类型',
};

const DIRECTION_LABELS: Record<string, string> = {
  inflow: '流入',
  outflow: '流出',
  debit: '流出',
  credit: '流入',
  income: '流入',
  expense: '流出',
  unknown: '未知',
};

const CATEGORY_LABELS: Record<string, string> = {
  operating: '真实经营',
  real_business: '真实经营',
  internal_transfer: '内部转账',
  related_party: '关联方往来',
  personal: '个人往来',
  personal_transfer: '个人往来',
  tax_salary_fee: '税费/工资/费用',
  fee_tax_salary: '税费/工资/费用',
  unknown: '未分类',
};

export function getDocumentTypeLabel(type?: unknown): string {
  const text = String(type || '').trim();
  if (!text) return '未知类型';
  return DOCUMENT_TYPE_LABELS[text] || '未知类型';
}

export function getFlowDirectionLabel(direction?: unknown, label?: unknown): string {
  const explicit = String(label || '').trim();
  if (explicit) return explicit;
  const text = String(direction || '').trim();
  if (!text) return '未知';
  return DIRECTION_LABELS[text] || '未知';
}

export function getExcludedCategoryLabel(category?: unknown, label?: unknown): string {
  const explicit = String(label || '').trim();
  if (explicit) return explicit;
  const text = String(category || '').trim();
  if (!text) return '未分类';
  return CATEGORY_LABELS[text] || '未分类';
}

export function isEnterpriseBankStatementType(documentType?: unknown): boolean {
  return ENTERPRISE_BANK_STATEMENT_TYPES.has(String(documentType || '').trim());
}

export function parseMaybeJson(value: unknown): Record<string, unknown> | null {
  if (!value) return null;
  if (typeof value === 'object') return value as Record<string, unknown>;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null;
    } catch (error) {
      if (import.meta.env.DEV) console.debug('[EnterpriseBankStatementView] JSON.parse failed', error);
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
  if (!parsed) return null;
  if (hasEnterpriseFlowShape(parsed)) return normalizeEnterpriseFlowFieldNames(parsed);
  const nested = [
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
  for (const candidate of nested) {
    const normalized = normalizeEnterpriseFlowData(candidate, depth + 1);
    if (normalized && hasEnterpriseFlowShape(normalized)) return normalized;
  }
  return parsed as EnterpriseBankStatementExtraction;
}

export function looksLikeEnterpriseBankStatementData(value: unknown): value is EnterpriseBankStatementExtraction {
  const data = normalizeEnterpriseFlowData(value);
  if (!data) return false;
  return data.normalized_document_type === 'enterprise_bank_statement' || data.document_type === 'enterprise_flow' || Boolean(data.summary);
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
  const text = new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency: 'CNY',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(number));
  return number < 0 ? `-${text}` : text;
}

function formatRatio(value: unknown): string {
  const number = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(number)) return EMPTY;
  return `${(number * 100).toFixed(1)}%`;
}

function toNumber(value: unknown): number {
  const number = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(number) ? number : 0;
}

function splitLines(value: string): string[] {
  return value.split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean);
}

function joinLines(value: unknown): string {
  return Array.isArray(value) ? value.map(String).join('\n') : '';
}

function natureLabel(value?: unknown): string {
  return getExcludedCategoryLabel(value);
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
  if (!items.length) return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">暂无数据</div>;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>{['对手方', '收入金额', '支出金额', '净额', '笔数', '分类判断', '是否剔除'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
        </thead>
        <tbody>
          {items.slice(0, 10).map((item, index) => (
            <tr key={`${item.name || 'counterparty'}-${index}`} className="odd:bg-white even:bg-slate-50/60">
              <td className="max-w-[240px] whitespace-normal break-words border-b border-slate-100 px-3 py-2 text-slate-800">{display(item.name)}</td>
              <MoneyCell value={item.inflow} />
              <MoneyCell value={item.outflow} />
              <MoneyCell value={item.net} strong />
              <td className="border-b border-slate-100 px-3 py-2 text-right text-slate-700">{item.transaction_count ?? item.count ?? 0}</td>
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{getExcludedCategoryLabel(item.category_guess || item.nature, (item as Record<string, unknown>).category_label || (item as Record<string, unknown>).display_name)}</td>
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.exclude_from_operating ? '是' : '否'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TransactionTable({
  items,
  customerId,
  onReviewed,
}: {
  items: Record<string, unknown>[];
  customerId?: string;
  onReviewed?: () => void | Promise<void>;
}) {
  const [reviewingId, setReviewingId] = useState<string | null>(null);
  const review = async (item: Record<string, unknown>, nature: string, exclude: boolean) => {
    const transactionId = String(item.transaction_id || '');
    if (!customerId || !transactionId) return;
    setReviewingId(transactionId);
    try {
      await reviewEnterpriseFlowTransaction(customerId, transactionId, {
        nature,
        exclude_from_operating: exclude,
        manual_reason: '前端人工复核',
      });
      await onReviewed?.();
    } finally {
      setReviewingId(null);
    }
  };
  if (!items.length) return <div className="rounded-lg border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">暂无交易明细</div>;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>{['日期', '方向', '金额', '对方名称', '对方账号', '类型', '剔除原因', '置信度', '人工复核'].map((label) => <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>)}</tr>
        </thead>
        <tbody>
          {items.slice(0, 50).map((item, index) => {
            const transactionId = String(item.transaction_id || index);
            return (
              <tr key={transactionId} className="odd:bg-white even:bg-slate-50/60">
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{display(item.date || item.transaction_date)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{getFlowDirectionLabel(item.direction, item.direction_label)}</td>
                <MoneyCell value={item.amount || item.credit_amount || item.debit_amount} />
                <td className="max-w-[240px] whitespace-normal break-words border-b border-slate-100 px-3 py-2">{display(item.counterparty_name)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 font-mono">{display(item.counterparty_account)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{natureLabel(item.nature)}</td>
                <td className="min-w-[180px] whitespace-normal border-b border-slate-100 px-3 py-2">{display(item.reason || item.classification_reason)}</td>
                <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2">{formatRatio(item.confidence || item.classification_confidence)}</td>
                <td className="min-w-[260px] border-b border-slate-100 px-3 py-2">
                  {customerId ? (
                    <div className="flex flex-wrap gap-1.5">
                      <button type="button" disabled={reviewingId === transactionId} onClick={() => review(item, 'operating', false)} className="rounded border border-emerald-200 bg-emerald-50 px-2 py-1 text-xs text-emerald-700">标记经营</button>
                      <button type="button" disabled={reviewingId === transactionId} onClick={() => review(item, 'internal_transfer', true)} className="rounded border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-700">内部转账</button>
                      <button type="button" disabled={reviewingId === transactionId} onClick={() => review(item, 'related_party', true)} className="rounded border border-violet-200 bg-violet-50 px-2 py-1 text-xs text-violet-700">关联方</button>
                      <button type="button" disabled={reviewingId === transactionId} onClick={() => review(item, 'personal_transfer', false)} className="rounded border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700">个人往来</button>
                    </div>
                  ) : <span className="text-xs text-slate-400">暂无客户ID</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export const EnterpriseBankStatementView: React.FC<EnterpriseBankStatementViewProps> = ({ data, markdown, customerId, onRulesSaved }) => {
  const statement = (normalizeEnterpriseFlowData(data) || {}) as EnterpriseBankStatementExtraction;
  const summary = statement.summary;
  const [viewMode, setViewMode] = useState<'raw' | 'operating' | 'excluded'>('operating');
  const [rulesOpen, setRulesOpen] = useState(false);
  const [rulesLoading, setRulesLoading] = useState(false);
  const [rulesSaving, setRulesSaving] = useState(false);
  const [rulesMessage, setRulesMessage] = useState('');
  const [rulesDraft, setRulesDraft] = useState({
    related_company_names: '',
    self_account_numbers: '',
    internal_transfer_keywords: '',
    operating_counterparty_whitelist: '',
    internal_counterparty_blacklist: '',
    personal_counterparty_names: '',
    manual_overrides: '{}',
  });

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
  const sourceFiles = asArray<Record<string, unknown>>((statement as Record<string, unknown>).source_files);
  const accountNameFallback = accounts.find((item) => item.account_name)?.account_name;
  const rawTotalInflow = summary.raw_total_inflow ?? summary.total_inflow;
  const rawTotalOutflow = summary.raw_total_outflow ?? summary.total_outflow;
  const rawNetCashflow = summary.raw_net_cashflow ?? summary.net_cashflow;
  const operatingInflow = summary.operating_inflow ?? summary.estimated_operating_inflow ?? financing.bank_recognizable_inflow;
  const operatingOutflow = summary.operating_outflow ?? summary.estimated_operating_outflow;
  const operatingNetCashflow = toNumber(operatingInflow) - toNumber(operatingOutflow);
  const viewData = statement.views || {};
  const rawView = viewData.raw || {};
  const excludedView = viewData.excluded || {};
  const classificationSummary = statement.classification_summary || {};
  const internalTransferTransactions = asArray<Record<string, unknown>>(statement.internal_transfer_transactions);
  const excludedTransactions = asArray<Record<string, unknown>>(excludedView.transactions).length
    ? asArray<Record<string, unknown>>(excludedView.transactions)
    : internalTransferTransactions;
  const expectedExcludedInflow = Math.round((toNumber(rawTotalInflow) - toNumber(operatingInflow)) * 100) / 100;
  const expectedExcludedOutflow = Math.round((toNumber(rawTotalOutflow) - toNumber(operatingOutflow)) * 100) / 100;
  const displayedExcludedInflow = toNumber(excludedView.inflow ?? summary.excluded_inflow_total);
  const displayedExcludedOutflow = toNumber(excludedView.outflow ?? summary.excluded_outflow_total);
  const excludedInflowConsistent = Math.abs(expectedExcludedInflow - displayedExcludedInflow) <= 0.01;
  const excludedOutflowConsistent = Math.abs(expectedExcludedOutflow - displayedExcludedOutflow) <= 0.01;
  const operatingNetFromSummary = summary.operating_net_cashflow ?? summary.estimated_operating_net_cashflow;
  const operatingNetConsistent = operatingNetFromSummary === undefined || Math.abs(toNumber(operatingNetFromSummary) - operatingNetCashflow) <= 0.01;

  const openRules = async () => {
    setRulesOpen(true);
    if (!customerId) {
      setRulesMessage('当前页面缺少客户ID，暂不能保存规则。');
      return;
    }
    setRulesLoading(true);
    setRulesMessage('');
    try {
      const rules = await getEnterpriseFlowRules(customerId);
      setRulesDraft({
        related_company_names: joinLines(rules.related_company_names),
        self_account_numbers: joinLines(rules.self_account_numbers),
        internal_transfer_keywords: joinLines(rules.internal_transfer_keywords),
        operating_counterparty_whitelist: joinLines(rules.operating_counterparty_whitelist),
        internal_counterparty_blacklist: joinLines(rules.internal_counterparty_blacklist),
        personal_counterparty_names: joinLines(rules.personal_counterparty_names),
        manual_overrides: JSON.stringify(rules.manual_overrides || {}, null, 2),
      });
    } catch (error) {
      setRulesMessage(error instanceof Error ? error.message : '规则加载失败');
    } finally {
      setRulesLoading(false);
    }
  };

  const saveRules = async () => {
    if (!customerId) return;
    setRulesSaving(true);
    setRulesMessage('');
    try {
      let manualOverrides: Record<string, unknown> = {};
      try {
        manualOverrides = JSON.parse(rulesDraft.manual_overrides || '{}');
      } catch {
        setRulesMessage('人工复核 JSON 格式不正确。');
        setRulesSaving(false);
        return;
      }
      await saveEnterpriseFlowRules(customerId, {
        related_company_names: splitLines(rulesDraft.related_company_names),
        self_account_numbers: splitLines(rulesDraft.self_account_numbers),
        internal_transfer_keywords: splitLines(rulesDraft.internal_transfer_keywords),
        operating_counterparty_whitelist: splitLines(rulesDraft.operating_counterparty_whitelist),
        internal_counterparty_blacklist: splitLines(rulesDraft.internal_counterparty_blacklist),
        personal_counterparty_names: splitLines(rulesDraft.personal_counterparty_names),
        manual_overrides: manualOverrides,
      });
      setRulesMessage('经营性口径已更新。');
      await onRulesSaved?.();
    } catch (error) {
      setRulesMessage(error instanceof Error ? error.message : '规则保存失败');
    } finally {
      setRulesSaving(false);
    }
  };

  const riskBadge = riskMeta(risk.overall_level);
  const metricCards = [
    { label: '总收入', value: rawTotalInflow, icon: <TrendingUp className="h-4 w-4" />, tone: 'text-emerald-700 bg-emerald-50 border-emerald-100' },
    { label: '总支出', value: rawTotalOutflow, icon: <TrendingDown className="h-4 w-4" />, tone: 'text-rose-700 bg-rose-50 border-rose-100' },
    { label: '净流入', value: rawNetCashflow, icon: <Banknote className="h-4 w-4" />, tone: Number(rawNetCashflow || 0) < 0 ? 'text-rose-700 bg-rose-50 border-rose-100' : 'text-blue-700 bg-blue-50 border-blue-100' },
    { label: '月均收入', value: summary.average_monthly_inflow, icon: <TrendingUp className="h-4 w-4" />, tone: 'text-sky-700 bg-sky-50 border-sky-100' },
    { label: '月均支出', value: summary.average_monthly_outflow, icon: <TrendingDown className="h-4 w-4" />, tone: 'text-orange-700 bg-orange-50 border-orange-100' },
    { label: '银行认可经营性回款估算', value: operatingInflow, icon: <Building2 className="h-4 w-4" />, tone: 'text-indigo-700 bg-indigo-50 border-indigo-100' },
  ];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white p-3 shadow-sm">
        <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-1">
          {[
            ['raw', '原始流水'],
            ['operating', '净化流水'],
            ['excluded', '被剔除流水'],
          ].map(([key, label]) => (
            <button key={key} type="button" onClick={() => setViewMode(key as 'raw' | 'operating' | 'excluded')} className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${viewMode === key ? 'bg-white text-blue-700 shadow-sm' : 'text-slate-600 hover:text-slate-900'}`}>
              {label}
            </button>
          ))}
        </div>
        <button type="button" onClick={openRules} className="inline-flex items-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-3 py-1.5 text-sm font-medium text-blue-700 transition-colors hover:bg-blue-100">
          <Settings className="h-4 w-4" />
          经营性口径配置
        </button>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {metricCards.map((item) => (
          <div key={item.label} className={`rounded-xl border p-4 ${item.tone}`}>
            <div className="flex items-center gap-2 text-xs font-medium">{item.icon}{item.label}</div>
            <div className="mt-2 text-xl font-semibold tracking-normal">{formatMoney(item.value)}</div>
          </div>
        ))}
      </div>

      {viewMode === 'raw' ? (
        <Section title="原始流水视图">
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">原始总收入</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(rawView.inflow ?? rawTotalInflow)}</div></div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">原始总支出</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(rawView.outflow ?? rawTotalOutflow)}</div></div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">原始净流入</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(Number(rawView.inflow ?? rawTotalInflow ?? 0) - Number(rawView.outflow ?? rawTotalOutflow ?? 0))}</div></div>
          </div>
        </Section>
      ) : null}

      {viewMode === 'operating' ? (
        <Section title="净化流水视图">
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-lg border border-indigo-200 bg-indigo-50 p-3"><div className="text-xs text-indigo-700">经营性回款</div><div className="mt-1 text-lg font-semibold text-indigo-900">{formatMoney(operatingInflow)}</div></div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">经营性支出</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(operatingOutflow)}</div></div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">经营性净流入</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(operatingNetCashflow)}</div></div>
          </div>
          {!operatingNetConsistent ? (
            <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">口径校验异常：经营性净流入与经营性回款-经营性支出不一致。</div>
          ) : null}
        </Section>
      ) : null}

      {viewMode === 'excluded' ? (
        <Section title="被剔除流水视图">
          <div className="mb-3 grid gap-3 md:grid-cols-4">
            {Object.entries(classificationSummary).map(([key, value]) => (
              <div key={key} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                <div className="text-xs text-slate-500">{natureLabel(key)}</div>
                <div className="mt-1 text-sm text-slate-700">笔数 {value?.count ?? 0}</div>
                <div className="mt-1 text-sm font-semibold text-slate-800">{formatMoney((value?.inflow || 0) + (value?.outflow || 0))}</div>
              </div>
            ))}
          </div>
          {!excludedInflowConsistent || !excludedOutflowConsistent ? (
            <div className="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
              口径校验异常：被剔除流水合计与原始流水-净化流水差额不一致。
              收入差额应为 {formatMoney(expectedExcludedInflow)}，当前为 {formatMoney(displayedExcludedInflow)}；
              支出差额应为 {formatMoney(expectedExcludedOutflow)}，当前为 {formatMoney(displayedExcludedOutflow)}。
            </div>
          ) : null}
          <TransactionTable items={excludedTransactions} customerId={customerId} onReviewed={onRulesSaved} />
        </Section>
      ) : null}

      <Section title="基础信息" action={<span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${riskBadge.className}`}>{riskBadge.label} {risk.overall_score ?? 0}分</span>}>
        <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
          <div><div className="text-xs text-slate-500">客户名称</div><div className="mt-1 text-slate-800">{display(statement.company_name || accountNameFallback)}</div></div>
          <div><div className="text-xs text-slate-500">资料来源文件</div><div className="mt-1 break-words text-slate-800">{sourceFiles.length > 0 ? `共 ${sourceFiles.length} 份` : display(statement.source_file)}</div></div>
          <div><div className="text-xs text-slate-500">流水期间</div><div className="mt-1 text-slate-800">{display(statement.statement_period?.start_date)} 至 {display(statement.statement_period?.end_date)}</div></div>
          <div><div className="text-xs text-slate-500">月份/交易/账户/银行</div><div className="mt-1 text-slate-800">{statement.statement_period?.months_count ?? 0} 月 · {summary.transaction_count ?? 0} 笔 · {summary.account_count ?? 0} 户 · {summary.bank_count ?? 0} 家</div></div>
        </div>
      </Section>

      <Section title="总体流水汇总">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <tbody>
              {[
                ['总收入', rawTotalInflow],
                ['总支出', rawTotalOutflow],
                ['净流入', rawNetCashflow],
                ['月均收入', summary.average_monthly_inflow],
                ['月均支出', summary.average_monthly_outflow],
                ['银行可能认可经营性回款估算', operatingInflow],
                ['剔除内部转账收入', summary.internal_transfer_inflow],
                ['剔除内部转账支出', summary.internal_transfer_outflow],
                ['剔除关联方收入', summary.related_party_inflow ?? summary.excluded_related_party_inflow],
                ['剔除个人往来收入', summary.personal_transfer_inflow ?? summary.excluded_personal_inflow],
                ['已人工复核交易数', summary.reviewed_transaction_count],
                ['待复核可疑交易数', summary.unreviewed_suspicious_count],
              ].map(([label, value]) => (
                <tr key={String(label)} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2 text-slate-500">{label}</td>
                  <td className="border-b border-slate-100 px-3 py-2 text-right font-medium text-slate-800">{typeof value === 'number' || Number.isFinite(Number(value)) ? formatMoney(value) : display(value)}</td>
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

      <Section title="融资建议">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">银行可能认可流水口径</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(operatingInflow)}</div></div>
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3"><div className="text-xs text-slate-500">调整后经营性进账</div><div className="mt-1 text-lg font-semibold text-slate-800">{formatMoney(operatingInflow)}</div></div>
        </div>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议产品</div><div className="flex flex-wrap gap-2">{asArray<string>(financing.suggested_credit_products).map((item) => <span key={item} className="rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs text-blue-700">{item}</span>)}</div></div>
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议补充材料</div><div className="flex flex-wrap gap-2">{asArray<string>(financing.material_checklist).map((item) => <span key={item} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-700">{item}</span>)}</div></div>
        </div>
        <div className="mt-3 rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm leading-6 text-indigo-800">{display(financing.conclusion)}</div>
      </Section>

      <Section title="风险信号">
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
              </div>
            );
          }) : <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确风险信号</div>}
        </div>
      </Section>

      {rulesOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4">
          <div className="max-h-[88vh] w-full max-w-3xl overflow-auto rounded-2xl bg-white p-5 shadow-2xl">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <h3 className="text-base font-semibold text-slate-900">经营性口径配置</h3>
                <p className="mt-1 text-sm text-slate-500">保存后会重新刷新客户级企业流水汇总。</p>
              </div>
              <button type="button" onClick={() => setRulesOpen(false)} className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50">关闭</button>
            </div>
            {rulesLoading ? <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">规则加载中...</div> : null}
            <div className="grid gap-3 md:grid-cols-2">
              {[
                ['related_company_names', '关联公司名称'],
                ['self_account_numbers', '本方账户'],
                ['internal_transfer_keywords', '内部转账关键词'],
                ['operating_counterparty_whitelist', '经营性客户白名单'],
                ['internal_counterparty_blacklist', '内部往来黑名单'],
                ['personal_counterparty_names', '个人往来名单'],
              ].map(([key, label]) => (
                <label key={key} className="text-sm">
                  <span className="mb-1 block font-medium text-slate-700">{label}</span>
                  <textarea
                    value={rulesDraft[key as keyof typeof rulesDraft] || ''}
                    onChange={(event) => setRulesDraft((prev) => ({ ...prev, [key]: event.target.value }))}
                    rows={5}
                    className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-100"
                    placeholder="一行一个，可用逗号分隔"
                  />
                </label>
              ))}
            </div>
            <label className="mt-3 block text-sm">
              <span className="mb-1 block font-medium text-slate-700">人工复核覆盖 JSON</span>
              <textarea
                value={rulesDraft.manual_overrides || '{}'}
                onChange={(event) => setRulesDraft((prev) => ({ ...prev, manual_overrides: event.target.value }))}
                rows={6}
                className="w-full rounded-lg border border-slate-200 px-3 py-2 font-mono text-xs outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-100"
              />
            </label>
            {rulesMessage ? <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">{rulesMessage}</div> : null}
            <div className="mt-4 flex justify-end gap-2">
              <button type="button" onClick={() => setRulesOpen(false)} className="rounded-lg border border-slate-200 px-4 py-2 text-sm text-slate-600 hover:bg-slate-50">取消</button>
              <button type="button" disabled={rulesSaving || !customerId} onClick={saveRules} className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-slate-300">
                {rulesSaving ? '保存中...' : '保存并刷新'}
              </button>
            </div>
          </div>
        </div>
      ) : null}

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
