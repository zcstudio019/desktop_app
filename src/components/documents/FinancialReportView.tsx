import React, { useMemo } from 'react';
import { AlertTriangle, BadgeCheck, CircleDollarSign, TrendingUp } from 'lucide-react';
import {
  buildFinancialReportRightPanel,
  type DisplayRow,
  type RatioRisk,
} from './financialReportRightPanelBuilder';

type FinancialReportViewProps = {
  data?: unknown;
  reports?: unknown[];
};

const EMPTY = '-';

function money(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return EMPTY;
  return new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function ratioValue(value: number | null, format: 'amount' | 'ratio' | 'multiple'): string {
  if (value === null || !Number.isFinite(value)) return EMPTY;
  if (format === 'amount') return money(value);
  if (format === 'ratio') return `${(value * 100).toFixed(2)}%`;
  return `${value.toFixed(2)} 倍`;
}

function riskMeta(level: unknown) {
  const value = String(level || '').toLowerCase();
  if (value === 'high' || value === 'medium_high' || value === 'risk') {
    return { label: '高', className: 'border-rose-200 bg-rose-50 text-rose-700' };
  }
  if (value === 'medium' || value === 'weak') {
    return { label: '中', className: 'border-amber-200 bg-amber-50 text-amber-700' };
  }
  return { label: '低', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
}

function judgmentMeta(value: RatioRisk) {
  if (value === 'risk') return { label: '风险', className: 'border-rose-200 bg-rose-50 text-rose-700' };
  if (value === 'weak') return { label: '偏弱', className: 'border-amber-200 bg-amber-50 text-amber-700' };
  return { label: '正常', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
}

const Section: React.FC<{ title: string; children: React.ReactNode; action?: React.ReactNode }> = ({ title, children, action }) => (
  <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
    <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
      <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
      {action}
    </div>
    {children}
  </section>
);

function AmountTable({ rows }: { rows: DisplayRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>
            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium">项目</th>
            <th className="border-b border-slate-200 px-3 py-2 text-right font-medium">金额（元）</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((item) => (
            <tr key={item.label} className="odd:bg-white even:bg-slate-50/60">
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.label}</td>
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right font-medium text-slate-800">{money(item.value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Tags({ items, emptyText }: { items: string[]; emptyText: string }) {
  if (!items.length) return <div className="text-sm text-slate-500">{emptyText}</div>;
  return (
    <div className="flex flex-wrap gap-2">
      {items.map((item) => (
        <span key={item} className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs text-slate-700">{item}</span>
      ))}
    </div>
  );
}

export const FinancialReportView: React.FC<FinancialReportViewProps> = ({ data, reports = [] }) => {
  const panel = useMemo(() => buildFinancialReportRightPanel(data, reports), [data, reports]);
  if (!panel.available) {
    return (
      <div className="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-7 text-sm text-slate-500">
        暂未生成财务报表分析
      </div>
    );
  }

  const overallRisk = riskMeta(panel.creditConclusion.riskLevel);
  const totals = panel.balanceSheetSummary.reduce<Record<string, number | null>>((result, item) => {
    result[item.label] = item.value;
    return result;
  }, {});
  const profits = panel.incomeStatementSummary.reduce<Record<string, number | null>>((result, item) => {
    result[item.label] = item.value;
    return result;
  }, {});

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {[
          { label: '资产总计', value: totals['资产总计'], tone: 'border-blue-100 bg-blue-50 text-blue-700' },
          { label: '负债合计', value: totals['负债合计'], tone: 'border-orange-100 bg-orange-50 text-orange-700' },
          { label: '营业收入', value: profits['营业收入'], tone: 'border-emerald-100 bg-emerald-50 text-emerald-700' },
          { label: '净利润', value: profits['净利润'], tone: 'border-indigo-100 bg-indigo-50 text-indigo-700' },
        ].map((item) => (
          <div key={item.label} className={`rounded-xl border p-4 ${item.tone}`}>
            <div className="flex items-center gap-2 text-xs font-medium"><CircleDollarSign className="h-4 w-4" />{item.label}</div>
            <div className="mt-2 text-lg font-semibold">{money(item.value)}</div>
          </div>
        ))}
      </div>

      <Section title="基础信息" action={<span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${overallRisk.className}`}>综合风险：{overallRisk.label}</span>}>
        <div className="grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-3">
          {panel.baseInfo.map(([label, value]) => (
            <div key={label} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
              <div className="text-xs text-slate-500">{label}</div>
              <div className="mt-1 break-words font-medium text-slate-800">{value}</div>
            </div>
          ))}
        </div>
      </Section>

      <div className="grid gap-4 xl:grid-cols-2">
        <Section title="资产负债表摘要"><AmountTable rows={panel.balanceSheetSummary} /></Section>
        <Section title="利润表摘要"><AmountTable rows={panel.incomeStatementSummary} /></Section>
      </div>

      <Section title="现金流量表摘要"><AmountTable rows={panel.cashFlowSummary} /></Section>

      <Section title="银行授信核心指标表">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>
                {['指标名称', '指标值', '风险判断', '简短说明'].map((label) => (
                  <th key={label} className="border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {panel.coreRatios.map((item) => {
                const meta = judgmentMeta(item.judgment);
                return (
                  <tr key={item.label} className="odd:bg-white even:bg-slate-50/60">
                    <td className="border-b border-slate-100 px-3 py-2 font-medium text-slate-800">{item.label}</td>
                    <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{ratioValue(item.value, item.format)}</td>
                    <td className="border-b border-slate-100 px-3 py-2">
                      <span className={`rounded-full border px-2 py-0.5 text-xs font-medium ${meta.className}`}>{meta.label}</span>
                    </td>
                    <td className="border-b border-slate-100 px-3 py-2 text-slate-600">{item.explanation}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="异常财务项" action={<AlertTriangle className="h-4 w-4 text-amber-500" />}>
        <div className="space-y-3">
          {panel.riskFlags.length ? panel.riskFlags.map((finding, index) => {
            const meta = riskMeta(finding.level);
            return (
              <div key={`${finding.title}-${index}`} className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${meta.className}`}>{meta.label}</span>
                  <span className="text-sm font-semibold text-slate-800">{finding.title}</span>
                </div>
                <div className="mt-2 text-sm text-slate-600">证据：{finding.evidence.join('；') || EMPTY}</div>
                <div className="mt-1 text-sm text-slate-600">银行关注点：{finding.bankAttention}</div>
              </div>
            );
          }) : (
            <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确异常财务项</div>
          )}
        </div>
      </Section>

      <Section title="多期趋势分析" action={<TrendingUp className="h-4 w-4 text-blue-500" />}>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>
                {['所属期', '营业收入', '净利润', '经营现金流', '资产总计', '负债合计'].map((label) => (
                  <th key={label} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-medium">{label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {panel.trendRows.map((item) => (
                <tr key={item.period} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.period}</td>
                  {[item.revenue, item.netProfit, item.operatingCashFlow, item.totalAssets, item.totalLiabilities].map((value, index) => (
                    <td key={index} className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{money(value)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="综合授信分析" action={<BadgeCheck className="h-4 w-4 text-blue-500" />}>
        <div className="rounded-lg border border-blue-100 bg-blue-50 p-3 text-sm leading-6 text-blue-900">
          <span className={`mr-2 rounded-full border px-2 py-0.5 text-xs font-medium ${overallRisk.className}`}>风险 {overallRisk.label}</span>
          {panel.creditConclusion.conclusion}
        </div>
        <div className="mt-3 grid gap-4 md:grid-cols-2">
          <div><div className="mb-2 text-xs font-medium text-slate-500">正向因素</div><Tags items={panel.creditConclusion.positiveFactors} emptyText="-" /></div>
          <div><div className="mb-2 text-xs font-medium text-slate-500">负向因素</div><Tags items={panel.creditConclusion.negativeFactors} emptyText="-" /></div>
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议补充材料</div><Tags items={panel.creditConclusion.missingMaterials} emptyText="-" /></div>
          <div><div className="mb-2 text-xs font-medium text-slate-500">建议授信策略</div><div className="text-sm leading-6 text-slate-700">{panel.creditConclusion.strategy}</div></div>
        </div>
      </Section>
    </div>
  );
};

export default FinancialReportView;
