import React, { useMemo } from 'react';
import { AlertTriangle, BadgeCheck, CircleDollarSign, FileText } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  buildFinancialReportCustomerSummary,
  type BalanceChangeRow,
  type RatioRisk,
} from './financialReportRightPanelBuilder';

type FinancialReportViewProps = {
  data?: unknown;
  reports?: unknown[];
  profileMarkdown?: string;
};

const EMPTY = '-';

function money(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return EMPTY;
  return new Intl.NumberFormat('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function percent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return EMPTY;
  return `${(value * 100).toFixed(2)}%`;
}

function ratioValue(value: number | string | null, format: 'amount' | 'ratio' | 'multiple' | 'text'): string {
  if (typeof value === 'string') return value || EMPTY;
  if (value === null || !Number.isFinite(value)) return EMPTY;
  if (format === 'amount') return money(value);
  if (format === 'ratio') return percent(value);
  if (format === 'multiple') return `${value.toFixed(2)} 倍`;
  return String(value);
}

function riskMeta(level: unknown) {
  const value = String(level || '').toLowerCase();
  if (value === 'high' || value === 'risk') {
    return { label: '高', className: 'border-rose-200 bg-rose-50 text-rose-700' };
  }
  if (value === 'medium_high') {
    return { label: '中高', className: 'border-orange-200 bg-orange-50 text-orange-700' };
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

function BalanceChangeTable({ rows }: { rows: BalanceChangeRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="bg-slate-50 text-xs text-slate-500">
          <tr>
            {['项目', '最新一期', '上一期', '变化额', '变化率'].map((label) => (
              <th key={label} className={`whitespace-nowrap border-b border-slate-200 px-3 py-2 font-medium ${label === '项目' ? 'text-left' : 'text-right'}`}>{label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((item) => (
            <tr key={item.label} className="odd:bg-white even:bg-slate-50/60">
              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.label}</td>
              {[item.latest, item.previous, item.change].map((value, index) => (
                <td key={index} className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right font-medium text-slate-800">{money(value)}</td>
              ))}
              <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-700">{percent(item.changeRate)}</td>
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

export const FinancialReportView: React.FC<FinancialReportViewProps> = ({ data, reports = [], profileMarkdown = '' }) => {
  const panel = useMemo(
    () => buildFinancialReportCustomerSummary([...reports, data], profileMarkdown),
    [data, reports, profileMarkdown],
  );
  if (!panel.available) {
    return (
      <div className="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-7 text-sm text-slate-500">
        暂未生成财务报表分析
      </div>
    );
  }

  const overallRisk = riskMeta(panel.creditConclusion.riskLevel);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-base font-semibold text-slate-900">{panel.title}</h3>
          <p className="mt-1 text-xs text-slate-500">{panel.subtitle}</p>
        </div>
        <span className="rounded-full border border-indigo-200 bg-indigo-50 px-2.5 py-1 text-xs font-medium text-indigo-700">财务报表</span>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {panel.topMetrics.map((item, index) => (
          <div key={item.label} className={`rounded-xl border p-4 ${[
            'border-blue-100 bg-blue-50 text-blue-700',
            'border-orange-100 bg-orange-50 text-orange-700',
            'border-emerald-100 bg-emerald-50 text-emerald-700',
            'border-indigo-100 bg-indigo-50 text-indigo-700',
          ][index % 4]}`}>
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

      <Section title={panel.isSinglePeriod ? '资产负债表摘要（单期）' : '资产负债表摘要（最新一期与上一期变化）'}>
        <BalanceChangeTable rows={panel.latestBalanceSheet} />
      </Section>

      <Section title={panel.isSinglePeriod ? '利润表摘要（单期）' : '利润表摘要（多期趋势）'}>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>
                {['所属期', '营业收入', '营业成本', '毛利', '净利润', '毛利率', '净利率'].map((label) => (
                  <th key={label} className={`whitespace-nowrap border-b border-slate-200 px-3 py-2 font-medium ${label === '所属期' ? 'text-left' : 'text-right'}`}>{label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {panel.incomeTrendRows.map((item) => (
                <tr key={item.period} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.period}</td>
                  {[item.revenue, item.operatingCost, item.grossProfit, item.netProfit].map((value, index) => (
                    <td key={index} className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{money(value)}</td>
                  ))}
                  <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{percent(item.grossMargin)}</td>
                  <td className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{percent(item.netMargin)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title={panel.isSinglePeriod ? '现金流量表摘要（单期）' : '现金流量表摘要（多期趋势）'}>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>
                {['所属期', '经营现金流', '投资现金流', '筹资现金流', '期末现金余额'].map((label) => (
                  <th key={label} className={`whitespace-nowrap border-b border-slate-200 px-3 py-2 font-medium ${label === '所属期' ? 'text-left' : 'text-right'}`}>{label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {panel.cashFlowTrendRows.map((item) => (
                <tr key={item.period} className="odd:bg-white even:bg-slate-50/60">
                  <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{item.period}</td>
                  {[item.operatingCashFlow, item.investingCashFlow, item.financingCashFlow, item.endingCashBalance].map((value, index) => (
                    <td key={index} className="whitespace-nowrap border-b border-slate-100 px-3 py-2 text-right text-slate-800">{money(value)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="银行授信核心指标表">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-xs text-slate-500">
              <tr>
                {['指标', '数值', '判断', '说明'].map((label) => (
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

      <Section title="融资建议">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">财务口径可参考授信空间</div>
            <div className="mt-1 text-sm font-semibold leading-6 text-slate-800">{panel.financingAdvice.referenceCreditSpaceLabel}</div>
          </div>
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
            <div className="text-xs text-slate-500">建议授信策略</div>
            <div className="mt-1 text-sm font-semibold leading-6 text-slate-800">{panel.financingAdvice.suggestedStrategy}</div>
          </div>
        </div>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <div>
            <div className="mb-2 text-xs font-medium text-slate-500">建议产品</div>
            <Tags items={panel.financingAdvice.suggestedProducts} emptyText="-" />
          </div>
          <div>
            <div className="mb-2 text-xs font-medium text-slate-500">建议补充材料</div>
            <Tags items={panel.financingAdvice.suggestedMaterials} emptyText="-" />
          </div>
        </div>
        <div className="mt-3 rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-sm leading-6 text-indigo-800">
          {panel.financingAdvice.description}
        </div>
      </Section>

      <Section title="风险信号" action={<AlertTriangle className="h-4 w-4 text-amber-500" />}>
        <div className="space-y-3">
          {panel.riskSignals.length ? panel.riskSignals.map((signal, index) => {
            const meta = riskMeta(signal.level);
            return (
              <div key={`${signal.title}-${index}`} className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${meta.className}`}>{meta.label}</span>
                  <span className="text-sm font-semibold text-slate-800">{signal.title}</span>
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-600">{signal.description}</div>
                <div className="mt-1 text-sm text-slate-600">证据数据：{signal.evidence || EMPTY}</div>
              </div>
            );
          }) : (
            <div className="rounded-lg border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">暂未识别到明确风险信号</div>
          )}
        </div>
      </Section>

      <details className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800"><FileText className="mr-2 inline h-4 w-4" />查看原始分析报告</summary>
        <article className="prose prose-slate mt-3 max-w-none border-t border-slate-100 pt-3 text-sm">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{panel.rawSections.reportMarkdown || '暂无原始分析报告'}</ReactMarkdown>
        </article>
      </details>

      <details className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800"><AlertTriangle className="mr-2 inline h-4 w-4" />查看原始结构化数据</summary>
        <pre className="mt-3 max-h-[420px] overflow-auto rounded-lg bg-slate-950 p-3 text-xs leading-5 text-slate-100">
          {JSON.stringify(panel.rawSections.displayJson ?? panel.rawSections.structuredJson ?? {}, null, 2)}
        </pre>
      </details>

      <details className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <summary className="cursor-pointer text-sm font-semibold text-slate-800"><FileText className="mr-2 inline h-4 w-4" />查看原始资料汇总 Markdown</summary>
        <article className="prose prose-slate mt-3 max-w-none border-t border-slate-100 pt-3 text-sm">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{panel.rawSections.profileMarkdown || '暂无内容'}</ReactMarkdown>
        </article>
      </details>
    </div>
  );
};

export default FinancialReportView;
