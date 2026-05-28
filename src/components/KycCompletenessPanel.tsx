import React from 'react';
import type { KycCompletenessResult } from '../services/types';

function ListBlock({ title, items, tone }: { title: string; items: string[]; tone: 'red' | 'amber' | 'blue' | 'slate' }) {
  const toneClass = {
    red: 'border-red-200 bg-red-50 text-red-700',
    amber: 'border-amber-200 bg-amber-50 text-amber-700',
    blue: 'border-blue-200 bg-blue-50 text-blue-700',
    slate: 'border-slate-200 bg-slate-50 text-slate-600',
  }[tone];
  return (
    <div className={`rounded-xl border p-3 ${toneClass}`}>
      <div className="text-sm font-semibold">{title}</div>
      {items.length ? (
        <ul className="mt-2 space-y-1 text-sm">
          {items.map((item) => <li key={item}>{item}</li>)}
        </ul>
      ) : (
        <div className="mt-2 text-sm opacity-80">无</div>
      )}
    </div>
  );
}

const KycCompletenessPanel: React.FC<{ completeness: KycCompletenessResult | null; loading?: boolean }> = ({ completeness, loading }) => {
  if (loading) return null;
  const result = completeness || {
    completeness_score: 0,
    required_missing: [],
    optional_missing: [],
    warnings: [],
    conflicts: [],
    suggestions: [],
  };
  return (
    <section className="border-b border-slate-200 bg-white px-6 py-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">KYC资料完整性</h3>
          <p className="mt-1 text-sm text-slate-500">检查必需证照、账户资料、字段冲突和补充建议。</p>
        </div>
        <div className="rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm font-semibold text-emerald-700">
          完整度 {result.completeness_score}
        </div>
      </div>
      <div className="grid gap-3 xl:grid-cols-3">
        <ListBlock title="必缺资料" items={result.required_missing || []} tone="red" />
        <ListBlock title="可选缺失资料" items={result.optional_missing || []} tone="slate" />
        <ListBlock title="字段冲突" items={result.conflicts || []} tone="amber" />
      </div>
      <div className="mt-3 grid gap-3 xl:grid-cols-2">
        <ListBlock title="校验提醒" items={result.warnings || []} tone="blue" />
        <ListBlock title="补充建议" items={result.suggestions || []} tone="slate" />
      </div>
    </section>
  );
};

export default KycCompletenessPanel;
