import React from 'react';
import type { FinancingKycDiagnosticResult } from '../services/types';

const READINESS_LABELS: Record<string, string> = {
  not_ready: '未就绪',
  basic_ready: '基本就绪',
  ready: '已就绪',
};

const STATUS_LABELS: Record<string, string> = {
  missing: '缺失',
  partial: '部分完整',
  complete: '完整',
  none: '无资产资料',
};

function ListBlock({ title, items, emptyText }: { title: string; items: string[]; emptyText: string }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
      <div className="text-sm font-semibold text-slate-800">{title}</div>
      {items.length > 0 ? (
        <ul className="mt-2 space-y-1 text-sm text-slate-700">
          {items.map((item) => <li key={item}>{item}</li>)}
        </ul>
      ) : (
        <div className="mt-2 text-sm text-slate-500">{emptyText}</div>
      )}
    </div>
  );
}

const FinancingKycDiagnosticPanel: React.FC<{
  diagnostic: FinancingKycDiagnosticResult | null;
  loading?: boolean;
}> = ({ diagnostic, loading }) => {
  if (loading) {
    return (
      <section className="border-b border-slate-200 bg-white px-6 py-5 text-sm text-slate-500">
        正在生成融资资料诊断...
      </section>
    );
  }

  if (!diagnostic) {
    return (
      <section className="border-b border-slate-200 bg-white px-6 py-5">
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
          暂无足够资料进行融资诊断
        </div>
      </section>
    );
  }

  const readinessLabel = READINESS_LABELS[diagnostic.readiness_level] || diagnostic.readiness_level || '未就绪';
  const usableText = diagnostic.usable_for_financing
    ? '是，已具备初步融资评估条件'
    : '否，请先补充关键资料或处理字段冲突';
  const readinessClass =
    diagnostic.readiness_level === 'ready'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : diagnostic.readiness_level === 'basic_ready'
        ? 'border-blue-200 bg-blue-50 text-blue-700'
        : 'border-amber-200 bg-amber-50 text-amber-700';

  return (
    <section className="border-b border-slate-200 bg-white px-6 py-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">融资资料诊断</h3>
          <p className="mt-1 text-sm text-slate-500">基于KYC画像、完整性检查和人工确认字段实时判断。</p>
        </div>
        <span className={`rounded-full border px-3 py-1.5 text-sm font-semibold ${readinessClass}`}>
          {readinessLabel}
        </span>
      </div>

      <div className="grid gap-3 xl:grid-cols-4">
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">初步融资评估条件</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{usableText}</div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">资料完整度分数</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{diagnostic.material_completeness_score}</div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">主体/身份/账户</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">
            {STATUS_LABELS[diagnostic.enterprise_status] || diagnostic.enterprise_status} / {STATUS_LABELS[diagnostic.identity_status] || diagnostic.identity_status} / {STATUS_LABELS[diagnostic.bank_account_status] || diagnostic.bank_account_status}
          </div>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="text-xs text-slate-500">资产资料状态</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{STATUS_LABELS[diagnostic.asset_status] || diagnostic.asset_status}</div>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm leading-6 text-slate-700">
        {diagnostic.summary || '暂无足够资料进行融资诊断'}
      </div>

      <div className="mt-4 grid gap-3 xl:grid-cols-2">
        <ListBlock title="主要风险" items={diagnostic.key_risks || []} emptyText="暂无明显KYC风险" />
        <ListBlock title="缺失资料" items={diagnostic.missing_materials || []} emptyText="暂无必备资料缺失" />
        <ListBlock title="建议操作" items={diagnostic.recommended_actions || []} emptyText="暂无额外建议" />
        <ListBlock title="字段冲突" items={diagnostic.conflicts || []} emptyText="暂无字段冲突" />
      </div>

      {diagnostic.next_step ? (
        <div className="mt-4 rounded-xl border border-blue-100 bg-blue-50 px-4 py-3 text-sm font-medium text-blue-800">
          下一步建议：{diagnostic.next_step}
        </div>
      ) : null}
    </section>
  );
};

export default FinancingKycDiagnosticPanel;
