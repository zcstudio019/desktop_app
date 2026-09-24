import { useEffect, useMemo, useState } from 'react';
import {
  getLatestProductMatching, runProductMatching,
  type FinancingRequirementData, type ProductMatchItemData, type ProductMatchingSnapshotData, type ProductMatchingStatus,
} from '../services/api';

const STATUS_LABELS: Record<ProductMatchingStatus, string> = {
  eligible: '符合当前已知硬条件', conditional: '条件性匹配', ineligible: '不符合明确硬条件',
  manual_review: '需要人工复核', product_configuration_error: '产品配置异常',
};
const CATEGORY_LABELS: Record<string, string> = {
  guarantee_fund: '担保基金', personal_mortgage: '个人抵押', personal_credit: '个人信用',
  technology_enterprise: '科技企业', enterprise_mortgage: '企业抵押', enterprise_credit: '企业信用',
};
const DOMAIN_LABELS: Record<string, string> = { asset: '资产', business: '税务与开票', qualification: '科技资质' };
const FIELD_LABELS: Record<string, string> = { 'cashflow.operating_inflow': '经营流入' };
const STATUS_ORDER: ProductMatchingStatus[] = ['eligible', 'conditional', 'ineligible', 'manual_review', 'product_configuration_error'];

function ProductCard({ item }: { item: ProductMatchItemData }) {
  const passed = item.rule_results.filter(rule => rule.result === 'passed').map(rule => rule.explanation);
  const configurationProblems = item.rule_results.filter(rule => rule.result === null).map(rule => rule.explanation);
  return <details className="rounded-lg border border-slate-200 bg-white p-3" data-status={item.overall_status}>
    <summary className="cursor-pointer list-none">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div><div className="font-medium text-slate-900">{item.institution_name} · {item.product_name}</div>
          <div className="mt-1 text-xs text-slate-500">{item.external_product_code} · {CATEGORY_LABELS[item.product_category] || item.product_category}</div></div>
        <span className="rounded-full bg-slate-100 px-2 py-1 text-xs text-slate-700">{STATUS_LABELS[item.overall_status]}</span>
      </div>
      <div className="mt-2 flex gap-4 text-xs text-slate-600">
        <span>最高额度：{item.max_amount ? `${Number(item.max_amount).toLocaleString('zh-CN')}元` : '资料不足'}</span>
        <span>最长期限：{item.max_term_months ? `${item.max_term_months}个月` : '资料不足'}</span>
      </div>
    </summary>
    <div className="mt-3 grid gap-2 border-t pt-3 text-xs text-slate-700">
      {passed.length > 0 && <section><strong>通过条件</strong><ul className="mt-1 list-disc pl-5">{passed.map((text, index) => <li key={`p-${index}`}>{text}</li>)}</ul></section>}
      {item.blocking_reasons.length > 0 && <section><strong>不通过条件</strong><ul className="mt-1 list-disc pl-5">{item.blocking_reasons.map((text, index) => <li key={`b-${index}`}>{text}</li>)}</ul></section>}
      {item.missing_information.length > 0 && <section><strong>资料不足</strong><ul className="mt-1 list-disc pl-5">{item.missing_information.map((text, index) => <li key={`m-${index}`}>{text}</li>)}</ul></section>}
      {item.review_reasons.length > 0 && <section><strong>人工复核</strong><ul className="mt-1 list-disc pl-5">{item.review_reasons.map((text, index) => <li key={`r-${index}`}>{text}</li>)}</ul></section>}
      {item.soft_gaps.length > 0 && <section><strong>软性差距</strong><ul className="mt-1 list-disc pl-5">{item.soft_gaps.map((text, index) => <li key={`s-${index}`}>{text}</li>)}</ul></section>}
      {configurationProblems.length > 0 && <section><strong>配置异常</strong><ul className="mt-1 list-disc pl-5">{configurationProblems.map((text, index) => <li key={`c-${index}`}>{text}</li>)}</ul></section>}
    </div>
  </details>;
}

export function ProductMatchingPanel({ requirement }: { requirement: FinancingRequirementData }) {
  const [snapshot, setSnapshot] = useState<ProductMatchingSnapshotData | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    void getLatestProductMatching(requirement.customer_id).then(value => {
      if (active && value?.requirement_id === requirement.requirement_id) setSnapshot(value);
    }).catch(() => undefined);
    return () => { active = false; };
  }, [requirement.customer_id, requirement.requirement_id]);

  const grouped = useMemo(() => Object.fromEntries(STATUS_ORDER.map(status => [status, snapshot?.items.filter(item => item.overall_status === status) || []])) as Record<ProductMatchingStatus, ProductMatchItemData[]>, [snapshot]);

  async function match() {
    setBusy(true); setError('');
    try { setSnapshot(await runProductMatching(requirement.customer_id, requirement.requirement_id)); }
    catch (cause) { setError(cause instanceof Error ? cause.message : '产品匹配失败'); }
    finally { setBusy(false); }
  }

  return <section className="mt-3 rounded-xl border border-indigo-200 bg-white p-3" data-testid="product-matching-panel">
    <div className="flex items-center justify-between gap-3">
      <div><h3 className="font-semibold text-slate-900">产品规则匹配</h3><p className="mt-1 text-xs text-slate-500">仅判断当前已知硬条件，不构成产品推荐或审批结论。</p></div>
      <button type="button" disabled={busy} onClick={() => void match()} className="rounded bg-indigo-700 px-3 py-1.5 text-sm text-white disabled:opacity-50">{busy ? '匹配中…' : snapshot ? '重新匹配' : '开始产品匹配'}</button>
    </div>
    {error && <p role="alert" className="mt-2 text-xs text-red-700">{error}</p>}
    {snapshot?.status === 'no_active_products' && <p className="mt-3 rounded bg-amber-50 p-2 text-sm text-amber-800">{snapshot.message}</p>}
    {snapshot?.status === 'completed' && <div className="mt-3 space-y-4">
      <div className="rounded bg-slate-50 p-3 text-xs text-slate-600">
        <div>融资需求：{requirement.requested_amount?.toLocaleString('zh-CN')}元 · {requirement.purpose_detail || requirement.financing_purpose} · {requirement.term_value}{requirement.term_unit === 'month' ? '个月' : requirement.term_unit === 'year' ? '年' : '天'}</div>
        <div className="mt-1">匹配时间：{snapshot.generated_at ? new Date(snapshot.generated_at).toLocaleString('zh-CN') : '—'} · 产品库时点：{snapshot.catalog_as_of_date || '—'}</div>
      </div>
      {(snapshot.data_quality.missing_domains?.length || snapshot.data_quality.preliminary_fields?.length) ? <div className="rounded bg-amber-50 p-3 text-xs text-amber-800">
        <strong>当前数据质量</strong>
        {!!snapshot.data_quality.missing_domains?.length && <div className="mt-1">缺失域：{snapshot.data_quality.missing_domains.map(value => DOMAIN_LABELS[value] || value).join('、')}</div>}
        {!!snapshot.data_quality.preliminary_fields?.length && <div className="mt-1">初步分类字段：{snapshot.data_quality.preliminary_fields.map(value => FIELD_LABELS[value] || value).join('、')}</div>}
      </div> : null}
      {STATUS_ORDER.map(status => <section key={status}>
        <h4 className="mb-2 font-medium text-slate-800">{STATUS_LABELS[status]}：{grouped[status].length}</h4>
        <div className="grid gap-2">{grouped[status].map(item => <ProductCard key={item.version_id} item={item} />)}</div>
      </section>)}
    </div>}
  </section>;
}
