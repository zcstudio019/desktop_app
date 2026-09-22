import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { RefreshCcw, X } from 'lucide-react';
import {
  changeCatalogVersion, deleteCatalogRule, getCatalogProductHistory, getCatalogRuleOptions, getCatalogVersion,
  getLocalCatalogConflicts, getLocalCatalogProducts, getLocalCatalogSources, listCatalogProducts, listCatalogVersions,
  patchCatalogDraft, saveCatalogRule, syncLocalCatalog,
  type CatalogProductRow, type CatalogRule, type CatalogVersionDetail, type LocalCatalogConflict,
  type LocalCatalogProductSummary, type LocalCatalogSourcesResponse,
} from '../../services/api';

type Tab = 'sources' | 'products' | 'review' | 'conflicts' | 'published' | 'history';
const TABS: { id: Tab; label: string }[] = [
  { id: 'sources', label: '产品源' }, { id: 'products', label: '产品列表' }, { id: 'review', label: '待审核' },
  { id: 'conflicts', label: '冲突' }, { id: 'published', label: '已发布' }, { id: 'history', label: '版本历史' },
];
const EDIT_FIELDS: { key: string; label: string; kind?: 'number' | 'list' | 'date' }[] = [
  { key: 'institution_name', label: '银行/机构' }, { key: 'product_name', label: '产品名称' },
  { key: 'loan_type', label: '贷款类型' }, { key: 'guarantee_type', label: '担保类型' },
  { key: 'rate_text', label: '利率原文' }, { key: 'min_amount', label: '最低额度', kind: 'number' },
  { key: 'max_amount', label: '最高额度', kind: 'number' }, { key: 'min_term_months', label: '最短期限（月）', kind: 'number' },
  { key: 'max_term_months', label: '最长期限（月）', kind: 'number' },
  { key: 'repayment_methods_json', label: '还款方式（每行一项）', kind: 'list' },
  { key: 'region_scope_json', label: '地区（每行一项）', kind: 'list' },
  { key: 'company_age_months', label: '企业成立月数', kind: 'number' },
  { key: 'borrower_age_min', label: '借款人最小年龄', kind: 'number' }, { key: 'borrower_age_max', label: '借款人最大年龄', kind: 'number' },
  { key: 'tax_grade', label: '纳税等级' }, { key: 'revenue_requirement', label: '营收要求' },
  { key: 'tax_requirement', label: '纳税要求' }, { key: 'invoice_requirement', label: '开票要求' },
  { key: 'credit_overdue_requirement', label: '逾期要求' }, { key: 'credit_query_requirement', label: '征信查询要求' },
  { key: 'debt_requirement', label: '负债要求' }, { key: 'collateral_requirement', label: '抵押物要求' },
  { key: 'materials_json', label: '材料（每行一项）', kind: 'list' },
  { key: 'suitable_customer_text', label: '适合客户' }, { key: 'notes', label: '备注' },
  { key: 'effective_from', label: '生效日期', kind: 'date' }, { key: 'effective_to', label: '失效日期', kind: 'date' },
];
const REVIEW_KEYS = ['external_product_code', 'institution_name', 'product_name', 'product_category', 'max_amount',
  'max_term_months', 'region_scope', 'guarantee_modes', 'collateral_types', 'materials', 'company_age_rule'];
const STATUS_LABELS: Record<string, string> = { draft: '草稿', needs_review: '待审核', published: '已发布',
  superseded: '已替代', expired: '已过期', disabled: '已停用' };
const REVIEW_LABELS: Record<string, string> = { unreviewed: '未审核', reviewing: '审核中', reviewed: '已审核', rejected: '已驳回' };

function fieldValue(value: unknown): string {
  if (Array.isArray(value)) return value.join('\n');
  return value == null ? '' : String(value);
}

const LocalProductCatalogSection: React.FC = () => {
  const [tab, setTab] = useState<Tab>('sources');
  const [sources, setSources] = useState<LocalCatalogSourcesResponse | null>(null);
  const [rows, setRows] = useState<CatalogProductRow[]>([]);
  const [parsed, setParsed] = useState<LocalCatalogProductSummary[]>([]);
  const [conflicts, setConflicts] = useState<LocalCatalogConflict[]>([]);
  const [detail, setDetail] = useState<CatalogVersionDetail | null>(null);
  const [history, setHistory] = useState<CatalogProductRow[]>([]);
  const [fields, setFields] = useState<Record<string, string>>({});
  const [review, setReview] = useState<Record<string, string>>({});
  const [reviewStatus, setReviewStatus] = useState('unreviewed');
  const [ruleOptions, setRuleOptions] = useState<{ fields: Record<string, string>; operators: string[] } | null>(null);
  const [rule, setRule] = useState({ rule_id: '', field_name: '', operator: 'eq', expected_value: '', severity: 'hard', failure_action: 'exclude', message: '', source_text: '' });
  const [filter, setFilter] = useState({ category: '', institution: '', status: '', needs_review: '', search: '', conflict: '' });
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const loadSources = useCallback(async () => setSources(await getLocalCatalogSources()), []);
  const loadConflicts = useCallback(async () => setConflicts((await getLocalCatalogConflicts()).items), []);
  const loadRows = useCallback(async (currentTab: Tab, currentFilter = filter) => {
    if (currentTab === 'history') {
      setRows((await listCatalogVersions()).items);
      return;
    }
    const params: Record<string, string> = {};
    for (const [key, value] of Object.entries(currentFilter)) if (value && key !== 'conflict') params[key] = value;
    if (currentTab === 'review') params.needs_review = 'true';
    if (currentTab === 'published') params.status = 'published';
    setRows((await listCatalogProducts(params)).items);
  }, [filter]);

  const refresh = useCallback(async (currentTab: Tab) => {
    setBusy('refresh');
    try {
      await Promise.all([loadSources(), loadConflicts()]);
      if (currentTab !== 'sources' && currentTab !== 'conflicts') await loadRows(currentTab);
      setError('');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '产品库加载失败');
    } finally { setBusy(''); }
  }, [loadSources, loadConflicts, loadRows]);

  useEffect(() => { void refresh('sources'); }, []);
  useEffect(() => { if (tab !== 'sources' && tab !== 'conflicts') void loadRows(tab).catch((cause) => setError(String(cause))); }, [tab, filter]);

  const openVersion = async (versionId: string, productId: string) => {
    setBusy('detail');
    try {
      const [result, versions, options] = await Promise.all([
        getCatalogVersion(versionId), getCatalogProductHistory(productId), getCatalogRuleOptions(),
      ]);
      setDetail(result);
      setHistory(versions.items.sort((a, b) => b.version.version_number - a.version.version_number));
      setRuleOptions(options);
      setFields(Object.fromEntries(EDIT_FIELDS.map((item) => [item.key, fieldValue(result.version[item.key])])));
      setReview(result.version.field_review_json || {});
      setReviewStatus(result.version.review_status);
      setRule({ rule_id: '', field_name: '', operator: 'eq', expected_value: '', severity: 'hard', failure_action: 'exclude', message: '', source_text: '' });
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '产品详情加载失败'); }
    finally { setBusy(''); }
  };

  const sync = async (category: string) => {
    setBusy(category);
    try {
      const result = await syncLocalCatalog(category);
      setNotice(`新增 ${result.created_drafts} 个草稿，未变化 ${result.unchanged} 个；冲突编号 ${result.conflicts.length} 个。未发布产品。`);
      await Promise.all([loadSources(), loadConflicts()]);
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '同步失败'); }
    finally { setBusy(''); }
  };

  const saveDraft = async () => {
    if (!detail) return;
    setBusy('save');
    try {
      const patch: Record<string, unknown> = { field_review_json: review, review_status: reviewStatus };
      for (const item of EDIT_FIELDS) {
        const value = fields[item.key] || '';
        patch[item.key] = item.kind === 'list' ? value.split('\n').map((part) => part.trim()).filter(Boolean)
          : item.kind === 'number' ? (value.trim() === '' ? null : Number(value))
            : item.kind === 'date' ? (value.trim() || null) : value;
      }
      const saved = await patchCatalogDraft(detail.version.version_id, patch);
      setDetail(saved);
      setNotice('草稿与人工审核状态已保存。');
      await loadRows(tab);
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '保存失败'); }
    finally { setBusy(''); }
  };

  const saveRule = async () => {
    if (!detail) return;
    setBusy('rule');
    try {
      let expected: unknown = null;
      if (!['exists', 'not_exists'].includes(rule.operator)) {
        try { expected = JSON.parse(rule.expected_value); } catch { expected = rule.expected_value; }
      }
      await saveCatalogRule(detail.version.version_id, { ...rule, expected_value: expected }, rule.rule_id || undefined);
      await openVersion(detail.version.version_id, detail.product.product_id);
      setNotice('规则已保存。');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '规则保存失败'); }
    finally { setBusy(''); }
  };

  const removeRule = async (item: CatalogRule) => {
    if (!detail || !window.confirm('确认删除这条草稿规则吗？')) return;
    try {
      await deleteCatalogRule(detail.version.version_id, item.rule_id);
      await openVersion(detail.version.version_id, detail.product.product_id);
    } catch (cause) { setError(cause instanceof Error ? cause.message : '规则删除失败'); }
  };

  const changeLifecycle = async (action: 'publish' | 'disable') => {
    if (!detail) return;
    const message = action === 'publish' ? '发布后该版本不可修改。后续修改将生成新版本。确认发布？' : '确认停用此产品版本？';
    if (!window.confirm(message)) return;
    setBusy(action);
    try {
      const changed = await changeCatalogVersion(detail.version.version_id, action);
      setDetail(changed);
      setNotice(action === 'publish' ? '产品版本已发布。' : '产品版本已停用。');
      await Promise.all([loadRows(tab), loadSources()]);
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '操作失败'); }
    finally { setBusy(''); }
  };

  const conflictCodes = useMemo(() => new Set(conflicts.map((item) => item.external_product_code)), [conflicts]);
  const displayRows = filter.conflict === 'yes' ? rows.filter((row) => conflictCodes.has(row.product.external_product_code || ''))
    : filter.conflict === 'no' ? rows.filter((row) => !conflictCodes.has(row.product.external_product_code || '')) : rows;
  const editable = !!detail && ['draft', 'needs_review'].includes(detail.version.status);
  const publishReady = editable && detail.version.review_status === 'reviewed' && !!detail.version.effective_from
    && !conflictCodes.has(detail.product.external_product_code || '')
    && REVIEW_KEYS.every((key) => ['confirmed', 'acknowledged_unknown'].includes(detail.version.field_review_json?.[key]));

  return <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm" data-testid="product-catalog-admin">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h2 className="text-xl font-semibold text-slate-900">产品库管理</h2><p className="text-sm text-slate-500">本地 Markdown 编辑源 → 待审核草稿 → 人工发布</p></div>
      <button type="button" onClick={() => void refresh(tab)} disabled={!!busy} className="flex items-center gap-2 rounded-lg border px-3 py-2 text-sm disabled:opacity-50"><RefreshCcw size={16} />刷新</button>
    </div>
    <nav className="mt-5 flex flex-wrap gap-2" aria-label="产品库管理导航">{TABS.map((item) => <button key={item.id} type="button" onClick={() => setTab(item.id)}
      className={`rounded-lg px-3 py-2 text-sm ${tab === item.id ? 'bg-blue-600 text-white' : 'bg-slate-100 text-slate-700'}`}>{item.label}</button>)}</nav>
    {error && <p className="mt-4 rounded-lg bg-rose-50 p-3 text-sm text-rose-700" role="alert">{error}</p>}
    {notice && <p className="mt-4 rounded-lg bg-emerald-50 p-3 text-sm text-emerald-700">{notice}</p>}

    {tab === 'sources' && <div className="mt-5 overflow-x-auto"><table className="min-w-full text-sm">
      <thead className="bg-slate-50 text-left"><tr>{['来源文件/分类', '资料更新', '来源哈希', '声明', '解析', '唯一', '重复', '冲突', 'Draft', '已发布', '上次同步', '操作'].map((label) => <th key={label} className="p-2">{label}</th>)}</tr></thead>
      <tbody className="divide-y">{sources?.sources.map((source) => <tr key={source.category}>
        <td className="p-2"><b>{source.label}</b><div className="text-xs text-slate-500">{source.source_file}<br />{source.category}</div></td>
        <td className="p-2">{source.source_update_date || '未知'}</td><td className="p-2 font-mono text-xs" title={source.source_snapshot_hash || ''}>{source.source_snapshot_hash?.slice(0, 12) || '—'}</td>
        <td className="p-2">{source.declared_count ?? '—'}</td><td className="p-2">{source.parsed_count}</td><td className="p-2">{source.unique_count}</td>
        <td className="p-2">{source.duplicate_count}</td><td className="p-2">{source.conflict_count}</td><td className="p-2">{source.draft_count ?? '—'}</td>
        <td className="p-2">{source.published_count ?? '—'}</td><td className="p-2 text-xs">{source.last_synced_at || '未同步'}</td>
        <td className="p-2"><div className="flex gap-1 whitespace-nowrap">
          <button type="button" disabled={!!busy || source.missing || sources.database_status !== 'available'} onClick={() => void sync(source.category)} className="rounded bg-blue-600 px-2 py-1 text-white disabled:opacity-40">同步</button>
          <button type="button" onClick={() => void getLocalCatalogProducts(source.category).then((result) => { setParsed(result.items); setNotice(`已读取 ${source.label}：${result.total} 条解析记录。`); }).catch((cause) => setError(String(cause)))} className="rounded border px-2 py-1">查看产品</button>
          <button type="button" onClick={() => setTab('conflicts')} className="rounded border px-2 py-1">查看冲突</button>
        </div></td>
      </tr>)}</tbody>
    </table>{parsed.length > 0 && <div className="mt-4 max-h-52 overflow-y-auto rounded-lg bg-slate-50 p-3 text-sm">{parsed.map((item, index) => <p key={`${item.external_product_code}-${index}`}>{item.external_product_code} · {item.institution_name} · {item.product_name}{item.duplicate_conflict ? ' · 编号冲突' : ''}</p>)}</div>}</div>}

    {['products', 'review', 'published', 'history'].includes(tab) && <div className="mt-5">
      <div className="mb-3 flex flex-wrap gap-2 text-sm">
        <input aria-label="搜索产品" placeholder="搜索编号/产品/银行" value={filter.search} onChange={(event) => setFilter({ ...filter, search: event.target.value })} className="rounded-lg border px-3 py-2" />
        <select aria-label="产品分类" value={filter.category} onChange={(event) => setFilter({ ...filter, category: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部分类</option>{sources?.sources.map((source) => <option key={source.category} value={source.category}>{source.label}</option>)}</select>
        <input aria-label="筛选银行" placeholder="银行/机构" value={filter.institution} onChange={(event) => setFilter({ ...filter, institution: event.target.value })} className="rounded-lg border px-3 py-2" />
        <select aria-label="版本状态" value={filter.status} onChange={(event) => setFilter({ ...filter, status: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部状态</option>{['draft', 'needs_review', 'published', 'superseded', 'expired', 'disabled'].map((value) => <option key={value} value={value}>{STATUS_LABELS[value]}</option>)}</select>
        <select aria-label="是否待审核" value={filter.needs_review} onChange={(event) => setFilter({ ...filter, needs_review: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部审核状态</option><option value="true">待审核</option><option value="false">已审核</option></select>
        <select aria-label="是否冲突" value={filter.conflict} onChange={(event) => setFilter({ ...filter, conflict: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部冲突状态</option><option value="yes">有冲突</option><option value="no">无冲突</option></select>
      </div>
      <div className="overflow-x-auto"><table className="min-w-full text-sm"><thead className="bg-slate-50 text-left"><tr>{['编号', '产品', '银行/机构', '分类', '版本', '状态', '额度上限', '期限上限', '待审核', '来源', '更新时间'].map((label) => <th key={label} className="p-2">{label}</th>)}</tr></thead>
        <tbody className="divide-y">{displayRows.map(({ product, version }) => <tr key={version.version_id} className="cursor-pointer hover:bg-slate-50" onClick={() => void openVersion(version.version_id, product.product_id)}>
          <td className="p-2 font-mono">{product.external_product_code || '—'}</td><td className="p-2 font-medium">{version.product_name}</td><td className="p-2">{version.institution_name}</td>
          <td className="p-2">{product.product_category}</td><td className="p-2">V{version.version_number}</td><td className="p-2">{STATUS_LABELS[version.status] || version.status}</td>
          <td className="p-2">{version.max_amount || '—'}</td><td className="p-2">{version.max_term_months ?? '—'}</td><td className="p-2">{version.needs_review ? '是' : '否'}</td>
          <td className="p-2">{version.source_file || '飞书'}</td><td className="p-2">{version.source_imported_at}</td>
        </tr>)}</tbody></table>{displayRows.length === 0 && <p className="p-6 text-center text-slate-500">暂无符合条件的产品。</p>}</div>
    </div>}

    {tab === 'conflicts' && <div className="mt-5 space-y-3">{conflicts.length === 0 ? <p>暂无冲突。</p> : conflicts.map((item) => <div key={item.external_product_code} className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm">
      <div className="font-semibold">{item.external_product_code} · 待处理</div><div className="mt-1">冲突字段：{item.conflict_fields?.join('、') || '产品标题或正文不同'}</div>
      {item.sides.map((side, index) => <div key={`${side.snapshot_hash}-${index}`} className="mt-2 rounded bg-white p-2">{index + 1}. {side.source_file} · {side.product_name}<div className="break-all font-mono text-xs">SHA-256: {side.snapshot_hash}</div></div>)}
    </div>)}</div>}

    {detail && <div className="fixed inset-0 z-50 flex justify-end bg-slate-900/40" role="dialog" aria-modal="true" aria-label="产品详情">
      <div className="h-full w-full max-w-6xl overflow-y-auto bg-white p-6 shadow-xl"><div className="flex items-start justify-between gap-3">
        <div><h3 className="text-xl font-semibold">{detail.product.external_product_code} · {detail.version.product_name}</h3><p className="text-sm text-slate-500">{detail.version.institution_name} · {detail.product.product_category} · V{detail.version.version_number} · {STATUS_LABELS[detail.version.status] || detail.version.status}</p></div>
        <button type="button" aria-label="关闭产品详情" onClick={() => setDetail(null)} className="rounded-lg border p-2"><X size={18} /></button>
      </div>
        {error && <p className="mt-3 rounded-lg bg-rose-50 p-3 text-sm text-rose-700" role="alert">{error}</p>}
        {notice && <p className="mt-3 rounded-lg bg-emerald-50 p-3 text-sm text-emerald-700">{notice}</p>}
        <div className="mt-4 flex flex-wrap gap-2">
          {editable && <button type="button" onClick={() => void saveDraft()} disabled={!!busy} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white disabled:opacity-50">保存草稿</button>}
          {editable && <button type="button" onClick={() => void changeLifecycle('publish')} disabled={!publishReady || !!busy} title={publishReady ? '' : '请先保存生效日期、人工审核状态与关键字段复核'} className="rounded-lg bg-emerald-600 px-4 py-2 text-sm text-white disabled:opacity-40">发布</button>}
          {detail.version.status === 'published' && <button type="button" onClick={() => void changeLifecycle('disable')} disabled={!!busy} className="rounded-lg border border-rose-400 px-4 py-2 text-sm text-rose-700">停用</button>}
        </div>
        <div className="mt-5 grid gap-6 lg:grid-cols-2"><div className="space-y-5">
          <div><h4 className="font-semibold">基本信息与结构化字段</h4><div className="mt-2 grid gap-3 sm:grid-cols-2">{EDIT_FIELDS.map((item) => <label key={item.key} className="text-sm">{item.label}
            {item.kind === 'list' || ['notes', 'suitable_customer_text', 'revenue_requirement', 'tax_requirement', 'invoice_requirement', 'credit_overdue_requirement', 'credit_query_requirement', 'debt_requirement', 'collateral_requirement'].includes(item.key)
              ? <textarea value={fields[item.key] || ''} disabled={!editable} onChange={(event) => setFields({ ...fields, [item.key]: event.target.value })} rows={item.kind === 'list' ? 2 : 3} className="mt-1 w-full rounded-lg border p-2 disabled:bg-slate-50" />
              : <input type={item.kind === 'date' ? 'date' : item.kind === 'number' ? 'number' : 'text'} value={fields[item.key] || ''} disabled={!editable} onChange={(event) => setFields({ ...fields, [item.key]: event.target.value })} className="mt-1 w-full rounded-lg border p-2 disabled:bg-slate-50" />}
          </label>)}</div></div>
          <div><h4 className="font-semibold">人工审核</h4><select aria-label="人工审核状态" value={reviewStatus} disabled={!editable} onChange={(event) => setReviewStatus(event.target.value)} className="mt-2 rounded-lg border p-2">{['unreviewed', 'reviewing', 'reviewed', 'rejected'].map((value) => <option key={value} value={value}>{REVIEW_LABELS[value]}</option>)}</select>
            <h5 className="mt-3 font-medium">待审核原因</h5><ul className="ml-5 list-disc text-sm">{detail.version.review_reasons_json?.length ? detail.version.review_reasons_json.map((reason) => <li key={reason}>{reason}</li>) : <li>暂无自动标记原因，请核对来源原文。</li>}</ul>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">{REVIEW_KEYS.map((key) => <label key={key} className="text-xs">{key}<select value={review[key] || 'insufficient_data'} disabled={!editable} onChange={(event) => setReview({ ...review, [key]: event.target.value })} className="mt-1 w-full rounded border p-1">{['extracted_review', 'needs_review', 'insufficient_data', 'confirmed', 'acknowledged_unknown'].map((value) => <option key={value}>{value}</option>)}</select></label>)}</div>
          </div>
          <div><h4 className="font-semibold">产品规则</h4>{detail.rules.length === 0 ? <p className="mt-2 text-sm text-slate-500">暂无结构化规则</p> : detail.rules.map((item) => <div key={item.rule_id} className="mt-2 rounded-lg border p-3 text-sm">
            <b>{item.field_name} {item.operator} {JSON.stringify(item.expected_value_json)}</b><div>{item.severity} · {item.failure_action} · {item.message}</div><div className="text-slate-500">来源：{item.source_text}</div>
            {editable && <div className="mt-2 flex gap-2"><button type="button" className="text-blue-700" onClick={() => setRule({ rule_id: item.rule_id, field_name: item.field_name, operator: item.operator, expected_value: JSON.stringify(item.expected_value_json), severity: item.severity, failure_action: item.failure_action, message: item.message, source_text: item.source_text })}>编辑</button><button type="button" className="text-rose-700" onClick={() => void removeRule(item)}>删除</button></div>}
          </div>)}
            {editable && <div className="mt-3 grid gap-2 rounded-lg bg-slate-50 p-3 text-sm sm:grid-cols-2">
              <label>字段<select aria-label="规则字段" value={rule.field_name} onChange={(event) => setRule({ ...rule, field_name: event.target.value })} className="mt-1 w-full rounded border p-2"><option value="">选择白名单字段</option>{Object.keys(ruleOptions?.fields || {}).map((name) => <option key={name}>{name}</option>)}</select></label>
              <label>操作符<select aria-label="规则操作符" value={rule.operator} onChange={(event) => setRule({ ...rule, operator: event.target.value })} className="mt-1 w-full rounded border p-2">{ruleOptions?.operators.map((value) => <option key={value}>{value}</option>)}</select></label>
              <label>预期值（JSON 或文本）<input value={rule.expected_value} onChange={(event) => setRule({ ...rule, expected_value: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <label>严重程度<select value={rule.severity} onChange={(event) => setRule({ ...rule, severity: event.target.value })} className="mt-1 w-full rounded border p-2">{['hard', 'soft', 'info'].map((value) => <option key={value}>{value}</option>)}</select></label>
              <label>失败动作<select value={rule.failure_action} onChange={(event) => setRule({ ...rule, failure_action: event.target.value })} className="mt-1 w-full rounded border p-2">{['exclude', 'conditional', 'review'].map((value) => <option key={value}>{value}</option>)}</select></label>
              <label>说明<input value={rule.message} onChange={(event) => setRule({ ...rule, message: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <label className="sm:col-span-2">来源原文<textarea value={rule.source_text} onChange={(event) => setRule({ ...rule, source_text: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <button type="button" onClick={() => void saveRule()} disabled={!rule.field_name || !rule.source_text || !!busy} className="rounded bg-blue-600 px-3 py-2 text-white disabled:opacity-40">{rule.rule_id ? '更新规则' : '新增规则'}</button>
            </div>}
          </div>
          <div><h4 className="font-semibold">版本历史</h4>{history.map((item) => <button key={item.version.version_id} type="button" onClick={() => void openVersion(item.version.version_id, item.product.product_id)} className="mt-2 block w-full rounded-lg border p-2 text-left text-sm hover:bg-slate-50">
            V{item.version.version_number} · {item.version.status} · {item.version.effective_from || '未生效'} ~ {item.version.effective_to || '无结束日'} · {item.version.published_at || '未发布'} · {item.version.published_by || '—'}<div className="break-all font-mono text-xs">{item.version.source_snapshot_hash}</div>
          </button>)}</div>
        </div><div className="lg:sticky lg:top-0 lg:self-start"><h4 className="font-semibold">来源原文</h4><p className="text-xs text-slate-500">{detail.version.source_file} · {detail.version.source_update_date || '未知日期'} · SHA-256 {detail.version.source_snapshot_hash}</p>
          <pre className="mt-2 max-h-[75vh] overflow-auto whitespace-pre-wrap rounded-lg bg-slate-50 p-4 text-sm leading-relaxed">{detail.version.source_snapshot}</pre>
        </div></div>
      </div>
    </div>}
  </section>;
};

export default LocalProductCatalogSection;
