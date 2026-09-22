import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { RefreshCcw, X } from 'lucide-react';
import {
  changeCatalogVersion, deleteCatalogRule, getCatalogProductHistory, getCatalogRuleOptions, getCatalogVersion,
  getCatalogConflict, getLocalCatalogConflicts, getLocalCatalogProducts, getLocalCatalogSources, listCatalogProducts, listCatalogVersions,
  patchCatalogDraft, resolveCatalogConflict, saveCatalogRule, syncLocalCatalog,
  type CatalogConflictDecision, type CatalogConflictDetail, type CatalogProductRow, type CatalogRule, type CatalogVersionDetail, type LocalCatalogConflict,
  type LocalCatalogProductSummary, type LocalCatalogSourcesResponse,
} from '../../services/api';
import {
  PRODUCT_CATEGORY_LABELS, PRODUCT_FIELD_LABELS, REVIEW_STATUS_LABELS, RULE_ACTION_LABELS, RULE_FIELD_LABELS,
  RULE_OPERATOR_LABELS, RULE_SEVERITY_LABELS, VERSION_STATUS_LABELS, productFieldLabel,
} from './productCatalogLabels';

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
const CORE_REVIEW_KEYS = new Set(['external_product_code', 'institution_name', 'product_name', 'product_category']);
const FIELD_REVIEW_OPTIONS = ['extracted_review', 'needs_review', 'insufficient_data', 'reviewed', 'rejected', 'not_applicable'];
const CONFLICT_LABELS: Record<string, string> = { unresolved: '待处理', resolved_keep_a: '已选择 A', resolved_keep_b: '已选择 B', resolved_merged: '已合并', resolved_split: '已拆分' };

function preview(value: unknown): string { return value == null ? '—' : typeof value === 'string' ? value : JSON.stringify(value, null, 2); }

function fieldValue(value: unknown): string {
  if (Array.isArray(value)) return value.join('\n');
  return value == null ? '' : String(value);
}

function hasValue(value: unknown): boolean {
  if (value == null) return false;
  if (typeof value === 'string') return value.trim() !== '';
  if (Array.isArray(value)) return value.length > 0;
  return true;
}

function reviewFieldValue(detail: CatalogVersionDetail, key: string): unknown {
  const values: Record<string, unknown> = {
    external_product_code: detail.product.external_product_code, institution_name: detail.version.institution_name,
    product_name: detail.version.product_name, product_category: detail.product.product_category,
    max_amount: detail.version.max_amount, max_term_months: detail.version.max_term_months,
    region_scope: detail.version.region_scope_json, guarantee_modes: detail.version.guarantee_modes_json,
    collateral_types: detail.version.collateral_types_json, materials: detail.version.materials_json,
    company_age_rule: detail.version.company_age_months,
  };
  return values[key];
}

function pendingReviewFields(detail: CatalogVersionDetail): string[] {
  return REVIEW_KEYS.filter((key) => {
    const status = detail.version.field_review_json?.[key];
    const valuePresent = hasValue(reviewFieldValue(detail, key));
    if (CORE_REVIEW_KEYS.has(key)) return !valuePresent || !['reviewed', 'confirmed'].includes(status);
    if (!status || ['extracted_review', 'needs_review'].includes(status)) return true;
    if (valuePresent) return !['reviewed', 'confirmed'].includes(status);
    return !['reviewed', 'confirmed', 'insufficient_data', 'not_applicable', 'rejected', 'acknowledged_unknown'].includes(status);
  });
}

const LocalProductCatalogSection: React.FC = () => {
  const [tab, setTab] = useState<Tab>('sources');
  const [sources, setSources] = useState<LocalCatalogSourcesResponse | null>(null);
  const [rows, setRows] = useState<CatalogProductRow[]>([]);
  const [parsed, setParsed] = useState<LocalCatalogProductSummary[]>([]);
  const [conflicts, setConflicts] = useState<LocalCatalogConflict[]>([]);
  const [conflictDetail, setConflictDetail] = useState<CatalogConflictDetail | null>(null);
  const [conflictStrategy, setConflictStrategy] = useState<CatalogConflictDecision['strategy'] | ''>('');
  const [fieldChoices, setFieldChoices] = useState<Record<string, 'a' | 'b'>>({});
  const [renameSide, setRenameSide] = useState<'a' | 'b'>('b');
  const [newCode, setNewCode] = useState('');
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
      setReview(Object.fromEntries(Object.entries(result.version.field_review_json || {}).map(([key, value]) => [
        key, value === 'confirmed' ? 'reviewed' : value === 'acknowledged_unknown' ? 'insufficient_data' : value,
      ])));
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

  const openConflict = async (code: string) => {
    setBusy('conflict');
    try {
      const result = await getCatalogConflict(code);
      setConflictDetail(result);
      setConflictStrategy('');
      setFieldChoices({});
      setRenameSide('b');
      setNewCode('');
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '冲突详情加载失败'); }
    finally { setBusy(''); }
  };

  const submitConflictDecision = async () => {
    if (!conflictDetail) return;
    if (!conflictStrategy) { setError('请先选择一种人工处理方式。'); return; }
    if (conflictStrategy === 'merge' && conflictDetail.differences.some((item) => !fieldChoices[item.field_name])) {
      setError('请为每个差异字段选择来源 A 或 B。'); return;
    }
    if (conflictStrategy === 'split' && !newCode.trim()) { setError('请填写新的唯一产品编号。'); return; }
    if (!window.confirm('将按当前人工选择生成草稿，并保留两份原始来源记录。确认处理此冲突？')) return;
    setBusy('resolve');
    try {
      const decision: CatalogConflictDecision = { strategy: conflictStrategy };
      if (conflictStrategy === 'merge') decision.field_choices = fieldChoices;
      if (conflictStrategy === 'split') { decision.rename_side = renameSide; decision.new_code = newCode.trim().toUpperCase(); }
      const result = await resolveCatalogConflict(conflictDetail.external_product_code, decision);
      setNotice(`冲突已处理，生成 ${result.created_drafts} 个待审核草稿；未发布产品。`);
      setConflictDetail(null);
      await Promise.all([loadSources(), loadConflicts()]);
      setError('');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '冲突处理失败'); }
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

  const confirmExtractedFields = () => {
    setReview(Object.fromEntries(Object.entries(review).map(([key, value]) => [key, value === 'extracted_review' ? 'reviewed' : value])));
    setNotice('已将所有“已提取，待人工确认”字段标记为“已确认”。请检查其余待复核字段并保存草稿。');
  };

  const conflictCodes = useMemo(() => new Set(conflicts.filter((item) => item.conflict !== false).map((item) => item.external_product_code)), [conflicts]);
  const displayRows = filter.conflict === 'yes' ? rows.filter((row) => conflictCodes.has(row.product.external_product_code || ''))
    : filter.conflict === 'no' ? rows.filter((row) => !conflictCodes.has(row.product.external_product_code || '')) : rows;
  const editable = !!detail && ['draft', 'needs_review'].includes(detail.version.status);
  const pendingFields = detail ? pendingReviewFields(detail) : [];
  const publishIssues = detail ? [
    ...(!detail.version.effective_from ? ['请先设置生效日期'] : []),
    ...(detail.version.review_status !== 'reviewed' ? ['请先完成人工审核'] : []),
    ...(pendingFields.length ? [`尚未完成复核：${pendingFields.map(productFieldLabel).join('、')}`] : []),
    ...(conflictCodes.has(detail.product.external_product_code || '') ? ['产品编号仍存在未解决冲突'] : []),
  ] : [];
  const publishReady = editable && publishIssues.length === 0;

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
        <select aria-label="版本状态" value={filter.status} onChange={(event) => setFilter({ ...filter, status: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部状态</option>{['draft', 'needs_review', 'published', 'superseded', 'expired', 'disabled'].map((value) => <option key={value} value={value}>{VERSION_STATUS_LABELS[value]}</option>)}</select>
        <select aria-label="是否待审核" value={filter.needs_review} onChange={(event) => setFilter({ ...filter, needs_review: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部审核状态</option><option value="true">待审核</option><option value="false">已审核</option></select>
        <select aria-label="是否冲突" value={filter.conflict} onChange={(event) => setFilter({ ...filter, conflict: event.target.value })} className="rounded-lg border px-3 py-2"><option value="">全部冲突状态</option><option value="yes">有冲突</option><option value="no">无冲突</option></select>
      </div>
      <div className="overflow-x-auto"><table className="min-w-full text-sm"><thead className="bg-slate-50 text-left"><tr>{['编号', '产品', '银行/机构', '分类', '版本', '状态', '额度上限', '期限上限', '待审核', '来源', '更新时间'].map((label) => <th key={label} className="p-2">{label}</th>)}</tr></thead>
        <tbody className="divide-y">{displayRows.map(({ product, version }) => <tr key={version.version_id} className="cursor-pointer hover:bg-slate-50" onClick={() => void openVersion(version.version_id, product.product_id)}>
          <td className="p-2 font-mono">{product.external_product_code || '—'}</td><td className="p-2 font-medium">{version.product_name}</td><td className="p-2">{version.institution_name}</td>
          <td className="p-2">{PRODUCT_CATEGORY_LABELS[product.product_category] || '其他'}</td><td className="p-2">V{version.version_number}</td><td className="p-2">{VERSION_STATUS_LABELS[version.status] || '未知状态'}</td>
          <td className="p-2">{version.max_amount || '—'}</td><td className="p-2">{version.max_term_months ?? '—'}</td><td className="p-2">{version.needs_review ? '是' : '否'}</td>
          <td className="p-2">{version.source_file || '飞书'}</td><td className="p-2">{version.source_imported_at}</td>
        </tr>)}</tbody></table>{displayRows.length === 0 && <p className="p-6 text-center text-slate-500">暂无符合条件的产品。</p>}</div>
    </div>}

    {tab === 'conflicts' && <div className="mt-5 space-y-3">{conflicts.length === 0 ? <p>暂无冲突。</p> : conflicts.map((item) => <div key={item.external_product_code} className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm">
      <button type="button" onClick={() => void openConflict(item.external_product_code)} className="font-semibold text-blue-700 underline">{item.external_product_code} · {CONFLICT_LABELS[item.status] || '待处理'} · 查看对照与处理</button><div className="mt-1">冲突字段：{item.conflict_fields?.join('、') || '产品标题或正文不同'}</div>
      {item.sides.map((side, index) => <div key={`${side.snapshot_hash}-${index}`} className="mt-2 rounded bg-white p-2">{index + 1}. {side.source_file} · {side.product_name}<div className="break-all font-mono text-xs">SHA-256: {side.snapshot_hash}</div></div>)}
    </div>)}</div>}

    {conflictDetail && <div className="fixed inset-0 z-50 overflow-y-auto bg-slate-900/50 p-3 sm:p-6" role="dialog" aria-modal="true" aria-label="冲突详情">
      <div className="mx-auto max-w-7xl rounded-xl bg-white p-5 shadow-xl">
        <div className="flex justify-between gap-3"><div><h3 className="text-xl font-semibold">{conflictDetail.external_product_code} · 来源冲突</h3><p className="text-sm text-slate-500">逐项核对来源后，由管理员选择处理方式；已发布版本不受影响。</p></div><button type="button" aria-label="关闭冲突详情" onClick={() => setConflictDetail(null)}><X size={20} /></button></div>
        {error && <p role="alert" className="mt-3 rounded bg-rose-50 p-2 text-sm text-rose-700">{error}</p>}
        <div className="mt-4 grid gap-4 lg:grid-cols-2">{conflictDetail.sides.map((side) => <div key={side.side} className="min-w-0 rounded-xl border p-4 text-sm">
          <h4 className="text-lg font-semibold">来源 {side.side.toUpperCase()}</h4>
          <dl className="mt-2 grid grid-cols-[9rem_1fr] gap-1 break-all">{[
            ['产品编号', side.external_product_code], ['产品名称', side.product_name], ['银行/机构', side.institution_name],
            ['产品分类', PRODUCT_CATEGORY_LABELS[side.product_category] || '其他'], ['来源文件', side.source_file], ['来源更新日期', side.source_update_date || '未知'],
            ['来源 SHA-256', side.source_snapshot_hash],
          ].map(([label, value]) => <React.Fragment key={label}><dt className="text-slate-500">{label}</dt><dd>{value}</dd></React.Fragment>)}</dl>
          <h5 className="mt-4 font-semibold">已结构化字段</h5><div className="mt-1 max-h-60 overflow-auto rounded bg-slate-50 p-2">{Object.entries(side.structured_fields).map(([key, value]) => <div key={key} className={conflictDetail.differences.some((diff) => diff.field_name === key) ? 'bg-amber-100' : ''}><b>{productFieldLabel(key)}：</b>{preview(value)}</div>)}</div>
          <h5 className="mt-4 font-semibold">来源原始字段</h5><div className="mt-1 max-h-60 overflow-auto rounded bg-slate-50 p-2">{Object.entries(side.raw_fields).map(([key, value]) => <div key={key} className={conflictDetail.differences.some((diff) => diff.field_name === `raw_fields.${key}`) ? 'bg-amber-100' : ''}><b>{key}：</b>{preview(value)}</div>)}</div>
          <h5 className="mt-4 font-semibold">原始 Markdown</h5><pre className="mt-1 max-h-80 overflow-auto whitespace-pre-wrap rounded bg-slate-50 p-3">{side.source_snapshot}</pre>
        </div>)}</div>
        <h4 className="mt-5 font-semibold">差异字段（{conflictDetail.differences.length}）</h4>
        <div className="mt-2 max-h-80 overflow-auto rounded border text-sm">{conflictDetail.differences.map((diff) => <div key={diff.field_name} className="grid gap-2 border-b bg-amber-50 p-2 sm:grid-cols-[12rem_1fr_1fr]"><b>{productFieldLabel(diff.field_name)}</b><div>A：{preview(diff.a)}</div><div>B：{preview(diff.b)}</div></div>)}</div>
        {(conflictDetail.source_line_changes?.a_only.length > 0 || conflictDetail.source_line_changes?.b_only.length > 0) && <div className="mt-4 rounded border p-3 text-sm"><h4 className="font-semibold">原文行差异</h4><div className="mt-2 grid gap-3 lg:grid-cols-2"><div><b>仅来源 A</b>{conflictDetail.source_line_changes.a_only.map((line, index) => <pre key={index} className="mt-1 whitespace-pre-wrap bg-rose-50 p-1">{line}</pre>)}</div><div><b>仅来源 B</b>{conflictDetail.source_line_changes.b_only.map((line, index) => <pre key={index} className="mt-1 whitespace-pre-wrap bg-emerald-50 p-1">{line}</pre>)}</div></div></div>}
        {conflictDetail.conflict !== false ? <div className="mt-5 rounded-xl border p-4 text-sm"><h4 className="font-semibold">人工处理</h4>
          <div className="mt-2 flex flex-wrap gap-4">{([['keep_a', '同一产品，选择 A'], ['keep_b', '同一产品，选择 B'], ['merge', '同一产品，逐字段合并'], ['split', '两个不同产品，改编号']] as const).map(([value, label]) => <label key={value}><input type="radio" name="conflict-strategy" checked={conflictStrategy === value} onChange={() => setConflictStrategy(value)} /> {label}</label>)}</div>
          {conflictStrategy === 'merge' && <div className="mt-3 max-h-72 overflow-auto rounded border p-3"><p className="mb-2">每个差异字段都必须选择来源：</p>{conflictDetail.differences.map((diff) => <div key={diff.field_name} className="flex flex-wrap items-center gap-3 border-b py-2"><b className="min-w-44">{productFieldLabel(diff.field_name)}</b><label><input type="radio" name={`choice-${diff.field_name}`} checked={fieldChoices[diff.field_name] === 'a'} onChange={() => setFieldChoices({ ...fieldChoices, [diff.field_name]: 'a' })} /> 使用 A</label><label><input type="radio" name={`choice-${diff.field_name}`} checked={fieldChoices[diff.field_name] === 'b'} onChange={() => setFieldChoices({ ...fieldChoices, [diff.field_name]: 'b' })} /> 使用 B</label></div>)}</div>}
          {conflictStrategy === 'split' && <div className="mt-3 flex flex-wrap gap-3"><label>修改哪一侧编号<select aria-label="修改编号的来源" value={renameSide} onChange={(event) => setRenameSide(event.target.value as 'a' | 'b')} className="ml-2 rounded border p-2"><option value="a">来源 A</option><option value="b">来源 B</option></select></label><label>新产品编号<input aria-label="新产品编号" value={newCode} onChange={(event) => setNewCode(event.target.value)} className="ml-2 rounded border p-2" placeholder="例如 NJB-007" /></label></div>}
          <button type="button" onClick={() => void submitConflictDecision()} disabled={!!busy || !conflictStrategy} className="mt-4 rounded bg-blue-600 px-4 py-2 text-white disabled:opacity-50">确认生成待审核草稿</button>
        </div> : <p className="mt-4 text-sm text-emerald-700">该冲突已有人工决议。如需变更，请联系管理员复核来源和现有草稿。</p>}
      </div>
    </div>}

    {detail && <div className="fixed inset-0 z-50 flex justify-end bg-slate-900/40" role="dialog" aria-modal="true" aria-label="产品详情">
      <div className="h-full w-full max-w-6xl overflow-y-auto bg-white p-6 shadow-xl"><div className="flex items-start justify-between gap-3">
        <div><h3 className="text-xl font-semibold">{detail.product.external_product_code} · {detail.version.product_name}</h3><p className="text-sm text-slate-500">{detail.version.institution_name} · {PRODUCT_CATEGORY_LABELS[detail.product.product_category] || '其他'} · V{detail.version.version_number} · {VERSION_STATUS_LABELS[detail.version.status] || '未知状态'}</p></div>
        <button type="button" aria-label="关闭产品详情" onClick={() => setDetail(null)} className="rounded-lg border p-2"><X size={18} /></button>
      </div>
        {error && <p className="mt-3 rounded-lg bg-rose-50 p-3 text-sm text-rose-700" role="alert">{error}</p>}
        {notice && <p className="mt-3 rounded-lg bg-emerald-50 p-3 text-sm text-emerald-700">{notice}</p>}
        <div className="mt-4 flex flex-wrap gap-2">
          {editable && <button type="button" onClick={() => void saveDraft()} disabled={!!busy} className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white disabled:opacity-50">保存草稿</button>}
          {editable && <button type="button" onClick={() => void changeLifecycle('publish')} disabled={!publishReady || !!busy} title={publishReady ? '' : publishIssues.join('；')} className="rounded-lg bg-emerald-600 px-4 py-2 text-sm text-white disabled:opacity-40">发布</button>}
          {detail.version.status === 'published' && <button type="button" onClick={() => void changeLifecycle('disable')} disabled={!!busy} className="rounded-lg border border-rose-400 px-4 py-2 text-sm text-rose-700">停用</button>}
        </div>
        <div className="mt-5 grid gap-6 lg:grid-cols-2"><div className="space-y-5">
          <div><h4 className="font-semibold">基本信息与结构化字段</h4><div className="mt-2 grid gap-3 sm:grid-cols-2">{EDIT_FIELDS.map((item) => <label key={item.key} className="text-sm">{item.label}
            {item.kind === 'list' || ['notes', 'suitable_customer_text', 'revenue_requirement', 'tax_requirement', 'invoice_requirement', 'credit_overdue_requirement', 'credit_query_requirement', 'debt_requirement', 'collateral_requirement'].includes(item.key)
              ? <textarea value={fields[item.key] || ''} disabled={!editable} onChange={(event) => setFields({ ...fields, [item.key]: event.target.value })} rows={item.kind === 'list' ? 2 : 3} className="mt-1 w-full rounded-lg border p-2 disabled:bg-slate-50" />
              : <input type={item.kind === 'date' ? 'date' : item.kind === 'number' ? 'number' : 'text'} value={fields[item.key] || ''} disabled={!editable} onChange={(event) => setFields({ ...fields, [item.key]: event.target.value })} className="mt-1 w-full rounded-lg border p-2 disabled:bg-slate-50" />}
          </label>)}</div></div>
          <div><h4 className="font-semibold">人工审核</h4><select aria-label="人工审核状态" value={reviewStatus} disabled={!editable} onChange={(event) => setReviewStatus(event.target.value)} className="mt-2 rounded-lg border p-2">{['unreviewed', 'reviewing', 'reviewed', 'rejected'].map((value) => <option key={value} value={value}>{REVIEW_STATUS_LABELS[value]}</option>)}</select>
            {editable && <button type="button" onClick={confirmExtractedFields} className="ml-2 rounded-lg border border-blue-300 px-3 py-2 text-sm text-blue-700">确认所有已提取字段</button>}
            {publishIssues.length > 0 && <div className="mt-3 rounded-lg bg-amber-50 p-3 text-sm text-amber-800" aria-label="发布阻断原因"><b>发布前还需处理：</b><ul className="ml-5 mt-1 list-disc">{publishIssues.map((issue) => <li key={issue}>{issue}</li>)}</ul></div>}
            <h5 className="mt-3 font-medium">待审核原因</h5><ul className="ml-5 list-disc text-sm">{detail.version.review_reasons_json?.length ? detail.version.review_reasons_json.map((reason) => <li key={reason}>{reason}</li>) : <li>暂无自动标记原因，请核对来源原文。</li>}</ul>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">{REVIEW_KEYS.map((key) => <label key={key} className="text-xs">{PRODUCT_FIELD_LABELS[key]}<select aria-label={`${PRODUCT_FIELD_LABELS[key]}复核状态`} value={review[key] || 'insufficient_data'} disabled={!editable} onChange={(event) => setReview({ ...review, [key]: event.target.value })} className="mt-1 w-full rounded border p-1">{FIELD_REVIEW_OPTIONS.map((value) => <option key={value} value={value}>{REVIEW_STATUS_LABELS[value]}</option>)}</select></label>)}</div>
          </div>
          <div><h4 className="font-semibold">产品规则</h4>{detail.rules.length === 0 ? <p className="mt-2 text-sm text-slate-500">暂无结构化规则</p> : detail.rules.map((item) => <div key={item.rule_id} className="mt-2 rounded-lg border p-3 text-sm">
            <b>{RULE_FIELD_LABELS[item.field_name] || '其他规则字段'} {RULE_OPERATOR_LABELS[item.operator] || '未知操作'} {JSON.stringify(item.expected_value_json)}</b><div>{RULE_SEVERITY_LABELS[item.severity] || '未知级别'} · {RULE_ACTION_LABELS[item.failure_action] || '未知动作'} · {item.message}</div><div className="text-slate-500">来源：{item.source_text}</div>
            {editable && <div className="mt-2 flex gap-2"><button type="button" className="text-blue-700" onClick={() => setRule({ rule_id: item.rule_id, field_name: item.field_name, operator: item.operator, expected_value: JSON.stringify(item.expected_value_json), severity: item.severity, failure_action: item.failure_action, message: item.message, source_text: item.source_text })}>编辑</button><button type="button" className="text-rose-700" onClick={() => void removeRule(item)}>删除</button></div>}
          </div>)}
            {editable && <div className="mt-3 grid gap-2 rounded-lg bg-slate-50 p-3 text-sm sm:grid-cols-2">
              <label>字段<select aria-label="规则字段" value={rule.field_name} onChange={(event) => setRule({ ...rule, field_name: event.target.value })} className="mt-1 w-full rounded border p-2"><option value="">选择白名单字段</option>{Object.keys(ruleOptions?.fields || {}).map((name) => <option key={name} value={name}>{RULE_FIELD_LABELS[name] || '其他规则字段'}</option>)}</select></label>
              <label>操作符<select aria-label="规则操作符" value={rule.operator} onChange={(event) => setRule({ ...rule, operator: event.target.value })} className="mt-1 w-full rounded border p-2">{ruleOptions?.operators.map((value) => <option key={value} value={value}>{RULE_OPERATOR_LABELS[value] || '未知操作'}</option>)}</select></label>
              <label>预期值（JSON 或文本）<input value={rule.expected_value} onChange={(event) => setRule({ ...rule, expected_value: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <label>严重程度<select value={rule.severity} onChange={(event) => setRule({ ...rule, severity: event.target.value })} className="mt-1 w-full rounded border p-2">{['hard', 'soft', 'info'].map((value) => <option key={value} value={value}>{RULE_SEVERITY_LABELS[value]}</option>)}</select></label>
              <label>失败动作<select value={rule.failure_action} onChange={(event) => setRule({ ...rule, failure_action: event.target.value })} className="mt-1 w-full rounded border p-2">{['exclude', 'conditional', 'review'].map((value) => <option key={value} value={value}>{RULE_ACTION_LABELS[value]}</option>)}</select></label>
              <label>说明<input value={rule.message} onChange={(event) => setRule({ ...rule, message: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <label className="sm:col-span-2">来源原文<textarea value={rule.source_text} onChange={(event) => setRule({ ...rule, source_text: event.target.value })} className="mt-1 w-full rounded border p-2" /></label>
              <button type="button" onClick={() => void saveRule()} disabled={!rule.field_name || !rule.source_text || !!busy} className="rounded bg-blue-600 px-3 py-2 text-white disabled:opacity-40">{rule.rule_id ? '更新规则' : '新增规则'}</button>
            </div>}
          </div>
          <div><h4 className="font-semibold">版本历史</h4>{history.map((item) => <button key={item.version.version_id} type="button" onClick={() => void openVersion(item.version.version_id, item.product.product_id)} className="mt-2 block w-full rounded-lg border p-2 text-left text-sm hover:bg-slate-50">
            V{item.version.version_number} · {VERSION_STATUS_LABELS[item.version.status] || '未知状态'} · {item.version.effective_from || '未生效'} ~ {item.version.effective_to || '无结束日'} · {item.version.published_at || '未发布'} · {item.version.published_by || '—'}<div className="break-all font-mono text-xs">{item.version.source_snapshot_hash}</div>
          </button>)}</div>
        </div><div className="lg:sticky lg:top-0 lg:self-start"><h4 className="font-semibold">来源原文</h4><p className="text-xs text-slate-500">{detail.version.source_file} · {detail.version.source_update_date || '未知日期'} · SHA-256 {detail.version.source_snapshot_hash}</p>
          <pre className="mt-2 max-h-[75vh] overflow-auto whitespace-pre-wrap rounded-lg bg-slate-50 p-4 text-sm leading-relaxed">{detail.version.source_snapshot}</pre>
        </div></div>
      </div>
    </div>}
  </section>;
};

export default LocalProductCatalogSection;
