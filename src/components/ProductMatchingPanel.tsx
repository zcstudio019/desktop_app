import { useEffect, useMemo, useState } from 'react';
import {
  compareFinancingPlanVersions, confirmFinancingPlan, createFinancingPlanFromCombination, createFinancingPlanSelection, createManualCandidateOverride,
  downloadFinancingPlanReportPdf, finalizeFinancingPlanSelection, generateFinancingPlanReport,
  generateFinancingPlanCombinations, getLatestProductMatching, runProductMatching,
  listCustomerFinancingPlans,
  updateFinancingPlan, updateFinancingPlanCondition, updateFinancingPlanMaterial, validateFinancingPlan,
  type FinancingPlanCombinationData, type FinancingPlanCombinationResponseData, type FinancingPlanData,
  type FinancingPlanReportData, type FinancingPlanSelectionData, type FinancingPlanVersionDiffData,
  type FinancingRequirementData, type ProductMatchItemData, type ProductMatchingSnapshotData, type ProductMatchingStatus,
} from '../services/api';
import { matchingFactLabel } from './matchingFactLabels';
import { FinancingExecutionPanel } from './FinancingExecutionPanel';
import { formatAmountWan } from '../utils/businessFormatters';

const STATUS_LABELS: Record<ProductMatchingStatus, string> = {
  eligible: '符合当前已知硬条件', conditional: '条件性匹配', ineligible: '不符合明确硬条件',
  manual_review: '需要人工复核', product_configuration_error: '产品配置异常',
};
const CATEGORY_LABELS: Record<string, string> = {
  guarantee_fund: '担保基金', personal_mortgage: '个人抵押', personal_credit: '个人信用',
  technology_enterprise: '科技企业', enterprise_mortgage: '企业抵押', enterprise_credit: '企业信用',
};
const DOMAIN_LABELS: Record<string, string> = { asset: '资产', business: '税务与开票', qualification: '科技资质' };
const STATUS_ORDER: ProductMatchingStatus[] = ['eligible', 'conditional', 'ineligible', 'manual_review', 'product_configuration_error'];
const DATA_QUALITY_FIELD_GROUPS = [
  ['missing_fields', '缺失字段'],
  ['preliminary_fields', '初步分类字段'],
  ['conflicted_fields', '冲突字段'],
  ['stale_fields', '过期字段'],
] as const;

function hasDataQuality(snapshot: ProductMatchingSnapshotData): boolean {
  return Boolean(snapshot.data_quality.missing_domains?.length
    || DATA_QUALITY_FIELD_GROUPS.some(([key]) => snapshot.data_quality[key]?.length));
}

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
        <span>最高额度：{item.max_amount ? formatAmountWan(item.max_amount) : '资料不足'}</span>
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

const money = formatAmountWan;

function PlanFinalizationPanel({ requirement, snapshot, plans, onError }: { requirement: FinancingRequirementData; snapshot: ProductMatchingSnapshotData; plans: FinancingPlanData[]; onError: (value: string) => void }) {
  const allVersions = plans.flatMap(plan => plan.versions);
  const confirmed = plans.flatMap(plan => plan.versions.filter(value => value.status === 'confirmed'));
  const [primaryId, setPrimaryId] = useState<string | null>(null);
  const [backupIds, setBackupIds] = useState<string[]>([]);
  const [conditionalIds, setConditionalIds] = useState<string[]>([]);
  const [selection, setSelection] = useState<FinancingPlanSelectionData | null>(null);
  const [report, setReport] = useState<FinancingPlanReportData | null>(null);
  const [reportType, setReportType] = useState<'internal' | 'customer'>('customer');
  const [fromVersion, setFromVersion] = useState('');
  const [toVersion, setToVersion] = useState('');
  const [diff, setDiff] = useState<FinancingPlanVersionDiffData | null>(null);
  const [busy, setBusy] = useState(false);
  const canWrite = ['admin', 'operator'].includes(localStorage.getItem('auth_role') || '');

  async function ensureSelection() {
    if (selection) return selection;
    const created = await createFinancingPlanSelection({
      customer_id: requirement.customer_id, requirement_id: requirement.requirement_id,
      primary_plan_version_id: primaryId,
      backup_plan_version_ids: backupIds,
      conditional_plan_version_ids: conditionalIds,
      notes: confirmed.length ? '业务人员明确选择方案角色' : '当前尚未形成可选融资方案',
    });
    setSelection(created); return created;
  }

  async function generate(type: 'internal' | 'customer') {
    setBusy(true); onError('');
    try { const selected = await ensureSelection(); setReportType(type); setReport(await generateFinancingPlanReport(selected.selection_id, type)); }
    catch (cause) { onError(cause instanceof Error ? cause.message : '方案报告生成失败'); }
    finally { setBusy(false); }
  }

  async function finalize() {
    if (!selection) return;
    setBusy(true); onError('');
    try { setSelection(await finalizeFinancingPlanSelection(selection.selection_id)); }
    catch (cause) { onError(cause instanceof Error ? cause.message : '方案定稿失败'); }
    finally { setBusy(false); }
  }

  async function compare() {
    if (!fromVersion || !toVersion) return;
    setBusy(true); onError('');
    try { setDiff(await compareFinancingPlanVersions(fromVersion, toVersion)); }
    catch (cause) { onError(cause instanceof Error ? cause.message : '版本对比失败'); }
    finally { setBusy(false); }
  }

  async function downloadPdf() {
    if (!report) return;
    setBusy(true); onError('');
    try {
      const blob = await downloadFinancingPlanReportPdf(report.report_id); const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a'); anchor.href = url; anchor.download = `融资规划方案_${report.report_type}_V${report.report_version}.pdf`; anchor.click(); URL.revokeObjectURL(url);
    } catch (cause) { onError(cause instanceof Error ? cause.message : 'PDF导出失败'); }
    finally { setBusy(false); }
  }

  return <section className="mt-3 rounded-lg border border-slate-200 bg-white p-3" data-testid="plan-finalization-panel">
    <h4 className="font-semibold text-slate-900">方案定稿与报告</h4>
    {!confirmed.length && <div className="mt-2 rounded bg-amber-50 p-2 text-xs text-amber-800"><p>当前产品库中尚无可直接形成正式融资方案的产品组合。</p><p className="mt-1">目标金额：{money(requirement.requested_amount)} · 当前覆盖：0万元 · 当前缺口：{money(requirement.requested_amount)}</p><p className="mt-1">人工复核候选：{snapshot.summary.manual_review || 0}个。下一步：完成人工复核、完善产品规则、补充必要资料后重新匹配和组合。</p></div>}
    {!!confirmed.length && <div className="mt-2 space-y-2"><h5 className="text-sm font-medium">方案列表</h5>{confirmed.map(version => <div key={version.plan_version_id} className="flex flex-wrap items-center gap-3 rounded bg-slate-50 p-2 text-xs"><span>V{version.version_no} · 覆盖{money(version.covered_amount)} · 缺口{money(version.funding_gap)}</span><label><input type="radio" name="primary-plan" checked={primaryId === version.plan_version_id} disabled={!canWrite || !!selection} onChange={() => { setPrimaryId(version.plan_version_id); setBackupIds(values => values.filter(id => id !== version.plan_version_id)); setConditionalIds(values => values.filter(id => id !== version.plan_version_id)); }} /> 主方案</label><label><input type="checkbox" checked={backupIds.includes(version.plan_version_id)} disabled={!canWrite || !!selection || primaryId === version.plan_version_id} onChange={event => { setBackupIds(values => event.target.checked ? [...values, version.plan_version_id] : values.filter(id => id !== version.plan_version_id)); if (event.target.checked) setConditionalIds(values => values.filter(id => id !== version.plan_version_id)); }} /> 备选方案</label><label><input type="checkbox" checked={conditionalIds.includes(version.plan_version_id)} disabled={!canWrite || !!selection || primaryId === version.plan_version_id} onChange={event => { setConditionalIds(values => event.target.checked ? [...values, version.plan_version_id] : values.filter(id => id !== version.plan_version_id)); if (event.target.checked) setBackupIds(values => values.filter(id => id !== version.plan_version_id)); }} /> 条件性方案</label></div>)}</div>}
    <div className="mt-3 flex flex-wrap gap-2">
      {canWrite && <button type="button" disabled={busy} onClick={() => void generate('internal')} className="rounded border px-2 py-1 text-xs">内部版预览</button>}
      {canWrite && <button type="button" disabled={busy} onClick={() => void generate('customer')} className="rounded border px-2 py-1 text-xs">客户版预览</button>}
      {canWrite && selection && selection.status === 'draft' && <button type="button" disabled={busy} onClick={() => void finalize()} className="rounded bg-indigo-700 px-2 py-1 text-xs text-white">定稿方案选择</button>}
      {report && <button type="button" disabled={busy} onClick={() => void downloadPdf()} className="rounded border border-indigo-300 px-2 py-1 text-xs text-indigo-700">导出PDF</button>}
    </div>
    {selection && <p className="mt-2 text-xs text-slate-500">定稿状态：{selection.status === 'finalized' ? '已定稿' : '草稿'}。定稿仅表示当前业务流程已选定方案结构，不代表金融机构审批结果。</p>}
    {allVersions.length > 1 && <section className="mt-3 border-t pt-3"><h5 className="text-sm font-medium">版本历史与对比</h5><div className="mt-2 flex flex-wrap gap-2 text-xs"><select aria-label="起始版本" value={fromVersion} onChange={event => setFromVersion(event.target.value)}><option value="">选择起始版本</option>{allVersions.map(value => <option key={value.plan_version_id} value={value.plan_version_id}>V{value.version_no}</option>)}</select><select aria-label="目标版本" value={toVersion} onChange={event => setToVersion(event.target.value)}><option value="">选择目标版本</option>{allVersions.map(value => <option key={value.plan_version_id} value={value.plan_version_id}>V{value.version_no}</option>)}</select><button type="button" disabled={busy || !fromVersion || !toVersion} onClick={() => void compare()} className="rounded border px-2 py-1">对比</button></div>{diff && <div className="mt-2 rounded bg-slate-50 p-2 text-xs"><p>{diff.summary}</p><p>金额变化：{Object.keys(diff.amount_changes).length}项 · 产品新增：{diff.product_changes.added.length} · 产品删除：{diff.product_changes.removed.length} · 条件变化：{diff.condition_changes.changed.length} · 材料变化：{diff.material_changes.changed.length}</p></div>}</section>}
    {report && <section className="mt-3 border-t pt-3"><div className="mb-2 text-xs text-slate-600">{reportType === 'internal' ? '内部版方案说明' : '客户版方案说明'} · 报告V{report.report_version}</div><iframe title="融资方案报告预览" className="h-[520px] w-full rounded border bg-white" srcDoc={report.rendered_html} /></section>}
  </section>;
}

function CombinationCard({ value, onCreate, busy }: { value: FinancingPlanCombinationData; onCreate: (id: string) => void; busy: boolean }) {
  const typeLabel = value.plan_type === 'primary_candidate' ? '主方案候选' : value.plan_type === 'backup_candidate' ? '备选方案候选' : '条件性方案';
  return <article className="rounded-lg border border-slate-200 bg-white p-3" data-testid="financing-plan-combination">
    <div className="flex flex-wrap items-start justify-between gap-2">
      <div><strong>{typeLabel}</strong><p className="mt-1 text-xs text-slate-600">目标 {money(value.target_amount)} · 覆盖 {money(value.covered_amount)} · 未覆盖 {money(value.funding_gap)}</p></div>
      <button type="button" disabled={busy} onClick={() => onCreate(value.combination_id)} className="rounded border border-indigo-300 px-2 py-1 text-xs text-indigo-700 disabled:opacity-50">选择此方案</button>
    </div>
    <p className="mt-2 text-xs text-slate-500">产品 {value.product_count} 个 · 机构 {value.institution_count} 个</p>
    <div className="mt-2 space-y-2">{value.items.map(item => <div key={item.product_version_id} className="rounded bg-slate-50 p-2 text-xs text-slate-700">
      <div className="font-medium">{item.institution_name} · {item.product_name}</div>
      <div className="mt-1">规划金额：{money(item.proposed_amount)} · 规划期限：{item.proposed_term_months}个月</div>
    </div>)}</div>
    {!!value.conditions.length && <p className="mt-2 text-xs text-amber-700">条件：{value.conditions.join('；')}</p>}
    {!!value.missing_information.length && <p className="mt-1 text-xs text-amber-700">缺失资料：{value.missing_information.join('；')}</p>}
    {!!value.risks.length && <p className="mt-1 text-xs text-slate-500">风险提示：{value.risks.join('；')}</p>}
  </article>;
}

function PlanDraftPanel({ plan, requirement, onPlanChange, onError }: { plan: FinancingPlanData; requirement: FinancingRequirementData; onPlanChange: (value: FinancingPlanData) => void; onError: (value: string) => void }) {
  const version = plan.versions.find(value => value.plan_version_id === plan.current_version_id) || plan.versions.at(-1)!;
  const [items, setItems] = useState(version.items);
  const [busy, setBusy] = useState(false);
  useEffect(() => setItems(version.items), [version.plan_version_id]);

  async function save() {
    setBusy(true); onError('');
    try {
      const updated = await updateFinancingPlan(plan.financing_plan_id, {
        customer_id: plan.customer_id, requirement_id: plan.requirement_id,
        match_snapshot_id: plan.source_match_snapshot_id, plan_type: version.plan_type,
        items: items.map((item, index) => ({
          product_match_item_id: item.product_match_item_id, proposed_amount: item.proposed_amount,
          proposed_term_months: item.proposed_term_months, item_role: item.item_role,
          sequence_no: index + 1, reason: item.reason, notes: item.notes,
          conditions: item.conditions, risks: item.risks,
        })),
      });
      onPlanChange(updated);
    } catch (cause) { onError(cause instanceof Error ? cause.message : '方案草稿保存失败'); }
    finally { setBusy(false); }
  }

  async function setCondition(conditionId: string, status: string) {
    setBusy(true); onError('');
    try {
      await updateFinancingPlanCondition(plan.financing_plan_id, conditionId, status);
      onPlanChange({ ...plan, versions: plan.versions.map(value => value.plan_version_id !== version.plan_version_id ? value : { ...value, condition_checklist: value.condition_checklist.map(row => row.condition_id === conditionId ? { ...row, status } : row) }) });
    } catch (cause) { onError(cause instanceof Error ? cause.message : '条件状态更新失败'); }
    finally { setBusy(false); }
  }

  async function setMaterial(materialId: string, status: string) {
    setBusy(true); onError('');
    try {
      await updateFinancingPlanMaterial(plan.financing_plan_id, materialId, status);
      onPlanChange({ ...plan, versions: plan.versions.map(value => value.plan_version_id !== version.plan_version_id ? value : { ...value, material_checklist: value.material_checklist.map(row => row.material_id === materialId ? { ...row, status } : row) }) });
    } catch (cause) { onError(cause instanceof Error ? cause.message : '材料状态更新失败'); }
    finally { setBusy(false); }
  }

  async function confirm() {
    setBusy(true); onError('');
    try {
      const validation = await validateFinancingPlan(plan.financing_plan_id);
      if (!validation.valid) { onError(validation.errors.join('；')); return; }
      onPlanChange(await confirmFinancingPlan(plan.financing_plan_id));
    } catch (cause) { onError(cause instanceof Error ? cause.message : '方案确认失败'); }
    finally { setBusy(false); }
  }

  return <section className="mt-4 space-y-3 rounded-lg border border-indigo-200 bg-indigo-50/30 p-3" data-testid="financing-plan-draft">
    <div className="flex flex-wrap items-center justify-between gap-2"><div><h4 className="font-semibold text-slate-900">融资方案草稿</h4><p className="text-xs text-slate-500">版本 V{version.version_no} · 状态：{version.status === 'confirmed' ? '已确认' : version.status === 'needs_review' ? '待审核' : '草稿'}</p></div><button type="button" disabled={busy || version.status === 'confirmed'} onClick={() => void confirm()} className="rounded bg-emerald-700 px-3 py-1.5 text-sm text-white disabled:opacity-50">确认方案</button></div>
    <div className="grid gap-2 text-sm sm:grid-cols-3"><div>目标金额：{money(version.target_amount)}</div><div>覆盖金额：{money(version.covered_amount)}</div><div>未覆盖金额：{money(version.funding_gap)}</div></div>
    <section><h5 className="mb-2 text-sm font-medium">产品结构</h5><div className="space-y-2">{items.map((item, index) => <div key={item.product_match_item_id} className="grid gap-2 rounded bg-white p-2 text-xs sm:grid-cols-2">
      <div className="sm:col-span-2 font-medium">{item.institution_name} · {item.product_name}</div>
      <label>规划金额<input aria-label={`${item.product_name}规划金额`} className="ml-2 rounded border px-2 py-1" value={item.proposed_amount} disabled={version.status === 'confirmed'} onChange={event => setItems(values => values.map((value, rowIndex) => rowIndex === index ? { ...value, proposed_amount: event.target.value } : value))} /></label>
      <label>规划期限<input aria-label={`${item.product_name}规划期限`} type="number" className="ml-2 w-20 rounded border px-2 py-1" value={item.proposed_term_months} disabled={version.status === 'confirmed'} onChange={event => setItems(values => values.map((value, rowIndex) => rowIndex === index ? { ...value, proposed_term_months: Number(event.target.value) } : value))} /></label>
      <label>产品角色<select className="ml-2 rounded border px-2 py-1" value={item.item_role} disabled={version.status === 'confirmed'} onChange={event => setItems(values => values.map((value, rowIndex) => rowIndex === index ? { ...value, item_role: event.target.value } : value))}><option value="primary">主融资产品</option><option value="supplementary">补充融资产品</option><option value="fallback">备选产品</option></select></label>
      <label>业务备注<input className="ml-2 rounded border px-2 py-1" value={item.notes || ''} disabled={version.status === 'confirmed'} onChange={event => setItems(values => values.map((value, rowIndex) => rowIndex === index ? { ...value, notes: event.target.value } : value))} /></label>
    </div>)}</div>{version.status !== 'confirmed' && <button type="button" disabled={busy} onClick={() => void save()} className="mt-2 rounded border border-indigo-300 px-3 py-1 text-xs text-indigo-700">保存方案修改</button>}</section>
    <section><h5 className="mb-2 text-sm font-medium">条件清单</h5>{version.condition_checklist.length ? <div className="space-y-1">{version.condition_checklist.map(row => <div key={row.condition_id} className="flex items-center justify-between gap-2 rounded bg-white p-2 text-xs"><span>{row.title}</span><select aria-label={`${row.title}状态`} value={row.status} disabled={busy || version.status === 'confirmed'} onChange={event => void setCondition(row.condition_id, event.target.value)}><option value="pending">待完成</option><option value="satisfied">已满足</option><option value="waived">已豁免</option><option value="not_applicable">不适用</option></select></div>)}</div> : <p className="text-xs text-slate-500">暂无待处理条件</p>}</section>
    <section><h5 className="mb-2 text-sm font-medium">材料清单</h5>{version.material_checklist.length ? <div className="space-y-1">{version.material_checklist.map(row => <div key={row.material_id} className="flex items-center justify-between gap-2 rounded bg-white p-2 text-xs"><span>{row.material_name}{row.required_by_products.length ? `（${row.required_by_products.join('、')}）` : ''}</span><select aria-label={`${row.material_name}状态`} value={row.status} disabled={busy || version.status === 'confirmed'} onChange={event => void setMaterial(row.material_id, event.target.value)}><option value="missing">缺失</option><option value="available">已有</option><option value="uploaded">已上传</option><option value="verified">已核验</option><option value="not_applicable">不适用</option></select></div>)}</div> : <p className="text-xs text-slate-500">当前结构化来源未提出材料要求</p>}</section>
    {!!version.gaps.length && <section><h5 className="text-sm font-medium">风险与缺口</h5>{version.gaps.map((gap, index) => <p className="mt-1 text-xs text-amber-700" key={`${gap.gap_type}-${index}`}>{gap.description}</p>)}</section>}
    {version.explanation && <section className="rounded bg-white p-2 text-xs text-slate-700"><h5 className="font-medium">方案说明</h5><p className="mt-1">{version.explanation.plan_summary}{version.explanation.coverage_summary}</p><p className="mt-1">{version.explanation.funding_gap_summary}</p><h6 className="mt-2 font-medium">下一步行动</h6>{version.explanation.next_actions.length ? <ol className="ml-5 mt-1 list-decimal">{version.explanation.next_actions.map(value => <li key={value}>{value}</li>)}</ol> : <p className="mt-1">暂无额外行动</p>}</section>}
    <p className="text-xs text-slate-500">方案金额为规划金额，不代表银行最终批复金额或利率。</p>
    <span className="sr-only">{requirement.borrower_entity}</span>
  </section>;
}

export function ProductMatchingPanel({ requirement }: { requirement: FinancingRequirementData }) {
  const [snapshot, setSnapshot] = useState<ProductMatchingSnapshotData | null>(null);
  const [combinations, setCombinations] = useState<FinancingPlanCombinationResponseData | null>(null);
  const [selectedPlan, setSelectedPlan] = useState<FinancingPlanData | null>(null);
  const [savedPlans, setSavedPlans] = useState<FinancingPlanData[]>([]);
  const [showManualReview, setShowManualReview] = useState(false);
  const [overrideReasons, setOverrideReasons] = useState<Record<number, string>>({});
  const [planMessage, setPlanMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [planBusy, setPlanBusy] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    void getLatestProductMatching(requirement.customer_id).then(value => {
      if (active && value?.requirement_id === requirement.requirement_id) setSnapshot(value);
    }).catch(() => undefined);
    void listCustomerFinancingPlans(requirement.customer_id, requirement.requirement_id).then(values => {
      if (active) { setSavedPlans(values); setSelectedPlan(values[0] || null); }
    }).catch(() => undefined);
    return () => { active = false; };
  }, [requirement.customer_id, requirement.requirement_id]);

  const grouped = useMemo(() => Object.fromEntries(STATUS_ORDER.map(status => [status, snapshot?.items.filter(item => item.overall_status === status) || []])) as Record<ProductMatchingStatus, ProductMatchItemData[]>, [snapshot]);

  async function match() {
    setBusy(true); setError(''); setCombinations(null); setSelectedPlan(null); setPlanMessage('');
    try { setSnapshot(await runProductMatching(requirement.customer_id, requirement.requirement_id)); }
    catch (cause) { setError(cause instanceof Error ? cause.message : '产品匹配失败'); }
    finally { setBusy(false); }
  }

  const canBuildPlanCandidates = Boolean(snapshot?.snapshot_id && ((snapshot.summary.eligible || 0) + (snapshot.summary.conditional || 0) > 0));

  async function buildPlanCombinations() {
    if (!snapshot?.snapshot_id || !canBuildPlanCandidates) return;
    setPlanBusy(true); setError(''); setPlanMessage('');
    try { setCombinations(await generateFinancingPlanCombinations(requirement.customer_id, requirement.requirement_id, snapshot.snapshot_id)); }
    catch (cause) { setError(cause instanceof Error ? cause.message : '融资方案组合生成失败'); }
    finally { setPlanBusy(false); }
  }

  async function createDraft(combinationId: string) {
    if (!snapshot?.snapshot_id) return;
    setPlanBusy(true); setError(''); setPlanMessage('');
    try {
      const result = await createFinancingPlanFromCombination(requirement.customer_id, requirement.requirement_id, snapshot.snapshot_id, combinationId);
      setSelectedPlan(result);
      setSavedPlans(values => [result, ...values.filter(value => value.financing_plan_id !== result.financing_plan_id)]);
      setPlanMessage(`方案草稿已创建：${result.financing_plan_id}`);
    } catch (cause) { setError(cause instanceof Error ? cause.message : '方案草稿创建失败'); }
    finally { setPlanBusy(false); }
  }

  async function includeManualCandidate(item: ProductMatchItemData) {
    if (!snapshot?.snapshot_id || !item.product_match_item_id) return;
    const reason = (overrideReasons[item.product_match_item_id] || '').trim();
    if (!reason) { setError('请填写将该产品纳入条件性候选的原因'); return; }
    setPlanBusy(true); setError('');
    try {
      await createManualCandidateOverride(requirement.customer_id, requirement.requirement_id, snapshot.snapshot_id, item.product_match_item_id, reason);
      const result = await generateFinancingPlanCombinations(requirement.customer_id, requirement.requirement_id, snapshot.snapshot_id);
      setCombinations(result);
      setPlanMessage('人工复核决定已记录，产品已作为条件性候选重新参与组合。');
    } catch (cause) { setError(cause instanceof Error ? cause.message : '人工复核候选处理失败'); }
    finally { setPlanBusy(false); }
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
        <div>融资需求：{money(requirement.requested_amount)} · {requirement.purpose_detail || requirement.financing_purpose} · {requirement.term_value}{requirement.term_unit === 'month' ? '个月' : requirement.term_unit === 'year' ? '年' : '天'}</div>
        <div className="mt-1">匹配时间：{snapshot.generated_at ? new Date(snapshot.generated_at).toLocaleString('zh-CN') : '—'} · 产品库时点：{snapshot.catalog_as_of_date || '—'}</div>
      </div>
      {hasDataQuality(snapshot) ? <div className="rounded bg-amber-50 p-3 text-xs text-amber-800">
        <strong>当前数据质量</strong>
        {!!snapshot.data_quality.missing_domains?.length && <div className="mt-1">缺失域：{snapshot.data_quality.missing_domains.map(value => DOMAIN_LABELS[value] || value).join('、')}</div>}
        {DATA_QUALITY_FIELD_GROUPS.map(([key, label]) => {
          const values = snapshot.data_quality[key];
          return values?.length ? <div className="mt-1" key={key}>{label}：{values.map(matchingFactLabel).join('、')}</div> : null;
        })}
      </div> : null}
      {STATUS_ORDER.map(status => <section key={status}>
        <h4 className="mb-2 font-medium text-slate-800">{STATUS_LABELS[status]}：{grouped[status].length}</h4>
        <div className="grid gap-2">{grouped[status].map(item => <ProductCard key={item.version_id} item={item} />)}</div>
      </section>)}
      <section className="rounded-lg border border-slate-200 bg-slate-50 p-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div><h4 className="font-medium text-slate-900">融资方案</h4><p className="mt-1 text-xs text-slate-600">先建立候选池，不会自动组合产品或生成正式主方案。</p></div>
          <button type="button" disabled={!canBuildPlanCandidates || planBusy} onClick={() => void buildPlanCombinations()} className="rounded bg-indigo-700 px-3 py-1.5 text-sm text-white disabled:cursor-not-allowed disabled:opacity-50">{planBusy ? '生成中…' : '生成融资方案'}</button>
        </div>
        {!canBuildPlanCandidates && <div className="mt-2 text-xs text-amber-700"><p>当前尚无可自动形成的融资方案。</p><p className="mt-1">需要人工复核产品：{snapshot.summary.manual_review || 0} · 已排除产品：{snapshot.summary.ineligible || 0} · 未覆盖金额：{money(requirement.requested_amount)}</p><p className="mt-1">下一步：先完成人工复核候选产品，再重新生成融资组合。</p>{(snapshot.summary.manual_review || 0) > 0 && <button type="button" onClick={() => setShowManualReview(value => !value)} className="mt-2 rounded border border-amber-400 px-2 py-1 text-amber-800">查看人工复核候选</button>}</div>}
        {showManualReview && <div className="mt-3 space-y-2">{grouped.manual_review.map(item => <article key={item.version_id} className="rounded border border-amber-200 bg-white p-3 text-xs"><strong>{item.institution_name} · {item.product_name}</strong><p className="mt-1">已知额度：{item.max_amount ? money(item.max_amount) : '资料不足'} · 已知期限：{item.max_term_months ? `${item.max_term_months}个月` : '资料不足'}</p><p className="mt-1">缺失字段：{item.missing_information.length ? item.missing_information.join('；') : '无明确缺失字段记录'}</p><p className="mt-1">人工复核原因：{item.review_reasons.join('；')}</p>{item.product_match_item_id && <div className="mt-2 flex flex-wrap gap-2"><input aria-label={`${item.product_name}人工纳入原因`} placeholder="填写人工核验原因" value={overrideReasons[item.product_match_item_id] || ''} onChange={event => setOverrideReasons(values => ({ ...values, [item.product_match_item_id!]: event.target.value }))} className="min-w-64 flex-1 rounded border px-2 py-1" /><button type="button" disabled={planBusy} onClick={() => void includeManualCandidate(item)} className="rounded border border-indigo-300 px-2 py-1 text-indigo-700">纳入条件性候选</button></div>}</article>)}</div>}
        {combinations && !(combinations.formal_combinations.length || combinations.conditional_combinations.length || combinations.partial_combinations.length) && <p className="mt-2 text-xs text-amber-700">当前没有足够可自动用于融资方案的产品。</p>}
        {combinations && <div className="mt-3 space-y-3">
          {([['完整方案候选', combinations.formal_combinations], ['条件性方案', combinations.conditional_combinations], ['部分覆盖方案', combinations.partial_combinations]] as const).map(([label, values]) => values.length ? <section key={label}><h5 className="mb-2 text-sm font-medium text-slate-800">{label}</h5><div className="grid gap-2">{values.map(value => <CombinationCard key={value.combination_id} value={value} busy={planBusy} onCreate={id => void createDraft(id)} />)}</div></section> : null)}
          <p className="text-xs text-slate-500">方案金额为规划金额，不代表银行最终批复金额。</p>
        </div>}
        {planMessage && <p className="mt-2 text-xs text-emerald-700">{planMessage}</p>}
      </section>
      {selectedPlan && <PlanDraftPanel plan={selectedPlan} requirement={requirement} onPlanChange={value => { setSelectedPlan(value); setSavedPlans(values => [value, ...values.filter(row => row.financing_plan_id !== value.financing_plan_id)]); }} onError={setError} />}
      <PlanFinalizationPanel requirement={requirement} snapshot={snapshot} plans={savedPlans} onError={setError} />
      <FinancingExecutionPanel requirement={requirement} plans={savedPlans} />
    </div>}
  </section>;
}
