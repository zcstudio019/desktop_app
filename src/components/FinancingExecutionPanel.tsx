import { useEffect, useMemo, useState } from 'react';
import {
  addApplicationReviewFeedback, blockFinancingApplicationTask, completeApplicationSupplement,
  completeFinancingApplicationTask, createApplicationDisbursement, createApplicationPackage,
  createSubmissionPackage, createSupplementPackage, freezeApplicationPackage,
  getApplicationMaterialExtraction, getApplicationMaterials, listApplicationPackages, matchApplicationMaterials,
  createFinancingApplications, listFinancingApplications, markApplicationReady,
  markApplicationUnderReview, recordApplicationApproval,
  recordApplicationRejection, requestApplicationSupplement, startApplicationPreparation,
  retryFinancingApplication,
  submitFinancingApplication, unblockFinancingApplicationTask, updateApplicationApprovalCondition,
  replaceApplicationMaterial, selectApplicationMaterial, updateApplicationMaterial,
  verifyApplicationMaterial, downloadSubmissionPackageZip, getFinancingApplicationOutcome,
  finalizeFinancingApplicationOutcome, getCustomerFinancingReview,
  type ApplicationMaterialData, type ApplicationMaterialSummaryData, type ApplicationPackageData,
  type ApplicationMaterialMatchResult,
  type FinancingApplicationData, type FinancingPlanData, type FinancingRequirementData,
  type SubmissionPackageData, type SupplementPackageData, type FinancingApplicationOutcomeData,
  type CustomerFinancingReviewData,
} from '../services/api';
import { APPLICATION_EVENT_LABELS, APPLICATION_MATERIAL_STATUS_LABELS, APPLICATION_STAGE_STATUS_LABELS, APPLICATION_STATUS_LABELS, APPLICATION_TASK_SOURCE_LABELS, APPLICATION_TASK_STATUS_LABELS, MATERIAL_PACKAGE_STATUS_LABELS, MATERIAL_TYPE_LABELS } from './financingExecutionLabels';
import { formatAmountWan } from '../utils/businessFormatters';
import { FinancingCommunicationPanel } from './FinancingCommunicationPanel';

const money = formatAmountWan;

const OUTCOME_LABELS: Record<string, string> = {
  approved_disbursed: '已批复并放款', approved_not_disbursed: '已批复未放款',
  partially_approved_disbursed: '部分批复并放款', partially_approved_not_disbursed: '部分批复未放款',
  rejected: '已拒绝', cancelled: '已取消', closed_without_result: '无结果关闭',
};
const EXECUTION_REVIEW_LABELS: Record<string, string> = {
  not_started: '未开始', in_progress: '执行中', partially_funded: '部分完成',
  fully_funded: '已完成', closed_unfunded: '未融资关闭', cancelled: '已取消',
};

function ApplicationMaterialManagement({ application, canWrite }: { application: FinancingApplicationData; canWrite: boolean }) {
  const [materials, setMaterials] = useState<ApplicationMaterialData[]>([]);
  const [summary, setSummary] = useState<ApplicationMaterialSummaryData | null>(null);
  const [packages, setPackages] = useState<ApplicationPackageData[]>([]);
  const [submissionPackages, setSubmissionPackages] = useState<SubmissionPackageData[]>([]);
  const [supplementPackages, setSupplementPackages] = useState<SupplementPackageData[]>([]);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [matchResults, setMatchResults] = useState<ApplicationMaterialMatchResult[]>([]);
  const [extraction, setExtraction] = useState<Record<string, unknown> | null>(null);

  async function load() {
    const [materialData, packageData] = await Promise.all([getApplicationMaterials(application.application_id), listApplicationPackages(application.application_id)]);
    setMaterials(materialData.materials); setSummary(materialData.summary); setPackages(packageData.packages);
    setSubmissionPackages(packageData.submission_packages); setSupplementPackages(packageData.supplement_packages);
  }
  useEffect(() => { void load().catch(cause => setMessage(cause instanceof Error ? cause.message : '材料管理加载失败')); }, [application.application_id]);
  async function run(action: () => Promise<unknown>, success: string) {
    setBusy(true); setMessage('');
    try { await action(); await load(); setMessage(success); }
    catch (cause) { setMessage(cause instanceof Error ? cause.message : '材料操作失败'); }
    finally { setBusy(false); }
  }
  async function matchExisting() {
    setBusy(true); setMessage('');
    try {
      const result = await matchApplicationMaterials(application.application_id);
      setMatchResults(result.results); await load(); setMessage('已完成现有资料匹配');
    } catch (cause) { setMessage(cause instanceof Error ? cause.message : '材料匹配失败'); }
    finally { setBusy(false); }
  }
  async function chooseMaterial(material: ApplicationMaterialData, documentId: string) {
    const action = material.source_document_id
      ? () => replaceApplicationMaterial(application.application_id, material.application_material_id, documentId, '人工替换本次申请材料')
      : () => selectApplicationMaterial(application.application_id, material.application_material_id, documentId);
    await run(action, material.source_document_id ? '材料已替换，原记录已保留' : '已选择用于本次申请的资料');
  }
  async function showExtraction(materialId: string) {
    setBusy(true); setMessage('');
    try { setExtraction(await getApplicationMaterialExtraction(application.application_id, materialId)); }
    catch (cause) { setMessage(cause instanceof Error ? cause.message : '结构化结果加载失败'); }
    finally { setBusy(false); }
  }
  async function downloadZip(item: SubmissionPackageData) {
    setBusy(true); setMessage('');
    try {
      const blob = await downloadSubmissionPackageZip(item.submission_package_id);
      const url = URL.createObjectURL(blob); const anchor = document.createElement('a');
      anchor.href = url; anchor.download = `${application.customer_name || application.customer_id}_${application.product_name}_进件材料.zip`;
      anchor.click(); URL.revokeObjectURL(url);
    } catch (cause) { setMessage(cause instanceof Error ? cause.message : '材料包下载失败'); }
    finally { setBusy(false); }
  }
  const groups = {
    required: materials.filter(value => value.required && !value.supplement_request_id && ['uploaded', 'verified', 'not_applicable'].includes(value.status)),
    missing: materials.filter(value => !value.supplement_request_id && value.status === 'required_missing'),
    review: materials.filter(value => !value.supplement_request_id && ['matched', 'needs_review', 'expired'].includes(value.status)),
    supplement: materials.filter(value => !!value.supplement_request_id),
  };
  return <section className="mt-3 rounded border border-indigo-200 p-3" data-testid="application-material-management">
    <div className="flex flex-wrap items-center justify-between gap-2"><div><h6 className="font-medium">材料管理</h6><p className="text-slate-500">客户原始资料仅被引用；已冻结或已提交材料包保持不变。</p></div>{canWrite && <div className="flex gap-1"><button disabled={busy} onClick={() => void matchExisting()} className="rounded border px-2 py-1">匹配现有资料</button><a href={`/upload?customerId=${encodeURIComponent(application.customer_id)}`} className="rounded border px-2 py-1">上传新资料</a><button disabled={busy || !!summary?.missing_count || !!summary?.expired_count || !!summary?.review_count} onClick={() => void run(() => createApplicationPackage(application.application_id), '已生成申请材料包')} className="rounded bg-indigo-700 px-2 py-1 text-white disabled:opacity-40">生成进件材料包</button></div>}</div>
    {message && <p className="mt-2 rounded bg-slate-50 p-2">{message}</p>}
    {summary && <div className="mt-2 grid grid-cols-3 gap-1 sm:grid-cols-6"><span>必需 {summary.total_required}</span><span>已核验 {summary.verified_count}</span><span>可用 {summary.available_count}</span><span>缺失 {summary.missing_count}</span><span>过期 {summary.expired_count}</span><span>待核验 {summary.review_count}</span></div>}
    <div className="mt-3 grid gap-2 lg:grid-cols-2">{([['必需材料', groups.required], ['缺失材料', groups.missing], ['待核验', groups.review], ['补件材料', groups.supplement]] as const).map(([title, values]) => <section key={title} className="rounded bg-slate-50 p-2"><strong>{title}</strong>{values.length ? <ul className="mt-1 space-y-1">{values.map(item => { const result = matchResults.find(value => value.application_material_id === item.application_material_id); return <li key={item.application_material_id} className="rounded bg-white p-2"><div className="flex flex-wrap justify-between gap-2"><span>{MATERIAL_TYPE_LABELS[item.material_type] || '其他'} · {item.material_name}{item.owner_name ? ` · ${item.owner_name}` : ''} · V{item.version_no}</span><b>{APPLICATION_MATERIAL_STATUS_LABELS[item.status] || '待确认'}</b></div><div className="mt-1 flex flex-wrap gap-1 text-slate-500">{item.file_reference && <a href={item.file_reference} target="_blank" rel="noreferrer" className="text-blue-700">查看原件</a>}{item.source_document_id && <button className="text-blue-700" onClick={() => void showExtraction(item.application_material_id)}>查看结构化结果</button>}{item.valid_to && <span>有效至：{item.valid_to}</span>}{item.coverage_start && <span>覆盖：{item.coverage_start} 至 {item.coverage_end || '待确认'}</span>}</div>{result && result.candidates.length > 1 && <div className="mt-1 rounded bg-amber-50 p-2"><span>找到多份可用资料，请选择：</span><div className="mt-1 flex flex-wrap gap-1">{result.candidates.map(candidate => <button key={candidate.source_document_id} disabled={!canWrite || busy} onClick={() => void chooseMaterial(item, candidate.source_document_id)} className="rounded border px-2 py-1">{item.source_document_id ? '替换为' : '选择'} {candidate.file_name}</button>)}</div></div>}<div className="mt-1 flex gap-1">{canWrite && ['matched', 'uploaded', 'needs_review'].includes(item.status) && <button disabled={busy} onClick={() => void run(() => verifyApplicationMaterial(application.application_id, item.application_material_id), '材料已核验')} className="rounded border px-2 py-1">核验</button>}{canWrite && item.status !== 'not_applicable' && <button disabled={busy} onClick={() => void run(() => updateApplicationMaterial(application.application_id, item.application_material_id, { status: 'not_applicable' }), '已标记为不适用')} className="rounded border px-2 py-1">标记不适用</button>}</div></li>; })}</ul> : <p className="mt-1 text-slate-500">暂无</p>}</section>)}</div>
    {extraction && <section className="mt-3 rounded bg-slate-900 p-3 text-slate-100"><div className="flex justify-between"><strong>结构化结果</strong><button onClick={() => setExtraction(null)}>关闭</button></div><pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap">{JSON.stringify(extraction.data || {}, null, 2)}</pre></section>}
    <section className="mt-3"><h6 className="font-medium">进件材料包</h6>{packages.length ? <div className="mt-1 space-y-1">{packages.map(item => <div key={item.package_id} className="flex flex-wrap items-center justify-between gap-2 rounded bg-slate-50 p-2"><span>V{item.package_version} · {MATERIAL_PACKAGE_STATUS_LABELS[item.status]} · {item.items.length}份材料</span>{canWrite && <div className="flex gap-1">{['draft', 'ready'].includes(item.status) && <button disabled={busy} onClick={() => void run(() => freezeApplicationPackage(item.package_id), '材料包已冻结')} className="rounded border px-2 py-1">冻结</button>}{['ready', 'frozen'].includes(item.status) && <button disabled={busy} onClick={() => void run(() => createSubmissionPackage(item.package_id), '银行进件包已生成')} className="rounded border px-2 py-1">生成银行进件包</button>}</div>}</div>)}</div> : <p className="mt-1 text-slate-500">尚未生成材料包</p>}</section>
    {!!submissionPackages.length && <section className="mt-3"><h6 className="font-medium">银行进件包</h6><div className="mt-1 space-y-1">{submissionPackages.map(item => <div key={item.submission_package_id} className="flex justify-between rounded bg-indigo-50 p-2"><span>V{item.submission_version} · {MATERIAL_PACKAGE_STATUS_LABELS[item.status] || '待确认'} · 哈希 {item.package_hash.slice(0, 8)}</span><button disabled={busy} className="text-blue-700" onClick={() => void downloadZip(item)}>打包下载</button></div>)}</div></section>}
    <section className="mt-3"><h6 className="font-medium">补件材料包</h6>{!!application.supplements?.length && <div className="mt-1 flex flex-wrap gap-1">{application.supplements.map(item => <button key={item.supplement_id} disabled={!canWrite || busy} onClick={() => void run(() => createSupplementPackage(item.supplement_id), `已生成${item.request_no}补件包`)} className="rounded border px-2 py-1">{item.request_no} · 生成补件包</button>)}</div>}{supplementPackages.length ? <ul className="mt-2 space-y-1">{supplementPackages.map(item => <li key={item.supplement_package_id} className="rounded bg-amber-50 p-2">V{item.package_version} · {MATERIAL_PACKAGE_STATUS_LABELS[item.status] || '待确认'} · {item.submission_reference || '尚未提交'}</li>)}</ul> : <p className="mt-1 text-slate-500">尚未生成补件包</p>}</section>
  </section>;
}

export function FinancingExecutionPanel({ requirement, plans }: { requirement: FinancingRequirementData; plans: FinancingPlanData[] }) {
  const confirmedVersions = useMemo(() => plans.flatMap(plan => plan.versions.filter(version => version.status === 'confirmed')), [plans]);
  const [applications, setApplications] = useState<FinancingApplicationData[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [planVersionId, setPlanVersionId] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [supplement, setSupplement] = useState('');
  const [decisionAmount, setDecisionAmount] = useState('');
  const [decisionTerm, setDecisionTerm] = useState('12');
  const [taskFilter, setTaskFilter] = useState('all');
  const [feedback, setFeedback] = useState('');
  const [outcome, setOutcome] = useState<FinancingApplicationOutcomeData | null>(null);
  const [review, setReview] = useState<CustomerFinancingReviewData | null>(null);
  const canWrite = ['admin', 'operator'].includes(localStorage.getItem('auth_role') || '');
  const selected = applications.find(value => value.application_id === selectedId) || applications[0] || null;
  const visibleTasks = (selected?.tasks || []).filter(task => {
    const overdue = !!task.due_date && task.due_date < new Date().toISOString().slice(0, 10) && !['done', 'cancelled'].includes(task.status);
    if (taskFilter === 'mine') return task.assignee_user_id === localStorage.getItem('auth_username');
    if (taskFilter === 'today') return task.due_date === new Date().toISOString().slice(0, 10);
    if (taskFilter === 'overdue') return overdue;
    if (taskFilter === 'high') return ['high', 'urgent'].includes(task.priority);
    if (taskFilter === 'blocked') return task.status === 'blocked';
    return true;
  });

  useEffect(() => {
    let active = true;
    void listFinancingApplications(requirement.customer_id, requirement.requirement_id).then(values => {
      if (active) { setApplications(values); setSelectedId(values[0]?.application_id || null); }
    }).catch(cause => { if (active) setError(cause instanceof Error ? cause.message : '融资申请加载失败'); });
    return () => { active = false; };
  }, [requirement.customer_id, requirement.requirement_id]);

  useEffect(() => {
    void getCustomerFinancingReview(requirement.customer_id, requirement.requirement_id)
      .then(setReview).catch(() => setReview(null));
  }, [requirement.customer_id, requirement.requirement_id, applications.length]);

  useEffect(() => {
    if (!selected?.application_id) { setOutcome(null); return; }
    void getFinancingApplicationOutcome(selected.application_id).then(setOutcome).catch(() => setOutcome(null));
  }, [selected?.application_id]);

  function replace(value: FinancingApplicationData) {
    setApplications(values => [value, ...values.filter(row => row.application_id !== value.application_id)]);
    setSelectedId(value.application_id);
  }

  async function act(action: () => Promise<FinancingApplicationData>) {
    setBusy(true); setError('');
    try { replace(await action()); }
    catch (cause) { setError(cause instanceof Error ? cause.message : '融资执行操作失败'); }
    finally { setBusy(false); }
  }

  async function finalizeOutcome() {
    if (!selected) return;
    setBusy(true); setError('');
    try {
      const result = await finalizeFinancingApplicationOutcome(selected.application_id, { final_notes: '真实执行结果已由业务人员确认' });
      setOutcome(result);
      setReview(await getCustomerFinancingReview(requirement.customer_id, requirement.requirement_id));
    } catch (cause) { setError(cause instanceof Error ? cause.message : '最终结果定稿失败'); }
    finally { setBusy(false); }
  }

  async function create() {
    if (!planVersionId) return;
    setBusy(true); setError('');
    try {
      const rows = await createFinancingApplications(requirement.customer_id, planVersionId);
      setApplications(values => [...rows, ...values]); setSelectedId(rows[0]?.application_id || null);
    } catch (cause) { setError(cause instanceof Error ? cause.message : '融资申请创建失败'); }
    finally { setBusy(false); }
  }

  const totals = applications.reduce((value, item) => ({
    target: value.target + Number(item.target_amount || 0), submitted: value.submitted + Number(item.submitted_amount || 0),
    approved: value.approved + Number(item.approved_amount || 0), disbursed: value.disbursed + Number(item.disbursed_amount || 0),
  }), { target: 0, submitted: 0, approved: 0, disbursed: 0 });

  return <section className="mt-4 rounded-lg border border-emerald-200 bg-white p-3" data-testid="financing-execution-panel">
    <div className="flex flex-wrap items-start justify-between gap-2"><div><h4 className="font-semibold text-slate-900">融资执行</h4><p className="mt-1 text-xs text-slate-500">将已确认方案中的每个产品作为独立申请推进，不会自动向银行提交。</p></div>{canWrite && confirmedVersions.length > 0 && <div className="flex gap-2"><select aria-label="选择已确认方案版本" value={planVersionId} onChange={event => setPlanVersionId(event.target.value)}><option value="">选择已确认方案</option>{confirmedVersions.map(version => <option key={version.plan_version_id} value={version.plan_version_id}>V{version.version_no} · 覆盖{money(version.covered_amount)}</option>)}</select><button type="button" disabled={busy || !planVersionId} onClick={() => void create()} className="rounded bg-emerald-700 px-3 py-1 text-xs text-white disabled:opacity-50">创建融资申请</button></div>}</div>
    {error && <p role="alert" className="mt-2 text-xs text-red-700">{error}</p>}
    {!confirmedVersions.length && !applications.length && <div className="mt-3 rounded bg-amber-50 p-2 text-sm text-amber-800"><p>当前没有可创建融资申请的已确认方案。</p><p>当前暂无融资申请，确认融资方案并创建申请后可准备进件材料。</p><p>当前暂无融资申请，创建申请后可记录银行及客户沟通。</p></div>}
    {review && <section className="mt-3 rounded border border-indigo-100 bg-indigo-50 p-3 text-xs" data-testid="customer-financing-review"><div className="flex justify-between gap-2"><strong>融资复盘</strong><span>{EXECUTION_REVIEW_LABELS[review.execution_status] || '待确认'}</span></div><p className="mt-1">需求：{money(review.requirement.amount)} · 送件：{money(review.submitted_amount)} · 批复：{money(review.approved_amount)} · 放款：{money(review.disbursed_amount)}</p><p className="mt-1">最终融资缺口：{money(review.funding_gap)}{review.application_count === 0 ? ` · ${review.message}` : ''}</p></section>}
    <div className="mt-3 grid gap-2 text-xs sm:grid-cols-5"><div>申请产品：{applications.length}个</div><div>目标：{money(String(totals.target))}</div><div>已提交：{money(String(totals.submitted))}</div><div>已批复：{money(String(totals.approved))}</div><div>已放款：{money(String(totals.disbursed))}</div></div>
    {!!applications.length && <div className="mt-3 grid gap-3 lg:grid-cols-[280px_1fr]"><aside className="space-y-2">{applications.map(item => <button type="button" key={item.application_id} onClick={() => setSelectedId(item.application_id)} className={`w-full rounded border p-2 text-left text-xs ${selected?.application_id === item.application_id ? 'border-emerald-500 bg-emerald-50' : 'border-slate-200'}`}><strong>{item.institution_name} · {item.product_name}</strong><div className="mt-1">目标：{money(item.target_amount)} · {APPLICATION_STATUS_LABELS[item.status]}</div><div className="mt-1 text-slate-500">负责人：{item.responsible_user_name || '待分配'}</div><div className="mt-1 text-slate-500">下一任务：{item.next_task?.title || '暂无'}</div></button>)}</aside>
      {selected && <article className="rounded border border-slate-200 p-3 text-xs"><div className="flex flex-wrap justify-between gap-2"><div><h5 className="text-sm font-semibold">{selected.institution_name} · {selected.product_name}</h5><p className="mt-1 text-slate-500">申请编号：{selected.application_no} · 第{selected.attempt_no}次申请{selected.parent_application_id ? ' · 已关联原申请' : ''}</p></div><span className="rounded-full bg-slate-100 px-2 py-1">{APPLICATION_STATUS_LABELS[selected.status]}</span></div>
        <div className="mt-3 grid gap-1 sm:grid-cols-4"><div>目标金额：{money(selected.target_amount)}</div><div>提交金额：{selected.submitted_amount ? money(selected.submitted_amount) : '未提交'}</div><div>批复金额：{selected.approved_amount ? money(selected.approved_amount) : '未批复'}</div><div>放款金额：{selected.disbursed_amount ? money(selected.disbursed_amount) : '未放款'}</div></div>
        {!!selected.blocking_items?.length && <section className="mt-3 rounded bg-amber-50 p-2 text-amber-800"><strong>当前阻塞项</strong><ul className="mt-1 list-disc pl-5">{selected.blocking_items.map(value => <li key={value}>{value}</li>)}</ul></section>}
        <p className="mt-2 rounded bg-blue-50 p-2 text-blue-800">下一步：{selected.next_action || '人工确认'}</p>
        <section className="mt-3"><h6 className="font-medium">阶段进度</h6><div className="mt-2 grid grid-cols-2 gap-1 sm:grid-cols-4 lg:grid-cols-7">{selected.stages.map(stage => <div key={stage.stage_id} className={`rounded p-2 text-center ${stage.status === 'in_progress' ? 'ring-2 ring-blue-500 bg-blue-100 text-blue-800' : stage.status === 'completed' ? 'bg-emerald-100 text-emerald-800' : stage.status === 'blocked' ? 'bg-red-100 text-red-800' : 'bg-slate-100 text-slate-500'}`}><div>{stage.status === 'completed' ? '✓ ' : ''}{stage.stage_name}</div><div>{APPLICATION_STAGE_STATUS_LABELS[stage.status]}</div>{stage.notes && <div className="mt-1">{stage.notes}</div>}</div>)}</div></section>
        <section className="mt-3"><div className="flex flex-wrap items-center justify-between gap-2"><h6 className="font-medium">任务面板</h6><select aria-label="任务筛选" value={taskFilter} onChange={event => setTaskFilter(event.target.value)}><option value="all">全部</option><option value="mine">我的任务</option><option value="today">今日到期</option><option value="overdue">已逾期</option><option value="high">高优先级</option><option value="blocked">已阻塞</option></select></div>{visibleTasks.length ? <div className="mt-2 space-y-1">{visibleTasks.map(task => { const overdue = !!task.due_date && task.due_date < new Date().toISOString().slice(0, 10) && !['done', 'cancelled'].includes(task.status); return <div key={task.task_id} className="rounded bg-slate-50 p-2"><div className="flex flex-wrap items-center justify-between gap-2"><span>{task.title} · {APPLICATION_TASK_SOURCE_LABELS[task.source_type] || '业务操作'} · {task.assignee_user_name || '待分配'} · {task.priority === 'urgent' ? '紧急' : task.priority === 'high' ? '高' : task.priority === 'low' ? '低' : '普通'} {overdue && <b className="text-red-700">· 已逾期</b>}</span><span>{APPLICATION_TASK_STATUS_LABELS[task.status]}</span></div><div className="mt-1 flex flex-wrap gap-1">{canWrite && task.status !== 'done' && <button onClick={() => void act(() => completeFinancingApplicationTask(selected.application_id, task.task_id))} className="rounded border px-2 py-1">完成</button>}{canWrite && !['blocked', 'done', 'cancelled'].includes(task.status) && <button onClick={() => void act(() => blockFinancingApplicationTask(selected.application_id, task.task_id, '待补充处理信息'))} className="rounded border px-2 py-1">阻塞</button>}{canWrite && task.status === 'blocked' && <button onClick={() => void act(() => unblockFinancingApplicationTask(selected.application_id, task.task_id, '已解除阻塞'))} className="rounded border px-2 py-1">解除阻塞</button>}</div></div>; })}</div> : <p className="mt-1 text-slate-500">暂无符合筛选条件的任务</p>}</section>
        {!!selected.supplements?.length && <section className="mt-3"><h6 className="font-medium">补件要求</h6><div className="mt-1 space-y-1">{selected.supplements.map(item => <div key={item.supplement_id} className="rounded bg-amber-50 p-2"><div>{item.description} · {item.status === 'completed' ? '已完成' : '待完成'}{item.due_date ? ` · 截止${item.due_date}` : ''}</div>{canWrite && item.status !== 'completed' && <button className="mt-1 rounded border px-2 py-1" onClick={() => void act(() => completeApplicationSupplement(selected.application_id, item.supplement_id))}>标记补件完成</button>}</div>)}</div></section>}
        <ApplicationMaterialManagement application={selected} canWrite={canWrite} />
        <FinancingCommunicationPanel application={selected} canWrite={canWrite} onApplicationChanged={replace} />
        {!!selected.approval_conditions?.length && <section className="mt-3"><h6 className="font-medium">批复条件</h6><div className="mt-1 space-y-1">{selected.approval_conditions.map(item => <div key={item.approval_condition_id} className="flex items-center justify-between rounded bg-slate-50 p-2"><span>{item.title}</span>{canWrite ? <select aria-label={`${item.title}条件状态`} value={item.status} onChange={event => void act(() => updateApplicationApprovalCondition(selected.application_id, item.approval_condition_id, event.target.value))}><option value="pending">待满足</option><option value="satisfied">已满足</option><option value="waived">已豁免</option><option value="not_applicable">不适用</option></select> : <span>{item.status === 'pending' ? '待满足' : '已处理'}</span>}</div>)}</div></section>}
        {!!selected.review_feedback?.length && <section className="mt-3"><h6 className="font-medium">审批反馈</h6><ul className="mt-1 space-y-1">{selected.review_feedback.map(item => <li key={item.feedback_id} className="rounded bg-slate-50 p-2">{new Date(item.feedback_date).toLocaleString('zh-CN')} · {item.content}</li>)}</ul></section>}
        {!!selected.approval_records?.length && <section className="mt-3"><h6 className="font-medium">批复记录</h6><ul className="mt-1 space-y-1">{selected.approval_records.map(item => <li key={item.approval_record_id} className="rounded bg-slate-50 p-2">{new Date(item.approval_date).toLocaleString('zh-CN')} · {item.approval_status === 'full_approval' ? '全额批复' : item.approval_status === 'partial_approval' ? '部分批复' : '拒绝'} · {item.approved_amount ? money(item.approved_amount) : '未批复金额'}{item.approved_term_months ? ` · ${item.approved_term_months}个月` : ''}{item.approval_reference ? ` · ${item.approval_reference}` : ''}</li>)}</ul></section>}
        {selected.status === 'rejected' && <section className="mt-3 rounded bg-red-50 p-2 text-red-800"><h6 className="font-medium">拒绝结果</h6><p className="mt-1">原因：{selected.rejection_reason || '未记录'}</p>{selected.rejection_code && <p>代码：{selected.rejection_code}</p>}{selected.rejected_at && <p>时间：{new Date(selected.rejected_at).toLocaleString('zh-CN')}</p>}</section>}
        {!!selected.disbursements?.length && <section className="mt-3"><h6 className="font-medium">放款记录</h6><ul className="mt-1 space-y-1">{selected.disbursements.map(item => <li key={item.disbursement_id} className="rounded bg-emerald-50 p-2">{new Date(item.disbursed_at).toLocaleString('zh-CN')} · {money(item.amount)} · {item.bank_reference || '未填银行流水号'}</li>)}</ul></section>}
        <section className="mt-3 rounded border border-slate-200 p-3" data-testid="application-outcome"><div className="flex flex-wrap items-center justify-between gap-2"><h6 className="font-medium">最终结果</h6>{canWrite && !outcome && ['disbursed', 'rejected', 'cancelled', 'closed'].includes(selected.status) && <button type="button" disabled={busy} onClick={() => void finalizeOutcome()} className="rounded border border-indigo-300 px-2 py-1 text-indigo-700">确认并定稿结果</button>}</div>{outcome ? <div className="mt-2 grid gap-1 sm:grid-cols-2"><p>结果：{OUTCOME_LABELS[outcome.final_status] || '待确认'} · V{outcome.outcome_version}</p><p>提交：{money(outcome.submitted_amount)}</p><p>批复：{money(outcome.approved_amount)}</p><p>放款：{money(outcome.disbursed_amount)}</p><p>期限：{outcome.approved_term_months ? `${outcome.approved_term_months}个月` : '未批复'}</p><p>利率：{outcome.approved_interest_rate || '未记录'}</p>{outcome.rejection_reasons.length > 0 && <p className="sm:col-span-2">拒绝原因：{outcome.rejection_reasons.map(value => value.reason_label).join('、')}</p>}</div> : <p className="mt-1 text-slate-500">尚未定稿真实执行结果。</p>}</section>
        {canWrite && <section className="mt-3 space-y-2 border-t pt-3"><h6 className="font-medium">业务操作</h6><div className="flex flex-wrap gap-2">{selected.status === 'draft' && <button disabled={busy} onClick={() => void act(() => startApplicationPreparation(selected.application_id))} className="rounded border px-2 py-1">开始准备</button>}{['draft', 'preparing'].includes(selected.status) && <button disabled={busy} onClick={() => void act(() => markApplicationReady(selected.application_id))} className="rounded border px-2 py-1">标记待提交</button>}{selected.status === 'ready_to_submit' && <button disabled={busy} onClick={() => void act(() => submitFinancingApplication(selected.application_id, { submission_channel: '线下', submitted_amount: selected.target_amount }))} className="rounded border px-2 py-1">确认银行进件</button>}{['submitted', 'supplement_required'].includes(selected.status) && <button disabled={busy} onClick={() => void act(() => markApplicationUnderReview(selected.application_id))} className="rounded border px-2 py-1">标记审批中</button>}</div>
          {['submitted', 'under_review'].includes(selected.status) && <div className="flex gap-2"><input aria-label="补件要求" value={supplement} onChange={event => setSupplement(event.target.value)} placeholder="输入银行补件要求" className="flex-1 rounded border px-2 py-1" /><button disabled={busy || !supplement.trim()} onClick={() => void act(() => requestApplicationSupplement(selected.application_id, { description: supplement, required_materials: [supplement] }))} className="rounded border px-2 py-1">记录补件</button></div>}
          {selected.status === 'under_review' && <div className="flex gap-2"><input aria-label="审批反馈" value={feedback} onChange={event => setFeedback(event.target.value)} placeholder="记录银行审批反馈" className="flex-1 rounded border px-2 py-1" /><button disabled={busy || !feedback.trim()} onClick={() => void act(() => addApplicationReviewFeedback(selected.application_id, { feedback_type: 'general', content: feedback }))} className="rounded border px-2 py-1">保存反馈</button></div>}
          {selected.status === 'under_review' && <div className="flex flex-wrap gap-2"><input aria-label="批复金额" value={decisionAmount} onChange={event => setDecisionAmount(event.target.value)} placeholder="批复金额（元）" className="rounded border px-2 py-1" /><input aria-label="批复期限" value={decisionTerm} onChange={event => setDecisionTerm(event.target.value)} placeholder="期限（月）" className="w-28 rounded border px-2 py-1" /><button disabled={busy || !decisionAmount} onClick={() => void act(() => recordApplicationApproval(selected.application_id, { approved_amount: decisionAmount, approved_term_months: Number(decisionTerm) }))} className="rounded border px-2 py-1">记录批复</button><button disabled={busy} onClick={() => void act(() => recordApplicationRejection(selected.application_id, { rejection_reason: '银行反馈不符合当前准入条件' }))} className="rounded border border-red-300 px-2 py-1 text-red-700">记录拒绝</button></div>}
          {['approved', 'partially_approved', 'disbursing'].includes(selected.status) && <button disabled={busy} onClick={() => void act(() => createApplicationDisbursement(selected.application_id, { disbursed_amount: Math.max(Number(selected.approved_amount || 0) - Number(selected.disbursed_amount || 0), 0) }))} className="rounded border px-2 py-1">记录放款</button>}
          {['rejected', 'cancelled', 'closed'].includes(selected.status) && <button disabled={busy} onClick={() => void act(() => retryFinancingApplication(selected.application_id))} className="rounded border px-2 py-1">重新申请</button>}
        </section>}
        <section className="mt-3 border-t pt-3"><h6 className="font-medium">审计时间线</h6><ol className="mt-2 space-y-1">{selected.events.map(event => <li key={event.event_id} className="rounded border-l-2 border-slate-300 py-1 pl-2">{event.created_at ? new Date(event.created_at).toLocaleString('zh-CN') : ''} · {APPLICATION_EVENT_LABELS[event.event_type] || '业务记录'} · {event.operator_name}{event.from_status && event.to_status ? ` · ${APPLICATION_STATUS_LABELS[event.from_status] || event.from_status} → ${APPLICATION_STATUS_LABELS[event.to_status] || event.to_status}` : ''}</li>)}</ol></section>
      </article>}
    </div>}
  </section>;
}

