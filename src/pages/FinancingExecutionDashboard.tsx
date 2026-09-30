import { useEffect, useMemo, useState } from 'react';
import { getFinancingExecutionDashboard, type FinancingExecutionDashboardData } from '../services/api';
import { APPLICATION_STATUS_LABELS } from '../components/financingExecutionLabels';
import { formatAmountWan } from '../utils/businessFormatters';

const STATUS_ORDER = ['preparing', 'ready_to_submit', 'submitted', 'supplement_required', 'under_review', 'approved', 'partially_approved', 'rejected', 'disbursing', 'disbursed', 'closed'];
const money = formatAmountWan;

export default function FinancingExecutionDashboard() {
  const [data, setData] = useState<FinancingExecutionDashboardData | null>(null);
  const [error, setError] = useState('');
  const [status, setStatus] = useState('');
  const [bank, setBank] = useState('');
  const [owner, setOwner] = useState('');
  const [overdueOnly, setOverdueOnly] = useState(false);
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');

  useEffect(() => {
    let active = true;
    void getFinancingExecutionDashboard({ status, institution_name: bank, responsible_user_id: owner, overdue_only: overdueOnly, date_from: dateFrom, date_to: dateTo })
      .then(value => { if (active) { setData(value); setError(''); } })
      .catch(cause => { if (active) setError(cause instanceof Error ? cause.message : '执行看板加载失败'); });
    return () => { active = false; };
  }, [status, bank, owner, overdueOnly, dateFrom, dateTo]);

  const banks = useMemo(() => [...new Set((data?.applications || []).map(value => value.institution_name))].sort(), [data]);
  const owners = useMemo(() => [...new Map((data?.applications || []).filter(value => value.responsible_user_id).map(value => [value.responsible_user_id!, value.responsible_user_name || value.responsible_user_id!])).entries()], [data]);
  const followUpGroups = useMemo(() => {
    const groups: Record<string, NonNullable<FinancingExecutionDashboardData['follow_up_metrics']>['items']> = { '今日': [], '明日': [], '本周': [], '已逾期': [], '已完成': [] };
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const tomorrow = new Date(today); tomorrow.setDate(today.getDate() + 1);
    const weekEnd = new Date(today); weekEnd.setDate(today.getDate() + 7);
    for (const item of data?.follow_up_metrics?.items || []) {
      const due = new Date(item.due_at); due.setHours(0, 0, 0, 0);
      if (item.status === 'completed') groups['已完成'].push(item);
      else if (item.overdue) groups['已逾期'].push(item);
      else if (due.getTime() === today.getTime()) groups['今日'].push(item);
      else if (due.getTime() === tomorrow.getTime()) groups['明日'].push(item);
      else if (due <= weekEnd) groups['本周'].push(item);
    }
    return groups;
  }, [data]);

  return <div className="min-h-full bg-slate-50 p-5" data-testid="financing-execution-dashboard">
    <header><h1 className="text-xl font-semibold text-slate-900">融资执行看板</h1><p className="mt-1 text-sm text-slate-500">跟进材料、进件、补件、审批、批复与放款进度</p></header>
    {error && <p role="alert" className="mt-3 rounded bg-red-50 p-2 text-sm text-red-700">{error}</p>}
    <section className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-7">
      <Stat label="总申请数" value={String(data?.total_applications || 0)} />
      <Stat label="目标融资" value={money(data?.amounts.target_amount)} />
      <Stat label="已提交" value={money(data?.amounts.submitted_amount)} />
      <Stat label="已批复" value={money(data?.amounts.approved_amount)} />
      <Stat label="已放款" value={money(data?.amounts.disbursed_amount)} />
      <Stat label="待批复" value={money(data?.amounts.pending_approval_amount)} />
      <Stat label="未放款" value={money(data?.amounts.undisbursed_amount)} />
    </section>
    <section className="mt-3 grid gap-2 sm:grid-cols-3 lg:grid-cols-6">{STATUS_ORDER.map(key => <Stat key={key} label={APPLICATION_STATUS_LABELS[key]} value={String(data?.status_counts[key] || 0)} />)}</section>
    <section className="mt-3 grid gap-2 sm:grid-cols-4">
      <Stat label="今日待跟进" value={String(data?.follow_up_metrics?.today_count || 0)} />
      <Stat label="逾期跟进" value={String(data?.follow_up_metrics?.overdue_count || 0)} />
      <Stat label="等待客户" value={String(data?.follow_up_metrics?.waiting_customer_count || 0)} />
      <Stat label="等待银行" value={String(data?.follow_up_metrics?.waiting_institution_count || 0)} />
    </section>
    <section className="mt-4 flex flex-wrap gap-2 rounded-lg border bg-white p-3">
      <select aria-label="申请状态" value={status} onChange={event => setStatus(event.target.value)}><option value="">全部状态</option>{Object.entries(APPLICATION_STATUS_LABELS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select>
      <select aria-label="银行机构" value={bank} onChange={event => setBank(event.target.value)}><option value="">全部银行</option>{banks.map(value => <option key={value}>{value}</option>)}</select>
      <select aria-label="负责人" value={owner} onChange={event => setOwner(event.target.value)}><option value="">全部负责人</option>{owners.map(([key, value]) => <option key={key} value={key}>{value}</option>)}</select>
      <label className="flex items-center gap-1"><input type="checkbox" checked={overdueOnly} onChange={event => setOverdueOnly(event.target.checked)} />仅看已逾期</label>
      <label>申请日期从 <input aria-label="开始日期" type="date" value={dateFrom} onChange={event => setDateFrom(event.target.value)} /></label>
      <label>到 <input aria-label="结束日期" type="date" value={dateTo} onChange={event => setDateTo(event.target.value)} /></label>
    </section>
    {!data?.applications.length ? <p className="mt-4 rounded-lg border bg-white p-8 text-center text-slate-500">当前暂无融资执行申请。请先在客户融资方案中确认方案并创建申请。</p> : <section className="mt-4 grid gap-3 lg:grid-cols-2">{data.applications.map(item => { const currentStage = item.stages.find(stage => stage.stage_code === item.current_stage_code); return <article key={item.application_id} className="rounded-lg border bg-white p-4 text-sm"><div className="flex justify-between gap-2"><div><strong>{item.customer_name}</strong><div>{item.institution_name} · {item.product_name}</div><div className="text-xs text-slate-500">{item.application_no} · 第{item.attempt_no}次申请</div></div><span>{APPLICATION_STATUS_LABELS[item.status]}</span></div><div className="mt-3 grid grid-cols-2 gap-1 text-xs"><div>目标：{money(item.target_amount)}</div><div>提交：{money(item.submitted_amount)}</div><div>批复：{money(item.approved_amount)}</div><div>放款：{money(item.disbursed_amount)}</div><div>当前阶段：{currentStage?.stage_name || '待确认'}</div><div>负责人：{item.responsible_user_name || '待分配'}</div><div>下一任务：{item.next_task?.title || '暂无'}</div><div>截止时间：{item.next_task?.due_date || '未设置'}</div><div className="col-span-2">最后更新：{item.updated_at ? new Date(item.updated_at).toLocaleString('zh-CN') : '暂无'}</div></div>{!!item.blocking_items?.length && <ul className="mt-2 rounded bg-amber-50 p-2 text-xs text-amber-800">{item.blocking_items.map(value => <li key={value}>{value}</li>)}</ul>}<p className="mt-2 text-xs text-blue-700">下一步：{item.next_action}</p></article>; })}</section>}
    {!!data?.follow_up_metrics?.items.length && <section className="mt-4 rounded-lg border bg-white p-4"><h2 className="font-semibold">我的跟进</h2><div className="mt-2 grid gap-3 lg:grid-cols-2">{Object.entries(followUpGroups).map(([label, items]) => <section key={label} className="rounded border p-2"><h3 className="font-medium">{label} · {items.length}</h3>{items.length ? <div className="mt-1 space-y-1">{items.map(item => <div key={item.follow_up_id} className={`rounded p-2 text-sm ${item.overdue ? 'bg-red-50 text-red-800' : 'bg-slate-50'}`}><div className="flex justify-between"><strong>{item.title}</strong><span>{item.overdue ? '已逾期' : new Date(item.due_at).toLocaleString('zh-CN')}</span></div><div>{item.assignee_user_name || '待分配'} · {item.description || '无补充说明'}</div></div>)}</div> : <p className="mt-1 text-sm text-slate-500">暂无</p>}</section>)}</div></section>}
  </div>;
}

function Stat({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg border bg-white p-3"><div className="text-xs text-slate-500">{label}</div><div className="mt-1 font-semibold text-slate-900">{value}</div></div>;
}

