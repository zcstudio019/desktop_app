import { useEffect, useMemo, useState } from 'react';
import {
  attachApplicationContact, completeFinancingFollowUp, createApplicationCommunication,
  createFinancingContact, createReviewFeedbackFromCommunication, createSupplementFromCommunication,
  createTaskFromCommunication, getFinancingApplicationTimeline, listApplicationCommunications,
  listApplicationContacts, listFinancingFollowUps,
  type FinancingApplicationData, type FinancingCommunicationData, type FinancingContactData,
  type FinancingFollowUpData, type FinancingTimelineItemData,
} from '../services/api';
import {
  APPLICATION_EVENT_LABELS, COMMUNICATION_CHANNEL_LABELS, COMMUNICATION_DIRECTION_LABELS,
  COMMUNICATION_OUTCOME_LABELS, COMMUNICATION_SIDE_LABELS, FOLLOW_UP_STATUS_LABELS,
} from './financingExecutionLabels';

const TEMPLATES: Record<string, { subject: string; content: string; side: string; outcome: string }> = {
  remind_material: { subject: '提醒补充材料', content: '请按材料清单补充本次申请所需资料。', side: 'customer', outcome: 'waiting_customer' },
  material_received: { subject: '确认资料收到', content: '已收到您提供的材料，将继续核验。', side: 'customer', outcome: 'resolved' },
  submission_notice: { subject: '通知已进件', content: '本次融资申请已提交金融机构。', side: 'customer', outcome: 'info_only' },
  bank_progress: { subject: '查询审批进度', content: '请协助确认当前审批进度及待办事项。', side: 'institution', outcome: 'waiting_institution' },
  bank_supplement: { subject: '确认补件要求', content: '请确认本次补件的具体材料、口径及截止时间。', side: 'institution', outcome: 'action_required' },
};

export function FinancingCommunicationPanel({ application, canWrite, onApplicationChanged }: {
  application: FinancingApplicationData; canWrite: boolean;
  onApplicationChanged: (value: FinancingApplicationData) => void;
}) {
  const [contacts, setContacts] = useState<FinancingContactData[]>([]);
  const [records, setRecords] = useState<FinancingCommunicationData[]>([]);
  const [followUps, setFollowUps] = useState<FinancingFollowUpData[]>([]);
  const [timeline, setTimeline] = useState<FinancingTimelineItemData[]>([]);
  const [sideFilter, setSideFilter] = useState('');
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [contactForm, setContactForm] = useState({ contact_type: 'institution', name: '', branch_name: '', mobile: '', email: '' });
  const [form, setForm] = useState({ contact_id: '', communication_side: 'institution', channel: 'phone', direction: 'inbound', subject: '', content: '', outcome: 'info_only', follow_up_required: false, next_follow_up_at: '' });

  async function load() {
    const [contactRows, communicationRows, followUpRows, timelineRows] = await Promise.all([
      listApplicationContacts(application.application_id),
      listApplicationCommunications(application.application_id, sideFilter),
      listFinancingFollowUps(application.application_id),
      getFinancingApplicationTimeline(application.application_id),
    ]);
    setContacts(contactRows); setRecords(communicationRows); setFollowUps(followUpRows); setTimeline(timelineRows);
  }
  useEffect(() => { void load().catch(cause => setMessage(cause instanceof Error ? cause.message : '沟通记录加载失败')); }, [application.application_id, sideFilter]);

  async function run(action: () => Promise<unknown>, success: string) {
    setBusy(true); setMessage('');
    try { await action(); await load(); setMessage(success); }
    catch (cause) { setMessage(cause instanceof Error ? cause.message : '沟通操作失败'); }
    finally { setBusy(false); }
  }

  async function addContact() {
    const payload = { ...contactForm,
      customer_id: contactForm.contact_type === 'customer' ? application.customer_id : null,
      institution_name: contactForm.contact_type === 'institution' ? application.institution_name : null,
      is_primary: false, notes: '' };
    const contact = await createFinancingContact(payload);
    await attachApplicationContact(application.application_id, contact.contact_id,
      contact.contact_type === 'institution' ? 'relationship_manager' : 'handler');
    setContactForm({ contact_type: 'institution', name: '', branch_name: '', mobile: '', email: '' });
  }

  async function addCommunication() {
    await createApplicationCommunication(application.application_id, {
      ...form, contact_id: form.communication_side === 'internal' ? null : form.contact_id || null,
      feedback_tag: null, occurred_at: new Date().toISOString(),
      next_follow_up_at: form.follow_up_required && form.next_follow_up_at ? new Date(form.next_follow_up_at).toISOString() : null,
      internal_note: '',
    });
    setForm(value => ({ ...value, subject: '', content: '', follow_up_required: false, next_follow_up_at: '' }));
  }

  function applyTemplate(key: string) {
    const value = TEMPLATES[key]; if (!value) return;
    setForm(current => ({ ...current, communication_side: value.side, subject: value.subject,
      content: value.content, outcome: value.outcome, contact_id: '' }));
  }

  const availableContacts = useMemo(() => contacts.filter(value => value.contact_type === form.communication_side), [contacts, form.communication_side]);

  return <section className="mt-3 rounded border border-cyan-200 p-3" data-testid="financing-communication-panel">
    <div className="flex flex-wrap items-center justify-between gap-2"><div><h6 className="font-medium">联系人与沟通跟进</h6><p className="text-slate-500">人工记录客户及金融机构沟通；沟通内容不会自动改变申请状态。</p></div></div>
    {message && <p className="mt-2 rounded bg-slate-50 p-2">{message}</p>}
    <div className="mt-3 grid gap-3 lg:grid-cols-3">
      <section className="rounded bg-slate-50 p-2"><strong>联系人</strong>{contacts.length ? <ul className="mt-2 space-y-1">{contacts.map(value => <li key={value.contact_id} className="rounded bg-white p-2"><div>{value.contact_type === 'institution' ? '金融机构' : '客户'} · {value.name}</div><div className="text-slate-500">{value.institution_name || application.customer_name} {value.branch_name || ''} · {value.mobile || '未填手机'}</div></li>)}</ul> : <p className="mt-1 text-slate-500">暂无联系人</p>}
        {canWrite && <div className="mt-2 grid gap-1"><select aria-label="联系人类型" value={contactForm.contact_type} onChange={event => setContactForm({ ...contactForm, contact_type: event.target.value })}><option value="customer">客户联系人</option><option value="institution">机构联系人</option></select><input aria-label="联系人姓名" placeholder="姓名" value={contactForm.name} onChange={event => setContactForm({ ...contactForm, name: event.target.value })} /><input aria-label="支行" placeholder="支行/部门" value={contactForm.branch_name} onChange={event => setContactForm({ ...contactForm, branch_name: event.target.value })} /><input aria-label="手机" placeholder="手机" value={contactForm.mobile} onChange={event => setContactForm({ ...contactForm, mobile: event.target.value })} /><button disabled={busy || !contactForm.name} onClick={() => void run(addContact, '联系人已关联')} className="rounded border px-2 py-1">新增并关联</button></div>}
      </section>
      <section className="rounded bg-slate-50 p-2 lg:col-span-2"><div className="flex justify-between"><strong>新增沟通</strong><select aria-label="沟通模板" defaultValue="" onChange={event => applyTemplate(event.target.value)}><option value="">选择快捷模板</option>{Object.entries(TEMPLATES).map(([key, value]) => <option key={key} value={key}>{value.subject}</option>)}</select></div>
        {canWrite ? <div className="mt-2 grid gap-1 sm:grid-cols-2"><select aria-label="沟通对象" value={form.communication_side} onChange={event => setForm({ ...form, communication_side: event.target.value, contact_id: '', direction: event.target.value === 'internal' ? 'internal' : form.direction })}><option value="customer">客户</option><option value="institution">金融机构</option><option value="internal">内部沟通</option></select><select aria-label="联系人" value={form.contact_id} disabled={form.communication_side === 'internal'} onChange={event => setForm({ ...form, contact_id: event.target.value })}><option value="">选择联系人</option>{availableContacts.map(value => <option key={value.contact_id} value={value.contact_id}>{value.name}</option>)}</select><select aria-label="沟通渠道" value={form.channel} onChange={event => setForm({ ...form, channel: event.target.value })}>{Object.entries(COMMUNICATION_CHANNEL_LABELS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select><select aria-label="沟通方向" value={form.direction} onChange={event => setForm({ ...form, direction: event.target.value })}>{Object.entries(COMMUNICATION_DIRECTION_LABELS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select><input aria-label="沟通主题" className="sm:col-span-2" placeholder="沟通主题" value={form.subject} onChange={event => setForm({ ...form, subject: event.target.value })} /><textarea aria-label="沟通内容" className="sm:col-span-2" placeholder="人工整理后的沟通内容" value={form.content} onChange={event => setForm({ ...form, content: event.target.value })} /><select aria-label="沟通结果" value={form.outcome} onChange={event => setForm({ ...form, outcome: event.target.value })}>{Object.entries(COMMUNICATION_OUTCOME_LABELS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select><label><input type="checkbox" checked={form.follow_up_required} onChange={event => setForm({ ...form, follow_up_required: event.target.checked })} /> 需要后续跟进</label>{form.follow_up_required && <input aria-label="下次跟进时间" type="datetime-local" value={form.next_follow_up_at} onChange={event => setForm({ ...form, next_follow_up_at: event.target.value })} />}<button disabled={busy || !form.subject || !form.content || (form.communication_side !== 'internal' && !form.contact_id)} onClick={() => void run(addCommunication, '沟通记录已保存')} className="rounded bg-cyan-700 px-2 py-1 text-white disabled:opacity-40">保存沟通记录</button></div> : <p className="mt-2 text-slate-500">当前账号只能查看沟通记录。</p>}
      </section>
    </div>
    <section className="mt-3"><div className="flex items-center justify-between"><h6 className="font-medium">沟通记录</h6><select aria-label="沟通筛选" value={sideFilter} onChange={event => setSideFilter(event.target.value)}><option value="">全部</option><option value="customer">客户</option><option value="institution">金融机构</option><option value="internal">内部</option></select></div>{records.length ? <div className="mt-2 space-y-2">{records.map(value => <article key={value.communication_id} className={`rounded p-2 ${value.status === 'voided' ? 'bg-slate-100 opacity-60' : 'bg-cyan-50'}`}><div className="flex justify-between"><strong>{value.subject}</strong><span>{new Date(value.occurred_at).toLocaleString('zh-CN')}</span></div><div className="text-slate-500">{COMMUNICATION_SIDE_LABELS[value.communication_side]} · {value.contact_name || '内部'} · {COMMUNICATION_CHANNEL_LABELS[value.channel]} · {COMMUNICATION_OUTCOME_LABELS[value.outcome]}</div><p className="mt-1">{value.content}</p>{canWrite && value.status !== 'voided' && <div className="mt-2 flex flex-wrap gap-1"><button disabled={busy} onClick={() => void run(async () => { onApplicationChanged(await createTaskFromCommunication(value.communication_id, {})); }, '已从沟通创建任务')} className="rounded border px-2 py-1">创建任务</button>{value.communication_side === 'institution' && <><button disabled={busy} onClick={() => void run(async () => { onApplicationChanged(await createSupplementFromCommunication(value.communication_id, [value.subject])); }, '已创建补件要求')} className="rounded border px-2 py-1">创建补件</button><button disabled={busy} onClick={() => void run(async () => { onApplicationChanged(await createReviewFeedbackFromCommunication(value.communication_id)); }, '已记录为审批反馈')} className="rounded border px-2 py-1">记录为审批反馈</button></>}</div>}</article>)}</div> : <p className="mt-1 text-slate-500">暂无沟通记录</p>}</section>
    <section className="mt-3"><h6 className="font-medium">跟进事项</h6>{followUps.length ? <div className="mt-2 grid gap-2 sm:grid-cols-2">{followUps.map(value => <div key={value.follow_up_id} className={`rounded p-2 ${value.overdue ? 'bg-red-50 text-red-800' : 'bg-slate-50'}`}><div className="flex justify-between"><strong>{value.title}</strong><span>{value.overdue ? '已逾期' : FOLLOW_UP_STATUS_LABELS[value.status]}</span></div><div>{new Date(value.due_at).toLocaleString('zh-CN')} · {value.assignee_user_name || '待分配'}</div>{canWrite && !['completed', 'cancelled'].includes(value.status) && <button disabled={busy} onClick={() => void run(() => completeFinancingFollowUp(value.follow_up_id, '已完成跟进'), '跟进已完成')} className="mt-1 rounded border px-2 py-1">完成跟进</button>}</div>)}</div> : <p className="mt-1 text-slate-500">暂无跟进事项</p>}</section>
    <section className="mt-3"><h6 className="font-medium">全部动态</h6>{timeline.length ? <ol className="mt-2 space-y-1">{timeline.map(value => <li key={`${value.type}-${value.id}`} className="rounded bg-slate-50 p-2"><div className="flex justify-between"><strong>{APPLICATION_EVENT_LABELS[value.title] || value.title}</strong><span>{value.occurred_at ? new Date(value.occurred_at).toLocaleString('zh-CN') : '时间待确认'}</span></div><div className="text-slate-600">{typeof value.description === 'string' ? value.description : JSON.stringify(value.description)}</div></li>)}</ol> : <p className="mt-1 text-slate-500">暂无动态</p>}</section>
  </section>;
}
