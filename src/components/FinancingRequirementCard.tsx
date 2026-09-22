import { useEffect, useState } from 'react';
import { confirmFinancingRequirement, createFinancingRequirementDraft, type FinancingRequirementData } from '../services/api';

const statusLabel: Record<FinancingRequirementData['status'], string> = {
  draft: '草稿', needs_confirmation: '待确认', confirmed: '已确认', superseded: '已被新版本替代', cancelled: '已取消',
};
const purposes = ['流动资金', '采购', '项目垫资', '支付工程款', '设备采购', '置换存量融资', '归还借款', '扩大经营', '其他'];

export function FinancingRequirementCard({ initial }: { initial: FinancingRequirementData }) {
  const [item, setItem] = useState(initial);
  const [editing, setEditing] = useState(false);
  const [amountWan, setAmountWan] = useState(item.requested_amount ? String(item.requested_amount / 10000) : '');
  const [purpose, setPurpose] = useState(item.financing_purpose || '');
  const [detail, setDetail] = useState(item.purpose_detail || '');
  const [months, setMonths] = useState(item.term_value ? String(item.term_unit === 'year' ? item.term_value * 12 : item.term_value) : '');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => { setItem(initial); }, [initial.requirement_id, initial.status]);
  const canConfirm = Boolean(item.borrower_entity && item.requested_amount && item.financing_purpose && item.term_value && item.amount_confirmed && item.term_confirmed);

  function toggleEdit() {
    if (!editing) {
      setAmountWan(item.requested_amount == null ? '' : String(item.requested_amount / 10000));
      setPurpose(item.financing_purpose || '');
      setDetail(item.purpose_detail || '');
      setMonths(item.term_value == null ? '' : String(item.term_unit === 'year' ? item.term_value * 12 : item.term_value));
    }
    setError('');
    setEditing(value => !value);
  }

  async function confirm() {
    setBusy(true); setError('');
    try {
      const updated = await confirmFinancingRequirement(item.customer_id, item.requirement_id);
      setItem(updated);
      window.dispatchEvent(new CustomEvent('financing-requirement-updated'));
    } catch (cause) { setError(cause instanceof Error ? cause.message : '确认失败'); }
    finally { setBusy(false); }
  }

  async function saveEdit() {
    setBusy(true); setError('');
    try {
      const amount = Number(amountWan);
      const term = Number(months);
      if (!(amount > 0) || !Number.isInteger(term) || term <= 0) throw new Error('请填写有效的金额和期限');
      const originalMonths = item.term_value == null ? null : item.term_unit === 'year' ? item.term_value * 12 : item.term_value;
      if (amount * 10000 === item.requested_amount && (purpose || null) === item.financing_purpose
          && detail.trim() === (item.purpose_detail || '').trim() && term === originalMonths) {
        throw new Error('融资需求未发生变化，无需保存新版本。');
      }
      const updated = await createFinancingRequirementDraft(item.customer_id, {
        requested_amount: amount * 10000, currency: 'CNY', amount_confirmed: true,
        financing_purpose: purpose || null, purpose_detail: detail.trim() || null,
        term_value: term, term_unit: 'month', term_confirmed: true, term_original: `${term}个月`,
      });
      setItem(updated); setEditing(false);
      window.dispatchEvent(new CustomEvent('financing-requirement-updated'));
    } catch (cause) { setError(cause instanceof Error ? cause.message : '保存草稿失败'); }
    finally { setBusy(false); }
  }

  return <div className="mt-3 rounded-xl border border-blue-200 bg-blue-50/60 p-3 text-sm" data-testid="financing-requirement-card">
    <div className="font-semibold text-slate-900">融资需求 V{item.version}</div>
    <div className="mt-1 text-xs text-slate-600">状态：{statusLabel[item.status]}</div>
    <div className="mt-2 grid gap-1 text-slate-700">
      <div>融资主体：{item.borrower_entity || '待确认'}</div>
      <div>融资金额：{item.requested_amount ? `${item.requested_amount.toLocaleString('zh-CN')}元` : '待确认'}{item.requested_amount && !item.amount_confirmed ? '（约数，需确认）' : ''}</div>
      <div>融资用途：{item.purpose_detail || item.financing_purpose || '待确认'}</div>
      <div>融资期限：{item.term_value ? `${item.term_value}${item.term_unit === 'month' ? '个月' : item.term_unit === 'year' ? '年' : '天'}` : '待确认'}</div>
    </div>
    {editing && <div className="mt-3 grid gap-2">
      <label>金额（万元）<input aria-label="金额（万元）" type="number" min="0.01" value={amountWan} onChange={e => setAmountWan(e.target.value)} className="ml-2 w-28 rounded border p-1" /></label>
      <label>用途<select aria-label="用途" value={purpose} onChange={e => setPurpose(e.target.value)} className="ml-2 rounded border p-1"><option value="">待确认</option>{purposes.map(value => <option key={value}>{value}</option>)}</select></label>
      <label>用途说明<input aria-label="用途说明" value={detail} onChange={e => setDetail(e.target.value)} className="ml-2 rounded border p-1" /></label>
      <label>期限（月）<input aria-label="期限（月）" type="number" min="1" value={months} onChange={e => setMonths(e.target.value)} className="ml-2 w-20 rounded border p-1" /></label>
      <button type="button" disabled={busy} onClick={() => void saveEdit()} className="w-fit rounded bg-slate-800 px-3 py-1 text-white">保存待确认版本</button>
    </div>}
    <div className="mt-3 flex gap-2">
      {item.status === 'needs_confirmation' && <button type="button" disabled={busy || !canConfirm} onClick={() => void confirm()} className="rounded bg-blue-700 px-3 py-1 text-white disabled:opacity-50">确认需求</button>}
      <button type="button" disabled={busy} onClick={toggleEdit} className="rounded border border-slate-300 px-3 py-1">{editing ? '取消修改' : '修改需求'}</button>
    </div>
    {!canConfirm && item.status === 'needs_confirmation' && <p className="mt-2 text-xs text-amber-700">需明确确认融资主体、金额、用途和期限。</p>}
    {error && <p role="alert" className="mt-2 text-xs text-red-700">{error}</p>}
  </div>;
}
