import React, { useCallback, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ArrowLeft, Eye, FileText, Pencil, RefreshCw, Save, Trash2 } from 'lucide-react';
import {
  deleteCustomer,
  deleteCustomerProfileMarkdown,
  getCustomerProfileMarkdown,
  listCustomers,
  updateCustomerProfileMarkdown,
} from '../services/api';
import type { CustomerListItem, CustomerProfileMarkdownResponse } from '../services/types';
import { useApp } from '../context/AppContext';
import ProcessFeedbackCard from './common/ProcessFeedbackCard';

interface CustomerDataPageProps {
  onBack?: () => void;
}

type EditorMode = 'edit' | 'preview';

function sanitizeProfileMarkdown(markdown: string): string {
  return markdown
    .replace(/^>.*customer_id=.*$/gm, '')
    .replace(/^- 客户ID：.*$/gm, '')
    .replace(/(- 客户类型：)\s*enterprise\b/g, '$1企业')
    .replace(/(- 客户类型：)\s*personal\b/g, '$1个人')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function formatProfileDateTime(value?: string | null): string {
  if (!value) {
    return '未记录';
  }

  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString('zh-CN', {
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

const CustomerDataPage: React.FC<CustomerDataPageProps> = ({ onBack }) => {
  const { state, setCurrentCustomer, recordSystemActivity } = useApp();
  const [customers, setCustomers] = useState<CustomerListItem[]>([]);
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(state.extraction.currentCustomerId);
  const [profile, setProfile] = useState<CustomerProfileMarkdownResponse | null>(null);
  const [draft, setDraft] = useState('');
  const [mode, setMode] = useState<EditorMode>('edit');
  const [loadingCustomers, setLoadingCustomers] = useState(true);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [customerSearch, setCustomerSearch] = useState('');
  const [error, setError] = useState<string | null>(null);

  const loadCustomers = useCallback(async () => {
    setLoadingCustomers(true);
    setError(null);
    try {
      const items = await listCustomers();
      setCustomers(items);
      setSelectedCustomerId((current) => current ?? items[0]?.record_id ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载客户列表失败');
    } finally {
      setLoadingCustomers(false);
    }
  }, []);

  const loadProfile = useCallback(
    async (customerId: string) => {
      setLoadingProfile(true);
      setError(null);
      try {
        const result = await getCustomerProfileMarkdown(customerId);
        const sanitizedMarkdown = sanitizeProfileMarkdown(result.markdown_content);
        setProfile(result);
        setDraft(sanitizedMarkdown);
        const matchedCustomer = customers.find((item) => item.record_id === customerId);
        setCurrentCustomer(matchedCustomer?.name ?? result.customer_name, customerId);
      } catch (err) {
        setError(err instanceof Error ? err.message : '加载资料汇总失败');
        setProfile(null);
        setDraft('');
      } finally {
        setLoadingProfile(false);
      }
    },
    [customers, setCurrentCustomer]
  );

  useEffect(() => {
    void loadCustomers();
  }, [loadCustomers]);

  useEffect(() => {
    if (!selectedCustomerId) return;
    void loadProfile(selectedCustomerId);
  }, [selectedCustomerId, loadProfile]);

  const selectedCustomer = useMemo(
    () => customers.find((item) => item.record_id === selectedCustomerId) ?? null,
    [customers, selectedCustomerId]
  );
  const filteredCustomers = useMemo(() => {
    const keyword = customerSearch.trim().toLowerCase();
    if (!keyword) {
      return customers;
    }
    return customers.filter((item) => {
      const name = (item.name || '').toLowerCase();
      const type = (item.customer_type || '').toLowerCase();
      return name.includes(keyword) || type.includes(keyword);
    });
  }, [customerSearch, customers]);

  const profileStatusLabel = profile?.source_mode === 'manual' ? '手动整理中' : '系统已整理';
  const profileStatusClassName =
    profile?.source_mode === 'manual'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : 'border-sky-200 bg-sky-50 text-sky-700';
  const profileVersionLabel = profile?.version ? `V${profile.version}` : 'V1';
  const profileHintText =
    profile?.source_mode === 'manual'
      ? '当前版本会优先用于资料问答与报告生成'
      : '当前内容由系统整理生成，可继续手动修订';

  const isDirty = draft !== (profile ? sanitizeProfileMarkdown(profile.markdown_content) : '');
  const profileFeedback = useMemo(() => {
    if (!selectedCustomerId) {
      return {
        tone: 'idle' as const,
        title: '等待选择客户',
        description: '请先选择客户，再查看、编辑或保存资料汇总。',
        persistenceHint: '尚未进入资料汇总处理。',
        nextStep: '先从左侧选择客户，再继续整理资料。',
      };
    }

    if (loadingProfile) {
      return {
        tone: 'processing' as const,
        title: '正在加载资料汇总',
        description: '系统正在读取当前客户的资料汇总内容与版本信息。',
        persistenceHint: '主流程处理中。',
        nextStep: '请稍候，加载完成后可继续查看或编辑。',
      };
    }

    if (saving) {
      return {
        tone: 'processing' as const,
        title: '正在保存资料汇总',
        description: '系统正在保存当前修改，并同步更新资料问答与风险评估使用的资料版本。',
        persistenceHint: '主流程处理中，保存完成后会立即生效。',
        nextStep: '请稍候，保存完成后建议去 AI 对话验证最新内容。',
      };
    }

    if (saveSuccess) {
      return {
        tone: 'success' as const,
        title: '资料汇总已保存',
        description: '当前客户资料汇总已经更新，资料问答和风险报告会优先读取这份最新版本。',
        persistenceHint: '主流程已保存成功。',
        nextStep: '建议前往 AI 对话验证资料问答或重新生成风险报告。',
      };
    }

    if (error) {
      return {
        tone: 'error' as const,
        title: '资料汇总处理失败',
        description: error,
        persistenceHint: profile ? '本次修改未保存，上一版资料汇总仍可继续使用。' : '当前没有保存成功的新版本。',
        nextStep: '请检查内容后重试，或先刷新当前客户资料。',
      };
    }

    if (isDirty) {
      return {
        tone: 'partial' as const,
        title: '检测到未保存修改',
        description: '你已经修改了当前资料汇总，但系统仍在使用上一版已保存内容。',
        persistenceHint: '主流程仍使用上一版已保存资料。',
        nextStep: '确认无误后点击保存，再去资料问答或风险报告查看变化。',
      };
    }

    return {
      tone: 'idle' as const,
      title: '资料汇总已就绪',
      description: '当前客户资料汇总可以继续查看、修订和预览。',
      persistenceHint: '当前展示的是系统可用版本。',
      nextStep: '如需更新内容，可直接编辑并保存。',
    };
  }, [selectedCustomerId, loadingProfile, saving, saveSuccess, error, profile, isDirty]);

  const handleSave = useCallback(async () => {
    if (!selectedCustomerId) return;
    setSaving(true);
    setSaveSuccess(false);
    setError(null);
    try {
      const result = await updateCustomerProfileMarkdown(selectedCustomerId, {
        markdown_content: draft,
        title: selectedCustomer?.name ? `${selectedCustomer.name} 资料汇总` : undefined,
      });
      setProfile(result);
      setDraft(sanitizeProfileMarkdown(result.markdown_content));
      recordSystemActivity({
        type: 'profile',
        title: '资料汇总已更新',
        description: '系统已保存最新资料整理内容，并同步刷新资料问答索引。',
        customerName: selectedCustomer?.name ?? result.customer_name,
        customerId: selectedCustomerId,
        status: 'success',
      });
      setSaveSuccess(true);
      window.setTimeout(() => setSaveSuccess(false), 2200);
    } catch (err) {
      setError(err instanceof Error ? err.message : '保存资料汇总失败');
    } finally {
      setSaving(false);
    }
  }, [draft, recordSystemActivity, selectedCustomer, selectedCustomerId]);

  const handleDeleteProfile = useCallback(async () => {
    if (!selectedCustomerId) return;
    const confirmed = window.confirm('确认回到系统整理稿吗？系统会立刻重新生成一份最新资料汇总。');
    if (!confirmed) return;
    try {
      await deleteCustomerProfileMarkdown(selectedCustomerId);
      await loadProfile(selectedCustomerId);
    } catch (err) {
      setError(err instanceof Error ? err.message : '恢复系统整理稿失败');
    }
  }, [loadProfile, selectedCustomerId]);

  const handleDeleteCustomer = useCallback(async () => {
    if (!selectedCustomerId || !selectedCustomer) return;
    const confirmed = window.confirm(`确认删除客户“${selectedCustomer.name}”及其全部相关数据吗？`);
    if (!confirmed) return;
    try {
      await deleteCustomer(selectedCustomerId);
      const nextCustomers = customers.filter((item) => item.record_id !== selectedCustomerId);
      setCustomers(nextCustomers);
      setSelectedCustomerId(nextCustomers[0]?.record_id ?? null);
      setProfile(null);
      setDraft('');
      if (!nextCustomers.length) {
        setCurrentCustomer(null, null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除客户失败');
    }
  }, [customers, selectedCustomer, selectedCustomerId, setCurrentCustomer]);

  return (
    <div className="flex h-full bg-slate-50">
      <aside className="w-72 border-r border-slate-200 bg-white">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-4">
          <div className="flex items-center gap-2">
            {onBack && (
              <button
                type="button"
                onClick={onBack}
                className="rounded-lg p-1.5 transition-colors hover:bg-slate-100"
                aria-label="返回"
              >
                <ArrowLeft className="h-4 w-4 text-slate-500" />
              </button>
            )}
            <div>
              <h1 className="text-sm font-semibold text-slate-800">资料汇总</h1>
              <p className="text-xs text-slate-400">客户资料整理与维护</p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => {
              void loadCustomers();
              if (selectedCustomerId) void loadProfile(selectedCustomerId);
            }}
            className="rounded-lg border border-slate-200 p-2 text-slate-500 transition-colors hover:bg-slate-50"
            aria-label="刷新"
          >
            <RefreshCw className="h-4 w-4" />
          </button>
        </div>

        <div className="p-3">
          <div className="mb-3">
            <input
              type="text"
              value={customerSearch}
              onChange={(e) => setCustomerSearch(e.target.value)}
              placeholder="搜索客户名称"
              className="w-full rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700 outline-none transition-colors focus:border-blue-300 focus:bg-white"
            />
          </div>
          {loadingCustomers ? (
            <div className="py-8 text-center text-sm text-slate-400">加载客户中...</div>
          ) : customers.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
              暂无客户
            </div>
          ) : filteredCustomers.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
              未找到匹配客户
            </div>
          ) : (
            <div className="space-y-2">
              {filteredCustomers.map((customer) => {
                const active = customer.record_id === selectedCustomerId;
                return (
                  <button
                    key={customer.record_id}
                    type="button"
                    onClick={() => setSelectedCustomerId(customer.record_id)}
                    className={`relative w-full rounded-2xl border px-3 py-3 text-left transition-all ${
                      active
                        ? 'border-blue-300 bg-gradient-to-r from-blue-50 to-white shadow-sm shadow-blue-100'
                        : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'
                    }`}
                  >
                    {active && <span className="absolute inset-y-3 left-0 w-1 rounded-r-full bg-blue-500" />}
                    <div className="text-sm font-medium text-slate-800">{customer.name || customer.record_id}</div>
                    <div className="mt-2 text-xs text-slate-500">最近上传：{customer.upload_time || '未记录'}</div>
                    <div className="mt-2 text-xs text-slate-400">
                      {customer.customer_type === 'personal' ? '个人' : '企业'}
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </aside>

      <main className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">
              {selectedCustomer?.name || profile?.customer_name || '请选择客户'}
            </h2>
            {profile && (
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <span className="inline-flex items-center rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-600">
                  版本 {profileVersionLabel}
                </span>
                <span
                  className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${profileStatusClassName}`}
                >
                  {profileStatusLabel}
                </span>
                <span className="text-xs text-slate-400">最近更新：{formatProfileDateTime(profile?.updated_at)}</span>
              </div>
            )}
            {profile && <div className="mt-2 text-xs text-slate-400">{profileHintText}</div>}
          </div>

          <div className="flex items-center gap-2">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-1">
              <button
                type="button"
                onClick={() => setMode('edit')}
                className={`rounded-lg px-3 py-1.5 text-sm ${
                  mode === 'edit' ? 'bg-white text-slate-800 shadow-sm' : 'text-slate-500'
                }`}
              >
                <span className="inline-flex items-center gap-1">
                  <Pencil className="h-3.5 w-3.5" />
                  编辑
                </span>
              </button>
              <button
                type="button"
                onClick={() => setMode('preview')}
                className={`rounded-lg px-3 py-1.5 text-sm ${
                  mode === 'preview' ? 'bg-white text-slate-800 shadow-sm' : 'text-slate-500'
                }`}
              >
                <span className="inline-flex items-center gap-1">
                  <Eye className="h-3.5 w-3.5" />
                  预览
                </span>
              </button>
            </div>

            <button
              type="button"
              onClick={() => selectedCustomerId && void loadProfile(selectedCustomerId)}
              disabled={!selectedCustomerId || loadingProfile}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              刷新
            </button>
            <button
              type="button"
              onClick={() => void handleSave()}
              disabled={!selectedCustomerId || saving || !isDirty}
              className="rounded-lg bg-blue-600 px-3 py-2 text-sm font-medium text-white shadow-sm shadow-blue-200 transition-colors hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="inline-flex items-center gap-1">
                <Save className="h-3.5 w-3.5" />
                {saving ? '保存中...' : '保存'}
              </span>
            </button>
            <button
              type="button"
              onClick={() => void handleDeleteProfile()}
              disabled={!selectedCustomerId}
              className="rounded-lg border border-amber-200 bg-amber-50/70 px-3 py-2 text-sm text-amber-700 transition-colors hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              回到系统整理稿
            </button>
            <button
              type="button"
              onClick={() => void handleDeleteCustomer()}
              disabled={!selectedCustomerId}
              className="rounded-lg border border-red-200 bg-white px-3 py-2 text-sm text-red-600 transition-colors hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="inline-flex items-center gap-1">
                <Trash2 className="h-3.5 w-3.5" />
                删除客户
              </span>
            </button>
          </div>
        </div>

        <div className="border-b border-slate-200 bg-slate-50/80 px-6 py-3">
          <div className="flex flex-wrap items-center gap-2 text-sm text-slate-600">
            <span className="rounded-full bg-white px-2.5 py-1 text-xs font-medium text-slate-600 shadow-sm">
              本页说明
            </span>
            <span>系统会先整理当前客户的核心资料，你也可以继续补充、修订，并保存为当前使用版本。</span>
          </div>
        </div>

        {error && (
          <div className="border-b border-red-100 bg-red-50 px-6 py-3 text-sm text-red-600">{error}</div>
        )}

        {saveSuccess && !error && (
          <div className="border-b border-emerald-100 bg-emerald-50 px-6 py-3 text-sm text-emerald-700">
            已为当前客户保存最新资料整理内容，资料问答会优先读取这份版本。
          </div>
        )}

        {isDirty && !saving && !error && (
          <div className="border-b border-amber-100 bg-amber-50 px-6 py-3 text-sm text-amber-700">
            当前有未保存修改。保存后，资料问答和风险评估会优先读取这份最新内容。
          </div>
        )}

        <div className="border-b border-slate-200 bg-white px-6 py-4">
          <ProcessFeedbackCard
            tone={profileFeedback.tone}
            title={profileFeedback.title}
            description={profileFeedback.description}
            persistenceHint={profileFeedback.persistenceHint}
            nextStep={profileFeedback.nextStep}
          />
        </div>

        {!selectedCustomerId ? (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-400">请选择客户</div>
        ) : loadingProfile ? (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-400">加载资料汇总中...</div>
        ) : (
          <div className="grid min-h-0 flex-1 grid-cols-2">
            <section className="flex min-h-0 flex-col border-r border-slate-200 bg-white">
              <div className="border-b border-slate-200 px-5 py-3">
                <div className="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1.5 text-sm font-medium text-blue-700">
                  <FileText className="h-4 w-4" />
                  资料内容
                </div>
              </div>
              {mode === 'edit' ? (
                <textarea
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  className="min-h-0 flex-1 resize-none border-0 p-5 font-mono text-sm leading-6 text-slate-700 outline-none"
                  placeholder="请输入资料汇总内容，支持标题、分段等格式"
                />
              ) : (
                <div className="min-h-0 flex-1 overflow-auto p-5">
                  <pre className="whitespace-pre-wrap break-words text-sm leading-6 text-slate-700">
                    {draft || '暂无内容'}
                  </pre>
                </div>
              )}
            </section>

            <section className="flex min-h-0 flex-col bg-[radial-gradient(circle_at_top,_rgba(148,163,184,0.14),_transparent_45%),linear-gradient(180deg,#f8fafc_0%,#f1f5f9_100%)]">
              <div className="border-b border-slate-200 px-5 py-3">
                <div className="inline-flex items-center gap-2 rounded-full bg-slate-100 px-3 py-1.5 text-sm font-medium text-slate-700">
                  <Eye className="h-4 w-4" />
                  阅读预览
                </div>
              </div>
              <div className="min-h-0 flex-1 overflow-auto px-6 py-6">
                <article className="prose prose-slate max-w-none rounded-[28px] border border-white/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)] backdrop-blur">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{draft || '暂无内容'}</ReactMarkdown>
                </article>
              </div>
            </section>
          </div>
        )}
      </main>
    </div>
  );
};

export default CustomerDataPage;

