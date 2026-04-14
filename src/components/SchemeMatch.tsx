import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, Download, Loader2, RefreshCw, Search, Sparkles, Trash2 } from 'lucide-react';
import { createSchemeMatchJob, getApplication, getChatJobStatus, listCustomers, listSavedApplications, parseNaturalLanguage, searchCustomer, type SavedApplicationListItem } from '../services/api';
import type { ChatJobStatusResponse, ChatJobSummaryResponse, CustomerListItem, SchemeMatchRequest } from '../services/types';
import { useAbortController } from '../hooks/useAbortController';
import { useApp } from '../context/AppContext';
import ProcessFeedbackCard, { type ProcessFeedbackTone } from './common/ProcessFeedbackCard';
import AsyncJobCard from './common/AsyncJobCard';
import SchemeMatchingResultCard from './common/SchemeMatchingResultCard';
import { getJobStatusText, getJobTypeLabel } from '../config/jobDisplay';

type CreditType = 'personal' | 'enterprise_credit' | 'enterprise_mortgage';
type DataSource = 'currentCustomer' | 'savedApplication' | 'manual' | 'searchCustomer';

const CREDIT_TYPES: Array<{ value: CreditType; label: string }> = [
  { value: 'personal', label: '个人贷款' },
  { value: 'enterprise_credit', label: '企业信用贷款' },
  { value: 'enterprise_mortgage', label: '企业抵押贷款' },
];

const APPLICATION_LOAN_TYPE_LABELS: Record<string, string> = {
  enterprise: '企业贷款',
  personal: '个人贷款',
};

const DATA_SOURCES: Array<{ value: DataSource; label: string; hint: string }> = [
  { value: 'currentCustomer', label: '当前客户资料', hint: '优先读取当前客户上下文中的最新资料。' },
  { value: 'savedApplication', label: '已保存申请表', hint: '使用已保存申请表中的结构化数据进行匹配。' },
  { value: 'manual', label: '手动输入资料', hint: '先输入资料摘要，再解析为结构化字段后匹配。' },
  { value: 'searchCustomer', label: '搜索客户资料', hint: '按客户名称搜索系统资料后直接匹配。' },
];

const mergeResults = (results: Array<{ content: Record<string, unknown> }>) =>
  results.reduce<Record<string, unknown>>((acc, item) => Object.assign(acc, item.content || {}), {});

const formatTime = (value?: string | null) => {
  if (!value) return '未记录';
  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
};

const formatApplicationLoanType = (value?: string | null) => {
  if (!value) return '贷款申请';
  return APPLICATION_LOAN_TYPE_LABELS[value] || value;
};

const downloadResult = (content: string, name: string) => {
  const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${name.replace(/[\\/:*?"<>|]/g, '_')}-融资方案匹配结果.md`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};

const SchemeMatchPage: React.FC = () => {
  const { state, setCurrentCustomer, setSchemeResult, setSchemeTaskStatus, recordSystemActivity } = useApp();
  const { getSignal, abort } = useAbortController();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [customers, setCustomers] = useState<CustomerListItem[]>([]);
  const [applications, setApplications] = useState<SavedApplicationListItem[]>([]);
  const [creditType, setCreditType] = useState<CreditType>('enterprise_credit');
  const [dataSource, setDataSource] = useState<DataSource>('currentCustomer');
  const [selectedApplicationId, setSelectedApplicationId] = useState('');
  const [manualInput, setManualInput] = useState('');
  const [manualData, setManualData] = useState<Record<string, unknown> | null>(null);
  const [manualFields, setManualFields] = useState<string[]>([]);
  const [manualError, setManualError] = useState<string | null>(null);
  const [searchName, setSearchName] = useState('');
  const [searchDataState, setSearchDataState] = useState<Record<string, unknown> | null>(null);
  const [searchStatus, setSearchStatus] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [jobPolling, setJobPolling] = useState(false);
  const [activeJobCard, setActiveJobCard] = useState<ChatJobSummaryResponse | null>(null);
  const [copySuccess, setCopySuccess] = useState(false);
  const recoverRef = useRef(false);
  const stopPollingRef = useRef(false);
  const matchRef = useRef<((t: CreditType, d: Record<string, unknown>, n?: string | null, i?: string | null) => Promise<void>) | null>(null);
  const basisRef = useRef<HTMLDivElement | null>(null);

  const currentCustomerName = state.extraction.currentCustomer;
  const currentCustomerId = state.extraction.currentCustomerId;
  const currentResults = currentCustomerName ? state.extraction.customerDataMap[currentCustomerName] || [] : [];
  const currentData = useMemo(() => mergeResults(currentResults), [currentResults]);
  const schemeResult = state.scheme.result;
  const activeResult = schemeResult?.result ?? null;
  const schemeJobLabel = getJobTypeLabel('scheme_match');
  const currentResultTaskLabel = activeJobCard ? getJobTypeLabel(activeJobCard.jobType, activeJobCard.jobTypeLabel) : schemeJobLabel;

  useEffect(() => {
    let mounted = true;
    Promise.all([listCustomers(undefined, getSignal()), listSavedApplications(getSignal())])
      .then(([customerList, saved]) => {
        if (!mounted) return;
        setCustomers(customerList);
        setApplications(saved);
      })
      .catch((issue) => {
        if (!(issue instanceof Error && issue.name === 'AbortError')) {
          console.error('Failed to load scheme page data:', issue);
        }
      });
    return () => {
      mounted = false;
    };
  }, [getSignal]);

  const pollSchemeJob = useCallback(async (
    jobId: string,
    type: CreditType,
    customerName?: string | null,
    customerId?: string | null,
  ) => {
    const startedAt = Date.now();
    let failureCount = 0;
    stopPollingRef.current = false;
    setJobPolling(true);
    setLoading(false);
    setActiveJobCard((prev) => prev ? { ...prev, status: 'running', progressMessage: '正在处理任务', errorMessage: null } : prev);

    try {
      while (true) {
        if (stopPollingRef.current) {
          return;
        }
        if (Date.now() - startedAt > 5 * 60 * 1000) {
          const message = '任务处理时间较长，请稍后重新进入页面查看。';
          setActionError(message);
          setError(new Error(message));
          setSchemeTaskStatus('idle', null);
          return;
        }

        let status: ChatJobStatusResponse;
        try {
          status = await getChatJobStatus(jobId, getSignal());
          setActiveJobCard({
            jobId: status.jobId,
            jobType: status.jobType,
            jobTypeLabel: status.jobTypeLabel,
            customerId: status.customerId,
            customerName: status.customerName || customerName || '',
            status: status.status,
            progressMessage: status.progressMessage,
            errorMessage: status.errorMessage,
            createdAt: status.createdAt,
            startedAt: status.startedAt,
            finishedAt: status.finishedAt,
            targetPage: status.targetPage,
            resultSummary: status.resultSummary,
          });
        } catch (issue) {
          failureCount += 1;
          if (failureCount >= 3) {
            const message = issue instanceof Error ? issue.message : '任务状态获取失败';
            setActionError(message);
            setError(new Error(message));
            setSchemeTaskStatus('idle', null);
            return;
          }
          await new Promise((resolve) => window.setTimeout(resolve, 2000));
          continue;
        }

        failureCount = 0;

        if (status.status === 'pending' || status.status === 'running') {
          await new Promise((resolve) => window.setTimeout(resolve, 2000));
          continue;
        }

        if (status.status === 'success' && status.result) {
          const response = status.result as unknown as { matchResult: string; matchingData?: Record<string, unknown> | null; creditType?: string; customerName?: string; customerId?: string };
          const resolvedCustomerName = response.customerName || customerName || currentCustomerName || null;
          const resolvedCustomerId = response.customerId || customerId || currentCustomerId || null;
          setSchemeResult({
            result: response.matchResult,
            matchingData: response.matchingData || null,
            lastCreditType: (response.creditType as CreditType | undefined) ?? type,
            customerId: resolvedCustomerId,
            customerName: resolvedCustomerName,
            matchedAt: status.finishedAt || new Date().toISOString(),
            stale: false,
            staleReason: '',
            staleAt: '',
          });
          setSchemeTaskStatus('done', null);
          setError(null);
          setActionError(null);
          recordSystemActivity({
            type: 'matching',
            title: getJobStatusText('scheme_match', 'success'),
            description: '已生成当前客户的融资方案匹配结果。',
            customerName: resolvedCustomerName,
            customerId: resolvedCustomerId,
            status: 'success',
          });
          return;
        }

        const message = status.errorMessage || getJobStatusText('scheme_match', 'failed');
        setActionError(message);
        setError(new Error(message));
        setSchemeTaskStatus('idle', null);
        return;
      }
    } finally {
      setJobPolling(false);
      stopPollingRef.current = false;
    }
  }, [currentCustomerId, currentCustomerName, getSignal, recordSystemActivity, setSchemeResult, setSchemeTaskStatus]);

  const doMatch = useCallback(async (type: CreditType, customerData: Record<string, unknown>, customerName?: string | null, customerId?: string | null) => {
    const request: SchemeMatchRequest = {
      creditType: type,
      customerData,
      customerName: customerName ?? currentCustomerName,
      customerId: customerId ?? currentCustomerId,
    };
    setLoading(true);
    setError(null);
    setActionError(null);
    setSchemeTaskStatus('matching', { creditType: type, customerData, customerName: request.customerName, customerId: request.customerId });
    try {
      const job = await createSchemeMatchJob(request, getSignal());
      setActiveJobCard({
        jobId: job.jobId,
        jobType: 'scheme_match',
        customerId: request.customerId || '',
        customerName: request.customerName || '',
        status: job.status,
        progressMessage: '任务已提交',
        errorMessage: null,
        createdAt: new Date().toISOString(),
        startedAt: '',
        finishedAt: '',
        targetPage: 'scheme',
        resultSummary: null,
      });
      setSchemeTaskStatus('idle', null);
      await pollSchemeJob(job.jobId, type, request.customerName, request.customerId);
    } catch (issue) {
      if (issue instanceof Error && issue.name === 'AbortError') {
        return;
      }
      const nextError = issue instanceof Error ? issue : new Error(getJobStatusText('scheme_match', 'failed'));
      setError(nextError);
      setActionError(nextError.message);
      setSchemeTaskStatus('idle', null);
    } finally {
      setLoading(false);
    }
  }, [currentCustomerId, currentCustomerName, getSignal, pollSchemeJob, setSchemeTaskStatus]);

  const handleOpenCurrentSchemeJob = useCallback((job: ChatJobSummaryResponse) => {
    if (job.status === 'success') {
      basisRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return;
    }
    if (!jobPolling) {
      void pollSchemeJob(
        job.jobId,
        creditType,
        job.customerName || currentCustomerName || undefined,
        job.customerId || currentCustomerId || undefined,
      );
    }
  }, [creditType, currentCustomerId, currentCustomerName, jobPolling, pollSchemeJob]);

  matchRef.current = doMatch;

  useEffect(() => {
    const task = state.tasks.scheme;
    if (task.status !== 'matching' || !task.params || recoverRef.current || !matchRef.current) return;
    const { creditType: taskCreditType, customerData, customerName, customerId } = task.params;
    if (!['personal', 'enterprise_credit', 'enterprise_mortgage'].includes(taskCreditType)) return;
    recoverRef.current = true;
    setCreditType(taskCreditType as CreditType);
    void matchRef.current(taskCreditType as CreditType, customerData, customerName, customerId).finally(() => {
      recoverRef.current = false;
    });
  }, [state.tasks.scheme]);

  const feedback = useMemo(() => {
    let tone: ProcessFeedbackTone = 'idle';
    let title = '等待开始方案匹配';
    let description = '选择客户和资料来源后，即可生成融资方案匹配结果。';
    let persistenceHint = '当前不会影响已保存的申请表与资料汇总。';
    let nextStep = '先选定客户，再确认本次匹配使用的资料来源。';
    const isMatching = loading || jobPolling;
    if (isMatching) {
      tone = 'processing';
      title = getJobStatusText('scheme_match', 'running');
      description = '系统正在读取资料并与产品规则进行比对。';
      persistenceHint = jobPolling ? '任务处理中，现有资料与旧结果仍会保留。' : '任务已提交，现有资料与旧结果仍会保留。';
      nextStep = '请稍候，完成后可直接查看结果与依据。';
    } else if (error || actionError) {
      tone = activeResult ? 'partial' : 'error';
      title = activeResult ? '本次匹配失败，但上一版结果仍可查看' : getJobStatusText('scheme_match', 'failed');
      description = actionError || error?.message || '本次匹配未完成，请稍后重试。';
      persistenceHint = activeResult ? '旧结果仍保留，可先继续查看。' : '主流程资料仍已保存，不影响其他页面继续使用。';
      nextStep = activeResult ? '确认资料更新后可重新匹配。' : '先确认客户资料来源是否可用，再重新发起匹配。';
    } else if (schemeResult?.stale) {
      tone = 'partial';
      title = '当前方案结果已被新资料覆盖';
      description = schemeResult.staleReason || '检测到客户资料更新，建议基于最新资料重新匹配。';
      persistenceHint = '旧结果仍可查看，但不建议继续作为最新结论使用。';
      nextStep = '点击“开始匹配方案”，使用最新资料重跑。';
    } else if (activeResult) {
      tone = 'success';
      title = getJobStatusText('scheme_match', 'success');
      description = '当前客户的匹配结果已生成，可继续查看、复制或下载。';
      persistenceHint = '结果已同步到当前客户上下文，可用于后续风险评估。';
      nextStep = '如客户资料更新，建议重新匹配保持结果最新。';
    }
    return { tone, title, description, persistenceHint, nextStep };
  }, [actionError, activeResult, error, loading, jobPolling, schemeResult?.stale, schemeResult?.staleReason]);

  const selectedApplication = applications.find((item) => item.id === selectedApplicationId) || null;

  const parseManual = async () => {
    if (!manualInput.trim()) {
      setManualError('请先输入要解析的客户资料。');
      return;
    }
    setManualError(null);
    const parsed = await parseNaturalLanguage({ text: manualInput.trim(), creditType }, getSignal());
    setManualData(parsed.customerData || {});
    setManualFields(parsed.parsedFields || []);
  };

  const searchNamedCustomer = async () => {
    setActionError(null);
    if (!searchName.trim()) {
      setSearchStatus('请输入客户名称后再搜索。');
      return;
    }
    setSearchStatus('正在搜索客户资料...');
    const result = await searchCustomer({ customerName: searchName.trim() }, getSignal());
    if (!result.found) {
      setSearchDataState(null);
      setSearchStatus('未找到该客户资料，请确认名称或先上传资料。');
      return;
    }
    setSearchDataState(result.customerData || {});
    setSearchStatus(`已找到“${searchName.trim()}”的客户资料，可直接用于匹配。`);
  };

  const resolvePayload = async () => {
    if (dataSource === 'currentCustomer') {
      if (!currentCustomerName) throw new Error('请先选择客户。');
      return { customerData: currentData, customerName: currentCustomerName, customerId: currentCustomerId };
    }
    if (dataSource === 'savedApplication') {
      if (!selectedApplicationId) throw new Error('请先选择一份已保存申请表。');
      const app = await getApplication(selectedApplicationId, getSignal());
      return { customerData: app.applicationData || {}, customerName: app.customerName, customerId: app.customerId ?? null };
    }
    if (dataSource === 'manual') {
      if (!manualData || Object.keys(manualData).length === 0) throw new Error('请先解析手动输入资料。');
      return { customerData: manualData, customerName: currentCustomerName, customerId: currentCustomerId };
    }
    if (!searchDataState || Object.keys(searchDataState).length === 0) throw new Error('请先搜索并确认客户资料。');
    return { customerData: searchDataState, customerName: searchName.trim() || currentCustomerName, customerId: currentCustomerId };
  };

  const startMatch = async () => {
    setError(null);
    setActionError(null);
    try {
      const payload = await resolvePayload();
      await doMatch(creditType, payload.customerData, payload.customerName, payload.customerId);
    } catch (issue) {
      const message = issue instanceof Error ? issue.message : '当前资料暂时无法直接匹配，请检查资料来源后重试。';
      setActionError(message);
      setSchemeTaskStatus('idle', null);
    }
  };

  const handleCancelMatch = () => {
    stopPollingRef.current = true;
    abort();
    setLoading(false);
    setJobPolling(false);
    setActionError('已停止查看当前任务状态，可稍后重新进入页面查看。');
    setError(new Error('已停止查看当前任务状态，可稍后重新进入页面查看。'));
    setSchemeTaskStatus('idle', null);
  };

  const copyResult = async () => {
    if (!activeResult) return;
    await navigator.clipboard.writeText(activeResult);
    setCopySuccess(true);
    window.setTimeout(() => setCopySuccess(false), 1800);
  };

  const scrollToBasis = () => {
    basisRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  return (
    <div className="space-y-6">
      <section className="rounded-[28px] border border-slate-200 bg-gradient-to-br from-white via-slate-50 to-blue-50/60 p-8 shadow-[0_18px_50px_rgba(15,23,42,0.06)]">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
          <div className="max-w-3xl">
            <div className="inline-flex items-center gap-2 rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-xs font-semibold text-blue-700"><Sparkles className="h-3.5 w-3.5" />{schemeJobLabel}</div>
            <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-900">方案匹配</h1>
            <p className="mt-3 text-sm leading-7 text-slate-600">先确定当前客户，再选择匹配所用资料。系统会基于最新客户资料、已保存申请表或手动整理内容输出可执行的融资方案建议。</p>
          </div>
          <div className="grid min-w-[320px] gap-3 rounded-3xl border border-slate-200 bg-white/90 p-4 shadow-sm sm:grid-cols-2">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><div className="text-xs font-medium text-slate-500">当前客户</div><div className="mt-2 text-base font-semibold text-slate-900">{currentCustomerName || '未选择客户'}</div></div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><div className="flex items-center justify-between"><div className="text-xs font-medium text-slate-500">当前状态</div><div className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-medium ${schemeResult?.stale ? 'bg-amber-100 text-amber-700' : activeResult ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-600'}`}>{schemeResult?.stale ? '待重匹配' : activeResult ? '最新结果' : '未生成'}</div></div><div className="mt-2 text-base font-semibold text-slate-900">{schemeResult?.stale ? '待重匹配' : activeResult ? '最新结果' : '未生成'}</div><div className="mt-1 text-xs text-slate-500">最近匹配：{formatTime(schemeResult?.matchedAt)}</div></div>
          </div>
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
          <div className="text-sm font-semibold text-slate-900">当前客户上下文</div>
          <div className="mt-4 grid gap-4 sm:grid-cols-[1fr_auto]">
            <label className="space-y-2"><span className="text-sm font-medium text-slate-700">选择客户</span><select value={currentCustomerId ?? ''} onChange={(e) => { const customer = customers.find((item) => item.record_id === e.target.value); setCurrentCustomer(customer?.name ?? null, customer?.record_id ?? null); }} className="h-11 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100"><option value="">请选择客户</option>{customers.map((customer) => <option key={customer.record_id} value={customer.record_id}>{customer.name}</option>)}</select></label>
            <button type="button" onClick={() => setCurrentCustomer(null, null)} className="mt-7 inline-flex h-11 items-center justify-center gap-2 rounded-2xl border border-slate-200 px-4 text-sm font-medium text-slate-600 transition hover:bg-slate-50"><Trash2 className="h-4 w-4" />清空当前客户</button>
          </div>
          <div className="mt-4 rounded-2xl border border-slate-100 bg-slate-50 p-4 text-sm leading-6 text-slate-600">当前客户会同步影响申请表、方案匹配、资料问答与风险评估。系统会始终围绕你当前选中的客户处理后续结果。</div>
        </div>
        <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm"><div className="text-sm font-semibold text-slate-900">匹配流程</div><div className="mt-4 flex flex-wrap gap-2">{['选择客户', '确认资料来源', '发起匹配', '查看结果'].map((step, index) => <div key={step} className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-600"><span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-white text-[11px] font-semibold text-blue-600">{index + 1}</span>{step}</div>)}</div></div>
      </div>

      <ProcessFeedbackCard tone={feedback.tone} title={feedback.title} description={feedback.description} persistenceHint={feedback.persistenceHint} nextStep={feedback.nextStep} />
      {activeJobCard ? (
        <div className="mb-6">
          <AsyncJobCard
            job={activeJobCard}
            variant="compact"
            isLatestCompleted={activeJobCard.status === 'success'}
            actionLabelOverride={activeJobCard.status === 'success' ? '查看当前结果' : undefined}
            onAction={(job) => handleOpenCurrentSchemeJob(job as ChatJobSummaryResponse)}
          />
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between"><div><div className="text-sm font-semibold text-slate-900">匹配参数</div><div className="mt-1 text-sm text-slate-500">根据资料来源选择适合的匹配输入方式。</div></div>{loading || jobPolling ? <button type="button" onClick={handleCancelMatch} className="inline-flex items-center gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-medium text-rose-700 transition hover:bg-rose-100"><AlertTriangle className="h-4 w-4" />取消本次匹配</button> : null}</div>
          <div className="mt-6 grid gap-4 sm:grid-cols-2">
            <label className="space-y-2"><span className="text-sm font-medium text-slate-700">融资类型</span><select value={creditType} onChange={(e) => setCreditType(e.target.value as CreditType)} className="h-11 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100">{CREDIT_TYPES.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
            <label className="space-y-2"><span className="text-sm font-medium text-slate-700">资料来源</span><select value={dataSource} onChange={(e) => setDataSource(e.target.value as DataSource)} className="h-11 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100">{DATA_SOURCES.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          </div>
          <div className="mt-4 rounded-2xl border border-slate-100 bg-slate-50 p-4 text-sm leading-6 text-slate-600">{DATA_SOURCES.find((item) => item.value === dataSource)?.hint}</div>

          {dataSource === 'currentCustomer' ? <div className="mt-6 rounded-3xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">{currentCustomerName && Object.keys(currentData).length > 0 ? `已就绪，将使用“${currentCustomerName}”的最新资料进行匹配。` : '当前客户暂时没有可直接用于匹配的结构化资料，建议先上传资料或切换其他来源。'}</div> : null}

          {dataSource === 'savedApplication' ? <div className="mt-6 space-y-3 rounded-3xl border border-slate-200 bg-slate-50 p-5"><label className="space-y-2"><span className="text-sm font-semibold text-slate-900">选择已保存申请表</span><select value={selectedApplicationId} onChange={(e) => setSelectedApplicationId(e.target.value)} className="h-11 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100"><option value="">请选择已保存申请表</option>{applications.map((item) => <option key={item.id} value={item.id}>{item.customerName} · {formatApplicationLoanType(item.loanType)} · {formatTime(item.savedAt)}</option>)}</select></label>{selectedApplication ? <div className="rounded-2xl border border-slate-100 bg-white p-4 text-sm leading-6 text-slate-600">当前选择：{selectedApplication.customerName} · {formatApplicationLoanType(selectedApplication.loanType)}，保存时间 {formatTime(selectedApplication.savedAt)}。</div> : null}</div> : null}

          {dataSource === 'manual' ? <div className="mt-6 space-y-4 rounded-3xl border border-slate-200 bg-slate-50 p-5"><textarea value={manualInput} onChange={(e) => setManualInput(e.target.value)} rows={8} placeholder="请输入客户资料摘要，例如企业主营业务、年营收、纳税情况、征信情况、抵押物信息等。" className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm leading-6 text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100" /><div className="flex flex-wrap gap-3"><button type="button" onClick={() => void parseManual()} className="inline-flex items-center gap-2 rounded-2xl border border-blue-200 bg-blue-50 px-4 py-2 text-sm font-medium text-blue-700 transition hover:bg-blue-100"><Search className="h-4 w-4" />先解析手动资料</button>{manualFields.length > 0 ? <div className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700">已识别 {manualFields.length} 个字段</div> : null}</div>{manualError ? <div className="text-sm text-rose-600">{manualError}</div> : null}</div> : null}

          {dataSource === 'searchCustomer' ? <div className="mt-6 space-y-4 rounded-3xl border border-slate-200 bg-slate-50 p-5"><div className="flex flex-col gap-3 sm:flex-row"><input value={searchName} onChange={(e) => setSearchName(e.target.value)} placeholder="请输入客户名称" className="h-11 flex-1 rounded-2xl border border-slate-200 bg-white px-4 text-sm text-slate-700 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100" /><button type="button" onClick={() => void searchNamedCustomer()} className="inline-flex h-11 items-center justify-center gap-2 rounded-2xl border border-blue-200 bg-blue-50 px-4 text-sm font-medium text-blue-700 transition hover:bg-blue-100"><Search className="h-4 w-4" />搜索客户资料</button></div>{searchStatus ? <div className="text-sm text-slate-600">{searchStatus}</div> : null}</div> : null}

          <div className="mt-6 flex flex-wrap gap-3">
            <button type="button" onClick={() => void startMatch()} disabled={loading || jobPolling} className="inline-flex items-center gap-2 rounded-2xl bg-blue-600 px-5 py-3 text-sm font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-60">{loading || jobPolling ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}{loading || jobPolling ? '正在匹配方案...' : '开始匹配方案'}</button>
            <button type="button" onClick={() => { stopPollingRef.current = true; setError(null); setActionError(null); setManualError(null); setSearchStatus(null); setManualData(null); setManualFields([]); setSearchDataState(null); }} className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 px-5 py-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50"><RefreshCw className="h-4 w-4" />重置本页输入</button>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-3xl border border-slate-200 bg-gradient-to-br from-white via-slate-50 to-emerald-50/60 p-6 shadow-sm">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <div className="text-sm font-semibold text-slate-900">结果查看区</div>
                <div className="mt-1 text-sm leading-6 text-slate-500">
                  右侧集中展示当前方案匹配结果、任务反馈和匹配依据，方便你核对本次输出是否基于正确客户和正确资料来源。
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <span className="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-600">
                  当前客户：{schemeResult?.customerName || currentCustomerName || '未选择客户'}
                </span>
                <span className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700">
                  当前任务：{currentResultTaskLabel}
                </span>
              </div>
            </div>
          </div>
          <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
            <div className="flex items-center justify-between"><div><div className="text-sm font-semibold text-slate-900">匹配结果</div><div className="mt-1 text-sm text-slate-500">生成后会同步写入当前客户上下文，供风险评估与资料问答复用。</div></div>{activeResult ? <div className="flex flex-wrap gap-2"><button type="button" onClick={copyResult} className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50">{copySuccess ? '已复制结果' : '复制匹配结果'}</button><button type="button" onClick={scrollToBasis} className="inline-flex items-center gap-2 rounded-2xl border border-blue-200 bg-blue-50 px-4 py-2 text-sm font-medium text-blue-700 transition hover:bg-blue-100">查看匹配依据</button><button type="button" onClick={() => downloadResult(activeResult, schemeResult?.customerName || currentCustomerName || '当前客户')} className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"><Download className="h-4 w-4" />下载结果</button></div> : null}</div>
            {schemeResult?.stale ? <div className="mt-5 rounded-2xl border border-amber-200 bg-amber-50 p-4 text-sm leading-6 text-amber-800">当前方案匹配结果已因资料更新失效。建议使用最新资料重新匹配，避免沿用旧结论。</div> : null}
            {activeResult ? <div className="mt-5 space-y-4"><div className="grid gap-3 sm:grid-cols-3"><div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><div className="flex items-center justify-between"><div className="text-xs font-medium text-slate-500">结果状态</div><div className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-medium ${schemeResult?.stale ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>{schemeResult?.stale ? '待重匹配' : '最新结果'}</div></div><div className="mt-2 text-base font-semibold text-slate-900">{schemeResult?.stale ? '待重匹配' : '最新结果'}</div></div><div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><div className="text-xs font-medium text-slate-500">最近匹配时间</div><div className="mt-2 text-base font-semibold text-slate-900">{formatTime(schemeResult?.matchedAt)}</div></div><div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><div className="text-xs font-medium text-slate-500">适用客户</div><div className="mt-2 text-base font-semibold text-slate-900">{schemeResult?.customerName || currentCustomerName || '当前客户'}</div></div></div><div className="rounded-3xl border border-slate-200 bg-slate-50 p-4"><SchemeMatchingResultCard matchResult={activeResult} matchingData={schemeResult?.matchingData || null} /></div></div> : <div className="mt-5 rounded-3xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center"><div className="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">尚未生成结果</div><div className="mt-4 text-base font-semibold text-slate-800">当前还没有匹配结果</div><div className="mt-2 text-sm leading-6 text-slate-500">选定客户并确认资料来源后，点击“开始匹配方案”即可生成融资建议、适配条件和后续建议。</div></div>}
          </div>
          <div ref={basisRef} className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm"><div className="text-sm font-semibold text-slate-900">匹配依据概览</div><div className="mt-4 space-y-3 text-sm leading-6 text-slate-600"><div className="rounded-2xl border border-slate-100 bg-slate-50 p-4">当前资料来源：<span className="ml-1 font-medium text-slate-900">{DATA_SOURCES.find((item) => item.value === dataSource)?.label}</span></div>{dataSource === 'manual' && manualFields.length > 0 ? <div className="rounded-2xl border border-slate-100 bg-slate-50 p-4">已识别字段：{manualFields.join('、')}</div> : null}{dataSource === 'searchCustomer' && searchName.trim() ? <div className="rounded-2xl border border-slate-100 bg-slate-50 p-4">已命中客户：{searchName.trim()}</div> : null}{dataSource === 'savedApplication' && selectedApplication ? <div className="rounded-2xl border border-slate-100 bg-slate-50 p-4">已使用申请表：{selectedApplication.customerName} · {formatApplicationLoanType(selectedApplication.loanType)}</div> : null}<div className="rounded-2xl border border-slate-100 bg-slate-50 p-4">说明：当前页会优先展示本次匹配使用的资料来源和命中对象，方便你核对结果是否基于正确客户与正确版本生成。</div></div></div>
        </div>
      </div>
    </div>
  );
};

export default SchemeMatchPage;

