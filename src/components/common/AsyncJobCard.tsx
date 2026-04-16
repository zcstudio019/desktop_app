import { AlertCircle, CheckCircle2, Clock3, FileText, Loader2, RefreshCw, Send, Trash2 } from 'lucide-react';
import type { ChatJobStatusResponse, ChatJobSummaryResponse } from '../../services/types';
import {
  canContinueViewingJob,
  getJobResultSummary,
  getJobSuccessAction,
  getJobTypeLabel,
  getReadableJobProgress,
} from '../../config/jobDisplay';
import type { TaskViewSource } from '../../hooks';

type AsyncJobLike = ChatJobSummaryResponse | ChatJobStatusResponse;

interface AsyncJobCardProps {
  job: AsyncJobLike;
  isLatestCompleted?: boolean;
  isActive?: boolean;
  activeSource?: TaskViewSource;
  onAction?: (job: AsyncJobLike) => void;
  onDelete?: (job: AsyncJobLike) => void;
  actionLabelOverride?: string;
  className?: string;
  variant?: 'standard' | 'compact';
}

function formatLocalDateTime(value?: string | null) {
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
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date);
}

function isRunningJobStale(job: Pick<AsyncJobLike, 'status' | 'startedAt' | 'createdAt'>): boolean {
  if (job.status !== 'running' && job.status !== 'retrying') {
    return false;
  }
  const startedAt = job.startedAt || job.createdAt;
  if (!startedAt) {
    return false;
  }
  const startTime = new Date(startedAt).getTime();
  if (Number.isNaN(startTime)) {
    return false;
  }
  return Date.now() - startTime > 10 * 60 * 1000;
}

function normalizeCustomerLabel(customerName?: string | null, customerId?: string | null) {
  const stripInternalId = (value: string) =>
    value
      .replace(/\s*\((enterprise|personal)_[^)]+\)\s*/gi, '')
      .replace(/\b(enterprise|personal)_/gi, '')
      .trim();

  if (customerName && customerName.trim()) {
    return stripInternalId(customerName);
  }

  if (customerId && customerId.trim()) {
    return stripInternalId(customerId);
  }

  return '未选择客户';
}

function getVisualState(job: AsyncJobLike) {
  if (isRunningJobStale(job)) {
    return {
      label: '可能中断',
      icon: AlertCircle,
      iconClassName: 'text-amber-600',
      badgeClassName: 'border-amber-200 bg-amber-50 text-amber-700',
      containerClassName: 'border-amber-200 bg-amber-50/40',
      accentClassName: 'bg-amber-400',
    };
  }

  switch (job.status) {
    case 'pending':
      return {
        label: '排队中',
        icon: Clock3,
        iconClassName: 'text-slate-600',
        badgeClassName: 'border-slate-200 bg-slate-50 text-slate-600',
        containerClassName: 'border-slate-200 bg-white',
        accentClassName: 'bg-slate-300',
      };
    case 'running':
      return {
        label: '处理中',
        icon: Loader2,
        iconClassName: 'text-blue-600 animate-spin',
        badgeClassName: 'border-blue-200 bg-blue-50 text-blue-700',
        containerClassName: 'border-blue-200 bg-blue-50/40',
        accentClassName: 'bg-blue-500',
      };
    case 'retrying':
      return {
        label: '重试中',
        icon: RefreshCw,
        iconClassName: 'text-violet-600 animate-spin',
        badgeClassName: 'border-violet-200 bg-violet-50 text-violet-700',
        containerClassName: 'border-violet-200 bg-violet-50/40',
        accentClassName: 'bg-violet-500',
      };
    case 'success':
      return {
        label: '已完成',
        icon: CheckCircle2,
        iconClassName: 'text-emerald-600',
        badgeClassName: 'border-emerald-200 bg-emerald-50 text-emerald-700',
        containerClassName: 'border-emerald-200 bg-emerald-50/40',
        accentClassName: 'bg-emerald-500',
      };
    case 'failed':
      return {
        label: '已失败',
        icon: AlertCircle,
        iconClassName: 'text-rose-600',
        badgeClassName: 'border-rose-200 bg-rose-50 text-rose-700',
        containerClassName: 'border-rose-200 bg-rose-50/40',
        accentClassName: 'bg-rose-500',
      };
    case 'timeout':
      return {
        label: '已超时',
        icon: AlertCircle,
        iconClassName: 'text-amber-600',
        badgeClassName: 'border-amber-200 bg-amber-50 text-amber-700',
        containerClassName: 'border-amber-200 bg-amber-50/40',
        accentClassName: 'bg-amber-500',
      };
    case 'interrupted':
      return {
        label: '已中断',
        icon: AlertCircle,
        iconClassName: 'text-amber-600',
        badgeClassName: 'border-amber-200 bg-amber-50 text-amber-700',
        containerClassName: 'border-amber-200 bg-amber-50/40',
        accentClassName: 'bg-amber-500',
      };
    default:
      return {
        label: job.status || '处理中',
        icon: Clock3,
        iconClassName: 'text-slate-600',
        badgeClassName: 'border-slate-200 bg-slate-50 text-slate-600',
        containerClassName: 'border-slate-200 bg-white',
        accentClassName: 'bg-slate-300',
      };
  }
}

function getActiveSourceMeta(source: TaskViewSource) {
  switch (source) {
    case 'manual':
      return {
        label: '历史任务查看',
        icon: FileText,
        className: 'border-blue-200 bg-blue-100 text-blue-700',
        iconClassName: 'text-blue-600',
      };
    case 'auto':
      return {
        label: '当前新任务',
        icon: Send,
        className: 'border-indigo-200 bg-indigo-100 text-indigo-700',
        iconClassName: 'text-indigo-600',
      };
    default:
      return {
        label: '当前选中',
        icon: FileText,
        className: 'border-blue-200 bg-blue-100 text-blue-700',
        iconClassName: 'text-blue-600',
      };
  }
}

export default function AsyncJobCard({
  job,
  isLatestCompleted = false,
  isActive = false,
  activeSource = 'none',
  onAction,
  onDelete,
  actionLabelOverride,
  className = '',
  variant = 'standard',
}: AsyncJobCardProps) {
  const customerLabel = normalizeCustomerLabel(job.customerName, job.customerId);
  const visual = getVisualState(job);
  const StatusIcon = visual.icon;
  const activeSourceMeta = getActiveSourceMeta(activeSource);
  const ActiveSourceIcon = activeSourceMeta.icon;
  const actionLabel = actionLabelOverride
    || (job.status === 'success'
      ? (isLatestCompleted ? '查看当前结果' : getJobSuccessAction(job.jobType, job.targetPage).actionLabel)
      : job.status === 'failed' || job.status === 'timeout' || job.status === 'interrupted'
        ? '查看结果'
        : '继续查看');
  const isCompact = variant === 'compact';
  const showDeleteAction = Boolean(onDelete) && (job.status === 'pending' || job.status === 'failed' || isRunningJobStale(job));
  const summary = getJobResultSummary(
    job.jobType,
    'result' in job ? job.result : undefined,
    job.customerName,
    job.resultSummary,
  ) || getReadableJobProgress(job, isRunningJobStale(job));

  return (
    <div
      className={`relative overflow-hidden rounded-2xl border px-4 py-3 shadow-sm transition-all ${
        isActive
          ? 'ring-2 ring-blue-200 shadow-md'
          : isLatestCompleted
            ? 'ring-1 ring-emerald-100'
            : 'hover:-translate-y-0.5 hover:shadow-md'
      } ${visual.containerClassName} ${className}`.trim()}
    >
      <div className={`absolute inset-y-0 left-0 w-1 ${isActive ? 'bg-blue-500' : visual.accentClassName}`} />
      <div className={`pl-2 flex ${isCompact ? 'flex-col gap-2 lg:flex-row lg:items-start lg:justify-between' : 'flex-col gap-3 md:flex-row md:items-start md:justify-between'}`}>
        <div className="space-y-1.5">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-slate-200 bg-white/80 px-2.5 py-0.5 text-[11px] font-medium text-slate-600">
              {getJobTypeLabel(job.jobType, job.jobTypeLabel)}
            </span>
            <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${visual.badgeClassName}`}>
              <StatusIcon className={`h-3.5 w-3.5 ${visual.iconClassName}`} />
              {visual.label}
            </span>
            {isActive ? (
              <span className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${activeSourceMeta.className}`}>
                <ActiveSourceIcon className={`h-3.5 w-3.5 ${activeSourceMeta.iconClassName}`} />
                {activeSourceMeta.label}
              </span>
            ) : null}
            {!isActive && isLatestCompleted ? (
              <span className="rounded-full border border-emerald-200 bg-emerald-100 px-2.5 py-0.5 text-[11px] font-medium text-emerald-700">
                当前结果
              </span>
            ) : null}
          </div>
          <div className="text-xs text-slate-500">
            关联客户：{customerLabel}
          </div>
          <div className={`${isCompact ? 'text-xs leading-6' : 'text-sm leading-6'} text-slate-700`}>
            {summary}
          </div>
          <div className={`flex flex-wrap ${isCompact ? 'gap-x-3' : 'gap-x-4'} gap-y-1 text-xs text-slate-500`}>
            <span>创建时间：{formatLocalDateTime(job.createdAt)}</span>
            {job.startedAt ? <span>开始时间：{formatLocalDateTime(job.startedAt)}</span> : null}
            {job.finishedAt ? <span>完成时间：{formatLocalDateTime(job.finishedAt)}</span> : null}
          </div>
          {job.errorMessage ? (
            <div className="text-xs text-rose-600">{job.errorMessage}</div>
          ) : null}
        </div>
        {((canContinueViewingJob(job.jobType) && onAction) || showDeleteAction) ? (
          <div className={`flex shrink-0 ${isCompact ? 'justify-start lg:justify-end' : ''} gap-2`}>
            {showDeleteAction ? (
              <button
                type="button"
                onClick={() => onDelete?.(job)}
                className={`rounded-lg border border-rose-200 bg-rose-50 ${isCompact ? 'px-2.5 py-1.5' : 'px-3 py-1.5'} text-xs font-medium text-rose-700 transition-colors hover:bg-rose-100`}
                title="删除任务"
              >
                <span className="flex items-center gap-1.5">
                  <Trash2 className="h-3.5 w-3.5" />
                  删除
                </span>
              </button>
            ) : null}
            {canContinueViewingJob(job.jobType) && onAction ? (
              <button
                type="button"
                onClick={() => onAction(job)}
                className={`rounded-lg border ${isCompact ? 'px-2.5 py-1.5' : 'px-3 py-1.5'} text-xs font-medium transition-colors ${
                  isActive
                    ? 'border-blue-200 bg-blue-100 text-blue-700 hover:bg-blue-200'
                    : job.status === 'success'
                      ? 'border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                      : 'border-blue-200 bg-blue-50 text-blue-700 hover:bg-blue-100'
                }`}
              >
                {actionLabel}
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
      {isRunningJobStale(job) ? (
        <div className="mt-3 ml-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
          任务可能已中断，请重新提交。
        </div>
      ) : null}
    </div>
  );
}
