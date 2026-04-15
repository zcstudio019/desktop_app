import { Trash2 } from 'lucide-react';
import type { ChatJobStatusResponse, ChatJobSummaryResponse } from '../../services/types';
import {
  canContinueViewingJob,
  getJobResultSummary,
  getJobSuccessAction,
  getJobTypeLabel,
  getReadableJobProgress,
} from '../../config/jobDisplay';

type AsyncJobLike = ChatJobSummaryResponse | ChatJobStatusResponse;

interface AsyncJobCardProps {
  job: AsyncJobLike;
  isLatestCompleted?: boolean;
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

function getStatusLabel(job: AsyncJobLike) {
  if (isRunningJobStale(job)) {
    return '可能已中断';
  }
  switch (job.status) {
    case 'pending':
      return '排队中';
    case 'running':
      return '处理中';
    case 'retrying':
      return '重试中';
    case 'success':
      return '已完成';
    case 'failed':
      return '已失败';
    default:
      return job.status || '处理中';
  }
}

function getStatusTone(job: AsyncJobLike) {
  if (isRunningJobStale(job)) {
    return 'border-amber-200 bg-amber-50 text-amber-700';
  }
  switch (job.status) {
    case 'pending':
      return 'border-slate-200 bg-slate-50 text-slate-600';
    case 'running':
      return 'border-blue-200 bg-blue-50 text-blue-700';
    case 'retrying':
      return 'border-violet-200 bg-violet-50 text-violet-700';
    case 'success':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700';
    case 'failed':
      return 'border-rose-200 bg-rose-50 text-rose-700';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-600';
  }
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

export default function AsyncJobCard({
  job,
  isLatestCompleted = false,
  onAction,
  onDelete,
  actionLabelOverride,
  className = '',
  variant = 'standard',
}: AsyncJobCardProps) {
  const customerLabel = normalizeCustomerLabel(job.customerName, job.customerId);
  const actionLabel = actionLabelOverride
    || (job.status === 'success'
      ? (isLatestCompleted ? '查看当前结果' : getJobSuccessAction(job.jobType, job.targetPage).actionLabel)
      : job.status === 'failed'
        ? '查看结果'
        : '继续查看');
  const isCompact = variant === 'compact';
  const showDeleteAction = Boolean(onDelete) && (job.status === 'pending' || job.status === 'failed' || isRunningJobStale(job));

  return (
    <div
      className={`rounded-xl border px-4 py-3 shadow-sm ${
        isLatestCompleted
          ? 'border-emerald-200 bg-emerald-50/60'
          : 'border-slate-200 bg-white'
      } ${className}`.trim()}
    >
      <div className={`flex ${isCompact ? 'flex-col gap-2 lg:flex-row lg:items-start lg:justify-between' : 'flex-col gap-3 md:flex-row md:items-start md:justify-between'}`}>
        <div className="space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-[11px] font-medium text-slate-600">
              {getJobTypeLabel(job.jobType, job.jobTypeLabel)}
            </span>
            <span className={`rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${getStatusTone(job)}`}>
              {getStatusLabel(job)}
            </span>
            {isLatestCompleted ? (
              <span className="rounded-full border border-emerald-200 bg-emerald-100 px-2.5 py-0.5 text-[11px] font-medium text-emerald-700">
                当前结果
              </span>
            ) : null}
          </div>
          <div className="text-xs text-slate-500">
            关联客户：{customerLabel}
          </div>
          <div className={`${isCompact ? 'text-xs leading-6' : 'text-sm'} text-slate-700`}>
            {getJobResultSummary(job.jobType, 'result' in job ? job.result : undefined, job.customerName, job.resultSummary) || getReadableJobProgress(job, isRunningJobStale(job))}
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
                  isLatestCompleted
                    ? 'border-emerald-200 bg-emerald-100 text-emerald-700 hover:bg-emerald-200'
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
        <div className="mt-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
          任务可能已中断，请重新提交。
        </div>
      ) : null}
    </div>
  );
}
