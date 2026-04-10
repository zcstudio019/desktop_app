import type { ChatJobStatusResponse, ChatJobSummaryResponse } from '../services/types';

export type SupportedJobType =
  | 'chat_extract'
  | 'risk_report'
  | 'scheme_match'
  | 'application_generate';

type JobDisplayConfig = {
  jobType: SupportedJobType;
  jobTypeLabel: string;
  targetPage: string | null;
  defaultStatusText: {
    pending: string;
    running: string;
    success: string;
    failed: string;
  };
  resultSummary: (result: Record<string, unknown> | null | undefined, customerName?: string | null) => string | null;
  supportsContinueView: boolean;
  supportsViewResult: boolean;
  supportsDirectNavigate: boolean;
  successActionLabel: string;
};

const DEFAULT_JOB_DISPLAY_CONFIG = {
  jobTypeLabel: '处理任务',
  targetPage: null,
  defaultStatusText: {
    pending: '任务已提交',
    running: '正在处理任务',
    success: '任务已完成',
    failed: '任务失败',
  },
  resultSummary: () => null,
  supportsContinueView: true,
  supportsViewResult: true,
  supportsDirectNavigate: false,
  successActionLabel: '查看结果',
} satisfies Omit<JobDisplayConfig, 'jobType'>;

export const JOB_DISPLAY_CONFIG: Record<SupportedJobType, JobDisplayConfig> = {
  chat_extract: {
    jobType: 'chat_extract',
    jobTypeLabel: '资料提取',
    targetPage: 'customerData',
    defaultStatusText: {
      pending: '已接收文件',
      running: '正在提取结构化内容',
      success: '资料提取已完成',
      failed: '资料提取失败',
    },
    resultSummary: () => '资料提取已完成，可查看提取结果并同步到资料汇总。',
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: true,
    successActionLabel: '直接跳资料汇总',
  },
  risk_report: {
    jobType: 'risk_report',
    jobTypeLabel: '风险报告',
    targetPage: 'chat',
    defaultStatusText: {
      pending: '已提交风险报告任务',
      running: '正在生成风险报告',
      success: '风险报告已完成',
      failed: '风险报告失败',
    },
    resultSummary: (result, customerName) => {
      const overall = (result?.report_json as Record<string, unknown> | undefined)?.overall_assessment as Record<string, unknown> | undefined;
      const score = overall?.total_score;
      const label = (customerName || '').trim() || '当前客户';
      return score != null ? `${label}风险报告已生成，综合评分 ${String(score)} 分。` : `${label}风险报告已生成。`;
    },
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: true,
    successActionLabel: '查看风险报告',
  },
  scheme_match: {
    jobType: 'scheme_match',
    jobTypeLabel: '方案匹配',
    targetPage: 'scheme',
    defaultStatusText: {
      pending: '已提交方案匹配任务',
      running: '正在生成融资方案匹配结果',
      success: '方案匹配已完成',
      failed: '方案匹配失败',
    },
    resultSummary: (_, customerName) => `${(customerName || '').trim() || '当前客户'}的融资方案匹配结果已生成。`,
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: true,
    successActionLabel: '跳转方案匹配页',
  },
  application_generate: {
    jobType: 'application_generate',
    jobTypeLabel: '申请表生成',
    targetPage: 'application',
    defaultStatusText: {
      pending: '已提交申请表生成任务',
      running: '正在生成申请表',
      success: '申请表生成已完成',
      failed: '申请表生成失败',
    },
    resultSummary: (_, customerName) => `${(customerName || '').trim() || '当前客户'}的申请表已生成。`,
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: true,
    successActionLabel: '跳转申请表页',
  },
};

export function getJobDisplayConfig(jobType?: string | null) {
  if (!jobType) {
    return DEFAULT_JOB_DISPLAY_CONFIG;
  }
  return JOB_DISPLAY_CONFIG[jobType as SupportedJobType] || DEFAULT_JOB_DISPLAY_CONFIG;
}

export function getJobTypeLabel(jobType?: string | null, fallbackLabel?: string | null) {
  return fallbackLabel || getJobDisplayConfig(jobType).jobTypeLabel;
}

export function getJobTargetPage(jobType?: string | null, targetPage?: string | null) {
  return targetPage || getJobDisplayConfig(jobType).targetPage;
}

export function getJobSuccessAction(jobType?: string | null, targetPage?: string | null) {
  const config = getJobDisplayConfig(jobType);
  const resolvedTargetPage = targetPage || config.targetPage;
  let actionLabel = config.successActionLabel;
  if (resolvedTargetPage === 'chat' && (!jobType || jobType !== 'risk_report')) {
    actionLabel = '查看结果';
  }
  return {
    targetPage: resolvedTargetPage,
    actionLabel,
  };
}

export function getJobStatusText(jobType?: string | null, status?: string | null) {
  const config = getJobDisplayConfig(jobType);
  if (status && status in config.defaultStatusText) {
    return config.defaultStatusText[status as keyof typeof config.defaultStatusText];
  }
  return '正在处理任务';
}

export function getJobResultSummary(
  jobType?: string | null,
  result?: Record<string, unknown> | null,
  customerName?: string | null,
  fallbackSummary?: string | null,
) {
  if (fallbackSummary) {
    return fallbackSummary;
  }
  return getJobDisplayConfig(jobType).resultSummary(result, customerName);
}

export function canContinueViewingJob(jobType?: string | null) {
  return getJobDisplayConfig(jobType).supportsContinueView;
}

export function canViewJobResult(jobType?: string | null) {
  return getJobDisplayConfig(jobType).supportsViewResult;
}

export function canDirectNavigateForJob(jobType?: string | null) {
  return getJobDisplayConfig(jobType).supportsDirectNavigate;
}

export function getReadableJobProgress(
  job: Pick<ChatJobSummaryResponse | ChatJobStatusResponse, 'status' | 'progressMessage' | 'errorMessage' | 'jobType'>,
  isStale = false,
) {
  if (isStale) {
    return '任务可能已中断，请重新提交。';
  }
  if (job.status === 'success') {
    return getJobStatusText(job.jobType, 'success');
  }
  if (job.status === 'failed') {
    return job.errorMessage || getJobStatusText(job.jobType, 'failed');
  }
  if (job.status === 'pending') {
    return getJobStatusText(job.jobType, 'pending');
  }
  return job.progressMessage || getJobStatusText(job.jobType, 'running');
}
