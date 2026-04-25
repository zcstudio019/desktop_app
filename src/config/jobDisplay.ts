import type { ChatJobStatusResponse, ChatJobSummaryResponse } from '../services/types';

export type SupportedJobType =
  | 'file_process'
  | 'chat_extract'
  | 'risk_report'
  | 'scheme_match'
  | 'application_generate';

type JobStatusText = {
  pending: string;
  running: string;
  retrying: string;
  success: string;
  failed: string;
  timeout?: string;
  interrupted?: string;
};

type JobDisplayConfig = {
  jobType: SupportedJobType;
  jobTypeLabel: string;
  targetPage: string | null;
  defaultStatusText: JobStatusText;
  resultSummary: (
    result: Record<string, unknown> | null | undefined,
    customerName?: string | null,
  ) => string | null;
  supportsContinueView: boolean;
  supportsViewResult: boolean;
  supportsDirectNavigate: boolean;
  successActionLabel: string;
};

const DEFAULT_JOB_DISPLAY_CONFIG: Omit<JobDisplayConfig, 'jobType'> = {
  jobTypeLabel: '处理任务',
  targetPage: null,
  defaultStatusText: {
    pending: '任务已提交',
    running: '正在处理任务',
    retrying: '系统正在自动重试',
    success: '任务已完成',
    failed: '任务失败',
    timeout: '任务执行超时',
    interrupted: '任务可能已中断',
  },
  resultSummary: () => null,
  supportsContinueView: true,
  supportsViewResult: true,
  supportsDirectNavigate: false,
  successActionLabel: '查看结果',
};

export const JOB_DISPLAY_CONFIG: Record<SupportedJobType, JobDisplayConfig> = {
  file_process: {
    jobType: 'file_process',
    jobTypeLabel: '上传资料',
    targetPage: 'upload',
    defaultStatusText: {
      pending: '文件已接收，等待处理',
      running: '正在后台处理资料',
      retrying: '上传处理暂时受阻，系统正在自动重试',
      success: '资料上传处理已完成',
      failed: '资料上传处理失败',
      timeout: '资料上传处理超时',
      interrupted: '资料上传处理可能已中断',
    },
    resultSummary: (_result, customerName) => `${(customerName || '').trim() || '当前客户'}的资料上传与处理已完成。`,
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: false,
    successActionLabel: '查看处理结果',
  },
  chat_extract: {
    jobType: 'chat_extract',
    jobTypeLabel: '资料提取',
    targetPage: 'customerData',
    defaultStatusText: {
      pending: '已接收文件',
      running: '正在提取结构化内容',
      retrying: '资料提取暂时受阻，系统正在自动重试',
      success: '资料提取已完成',
      failed: '资料提取失败',
      timeout: '资料提取超时',
      interrupted: '资料提取可能已中断',
    },
    resultSummary: () => '资料提取已完成，可查看提取结果并同步到资料汇总。',
    supportsContinueView: true,
    supportsViewResult: true,
    supportsDirectNavigate: true,
    successActionLabel: '直接跳转资料汇总',
  },
  risk_report: {
    jobType: 'risk_report',
    jobTypeLabel: '风险报告',
    targetPage: 'chat',
    defaultStatusText: {
      pending: '已提交风险报告任务',
      running: '正在生成风险评估报告',
      retrying: '风险报告生成暂时受阻，系统正在自动重试',
      success: '风险报告已完成',
      failed: '风险报告失败',
      timeout: '风险报告生成超时',
      interrupted: '风险报告生成可能已中断',
    },
    resultSummary: (result, customerName) => {
      const overall = (result?.report_json as Record<string, unknown> | undefined)?.overall_assessment as
        | Record<string, unknown>
        | undefined;
      const score = overall?.total_score;
      const label = (customerName || '').trim() || '当前客户';
      return score != null
        ? `${label}风险报告已生成，综合评分 ${String(score)} 分。`
        : `${label}风险报告已生成。`;
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
      retrying: '方案匹配暂时受阻，系统正在自动重试',
      success: '方案匹配已完成',
      failed: '方案匹配失败',
      timeout: '方案匹配超时',
      interrupted: '方案匹配可能已中断',
    },
    resultSummary: (_result, customerName) => `${(customerName || '').trim() || '当前客户'}的融资方案匹配结果已生成。`,
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
      retrying: '申请表生成暂时受阻，系统正在自动重试',
      success: '申请表生成已完成',
      failed: '申请表生成失败',
      timeout: '申请表生成超时',
      interrupted: '申请表生成可能已中断',
    },
    resultSummary: (_result, customerName) => `${(customerName || '').trim() || '当前客户'}的申请表已生成。`,
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
  if (status === 'timeout') {
    return config.defaultStatusText.timeout || DEFAULT_JOB_DISPLAY_CONFIG.defaultStatusText.timeout || '任务执行超时';
  }
  if (status === 'interrupted') {
    return config.defaultStatusText.interrupted || DEFAULT_JOB_DISPLAY_CONFIG.defaultStatusText.interrupted || '任务可能已中断';
  }
  if (status && status in config.defaultStatusText) {
    return config.defaultStatusText[status as keyof JobStatusText] || DEFAULT_JOB_DISPLAY_CONFIG.defaultStatusText.running;
  }
  return DEFAULT_JOB_DISPLAY_CONFIG.defaultStatusText.running;
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

export function formatJobErrorMessage(jobType?: string | null, errorMessage?: string | null) {
  const rawMessage = (errorMessage || '').trim();
  if (!rawMessage) {
    return getJobStatusText(jobType, 'failed');
  }

  const normalized = rawMessage.toLowerCase();

  if (normalized.includes('incorrect string value') || normalized.includes('pymysql.err.dataerror')) {
    return '任务结果保存失败，可能包含当前数据库暂不支持的特殊字符。请联系管理员检查数据库字符集后重试。';
  }

  if (normalized.includes('duplicate entry')) {
    return '任务保存失败，检测到重复记录，请刷新后重试。';
  }

  if (normalized.includes('lock wait timeout') || normalized.includes('deadlock')) {
    return '任务保存失败，数据库当前较忙，请稍后重试。';
  }

  if (normalized.includes('timeout')) {
    return getJobStatusText(jobType, 'timeout');
  }

  if (normalized.includes('interrupted') || normalized.includes('stale')) {
    return getJobStatusText(jobType, 'interrupted');
  }

  if (normalized.includes('connection refused') || normalized.includes('redis') || normalized.includes('broker')) {
    return '后台任务服务暂时不可用，请稍后重试。';
  }

  if (normalized.includes('invalid token') || normalized.includes('unauthorized')) {
    return '当前登录状态已失效，请重新登录后重试。';
  }

  return rawMessage;
}

export function hasUsableJobResult(jobType: string, result: Record<string, unknown> | null | undefined) {
  if (!result || typeof result !== 'object') {
    return false;
  }

  if (jobType === 'scheme_match') {
    return typeof (result as { matchResult?: unknown }).matchResult === 'string'
      && ((result as { matchResult?: string }).matchResult || '').trim().length > 0;
  }

  if (jobType === 'application_generate') {
    return typeof (result as { applicationContent?: unknown }).applicationContent === 'string'
      && ((result as { applicationContent?: string }).applicationContent || '').trim().length > 0;
  }

  if (jobType === 'risk_report') {
    return Boolean((result as { report_json?: unknown }).report_json);
  }

  if (jobType === 'chat_extract') {
    return typeof (result as { message?: unknown }).message === 'string'
      || Boolean((result as { data?: unknown }).data);
  }

  return Object.keys(result).length > 0;
}

export function normalizeJobStatusResponse(jobStatus: ChatJobStatusResponse): ChatJobStatusResponse {
  const shouldPromoteToSuccess = hasUsableJobResult(jobStatus.jobType, jobStatus.result) && jobStatus.status !== 'success';
  if (!shouldPromoteToSuccess) {
    return jobStatus;
  }

  return {
    ...jobStatus,
    status: 'success',
    errorMessage: null,
    progressMessage: jobStatus.progressMessage || getJobStatusText(jobStatus.jobType, 'success'),
  };
}

export function normalizeJobSummaryWithResult(
  job: ChatJobSummaryResponse,
  result: Record<string, unknown> | null | undefined,
): ChatJobSummaryResponse {
  if (!hasUsableJobResult(job.jobType, result) || job.status === 'success') {
    return job;
  }

  return {
    ...job,
    status: 'success',
    errorMessage: null,
    progressMessage: job.progressMessage || getJobStatusText(job.jobType, 'success'),
  };
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
    return formatJobErrorMessage(job.jobType, job.errorMessage);
  }
  if (job.status === 'timeout') {
    return formatJobErrorMessage(job.jobType, job.errorMessage || getJobStatusText(job.jobType, 'timeout'));
  }
  if (job.status === 'interrupted') {
    return formatJobErrorMessage(job.jobType, job.errorMessage || getJobStatusText(job.jobType, 'interrupted'));
  }
  if (job.status === 'retrying') {
    return job.progressMessage || getJobStatusText(job.jobType, 'retrying');
  }
  if (job.status === 'pending') {
    return getJobStatusText(job.jobType, 'pending');
  }
  return job.progressMessage || getJobStatusText(job.jobType, 'running');
}
