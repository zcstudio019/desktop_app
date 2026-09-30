/**
 * API client for frontend-backend integration.
 *
 * This module provides typed wrappers for all backend API endpoints.
 */

import type {
  ApplicationRequest,
  ApplicationResponse,
  ChatRequest,
  ChatJobCreateResponse,
  ChatJobSummaryResponse,
  ChatJobStatusResponse,
  ChatResponse,
  CustomerDetail,
  CustomerDocumentListItem,
  CustomerFinancingDiagnosticReport,
  FinancingDiagnosticReportSnapshotDetail,
  FinancingDiagnosticReportSnapshotSummary,
  CustomerKycProfileResponse,
  DocumentDetailResponse,
  ExtractionReviewResult,
  FinancingKycDiagnosticResult,
  CustomerListItem,
  CustomerProfileMarkdownResponse,
  CustomerRagChatRequest,
  CustomerRagChatResponse,
  CustomerRiskReportHistoryResponse,
  CustomerRiskReportResponse,
  CustomerTableRow,
  ExtractionGroup,
  FileProcessResponse,
  LoginResponse,
  RegisterResponse,
  SchemeMatchRequest,
  SchemeMatchResponse,
  StorageSaveRequest,
  StorageSaveResponse,
  TableField,
  UpdateCustomerProfileMarkdownRequest,
  UpdateCurrentUserProfileRequest,
  UpdateExtractionReviewPayload,
  SaveFinancingDiagnosticReportSnapshotResponse,
  UserInfo,
  ChangeCurrentUserPasswordRequest,
  SetCurrentUserSecurityQuestionRequest,
} from './types';

import { ApiError } from './types';

export { ApiError };

function resolveApiBase(): string {
  const base = import.meta.env?.VITE_API_BASE?.trim();
  if (base) return base.replace(/\/+$/, '');

  if (typeof window !== 'undefined' && window.location) {
    const { hostname, port, origin } = window.location;
    const isLocalDevServer =
      (hostname === '127.0.0.1' || hostname === 'localhost') && (port === '5173' || port === '5174');

    if (isLocalDevServer) {
      return 'http://127.0.0.1:8000';
    }

    return origin.replace(/\/+$/, '');
  }

  return '';
}

function resolveDirectJobApiBase(): string {
  const base = import.meta.env?.VITE_DIRECT_JOB_API_BASE?.trim();
  if (base) return base.replace(/\/+$/, '');

  if (typeof window !== 'undefined' && window.location) {
    const { hostname, port, origin } = window.location;
    const isLocalDevServer =
      (hostname === '127.0.0.1' || hostname === 'localhost') && (port === '5173' || port === '5174');

    if (isLocalDevServer) {
      return 'http://127.0.0.1:8000/api';
    }

    return `${origin.replace(/\/+$/, '')}/api`;
  }

  return '/api';
}

const API_BASE = resolveApiBase();
const DIRECT_JOB_API_BASE = resolveDirectJobApiBase();
export const FILE_JOB_CREATE_TIMEOUT_MS = 120000;
const LARGE_FILE_JOB_CREATE_TIMEOUT_MS = 15 * 60 * 1000;
const LARGE_UPLOAD_THRESHOLD_BYTES = 50 * 1024 * 1024;

function getFileJobCreateTimeoutMs(file: File): number {
  return file.size > LARGE_UPLOAD_THRESHOLD_BYTES
    ? LARGE_FILE_JOB_CREATE_TIMEOUT_MS
    : FILE_JOB_CREATE_TIMEOUT_MS;
}

function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  if (!API_BASE) return normalizedPath;
  const normalizedBase = API_BASE.replace(/\/+$/, '');
  if (normalizedBase.endsWith('/api') && normalizedPath.startsWith('/api/')) {
    return `${normalizedBase}${normalizedPath.slice('/api'.length)}`;
  }
  return `${normalizedBase}${normalizedPath}`;
}

function getAuthHeaders(): Record<string, string> {
  const token = localStorage.getItem('auth_token');
  if (token) {
    return { Authorization: `Bearer ${token}` };
  }
  return {};
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let errorMessage = 'Request failed';
    let errorDetails: unknown;

    try {
      const errorBody = await response.json();
      errorMessage = errorBody.error || errorBody.detail || errorBody.message || errorMessage;
      errorDetails = errorBody;
    } catch {
      errorMessage = response.statusText || errorMessage;
    }

    throw new ApiError(response.status, errorMessage, errorDetails);
  }

  return response.json();
}

export async function processFile(
  file: File,
  documentType?: string,
  signal?: AbortSignal
): Promise<FileProcessResponse> {
  const formData = new FormData();
  formData.append('file', file);
  if (documentType) {
    formData.append('documentType', documentType);
  }
  const authHeaders = getAuthHeaders();
  const requestInit: RequestInit = {
    method: 'POST',
    body: formData,
    signal,
  };
  if (Object.keys(authHeaders).length > 0) {
    requestInit.headers = authHeaders;
  }

  const response = await fetch(`${API_BASE}/api/file/process`, requestInit);
  return handleResponse<FileProcessResponse>(response);
}

export function normalizeJobId(payload: unknown): string {
  if (!payload || typeof payload !== 'object') return '';
  const record = payload as Record<string, unknown>;
  const nested = record.data && typeof record.data === 'object' ? record.data as Record<string, unknown> : {};
  const candidate = record.job_id ?? record.jobId ?? record.id ?? nested.job_id ?? nested.jobId;
  return typeof candidate === 'string' ? candidate.trim() : '';
}

export async function createFileProcessJob(
  file: File,
  options?: {
    documentType?: string;
    customerId?: string | null;
    customerName?: string | null;
  },
  signal?: AbortSignal,
  onUploadProgress?: (percent: number) => void,
): Promise<ChatJobCreateResponse> {
  const formData = new FormData();
  formData.append('file', file);
  if (options?.documentType) {
    formData.append('documentType', options.documentType);
    formData.append('document_type', options.documentType);
    formData.append('doc_type', options.documentType);
  }
  if (options?.customerId) {
    formData.append('customerId', options.customerId);
  }
  if (options?.customerName) {
    formData.append('customerName', options.customerName);
  }
  const authHeaders = getAuthHeaders();
  const requestInit: RequestInit = {
    method: 'POST',
    body: formData,
  };
  if (Object.keys(authHeaders).length > 0) {
    requestInit.headers = authHeaders;
  }
  const requestUrl = buildApiUrl('/api/file/process/jobs');
  const formDataKeys = Array.from(formData.keys());
  console.info('[UploadJob] create request', {
    url: requestUrl,
    method: requestInit.method,
    formDataKeys,
    customerId: options?.customerId || '',
    customerName: options?.customerName || '',
    documentType: options?.documentType || '',
    filename: file.name,
  });
  if (
    requestUrl.includes('undefined') ||
    requestUrl.endsWith('/file/process/jobs') ||
    requestUrl.includes('/api/file/process/job?') ||
    requestUrl.includes('/api/files/process/jobs')
  ) {
    console.error('[UploadJob] unexpected create request url', requestUrl);
  }
  const uploadResponse = await new Promise<{ status: number; statusText: string; responseText: string }>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', requestUrl);
    xhr.timeout = getFileJobCreateTimeoutMs(file);
    Object.entries(authHeaders).forEach(([key, value]) => xhr.setRequestHeader(key, value));
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && event.total > 0) {
        onUploadProgress?.(Math.min(100, Math.round((event.loaded / event.total) * 100)));
      }
    };
    xhr.onload = () => resolve({ status: xhr.status, statusText: xhr.statusText, responseText: xhr.responseText });
    xhr.onerror = () => reject(new Error('上传任务请求未到达后端或后端未响应，请检查 Nginx/接口日志。'));
    xhr.ontimeout = () => reject(new Error(`文件上传超时：${file.name}。大文件上传已等待 ${Math.round(xhr.timeout / 60000)} 分钟，请检查网络后重试`));
    xhr.onabort = () => reject(new DOMException('Upload aborted', 'AbortError'));
    const abortListener = () => xhr.abort();
    signal?.addEventListener('abort', abortListener, { once: true });
    xhr.onloadend = () => signal?.removeEventListener('abort', abortListener);
    xhr.send(formData);
  });
  const responseText = uploadResponse.responseText;
  let parsedBody: unknown = null;
  try {
    parsedBody = responseText ? JSON.parse(responseText) : null;
  } catch {
    parsedBody = null;
  }
  const logPayload = {
    status: uploadResponse.status,
    ok: uploadResponse.status >= 200 && uploadResponse.status < 300,
    body: parsedBody ?? responseText,
  };
  if (logPayload.ok) {
    console.debug('[UploadJob] create response', logPayload);
  } else {
    console.error('[UploadJob] create failed response', logPayload);
  }
  if (!logPayload.ok) {
    const errorBody = parsedBody && typeof parsedBody === 'object' ? parsedBody as Record<string, unknown> : {};
    const fallbackMessage = uploadResponse.status === 413
      ? '文件被上传网关拒绝，请确认 Nginx client_max_body_size 已更新并重新加载'
      : '上传任务创建失败';
    const message = String(errorBody.error_message || errorBody.detail || errorBody.message || uploadResponse.statusText || fallbackMessage);
    throw new ApiError(uploadResponse.status, message, parsedBody ?? responseText);
  }
  const created = (parsedBody || {}) as ChatJobCreateResponse;
  const jobId = normalizeJobId(created);
  if (created.success !== true || !jobId) {
    console.error('[UploadJob] create response missing job_id', created);
    throw new Error(created.error_message || '上传任务创建失败，后端未返回 success=true 和有效 job_id');
  }
  return { ...created, jobId };
}

export async function getFileProcessJob(
  jobId: string,
  signal?: AbortSignal
): Promise<ChatJobStatusResponse> {
  const response = await fetch(`${API_BASE}/api/file/process/jobs/${jobId}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ChatJobStatusResponse>(response);
}

export async function createShuimuiReportUrlExtractJob(
  customerId: string,
  url: string,
  signal?: AbortSignal
): Promise<ChatJobCreateResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/documents/extract-url`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ url, doc_type: 'shuimui_report' }),
    signal,
  });
  const payload = await handleResponse<ChatJobCreateResponse & { job_id?: string; message?: string }>(response);
  const jobId = normalizeJobId(payload);
  if (!jobId) {
    throw new Error('水母报告链接提取任务创建失败，后端返回格式异常');
  }
  return { ...payload, jobId };
}

export async function getShuimuiReportUrlExtractJob(
  customerId: string,
  jobId: string,
  signal?: AbortSignal
): Promise<ChatJobStatusResponse> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/documents/extract-url/jobs/${encodeURIComponent(jobId)}`,
    {
      method: 'GET',
      headers: { ...getAuthHeaders() },
      signal,
    },
  );
  return handleResponse<ChatJobStatusResponse>(response);
}

export async function saveToStorage(
  request: StorageSaveRequest,
  signal?: AbortSignal
): Promise<StorageSaveResponse> {
  const response = await fetch(`${API_BASE}/api/storage/save`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<StorageSaveResponse>(response);
}

// Legacy alias kept for compatibility with existing imports.
export const saveToFeishu = saveToStorage;

export async function getDocumentDetail(
  documentId: string,
  signal?: AbortSignal
): Promise<DocumentDetailResponse> {
  const response = await fetch(`${API_BASE}/api/documents/${documentId}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<DocumentDetailResponse>(response);
}

async function fetchDocumentBlob(
  documentId: string,
  mode: 'download' | 'preview',
  signal?: AbortSignal
): Promise<{ blob: Blob; fileName: string }> {
  const response = await fetch(`${API_BASE}/api/documents/${documentId}/${mode}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });

  if (!response.ok) {
    let errorMessage = 'Request failed';
    try {
      const errorBody = await response.json();
      errorMessage = errorBody.error || errorBody.detail || errorBody.message || errorMessage;
    } catch {
      errorMessage = response.statusText || errorMessage;
    }
    throw new ApiError(response.status, errorMessage);
  }

  const contentDisposition = response.headers.get('Content-Disposition') || '';
  const match = /filename="?([^"]+)"?/i.exec(contentDisposition);
  const fileName = match?.[1] || 'document';
  const blob = await response.blob();
  return { blob, fileName };
}

function parseDownloadFileName(contentDisposition: string, fallback: string): string {
  const encodedMatch = /filename\*=UTF-8''([^;]+)/i.exec(contentDisposition);
  if (encodedMatch?.[1]) {
    try {
      return decodeURIComponent(encodedMatch[1]);
    } catch {
      return encodedMatch[1];
    }
  }
  const match = /filename="?([^"]+)"?/i.exec(contentDisposition);
  return match?.[1] || fallback;
}

async function fetchSnapshotExportBlob(
  customerId: string,
  reportId: string,
  format: 'docx' | 'pdf',
  signal?: AbortSignal
): Promise<{ blob: Blob; fileName: string }> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-diagnostic-report/snapshots/${encodeURIComponent(reportId)}/export/${format}`,
    {
      method: 'GET',
      headers: { ...getAuthHeaders() },
      signal,
    }
  );

  if (!response.ok) {
    let errorMessage = format === 'pdf' ? 'PDF 导出暂不可用，请先导出 Word' : '报告导出失败';
    try {
      const errorBody = await response.json();
      errorMessage = errorBody.error || errorBody.detail || errorBody.message || errorMessage;
    } catch {
      errorMessage = response.statusText || errorMessage;
    }
    if (format === 'pdf' && errorMessage.includes('PDF 导出依赖未配置')) {
      errorMessage = 'PDF 导出暂不可用，请先导出 Word';
    }
    throw new ApiError(response.status, errorMessage);
  }

  const contentDisposition = response.headers.get('Content-Disposition') || '';
  const fileName = parseDownloadFileName(contentDisposition, `融资诊断报告.${format}`);
  const blob = await response.blob();
  return { blob, fileName };
}

function downloadBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export async function downloadDocumentOriginal(documentId: string, signal?: AbortSignal): Promise<void> {
  const { blob, fileName } = await fetchDocumentBlob(documentId, 'download', signal);
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export async function previewDocumentOriginal(documentId: string, signal?: AbortSignal): Promise<void> {
  const { blob } = await fetchDocumentBlob(documentId, 'preview', signal);
  const url = URL.createObjectURL(blob);
  window.open(url, '_blank', 'noopener,noreferrer');
  window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

export async function getDocumentExtractionReview(
  documentId: string,
  signal?: AbortSignal
): Promise<ExtractionReviewResult> {
  const response = await fetch(`${API_BASE}/api/documents/${encodeURIComponent(documentId)}/extraction-review`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ExtractionReviewResult>(response);
}

export async function updateDocumentExtractionReview(
  documentId: string,
  payload: UpdateExtractionReviewPayload,
  signal?: AbortSignal
): Promise<ExtractionReviewResult> {
  const response = await fetch(`${API_BASE}/api/documents/${encodeURIComponent(documentId)}/extraction-review`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(payload),
    signal,
  });
  return handleResponse<ExtractionReviewResult>(response);
}

export async function generateApplication(
  request: ApplicationRequest,
  signal?: AbortSignal
): Promise<ApplicationResponse> {
  const response = await fetch(`${API_BASE}/api/application/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<ApplicationResponse>(response);
}

export async function createApplicationJob(
  request: ApplicationRequest,
  signal?: AbortSignal
): Promise<ChatJobCreateResponse> {
  const response = await fetch(`${API_BASE}/api/application/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<ChatJobCreateResponse>(response);
}

export async function matchScheme(
  request: SchemeMatchRequest,
  signal?: AbortSignal
): Promise<SchemeMatchResponse> {
  const response = await fetch(`${API_BASE}/api/scheme/match`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<SchemeMatchResponse>(response);
}

export async function createSchemeMatchJob(
  request: SchemeMatchRequest,
  signal?: AbortSignal
): Promise<ChatJobCreateResponse> {
  const response = await fetch(`${API_BASE}/api/scheme/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<ChatJobCreateResponse>(response);
}

export async function sendChat(
  request: ChatRequest,
  signal?: AbortSignal
): Promise<ChatResponse> {
  const response = await fetch(`${API_BASE}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<ChatResponse>(response);
}

export interface FinancingRequirementData {
  requirement_id: string;
  customer_id: string;
  version: number;
  status: 'draft' | 'needs_confirmation' | 'confirmed' | 'superseded' | 'cancelled';
  draft_source?: 'chat_user_input' | 'manual_form' | 'application_form' | 'production_regression' | 'migration' | 'system_import' | null;
  borrower_entity: string | null;
  requested_amount: number | null;
  amount_confirmed: boolean;
  financing_purpose: string | null;
  purpose_detail: string | null;
  term_value: number | null;
  term_unit: 'day' | 'month' | 'year' | null;
  term_confirmed: boolean;
  [key: string]: unknown;
}

export async function getCurrentFinancingRequirement(customerId: string): Promise<FinancingRequirementData | null> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-requirements/current`, {
    headers: getAuthHeaders(),
  });
  return (await handleResponse<{ requirement: FinancingRequirementData | null }>(response)).requirement;
}

export async function getPendingFinancingRequirement(customerId: string): Promise<FinancingRequirementData | null> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-requirements/pending`, {
    headers: getAuthHeaders(),
  });
  return (await handleResponse<{ requirement: FinancingRequirementData | null }>(response)).requirement;
}

export async function createFinancingRequirementDraft(customerId: string, patch: Record<string, unknown>): Promise<FinancingRequirementData> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-requirements/draft`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(patch),
  });
  return (await handleResponse<{ requirement: FinancingRequirementData }>(response)).requirement;
}

export async function confirmFinancingRequirement(customerId: string, requirementId: string): Promise<FinancingRequirementData> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-requirements/${encodeURIComponent(requirementId)}/confirm`, {
    method: 'POST', headers: getAuthHeaders(),
  });
  return (await handleResponse<{ requirement: FinancingRequirementData }>(response)).requirement;
}

export type ProductMatchingStatus = 'eligible' | 'conditional' | 'ineligible' | 'manual_review' | 'product_configuration_error';

export interface ProductMatchRuleResult {
  rule_id: string; rule_source: 'product_rule' | 'system_builtin'; field_name: string;
  result: 'passed' | 'failed' | 'unknown' | 'review' | null; explanation: string;
}

export interface ProductMatchItemData {
  product_match_item_id?: number;
  product_id: string; version_id: string; external_product_code: string;
  institution_name: string; product_name: string; product_category: string;
  max_amount: string | null; max_term_months: number | null; overall_status: ProductMatchingStatus;
  blocking_reasons: string[]; missing_information: string[]; review_reasons: string[];
  soft_gaps: string[]; rule_results: ProductMatchRuleResult[];
}

export interface ProductMatchingSnapshotData {
  status: 'completed' | 'no_active_products'; snapshot_id: string | null; reused?: boolean;
  customer_id?: string; requirement_id?: string; requirement_version?: number;
  catalog_as_of_date?: string; catalog_version_hash?: string; generated_at?: string;
  message?: string;
  precheck?: { active_product_count: number; published_product_count: number; active_rule_count: number };
  summary: Record<ProductMatchingStatus, number> & { total: number; boundary_notice?: string };
  data_quality: { missing_domains?: string[]; missing_fields?: string[]; conflicted_fields?: string[]; stale_fields?: string[]; preliminary_fields?: string[] };
  items: ProductMatchItemData[];
}

export async function runProductMatching(customerId: string, requirementId: string): Promise<ProductMatchingSnapshotData> {
  const response = await fetch(`${API_BASE}/api/product-matching/run`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ customer_id: customerId, requirement_id: requirementId }),
  });
  return handleResponse<ProductMatchingSnapshotData>(response);
}

export async function getLatestProductMatching(customerId: string): Promise<ProductMatchingSnapshotData | null> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/product-matching/latest`, {
    headers: getAuthHeaders(),
  });
  return (await handleResponse<{ snapshot: ProductMatchingSnapshotData | null }>(response)).snapshot;
}

export type FinancingPlanCandidateStatus = 'usable' | 'conditional' | 'manual_review' | 'excluded';

export interface FinancingPlanCandidatePoolData {
  customer_id: string; requirement_id: string; requirement_version: number; match_snapshot_id: string;
  facts_hash: string; catalog_hash: string; target_amount: string; covered_amount: string; funding_gap: string;
  currency: string; generation_status: 'complete' | 'partial' | 'insufficient_candidates' | 'manual_review_required';
  counts: Record<FinancingPlanCandidateStatus, number>;
  candidates: Array<{
    product_match_item_id: number; product_id: string; product_version_id: string; external_product_code: string;
    institution_name: string; product_name: string; match_status: ProductMatchingStatus;
    max_amount: string | null; max_term_months: number | null; eligible_amount_cap: string | null;
    missing_information: string[]; review_reasons: string[]; blocking_reasons: string[];
    candidate_status: FinancingPlanCandidateStatus;
  }>;
}

export async function buildFinancingPlanCandidates(customerId: string, requirementId: string, matchSnapshotId: string): Promise<FinancingPlanCandidatePoolData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/candidates`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ customer_id: customerId, requirement_id: requirementId, match_snapshot_id: matchSnapshotId }),
  });
  return handleResponse<FinancingPlanCandidatePoolData>(response);
}

export interface FinancingPlanCombinationItemData {
  product_match_item_id: number; product_id: string; product_version_id: string;
  external_product_code: string; institution_name: string; product_name: string;
  match_status: ProductMatchingStatus;
  candidate_status: FinancingPlanCandidateStatus; proposed_amount: string; proposed_term_months: number;
  manual_override_id: string | null;
  allocation_cap: string; min_amount: string | null; conditions: string[];
  missing_information: string[]; review_reasons: string[];
}

export interface FinancingPlanCombinationData {
  combination_id: string; plan_type: 'primary_candidate' | 'backup_candidate' | 'conditional';
  generation_status: 'complete' | 'partial'; target_amount: string; covered_amount: string; funding_gap: string;
  items: FinancingPlanCombinationItemData[]; product_count: number; institution_count: number;
  institution_concentration: boolean; conditions: string[]; missing_information: string[]; risks: string[];
  gaps: Array<{ gap_type: string; description: string; severity: string }>;
  is_complete: boolean; requires_manual_review: boolean; source_match_snapshot_id: string;
}

export interface FinancingPlanCombinationResponseData {
  customer_id: string; requirement_id: string; requirement_version: number; source_match_snapshot_id: string;
  target_amount: string; covered_amount: string; funding_gap: string; currency: string;
  generation_status: 'complete' | 'partial' | 'insufficient_candidates' | 'manual_review_required';
  formal_combinations: FinancingPlanCombinationData[];
  conditional_combinations: FinancingPlanCombinationData[];
  partial_combinations: FinancingPlanCombinationData[];
  manual_review_candidates: FinancingPlanCandidatePoolData['candidates'];
  excluded_candidates: FinancingPlanCandidatePoolData['candidates'];
}

export async function generateFinancingPlanCombinations(customerId: string, requirementId: string, matchSnapshotId: string): Promise<FinancingPlanCombinationResponseData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/combinations`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ customer_id: customerId, requirement_id: requirementId, match_snapshot_id: matchSnapshotId }),
  });
  return handleResponse<FinancingPlanCombinationResponseData>(response);
}

export interface FinancingPlanData {
  financing_plan_id: string; customer_id: string; requirement_id: string; requirement_version: number;
  source_match_snapshot_id: string; current_version_id: string; status: string;
  versions: Array<{
    plan_version_id: string; version_no: number; plan_type: string; target_amount: string; covered_amount: string;
    funding_gap: string; status: string; generation_status: string;
    items: Array<{ product_match_item_id: number; institution_name: string; product_name: string; proposed_amount: string; proposed_term_months: number; item_role: string; reason: string; notes: string; conditions: string[]; risks: string[] }>;
    gaps: Array<{ gap_type: string; description: string; severity: string }>;
    condition_checklist: Array<{ condition_id: string; condition_type: string; title: string; description: string; status: string; required: boolean }>;
    material_checklist: Array<{ material_id: string; material_name: string; material_category: string; status: string; required: boolean; required_by_products: string[] }>;
    explanation: null | { plan_summary: string; coverage_summary: string; funding_gap_summary: string; key_conditions: string[]; risk_notes: string[]; next_actions: string[] };
  }>;
}

export async function createFinancingPlanFromCombination(customerId: string, requirementId: string, matchSnapshotId: string, combinationId: string): Promise<FinancingPlanData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/from-combination`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({
      customer_id: customerId, requirement_id: requirementId,
      match_snapshot_id: matchSnapshotId, combination_id: combinationId,
    }),
  });
  return handleResponse<FinancingPlanData>(response);
}

export async function createManualCandidateOverride(customerId: string, requirementId: string, matchSnapshotId: string, productMatchItemId: number, reason: string) {
  const response = await fetch(`${API_BASE}/api/financing-plans/manual-overrides`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ customer_id: customerId, requirement_id: requirementId, match_snapshot_id: matchSnapshotId, product_match_item_id: productMatchItemId, new_status: 'conditional', reason }),
  });
  return handleResponse<{ override_id: string; new_status: 'conditional' }>(response);
}

export async function updateFinancingPlan(planId: string, payload: Record<string, unknown>): Promise<FinancingPlanData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/${encodeURIComponent(planId)}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload),
  });
  return handleResponse<FinancingPlanData>(response);
}

export async function updateFinancingPlanCondition(planId: string, conditionId: string, status: string) {
  const response = await fetch(`${API_BASE}/api/financing-plans/${encodeURIComponent(planId)}/conditions/${encodeURIComponent(conditionId)}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ status }),
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function updateFinancingPlanMaterial(planId: string, materialId: string, status: string) {
  const response = await fetch(`${API_BASE}/api/financing-plans/${encodeURIComponent(planId)}/materials/${encodeURIComponent(materialId)}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ status }),
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function validateFinancingPlan(planId: string) {
  const response = await fetch(`${API_BASE}/api/financing-plans/${encodeURIComponent(planId)}/validate`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<{ valid: boolean; errors: string[] }>(response);
}

export async function confirmFinancingPlan(planId: string): Promise<FinancingPlanData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/${encodeURIComponent(planId)}/confirm`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<FinancingPlanData>(response);
}

export async function listCustomerFinancingPlans(customerId: string, requirementId?: string): Promise<FinancingPlanData[]> {
  const query = requirementId ? `?requirement_id=${encodeURIComponent(requirementId)}` : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-plans${query}`, { headers: getAuthHeaders() });
  return (await handleResponse<{ plans: FinancingPlanData[] }>(response)).plans;
}

export interface FinancingPlanSelectionData {
  selection_id: string; customer_id: string; requirement_id: string;
  primary_plan_version_id: string | null; backup_plan_version_ids: string[];
  conditional_plan_version_ids: string[]; status: 'draft' | 'finalized' | 'superseded' | 'cancelled';
  selected_by: string; selected_at: string; finalized_by?: string | null; finalized_at?: string | null; notes: string;
}

export interface FinancingPlanVersionDiffData {
  customer_id: string; from_version: { id: string; version_no: number }; to_version: { id: string; version_no: number };
  amount_changes: Record<string, { from: string; to: string }>;
  count_changes: { product_count: { from: number; to: number }; institution_count: { from: number; to: number } };
  product_changes: { added: Array<Record<string, unknown>>; removed: Array<Record<string, unknown>>; changed: Array<Record<string, unknown>> };
  condition_changes: { added: string[]; removed: string[]; changed: Array<{ name: string; from: string; to: string }> };
  material_changes: { added: string[]; removed: string[]; changed: Array<{ name: string; from: string; to: string }> };
  gap_changes: { added: string[]; removed: string[]; changed: Array<{ name: string; from: string; to: string }> };
  summary: string;
}

export interface FinancingPlanReportData {
  report_id: string; plan_selection_id: string; report_type: 'internal' | 'customer'; report_version: number;
  structured_payload: Record<string, unknown>; rendered_html: string; generated_at: string;
}

export async function createFinancingPlanSelection(payload: {
  customer_id: string; requirement_id: string; primary_plan_version_id?: string | null;
  backup_plan_version_ids?: string[]; conditional_plan_version_ids?: string[]; notes?: string;
}): Promise<FinancingPlanSelectionData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/selections`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingPlanSelectionData>(response);
}

export async function finalizeFinancingPlanSelection(selectionId: string): Promise<FinancingPlanSelectionData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/selections/${encodeURIComponent(selectionId)}/finalize`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<FinancingPlanSelectionData>(response);
}

export async function compareFinancingPlanVersions(versionA: string, versionB: string): Promise<FinancingPlanVersionDiffData> {
  const response = await fetch(`${API_BASE}/api/financing-plans/compare`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ version_a: versionA, version_b: versionB }) });
  return handleResponse<FinancingPlanVersionDiffData>(response);
}

export async function generateFinancingPlanReport(selectionId: string, reportType: 'internal' | 'customer'): Promise<FinancingPlanReportData> {
  const response = await fetch(`${API_BASE}/api/financing-plan-reports/generate`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ selection_id: selectionId, report_type: reportType }) });
  return handleResponse<FinancingPlanReportData>(response);
}

export async function downloadFinancingPlanReportPdf(reportId: string): Promise<Blob> {
  const response = await fetch(`${API_BASE}/api/financing-plan-reports/${encodeURIComponent(reportId)}/pdf`, { headers: getAuthHeaders() });
  if (!response.ok) await handleResponse(response);
  return response.blob();
}

export interface FinancingApplicationData {
  application_id: string; parent_application_id: string | null; attempt_no: number;
  customer_id: string; customer_name?: string; requirement_id: string; requirement_version: number;
  plan_id: string; plan_version_id: string; plan_item_id: string; product_id: string; product_version_id: string;
  external_product_code: string; institution_name: string; product_name: string; application_no: string; status: string;
  target_amount: string; submitted_amount: string | null; approved_amount: string | null; disbursed_amount: string | null;
  target_term_months: number; approved_term_months: number | null; target_interest_rate: string | null; approved_interest_rate: string | null;
  responsible_user_id: string | null; responsible_user_name: string | null; current_stage_code: string;
  submission_channel: string | null; submission_reference: string | null; approval_reference: string | null;
  rejection_reason: string; rejection_code?: string | null; rejected_at?: string | null;
  disbursement_reference: string | null; final_result: string | null;
  updated_at?: string | null; next_action?: string; overdue_task_count?: number;
  blocking_items?: string[]; next_task?: { task_id: string; title: string; due_date: string | null } | null;
  stages: Array<{ stage_id: string; stage_code: string; stage_name: string; status: string; sequence_no: number; notes?: string }>;
  tasks: Array<{ task_id: string; stage_id: string; task_type: string; title: string; description: string; status: string; priority: string; assignee_user_id?: string | null; assignee_user_name: string | null; due_date: string | null; source_type: string; source_ref: string | null; required: boolean; completed_by?: string | null; completed_at?: string | null; notes: string }>;
  events: Array<{ event_id: string; event_type: string; from_status: string | null; to_status: string | null; operator_name: string; payload: Record<string, unknown>; created_at: string | null }>;
  supplements?: Array<{ supplement_id: string; request_no: string; description: string; requested_by_bank: string | null; requested_at: string; due_date: string | null; status: string; completed_at: string | null; completed_by: string | null; notes: string }>;
  application_materials?: ApplicationMaterialData[];
  review_feedback?: Array<{ feedback_id: string; feedback_type: string; feedback_date: string; institution_contact: string | null; content: string; related_stage: string; created_by: string }>;
  approval_records?: Array<{ approval_record_id: string; approval_status: string; submitted_amount: string; approved_amount: string | null; approved_term_months: number | null; approved_interest_rate: string | null; guarantee_method: string | null; repayment_method: string | null; approval_reference: string | null; approval_date: string; approval_expiry_date: string | null; notes: string }>;
  approval_conditions?: Array<{ approval_condition_id: string; approval_record_id: string; title: string; description: string; status: string; required: boolean }>;
  disbursements?: Array<{ disbursement_id: string; disbursement_no: string; amount: string; disbursed_at: string; bank_reference: string | null; recipient_name: string | null; recipient_account_masked: string | null; purpose: string | null; notes: string }>;
}

export interface ApplicationMaterialData {
  application_material_id: string; supplement_request_id: string | null;
  material_type: string; material_name: string; material_category: string;
  owner_type: string; owner_id: string | null; owner_name: string | null;
  required: boolean; required_verified: boolean; status: string; source_type: string;
  source_id: string | null; source_document_id: string | null; customer_material_id: string | null;
  source_file_id: string | null; file_reference: string | null; valid_from: string | null; valid_to: string | null;
  coverage_start: string | null; coverage_end: string | null; version_no: number;
  replaces_material_id: string | null; rejection_reason: string; verified_by: string | null;
  verified_at: string | null; notes: string;
}

export interface ApplicationMaterialSummaryData {
  total_required: number; verified_count: number; available_count: number;
  missing_count: number; expired_count: number; review_count: number;
}

export interface ApplicationMaterialMatchCandidate {
  customer_material_id: string; source_file_id: string; source_document_id: string;
  file_name: string; file_hash: string; file_path: string; document_type: string;
  owner_id: string; owner_name: string; owner_type: string; valid_to: string | null;
  period_start: string | null; period_end: string | null; confirmed: boolean;
  extraction_id: string | null; status: string;
}

export interface ApplicationMaterialMatchResult {
  application_material_id: string; material_name: string; result: string;
  candidates: ApplicationMaterialMatchCandidate[];
}

export interface ApplicationPackageData {
  package_id: string; application_id: string; package_version: number; status: string;
  created_by: string; created_at: string | null;
  items: Array<{ package_item_id: string; application_material_id: string; material_type: string;
    material_name: string; owner_type: string; owner_name: string | null; file_name: string | null;
    file_hash: string; package_file_name: string | null; required: boolean; included: boolean }>;
}

export interface SubmissionPackageData {
  submission_package_id: string; application_id: string; application_package_id: string;
  submission_version: number; institution_name: string; product_name: string; status: string;
  manifest: Record<string, unknown>; package_hash: string; submitted_at: string | null;
}

export interface SupplementPackageData {
  supplement_package_id: string; application_id: string; supplement_request_id: string;
  package_version: number; status: string; manifest: Record<string, unknown>;
  package_hash: string; submitted_at: string | null; submission_reference: string | null;
}

export interface FinancingExecutionDashboardData {
  total_applications: number;
  status_counts: Record<string, number>;
  amounts: Record<string, string>;
  applications: FinancingApplicationData[];
  follow_up_metrics?: { today_count: number; overdue_count: number; waiting_customer_count: number;
    waiting_institution_count: number; items: FinancingFollowUpData[] };
}

export interface FinancingContactData {
  contact_id: string; contact_type: string; customer_id: string | null; institution_name: string | null;
  branch_name: string | null; name: string; title: string | null; department: string | null;
  mobile: string | null; phone: string | null; email: string | null; wechat: string | null;
  is_primary: boolean; related_person_id: string | null; status: string; notes: string;
  role?: string; application_primary?: boolean;
}

export interface FinancingCommunicationData {
  communication_id: string; application_id: string; customer_id: string; contact_id: string | null;
  contact_name: string | null; contact_organization: string | null; communication_side: string;
  channel: string; direction: string; feedback_tag: string | null; subject: string; content: string;
  occurred_at: string; operator_name: string; related_task_id: string | null;
  related_supplement_id: string | null; related_review_feedback_id: string | null;
  related_approval_record_id: string | null; follow_up_required: boolean;
  next_follow_up_at: string | null; outcome: string; internal_note: string; status: string;
}

export interface FinancingFollowUpData {
  follow_up_id: string; customer_id: string; application_id: string;
  communication_record_id: string | null; related_task_id: string | null; follow_up_type: string;
  title: string; description: string; assignee_user_id: string | null; assignee_user_name: string | null;
  due_at: string; status: string; priority: string; overdue: boolean;
  completed_at: string | null; completed_by: string | null; result: string;
}

export interface FinancingTimelineItemData {
  type: string; id: string; occurred_at: string; title: string;
  description: string | Record<string, unknown>; operator_name?: string; status?: string;
}

export interface FinancingApplicationOutcomeData {
  outcome_id: string; application_id: string; customer_id: string; product_id: string;
  product_version_id: string; requirement_id: string; plan_version_id: string;
  outcome_version: number; supersedes_outcome_id: string | null; final_status: string;
  submitted_amount: string | null; approved_amount: string | null; disbursed_amount: string | null;
  submitted_term_months: number | null; approved_term_months: number | null;
  submitted_interest_rate: string | null; approved_interest_rate: string | null;
  approval_date: string | null; disbursement_date: string | null;
  rejection_code: string | null; rejection_reason: string; final_notes: string;
  status: string; closed_at: string; rejection_reasons: Array<{ reason_code: string; reason_label: string; description: string; source_type: string }>;
  approval_variance: { amount_delta: string | null; approval_ratio: string | null; term_delta_months: number | null; interest_rate_delta: string | null };
  disbursement_variance: { amount_delta: string | null; reason: string | null };
}

export interface ProductExecutionMetricsData {
  product_id: string; product_version_id: string | null; sample_size: number;
  application_count: number; submitted_count: number; approved_count: number;
  partially_approved_count: number; rejected_count: number; disbursed_count: number;
  submitted_amount_total: string; approved_amount_total: string; disbursed_amount_total: string;
  supplement_application_count: number; supplement_round_total: number;
  average_approval_days: number | null; average_disbursement_days: number | null;
  pending_rule_feedback_count: number;
}

export interface CustomerFinancingReviewData {
  customer_id: string; requirement_id: string; requirement_version: number;
  requirement: { amount: string | null; purpose: string | null; term_value: number | null; term_unit: string | null };
  application_count: number; outcome_count: number; submitted_amount: string;
  approved_amount: string; disbursed_amount: string; funding_gap: string;
  actual_coverage_ratio: string | null; execution_status: string; message: string;
  rejection_reasons: Array<{ reason_code: string; reason_label: string; description: string }>;
  rule_feedback: Array<Record<string, unknown>>;
  lifecycle_timeline?: Array<{ type: string; title: string; occurred_at: string | null; reference_id: string; application_id?: string }>;
}

async function applicationAction(applicationId: string, action: string, payload?: Record<string, unknown>): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/${action}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  });
  return handleResponse<FinancingApplicationData>(response);
}

export async function listFinancingApplications(customerId: string, requirementId?: string): Promise<FinancingApplicationData[]> {
  const query = requirementId ? `?requirement_id=${encodeURIComponent(requirementId)}` : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-applications${query}`, { headers: getAuthHeaders() });
  return (await handleResponse<{ applications: FinancingApplicationData[] }>(response)).applications;
}

export async function createFinancingApplications(customerId: string, planVersionId: string): Promise<FinancingApplicationData[]> {
  const response = await fetch(`${API_BASE}/api/financing-applications/from-plan`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ customer_id: customerId, plan_version_id: planVersionId }) });
  return (await handleResponse<{ applications: FinancingApplicationData[] }>(response)).applications;
}

export const startApplicationPreparation = (id: string) => applicationAction(id, 'start-preparation');
export const markApplicationReady = (id: string) => applicationAction(id, 'ready-to-submit');
export const submitFinancingApplication = (id: string, payload: Record<string, unknown> = {}) => applicationAction(id, 'submit', payload);
export const markApplicationUnderReview = (id: string) => applicationAction(id, 'under-review');
export const requestApplicationSupplement = (id: string, payload: { description: string; required_materials: string[]; due_date?: string | null }) => applicationAction(id, 'supplement', payload);
export const recordApplicationApproval = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'approval', payload);
export const recordApplicationRejection = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'rejection', payload);
export const recordApplicationDisbursement = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'disbursement', payload);
export const retryFinancingApplication = (id: string) => applicationAction(id, 'retry');
export const advanceFinancingApplicationStage = (id: string, targetStageCode: string) => applicationAction(id, 'advance-stage', { target_stage_code: targetStageCode });

export async function getFinancingExecutionDashboard(filters: Record<string, string | boolean> = {}): Promise<FinancingExecutionDashboardData> {
  const query = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => { if (value !== '' && value !== false) query.set(key, String(value)); });
  const response = await fetch(`${API_BASE}/api/financing-execution/dashboard${query.size ? `?${query}` : ''}`, { headers: getAuthHeaders() });
  return handleResponse<FinancingExecutionDashboardData>(response);
}

export async function listApplicationContacts(applicationId: string): Promise<FinancingContactData[]> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/contacts`, { headers: getAuthHeaders() });
  return (await handleResponse<{ contacts: FinancingContactData[] }>(response)).contacts;
}

export async function createFinancingContact(payload: Record<string, unknown>): Promise<FinancingContactData> {
  const response = await fetch(`${API_BASE}/api/financing-contacts`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingContactData>(response);
}

export async function attachApplicationContact(applicationId: string, contactId: string, role: string): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/contacts`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ contact_id: contactId, role, is_primary: false }) });
  return handleResponse<Record<string, unknown>>(response);
}

export async function listApplicationCommunications(applicationId: string, side = ''): Promise<FinancingCommunicationData[]> {
  const query = side ? `?side=${encodeURIComponent(side)}` : '';
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/communications${query}`, { headers: getAuthHeaders() });
  return (await handleResponse<{ communications: FinancingCommunicationData[] }>(response)).communications;
}

export async function createApplicationCommunication(applicationId: string, payload: Record<string, unknown>): Promise<FinancingCommunicationData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/communications`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingCommunicationData>(response);
}

export async function createTaskFromCommunication(communicationId: string, payload: Record<string, unknown> = {}): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-communications/${encodeURIComponent(communicationId)}/create-task`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingApplicationData>(response);
}

export async function createSupplementFromCommunication(communicationId: string, requiredMaterials: string[]): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-communications/${encodeURIComponent(communicationId)}/create-supplement`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ required_materials: requiredMaterials }) });
  return handleResponse<FinancingApplicationData>(response);
}

export async function createReviewFeedbackFromCommunication(communicationId: string, feedbackType = 'general'): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-communications/${encodeURIComponent(communicationId)}/create-review-feedback`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ feedback_type: feedbackType }) });
  return handleResponse<FinancingApplicationData>(response);
}

export async function listFinancingFollowUps(applicationId?: string): Promise<FinancingFollowUpData[]> {
  const query = applicationId ? `?application_id=${encodeURIComponent(applicationId)}` : '';
  const response = await fetch(`${API_BASE}/api/financing-followups${query}`, { headers: getAuthHeaders() });
  return (await handleResponse<{ follow_ups: FinancingFollowUpData[] }>(response)).follow_ups;
}

export async function completeFinancingFollowUp(followUpId: string, result = ''): Promise<FinancingFollowUpData> {
  const response = await fetch(`${API_BASE}/api/financing-followups/${encodeURIComponent(followUpId)}/complete`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ result }) });
  return handleResponse<FinancingFollowUpData>(response);
}

export async function getFinancingApplicationTimeline(applicationId: string): Promise<FinancingTimelineItemData[]> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/timeline`, { headers: getAuthHeaders() });
  return (await handleResponse<{ timeline: FinancingTimelineItemData[] }>(response)).timeline;
}

export async function getFinancingApplicationOutcome(applicationId: string): Promise<FinancingApplicationOutcomeData | null> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/outcome`, { headers: getAuthHeaders() });
  return (await handleResponse<{ outcome: FinancingApplicationOutcomeData | null }>(response)).outcome;
}

export async function finalizeFinancingApplicationOutcome(applicationId: string, payload: Record<string, unknown> = {}): Promise<FinancingApplicationOutcomeData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/outcome/finalize`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingApplicationOutcomeData>(response);
}

export async function getProductExecutionMetrics(productId: string, productVersionId?: string): Promise<ProductExecutionMetricsData> {
  const query = productVersionId ? `?product_version_id=${encodeURIComponent(productVersionId)}` : '';
  const response = await fetch(`${API_BASE}/api/products/${encodeURIComponent(productId)}/execution-metrics${query}`, { headers: getAuthHeaders() });
  return handleResponse<ProductExecutionMetricsData>(response);
}

export async function getCustomerFinancingReview(customerId: string, requirementId?: string): Promise<CustomerFinancingReviewData> {
  const query = requirementId ? `?requirement_id=${encodeURIComponent(requirementId)}` : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-review${query}`, { headers: getAuthHeaders() });
  return handleResponse<CustomerFinancingReviewData>(response);
}

export const createApplicationSupplement = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'supplements', payload);
export async function completeApplicationSupplement(id: string, supplementId: string): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(id)}/supplements/${encodeURIComponent(supplementId)}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ complete: true }) });
  return handleResponse<FinancingApplicationData>(response);
}
export const addApplicationReviewFeedback = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'review-feedback', payload);
export const updateApplicationApprovalCondition = (id: string, conditionId: string, status: string) => applicationAction(id, `approval-conditions/${encodeURIComponent(conditionId)}`, { status });
export const createApplicationDisbursement = (id: string, payload: Record<string, unknown>) => applicationAction(id, 'disbursements', payload);

export async function updateApplicationMaterial(applicationId: string, materialId: string, payload: Record<string, unknown>): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/${encodeURIComponent(materialId)}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingApplicationData>(response);
}

export async function getApplicationMaterials(applicationId: string): Promise<{ application_id: string; materials: ApplicationMaterialData[]; summary: ApplicationMaterialSummaryData; events: Array<Record<string, unknown>> }> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials`, { headers: getAuthHeaders() });
  return handleResponse(response);
}

export const matchApplicationMaterials = async (applicationId: string) => {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/match`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<{ application_id: string; results: ApplicationMaterialMatchResult[]; summary: ApplicationMaterialSummaryData }>(response);
};

export async function selectApplicationMaterial(applicationId: string, materialId: string, documentId: string): Promise<ApplicationMaterialData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/${encodeURIComponent(materialId)}/select`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ document_id: documentId }) });
  return handleResponse<ApplicationMaterialData>(response);
}

export async function replaceApplicationMaterial(applicationId: string, materialId: string, documentId: string, rejectionReason: string): Promise<ApplicationMaterialData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/${encodeURIComponent(materialId)}/replace`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ document_id: documentId, rejection_reason: rejectionReason }) });
  return handleResponse<ApplicationMaterialData>(response);
}

export async function getApplicationMaterialExtraction(applicationId: string, materialId: string): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/${encodeURIComponent(materialId)}/extraction`, { headers: getAuthHeaders() });
  return handleResponse<Record<string, unknown>>(response);
}

export const verifyApplicationMaterial = async (applicationId: string, materialId: string) => {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/materials/${encodeURIComponent(materialId)}/verify`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<ApplicationMaterialData>(response);
};

export async function createApplicationPackage(applicationId: string): Promise<ApplicationPackageData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/packages`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<ApplicationPackageData>(response);
}

export async function listApplicationPackages(applicationId: string): Promise<{ packages: ApplicationPackageData[]; submission_packages: SubmissionPackageData[]; supplement_packages: SupplementPackageData[] }> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/packages`, { headers: getAuthHeaders() });
  return handleResponse(response);
}

export async function freezeApplicationPackage(packageId: string): Promise<ApplicationPackageData> {
  const response = await fetch(`${API_BASE}/api/financing-application-packages/${encodeURIComponent(packageId)}/freeze`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse<ApplicationPackageData>(response);
}

export async function createSubmissionPackage(packageId: string): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/financing-application-packages/${encodeURIComponent(packageId)}/submission-package`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse(response);
}

export async function downloadSubmissionPackageZip(submissionPackageId: string): Promise<Blob> {
  const response = await fetch(`${API_BASE}/api/financing-submission-packages/${encodeURIComponent(submissionPackageId)}/zip`, { headers: getAuthHeaders() });
  if (!response.ok) await handleResponse(response);
  return response.blob();
}

export async function createSupplementPackage(supplementId: string): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/financing-supplements/${encodeURIComponent(supplementId)}/packages`, { method: 'POST', headers: getAuthHeaders() });
  return handleResponse(response);
}

async function taskAction(applicationId: string, taskId: string, action: string, payload: Record<string, unknown>): Promise<FinancingApplicationData> {
  void applicationId;
  const response = await fetch(`${API_BASE}/api/financing-applications/tasks/${encodeURIComponent(taskId)}/${action}`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingApplicationData>(response);
}

export const completeFinancingApplicationTask = (applicationId: string, taskId: string, notes = '') => taskAction(applicationId, taskId, 'complete', { notes });
export const blockFinancingApplicationTask = (applicationId: string, taskId: string, blockReason: string) => taskAction(applicationId, taskId, 'block', { block_reason: blockReason });
export const unblockFinancingApplicationTask = (applicationId: string, taskId: string, reason = '') => taskAction(applicationId, taskId, 'unblock', { reason, target_status: 'in_progress' });

export async function updateFinancingApplicationTask(applicationId: string, taskId: string, payload: Record<string, unknown>): Promise<FinancingApplicationData> {
  const response = await fetch(`${API_BASE}/api/financing-applications/${encodeURIComponent(applicationId)}/tasks/${encodeURIComponent(taskId)}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(payload) });
  return handleResponse<FinancingApplicationData>(response);
}

export async function createChatJob(
  request: ChatRequest,
  signal?: AbortSignal
): Promise<ChatJobCreateResponse> {
  const response = await fetch(`${DIRECT_JOB_API_BASE}/chat/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<ChatJobCreateResponse>(response);
}

export async function getChatJobStatus(
  jobId: string,
  signal?: AbortSignal
): Promise<ChatJobStatusResponse> {
  const response = await fetch(`${API_BASE}/api/chat/jobs/${jobId}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ChatJobStatusResponse>(response);
}

export async function listChatJobs(
  limit = 10,
  signal?: AbortSignal
): Promise<ChatJobSummaryResponse[]> {
  const response = await fetch(`${API_BASE}/api/chat/jobs?limit=${limit}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ChatJobSummaryResponse[]>(response);
}

export async function deleteChatJob(
  jobId: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/chat/jobs/${jobId}`, {
    method: 'DELETE',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}

export async function clearCustomerCache(signal?: AbortSignal): Promise<{ message: string }> {
  const response = await fetch(`${API_BASE}/api/chat/clear-customer-cache`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ message: string }>(response);
}

// ==================== Dashboard API ====================

export interface DashboardStats {
  todayUploads: number;
  pending: number;
  completed: number;
  totalCustomers: number;
  pendingMaterialCustomers?: number;
  reportedCustomers?: number;
  highRiskCustomers?: number;
}

export interface Activity {
  id: string;
  type: string;
  time: string;
  createdAt?: string;
  status: string;
  fileName?: string;
  fileType?: string;
  customerName?: string;
  customerId?: string;
  username?: string;
  title?: string;
  description?: string;
  metadata?: Record<string, unknown>;
}

export interface ActivitiesResponse {
  activities: Activity[];
}

export async function getDashboardStats(signal?: AbortSignal): Promise<DashboardStats> {
  const response = await fetch(`${API_BASE}/api/dashboard/stats`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<DashboardStats>(response);
}

export async function getDashboardActivities(
  limit: number = 10,
  signal?: AbortSignal
): Promise<ActivitiesResponse> {
  const response = await fetch(`${API_BASE}/api/dashboard/activities?limit=${limit}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ActivitiesResponse>(response);
}

export async function getCustomerRiskReportHistory(
  customerId: string,
  limit: number = 2,
  signal?: AbortSignal
): Promise<CustomerRiskReportHistoryResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${customerId}/risk-reports/history?limit=${limit}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerRiskReportHistoryResponse>(response);
}

// ==================== Wiki/Product Cache API ====================

export interface WikiCacheStatusResponse {
  cached: boolean;
  lastUpdated: string | null;
  enterpriseProductCount: number;
  personalProductCount: number;
}

export interface WikiCacheContentResponse {
  enterprise: string;
  personal: string;
  lastUpdated: string;
}

export interface WikiRefreshResponse {
  success: boolean;
  message: string;
  lastUpdated: string;
}

/**
 * Get product cache status.
 */
export async function getWikiCacheStatus(signal?: AbortSignal): Promise<WikiCacheStatusResponse> {
  const response = await fetch(`${API_BASE}/api/wiki/cache-status`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<WikiCacheStatusResponse>(response);
}

/**
 * Get product cache content.
 */
export async function getWikiCache(signal?: AbortSignal): Promise<WikiCacheContentResponse> {
  const response = await fetch(`${API_BASE}/api/wiki/cache`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<WikiCacheContentResponse>(response);
}

export async function refreshWikiCache(signal?: AbortSignal): Promise<WikiRefreshResponse> {
  const response = await fetch(`${API_BASE}/api/wiki/refresh`, {
    method: 'POST',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<WikiRefreshResponse>(response);
}

// ==================== Saved Application API ====================

export interface SavedApplicationListItem {
  id: string;
  versionGroupId?: string;
  previousApplicationId?: string;
  versionNo?: number;
  customerName: string;
  customerId?: string | null;
  loanType: string;
  savedAt: string;
}

export interface LocalCatalogSourceStatus {
  category: string;
  label: string;
  source_file: string;
  missing: boolean;
  file_updated_at: string | null;
  source_update_date: string | null;
  source_snapshot_hash: string | null;
  declared_count: number | null;
  parsed_count: number;
  unique_count: number;
  duplicate_count: number;
  conflict_count: number;
  needs_review_count: number;
  published_count: number | null;
  draft_count: number | null;
  last_synced_at: string | null;
}

export interface LocalCatalogSourcesResponse {
  sources: LocalCatalogSourceStatus[];
  database_status: string;
  totals: { parsed_count: number; unique_count: number; duplicate_code_count: number; conflict_count: number; needs_review_count: number };
}

export interface LocalCatalogProductSummary {
  external_product_code: string;
  product_name: string;
  institution_name: string;
  source_file: string;
  snapshot_hash: string;
  needs_review: boolean;
  duplicate_conflict: boolean;
}

export interface LocalCatalogConflict {
  external_product_code: string;
  status: string;
  conflict?: boolean;
  stale_resolution?: boolean;
  needs_review: boolean;
  sides: { source_file: string; product_name: string; snapshot_hash: string }[];
  conflict_fields: string[];
}

export interface CatalogConflictSide {
  side: 'a' | 'b'; external_product_code: string; product_name: string; institution_name: string;
  product_category: string; source_file: string; source_update_date: string | null;
  source_snapshot_hash: string; source_snapshot: string; raw_fields: Record<string, unknown>;
  structured_fields: Record<string, unknown>;
}
export interface CatalogConflictDetail extends Omit<LocalCatalogConflict, 'sides'> {
  sides: CatalogConflictSide[];
  differences: { field_name: string; a: unknown; b: unknown; kind: string }[];
  source_line_changes: { a_only: string[]; b_only: string[] };
}
export interface CatalogConflictDecision {
  strategy: 'keep_a' | 'keep_b' | 'merge' | 'split';
  field_choices?: Record<string, 'a' | 'b'>; rename_side?: 'a' | 'b'; new_code?: string;
}

export interface CatalogProduct { product_id: string; external_product_code: string | null; product_category: string; source_ref: string; institution_name: string; product_name: string }
export interface CatalogVersion {
  version_id: string; product_id: string; version_number: number; status: string; institution_name: string; product_name: string;
  effective_from: string | null; effective_to: string | null; published_at: string | null; published_by: string;
  source_file: string; source_update_date: string | null; source_snapshot: string; source_snapshot_hash: string;
  source_imported_at: string; needs_review: number; review_status: string; review_reasons_json: string[];
  field_review_json: Record<string, string>; raw_fields_json: Record<string, string | string[]>;
  min_amount: string | null; max_amount: string | null; min_term_months: number | null; max_term_months: number | null;
  [key: string]: unknown;
}
export interface CatalogRule {
  rule_id: string; version_id: string; field_name: string; operator: string; expected_value_json: unknown;
  severity: string; failure_action: string; message: string; source_text: string; rule_group: string; sort_order: number;
}
export interface CatalogVersionDetail { product: CatalogProduct; version: CatalogVersion; rules: CatalogRule[] }
export interface CatalogProductRow { product: CatalogProduct; version: CatalogVersion }

export async function getLocalCatalogSources(signal?: AbortSignal): Promise<LocalCatalogSourcesResponse> {
  const response = await fetch(`${API_BASE}/api/product-catalog/sources`, { headers: { ...getAuthHeaders() }, signal });
  return handleResponse<LocalCatalogSourcesResponse>(response);
}

export async function getLocalCatalogProducts(category: string, signal?: AbortSignal): Promise<{ items: LocalCatalogProductSummary[]; total: number; missing: boolean }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/sources/${encodeURIComponent(category)}/products`, { headers: { ...getAuthHeaders() }, signal });
  return handleResponse<{ items: LocalCatalogProductSummary[]; total: number; missing: boolean }>(response);
}

export async function getLocalCatalogConflicts(signal?: AbortSignal): Promise<{ items: LocalCatalogConflict[]; total: number }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/sources/conflicts`, { headers: { ...getAuthHeaders() }, signal });
  return handleResponse<{ items: LocalCatalogConflict[]; total: number }>(response);
}

export async function getCatalogConflict(code: string): Promise<CatalogConflictDetail> {
  const response = await fetch(`${API_BASE}/api/product-catalog/conflicts/${encodeURIComponent(code)}`, { headers: getAuthHeaders() });
  return handleResponse<CatalogConflictDetail>(response);
}

export async function resolveCatalogConflict(code: string, decision: CatalogConflictDecision): Promise<{ status: string; created_drafts: number; version_ids: string[] }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/conflicts/${encodeURIComponent(code)}/resolve`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify(decision),
  });
  return handleResponse<{ status: string; created_drafts: number; version_ids: string[] }>(response);
}

export async function syncLocalCatalog(category: string, signal?: AbortSignal): Promise<{ created_drafts: number; unchanged: number; conflicts: LocalCatalogConflict[] }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/sources/${encodeURIComponent(category)}/sync`, {
    method: 'POST', headers: { ...getAuthHeaders() }, signal,
  });
  return handleResponse<{ created_drafts: number; unchanged: number; conflicts: LocalCatalogConflict[] }>(response);
}

export async function listCatalogProducts(filters: Record<string, string> = {}): Promise<{ items: CatalogProductRow[]; total: number }> {
  const query = new URLSearchParams(filters).toString();
  const response = await fetch(`${API_BASE}/api/product-catalog/products${query ? `?${query}` : ''}`, { headers: getAuthHeaders() });
  return handleResponse<{ items: CatalogProductRow[]; total: number }>(response);
}

export async function getCatalogVersion(versionId: string): Promise<CatalogVersionDetail> {
  const response = await fetch(`${API_BASE}/api/product-catalog/versions/${encodeURIComponent(versionId)}`, { headers: getAuthHeaders() });
  return handleResponse<CatalogVersionDetail>(response);
}

export async function getCatalogProductHistory(productId: string): Promise<{ items: CatalogProductRow[]; total: number }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/products/${encodeURIComponent(productId)}/versions`, { headers: getAuthHeaders() });
  return handleResponse<{ items: CatalogProductRow[]; total: number }>(response);
}

export async function listCatalogVersions(status?: string): Promise<{ items: CatalogProductRow[]; total: number }> {
  const query = status ? `?status=${encodeURIComponent(status)}` : '';
  const response = await fetch(`${API_BASE}/api/product-catalog/versions${query}`, { headers: getAuthHeaders() });
  return handleResponse<{ items: CatalogProductRow[]; total: number }>(response);
}

export async function patchCatalogDraft(versionId: string, fields: Record<string, unknown>): Promise<CatalogVersionDetail> {
  const response = await fetch(`${API_BASE}/api/product-catalog/versions/${encodeURIComponent(versionId)}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ fields }),
  });
  return handleResponse<CatalogVersionDetail>(response);
}

export async function getCatalogRuleOptions(): Promise<{ fields: Record<string, string>; operators: string[]; severities: string[]; failure_actions: string[] }> {
  const response = await fetch(`${API_BASE}/api/product-catalog/rule-options`, { headers: getAuthHeaders() });
  return handleResponse<{ fields: Record<string, string>; operators: string[]; severities: string[]; failure_actions: string[] }>(response);
}

export async function saveCatalogRule(versionId: string, rule: Record<string, unknown>, ruleId?: string): Promise<CatalogRule> {
  const suffix = ruleId ? `/rules/${encodeURIComponent(ruleId)}` : '/rules';
  const response = await fetch(`${API_BASE}/api/product-catalog/versions/${encodeURIComponent(versionId)}${suffix}`, {
    method: ruleId ? 'PATCH' : 'POST', headers: { 'Content-Type': 'application/json', ...getAuthHeaders() }, body: JSON.stringify({ rule }),
  });
  return handleResponse<CatalogRule>(response);
}

export async function deleteCatalogRule(versionId: string, ruleId: string): Promise<void> {
  const response = await fetch(`${API_BASE}/api/product-catalog/versions/${encodeURIComponent(versionId)}/rules/${encodeURIComponent(ruleId)}`, {
    method: 'DELETE', headers: getAuthHeaders(),
  });
  await handleResponse(response);
}

export async function changeCatalogVersion(versionId: string, action: 'publish' | 'disable'): Promise<CatalogVersionDetail> {
  const response = await fetch(`${API_BASE}/api/product-catalog/versions/${encodeURIComponent(versionId)}/${action}`, {
    method: 'POST', headers: getAuthHeaders(),
  });
  return handleResponse<CatalogVersionDetail>(response);
}

export interface SavedApplication extends SavedApplicationListItem {
  applicationData: Record<string, unknown>;
}

export interface SaveApplicationRequest {
  customerName: string;
  customerId?: string | null;
  loanType: string;
  applicationData: Record<string, unknown>;
  baseApplicationId?: string;
  versionGroupId?: string;
}

/**
 * Natural-language parsing request.
 */
export interface NaturalLanguageRequest {
  text: string;
  creditType: string;
}

/**
 * Natural-language parsing response.
 */
export interface NaturalLanguageResponse {
  customerData: Record<string, unknown>;
  parsedFields: string[];
}

/**
 * List saved applications.
 */
export async function listSavedApplications(signal?: AbortSignal): Promise<SavedApplicationListItem[]> {
  const response = await fetch(`${API_BASE}/api/scheme/applications`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<SavedApplicationListItem[]>(response);
}

/**
 * Save an application.
 */
export async function saveApplication(
  request: SaveApplicationRequest,
  signal?: AbortSignal
): Promise<SavedApplication> {
  const response = await fetch(`${API_BASE}/api/scheme/applications`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<SavedApplication>(response);
}

/**
 * Get a saved application by ID.
 */
export async function getApplication(
  applicationId: string,
  signal?: AbortSignal
): Promise<SavedApplication> {
  const response = await fetch(`${API_BASE}/api/scheme/applications/${applicationId}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<SavedApplication>(response);
}

/**
 * Delete a saved application.
 */
export async function deleteApplication(
  applicationId: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/scheme/applications/${applicationId}`, {
    method: 'DELETE',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}

/**
 * Parse natural language into structured customer data.
 */
export async function parseNaturalLanguage(
  request: NaturalLanguageRequest,
  signal?: AbortSignal
): Promise<NaturalLanguageResponse> {
  const response = await fetch(`${API_BASE}/api/scheme/parse-natural-language`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<NaturalLanguageResponse>(response);
}

// ==================== Customer Search API ====================

/**
 * Search customer request.
 */
export interface SearchCustomerRequest {
  customerName: string;
}

/**
 * Search customer response.
 */
export interface SearchCustomerResponse {
  found: boolean;
  customerData: Record<string, unknown>;
  recordId?: string;
}

/**
 * Search a customer by name.
 */
export async function searchCustomer(
  request: SearchCustomerRequest,
  signal?: AbortSignal
): Promise<SearchCustomerResponse> {
  const response = await fetch(`${API_BASE}/api/scheme/search-customer`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<SearchCustomerResponse>(response);
}

// ==================== Auth API ====================

export async function login(
  username: string,
  password: string,
  signal?: AbortSignal
): Promise<LoginResponse> {
  const response = await fetch(`${API_BASE}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
    signal,
  });
  return handleResponse<LoginResponse>(response);
}

export async function register(
  username: string,
  password: string,
  securityQuestion?: string,
  securityAnswer?: string,
  signal?: AbortSignal
): Promise<RegisterResponse> {
  const body: Record<string, string> = { username, password };
  if (securityQuestion && securityAnswer) {
    body.security_question = securityQuestion;
    body.security_answer = securityAnswer;
  }

  const response = await fetch(`${API_BASE}/api/auth/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  });
  return handleResponse<RegisterResponse>(response);
}

export async function resetPassword(
  username: string,
  newPassword: string,
  signal?: AbortSignal
): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE}/api/auth/reset-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ username, new_password: newPassword }),
    signal,
  });
  return handleResponse<{ success: boolean; message: string }>(response);
}

export async function getSecurityQuestion(
  username: string,
  signal?: AbortSignal
): Promise<{ has_question: boolean; question: string }> {
  const response = await fetch(`${API_BASE}/api/auth/security-question?username=${encodeURIComponent(username)}`, {
    method: 'GET',
    signal,
  });
  return handleResponse<{ has_question: boolean; question: string }>(response);
}

export async function forgotPassword(
  username: string,
  securityAnswer: string,
  newPassword: string,
  signal?: AbortSignal
): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE}/api/auth/forgot-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      username,
      security_answer: securityAnswer,
      new_password: newPassword,
    }),
    signal,
  });
  return handleResponse<{ success: boolean; message: string }>(response);
}

export async function getCurrentUser(signal?: AbortSignal): Promise<UserInfo> {
  const response = await fetch(`${API_BASE}/api/auth/me`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<UserInfo>(response);
}

export async function updateCurrentUserProfile(
  request: UpdateCurrentUserProfileRequest,
  signal?: AbortSignal
): Promise<UserInfo> {
  const response = await fetch(`${API_BASE}/api/auth/me`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<UserInfo>(response);
}

export async function changeCurrentUserPassword(
  request: ChangeCurrentUserPasswordRequest,
  signal?: AbortSignal
): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE}/api/auth/change-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<{ success: boolean; message: string }>(response);
}

export async function setCurrentUserSecurityQuestion(
  request: SetCurrentUserSecurityQuestionRequest,
  signal?: AbortSignal
): Promise<UserInfo> {
  const response = await fetch(`${API_BASE}/api/auth/security-question`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<UserInfo>(response);
}

export async function listUsers(signal?: AbortSignal): Promise<UserInfo[]> {
  const response = await fetch(`${API_BASE}/api/auth/users`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<UserInfo[]>(response);
}

export async function deleteUser(
  username: string,
  signal?: AbortSignal
): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE}/api/auth/users/${encodeURIComponent(username)}`, {
    method: 'DELETE',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ success: boolean; message: string }>(response);
}

// ==================== Customer API ====================

export async function listCustomers(
  search?: string,
  signal?: AbortSignal
): Promise<CustomerListItem[]> {
  const params = search ? `?search=${encodeURIComponent(search)}` : '';
  const response = await fetch(`${API_BASE}/api/customers${params}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerListItem[]>(response);
}

export async function getCustomerDetail(
  recordId: string,
  signal?: AbortSignal
): Promise<CustomerDetail> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(recordId)}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerDetail>(response);
}

export async function getCustomerKycProfile(
  customerId: string,
  signal?: AbortSignal
): Promise<CustomerKycProfileResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/kyc-profile`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerKycProfileResponse>(response);
}

export async function getCustomerFinancingKycDiagnostic(
  customerId: string,
  signal?: AbortSignal
): Promise<FinancingKycDiagnosticResult> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-kyc-diagnostic`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<FinancingKycDiagnosticResult>(response);
}

export async function getCustomerFinancingDiagnosticReport(
  customerId: string,
  signal?: AbortSignal
): Promise<CustomerFinancingDiagnosticReport> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-diagnostic-report`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerFinancingDiagnosticReport>(response);
}

export async function saveCustomerFinancingDiagnosticReportSnapshot(
  customerId: string
): Promise<SaveFinancingDiagnosticReportSnapshotResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-diagnostic-report/snapshot`, {
    method: 'POST',
    headers: { ...getAuthHeaders() },
  });
  return handleResponse<SaveFinancingDiagnosticReportSnapshotResponse>(response);
}

export async function listCustomerFinancingDiagnosticReportSnapshots(
  customerId: string,
  signal?: AbortSignal
): Promise<FinancingDiagnosticReportSnapshotSummary[]> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-diagnostic-report/snapshots`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<FinancingDiagnosticReportSnapshotSummary[]>(response);
}

export async function getCustomerFinancingDiagnosticReportSnapshot(
  customerId: string,
  reportId: string,
  signal?: AbortSignal
): Promise<FinancingDiagnosticReportSnapshotDetail> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/financing-diagnostic-report/snapshots/${encodeURIComponent(reportId)}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<FinancingDiagnosticReportSnapshotDetail>(response);
}

export async function exportCustomerFinancingDiagnosticReportSnapshotDocx(
  customerId: string,
  reportId: string,
  signal?: AbortSignal
): Promise<void> {
  const { blob, fileName } = await fetchSnapshotExportBlob(customerId, reportId, 'docx', signal);
  downloadBlob(blob, fileName);
}

export async function exportCustomerFinancingDiagnosticReportSnapshotPdf(
  customerId: string,
  reportId: string,
  signal?: AbortSignal
): Promise<void> {
  const { blob, fileName } = await fetchSnapshotExportBlob(customerId, reportId, 'pdf', signal);
  downloadBlob(blob, fileName);
}

/** Fetch only same-origin credit snapshot endpoints with the existing auth flow. */
export async function fetchCreditReportArtifact(path: string): Promise<{ blob: Blob; fileName: string }> {
  if (!/^\/api\/customers\/[^/]+\/credit-report\/snapshots\/[a-f0-9]{32}\/(preview|export\/pdf)$/.test(path)) {
    throw new Error('报告链接无效，请重新生成报告');
  }
  const response = await fetch(buildApiUrl(path), { headers: getAuthHeaders() });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || '报告读取失败，请稍后重试');
  }
  const disposition = response.headers.get('Content-Disposition') || '';
  const encoded = disposition.match(/filename\*=UTF-8''([^;]+)/i)?.[1];
  return { blob: await response.blob(), fileName: encoded ? decodeURIComponent(encoded) : '征信速览报告.pdf' };
}

export async function downloadCreditReportPdf(path: string): Promise<void> {
  const { blob, fileName } = await fetchCreditReportArtifact(path);
  downloadBlob(blob, fileName);
}

/** Fetch one frozen comprehensive report through the existing authenticated API. */
export async function fetchComprehensiveReportArtifact(path: string): Promise<{ blob: Blob; fileName: string }> {
  if (!/^\/api\/customers\/[^/]+\/comprehensive-financing-report\/snapshots\/[a-f0-9]{32}\/(preview|export\/pdf)$/.test(path)) {
    throw new Error('综合报告链接无效，请重新生成报告');
  }
  const response = await fetch(buildApiUrl(path), { headers: getAuthHeaders() });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || '综合报告读取失败，请稍后重试');
  }
  const disposition = response.headers.get('Content-Disposition') || '';
  const encoded = disposition.match(/filename\*=UTF-8''([^;]+)/i)?.[1];
  return { blob: await response.blob(), fileName: encoded ? decodeURIComponent(encoded) : '客户综合融资分析报告.pdf' };
}

export async function downloadComprehensiveReportPdf(path: string): Promise<void> {
  const { blob, fileName } = await fetchComprehensiveReportArtifact(path);
  downloadBlob(blob, fileName);
}

export async function getCustomerExtractions(
  customerId: string,
  signal?: AbortSignal,
  includeData = false
): Promise<ExtractionGroup[]> {
  const query = includeData ? '?include_data=true' : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/extractions${query}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ExtractionGroup[]>(response);
}

export async function getCustomerDocumentStatus(
  customerId: string,
  signal?: AbortSignal
): Promise<{ items: CustomerDocumentListItem[]; total: number }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/document-status`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ items: CustomerDocumentListItem[]; total: number }>(response);
}

export async function getCustomerDocuments(
  customerId: string,
  signal?: AbortSignal
): Promise<CustomerDocumentListItem[]> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/documents`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerDocumentListItem[]>(response);
}

export async function getLatestEnterpriseFlowExtraction(
  customerId: string,
  signal?: AbortSignal
): Promise<{ item: Record<string, unknown> | null }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/latest`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ item: Record<string, unknown> | null }>(response);
}

export async function getCustomerEnterpriseFlowSummary(
  customerId: string,
  signal?: AbortSignal
): Promise<{ item: Record<string, unknown> | null }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/summary`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ item: Record<string, unknown> | null }>(response);
}

export async function getCustomerPersonalFlowSummary(
  customerId: string,
  signal?: AbortSignal
): Promise<{ item: Record<string, unknown> | null }> {
  const debugQuery = import.meta.env.DEV ? '?debug=true' : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/personal-flow/summary${debugQuery}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ item: Record<string, unknown> | null }>(response);
}

// Alias kept for feature-specific imports.
export const getPersonalFlowSummary = getCustomerPersonalFlowSummary;

export type PersonalFlowIncomeConfirmationPayload = {
  income_type?: 'suspected_salary';
  target_type?: 'confirmed_salary';
  counterparty_name: string;
  amount: number;
  months?: string[];
  transaction_ids?: string[];
  manual_status: 'confirmed' | 'rejected' | 'pending';
  reason?: string;
};

export async function savePersonalFlowIncomeConfirmation(
  customerId: string,
  documentId: string,
  payload: PersonalFlowIncomeConfirmationPayload,
  signal?: AbortSignal
): Promise<{
  success: boolean;
  item: Record<string, unknown>;
  summary?: Record<string, unknown> | null;
  profile_markdown?: string;
}> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/personal-flow/${encodeURIComponent(documentId)}/income-confirmations`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(payload),
      signal,
    }
  );
  return handleResponse<{
    success: boolean;
    item: Record<string, unknown>;
    summary?: Record<string, unknown> | null;
    profile_markdown?: string;
  }>(response);
}

export async function getEnterpriseFlowRules(
  customerId: string,
  signal?: AbortSignal
): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/rules`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function saveEnterpriseFlowRules(
  customerId: string,
  payload: Record<string, unknown>,
  signal?: AbortSignal
): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/rules`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(payload),
    signal,
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function reviewEnterpriseFlowTransaction(
  customerId: string,
  transactionId: string,
  payload: Record<string, unknown>,
  signal?: AbortSignal
): Promise<Record<string, unknown>> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/transactions/${encodeURIComponent(transactionId)}/review`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(payload),
      signal,
    }
  );
  return handleResponse<Record<string, unknown>>(response);
}

export async function getEnterpriseFlowExcludedTransactions(
  customerId: string,
  nature = 'all',
  signal?: AbortSignal
): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/enterprise-flow/excluded-transactions?nature=${encodeURIComponent(nature)}`,
    {
      method: 'GET',
      headers: { ...getAuthHeaders() },
      signal,
    }
  );
  return handleResponse<{ items: Record<string, unknown>[]; total: number }>(response);
}

export async function updateExtractionField(
  customerId: string,
  extractionId: string,
  field: string,
  value: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/extractions/${encodeURIComponent(extractionId)}`,
    {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify({ field, value }),
      signal,
    }
  );
  return handleResponse<{ success: boolean }>(response);
}

export async function deleteCustomerDocument(
  customerId: string,
  docId: string,
  signal?: AbortSignal
): Promise<{ success: boolean; document_id?: string; message?: string }> {
  const response = await fetch(
    `${API_BASE}/api/customers/${encodeURIComponent(customerId)}/documents/${encodeURIComponent(docId)}`,
    {
      method: 'DELETE',
      headers: { ...getAuthHeaders() },
      signal,
    }
  );
  return handleResponse<{ success: boolean }>(response);
}

export async function deleteCustomer(
  customerId: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}`, {
    method: 'DELETE',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}

export async function getCustomerProfileMarkdown(
  customerId: string,
  signal?: AbortSignal,
  force = false
): Promise<CustomerProfileMarkdownResponse> {
  const query = force ? '?force=true' : '';
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/profile-markdown${query}`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerProfileMarkdownResponse>(response);
}

export async function updateCustomerProfileMarkdown(
  customerId: string,
  request: UpdateCustomerProfileMarkdownRequest,
  signal?: AbortSignal
): Promise<CustomerProfileMarkdownResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/profile-markdown`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<CustomerProfileMarkdownResponse>(response);
}

export async function deleteCustomerProfileMarkdown(
  customerId: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/profile-markdown`, {
    method: 'DELETE',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}

export async function customerRagChat(
  customerId: string,
  request: CustomerRagChatRequest,
  signal?: AbortSignal
): Promise<CustomerRagChatResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/rag-chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify(request),
    signal,
  });
  return handleResponse<CustomerRagChatResponse>(response);
}

export async function generateCustomerRiskReport(
  customerId: string,
  signal?: AbortSignal
): Promise<CustomerRiskReportResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/risk-report/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerRiskReportResponse>(response);
}

export async function runFinancingAgent(
  customerId: string,
  taskType = 'full',
  signal?: AbortSignal
): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/agent/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ task_type: taskType }),
    signal,
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function getLatestFinancingAgent(
  customerId: string,
  signal?: AbortSignal
): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/agent/latest`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<Record<string, unknown>>(response);
}

export async function createCustomerRiskReportJob(
  customerId: string,
  signal?: AbortSignal
): Promise<ChatJobCreateResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/risk-report/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ChatJobCreateResponse>(response);
}

// ==================== Customer Table API ====================

/**
 * Get the customer summary table.
 */
export async function getCustomersTable(signal?: AbortSignal): Promise<CustomerTableRow[]> {
  const response = await fetch(`${API_BASE}/api/customers/table`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<CustomerTableRow[]>(response);
}

export async function updateCustomerField(
  customerId: string,
  field: string,
  value: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/fields`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ field, value }),
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}

/**
 * Get dynamic table field definitions.
 */
export async function getTableFields(signal?: AbortSignal): Promise<TableField[]> {
  const response = await fetch(`${API_BASE}/api/customers/fields`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<TableField[]>(response);
}

/**
 * Update a table field display name.
 */
export async function updateTableField(
  fieldId: string,
  fieldName: string,
  signal?: AbortSignal
): Promise<{ success: boolean }> {
  const response = await fetch(`${API_BASE}/api/customers/fields/${encodeURIComponent(fieldId)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
    body: JSON.stringify({ field_name: fieldName }),
    signal,
  });
  return handleResponse<{ success: boolean }>(response);
}


