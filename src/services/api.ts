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
  customerName: string;
  customerId?: string | null;
  loanType: string;
  savedAt: string;
}

export interface SavedApplication extends SavedApplicationListItem {
  applicationData: Record<string, unknown>;
}

export interface SaveApplicationRequest {
  customerName: string;
  customerId?: string | null;
  loanType: string;
  applicationData: Record<string, unknown>;
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

export async function getCustomerExtractions(
  customerId: string,
  signal?: AbortSignal
): Promise<ExtractionGroup[]> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/extractions`, {
    method: 'GET',
    headers: { ...getAuthHeaders() },
    signal,
  });
  return handleResponse<ExtractionGroup[]>(response);
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
): Promise<{ success: boolean }> {
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
  signal?: AbortSignal
): Promise<CustomerProfileMarkdownResponse> {
  const response = await fetch(`${API_BASE}/api/customers/${encodeURIComponent(customerId)}/profile-markdown`, {
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


