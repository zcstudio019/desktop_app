/**
 * API client for frontend-backend integration.
 *
 * This module provides typed wrappers for all backend API endpoints.
 */

import type {
  ApplicationRequest,
  ApplicationResponse,
  ChatRequest,
  ChatResponse,
  CustomerDetail,
  CustomerListItem,
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
  UserInfo,
} from './types';

import { ApiError } from './types';

export { ApiError };

function resolveApiBase(): string {
  const configuredBase = typeof import.meta !== 'undefined' ? import.meta.env?.VITE_API_BASE : undefined;
  if (configuredBase && typeof configuredBase === 'string') {
    return configuredBase.replace(/\/+$/, '');
  }

  if (typeof window !== 'undefined' && window.location) {
    const origin = window.location.origin;
    const protocol = window.location.protocol;
    const hostname = window.location.hostname;
    const port = window.location.port;

    if (!origin || origin === 'null' || protocol === 'about:') {
      return 'http://127.0.0.1:8000';
    }

    // 开发环境：前端在5173/5174端口，后端在8000端口
    if ((hostname === '127.0.0.1' || hostname === 'localhost') && (port === '5173' || port === '5174')) {
      return 'http://127.0.0.1:8000';
    }

    // 生产环境：前端和后端在同一域名下，使用相同的origin
    return origin.replace(/\/+$/, '');
  }

  return 'http://127.0.0.1:8000';
}

const API_BASE = resolveApiBase();

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
}

export interface Activity {
  id: string;
  type: string;
  time: string;
  status: string;
  fileName?: string;
  fileType?: string;
  customerName?: string;
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
  loanType: string;
  savedAt: string;
}

export interface SavedApplication extends SavedApplicationListItem {
  applicationData: Record<string, unknown>;
}

export interface SaveApplicationRequest {
  customerName: string;
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
