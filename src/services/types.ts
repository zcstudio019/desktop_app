/**
 * TypeScript type definitions for API communication
 * 
 * These types match the backend FastAPI schemas and provide
 * type safety for all API requests and responses.
 * 
 * Feature: frontend-backend-integration
 */

// ============================================
// File Processing Types
// ============================================

/**
 * Response from file processing endpoint
 */
export interface FileProcessResponse {
  /** Type of document detected/processed (e.g., 'enterprise_credit', 'personal_credit') */
  documentType: string;
  /** Extracted content as key-value pairs */
  content: Record<string, unknown>;
  /** Customer name extracted from document, if available */
  customerName: string | null;
}

export interface KycExtractionResult {
  agent_type?: string;
  doc_type: string;
  doc_type_name: string;
  owner_type?: string;
  extraction_status: 'success' | 'partial' | 'failed';
  fields: Record<string, unknown>;
  validation?: {
    is_valid?: boolean;
    warnings?: string[];
    errors?: string[];
  };
  confidence?: {
    overall?: number;
    fields?: Record<string, number>;
  };
  evidence?: Record<string, unknown>;
  missing_fields?: string[];
  markdown?: string;
}

// ============================================
// Storage Save Types
// ============================================

/**
 * Request to save extracted data to local storage
 */
export interface StorageSaveRequest {
  /** Type of document being saved */
  documentType: string;
  /** Customer name for record matching */
  customerName: string;
  /** Stable customer ID for forcing the save into the current customer context */
  customerId?: string | null;
  /** Content to save */
  content: Record<string, unknown>;
  /** Original uploaded file name for structured summary tracing */
  fileName?: string;
  /** Legacy optional raw file payload; raw binaries are skipped by default in local storage */
  fileContent?: string;
}

/**
 * Response from local save operation
 */
export interface StorageSaveResponse {
  /** Whether the save was successful */
  success: boolean;
  /** Record ID if saved successfully */
  recordId: string | null;
  /** Backing document ID for original preview/download */
  documentId?: string | null;
  /** Stable customer context ID for later profile/RAG/report operations */
  customerId?: string | null;
  /** Whether the original file was retained */
  originalAvailable?: boolean;
  /** Whether a new record was created (vs updated) */
  isNew: boolean;
  /** Error message if save failed */
  error: string | null;
}

export interface DocumentDetailResponse {
  document_id?: string;
  doc_id: string;
  customer_id: string;
  customer_name: string;
  document_type?: string;
  source_file?: string;
  file_name: string;
  file_type: string;
  file_type_name: string;
  file_size: number;
  upload_time: string;
  created_at?: string;
  updated_at?: string;
  report_markdown?: string;
  reportMarkdown?: string;
  extraction?: Record<string, unknown>;
  latest_extraction?: Record<string, unknown>;
  latestExtraction?: Record<string, unknown>;
  extracted_json?: Record<string, unknown>;
  structured_json?: Record<string, unknown>;
  original_available: boolean;
  original_status: string;
  store_original: boolean;
}

export interface EnterpriseBankAccountStatement {
  account_id?: string;
  bank_name?: string | null;
  account_name?: string | null;
  account_number?: string | null;
  currency?: string | null;
  sheet_name?: string | null;
  opening_balance?: number | null;
  ending_balance?: number | null;
  total_inflow?: number;
  total_outflow?: number;
  net_cashflow?: number;
  transaction_count?: number;
}

export interface EnterpriseBankStatementSummary {
  raw_total_inflow?: number;
  raw_total_outflow?: number;
  raw_net_cashflow?: number;
  total_inflow?: number;
  total_outflow?: number;
  net_cashflow?: number;
  transaction_count?: number;
  inflow_count?: number;
  outflow_count?: number;
  account_count?: number;
  bank_count?: number;
  average_monthly_inflow?: number | null;
  average_monthly_outflow?: number | null;
  average_monthly_net_cashflow?: number | null;
  max_single_inflow?: number | null;
  max_single_outflow?: number | null;
  low_balance_transaction_count?: number | null;
  low_balance_threshold?: number;
  estimated_operating_inflow?: number | null;
  estimated_operating_outflow?: number | null;
  estimated_operating_net_cashflow?: number | null;
  excluded_internal_transfer_amount?: number | null;
  excluded_related_party_inflow?: number | null;
  excluded_personal_inflow?: number | null;
  internal_transfer_inflow?: number | null;
  internal_transfer_outflow?: number | null;
  internal_transfer_total?: number | null;
  related_party_inflow?: number | null;
  related_party_outflow?: number | null;
  personal_transfer_inflow?: number | null;
  personal_transfer_outflow?: number | null;
  operating_inflow?: number | null;
  operating_outflow?: number | null;
  operating_net_cashflow?: number | null;
  excluded_inflow_total?: number | null;
  excluded_outflow_total?: number | null;
  reviewed_transaction_count?: number | null;
  unreviewed_suspicious_count?: number | null;
}

export interface EnterpriseMonthlyCashflowSummary {
  month?: string;
  inflow?: number;
  outflow?: number;
  net_cashflow?: number;
  inflow_count?: number;
  outflow_count?: number;
  ending_balance?: number | null;
}

export interface EnterpriseCounterpartyStat {
  name?: string;
  account?: string | null;
  bank?: string | null;
  inflow?: number;
  outflow?: number;
  net?: number;
  amount?: number;
  count?: number;
  transaction_count?: number;
  first_date?: string | null;
  last_date?: string | null;
  nature?: string | null;
  exclude_from_operating?: boolean;
  category_guess?: string | null;
  is_internal_transfer?: boolean;
  is_related_party?: boolean;
  is_personal_counterparty?: boolean;
  risk_note?: string | null;
}

export interface EnterpriseCounterpartySummary {
  top_inflow_counterparties?: EnterpriseCounterpartyStat[];
  top_outflow_counterparties?: EnterpriseCounterpartyStat[];
  internal_transfer_counterparties?: EnterpriseCounterpartyStat[];
  related_party_counterparties?: EnterpriseCounterpartyStat[];
  personal_counterparties?: EnterpriseCounterpartyStat[];
  customer_concentration_top5_ratio?: number | null;
  supplier_concentration_top5_ratio?: number | null;
}

export interface EnterpriseRiskSignal {
  code?: string;
  level?: 'low' | 'medium' | 'high' | string;
  title?: string;
  description?: string;
  amount?: number | null;
  ratio?: number | null;
  evidence_refs?: string[];
  suggestion?: string | null;
}

export interface EnterpriseBankStatementRiskAnalysis {
  overall_level?: 'low' | 'medium' | 'high' | string;
  overall_score?: number;
  signals?: EnterpriseRiskSignal[];
  strengths?: string[];
  weaknesses?: string[];
}

export interface EnterpriseFinancingView {
  bank_recognizable_inflow?: number | null;
  adjusted_operating_inflow?: number | null;
  excluded_internal_transfer_amount?: number | null;
  excluded_related_party_inflow?: number | null;
  excluded_personal_inflow?: number | null;
  suggested_credit_products?: string[];
  material_checklist?: string[];
  bank_explanation?: string[];
  conclusion?: string;
}

export interface EnterpriseBankTransaction {
  transaction_id?: string;
  transaction_date?: string | null;
  date?: string | null;
  direction?: string | null;
  counterparty_name?: string | null;
  counterparty_account?: string | null;
  debit_amount?: number | null;
  credit_amount?: number | null;
  amount?: number | null;
  balance?: number | null;
  category?: string | null;
  nature?: string | null;
  exclude_from_operating?: boolean;
  classification_reason?: string | null;
  classification_confidence?: number | null;
  manual_reviewed?: boolean;
  review_status?: string | null;
}

export interface EnterpriseFlowRules {
  customer_id?: string;
  related_company_names?: string[];
  self_account_numbers?: string[];
  internal_transfer_keywords?: string[];
  operating_counterparty_whitelist?: string[];
  internal_counterparty_blacklist?: string[];
  personal_counterparty_names?: string[];
  manual_overrides?: Record<string, unknown>;
}

export interface EnterpriseFlowViewData {
  inflow?: number;
  outflow?: number;
  transactions?: EnterpriseBankTransaction[];
}

export interface EnterpriseFlowClassificationSummaryItem {
  count?: number;
  inflow?: number;
  outflow?: number;
}

export interface EnterpriseBankStatementExtraction {
  document_type?: string;
  normalized_document_type?: string;
  company_name?: string | null;
  source_file?: string | null;
  statement_period?: {
    start_date?: string | null;
    end_date?: string | null;
    months_count?: number | null;
  };
  accounts?: EnterpriseBankAccountStatement[];
  transactions?: EnterpriseBankTransaction[];
  views?: {
    raw?: EnterpriseFlowViewData;
    operating?: EnterpriseFlowViewData;
    excluded?: EnterpriseFlowViewData;
  };
  classification_summary?: Record<string, EnterpriseFlowClassificationSummaryItem>;
  summary?: EnterpriseBankStatementSummary;
  monthly_summary?: EnterpriseMonthlyCashflowSummary[];
  counterparty_summary?: EnterpriseCounterpartySummary;
  risk_analysis?: EnterpriseBankStatementRiskAnalysis;
  financing_view?: EnterpriseFinancingView;
  internal_transfer_summary?: Record<string, unknown>;
  internal_transfer_transactions?: Array<Record<string, unknown>>;
  evidence?: unknown[];
  warnings?: string[];
}

export interface CustomerDocumentListItem {
  doc_id: string;
  customer_id: string;
  file_name: string;
  file_type: string;
  file_type_name: string;
  file_size: number;
  upload_time: string;
  original_available: boolean;
  original_status: string;
  store_original: boolean;
  is_latest: boolean;
}

// Legacy aliases kept for compatibility with existing imports.
export type FeishuSaveRequest = StorageSaveRequest;
export type FeishuSaveResponse = StorageSaveResponse;

// ============================================
// Application Generation Types
// ============================================

/**
 * Request to generate loan application
 */
export interface ApplicationRequest {
  /** Customer name to generate application for */
  customerName: string;
  /** Stable customer ID for summary sync */
  customerId?: string | null;
  /** Type of loan */
  loanType: 'enterprise' | 'personal';
}

/**
 * Response from application generation
 */
export interface ApplicationResponse {
  /** Generated application content in Markdown format */
  applicationContent: string;
  /** Structured application data for card rendering */
  applicationData?: Record<string, Record<string, string>>;
  /** Whether customer data was found */
  customerFound: boolean;
  /** Any warnings during generation */
  warnings: string[];
  /** Generation metadata and profile version context */
  metadata?: {
    generated_at?: string;
    customer_id?: string;
    profile_version?: number;
    profile_updated_at?: string;
    data_sources?: string[];
    stale?: boolean;
    stale_reason?: string;
    stale_at?: string;
    saved_application_id?: string;
    previous_application_id?: string;
    saved_application_version_group_id?: string;
    saved_application_version_no?: number;
  };
}

// ============================================
// Scheme Matching Types
// ============================================

/**
 * Request to match customer against loan schemes
 */
export interface SchemeMatchRequest {
  /** Customer data for matching */
  customerData: Record<string, unknown>;
  /** Stable customer ID for snapshot sync */
  customerId?: string | null;
  /** Customer name for snapshot sync */
  customerName?: string | null;
  /** Type of credit to match against */
  creditType: 'personal' | 'enterprise_credit' | 'enterprise_mortgage';
}

/**
 * Response from scheme matching
 */
export interface SchemeMatchResponse {
  /** Matching result in formatted text */
  matchResult: string;
  /** Structured matching result for card rendering */
  matchingData?: Record<string, unknown> | null;
}

// ============================================
// Chat Types
// ============================================

/**
 * A single chat message
 */
export interface ChatMessage {
  /** Role of the message sender */
  role: 'user' | 'assistant' | 'system';
  /** Message content */
  content: string;
  /** Optional reasoning for assistant responses */
  reasoning?: string | null;
  /** Intent for structured task messages */
  intent?: 'extract' | 'application' | 'matching' | 'chat' | null;
  /** Structured payload associated with the message */
  data?: Record<string, unknown> | null;
  /** Weak task association for timeline/result linking */
  relatedJobId?: string | null;
  /** Semantic message category */
  messageType?: 'text' | 'task_result' | 'task_feedback' | 'error';
  /** Client-side created time */
  createdAt?: string;
  /** Client-side message id for optimistic rendering */
  clientMessageId?: string;
  /** Local delivery status for optimistic rendering */
  deliveryStatus?: 'pending' | 'sent' | 'failed';
  /** Task-aware message lifecycle status */
  status?: 'sending' | 'sent' | 'processing' | 'success' | 'failed';
  /** Optional local delivery error */
  deliveryError?: string | null;
}

/**
 * File attachment for chat
 */
export interface ChatFile {
  /** File name */
  name: string;
  /** MIME type */
  type: string;
  /** Base64 encoded content */
  content: string;
}

/**
 * Similar customer candidate returned when a name collision is detected
 */
export interface SimilarCustomer {
  customer_id: string;
  name: string;
  shared_keywords: string[];
}

/**
 * Request to send chat message
 */
export interface ChatRequest {
  /** Conversation history */
  messages: ChatMessage[];
  /** Optional file attachments */
  files?: ChatFile[];
  /** Optional persisted chat session ID */
  sessionId?: string | null;
  /** Current selected customer context */
  customerId?: string | null;
  /** Current selected customer name */
  customerName?: string | null;
  /** User merge decisions: { customerName -> target_customer_id } */
  mergeDecisions?: Record<string, string>;
}

/**
 * Response from chat endpoint
 */
export interface ChatResponse {
  /** AI response message */
  message: string;
  /** Detected intent, if any */
  intent: 'extract' | 'application' | 'matching' | 'chat' | null;
  /** Additional data based on intent */
  data: Record<string, unknown> | null;
  /** AI reasoning/thinking process (from DeepSeek thinking feature) */
  reasoning?: string | null;
}

export interface ChatSessionSummary {
  sessionId: string;
  title?: string;
  customerId?: string | null;
  customerName?: string | null;
  lastMessagePreview?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface ChatJobCreateResponse {
  jobId: string;
  job_id?: string;
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | 'timeout' | 'interrupted' | string;
  message?: string;
  enqueue_success?: boolean;
}

export interface ChatIntentAsyncJob {
  jobId: string;
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | 'timeout' | 'interrupted' | string;
  jobType?: string | null;
  customerId?: string | null;
  customerName?: string | null;
  targetPage?: string | null;
}

export interface ChatJobSummaryResponse {
  jobId: string;
  jobType: string;
  jobTypeLabel?: string;
  customerId: string;
  customerName: string;
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | 'timeout' | 'interrupted' | string;
  progressMessage: string;
  errorMessage?: string | null;
  createdAt: string;
  startedAt: string;
  finishedAt: string;
  targetPage?: string | null;
  resultSummary?: string | null;
}

export interface ChatJobStatusResponse {
  jobId: string;
  jobType: string;
  jobTypeLabel?: string;
  customerId: string;
  customerName: string;
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | 'timeout' | 'interrupted' | string;
  progressMessage: string;
  result: Record<string, unknown> | null;
  errorMessage?: string | null;
  createdAt: string;
  startedAt: string;
  finishedAt: string;
  targetPage?: string | null;
  resultSummary?: string | null;
}


// ============================================
// Error Types
// ============================================

/**
 * Custom error class for API errors
 * Contains HTTP status code and error message from response body
 * 
 * Feature: frontend-backend-integration, Property 2: API Error Handling Consistency
 */
export class ApiError extends Error {
  /** HTTP status code */
  public status: number;
  /** Additional error details from the response */
  public details?: unknown;

  constructor(status: number, message: string, details?: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.details = details;
    
    // Maintains proper stack trace for where error was thrown (V8 engines)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ApiError);
    }
  }
}

// ============================================
// Error Classification Types
// ============================================

/**
 * Classification of error types for handling
 */
export enum ErrorType {
  /** Validation errors (4xx status codes) */
  VALIDATION = 'validation',
  /** Service errors (5xx status codes) */
  SERVICE = 'service',
  /** Network errors (no response) */
  NETWORK = 'network',
  /** Request was cancelled */
  CANCELLED = 'cancelled',
}

/**
 * Classify an error into a type for appropriate handling
 * 
 * Feature: frontend-backend-integration, Property 12: Error Classification by Status
 */
export function classifyError(error: unknown): ErrorType {
  if (error instanceof ApiError) {
    if (error.status >= 400 && error.status < 500) {
      return ErrorType.VALIDATION;
    }
    if (error.status >= 500) {
      return ErrorType.SERVICE;
    }
  }
  if (error instanceof DOMException && error.name === 'AbortError') {
    return ErrorType.CANCELLED;
  }
  return ErrorType.NETWORK;
}

// ============================================
// Auth Types
// ============================================

/**
 * Login request payload
 */
export interface LoginRequest {
  username: string;
  password: string;
}

/**
 * Login response from /api/auth/login
 */
export interface LoginResponse {
  token: string;
  username: string;
  role: string;
}

/**
 * Register request payload
 */
export interface RegisterRequest {
  username: string;
  password: string;
  security_question?: string;
  security_answer?: string;
}

/**
 * Register response from /api/auth/register
 */
export interface RegisterResponse {
  username: string;
  role: string;
}

/**
 * Security question response from /api/auth/security-question
 */
export interface SecurityQuestionResponse {
  has_question: boolean;
  question: string;
}

/**
 * Forgot password request payload
 */
export interface ForgotPasswordRequest {
  username: string;
  security_answer: string;
  new_password: string;
}

/**
 * User info from /api/auth/me or /api/auth/users
 */
export interface UserInfo {
  username: string;
  role: string;
  created_at?: string;
  last_login_at?: string;
  updated_at?: string;
  display_name?: string;
  phone?: string;
  has_security_question?: boolean;
}

export interface UpdateCurrentUserProfileRequest {
  display_name?: string;
  phone?: string;
}

export interface ChangeCurrentUserPasswordRequest {
  current_password: string;
  new_password: string;
}

export interface SetCurrentUserSecurityQuestionRequest {
  security_question: string;
  security_answer: string;
}

// ============================================
// Customer List Types
// ============================================

/**
 * Customer list item from /api/customers
 */
export interface CustomerListItem {
  name: string;
  record_id: string;
  uploader: string;
  upload_time: string;
  customer_type: string;
  risk_level?: string;
  last_report_generated_at?: string;
  profile_version?: number | null;
}

/**
 * Customer detail with all stored fields from /api/customers/{record_id}
 */
export interface CustomerDetail {
  name: string;
  record_id: string;
  uploader: string;
  upload_time: string;
  fields: Record<string, string>;
}

export interface CustomerProfileMarkdownResponse {
  customer_id: string;
  customer_name: string;
  markdown_content: string;
  source_mode: 'auto' | 'manual' | string;
  auto_generated: boolean;
  version: number;
  updated_at?: string | null;
  rag_source_priority: string[];
  risk_report_schema: Record<string, unknown>;
  credit_debug?: Record<string, unknown>;
}

export interface CustomerKycProfile {
  customer_id: string;
  person_identity?: Record<string, unknown>;
  enterprise_identity?: Record<string, unknown>;
  bank_account?: Record<string, unknown>;
  marriage?: Record<string, unknown>;
  assets?: {
    properties?: Array<Record<string, unknown>>;
    vehicles?: Array<Record<string, unknown>>;
  };
  licenses?: Array<Record<string, unknown>>;
  documents?: Array<Record<string, unknown>>;
  updated_at?: string;
}

export interface KycCompletenessResult {
  completeness_score: number;
  required_missing: string[];
  optional_missing: string[];
  warnings: string[];
  conflicts: string[];
  suggestions: string[];
}

export interface CustomerKycProfileResponse {
  profile: CustomerKycProfile;
  completeness: KycCompletenessResult;
}

export interface ExtractionReviewResult {
  document_id: string;
  doc_type: string;
  doc_type_name: string;
  agent_type: string;
  extracted_data: Record<string, unknown>;
  confirmed_data: {
    confirmed_fields?: Record<string, unknown>;
    confirm_status?: string;
    confirmed_by?: string;
    confirmed_at?: string;
    [key: string]: unknown;
  };
  merged_fields: Record<string, unknown>;
  confirm_status: 'unconfirmed' | 'partial' | 'confirmed' | string;
  confirmed_by?: string;
  confirmed_at?: string;
  validation?: {
    is_valid?: boolean;
    warnings?: string[];
    errors?: string[];
  };
  evidence?: Record<string, unknown>;
  missing_fields?: string[];
}

export interface UpdateExtractionReviewPayload {
  confirmed_fields: Record<string, unknown>;
  confirm_status: 'partial' | 'confirmed';
}

export interface FinancingKycDiagnosticResult {
  customer_id: string;
  diagnostic_type: 'kyc_financing_readiness' | string;
  material_completeness_score: number;
  usable_for_financing: boolean;
  readiness_level: 'not_ready' | 'basic_ready' | 'ready' | string;
  summary: string;
  identity_status: 'missing' | 'partial' | 'complete' | string;
  enterprise_status: 'missing' | 'partial' | 'complete' | string;
  bank_account_status: 'missing' | 'partial' | 'complete' | string;
  asset_status: 'none' | 'partial' | 'complete' | string;
  key_risks: string[];
  missing_materials: string[];
  conflicts: string[];
  recommended_actions: string[];
  next_step: string;
}

export interface EnterpriseCreditDiagnostic {
  has_enterprise_credit_report: boolean;
  credit_status: 'unknown' | 'normal' | 'attention' | 'risky' | string;
  debt_summary: {
    total_unsettled_balance?: number | null;
    short_term_loan_balance?: number | null;
    long_term_loan_balance?: number | null;
    credit_limit_total?: number | null;
    used_credit_total?: number | null;
    credit_usage_rate?: number | null;
  };
  loan_summary: {
    active_loan_count: number;
    upcoming_due_loans: Array<Record<string, unknown>>;
    overdue_loans: Array<Record<string, unknown>>;
    abnormal_classification_loans: Array<Record<string, unknown>>;
  };
  guarantee_summary: {
    has_external_guarantee: boolean;
    external_guarantee_balance?: number | null;
    guarantee_risks: string[];
  };
  key_risks: string[];
  positive_signals: string[];
  recommended_actions: string[];
  summary: string;
}

export interface PersonalCreditDiagnostic {
  has_personal_credit_report: boolean;
  credit_status: 'unknown' | 'normal' | 'attention' | 'risky' | string;
  debt_summary: {
    loan_balance?: number | null;
    credit_card_used_amount?: number | null;
    external_guarantee_balance?: number | null;
  };
  overdue_summary: {
    has_loan_overdue: boolean;
    has_credit_card_overdue: boolean;
    overdue_records: Array<Record<string, unknown>>;
  };
  query_summary: {
    last_3_months_query_count?: number | null;
    last_6_months_query_count?: number | null;
    query_risk_level: 'unknown' | 'low' | 'medium' | 'high' | string;
  };
  serious_negative_summary: {
    has_serious_negative: boolean;
    items: Array<Record<string, unknown>>;
  };
  key_risks: string[];
  positive_signals: string[];
  recommended_actions: string[];
  summary: string;
}

export interface EnterpriseBankFlowDiagnostic {
  has_enterprise_bank_flow: boolean;
  flow_status: 'unknown' | 'normal' | 'attention' | 'risky' | string;
  summary_metrics: {
    period_start?: string | null;
    period_end?: string | null;
    month_count: number;
    total_income?: number | null;
    total_expense?: number | null;
    net_income?: number | null;
    average_monthly_income?: number | null;
    average_monthly_expense?: number | null;
    average_monthly_net_income?: number | null;
  };
  quality_metrics: {
    stable_month_count: number;
    zero_or_low_income_month_count: number;
    large_in_out_count: number;
    internal_transfer_amount?: number | null;
    internal_transfer_ratio?: number | null;
    real_income_amount?: number | null;
    real_income_ratio?: number | null;
  };
  account_consistency: {
    account_name: string;
    company_name: string;
    is_consistent?: boolean | null;
    warnings: string[];
  };
  key_risks: string[];
  positive_signals: string[];
  recommended_actions: string[];
  summary: string;
}

export interface FinancialStatementDiagnostic {
  has_financial_statement: boolean;
  financial_status: 'unknown' | 'normal' | 'attention' | 'risky' | string;
  period: {
    latest_period?: string | null;
    statement_type?: string | null;
  };
  profitability: {
    revenue?: number | null;
    operating_cost?: number | null;
    gross_profit?: number | null;
    net_profit?: number | null;
    net_profit_margin?: number | null;
  };
  debt_capacity: {
    total_assets?: number | null;
    total_liabilities?: number | null;
    owner_equity?: number | null;
    asset_liability_ratio?: number | null;
    short_term_borrowing?: number | null;
    long_term_borrowing?: number | null;
  };
  liquidity: {
    current_assets?: number | null;
    current_liabilities?: number | null;
    current_ratio?: number | null;
    cash_balance?: number | null;
  };
  cash_flow: {
    operating_cash_flow_net?: number | null;
  };
  key_risks: string[];
  positive_signals: string[];
  recommended_actions: string[];
  summary: string;
}

export interface ComprehensiveFinancingAdvice {
  overall_status: 'not_ready' | 'cautious' | 'recommendable' | 'high_quality' | string;
  financing_readiness_score: number;
  recommended_product_directions: Array<{
    product_type:
      | 'mortgage_loan'
      | 'credit_business_loan'
      | 'tax_invoice_loan'
      | 'bank_flow_loan'
      | 'renewal_or_refinance'
      | 'short_term_turnover'
      | 'defer_application'
      | string;
    product_name: string;
    fit_level: 'high' | 'medium' | 'low' | 'not_suitable' | string;
    reason: string;
  }>;
  main_shortcomings: string[];
  key_strengths: string[];
  priority_actions: string[];
  risk_summary: string[];
  sales_follow_up_script: string;
  summary: string;
}

export interface CustomerFinancingDiagnosticReport {
  customer_id: string;
  report_type: 'customer_financing_diagnostic' | string;
  report_status: 'draft' | string;
  customer_summary: {
    customer_name?: string;
    customer_type?: string;
    phone?: string;
    intent_level?: string;
    status?: string;
    [key: string]: unknown;
  };
  kyc_diagnostic: FinancingKycDiagnosticResult | Record<string, unknown>;
  enterprise_credit_diagnostic?: EnterpriseCreditDiagnostic;
  personal_credit_diagnostic?: PersonalCreditDiagnostic;
  enterprise_bank_flow_diagnostic?: EnterpriseBankFlowDiagnostic;
  financial_statement_diagnostic?: FinancialStatementDiagnostic;
  comprehensive_financing_advice?: ComprehensiveFinancingAdvice;
  material_checklist: {
    required_missing: string[];
    optional_missing: string[];
    recommended_supplements: string[];
  };
  risk_highlights: string[];
  financing_readiness: {
    usable_for_financing: boolean;
    readiness_level: 'not_ready' | 'basic_ready' | 'ready' | string;
    score: number;
    summary: string;
  };
  next_actions: string[];
  report_markdown: string;
}

export interface FinancingDiagnosticReportSnapshotSummary {
  id: string;
  report_version: string;
  report_status: 'draft' | string;
  generated_by: string;
  generated_at: string;
  source_summary: {
    has_kyc_profile?: boolean;
    has_enterprise_credit_report?: boolean;
    has_personal_credit_report?: boolean;
    has_enterprise_bank_flow?: boolean;
    has_financial_statement?: boolean;
    overall_status?: string;
    financing_readiness_score?: number;
    [key: string]: unknown;
  };
  summary: string;
}

export interface FinancingDiagnosticReportSnapshotDetail {
  id: string;
  customer_id: string;
  report_version: string;
  report_status: 'draft' | string;
  report_json: CustomerFinancingDiagnosticReport | Record<string, unknown>;
  report_markdown: string;
  source_summary: FinancingDiagnosticReportSnapshotSummary['source_summary'];
  generated_by: string;
  generated_at: string;
}

export interface SaveFinancingDiagnosticReportSnapshotResponse {
  success: boolean;
  report_id: string;
  report_version: string;
  generated_at: string;
  message: string;
}

export interface UpdateCustomerProfileMarkdownRequest {
  markdown_content: string;
  title?: string;
}

export interface CustomerRagChatRequest {
  question: string;
}

export interface RagEvidenceItem {
  source_type: string;
  text: string;
  score: number;
}

export interface CustomerRagChatResponse {
  answer: string;
  evidence: RagEvidenceItem[];
  missing_info: string[];
}

export interface RiskReportBasisItem {
  source_type: string;
  text: string;
  score: number;
}

export interface RiskDimensionAssessment {
  dimension: string;
  score: number;
  risk_level: 'low' | 'medium' | 'high' | string;
  summary: string;
  basis: RiskReportBasisItem[];
  missing_info: string[];
}

export interface CustomerRiskReportJson {
  generated_at?: string;
  profile_version?: number;
  profile_updated_at?: string;
  customer_summary: {
    customer_id: string;
    customer_name: string;
    customer_type: string;
    industry: string;
    financing_need: string;
    data_completeness: {
      status: string;
      score: number;
      missing_items: string[];
    };
  };
  overall_assessment: {
    total_score: number;
    risk_level: 'low' | 'medium' | 'high' | string;
    conclusion: string;
    immediate_application_recommended: boolean;
    basis: RiskReportBasisItem[];
  };
  risk_dimensions: RiskDimensionAssessment[];
  matched_schemes: {
    has_match: boolean;
    items: Array<{
      product_name: string;
      estimated_limit: string;
      estimated_rate: string;
      match_reason: string;
      constraints: string[];
      basis: RiskReportBasisItem[];
    }>;
  };
  no_match_analysis: {
    has_no_match_issue: boolean;
    reasons: string[];
    core_shortboards: string[];
    basis: RiskReportBasisItem[];
  };
  optimization_suggestions: {
    short_term: string[];
    mid_term: string[];
    document_supplement: string[];
    credit_optimization: string[];
    debt_optimization: string[];
  };
  financing_plan: {
    current_stage: string;
    one_to_three_months: string[];
    three_to_six_months: string[];
    alternative_paths: string[];
  };
  final_recommendation: {
    action: string;
    priority_product_types: string[];
    next_steps: string[];
    basis: RiskReportBasisItem[];
  };
}

export interface CustomerRiskReportResponse {
  report_json: CustomerRiskReportJson;
  report_markdown: string;
  generated_at: string;
  profile_version?: number;
  profile_updated_at?: string;
  previous_report?: CustomerRiskReportHistoryItem | null;
}

export interface CustomerRiskReportHistoryItem {
  report_id: string;
  customer_id: string;
  generated_at: string;
  profile_version?: number;
  profile_updated_at?: string;
  report_json: CustomerRiskReportJson;
  report_markdown: string;
}

export interface CustomerRiskReportHistoryResponse {
  items: CustomerRiskReportHistoryItem[];
}

// ============================================
// Extraction Data Types
// ============================================

/**
 * Single extraction record
 */
export interface ExtractionItem {
  extraction_id: string;
  doc_id?: string;
  extraction_type: string;
  extracted_data: Record<string, string>;
  created_at: string;
  extraction_status?: string;
  has_extraction?: boolean;
  summary_available?: boolean;
}

/**
 * Extraction records grouped by document type
 */
export interface ExtractionGroup {
  extraction_type: string;
  items: ExtractionItem[];
}

/**
 * Request to update a single extraction field
 */
export interface UpdateExtractionRequest {
  field: string;
  value: string;
}

// ============================================
// Dynamic Table Field Types
// ============================================

/**
 * A dynamic table field configuration from backend
 */
export interface TableField {
  field_id: string;
  field_name: string;
  field_key: string;
  doc_type: string;
  field_order: number;
  editable: boolean;
}

// ============================================
// Customer Table Types (dynamic fields)
// ============================================

/**
 * Full extraction data for a single cell (OCR fields)
 */
export interface CellFullData {
  /** Short preview string for display in the cell */
  summary: string;
  /** Full structured extraction data for modal display */
  full: Record<string, unknown>;
  /** Source customer ID for edit/delete actions */
  customer_id?: string;
  /** Latest source document ID */
  doc_id?: string;
  /** Latest source extraction ID */
  extraction_id?: string;
  /** Whether the cell content can be edited */
  editable?: boolean;
  /** Whether the source document can be deleted */
  deletable?: boolean;
  /** Individual source items when multiple documents were merged into one cell */
  items?: Array<{
    doc_id: string;
    extraction_id: string;
    summary: string;
    full: Record<string, unknown>;
    editable: boolean;
    deletable: boolean;
  }>;
}

/**
 * A single row in the customer summary table (dynamic fields)
 * Fixed keys: customer_id, name, customer_type (always strings)
 * Dynamic keys: field_key values 鈥?string for editable fields, CellFullData for OCR fields
 */
export type CustomerTableRow = Record<string, string | CellFullData>;

/**
 * Request to update a customer field
 */
export interface UpdateCustomerFieldRequest {
  field: string;
  value: string;
}

