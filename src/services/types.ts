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
  /** Stable customer context ID for later profile/RAG/report operations */
  customerId?: string | null;
  /** Whether a new record was created (vs updated) */
  isNew: boolean;
  /** Error message if save failed */
  error: string | null;
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
  role: 'user' | 'assistant';
  /** Message content */
  content: string;
  /** Client-side message id for optimistic rendering */
  clientMessageId?: string;
  /** Local delivery status for optimistic rendering */
  deliveryStatus?: 'pending' | 'sent' | 'failed';
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
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | string;
}

export interface ChatIntentAsyncJob {
  jobId: string;
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | string;
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
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | string;
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
  status: 'pending' | 'running' | 'retrying' | 'success' | 'failed' | string;
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
  extraction_type: string;
  extracted_data: Record<string, string>;
  created_at: string;
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

