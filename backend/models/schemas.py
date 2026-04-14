"""
Pydantic models for request/response validation.

This module defines all the data models used by the FastAPI backend
for validating incoming requests and formatting outgoing responses.
"""

from typing import Any

from pydantic import BaseModel, Field

# =============================================================================
# Request Models
# =============================================================================


class FileProcessRequest(BaseModel):
    """
    Request model for file processing.

    Note: The actual file is received via multipart/form-data,
    not as part of this model. This model is for the optional
    documentType parameter.
    """

    documentType: str | None = Field(
        default=None, description="Optional document type hint (e.g., '企业征信提取', '个人征信提取')"
    )


class FeishuSaveRequest(BaseModel):
    """Request model for saving data to Feishu Bitable."""

    documentType: str = Field(..., description="Document type (e.g., '企业征信提取', '个人征信提取')")
    customerName: str = Field(..., description="Customer name for matching existing records")
    customerId: str | None = Field(default=None, description="Stable customer ID for customer-scoped save/merge")
    content: dict[str, Any] = Field(..., description="Extracted content to save")
    fileName: str | None = Field(default=None, description="Original uploaded file name")
    fileContent: str | None = Field(default=None, description="Base64 encoded original file content")


class ApplicationRequest(BaseModel):
    """Request model for loan application generation."""

    customerName: str = Field(..., description="Customer name to search for")
    customerId: str | None = Field(default=None, description="Stable customer ID for summary sync")
    loanType: str = Field(..., description="Loan type: 'enterprise' or 'personal'")


class SchemeMatchRequest(BaseModel):
    """Request model for scheme matching."""

    customerData: dict[str, Any] = Field(..., description="Customer data for matching")
    customerId: str | None = Field(default=None, description="Stable customer ID for scheme snapshot sync")
    customerName: str | None = Field(default=None, description="Customer name for scheme snapshot sync")
    creditType: str = Field(..., description="Credit type: 'personal', 'enterprise_credit', or 'enterprise_mortgage'")


class SaveApplicationRequest(BaseModel):
    """Request model for saving application to local cache."""

    customerName: str = Field(..., description="Customer name")
    customerId: str | None = Field(default=None, description="Stable customer ID")
    loanType: str = Field(..., description="Loan type: 'enterprise' or 'personal'")
    applicationData: dict[str, Any] = Field(..., description="Application data to save")


class SavedApplication(BaseModel):
    """Model for a saved application."""

    id: str = Field(..., description="Unique application ID")
    customerName: str = Field(..., description="Customer name")
    customerId: str | None = Field(default=None, description="Stable customer ID")
    loanType: str = Field(..., description="Loan type")
    applicationData: dict[str, Any] = Field(..., description="Application data")
    savedAt: str = Field(..., description="ISO format timestamp when saved")


class SavedApplicationListItem(BaseModel):
    """Model for application list item (without full data)."""

    id: str = Field(..., description="Unique application ID")
    customerName: str = Field(..., description="Customer name")
    customerId: str | None = Field(default=None, description="Stable customer ID")
    loanType: str = Field(..., description="Loan type")
    savedAt: str = Field(..., description="ISO format timestamp when saved")


class NaturalLanguageRequest(BaseModel):
    """Request model for natural language parsing."""

    text: str = Field(..., description="Natural language text to parse")
    creditType: str = Field(default="enterprise", description="Credit type: 'enterprise' or 'personal'")


class NaturalLanguageResponse(BaseModel):
    """Response model for natural language parsing."""

    customerData: dict[str, Any] = Field(..., description="Parsed customer data")
    parsedFields: list[str] = Field(default_factory=list, description="List of fields that were parsed")


class ChatMessage(BaseModel):
    """A single message in the chat conversation."""

    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatFile(BaseModel):
    """A file attached to a chat message."""

    name: str = Field(..., description="File name")
    type: str = Field(..., description="MIME type of the file")
    content: str = Field(..., description="Base64 encoded file content")


class ChatRequest(BaseModel):
    """Request model for chat endpoint."""

    messages: list[ChatMessage] = Field(..., description="Conversation history")
    files: list[ChatFile] | None = Field(default=None, description="Optional attached files")
    sessionId: str | None = Field(default=None, description="Optional persisted chat session ID")
    customerId: str | None = Field(default=None, description="Current selected customer ID")
    customerName: str | None = Field(default=None, description="Current selected customer name")
    mergeDecisions: dict[str, str] | None = Field(
        default=None,
        description="User merge decisions: {customerName -> target_customer_id}",
    )


class CustomerRagChatRequest(BaseModel):
    """Request model for customer-scoped RAG chat."""

    question: str = Field(..., description="Question about the current customer")


class RagEvidenceItem(BaseModel):
    """A single evidence snippet returned by RAG."""

    source_type: str = Field(..., description="Retrieval source type")
    text: str = Field(..., description="Matched text snippet")
    score: float = Field(..., description="Similarity score")


class CustomerRagChatResponse(BaseModel):
    """Structured response for customer-scoped RAG chat."""

    answer: str = Field(..., description="Final answer generated from retrieved evidence")
    evidence: list[RagEvidenceItem] = Field(default_factory=list, description="Supporting evidence snippets")
    missing_info: list[str] = Field(default_factory=list, description="Missing information needed for a better answer")


class RiskReportBasisItem(BaseModel):
    """Reusable evidence item for risk reports."""

    source_type: str = Field(default="", description="Evidence source type")
    text: str = Field(default="", description="Evidence text snippet")
    score: float = Field(default=0.0, description="Evidence relevance score")


class RiskDimensionAssessment(BaseModel):
    """Assessment result for one risk dimension."""

    dimension: str = Field(default="", description="Dimension identifier")
    score: int = Field(default=0, description="Dimension score from rules")
    risk_level: str = Field(default="high", description="Dimension risk level: low, medium, high")
    summary: str = Field(default="", description="Narrative explanation for the dimension")
    basis: list[RiskReportBasisItem] = Field(default_factory=list, description="Supporting evidence")
    missing_info: list[str] = Field(default_factory=list, description="Missing information for this dimension")


class CustomerSummaryDataCompleteness(BaseModel):
    """Data completeness summary for a customer."""

    status: str = Field(default="", description="Completeness status label")
    score: int = Field(default=0, description="Completeness score from rules")
    missing_items: list[str] = Field(default_factory=list, description="Missing key materials")


class RiskReportCustomerSummary(BaseModel):
    """Customer overview section."""

    customer_id: str = Field(default="", description="Customer ID")
    customer_name: str = Field(default="", description="Customer name")
    customer_type: str = Field(default="", description="Customer type")
    industry: str = Field(default="", description="Industry")
    financing_need: str = Field(default="", description="Financing need summary")
    data_completeness: CustomerSummaryDataCompleteness = Field(default_factory=CustomerSummaryDataCompleteness)


class RiskReportOverallAssessment(BaseModel):
    """Overall risk conclusion section."""

    total_score: int = Field(default=0, description="Total score from rules")
    risk_level: str = Field(default="high", description="Overall risk level: low, medium, high")
    conclusion: str = Field(default="", description="Overall assessment narrative")
    immediate_application_recommended: bool = Field(default=False, description="Whether immediate application is advised")
    basis: list[RiskReportBasisItem] = Field(default_factory=list, description="Supporting evidence")


class MatchedSchemeItem(BaseModel):
    """One matched financing scheme."""

    product_name: str = Field(default="", description="Recommended product name")
    estimated_limit: str = Field(default="", description="Estimated credit limit")
    estimated_rate: str = Field(default="", description="Estimated interest rate")
    match_reason: str = Field(default="", description="Why the scheme matches")
    constraints: list[str] = Field(default_factory=list, description="Constraints or caveats")
    basis: list[RiskReportBasisItem] = Field(default_factory=list, description="Supporting evidence")


class MatchedSchemesSection(BaseModel):
    """Matched scheme section."""

    has_match: bool = Field(default=False, description="Whether matched schemes exist")
    items: list[MatchedSchemeItem] = Field(default_factory=list, description="Matched scheme list")


class NoMatchAnalysisSection(BaseModel):
    """No-match analysis section."""

    has_no_match_issue: bool = Field(default=False, description="Whether no-match issues exist")
    reasons: list[str] = Field(default_factory=list, description="Reasons for no match")
    core_shortboards: list[str] = Field(default_factory=list, description="Core shortboards")
    basis: list[RiskReportBasisItem] = Field(default_factory=list, description="Supporting evidence")


class OptimizationSuggestionsSection(BaseModel):
    """Optimization suggestions section."""

    short_term: list[str] = Field(default_factory=list, description="Short-term suggestions")
    mid_term: list[str] = Field(default_factory=list, description="Mid-term suggestions")
    document_supplement: list[str] = Field(default_factory=list, description="Document supplement suggestions")
    credit_optimization: list[str] = Field(default_factory=list, description="Credit optimization suggestions")
    debt_optimization: list[str] = Field(default_factory=list, description="Debt optimization suggestions")


class FinancingPlanSection(BaseModel):
    """Financing plan section."""

    current_stage: str = Field(default="", description="Current-stage plan summary")
    one_to_three_months: list[str] = Field(default_factory=list, description="1-3 month plan")
    three_to_six_months: list[str] = Field(default_factory=list, description="3-6 month plan")
    alternative_paths: list[str] = Field(default_factory=list, description="Alternative financing paths")


class FinalRecommendationSection(BaseModel):
    """Final recommendation section."""

    action: str = Field(default="", description="Recommended action")
    priority_product_types: list[str] = Field(default_factory=list, description="Priority product types")
    next_steps: list[str] = Field(default_factory=list, description="Next steps")
    basis: list[RiskReportBasisItem] = Field(default_factory=list, description="Supporting evidence")


class CustomerRiskReportJson(BaseModel):
    """Stable structured risk report payload."""

    customer_summary: RiskReportCustomerSummary = Field(default_factory=RiskReportCustomerSummary)
    overall_assessment: RiskReportOverallAssessment = Field(default_factory=RiskReportOverallAssessment)
    risk_dimensions: list[RiskDimensionAssessment] = Field(default_factory=list)
    matched_schemes: MatchedSchemesSection = Field(default_factory=MatchedSchemesSection)
    no_match_analysis: NoMatchAnalysisSection = Field(default_factory=NoMatchAnalysisSection)
    optimization_suggestions: OptimizationSuggestionsSection = Field(default_factory=OptimizationSuggestionsSection)
    financing_plan: FinancingPlanSection = Field(default_factory=FinancingPlanSection)
    final_recommendation: FinalRecommendationSection = Field(default_factory=FinalRecommendationSection)


class CustomerRiskReportResponse(BaseModel):
    """Response model for generated customer risk reports."""

    report_json: CustomerRiskReportJson = Field(default_factory=CustomerRiskReportJson)
    report_markdown: str = Field(default="", description="Markdown version of the report")
    generated_at: str = Field(default="", description="ISO timestamp for report generation")
    profile_version: int = Field(default=1, description="Profile markdown version used by the report")
    profile_updated_at: str = Field(default="", description="Profile markdown update time used by the report")
    previous_report: "CustomerRiskReportHistoryItem | None" = Field(
        default=None,
        description="Most recent previous report for before/after comparison",
    )


class CustomerRiskReportHistoryItem(BaseModel):
    """Stored historical risk report snapshot for comparison."""

    report_id: str = Field(default="", description="Unique report history ID")
    customer_id: str = Field(default="", description="Customer ID")
    generated_at: str = Field(default="", description="Report generation time")
    profile_version: int = Field(default=1, description="Profile version used by the report")
    profile_updated_at: str = Field(default="", description="Profile update time used by the report")
    report_json: CustomerRiskReportJson = Field(default_factory=CustomerRiskReportJson)
    report_markdown: str = Field(default="", description="Markdown report snapshot")


class CustomerRiskReportHistoryResponse(BaseModel):
    """History response for customer risk reports."""

    items: list[CustomerRiskReportHistoryItem] = Field(default_factory=list, description="Historical reports")


# =============================================================================
# Response Models
# =============================================================================


class FileProcessResponse(BaseModel):
    """Response model for file processing."""

    documentType: str = Field(..., description="Detected or provided document type")
    content: dict[str, Any] = Field(..., description="Extracted structured content")
    customerName: str | None = Field(default=None, description="Extracted customer name (if found)")


class FeishuSaveResponse(BaseModel):
    """Response model for Feishu save operation."""

    success: bool = Field(..., description="Whether the operation succeeded")
    recordId: str | None = Field(default=None, description="Feishu record ID")
    customerId: str | None = Field(default=None, description="Stable customer ID for local storage context")
    isNew: bool = Field(..., description="True if a new record was created")
    error: str | None = Field(default=None, description="Error message if operation failed")


class ApplicationResponse(BaseModel):
    """Response model for application generation."""

    applicationContent: str = Field(..., description="Generated application content in Markdown format")
    applicationData: dict[str, dict[str, str]] | None = Field(
        default=None, description="Structured application data for card rendering (section -> field -> value)"
    )
    customerFound: bool = Field(..., description="Whether customer data was found")
    warnings: list[str] = Field(default_factory=list, description="Validation warnings")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Generation metadata and version context")


class SchemeMatchResponse(BaseModel):
    """Response model for scheme matching."""

    matchResult: str = Field(..., description="Matching result in Markdown format")
    matchingData: dict[str, Any] | None = Field(default=None, description="Structured matching result data")


class ChatResponse(BaseModel):
    """Response model for chat endpoint."""

    message: str = Field(..., description="AI response message")
    intent: str | None = Field(
        default=None, description="Identified intent: 'extract', 'application', 'matching', or 'chat'"
    )
    data: dict[str, Any] | None = Field(default=None, description="Associated data (e.g., extraction result)")
    reasoning: str | None = Field(
        default=None, description="AI reasoning/thinking process (from DeepSeek thinking feature)"
    )


# ChatJobStatusResponse.model_rebuild()


class ChatSessionCreateRequest(BaseModel):
    """Request model for creating a chat session."""

    title: str | None = Field(default=None, description="Optional session title")
    customerId: str | None = Field(default=None, description="Current customer ID")
    customerName: str | None = Field(default=None, description="Current customer name")


class ChatJobCreateResponse(BaseModel):
    """Response returned immediately after creating an async chat job."""

    jobId: str = Field(..., description="Async job ID")
    status: str = Field(default="pending", description="Initial job status")


class ChatJobStatusResponse(BaseModel):
    """Status payload for async chat extraction jobs."""

    jobId: str = Field(..., description="Async job ID")
    jobType: str = Field(default="chat_extract", description="Job type")
    customerId: str = Field(default="", description="Linked customer ID")
    customerName: str = Field(default="", description="Linked customer name")
    status: str = Field(default="pending", description="pending/running/success/failed")
    progressMessage: str = Field(default="", description="Human-readable progress message")
    result: dict[str, Any] | None = Field(default=None, description="Completed async job result payload")
    errorMessage: str | None = Field(default=None, description="Failure reason")
    createdAt: str = Field(default="", description="Job creation time")
    startedAt: str = Field(default="", description="Job start time")
    finishedAt: str = Field(default="", description="Job finish time")
    jobTypeLabel: str = Field(default="处理任务", description="Localized job type label")
    targetPage: str | None = Field(default=None, description="Recommended target page after success")
    resultSummary: str | None = Field(default=None, description="Short summary for completed job result")


class ChatJobSummaryResponse(BaseModel):
    """Lightweight summary payload for recent async chat jobs."""

    jobId: str = Field(..., description="Async job ID")
    jobType: str = Field(default="chat_extract", description="Job type")
    customerId: str = Field(default="", description="Linked customer ID")
    customerName: str = Field(default="", description="Linked customer name")
    status: str = Field(default="pending", description="pending/running/success/failed")
    progressMessage: str = Field(default="", description="Human-readable progress message")
    errorMessage: str | None = Field(default=None, description="Failure reason")
    createdAt: str = Field(default="", description="Job creation time")
    startedAt: str = Field(default="", description="Job start time")
    finishedAt: str = Field(default="", description="Job finish time")
    jobTypeLabel: str = Field(default="处理任务", description="Localized job type label")
    targetPage: str | None = Field(default=None, description="Recommended target page after success")
    resultSummary: str | None = Field(default=None, description="Short summary for completed job result")


class ChatSessionSummary(BaseModel):
    """Lightweight chat session summary."""

    sessionId: str = Field(..., description="Unique chat session ID")
    title: str = Field(default="", description="Chat session title")
    customerId: str = Field(default="", description="Linked customer ID")
    customerName: str = Field(default="", description="Linked customer name")
    lastMessagePreview: str = Field(default="", description="Last message preview")
    createdAt: str = Field(default="", description="Session creation time")
    updatedAt: str = Field(default="", description="Session update time")


class ChatMessageRecordResponse(BaseModel):
    """Persisted chat message payload."""

    messageId: str = Field(..., description="Unique message ID")
    sessionId: str = Field(..., description="Parent session ID")
    role: str = Field(..., description="Message role")
    content: str = Field(default="", description="Message content")
    sequence: int = Field(default=0, description="Message order in the session")
    createdAt: str = Field(default="", description="Message creation time")


class ErrorResponse(BaseModel):
    """Standard error response model."""

    error: str = Field(..., description="Error message")
    detail: str | None = Field(default=None, description="Detailed error information")


class SearchCustomerRequest(BaseModel):
    """搜索客户请求"""

    customerName: str = Field(..., description="Customer name to search for in Feishu Bitable")


class SearchCustomerResponse(BaseModel):
    """搜索客户响应"""

    found: bool = Field(..., description="Whether the customer was found")
    customerData: dict[str, Any] = Field(default_factory=dict, description="Customer data from Feishu Bitable")
    recordId: str | None = Field(default=None, description="Feishu record ID if found")


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str = Field(..., description="Service status")


# =============================================================================
# Auth Models
# =============================================================================


class LoginRequest(BaseModel):
    """Request model for user login."""

    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class LoginResponse(BaseModel):
    """Response model for successful login."""

    token: str = Field(..., description="JWT token")
    username: str = Field(..., description="Username")
    role: str = Field(..., description="User role: admin or user")


class RegisterRequest(BaseModel):
    """Request model for user registration."""

    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    security_question: str | None = Field(default=None, description="Security question for password reset")
    security_answer: str | None = Field(default=None, description="Answer to security question")


class RegisterResponse(BaseModel):
    """Response model for successful registration."""

    username: str = Field(..., description="Username")
    role: str = Field(..., description="User role")


class ResetPasswordRequest(BaseModel):
    """Request model for admin password reset."""

    username: str = Field(..., description="Target username")
    new_password: str = Field(..., description="New password")


class UserInfo(BaseModel):
    """User information model."""

    username: str = Field(..., description="Username")
    role: str = Field(..., description="User role")
    created_at: str | None = Field(default=None, description="Account creation time")
    last_login_at: str | None = Field(default=None, description="Last login time")
    updated_at: str | None = Field(default=None, description="Last profile update time")
    display_name: str | None = Field(default=None, description="Optional display name shown in the UI")
    phone: str | None = Field(default=None, description="Optional phone number")
    has_security_question: bool = Field(default=False, description="Whether the user has configured a security question")


class UpdateCurrentUserProfileRequest(BaseModel):
    """Request for updating the current user's profile."""

    display_name: str | None = Field(default=None, description="Optional display name")
    phone: str | None = Field(default=None, description="Optional contact phone number")


class ChangeCurrentUserPasswordRequest(BaseModel):
    """Request for changing the current user's password."""

    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., description="New password")


class SetCurrentUserSecurityQuestionRequest(BaseModel):
    """Request for setting the current user's security question."""

    security_question: str = Field(..., description="Security question")
    security_answer: str = Field(..., description="Security answer")


class SecurityQuestionResponse(BaseModel):
    """Response for getting user's security question."""

    has_question: bool = Field(..., description="Whether user has set a security question")
    question: str = Field(default="", description="The security question text")


class ForgotPasswordRequest(BaseModel):
    """Request for self-service password reset via security question."""

    username: str = Field(..., description="Username")
    security_answer: str = Field(..., description="Answer to security question")
    new_password: str = Field(..., description="New password")


class CustomerListItem(BaseModel):
    """Customer list item from Feishu records."""

    name: str = Field(..., description="Customer name")
    record_id: str = Field(..., description="Feishu record ID")
    uploader: str = Field(default="", description="Uploader username")
    upload_time: str = Field(default="", description="Upload time")
    customer_type: str = Field(default="enterprise", description="Customer type: personal or enterprise")
    risk_level: str = Field(default="", description="Latest risk level if a risk report exists")
    last_report_generated_at: str = Field(default="", description="Latest risk report generation time")
    profile_version: int | None = Field(default=None, description="Latest profile version linked to risk report")


class CustomerDetail(BaseModel):
    """Customer detail with all Feishu fields."""

    name: str = Field(default="", description="Customer name")
    record_id: str = Field(..., description="Feishu record ID")
    uploader: str = Field(default="", description="Uploader username")
    upload_time: str = Field(default="", description="Upload time")
    fields: dict[str, Any] = Field(default_factory=dict, description="All Feishu fields as key-value pairs (can be nested objects)")


class CustomerProfileMarkdownResponse(BaseModel):
    """Markdown profile payload for a customer."""

    customer_id: str = Field(..., description="Unique customer ID")
    customer_name: str = Field(default="", description="Customer name")
    markdown_content: str = Field(default="", description="Profile markdown content")
    source_mode: str = Field(default="auto", description="Profile source mode: auto/manual")
    auto_generated: bool = Field(default=False, description="Whether the returned markdown was auto-generated on demand")
    version: int = Field(default=1, description="Profile version")
    updated_at: str | None = Field(default=None, description="Last update time")
    rag_source_priority: list[str] = Field(
        default_factory=list,
        description="Ordered retrieval source priority for future RAG usage",
    )
    risk_report_schema: dict[str, Any] = Field(
        default_factory=dict,
        description="Reserved JSON schema structure for future risk assessment reports",
    )


class UpdateCustomerProfileMarkdownRequest(BaseModel):
    """Request body for updating customer markdown profile."""

    markdown_content: str = Field(..., description="Markdown content to save")
    title: str | None = Field(default=None, description="Optional profile title")


CustomerRiskReportResponse.model_rebuild()

