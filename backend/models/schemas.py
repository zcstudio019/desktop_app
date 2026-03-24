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
    content: dict[str, Any] = Field(..., description="Extracted content to save")
    fileName: str | None = Field(default=None, description="Original uploaded file name")
    fileContent: str | None = Field(default=None, description="Base64 encoded original file content")


class ApplicationRequest(BaseModel):
    """Request model for loan application generation."""

    customerName: str = Field(..., description="Customer name to search for")
    loanType: str = Field(..., description="Loan type: 'enterprise' or 'personal'")


class SchemeMatchRequest(BaseModel):
    """Request model for scheme matching."""

    customerData: dict[str, Any] = Field(..., description="Customer data for matching")
    creditType: str = Field(..., description="Credit type: 'personal', 'enterprise_credit', or 'enterprise_mortgage'")


class SaveApplicationRequest(BaseModel):
    """Request model for saving application to local cache."""

    customerName: str = Field(..., description="Customer name")
    loanType: str = Field(..., description="Loan type: 'enterprise' or 'personal'")
    applicationData: dict[str, Any] = Field(..., description="Application data to save")


class SavedApplication(BaseModel):
    """Model for a saved application."""

    id: str = Field(..., description="Unique application ID")
    customerName: str = Field(..., description="Customer name")
    loanType: str = Field(..., description="Loan type")
    applicationData: dict[str, Any] = Field(..., description="Application data")
    savedAt: str = Field(..., description="ISO format timestamp when saved")


class SavedApplicationListItem(BaseModel):
    """Model for application list item (without full data)."""

    id: str = Field(..., description="Unique application ID")
    customerName: str = Field(..., description="Customer name")
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
    mergeDecisions: dict[str, str] | None = Field(
        default=None,
        description="User merge decisions: {customerName -> target_customer_id}",
    )


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


class SchemeMatchResponse(BaseModel):
    """Response model for scheme matching."""

    matchResult: str = Field(..., description="Matching result in Markdown format")


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


class CustomerDetail(BaseModel):
    """Customer detail with all Feishu fields."""

    name: str = Field(default="", description="Customer name")
    record_id: str = Field(..., description="Feishu record ID")
    uploader: str = Field(default="", description="Uploader username")
    upload_time: str = Field(default="", description="Upload time")
    fields: dict[str, Any] = Field(default_factory=dict, description="All Feishu fields as key-value pairs (can be nested objects)")

