/**
 * Error Handler Utilities
 *
 * Provides consistent error handling, classification, and user-friendly messages.
 *
 * Feature: frontend-backend-integration
 * Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5
 */

import { ApiError, ErrorType, classifyError } from '../services/types';

// Re-export for convenience
export { ApiError, ErrorType, classifyError };

/**
 * Error message mapping by ErrorType
 *
 * | Error Type | User Message |
 * |------------|--------------|
 * | Validation | "请检查输入：{detail}" |
 * | Service | "服务暂时不可用，请稍后重试" |
 * | Network | "网络连接失败，请检查网络" |
 * | Cancelled | (no message - silent) |
 */
const ERROR_MESSAGES: Record<ErrorType, string> = {
  [ErrorType.VALIDATION]: '请检查输入',
  [ErrorType.SERVICE]: '服务暂时不可用，请稍后重试',
  [ErrorType.NETWORK]: '网络连接失败，请检查网络',
  [ErrorType.CANCELLED]: '',
};

/**
 * Get a user-friendly error message based on the error type.
 */
export function getErrorMessage(error: unknown): string {
  const errorType = classifyError(error);

  if (errorType === ErrorType.CANCELLED) {
    return '';
  }

  if (errorType === ErrorType.VALIDATION && error instanceof ApiError) {
    const detail = error.message || '';
    if (detail) {
      return `${ERROR_MESSAGES[ErrorType.VALIDATION]}：${detail}`;
    }
  }

  return ERROR_MESSAGES[errorType];
}

/**
 * Log an error to the console for debugging.
 */
export function logError(error: unknown, context?: string): void {
  const errorType = classifyError(error);

  if (errorType === ErrorType.CANCELLED) {
    return;
  }

  const prefix = context ? `[${context}]` : '[Error]';

  if (error instanceof ApiError) {
    console.error(`${prefix} API Error (${error.status}):`, error.message, error.details);
  } else if (error instanceof Error) {
    console.error(`${prefix} ${error.name}:`, error.message, error.stack);
  } else {
    console.error(`${prefix} Unknown error:`, error);
  }
}

/**
 * Check if an error should be displayed to the user.
 */
export function shouldDisplayError(error: unknown): boolean {
  return classifyError(error) !== ErrorType.CANCELLED;
}

/**
 * Check if an error is retryable.
 * Network and service errors are typically retryable.
 */
export function isRetryableError(error: unknown): boolean {
  const errorType = classifyError(error);
  return errorType === ErrorType.NETWORK || errorType === ErrorType.SERVICE;
}
