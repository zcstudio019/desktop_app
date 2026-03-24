/**
 * ErrorToast Component
 * 
 * Provides consistent error display UI for all errors in the application.
 * 
 * Feature: frontend-backend-integration
 * Validates: Requirement 7.4 - THE Error_Handler SHALL provide a consistent 
 * toast/notification UI for all errors
 */

import React, { useEffect, useState, useCallback } from 'react';
import { AlertCircle, X, RefreshCw } from 'lucide-react';
import { 
  getErrorMessage, 
  logError, 
  shouldDisplayError, 
  isRetryableError,
  ErrorType,
  classifyError,
} from '../utils/errorHandler';

export interface ErrorToastProps {
  /** The error to display (Error, ApiError, or null) */
  error: Error | null;
  /** Optional callback for retry button */
  onRetry?: () => void;
  /** Optional callback when toast is dismissed */
  onDismiss?: () => void;
  /** Auto-dismiss timeout in milliseconds (0 = no auto-dismiss) */
  autoDismissMs?: number;
  /** Optional context for error logging */
  context?: string;
}

/**
 * ErrorToast component for displaying error messages
 * 
 * Features:
 * - Displays appropriate message based on error type
 * - Support dismiss functionality
 * - Support retry button (optional callback)
 * - Auto-dismiss after timeout (optional)
 */
export function ErrorToast({
  error,
  onRetry,
  onDismiss,
  autoDismissMs = 0,
  context,
}: ErrorToastProps): React.ReactElement | null {
  // Track which error was dismissed to avoid re-showing it
  const [dismissedError, setDismissedError] = useState<Error | null>(null);
  const loggedErrorRef = React.useRef<Error | null>(null);

  // Derive visibility from props: visible if there's a displayable error that hasn't been dismissed
  const displayableError = error && shouldDisplayError(error) ? error : null;
  const visible = displayableError !== null && dismissedError !== displayableError;

  // Reset dismissed state when a new error arrives
  if (displayableError && dismissedError && dismissedError !== displayableError) {
    setDismissedError(null);
  }

  // Handle dismiss
  const handleDismiss = useCallback(() => {
    setDismissedError(error);
    onDismiss?.();
  }, [onDismiss, error]);

  // Handle retry
  const handleRetry = useCallback(() => {
    handleDismiss();
    onRetry?.();
  }, [handleDismiss, onRetry]);

  // Log error and set up auto-dismiss timer (side effects only)
  useEffect(() => {
    if (displayableError && loggedErrorRef.current !== displayableError) {
      logError(displayableError, context);
      loggedErrorRef.current = displayableError;
    }

    if (visible && autoDismissMs > 0) {
      const timer = setTimeout(handleDismiss, autoDismissMs);
      return () => clearTimeout(timer);
    }
  }, [displayableError, visible, autoDismissMs, context, handleDismiss]);

  // Don't render if not visible
  if (!visible || !displayableError) {
    return null;
  }

  const message = getErrorMessage(displayableError);
  const errorType = classifyError(displayableError);
  const showRetry = onRetry && isRetryableError(displayableError);

  // Get background color based on error type
  const getBgColor = () => {
    switch (errorType) {
      case ErrorType.VALIDATION:
        return 'bg-yellow-50 border-yellow-200';
      case ErrorType.SERVICE:
      case ErrorType.NETWORK:
        return 'bg-red-50 border-red-200';
      default:
        return 'bg-gray-50 border-gray-200';
    }
  };

  // Get icon color based on error type
  const getIconColor = () => {
    switch (errorType) {
      case ErrorType.VALIDATION:
        return 'text-yellow-500';
      case ErrorType.SERVICE:
      case ErrorType.NETWORK:
        return 'text-red-500';
      default:
        return 'text-gray-500';
    }
  };

  return (
    <div
      role="alert"
      aria-live="assertive"
      className={`fixed bottom-4 right-4 max-w-md p-4 rounded-lg border shadow-lg ${getBgColor()} animate-slide-up`}
      data-testid="error-toast"
    >
      <div className="flex items-start gap-3">
        {/* Error Icon */}
        <AlertCircle className={`w-5 h-5 flex-shrink-0 mt-0.5 ${getIconColor()}`} />
        
        {/* Message Content */}
        <div className="flex-1 min-w-0">
          <p className="text-sm text-gray-800" data-testid="error-message">
            {message}
          </p>
          
          {/* Retry Button */}
          {showRetry && (
            <button
              onClick={handleRetry}
              className="mt-2 inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800 font-medium"
              data-testid="retry-button"
            >
              <RefreshCw className="w-4 h-4" />
              重试
            </button>
          )}
        </div>
        
        {/* Dismiss Button */}
        <button
          onClick={handleDismiss}
          className="flex-shrink-0 p-1 rounded hover:bg-gray-200 transition-colors"
          aria-label="关闭"
          data-testid="dismiss-button"
        >
          <X className="w-4 h-4 text-gray-500" />
        </button>
      </div>
    </div>
  );
}

export default ErrorToast;
