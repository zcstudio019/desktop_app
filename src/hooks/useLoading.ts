import { useState, useCallback, useRef } from 'react';

/**
 * Result returned from `execute()`.
 * Includes both `data` and `error` so callers can inspect the outcome immediately.
 */
export interface ExecuteResult<T> {
  /** Data returned from the async operation, or `null` when it failed or was ignored. */
  data: T | null;
  /** Error object, or `null` when the call succeeded or was ignored. */
  error: Error | null;
}

/**
 * Return type for useLoading.
 */
export interface UseLoadingReturn<T> {
  /** Whether an async operation is currently running. */
  loading: boolean;
  /** Last error, if any. */
  error: Error | null;
  /** Last successful result, if any. */
  data: T | null;
  /**
   * Execute an async function while managing `loading` and `error` state.
   * When `loading=true`, later calls are ignored and return `{ data: null, error: null }`.
   */
  execute: (asyncFn: () => Promise<T>) => Promise<ExecuteResult<T>>;
  /** Reset all state to its initial value. */
  reset: () => void;
}

/**
 * Hook for managing loading and error state around async operations.
 *
 * Features:
 * - Automatically manages `loading`
 * - Captures thrown errors
 * - Prevents duplicate submission while loading (Property 13)
 * - Returns `{ data, error }` directly to avoid stale closure reads
 *
 * @returns Loading state, latest result/error, and helper actions
 *
 * @example
 * ```tsx
 * const { loading, error, data, execute } = useLoading<User>();
 *
 * const handleFetch = async () => {
 *   const { data: result, error: execError } = await execute(async () => {
 *     const response = await fetch('/api/user');
 *     return response.json();
 *   });
 *
 *   if (execError?.name === 'AbortError') {
 *     // Handle cancellation
 *   }
 * };
 * ```
 */
export function useLoading<T = unknown>(): UseLoadingReturn<T> {
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);
  const [data, setData] = useState<T | null>(null);

  // Track execution synchronously with a ref to block duplicate submissions.
  const isExecutingRef = useRef<boolean>(false);

  /**
   * Execute an async operation with duplicate-submission protection.
   */
  const execute = useCallback(async (asyncFn: () => Promise<T>): Promise<ExecuteResult<T>> => {
    // Property 13: Duplicate Submission Prevention
    // Ignore later calls while the current operation is still running.
    if (isExecutingRef.current) {
      return { data: null, error: null };
    }

    isExecutingRef.current = true;
    setLoading(true);
    setError(null);

    try {
      const result = await asyncFn();
      setData(result);
      return { data: result, error: null };
    } catch (err) {
      const nextError = err instanceof Error ? err : new Error(String(err));
      setError(nextError);
      return { data: null, error: nextError };
    } finally {
      setLoading(false);
      isExecutingRef.current = false;
    }
  }, []);

  /**
   * Reset all tracked state.
   */
  const reset = useCallback(() => {
    setLoading(false);
    setError(null);
    setData(null);
    isExecutingRef.current = false;
  }, []);

  return {
    loading,
    error,
    data,
    execute,
    reset,
  };
}

export default useLoading;
