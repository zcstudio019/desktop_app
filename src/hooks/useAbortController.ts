import { useRef, useCallback, useEffect } from 'react';

/**
 * Return type for useAbortController.
 */
export interface UseAbortControllerReturn {
  /** Get the current AbortSignal. Create a new controller if the previous one was aborted. */
  getSignal: () => AbortSignal;
  /** Cancel the current request. */
  abort: () => void;
}

/**
 * Manage an AbortController for cancelable async requests.
 *
 * Features:
 * - Exposes `getSignal()` to obtain an AbortSignal
 * - Exposes `abort()` to cancel the current request
 * - Automatically aborts pending work on unmount
 *
 * @returns Helpers for retrieving a signal and aborting the active request
 *
 * @example
 * ```tsx
 * const { getSignal, abort } = useAbortController();
 *
 * const handleFetch = async () => {
 *   try {
 *     const response = await fetch('/api/data', {
 *       signal: getSignal(),
 *     });
 *     return response.json();
 *   } catch (err) {
 *     if (err instanceof Error && err.name === 'AbortError') {
 *       console.log('Request was cancelled');
 *     }
 *     throw err;
 *   }
 * };
 *
 * const handleCancel = () => {
 *   abort();
 * };
 * ```
 */
export function useAbortController(): UseAbortControllerReturn {
  const controllerRef = useRef<AbortController | null>(null);

  /**
   * Get an AbortSignal.
   * Create a fresh controller if none exists or if the previous one was aborted.
   */
  const getSignal = useCallback((): AbortSignal => {
    // Recreate the controller after an abort so later requests still work.
    if (!controllerRef.current || controllerRef.current.signal.aborted) {
      controllerRef.current = new AbortController();
    }
    return controllerRef.current.signal;
  }, []);

  /**
   * Abort the current request, if any.
   */
  const abort = useCallback((): void => {
    if (controllerRef.current) {
      controllerRef.current.abort();
      controllerRef.current = null;
    }
  }, []);

  // Abort pending work when the component unmounts.
  useEffect(() => {
    return () => {
      if (controllerRef.current) {
        controllerRef.current.abort();
        controllerRef.current = null;
      }
    };
  }, []);

  return {
    getSignal,
    abort,
  };
}

export default useAbortController;
