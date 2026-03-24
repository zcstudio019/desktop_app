/**
 * Services module - API client and types
 * 
 * This module provides all API communication functionality
 * for the React frontend.
 * 
 * @example
 * ```typescript
 * import { processFile, saveToStorage, ApiError } from '@/services';
 * 
 * try {
 *   const result = await processFile(file);
 *   await saveToStorage({
 *     documentType: result.documentType,
 *     customerName: result.customerName || 'Unknown',
 *     content: result.content
 *   });
 * } catch (error) {
 *   if (error instanceof ApiError) {
 *     console.error('API Error:', error.status, error.message);
 *   }
 * }
 * ```
 */

// Re-export all API functions
export {
  processFile,
  saveToStorage,
  saveToFeishu,
  generateApplication,
  matchScheme,
  sendChat,
} from './api';

// Re-export all types
export type {
  FileProcessResponse,
  StorageSaveRequest,
  StorageSaveResponse,
  FeishuSaveRequest,
  FeishuSaveResponse,
  ApplicationRequest,
  ApplicationResponse,
  SchemeMatchRequest,
  SchemeMatchResponse,
  ChatMessage,
  ChatFile,
  ChatRequest,
  ChatResponse,
} from './types';

// Re-export error types and utilities
export { ApiError, ErrorType, classifyError } from './types';
