/**
 * Context Module Exports
 *
 * Re-exports all context-related components, hooks, and types.
 *
 * Feature: frontend-backend-integration
 */

export {
  // Provider Component
  AppProvider,
  // Hook
  useApp,
  // Context (for advanced use cases)
  AppContext,
  // Initial state (for testing)
  initialState,
  // Types
  type ExtractionResult,
  type ApplicationResult,
  type ChatMessage,
  type AppState,
  type AppAction,
  type AppContextValue,
} from './AppContext';
