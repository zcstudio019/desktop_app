/**
 * App Context - Global State Management
 *
 * Provides centralized state management for the loan application assistant.
 * Uses React Context with useReducer for predictable state updates.
 *
 * Feature: frontend-backend-integration
 * Property 4: State Management Completeness
 * Property 5: State Persistence Across Navigation
 */

import React, { createContext, useCallback, useContext, useEffect, useMemo, useReducer, type ReactNode } from 'react';

// ============================================
// Type Definitions
// ============================================

/**
 * Result from document extraction
 */
export interface ExtractionResult {
  /** Type of document (e.g., 'enterprise_credit', 'personal_credit') */
  documentType: string;
  /** Extracted content as key-value pairs */
  content: Record<string, unknown>;
  /** Customer name extracted from document, if available */
  customerName: string | null;
  /** Whether the result has been saved to local storage. Legacy field name kept for compatibility. */
  savedToFeishu: boolean;
  /** Stored record ID if saved */
  recordId: string | null;
  /** Stable customer ID for customer-scoped features */
  customerId?: string | null;
  /** Backing document ID for original preview/download */
  documentId?: string | null;
  /** Whether the original file is retained */
  originalAvailable?: boolean;
}

/**
 * Application generation result
 */
export interface ApplicationResult {
  /** Generated application content in Markdown format */
  content: string;
  /** Whether customer data was found */
  customerFound: boolean;
  /** Any warnings during generation */
  warnings: string[];
  /** Structured application data by section */
  applicationData?: Record<string, Record<string, string>>;
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

export interface SchemeResult {
  result: string | null;
  matchingData?: Record<string, unknown> | null;
  lastCreditType: string | null;
  customerId?: string | null;
  customerName?: string | null;
  matchedAt?: string;
  stale?: boolean;
  staleReason?: string;
  staleAt?: string;
}

/**
 * Chat message
 */
export interface ChatMessage {
  /** Role of the message sender */
  role: 'user' | 'assistant' | 'system';
  /** Message content */
  content: string;
  /** AI reasoning/thinking process */
  reasoning?: string | null;
  /** Detected intent for structured task messages */
  intent?: 'extract' | 'application' | 'matching' | 'chat' | null;
  /** Structured payload rendered below the message */
  data?: Record<string, unknown> | null;
  /** Weak task association for task-aware conversation rendering */
  relatedJobId?: string | null;
  /** Semantic message category */
  messageType?: 'text' | 'task_result' | 'task_feedback' | 'error';
  /** Created time for ordering/recovery */
  createdAt?: string;
  /** Optimistic rendering status */
  deliveryStatus?: 'pending' | 'sent' | 'failed';
  /** Task-aware message lifecycle status */
  status?: 'sending' | 'sent' | 'processing' | 'success' | 'failed';
  /** Delivery error for optimistic rendering */
  deliveryError?: string | null;
  /** Client-side message id */
  clientMessageId?: string;
}

/**
 * Upload queue item for task persistence
 */
export interface UploadQueueItem {
  id: string;
  fileName: string;
  documentType: string;
  status: 'pending' | 'processing' | 'success' | 'error';
}

/**
 * Task states for page navigation recovery
 * Feature: Task State Persistence
 */
export interface TaskStates {
  /** Upload task state */
  upload: {
    status: 'idle' | 'processing' | 'done';
    queue: UploadQueueItem[];
  };
  /** Application generation task state */
  application: {
    status: 'idle' | 'generating' | 'done';
    params: { customerName: string; loanType: string } | null;
  };
  /** Scheme matching task state */
  scheme: {
    status: 'idle' | 'matching' | 'done';
    params: {
      creditType: string;
      customerData: Record<string, unknown>;
      customerId?: string | null;
      customerName?: string | null;
    } | null;
  };
  /** Chat task state */
  chat: {
    status: 'idle' | 'sending' | 'done';
    pendingMessage: string | null;
    pendingFiles: Array<{ name: string; type: string; content: string }> | null;
  };
}

export interface SystemActivityItem {
  id: string;
  type: 'upload' | 'profile' | 'application' | 'matching' | 'rag' | 'risk';
  title: string;
  description: string;
  customerName?: string | null;
  customerId?: string | null;
  status: 'success' | 'processing' | 'warning';
  createdAt: string;
}

/**
 * Complete application state
 */
export interface AppState {
  /** Extraction state */
  extraction: {
    /** All extraction results */
    results: ExtractionResult[];
    /** Current customer name being worked on */
    currentCustomer: string | null;
    /** Current customer ID being worked on */
    currentCustomerId: string | null;
    /** Customer data grouped by customer name */
    customerDataMap: Record<string, ExtractionResult[]>;
  };
  /** Application generation state */
  application: {
    /** Generated application result */
    result: ApplicationResult | null;
    /** Last customer name used for generation */
    lastCustomer: string | null;
  };
  /** Scheme matching state */
  scheme: {
    /** Matching result and metadata */
    result: SchemeResult | null;
  };
  /** Chat state */
  chat: {
    /** Conversation history */
    messages: ChatMessage[];
  };
  /** Task states for page navigation recovery */
  tasks: TaskStates;
  /** Lightweight system activity feed for product-style visibility */
  system: {
    recentActivities: SystemActivityItem[];
  };
}

/**
 * Action types for state reducer
 */
export type AppAction =
  | { type: 'ADD_EXTRACTION'; payload: ExtractionResult }
  | { type: 'ADD_CUSTOMER_DATA'; payload: { customerName: string; result: ExtractionResult } }
  | { type: 'SET_CUSTOMER'; payload: { name: string | null; customerId?: string | null } }
  | { type: 'SET_APPLICATION'; payload: ApplicationResult | null; customer?: string }
  | { type: 'SET_SCHEME'; payload: SchemeResult | null }
  | { type: 'ADD_CHAT_MESSAGE'; payload: ChatMessage }
  | { type: 'UPDATE_CHAT_MESSAGES_BY_JOB'; payload: { jobId: string; patch: Partial<ChatMessage> } }
  | { type: 'CLEAR_CHAT' }
  | { type: 'SET_UPLOAD_TASK'; payload: { status: TaskStates['upload']['status']; queue: UploadQueueItem[] } }
  | { type: 'SET_APPLICATION_TASK'; payload: { status: TaskStates['application']['status']; params: TaskStates['application']['params'] } }
  | { type: 'SET_SCHEME_TASK'; payload: { status: TaskStates['scheme']['status']; params: TaskStates['scheme']['params'] } }
  | { type: 'SET_CHAT_TASK'; payload: { status: TaskStates['chat']['status']; pendingMessage: string | null; pendingFiles: TaskStates['chat']['pendingFiles'] } }
  | { type: 'ADD_SYSTEM_ACTIVITY'; payload: SystemActivityItem }
  | { type: 'RESET' };

/**
 * Context value including state and action dispatchers
 */
export interface AppContextValue {
  /** Current application state */
  state: AppState;
  /** Dispatch function for state updates */
  dispatch: React.Dispatch<AppAction>;
  /** Add an extraction result to the state */
  addExtractionResult: (result: ExtractionResult) => void;
  /** Add extraction result grouped by customer name */
  addCustomerData: (customerName: string, result: ExtractionResult) => void;
  /** Set the current customer name */
  setCurrentCustomer: (name: string | null, customerId?: string | null) => void;
  /** Set the application generation result */
  setApplicationResult: (result: ApplicationResult | null, customer?: string) => void;
  /** Set the scheme matching result */
  setSchemeResult: (result: SchemeResult | null) => void;
  /** Add a chat message to history */
  addChatMessage: (message: ChatMessage) => void;
  /** Patch chat messages associated with a job */
  updateChatMessagesByJob: (jobId: string, patch: Partial<ChatMessage>) => void;
  /** Clear all chat history */
  clearChatHistory: () => void;
  /** Set upload task status */
  setUploadTaskStatus: (status: TaskStates['upload']['status'], queue: UploadQueueItem[]) => void;
  /** Set application task status */
  setApplicationTaskStatus: (status: TaskStates['application']['status'], params: TaskStates['application']['params']) => void;
  /** Set scheme task status */
  setSchemeTaskStatus: (status: TaskStates['scheme']['status'], params: TaskStates['scheme']['params']) => void;
  /** Set chat task status */
  setChatTaskStatus: (status: TaskStates['chat']['status'], pendingMessage: string | null, pendingFiles: TaskStates['chat']['pendingFiles']) => void;
  /** Add one system activity item */
  recordSystemActivity: (activity: Omit<SystemActivityItem, 'id' | 'createdAt'>) => void;
  /** Reset all state to initial values */
  reset: () => void;
}

// ============================================
// Initial State
// ============================================

/**
 * Initial application state
 */
const initialState: AppState = {
  extraction: {
    results: [],
    currentCustomer: null,
    currentCustomerId: null,
    customerDataMap: {},
  },
  application: {
    result: null,
    lastCustomer: null,
  },
  scheme: {
    result: null,
  },
  chat: {
    messages: [],
  },
  tasks: {
    upload: {
      status: 'idle',
      queue: [],
    },
    application: {
      status: 'idle',
      params: null,
    },
    scheme: {
      status: 'idle',
      params: null,
    },
    chat: {
      status: 'idle',
      pendingMessage: null,
      pendingFiles: null,
    },
  },
  system: {
    recentActivities: [],
  },
};

const PERSISTED_APP_STATE_KEY = 'loan-assistant-app-state';

function loadPersistedAppState(): Partial<AppState> | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(PERSISTED_APP_STATE_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as Partial<AppState>;
  } catch {
    return null;
  }
}

function buildInitialState(initialStateOverride?: AppState): AppState {
  if (initialStateOverride) {
    return initialStateOverride;
  }

  const persisted = loadPersistedAppState();
  if (!persisted) {
    return initialState;
  }

  return {
    ...initialState,
    extraction: {
      ...initialState.extraction,
      currentCustomer: persisted.extraction?.currentCustomer ?? initialState.extraction.currentCustomer,
      currentCustomerId: persisted.extraction?.currentCustomerId ?? initialState.extraction.currentCustomerId,
    },
    chat: {
      messages: persisted.chat?.messages ?? initialState.chat.messages,
    },
    system: {
      recentActivities: persisted.system?.recentActivities ?? initialState.system.recentActivities,
    },
  };
}

// ============================================
// Reducer
// ============================================

/**
 * State reducer for handling all app actions
 *
 * Feature: frontend-backend-integration
 * Property 4: State Management Completeness - All fields are preserved
 */
function appReducer(state: AppState, action: AppAction): AppState {
  switch (action.type) {
    case 'ADD_EXTRACTION':
      // Property 7: Sequential File Processing - Results are accumulated
      return {
        ...state,
        extraction: {
          ...state.extraction,
          results: [...state.extraction.results, action.payload],
          // Auto-set current customer if extracted
          currentCustomer: action.payload.customerName ?? state.extraction.currentCustomer,
          currentCustomerId: action.payload.customerId ?? state.extraction.currentCustomerId,
        },
      };

    case 'ADD_CUSTOMER_DATA': {
      // Add extraction result grouped by customer name
      const { customerName, result } = action.payload;
      const existingData = state.extraction.customerDataMap[customerName] || [];
      return {
        ...state,
        extraction: {
          ...state.extraction,
          results: [...state.extraction.results, result],
          customerDataMap: {
            ...state.extraction.customerDataMap,
            [customerName]: [...existingData, result],
          },
          // Auto-set current customer if not set
          currentCustomer: state.extraction.currentCustomer ?? customerName,
          currentCustomerId: state.extraction.currentCustomerId ?? result.customerId ?? null,
        },
      };
    }

    case 'SET_CUSTOMER':
      if (
        state.extraction.currentCustomer === action.payload.name &&
        (
          action.payload.customerId === undefined ||
          state.extraction.currentCustomerId === action.payload.customerId
        )
      ) {
        return state;
      }
      return {
        ...state,
        extraction: {
          ...state.extraction,
          currentCustomer: action.payload.name,
          currentCustomerId:
            action.payload.customerId === undefined
              ? state.extraction.currentCustomerId
              : action.payload.customerId,
        },
      };

    case 'SET_APPLICATION':
      return {
        ...state,
        application: {
          result: action.payload,
          lastCustomer: action.customer ?? state.application.lastCustomer,
        },
      };

    case 'SET_SCHEME':
      return {
        ...state,
        scheme: {
          result: action.payload,
        },
      };

    case 'ADD_CHAT_MESSAGE':
      // Property 10: Chat Message Accumulation - Messages in chronological order
      return {
        ...state,
        chat: {
          ...state.chat,
          messages: [...state.chat.messages, action.payload],
        },
      };

    case 'UPDATE_CHAT_MESSAGES_BY_JOB':
      return {
        ...state,
        chat: {
          ...state.chat,
          messages: state.chat.messages.map((message) =>
            message.relatedJobId === action.payload.jobId
              ? {
                  ...message,
                  ...action.payload.patch,
                }
              : message,
          ),
        },
      };

    case 'CLEAR_CHAT':
      return {
        ...state,
        chat: {
          messages: [],
        },
      };

    case 'SET_UPLOAD_TASK':
      return {
        ...state,
        tasks: {
          ...state.tasks,
          upload: {
            status: action.payload.status,
            queue: action.payload.queue,
          },
        },
      };

    case 'SET_APPLICATION_TASK':
      return {
        ...state,
        tasks: {
          ...state.tasks,
          application: {
            status: action.payload.status,
            params: action.payload.params,
          },
        },
      };

    case 'SET_SCHEME_TASK':
      return {
        ...state,
        tasks: {
          ...state.tasks,
          scheme: {
            status: action.payload.status,
            params: action.payload.params,
          },
        },
      };

    case 'SET_CHAT_TASK':
      return {
        ...state,
        tasks: {
          ...state.tasks,
          chat: {
            status: action.payload.status,
            pendingMessage: action.payload.pendingMessage,
            pendingFiles: action.payload.pendingFiles,
          },
        },
      };

    case 'ADD_SYSTEM_ACTIVITY':
      return {
        ...state,
        system: {
          recentActivities: [
            action.payload,
            ...state.system.recentActivities,
          ].slice(0, 12),
        },
      };

    case 'RESET':
      return initialState;

    default:
      return state;
  }
}

// ============================================
// Context
// ============================================

/**
 * App Context - null when not within provider
 */
const AppContext = createContext<AppContextValue | null>(null);

// ============================================
// Provider Component
// ============================================

/**
 * Props for AppProvider component
 */
interface AppProviderProps {
  /** Child components to wrap */
  children: ReactNode;
  /** Optional initial state for testing */
  initialStateOverride?: AppState;
}

/**
 * App Provider Component
 *
 * Wraps the application and provides global state management.
 * All components that need access to app state must be descendants of this provider.
 *
 * @example
 * ```tsx
 * <AppProvider>
 *   <App />
 * </AppProvider>
 * ```
 *
 * Feature: frontend-backend-integration
 * Property 5: State Persistence Across Navigation
 */
export function AppProvider({ children, initialStateOverride }: AppProviderProps): React.ReactElement {
  const [state, dispatch] = useReducer(appReducer, initialStateOverride, buildInitialState);

  /**
   * Add an extraction result to the state
   * @param result - The extraction result to add
   */
  const addExtractionResult = useCallback((result: ExtractionResult): void => {
    dispatch({ type: 'ADD_EXTRACTION', payload: result });
  }, []);

  /**
   * Add extraction result grouped by customer name
   * @param customerName - The customer name to group by
   * @param result - The extraction result to add
   */
  const addCustomerData = useCallback((customerName: string, result: ExtractionResult): void => {
    dispatch({ type: 'ADD_CUSTOMER_DATA', payload: { customerName, result } });
  }, []);

  /**
   * Set the current customer name
   * @param name - Customer name or null to clear
   */
  const setCurrentCustomer = useCallback((name: string | null, customerId?: string | null): void => {
    dispatch({ type: 'SET_CUSTOMER', payload: { name, customerId } });
  }, []);

  /**
   * Set the application generation result
   * @param result - Application result or null to clear
   * @param customer - Optional customer name to associate
   */
  const setApplicationResult = useCallback((result: ApplicationResult | null, customer?: string): void => {
    dispatch({ type: 'SET_APPLICATION', payload: result, customer });
  }, []);

  /**
   * Set the scheme matching result
   * @param result - Matching result or null to clear
   * @param creditType - Optional credit type to associate
   */
  const setSchemeResult = useCallback((result: SchemeResult | null): void => {
    dispatch({ type: 'SET_SCHEME', payload: result });
  }, []);

  /**
   * Add a chat message to history
   * @param message - The message to add
   */
  const addChatMessage = useCallback((message: ChatMessage): void => {
    dispatch({ type: 'ADD_CHAT_MESSAGE', payload: message });
  }, []);

  /**
   * Patch chat messages associated with a job
   * @param jobId - Related async job id
   * @param patch - Partial message updates to apply
   */
  const updateChatMessagesByJob = useCallback((jobId: string, patch: Partial<ChatMessage>): void => {
    dispatch({ type: 'UPDATE_CHAT_MESSAGES_BY_JOB', payload: { jobId, patch } });
  }, []);

  /**
   * Clear all chat history
   */
  const clearChatHistory = useCallback((): void => {
    dispatch({ type: 'CLEAR_CHAT' });
  }, []);

  /**
   * Set upload task status
   * @param status - Task status
   * @param queue - Upload queue items
   */
  const setUploadTaskStatus = useCallback((status: TaskStates['upload']['status'], queue: UploadQueueItem[]): void => {
    dispatch({ type: 'SET_UPLOAD_TASK', payload: { status, queue } });
  }, []);

  /**
   * Set application task status
   * @param status - Task status
   * @param params - Task parameters
   */
  const setApplicationTaskStatus = useCallback((status: TaskStates['application']['status'], params: TaskStates['application']['params']): void => {
    dispatch({ type: 'SET_APPLICATION_TASK', payload: { status, params } });
  }, []);

  /**
   * Set scheme task status
   * @param status - Task status
   * @param params - Task parameters
   */
  const setSchemeTaskStatus = useCallback((status: TaskStates['scheme']['status'], params: TaskStates['scheme']['params']): void => {
    dispatch({ type: 'SET_SCHEME_TASK', payload: { status, params } });
  }, []);

  /**
   * Set chat task status
   * @param status - Task status
   * @param pendingMessage - Pending message
   * @param pendingFiles - Pending files
   */
  const setChatTaskStatus = useCallback((status: TaskStates['chat']['status'], pendingMessage: string | null, pendingFiles: TaskStates['chat']['pendingFiles']): void => {
    dispatch({ type: 'SET_CHAT_TASK', payload: { status, pendingMessage, pendingFiles } });
  }, []);

  const recordSystemActivity = useCallback((activity: Omit<SystemActivityItem, 'id' | 'createdAt'>): void => {
    dispatch({
      type: 'ADD_SYSTEM_ACTIVITY',
      payload: {
        ...activity,
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        createdAt: new Date().toISOString(),
      },
    });
  }, []);

  /**
   * Reset all state to initial values
   */
  const reset = useCallback((): void => {
    dispatch({ type: 'RESET' });
    if (typeof window !== 'undefined') {
      window.localStorage.removeItem(PERSISTED_APP_STATE_KEY);
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(
      PERSISTED_APP_STATE_KEY,
      JSON.stringify({
        extraction: {
          currentCustomer: state.extraction.currentCustomer,
          currentCustomerId: state.extraction.currentCustomerId,
        },
        chat: {
          messages: state.chat.messages,
        },
        system: {
          recentActivities: state.system.recentActivities,
        },
      }),
    );
  }, [
    state.chat.messages,
    state.extraction.currentCustomer,
    state.extraction.currentCustomerId,
    state.system.recentActivities,
  ]);

  const contextValue: AppContextValue = useMemo(() => ({
    state,
    dispatch,
    addExtractionResult,
    addCustomerData,
    setCurrentCustomer,
    setApplicationResult,
    setSchemeResult,
    addChatMessage,
    updateChatMessagesByJob,
    clearChatHistory,
    setUploadTaskStatus,
    setApplicationTaskStatus,
    setSchemeTaskStatus,
    setChatTaskStatus,
    recordSystemActivity,
    reset,
  }), [
    state,
    addExtractionResult,
    addCustomerData,
    setCurrentCustomer,
    setApplicationResult,
    setSchemeResult,
    addChatMessage,
    updateChatMessagesByJob,
    clearChatHistory,
    setUploadTaskStatus,
    setApplicationTaskStatus,
    setSchemeTaskStatus,
    setChatTaskStatus,
    recordSystemActivity,
    reset,
  ]);

  return <AppContext.Provider value={contextValue}>{children}</AppContext.Provider>;
}

// ============================================
// Hook
// ============================================

/**
 * Hook to access App Context
 *
 * Must be used within an AppProvider. Throws an error if used outside.
 *
 * @returns The app context value with state and actions
 * @throws Error if used outside of AppProvider
 *
 * @example
 * ```tsx
 * function MyComponent() {
 *   const { state, addExtractionResult } = useApp();
 *   // Use state and actions...
 * }
 * ```
 *
 * Feature: frontend-backend-integration
 */
export function useApp(): AppContextValue {
  const context = useContext(AppContext);

  if (context === null) {
    throw new Error('useApp must be used within an AppProvider. ' +
      'Wrap your component tree with <AppProvider>.');
  }

  return context;
}

// ============================================
// Exports
// ============================================

export { AppContext, initialState };
