import { useCallback, useSyncExternalStore } from 'react';
import type { ChatFile, ChatMessage } from '../services/types';

export type WorkspaceAIContext = {
  currentPage: 'workspace';
  selectedCustomer: null | {
    id: string;
    name: string;
  };
  dashboardStats: Record<string, unknown>;
};

type ChatStatus = 'idle' | 'sending' | 'error';

type ChatStoreState = {
  messages: ChatMessage[];
  status: ChatStatus;
  error: string | null;
  sessionId: string | null;
  selectedModel: string;
  files: ChatFile[];
  aiContext: WorkspaceAIContext;
};

type ChatStoreActions = {
  setMessages: (messages: ChatMessage[]) => void;
  addMessage: (message: ChatMessage) => void;
  setStatus: (status: ChatStatus) => void;
  setError: (error: string | null) => void;
  setSessionId: (sessionId: string | null) => void;
  setSelectedModel: (model: string) => void;
  setFiles: (files: ChatFile[]) => void;
  clearFiles: () => void;
  clearConversation: () => void;
  setAIContext: (context: Partial<WorkspaceAIContext>) => void;
};

const env = import.meta.env as Record<string, string | undefined>;

export const DEFAULT_CHAT_MODEL =
  env.VITE_DEFAULT_MODEL ||
  env.DEFAULT_MODEL ||
  'DeepSeek-V3.2';

export const OPENAI_CHAT_MODEL =
  env.VITE_OPENAI_MODEL ||
  env.OPENAI_MODEL ||
  'gpt-4.1';

const initialState: ChatStoreState = {
  messages: [],
  status: 'idle',
  error: null,
  sessionId: null,
  selectedModel: DEFAULT_CHAT_MODEL,
  files: [],
  aiContext: {
    currentPage: 'workspace',
    selectedCustomer: null,
    dashboardStats: {},
  },
};

let state = initialState;
const listeners = new Set<() => void>();

function emit(nextState: ChatStoreState): void {
  state = nextState;
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function getSnapshot(): ChatStoreState {
  return state;
}

const actions: ChatStoreActions = {
  setMessages(messages) {
    emit({ ...state, messages });
  },
  addMessage(message) {
    emit({ ...state, messages: [...state.messages, message] });
  },
  setStatus(status) {
    emit({ ...state, status });
  },
  setError(error) {
    emit({ ...state, error });
  },
  setSessionId(sessionId) {
    emit({ ...state, sessionId });
  },
  setSelectedModel(model) {
    emit({ ...state, selectedModel: model });
  },
  setFiles(files) {
    emit({ ...state, files });
  },
  clearFiles() {
    emit({ ...state, files: [] });
  },
  clearConversation() {
    emit({ ...initialState, selectedModel: state.selectedModel, aiContext: state.aiContext });
  },
  setAIContext(context) {
    emit({ ...state, aiContext: { ...state.aiContext, ...context } });
  },
};

export function useChatStore(): ChatStoreState & ChatStoreActions {
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return {
    ...snapshot,
    setMessages: useCallback(actions.setMessages, []),
    addMessage: useCallback(actions.addMessage, []),
    setStatus: useCallback(actions.setStatus, []),
    setError: useCallback(actions.setError, []),
    setSessionId: useCallback(actions.setSessionId, []),
    setSelectedModel: useCallback(actions.setSelectedModel, []),
    setFiles: useCallback(actions.setFiles, []),
    clearFiles: useCallback(actions.clearFiles, []),
    clearConversation: useCallback(actions.clearConversation, []),
    setAIContext: useCallback(actions.setAIContext, []),
  };
}
