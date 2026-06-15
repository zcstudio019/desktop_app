import { useCallback, useSyncExternalStore } from 'react';

export type NewChatRole = 'user' | 'assistant';

export type NewChatMessage = {
  id: string;
  role: NewChatRole;
  content: string;
  createdAt: string;
};

export type NewChatSession = {
  id: string;
  title: string;
  createdAt: string;
};

type NewChatState = {
  sessions: NewChatSession[];
  currentSessionId: string;
  messages: Record<string, NewChatMessage[]>;
};

type NewChatActions = {
  createSession: () => string;
  switchSession: (sessionId: string) => void;
  appendMessage: (sessionId: string, message: NewChatMessage) => void;
  clearCurrentMessages: () => void;
  updateSessionTitle: (sessionId: string, title: string) => void;
};

const now = new Date().toISOString();

const initialSessions: NewChatSession[] = [
  { id: 'new-chat-1', title: '新对话', createdAt: now },
  { id: 'customer-consulting', title: '客户咨询', createdAt: now },
  { id: 'financing-analysis', title: '融资分析', createdAt: now },
  { id: 'new-chat-2', title: '新对话', createdAt: now },
];

let state: NewChatState = {
  sessions: initialSessions,
  currentSessionId: initialSessions[0]?.id || '',
  messages: initialSessions.reduce<Record<string, NewChatMessage[]>>((accumulator, session) => {
    accumulator[session.id] = [];
    return accumulator;
  }, {}),
};

const listeners = new Set<() => void>();

function emit(nextState: NewChatState): void {
  state = nextState;
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function getSnapshot(): NewChatState {
  return state;
}

function createMessageId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function createNewChatMessage(role: NewChatRole, content: string): NewChatMessage {
  return {
    id: createMessageId(role),
    role,
    content,
    createdAt: new Date().toISOString(),
  };
}

const actions: NewChatActions = {
  createSession() {
    const sessionId = createMessageId('session');
    const session: NewChatSession = {
      id: sessionId,
      title: '新对话',
      createdAt: new Date().toISOString(),
    };

    emit({
      sessions: [session, ...state.sessions],
      currentSessionId: sessionId,
      messages: {
        ...state.messages,
        [sessionId]: [],
      },
    });

    return sessionId;
  },
  switchSession(sessionId) {
    if (!state.sessions.some((session) => session.id === sessionId)) return;
    emit({ ...state, currentSessionId: sessionId });
  },
  appendMessage(sessionId, message) {
    emit({
      ...state,
      messages: {
        ...state.messages,
        [sessionId]: [...(state.messages[sessionId] || []), message],
      },
    });
  },
  clearCurrentMessages() {
    if (!state.currentSessionId) return;
    emit({
      ...state,
      messages: {
        ...state.messages,
        [state.currentSessionId]: [],
      },
    });
  },
  updateSessionTitle(sessionId, title) {
    emit({
      ...state,
      sessions: state.sessions.map((session) =>
        session.id === sessionId ? { ...session, title } : session
      ),
    });
  },
};

export function useNewChatStore(): NewChatState & NewChatActions {
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return {
    ...snapshot,
    createSession: useCallback(actions.createSession, []),
    switchSession: useCallback(actions.switchSession, []),
    appendMessage: useCallback(actions.appendMessage, []),
    clearCurrentMessages: useCallback(actions.clearCurrentMessages, []),
    updateSessionTitle: useCallback(actions.updateSessionTitle, []),
  };
}
