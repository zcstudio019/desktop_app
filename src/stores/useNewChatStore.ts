import { useCallback, useSyncExternalStore } from 'react';
import { sendChat } from '../services/api';

export type NewChatTab = 'partner' | 'chat';
export type NewChatRole = 'user' | 'assistant';

export type ChatSession = {
  id: string;
  title: string;
};

export type ChatMessage = {
  id: string;
  role: NewChatRole;
  content: string;
  createdAt: string;
};

type NewChatState = {
  activeTab: NewChatTab;
  sessions: ChatSession[];
  currentSessionId: string | null;
  messages: Record<string, ChatMessage[]>;
  sending: boolean;
  error: string | null;
};

type NewChatActions = {
  createSession: () => void;
  switchSession: (id: string) => void;
  sendMessage: (content: string) => Promise<void>;
  setActiveTab: (tab: NewChatTab) => void;
};

const initialSessions: ChatSession[] = [
  { id: 'new-chat-1', title: '新对话' },
  { id: 'customer-consulting', title: '客户咨询' },
  { id: 'financing-analysis', title: '融资分析' },
  { id: 'new-chat-2', title: '新对话' },
];

let state: NewChatState = {
  activeTab: 'chat',
  sessions: initialSessions,
  currentSessionId: initialSessions[0]?.id || null,
  messages: initialSessions.reduce<Record<string, ChatMessage[]>>((accumulator, session) => {
    accumulator[session.id] = [];
    return accumulator;
  }, {}),
  sending: false,
  error: null,
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

function createId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function createMessage(role: NewChatRole, content: string): ChatMessage {
  return {
    id: createId(role),
    role,
    content,
    createdAt: new Date().toISOString(),
  };
}

function ensureSession(): string {
  if (state.currentSessionId) return state.currentSessionId;

  const sessionId = createId('session');
  const session: ChatSession = {
    id: sessionId,
    title: '新对话',
  };

  emit({
    ...state,
    sessions: [session, ...state.sessions],
    currentSessionId: sessionId,
    messages: {
      ...state.messages,
      [sessionId]: [],
    },
  });

  return sessionId;
}

const actions: NewChatActions = {
  createSession() {
    const sessionId = createId('session');
    const session: ChatSession = {
      id: sessionId,
      title: '新对话',
    };

    emit({
      ...state,
      activeTab: 'chat',
      sessions: [session, ...state.sessions],
      currentSessionId: sessionId,
      messages: {
        ...state.messages,
        [sessionId]: [],
      },
      error: null,
    });
  },
  switchSession(id) {
    if (!state.sessions.some((session) => session.id === id)) return;
    emit({
      ...state,
      activeTab: 'chat',
      currentSessionId: id,
      error: null,
    });
  },
  async sendMessage(content) {
    const trimmed = content.trim();
    if (!trimmed || state.sending) return;

    const sessionId = ensureSession();
    const currentMessages = state.messages[sessionId] || [];
    const userMessage = createMessage('user', trimmed);
    const nextMessages = [...currentMessages, userMessage];
    const shouldRename = currentMessages.length === 0;

    emit({
      ...state,
      activeTab: 'chat',
      sending: true,
      error: null,
      sessions: shouldRename
        ? state.sessions.map((session) =>
            session.id === sessionId ? { ...session, title: trimmed.slice(0, 18) || '新对话' } : session
          )
        : state.sessions,
      messages: {
        ...state.messages,
        [sessionId]: nextMessages,
      },
    });

    try {
      const response = await sendChat({
        messages: nextMessages.map((message) => ({
          role: message.role,
          content: message.content,
        })),
      });
      emit({
        ...state,
        sending: false,
        messages: {
          ...state.messages,
          [sessionId]: [
            ...(state.messages[sessionId] || []),
            createMessage('assistant', response.message || ''),
          ],
        },
      });
    } catch (error) {
      const message = error instanceof Error ? `发送失败：${error.message}` : '发送失败，请稍后重试。';
      emit({
        ...state,
        sending: false,
        error: message,
        messages: {
          ...state.messages,
          [sessionId]: [
            ...(state.messages[sessionId] || []),
            createMessage('assistant', message),
          ],
        },
      });
    }
  },
  setActiveTab(tab) {
    emit({
      ...state,
      activeTab: tab,
    });
  },
};

export function useNewChatStore(): NewChatState & NewChatActions {
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return {
    ...snapshot,
    createSession: useCallback(actions.createSession, []),
    switchSession: useCallback(actions.switchSession, []),
    sendMessage: useCallback(actions.sendMessage, []),
    setActiveTab: useCallback(actions.setActiveTab, []),
  };
}
