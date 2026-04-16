import { useCallback, useMemo, useState } from 'react';
import type { ChatMessage } from '../services/types';

export type ConversationStatus = 'idle' | 'sending' | 'error';

type UseConversationStateOptions = {
  loading: boolean;
  activeError: Error | null;
  addChatMessage?: (message: ChatMessage) => void;
};

export function useConversationState<T extends ChatMessage>({
  loading,
  activeError,
  addChatMessage,
}: UseConversationStateOptions) {
  const [messages, setMessages] = useState<T[]>([]);

  const appendMessage = useCallback((message: T) => {
    setMessages((prev) => [...prev, message]);
    addChatMessage?.(message);
  }, [addChatMessage]);

  const pendingUserMessageId = useMemo(
    () =>
      [...messages]
        .reverse()
        .find((message) => message.role === 'user' && (message.deliveryStatus === 'pending' || message.status === 'sending'))
        ?.clientMessageId ?? null,
    [messages],
  );

  const conversationStatus = useMemo<ConversationStatus>(() => {
    if (activeError) {
      return 'error';
    }
    if (loading) {
      return 'sending';
    }
    return 'idle';
  }, [activeError, loading]);

  const conversationLayer = useMemo(
    () => ({
      messages,
      pendingUserMessageId,
      conversationStatus,
    }),
    [messages, pendingUserMessageId, conversationStatus],
  );

  return {
    messages,
    setMessages,
    appendMessage,
    pendingUserMessageId,
    conversationStatus,
    conversationLayer,
  };
}
