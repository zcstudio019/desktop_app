import React, { useState } from 'react';
import { sendChat } from '../../services/api';
import { createNewChatMessage, useNewChatStore } from '../../stores/useNewChatStore';

const ChatInput: React.FC = () => {
  const [inputValue, setInputValue] = useState('');
  const [sending, setSending] = useState(false);
  const {
    currentSessionId,
    messages,
    appendMessage,
    createSession,
    updateSessionTitle,
  } = useNewChatStore();

  const handleSend = async (): Promise<void> => {
    const content = inputValue.trim();
    if (!content || sending) return;

    const sessionId = currentSessionId || createSession();
    const previousMessages = messages[sessionId] || [];
    const userMessage = createNewChatMessage('user', content);

    setInputValue('');
    appendMessage(sessionId, userMessage);
    if (previousMessages.length === 0) {
      updateSessionTitle(sessionId, content.slice(0, 18));
    }

    setSending(true);
    try {
      const response = await sendChat({
        messages: [...previousMessages, userMessage].map((message) => ({
          role: message.role,
          content: message.content,
        })),
      });
      appendMessage(sessionId, createNewChatMessage('assistant', response.message || ''));
    } catch (error) {
      appendMessage(
        sessionId,
        createNewChatMessage(
          'assistant',
          error instanceof Error ? `发送失败：${error.message}` : '发送失败，请稍后重试。'
        )
      );
    } finally {
      setSending(false);
    }
  };

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'Enter') {
      event.preventDefault();
      void handleSend();
    }
  };

  return (
    <div className="mx-auto flex w-full max-w-3xl gap-2" data-testid="new-chat-input">
      <input
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="向 AI 提问..."
        disabled={sending}
        className="h-10 flex-1 rounded-lg border border-slate-300 px-3 text-sm outline-none transition-colors focus:border-slate-500 disabled:bg-slate-100"
      />
      <button
        type="button"
        onClick={() => void handleSend()}
        disabled={sending || !inputValue.trim()}
        className="h-10 rounded-lg bg-slate-900 px-4 text-sm font-medium text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        发送
      </button>
    </div>
  );
};

export default ChatInput;
