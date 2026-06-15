import React, { useEffect, useMemo, useRef } from 'react';
import { useNewChatStore, type ChatMessage } from '../../stores/useNewChatStore';

const ChatMessages: React.FC = () => {
  const { currentSessionId, messages } = useNewChatStore();
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const currentMessages = useMemo<ChatMessage[]>(
    () => (currentSessionId ? messages[currentSessionId] || [] : []),
    [currentSessionId, messages]
  );

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [currentMessages]);

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4" data-testid="new-chat-messages">
      {currentMessages.map((message) => {
        const isUser = message.role === 'user';
        return (
          <div key={message.id} className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
            <div
              className={`max-w-[78%] rounded-2xl px-4 py-2.5 text-sm leading-6 ${
                isUser
                  ? 'bg-slate-900 text-white'
                  : 'border border-slate-200 bg-white text-slate-800'
              }`}
            >
              <div className="whitespace-pre-wrap">{message.content}</div>
            </div>
          </div>
        );
      })}
      <div ref={bottomRef} />
    </div>
  );
};

export default ChatMessages;
