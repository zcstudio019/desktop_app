import React, { useEffect, useRef } from 'react';
import { Bot, Loader2, Sparkles } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useChatStore } from '../../stores/useChatStore';

const ChatMessages: React.FC = () => {
  const { messages, status, error } = useChatStore();
  const bottomRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, status]);

  if (messages.length === 0) {
    return (
      <div className="flex min-h-full flex-col items-center justify-center px-6 py-10 text-center">
        <div className="mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-blue-600 text-white shadow-lg shadow-blue-900/20">
          <Sparkles className="h-7 w-7" />
        </div>
        <div className="text-sm font-semibold text-slate-800">AI 工作台助手</div>
        <p className="mt-2 max-w-xs text-xs leading-5 text-slate-500">
          可围绕当前工作台数据、客户资料和业务流程持续对话。
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4 px-4 py-5" data-testid="ai-chat-messages">
      {messages.map((message, index) => {
        const isUser = message.role === 'user';
        return (
          <div key={message.clientMessageId || `${message.role}-${index}`} className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
            {!isUser ? (
              <div className="mr-2 mt-1 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-full bg-slate-900 text-white">
                <Bot className="h-4 w-4" />
              </div>
            ) : null}
            <div
              className={`max-w-[82%] rounded-2xl px-3.5 py-2.5 text-sm leading-6 shadow-sm ${
                isUser
                  ? 'rounded-br-md bg-blue-600 text-white'
                  : 'rounded-bl-md border border-slate-200 bg-white text-slate-700'
              }`}
            >
              <div className="prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
              </div>
            </div>
          </div>
        );
      })}

      {status === 'sending' ? (
        <div className="flex justify-start">
          <div className="mr-2 mt-1 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-full bg-slate-900 text-white">
            <Bot className="h-4 w-4" />
          </div>
          <div className="inline-flex items-center gap-2 rounded-2xl rounded-bl-md border border-slate-200 bg-white px-3.5 py-2.5 text-sm text-slate-500 shadow-sm">
            <Loader2 className="h-4 w-4 animate-spin" />
            正在思考
          </div>
        </div>
      ) : null}

      {error ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-600">
          {error}
        </div>
      ) : null}
      <div ref={bottomRef} />
    </div>
  );
};

export default ChatMessages;
