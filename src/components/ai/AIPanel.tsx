import React from 'react';
import { Bot, RotateCcw, SlidersHorizontal } from 'lucide-react';
import { useChatStore } from '../../stores/useChatStore';
import ChatInput from './ChatInput';
import ChatMessages from './ChatMessages';
import ModelSelector from './ModelSelector';

const AIPanel: React.FC = () => {
  const { clearConversation } = useChatStore();

  return (
    <aside
      className="sticky right-0 top-0 flex h-full min-h-0 w-full flex-col border-l border-slate-200 bg-slate-50/80 p-3"
      data-testid="ai-panel"
    >
      <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
        <div className="flex h-14 flex-shrink-0 items-center gap-3 border-b border-slate-200 px-4">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-slate-900 text-white">
            <Bot className="h-5 w-5" />
          </div>
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold text-slate-900">AI 对话助手</div>
            <div className="text-xs text-emerald-600">在线</div>
          </div>

          <div className="ml-auto flex items-center gap-2">
            <button
              type="button"
              className="rounded-lg p-2 text-slate-500 transition-colors hover:bg-slate-100"
              title="工具栏"
            >
              <SlidersHorizontal className="h-4 w-4" />
            </button>
            <button
              type="button"
              onClick={clearConversation}
              className="rounded-lg p-2 text-slate-500 transition-colors hover:bg-slate-100"
              title="清空对话"
            >
              <RotateCcw className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="flex flex-1 flex-col overflow-hidden">
          <div className="flex items-center justify-end border-b border-slate-100 px-4 py-2">
            <ModelSelector />
          </div>
          <div className="flex-1 overflow-y-auto bg-slate-50/70">
            <ChatMessages />
          </div>
          <ChatInput />
        </div>
      </div>
    </aside>
  );
};

export default AIPanel;
