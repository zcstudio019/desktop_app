import React, { useMemo } from 'react';
import { useNewChatStore } from '../../stores/useNewChatStore';
import ChatMessages from './ChatMessages';
import ChatInput from './ChatInput';

const NewChatMain: React.FC = () => {
  const { sessions, currentSessionId, sending } = useNewChatStore();
  const currentTitle = useMemo(
    () => sessions.find((session) => session.id === currentSessionId)?.title || '新对话',
    [currentSessionId, sessions]
  );

  return (
    <div className="flex h-full flex-1 flex-col" data-testid="new-chat-main">
      <div className="flex h-14 flex-shrink-0 items-center justify-between border-b border-slate-200 px-4">
        <div className="truncate text-sm font-semibold text-slate-900">{currentTitle}</div>
        <div className="text-xs text-slate-400">{sending ? '正在回复' : '就绪'}</div>
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        <ChatMessages />
      </div>

      <div className="border-t border-slate-200 p-4">
        <ChatInput />
      </div>
    </div>
  );
};

export default NewChatMain;
