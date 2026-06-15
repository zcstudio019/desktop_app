import React from 'react';
import { Plus } from 'lucide-react';
import { useNewChatStore } from '../../stores/useNewChatStore';

const NewChatSidebar: React.FC = () => {
  const { sessions, currentSessionId, createSession, switchSession, clearCurrentMessages } = useNewChatStore();

  const handleCreateSession = (): void => {
    createSession();
    clearCurrentMessages();
  };

  return (
    <div className="flex h-full flex-col p-3" data-testid="new-chat-sidebar">
      <button
        type="button"
        onClick={handleCreateSession}
        className="w-full rounded-lg bg-slate-900 px-3 py-2 text-sm font-medium text-white transition-colors hover:bg-slate-800"
      >
        <span className="inline-flex items-center gap-2">
          <Plus className="h-4 w-4" />
          新对话
        </span>
      </button>

      <div className="mt-3 flex-1 space-y-1 overflow-y-auto">
        {sessions.map((session) => {
          const active = session.id === currentSessionId;
          return (
            <button
              key={session.id}
              type="button"
              onClick={() => switchSession(session.id)}
              className={`w-full truncate rounded-lg px-3 py-2 text-left text-sm transition-colors ${
                active
                  ? 'bg-white font-medium text-slate-900 shadow-sm'
                  : 'text-slate-600 hover:bg-white hover:text-slate-900'
              }`}
              aria-current={active ? 'page' : undefined}
            >
              {session.title}
            </button>
          );
        })}
      </div>
    </div>
  );
};

export default NewChatSidebar;
