import React from 'react';
import { MessageSquarePlus, Search, Sparkles, Users } from 'lucide-react';
import { useNewChatStore, type NewChatTab } from '../../stores/useNewChatStore';

const TABS: Array<{ id: NewChatTab; label: string }> = [
  { id: 'partner', label: 'Partner' },
  { id: 'chat', label: '对话' },
];

const partnerItems = ['业务助手', '融资顾问', '客户分析', '资料问答'];

const NewChatSecondarySidebar: React.FC = () => {
  const {
    activeTab,
    sessions,
    currentSessionId,
    createSession,
    switchSession,
    setActiveTab,
  } = useNewChatStore();

  return (
    <div className="flex h-full flex-col" data-testid="new-chat-secondary-sidebar">
      <div className="flex h-14 flex-shrink-0 items-center gap-3 border-b border-slate-200 px-4">
        <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-slate-900 text-white">
          <Sparkles className="h-4 w-4" />
        </div>
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-slate-900">智能对话</div>
          <div className="truncate text-xs text-slate-500">New Chat</div>
        </div>
      </div>

      <div className="border-b border-slate-200 px-3 py-3">
        <div className="grid grid-cols-2 rounded-xl bg-slate-200/70 p-1">
          {TABS.map((tab) => {
            const active = tab.id === activeTab;
            return (
              <button
                key={tab.id}
                type="button"
                onClick={() => setActiveTab(tab.id)}
                className={`h-8 rounded-lg text-sm font-medium transition-colors ${
                  active ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500 hover:text-slate-800'
                }`}
              >
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      {activeTab === 'partner' ? (
        <div className="flex-1 overflow-y-auto p-3">
          <div className="mb-3 rounded-xl border border-slate-200 bg-white p-3">
            <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-slate-800">
              <Users className="h-4 w-4" />
              Partner
            </div>
            <p className="text-xs leading-5 text-slate-500">选择一个智能伙伴，后续可扩展为不同业务角色。</p>
          </div>
          <div className="space-y-2">
            {partnerItems.map((item) => (
              <button
                key={item}
                type="button"
                className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-left text-sm text-slate-700 transition-colors hover:border-slate-300 hover:bg-slate-50"
              >
                {item}
              </button>
            ))}
          </div>
        </div>
      ) : (
        <>
          <div className="border-b border-slate-200 p-3">
            <button
              type="button"
              onClick={createSession}
              className="flex h-10 w-full items-center justify-center gap-2 rounded-xl bg-slate-900 text-sm font-medium text-white transition-colors hover:bg-slate-800"
            >
              <MessageSquarePlus className="h-4 w-4" />
              新对话
            </button>
            <div className="mt-3 flex h-9 items-center gap-2 rounded-xl border border-slate-200 bg-white px-3 text-slate-400">
              <Search className="h-4 w-4" />
              <span className="text-xs">搜索会话</span>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto p-3">
            <div className="space-y-1">
              {sessions.map((session) => {
                const active = session.id === currentSessionId;
                return (
                  <button
                    key={session.id}
                    type="button"
                    onClick={() => switchSession(session.id)}
                    className={`w-full truncate rounded-xl px-3 py-2.5 text-left text-sm transition-colors ${
                      active
                        ? 'bg-white font-medium text-slate-900 shadow-sm ring-1 ring-slate-200'
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
        </>
      )}
    </div>
  );
};

export default NewChatSecondarySidebar;
