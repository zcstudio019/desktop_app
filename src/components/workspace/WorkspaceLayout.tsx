import React, { useState } from 'react';
import { MessageSquare, X } from 'lucide-react';
import AIPanel from '../ai/AIPanel';

export interface WorkspaceLayoutProps {
  children: React.ReactNode;
}

const WorkspaceLayout: React.FC<WorkspaceLayoutProps> = ({ children }) => {
  const [drawerOpen, setDrawerOpen] = useState(false);

  return (
    <div className="relative flex h-full min-h-0 bg-slate-50" data-testid="workspace-layout">
      <div className="flex-1 overflow-y-auto p-4 lg:basis-[70%] lg:grow-0" data-testid="workspace-left-workbench">
        {children}
      </div>

      <div className="hidden min-w-[360px] lg:flex lg:basis-[30%]" data-testid="workspace-right-ai">
        <AIPanel />
      </div>

      <button
        type="button"
        onClick={() => setDrawerOpen(true)}
        className="fixed bottom-5 right-5 z-30 flex h-12 w-12 items-center justify-center rounded-full bg-blue-600 text-white shadow-xl shadow-blue-900/25 transition-colors hover:bg-blue-700 lg:hidden"
        aria-label="打开 AI 对话助手"
        data-testid="floating-ai-button"
      >
        <MessageSquare className="h-5 w-5" />
      </button>

      {drawerOpen ? (
        <div className="fixed inset-0 z-40 lg:hidden" data-testid="ai-drawer">
          <button
            type="button"
            className="absolute inset-0 h-full w-full bg-slate-900/40"
            onClick={() => setDrawerOpen(false)}
            aria-label="关闭 AI 对话助手遮罩"
          />
          <div className="absolute bottom-0 right-0 top-0 flex w-full max-w-[420px] flex-col bg-white shadow-2xl">
            <button
              type="button"
              onClick={() => setDrawerOpen(false)}
              className="absolute right-4 top-4 z-10 rounded-lg bg-white p-2 text-slate-500 shadow-sm transition-colors hover:bg-slate-100"
              aria-label="关闭 AI 对话助手"
            >
              <X className="h-4 w-4" />
            </button>
            <AIPanel />
          </div>
        </div>
      ) : null}
    </div>
  );
};

export default WorkspaceLayout;
