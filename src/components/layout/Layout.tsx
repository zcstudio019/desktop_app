/* eslint-disable react-refresh/only-export-components -- Exports PAGE_TITLES alongside Layout. */
import React from 'react';
import Sidebar, { PageType } from './Sidebar';
import Header from './Header';

export const PAGE_TITLES: Record<PageType, string> = {
  dashboard: '工作台',
  customers: '客户列表',
  data: '资料汇总',
  upload: '上传资料',
  application: '申请表生成',
  scheme: '方案匹配',
  chat: 'AI 对话',
  admin: '账号管理',
};

export interface LayoutProps {
  children: React.ReactNode;
  currentPage: PageType;
  onNavigate: (page: PageType) => void;
  userName?: string;
  userRole?: string;
  notificationCount?: number;
  onLogout?: () => void;
}

const Layout: React.FC<LayoutProps> = ({
  children,
  currentPage,
  onNavigate,
  userName = '用户',
  userRole,
  notificationCount = 0,
  onLogout,
}) => {
  const pageTitle = PAGE_TITLES[currentPage];

  return (
    <div className="flex h-screen" data-testid="layout">
      <Sidebar
        currentPage={currentPage}
        onNavigate={onNavigate}
        username={userName}
        userRole={userRole}
        onLogout={onLogout}
      />

      <div className="flex-1 flex flex-col overflow-hidden" data-testid="main-content">
        <Header
          pageTitle={pageTitle}
          userName={userName}
          userRole={userRole}
          notificationCount={notificationCount}
          onNavigate={onNavigate}
          onLogout={onLogout}
        />


        <main
          className="relative z-0 flex-1 overflow-auto bg-slate-50"
          style={{ backgroundColor: '#F8FAFC' }}
          data-testid="content-area"
        >
          {children}
        </main>
      </div>
    </div>
  );
};

export default Layout;
