/* eslint-disable react-refresh/only-export-components -- Exports shared page types and nav config. */
import React from 'react';
import {
  LayoutDashboard,
  Upload,
  FileText,
  Target,
  MessageSquare,
  Wallet,
  Users,
  LogOut,
  Sheet,
  Shield,
  LucideIcon,
} from 'lucide-react';

export type PageType =
  | 'dashboard'
  | 'customers'
  | 'upload'
  | 'application'
  | 'scheme'
  | 'chat'
  | 'data'
  | 'admin';

export interface NavItemConfig {
  id: PageType;
  icon: LucideIcon;
  label: string;
  description?: string;
}

interface NavItemProps {
  icon: LucideIcon;
  label: string;
  description?: string;
  active: boolean;
  onClick: () => void;
}

export interface SidebarProps {
  currentPage: PageType;
  onNavigate: (page: PageType) => void;
  username?: string;
  userRole?: string;
  onLogout?: () => void;
}

const BASE_NAV_ITEMS: NavItemConfig[] = [
  { id: 'dashboard', icon: LayoutDashboard, label: '工作台', description: '查看系统动态与业务概览' },
  { id: 'customers', icon: Users, label: '客户管理', description: '查看并维护客户主体' },
  { id: 'upload', icon: Upload, label: '上传资料', description: '上传征信、流水和财务资料' },
  { id: 'data', icon: Sheet, label: '资料汇总', description: '维护客户核心资料汇总' },
  { id: 'application', icon: FileText, label: '申请表', description: '生成并编辑贷款申请表' },
  { id: 'scheme', icon: Target, label: '方案匹配', description: '匹配融资产品与建议方案' },
  { id: 'chat', icon: MessageSquare, label: 'AI 对话', description: '资料问答与风险评估报告' },
];

const ADMIN_NAV_ITEM: NavItemConfig = {
  id: 'admin',
  icon: Shield,
  label: '账号管理',
  description: '管理系统账号与权限',
};

export const NAV_ITEMS = BASE_NAV_ITEMS;

export const NavItem: React.FC<NavItemProps> = ({ icon: Icon, label, description, active, onClick }) => (
  <button
    onClick={onClick}
    className={`w-full min-h-[58px] flex items-center gap-3 px-4 py-3 transition-all duration-200 cursor-pointer ${
      active
        ? 'rounded-2xl bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-lg shadow-blue-900/20'
        : 'rounded-2xl text-white/80 hover:bg-white/10 hover:text-white'
    }`}
    aria-current={active ? 'page' : undefined}
    data-testid={`nav-item-${label}`}
  >
    <Icon size={20} className="flex-shrink-0" />
    <div className="min-w-0 text-left">
      <div className="text-sm font-medium">{label}</div>
      {description ? (
        <div className={`truncate text-[11px] ${active ? 'text-white/85' : 'text-white/50'}`}>{description}</div>
      ) : null}
    </div>
  </button>
);

const Logo: React.FC = () => (
  <div className="flex h-20 items-center gap-3 border-b border-white/10 px-5">
    <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-blue-500 shadow-lg shadow-blue-950/20">
      <Wallet size={18} className="text-white" />
    </div>
    <div className="min-w-0">
      <div className="truncate text-lg font-semibold text-white">贷款助手</div>
      <div className="truncate text-[11px] text-white/50">客户资料驱动的融资处理系统</div>
    </div>
  </div>
);

const Sidebar: React.FC<SidebarProps> = ({ currentPage, onNavigate, username, userRole, onLogout }) => {
  const navItems = userRole === 'admin' ? [...BASE_NAV_ITEMS, ADMIN_NAV_ITEM] : BASE_NAV_ITEMS;

  return (
    <aside className="flex h-screen w-64 flex-col bg-sidebar-bg" style={{ backgroundColor: '#1E293B' }} data-testid="sidebar">
      <Logo />

      <nav className="flex-1 px-3 py-4" role="navigation" aria-label="主导航">
        <ul className="space-y-2">
          {navItems.map((item) => (
            <li key={item.id}>
              <NavItem
                icon={item.icon}
                label={item.label}
                description={item.description}
                active={currentPage === item.id}
                onClick={() => onNavigate(item.id)}
              />
            </li>
          ))}
        </ul>
      </nav>

      <div className="border-t border-white/10 p-4">
        {username && <div className="mb-2 truncate text-xs text-white/60">{username}</div>}
        {onLogout && (
          <button
            onClick={onLogout}
            className="flex w-full items-center gap-2 rounded-xl px-3 py-2.5 text-sm text-white/60 transition-colors hover:bg-white/10 hover:text-white"
          >
            <LogOut size={16} />
            <span>退出登录</span>
          </button>
        )}
      </div>
    </aside>
  );
};

export default Sidebar;
