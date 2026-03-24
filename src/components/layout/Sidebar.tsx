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
}

interface NavItemProps {
  icon: LucideIcon;
  label: string;
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
  { id: 'dashboard', icon: LayoutDashboard, label: '工作台' },
  { id: 'customers', icon: Users, label: '客户列表' },
  { id: 'data', icon: Sheet, label: '资料汇总' },
  { id: 'upload', icon: Upload, label: '上传资料' },
  { id: 'application', icon: FileText, label: '申请表生成' },
  { id: 'scheme', icon: Target, label: '方案匹配' },
  { id: 'chat', icon: MessageSquare, label: 'AI 对话' },
];

const ADMIN_NAV_ITEM: NavItemConfig = { id: 'admin', icon: Shield, label: '账号管理' };

export const NAV_ITEMS = BASE_NAV_ITEMS;

export const NavItem: React.FC<NavItemProps> = ({ icon: Icon, label, active, onClick }) => (
  <button
    onClick={onClick}
    className={`w-full h-11 flex items-center gap-3 px-4 py-2.5 transition-colors duration-200 cursor-pointer ${
      active ? 'bg-blue-500 text-white rounded-r-lg' : 'text-white/80 hover:text-white hover:bg-white/10'
    }`}
    style={{
      borderRadius: active ? '0 8px 8px 0' : '0',
    }}
    aria-current={active ? 'page' : undefined}
    data-testid={`nav-item-${label}`}
  >
    <Icon size={20} className="flex-shrink-0" />
    <span className="text-sm font-medium">{label}</span>
  </button>
);

const Logo: React.FC = () => (
  <div className="h-16 flex items-center gap-3 px-4 border-b border-white/10">
    <div className="w-8 h-8 bg-blue-500 rounded-lg flex items-center justify-center">
      <Wallet size={18} className="text-white" />
    </div>
    <span className="text-white font-semibold text-lg">贷款助手</span>
  </div>
);

const Sidebar: React.FC<SidebarProps> = ({ currentPage, onNavigate, username, userRole, onLogout }) => {
  const navItems = userRole === 'admin' ? [...BASE_NAV_ITEMS, ADMIN_NAV_ITEM] : BASE_NAV_ITEMS;

  return (
    <aside className="w-60 h-screen flex flex-col bg-sidebar-bg" style={{ backgroundColor: '#1E293B' }} data-testid="sidebar">
      <Logo />

      <nav className="flex-1 py-4" role="navigation" aria-label="主导航">
        <ul className="space-y-1">
          {navItems.map((item) => (
            <li key={item.id}>
              <NavItem
                icon={item.icon}
                label={item.label}
                active={currentPage === item.id}
                onClick={() => onNavigate(item.id)}
              />
            </li>
          ))}
        </ul>
      </nav>

      <div className="border-t border-white/10 p-4">
        {username && <div className="text-white/60 text-xs mb-2 truncate">{username}</div>}
        {onLogout && (
          <button
            onClick={onLogout}
            className="w-full flex items-center gap-2 px-3 py-2 text-white/60 hover:text-white hover:bg-white/10 rounded-lg transition-colors text-sm"
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
