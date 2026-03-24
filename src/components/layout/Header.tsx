import React from 'react';
import { Bell, User } from 'lucide-react';

/**
 * Header Props
 */
export interface HeaderProps {
  pageTitle: string;
  userName?: string;
  notificationCount?: number;
}

/**
 * NotificationBell 组件 - 通知图标带红点
 */
interface NotificationBellProps {
  count?: number;
}

const NotificationBell: React.FC<NotificationBellProps> = ({ count = 0 }) => {
  const hasNotifications = count > 0;
  
  return (
    <button
      className="relative p-2 rounded-lg hover:bg-gray-100 transition-colors"
      aria-label={hasNotifications ? `${count} 条未读通知` : '通知'}
      data-testid="notification-bell"
    >
      <Bell size={20} className="text-gray-600" />
      {hasNotifications && (
        <span
          className="absolute top-1.5 right-1.5 w-2 h-2 bg-red-500 rounded-full"
          style={{ backgroundColor: '#EF4444' }}
          data-testid="notification-dot"
          aria-hidden="true"
        />
      )}
    </button>
  );
};

/**
 * UserAvatar 组件 - 用户头像和名称
 */
interface UserAvatarProps {
  userName?: string;
}

const UserAvatar: React.FC<UserAvatarProps> = ({ userName = '用户' }) => {
  // 获取用户名首字母作为头像显示
  const initial = userName.charAt(0).toUpperCase();
  
  return (
    <div className="flex items-center gap-3" data-testid="user-avatar">
      <div
        className="w-10 h-10 rounded-full bg-blue-500 flex items-center justify-center"
        style={{ backgroundColor: '#3B82F6' }}
      >
        {initial ? (
          <span className="text-white font-medium text-sm">{initial}</span>
        ) : (
          <User size={20} className="text-white" />
        )}
      </div>
      <span className="text-sm font-medium text-gray-700" data-testid="user-name">
        {userName}
      </span>
    </div>
  );
};

/**
 * Header 组件 - 顶部标题栏
 * 
 * 设计规范：
 * - 高度: 64px
 * - 背景色: 白色 (#FFFFFF)
 * - 阴影: shadow-sm (0 1px 3px rgba(0,0,0,0.05))
 * - 左侧: 页面标题 (20px, font-weight: 600, #1F2937)
 * - 右侧: 通知图标 (Bell) + 红点 + 用户头像 + 用户名
 * - 通知红点: 8px 圆形, #EF4444
 * - 用户头像: 40px 圆形, bg-blue-500
 * 
 * Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
 */
const Header: React.FC<HeaderProps> = ({
  pageTitle,
  userName = '用户',
  notificationCount = 0,
}) => {
  return (
    <header
      className="h-16 bg-white flex items-center justify-between px-6"
      style={{
        height: '64px',
        backgroundColor: '#FFFFFF',
        boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
      }}
      data-testid="header"
    >
      {/* 左侧：页面标题 */}
      <h1
        className="text-xl font-semibold text-gray-800"
        style={{
          fontSize: '20px',
          fontWeight: 600,
          color: '#1F2937',
        }}
        data-testid="page-title"
      >
        {pageTitle}
      </h1>
      
      {/* 右侧：通知图标 + 用户头像 */}
      <div className="flex items-center gap-4">
        <NotificationBell count={notificationCount} />
        <UserAvatar userName={userName} />
      </div>
    </header>
  );
};

export default Header;
