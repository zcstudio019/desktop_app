import React, { useEffect, useState } from 'react';
import {
  Upload,
  Clock,
  CheckCircle,
  Users,
  FileText,
  LucideIcon,
  RefreshCw,
  AlertCircle,
} from 'lucide-react';
import StatCard from './dashboard/StatCard';
import { 
  getDashboardStats, 
  getDashboardActivities, 
  getWikiCacheStatus,
  refreshWikiCache,
  WikiCacheStatusResponse,
  DashboardStats, 
  Activity 
} from '../services/api';
import { PageType } from './layout/Sidebar';

// ============================================
// Types
// ============================================

/**
 * DashboardPage Props
 */
export interface DashboardPageProps {
  onNavigate: (page: PageType) => void;
}

// ============================================
// Constants
// ============================================

/**
 * 统计卡片配置
 */
interface StatCardConfig {
  id: keyof DashboardStats;
  icon: LucideIcon;
  iconColor: string;
  iconBgColor: string;
  title: string;
}

const STAT_CARD_CONFIGS: StatCardConfig[] = [
  {
    id: 'todayUploads',
    icon: Upload,
    iconColor: 'text-blue-500',
    iconBgColor: 'bg-blue-100',
    title: '今日上传',
  },
  {
    id: 'pending',
    icon: Clock,
    iconColor: 'text-amber-500',
    iconBgColor: 'bg-amber-100',
    title: '待处理',
  },
  {
    id: 'completed',
    icon: CheckCircle,
    iconColor: 'text-green-500',
    iconBgColor: 'bg-green-100',
    title: '已完成',
  },
  {
    id: 'totalCustomers',
    icon: Users,
    iconColor: 'text-purple-500',
    iconBgColor: 'bg-purple-100',
    title: '客户总数',
  },
];

/**
 * 默认统计数据（加载中或错误时显示）
 */
const DEFAULT_STATS: DashboardStats = {
  todayUploads: 0,
  pending: 0,
  completed: 0,
  totalCustomers: 0,
};

// ============================================
// Sub Components
// ============================================

/**
 * 活动状态徽章组件
 */
interface StatusBadgeProps {
  status: string;
}

const StatusBadge: React.FC<StatusBadgeProps> = ({ status }) => {
  const statusConfig: Record<string, { label: string; bgColor: string; textColor: string }> = {
    completed: {
      label: '已完成',
      bgColor: 'bg-green-100',
      textColor: 'text-green-700',
    },
    processing: {
      label: '处理中',
      bgColor: 'bg-amber-100',
      textColor: 'text-amber-700',
    },
    error: {
      label: '失败',
      bgColor: 'bg-red-100',
      textColor: 'text-red-700',
    },
  };

  const config = statusConfig[status] || statusConfig.completed;

  return (
    <span
      className={`px-2 py-1 text-xs font-medium rounded-full ${config.bgColor} ${config.textColor}`}
      data-testid={`status-badge-${status}`}
    >
      {config.label}
    </span>
  );
};

/**
 * 活动类型图标和颜色配置
 */
const getActivityTypeConfig = (type: string) => {
  const configs: Record<string, { icon: LucideIcon; bgColor: string; iconColor: string; label: string }> = {
    upload: {
      icon: Upload,
      bgColor: 'bg-blue-100',
      iconColor: 'text-blue-500',
      label: '上传文件',
    },
    application: {
      icon: FileText,
      bgColor: 'bg-amber-100',
      iconColor: 'text-amber-500',
      label: '生成申请表',
    },
    matching: {
      icon: CheckCircle,
      bgColor: 'bg-green-100',
      iconColor: 'text-green-500',
      label: '方案匹配',
    },
  };
  return configs[type] || configs.upload;
};

/**
 * 活动项组件
 */
interface ActivityItemProps {
  activity: Activity;
}

const ActivityItem: React.FC<ActivityItemProps> = ({ activity }) => {
  const typeConfig = getActivityTypeConfig(activity.type);
  const Icon = typeConfig.icon;
  
  // 构建显示文本
  let displayText: string;
  if (activity.type === 'upload' && activity.fileName) {
    displayText = activity.fileName;
  } else if (activity.customerName) {
    displayText = `${typeConfig.label} - ${activity.customerName}`;
  } else {
    displayText = typeConfig.label;
  }
  
  // 构建副标题
  let subtitle = activity.time;
  if (activity.type === 'upload' && activity.customerName) {
    subtitle = `${activity.customerName} · ${activity.time}`;
  } else if (activity.fileType) {
    subtitle = `${activity.fileType} · ${activity.time}`;
  }

  return (
    <div
      className="flex items-center justify-between py-3 border-b border-gray-100 last:border-b-0"
      data-testid={`activity-item-${activity.id}`}
    >
      <div className="flex items-center gap-3">
        {/* 活动类型图标 */}
        <div className={`w-10 h-10 ${typeConfig.bgColor} rounded-lg flex items-center justify-center`}>
          <Icon size={20} className={typeConfig.iconColor} />
        </div>
        
        {/* 活动信息 */}
        <div>
          <p className="text-sm font-medium text-gray-800">{displayText}</p>
          <p className="text-xs text-gray-500">{subtitle}</p>
        </div>
      </div>
      
      {/* 状态徽章 */}
      <StatusBadge status={activity.status} />
    </div>
  );
};

/**
 * 加载骨架屏组件
 */
const ActivitySkeleton: React.FC = () => (
  <div className="animate-pulse">
    {[1, 2, 3, 4].map((i) => (
      <div key={i} className="flex items-center justify-between py-3 border-b border-gray-100 last:border-b-0">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-gray-200 rounded-lg" />
          <div>
            <div className="h-4 w-32 bg-gray-200 rounded mb-1" />
            <div className="h-3 w-24 bg-gray-200 rounded" />
          </div>
        </div>
        <div className="h-6 w-16 bg-gray-200 rounded-full" />
      </div>
    ))}
  </div>
);

// ============================================
// Main Component
// ============================================

/**
 * DashboardPage 组件 - 工作台首页
 * 
 * 设计规范：
 * - 页面背景: #F8FAFC (slate-50)
 * - 卡片背景: #FFFFFF
 * - 卡片圆角: 12px
 * - 卡片阴影: shadow-sm
 * - 内边距: 24px
 * 
 * Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
 */
const DashboardPage: React.FC<DashboardPageProps> = ({ onNavigate: _onNavigate }) => {
  // Note: onNavigate is available for future use (e.g., clicking on pending items to navigate)
  // State for stats and activities
  const [stats, setStats] = useState<DashboardStats>(DEFAULT_STATS);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  
  // State for wiki cache
  const [cacheStatus, setCacheStatus] = useState<WikiCacheStatusResponse | null>(null);
  const [cacheLoading, setCacheLoading] = useState(false);
  const [cacheError, setCacheError] = useState<string | null>(null);

  // Fetch data function
  const fetchData = async (isRefresh = false) => {
    if (isRefresh) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    setError(null);

    try {
      // Fetch stats, activities, and cache status in parallel
      const [statsData, activitiesData, cacheStatusData] = await Promise.all([
        getDashboardStats(),
        getDashboardActivities(10),
        getWikiCacheStatus().catch(() => null), // 缓存状态获取失败不影响其他数据
      ]);

      setStats(statsData);
      setActivities(activitiesData.activities);
      setCacheStatus(cacheStatusData);
    } catch (err) {
      console.error('Failed to fetch dashboard data:', err);
      setError(err instanceof Error ? err.message : '获取数据失败');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  // Handle refresh wiki cache
  const handleRefreshCache = async () => {
    setCacheLoading(true);
    setCacheError(null);
    
    try {
      const result = await refreshWikiCache();
      // 刷新成功后更新状态
      setCacheStatus({
        cached: true,
        lastUpdated: result.lastUpdated,
        enterpriseProductCount: 0, // 会在下次获取状态时更新
        personalProductCount: 0,
      });
      // 重新获取完整状态
      const newStatus = await getWikiCacheStatus();
      setCacheStatus(newStatus);
    } catch (err) {
      console.error('Failed to refresh cache:', err);
      setCacheError(err instanceof Error ? err.message : '刷新失败，请重试');
    } finally {
      setCacheLoading(false);
    }
  };

  // Initial fetch
  useEffect(() => {
    fetchData();
  }, []);

  // Handle refresh
  const handleRefresh = () => {
    fetchData(true);
  };

  return (
    <div className="p-6" data-testid="dashboard-page">
      {/* 统计卡片区域 - 2x2 网格 */}
      <section className="mb-6" data-testid="stats-section">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {STAT_CARD_CONFIGS.map((config) => (
            <StatCard
              key={config.id}
              icon={config.icon}
              iconColor={config.iconColor}
              iconBgColor={config.iconBgColor}
              title={config.title}
              value={loading ? '-' : stats[config.id]}
            />
          ))}
        </div>
      </section>

      {/* 产品库缓存管理区域 */}
      <section
        className="bg-white rounded-xl shadow-sm p-5 mb-6"
        style={{ borderRadius: '12px' }}
        data-testid="product-cache-section"
      >
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-800">产品库缓存</h2>
          <button
            onClick={handleRefreshCache}
            disabled={cacheLoading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            data-testid="refresh-cache-button"
          >
            <RefreshCw size={16} className={cacheLoading ? 'animate-spin' : ''} />
            {cacheLoading ? '更新中...' : '更新产品库'}
          </button>
        </div>

        {/* 错误提示 */}
        {cacheError && (
          <div className="flex items-center gap-2 p-4 bg-red-50 text-red-600 rounded-lg mb-4">
            <AlertCircle size={20} />
            <span>{cacheError}</span>
          </div>
        )}

        {/* 缓存状态信息 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="p-3 bg-gray-50 rounded-lg">
            <p className="text-sm text-gray-500">缓存状态</p>
            <p className="text-lg font-medium text-gray-800">
              {cacheStatus?.cached ? '已缓存' : '未缓存'}
            </p>
          </div>
          <div className="p-3 bg-gray-50 rounded-lg">
            <p className="text-sm text-gray-500">最后更新</p>
            <p className="text-lg font-medium text-gray-800">
              {cacheStatus?.lastUpdated 
                ? new Date(cacheStatus.lastUpdated).toLocaleString('zh-CN')
                : '-'}
            </p>
          </div>
          <div className="p-3 bg-gray-50 rounded-lg">
            <p className="text-sm text-gray-500">企业产品</p>
            <p className="text-lg font-medium text-gray-800">
              {cacheStatus?.enterpriseProductCount || 0} 个
            </p>
          </div>
          <div className="p-3 bg-gray-50 rounded-lg">
            <p className="text-sm text-gray-500">个人产品</p>
            <p className="text-lg font-medium text-gray-800">
              {cacheStatus?.personalProductCount || 0} 个
            </p>
          </div>
        </div>
      </section>

      {/* 最近活动区域 - 全宽 */}
      <section
        className="bg-white rounded-xl shadow-sm p-5"
        style={{ borderRadius: '12px' }}
        data-testid="recent-activity-section"
      >
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-800">最近活动</h2>
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50"
            data-testid="refresh-button"
          >
            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
            刷新
          </button>
        </div>

        {/* Error state */}
        {error && (
          <div className="flex items-center gap-2 p-4 bg-red-50 text-red-600 rounded-lg mb-4">
            <AlertCircle size={20} />
            <span>{error}</span>
          </div>
        )}

        {/* Loading state */}
        {loading && <ActivitySkeleton />}

        {/* Activities list */}
        {!loading && (
          <div className="divide-y divide-gray-100">
            {activities.length > 0 ? (
              activities.map((activity) => (
                <ActivityItem key={activity.id} activity={activity} />
              ))
            ) : (
              <p className="text-gray-500 text-center py-8">暂无活动记录</p>
            )}
          </div>
        )}
      </section>
    </div>
  );
};

export default DashboardPage;

// Re-export types for external use
export type { DashboardStats, Activity };
