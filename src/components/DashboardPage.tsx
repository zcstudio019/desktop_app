import React, { useEffect, useMemo, useState } from 'react';
import {
  Upload,
  Clock,
  CheckCircle,
  Users,
  FileText,
  LucideIcon,
  RefreshCw,
  AlertCircle,
  FolderSync,
  ShieldAlert,
  ArrowRight,
} from 'lucide-react';
import StatCard from './dashboard/StatCard';
import { useApp } from '../context/AppContext';
import {
  getDashboardStats,
  getDashboardActivities,
  getWikiCacheStatus,
  refreshWikiCache,
  WikiCacheStatusResponse,
  DashboardStats,
  Activity,
  listCustomers,
} from '../services/api';
import { PageType } from './layout/Sidebar';
import type { CustomerListItem } from '../services/types';
import { SYSTEM_INFO, getSystemVersionLabel } from '../config/systemInfo';

export interface DashboardPageProps {
  onNavigate: (page: PageType) => void;
}

interface StatCardConfig {
  id: 'totalCustomers' | 'todayUploads' | 'pendingMaterialCustomers' | 'reportedCustomers';
  icon: LucideIcon;
  iconColor: string;
  iconBgColor: string;
  title: string;
  subtitle: string;
}

type ActivityBadgeConfig = {
  label: string;
  className: string;
};

type ActivityAction = {
  label: string;
  page: PageType;
};

type DashboardQuickLink = {
  label: string;
  page: PageType;
  className: string;
};

function formatDashboardCustomerLabel(customerName?: string | null, customerId?: string | null): string {
  const stripInternalId = (value: string) =>
    value
      .replace(/\s*\((enterprise|personal)_[^)]+\)\s*/gi, '')
      .replace(/\b(enterprise|personal)_/gi, '')
      .trim();

  if (customerName && customerName.trim()) {
    return stripInternalId(customerName);
  }
  if (!customerId) {
    return '';
  }
  return stripInternalId(customerId);
}

function formatLocalDateTime(value?: string | null): string {
  if (!value) {
    return '未记录';
  }
  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
}

const STAT_CARD_CONFIGS: StatCardConfig[] = [
  { id: 'totalCustomers', icon: Users, iconColor: 'text-violet-500', iconBgColor: 'bg-violet-100', title: '客户总数', subtitle: '当前系统已纳入管理的客户主体' },
  { id: 'todayUploads', icon: Upload, iconColor: 'text-blue-500', iconBgColor: 'bg-blue-100', title: '今日上传数', subtitle: '按今日成功入库的上传动作统计' },
  { id: 'pendingMaterialCustomers', icon: Clock, iconColor: 'text-amber-500', iconBgColor: 'bg-amber-100', title: '待补资料客户数', subtitle: '仍缺关键资料、建议优先补件的客户' },
  { id: 'reportedCustomers', icon: CheckCircle, iconColor: 'text-emerald-500', iconBgColor: 'bg-emerald-100', title: '已生成报告客户数', subtitle: '至少完成过一次风险评估报告生成' },
];

const DEFAULT_STATS: DashboardStats = {
  todayUploads: 0,
  pending: 0,
  completed: 0,
  totalCustomers: 0,
  pendingMaterialCustomers: 0,
  reportedCustomers: 0,
  highRiskCustomers: 0,
};

function isSameLocalDay(value?: string | null): boolean {
  if (!value) {
    return false;
  }
  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    return false;
  }
  const now = new Date();
  return (
    date.getFullYear() === now.getFullYear() &&
    date.getMonth() === now.getMonth() &&
    date.getDate() === now.getDate()
  );
}

function buildFallbackStats(customers: CustomerListItem[], activities: Activity[]): DashboardStats {
  const uploadActivitiesToday = activities.filter(
    (activity) => activity.type === 'upload' && isSameLocalDay(activity.createdAt || activity.time)
  ).length;
  const customerUploadsToday = customers.filter((customer) => isSameLocalDay(customer.upload_time)).length;
  const reportedCustomers = customers.filter((customer) => Boolean(customer.last_report_generated_at)).length;
  const pendingMaterialCustomers = customers.filter(
    (customer) => !customer.last_report_generated_at || !customer.profile_version
  ).length;
  const highRiskCustomers = customers.filter((customer) => String(customer.risk_level || '').toLowerCase() === 'high').length;

  return {
    todayUploads: Math.max(uploadActivitiesToday, customerUploadsToday),
    pending: 0,
    completed: 0,
    totalCustomers: customers.length,
    pendingMaterialCustomers,
    reportedCustomers,
    highRiskCustomers,
  };
}

function mergeDashboardStats(primary: DashboardStats, fallback: DashboardStats): DashboardStats {
  return {
    ...primary,
    totalCustomers: primary.totalCustomers || fallback.totalCustomers,
    todayUploads: primary.todayUploads || fallback.todayUploads,
    pendingMaterialCustomers: primary.pendingMaterialCustomers || fallback.pendingMaterialCustomers || 0,
    reportedCustomers: primary.reportedCustomers || fallback.reportedCustomers || 0,
    highRiskCustomers: primary.highRiskCustomers || fallback.highRiskCustomers || 0,
  };
}

const WORKFLOW_STEPS: Array<{
  key: string;
  title: string;
  description: string;
  icon: LucideIcon;
  page: PageType;
}> = [
  { key: 'upload', title: '上传资料', description: '上传征信、流水、财务和辅助材料，建立客户资料池。', icon: Upload, page: 'upload' },
  { key: 'profile', title: '资料整理', description: '系统自动整理客户资料汇总，人工可继续修订。', icon: FileText, page: 'data' },
  { key: 'application', title: '申请表', description: '基于最新客户资料生成贷款申请表，并保留版本上下文。', icon: FileText, page: 'application' },
  { key: 'matching', title: '方案匹配', description: '结合申请表和产品库，输出融资方案与未命中原因。', icon: FolderSync, page: 'scheme' },
  { key: 'risk', title: '风险报告', description: '规则评分 + 资料依据，生成结构化风险评估报告。', icon: ShieldAlert, page: 'chat' },
];

const DASHBOARD_QUICK_LINKS: DashboardQuickLink[] = [
  { label: '客户资料流转', page: 'data', className: 'border-blue-200 bg-blue-50 text-blue-700' },
  { label: '系统动态可追踪', page: 'dashboard', className: 'border-slate-200 bg-white text-slate-600' },
  { label: '产品库状态可核查', page: 'scheme', className: 'border-slate-200 bg-white text-slate-600' },
];

const STATUS_CONFIG: Record<string, ActivityBadgeConfig> = {
  completed: { label: '已完成', className: 'bg-emerald-100 text-emerald-700' },
  processing: { label: '处理中', className: 'bg-blue-100 text-blue-700' },
  warning: { label: '需关注', className: 'bg-amber-100 text-amber-700' },
  error: { label: '失败', className: 'bg-rose-100 text-rose-700' },
};

function getStatusConfig(status: string): ActivityBadgeConfig {
  return STATUS_CONFIG[status] || STATUS_CONFIG.completed;
}

function getActivityTypeConfig(type: string) {
  const configs: Record<string, { icon: LucideIcon; bgColor: string; iconColor: string; fallbackTitle: string }> = {
    upload: { icon: Upload, bgColor: 'bg-blue-100', iconColor: 'text-blue-500', fallbackTitle: '客户资料上传' },
    application: { icon: FileText, bgColor: 'bg-amber-100', iconColor: 'text-amber-500', fallbackTitle: '贷款申请表' },
    matching: { icon: FolderSync, bgColor: 'bg-cyan-100', iconColor: 'text-cyan-600', fallbackTitle: '融资方案匹配' },
    profile: { icon: FileText, bgColor: 'bg-violet-100', iconColor: 'text-violet-600', fallbackTitle: '资料汇总更新' },
    risk: { icon: ShieldAlert, bgColor: 'bg-rose-100', iconColor: 'text-rose-600', fallbackTitle: '风险评估报告' },
    rag: { icon: FileText, bgColor: 'bg-sky-100', iconColor: 'text-sky-600', fallbackTitle: '资料问答' },
  };
  return configs[type] || configs.upload;
}

function buildActivityTitle(activity: Activity): string {
  if (activity.title) return activity.title;

  const typeConfig = getActivityTypeConfig(activity.type);
  if (activity.type === 'upload' && activity.fileName) {
    return `${typeConfig.fallbackTitle}：${activity.fileName}`;
  }
  if (activity.customerName) {
    return `${typeConfig.fallbackTitle}：${activity.customerName}`;
  }
  return typeConfig.fallbackTitle;
}

function buildActivityDescription(activity: Activity): string {
  if (activity.description) return activity.description;
  if (activity.fileType && activity.customerName) {
    return `${activity.customerName} · ${activity.fileType}`;
  }
  if (activity.customerName) {
    return `当前客户：${activity.customerName}`;
  }
  if (activity.fileName) {
    return `处理文件：${activity.fileName}`;
  }
  return '系统已记录这次业务动作。';
}

function getActivityBadge(activity: Activity): ActivityBadgeConfig {
  const titleText = `${activity.title || ''} ${activity.description || ''}`;
  const metadata = activity.metadata || {};

  if (activity.status === 'error') return STATUS_CONFIG.error;
  if (activity.status === 'processing') return STATUS_CONFIG.processing;

  if (activity.type === 'application') {
    if (activity.status === 'warning' || titleText.includes('重新生成') || titleText.includes('失效')) {
      return { label: '待刷新', className: 'bg-amber-100 text-amber-700' };
    }
    return { label: '最新可用', className: 'bg-emerald-100 text-emerald-700' };
  }

  if (activity.type === 'matching') {
    if (activity.status === 'warning' || titleText.includes('重新执行') || titleText.includes('重新匹配')) {
      return { label: '待重匹配', className: 'bg-amber-100 text-amber-700' };
    }
    return { label: '最新结果', className: 'bg-cyan-100 text-cyan-700' };
  }

  if (activity.type === 'risk') {
    const profileVersion = metadata.profileVersion ?? metadata.profile_version;
    if (profileVersion) {
      return { label: `资料V${profileVersion}`, className: 'bg-sky-100 text-sky-700' };
    }
    return { label: '报告已生成', className: 'bg-rose-100 text-rose-700' };
  }

  if (activity.type === 'profile') {
    const sourceMode = String(metadata.sourceMode || metadata.source_mode || '');
    if (titleText.includes('恢复') || sourceMode === 'auto') {
      return { label: '系统整理稿', className: 'bg-violet-100 text-violet-700' };
    }
    return { label: '手动整理稿', className: 'bg-violet-100 text-violet-700' };
  }

  if (activity.type === 'upload') {
    return { label: '已入库', className: 'bg-blue-100 text-blue-700' };
  }

  if (activity.type === 'rag') {
    return { label: '问答已完成', className: 'bg-sky-100 text-sky-700' };
  }

  return getStatusConfig(activity.status);
}

function getSecondaryBadge(activity: Activity): ActivityBadgeConfig | null {
  const metadata = activity.metadata || {};

  if (activity.type === 'risk') {
    if (metadata.generatedAt || metadata.generated_at) {
      return { label: '已生成报告', className: 'bg-slate-100 text-slate-600' };
    }
    return null;
  }

  if (activity.type === 'profile') {
    const profileVersion = metadata.profileVersion ?? metadata.profile_version;
    if (profileVersion) {
      return { label: `版本 V${profileVersion}`, className: 'bg-slate-100 text-slate-600' };
    }
    return null;
  }

  if (activity.type === 'application') {
    const profileVersion = metadata.profileVersion ?? metadata.profile_version;
    if (profileVersion) {
      return { label: `资料 V${profileVersion}`, className: 'bg-slate-100 text-slate-600' };
    }
    return null;
  }

  return null;
}

function getActivityAction(activity: Activity): ActivityAction | null {
  const titleText = `${activity.title || ''} ${activity.description || ''}`;

  if (activity.type === 'application') {
    if (activity.status === 'warning' || titleText.includes('重新生成') || titleText.includes('失效')) {
      return { label: '去申请表页处理', page: 'application' };
    }
    return { label: '查看申请表', page: 'application' };
  }

  if (activity.type === 'matching') {
    if (activity.status === 'warning' || titleText.includes('重新执行') || titleText.includes('重新匹配')) {
      return { label: '去方案匹配页处理', page: 'scheme' };
    }
    return { label: '查看匹配结果', page: 'scheme' };
  }

  if (activity.type === 'risk' || activity.type === 'rag') {
    return { label: '去 AI 对话查看', page: 'chat' };
  }

  if (activity.type === 'profile') {
    return { label: '去资料汇总查看', page: 'data' };
  }

  if (activity.type === 'upload') {
    return { label: '去上传资料页', page: 'upload' };
  }

  return null;
}

function getSecondaryAction(activity: Activity): ActivityAction | null {
  if (activity.type === 'risk') {
    return { label: '查看报告版本', page: 'chat' };
  }

  if (activity.type === 'profile') {
    return { label: '查看资料版本', page: 'data' };
  }

  if (activity.type === 'application') {
    return { label: '查看申请表版本', page: 'application' };
  }

  return null;
}

function getStatCardAction(id: StatCardConfig['id']): ActivityAction {
  switch (id) {
    case 'totalCustomers':
      return { label: '查看客户列表', page: 'customers' };
    case 'todayUploads':
      return { label: '去上传资料页', page: 'upload' };
    case 'pendingMaterialCustomers':
      return { label: '去资料汇总查看', page: 'data' };
    case 'reportedCustomers':
      return { label: '去 AI 对话查看', page: 'chat' };
    default:
      return { label: '查看详情', page: 'dashboard' };
  }
}

function ActivityRow({
  activity,
  onNavigate,
  onSelectCustomer,
}: {
  activity: Activity;
  onNavigate: (page: PageType) => void;
  onSelectCustomer: (customerName: string | null, customerId?: string | null) => void;
}) {
  const typeConfig = getActivityTypeConfig(activity.type);
  const Icon = typeConfig.icon;
  const title = buildActivityTitle(activity);
  const description = buildActivityDescription(activity);
  const badge = getActivityBadge(activity);
  const secondaryBadge = getSecondaryBadge(activity);
  const action = getActivityAction(activity);
  const secondaryAction = getSecondaryAction(activity);
  const customerLabel = formatDashboardCustomerLabel(activity.customerName, activity.customerId);
  const handleActionClick = () => {
    if (activity.customerName || activity.customerId) {
      onSelectCustomer(activity.customerName || null, activity.customerId || null);
    }
    if (action) {
      onNavigate(action.page);
    }
  };
  const handleSecondaryActionClick = () => {
    if (activity.customerName || activity.customerId) {
      onSelectCustomer(activity.customerName || null, activity.customerId || null);
    }
    if (secondaryAction) {
      onNavigate(secondaryAction.page);
    }
  };

  return (
    <div className="flex items-start justify-between gap-4 border-b border-slate-100 py-3 last:border-b-0">
      <div className="flex items-start gap-3">
        <div className={`mt-0.5 flex h-10 w-10 items-center justify-center rounded-xl ${typeConfig.bgColor}`}>
          <Icon size={18} className={typeConfig.iconColor} />
        </div>
        <div>
          <div className="text-sm font-semibold text-slate-800">{title}</div>
          {customerLabel ? (
            <div className="mt-2">
              <button
                type="button"
                onClick={() => {
                  onSelectCustomer(activity.customerName || null, activity.customerId || null);
                  onNavigate('data');
                }}
                className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] font-medium text-slate-600 transition-colors hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700"
              >
                当前客户：{customerLabel}
              </button>
            </div>
          ) : null}
          <div className="mt-1 text-sm text-slate-600">{description}</div>
          <div className="mt-2 text-xs text-slate-400">
            {activity.username ? `${activity.username} · ` : ''}
            {activity.time}
          </div>
          {action ? (
            <button
              type="button"
              onClick={handleActionClick}
              className="mt-3 inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700"
            >
              <span>{action.label}</span>
              <ArrowRight size={12} />
            </button>
          ) : null}
        </div>
      </div>

      <div className="flex flex-col items-end gap-2">
        {action ? (
          <button
            type="button"
            onClick={handleActionClick}
            className={`rounded-full px-2.5 py-1 text-xs font-medium transition-opacity hover:opacity-85 ${badge.className}`}
          >
            {badge.label}
          </button>
        ) : (
          <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${badge.className}`}>{badge.label}</span>
        )}
        {secondaryBadge ? (
          secondaryAction ? (
            <button
              type="button"
              onClick={handleSecondaryActionClick}
              className={`rounded-full px-2.5 py-1 text-[11px] font-medium transition-opacity hover:opacity-85 ${secondaryBadge.className}`}
              title={secondaryAction.label}
            >
              {secondaryBadge.label}
            </button>
          ) : (
            <span className={`rounded-full px-2.5 py-1 text-[11px] font-medium ${secondaryBadge.className}`}>
              {secondaryBadge.label}
            </span>
          )
        ) : null}
      </div>
    </div>
  );
}

const ActivitySkeleton: React.FC = () => (
  <div className="animate-pulse">
    {[1, 2, 3, 4].map((item) => (
      <div key={item} className="flex items-center justify-between border-b border-slate-100 py-3 last:border-b-0">
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 rounded-xl bg-slate-200" />
          <div>
            <div className="mb-1 h-4 w-44 rounded bg-slate-200" />
            <div className="h-3 w-28 rounded bg-slate-200" />
          </div>
        </div>
        <div className="h-6 w-16 rounded-full bg-slate-200" />
      </div>
    ))}
  </div>
);

const DashboardPage: React.FC<DashboardPageProps> = ({ onNavigate }) => {
  const { state, setCurrentCustomer } = useApp();
  const [stats, setStats] = useState<DashboardStats>(DEFAULT_STATS);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [cacheStatus, setCacheStatus] = useState<WikiCacheStatusResponse | null>(null);
  const [cacheLoading, setCacheLoading] = useState(false);
  const [cacheError, setCacheError] = useState<string | null>(null);

  const fetchData = async (isRefresh = false) => {
    if (isRefresh) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    setError(null);

      try {
      const [statsData, activitiesData, cacheStatusData, customersData] = await Promise.all([
          getDashboardStats(),
          getDashboardActivities(10),
          getWikiCacheStatus().catch(() => null),
          listCustomers().catch(() => []),
        ]);

      const fallbackStats = buildFallbackStats(customersData, activitiesData.activities);
      setStats(mergeDashboardStats(statsData, fallbackStats));
        setActivities(activitiesData.activities);
        setCacheStatus(cacheStatusData);
      } catch (err) {
      console.error('Failed to fetch dashboard data:', err);
      setError(err instanceof Error ? err.message : '获取工作台数据失败，请稍后重试。');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefreshCache = async () => {
    setCacheLoading(true);
    setCacheError(null);

    try {
      const result = await refreshWikiCache();
      setCacheStatus((previous) => ({
        cached: true,
        lastUpdated: result.lastUpdated,
        enterpriseProductCount: previous?.enterpriseProductCount || 0,
        personalProductCount: previous?.personalProductCount || 0,
      }));
      const newStatus = await getWikiCacheStatus();
      setCacheStatus(newStatus);
    } catch (err) {
      console.error('Failed to refresh cache:', err);
      setCacheError(err instanceof Error ? err.message : '刷新产品库失败，请稍后再试。');
    } finally {
      setCacheLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const sessionActivities = state.system.recentActivities.map((item) => ({
    id: item.id,
    type: item.type,
    time: formatLocalDateTime(item.createdAt),
    createdAt: item.createdAt,
    status: item.status,
    customerName: item.customerName || '',
    customerId: item.customerId || '',
    title: item.title,
    description: item.description,
    username: '',
    metadata: {},
  }));

  const systemFeed = useMemo(() => {
    const merged = [...activities, ...sessionActivities];
    const seen = new Set<string>();

    return merged
      .filter((activity) => {
        const key = [activity.type, activity.title || '', activity.customerId || '', activity.time].join('|');
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .slice(0, 5);
  }, [activities, sessionActivities]);

  return (
    <div className="bg-[linear-gradient(180deg,#f8fafc_0%,#f1f5f9_100%)] p-6" data-testid="dashboard-page">
      <section className="mb-6 rounded-[28px] border border-slate-200/80 bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.16),_transparent_36%),linear-gradient(135deg,#ffffff_0%,#f8fafc_100%)] p-6 shadow-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-800">贷款助手工作台</h1>
            <p className="mt-1 text-sm text-slate-500">集中查看客户业务进展、系统动态和方案产品库状态，适合日常处理与演示验收。</p>
            <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-slate-500">
              <span className="rounded-full border border-slate-200 bg-white px-3 py-1">{getSystemVersionLabel()}</span>
              <span className="rounded-full border border-slate-200 bg-white px-3 py-1">构建日期 {SYSTEM_INFO.releaseDate}</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {DASHBOARD_QUICK_LINKS.map((link) => (
              <button
                key={link.label}
                type="button"
                onClick={() => onNavigate(link.page)}
                className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors hover:opacity-85 ${link.className}`}
              >
                {link.label}
              </button>
            ))}
          </div>
        </div>
      </section>

      <section className="mb-6" data-testid="stats-section">
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          {STAT_CARD_CONFIGS.map((config) => (
            <div key={config.id} className="space-y-2">
              <button
                type="button"
                onClick={() => onNavigate(getStatCardAction(config.id).page)}
                className="block w-full rounded-[24px] text-left transition-transform hover:-translate-y-0.5 focus:outline-none focus:ring-2 focus:ring-blue-400/40"
                title={getStatCardAction(config.id).label}
              >
                <StatCard
                  icon={config.icon}
                  iconColor={config.iconColor}
                  iconBgColor={config.iconBgColor}
                  title={config.title}
                  value={loading ? '-' : stats[config.id] ?? 0}
                />
              </button>
              <p className="px-1 text-xs leading-5 text-slate-500">{config.subtitle}</p>
            </div>
          ))}
        </div>
        <div className="mt-4 grid gap-4 lg:grid-cols-[1.4fr_0.8fr]">
          <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-slate-800">时间轴式业务流程</h2>
                <p className="mt-1 text-sm text-slate-500">从资料上传到风险报告的完整处理链，适合演示客户资料如何进入后续流程。</p>
              </div>
              <span className="rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700">资料驱动</span>
            </div>
            <div className="mt-5 grid gap-3 lg:grid-cols-5">
              {WORKFLOW_STEPS.map((step, index) => {
                const Icon = step.icon;
                return (
                  <button
                    key={step.key}
                    type="button"
                    onClick={() => onNavigate(step.page)}
                    className="relative rounded-2xl border border-slate-200 bg-slate-50 p-4 text-left transition-all hover:border-blue-200 hover:bg-blue-50/50 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-400/40"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-white text-blue-600 shadow-sm">
                        <Icon size={18} />
                      </div>
                      <span className="text-xs font-medium text-slate-400">0{index + 1}</span>
                    </div>
                    <div className="mt-4 text-sm font-semibold text-slate-800">{step.title}</div>
                    <div className="mt-2 text-xs leading-5 text-slate-500">{step.description}</div>
                    {index < WORKFLOW_STEPS.length - 1 ? (
                      <div className="pointer-events-none absolute -right-2 top-1/2 hidden -translate-y-1/2 lg:block">
                        <ArrowRight size={16} className="text-slate-300" />
                      </div>
                    ) : null}
                  </button>
                );
              })}
            </div>
          </div>

          <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-slate-800">风险概览</h2>
                <p className="mt-1 text-sm text-slate-500">帮助你快速判断当前需要优先处理哪些客户。</p>
              </div>
              <span className="rounded-full border border-rose-200 bg-rose-50 px-3 py-1 text-xs font-medium text-rose-700">
                高风险 {stats.highRiskCustomers ?? 0} 位
              </span>
            </div>
            <div className="mt-5 space-y-3">
              <button
                type="button"
                onClick={() => onNavigate('customers')}
                className="block w-full rounded-2xl border border-rose-100 bg-[linear-gradient(135deg,#fff1f2_0%,#fff7ed_100%)] p-4 text-left transition-all hover:-translate-y-0.5 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-rose-300/40"
              >
                <div className="text-xs font-medium text-rose-600">高风险客户预警</div>
                <div className="mt-2 text-2xl font-semibold text-slate-800">{stats.highRiskCustomers ?? 0}</div>
                <div className="mt-1 text-sm leading-6 text-slate-500">已生成风险报告且风险等级为高的客户会在列表和报告页以克制的预警样式提示。</div>
                <div className="mt-3 inline-flex items-center gap-1 text-xs font-medium text-rose-700">
                  查看高风险客户
                  <ArrowRight size={12} />
                </div>
              </button>
              <button
                type="button"
                onClick={() => onNavigate('data')}
                className="block w-full rounded-2xl border border-amber-100 bg-amber-50 p-4 text-left transition-all hover:-translate-y-0.5 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-amber-300/40"
              >
                <div className="text-xs font-medium text-amber-700">待补资料提醒</div>
                <div className="mt-2 text-2xl font-semibold text-slate-800">{stats.pendingMaterialCustomers ?? 0}</div>
                <div className="mt-1 text-sm leading-6 text-slate-500">缺少征信、流水、纳税等关键资料的客户，建议先补件再重新生成结果。</div>
                <div className="mt-3 inline-flex items-center gap-1 text-xs font-medium text-amber-700">
                  去资料汇总处理
                  <ArrowRight size={12} />
                </div>
              </button>
            </div>
          </div>
        </div>
      </section>

      <section className="mb-6 rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm" data-testid="system-activity-section">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">系统动态</h2>
            <p className="mt-1 text-sm text-slate-500">优先展示最近发生的关键业务动作，也会标出哪些结果已经需要重新生成或重新匹配。</p>
          </div>
          <span className="rounded-full bg-slate-100 px-3 py-1 text-xs text-slate-500">最近 {Math.min(systemFeed.length, 5)} 条</span>
        </div>

        {systemFeed.length > 0 ? (
          <div className="space-y-1">
            {systemFeed.map((activity) => (
              <ActivityRow
                key={`system-${activity.id}`}
                activity={activity}
                onNavigate={onNavigate}
                onSelectCustomer={setCurrentCustomer}
              />
            ))}
          </div>
        ) : (
          <div className="rounded-xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
            暂无系统动态。上传资料、保存资料汇总、生成申请表、方案匹配或风险报告后会自动显示在这里。</div>
        )}
      </section>

      <section className="mb-6 rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm" data-testid="product-cache-section">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">产品库缓存</h2>
            <p className="mt-1 text-sm text-slate-500">用于方案匹配的产品库缓存状态，建议在演示前先完成刷新。</p>
          </div>
          <button
            onClick={handleRefreshCache}
            disabled={cacheLoading}
            className="flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2.5 text-white shadow-sm transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
            data-testid="refresh-cache-button"
          >
            <RefreshCw size={16} className={cacheLoading ? 'animate-spin' : ''} />
            {cacheLoading ? '更新中...' : '更新产品库'}
          </button>
        </div>

        {cacheError ? (
          <div className="mb-4 flex items-center gap-2 rounded-lg bg-rose-50 p-4 text-rose-600">
            <AlertCircle size={20} />
            <span>{cacheError}</span>
          </div>
        ) : null}

        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <div className="rounded-lg bg-slate-50 p-3">
            <p className="text-sm text-slate-500">缓存状态</p>
            <p className="text-lg font-medium text-slate-800">{cacheStatus?.cached ? '已缓存' : '未缓存'}</p>
          </div>
          <div className="rounded-lg bg-slate-50 p-3">
            <p className="text-sm text-slate-500">最后更新</p>
            <p className="text-sm font-medium text-slate-800">
              {cacheStatus?.lastUpdated ? formatLocalDateTime(cacheStatus.lastUpdated) : '-'}
            </p>
          </div>
          <div className="rounded-lg bg-slate-50 p-3">
            <p className="text-sm text-slate-500">企业产品</p>
            <p className="text-lg font-medium text-slate-800">{cacheStatus?.enterpriseProductCount || 0} 项</p>
          </div>
          <div className="rounded-lg bg-slate-50 p-3">
            <p className="text-sm text-slate-500">个人产品</p>
            <p className="text-lg font-medium text-slate-800">{cacheStatus?.personalProductCount || 0} 项</p>
          </div>
        </div>
      </section>

      <section className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm" data-testid="recent-activity-section">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">后台活动记录</h2>
            <p className="mt-1 text-sm text-slate-500">展示持久化保存的最近活动，可用于演示和验收追踪。</p>
          </div>
          <button
            onClick={() => fetchData(true)}
            disabled={refreshing}
            className="flex items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3.5 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-100 hover:text-slate-800 disabled:opacity-50"
            data-testid="refresh-button"
          >
            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
            刷新
          </button>
        </div>

        {error ? (
          <div className="mb-4 flex items-center gap-2 rounded-lg bg-rose-50 p-4 text-rose-600">
            <AlertCircle size={20} />
            <span>{error}</span>
          </div>
        ) : null}

        {loading ? (
          <ActivitySkeleton />
        ) : activities.length > 0 ? (
          <div className="divide-y divide-slate-100">
            {activities.map((activity) => (
              <ActivityRow
                key={activity.id}
                activity={activity}
                onNavigate={onNavigate}
                onSelectCustomer={setCurrentCustomer}
              />
            ))}
          </div>
        ) : (
          <p className="py-8 text-center text-slate-500">暂无后台活动记录</p>
        )}
      </section>
    </div>
  );
};

export default DashboardPage;

export type { DashboardStats, Activity };




