import React from 'react';
import { LucideIcon, ArrowUp, ArrowDown } from 'lucide-react';

/**
 * 趋势数据接口
 */
export interface TrendData {
  value: number;        // 百分比变化
  direction: 'up' | 'down';
}

/**
 * StatCard Props
 */
export interface StatCardProps {
  icon: LucideIcon;
  iconColor: string;      // Tailwind 颜色类，如 'text-blue-500'
  iconBgColor: string;    // 图标背景色，如 'bg-blue-100'
  title: string;
  value: number | string;
  trend?: TrendData;
}

/**
 * StatCard 组件 - 统计卡片
 * 
 * 设计规范：
 * - 卡片: 白色背景 (#FFFFFF), 圆角 12px, 阴影
 * - 图标区域: 48px 圆形背景
 * - 标题: 灰色小字 (#6B7280)
 * - 数值: 大号粗体 (#1F2937)
 * - 趋势上升: 绿色 (#10B981) + ArrowUp 图标
 * - 趋势下降: 红色 (#EF4444) + ArrowDown 图标
 * 
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
 */
const StatCard: React.FC<StatCardProps> = ({
  icon: Icon,
  iconColor,
  iconBgColor,
  title,
  value,
  trend,
}) => {
  // 格式化显示值
  const displayValue = value === undefined || value === null ? '—' : value;

  return (
    <div
      className="bg-white rounded-xl shadow-sm p-5 flex items-start gap-4"
      style={{
        borderRadius: '12px',
        boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.1)',
      }}
      data-testid="stat-card"
    >
      {/* 图标区域 - 48px 圆形背景 */}
      <div
        className={`w-12 h-12 rounded-full flex items-center justify-center ${iconBgColor}`}
        data-testid="stat-card-icon-container"
      >
        <Icon
          className={iconColor}
          size={24}
          data-testid="stat-card-icon"
        />
      </div>

      {/* 内容区域 */}
      <div className="flex-1 min-w-0">
        {/* 标题 */}
        <p
          className="text-sm font-medium"
          style={{ color: '#6B7280' }}
          data-testid="stat-card-title"
        >
          {title}
        </p>

        {/* 数值和趋势 */}
        <div className="flex items-baseline gap-2 mt-1">
          {/* 数值 */}
          <span
            className="text-2xl font-bold"
            style={{ color: '#1F2937' }}
            data-testid="stat-card-value"
          >
            {displayValue}
          </span>

          {/* 趋势指示器 */}
          {trend && (
            <div
              className={`flex items-center text-sm font-medium ${
                trend.direction === 'up' ? 'text-green-500' : 'text-red-500'
              }`}
              style={{
                color: trend.direction === 'up' ? '#10B981' : '#EF4444',
              }}
              data-testid="stat-card-trend"
            >
              {trend.direction === 'up' ? (
                <ArrowUp size={16} data-testid="trend-arrow-up" />
              ) : (
                <ArrowDown size={16} data-testid="trend-arrow-down" />
              )}
              <span data-testid="trend-value">{Math.abs(trend.value)}%</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default StatCard;
