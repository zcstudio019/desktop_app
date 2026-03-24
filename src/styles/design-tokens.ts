/**
 * Design Tokens
 * 
 * Centralized design system constants for consistent styling.
 * These values should match tailwind.config.js extensions.
 * 
 * @see design.md - Data Models section for specification
 */

export const COLORS = {
  // 主色
  primary: '#3B82F6',
  
  // 侧边栏
  sidebarBg: '#1E293B',
  sidebarText: '#FFFFFF',
  sidebarTextMuted: '#FFFFFFCC',
  
  // 页面
  pageBg: '#F8FAFC',
  cardBg: '#FFFFFF',
  
  // 文字
  textPrimary: '#1F2937',
  textSecondary: '#6B7280',
  
  // 状态
  success: '#10B981',
  warning: '#F59E0B',
  error: '#EF4444',
  
  // 边框
  border: '#E5E7EB',
  
  // 聊天
  aiAvatar: '#6366F1',
  userMessage: '#3B82F6',
  aiMessage: '#F3F4F6',
  
  // Extended color palette for components
  sidebar: {
    bg: '#1E293B',
    hover: '#334155',
    active: '#3B82F6',
    text: 'rgba(255, 255, 255, 0.8)',
    textActive: '#FFFFFF',
  },
  primaryPalette: {
    50: '#EFF6FF',
    100: '#DBEAFE',
    500: '#3B82F6',
    600: '#2563EB',
    700: '#1D4ED8',
  },
  status: {
    success: '#10B981',
    warning: '#F59E0B',
    error: '#EF4444',
    info: '#3B82F6',
  },
  neutral: {
    50: '#F8FAFC',
    100: '#F1F5F9',
    200: '#E2E8F0',
    300: '#CBD5E1',
    400: '#94A3B8',
    500: '#64748B',
    600: '#475569',
    700: '#334155',
    800: '#1E293B',
    900: '#0F172A',
  },
} as const;

export const SPACING = {
  sidebarWidth: 240,
  headerHeight: 64,
  contentPadding: 24,
  cardPadding: 20,
  gap: {
    sm: 8,
    md: 12,
    lg: 16,
    xl: 24,
  },
  // Legacy spacing values for backward compatibility
  sidebar: 240,
  header: 64,
  sectionGap: 24,
  inputPadding: {
    x: 16,
    y: 10,
  },
} as const;

export const BORDER_RADIUS = {
  sm: 6,
  md: 8,
  lg: 12,
  xl: 16,
  full: 9999,
  messageBubble: {
    user: '18px 18px 4px 18px',
    ai: '18px 18px 18px 4px',
  },
  // Legacy values for backward compatibility
  card: 12,
  button: 8,
  input: 8,
  badge: 6,
} as const;

export const SHADOWS = {
  card: '0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1)',
  cardHover: '0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)',
} as const;

export const TYPOGRAPHY = {
  fontFamily: {
    sans: ['Inter', 'system-ui', 'sans-serif'],
    mono: ['JetBrains Mono', 'monospace'],
  },
  fontSize: {
    xs: 12,
    sm: 14,
    base: 16,
    lg: 18,
    xl: 20,
    '2xl': 24,
  },
} as const;

// Icon sizing constants (Requirements 10.2)
export const ICON_SIZES = {
  navigation: 20,  // 20px for navigation icons
  inline: 16,      // 16px for inline icons
  large: 24,       // 24px for large icons
} as const;

// Type exports for TypeScript usage
export type ColorKey = keyof typeof COLORS;
export type SpacingKey = keyof typeof SPACING;
export type BorderRadiusKey = keyof typeof BORDER_RADIUS;
