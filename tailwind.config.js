/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Primary color
        primary: {
          DEFAULT: '#3B82F6',
          50: '#EFF6FF',
          100: '#DBEAFE',
          500: '#3B82F6',
          600: '#2563EB',
          700: '#1D4ED8',
        },
        // Sidebar colors
        sidebar: {
          bg: '#1E293B',
          hover: '#334155',
          active: '#3B82F6',
          text: '#FFFFFF',
          textMuted: 'rgba(255, 255, 255, 0.8)',
        },
        // Page colors
        page: {
          bg: '#F8FAFC',
        },
        card: {
          bg: '#FFFFFF',
        },
        // Text colors
        text: {
          primary: '#1F2937',
          secondary: '#6B7280',
        },
        // Status colors
        status: {
          success: '#10B981',
          warning: '#F59E0B',
          error: '#EF4444',
          info: '#3B82F6',
        },
        // Border color
        border: {
          DEFAULT: '#E5E7EB',
        },
        // Chat colors
        chat: {
          aiAvatar: '#6366F1',
          userMessage: '#3B82F6',
          aiMessage: '#F3F4F6',
        },
        // Legacy colors (kept for backward compatibility)
        'sidebar-bg': '#1E293B',
        'page-bg': '#F8FAFC',
        'card-bg': '#FFFFFF',
        'text-primary': '#1F2937',
        'text-secondary': '#6B7280',
        'text-white': '#FFFFFF',
        success: '#10B981',
        warning: '#F59E0B',
        error: '#EF4444',
      },
      borderRadius: {
        'sm': '6px',
        'md': '8px',
        'lg': '12px',
        'xl': '16px',
        'card': '12px',
        'button': '8px',
        'input': '8px',
        'badge': '6px',
        'message-user': '18px 18px 4px 18px',
        'message-ai': '18px 18px 18px 4px',
      },
      boxShadow: {
        'card': '0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1)',
        'card-hover': '0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)',
      },
      spacing: {
        'sidebar': '240px',
        'header': '64px',
        'content-padding': '24px',
        'card-padding': '20px',
        'gap-sm': '8px',
        'gap-md': '12px',
        'gap-lg': '16px',
        'gap-xl': '24px',
      },
    },
  },
  plugins: [],
}
