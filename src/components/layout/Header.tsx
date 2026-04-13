import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Bell, CheckCircle2, KeyRound, LogOut, PencilLine, Settings, ShieldAlert, ShieldCheck, User, X } from 'lucide-react';
import {
  changeCurrentUserPassword,
  getCurrentUser,
  setCurrentUserSecurityQuestion,
  updateCurrentUserProfile,
} from '../../services/api';
import type { UserInfo } from '../../services/types';
import { ApiError } from '../../services/types';
import { SYSTEM_INFO, getSystemVersionLabel } from '../../config/systemInfo';
import type { PageType } from './Sidebar';

export interface HeaderProps {
  pageTitle: string;
  userName?: string;
  userRole?: string;
  notificationCount?: number;
  onNavigate?: (page: PageType) => void;
  onLogout?: () => void;
}

interface NotificationBellProps {
  count?: number;
}

type BannerState =
  | {
      type: 'success' | 'error';
      title: string;
      message: string;
    }
  | null;

function formatLocalDateTime(value?: string | null): string {
  if (!value) return '未记录';
  const hasExplicitTimezone = /([zZ]|[+\-]\d{2}:\d{2})$/.test(value);
  const normalized = value.includes('T')
    ? hasExplicitTimezone
      ? value
      : `${value}Z`
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return value;
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
}

function StatusChip({
  tone,
  children,
}: {
  tone: 'blue' | 'green' | 'amber' | 'slate';
  children: React.ReactNode;
}) {
  const toneClass =
    tone === 'green'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : tone === 'amber'
        ? 'border-amber-200 bg-amber-50 text-amber-700'
        : tone === 'blue'
          ? 'border-blue-200 bg-blue-50 text-blue-700'
          : 'border-slate-200 bg-slate-50 text-slate-600';

  return <span className={`rounded-full border px-2.5 py-1 text-[11px] ${toneClass}`}>{children}</span>;
}

const NotificationBell: React.FC<NotificationBellProps> = ({ count = 0 }) => {
  const hasNotifications = count > 0;

  return (
    <button
      className="relative rounded-xl p-2.5 transition-colors hover:bg-slate-100"
      aria-label={hasNotifications ? `${count} 条未读通知` : '通知'}
      data-testid="notification-bell"
      type="button"
    >
      <Bell size={20} className="text-slate-600" />
      {hasNotifications ? (
        <span
          className="absolute right-1.5 top-1.5 h-2 w-2 rounded-full bg-red-500"
          data-testid="notification-dot"
          aria-hidden="true"
        />
      ) : null}
    </button>
  );
};

interface ModalShellProps {
  title: string;
  description: string;
  onClose: () => void;
  children: React.ReactNode;
}

const ModalShell: React.FC<ModalShellProps> = ({ title, description, onClose, children }) => (
  <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/30 p-4">
    <div className="w-full max-w-md rounded-3xl border border-slate-200 bg-white shadow-2xl">
      <div className="flex items-start justify-between border-b border-slate-100 px-6 py-5">
        <div>
          <h3 className="text-lg font-semibold text-slate-800">{title}</h3>
          <p className="mt-1 text-sm text-slate-500">{description}</p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="rounded-xl p-2 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-600"
        >
          <X size={18} />
        </button>
      </div>
      <div className="px-6 py-5">{children}</div>
    </div>
  </div>
);

const Header: React.FC<HeaderProps> = ({
  pageTitle,
  userName = '用户',
  userRole = 'user',
  notificationCount = 0,
  onNavigate,
  onLogout,
}) => {
  const [menuOpen, setMenuOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [passwordOpen, setPasswordOpen] = useState(false);
  const [securityOpen, setSecurityOpen] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [savingProfile, setSavingProfile] = useState(false);
  const [changingPassword, setChangingPassword] = useState(false);
  const [savingSecurity, setSavingSecurity] = useState(false);
  const [profile, setProfile] = useState<UserInfo | null>(null);
  const [banner, setBanner] = useState<BannerState>(null);
  const [profileForm, setProfileForm] = useState({ displayName: '', phone: '' });
  const [passwordForm, setPasswordForm] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: '',
  });
  const [securityForm, setSecurityForm] = useState({
    question: '',
    answer: '',
  });
  const menuRef = useRef<HTMLDivElement | null>(null);

  const displayName = useMemo(() => profile?.display_name?.trim() || userName, [profile?.display_name, userName]);
  const initial = displayName.charAt(0).toUpperCase();
  const roleLabel = (profile?.role || userRole) === 'admin' ? '管理员' : '普通用户';
  const hasSecurityQuestion = Boolean(profile?.has_security_question);
  const profileComplete = Boolean(profile?.phone || profile?.display_name);
  const updatedAtLabel = formatLocalDateTime(profile?.updated_at || profile?.created_at);

  const loadProfile = async (): Promise<void> => {
    setLoadingProfile(true);
    try {
      const current = await getCurrentUser();
      setProfile(current);
      setProfileForm({
        displayName: current.display_name || '',
        phone: current.phone || '',
      });
    } catch (error) {
      const message = error instanceof ApiError ? error.message : '暂时无法获取账号信息';
      setBanner({ type: 'error', title: '账号信息加载失败', message });
    } finally {
      setLoadingProfile(false);
    }
  };

  useEffect(() => {
    void loadProfile();
  }, []);

  useEffect(() => {
    if (!banner) return undefined;
    const timer = window.setTimeout(() => setBanner(null), 3200);
    return () => window.clearTimeout(timer);
  }, [banner]);

  useEffect(() => {
    if (!menuOpen) return undefined;
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [menuOpen]);

  const openProfile = async (): Promise<void> => {
    setMenuOpen(false);
    setProfileOpen(true);
    await loadProfile();
  };

  const openPassword = async (): Promise<void> => {
    setMenuOpen(false);
    setPasswordOpen(true);
    if (!profile) await loadProfile();
  };

  const openSecurity = async (): Promise<void> => {
    setMenuOpen(false);
    setSecurityOpen(true);
    if (!profile) await loadProfile();
  };

  const handleSaveProfile = async (): Promise<void> => {
    setSavingProfile(true);
    try {
      const updated = await updateCurrentUserProfile({
        display_name: profileForm.displayName.trim(),
        phone: profileForm.phone.trim(),
      });
      setProfile(updated);
      setBanner({
        type: 'success',
        title: '账号信息已更新',
        message: `最近更新时间：${formatLocalDateTime(updated.updated_at || updated.created_at)}`,
      });
      setProfileOpen(false);
    } catch (error) {
      const message = error instanceof ApiError ? error.message : '账号信息更新失败，请稍后再试';
      setBanner({ type: 'error', title: '账号信息更新失败', message });
    } finally {
      setSavingProfile(false);
    }
  };

  const handleChangePassword = async (): Promise<void> => {
    if (!passwordForm.currentPassword || !passwordForm.newPassword) {
      setBanner({ type: 'error', title: '修改密码失败', message: '请先填写完整的密码信息。' });
      return;
    }
    if (passwordForm.newPassword.length < 6) {
      setBanner({ type: 'error', title: '修改密码失败', message: '新密码长度至少 6 位。' });
      return;
    }
    if (passwordForm.newPassword !== passwordForm.confirmPassword) {
      setBanner({ type: 'error', title: '修改密码失败', message: '两次输入的新密码不一致。' });
      return;
    }

    setChangingPassword(true);
    try {
      const result = await changeCurrentUserPassword({
        current_password: passwordForm.currentPassword,
        new_password: passwordForm.newPassword,
      });
      setBanner({
        type: 'success',
        title: '密码已更新',
        message: `${result.message || '新密码将在下次登录时生效。'} 为了账号安全，请不要与他人共享密码。`,
      });
      setPasswordForm({ currentPassword: '', newPassword: '', confirmPassword: '' });
      setPasswordOpen(false);
      await loadProfile();
    } catch (error) {
      const message = error instanceof ApiError ? error.message : '密码修改失败，请稍后再试';
      setBanner({ type: 'error', title: '修改密码失败', message });
    } finally {
      setChangingPassword(false);
    }
  };

  const handleSaveSecurityQuestion = async (): Promise<void> => {
    if (!securityForm.question.trim() || !securityForm.answer.trim()) {
      setBanner({ type: 'error', title: '设置密保失败', message: '请先填写完整的密保问题和答案。' });
      return;
    }

    setSavingSecurity(true);
    try {
      const updated = await setCurrentUserSecurityQuestion({
        security_question: securityForm.question.trim(),
        security_answer: securityForm.answer.trim(),
      });
      setProfile(updated);
      setBanner({
        type: 'success',
        title: '密保已设置',
        message: '后续如忘记密码，可通过密保问题进行自助找回。',
      });
      setSecurityForm({ question: '', answer: '' });
      setSecurityOpen(false);
    } catch (error) {
      const message = error instanceof ApiError ? error.message : '密保设置失败，请稍后再试';
      setBanner({ type: 'error', title: '设置密保失败', message });
    } finally {
      setSavingSecurity(false);
    }
  };

  return (
    <>
      <header
        className="relative z-40 flex h-16 items-center justify-between border-b border-slate-200 bg-white/95 px-6 backdrop-blur"
        style={{ boxShadow: '0 1px 3px rgba(15,23,42,0.04)' }}
        data-testid="header"
      >
        <div>
          <h1 className="text-xl font-semibold text-slate-800" data-testid="page-title">
            {pageTitle}
          </h1>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-slate-400">
            <span>{SYSTEM_INFO.subtitle}</span>
            <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] text-slate-500">
              {getSystemVersionLabel()}
            </span>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <NotificationBell count={notificationCount} />

          <div className="relative z-50" ref={menuRef}>
            <button
              type="button"
              onClick={() => setMenuOpen((prev) => !prev)}
              className="flex items-center gap-3 rounded-2xl border border-slate-200 px-2.5 py-1.5 transition-colors hover:bg-slate-50"
              data-testid="user-avatar"
            >
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-blue-500 shadow-sm shadow-blue-200">
                {initial ? <span className="text-sm font-medium text-white">{initial}</span> : <User size={20} className="text-white" />}
              </div>
              <div className="hidden text-left sm:block">
                <div className="text-sm font-medium text-slate-700" data-testid="user-name">
                  {displayName}
                </div>
                <div className="text-[11px] text-slate-400">当前登录账号</div>
              </div>
            </button>

            {menuOpen ? (
              <div className="absolute right-0 top-[calc(100%+10px)] z-[70] w-80 rounded-3xl border border-slate-200 bg-white p-4 shadow-xl">
                <div className="rounded-2xl border border-slate-100 bg-slate-50 px-4 py-4">
                  <div className="flex items-center gap-3">
                    <div className="flex h-12 w-12 items-center justify-center rounded-full bg-blue-500 text-base font-semibold text-white">
                      {initial || 'U'}
                    </div>
                    <div className="min-w-0">
                      <div className="truncate text-sm font-semibold text-slate-800">{displayName}</div>
                      <div className="mt-1 text-xs text-slate-500">用户名：{profile?.username || userName}</div>
                      <div className="mt-1 text-xs text-slate-500">{roleLabel}</div>
                    </div>
                  </div>
                </div>

                <div className="mt-3 rounded-2xl border border-slate-100 bg-white px-4 py-3">
                  <div className="flex items-center gap-2 text-sm font-medium text-slate-700">
                    <ShieldCheck size={16} className="text-emerald-500" />
                    账号状态
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-3">
                    <div className="rounded-2xl border border-blue-100 bg-blue-50/70 px-3 py-3">
                      <div className="flex items-center gap-1.5 text-[11px] text-blue-500">
                        <CheckCircle2 size={12} />
                        最近登录
                      </div>
                      <div className="mt-1 text-xs font-medium text-slate-700">
                        {formatLocalDateTime(profile?.last_login_at)}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 px-3 py-3">
                      <div className="flex items-center gap-1.5 text-[11px] text-slate-500">
                        <PencilLine size={12} />
                        最近修改
                      </div>
                      <div className="mt-1 text-xs font-medium text-slate-700">{updatedAtLabel}</div>
                    </div>
                    <div className="rounded-2xl border border-slate-100 bg-white px-3 py-3">
                      <div className="flex items-center gap-1.5 text-[11px] text-slate-500">
                        {hasSecurityQuestion ? <ShieldCheck size={12} className="text-emerald-500" /> : <ShieldAlert size={12} className="text-amber-500" />}
                        安全状态
                      </div>
                      <div className="mt-2">
                        <button
                          type="button"
                          onClick={!hasSecurityQuestion ? openSecurity : undefined}
                          className={!hasSecurityQuestion ? 'cursor-pointer' : 'cursor-default'}
                        >
                          <StatusChip tone={hasSecurityQuestion ? 'green' : 'amber'}>
                            {hasSecurityQuestion ? '已设置密保' : '未设置密保'}
                          </StatusChip>
                        </button>
                      </div>
                    </div>
                    <div className="rounded-2xl border border-slate-100 bg-white px-3 py-3">
                      <div className="flex items-center gap-1.5 text-[11px] text-slate-500">
                        {profileComplete ? <CheckCircle2 size={12} className="text-emerald-500" /> : <ShieldAlert size={12} className="text-amber-500" />}
                        资料完善情况
                      </div>
                      <div className="mt-2">
                        <button
                          type="button"
                          onClick={openProfile}
                          className="cursor-pointer"
                        >
                          <StatusChip tone={profileComplete ? 'green' : 'amber'}>
                            {profileComplete ? '资料已完善' : '建议补充资料'}
                          </StatusChip>
                        </button>
                      </div>
                      <div className="mt-2 text-[11px] leading-5 text-slate-400">
                        {profileComplete ? '当前账号的基础资料已可用于系统展示。' : '补充展示名称或联系电话后，这里会自动更新为已完善。'}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="mt-4 space-y-2">
                  <button
                    type="button"
                    onClick={openProfile}
                    className="flex w-full items-center justify-between rounded-2xl border border-slate-200 px-4 py-3 text-left transition-colors hover:bg-slate-50"
                  >
                    <div>
                      <div className="text-sm font-medium text-slate-700">查看当前账号信息</div>
                      <div className="mt-1 text-xs text-slate-500">查看用户名、角色、登录时间，并维护展示名称与联系电话。</div>
                    </div>
                    <PencilLine size={16} className="text-slate-400" />
                  </button>

                  <button
                    type="button"
                    onClick={openPassword}
                    className="flex w-full items-center justify-between rounded-2xl border border-slate-200 px-4 py-3 text-left transition-colors hover:bg-slate-50"
                  >
                    <div>
                      <div className="text-sm font-medium text-slate-700">修改密码</div>
                      <div className="mt-1 text-xs text-slate-500">修改当前账号登录密码，更新后下次登录生效。</div>
                    </div>
                    <KeyRound size={16} className="text-slate-400" />
                  </button>

                  {(profile?.role || userRole) === 'admin' ? (
                    <button
                      type="button"
                      onClick={() => {
                        setMenuOpen(false);
                        onNavigate?.('admin');
                      }}
                      className="flex w-full items-center justify-between rounded-2xl border border-slate-200 px-4 py-3 text-left transition-colors hover:bg-slate-50"
                    >
                      <div>
                        <div className="text-sm font-medium text-slate-700">进入账号管理</div>
                        <div className="mt-1 text-xs text-slate-500">查看系统账号列表，并执行管理员侧维护操作。</div>
                      </div>
                      <Settings size={16} className="text-slate-400" />
                    </button>
                  ) : null}

                  <button
                    type="button"
                    onClick={() => {
                      setMenuOpen(false);
                      onLogout?.();
                    }}
                    className="flex w-full items-center justify-between rounded-2xl border border-rose-200 px-4 py-3 text-left transition-colors hover:bg-rose-50"
                  >
                    <div>
                      <div className="text-sm font-medium text-rose-700">退出登录</div>
                      <div className="mt-1 text-xs text-rose-500">退出当前账号，返回登录页面。</div>
                    </div>
                    <LogOut size={16} className="text-rose-400" />
                  </button>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </header>

      {banner ? (
        <div className="fixed right-6 top-20 z-50 max-w-sm">
          <div
            className={`rounded-2xl border px-4 py-3 shadow-lg ${
              banner.type === 'success'
                ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                : 'border-rose-200 bg-rose-50 text-rose-700'
            }`}
          >
            <div className="text-sm font-semibold">{banner.title}</div>
            <div className="mt-1 text-sm">{banner.message}</div>
          </div>
        </div>
      ) : null}

      {profileOpen ? (
        <ModalShell title="当前账号信息" description="你可以查看当前账号信息，并维护展示名称与联系电话。" onClose={() => setProfileOpen(false)}>
          {loadingProfile ? (
            <div className="text-sm text-slate-500">正在加载账号信息...</div>
          ) : (
            <div className="space-y-4">
              <div className="rounded-3xl border border-slate-100 bg-[linear-gradient(135deg,#eff6ff_0%,#f8fafc_55%,#ffffff_100%)] p-5">
                <div className="flex items-start gap-4">
                  <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-blue-500 text-xl font-semibold text-white shadow-lg shadow-blue-200/60">
                    {initial || 'U'}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="truncate text-base font-semibold text-slate-800">{displayName}</div>
                      <StatusChip tone={(profile?.role || userRole) === 'admin' ? 'blue' : 'slate'}>{roleLabel}</StatusChip>
                    </div>
                    <div className="mt-2 text-sm text-slate-500">用户名：{profile?.username || userName}</div>
                    <div className="mt-3 flex flex-wrap gap-2">
                      <StatusChip tone="blue">最近登录：{formatLocalDateTime(profile?.last_login_at)}</StatusChip>
                      <StatusChip tone="slate">最近修改：{updatedAtLabel}</StatusChip>
                    </div>
                  </div>
                </div>
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-xs text-slate-400">用户名</div>
                  <div className="mt-1 text-sm font-medium text-slate-700">{profile?.username || userName}</div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-xs text-slate-400">账号角色</div>
                  <div className="mt-1 text-sm font-medium text-slate-700">{roleLabel}</div>
                </div>
              </div>

              <div className="grid gap-4 sm:grid-cols-3">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="flex items-center gap-1.5 text-xs text-slate-400">
                    <User size={12} />
                    创建时间
                  </div>
                  <div className="mt-1 text-sm font-medium text-slate-700">{formatLocalDateTime(profile?.created_at)}</div>
                </div>
                <div className="rounded-2xl border border-blue-100 bg-blue-50/70 p-4">
                  <div className="flex items-center gap-1.5 text-xs text-blue-500">
                    <CheckCircle2 size={12} />
                    最近登录
                  </div>
                  <div className="mt-1 text-sm font-medium text-slate-700">{formatLocalDateTime(profile?.last_login_at)}</div>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="flex items-center gap-1.5 text-xs text-slate-400">
                    <PencilLine size={12} />
                    最近修改
                  </div>
                  <div className="mt-1 text-sm font-medium text-slate-700">{updatedAtLabel}</div>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-100 bg-white p-4">
                <div className="flex items-center gap-2 text-sm font-medium text-slate-700">
                  <ShieldCheck size={16} className="text-emerald-500" />
                  账号安全概览
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <StatusChip tone={hasSecurityQuestion ? 'green' : 'amber'}>
                    {hasSecurityQuestion ? '密保已设置' : '建议设置密保'}
                  </StatusChip>
                  <StatusChip tone={profileComplete ? 'green' : 'amber'}>
                    {profileComplete ? '资料已完善' : '建议补充资料'}
                  </StatusChip>
                  <StatusChip tone={profile?.last_login_at ? 'blue' : 'amber'}>
                    {profile?.last_login_at ? '登录记录正常' : '登录记录待确认'}
                  </StatusChip>
                </div>
                <div className="mt-3 text-xs leading-6 text-slate-500">
                  建议定期更新密码，并保持展示名称、联系电话和密保信息完整，便于账号维护与安全找回。
                </div>
              </div>

              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">展示名称</label>
                <input
                  value={profileForm.displayName}
                  onChange={(event) => setProfileForm((prev) => ({ ...prev, displayName: event.target.value }))}
                  placeholder="用于页面右上角展示，可留空"
                  className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
                />
              </div>

              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">联系电话</label>
                <input
                  value={profileForm.phone}
                  onChange={(event) => setProfileForm((prev) => ({ ...prev, phone: event.target.value }))}
                  placeholder="选填，便于账号信息维护"
                  className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
                />
              </div>

              <div className="flex justify-end gap-3 pt-2">
                <button
                  type="button"
                  onClick={() => setProfileOpen(false)}
                  className="rounded-2xl border border-slate-200 px-4 py-2.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
                >
                  关闭
                </button>
                <button
                  type="button"
                  onClick={handleSaveProfile}
                  disabled={savingProfile}
                  className="rounded-2xl bg-blue-500 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-blue-600 disabled:opacity-50"
                >
                  {savingProfile ? '保存中...' : '保存修改'}
                </button>
              </div>
            </div>
          )}
        </ModalShell>
      ) : null}

      {passwordOpen ? (
        <ModalShell title="修改密码" description="请输入当前密码，并设置新的登录密码。" onClose={() => setPasswordOpen(false)}>
          <div className="space-y-4">
            <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
              修改成功后，建议你妥善保存新密码，并在下次登录时确认账号可正常使用。
            </div>

            <div>
              <label className="mb-1.5 block text-sm font-medium text-slate-700">当前密码</label>
              <input
                type="password"
                value={passwordForm.currentPassword}
                onChange={(event) => setPasswordForm((prev) => ({ ...prev, currentPassword: event.target.value }))}
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </div>
            <div>
              <label className="mb-1.5 block text-sm font-medium text-slate-700">新密码</label>
              <input
                type="password"
                value={passwordForm.newPassword}
                onChange={(event) => setPasswordForm((prev) => ({ ...prev, newPassword: event.target.value }))}
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </div>
            <div>
              <label className="mb-1.5 block text-sm font-medium text-slate-700">确认新密码</label>
              <input
                type="password"
                value={passwordForm.confirmPassword}
                onChange={(event) => setPasswordForm((prev) => ({ ...prev, confirmPassword: event.target.value }))}
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </div>

            <div className="flex justify-end gap-3 pt-2">
              <button
                type="button"
                onClick={() => setPasswordOpen(false)}
                className="rounded-2xl border border-slate-200 px-4 py-2.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
              >
                取消
              </button>
              <button
                type="button"
                onClick={handleChangePassword}
                disabled={changingPassword}
                className="rounded-2xl bg-blue-500 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-blue-600 disabled:opacity-50"
              >
                {changingPassword ? '修改中...' : '确认修改'}
              </button>
            </div>
          </div>
        </ModalShell>
      ) : null}

      {securityOpen ? (
        <ModalShell title="设置密保" description="设置密保问题与答案，便于后续忘记密码时自助找回。" onClose={() => setSecurityOpen(false)}>
          <div className="space-y-4">
            <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
              建议使用只有你自己知道、但容易记住的问题与答案，避免设置过于公开的信息。
            </div>

            <div>
              <label className="mb-1.5 block text-sm font-medium text-slate-700">密保问题</label>
              <input
                value={securityForm.question}
                onChange={(event) => setSecurityForm((prev) => ({ ...prev, question: event.target.value }))}
                placeholder="例如：你的出生城市是？"
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </div>
            <div>
              <label className="mb-1.5 block text-sm font-medium text-slate-700">密保答案</label>
              <input
                value={securityForm.answer}
                onChange={(event) => setSecurityForm((prev) => ({ ...prev, answer: event.target.value }))}
                placeholder="请输入密保答案"
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm focus:border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </div>

            <div className="flex justify-end gap-3 pt-2">
              <button
                type="button"
                onClick={() => setSecurityOpen(false)}
                className="rounded-2xl border border-slate-200 px-4 py-2.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
              >
                取消
              </button>
              <button
                type="button"
                onClick={handleSaveSecurityQuestion}
                disabled={savingSecurity}
                className="rounded-2xl bg-blue-500 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-blue-600 disabled:opacity-50"
              >
                {savingSecurity ? '保存中...' : '保存密保'}
              </button>
            </div>
          </div>
        </ModalShell>
      ) : null}
    </>
  );
};

export default Header;
