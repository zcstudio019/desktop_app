import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { KeyRound, RefreshCcw, Shield, Trash2, UserCog, X } from 'lucide-react';
import { ApiError, type UserInfo } from '../services/types';
import { deleteUser, listUsers, resetPassword } from '../services/api';

interface AdminUsersPageProps {
  currentUsername?: string;
}

function formatCreatedAt(value?: string): string {
  if (!value) {
    return '未记录';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString('zh-CN', { hour12: false });
}

const AdminUsersPage: React.FC<AdminUsersPageProps> = ({ currentUsername }) => {
  const [users, setUsers] = useState<UserInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [resetTarget, setResetTarget] = useState<UserInfo | null>(null);
  const [newPassword, setNewPassword] = useState('');
  const [resetError, setResetError] = useState('');

  const adminCount = useMemo(() => users.filter((user) => user.role === 'admin').length, [users]);

  const loadUsers = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const result = await listUsers();
      setUsers(result);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('加载账号列表失败，请稍后重试。');
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  const handleDelete = async (user: UserInfo): Promise<void> => {
    if (!window.confirm(`确认删除账号“${user.username}”吗？`)) {
      return;
    }

    setActionLoading(`delete:${user.username}`);
    setError('');
    setNotice('');
    try {
      const result = await deleteUser(user.username);
      setUsers((current) => current.filter((item) => item.username !== user.username));
      setNotice(result.message);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('删除账号失败，请稍后重试。');
      }
    } finally {
      setActionLoading(null);
    }
  };

  const handleResetPassword = async (): Promise<void> => {
    if (!resetTarget) {
      return;
    }
    if (!newPassword.trim()) {
      setResetError('请输入新的临时密码。');
      return;
    }

    setActionLoading(`reset:${resetTarget.username}`);
    setResetError('');
    setError('');
    setNotice('');
    try {
      const result = await resetPassword(resetTarget.username, newPassword.trim());
      setNotice(`已为 ${resetTarget.username} ${result.message}`);
      setResetTarget(null);
      setNewPassword('');
    } catch (err) {
      if (err instanceof ApiError) {
        setResetError(err.message);
      } else if (err instanceof Error) {
        setResetError(err.message);
      } else {
        setResetError('重置密码失败，请稍后重试。');
      }
    } finally {
      setActionLoading(null);
    }
  };

  return (
    <div className="p-6 md:p-8 space-y-6" data-testid="admin-users-page">
      <section className="bg-white border border-slate-200 rounded-2xl shadow-sm p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-50 text-blue-700 text-sm font-medium mb-3">
              <Shield className="w-4 h-4" />
              管理员功能
            </div>
            <h2 className="text-2xl font-bold text-slate-900">账号管理</h2>
            <p className="text-sm text-slate-500 mt-2">查看已注册账号，删除停用账号，或远程为同事重置登录密码。</p>
          </div>
          <button
            type="button"
            onClick={() => void loadUsers()}
            disabled={loading}
            className="inline-flex items-center justify-center gap-2 px-4 py-2 border border-slate-200 rounded-xl text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-60"
          >
            <RefreshCcw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            刷新账号列表
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm text-slate-500">账号总数</div>
            <div className="mt-2 text-2xl font-semibold text-slate-900" data-testid="user-count">{users.length}</div>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm text-slate-500">管理员</div>
            <div className="mt-2 text-2xl font-semibold text-slate-900">{adminCount}</div>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm text-slate-500">当前账号</div>
            <div className="mt-2 text-lg font-semibold text-slate-900 truncate">{currentUsername || '未识别'}</div>
          </div>
        </div>
      </section>

      {notice && (
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700" data-testid="admin-notice">
          {notice}
        </div>
      )}
      {error && (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700" data-testid="admin-error">
          {error}
        </div>
      )}

      <section className="bg-white border border-slate-200 rounded-2xl shadow-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-200">
          <h3 className="text-lg font-semibold text-slate-900">已注册账号</h3>
        </div>

        {loading ? (
          <div className="px-6 py-12 text-center text-slate-500">正在加载账号列表...</div>
        ) : users.length === 0 ? (
          <div className="px-6 py-12 text-center text-slate-500">当前还没有注册账号。</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-slate-500">
                <tr>
                  <th className="px-6 py-3 text-left font-medium">用户名</th>
                  <th className="px-6 py-3 text-left font-medium">角色</th>
                  <th className="px-6 py-3 text-left font-medium">创建时间</th>
                  <th className="px-6 py-3 text-left font-medium">操作</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {users.map((user) => {
                  const isCurrentUser = user.username === currentUsername;
                  const isOnlyAdmin = user.role === 'admin' && adminCount <= 1;
                  return (
                    <tr key={user.username} data-testid={`user-row-${user.username}`}>
                      <td className="px-6 py-4">
                        <div className="font-medium text-slate-900">{user.username}</div>
                        {isCurrentUser && <div className="text-xs text-slate-500 mt-1">当前登录账号</div>}
                      </td>
                      <td className="px-6 py-4">
                        <span
                          className={`inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium ${
                            user.role === 'admin' ? 'bg-blue-50 text-blue-700' : 'bg-slate-100 text-slate-700'
                          }`}
                        >
                          {user.role === 'admin' ? '管理员' : '普通员工'}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-slate-600">{formatCreatedAt(user.created_at)}</td>
                      <td className="px-6 py-4">
                        <div className="flex flex-wrap gap-2">
                          <button
                            type="button"
                            onClick={() => {
                              setResetTarget(user);
                              setNewPassword('');
                              setResetError('');
                            }}
                            className="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-slate-700 hover:bg-slate-50"
                          >
                            <KeyRound className="w-4 h-4" />
                            重置密码
                          </button>
                          <button
                            type="button"
                            onClick={() => void handleDelete(user)}
                            disabled={isCurrentUser || isOnlyAdmin || actionLoading === `delete:${user.username}`}
                            className="inline-flex items-center gap-2 rounded-lg border border-red-200 px-3 py-2 text-red-600 hover:bg-red-50 disabled:opacity-50 disabled:cursor-not-allowed"
                          >
                            <Trash2 className="w-4 h-4" />
                            删除账号
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {resetTarget && (
        <div className="fixed inset-0 z-50 bg-slate-900/40 flex items-center justify-center p-4">
          <div className="w-full max-w-md rounded-2xl bg-white shadow-2xl border border-slate-200">
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-200">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center">
                  <UserCog className="w-5 h-5 text-blue-600" />
                </div>
                <div>
                  <h3 className="font-semibold text-slate-900">远程重置密码</h3>
                  <p className="text-sm text-slate-500">目标账号：{resetTarget.username}</p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => {
                  setResetTarget(null);
                  setNewPassword('');
                  setResetError('');
                }}
                className="text-slate-400 hover:text-slate-600"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="px-6 py-5 space-y-4">
              <div>
                <label htmlFor="new-password" className="block text-sm font-medium text-slate-700 mb-2">
                  新的临时密码
                </label>
                <input
                  id="new-password"
                  type="text"
                  value={newPassword}
                  onChange={(event) => setNewPassword(event.target.value)}
                  placeholder="请输入新的临时密码"
                  className="w-full rounded-xl border border-slate-200 px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
              {resetError && <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">{resetError}</div>}
              <div className="flex justify-end gap-3">
                <button
                  type="button"
                  onClick={() => {
                    setResetTarget(null);
                    setNewPassword('');
                    setResetError('');
                  }}
                  className="rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
                >
                  取消
                </button>
                <button
                  type="button"
                  onClick={() => void handleResetPassword()}
                  disabled={actionLoading === `reset:${resetTarget.username}`}
                  className="rounded-xl bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-60"
                >
                  {actionLoading === `reset:${resetTarget.username}` ? '重置中...' : '确认重置'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AdminUsersPage;
