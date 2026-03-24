/**
 * LoginPage Component - Full-page login/register with forgot password
 *
 * Standalone page (NOT wrapped in Layout).
 * Two tabs: Login and Register.
 * Register includes security question for self-service password reset.
 * On successful login, calls onLogin callback with token, username, role.
 */

import React, { useState } from 'react';
import { Wallet, Eye, EyeOff } from 'lucide-react';
import { login, register, getSecurityQuestion, forgotPassword } from '../services/api';
import { ApiError } from '../services/types';

interface LoginPageProps {
  onLogin: (token: string, username: string, role: string) => void;
}

type TabType = 'login' | 'register';
type ForgotStep = 'username' | 'answer';

const SECURITY_QUESTIONS = [
  '你的出生城市是？',
  '你母亲的姓名是？',
  '你的第一所学校是？',
  '你最喜欢的食物是？',
  '你童年最好的朋友叫什么？',
];

const LoginPage: React.FC<LoginPageProps> = ({ onLogin }) => {
  const [activeTab, setActiveTab] = useState<TabType>('login');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');

  // Register: security question
  const [securityQuestion, setSecurityQuestion] = useState(SECURITY_QUESTIONS[0]);
  const [securityAnswer, setSecurityAnswer] = useState('');

  // Forgot password state
  const [showForgot, setShowForgot] = useState(false);
  const [forgotStep, setForgotStep] = useState<ForgotStep>('username');
  const [forgotUsername, setForgotUsername] = useState('');
  const [forgotQuestion, setForgotQuestion] = useState('');
  const [forgotAnswer, setForgotAnswer] = useState('');
  const [forgotNewPassword, setForgotNewPassword] = useState('');
  const [forgotConfirmPassword, setForgotConfirmPassword] = useState('');
  const [forgotError, setForgotError] = useState('');

  const resetForm = (): void => {
    setUsername('');
    setPassword('');
    setConfirmPassword('');
    setError('');
    setSuccessMessage('');
    setShowPassword(false);
    setShowConfirmPassword(false);
    setSecurityQuestion(SECURITY_QUESTIONS[0]);
    setSecurityAnswer('');
  };

  const resetForgot = (): void => {
    setShowForgot(false);
    setForgotStep('username');
    setForgotUsername('');
    setForgotQuestion('');
    setForgotAnswer('');
    setForgotNewPassword('');
    setForgotConfirmPassword('');
    setForgotError('');
  };

  const handleTabSwitch = (tab: TabType): void => {
    setActiveTab(tab);
    resetForm();
  };

  const handleLogin = async (e: React.FormEvent): Promise<void> => {
    e.preventDefault();
    setError('');
    setSuccessMessage('');

    if (!username.trim()) { setError('请输入用户名'); return; }
    if (!password) { setError('请输入密码'); return; }

    setLoading(true);
    try {
      const response = await login(username.trim(), password);
      onLogin(response.token, response.username, response.role);
    } catch (err) {
      if (err instanceof ApiError) { setError(err.message); }
      else if (err instanceof Error) { setError(err.message); }
      else { setError('登录失败，请稍后重试'); }
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async (e: React.FormEvent): Promise<void> => {
    e.preventDefault();
    setError('');
    setSuccessMessage('');

    if (!username.trim()) { setError('请输入用户名'); return; }
    if (!password) { setError('请输入密码'); return; }
    if (password.length < 6) { setError('密码长度至少 6 位'); return; }
    if (password !== confirmPassword) { setError('两次输入的密码不一致'); return; }
    if (!securityAnswer.trim()) { setError('请填写密保答案'); return; }

    setLoading(true);
    try {
      await register(username.trim(), password, securityQuestion, securityAnswer.trim());
      setSuccessMessage('注册成功！请登录');
      setActiveTab('login');
      setPassword('');
      setConfirmPassword('');
      setSecurityAnswer('');
    } catch (err) {
      if (err instanceof ApiError) { setError(err.message); }
      else if (err instanceof Error) { setError(err.message); }
      else { setError('注册失败，请稍后重试'); }
    } finally {
      setLoading(false);
    }
  };

  const handleForgotLookup = async (): Promise<void> => {
    setForgotError('');
    if (!forgotUsername.trim()) { setForgotError('请输入用户名'); return; }
    setLoading(true);
    try {
      const res = await getSecurityQuestion(forgotUsername.trim());
      if (!res.has_question) {
        setForgotError('该用户未设置密保问题，请联系管理员重置密码');
      } else {
        setForgotQuestion(res.question);
        setForgotStep('answer');
      }
    } catch (err) {
      if (err instanceof ApiError) { setForgotError(err.message); }
      else { setForgotError('查询失败，请稍后重试'); }
    } finally {
      setLoading(false);
    }
  };

  const handleForgotReset = async (): Promise<void> => {
    setForgotError('');
    if (!forgotAnswer.trim()) { setForgotError('请输入密保答案'); return; }
    if (!forgotNewPassword || forgotNewPassword.length < 6) { setForgotError('新密码长度至少 6 位'); return; }
    if (forgotNewPassword !== forgotConfirmPassword) { setForgotError('两次输入的密码不一致'); return; }
    setLoading(true);
    try {
      await forgotPassword(forgotUsername.trim(), forgotAnswer.trim(), forgotNewPassword);
      setSuccessMessage('密码重置成功！请用新密码登录');
      resetForgot();
    } catch (err) {
      if (err instanceof ApiError) { setForgotError(err.message); }
      else { setForgotError('重置失败，请稍后重试'); }
    } finally {
      setLoading(false);
    }
  };

  const inputClass = "w-full px-4 py-2.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-shadow";

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-slate-100 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo and Title */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 bg-blue-500 rounded-2xl mb-4 shadow-lg">
            <Wallet size={32} className="text-white" />
          </div>
          <h1 className="text-2xl font-bold text-gray-800">贷款助手</h1>
          <p className="text-gray-500 mt-1 text-sm">智能贷款申请管理系统</p>
        </div>

        {/* Card */}
        <div className="bg-white rounded-2xl shadow-xl border border-gray-100 overflow-hidden">
          {/* Tabs */}
          <div className="flex border-b border-gray-100">
            <button
              onClick={() => handleTabSwitch('login')}
              className={`flex-1 py-4 text-sm font-medium transition-colors ${
                activeTab === 'login'
                  ? 'text-blue-600 border-b-2 border-blue-500 bg-blue-50/50'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              登录
            </button>
            <button
              onClick={() => handleTabSwitch('register')}
              className={`flex-1 py-4 text-sm font-medium transition-colors ${
                activeTab === 'register'
                  ? 'text-blue-600 border-b-2 border-blue-500 bg-blue-50/50'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              注册
            </button>
          </div>

          {/* Form */}
          <div className="p-6">
            {successMessage && (
              <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-lg text-green-700 text-sm">
                {successMessage}
              </div>
            )}
            {error && (
              <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                {error}
              </div>
            )}

            {activeTab === 'login' ? (
              <form onSubmit={handleLogin} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">用户名</label>
                  <input type="text" value={username} onChange={(e) => setUsername(e.target.value)}
                    placeholder="请输入用户名" className={inputClass} disabled={loading} autoComplete="username" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">密码</label>
                  <div className="relative">
                    <input type={showPassword ? 'text' : 'password'} value={password}
                      onChange={(e) => setPassword(e.target.value)} placeholder="请输入密码"
                      className={`${inputClass} pr-10`} disabled={loading} autoComplete="current-password" />
                    <button type="button" onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600" tabIndex={-1}>
                      {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                    </button>
                  </div>
                </div>
                <button type="submit" disabled={loading}
                  className="w-full py-2.5 bg-blue-500 text-white rounded-lg font-medium text-sm hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                  {loading ? '登录中...' : '登录'}
                </button>
                <div className="text-center">
                  <button type="button"
                    onClick={() => { setShowForgot(true); setError(''); setSuccessMessage(''); }}
                    className="text-sm text-blue-500 hover:text-blue-600 transition-colors">
                    忘记密码？
                  </button>
                </div>
              </form>
            ) : (
              <form onSubmit={handleRegister} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">用户名</label>
                  <input type="text" value={username} onChange={(e) => setUsername(e.target.value)}
                    placeholder="请输入用户名" className={inputClass} disabled={loading} autoComplete="username" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">密码</label>
                  <div className="relative">
                    <input type={showPassword ? 'text' : 'password'} value={password}
                      onChange={(e) => setPassword(e.target.value)} placeholder="请输入密码（至少 6 位）"
                      className={`${inputClass} pr-10`} disabled={loading} autoComplete="new-password" />
                    <button type="button" onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600" tabIndex={-1}>
                      {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                    </button>
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">确认密码</label>
                  <div className="relative">
                    <input type={showConfirmPassword ? 'text' : 'password'} value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)} placeholder="请再次输入密码"
                      className={`${inputClass} pr-10`} disabled={loading} autoComplete="new-password" />
                    <button type="button" onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600" tabIndex={-1}>
                      {showConfirmPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                    </button>
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">密保问题</label>
                  <select value={securityQuestion} onChange={(e) => setSecurityQuestion(e.target.value)}
                    className={`${inputClass} bg-white`} disabled={loading}>
                    {SECURITY_QUESTIONS.map((q) => (
                      <option key={q} value={q}>{q}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">密保答案</label>
                  <input type="text" value={securityAnswer} onChange={(e) => setSecurityAnswer(e.target.value)}
                    placeholder="请输入密保答案（用于找回密码）" className={inputClass} disabled={loading} />
                </div>
                <button type="submit" disabled={loading}
                  className="w-full py-2.5 bg-blue-500 text-white rounded-lg font-medium text-sm hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                  {loading ? '注册中...' : '注册'}
                </button>
              </form>
            )}
          </div>
        </div>

        <p className="text-center text-xs text-gray-400 mt-6">
          © 2024 贷款助手 · 智能贷款申请管理系统
        </p>
      </div>

      {/* Forgot Password Modal */}
      {showForgot && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden">
            <div className="flex items-center justify-between p-5 border-b border-gray-100">
              <h2 className="text-lg font-semibold text-gray-800">找回密码</h2>
              <button onClick={resetForgot} className="text-gray-400 hover:text-gray-600 text-xl leading-none">&times;</button>
            </div>
            <div className="p-6">
              {forgotError && (
                <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                  {forgotError}
                </div>
              )}

              {forgotStep === 'username' ? (
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">用户名</label>
                    <input type="text" value={forgotUsername} onChange={(e) => setForgotUsername(e.target.value)}
                      placeholder="请输入注册时的用户名" className={inputClass} disabled={loading} />
                  </div>
                  <button onClick={handleForgotLookup} disabled={loading}
                    className="w-full py-2.5 bg-blue-500 text-white rounded-lg font-medium text-sm hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                    {loading ? '查询中...' : '下一步'}
                  </button>
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg text-blue-700 text-sm">
                    密保问题：{forgotQuestion}
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">密保答案</label>
                    <input type="text" value={forgotAnswer} onChange={(e) => setForgotAnswer(e.target.value)}
                      placeholder="请输入密保答案" className={inputClass} disabled={loading} />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">新密码</label>
                    <input type="password" value={forgotNewPassword} onChange={(e) => setForgotNewPassword(e.target.value)}
                      placeholder="请输入新密码（至少 6 位）" className={inputClass} disabled={loading} />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1.5">确认新密码</label>
                    <input type="password" value={forgotConfirmPassword} onChange={(e) => setForgotConfirmPassword(e.target.value)}
                      placeholder="请再次输入新密码" className={inputClass} disabled={loading} />
                  </div>
                  <div className="flex gap-3">
                    <button onClick={() => { setForgotStep('username'); setForgotError(''); }} disabled={loading}
                      className="flex-1 py-2.5 border border-gray-200 text-gray-700 rounded-lg font-medium text-sm hover:bg-gray-50 disabled:opacity-50 transition-colors">
                      上一步
                    </button>
                    <button onClick={handleForgotReset} disabled={loading}
                      className="flex-1 py-2.5 bg-blue-500 text-white rounded-lg font-medium text-sm hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                      {loading ? '重置中...' : '重置密码'}
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default LoginPage;
