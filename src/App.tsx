import React, { Component, ErrorInfo, ReactNode, useCallback, useEffect, useState } from 'react';
import { AppProvider } from './context/AppContext';
import { Layout, PageType } from './components/layout';
import DashboardPage from './components/DashboardPage';
import UploadPage from './components/UploadPage';
import ApplicationPage from './components/ApplicationPage';
import SchemeMatchPage from './components/SchemeMatch';
import ChatPage from './components/ChatPage';
import LoginPage from './components/LoginPage';
import CustomerListPage from './components/CustomerListPage';
import CustomerDataPage from './components/CustomerDataPage';
import AdminUsersPage from './components/AdminUsersPage';
import { getCurrentUser } from './services/api';

interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error('React Error Boundary caught an error:', error, errorInfo);
  }

  handleRetry = (): void => {
    this.setState({ hasError: false, error: null });
  };

  render(): ReactNode {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <div className="min-h-screen bg-slate-50 flex items-center justify-center p-6">
          <div className="bg-white rounded-xl border border-red-200 p-8 max-w-md text-center shadow-lg">
            <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg className="w-8 h-8 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
            <h2 className="text-xl font-bold text-gray-800 mb-2">出现错误</h2>
            <p className="text-gray-600 mb-4">{this.state.error?.message || '应用程序遇到了一个错误。'}</p>
            <button
              onClick={this.handleRetry}
              className="px-6 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
            >
              重试
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

const App: React.FC = () => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [username, setUsername] = useState('');
  const [role, setRole] = useState('');
  const [authChecking, setAuthChecking] = useState(true);
  const [currentPage, setCurrentPage] = useState<PageType>('dashboard');

  useEffect(() => {
    const checkAuth = async (): Promise<void> => {
      const token = localStorage.getItem('auth_token');
      if (!token) {
        setAuthChecking(false);
        return;
      }

      try {
        const userInfo = await getCurrentUser();
        setIsLoggedIn(true);
        setUsername(userInfo.username);
        setRole(userInfo.role);
        localStorage.setItem('auth_username', userInfo.username);
        localStorage.setItem('auth_role', userInfo.role);
      } catch {
        localStorage.removeItem('auth_token');
        localStorage.removeItem('auth_username');
        localStorage.removeItem('auth_role');
        setIsLoggedIn(false);
      } finally {
        setAuthChecking(false);
      }
    };

    checkAuth();
  }, []);

  const handleLogin = useCallback((token: string, loginUsername: string, loginRole: string): void => {
    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_username', loginUsername);
    localStorage.setItem('auth_role', loginRole);
    setIsLoggedIn(true);
    setUsername(loginUsername);
    setRole(loginRole);
    setCurrentPage('dashboard');
  }, []);

  const handleLogout = useCallback((): void => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_username');
    localStorage.removeItem('auth_role');
    setIsLoggedIn(false);
    setUsername('');
    setRole('');
    setCurrentPage('dashboard');
  }, []);

  const handleNavigate = useCallback(
    (page: string): void => {
      const normalizedPage = page === 'matching' ? 'scheme' : page;
      const validPages: PageType[] = ['dashboard', 'customers', 'upload', 'application', 'scheme', 'chat', 'data', 'admin'];
      if (!validPages.includes(normalizedPage as PageType)) {
        return;
      }
      if (normalizedPage === 'admin' && role !== 'admin') {
        setCurrentPage('dashboard');
        return;
      }
      setCurrentPage(normalizedPage as PageType);
    },
    [role]
  );

  const renderPage = (): ReactNode => {
    switch (currentPage) {
      case 'dashboard':
        return <DashboardPage onNavigate={setCurrentPage} />;
      case 'customers':
        return <CustomerListPage userRole={role} username={username} />;
      case 'data':
        return <CustomerDataPage />;
      case 'upload':
        return <UploadPage />;
      case 'application':
        return <ApplicationPage />;
      case 'scheme':
        return <SchemeMatchPage />;
      case 'chat':
        return <ChatPage onNavigate={handleNavigate} />;
      case 'admin':
        return role === 'admin' ? <AdminUsersPage currentUsername={username} /> : <DashboardPage onNavigate={setCurrentPage} />;
      default:
        return <DashboardPage onNavigate={setCurrentPage} />;
    }
  };

  if (authChecking) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-slate-100 flex items-center justify-center">
        <div className="text-center">
          <div className="w-10 h-10 border-3 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
          <p className="text-sm text-gray-500">加载中...</p>
        </div>
      </div>
    );
  }

  if (!isLoggedIn) {
    return (
      <ErrorBoundary>
        <LoginPage onLogin={handleLogin} />
      </ErrorBoundary>
    );
  }

  return (
    <ErrorBoundary>
      <AppProvider>
        <Layout
          currentPage={currentPage}
          onNavigate={(page) => handleNavigate(page)}
          userName={username}
          userRole={role}
          onLogout={handleLogout}
        >
          {renderPage()}
        </Layout>
      </AppProvider>
    </ErrorBoundary>
  );
};

export default App;
