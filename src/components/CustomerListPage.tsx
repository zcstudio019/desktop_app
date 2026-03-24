/**
 * CustomerListPage Component - Customer list with card-based layout
 *
 * Features:
 * - Initial "加载客户" button instead of auto-loading
 * - Search bar with debounced search (after initial load)
 * - Responsive card grid (1/2/3 columns)
 * - Admin view: uploader filter dropdown
 * - Click card to open detail modal with all customer fields
 * - Loading, empty, and error states
 */

import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { Search, Users, Clock, User, Filter, Download, RefreshCw, X } from 'lucide-react';
import { listCustomers, getCustomerDetail } from '../services/api';
import { ApiError } from '../services/types';
import type { CustomerListItem, CustomerDetail } from '../services/types';
import { DataSectionCard, ArrayDataCard, isArrayOfObjects } from './DataDisplayComponents';

interface CustomerListPageProps {
  userRole: string;
  username: string;
}

function formatTime(timeStr: string): string {
  if (!timeStr) return '未知';
  if (/^\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}$/.test(timeStr)) return timeStr;
  if (/^\d{4}\.\d{2}\.\d{2}$/.test(timeStr)) return timeStr.replace(/\./g, '/');

  try {
    const date = new Date(timeStr);
    if (Number.isNaN(date.getTime())) return timeStr;
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    const h = String(date.getHours()).padStart(2, '0');
    const min = String(date.getMinutes()).padStart(2, '0');
    return h === '00' && min === '00' ? `${y}/${m}/${d}` : `${y}/${m}/${d} ${h}:${min}`;
  } catch {
    return timeStr;
  }
}

const CustomerListPage: React.FC<CustomerListPageProps> = ({ userRole, username }) => {
  const [hasLoaded, setHasLoaded] = useState(() => {
    const saved = localStorage.getItem('customerList_hasLoaded');
    return saved === 'true';
  });
  const [customers, setCustomers] = useState<CustomerListItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedQuery, setDebouncedQuery] = useState('');
  const [uploaderFilter, setUploaderFilter] = useState('');

  const [selectedDetail, setSelectedDetail] = useState<CustomerDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState('');
  const [showModal, setShowModal] = useState(false);

  const isAdmin = userRole === 'admin';
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!hasLoaded) return;
    const timer = setTimeout(() => {
      setDebouncedQuery(searchQuery);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery, hasLoaded]);

  const fetchCustomers = useCallback(async (search: string): Promise<void> => {
    setLoading(true);
    setError('');
    try {
      const data = await listCustomers(search || undefined);
      setCustomers(data);
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else if (err instanceof Error) {
        if (err.name === 'AbortError') return;
        setError(err.message);
      } else {
        setError('获取客户列表失败');
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!hasLoaded) return;
    fetchCustomers(debouncedQuery);
  }, [debouncedQuery, fetchCustomers, hasLoaded]);

  const handleInitialLoad = useCallback(() => {
    setHasLoaded(true);
    localStorage.setItem('customerList_hasLoaded', 'true');
    fetchCustomers('');
  }, [fetchCustomers]);

  const handleRefresh = useCallback(() => {
    fetchCustomers(debouncedQuery);
  }, [fetchCustomers, debouncedQuery]);

  const handleCardClick = useCallback(async (recordId: string) => {
    setShowModal(true);
    setDetailLoading(true);
    setDetailError('');
    setSelectedDetail(null);
    try {
      const detail = await getCustomerDetail(recordId);
      setSelectedDetail(detail);
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        setDetailError(err.message);
      } else if (err instanceof Error) {
        setDetailError(err.message);
      } else {
        setDetailError('获取客户详情失败');
      }
    } finally {
      setDetailLoading(false);
    }
  }, []);

  const closeModal = useCallback(() => {
    setShowModal(false);
    setSelectedDetail(null);
    setDetailError('');
  }, []);

  const handleBackdropClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (modalRef.current && !modalRef.current.contains(e.target as Node)) {
        closeModal();
      }
    },
    [closeModal]
  );

  useEffect(() => {
    if (!showModal) return;
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeModal();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [showModal, closeModal]);

  const uniqueUploaders = useMemo(() => {
    const uploaders = new Set<string>();
    customers.forEach((customer) => {
      if (customer.uploader) uploaders.add(customer.uploader);
    });
    return Array.from(uploaders).sort();
  }, [customers]);

  const filteredCustomers = useMemo(() => {
    if (!isAdmin || !uploaderFilter) return customers;
    return customers.filter((customer) => customer.uploader === uploaderFilter);
  }, [customers, uploaderFilter, isAdmin]);

  return (
    <div className="p-6" data-testid="customer-list-page">
      <div className="mb-6">
        <h2 className="text-lg font-semibold text-gray-800">客户列表</h2>
        <p className="text-sm text-gray-500 mt-1">
          {isAdmin ? '管理所有客户资料' : `${username} 的客户资料`}
        </p>
      </div>

      {!hasLoaded && (
        <div className="flex flex-col items-center justify-center py-20" data-testid="initial-state">
          <div className="w-20 h-20 bg-blue-50 rounded-full flex items-center justify-center mb-6">
            <Users size={36} className="text-blue-500" />
          </div>
          <h3 className="text-lg font-medium text-gray-700 mb-2">客户资料</h3>
          <p className="text-sm text-gray-400 mb-6">点击下方按钮加载本地客户列表</p>
          <button
            onClick={handleInitialLoad}
            className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 active:bg-blue-800 transition-colors text-base font-medium shadow-sm"
          >
            <Download size={20} />
            加载客户
          </button>
        </div>
      )}

      {hasLoaded && (
        <>
          <div className="flex flex-col sm:flex-row gap-3 mb-6">
            <div className="relative flex-1">
              <Search size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="搜索客户名称..."
                className="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white transition-shadow"
              />
            </div>

            {isAdmin && uniqueUploaders.length > 0 && (
              <div className="relative">
                <Filter size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                <select
                  value={uploaderFilter}
                  onChange={(e) => setUploaderFilter(e.target.value)}
                  className="pl-9 pr-8 py-2.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white appearance-none cursor-pointer min-w-[160px]"
                >
                  <option value="">全部上传者</option>
                  {uniqueUploaders.map((uploader) => (
                    <option key={uploader} value={uploader}>
                      {uploader}
                    </option>
                  ))}
                </select>
              </div>
            )}

            <button
              onClick={handleRefresh}
              disabled={loading}
              className="inline-flex items-center gap-1.5 px-4 py-2.5 border border-gray-200 rounded-lg text-sm text-gray-600 hover:bg-gray-50 hover:border-gray-300 active:bg-gray-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
              刷新
            </button>
          </div>

          {error && (
            <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
              {error}
              <button onClick={handleRefresh} className="ml-3 text-red-600 underline hover:text-red-800">
                重试
              </button>
            </div>
          )}

          {loading && (
            <div className="flex items-center justify-center py-20" data-testid="customer-list-loading">
              <div className="text-center">
                <div className="w-10 h-10 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
                <p className="text-sm text-gray-500">加载中...</p>
              </div>
            </div>
          )}

          {!loading && !error && filteredCustomers.length === 0 && (
            <div className="flex flex-col items-center justify-center py-20" data-testid="customer-list-empty">
              <div className="w-16 h-16 bg-gray-100 rounded-full flex items-center justify-center mb-4">
                <Users size={28} className="text-gray-400" />
              </div>
              <h3 className="text-base font-medium text-gray-600 mb-1">
                {searchQuery ? '未找到匹配的客户' : '暂无客户数据'}
              </h3>
              <p className="text-sm text-gray-400">
                {searchQuery ? '请尝试其他搜索关键词' : '上传客户资料后将在此显示'}
              </p>
            </div>
          )}

          {!loading && filteredCustomers.length > 0 && (
            <>
              <div className="text-sm text-gray-500 mb-4">
                共 {filteredCustomers.length} 位客户
                {uploaderFilter && ` · 筛选：${uploaderFilter}`}
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4" data-testid="customer-card-grid">
                {filteredCustomers.map((customer) => (
                  <div
                    key={customer.record_id}
                    onClick={() => handleCardClick(customer.record_id)}
                    className="bg-white rounded-xl border border-gray-100 p-5 hover:shadow-md hover:border-blue-200 transition-all cursor-pointer group"
                  >
                    <div className="flex items-start gap-3 mb-3">
                      <div
                        className={`w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0 transition-colors ${
                          customer.customer_type === 'personal'
                            ? 'bg-purple-50 group-hover:bg-purple-100'
                            : 'bg-blue-50 group-hover:bg-blue-100'
                        }`}
                      >
                        <User
                          size={20}
                          className={customer.customer_type === 'personal' ? 'text-purple-500' : 'text-blue-500'}
                        />
                      </div>
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2 flex-wrap">
                          <h3 className="text-base font-semibold text-gray-800 truncate">
                            {customer.name || '未命名客户'}
                          </h3>
                          <span
                            className={`px-1.5 py-0.5 text-xs rounded font-medium shrink-0 ${
                              customer.customer_type === 'personal'
                                ? 'bg-purple-100 text-purple-600'
                                : 'bg-blue-100 text-blue-600'
                            }`}
                          >
                            {customer.customer_type === 'personal' ? '个人' : '企业'}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="space-y-2 text-sm">
                      {isAdmin && customer.uploader && (
                        <div className="flex items-center gap-2 text-gray-500">
                          <Users size={14} className="flex-shrink-0" />
                          <span className="truncate">上传者：{customer.uploader}</span>
                        </div>
                      )}
                      {customer.upload_time && (
                        <div className="flex items-center gap-2 text-gray-400">
                          <Clock size={14} className="flex-shrink-0" />
                          <span>{formatTime(customer.upload_time)}</span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </>
      )}

      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={handleBackdropClick}>
          <div
            ref={modalRef}
            className="bg-white rounded-2xl shadow-xl w-full max-w-2xl mx-4 max-h-[85vh] flex flex-col"
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
              <h3 className="text-lg font-semibold text-gray-800 truncate">
                {detailLoading ? '加载中...' : selectedDetail ? selectedDetail.name || '客户详情' : '客户详情'}
              </h3>
              <button
                onClick={closeModal}
                className="p-1.5 rounded-lg text-gray-400 hover:text-gray-600 hover:bg-gray-100 transition-colors"
              >
                <X size={20} />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto px-6 py-4">
              {detailLoading && (
                <div className="flex items-center justify-center py-12">
                  <div className="text-center">
                    <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
                    <p className="text-sm text-gray-500">加载客户详情...</p>
                  </div>
                </div>
              )}

              {!detailLoading && detailError && (
                <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">{detailError}</div>
              )}

              {!detailLoading && !detailError && selectedDetail && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-xs text-gray-400 mb-1">记录 ID</div>
                      <div className="text-sm text-gray-700 font-mono break-all">{selectedDetail.record_id}</div>
                    </div>
                    {selectedDetail.uploader && (
                      <div className="bg-gray-50 rounded-lg p-3">
                        <div className="text-xs text-gray-400 mb-1">上传者</div>
                        <div className="text-sm text-gray-700">{selectedDetail.uploader}</div>
                      </div>
                    )}
                    {selectedDetail.upload_time && (
                      <div className="bg-gray-50 rounded-lg p-3">
                        <div className="text-xs text-gray-400 mb-1">上传时间</div>
                        <div className="text-sm text-gray-700">{formatTime(selectedDetail.upload_time)}</div>
                      </div>
                    )}
                  </div>

                  {Object.keys(selectedDetail.fields).length > 0 ? (
                    <div className="space-y-4">
                      {Object.entries(selectedDetail.fields).map(([sectionName, sectionValue]) => {
                        let parsedValue: unknown = sectionValue;
                        if (typeof sectionValue === 'string' && (sectionValue.startsWith('{') || sectionValue.startsWith('['))) {
                          try {
                            parsedValue = JSON.parse(sectionValue);
                          } catch {
                            parsedValue = sectionValue;
                          }
                        }

                        if (typeof parsedValue === 'object' && parsedValue !== null && !Array.isArray(parsedValue)) {
                          return (
                            <DataSectionCard
                              key={sectionName}
                              title={sectionName}
                              data={parsedValue as Record<string, unknown>}
                            />
                          );
                        }

                        if (isArrayOfObjects(parsedValue)) {
                          return (
                            <ArrayDataCard
                              key={sectionName}
                              title={sectionName}
                              data={parsedValue as Array<Record<string, unknown>>}
                            />
                          );
                        }

                        return (
                          <div key={sectionName} className="border border-gray-100 rounded-lg p-3">
                            <div className="text-xs font-medium text-gray-400 mb-1">{sectionName}</div>
                            <div className="text-sm text-gray-700 whitespace-pre-wrap break-words">
                              {typeof parsedValue === 'string' ? (
                                parsedValue || <span className="text-gray-300 italic">（空）</span>
                              ) : (
                                String(parsedValue)
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="text-center py-8 text-sm text-gray-400">该客户暂无详细字段数据</div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default CustomerListPage;
