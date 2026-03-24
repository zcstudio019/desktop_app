/* eslint-disable react-refresh/only-export-components -- Exports types, constants, and utility functions alongside components */
/**
 * SchemeMatch Component
 * 
 * Matches customer data against loan schemes using the backend API.
 * Supports three data sources:
 * 1. Extracted customer data (from file processing)
 * 2. Saved applications (from local cache)
 * 3. Manual input (natural language or JSON)
 * 
 * Feature: frontend-backend-integration
 * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6
 */

import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { 
  matchScheme, 
  listSavedApplications, 
  getApplication, 
  parseNaturalLanguage,
  searchCustomer,
  type SavedApplicationListItem 
} from '../services/api';
import type { SchemeMatchRequest } from '../services/types';
import { useLoading } from '../hooks/useLoading';
import { useAbortController } from '../hooks/useAbortController';
import { useApp } from '../context/AppContext';

// ============================================
// Types
// ============================================

/** Valid loan types for scheme matching - matches SchemeMatchRequest.creditType */
export type CreditType = 'personal' | 'enterprise_credit' | 'enterprise_mortgage';

/** Data source types for scheme matching */
export type DataSource = 'extracted' | 'savedApplication' | 'manual';

/** Loan type options for the selector */
export const CREDIT_TYPE_OPTIONS: Array<{ value: CreditType; label: string }> = [
  { value: 'personal', label: '个人贷款' },
  { value: 'enterprise_credit', label: '企业信用贷' },
  { value: 'enterprise_mortgage', label: '企业抵押贷' },
];

/**
 * Validates that a loan type is one of the allowed values
 * Property 8: Loan Type Validation
 */
export function isValidCreditType(value: string): value is CreditType {
  return value === 'personal' || value === 'enterprise_credit' || value === 'enterprise_mortgage';
}

// ============================================
// Sub-components
// ============================================

export interface SchemeCardProps {
  bank: string;
  product: string;
  matchScore: number;
  reason: string;
  rateMin: number;
  rateMax: number;
  limit: number;
  termMin: number;
  termMax: number;
  rank: number;
  isTopRank?: boolean;
}

export const SchemeCard = ({ bank, product, matchScore, reason, rateMin, rateMax, limit, termMin, termMax, rank, isTopRank = false }: SchemeCardProps) => {
  const borderClass = isTopRank ? 'border-blue-500 border-2' : 'border-gray-200 border';
  const badgeClass = isTopRank ? 'bg-blue-100 text-blue-500' : 'bg-gray-100 text-gray-500';
  const matchClass = matchScore >= 90 ? 'text-green-500' : 'text-gray-500';

  return (
    <div
      className={`${borderClass} bg-white p-6 mb-4 shadow-sm`}
      style={{ borderRadius: '12px' }}
      data-testid="scheme-card"
    >
      <div className="flex justify-between items-center pb-4 border-b border-gray-200">
        <div className="flex items-center gap-3">
          <span className={`${badgeClass} px-3 py-1.5 text-xs font-bold`} style={{ borderRadius: '8px' }}>
            TOP {rank}
          </span>
          <span className="text-gray-800 text-lg font-semibold" data-testid="scheme-name">
            {bank} - {product}
          </span>
        </div>
        <span className={`${matchClass} text-sm font-semibold`} data-testid="match-score">
          匹配度 {matchScore}%
        </span>
      </div>

      <div className="flex flex-col gap-3 pt-4">
        <div className="flex gap-2 items-center">
          <span className="text-gray-500 text-xs">推荐理由：</span>
          <span className="text-gray-800 text-sm" data-testid="scheme-reason">{reason}</span>
        </div>

        <div className="flex gap-8">
          <div className="flex flex-col gap-1.5">
            <span className="text-gray-500 text-xs">产品利率</span>
            <span className="text-gray-800 text-lg font-bold" data-testid="scheme-rate">
              {rateMin}% - {rateMax}%
            </span>
          </div>
          <div className="flex flex-col gap-1.5">
            <span className="text-gray-500 text-xs">最高额度</span>
            <span className="text-gray-800 text-lg font-bold" data-testid="scheme-limit">{limit}万</span>
          </div>
          <div className="flex flex-col gap-1.5">
            <span className="text-gray-500 text-xs">还款期限</span>
            <span className="text-gray-800 text-lg font-bold" data-testid="scheme-term">{termMin}-{termMax}月</span>
          </div>
        </div>
      </div>

      <div className="flex justify-between items-center pt-4 border-t border-gray-200">
        <span className="text-gray-500 text-xs">已为您准备申请材料，可直接提交</span>
        <button
          className="flex items-center gap-2 px-6 py-3 bg-blue-500 text-white text-sm font-semibold hover:bg-blue-600 transition-colors"
          style={{ borderRadius: '8px' }}
        >
          <ArrowRight className="w-4 h-4" />
          <span>立即申请</span>
        </button>
      </div>
    </div>
  );
};

interface CustomerInfoProps {
  customerName: string;
  loanType: string;
  matchTime: string;
  matchCount: number;
}

const CustomerInfo = ({ customerName, loanType, matchTime, matchCount }: CustomerInfoProps) => {
  return (
    <div
      className="bg-white border border-gray-200 p-6 mb-5"
      style={{ borderRadius: '12px' }}
      data-testid="customer-info"
    >
      <div className="flex justify-between items-center mb-4">
        <span className="text-gray-800 text-base font-semibold">客户信息</span>
        <div className="flex items-center gap-2 px-3 py-1.5 bg-green-100" style={{ borderRadius: '12px' }}>
          <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />
          <span className="text-green-500 text-xs">已匹配 {matchCount} 个方案</span>
        </div>
      </div>

      <div className="flex gap-10">
        <div className="flex flex-col gap-1.5">
          <span className="text-gray-500 text-xs">客户名称</span>
          <span className="text-gray-800 text-sm font-medium">{customerName}</span>
        </div>
        <div className="flex flex-col gap-1.5">
          <span className="text-gray-500 text-xs">贷款类型</span>
          <span className="text-gray-800 text-sm font-medium">{loanType}</span>
        </div>
        <div className="flex flex-col gap-1.5">
          <span className="text-gray-500 text-xs">匹配时间</span>
          <span className="text-gray-800 text-sm font-medium">{matchTime}</span>
        </div>
      </div>
    </div>
  );
};

// ============================================
// Icons
// ============================================

const ArrowRight = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <line x1="5" y1="12" x2="19" y2="12"></line>
    <polyline points="12 5 19 12 12 19"></polyline>
  </svg>
);

const CheckCircle2 = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
    <polyline points="22 4 12 14.01 9 11.01"></polyline>
  </svg>
);

const Loader2 = ({ className }: { className?: string }) => (
  <svg className={`${className} animate-spin`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 12a9 9 0 1 1-6.219-8.56"></path>
  </svg>
);

const AlertCircle = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10"></circle>
    <line x1="12" y1="8" x2="12" y2="12"></line>
    <line x1="12" y1="16" x2="12.01" y2="16"></line>
  </svg>
);

const Download = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
    <polyline points="7 10 12 15 17 10"></polyline>
    <line x1="12" y1="15" x2="12" y2="3"></line>
  </svg>
);

const Search = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="8"></circle>
    <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
  </svg>
);

const Sparkles = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z"></path>
    <path d="M5 3v4"></path>
    <path d="M19 17v4"></path>
    <path d="M3 5h4"></path>
    <path d="M17 19h4"></path>
  </svg>
);

const FileText = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path>
    <polyline points="14 2 14 8 20 8"></polyline>
    <line x1="16" y1="13" x2="8" y2="13"></line>
    <line x1="16" y1="17" x2="8" y2="17"></line>
    <line x1="10" y1="9" x2="8" y2="9"></line>
  </svg>
);

// ============================================
// Helper Functions
// ============================================

/**
 * Parse match result string into structured scheme data
 * The backend returns a formatted string, we try to parse it for display
 */
interface ParsedScheme {
  bank: string;
  product: string;
  matchScore: number;
  reason: string;
  rateMin: number;
  rateMax: number;
  limit: number;
  termMin: number;
  termMax: number;
  rank: number;
}

function parseMatchResult(matchResult: string): ParsedScheme[] {
  // If the result is empty or just whitespace, return empty array
  if (!matchResult || !matchResult.trim()) {
    return [];
  }

  // Try to parse as JSON first (in case backend returns structured data)
  try {
    const parsed = JSON.parse(matchResult);
    if (Array.isArray(parsed)) {
      return parsed.map((item, index) => ({
        bank: item.bank || '未知银行',
        product: item.product || '未知产品',
        matchScore: item.matchScore || item.match_score || 0,
        reason: item.reason || '暂无推荐理由',
        rateMin: item.rateMin || item.rate_min || 0,
        rateMax: item.rateMax || item.rate_max || 0,
        limit: item.limit || 0,
        termMin: item.termMin || item.term_min || 0,
        termMax: item.termMax || item.term_max || 0,
        rank: index + 1,
      }));
    }
  } catch {
    // Not JSON, treat as plain text
  }

  // Return a single card with the raw result as reason
  return [{
    bank: '匹配结果',
    product: '方案详情',
    matchScore: 0,
    reason: matchResult,
    rateMin: 0,
    rateMax: 0,
    limit: 0,
    termMin: 0,
    termMax: 0,
    rank: 1,
  }];
}

/**
 * Get customer name from extraction results
 * Property 9: Context Data Integration
 */
function getCustomerNameFromResults(results: Array<{ customerName: string | null }>): string {
  for (const result of results) {
    if (result.customerName) {
      return result.customerName;
    }
  }
  return '未知客户';
}

/**
 * Merge extraction results into a single customer data object
 * Property 9: Context Data Integration
 */
export function mergeExtractionResults(results: Array<{ content: Record<string, unknown> }>): Record<string, unknown> {
  const merged: Record<string, unknown> = {};
  for (const result of results) {
    Object.assign(merged, result.content);
  }
  return merged;
}

// ============================================
// Main Component
// ============================================

export const SchemeMatchPage = () => {
  // State - Data Source
  const [dataSource, setDataSource] = useState<DataSource>('extracted');
  
  // State - Credit Type
  const [creditType, setCreditType] = useState<CreditType>('enterprise_credit');
  
  // State - Manual Input
  const [naturalLanguageInput, setNaturalLanguageInput] = useState<string>('');
  const [parsedData, setParsedData] = useState<Record<string, unknown> | null>(null);
  const [isParsing, setIsParsing] = useState<boolean>(false);
  const [parseError, setParseError] = useState<string | null>(null);
  
  // State - Saved Applications
  const [savedApplications, setSavedApplications] = useState<SavedApplicationListItem[]>([]);
  const [selectedApplicationId, setSelectedApplicationId] = useState<string>('');
  const [isLoadingApplications, setIsLoadingApplications] = useState<boolean>(false);
  
  // State - Match Results
  const [matchTime, setMatchTime] = useState<string>('');
  
  // State - Customer Selection (for extracted data)
  const [selectedCustomer, setSelectedCustomer] = useState<string>('');
  const [customerSearchQuery, setCustomerSearchQuery] = useState<string>('');

  // Legacy state for backward compatibility
  const useContextData = dataSource === 'extracted';

  // Hooks
  const { loading, error, data: matchResult, execute, reset } = useLoading<string>();
  const { getSignal } = useAbortController();
  const { state, setSchemeResult, setSchemeTaskStatus } = useApp();

  // Ref to track if recovery is in progress
  const isRecoveringRef = useRef(false);
  // Ref to store the latest match function to avoid closure issues (踩坑点 #31)
  const matchRef = useRef<((type: CreditType, data: Record<string, unknown>) => Promise<void>) | null>(null);

  // Get all customer names from customerDataMap
  const customerNames = useMemo(() => {
    return Object.keys(state.extraction.customerDataMap);
  }, [state.extraction.customerDataMap]);

  // Filter customer names based on search query
  const filteredCustomerNames = useMemo(() => {
    if (!customerSearchQuery.trim()) {
      return customerNames;
    }
    const query = customerSearchQuery.toLowerCase();
    return customerNames.filter(name => name.toLowerCase().includes(query));
  }, [customerNames, customerSearchQuery]);

  // Get customer data for selected customer
  const selectedCustomerData = useMemo(() => {
    if (!selectedCustomer || !state.extraction.customerDataMap[selectedCustomer]) {
      return {};
    }
    // Merge all extraction results for the selected customer
    const results = state.extraction.customerDataMap[selectedCustomer];
    const merged: Record<string, unknown> = {};
    for (const result of results) {
      Object.assign(merged, result.content);
    }
    return merged;
  }, [selectedCustomer, state.extraction.customerDataMap]);

  // Get customer data from context (Property 9: Context Data Integration)
  // Now uses selectedCustomerData if a customer is selected, otherwise falls back to all results
  const contextCustomerData = useMemo(() => {
    if (selectedCustomer && Object.keys(selectedCustomerData).length > 0) {
      return selectedCustomerData;
    }
    return mergeExtractionResults(state.extraction.results);
  }, [selectedCustomer, selectedCustomerData, state.extraction.results]);

  const contextCustomerName = useMemo(() => {
    // If a customer is selected, use that name
    if (selectedCustomer) {
      return selectedCustomer;
    }
    return getCustomerNameFromResults(state.extraction.results);
  }, [selectedCustomer, state.extraction.results]);

  const hasContextData = state.extraction.results.length > 0;
  const hasCustomerData = customerNames.length > 0;

  // Auto-select first customer if none selected and customers exist
  useEffect(() => {
    if (!selectedCustomer && customerNames.length > 0) {
      setSelectedCustomer(customerNames[0]);
    }
  }, [customerNames, selectedCustomer]);

  // Load saved applications on mount
  useEffect(() => {
    const loadSavedApplications = async () => {
      setIsLoadingApplications(true);
      try {
        const apps = await listSavedApplications();
        setSavedApplications(apps);
      } catch (e) {
        console.error('Failed to load saved applications:', e);
      } finally {
        setIsLoadingApplications(false);
      }
    };
    loadSavedApplications();
  }, []);

  // Auto-select data source on initial load only (not on every change)
  // This runs once to set a sensible default, but doesn't prevent user from selecting any option
  useEffect(() => {
    // Only auto-select on initial mount, not when user manually changes
    // The 'extracted' option now supports querying Feishu directly, so it should always be selectable
  }, []);

  // Get the credit type label for display
  const creditTypeLabel = useMemo(() => {
    const option = CREDIT_TYPE_OPTIONS.find(opt => opt.value === creditType);
    return option?.label || creditType;
  }, [creditType]);

  /**
   * Handle natural language parsing
   * Converts user's natural language description to structured customer data
   */
  const handleParseNaturalLanguage = useCallback(async () => {
    if (!naturalLanguageInput.trim()) return;
    
    setIsParsing(true);
    setParseError(null);
    setParsedData(null);
    
    try {
      const response = await parseNaturalLanguage({
        text: naturalLanguageInput,
        creditType: creditType
      }, getSignal());
      setParsedData(response.customerData);
    } catch (e) {
      console.error('Failed to parse natural language:', e);
      const errorMessage = e instanceof Error ? e.message : '解析失败，请重试';
      setParseError(errorMessage);
    } finally {
      setIsParsing(false);
    }
  }, [naturalLanguageInput, creditType, getSignal]);

  /**
   * Perform scheme matching with given parameters
   * Extracted to avoid closure issues during recovery
   */
  const doMatch = useCallback(async (type: CreditType, customerData: Record<string, unknown>) => {
    const request: SchemeMatchRequest = {
      customerData,
      creditType: type,
    };

    const { data: result, error: execError } = await execute(async () => {
      const response = await matchScheme(request, getSignal());
      return response.matchResult;
    });

    if (result !== null) {
      // Store result in AppContext
      setSchemeResult(result, type);
      // Record match time
      setMatchTime(new Date().toLocaleString('zh-CN'));
      // Mark task as done
      setSchemeTaskStatus('done', null);
    } else {
      // Check if it was an abort (page switch) vs real error
      // 使用 execute 返回的 error，避免闭包问题（踩坑点 #33）
      const isAbortError = execError?.name === 'AbortError';
      if (!isAbortError) {
        // Real failure, reset status
        setSchemeTaskStatus('idle', null);
      }
      // If aborted, keep 'matching' status for recovery
    }
  }, [execute, getSignal, setSchemeResult, setSchemeTaskStatus]);

  // Keep ref updated with latest function
  useEffect(() => {
    matchRef.current = doMatch;
  }, [doMatch]);

  // Handle task recovery on mount
  useEffect(() => {
    const taskState = state.tasks.scheme;
    if (taskState.status === 'matching' && taskState.params && !isRecoveringRef.current) {
      isRecoveringRef.current = true;
      const { creditType: savedType, customerData: savedData } = taskState.params;
      setCreditType(savedType as CreditType);
      
      // Use setTimeout to ensure state is updated before calling match
      // Pass params directly to avoid closure issues (踩坑点 #31)
      setTimeout(() => {
        if (matchRef.current) {
          matchRef.current(savedType as CreditType, savedData);
        }
        isRecoveringRef.current = false;
      }, 100);
    }
  }, [state.tasks.scheme]); // Re-run when task state changes for recovery

  /**
   * Handle scheme matching
   * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
   * Supports three data sources: extracted, savedApplication, manual
   */
  const handleMatch = useCallback(async () => {
    // Property 8: Credit Type Validation
    if (!isValidCreditType(creditType)) {
      console.error('Invalid credit type:', creditType);
      return;
    }

    // Determine customer data source based on dataSource state
    let customerData: Record<string, unknown>;
    
    if (dataSource === 'extracted') {
      // 优先使用本地已提取的数据
      if (hasContextData && selectedCustomer) {
        customerData = contextCustomerData;
      } else if (customerSearchQuery.trim()) {
        // 没有本地数据，查询飞书
        try {
          const response = await searchCustomer({ customerName: customerSearchQuery.trim() }, getSignal());
          if (!response.found) {
            console.error('Customer not found in Feishu');
            // TODO: 显示错误提示给用户
            return;
          }
          customerData = response.customerData;
        } catch (e) {
          console.error('Failed to search customer:', e);
          return;
        }
      } else {
        console.error('No customer name provided');
        return;
      }
    } else if (dataSource === 'savedApplication' && selectedApplicationId) {
      // Get data from saved application
      try {
        const app = await getApplication(selectedApplicationId, getSignal());
        customerData = app.applicationData;
      } catch (e) {
        console.error('Failed to load application:', e);
        return;
      }
    } else if (dataSource === 'manual') {
      // Use parsed data if available, otherwise try to parse natural language
      if (parsedData) {
        customerData = parsedData;
      } else if (naturalLanguageInput.trim()) {
        // Try to parse natural language first
        try {
          const response = await parseNaturalLanguage({
            text: naturalLanguageInput,
            creditType: creditType
          }, getSignal());
          customerData = response.customerData;
          setParsedData(response.customerData);
        } catch (e) {
          console.error('Failed to parse natural language:', e);
          // Fall back to treating as raw input
          customerData = { rawInput: naturalLanguageInput };
        }
      } else {
        console.error('No customer data available');
        return;
      }
    } else {
      console.error('Invalid data source or missing data');
      return;
    }

    // Save task state before starting (for recovery on page switch)
    setSchemeTaskStatus('matching', { creditType, customerData });

    // Use the extracted match function
    await doMatch(creditType, customerData);
  }, [creditType, dataSource, hasContextData, contextCustomerData, selectedApplicationId, parsedData, naturalLanguageInput, doMatch, setSchemeTaskStatus, getSignal, customerSearchQuery, selectedCustomer]);

  /**
   * Handle credit type change
   * Property 8: Credit Type Validation
   */
  const handleCreditTypeChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = e.target.value;
    if (isValidCreditType(value)) {
      setCreditType(value);
    }
  }, []);

  /**
   * Clear results and reset state
   */
  const handleClear = useCallback(() => {
    reset();
    setSchemeResult(null);
    setSchemeTaskStatus('idle', null);
    setMatchTime('');
  }, [reset, setSchemeResult, setSchemeTaskStatus]);

  // Parse match result for display
  const parsedSchemes = useMemo(() => {
    return matchResult ? parseMatchResult(matchResult) : [];
  }, [matchResult]);

  const customerName = useContextData && hasContextData ? contextCustomerName : '手动输入';

  /**
   * Download match result as markdown file
   * Requirement 8.4: Download button
   */
  const handleDownload = useCallback(() => {
    if (!matchResult) return;
    
    const content = `# 方案匹配结果\n\n**客户名称**: ${customerName}\n**贷款类型**: ${creditTypeLabel}\n**匹配时间**: ${matchTime}\n\n---\n\n${matchResult}`;
    const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `方案匹配_${customerName}_${new Date().toISOString().slice(0, 10)}.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [matchResult, customerName, creditTypeLabel, matchTime]);

  return (
    <div className="min-h-screen bg-slate-50 p-6">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-800 mb-1">方案匹配</h1>
              <p className="text-gray-500 text-sm">根据客户资质匹配最优贷款方案</p>
            </div>
            {matchResult && (
              <button
                onClick={handleDownload}
                className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white text-sm font-medium hover:bg-blue-600 transition-colors shadow-md"
                style={{ borderRadius: '8px' }}
                data-testid="download-button"
              >
                <Download className="w-4 h-4" />
                <span>下载结果</span>
              </button>
            )}
          </div>
        </div>

        {/* Input Section */}
        <div 
          className="bg-white border border-gray-200 p-6 mb-5 shadow-sm"
          style={{ borderRadius: '12px' }}
          data-testid="settings-card"
        >
          <h2 className="text-lg font-semibold text-gray-800 mb-4">匹配设置</h2>
          
          {/* Credit Type Selection - Property 8: Credit Type Validation */}
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-600 mb-1.5">
              贷款类型
            </label>
            <select
              value={creditType}
              onChange={handleCreditTypeChange}
              disabled={loading}
              className="w-full px-4 py-2.5 border text-sm text-gray-800 outline-none transition-all bg-white appearance-none cursor-pointer disabled:bg-gray-100 disabled:cursor-not-allowed"
              style={{ 
                borderRadius: '8px',
                borderColor: '#D1D5DB',
                backgroundImage: `url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e")`,
                backgroundPosition: 'right 0.5rem center',
                backgroundRepeat: 'no-repeat',
                backgroundSize: '1.5em 1.5em',
                paddingRight: '2.5rem'
              }}
              onFocus={(e) => {
                e.target.style.borderColor = '#3B82F6';
                e.target.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
              }}
              onBlur={(e) => {
                e.target.style.borderColor = '#D1D5DB';
                e.target.style.boxShadow = 'none';
              }}
              data-testid="credit-type-select"
            >
              {CREDIT_TYPE_OPTIONS.map(option => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          {/* Data Source Selection - Three options */}
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-600 mb-2">
              客户数据来源
            </label>
            <div className="flex flex-col gap-2">
              {/* Option 1: Feishu Data (飞书多维表格) */}
              <div 
                className={`p-3 border rounded-lg transition-all cursor-pointer ${
                  dataSource === 'extracted' 
                    ? 'border-blue-500 bg-blue-50' 
                    : 'border-gray-200 hover:border-gray-300'
                }`}
                onClick={() => !loading && setDataSource('extracted')}
              >
                <div className="flex items-center gap-3">
                  <input
                    type="radio"
                    name="dataSource"
                    checked={dataSource === 'extracted'}
                    onChange={() => setDataSource('extracted')}
                    disabled={loading}
                    className="w-4 h-4 text-blue-500 border-gray-300 focus:ring-blue-500 focus:ring-2 accent-blue-500"
                    style={{ accentColor: '#3B82F6' }}
                    data-testid="use-extracted-data"
                  />
                  <div className="flex-1">
                    <span className="text-sm font-medium text-gray-700">
                      本地客户资料
                    </span>
                    <span className="ml-2 text-xs text-gray-400">
                      (按客户名称查询)
                    </span>
                  </div>
                </div>
                {/* Customer Name Input for Feishu Query */}
                {dataSource === 'extracted' && (
                  <div className="mt-3 pl-7">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                      <input
                        type="text"
                        value={customerSearchQuery}
                        onChange={(e) => setCustomerSearchQuery(e.target.value)}
                        placeholder="输入客户名称查询..."
                        disabled={loading}
                        className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 outline-none transition-all bg-white disabled:bg-gray-100"
                        style={{ borderRadius: '6px' }}
                        onFocus={(e) => {
                          e.target.style.borderColor = '#3B82F6';
                          e.target.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
                        }}
                        onBlur={(e) => {
                          e.target.style.borderColor = '#D1D5DB';
                          e.target.style.boxShadow = 'none';
                        }}
                        data-testid="feishu-customer-name-input"
                      />
                    </div>
                    {hasCustomerData && customerSearchQuery && (
                      <div className="mt-2 max-h-32 overflow-y-auto border border-gray-200 rounded-md bg-white">
                        {filteredCustomerNames.length > 0 ? (
                          filteredCustomerNames.map((name) => (
                            <div
                              key={name}
                              onClick={() => {
                                setSelectedCustomer(name);
                                setCustomerSearchQuery(name);
                              }}
                              className={`px-3 py-2 cursor-pointer text-sm transition-colors ${
                                selectedCustomer === name 
                                  ? 'bg-blue-50 text-blue-600' 
                                  : 'hover:bg-gray-50 text-gray-700'
                              }`}
                            >
                              {name}
                            </div>
                          ))
                        ) : (
                          <div className="px-3 py-2 text-sm text-gray-400">
                            未找到匹配的客户
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Option 2: Saved Applications */}
              <div 
                className={`p-3 border rounded-lg transition-all cursor-pointer ${
                  dataSource === 'savedApplication' 
                    ? 'border-blue-500 bg-blue-50' 
                    : 'border-gray-200 hover:border-gray-300'
                } ${savedApplications.length === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
                onClick={() => !loading && savedApplications.length > 0 && setDataSource('savedApplication')}
              >
                <div className="flex items-center gap-3">
                  <input
                    type="radio"
                    name="dataSource"
                    checked={dataSource === 'savedApplication'}
                    onChange={() => setDataSource('savedApplication')}
                    disabled={loading || savedApplications.length === 0}
                    className="w-4 h-4 text-blue-500 border-gray-300 focus:ring-blue-500 focus:ring-2 accent-blue-500"
                    style={{ accentColor: '#3B82F6' }}
                    data-testid="use-saved-application"
                  />
                  <div className="flex-1 flex items-center gap-2">
                    <FileText className="w-4 h-4 text-gray-500" />
                    <span className={`text-sm font-medium ${savedApplications.length === 0 ? 'text-gray-400' : 'text-gray-700'}`}>
                      已保存的申请表
                    </span>
                    <span className={`text-xs ${savedApplications.length > 0 ? 'text-blue-500' : 'text-gray-400'}`}>
                      {isLoadingApplications ? '(加载中...)' : `(${savedApplications.length} 条)`}
                    </span>
                  </div>
                </div>
                {/* Application Selection Dropdown */}
                {dataSource === 'savedApplication' && savedApplications.length > 0 && (
                  <div className="mt-3 pl-7">
                    <select
                      value={selectedApplicationId}
                      onChange={(e) => setSelectedApplicationId(e.target.value)}
                      disabled={loading}
                      className="w-full px-3 py-2 border text-sm text-gray-800 outline-none transition-all bg-white appearance-none cursor-pointer disabled:bg-gray-100"
                      style={{ 
                        borderRadius: '6px',
                        borderColor: '#D1D5DB',
                        backgroundImage: `url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e")`,
                        backgroundPosition: 'right 0.5rem center',
                        backgroundRepeat: 'no-repeat',
                        backgroundSize: '1.5em 1.5em',
                        paddingRight: '2rem'
                      }}
                      onFocus={(e) => {
                        e.target.style.borderColor = '#3B82F6';
                        e.target.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
                      }}
                      onBlur={(e) => {
                        e.target.style.borderColor = '#D1D5DB';
                        e.target.style.boxShadow = 'none';
                      }}
                      data-testid="saved-application-select"
                    >
                      <option value="">请选择申请表</option>
                      {savedApplications.map(app => (
                        <option key={app.id} value={app.id}>
                          {app.customerName} - {app.loanType} ({new Date(app.savedAt).toLocaleDateString('zh-CN')})
                        </option>
                      ))}
                    </select>
                  </div>
                )}
              </div>

              {/* Option 3: Manual Input */}
              <div 
                className={`p-3 border rounded-lg cursor-pointer transition-all ${
                  dataSource === 'manual' 
                    ? 'border-blue-500 bg-blue-50' 
                    : 'border-gray-200 hover:border-gray-300'
                }`}
                onClick={() => !loading && setDataSource('manual')}
              >
                <div className="flex items-center gap-3">
                  <input
                    type="radio"
                    name="dataSource"
                    checked={dataSource === 'manual'}
                    onChange={() => setDataSource('manual')}
                    disabled={loading}
                    className="w-4 h-4 text-blue-500 border-gray-300 focus:ring-blue-500 focus:ring-2 accent-blue-500"
                    style={{ accentColor: '#3B82F6' }}
                    data-testid="use-manual-data"
                  />
                  <div className="flex-1 flex items-center gap-2">
                    <Sparkles className="w-4 h-4 text-gray-500" />
                    <span className="text-sm font-medium text-gray-700">
                      手动输入
                    </span>
                    <span className="text-xs text-gray-400">
                      (支持自然语言描述)
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Manual Input Section - Only shown when using manual input */}
          {dataSource === 'manual' && (
            <div className="mb-4 space-y-4">
              {/* Natural Language Input */}
              <div>
                <label className="block text-sm font-medium text-gray-600 mb-1.5">
                  自然语言描述
                </label>
                <div className="relative">
                  <textarea
                    value={naturalLanguageInput}
                    onChange={(e) => {
                      setNaturalLanguageInput(e.target.value);
                      setParsedData(null);
                      setParseError(null);
                    }}
                    disabled={loading || isParsing}
                    placeholder="请描述客户情况，例如：年开票3000万，负债1500万，征信良好，想贷款500万"
                    className="w-full px-4 py-3 border text-sm text-gray-800 outline-none transition-all bg-white disabled:bg-gray-100 disabled:cursor-not-allowed min-h-[80px] resize-none"
                    style={{ 
                      borderRadius: '8px',
                      borderColor: '#D1D5DB'
                    }}
                    onFocus={(e) => {
                      e.target.style.borderColor = '#3B82F6';
                      e.target.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
                    }}
                    onBlur={(e) => {
                      e.target.style.borderColor = '#D1D5DB';
                      e.target.style.boxShadow = 'none';
                    }}
                    data-testid="natural-language-input"
                  />
                </div>
                <div className="flex items-center justify-between mt-2">
                  <p className="text-xs text-gray-400">
                    AI 将自动解析您的描述并转换为结构化数据
                  </p>
                  <button
                    onClick={handleParseNaturalLanguage}
                    disabled={loading || isParsing || !naturalLanguageInput.trim()}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-purple-500 text-white text-xs font-medium hover:bg-purple-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    style={{ borderRadius: '6px' }}
                    data-testid="parse-button"
                  >
                    {isParsing ? (
                      <>
                        <Loader2 className="w-3 h-3" />
                        <span>解析中...</span>
                      </>
                    ) : (
                      <>
                        <Sparkles className="w-3 h-3" />
                        <span>解析</span>
                      </>
                    )}
                  </button>
                </div>
              </div>

              {/* Parse Error */}
              {parseError && (
                <div className="flex items-start gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
                  <AlertCircle className="w-4 h-4 text-red-500 flex-shrink-0 mt-0.5" />
                  <p className="text-sm text-red-600">{parseError}</p>
                </div>
              )}

              {/* Parsed Data Preview */}
              {parsedData && (
                <div>
                  <label className="block text-sm font-medium text-gray-600 mb-1.5">
                    解析结果预览
                  </label>
                  <div 
                    className="bg-green-50 border border-green-200 p-4 max-h-[150px] overflow-auto"
                    style={{ borderRadius: '8px' }}
                    data-testid="parsed-data-preview"
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <CheckCircle2 className="w-4 h-4 text-green-500" />
                      <span className="text-sm font-medium text-green-700">解析成功</span>
                    </div>
                    <pre className="text-xs text-gray-600 whitespace-pre-wrap font-mono">
                      {JSON.stringify(parsedData, null, 2)}
                    </pre>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Context Data Preview (shown when using extracted data) */}
          {dataSource === 'extracted' && hasContextData && (
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-600 mb-1.5">
                已提取的客户数据预览
              </label>
              <div 
                className="bg-slate-50 p-4 max-h-[200px] overflow-auto border border-gray-100"
                style={{ borderRadius: '8px' }}
                data-testid="context-data-preview"
              >
                <pre className="text-xs text-gray-600 whitespace-pre-wrap font-mono">
                  {JSON.stringify(contextCustomerData, null, 2)}
                </pre>
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-3 pt-2">
            <button
              onClick={handleMatch}
              disabled={loading || isParsing || 
                (dataSource === 'extracted' && !selectedCustomer && !customerSearchQuery.trim()) ||
                (dataSource === 'savedApplication' && !selectedApplicationId) ||
                (dataSource === 'manual' && !parsedData && !naturalLanguageInput.trim())
              }
              className="flex items-center gap-2 px-6 py-2.5 bg-blue-500 text-white text-sm font-medium hover:bg-blue-600 transition-colors shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ borderRadius: '8px' }}
              data-testid="match-button"
            >
              {loading ? (
                <>
                  <Loader2 className="w-4 h-4" />
                  <span>匹配中...</span>
                </>
              ) : (
                <>
                  <Search className="w-4 h-4" />
                  <span>开始匹配</span>
                </>
              )}
            </button>
            {matchResult && (
              <button
                onClick={handleClear}
                disabled={loading}
                className="px-4 py-2.5 border border-gray-300 text-gray-600 text-sm hover:bg-gray-50 transition-colors disabled:opacity-50"
                style={{ borderRadius: '8px' }}
                data-testid="clear-button"
              >
                清除结果
              </button>
            )}
          </div>
        </div>

        {/* Error Display - Requirement 5.5 */}
        {error && (
          <div 
            className="bg-red-50 border border-red-200 p-4 mb-5 flex items-start gap-3" 
            style={{ borderRadius: '12px' }}
            data-testid="error-message"
          >
            <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-red-800 font-medium">匹配失败</p>
              <p className="text-red-600 text-sm">{error.message}</p>
            </div>
          </div>
        )}

        {/* Loading Indicator - Requirement 5.2 */}
        {loading && (
          <div 
            className="bg-blue-50 border border-blue-200 p-6 mb-5 flex items-center justify-center gap-3" 
            style={{ borderRadius: '12px' }}
            data-testid="loading-indicator"
          >
            <Loader2 className="w-6 h-6 text-blue-500" />
            <span className="text-blue-700 font-medium">正在匹配方案，请稍候...</span>
          </div>
        )}

        {/* Results Section - Requirement 5.3 */}
        {matchResult && !loading && (
          <>
            {/* Customer Info */}
            <CustomerInfo
              customerName={customerName}
              loanType={creditTypeLabel}
              matchTime={matchTime}
              matchCount={parsedSchemes.length}
            />

            {/* Section Header */}
            <div className="flex items-center justify-between pb-3">
              <div className="flex items-center gap-3">
                <h2 className="text-lg font-bold text-gray-800">推荐方案</h2>
                <span className="text-gray-500 text-xs">基于客户资质和飞书知识库匹配</span>
              </div>
            </div>

            {/* Scheme Cards */}
            <div className="flex-1" data-testid="match-results">
              {parsedSchemes.length > 0 ? (
                parsedSchemes.map((scheme, index) => (
                  <SchemeCard
                    key={index}
                    bank={scheme.bank}
                    product={scheme.product}
                    matchScore={scheme.matchScore}
                    reason={scheme.reason}
                    rateMin={scheme.rateMin}
                    rateMax={scheme.rateMax}
                    limit={scheme.limit}
                    termMin={scheme.termMin}
                    termMax={scheme.termMax}
                    rank={scheme.rank}
                    isTopRank={index === 0}
                  />
                ))
              ) : (
                <div 
                  className="bg-white border border-gray-200 p-6 text-center shadow-sm"
                  style={{ borderRadius: '12px' }}
                >
                  <p className="text-gray-500">暂无匹配方案</p>
                </div>
              )}
            </div>

            {/* Raw Result (for debugging/display) */}
            <div 
              className="mt-4 bg-white border border-gray-200 p-4 shadow-sm"
              style={{ borderRadius: '12px' }}
            >
              <details>
                <summary className="text-sm text-gray-500 cursor-pointer hover:text-gray-700 transition-colors">
                  查看原始匹配结果
                </summary>
                <pre 
                  className="mt-2 text-xs text-gray-600 whitespace-pre-wrap bg-slate-50 p-3 overflow-auto max-h-[300px] font-mono"
                  style={{ borderRadius: '8px' }}
                >
                  {matchResult}
                </pre>
              </details>
            </div>
          </>
        )}

        {/* Empty State */}
        {!matchResult && !loading && !error && (
          <div 
            className="bg-white border border-gray-200 p-12 text-center shadow-sm"
            style={{ borderRadius: '12px' }}
            data-testid="empty-state"
          >
            <div className="text-gray-400 mb-4">
              <Search className="w-16 h-16 mx-auto" />
            </div>
            <h3 className="text-lg font-medium text-gray-700 mb-2">开始方案匹配</h3>
            <p className="text-gray-500 text-sm max-w-md mx-auto">
              {hasContextData 
                ? '选择贷款类型并点击"开始匹配"按钮，系统将根据已提取的客户数据为您推荐合适的贷款方案。'
                : '请先上传客户资料进行数据提取，或手动输入客户数据后开始匹配。'
              }
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default SchemeMatchPage;
