/**
 * ApplicationPage - Loan Application Generation
 *
 * This component allows users to generate loan application forms
 * based on customer data stored in local storage.
 *
 * Feature: frontend-backend-integration, frontend-ui-optimization
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7
 * Task 8.1: Update application page layout
 */

import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { 
  FileText, Download, Loader2, AlertTriangle, CheckCircle, XCircle, 
  User, Search, Edit3, Save, ChevronDown, ChevronRight,
  Building2, CreditCard, Banknote, Calendar, DollarSign, Building, 
  BadgeCheck, FileCheck, FileSpreadsheet, Copy
} from 'lucide-react';
import { createApplicationJob, getChatJobStatus, saveApplication } from '../services/api';
import type { ApplicationResponse, ChatJobStatusResponse, ChatJobSummaryResponse } from '../services/types';
import { useAbortController } from '../hooks/useAbortController';
import { useApp } from '../context/AppContext';
import ProcessFeedbackCard, { type ProcessFeedbackTone } from './common/ProcessFeedbackCard';
import AsyncJobCard from './common/AsyncJobCard';
import { getJobStatusText, getJobTypeLabel } from '../config/jobDisplay';

/**
 * Loan type options
 */
type LoanType = 'enterprise' | 'personal';

// ============================================
// Utility Functions
// ============================================

/**
 * Get section icon based on section name
 */
function getSectionIcon(sectionName: string): React.ReactNode {
  const iconMap: Record<string, React.ReactNode> = {
    '基本信息': <User className="w-4 h-4" />,
    '个人信息': <User className="w-4 h-4" />,
    '企业信息': <Building2 className="w-4 h-4" />,
    '贷款信息': <CreditCard className="w-4 h-4" />,
    '财务信息': <DollarSign className="w-4 h-4" />,
    '资产信息': <Building className="w-4 h-4" />,
    '抵押物信息': <Building className="w-4 h-4" />,
    '征信信息': <BadgeCheck className="w-4 h-4" />,
    '流水信息': <Banknote className="w-4 h-4" />,
    '其他信息': <FileCheck className="w-4 h-4" />,
  };

  if (iconMap[sectionName]) {
    return iconMap[sectionName];
  }

  for (const [key, icon] of Object.entries(iconMap)) {
    if (sectionName.includes(key) || key.includes(sectionName)) {
      return icon;
    }
  }

  return <FileSpreadsheet className="w-4 h-4" />;
}

/**
 * Get icon for field name
 */
function getFieldIcon(fieldName: string): React.ReactNode {
  const fieldIcons: Record<string, React.ReactNode> = {
    '姓名': <User className="w-3.5 h-3.5" />,
    '企业名称': <Building2 className="w-3.5 h-3.5" />,
    '公司名称': <Building2 className="w-3.5 h-3.5" />,
    '身份证号': <CreditCard className="w-3.5 h-3.5" />,
    '统一社会信用代码': <BadgeCheck className="w-3.5 h-3.5" />,
    '贷款金额': <DollarSign className="w-3.5 h-3.5" />,
    '利率': <DollarSign className="w-3.5 h-3.5" />,
    '日期': <Calendar className="w-3.5 h-3.5" />,
  };

  for (const [key, icon] of Object.entries(fieldIcons)) {
    if (fieldName.includes(key)) {
      return icon;
    }
  }
  return <FileText className="w-3.5 h-3.5" />;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function createDownloadLink(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function normalizeSectionTitle(value: string): string {
  const map: Record<string, string> = {
    '鍩烘湰淇℃伅': '基本信息',
    '涓汉淇℃伅': '个人信息',
    '浼佷笟淇℃伅': '企业信息',
    '璐锋淇℃伅': '贷款信息',
    '璐㈠姟淇℃伅': '财务信息',
    '璧勪骇淇℃伅': '资产信息',
    '寰佷俊淇℃伅': '征信信息',
    '娴佹按淇℃伅': '流水信息',
    '鍏朵粬淇℃伅': '其他信息',
  };
  return map[value] || value;
}

function normalizeFieldLabel(value: string): string {
  const map: Record<string, string> = {
    '濮撳悕': '姓名',
    '浼佷笟鍚嶇О': '企业名称',
    '鍏徃鍚嶇О': '公司名称',
    '韬唤璇佸彿': '身份证号',
    '缁熶竴绀句細淇＄敤浠ｇ爜': '统一社会信用代码',
    '璐锋閲戦': '贷款金额',
    '鍒╃巼': '利率',
    '鏃ユ湡': '日期',
  };
  return map[value] || value;
}

function buildApplicationFormHtml(
  customerName: string,
  loanType: LoanType,
  applicationData: Record<string, Record<string, string>>
): string {
  const safeCustomerName = customerName.trim() || '\u672a\u547d\u540d';
  const loanTypeLabel =
    loanType === 'enterprise' ? '\u4f01\u4e1a\u8d37\u6b3e' : '\u4e2a\u4eba\u8d37\u6b3e';
  const exportedAt = new Date().toLocaleString('zh-CN', { hour12: false });

  const sectionsHtml = Object.entries(applicationData)
    .map(([sectionName, sectionData]) => {
      const rows = Object.entries(sectionData)
        .map(([fieldName, value]) => {
          const renderedValue = escapeHtml((value || '-').trim() || '-').replace(/\r?\n/g, '<br />');
          return `
            <tr>
              <th>${escapeHtml(fieldName)}</th>
              <td>${renderedValue}</td>
            </tr>
          `;
        })
        .join('');

      return `
        <section class="section-card">
          <div class="section-header">
            <div class="section-title">${escapeHtml(sectionName)}</div>
            <div class="section-count">${Object.keys(sectionData).length} \u9879</div>
          </div>
          <div class="table-shell">
            <table>
              <tbody>
                ${rows}
              </tbody>
            </table>
          </div>
        </section>
      `;
    })
    .join('');

  return `<!DOCTYPE html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>\u8d37\u6b3e\u7533\u8bf7\u8868 - ${escapeHtml(safeCustomerName)}</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f8fafc;
        --card: #ffffff;
        --border: #e2e8f0;
        --text: #0f172a;
        --muted: #64748b;
        --header: #eff6ff;
        --header-border: #bfdbfe;
        --accent: #2563eb;
        --row-alt: #f8fafc;
      }

      * { box-sizing: border-box; }

      body {
        margin: 0;
        padding: 32px;
        font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans SC", sans-serif;
        background: var(--bg);
        color: var(--text);
      }

      .page {
        max-width: 1120px;
        margin: 0 auto;
      }

      .hero {
        background: linear-gradient(135deg, #eff6ff 0%, #ffffff 100%);
        border: 1px solid var(--header-border);
        border-radius: 18px;
        padding: 28px 32px;
        margin-bottom: 24px;
      }

      .hero h1 {
        margin: 0 0 10px;
        font-size: 28px;
      }

      .hero-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        color: var(--muted);
        font-size: 14px;
      }

      .hero-chip {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        border-radius: 999px;
        background: #dbeafe;
        color: var(--accent);
        font-weight: 600;
      }

      .section-card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 16px;
        overflow: hidden;
        margin-bottom: 18px;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.04);
      }

      .section-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 20px;
        background: linear-gradient(90deg, #f8fafc 0%, #ffffff 100%);
        border-bottom: 1px solid var(--border);
      }

      .section-title {
        font-size: 18px;
        font-weight: 700;
      }

      .section-count {
        font-size: 13px;
        color: var(--muted);
      }

      .table-shell {
        padding: 18px;
      }

      table {
        width: 100%;
        border-collapse: collapse;
        overflow: hidden;
        border-radius: 12px;
        border: 1px solid var(--border);
      }

      th, td {
        padding: 14px 16px;
        text-align: left;
        vertical-align: top;
        border-bottom: 1px solid var(--border);
      }

      tr:last-child th,
      tr:last-child td {
        border-bottom: none;
      }

      th {
        width: 32%;
        background: #ffffff;
        color: var(--muted);
        font-weight: 600;
        border-right: 1px solid var(--border);
      }

      tr:nth-child(even) th,
      tr:nth-child(even) td {
        background: var(--row-alt);
      }

      td {
        color: var(--text);
        word-break: break-word;
      }

      @media print {
        body {
          padding: 0;
          background: #ffffff;
        }

        .section-card,
        .hero {
          box-shadow: none;
        }
      }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <h1>\u8d37\u6b3e\u7533\u8bf7\u8868</h1>
        <div class="hero-meta">
          <span class="hero-chip">${escapeHtml(safeCustomerName)}</span>
          <span>\u8d37\u6b3e\u7c7b\u578b\uff1a${escapeHtml(loanTypeLabel)}</span>
          <span>\u5bfc\u51fa\u65f6\u95f4\uff1a${escapeHtml(exportedAt)}</span>
        </div>
      </section>
      ${sectionsHtml}
    </main>
  </body>
</html>`;
}

// ============================================
// Editable Data Section Card Component
// ============================================

interface EditableDataSectionCardProps {
  title: string;
  data: Record<string, string>;
  editMode: boolean;
  onFieldChange: (sectionTitle: string, fieldName: string, value: string) => void;
}

/**
 * EditableDataSectionCard Component
 * 
 * Renders a section with title and data table.
 * In edit mode, field values become editable inputs.
 */
const EditableDataSectionCard: React.FC<EditableDataSectionCardProps> = ({ 
  title, 
  data, 
  editMode, 
  onFieldChange 
}) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const entries = Object.entries(data);
  const normalizedTitle = normalizeSectionTitle(title);
  
  if (entries.length === 0) return null;
  
  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      {/* Section Header */}
      <div 
        className="px-3 py-2 bg-gradient-to-r from-slate-50 to-gray-50 border-b border-gray-100 cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-md flex items-center justify-center bg-blue-100 text-blue-600">
              {getSectionIcon(title)}
            </div>
            <span className="font-medium text-gray-700 text-sm">{normalizedTitle}</span>
            <span className="text-xs text-gray-400">({entries.length} 项)</span>
          </div>
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-400" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          )}
        </div>
      </div>
      
      {/* Section Content */}
      {isExpanded && (
        <div className="p-3">
          <div className="overflow-hidden rounded-lg border border-gray-200">
            <table className="w-full text-sm">
              <tbody>
                {entries.map(([key, value], idx) => (
                  <tr 
                    key={key} 
                    className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}
                  >
                    <td className="px-3 py-2 text-gray-500 font-medium w-1/3 border-r border-gray-100">
                      <div className="flex items-center gap-2">
                        <span className="text-gray-400">{getFieldIcon(key)}</span>
                        <span className="truncate">{normalizeFieldLabel(key)}</span>
                      </div>
                    </td>
                    <td className="px-3 py-2 text-gray-800">
                      {editMode ? (
                        <input
                          type="text"
                          value={value}
                          onChange={(e) => onFieldChange(title, key, e.target.value)}
                          className="w-full px-2 py-1 border border-blue-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-200"
                          data-testid={`edit-field-${title}-${key}`}
                        />
                      ) : (
                        <span className="break-words" title={value}>
                          {value || '-'}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};


// ============================================
// Main Component
// ============================================

/**
 * ApplicationPage Component
 *
 * Provides UI for:
 * - Entering customer name with search functionality
 * - Selecting loan type (enterprise/personal)
 * - Generating application via API
 * - Displaying generated content in card format with edit support
 * - Downloading application as Markdown or HTML form
 */
const ApplicationPage: React.FC = () => {
  // Form state
  const [customerName, setCustomerName] = useState<string>('');
  const [loanType, setLoanType] = useState<LoanType>('enterprise');

  // API integration hooks
  const { getSignal, abort } = useAbortController();
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);
  const reset = useCallback(() => setError(null), []);

  // Global state
  const { state, setApplicationResult, setApplicationTaskStatus } = useApp();

  // Local result state (synced with context)
  const [result, setResult] = useState<ApplicationResponse | null>(null);
  
  // Edit mode state
  const [editMode, setEditMode] = useState<boolean>(false);
  const [editedData, setEditedData] = useState<Record<string, Record<string, string>>>({});
  
  // Save to cache state
    const [saving, setSaving] = useState<boolean>(false);
    const [saveSuccess, setSaveSuccess] = useState<boolean>(false);
    const [copySuccess, setCopySuccess] = useState<boolean>(false);
    const [jobPolling, setJobPolling] = useState<boolean>(false);
    const [jobError, setJobError] = useState<string | null>(null);
    const [activeJobCard, setActiveJobCard] = useState<ChatJobSummaryResponse | null>(null);
    const stopPollingRef = useRef(false);
    const resultSectionRef = useRef<HTMLDivElement | null>(null);

    const currentCustomerName = state.extraction.currentCustomer;
    const currentCustomerId = state.extraction.currentCustomerId;

    const pollApplicationJob = useCallback(async (jobId: string, customerName: string) => {
      const startedAt = Date.now();
      let failureCount = 0;
      stopPollingRef.current = false;
      setJobPolling(true);
      setJobError(null);
      setLoading(false);
      setActiveJobCard((prev) => prev ? { ...prev, status: 'running', progressMessage: '正在处理任务', errorMessage: null } : prev);

      try {
        while (true) {
          if (stopPollingRef.current) {
            return;
          }
          if (Date.now() - startedAt > 5 * 60 * 1000) {
            setJobError('任务处理时间较长，请稍后重新进入页面查看。');
            setError(new Error('任务处理时间较长，请稍后重新进入页面查看。'));
            setApplicationTaskStatus('idle', null);
            return;
          }

          let status: ChatJobStatusResponse | null = null;
          try {
            status = await getChatJobStatus(jobId, getSignal());
            setActiveJobCard({
              jobId: status.jobId,
              jobType: status.jobType,
              jobTypeLabel: status.jobTypeLabel,
              customerId: status.customerId,
              customerName: status.customerName || customerName,
              status: status.status,
              progressMessage: status.progressMessage,
              errorMessage: status.errorMessage,
              createdAt: status.createdAt,
              startedAt: status.startedAt,
              finishedAt: status.finishedAt,
              targetPage: status.targetPage,
              resultSummary: status.resultSummary,
            });
          } catch (issue) {
            failureCount += 1;
            if (failureCount >= 3) {
              const message = issue instanceof Error ? issue.message : '任务状态获取失败';
              setJobError(message);
              setError(new Error(message));
              setApplicationTaskStatus('idle', null);
              return;
            }
            await new Promise((resolve) => window.setTimeout(resolve, 2000));
            continue;
          }

          failureCount = 0;

          if (status.status === 'pending' || status.status === 'running') {
            await new Promise((resolve) => window.setTimeout(resolve, 2000));
            continue;
          }

          if (status.status === 'success' && status.result) {
            const response = status.result as unknown as ApplicationResponse;
            setResult(response);
            setError(null);
            if (response.applicationData) {
              setEditedData(response.applicationData);
            }
            setApplicationResult(
              {
                content: response.applicationContent,
                customerFound: response.customerFound,
                warnings: response.warnings,
                applicationData: response.applicationData,
                metadata: response.metadata,
              },
              customerName,
            );
            setApplicationTaskStatus('done', null);
            return;
          }

          const message = status.errorMessage || '申请表生成失败';
          setJobError(message);
          setError(new Error(message));
          setApplicationTaskStatus('idle', null);
          return;
        }
      } finally {
        setJobPolling(false);
        stopPollingRef.current = false;
      }
    }, [getSignal, setApplicationResult, setApplicationTaskStatus]);

    /**
     * Generate application with given parameters
     * Extracted to avoid closure issues during recovery
     */
  const doGenerate = useCallback(async (name: string, type: LoanType) => {
    setLoading(true);
    setError(null);
    setJobError(null);
    try {
      const job = await createApplicationJob(
        {
          customerName: name,
          customerId: currentCustomerId,
          loanType: type,
        },
        getSignal(),
      );
      setActiveJobCard({
        jobId: job.jobId,
        jobType: 'application_generate',
        customerId: currentCustomerId || '',
        customerName: name,
        status: job.status,
        progressMessage: '任务已提交',
        errorMessage: null,
        createdAt: new Date().toISOString(),
        startedAt: '',
        finishedAt: '',
        targetPage: 'application',
        resultSummary: null,
      });
      setApplicationTaskStatus('idle', null);
      await pollApplicationJob(job.jobId, name);
    } catch (issue) {
      if (issue instanceof Error && issue.name === 'AbortError') {
        return;
      }
      const nextError = issue instanceof Error ? issue : new Error('申请表生成任务提交失败');
      setError(nextError);
      setJobError(nextError.message);
      setApplicationTaskStatus('idle', null);
    } finally {
      setLoading(false);
    }
  }, [currentCustomerId, getSignal, pollApplicationJob, setApplicationTaskStatus]);

  // Sync with context on mount and handle task recovery
  useEffect(() => {
    // Restore previous result if available
    if (state.application.result) {
      setResult({
        applicationContent: state.application.result.content,
        customerFound: state.application.result.customerFound,
        warnings: state.application.result.warnings,
        applicationData: state.application.result.applicationData,
        metadata: state.application.result.metadata,
      });
      // Also restore editedData so it's ready if user enters edit mode
      if (state.application.result.applicationData) {
        setEditedData(state.application.result.applicationData);
      }
    }
    if (state.application.lastCustomer) {
      setCustomerName(state.application.lastCustomer);
    }

  }, [state.application.lastCustomer, state.application.result]);

  /**
   * Handle form submission
   * Requirement 4.1: Call POST /api/application/generate
   * Requirement 4.2: Display loading indicator
   */
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!customerName.trim()) {
      return;
    }

    const trimmedName = customerName.trim();
    
    // Reset edit mode when generating new application
    setEditMode(false);
    setEditedData({});
    
    // Save task state before starting (for recovery on page switch)
    setApplicationTaskStatus('generating', { customerName: trimmedName, loanType });

    // Use the extracted generate function
    await doGenerate(trimmedName, loanType);
  };

  /**
   * Handle cancel request
   */
  const handleCancel = () => {
    stopPollingRef.current = true;
    abort();
    setLoading(false);
    setJobPolling(false);
    setApplicationTaskStatus('idle', null);
    setJobError('已停止查看当前任务状态，可稍后重新进入页面查看。');
    setError(new Error('已停止查看当前任务状态，可稍后重新进入页面查看。'));
    reset();
  };

  /**
   * Handle field change in edit mode
   */
  const handleFieldChange = useCallback((sectionTitle: string, fieldName: string, value: string) => {
    setEditedData(prev => ({
      ...prev,
      [sectionTitle]: {
        ...prev[sectionTitle],
        [fieldName]: value,
      },
    }));
  }, []);

  /**
   * Toggle edit mode
   */
  const toggleEditMode = () => {
    if (!editMode && result?.applicationData) {
      // Entering edit mode - initialize editedData from result
      setEditedData(result.applicationData);
    }
    setEditMode(!editMode);
  };
  /**
   * Save edited data to backend (merged with handleSaveToCache)
   * This function now saves directly to backend instead of just updating local state
   */
  const saveEditedData = async () => {
    const dataToSave = editMode ? editedData : (result?.applicationData || {});
    if (!dataToSave || Object.keys(dataToSave).length === 0) return;

    setSaving(true);
    setSaveSuccess(false);

    try {
      const safeCustomerName = customerName.trim() || '未命名客户';

      await saveApplication({
        customerName: safeCustomerName,
        loanType,
        applicationData: dataToSave,
      });

      if (result) {
        const updatedResult = {
          ...result,
          applicationData: dataToSave,
        };
        setResult(updatedResult);
        setApplicationResult(
          {
            content: updatedResult.applicationContent,
            customerFound: updatedResult.customerFound,
            warnings: updatedResult.warnings,
            applicationData: dataToSave,
            metadata: updatedResult.metadata,
          },
          safeCustomerName
        );
      }

      setEditMode(false);
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      console.error('Failed to save application:', err);
    } finally {
      setSaving(false);
    }
  };

  /**
   * Download application as .md file
   * Requirement 4.7: Allow downloading generated application
   */
  const downloadMarkdown = () => {
    if (!result?.applicationContent) return;

    const blob = new Blob([result.applicationContent], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `贷款申请表__.md`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  /**
   * Download application as a standalone HTML form
   */
  const downloadFormHtml = () => {
    const dataToDownload = editMode ? editedData : (result?.applicationData || editedData);
    if (!dataToDownload || Object.keys(dataToDownload).length === 0) return;

    const htmlContent = buildApplicationFormHtml(customerName, loanType, dataToDownload);
    const blob = new Blob([htmlContent], { type: 'text/html;charset=utf-8' });
    createDownloadLink(
      blob,
      `\u8d37\u6b3e\u7533\u8bf7\u8868_${customerName || '\u672a\u547d\u540d'}_${new Date().toISOString().split('T')[0]}.html`
    );
  };

  /**
   * Clear result and reset form
   */
  const handleClear = () => {
    stopPollingRef.current = true;
    setResult(null);
    setEditMode(false);
    setEditedData({});
    setApplicationResult(null);
    setApplicationTaskStatus('idle', null);
    reset();
  };

  const copyApplication = async () => {
    if (!result?.applicationContent) return;
    await navigator.clipboard.writeText(result.applicationContent);
    setCopySuccess(true);
    setTimeout(() => setCopySuccess(false), 1800);
  };

  // Determine which data to display (edited or original)
  const displayData = editMode ? editedData : (result?.applicationData || {});
  const hasStructuredData = Object.keys(displayData).length > 0;
  const applicationJobLabel = getJobTypeLabel('application_generate');
  const handleOpenCurrentApplicationJob = useCallback((job: ChatJobSummaryResponse) => {
    if (job.status === 'success') {
      resultSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return;
    }
    if (!jobPolling && job.customerName) {
      void pollApplicationJob(job.jobId, job.customerName);
    }
  }, [jobPolling, pollApplicationJob]);

  const applicationFeedback = useMemo<{
    tone: ProcessFeedbackTone;
    title: string;
    description: string;
    persistenceHint?: string;
    nextStep?: string;
  }>(() => {
    const isGenerating = loading || jobPolling;

    if (isGenerating) {
      return {
        tone: 'processing',
        title: getJobStatusText('application_generate', 'running'),
        description: '系统正在后台读取客户资料、资料汇总和申请表模板，请稍候。',
        persistenceHint: jobPolling ? '任务处理中' : '任务已提交',
        nextStep: '稍候片刻，生成完成后可直接查看、复制或下载申请表。',
      };
    }

    if (jobError || error) {
      return {
        tone: result ? 'partial' : 'error',
        title: result ? '生成异常，但上一版申请表仍可查看' : getJobStatusText('application_generate', 'failed'),
        description: result
          ? '本次生成没有完成，但上一版申请表仍保留在当前页面，可继续查看。'
          : jobError || error?.message || '本次没有成功生成申请表，请检查客户资料是否完整后再试一次。',
        persistenceHint: result ? '主流程已保留上一版结果' : '本次未生成新结果',
        nextStep: result ? '确认资料更新后重新生成，或先查看现有申请表。' : '先补齐客户资料，再重新生成申请表。',
      };
    }

    if (saveSuccess) {
      return {
        tone: 'success',
        title: '申请表已保存',
        description: '当前申请表修改已保存，后续页面切换后仍可继续使用。',
        persistenceHint: '保存成功',
        nextStep: '可继续导出申请表，或进入方案匹配使用最新申请表。',
      };
    }

    if (result?.metadata?.stale) {
      return {
        tone: 'partial',
        title: '当前申请表需要刷新',
        description: result.metadata.stale_reason || '检测到客户资料已更新，建议基于最新资料重新生成申请表。',
        persistenceHint: '旧结果仍可查看',
        nextStep: '请点击重新生成，确保申请表和最新客户资料保持一致。',
      };
    }

    if (result) {
      return {
        tone: 'success',
        title: result.customerFound ? getJobStatusText('application_generate', 'success') : '已生成可编辑模板',
        description: result.customerFound
          ? '系统已根据当前客户资料生成申请表，并同步写入当前上下文。'
          : '暂未找到完整客户资料，系统已提供可继续补充的申请表模板。',
        persistenceHint: result.customerFound ? '生成成功' : '主流程已生成模板',
        nextStep: result.customerFound ? '建议先核对字段，再保存、下载或进入方案匹配。' : '请先补充关键字段，再保存申请表。',
      };
    }

    return {
      tone: 'idle',
      title: customerName.trim() ? '已准备好生成申请表' : '等待开始生成申请表',
      description: customerName.trim()
        ? '当前客户信息已填写，可直接开始生成申请表。'
        : '请输入客户名称并选择贷款类型，系统会基于现有资料生成申请表。',
      persistenceHint: '待开始',
      nextStep: customerName.trim() ? '点击开始生成申请表即可进入下一步。' : '先填写客户名称，再开始生成申请表。',
    };
  }, [customerName, error, jobError, loading, jobPolling, result, saveSuccess]);

  return (
    <div data-testid="application-page" className="min-h-screen bg-slate-50 p-6">
      <section className="mb-6 rounded-[28px] border border-slate-200 bg-gradient-to-br from-white via-slate-50 to-blue-50/60 p-8 shadow-[0_18px_50px_rgba(15,23,42,0.06)]">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
          <div className="max-w-3xl">
            <div className="inline-flex items-center gap-2 rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-xs font-semibold text-blue-700">
              <FileText className="h-3.5 w-3.5" />
              {applicationJobLabel}
            </div>
            <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-900">申请表生成</h1>
            <p className="mt-3 text-sm leading-7 text-slate-600">基于当前客户资料、资料汇总和结构化提取内容生成贷款申请表，并保留版本信息供后续匹配与风控复用。</p>
          </div>
          <div className="grid min-w-[320px] gap-3 rounded-3xl border border-slate-200 bg-white/90 p-4 shadow-sm sm:grid-cols-2">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="text-xs font-medium text-slate-500">当前客户</div>
              <div className="mt-2 text-base font-semibold text-slate-900">{currentCustomerName || '未选择客户'}</div>
              <div className="mt-1 text-xs text-slate-500">{currentCustomerId ? '已绑定当前客户上下文' : '当前未绑定客户'}</div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-center justify-between">
                <div className="text-xs font-medium text-slate-500">当前状态</div>
                <div className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-medium ${result?.metadata?.stale ? 'bg-amber-100 text-amber-700' : result ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-600'}`}>
                  {result?.metadata?.stale ? '待刷新' : result ? '最新可用' : '未生成'}
                </div>
              </div>
              <div className="mt-2 text-base font-semibold text-slate-900">
                {result?.metadata?.stale ? '申请表待刷新' : result ? '申请表最新可用' : '尚未生成申请表'}
              </div>
              <div className="mt-1 text-xs text-slate-500">
                资料版本：{result?.metadata?.profile_version ? `V${result.metadata.profile_version}` : '未标记'}
              </div>
            </div>
          </div>
        </div>
      </section>

      <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm mb-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="text-sm font-semibold text-gray-800">当前客户上下文</div>
            <div className="mt-2 text-base font-semibold text-gray-900">
              {currentCustomerName || '未选择客户'}
            </div>
            <div className="mt-1 text-sm text-gray-500">
              {currentCustomerId
                ? '申请表会默认基于当前客户资料、资料汇总和版本信息生成。'
                : '如果你已经在其他页面选择了客户，这里可以一键带入当前客户。'}
            </div>
          </div>
          <div className="flex flex-wrap gap-3">
            <button
              type="button"
              onClick={() => {
                if (currentCustomerName) {
                  setCustomerName(currentCustomerName);
                }
              }}
              disabled={!currentCustomerName || loading || jobPolling}
              className="inline-flex items-center gap-2 px-4 py-2.5 border border-blue-200 bg-blue-50 text-blue-700 text-sm font-medium rounded-lg disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <User className="w-4 h-4" />
              带入当前客户
            </button>
            <div className="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-600">
              {currentCustomerId ? '已绑定当前客户' : '当前未绑定客户'}
            </div>
            <div className="inline-flex items-center rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700">
              {result?.metadata?.stale ? '申请表待刷新' : result ? '申请表最新可用' : '尚未生成申请表'}
            </div>
          </div>
        </div>
      </div>

      {/* Form Section - Requirement 7.1, 7.2 */}
      <div 
        data-testid="form-card"
        className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm mb-6"
        style={{ borderRadius: '12px' }}
      >
        <h2 className="text-gray-800 text-base font-semibold mb-4 flex items-center gap-2">
          <User className="w-4 h-4 text-blue-500" />
          {'客户信息'}
        </h2>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Customer Name Input - Requirement 7.1 */}
          <div>
            <label 
              htmlFor="customerName" 
              data-testid="customer-name-label"
              className="text-gray-600 text-sm mb-1.5 block font-medium"
            >
              {'客户名称'} <span className="text-red-500">*</span>
            </label>
            <div className="relative">
              <input
                id="customerName"
                data-testid="customer-name-input"
                type="text"
                value={customerName}
                onChange={(e) => setCustomerName(e.target.value)}
                placeholder="请输入企业名称或个人姓名"
                className="w-full pl-10 pr-4 py-2.5 border border-gray-300 text-sm text-gray-800 outline-none transition-all bg-white"
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
                disabled={loading || jobPolling}
                required
              />
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
            </div>
          </div>

          {/* Loan Type Selection - Requirement 7.2 */}
          <div>
            <label 
              htmlFor="loanType" 
              data-testid="loan-type-label"
              className="text-gray-600 text-sm mb-1.5 block font-medium"
            >
              {'贷款类型'}
            </label>
            <select
              id="loanType"
              data-testid="loan-type-select"
              value={loanType}
              onChange={(e) => setLoanType(e.target.value as LoanType)}
              className="w-full px-4 py-2.5 border border-gray-300 text-sm text-gray-800 outline-none transition-all bg-white appearance-none cursor-pointer"
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
              disabled={loading || jobPolling}
            >
              <option value="enterprise">{'企业贷款'}</option>
              <option value="personal">{'个人贷款'}</option>
            </select>
          </div>

          {/* Submit Button */}
          <div className="flex gap-3 pt-2">
            <button
              type="submit"
              data-testid="submit-button"
              disabled={loading || jobPolling || !customerName.trim()}
              className="flex items-center gap-2 px-6 py-2.5 bg-blue-500 text-white text-sm font-medium hover:bg-blue-600 transition-colors shadow-md disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ borderRadius: '8px' }}
            >
              {loading || jobPolling ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  {'正在生成...'}
                </>
              ) : (
                <>
                  <FileText className="w-4 h-4" />
                  {'开始生成申请表'}
                </>
              )}
            </button>

            {(loading || jobPolling) && (
              <button
                type="button"
                data-testid="cancel-button"
                onClick={handleCancel}
                className="px-4 py-2.5 border border-gray-300 text-gray-600 text-sm hover:bg-gray-50 transition-colors"
                style={{ borderRadius: '8px' }}
              >
                {'取消本次生成'}
              </button>
            )}
          </div>
        </form>
      </div>


      <ProcessFeedbackCard
        tone={applicationFeedback.tone}
        title={applicationFeedback.title}
        description={applicationFeedback.description}
        persistenceHint={applicationFeedback.persistenceHint}
        nextStep={applicationFeedback.nextStep}
        className="mb-6"
      />
      {activeJobCard ? (
        <div className="mb-6">
          <AsyncJobCard
            job={activeJobCard}
            variant="compact"
            isLatestCompleted={activeJobCard.status === 'success'}
            actionLabelOverride={activeJobCard.status === 'success' ? '查看当前结果' : undefined}
            onAction={(job) => handleOpenCurrentApplicationJob(job as ChatJobSummaryResponse)}
          />
        </div>
      ) : null}
      {/* Error Display */}
      {/* Requirement 4.6: Display error message if generation fails */}
      {error && (
        <div 
          data-testid="error-card"
          className="bg-red-50 border border-red-200 p-4 flex items-start gap-3 mb-6"
          style={{ borderRadius: '12px' }}
        >
          <XCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <span className="text-red-800 text-base font-semibold">{'申请表生成失败'}</span>
            <p className="text-red-600 text-sm mt-1">{error.message}</p>
          </div>
        </div>
      )}

      {/* Result Section - Requirement 7.3 */}
      {result && (
        <div ref={resultSectionRef}>
          {/* Status Banner */}
          {/* Requirement 4.4: Indicate whether customer data was found */}
          <div
            data-testid="status-banner"
            className={`p-4 flex items-center gap-3 mb-6 ${
              result.customerFound
                ? 'bg-green-50 border border-green-200'
                : 'bg-yellow-50 border border-yellow-200'
            }`}
            style={{ borderRadius: '12px' }}
          >
            {result.customerFound ? (
              <CheckCircle className="w-5 h-5 text-green-600" />
            ) : (
              <AlertTriangle className="w-5 h-5 text-yellow-600" />
            )}
            <div>
              <span
                data-testid="status-title"
                className={`text-base font-semibold ${
                  result.customerFound ? 'text-green-800' : 'text-yellow-800'
                }`}
              >
                {result.customerFound ? '申请表生成完成' : '申请表已生成（空白模板）'}
              </span>
              <span className="ml-3 inline-flex items-center rounded-full bg-white/80 px-2.5 py-1 text-xs font-medium text-slate-700">
                {result.metadata?.stale ? '待刷新' : '最新可用'}
              </span>
              <span
                data-testid="status-subtitle"
                className={`text-sm ml-2 ${
                  result.customerFound ? 'text-green-600' : 'text-yellow-600'
                }`}
              >
                {result.customerFound
                  ? '· 已结合当前客户资料自动填充'
                  : '· 暂未找到完整客户资料，可继续补充后保存'}
              </span>
            </div>
          </div>

          {/* Warnings Display */}
          {/* Requirement 4.5: Display warnings if any */}
          {result.warnings && result.warnings.length > 0 && (
            <div 
              data-testid="warnings-card"
              className="bg-amber-50 border border-amber-200 p-4 mb-6"
              style={{ borderRadius: '12px' }}
            >
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="w-4 h-4 text-amber-600" />
                <span className="text-amber-800 font-semibold text-sm">需要关注</span>
              </div>
              <ul className="list-disc list-inside space-y-1">
                {result.warnings.map((warning, index) => (
                  <li key={index} className="text-amber-700 text-sm">
                    {warning}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Application Content Card - Requirement 7.3 */}
          {/* Requirement 4.3: Display applicationContent in card format */}
          <div 
            data-testid="result-card"
            className="bg-white border border-gray-200 p-6 shadow-sm"
            style={{ borderRadius: '12px' }}
          >
            <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <h2 className="text-gray-800 text-base font-semibold flex items-center gap-2">
                <FileText className="w-4 h-4 text-blue-500" />
                {'申请表内容'}
                {editMode && (
                  <span className="text-xs text-blue-500 bg-blue-50 px-2 py-0.5 rounded">{'编辑中'}</span>
                )}
              </h2>
              <div className="flex flex-wrap gap-2">
                {/* Edit/Save Button - Combined functionality */}
                {hasStructuredData && (
                  editMode ? (
                    <button
                      data-testid="save-button"
                      onClick={saveEditedData}
                      disabled={saving}
                      className={`flex items-center gap-2 px-4 py-2 text-sm font-medium transition-colors shadow-md ${
                        saveSuccess 
                          ? 'bg-green-500 text-white' 
                          : 'bg-green-500 text-white hover:bg-green-600'
                      }`}
                      style={{ borderRadius: '8px' }}
                    >
                      {saving ? (
                        <>
                          <Loader2 className="w-4 h-4 animate-spin" />
                          正在保存...
                        </>
                      ) : saveSuccess ? (
                        <>
                          <CheckCircle className="w-4 h-4" />
                          已保存
                        </>
                      ) : (
                        <>
                          <Save className="w-4 h-4" />
                          保存修改
                        </>
                      )}
                    </button>
                  ) : (
                    <>
                      <button
                        data-testid="edit-button"
                        onClick={toggleEditMode}
                        className="flex items-center gap-2 rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                      >
                        <Edit3 className="w-4 h-4" />
                        编辑
                      </button>
                      <button
                        data-testid="save-to-cache-button"
                        onClick={saveEditedData}
                        disabled={saving}
                        className={`flex items-center gap-2 rounded-2xl px-4 py-2 text-sm font-medium transition-colors shadow-md ${
                          saveSuccess 
                            ? 'bg-green-500 text-white' 
                            : 'bg-orange-500 text-white hover:bg-orange-600'
                        }`}
                      >
                        {saving ? (
                          <>
                            <Loader2 className="w-4 h-4 animate-spin" />
                            正在保存...
                          </>
                        ) : saveSuccess ? (
                          <>
                            <CheckCircle className="w-4 h-4" />
                            已保存
                          </>
                        ) : (
                          <>
                            <Save className="w-4 h-4" />
                            保存申请表
                          </>
                        )}
                      </button>
                    </>
                  )
                )}
                {/* Download HTML Form Button */}
                {hasStructuredData && (
                  <button
                    type="button"
                    onClick={copyApplication}
                    className="flex items-center gap-2 rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                  >
                    <Copy className="w-4 h-4" />
                    {copySuccess ? '已复制申请表' : '复制申请表'}
                  </button>
                )}
                {hasStructuredData && (
                  <button
                    data-testid="download-form-button"
                    onClick={downloadFormHtml}
                    className="flex items-center gap-2 rounded-2xl border border-violet-200 bg-violet-50 px-4 py-2 text-sm font-medium text-violet-700 transition hover:bg-violet-100"
                  >
                    <Download className="w-4 h-4" />
                    导出表单
                  </button>
                )}
                {/* Download Markdown Button - Requirement 7.4 */}
                <button
                  data-testid="download-button"
                  onClick={downloadMarkdown}
                  className="flex items-center gap-2 rounded-2xl border border-blue-200 bg-blue-50 px-4 py-2 text-sm font-medium text-blue-700 transition hover:bg-blue-100"
                >
                  <Download className="w-4 h-4" />
                  导出文稿
                </button>
                <button
                  data-testid="clear-button"
                  onClick={handleClear}
                  className="rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-600 transition hover:bg-slate-50"
                >
                  清空当前结果
                </button>
              </div>
            </div>

            {/* Content Display */}
            {hasStructuredData ? (
              // Render as grouped cards using EditableDataSectionCard
              <div className="space-y-4" data-testid="application-structured-data">
                {Object.entries(displayData).map(([sectionName, sectionData]) => {
                  if (typeof sectionData === 'object' && sectionData !== null && !Array.isArray(sectionData)) {
                    return (
                      <EditableDataSectionCard 
                        key={sectionName} 
                        title={sectionName} 
                        data={sectionData as Record<string, string>}
                        editMode={editMode}
                        onFieldChange={handleFieldChange}
                      />
                    );
                  }
                  return null;
                })}
              </div>
            ) : (
              // Fallback to Markdown display
              <div className="prose prose-sm max-w-none">
                <pre 
                  data-testid="application-content"
                  className="bg-gray-50 p-4 overflow-auto text-sm text-gray-800 whitespace-pre-wrap font-mono"
                  style={{ borderRadius: '8px' }}
                >
                  {result.applicationContent}
                </pre>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Empty State */}
      {!result && !loading && !error && (
        <div 
          data-testid="empty-state"
          className="bg-white border border-gray-200 p-12 shadow-sm text-center"
          style={{ borderRadius: '12px' }}
        >
          <FileText className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <h3 className="text-gray-600 font-medium mb-2">{'当前还没有申请表结果'}</h3>
          <p className="text-gray-400 text-sm">{'确认客户资料后，点击“开始生成申请表”即可进入下一步。'}</p>
        </div>
      )}
    </div>
  );
};

export default ApplicationPage;



