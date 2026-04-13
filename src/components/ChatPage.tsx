/* eslint-disable react-refresh/only-export-components -- Exports utility functions and types alongside components for test access */
/**
 * ChatPage Component - AI Chat Interface
 * 
 * Provides an interactive chat interface for the loan assistant.
 * Integrates with the backend chat API and supports file attachments.
 * 
 * Feature: frontend-backend-integration, frontend-ui-optimization
 * Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7
 */

import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { 
  Send, Paperclip, X, FileText, Upload, ClipboardList, Target, Loader2, 
  FileSpreadsheet, Image, File, ChevronDown, ChevronRight,
  User, Building2, CreditCard, Banknote, AlertCircle, CheckCircle2,
  FileCheck, Percent, Calendar, DollarSign, Building,
  Edit3, Save, Download, RefreshCw
} from 'lucide-react';
import { createChatJob, createCustomerRiskReportJob, getChatJobStatus, listChatJobs, sendChat, clearCustomerCache, customerRagChat, getCustomerRiskReportHistory, listCustomers } from '../services/api';
import {
  getFieldIcon, getSectionIcon, formatTableValue, isNestedObject, isArrayOfObjects,
  DataSectionCard, ArrayDataCard
} from './DataDisplayComponents';
import AsyncJobCard from './common/AsyncJobCard';
import {
  getJobResultSummary,
  getJobSuccessAction,
  getJobTypeLabel,
  getReadableJobProgress,
} from '../config/jobDisplay';
import { useLoading } from '../hooks/useLoading';
import { useAbortController } from '../hooks/useAbortController';
import { useApp } from '../context/AppContext';
import ProcessFeedbackCard, { type ProcessFeedbackTone } from './common/ProcessFeedbackCard';
import type {
  ChatMessage,
  ChatFile,
  ChatJobSummaryResponse,
  ChatJobStatusResponse,
  ChatResponse,
  CustomerListItem,
  CustomerRiskReportJson,
  CustomerRiskReportHistoryItem,
  CustomerRagChatResponse,
  CustomerRiskReportResponse,
} from '../services/types';

// ============================================
// Utility Functions
// ============================================

/**
 * Convert a File to base64 encoded string
 * Feature: frontend-backend-integration, Property 11: File Base64 Encoding
 * 
 * @param file - The file to convert
 * @returns Promise resolving to base64 encoded string
 */
export function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result as string;
      // Remove the data URL prefix (e.g., "data:application/pdf;base64,")
      const base64 = result.split(',')[1] || '';
      resolve(base64);
    };
    reader.onerror = () => reject(new Error('Failed to read file'));
    reader.readAsDataURL(file);
  });
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

function formatCustomerContextLabel(customerId: string | null, customerName: string | null): string {
  if (customerName && customerName.trim()) {
    return customerName.trim();
  }
  if (!customerId) {
    return '未选择客户';
  }
  return customerId.replace(/^(enterprise_|personal_)/, '');
}

function formatLocalDateTime(value?: string | null): string {
  if (!value) {
    return '未记录';
  }
  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
}

function formatRiskLevelLabel(level: string | null | undefined): string {
  switch ((level || '').toLowerCase()) {
    case 'low':
      return '低风险';
    case 'medium':
      return '中风险';
    case 'high':
      return '高风险';
    default:
      return '待评估';
  }
}

function formatCustomerTypeLabel(type: string | null | undefined): string {
  switch ((type || '').toLowerCase()) {
    case 'enterprise':
      return '企业';
    case 'personal':
      return '个人';
    default:
      return type || '未说明';
  }
}

function formatCompletenessStatus(status: string | null | undefined): string {
  switch ((status || '').toLowerCase()) {
    case 'complete':
      return '完整';
    case 'partial':
      return '部分完整';
    case 'missing':
      return '缺失较多';
    default:
      return status || '待补充';
  }
}

function formatRiskDimensionLabel(dimension: string | null | undefined): string {
  switch ((dimension || '').toLowerCase()) {
    case 'subject_qualification':
      return '主体资质';
    case 'credit_and_debt':
      return '征信与负债';
    case 'business_stability':
      return '经营稳定性';
    case 'repayment_source':
      return '还款来源';
    case 'data_completeness':
      return '资料完整性';
    default:
      return dimension || '风险维度';
  }
}

function formatRecommendationActionLabel(action: string | null | undefined): string {
  switch ((action || '').toLowerCase()) {
    case 'apply_now':
      return '建议立即申请';
    case 'optimize_then_apply':
      return '建议优化后再申请';
    case 'supplement_documents':
      return '建议先补充资料';
    case 'alternative_financing':
      return '建议考虑替代融资路径';
    case 'observe_and_reassess':
      return '建议观察后再评估';
    default:
      return action || '建议补充资料后再判断';
  }
}

function formatProductTypeLabel(productType: string | null | undefined): string {
  switch ((productType || '').toLowerCase()) {
    case 'mortgage':
    case 'mortgage_loan':
      return '抵押类融资';
    case 'guarantee':
    case 'guarantee_loan':
      return '担保类融资';
    case 'supply_chain':
    case 'supply_chain_finance':
      return '供应链融资';
    case 'credit':
    case 'credit_loan':
      return '信用类融资';
    case 'invoice':
    case 'invoice_finance':
      return '发票融资';
    case 'tax':
    case 'tax_loan':
      return '税贷类融资';
    default:
      return productType || '待评估';
  }
}

function getRiskTone(level: string | null | undefined): string {
  switch ((level || '').toLowerCase()) {
    case 'low':
      return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    case 'medium':
      return 'bg-amber-50 text-amber-700 border-amber-200';
    case 'high':
      return 'bg-rose-50 text-rose-700 border-rose-200';
    default:
      return 'bg-slate-50 text-slate-700 border-slate-200';
  }
}

function getRiskBarTone(level: string | null | undefined): string {
  switch ((level || '').toLowerCase()) {
    case 'low':
      return 'bg-emerald-500';
    case 'medium':
      return 'bg-amber-500';
    case 'high':
      return 'bg-rose-500';
    default:
      return 'bg-slate-400';
  }
}

function buildApplicationFormHtml(
  customerName: string,
  loanType: string,
  applicationData: Record<string, Record<string, unknown>>
): string {
  const safeCustomerName = customerName.trim() || '未命名';
  const loanTypeLabel = loanType === 'personal' ? '个人贷款' : '企业贷款';
  const exportedAt = new Date().toLocaleString('zh-CN', { hour12: false });

  const sectionsHtml = Object.entries(applicationData)
    .map(([sectionName, sectionData]) => {
      const rows = Object.entries(sectionData)
        .map(([fieldName, value]) => {
          const renderedValue = escapeHtml(String(value ?? '-').trim() || '-').replace(/\r?\n/g, '<br />');
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
            <div class="section-count">${Object.keys(sectionData).length} 项</div>
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
    <title>贷款申请表 - ${escapeHtml(safeCustomerName)}</title>
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
        --chip: #dbeafe;
        --chip-text: #1d4ed8;
      }

      * { box-sizing: border-box; }

      body {
        margin: 0;
        font-family: "Microsoft YaHei", "PingFang SC", "Segoe UI", sans-serif;
        background: var(--bg);
        color: var(--text);
      }

      main {
        max-width: 1080px;
        margin: 0 auto;
        padding: 32px 24px 48px;
      }

      .hero {
        background: linear-gradient(135deg, #eff6ff 0%, #ffffff 100%);
        border: 1px solid var(--header-border);
        border-radius: 24px;
        padding: 28px;
        margin-bottom: 24px;
      }

      h1 {
        margin: 0 0 12px;
        font-size: 32px;
        line-height: 1.2;
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
        background: var(--chip);
        color: var(--chip-text);
        font-weight: 600;
      }

      .section-card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 20px;
        overflow: hidden;
        margin-bottom: 20px;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
      }

      .section-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        padding: 18px 20px;
        background: var(--header);
        border-bottom: 1px solid var(--header-border);
      }

      .section-title {
        font-size: 20px;
        font-weight: 700;
      }

      .section-count {
        color: var(--muted);
        font-size: 14px;
      }

      .table-shell {
        padding: 20px;
      }

      table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
      }

      th,
      td {
        border: 1px solid var(--border);
        padding: 12px 14px;
        text-align: left;
        vertical-align: top;
        word-break: break-word;
      }

      th {
        width: 32%;
        background: #f8fafc;
        color: var(--muted);
        font-weight: 600;
      }

      @media print {
        body { background: #fff; }
        main { padding: 0; }
        .hero,
        .section-card {
          box-shadow: none;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <h1>贷款申请表</h1>
        <div class="hero-meta">
          <span class="hero-chip">${escapeHtml(safeCustomerName)}</span>
          <span>贷款类型：${escapeHtml(loanTypeLabel)}</span>
          <span>导出时间：${escapeHtml(exportedAt)}</span>
        </div>
      </section>
      ${sectionsHtml}
    </main>
  </body>
</html>`;
}

// ============================================
// Sub-Components
// ============================================

// ============================================
// Design Tokens for ChatPage
// Feature: frontend-ui-optimization
// ============================================

/** AI Avatar styling - indigo background with bot icon */
const AI_AVATAR_STYLE = {
  size: 36,
  bgColor: '#6366F1',  // indigo-500
  iconColor: '#FFFFFF',
};

/** User message bubble styling */
const USER_MESSAGE_STYLE = {
  bgColor: '#3B82F6',  // blue-500
  textColor: '#FFFFFF',
  borderRadius: '18px 18px 4px 18px',
};

/** AI message bubble styling */
const AI_MESSAGE_STYLE = {
  bgColor: '#F3F4F6',  // gray-100
  textColor: '#1F2937',
  borderRadius: '18px 18px 18px 4px',
};

// ============================================
// Extended Message Type with Reasoning
// ============================================

/**
 * Extended chat message with optional reasoning and structured data
 */
interface ChatMessageWithReasoning extends ChatMessage {
  /** AI reasoning/thinking process */
  reasoning?: string | null;
  /** Detected intent for this message */
  intent?: ChatResponse['intent'];
  /** Structured data associated with the response */
  data?: Record<string, unknown> | null;
}

// ============================================
// Reasoning Collapse Component
// Feature: AI thinking process display
// ============================================

interface ReasoningCollapseProps {
  reasoning: string;
}

/**
 * ReasoningCollapse Component
 * 
 * Displays AI thinking/reasoning process in a collapsible section.
 * Default collapsed, click to expand.
 */
const ReasoningCollapse: React.FC<ReasoningCollapseProps> = ({ reasoning }) => {
  const [isExpanded, setIsExpanded] = useState(false);

  if (!reasoning || !reasoning.trim()) return null;

  return (
    <div className="mb-2 text-xs" data-testid="reasoning-collapse">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center gap-1 text-gray-500 transition-colors hover:text-gray-700"
        data-testid="reasoning-toggle"
      >
        {isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
        <span className="font-medium">思考过程</span>
      </button>
      {isExpanded && (
        <div
          className="mt-2 rounded-lg bg-gray-50 p-3 italic whitespace-pre-wrap text-gray-600"
          data-testid="reasoning-content"
        >
          {reasoning}
        </div>
      )}
    </div>
  );
};

// ============================================
// Structured Data Display Components
// Feature: Structured data cards for extract/application/matching intents
// ============================================

/**
 * Get icon for document type
 */
function getDocumentTypeIcon(documentType: string): React.ReactNode {
  const typeIcons: Record<string, React.ReactNode> = {
    '个人征信提取': <User className="w-4 h-4" />,
    '企业征信提取': <Building2 className="w-4 h-4" />,
    '个人流水提取': <Banknote className="w-4 h-4" />,
    '企业流水提取': <Banknote className="w-4 h-4" />,
    '财务数据提取': <FileSpreadsheet className="w-4 h-4" />,
    '抵押物信息提取': <Building className="w-4 h-4" />,
    '水母报告提取': <FileCheck className="w-4 h-4" />,
    '个人收入纳税/公积金': <CreditCard className="w-4 h-4" />,
  };
  return typeIcons[documentType] || <FileText className="w-4 h-4" />;
}

// getFieldIcon, getSectionIcon, formatTableValue, isNestedObject, isArrayOfObjects,
// DataTable, DataSectionCard, ArrayDataCard are imported from DataDisplayComponents



// ============================================
// Extraction Result Card Component
// ============================================

interface ExtractionFileResult {
  filename: string;
  documentType?: string;
  content?: Record<string, unknown>;
  customerName?: string | null;
  error?: string;
  saveError?: string;
  savedToFeishu?: boolean;
}

interface ExtractionResultCardProps {
  files: ExtractionFileResult[];
}

// getSectionIcon moved to DataDisplayComponents

// formatTableValue, isNestedObject, isArrayOfObjects moved to DataDisplayComponents

// DataTable, DataSectionCard, ArrayDataCard moved to DataDisplayComponents

/**
 * ExtractionResultCard Component
 * 
 * Displays extraction results from uploaded files in a grouped card format.
 * Shows document type, customer name, and all extracted fields organized by category.
 * Each category is displayed as a separate card with a table layout.
 */
const ExtractionResultCard: React.FC<ExtractionResultCardProps> = ({ files }) => {
  if (!files || files.length === 0) return null;
  
  return (
    <div className="mt-3 space-y-4" data-testid="extraction-result-card">
      {files.map((file, index) => (
        <div 
          key={`${file.filename}-${index}`}
          className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm"
          data-testid={`extraction-file-${index}`}
        >
          {/* File Header */}
          <div className="px-4 py-3 bg-gradient-to-r from-blue-50 to-indigo-50 border-b border-gray-100">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {/* Document Type Icon */}
                <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                  file.error ? 'bg-red-100 text-red-600' : 'bg-blue-100 text-blue-600'
                }`}>
                  {file.error ? (
                    <AlertCircle className="w-5 h-5" />
                  ) : (
                    getDocumentTypeIcon(file.documentType || '')
                  )}
                </div>
                <div>
                  <div className="font-medium text-gray-800 text-sm">{file.filename}</div>
                  {file.documentType && !file.error && (
                    <div className="text-xs text-gray-500 mt-0.5">{file.documentType}</div>
                  )}
                </div>
              </div>
              {/* Status Badge */}
              {file.error ? (
                <span className="px-2.5 py-1 bg-red-100 text-red-600 text-xs rounded-full font-medium">
                  处理失败
                </span>
              ) : file.saveError ? (
                <span className="px-2.5 py-1 bg-amber-100 text-amber-700 text-xs rounded-full font-medium">
                  已提取未保存
                </span>
              ) : (
                <span className="px-2.5 py-1 bg-green-100 text-green-600 text-xs rounded-full font-medium flex items-center gap-1">
                  <CheckCircle2 className="w-3 h-3" />
                  提取成功
                </span>
              )}
            </div>
          </div>
          
          {/* Error Message */}
          {file.error && (
            <div className="px-4 py-3 bg-red-50 text-red-600 text-sm">
              {file.error}
            </div>
          )}

          {file.saveError && !file.error && (
            <div className="px-4 py-3 bg-amber-50 text-amber-700 text-sm">
              {file.saveError}
            </div>
          )}
          
          {/* Content - Grouped Cards */}
          {!file.error && file.content && (
            <div className="p-4 space-y-4">
              {/* Customer Name Banner */}
              {file.customerName && (
                <div className="flex items-center gap-2 px-3 py-2 bg-indigo-50 rounded-lg border border-indigo-100">
                  <User className="w-4 h-4 text-indigo-500" />
                  <span className="text-sm text-indigo-600">客户名称：</span>
                  <span className="text-sm font-semibold text-indigo-800">{file.customerName}</span>
                </div>
              )}
              
              {/* Data Sections - Group by top-level keys */}
              {Object.entries(file.content).map(([key, value]) => {
                // 如果是嵌套对象，渲染为独立卡片
                if (isNestedObject(value)) {
                  return (
                    <DataSectionCard 
                      key={key} 
                      title={key} 
                      data={value as Record<string, unknown>} 
                    />
                  );
                }
                // 如果是对象数组，渲染为表格卡片
                if (isArrayOfObjects(value)) {
                  return (
                    <ArrayDataCard 
                      key={key} 
                      title={key} 
                      data={value as Array<Record<string, unknown>>} 
                    />
                  );
                }
                // 简单值会被收集到"其他信息"卡片中
                return null;
              })}
              
              {/* Collect simple top-level values into "其他信息" card */}
              {(() => {
                const simpleEntries = Object.entries(file.content).filter(
                  ([, value]) => !isNestedObject(value) && !isArrayOfObjects(value)
                );
                if (simpleEntries.length === 0) return null;
                return (
                  <DataSectionCard 
                    title="其他信息" 
                    data={Object.fromEntries(simpleEntries)} 
                  />
                );
              })()}
            </div>
          )}
        </div>
      ))}
    </div>
  );
};

// ============================================
// Application Guide Card Component
// ============================================

interface ApplicationGuideCardProps {
  data: {
    action?: string;
    requiredFields?: string[];
  };
  onNavigate?: (page: string) => void;
}

/**
 * ApplicationGuideCard Component
 * 
 * Displays guidance for application generation with required fields.
 */
const ApplicationGuideCard: React.FC<ApplicationGuideCardProps> = ({ data, onNavigate }) => {
  const fieldLabels: Record<string, string> = {
    customerName: '客户名称',
    loanType: '贷款类型',
  };
  
  return (
    <div 
      className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm"
      data-testid="application-guide-card"
    >
      <div className="px-4 py-3 bg-gradient-to-r from-amber-50 to-orange-50 border-b border-gray-100">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-amber-100 text-amber-600 flex items-center justify-center">
            <ClipboardList className="w-5 h-5" />
          </div>
          <div>
            <div className="font-medium text-gray-800 text-sm">申请表生成</div>
            <div className="text-xs text-gray-500 mt-0.5">请提供以下信息</div>
          </div>
        </div>
      </div>
      
      <div className="px-4 py-3">
        <div className="space-y-2">
          {data.requiredFields?.map((field) => (
            <div key={field} className="flex items-center gap-2 text-sm">
              <div className="w-5 h-5 rounded-full bg-amber-100 text-amber-600 flex items-center justify-center text-xs">
                {data.requiredFields?.indexOf(field) !== undefined ? data.requiredFields.indexOf(field) + 1 : '•'}
              </div>
              <span className="text-gray-700">{fieldLabels[field] || field}</span>
            </div>
          ))}
        </div>
        
        {onNavigate && (
          <button
            onClick={() => onNavigate('application')}
            className="mt-4 w-full py-2.5 bg-amber-500 hover:bg-amber-600 text-white rounded-lg text-sm font-medium transition-colors flex items-center justify-center gap-2"
          >
            <ClipboardList className="w-4 h-4" />
            前往申请表生成
          </button>
        )}
      </div>
    </div>
  );
};

// ============================================
// Matching Guide Card Component
// ============================================

interface MatchingGuideCardProps {
  data: {
    action?: string;
    requiredFields?: string[];
  };
  onNavigate?: (page: string) => void;
}

/**
 * MatchingGuideCard Component
 * 
 * Displays guidance for scheme matching with required information.
 */
const MatchingGuideCard: React.FC<MatchingGuideCardProps> = ({ data: _data, onNavigate }) => {
  
  const infoItems = [
    { icon: <CreditCard className="w-4 h-4" />, label: '征信情况' },
    { icon: <Banknote className="w-4 h-4" />, label: '流水情况' },
    { icon: <Building className="w-4 h-4" />, label: '资产情况' },
  ];
  
  return (
    <div 
      className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm"
      data-testid="matching-guide-card"
    >
      <div className="px-4 py-3 bg-gradient-to-r from-emerald-50 to-teal-50 border-b border-gray-100">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-emerald-100 text-emerald-600 flex items-center justify-center">
            <Target className="w-5 h-5" />
          </div>
          <div>
            <div className="font-medium text-gray-800 text-sm">方案匹配</div>
            <div className="text-xs text-gray-500 mt-0.5">请提供客户基本信息</div>
          </div>
        </div>
      </div>
      
      <div className="px-4 py-3">
        <div className="space-y-2">
          {infoItems.map((item, index) => (
            <div key={index} className="flex items-center gap-2 text-sm">
              <div className="w-6 h-6 rounded-full bg-emerald-100 text-emerald-600 flex items-center justify-center">
                {item.icon}
              </div>
              <span className="text-gray-700">{item.label}</span>
            </div>
          ))}
        </div>
        
        {onNavigate && (
          <button
            onClick={() => onNavigate('matching')}
            className="mt-4 w-full py-2.5 bg-emerald-500 hover:bg-emerald-600 text-white rounded-lg text-sm font-medium transition-colors flex items-center justify-center gap-2"
          >
            <Target className="w-4 h-4" />
            前往方案匹配
          </button>
        )}
      </div>
    </div>
  );
};

// ============================================
// Application Result Card Component
// ============================================

interface ApplicationResultCardProps {
  data: {
    customerFound?: boolean;
    customerName?: string;
    loanType?: string;
    applicationData?: Record<string, Record<string, unknown>>;  // JSON structured data
    applicationContent?: string;  // Markdown fallback
    warnings?: string[];
    metadata?: {
      generated_at?: string;
      customer_id?: string;
      profile_version?: number;
      profile_updated_at?: string;
      data_sources?: string[];
      stale?: boolean;
      stale_reason?: string;
      stale_at?: string;
    };
    needsInput?: boolean;
    requiredFields?: string[];
  };
  onNavigate?: (page: string) => void;
}

function buildChatApplicationFieldSource(fieldName: string, value: unknown, metadata?: ApplicationResultCardProps['data']['metadata']) {
  const text = String(value ?? '').trim();
  const profileVersion = metadata?.profile_version ? `资料汇总 V${metadata.profile_version}` : '当前资料汇总';
  const profileTime = metadata?.profile_updated_at
    ? `，最近更新于 ${formatLocalDateTime(metadata.profile_updated_at)}`
    : '';

  if (!text || text === '-' || /待补充|无/.test(text)) {
    return {
      label: '待补字段',
      detail: `当前客户资料中未找到可直接引用的内容，建议补充材料后重新生成。本次判断基于 ${profileVersion}${profileTime}。`,
    };
  }

  if (/经营地址|注册地址|经营状态|统一社会信用代码|行业类型|成立时间/.test(fieldName)) {
    return {
      label: '企业征信 / 资料汇总',
      detail: `主要来自企业征信报告基本信息和资料汇总，本次生成基于 ${profileVersion}${profileTime}。`,
    };
  }

  if (/纳税|开票|营收|利润|财务|流水|收入|回款/.test(fieldName)) {
    return {
      label: '财务 / 纳税 / 流水资料',
      detail: `主要来自财务数据、纳税资料、银行流水和经营类报告，本次生成基于 ${profileVersion}${profileTime}。`,
    };
  }

  if (/征信|负债|逾期|担保|诉讼|信用卡|隐形负债/.test(fieldName)) {
    return {
      label: '征信 / 负债资料',
      detail: `主要来自企业征信、个人征信、公共记录和负债资料；如字段可计算，系统会自动综合推导。本次生成基于 ${profileVersion}${profileTime}。`,
    };
  }

  if (/抵押|资产|存货|固定资产|净资产/.test(fieldName)) {
    return {
      label: '资产 / 抵押材料',
      detail: `主要来自抵押物资料、资产清单和补充材料，本次生成基于 ${profileVersion}${profileTime}。`,
    };
  }

  return {
    label: '资料汇总 / 结构化提取',
    detail: `系统综合读取资料汇总与结构化提取结果生成该字段，本次依据为 ${profileVersion}${profileTime}。`,
  };
}

/**
 * EditableDataSectionCardChat Component
 * 
 * Renders a section with title and data table for ChatPage.
 * In edit mode, field values become editable inputs.
 */
interface EditableDataSectionCardChatProps {
  title: string;
  data: Record<string, unknown>;
  editMode: boolean;
  onFieldChange: (sectionTitle: string, fieldName: string, value: string) => void;
  metadata?: ApplicationResultCardProps['data']['metadata'];
}

const EditableDataSectionCardChat: React.FC<EditableDataSectionCardChatProps> = ({ 
  title, 
  data, 
  editMode, 
  onFieldChange,
  metadata,
}) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [expandedSourceKey, setExpandedSourceKey] = useState<string | null>(null);
  const entries = Object.entries(data).filter(
    ([, value]) => typeof value !== 'object' || value === null
  );
  const nestedEntries = Object.entries(data).filter(
    ([, value]) => typeof value === 'object' && value !== null && !Array.isArray(value)
  );
  
  if (entries.length === 0 && nestedEntries.length === 0) return null;
  
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
            <span className="font-medium text-gray-700 text-sm">{title}</span>
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
        <div className="p-3 space-y-3">
          {/* Simple key-value pairs */}
          {entries.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-gray-200">
              <table className="w-full text-sm">
                <tbody>
                  {entries.map(([key, value], idx) => {
                    const sourceInfo = buildChatApplicationFieldSource(key, value, metadata);
                    const rowKey = `${title}-${key}`;
                    const showSourceDetail = expandedSourceKey === rowKey;
                    return (
                    <tr 
                      key={key} 
                      className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}
                    >
                      <td className="px-3 py-2 text-gray-500 font-medium w-1/3 border-r border-gray-100">
                        <div className="flex items-center gap-2">
                          <span className="text-gray-400">{getFieldIcon(key)}</span>
                          <span className="truncate">{key}</span>
                        </div>
                      </td>
                      <td className="px-3 py-2 text-gray-800">
                        {editMode ? (
                          <input
                            type="text"
                            value={String(value ?? '')}
                            onChange={(e) => onFieldChange(title, key, e.target.value)}
                            className="w-full px-2 py-1 border border-blue-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-200"
                            data-testid={`edit-field-${title}-${key}`}
                          />
                        ) : (
                          <div>
                            <div className="break-words" title={String(value ?? '')}>
                              {formatTableValue(value)}
                            </div>
                            <div className="mt-2 flex flex-wrap items-center gap-2">
                              <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-slate-600">
                                来源：{sourceInfo.label}
                              </span>
                              <button
                                type="button"
                                onClick={() => setExpandedSourceKey((prev) => (prev === rowKey ? null : rowKey))}
                                className="text-[11px] font-medium text-blue-600 hover:text-blue-700"
                              >
                                {showSourceDetail ? '收起来源' : '查看来源'}
                              </button>
                            </div>
                            {showSourceDetail && (
                              <div className="mt-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs leading-5 text-slate-600">
                                {sourceInfo.detail}
                              </div>
                            )}
                          </div>
                        )}
                      </td>
                    </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          
          {/* Nested objects as sub-cards */}
          {nestedEntries.map(([key, value]) => (
            <EditableDataSectionCardChat 
              key={key} 
              title={key} 
              data={value as Record<string, unknown>}
              editMode={editMode}
              onFieldChange={onFieldChange}
              metadata={metadata}
            />
          ))}
        </div>
      )}
    </div>
  );
};

/**
 * ApplicationResultCard Component
 * 
 * Displays generated application form content.
 * - If applicationData (JSON) is available, renders as grouped cards with edit support
 * - Falls back to Markdown rendering if only applicationContent is available
 * Shows customer info and warnings if any.
 */
const ApplicationResultCard: React.FC<ApplicationResultCardProps> = ({ data, onNavigate }) => {
  const { state } = useApp();
  const [isExpanded, setIsExpanded] = useState(true);
  const [editMode, setEditMode] = useState(false);
  const [editedData, setEditedData] = useState<Record<string, Record<string, unknown>>>({});
  // savedData 持久化已保存的编辑内容，退出编辑后仍显示最新数据
  const [savedData, setSavedData] = useState<Record<string, Record<string, unknown>> | null>(null);
  
  /**
   * Handle field change in edit mode
   * 使用 useCallback 避免闭包陷阱（踩坑点 #31）
   * Note: Must be declared before any early returns to satisfy rules-of-hooks
   */
  const handleFieldChange = useCallback((sectionTitle: string, fieldName: string, value: string) => {
    setEditedData(prev => ({
      ...prev,
      [sectionTitle]: {
        ...(prev[sectionTitle] || {}),
        [fieldName]: value,
      },
    }));
  }, []);
  
  // If needs input, show the guide card instead
  if (data.needsInput) {
    return <ApplicationGuideCard data={data} onNavigate={onNavigate} />;
  }
  
  // If no application data at all, show guide card
  if (!data.applicationData && !data.applicationContent) {
    return <ApplicationGuideCard data={data} onNavigate={onNavigate} />;
  }
  
  const loanTypeLabel = data.loanType === 'personal' ? '个人贷款' : '企业贷款';
  const hasStructuredData = data.applicationData && Object.keys(data.applicationData).length > 0;
  const profileVersionLabel = data.metadata?.profile_version ? `V${data.metadata.profile_version}` : '版本待确认';
  const profileUpdatedAtLabel = data.metadata?.profile_updated_at
    ? formatLocalDateTime(data.metadata.profile_updated_at)
    : '未记录';
  const generatedAtLabel = data.metadata?.generated_at
    ? formatLocalDateTime(data.metadata.generated_at)
    : '刚刚生成';
  const currentApplicationMetadata = state.application.result?.metadata;
  const sameCustomerStale =
    Boolean(currentApplicationMetadata?.stale) &&
    Boolean(currentApplicationMetadata?.customer_id) &&
    currentApplicationMetadata?.customer_id === data.metadata?.customer_id;
  const staleReason = currentApplicationMetadata?.stale_reason || data.metadata?.stale_reason || '客户资料已更新，请重新生成申请表。';
  const staleAtLabel = currentApplicationMetadata?.stale_at
    ? formatLocalDateTime(currentApplicationMetadata.stale_at)
    : data.metadata?.stale_at
      ? formatLocalDateTime(data.metadata.stale_at)
      : '';
  const applicationStatusBadge = sameCustomerStale
    ? { label: '待刷新', className: 'border-amber-200 bg-amber-50 text-amber-700' }
    : { label: '最新可用', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
  
  // Determine which data to display: savedData > editMode editedData > original
  const displayData = savedData
    ? savedData
    : (editMode && Object.keys(editedData).length > 0
      ? editedData
      : (data.applicationData || {}));
  
  /**
   * Toggle edit mode - 进入编辑时优先从 savedData 初始化，其次从原始数据
   */
  const toggleEditMode = () => {
    if (!editMode) {
      // Entering edit mode - initialize from savedData or original data
      const baseData = savedData || data.applicationData;
      if (baseData) {
        setEditedData(baseData);
      }
    }
    setEditMode(!editMode);
  };
  
  /**
   * Save edited data - 持久化到 savedData，退出编辑后仍显示最新内容
   */
  const saveEditedData = () => {
    if (Object.keys(editedData).length > 0) {
      setSavedData(editedData);
    }
    setEditMode(false);
  };
  
  /**
   * Download application as .json file
   */
  const downloadJSON = () => {
    const dataToDownload = savedData || (Object.keys(editedData).length > 0 ? editedData : data.applicationData);
    if (!dataToDownload || Object.keys(dataToDownload).length === 0) return;

    const jsonContent = JSON.stringify(dataToDownload, null, 2);
    const blob = new Blob([jsonContent], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `贷款申请表_${data.customerName || '未命名'}_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  void downloadJSON;

  const downloadFormHtml = () => {
    const dataToDownload = savedData || (Object.keys(editedData).length > 0 ? editedData : data.applicationData);
    if (!dataToDownload || Object.keys(dataToDownload).length === 0) return;

    const htmlContent = buildApplicationFormHtml(
      data.customerName || '',
      data.loanType || 'enterprise',
      dataToDownload
    );
    const blob = new Blob([htmlContent], { type: 'text/html;charset=utf-8' });
    createDownloadLink(
      blob,
      `贷款申请表_${data.customerName || '未命名'}_${new Date().toISOString().split('T')[0]}.html`
    );
  };
  
  return (
    <div 
      className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm"
      data-testid="application-result-card"
    >
      {/* Header */}
      <div className="px-4 py-3 bg-gradient-to-r from-amber-50 to-orange-50 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
              data.customerFound ? 'bg-green-100 text-green-600' : 'bg-amber-100 text-amber-600'
            }`}>
              {data.customerFound ? (
                <CheckCircle2 className="w-5 h-5" />
              ) : (
                <ClipboardList className="w-5 h-5" />
              )}
            </div>
            <div>
              <div className="font-medium text-gray-800 text-sm">
                {data.customerFound ? '申请表已生成' : '空白申请表模板'}
                <span className={`ml-2 inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${applicationStatusBadge.className}`}>
                  {applicationStatusBadge.label}
                </span>
                {editMode && (
                  <span className="ml-2 text-xs text-blue-500 bg-blue-50 px-2 py-0.5 rounded">编辑中</span>
                )}
              </div>
              <div className="text-xs text-gray-500 mt-0.5">
                {data.customerName && `客户：${data.customerName}`}
                {data.customerName && ' · '}
                {loanTypeLabel}
              </div>
              <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-slate-500">
                <span>生成时间：{generatedAtLabel}</span>
                <span>资料汇总版本：{profileVersionLabel}</span>
                <span>资料更新时间：{profileUpdatedAtLabel}</span>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {/* Edit/Save Button */}
            {hasStructuredData && (
              editMode ? (
                <button
                  onClick={saveEditedData}
                  className="flex items-center gap-1 px-2.5 py-1.5 bg-green-500 text-white text-xs font-medium hover:bg-green-600 transition-colors rounded-lg"
                  data-testid="save-button"
                >
                  <Save className="w-3.5 h-3.5" />
                  保存
                </button>
              ) : (
                <button
                  onClick={toggleEditMode}
                  className="flex items-center gap-1 px-2.5 py-1.5 bg-gray-100 text-gray-700 text-xs font-medium hover:bg-gray-200 transition-colors rounded-lg"
                  data-testid="edit-button"
                >
                  <Edit3 className="w-3.5 h-3.5" />
                  编辑
                </button>
              )
            )}
            {/* Download form button */}
            {hasStructuredData && (
              <button
                onClick={downloadFormHtml}
                className="flex items-center gap-1 px-2.5 py-1.5 bg-purple-500 text-white text-xs font-medium hover:bg-purple-600 transition-colors rounded-lg"
                data-testid="download-form-button"
              >
                <Download className="w-3.5 h-3.5" />
                下载表单
              </button>
            )}
            {/* Expand/Collapse Button */}
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="p-1.5 hover:bg-amber-100 rounded-lg transition-colors"
            >
              {isExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-500" />
              )}
            </button>
          </div>
        </div>
      </div>
      
      {/* Warnings */}
      {data.warnings && data.warnings.length > 0 && (
        <div className="px-4 py-2 bg-yellow-50 border-b border-yellow-100">
          {data.warnings.map((warning, index) => (
            <div key={index} className="flex items-center gap-2 text-xs text-yellow-700">
              <AlertCircle className="w-3.5 h-3.5" />
              <span>{warning}</span>
            </div>
          ))}
        </div>
      )}

      <div className="px-4 py-3 border-b border-slate-100 bg-slate-50">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg bg-white px-3 py-2">
            <div className="text-[11px] text-slate-500">生成时间</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{generatedAtLabel}</div>
          </div>
          <div className="rounded-lg bg-white px-3 py-2">
            <div className="text-[11px] text-slate-500">资料汇总版本</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{profileVersionLabel}</div>
          </div>
          <div className="rounded-lg bg-white px-3 py-2">
            <div className="text-[11px] text-slate-500">资料汇总更新时间</div>
            <div className="mt-1 text-sm font-semibold text-slate-800">{profileUpdatedAtLabel}</div>
          </div>
        </div>
      </div>

      {sameCustomerStale && (
        <div className="border-b border-amber-100 bg-amber-50/80 px-4 py-4">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-sm font-semibold text-amber-900">这份申请表已被新上传资料覆盖</div>
              <div className="mt-1 text-sm text-amber-800">{staleReason}</div>
              <div className="mt-1 text-xs text-amber-700">
                {staleAtLabel ? `失效时间：${staleAtLabel}` : '请重新生成后再用于方案匹配或后续沟通。'}
              </div>
            </div>
            <button
              type="button"
              onClick={() => onNavigate?.('application')}
              className="inline-flex items-center justify-center rounded-xl bg-amber-500 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-amber-600"
            >
              去申请表页重新生成
            </button>
          </div>
        </div>
      )}
      
      {/* Content */}
      {isExpanded && (
        <div className="p-4">
          {hasStructuredData ? (
            // Render as grouped cards using EditableDataSectionCardChat
            <div className="space-y-4" data-testid="application-structured-data">
              {Object.entries(displayData).map(([sectionName, sectionData]) => {
                if (typeof sectionData === 'object' && sectionData !== null && !Array.isArray(sectionData)) {
                  return (
                    <EditableDataSectionCardChat 
                      key={sectionName} 
                      title={sectionName} 
                      data={sectionData as Record<string, unknown>}
                      editMode={editMode}
                      onFieldChange={handleFieldChange}
                      metadata={data.metadata}
                    />
                  );
                }
                return null;
              })}
            </div>
          ) : (
            // Fallback to Markdown rendering
            <div 
              className="prose prose-sm max-w-none text-gray-700 overflow-x-auto prose-table:border-collapse prose-th:border prose-th:border-gray-300 prose-th:bg-gray-100 prose-th:px-3 prose-th:py-2 prose-td:border prose-td:border-gray-300 prose-td:px-3 prose-td:py-2"
              data-testid="application-markdown-content"
            >
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {data.applicationContent || ''}
              </ReactMarkdown>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ============================================
// Matching Result Card Component
// ============================================

interface MatchingResultCardProps {
  data: {
    customerFound?: boolean;
    customerName?: string;
    creditType?: string;
    matchingData?: {  // JSON structured data (new)
      核心发现?: Record<string, string>;
      客户资料摘要?: Record<string, string>;
      待补充资料?: {
        必须补充?: string[];
        建议补充?: string[];
      };
      推荐方案?: MatchingSchemeItem[];
      不推荐产品?: Array<Record<string, string>>;
      下一步建议?: string;
      准备材料?: Record<string, string[]>;
      审批流程?: Array<{ 步骤: string; 内容: string; 预计时间: string }>;
    };
    matchResult?: string;  // Markdown fallback
    needsInput?: boolean;
    requiredFields?: string[];
  };
  onNavigate?: (page: string) => void;
}

/**
 * MatchingDataSectionCard Component
 * 
 * Renders a section of matching data as a card with table layout.
 */
interface MatchingDataSectionCardProps {
  title: string;
  data: Record<string, unknown>;
  icon?: React.ReactNode;
  iconBgColor?: string;
}

const MatchingDataSectionCard: React.FC<MatchingDataSectionCardProps> = ({ 
  title, 
  data, 
  icon,
  iconBgColor = 'bg-emerald-100 text-emerald-600'
}) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const entries = Object.entries(data).filter(
    ([, value]) => typeof value !== 'object' || value === null
  );
  
  if (entries.length === 0) return null;
  
  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      {/* Section Header */}
      <div 
        className="px-3 py-2 bg-gradient-to-r from-emerald-50 to-teal-50 border-b border-gray-100 cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className={`w-7 h-7 rounded-md flex items-center justify-center ${iconBgColor}`}>
              {icon || <Target className="w-4 h-4" />}
            </div>
            <span className="font-medium text-gray-700 text-sm">{title}</span>
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
                      <span className="truncate">{key}</span>
                    </td>
                    <td className="px-3 py-2 text-gray-800">
                      <span className="break-words">
                        {formatTableValue(value)}
                      </span>
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

/**
 * MatchingSupplementCard Component
 * 
 * Renders the "待补充资料" section with lists.
 */
interface MatchingSupplementCardProps {
  data: {
    必须补充?: string[];
    建议补充?: string[];
  };
}

const MatchingSupplementCard: React.FC<MatchingSupplementCardProps> = ({ data }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const requiredItems = data.必须补充 || [];
  const suggestedItems = data.建议补充 || [];
  
  if (requiredItems.length === 0 && suggestedItems.length === 0) return null;
  
  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      {/* Section Header */}
      <div 
        className="px-3 py-2 bg-gradient-to-r from-amber-50 to-orange-50 border-b border-gray-100 cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-md flex items-center justify-center bg-amber-100 text-amber-600">
              <AlertCircle className="w-4 h-4" />
            </div>
            <span className="font-medium text-gray-700 text-sm">待补充资料</span>
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
        <div className="p-3 space-y-3">
          {/* Required Items */}
          {requiredItems.length > 0 && (
            <div>
              <div className="text-xs font-medium text-red-600 mb-2 flex items-center gap-1">
                <span className="w-2 h-2 bg-red-500 rounded-full"></span>
                必须补充
              </div>
              <div className="flex flex-wrap gap-2">
                {requiredItems.map((item, idx) => (
                  <span 
                    key={idx}
                    className="px-2.5 py-1 bg-red-50 text-red-700 text-xs rounded-full border border-red-200"
                  >
                    {item}
                  </span>
                ))}
              </div>
            </div>
          )}
          
          {/* Suggested Items */}
          {suggestedItems.length > 0 && (
            <div>
              <div className="text-xs font-medium text-amber-600 mb-2 flex items-center gap-1">
                <span className="w-2 h-2 bg-amber-500 rounded-full"></span>
                建议补充
              </div>
              <div className="flex flex-wrap gap-2">
                {suggestedItems.map((item, idx) => (
                  <span 
                    key={idx}
                    className="px-2.5 py-1 bg-amber-50 text-amber-700 text-xs rounded-full border border-amber-200"
                  >
                    {item}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ============================================
// Markdown to Structured Data Parser
// Feature: Parse AI matching result markdown to structured data
// ============================================

/**
 * Parsed scheme from markdown
 */
interface ParsedScheme {
  方案名称: string;
  银行名称?: string;
  产品名称?: string;
  可贷额度?: string;
  参考利率?: string;
  贷款期限?: string;
  还款方式?: string;
  准入条件?: string[];
  [key: string]: string | string[] | undefined;
}

/**
 * Parsed matching result from markdown
 */
interface ParsedMatchingResult {
  客户资料摘要?: Record<string, string>;
  推荐方案?: ParsedScheme[];
  不推荐产品?: Array<{ 产品: string; 原因: string }>;
  替代建议?: string[];
  需补充信息?: string[];
  准备材料?: Record<string, string[]>;
  审批流程?: Array<{ 步骤: string; 内容: string; 预计时间: string }>;
  rawMarkdown?: string;
}

/**
 * Parse markdown matching result to structured data
 * 
 * Extracts:
 * - 客户资料摘要 (table format)
 * - 推荐方案 (#### 方案N: format)
 * - 不推荐产品 (table format)
 * - 替代建议 (list format)
 * - 需补充信息 (list format)
 * 
 * @param markdown - The markdown content from AI
 * @returns Parsed structured data or null if parsing fails
 */
function parseMarkdownToSchemes(markdown: string): ParsedMatchingResult | null {
  if (!markdown || typeof markdown !== 'string') {
    return null;
  }

  const result: ParsedMatchingResult = {
    rawMarkdown: markdown,
  };

  try {
    // Debug: log first 500 chars to see actual format
    console.warn('[parseMarkdownToSchemes] Input preview:', markdown.substring(0, 500));
    
    // 1. Parse 客户资料摘要 (table format)
    // Very flexible pattern: match any line containing "客户资料摘要" followed by a table
    const summaryMatch = markdown.match(/(?:.*)?客户资料摘要[^\n]*\n([\s\S]*?\|[\s\S]*?)(?=\n(?:二、|###|🚨|$))/i);
    if (summaryMatch) {
      const tableRows = summaryMatch[0].match(/\|([^|]+)\|([^|]+)\|/g);
      if (tableRows && tableRows.length > 1) {
        const summary: Record<string, string> = {};
        tableRows.slice(1).forEach(row => {
          // Skip header separator row (|---|---|)
          if (row.includes('---')) return;
          const cells = row.split('|').filter(c => c.trim());
          if (cells.length >= 2) {
            const key = cells[0].trim();
            const value = cells[1].trim();
            if (key && value && key !== '项目' && key !== '内容') {
              summary[key] = value;
            }
          }
        });
        if (Object.keys(summary).length > 0) {
          result.客户资料摘要 = summary;
        }
      }
    }

    // 2. Parse 推荐方案 (multiple formats supported)
    // Format 1: #### 方案1：【华瑞银行】线上抵押贷
    // Format 2: ### 方案1：【银行名称】产品名称
    // Format 3: **方案1：【银行名称】产品名称**
    const schemeRegex = /(?:#{2,4}\s*)?(?:\*\*)?方案\s*(\d+)[：:]\s*【([^】]+)】([^\n*]+)(?:\*\*)?([\s\S]*?)(?=(?:#{2,4}\s*)?(?:\*\*)?方案\s*\d+|###|##|$)/g;
    const schemes: ParsedScheme[] = [];
    let schemeMatch;
    
    console.warn('[parseMarkdownToSchemes] Looking for schemes...');

    while ((schemeMatch = schemeRegex.exec(markdown)) !== null) {
      const [, , bankName, productName, content] = schemeMatch;
      console.warn('[parseMarkdownToSchemes] Found scheme:', bankName, productName);
      const scheme: ParsedScheme = {
        方案名称: `【${bankName}】${productName.trim()}`,
        银行名称: bankName.trim(),
        产品名称: productName.trim(),
      };

      // Parse scheme details
      const lines = content.split('\n');
      const conditions: string[] = [];

      for (const line of lines) {
        const trimmedLine = line.trim();
        if (!trimmedLine || trimmedLine.startsWith('#')) continue;

        // Parse key-value pairs like "- 可贷额度：xxx"
        const kvMatch = trimmedLine.match(/^-\s*([^：:]+)[：:](.+)$/);
        if (kvMatch) {
          const [, key, value] = kvMatch;
          const cleanKey = key.trim();
          const cleanValue = value.trim();

          if (cleanKey === '准入条件核对' || cleanKey === '准入条件') {
            // Skip, conditions are parsed separately
            continue;
          }

          // Map common keys
          if (cleanKey.includes('额度')) {
            scheme.可贷额度 = cleanValue;
          } else if (cleanKey.includes('利率')) {
            scheme.参考利率 = cleanValue;
          } else if (cleanKey.includes('期限')) {
            scheme.贷款期限 = cleanValue;
          } else if (cleanKey.includes('还款')) {
            scheme.还款方式 = cleanValue;
          } else if (cleanKey.includes('来源')) {
            scheme.来源 = cleanValue;
          } else {
            scheme[cleanKey] = cleanValue;
          }
        }

        // Parse condition items like "  - ✅ 条件1：xxx" or "  - ⚠️ 需沟通"
        const conditionMatch = trimmedLine.match(/^-\s*(✅|⚠️|❌)\s*(.+)$/u);
        if (conditionMatch) {
          conditions.push(`${conditionMatch[1]} ${conditionMatch[2]}`);
        }
      }

      if (conditions.length > 0) {
        scheme.准入条件 = conditions;
      }

      schemes.push(scheme);
    }

    // Alternative pattern: ### 二、推荐方案 with #### 方案1：
    if (schemes.length === 0) {
      const altSchemeRegex = /####\s*方案\s*(\d+)[：:]\s*([^\n]+)([\s\S]*?)(?=####\s*方案|###|$)/g;
      while ((schemeMatch = altSchemeRegex.exec(markdown)) !== null) {
        const [, , title, content] = schemeMatch;
        const scheme: ParsedScheme = {
          方案名称: title.trim(),
        };

        // Try to extract bank name from title like "【银行名】产品名"
        const bankMatch = title.match(/【([^】]+)】(.+)/);
        if (bankMatch) {
          scheme.银行名称 = bankMatch[1].trim();
          scheme.产品名称 = bankMatch[2].trim();
        }

        // Parse content
        const lines = content.split('\n');
        const conditions: string[] = [];

        for (const line of lines) {
          const trimmedLine = line.trim();
          if (!trimmedLine || trimmedLine.startsWith('#')) continue;

          const kvMatch = trimmedLine.match(/^-\s*([^：:]+)[：:](.+)$/);
          if (kvMatch) {
            const [, key, value] = kvMatch;
            const cleanKey = key.trim();
            const cleanValue = value.trim();

            if (cleanKey.includes('额度')) {
              scheme.可贷额度 = cleanValue;
            } else if (cleanKey.includes('利率')) {
              scheme.参考利率 = cleanValue;
            } else if (cleanKey.includes('期限')) {
              scheme.贷款期限 = cleanValue;
            } else if (cleanKey.includes('还款')) {
              scheme.还款方式 = cleanValue;
            } else if (!cleanKey.includes('准入条件')) {
              scheme[cleanKey] = cleanValue;
            }
          }

          const conditionMatch = trimmedLine.match(/^-\s*(✅|⚠️|❌)\s*(.+)$/u);
          if (conditionMatch) {
            conditions.push(`${conditionMatch[1]} ${conditionMatch[2]}`);
          }
        }

        if (conditions.length > 0) {
          scheme.准入条件 = conditions;
        }

        if (scheme.方案名称) {
          schemes.push(scheme);
        }
      }
    }

    if (schemes.length > 0) {
      result.推荐方案 = schemes;
    }

    // 3. Parse 不推荐产品 (table format)
    const notRecommendMatch = markdown.match(/###?\s*三、不推荐的产品及原因[\s\S]*?\|[\s\S]*?(?=###|$)/);
    if (notRecommendMatch) {
      const tableRows = notRecommendMatch[0].match(/\|([^|]+)\|([^|]+)\|/g);
      if (tableRows && tableRows.length > 1) {
        const notRecommended: Array<{ 产品: string; 原因: string }> = [];
        tableRows.slice(1).forEach(row => {
          if (row.includes('---')) return;
          const cells = row.split('|').filter(c => c.trim());
          if (cells.length >= 2) {
            const product = cells[0].trim();
            const reason = cells[1].trim();
            if (product && reason && product !== '产品' && product !== '不符合原因') {
              notRecommended.push({ 产品: product, 原因: reason });
            }
          }
        });
        if (notRecommended.length > 0) {
          result.不推荐产品 = notRecommended;
        }
      }
    }

    // 4. Parse 替代建议 (list format)
    const alternativeMatch = markdown.match(/###?\s*四、替代建议[\s\S]*?(?=###|$)/);
    if (alternativeMatch) {
      const listItems = alternativeMatch[0].match(/^-\s+(.+)$/gm);
      if (listItems && listItems.length > 0) {
        result.替代建议 = listItems.map(item => item.replace(/^-\s+/, '').trim());
      }
    }

    // 5. Parse 需补充信息 (list format)
    const supplementMatch = markdown.match(/###?\s*五、需补充信息[\s\S]*?(?=###|$)/);
    if (supplementMatch) {
      const listItems = supplementMatch[0].match(/^\d+\.\s+(.+)$/gm);
      if (listItems && listItems.length > 0) {
        result.需补充信息 = listItems.map(item => item.replace(/^\d+\.\s+/, '').trim());
      }
    }

    // 6. Parse 准备材料 section
    const materialsMatch = markdown.match(/(?:#{2,4}\s*)?(?:六、)?准备材料[^\n]*\n([\s\S]*?)(?=(?:#{2,4}\s*)?(?:七、|审批流程|$))/i);
    if (materialsMatch) {
      const materialsContent = materialsMatch[1];
      const materials: Record<string, string[]> = {};
      let currentCategory = '其他';
      
      for (const line of materialsContent.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        
        // Category header: **基础材料（必备）**: or **经营证明材料**:
        const categoryMatch = trimmed.match(/^\*\*([^*]+)\*\*[：:]*$/);
        if (categoryMatch) {
          currentCategory = categoryMatch[1].replace(/[（(][^）)]*[）)]/g, '').trim();
          if (!materials[currentCategory]) materials[currentCategory] = [];
          continue;
        }
        
        // List item: - 营业执照复印件
        const itemMatch = trimmed.match(/^[-•]\s+(.+)$/);
        if (itemMatch) {
          if (!materials[currentCategory]) materials[currentCategory] = [];
          materials[currentCategory].push(itemMatch[1].trim());
        }
      }
      
      if (Object.keys(materials).length > 0) {
        result.准备材料 = materials;
      }
    }

    // 7. Parse 审批流程 section (table format)
    const processMatch = markdown.match(/(?:#{2,4}\s*)?(?:七、)?审批流程[^\n]*\n([\s\S]*?)(?=(?:#{2,4}\s*)?(?:八、|⚠️|---|\n##|\n#|$))/i);
    if (processMatch) {
      const processContent = processMatch[1];
      const steps: Array<{ 步骤: string; 内容: string; 预计时间: string }> = [];
      
      const tableRows = processContent.match(/\|([^|]+)\|([^|]+)\|([^|]+)\|/g);
      if (tableRows) {
        for (const row of tableRows) {
          if (row.includes('---') || row.includes('步骤') || row.includes('内容')) continue;
          const cells = row.split('|').filter(c => c.trim());
          if (cells.length >= 3) {
            const step = cells[0].trim().replace(/^\d+\.\s*/, '');
            const content = cells[1].trim();
            const time = cells[2].trim();
            if (step && content) {
              steps.push({ 步骤: step, 内容: content, 预计时间: time });
            }
          }
        }
      }
      
      if (steps.length > 0) {
        result.审批流程 = steps;
      }
    }

    // Return result if we parsed something useful
    const hasUsefulData = 
      (result.推荐方案 && result.推荐方案.length > 0) ||
      (result.客户资料摘要 && Object.keys(result.客户资料摘要).length > 0) ||
      (result.不推荐产品 && result.不推荐产品.length > 0) ||
      (result.替代建议 && result.替代建议.length > 0) ||
      (result.需补充信息 && result.需补充信息.length > 0) ||
      (result.准备材料 && Object.keys(result.准备材料).length > 0) ||
      (result.审批流程 && result.审批流程.length > 0);
    
    console.warn('[parseMarkdownToSchemes] Parse result:', {
      hasUsefulData,
      schemesCount: result.推荐方案?.length || 0,
      summaryKeys: result.客户资料摘要 ? Object.keys(result.客户资料摘要) : [],
      notRecommendedCount: result.不推荐产品?.length || 0,
    });
    
    if (hasUsefulData) {
      return result;
    }

    // If nothing useful found, return null to fallback to markdown rendering
    return null;
  } catch (error) {
    console.error('Failed to parse markdown to schemes:', error);
    return null;
  }
}

/**
 * ParsedSchemeCard Component
 * 
 * Renders a single parsed scheme as a card with green gradient styling.
 * Similar to MatchingSchemeCard but for parsed markdown data.
 */
interface ParsedSchemeCardProps {
  scheme: ParsedScheme;
  index: number;
}

const ParsedSchemeCard: React.FC<ParsedSchemeCardProps> = ({ scheme, index }) => {
  const [showConditions, setShowConditions] = useState(false);
  
  // Key fields to display prominently
  const keyFields = ['可贷额度', '参考利率', '贷款期限', '还款方式'];
  // Fields to exclude from "other info"
  const excludeFields = ['方案名称', '银行名称', '产品名称', '准入条件', ...keyFields];
  
  // Get other fields
  const otherFields = Object.entries(scheme).filter(
    ([key, value]) => !excludeFields.includes(key) && typeof value === 'string'
  );
  
  return (
    <div 
      className="p-4 bg-gradient-to-r from-green-50 to-emerald-50 rounded-xl border border-green-200 shadow-sm"
      data-testid={`parsed-scheme-${index}`}
    >
      {/* Header */}
      <div className="flex items-center gap-3 mb-3">
        <span className="w-8 h-8 bg-green-500 text-white text-sm font-bold rounded-full flex items-center justify-center shadow-sm">
          {index + 1}
        </span>
        <div className="flex-1">
          <div className="font-semibold text-green-800 text-base">
            {scheme.方案名称 || `方案 ${index + 1}`}
          </div>
          {scheme.银行名称 && scheme.产品名称 && scheme.方案名称 !== `【${scheme.银行名称}】${scheme.产品名称}` && (
            <div className="text-xs text-green-600 mt-0.5">
              {scheme.银行名称} · {scheme.产品名称}
            </div>
          )}
        </div>
      </div>
      
      {/* Key Fields Grid */}
      <div className="grid grid-cols-2 gap-3 mb-3">
        {keyFields.map(field => {
          const value = scheme[field];
          if (!value || typeof value !== 'string') return null;
          
          const icons: Record<string, React.ReactNode> = {
            '可贷额度': <DollarSign className="w-4 h-4" />,
            '参考利率': <Percent className="w-4 h-4" />,
            '贷款期限': <Calendar className="w-4 h-4" />,
            '还款方式': <CreditCard className="w-4 h-4" />,
          };
          
          return (
            <div 
              key={field}
              className="flex items-start gap-2 p-2 bg-white/60 rounded-lg"
            >
              <div className="w-6 h-6 rounded-md bg-green-100 text-green-600 flex items-center justify-center shrink-0">
                {icons[field] || <FileText className="w-4 h-4" />}
              </div>
              <div className="min-w-0">
                <div className="text-xs text-gray-500">{field}</div>
                <div className="text-sm font-medium text-gray-800 break-words">{value}</div>
              </div>
            </div>
          );
        })}
      </div>
      
      {/* Other Fields */}
      {otherFields.length > 0 && (
        <div className="text-sm text-gray-600 mb-3 space-y-1">
          {otherFields.map(([key, value]) => (
            <div key={key} className="flex items-start gap-1">
              <span className="text-gray-400 shrink-0">{key}:</span>
              <span className="text-gray-700">{value as string}</span>
            </div>
          ))}
        </div>
      )}
      
      {/* Conditions */}
      {scheme.准入条件 && scheme.准入条件.length > 0 && (
        <div className="border-t border-green-200 pt-3 mt-3">
          <button
            onClick={() => setShowConditions(!showConditions)}
            className="flex items-center gap-1 text-xs text-green-600 hover:text-green-700 transition-colors"
          >
            {showConditions ? (
              <ChevronDown className="w-3 h-3" />
            ) : (
              <ChevronRight className="w-3 h-3" />
            )}
            <span className="font-medium">准入条件核对</span>
            <span className="text-green-500">({scheme.准入条件.length} 项)</span>
          </button>
          {showConditions && (
            <div className="mt-2 space-y-1 pl-4">
              {scheme.准入条件.map((condition, idx) => (
                <div 
                  key={idx}
                  className={`text-xs ${
                    condition.startsWith('✅') ? 'text-green-600' :
                    condition.startsWith('⚠️') ? 'text-amber-600' :
                    condition.startsWith('❌') ? 'text-red-600' :
                    'text-gray-600'
                  }`}
                >
                  {condition}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

/**
 * ParsedMatchingResultDisplay Component
 * 
 * Renders the full parsed matching result with all sections.
 */
interface ParsedMatchingResultDisplayProps {
  data: ParsedMatchingResult;
}

const ParsedMatchingResultDisplay: React.FC<ParsedMatchingResultDisplayProps> = ({ data }) => {
  return (
    <div className="space-y-4" data-testid="parsed-matching-result">
      {/* 客户资料摘要 */}
      {data.客户资料摘要 && Object.keys(data.客户资料摘要).length > 0 && (
        <MatchingDataSectionCard 
          title="客户资料摘要" 
          data={data.客户资料摘要}
          icon={<User className="w-4 h-4" />}
          iconBgColor="bg-indigo-100 text-indigo-600"
        />
      )}
      
      {/* 推荐方案 */}
      {data.推荐方案 && data.推荐方案.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-3 py-2 bg-gradient-to-r from-green-50 to-emerald-50 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-md flex items-center justify-center bg-green-100 text-green-600">
                <CheckCircle2 className="w-4 h-4" />
              </div>
              <span className="font-medium text-gray-700 text-sm">推荐方案</span>
              <span className="text-xs text-gray-400">({data.推荐方案.length} 个)</span>
            </div>
          </div>
          <div className="p-3 space-y-3">
            {data.推荐方案.map((scheme, idx) => (
              <ParsedSchemeCard key={idx} scheme={scheme} index={idx} />
            ))}
          </div>
        </div>
      )}
      
      {/* 不推荐产品 */}
      {data.不推荐产品 && data.不推荐产品.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-3 py-2 bg-gradient-to-r from-gray-50 to-slate-50 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-md flex items-center justify-center bg-gray-200 text-gray-600">
                <X className="w-4 h-4" />
              </div>
              <span className="font-medium text-gray-700 text-sm">不推荐的产品</span>
              <span className="text-xs text-gray-400">({data.不推荐产品.length} 个)</span>
            </div>
          </div>
          <div className="p-3">
            <div className="overflow-hidden rounded-lg border border-gray-200">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-3 py-2 text-left text-gray-600 font-medium">产品</th>
                    <th className="px-3 py-2 text-left text-gray-600 font-medium">不符合原因</th>
                  </tr>
                </thead>
                <tbody>
                  {data.不推荐产品.map((item, idx) => (
                    <tr key={idx} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-3 py-2 text-gray-700">{item.产品}</td>
                      <td className="px-3 py-2 text-gray-600">{item.原因}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
      
      {/* 替代建议 */}
      {data.替代建议 && data.替代建议.length > 0 && (
        <div className="p-3 bg-gradient-to-r from-purple-50 to-indigo-50 rounded-lg border border-purple-200">
          <div className="flex items-start gap-2">
            <div className="w-6 h-6 rounded-md flex items-center justify-center bg-purple-100 text-purple-600 shrink-0 mt-0.5">
              <Target className="w-3.5 h-3.5" />
            </div>
            <div className="flex-1">
              <div className="text-xs font-medium text-purple-600 mb-2">替代建议</div>
              <div className="space-y-1">
                {data.替代建议.map((item, idx) => (
                  <div key={idx} className="text-sm text-gray-700 flex items-start gap-2">
                    <span className="text-purple-400">•</span>
                    <span>{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
      
      {/* 需补充信息 */}
      {data.需补充信息 && data.需补充信息.length > 0 && (
        <div className="p-3 bg-gradient-to-r from-amber-50 to-orange-50 rounded-lg border border-amber-200">
          <div className="flex items-start gap-2">
            <div className="w-6 h-6 rounded-md flex items-center justify-center bg-amber-100 text-amber-600 shrink-0 mt-0.5">
              <AlertCircle className="w-3.5 h-3.5" />
            </div>
            <div className="flex-1">
              <div className="text-xs font-medium text-amber-600 mb-2">需补充信息</div>
              <div className="space-y-1">
                {data.需补充信息.map((item, idx) => (
                  <div key={idx} className="text-sm text-gray-700 flex items-start gap-2">
                    <span className="text-amber-500 font-medium">{idx + 1}.</span>
                    <span>{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* 准备材料 */}
      {data.准备材料 && Object.keys(data.准备材料).length > 0 && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-3 py-2 bg-gradient-to-r from-cyan-50 to-blue-50 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-md flex items-center justify-center bg-cyan-100 text-cyan-600">
                <FileCheck className="w-4 h-4" />
              </div>
              <span className="font-medium text-gray-700 text-sm">准备材料</span>
            </div>
          </div>
          <div className="p-3 space-y-3">
            {Object.entries(data.准备材料).map(([category, items], idx) => (
              <div key={idx}>
                <div className="text-xs font-medium text-gray-600 mb-1.5">{category}</div>
                <div className="space-y-1">
                  {items.map((item, itemIdx) => (
                    <div key={itemIdx} className="text-sm text-gray-700 flex items-start gap-2 pl-2">
                      <span className="text-cyan-500 mt-1">•</span>
                      <span>{item}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 审批流程 */}
      {data.审批流程 && data.审批流程.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-3 py-2 bg-gradient-to-r from-teal-50 to-emerald-50 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-md flex items-center justify-center bg-teal-100 text-teal-600">
                <ClipboardList className="w-4 h-4" />
              </div>
              <span className="font-medium text-gray-700 text-sm">审批流程</span>
            </div>
          </div>
          <div className="p-3">
            <div className="overflow-hidden rounded-lg border border-gray-200">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-3 py-2 text-left text-gray-600 font-medium">步骤</th>
                    <th className="px-3 py-2 text-left text-gray-600 font-medium">内容</th>
                    <th className="px-3 py-2 text-left text-gray-600 font-medium">预计时间</th>
                  </tr>
                </thead>
                <tbody>
                  {data.审批流程.map((step, idx) => (
                    <tr key={idx} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-3 py-2 text-gray-700 font-medium">{step.步骤}</td>
                      <td className="px-3 py-2 text-gray-600">{step.内容}</td>
                      <td className="px-3 py-2 text-gray-500">{step.预计时间}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

interface MatchingSchemeItem {
  方案名称?: string;
  银行名称?: string;
  产品名称?: string;
  可贷额度?: string;
  参考利率?: string;
  贷款期限?: string;
  还款方式?: string;
  匹配理由?: string;
  审批说明?: string;
  准备材料?: Record<string, string[]>;
  审批流程?: Array<{步骤: string; 内容: string; 预计时间: string}>;
  [key: string]: string | Record<string, string[]> | Array<{步骤: string; 内容: string; 预计时间: string}> | undefined;
}

/**
 * MatchingSchemeCard Component
 * 
 * Renders the "推荐方案" section as cards.
 */
interface MatchingSchemeCardProps {
  schemes: MatchingSchemeItem[];
}

const MatchingSchemeCard: React.FC<MatchingSchemeCardProps> = ({ schemes }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  
  if (!schemes || schemes.length === 0) return null;
  
  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      {/* Section Header */}
      <div 
        className="px-3 py-2 bg-gradient-to-r from-green-50 to-emerald-50 border-b border-gray-100 cursor-pointer"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-md flex items-center justify-center bg-green-100 text-green-600">
              <CheckCircle2 className="w-4 h-4" />
            </div>
            <span className="font-medium text-gray-700 text-sm">推荐方案</span>
            <span className="text-xs text-gray-400">({schemes.length} 个)</span>
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
        <div className="p-3 space-y-4">
          {schemes.map((scheme, idx) => {
            // 提取准备材料和审批流程，其余字段正常显示
            const 准备材料 = scheme['准备材料'] as unknown as Record<string, string[]> | undefined;
            const 审批流程 = scheme['审批流程'] as unknown as Array<{步骤: string; 内容: string; 预计时间: string}> | undefined;
            const 审批说明 = scheme['审批说明'] as string | undefined;
            const basicEntries = Object.entries(scheme).filter(
              ([key]) => key !== '方案名称' && key !== '准备材料' && key !== '审批流程' && key !== '审批说明'
            );
            
            return (
              <div 
                key={idx}
                className="border border-green-200 rounded-xl overflow-hidden"
              >
                {/* 方案标题 */}
                <div className="flex items-center gap-2 px-3 py-2.5 bg-gradient-to-r from-green-50 to-emerald-50">
                  <span className="w-6 h-6 bg-green-500 text-white text-xs font-bold rounded-full flex items-center justify-center shrink-0">
                    {idx + 1}
                  </span>
                  <span className="font-medium text-green-800 text-sm">
                    {scheme['方案名称'] as string || `方案 ${idx + 1}`}
                  </span>
                </div>
                
                {/* 基本信息 */}
                <div className="px-3 py-2 grid grid-cols-2 gap-2 text-sm border-b border-green-100">
                  {basicEntries.map(([key, value]) => (
                    <div key={key} className="flex items-start gap-1">
                      <span className="text-gray-500 shrink-0">{key}:</span>
                      <span className="text-gray-800">{String(value) || '-'}</span>
                    </div>
                  ))}
                </div>
                
                {/* 准备材料 */}
                {准备材料 && Object.keys(准备材料).length > 0 && (
                  <div className="px-3 py-2 border-b border-green-100">
                    <div className="flex items-center gap-1.5 mb-2">
                      <FileCheck className="w-3.5 h-3.5 text-cyan-600" />
                      <span className="text-xs font-medium text-cyan-700">准备材料</span>
                    </div>
                    <div className="space-y-2">
                      {Object.entries(准备材料).map(([category, items], cIdx) => (
                        <div key={cIdx}>
                          <div className="text-xs text-gray-500 mb-1">{category}</div>
                          <div className="flex flex-wrap gap-1">
                            {(items as string[]).map((item, iIdx) => (
                              <span key={iIdx} className="px-2 py-0.5 bg-cyan-50 text-cyan-700 text-xs rounded border border-cyan-100">
                                {item}
                              </span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
                
                {/* 审批流程 */}
                {(审批流程 && 审批流程.length > 0) || 审批说明 ? (
                  <div className="px-3 py-2">
                    <div className="flex items-center gap-1.5 mb-2">
                      <ClipboardList className="w-3.5 h-3.5 text-teal-600" />
                      <span className="text-xs font-medium text-teal-700">审批流程</span>
                    </div>
                    {审批流程 && 审批流程.length > 0 && (
                      <div className="flex flex-wrap gap-1.5">
                        {审批流程.map((step, sIdx) => (
                          <div key={sIdx} className="flex items-center gap-1 text-xs text-gray-600">
                            <span className="w-4 h-4 bg-teal-100 text-teal-700 rounded-full flex items-center justify-center font-medium shrink-0">
                              {sIdx + 1}
                            </span>
                            <span>{step.步骤}</span>
                            {step.预计时间 && <span className="text-gray-400">({step.预计时间})</span>}
                            {sIdx < 审批流程.length - 1 && <span className="text-gray-300 mx-0.5">→</span>}
                          </div>
                        ))}
                      </div>
                    )}
                    {审批说明 && (
                      <div className="mt-2 text-xs text-gray-600 bg-teal-50 border border-teal-100 rounded-md px-2 py-1.5">
                        {审批说明}
                      </div>
                    )}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

/**
 * MatchingResultCard Component
 * 
 * Displays scheme matching results.
 * - If matchingData (JSON) is available, renders as grouped cards
 * - Falls back to Markdown rendering if only matchResult is available
 */
const MatchingResultCard: React.FC<MatchingResultCardProps> = ({ data, onNavigate }) => {
  const { state } = useApp();
  const [isExpanded, setIsExpanded] = useState(true);
  
  // If needs input, show the guide card instead
  if (data.needsInput) {
    return <MatchingGuideCard data={data} onNavigate={onNavigate} />;
  }
  
  // If no match result at all, show guide card
  if (!data.matchingData && !data.matchResult) {
    return <MatchingGuideCard data={data} onNavigate={onNavigate} />;
  }
  
  const creditTypeLabels: Record<string, string> = {
    'personal': '个人贷款',
    'enterprise': '企业贷款',
    'enterprise_credit': '企业信用贷',
  };
  const creditTypeLabel = creditTypeLabels[data.creditType || ''] || '企业贷款';
  
  // Check if we have structured data
  const hasStructuredData = data.matchingData && Object.keys(data.matchingData).length > 0;
  const currentSchemeMeta = state.scheme.result;
  const sameCustomerStale =
    Boolean(currentSchemeMeta?.stale) &&
    Boolean(currentSchemeMeta?.customerId) &&
    currentSchemeMeta?.customerId === state.extraction.currentCustomerId;
  const staleReason = currentSchemeMeta?.staleReason || '客户资料已更新，请重新匹配方案。';
  const staleAtLabel = currentSchemeMeta?.staleAt
    ? formatLocalDateTime(currentSchemeMeta.staleAt)
    : '';
  const matchingStatusBadge = sameCustomerStale
    ? { label: '待重匹配', className: 'border-amber-200 bg-amber-50 text-amber-700' }
    : { label: '最新结果', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };
  
  return (
    <div 
      className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm"
      data-testid="matching-result-card"
    >
      {/* Header */}
      <div className="px-4 py-3 bg-gradient-to-r from-emerald-50 to-teal-50 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-emerald-100 text-emerald-600 flex items-center justify-center">
              <Target className="w-5 h-5" />
            </div>
            <div>
              <div className="font-medium text-gray-800 text-sm">
                方案匹配结果
                <span className={`ml-2 inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${matchingStatusBadge.className}`}>
                  {matchingStatusBadge.label}
                </span>
              </div>
              <div className="text-xs text-gray-500 mt-0.5">
                {data.customerName && `客户：${data.customerName}`}
                {data.customerName && ' · '}
                {creditTypeLabel}
              </div>
            </div>
          </div>
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="p-1.5 hover:bg-emerald-100 rounded-lg transition-colors"
          >
            {isExpanded ? (
              <ChevronDown className="w-4 h-4 text-gray-500" />
            ) : (
              <ChevronRight className="w-4 h-4 text-gray-500" />
            )}
          </button>
        </div>
      </div>

      {sameCustomerStale && (
        <div className="border-b border-amber-100 bg-amber-50/80 px-4 py-4">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-sm font-semibold text-amber-900">这份方案匹配结果已被新上传资料覆盖</div>
              <div className="mt-1 text-sm text-amber-800">{staleReason}</div>
              <div className="mt-1 text-xs text-amber-700">
                {staleAtLabel ? `失效时间：${staleAtLabel}` : '请重新匹配后再用于风险评估或客户沟通。'}
              </div>
            </div>
            <button
              type="button"
              onClick={() => onNavigate?.('scheme')}
              className="inline-flex items-center justify-center rounded-xl bg-amber-500 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-amber-600"
            >
              去方案匹配页重新匹配
            </button>
          </div>
        </div>
      )}
      
      {/* Content */}
      {isExpanded && (
        <div className="p-4">
          {hasStructuredData ? (
            // Render as grouped cards
            <div className="space-y-4" data-testid="matching-structured-data">
              {/* 核心发现 */}
              {data.matchingData?.核心发现 && (
                <MatchingDataSectionCard 
                  title="核心发现" 
                  data={data.matchingData.核心发现}
                  icon={<FileText className="w-4 h-4" />}
                  iconBgColor="bg-blue-100 text-blue-600"
                />
              )}
              
              {/* 客户资料摘要 */}
              {data.matchingData?.客户资料摘要 && (
                <MatchingDataSectionCard 
                  title="客户资料摘要" 
                  data={data.matchingData.客户资料摘要}
                  icon={<User className="w-4 h-4" />}
                  iconBgColor="bg-indigo-100 text-indigo-600"
                />
              )}
              
              {/* 待补充资料 */}
              {data.matchingData?.待补充资料 && (
                <MatchingSupplementCard data={data.matchingData.待补充资料} />
              )}
              
              {/* 推荐方案 */}
              {data.matchingData?.推荐方案 && data.matchingData.推荐方案.length > 0 && (
                <MatchingSchemeCard schemes={data.matchingData.推荐方案} />
              )}
              
              {/* 下一步建议 */}
              {data.matchingData?.下一步建议 && (
                <div className="p-3 bg-gradient-to-r from-purple-50 to-indigo-50 rounded-lg border border-purple-200">
                  <div className="flex items-start gap-2">
                    <div className="w-6 h-6 rounded-md flex items-center justify-center bg-purple-100 text-purple-600 shrink-0 mt-0.5">
                      <Target className="w-3.5 h-3.5" />
                    </div>
                    <div>
                      <div className="text-xs font-medium text-purple-600 mb-1">下一步建议</div>
                      <div className="text-sm text-gray-700">{data.matchingData.下一步建议}</div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          ) : (
            // Try to parse Markdown to structured data first
            (() => {
              const parsedData = parseMarkdownToSchemes(data.matchResult || '');
              
              // Use card rendering if we have any useful parsed data
              if (parsedData) {
                // Successfully parsed - render as cards
                return <ParsedMatchingResultDisplay data={parsedData} />;
              }
              
              // Fallback to Markdown rendering with enhanced styling
              return (
                <div 
                  className="prose prose-sm max-w-none text-gray-700 overflow-x-auto 
                    prose-headings:text-gray-800 prose-headings:font-semibold
                    prose-h1:text-lg prose-h1:border-b prose-h1:border-gray-200 prose-h1:pb-2
                    prose-h2:text-base prose-h2:text-blue-700 prose-h2:mt-4
                    prose-h3:text-sm prose-h3:text-gray-700
                    prose-h4:text-sm prose-h4:font-medium prose-h4:text-emerald-700
                    prose-strong:text-gray-800
                    prose-table:border-collapse prose-table:w-full prose-table:text-sm
                    prose-th:border prose-th:border-gray-300 prose-th:bg-gray-100 prose-th:px-3 prose-th:py-2 prose-th:text-left
                    prose-td:border prose-td:border-gray-300 prose-td:px-3 prose-td:py-2
                    prose-ul:my-2 prose-li:my-0.5
                    prose-p:my-2 prose-p:leading-relaxed
                    [&_ul]:list-none [&_ul]:pl-0
                    [&_li]:relative [&_li]:pl-5
                    [&_li:before]:content-['✅'] [&_li:before]:absolute [&_li:before]:left-0
                    [&_.方案]:bg-blue-50 [&_.方案]:p-3 [&_.方案]:rounded-lg [&_.方案]:my-2"
                  data-testid="matching-markdown-content"
                >
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {data.matchResult || ''}
                  </ReactMarkdown>
                </div>
              );
            })()
          )}
        </div>
      )}
    </div>
  );
};

// ============================================
// Structured Data Card Renderer
// ============================================

interface StructuredDataCardProps {
  intent?: ChatResponse['intent'] | null;
  data: Record<string, unknown> | null;
  onNavigate?: (page: string) => void;
}

function isRiskReportData(data: Record<string, unknown> | null): boolean {
  if (!data) return false;
  return 'customer_summary' in data && 'overall_assessment' in data && 'risk_dimensions' in data;
}

function buildRiskReportComparison(
  currentReport: CustomerRiskReportJson,
  previousReport?: CustomerRiskReportHistoryItem | null,
): {
  available: boolean;
  scoreDelta: number;
  completenessDelta: number;
  previousRiskLevel: string;
  addedMissingItems: string[];
  resolvedMissingItems: string[];
  actionChanged: boolean;
} {
  if (!previousReport) {
    return {
      available: false,
      scoreDelta: 0,
      completenessDelta: 0,
      previousRiskLevel: '',
      addedMissingItems: [],
      resolvedMissingItems: [],
      actionChanged: false,
    };
  }

  const currentAssessment = currentReport.overall_assessment;
  const previousAssessment = previousReport.report_json?.overall_assessment;
  const currentCompleteness = currentReport.customer_summary?.data_completeness;
  const previousCompleteness = previousReport.report_json?.customer_summary?.data_completeness;
  const currentMissing = new Set(currentCompleteness?.missing_items || []);
  const previousMissing = new Set(previousCompleteness?.missing_items || []);

  return {
    available: true,
    scoreDelta: (currentAssessment?.total_score || 0) - (previousAssessment?.total_score || 0),
    completenessDelta: (currentCompleteness?.score || 0) - (previousCompleteness?.score || 0),
    previousRiskLevel: previousAssessment?.risk_level || '',
    addedMissingItems: [...currentMissing].filter((item) => !previousMissing.has(item)),
    resolvedMissingItems: [...previousMissing].filter((item) => !currentMissing.has(item)),
    actionChanged:
      (currentReport.final_recommendation?.action || '') !== (previousReport.report_json?.final_recommendation?.action || ''),
  };
}

type CustomerRiskReportCardData = CustomerRiskReportJson & {
  generated_at?: string;
  profile_version?: number;
  profile_updated_at?: string;
  previous_report?: CustomerRiskReportHistoryItem | null;
};

const RiskReportCard: React.FC<{ report: CustomerRiskReportCardData }> = ({ report }) => {
  const [copyState, setCopyState] = useState<'idle' | 'done'>('idle');
  const [reportHistory, setReportHistory] = useState<CustomerRiskReportHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [showComparison, setShowComparison] = useState(false);
  const summary = report.customer_summary;
  const assessment = report.overall_assessment;
  const generatedAt = report.generated_at;
  const profileVersion = report.profile_version;
  const profileUpdatedAt = report.profile_updated_at;
  const previousReport = (report.previous_report as CustomerRiskReportHistoryItem | undefined) || reportHistory[1] || reportHistory[0] || null;
  const comparison = buildRiskReportComparison(report, previousReport);
  const reportStatusBadge = profileVersion
    ? { label: `基于资料汇总 V${profileVersion}`, className: 'border-sky-200 bg-sky-50 text-sky-700' }
    : { label: '版本待确认', className: 'border-slate-200 bg-slate-50 text-slate-600' };
  const dimensions = report.risk_dimensions || [];
  const noMatch = report.no_match_analysis;
  const matchedSchemes = report.matched_schemes;
  const suggestions = report.optimization_suggestions;
  const plan = report.financing_plan;
  const finalRecommendation = report.final_recommendation;
  const riskBarWidth = `${Math.max(8, Math.min(assessment.total_score ?? 0, 100))}%`;
  const suggestionTimeline = [
    {
      title: '短期优化',
      description: suggestions.short_term?.length ? suggestions.short_term.join('；') : '暂无',
      tone: 'border-blue-200 bg-blue-50 text-blue-700',
    },
    {
      title: '中期优化',
      description: suggestions.mid_term?.length ? suggestions.mid_term.join('；') : '暂无',
      tone: 'border-violet-200 bg-violet-50 text-violet-700',
    },
    {
      title: '补件建议',
      description: suggestions.document_supplement?.length ? suggestions.document_supplement.join('、') : '暂无',
      tone: 'border-amber-200 bg-amber-50 text-amber-700',
    },
    {
      title: '征信与负债优化',
      description: [
        suggestions.credit_optimization?.length ? `征信优化：${suggestions.credit_optimization.join('；')}` : '',
        suggestions.debt_optimization?.length ? `负债优化：${suggestions.debt_optimization.join('；')}` : '',
      ].filter(Boolean).join('  ') || '暂无',
      tone: 'border-rose-200 bg-rose-50 text-rose-700',
    },
  ];
  const financingTimeline = [
    {
      title: '当前阶段',
      description: plan.current_stage || '待补充',
      tone: 'border-slate-200 bg-slate-50 text-slate-700',
    },
    {
      title: '1-3个月规划',
      description: plan.one_to_three_months?.length ? plan.one_to_three_months.join('；') : '暂无',
      tone: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    },
    {
      title: '3-6个月规划',
      description: plan.three_to_six_months?.length ? plan.three_to_six_months.join('；') : '暂无',
      tone: 'border-cyan-200 bg-cyan-50 text-cyan-700',
    },
    {
      title: '替代融资路径',
      description: plan.alternative_paths?.length ? plan.alternative_paths.join('；') : '暂无',
      tone: 'border-fuchsia-200 bg-fuchsia-50 text-fuchsia-700',
    },
  ];

  useEffect(() => {
    const customerId = summary.customer_id;
    if (!customerId) return;
    let active = true;
    setHistoryLoading(true);
    getCustomerRiskReportHistory(customerId, 2)
      .then((response) => {
        if (!active) return;
        setReportHistory(response.items || []);
      })
      .catch(() => {
        if (!active) return;
        setReportHistory([]);
      })
      .finally(() => {
        if (active) {
          setHistoryLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, [summary.customer_id, generatedAt, profileVersion]);

  const exportMarkdown = useCallback(() => {
    const markdownLines = [
      '# 风险评估报告',
      '',
      `- 客户名称：${summary.customer_name || '未命名客户'}`,
      `- 生成时间：${generatedAt ? formatLocalDateTime(generatedAt) : '刚刚生成'}`,
      `- 资料汇总版本：${profileVersion ? `V${profileVersion}` : '版本待确认'}`,
      `- 资料汇总更新时间：${profileUpdatedAt ? formatLocalDateTime(profileUpdatedAt) : '未记录'}`,
      `- 客户类型：${formatCustomerTypeLabel(summary.customer_type)}`,
      `- 综合评分：${assessment.total_score ?? '-'}`,
      `- 风险等级：${formatRiskLevelLabel(assessment.risk_level)}`,
      `- 资料完整度：${formatCompletenessStatus(summary.data_completeness?.status)}`,
      `- 资料完整度评分：${summary.data_completeness?.score ?? '-'}`,
      `- 申请建议：${assessment.immediate_application_recommended ? '建议立即申请' : '建议先优化后申请'}`,
      '',
      '## 客户概况',
      `- 所属行业：${summary.industry || '待补充'}`,
      `- 融资需求：${summary.financing_need || '待补充'}`,
      `- 缺失资料：${summary.data_completeness?.missing_items?.length ? summary.data_completeness.missing_items.join('、') : '无'}`,
      '',
      '## 综合结论',
      assessment.conclusion || '暂无结论',
      '',
      '## 风险维度评估',
      ...dimensions.flatMap((item) => [
        `### ${formatRiskDimensionLabel(item.dimension)}`,
        `- 评分：${item.score} / 20`,
        `- 风险等级：${formatRiskLevelLabel(item.risk_level)}`,
        `- 说明：${item.summary || '暂无说明'}`,
        `- 缺失资料：${item.missing_info?.length ? item.missing_info.join('、') : '无'}`,
        '',
      ]),
      '## 融资方案分析',
      ...(matchedSchemes.has_match && matchedSchemes.items.length > 0
        ? matchedSchemes.items.flatMap((item, index) => [
            `### 方案 ${index + 1}${item.product_name ? `：${item.product_name}` : ''}`,
            `- 预计额度：${item.estimated_limit || '待评估'}`,
            `- 预计利率：${item.estimated_rate || '待评估'}`,
            `- 匹配原因：${item.match_reason || '待补充'}`,
            `- 限制条件：${item.constraints?.length ? item.constraints.join('、') : '无'}`,
            '',
          ])
        : [
            '> 当前暂无可直接采用的匹配方案，建议先补强短板后再进行新一轮匹配。',
            '',
            `- 未匹配原因：${noMatch.reasons?.length ? noMatch.reasons.join('；') : '系统未命中可直接进件方案。'}`,
            `- 核心短板：${noMatch.core_shortboards?.length ? noMatch.core_shortboards.join('、') : '需进一步补充资料评估。'}`,
            '',
          ]),
      '## 优化建议',
      ...suggestionTimeline.flatMap((item, index) => [
        `### ${index + 1}. ${item.title}`,
        item.description,
        '',
      ]),
      '',
      '## 融资规划',
      ...financingTimeline.flatMap((item, index) => [
        `### ${index + 1}. ${item.title}`,
        item.description,
        '',
      ]),
      '',
      '## 最终建议',
      `- 当前动作：${formatRecommendationActionLabel(finalRecommendation.action)}`,
      `- 优先产品类型：${finalRecommendation.priority_product_types?.length ? finalRecommendation.priority_product_types.map((item) => formatProductTypeLabel(item)).join('、') : '待评估'}`,
      `- 下一步：${finalRecommendation.next_steps?.length ? finalRecommendation.next_steps.join('；') : '先补齐核心资料'}`,
      '',
    ];
    const blob = new Blob([markdownLines.join('\n')], { type: 'text/markdown;charset=utf-8' });
    createDownloadLink(blob, `风险评估报告_${summary.customer_name || '未命名客户'}.md`);
  }, [assessment, dimensions, finalRecommendation, financingTimeline, generatedAt, matchedSchemes, noMatch, profileUpdatedAt, profileVersion, suggestionTimeline, summary]);

  const copyReport = useCallback(async () => {
    const text = [
      `风险评估报告 - ${summary.customer_name || '未命名客户'}`,
      `客户类型：${formatCustomerTypeLabel(summary.customer_type)}`,
      `风险等级：${formatRiskLevelLabel(assessment.risk_level)}`,
      `综合评分：${assessment.total_score ?? '-'}`,
      `资料完整度：${formatCompletenessStatus(summary.data_completeness?.status)}`,
      `综合结论：${assessment.conclusion || '暂无结论'}`,
      '',
      `客户概况：所属行业 ${summary.industry || '待补充'}；融资需求 ${summary.financing_need || '待补充'}；缺失资料 ${summary.data_completeness?.missing_items?.length ? summary.data_completeness.missing_items.join('、') : '无'}`,
      '',
      '风险维度评估：',
      ...dimensions.map((item) => `- ${formatRiskDimensionLabel(item.dimension)}：${item.score}/20，${formatRiskLevelLabel(item.risk_level)}，${item.summary || '暂无说明'}`),
      '',
      matchedSchemes.has_match && matchedSchemes.items.length > 0
        ? `融资方案分析：${matchedSchemes.items.map((item) => `${item.product_name || '未命名方案'}（${item.match_reason || '待补充'}）`).join('；')}`
        : `融资方案分析：当前暂无可直接采用的匹配方案。未匹配原因：${noMatch.reasons?.join('；') || '系统未命中可直接进件方案。'}；核心短板：${noMatch.core_shortboards?.join('、') || '需进一步补充资料评估。'}`,
      '',
      `优化建议：${suggestionTimeline.map((item) => `${item.title}：${item.description}`).join('；')}`,
      `融资规划：${financingTimeline.map((item) => `${item.title}：${item.description}`).join('；')}`,
      `最终建议：当前动作 ${formatRecommendationActionLabel(finalRecommendation.action)}；优先产品类型 ${finalRecommendation.priority_product_types?.length ? finalRecommendation.priority_product_types.map((item) => formatProductTypeLabel(item)).join('、') : '待评估'}；下一步 ${finalRecommendation.next_steps?.length ? finalRecommendation.next_steps.join('；') : '先补齐核心资料'}`,
    ].join('\n');
    await navigator.clipboard.writeText(text);
    setCopyState('done');
    window.setTimeout(() => setCopyState('idle'), 1800);
  }, [assessment, dimensions, finalRecommendation, financingTimeline, generatedAt, matchedSchemes, noMatch, profileUpdatedAt, profileVersion, suggestionTimeline, summary]);

  const printReport = useCallback(() => {
    const printWindow = window.open('', '_blank', 'width=1000,height=760');
    if (!printWindow) return;
    const printHtml = `
      <!doctype html>
      <html lang="zh-CN">
      <head>
        <meta charset="utf-8" />
        <title>风险评估报告</title>
        <style>
          @page { margin: 92px 32px 72px; }
          body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; margin: 32px; color: #0f172a; }
          body { position: relative; }
          h1 { font-size: 26px; margin-bottom: 8px; }
          h2 { font-size: 18px; margin: 24px 0 10px; }
          .meta { color: #475569; margin-bottom: 18px; }
          .page-header { position: fixed; top: -62px; left: 0; right: 0; display:flex; justify-content:space-between; align-items:flex-end; border-bottom:1px solid #cbd5e1; padding-bottom:10px; }
          .page-footer { position: fixed; bottom: -44px; left: 0; right: 0; display:flex; justify-content:space-between; color:#64748b; font-size:12px; border-top:1px solid #e2e8f0; padding-top:10px; }
          .tag { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #eef2ff; color: #4338ca; font-weight: 600; }
          .summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }
          .summary-item { border: 1px solid #e2e8f0; border-radius: 14px; padding: 14px; background: #f8fafc; }
          .summary-label { color: #64748b; font-size: 12px; }
          .summary-value { margin-top: 6px; font-size: 20px; font-weight: 700; }
          .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
          .card { border: 1px solid #e2e8f0; border-radius: 16px; padding: 16px; margin-bottom: 14px; }
          .alert { border: 1px solid #fcd34d; background: linear-gradient(135deg, #fff7ed 0%, #fffbeb 100%); }
          .muted { color: #475569; line-height: 1.7; }
          .progress-shell { height: 8px; border-radius: 999px; overflow: hidden; background: #e2e8f0; margin: 10px 0 8px; }
          .progress-bar { height: 8px; border-radius: 999px; }
          .timeline-item { display: grid; grid-template-columns: 36px 1fr; gap: 12px; align-items: start; margin-bottom: 14px; }
          .timeline-dot { width: 32px; height: 32px; border-radius: 999px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700; border: 1px solid #cbd5e1; background: #f8fafc; color: #334155; }
          .timeline-card { border: 1px solid #e2e8f0; border-radius: 14px; padding: 12px 14px; background: #f8fafc; }
          .timeline-title { font-weight: 700; margin-bottom: 6px; }
          ul { padding-left: 18px; }
          li { margin: 6px 0; }
        </style>
      </head>
      <body>
        <div class="page-header">
          <div>
            <div style="font-size:12px;color:#64748b;">贷款助手风险评估中心</div>
            <div style="font-size:16px;font-weight:700;color:#0f172a;">${escapeHtml(summary.customer_name || '未命名客户')}</div>
          </div>
          <div style="text-align:right;font-size:12px;color:#64748b;">
            <div>生成时间：${escapeHtml(generatedAt ? formatLocalDateTime(generatedAt) : '刚刚生成')}</div>
            <div>资料汇总版本：${escapeHtml(profileVersion ? `V${profileVersion}` : '版本待确认')}</div>
          </div>
        </div>
        <h1>风险评估报告</h1>
        <div class="meta">${escapeHtml(summary.customer_name || '未命名客户')} · ${escapeHtml(formatCustomerTypeLabel(summary.customer_type))}</div>
        <div class="tag">${escapeHtml(formatRiskLevelLabel(assessment.risk_level))} / ${escapeHtml(String(assessment.total_score ?? '-'))} 分</div>
        <div class="summary-grid">
          <div class="summary-item">
            <div class="summary-label">综合评分</div>
            <div class="summary-value">${escapeHtml(String(assessment.total_score ?? '-'))}</div>
          </div>
          <div class="summary-item">
            <div class="summary-label">资料完整度</div>
            <div class="summary-value" style="font-size:16px;">${escapeHtml(formatCompletenessStatus(summary.data_completeness?.status))}</div>
          </div>
          <div class="summary-item">
            <div class="summary-label">资料完整度评分</div>
            <div class="summary-value" style="font-size:16px;">${escapeHtml(String(summary.data_completeness?.score ?? '-'))}</div>
          </div>
          <div class="summary-item">
            <div class="summary-label">申请建议</div>
            <div class="summary-value" style="font-size:16px;">${escapeHtml(assessment.immediate_application_recommended ? '建议立即申请' : '建议先优化后申请')}</div>
          </div>
        </div>
        <h2>综合结论</h2>
        <div class="card muted">${escapeHtml(assessment.conclusion || '暂无结论')}</div>
        <h2>风险维度评估</h2>
        <div class="grid">
          ${dimensions.map((item) => `
            <div class="card">
              <strong>${escapeHtml(formatRiskDimensionLabel(item.dimension))}</strong><br/>
              <span class="muted">评分：${escapeHtml(String(item.score))} / 风险等级：${escapeHtml(formatRiskLevelLabel(item.risk_level))}</span>
              <div class="progress-shell">
                <div class="progress-bar" style="width:${Math.max(8, Math.min(100, (item.score / 20) * 100))}%; background:${item.risk_level === 'high' ? '#f43f5e' : item.risk_level === 'medium' ? '#f59e0b' : '#10b981'};"></div>
              </div>
              <div class="muted" style="margin-top:8px;">${escapeHtml(item.summary || '暂无说明')}</div>
            </div>
          `).join('')}
        </div>
        ${!matchedSchemes.has_match || matchedSchemes.items.length === 0 ? `
          <h2>未匹配方案提示</h2>
          <div class="card alert">
            <div style="font-weight:700; color:#92400e;">当前暂无可直接采用的匹配方案</div>
            <div class="muted" style="margin-top:8px;">现阶段更适合先补强短板，再进入下一轮产品匹配。</div>
            <div class="grid" style="margin-top:12px;">
              <div class="card" style="background:#fff; margin-bottom:0;">
                <div style="font-size:12px; color:#b45309; font-weight:700;">未匹配原因</div>
                <div class="muted" style="margin-top:8px;">${escapeHtml(noMatch.reasons?.join('；') || '系统未命中可直接进件方案。')}</div>
              </div>
              <div class="card" style="background:#fff; margin-bottom:0;">
                <div style="font-size:12px; color:#b45309; font-weight:700;">核心短板</div>
                <div class="muted" style="margin-top:8px;">${escapeHtml(noMatch.core_shortboards?.join('、') || '需进一步补充资料评估。')}</div>
              </div>
            </div>
          </div>
        ` : ''}
        <h2>优化建议</h2>
        <div class="card">
          ${suggestionTimeline.map((item, index) => `
            <div class="timeline-item">
              <div class="timeline-dot">${index + 1}</div>
              <div class="timeline-card">
                <div class="timeline-title">${escapeHtml(item.title)}</div>
                <div class="muted">${escapeHtml(item.description)}</div>
              </div>
            </div>
          `).join('')}
        </div>
        <h2>融资规划</h2>
        <div class="card">
          ${financingTimeline.map((item, index) => `
            <div class="timeline-item">
              <div class="timeline-dot">${index + 1}</div>
              <div class="timeline-card">
                <div class="timeline-title">${escapeHtml(item.title)}</div>
                <div class="muted">${escapeHtml(item.description)}</div>
              </div>
            </div>
          `).join('')}
        </div>
        <div class="page-footer">
          <div>贷款助手智能贷款审批管理系统</div>
          <div>${escapeHtml(summary.customer_name || '未命名客户')} · ${escapeHtml(generatedAt ? formatLocalDateTime(generatedAt) : '刚刚生成')}</div>
        </div>
      </body>
      </html>
    `;
    printWindow.document.open();
    printWindow.document.write(printHtml);
    printWindow.document.close();
    printWindow.focus();
    printWindow.print();
  }, [assessment, dimensions, financingTimeline, generatedAt, matchedSchemes, noMatch, plan, profileUpdatedAt, profileVersion, suggestionTimeline, suggestions, summary]);

  return (
    <div className="mt-3 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-100 bg-[linear-gradient(135deg,#fff7ed_0%,#eff6ff_100%)] px-5 py-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="flex flex-wrap items-center gap-2 text-lg font-semibold text-slate-800">
              <span>风险评估报告</span>
              <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${reportStatusBadge.className}`}>
                {reportStatusBadge.label}
              </span>
            </div>
            <div className="mt-1 text-sm text-slate-500">
              {summary.customer_name || '未命名客户'}
              {' · '}
              {formatCustomerTypeLabel(summary.customer_type)}
            </div>
          </div>
          <div className={`rounded-full border px-3 py-1 text-sm font-medium ${getRiskTone(assessment.risk_level)}`}>
            {formatRiskLevelLabel(assessment.risk_level)}
          </div>
        </div>
        <div className="mt-4 overflow-hidden rounded-full bg-white/80">
          <div className={`h-2 rounded-full ${assessment.risk_level === 'high' ? 'bg-rose-500' : assessment.risk_level === 'medium' ? 'bg-amber-500' : 'bg-emerald-500'}`} style={{ width: riskBarWidth }} />
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-6">
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">综合评分</div>
            <div className="mt-1 text-2xl font-semibold text-slate-800">{assessment.total_score ?? '-'}</div>
          </div>
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">资料完整度</div>
            <div className="mt-1 text-base font-semibold text-slate-800">
              {formatCompletenessStatus(summary.data_completeness?.status)}
            </div>
          </div>
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">资料完整度评分</div>
            <div className="mt-1 text-base font-semibold text-slate-800">{summary.data_completeness?.score ?? '-'}</div>
          </div>
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">是否建议立即申请</div>
            <div className="mt-1 text-base font-semibold text-slate-800">
              {assessment.immediate_application_recommended ? '建议立即申请' : '建议先优化后申请'}
            </div>
          </div>
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">生成时间</div>
            <div className="mt-1 text-base font-semibold text-slate-800">
              {generatedAt ? formatLocalDateTime(generatedAt) : '刚刚生成'}
            </div>
          </div>
          <div className="rounded-xl bg-white/80 px-4 py-3">
            <div className="text-xs text-slate-500">资料汇总版本</div>
            <div className="mt-1 text-base font-semibold text-slate-800">{profileVersion ? `V${profileVersion}` : '版本待确认'}</div>
            <div className="mt-1 text-[11px] text-slate-400">
              {profileUpdatedAt ? formatLocalDateTime(profileUpdatedAt) : '未记录更新时间'}
            </div>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <button
            onClick={() => void copyReport()}
            className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
          >
            {copyState === 'done' ? '已复制' : '复制报告'}
          </button>
          <button
            onClick={exportMarkdown}
            className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
          >
            导出文稿
          </button>
          <button
            onClick={printReport}
            className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
          >
            打印报告
          </button>
          <button
            onClick={() => setShowComparison((value) => !value)}
            disabled={!comparison.available && !historyLoading}
            className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {showComparison ? '收起前后对比' : '查看前后对比'}
          </button>
        </div>
      </div>

      <div className="space-y-4 p-5">
        {assessment.risk_level === 'high' && (
          <div className="rounded-2xl border border-rose-200 bg-[linear-gradient(135deg,#fff1f2_0%,#fff7ed_100%)] p-4 shadow-sm">
            <div className="flex items-start gap-3">
              <div className="rounded-full bg-rose-100 p-2 text-rose-600">
                <AlertCircle className="h-4 w-4" />
              </div>
              <div>
                <div className="text-sm font-semibold text-rose-800">高风险预警</div>
                <div className="mt-1 text-sm leading-6 text-rose-900/80">
                  当前客户风险等级为高风险，建议先处理待补资料与核心短板，再重新评估是否进入申请或方案匹配。
                </div>
              </div>
            </div>
          </div>
        )}

        {showComparison && (
          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold text-slate-800">结果对比</div>
                <div className="mt-1 text-xs text-slate-500">
                  对比当前报告与上一版报告的评分、缺失资料和建议变化。
                </div>
              </div>
              {previousReport ? (
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs text-slate-600">
                  对比上一版 V{previousReport.profile_version || '-'}
                </span>
              ) : null}
            </div>
            {historyLoading ? (
              <div className="mt-4 rounded-xl border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">
                正在读取历史报告版本...
              </div>
            ) : comparison.available && previousReport ? (
              <div className="mt-4 space-y-4">
                <div className="grid gap-3 md:grid-cols-4">
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">综合评分变化</div>
                    <div className={`mt-1 text-lg font-semibold ${comparison.scoreDelta >= 0 ? 'text-emerald-600' : 'text-rose-600'}`}>
                      {comparison.scoreDelta >= 0 ? '+' : ''}{comparison.scoreDelta}
                    </div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">完整度评分变化</div>
                    <div className={`mt-1 text-lg font-semibold ${comparison.completenessDelta >= 0 ? 'text-emerald-600' : 'text-rose-600'}`}>
                      {comparison.completenessDelta >= 0 ? '+' : ''}{comparison.completenessDelta}
                    </div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">上一版风险等级</div>
                    <div className="mt-1 text-base font-semibold text-slate-800">{formatRiskLevelLabel(comparison.previousRiskLevel)}</div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-xs text-slate-500">建议动作变化</div>
                    <div className="mt-1 text-base font-semibold text-slate-800">{comparison.actionChanged ? '已变化' : '保持一致'}</div>
                  </div>
                </div>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4">
                    <div className="text-xs font-medium text-emerald-700">已补齐资料</div>
                    <div className="mt-2 text-sm leading-6 text-slate-700">
                      {comparison.resolvedMissingItems.length > 0 ? comparison.resolvedMissingItems.join('、') : '本次未识别到已补齐资料。'}
                    </div>
                  </div>
                  <div className="rounded-xl border border-amber-200 bg-amber-50 p-4">
                    <div className="text-xs font-medium text-amber-700">新增缺口</div>
                    <div className="mt-2 text-sm leading-6 text-slate-700">
                      {comparison.addedMissingItems.length > 0 ? comparison.addedMissingItems.join('、') : '本次未出现新增缺失资料。'}
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="mt-4 rounded-xl border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">
                当前还没有可用于对比的上一版风险报告。建议先在资料更新前后各生成一次报告，再查看变化。
              </div>
            )}
          </div>
        )}

        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm font-semibold text-slate-800">客户概况</div>
            <div className="mt-3 space-y-2 text-sm text-slate-600">
              <div>所属行业：{summary.industry || '待补充'}</div>
              <div>融资需求：{summary.financing_need || '待补充'}</div>
              <div>资料版本：{profileVersion ? `V${profileVersion}` : '版本待确认'}</div>
              <div>
                缺失资料：
                {(summary.data_completeness?.missing_items || []).length > 0
                  ? ` ${(summary.data_completeness?.missing_items || []).join('、')}`
                  : ' 无'}
              </div>
            </div>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm font-semibold text-slate-800">综合结论</div>
            <div className="mt-3 text-sm leading-6 text-slate-600">
              {assessment.conclusion || '当前暂无综合结论。'}
            </div>
          </div>
        </div>

        <div className="rounded-2xl border border-slate-200 bg-white p-4">
          <div className="text-sm font-semibold text-slate-800">风险维度评估</div>
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {dimensions.map((item) => (
              <div key={item.dimension} className="rounded-xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-medium text-slate-800">{formatRiskDimensionLabel(item.dimension)}</div>
                  <div className={`rounded-full border px-2.5 py-1 text-xs font-medium ${getRiskTone(item.risk_level)}`}>
                    {formatRiskLevelLabel(item.risk_level)}
                  </div>
                </div>
                <div className="mt-3">
                  <div className="flex items-center justify-between text-xs text-slate-500">
                    <span>评分进度</span>
                    <span>{item.score} / 20</span>
                  </div>
                  <div className="mt-2 h-2 overflow-hidden rounded-full bg-slate-200">
                    <div
                      className={`h-2 rounded-full ${getRiskBarTone(item.risk_level)}`}
                      style={{ width: `${Math.max(8, Math.min(100, (item.score / 20) * 100))}%` }}
                    />
                  </div>
                </div>
                <div className="mt-2 text-sm leading-6 text-slate-600">{item.summary || '暂无说明'}</div>
                {(item.missing_info || []).length > 0 && (
                  <div className="mt-3 flex flex-wrap gap-2">
                    {item.missing_info.map((missing) => (
                      <span key={missing} className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-xs text-amber-700">
                        待补：{missing}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-semibold text-slate-800">融资方案分析</div>
            {matchedSchemes.has_match && matchedSchemes.items.length > 0 ? (
              <div className="mt-3 space-y-3">
                {matchedSchemes.items.map((item, index) => (
                  <div key={`${item.product_name}-${index}`} className="rounded-xl border border-emerald-200 bg-emerald-50 p-3">
                    <div className="text-sm font-medium text-emerald-800">{item.product_name || `方案 ${index + 1}`}</div>
                    <div className="mt-2 space-y-1 text-sm text-slate-700">
                      <div>预计额度：{item.estimated_limit || '待评估'}</div>
                      <div>预计利率：{item.estimated_rate || '待评估'}</div>
                      <div>匹配原因：{item.match_reason || '待补充'}</div>
                      <div>限制条件：{item.constraints?.length ? item.constraints.join('、') : '无'}</div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="mt-3 rounded-2xl border border-amber-200 bg-[linear-gradient(135deg,#fff7ed_0%,#fffbeb_100%)] p-4 shadow-sm">
                <div className="flex items-start gap-3">
                  <div className="mt-0.5 rounded-full bg-amber-100 p-2 text-amber-700">
                    <AlertCircle className="h-4 w-4" />
                  </div>
                  <div className="flex-1">
                    <div className="text-sm font-semibold text-amber-900">当前暂无可直接采用的匹配方案</div>
                    <div className="mt-2 text-sm leading-6 text-amber-900/80">
                      系统已结合当前资料完成方案分析，现阶段更适合先补强短板，再进入下一轮产品匹配。
                    </div>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <div className="rounded-xl bg-white/80 p-3">
                        <div className="text-xs font-medium text-amber-700">未匹配原因</div>
                        <div className="mt-2 text-sm leading-6 text-slate-700">
                          {noMatch.reasons?.join('；') || '系统未命中可直接进件方案。'}
                        </div>
                      </div>
                      <div className="rounded-xl bg-white/80 p-3">
                        <div className="text-xs font-medium text-amber-700">核心短板</div>
                        <div className="mt-2 text-sm leading-6 text-slate-700">
                          {noMatch.core_shortboards?.join('、') || '需进一步补充资料评估。'}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-semibold text-slate-800">最终建议</div>
            <div className="mt-3 space-y-2 text-sm leading-6 text-slate-600">
              <div>当前动作：{formatRecommendationActionLabel(finalRecommendation.action)}</div>
              <div>优先产品类型：{finalRecommendation.priority_product_types?.length ? finalRecommendation.priority_product_types.map((item) => formatProductTypeLabel(item)).join('、') : '待评估'}</div>
              <div>下一步：{finalRecommendation.next_steps?.length ? finalRecommendation.next_steps.join('；') : '先补齐核心资料'}</div>
            </div>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-semibold text-slate-800">优化建议</div>
            <div className="mt-4 space-y-4">
              {suggestionTimeline.map((item, index) => (
                <div key={item.title} className="flex gap-3">
                  <div className="flex flex-col items-center">
                    <div className={`flex h-8 w-8 items-center justify-center rounded-full border text-xs font-semibold ${item.tone}`}>
                      {index + 1}
                    </div>
                    {index < suggestionTimeline.length - 1 ? <div className="mt-2 h-full min-h-[28px] w-px bg-slate-200" /> : null}
                  </div>
                  <div className="flex-1 rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-sm font-medium text-slate-800">{item.title}</div>
                    <div className="mt-2 text-sm leading-6 text-slate-600">{item.description}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-semibold text-slate-800">融资规划</div>
            <div className="mt-4 space-y-4">
              {financingTimeline.map((item, index) => (
                <div key={item.title} className="flex gap-3">
                  <div className="flex flex-col items-center">
                    <div className={`flex h-8 w-8 items-center justify-center rounded-full border text-xs font-semibold ${item.tone}`}>
                      {index + 1}
                    </div>
                    {index < financingTimeline.length - 1 ? <div className="mt-2 h-full min-h-[28px] w-px bg-slate-200" /> : null}
                  </div>
                  <div className="flex-1 rounded-xl border border-slate-200 bg-slate-50 p-3">
                    <div className="text-sm font-medium text-slate-800">{item.title}</div>
                    <div className="mt-2 text-sm leading-6 text-slate-600">{item.description}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

/**
 * StructuredDataCard Component
 * 
 * Renders the appropriate data card based on intent type.
 */
const StructuredDataCard: React.FC<StructuredDataCardProps> = ({ intent, data, onNavigate }) => {
  if (!data) return null;

  let sectionTitle = '结构化结果';
  let sectionDescription = '系统已整理当前任务的结构化内容，可在下方继续核对细节。';
  let sectionIcon: React.ReactNode = <FileText className="w-4 h-4" />;
  let sectionTone = 'border-slate-200 bg-slate-50/80 text-slate-800';

  if (isRiskReportData(data)) {
    sectionTitle = '风险评估结果';
    sectionDescription = '系统已生成当前客户的风险评估报告，可重点查看综合结论、风险维度和优化建议。';
    sectionIcon = <AlertCircle className="w-4 h-4" />;
    sectionTone = 'border-amber-200 bg-amber-50/80 text-amber-800';
    return (
      <div className="mt-3 space-y-3">
        <div className={`rounded-xl border px-4 py-3 ${sectionTone}`}>
          <div className="flex items-center gap-2">
            <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-current/15 bg-white/70">
              {sectionIcon}
            </span>
            <div className="text-sm font-semibold">{sectionTitle}</div>
          </div>
          <div className="mt-2 text-xs leading-5 text-slate-600">{sectionDescription}</div>
        </div>
        <RiskReportCard report={data as unknown as CustomerRiskReportJson} />
      </div>
    );
  }

  let content: React.ReactNode = null;
  
  switch (intent) {
    case 'extract':
      sectionTitle = '资料提取结果';
      sectionDescription = '系统已按文档类型整理提取内容，右侧可直接核对字段和结构化结果。';
      sectionIcon = <FileCheck className="w-4 h-4" />;
      sectionTone = 'border-blue-200 bg-blue-50/80 text-blue-800';
      if (data.files && Array.isArray(data.files)) {
        content = <ExtractionResultCard files={data.files as ExtractionFileResult[]} />;
        break;
      }
      return null;
    
    case 'application':
      sectionTitle = '申请表结果';
      sectionDescription = '系统已整理申请表内容，支持继续核对结构化字段、版本信息和生成依据。';
      sectionIcon = <ClipboardList className="w-4 h-4" />;
      sectionTone = 'border-orange-200 bg-orange-50/80 text-orange-800';
      // Check if we have application data (JSON or Markdown)
      if (data.applicationData || data.applicationContent) {
        content = <ApplicationResultCard data={data as ApplicationResultCardProps['data']} onNavigate={onNavigate} />;
        break;
      }
      content = <ApplicationGuideCard data={data as { action?: string; requiredFields?: string[] }} onNavigate={onNavigate} />;
      break;
    
    case 'matching':
      sectionTitle = '方案匹配结果';
      sectionDescription = '系统已整理当前客户的方案匹配结果，可继续查看推荐方案、限制条件和补充建议。';
      sectionIcon = <Target className="w-4 h-4" />;
      sectionTone = 'border-emerald-200 bg-emerald-50/80 text-emerald-800';
      // Check if we have match result (JSON or Markdown) or just guide
      if (data.matchingData || data.matchResult) {
        content = <MatchingResultCard data={data as MatchingResultCardProps['data']} onNavigate={onNavigate} />;
        break;
      }
      content = <MatchingGuideCard data={data as { action?: string; requiredFields?: string[] }} onNavigate={onNavigate} />;
      break;
    
    default:
      return null;
  }

  return (
    <div className="mt-3 space-y-3">
      <div className={`rounded-xl border px-4 py-3 ${sectionTone}`}>
        <div className="flex items-center gap-2">
          <span className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-current/15 bg-white/70">
            {sectionIcon}
          </span>
          <div className="text-sm font-semibold">{sectionTitle}</div>
        </div>
        <div className="mt-2 text-xs leading-5 text-slate-600">{sectionDescription}</div>
      </div>
      {content}
    </div>
  );
};

// ============================================
// File Attachment Display Component
// ============================================

/** File info embedded in message content */
interface FileInfo {
  name: string;
  type: string;
}

/**
 * Parse file info from message content
 * Format: [FILE:name:type]
 */
function parseFileInfoFromContent(content: string): { text: string; files: FileInfo[] } {
  const filePattern = /\[FILE:([^:]+):([^\]]+)\]/g;
  const files: FileInfo[] = [];
  let match;
  
  while ((match = filePattern.exec(content)) !== null) {
    files.push({ name: match[1], type: match[2] });
  }
  
  const text = content.replace(filePattern, '').trim();
  return { text, files };
}

/**
 * Get file icon based on file type/extension
 */
function getFileIcon(fileName: string, fileType: string): React.ReactNode {
  const ext = fileName.split('.').pop()?.toLowerCase() || '';
  
  // PDF
  if (ext === 'pdf' || fileType.includes('pdf')) {
    return <FileText className="w-4 h-4 text-red-500" />;
  }
  // Excel
  if (['xlsx', 'xls'].includes(ext) || fileType.includes('spreadsheet') || fileType.includes('excel')) {
    return <FileSpreadsheet className="w-4 h-4 text-green-600" />;
  }
  // Word
  if (['doc', 'docx'].includes(ext) || fileType.includes('word')) {
    return <FileText className="w-4 h-4 text-blue-600" />;
  }
  // Image
  if (['png', 'jpg', 'jpeg', 'gif', 'webp'].includes(ext) || fileType.startsWith('image/')) {
    return <Image className="w-4 h-4 text-purple-500" />;
  }
  // Default
  return <File className="w-4 h-4 text-gray-500" />;
}

/**
 * FileAttachmentList Component
 * Displays file attachments in message bubble
 */
const FileAttachmentList: React.FC<{ files: FileInfo[]; isUser: boolean }> = ({ files, isUser }) => {
  if (files.length === 0) return null;
  
  return (
    <div className="flex flex-col gap-1.5">
      {files.map((file, index) => (
        <div 
          key={`${file.name}-${index}`}
          className={`flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs ${
            isUser 
              ? 'bg-blue-400 bg-opacity-30' 
              : 'bg-gray-200'
          }`}
        >
          {getFileIcon(file.name, file.type)}
          <span className="truncate max-w-[180px]" title={file.name}>
            {file.name}
          </span>
        </div>
      ))}
    </div>
  );
};

// ============================================
// Message Bubble Component
// Feature: frontend-ui-optimization
// Property 7: Message Bubble Styling Based on Role
// ============================================

interface MessageBubbleProps {
  message: ChatMessageWithReasoning;
  isTyping?: boolean;
  onNavigate?: (page: string) => void;
}

/**
 * MessageBubble Component
 * 
 * Renders chat messages with role-based styling.
 * - User messages: blue background (#3B82F6), right-aligned, rounded 18px 18px 4px 18px
 * - AI messages: gray background (#F3F4F6), left-aligned, rounded 18px 18px 18px 4px
 * - AI messages with reasoning: show collapsible thinking process above the message
 * - AI messages with structured data: show data cards below the message
 * 
 * Feature: frontend-ui-optimization
 * Property 7: Message Bubble Styling Based on Role
 * Validates: Requirements 5.2, 5.3
 */
const MessageBubble: React.FC<MessageBubbleProps> = ({ message, isTyping, onNavigate }) => {
  const { text, files } = parseFileInfoFromContent(message.content);
  
  if (message.role === 'user') {
    return (
      <div 
        className="flex justify-end mb-4"
        data-testid="message-bubble-user"
        data-role="user"
      >
        <div 
          className="text-white px-4 py-3 max-w-[70%] text-sm"
          style={{
            backgroundColor: USER_MESSAGE_STYLE.bgColor,
            borderRadius: USER_MESSAGE_STYLE.borderRadius,
          }}
          data-testid="user-message-content"
        >
          {/* File attachments */}
          {files.length > 0 && (
            <div className={text ? 'mb-2' : ''}>
              <FileAttachmentList files={files} isUser={true} />
            </div>
          )}
          {/* Text content */}
          {text && <div className="whitespace-pre-wrap">{text}</div>}
        </div>
      </div>
    );
  }

  return (
    <div 
      className="flex gap-3 mb-4 items-start"
      data-testid="message-bubble-assistant"
      data-role="assistant"
    >
      {/* AI Avatar */}
      <div 
        className="flex items-center justify-center flex-shrink-0"
        style={{
          width: AI_AVATAR_STYLE.size,
          height: AI_AVATAR_STYLE.size,
          backgroundColor: AI_AVATAR_STYLE.bgColor,
          borderRadius: '50%',
        }}
        data-testid="ai-avatar"
      >
        <BotIcon className="w-[18px] h-[18px] text-white" />
      </div>
      {/* AI Message Content */}
      <div className="flex flex-col max-w-[85%]">
        {/* Reasoning Collapse - shown above message if available */}
        {message.reasoning && (
          <ReasoningCollapse reasoning={message.reasoning} />
        )}
        <div 
          className="text-gray-800 px-4 py-3 text-sm"
          style={{
            backgroundColor: AI_MESSAGE_STYLE.bgColor,
            borderRadius: AI_MESSAGE_STYLE.borderRadius,
          }}
          data-testid="ai-message-content"
        >
          {isTyping && (
            <div className="flex gap-2 items-center pb-2">
              <Loader2 className="w-4 h-4 animate-spin text-gray-400" />
              <span className="text-gray-400 text-xs">正在输入...</span>
            </div>
          )}
          {/* 使用 ReactMarkdown 渲染 AI 消息，支持 Markdown 格式 */}
          <div className="prose prose-sm max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5 prose-headings:my-2 prose-hr:my-2">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {message.content}
            </ReactMarkdown>
          </div>
        </div>
        {/* Structured Data Card - shown below message based on intent */}
        {message.data && (
          <StructuredDataCard 
            intent={message.intent} 
            data={message.data} 
            onNavigate={onNavigate}
          />
        )}
      </div>
    </div>
  );
};

interface IntentActionsProps {
  intent: ChatResponse['intent'];
  onAction: (action: string) => void;
}

const IntentActions: React.FC<IntentActionsProps> = ({ intent, onAction }) => {
  if (!intent || intent === 'chat') return null;

  const actions = {
    extract: { icon: Upload, label: '上传资料', action: 'upload' },
    application: { icon: ClipboardList, label: '生成申请表', action: 'application' },
    matching: { icon: Target, label: '匹配方案', action: 'matching' },
  };

  const actionConfig = actions[intent];
  if (!actionConfig) return null;

  const Icon = actionConfig.icon;

  return (
    <div className="flex gap-2 mt-3 pt-3 border-t border-gray-200">
      <button
        onClick={() => onAction(actionConfig.action)}
        className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-lg text-sm hover:bg-blue-600 transition-colors"
      >
        <Icon className="w-4 h-4" />
        {actionConfig.label}
      </button>
    </div>
  );
};

interface FilePreviewProps {
  files: File[];
  onRemove: (index: number) => void;
}

const FilePreview: React.FC<FilePreviewProps> = ({ files, onRemove }) => {
  if (files.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2 px-4 py-2 border-t border-gray-200">
      {files.map((file, index) => (
        <div
          key={`${file.name}-${index}`}
          className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 rounded-lg text-sm"
        >
          <FileText className="w-4 h-4 text-gray-500" />
          <span className="max-w-[150px] truncate">{file.name}</span>
          <button
            onClick={() => onRemove(index)}
            className="p-0.5 hover:bg-gray-200 rounded"
          >
            <X className="w-3 h-3 text-gray-500" />
          </button>
        </div>
      ))}
    </div>
  );
};

const TypingIndicator: React.FC = () => (
  <div className="flex gap-3 mb-4 items-start" data-testid="typing-indicator">
    {/* AI Avatar */}
    <div 
      className="flex items-center justify-center flex-shrink-0"
      style={{
        width: AI_AVATAR_STYLE.size,
        height: AI_AVATAR_STYLE.size,
        backgroundColor: AI_AVATAR_STYLE.bgColor,
        borderRadius: '50%',
      }}
    >
      <BotIcon className="w-[18px] h-[18px] text-white" />
    </div>
    <div 
      className="px-4 py-3"
      style={{
        backgroundColor: AI_MESSAGE_STYLE.bgColor,
        borderRadius: AI_MESSAGE_STYLE.borderRadius,
      }}
    >
      <div className="flex gap-1">
        <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
        <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
        <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
      </div>
    </div>
  </div>
);

const WelcomeMessage: React.FC = () => (
  <div className="flex flex-col items-center gap-6 py-8 text-center" data-testid="welcome-message">
    <div 
      className="w-20 h-20 flex items-center justify-center mb-4"
      style={{
        backgroundColor: AI_AVATAR_STYLE.bgColor,
        borderRadius: '50%',
      }}
    >
      <SparklesIcon className="w-10 h-10 text-white" />
    </div>
    <div className="text-gray-700 text-base leading-relaxed">
      你好！我是智能助手，可以帮你处理贷款相关问题。<br />
      你可以上传资料、生成申请表，或直接问我任何问题。
    </div>
  </div>
);

// Icon Components
const SparklesIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 3l1.5 4.5L18 9l-4.5 1.5L12 15l-1.5-4.5L6 9l4.5-1.5L12 3z" />
    <path d="M5 19l1 3 1-3 3-1-3-1-1-3-1 3-3 1 3 1z" />
    <path d="M19 5l1 2 1-2 2-1-2-1-1-2-1 2-2 1 2 1z" />
  </svg>
);

const BotIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="11" width="18" height="10" rx="2" />
    <circle cx="12" cy="5" r="2" />
    <path d="M12 7v4" />
    <line x1="8" y1="16" x2="8" y2="16" />
    <line x1="16" y1="16" x2="16" y2="16" />
  </svg>
);

// ============================================
// Main Component
// ============================================

interface ChatPageProps {
  /** Callback when user wants to navigate to another page */
  onNavigate?: (page: string) => void;
}

const CHAT_JOB_STORAGE_KEY = 'loan-assistant-chat-job';
const CHAT_JOB_COMPLETED_STORAGE_KEY = 'loan-assistant-chat-job-completed';
const CHAT_JOB_POLL_INTERVAL_MS = 2000;
const CHAT_JOB_MAX_POLL_MS = 5 * 60 * 1000;
const CHAT_JOB_MAX_FAILURES = 3;
const CHAT_JOB_STALE_MS = 10 * 60 * 1000;

interface PendingChatJobState {
  jobId: string;
  customerId: string | null;
  customerName: string | null;
  createdAt: string;
  requestMessages: ChatMessageWithReasoning[];
}

interface ChatJobCompletionTarget {
  jobId: string;
  jobType: string;
  customerId: string | null;
  customerName: string | null;
  targetPage: string | null;
  actionLabel: string;
}

function isRunningJobStale(job: Pick<ChatJobSummaryResponse, 'status' | 'startedAt' | 'createdAt'>): boolean {
  if (job.status !== 'running') {
    return false;
  }
  const base = job.startedAt || job.createdAt;
  if (!base) {
    return false;
  }
  const startedAt = new Date(base).getTime();
  if (Number.isNaN(startedAt)) {
    return false;
  }
  return Date.now() - startedAt > CHAT_JOB_STALE_MS;
}

function getReadableChatJobProgress(job: Pick<ChatJobSummaryResponse, 'status' | 'progressMessage' | 'errorMessage' | 'startedAt' | 'createdAt' | 'jobType'>): string {
  return getReadableJobProgress(job, isRunningJobStale(job as ChatJobSummaryResponse));
}

/**
 * ChatPage Component
 * 
 * Main chat interface for interacting with the AI assistant.
 * Supports text messages and file attachments.
 * 
 * Feature: frontend-backend-integration
 * Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7
 */
const ChatPage: React.FC<ChatPageProps> = ({ onNavigate }) => {
  // Local state for conversation
  const [messages, setMessages] = useState<ChatMessageWithReasoning[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [attachedFiles, setAttachedFiles] = useState<File[]>([]);
  const [lastIntent, setLastIntent] = useState<ChatResponse['intent']>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [ragMode, setRagMode] = useState(false);
  const [customerOptions, setCustomerOptions] = useState<CustomerListItem[]>([]);
  const [customersLoading, setCustomersLoading] = useState(false);

  const [mergeModal, setMergeModal] = useState<{
    customerName: string;
    candidates: { customer_id: string; name: string; shared_keywords: string[] }[];
    pendingMessage: string;
    pendingFiles: ChatFile[] | null;
    pendingMessages: ChatMessageWithReasoning[];
  } | null>(null);

  // Hooks
  const { loading, error, execute, reset: resetChatRequestState } = useLoading<ChatResponse>();
  const { loading: ragLoading, error: ragError, execute: executeRag, reset: resetRagState } = useLoading<CustomerRagChatResponse>();
  const { getSignal } = useAbortController();
  const { addChatMessage, state, setApplicationResult, setChatTaskStatus, setCurrentCustomer, setSchemeResult, recordSystemActivity } = useApp();
  const currentCustomerId = state.extraction.currentCustomerId;
  const currentCustomerName = state.extraction.currentCustomer;
  const [chatJobPolling, setChatJobPolling] = useState(false);
  const busy = loading || ragLoading || chatJobPolling;
  const [chatJobSubmitError, setChatJobSubmitError] = useState<string | null>(null);
  const [chatJobPollError, setChatJobPollError] = useState<string | null>(null);
  const [chatJobPollErrorJobId, setChatJobPollErrorJobId] = useState<string | null>(null);
  const [chatJobFeedback, setChatJobFeedback] = useState<{
    tone: ProcessFeedbackTone;
    title: string;
    description: string;
    persistenceHint: string;
    nextStep: string;
  } | null>(null);
  const activeError = chatJobFeedback
    ? null
    : (chatJobPollError ? new Error(chatJobPollError) : chatJobSubmitError ? new Error(chatJobSubmitError) : (error || ragError));
  const [riskFeedback, setRiskFeedback] = useState<{
    tone: ProcessFeedbackTone;
    title: string;
    description: string;
    persistenceHint: string;
    nextStep: string;
  } | null>(null);
  const [recentChatJobs, setRecentChatJobs] = useState<ChatJobSummaryResponse[]>([]);
  const [jobsLoading, setJobsLoading] = useState(false);
  const [jobFilterMode, setJobFilterMode] = useState<'current' | 'all'>('current');
  const [showRecentJobs, setShowRecentJobs] = useState(true);
  const [collapsedJobGroups, setCollapsedJobGroups] = useState<Record<string, boolean>>({
    running: false,
    success: true,
    failed: false,
  });
  const [latestCompletedChatJob, setLatestCompletedChatJob] = useState<ChatJobCompletionTarget | null>(null);
  const [currentJob, setCurrentJob] = useState<ChatJobSummaryResponse | null>(null);
  const [chatJobResult, setChatJobResult] = useState<Record<string, unknown> | null>(null);
  
  // Refs
  const resultTopRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const chatJobPollTimeoutRef = useRef<number | null>(null);
  const chatJobFailureCountRef = useRef(0);
  const completedChatJobIdsRef = useRef<Set<string>>(new Set());
  // Ref to track if recovery is in progress
  const isRecoveringRef = useRef(false);
  // Ref to track if initial recovery check has been done
  const hasCheckedRecoveryRef = useRef(false);
  // Ref to store the latest send function to avoid closure issues (踩坑点 #31)
  const sendRef = useRef<((msg: string, files: ChatFile[] | null, currentMessages: ChatMessageWithReasoning[]) => Promise<void>) | null>(null);

  const persistPendingChatJob = useCallback((payload: PendingChatJobState) => {
    try {
      window.localStorage.setItem(CHAT_JOB_STORAGE_KEY, JSON.stringify(payload));
    } catch (storageError) {
      console.warn('Failed to persist pending chat job:', storageError);
    }
  }, []);

  const clearPendingChatJob = useCallback(() => {
    try {
      window.localStorage.removeItem(CHAT_JOB_STORAGE_KEY);
    } catch (storageError) {
      console.warn('Failed to clear pending chat job:', storageError);
    }
  }, []);

  const readPendingChatJob = useCallback((): PendingChatJobState | null => {
    try {
      const raw = window.localStorage.getItem(CHAT_JOB_STORAGE_KEY);
      if (!raw) {
        return null;
      }
      const parsed = JSON.parse(raw) as PendingChatJobState;
      if (!parsed?.jobId || !parsed?.createdAt || !Array.isArray(parsed?.requestMessages)) {
        return null;
      }
      return parsed;
    } catch (storageError) {
      console.warn('Failed to read pending chat job:', storageError);
      return null;
    }
  }, []);

  const clearAsyncChatJobErrors = useCallback(() => {
    setChatJobSubmitError(null);
    setChatJobPollError(null);
    setChatJobPollErrorJobId(null);
  }, []);

  const clearObsoletePollFailureForJob = useCallback((jobId: string) => {
    if (!jobId || chatJobPollErrorJobId !== jobId) {
      return;
    }
    setChatJobPollError(null);
    setChatJobPollErrorJobId(null);
    setChatJobFeedback((prev) => {
      if (!prev) {
        return prev;
      }
      const transientFailureTitles = new Set([
        '任务状态获取失败',
        '正在重新连接任务状态',
        '任务处理时间较长',
        '任务可能已中断',
      ]);
      if ((prev.tone === 'error' || prev.tone === 'partial') && transientFailureTitles.has(prev.title)) {
        return null;
      }
      return prev;
    });
  }, [chatJobPollErrorJobId]);

  const loadCompletedChatJobIds = useCallback(() => {
    try {
      const raw = window.localStorage.getItem(CHAT_JOB_COMPLETED_STORAGE_KEY);
      if (!raw) {
        completedChatJobIdsRef.current = new Set();
        return;
      }
      const ids = JSON.parse(raw) as string[];
      completedChatJobIdsRef.current = new Set(Array.isArray(ids) ? ids : []);
    } catch (storageError) {
      console.warn('Failed to load completed chat job ids:', storageError);
      completedChatJobIdsRef.current = new Set();
    }
  }, []);

  const markChatJobCompleted = useCallback((jobId: string) => {
    completedChatJobIdsRef.current.add(jobId);
    try {
      window.localStorage.setItem(
        CHAT_JOB_COMPLETED_STORAGE_KEY,
        JSON.stringify(Array.from(completedChatJobIdsRef.current).slice(-50)),
      );
    } catch (storageError) {
      console.warn('Failed to persist completed chat job ids:', storageError);
    }
  }, []);

  const restoreCompletedChatJobFeedback = useCallback((job: ChatJobSummaryResponse) => {
    const successAction = getJobSuccessAction(job.jobType, job.targetPage);
    setLatestCompletedChatJob({
      jobId: job.jobId,
      jobType: job.jobType,
      customerId: job.customerId || null,
      customerName: job.customerName || null,
      targetPage: successAction.targetPage,
      actionLabel: successAction.actionLabel,
    });
    setChatJobFeedback({
      tone: 'success',
      title: `${getJobTypeLabel(job.jobType, job.jobTypeLabel)}已完成`,
      description: getJobResultSummary(job.jobType, undefined, job.customerName, job.resultSummary) || getReadableChatJobProgress(job),
      persistenceHint: '主流程已生成成功。',
      nextStep: job.jobType === 'chat_extract'
        ? '建议继续核对提取结果，并前往资料汇总查看最新变化。'
        : '可继续查看结果，或跳转到对应业务页面处理。',
    });
  }, []);

  const buildJobSummaryFromStatus = useCallback((jobStatus: ChatJobStatusResponse): ChatJobSummaryResponse => ({
    jobId: jobStatus.jobId,
    jobType: jobStatus.jobType,
    jobTypeLabel: jobStatus.jobTypeLabel,
    customerId: jobStatus.customerId,
    customerName: jobStatus.customerName,
    status: jobStatus.status,
    progressMessage: jobStatus.progressMessage,
    errorMessage: jobStatus.errorMessage ?? null,
    createdAt: jobStatus.createdAt,
    startedAt: jobStatus.startedAt,
    finishedAt: jobStatus.finishedAt,
    targetPage: jobStatus.targetPage ?? null,
    resultSummary: jobStatus.resultSummary ?? null,
  }), []);

  const restoreCompletedJobFromStatus = useCallback((jobStatus: ChatJobStatusResponse) => {
    const recoveredJob = buildJobSummaryFromStatus(jobStatus);
    setCurrentJob(recoveredJob);
    setChatJobResult(jobStatus.result ?? null);
    clearObsoletePollFailureForJob(recoveredJob.jobId);
    restoreCompletedChatJobFeedback(recoveredJob);
  }, [buildJobSummaryFromStatus, clearObsoletePollFailureForJob, restoreCompletedChatJobFeedback]);

  const consumeCompletedJobResult = useCallback((
    jobStatus: ChatJobStatusResponse,
    requestMessages: ChatMessageWithReasoning[],
    resolvedCustomerId: string | null,
    resolvedCustomerName: string | null,
  ) => {
    clearAsyncChatJobErrors();
    resetChatRequestState();
    resetRagState();
    const result = jobStatus.result ?? null;
    const successAction = getJobSuccessAction(jobStatus.jobType, jobStatus.targetPage);
    const completedJobSummary: ChatJobSummaryResponse = {
      jobId: jobStatus.jobId,
      jobType: jobStatus.jobType,
      jobTypeLabel: jobStatus.jobTypeLabel,
      customerId: resolvedCustomerId ?? jobStatus.customerId,
      customerName: resolvedCustomerName ?? jobStatus.customerName,
      status: jobStatus.status,
      progressMessage: jobStatus.progressMessage,
      errorMessage: jobStatus.errorMessage ?? null,
      createdAt: jobStatus.createdAt,
      startedAt: jobStatus.startedAt,
      finishedAt: jobStatus.finishedAt,
      targetPage: successAction.targetPage,
      resultSummary: jobStatus.resultSummary ?? null,
    };
    setCurrentJob(completedJobSummary);
    setChatJobResult(result);
    setLatestCompletedChatJob({
      jobId: jobStatus.jobId,
      jobType: jobStatus.jobType,
      customerId: resolvedCustomerId,
      customerName: resolvedCustomerName,
      targetPage: successAction.targetPage,
      actionLabel: successAction.actionLabel,
    });

    if (completedChatJobIdsRef.current.has(jobStatus.jobId)) {
      return;
    }

    if (jobStatus.jobType === 'chat_extract' && result) {
      const response = result as unknown as ChatResponse;
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: response.message,
        reasoning: response.reasoning,
        intent: response.intent,
        data: response.data,
      };
      setMessages((prev) => {
        const baseMessages = prev.length > 0 ? prev : requestMessages;
        return [...baseMessages, assistantMessage];
      });
      addChatMessage(assistantMessage);
      setLastIntent(response.intent);
    }

    if (jobStatus.jobType === 'risk_report' && result) {
      const response = result as unknown as CustomerRiskReportResponse;
      const overall = response.report_json?.overall_assessment;
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: `已生成当前客户的风险评估报告。综合结论为：${formatRiskLevelLabel(overall?.risk_level)}，综合评分 ${overall?.total_score ?? '-'} 分。`,
        data: {
          ...(response.report_json as unknown as Record<string, unknown>),
          generated_at: response.generated_at,
          profile_version: response.profile_version,
          profile_updated_at: response.profile_updated_at,
          previous_report: response.previous_report,
        },
      };
      setMessages((prev) => {
        const baseMessages = prev.length > 0 ? prev : requestMessages;
        return [...baseMessages, assistantMessage];
      });
      addChatMessage(assistantMessage);
    }

    if (jobStatus.jobType === 'application_generate' && result) {
      const response = result as unknown as import('../services/types').ApplicationResponse;
      setApplicationResult(
        {
          content: response.applicationContent,
          customerFound: response.customerFound,
          warnings: response.warnings,
          applicationData: response.applicationData,
          metadata: response.metadata,
        },
        resolvedCustomerName ?? undefined,
      );
    }

    if (jobStatus.jobType === 'scheme_match' && result) {
      const response = result as unknown as import('../services/types').SchemeMatchResponse & { creditType?: string };
      setSchemeResult({
        result: response.matchResult,
        lastCreditType: response.creditType ?? null,
        customerId: resolvedCustomerId,
        customerName: resolvedCustomerName,
        matchedAt: jobStatus.finishedAt || new Date().toISOString(),
        stale: false,
        staleReason: '',
        staleAt: '',
      });
    }

    markChatJobCompleted(jobStatus.jobId);
  }, [addChatMessage, clearAsyncChatJobErrors, markChatJobCompleted, resetChatRequestState, resetRagState, setApplicationResult, setSchemeResult]);

  const loadRecentChatJobs = useCallback(async () => {
    setJobsLoading(true);
    try {
      const jobs = await listChatJobs(8, getSignal());
      setRecentChatJobs(jobs);
      const shouldRecoverSuccessfulJob = (!currentJob?.jobId || Boolean(chatJobPollErrorJobId)) && !chatJobSubmitError && !chatJobPolling;
      if (shouldRecoverSuccessfulJob) {
        const recoveredJob = chatJobPollErrorJobId
          ? jobs.find((job) => job.jobId === chatJobPollErrorJobId && job.status === 'success')
          : jobs.find((job) => job.status === 'success');
        if (recoveredJob) {
          try {
            const recoveredStatus = await getChatJobStatus(recoveredJob.jobId, getSignal());
            if (recoveredStatus.status === 'success') {
              restoreCompletedJobFromStatus(recoveredStatus);
            }
          } catch (recoverError) {
            console.warn('Failed to restore completed chat job result:', recoverError);
          }
        }
      }
    } catch (jobsError) {
      console.error('Failed to load recent chat jobs:', jobsError);
      setRecentChatJobs([]);
    } finally {
      setJobsLoading(false);
    }
  }, [chatJobPollErrorJobId, chatJobPolling, chatJobSubmitError, currentJob?.jobId, getSignal, restoreCompletedJobFromStatus]);

  /**
   * Send message with given parameters
   * Extracted to avoid closure issues during recovery
   */
  const doSend = useCallback(async (
    messageContent: string,
    chatFiles: ChatFile[] | null,
    currentMessages: ChatMessageWithReasoning[],
    mergeDecisions?: Record<string, string>,
  ) => {
    // Create user message
    const userMessage: ChatMessage = {
      role: 'user',
      content: messageContent,
    };

    // Update local state immediately
    const newMessages = [...currentMessages, userMessage];
    setMessages(newMessages);
    addChatMessage(userMessage);

    if (chatFiles && chatFiles.length > 0) {
      resetChatRequestState();
      resetRagState();
      clearAsyncChatJobErrors();
      setChatJobFeedback({
        tone: 'processing',
        title: '资料提取任务已提交',
        description: '系统已接收本次资料提取任务，正在后台排队处理。',
        persistenceHint: '主流程已进入后台处理，页面会自动轮询状态。',
        nextStep: '请稍候，完成后会自动展示提取结果。',
      });

      try {
        const job = await createChatJob(
          {
            messages: newMessages,
            files: chatFiles,
            customerId: currentCustomerId,
            customerName: currentCustomerName,
            mergeDecisions,
          },
          getSignal(),
        );
        setChatJobSubmitError(null);
        setChatJobPollError(null);
        persistPendingChatJob({
          jobId: job.jobId,
          customerId: currentCustomerId,
          customerName: currentCustomerName,
          createdAt: new Date().toISOString(),
          requestMessages: newMessages,
        });
        void loadRecentChatJobs();
        setChatTaskStatus('idle', null, null);
        await pollChatJob(job.jobId, newMessages, { startedAt: Date.now() });
      } catch (jobError) {
        console.error('Failed to create chat job:', jobError);
        clearPendingChatJob();
        const submitMessage = jobError instanceof Error ? jobError.message : '本次提取任务未能成功提交。';
        setChatJobSubmitError(submitMessage);
        setChatJobPollError(null);
        setChatJobFeedback({
          tone: 'error',
          title: '资料提取任务提交失败',
          description: submitMessage,
          persistenceHint: '主流程尚未进入后台处理。',
          nextStep: '请检查网络或稍后重试一次。',
        });
        setChatTaskStatus('idle', null, null);
      }
      return;
    }

    // Send to API
    const { data: response, error: execError } = await execute(async () => {
      return sendChat(
        {
          messages: newMessages,
          files: chatFiles || undefined,
          customerId: currentCustomerId,
          customerName: currentCustomerName,
          mergeDecisions,
        },
        getSignal()
      );
    });

    if (response) {
      // Check if any file has similarCustomers (needs merge decision)
      const filesWithSimilar: Array<{
        customerName?: string;
        similarCustomers?: { customer_id: string; name: string; shared_keywords: string[] }[];
      }> = [];

      if (filesWithSimilar && filesWithSimilar.length > 0) {
        // Show merge modal for the first unresolved file
        const first = filesWithSimilar[0];
        setMergeModal({
          customerName: first.customerName || '',
          candidates: first.similarCustomers!,
          pendingMessage: messageContent,
          pendingFiles: chatFiles,
          pendingMessages: currentMessages,
        });
        // Don't add assistant message yet — wait for user decision
        return;
      }

      // Add assistant response with reasoning, intent, and data
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: response.message,
        reasoning: response.reasoning,
        intent: response.intent,
        data: response.data,
      };
      setMessages(prev => [...prev, assistantMessage]);
      addChatMessage(assistantMessage);
      setLastIntent(response.intent);
      if (response.intent === 'application') {
        recordSystemActivity({
          type: 'application',
          title: 'AI 对话已生成申请表',
          description: '系统已按当前客户上下文完成申请表生成。',
          customerName: currentCustomerName,
          customerId: currentCustomerId,
          status: 'success',
        });
      } else if (response.intent === 'matching') {
        recordSystemActivity({
          type: 'matching',
          title: 'AI 对话已完成方案匹配',
          description: '系统已按当前客户上下文完成方案匹配。',
          customerName: currentCustomerName,
          customerId: currentCustomerId,
          status: 'success',
        });
      }
      // Mark task as done
      setChatTaskStatus('done', null, null);
    } else {
      // Check if it was an abort (page switch) vs real error
      // 使用 execute 返回的 error，避免闭包问题（踩坑点 #33）
      const isAbortError = execError?.name === 'AbortError';
      if (!isAbortError) {
        // Real failure, reset status
        setChatTaskStatus('idle', null, null);
      }
      // If aborted, keep 'sending' status for recovery
    }
  }, [execute, getSignal, addChatMessage, currentCustomerId, currentCustomerName, recordSystemActivity, setChatTaskStatus, pollChatJob, persistPendingChatJob, clearPendingChatJob, loadRecentChatJobs, clearAsyncChatJobErrors, resetChatRequestState, resetRagState]);

  async function pollChatJob(
    jobId: string,
    requestMessages: ChatMessageWithReasoning[],
    options?: { startedAt?: number; restored?: boolean; customerId?: string | null; customerName?: string | null },
  ) {
    const startedAt = options?.startedAt ?? Date.now();
    chatJobFailureCountRef.current = 0;
    setChatJobPolling(true);
    setChatJobSubmitError(null);
    setChatJobPollError(null);

    const waitForNextPoll = (): Promise<void> =>
      new Promise((resolve) => {
        chatJobPollTimeoutRef.current = window.setTimeout(() => {
          chatJobPollTimeoutRef.current = null;
          resolve();
        }, CHAT_JOB_POLL_INTERVAL_MS);
      });

    const fetchJobStatus = async (): Promise<ChatJobStatusResponse | null> => {
      try {
        return await getChatJobStatus(jobId, getSignal());
      } catch (pollError) {
        console.error('Failed to poll chat job status:', pollError);
        chatJobFailureCountRef.current += 1;
        return null;
      }
    };

    try {
      let status: ChatJobStatusResponse | null = await fetchJobStatus();

      while (true) {
        if (Date.now() - startedAt > CHAT_JOB_MAX_POLL_MS) {
          clearPendingChatJob();
          setChatJobFeedback({
            tone: 'partial',
            title: '任务处理时间较长',
            description: '本次任务仍未完成，已停止自动轮询。',
            persistenceHint: '后台任务可能仍在继续执行。',
            nextStep: '请稍后重新进入页面查看，或在最近任务列表中继续查看。',
          });
          setChatTaskStatus('idle', null, null);
          return;
        }

        if (!status) {
          if (chatJobFailureCountRef.current >= CHAT_JOB_MAX_FAILURES) {
            clearPendingChatJob();
            setChatJobPollError('任务状态获取失败');
            setChatJobPollErrorJobId(jobId);
            setChatJobFeedback({
              tone: 'error',
              title: '任务状态获取失败',
              description: '连续多次获取任务状态失败，已停止自动轮询。',
              persistenceHint: '后台任务可能仍在继续执行，但当前页面无法确认最终结果。',
              nextStep: '请稍后刷新页面重试，或前往资料汇总查看是否已写入资料。',
            });
            setChatTaskStatus('idle', null, null);
            return;
          }

          setChatJobFeedback({
            tone: 'partial',
            title: '正在重新连接任务状态',
            description: '当前网络波动，系统正在尝试重新获取任务进度。',
            persistenceHint: '后台任务仍可能在继续执行。',
            nextStep: '请保持页面打开，系统会继续自动重试。',
          });
          await waitForNextPoll();
          status = await fetchJobStatus();
          continue;
        }

        chatJobFailureCountRef.current = 0;
        setChatJobPollError(null);
        setChatJobPollErrorJobId(null);

        if (isRunningJobStale(status)) {
          clearPendingChatJob();
          setChatJobPollError(null);
          setChatJobPollErrorJobId(null);
          setChatJobFeedback({
            tone: 'partial',
            title: '任务可能已中断',
            description: '这条任务已运行较长时间，系统判断它可能没有继续推进。',
            persistenceHint: '后台任务可能已中断，当前页面已停止自动轮询。',
            nextStep: '建议重新提交一次资料提取任务。',
          });
          setChatTaskStatus('idle', null, null);
          return;
        }

        if (status.status === 'pending' || status.status === 'running') {
          setChatJobFeedback({
            tone: 'processing',
            title: options?.restored ? `已恢复${getJobTypeLabel(status.jobType, status.jobTypeLabel)}` : `${getJobTypeLabel(status.jobType, status.jobTypeLabel)}处理中`,
            description: getReadableChatJobProgress(status),
            persistenceHint: '主流程已进入后台处理，本页会自动轮询最新结果。',
            nextStep: '请稍候，完成后会自动展示结果或提供对应业务页跳转。',
          });
          await waitForNextPoll();
          status = await fetchJobStatus();
          continue;
        }

        if (status.status === 'success' && status.result) {
          if (status.jobType === 'chat_extract') {
            clearPendingChatJob();
          }
          setChatJobSubmitError(null);
          setChatJobPollError(null);
          setChatJobPollErrorJobId(null);
          const resolvedCustomerId = status.customerId || options?.customerId || currentCustomerId;
          const resolvedCustomerName =
            status.customerName ||
            options?.customerName ||
            customerOptions.find((item) => item.record_id === resolvedCustomerId)?.name ||
            currentCustomerName ||
            null;
          consumeCompletedJobResult(status, requestMessages, resolvedCustomerId ?? null, resolvedCustomerName);
          setChatJobFeedback({
            tone: 'success',
            title: `${getJobTypeLabel(status.jobType, status.jobTypeLabel)}已完成`,
            description: getJobResultSummary(status.jobType, status.result as Record<string, unknown> | null | undefined, status.customerName, status.resultSummary) || getReadableChatJobProgress(status),
            persistenceHint: '主流程已生成成功。',
            nextStep: status.jobType === 'chat_extract' ? '建议继续核对提取结果，并前往资料汇总查看最新变化。' : '可继续查看结果，或跳转到对应业务页面处理。',
          });
          recordSystemActivity(
            status.jobType === 'risk_report'
              ? {
                  type: 'risk',
                  title: '风险评估报告已完成',
                  description: '系统已在后台完成风险评估并生成报告。',
                  customerName: resolvedCustomerName,
                  customerId: resolvedCustomerId,
                  status: 'success',
                }
              : status.jobType === 'scheme_match'
                ? {
                    type: 'matching',
                    title: '方案匹配已完成',
                    description: '系统已在后台完成融资方案匹配。',
                    customerName: resolvedCustomerName,
                    customerId: resolvedCustomerId,
                    status: 'success',
                  }
                : status.jobType === 'application_generate'
                  ? {
                      type: 'application',
                      title: '申请表生成已完成',
                      description: '系统已在后台生成最新申请表。',
                      customerName: resolvedCustomerName,
                      customerId: resolvedCustomerId,
                      status: 'success',
                    }
                  : {
                      type: 'upload',
                      title: 'AI 对话资料提取已完成',
                      description: '系统已在后台完成资料提取、保存和资料汇总同步。',
                      customerName: resolvedCustomerName,
                      customerId: resolvedCustomerId,
                      status: 'success',
                    },
          );
          setChatTaskStatus('done', null, null);
          return;
        }

        if (status.jobType === 'chat_extract') {
          clearPendingChatJob();
        }
        setChatJobSubmitError(null);
        setChatJobPollError(null);
        setChatJobPollErrorJobId(null);
        setChatJobFeedback({
          tone: 'error',
          title: `${getJobTypeLabel(status.jobType, status.jobTypeLabel)}失败`,
          description: status.errorMessage || '本次任务未成功完成。',
          persistenceHint: '主流程未成功完成。',
          nextStep: '请检查当前资料或网络状态后重试，必要时缩小单次处理范围。',
        });
        setChatTaskStatus('idle', null, null);
        if (status.jobType === 'chat_extract') {
          setMessages(requestMessages);
        }
        return;
      }
    } finally {
      void loadRecentChatJobs();
      setChatJobPolling(false);
    }
  }

  const doRagSend = useCallback(async (
    question: string,
    currentMessages: ChatMessageWithReasoning[],
  ) => {
    if (!currentCustomerId) {
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: '请先选择或上传客户资料，再进行资料问答。',
      };
      setMessages((prev) => [...prev, assistantMessage]);
      addChatMessage(assistantMessage);
      return;
    }

    const userMessage: ChatMessage = {
      role: 'user',
      content: question,
    };

    const newMessages = [...currentMessages, userMessage];
    setMessages(newMessages);
    addChatMessage(userMessage);

    const { data: response } = await executeRag(async () => {
      return customerRagChat(currentCustomerId, { question }, getSignal());
    });

    if (response) {
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: response.answer,
        data: {
          answer: response.answer,
          evidence: response.evidence,
          missing_info: response.missing_info,
        },
      };
      setMessages((prev) => [...prev, assistantMessage]);
      addChatMessage(assistantMessage);
      recordSystemActivity({
        type: 'rag',
        title: '资料问答已完成',
        description: `系统已基于当前客户资料回答问题，并返回 ${response.evidence?.length ?? 0} 条证据。`,
        customerName: currentCustomerName,
        customerId: currentCustomerId,
        status: 'success',
      });
      setChatTaskStatus('done', null, null);
    } else {
      setChatTaskStatus('idle', null, null);
    }
  }, [addChatMessage, currentCustomerId, currentCustomerName, executeRag, getSignal, recordSystemActivity, setChatTaskStatus]);

  const handleGenerateRiskReport = useCallback(async () => {
    if (!currentCustomerId) {
      const assistantMessage: ChatMessageWithReasoning = {
        role: 'assistant',
        content: '请先选择客户，再生成风险评估报告。',
      };
      setMessages((prev) => [...prev, assistantMessage]);
      addChatMessage(assistantMessage);
      return;
    }

    setRiskFeedback({
      tone: 'processing',
      title: '正在生成风险评估报告',
      description: '系统正在读取当前客户资料、执行规则评分，并组织结构化风险报告。',
      persistenceHint: '主流程处理中，生成完成后会同步展示最新报告。',
      nextStep: '请稍候，完成后建议直接核对综合结论和优化建议。',
    });

    const userMessage: ChatMessage = {
      role: 'user',
      content: '请基于当前客户资料生成风险评估报告。',
    };

    const newMessages = [...messages, userMessage];
    setMessages(newMessages);
    addChatMessage(userMessage);
    setChatTaskStatus('sending', userMessage.content, null);

    try {
      const job = await createCustomerRiskReportJob(currentCustomerId, getSignal());
      void loadRecentChatJobs();
      await pollChatJob(job.jobId, newMessages, {
        startedAt: Date.now(),
        customerId: currentCustomerId,
        customerName: currentCustomerName,
      });
    } catch (jobError) {
      setRiskFeedback({
        tone: 'error',
        title: '风险评估报告提交失败',
        description: jobError instanceof Error ? jobError.message : '本次风险评估任务未能成功提交。',
        persistenceHint: '主流程尚未进入后台处理。',
        nextStep: '请检查网络或稍后重试一次。',
      });
      setChatTaskStatus('idle', null, null);
    }
  }, [addChatMessage, currentCustomerId, currentCustomerName, getSignal, loadRecentChatJobs, messages, pollChatJob, setChatTaskStatus]);

  // Keep ref updated with latest function
  useEffect(() => {
    sendRef.current = doSend;
  }, [doSend]);

  useEffect(() => {
    loadCompletedChatJobIds();
  }, [loadCompletedChatJobIds]);

  // Sync with context on mount and handle task recovery
  // Note: This effect intentionally runs only on mount to restore state from context.
  // The setState calls sync external (context) state into local state, which is a valid pattern.
  useEffect(() => {
    // 只在首次挂载时从 Context 恢复消息和任务
    if (hasCheckedRecoveryRef.current) {
      return; // 已经检查过恢复，不再重复
    }
    
    // 恢复消息历史
    if (state.chat.messages.length > 0) {
      setMessages(state.chat.messages);
    }

    const pendingJob = readPendingChatJob();
    if (pendingJob && !isRecoveringRef.current) {
      const pendingStartedAt = new Date(pendingJob.createdAt).getTime();
      if (!Number.isNaN(pendingStartedAt) && Date.now() - pendingStartedAt > CHAT_JOB_MAX_POLL_MS) {
        clearPendingChatJob();
        setChatJobFeedback({
          tone: 'partial',
          title: '发现未完成的旧任务',
          description: '这条资料提取任务处理时间过长，已停止自动恢复轮询。',
          persistenceHint: '后台任务可能已经完成，也可能仍在继续执行。',
          nextStep: '建议前往资料汇总查看是否已写入资料，或重新发起一次提取。',
        });
        hasCheckedRecoveryRef.current = true;
        return;
      }

      isRecoveringRef.current = true;
      if (pendingJob.customerId || pendingJob.customerName) {
        setCurrentCustomer(pendingJob.customerName ?? null, pendingJob.customerId ?? null);
      }
      if (state.chat.messages.length === 0 && pendingJob.requestMessages.length > 0) {
        setMessages(pendingJob.requestMessages);
      }
      setTimeout(() => {
        void pollChatJob(pendingJob.jobId, pendingJob.requestMessages, {
          startedAt: pendingStartedAt,
          restored: true,
        });
        isRecoveringRef.current = false;
      }, 100);
      hasCheckedRecoveryRef.current = true;
      return;
    }

    // Check if there's a task to recover (only on mount)
    const taskState = state.tasks.chat;
    if (taskState.status === 'sending' && taskState.pendingMessage && !isRecoveringRef.current) {
      isRecoveringRef.current = true;
      const savedMessage = taskState.pendingMessage;
      const savedFiles = taskState.pendingFiles;
      const currentMsgs = state.chat.messages.length > 0 ? state.chat.messages : [];

      // Use setTimeout to ensure state is updated before calling send
      // Pass params directly to avoid closure issues (踩坑点 #31)
      setTimeout(() => {
        if (sendRef.current) {
          sendRef.current(savedMessage, savedFiles, currentMsgs as ChatMessageWithReasoning[]);
        }
        isRecoveringRef.current = false;
      }, 100);
    }
    
    // 标记已完成恢复检查
    hasCheckedRecoveryRef.current = true;
  // eslint-disable-next-line react-hooks/exhaustive-deps -- Mount-only effect: intentionally excludes state deps to avoid re-running after mount
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadCustomers = async () => {
      setCustomersLoading(true);
      try {
        const items = await listCustomers(undefined, getSignal());
        if (!cancelled) {
          setCustomerOptions(items);
        }
      } catch (err) {
        if (!cancelled) {
          console.error('Failed to load customers for chat selector:', err);
          setCustomerOptions([]);
        }
      } finally {
        if (!cancelled) {
          setCustomersLoading(false);
        }
      }
    };

    void loadCustomers();
    void loadRecentChatJobs();

    return () => {
      cancelled = true;
    };
  }, [getSignal, loadRecentChatJobs]);

  useEffect(() => {
    return () => {
      if (chatJobPollTimeoutRef.current) {
        window.clearTimeout(chatJobPollTimeoutRef.current);
      }
    };
  }, []);

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, busy]);

  useEffect(() => {
    if (!latestCompletedChatJob?.jobId) {
      return;
    }
    resultTopRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, [latestCompletedChatJob?.jobId]);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 150)}px`;
    }
  }, [inputValue]);

  /**
   * Handle file selection
   */
  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    // Deduplicate: only add files not already in attachedFiles (by name)
    setAttachedFiles(prev => {
      const existingNames = new Set(prev.map(f => f.name));
      const newFiles = files.filter(f => !existingNames.has(f.name));
      return [...prev, ...newFiles];
    });
    // Reset input so same file can be selected again
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, []);

  /**
   * Handle drag events for file drop
   */
  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    // Only set dragging to false if we're leaving the container
    if (e.currentTarget === e.target) {
      setIsDragging(false);
    }
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      // Filter for supported file types
      const supportedTypes = ['.pdf', '.xlsx', '.xls', '.doc', '.docx', '.png', '.jpg', '.jpeg'];
      const validFiles = files.filter(file => {
        const ext = '.' + file.name.split('.').pop()?.toLowerCase();
        return supportedTypes.includes(ext);
      });
      
      if (validFiles.length > 0) {
        // Deduplicate: only add files not already in attachedFiles (by name)
        setAttachedFiles(prev => {
          const existingNames = new Set(prev.map(f => f.name));
          const newFiles = validFiles.filter(f => !existingNames.has(f.name));
          return [...prev, ...newFiles];
        });
      }
    }
  }, []);

  /**
   * Remove attached file
   */
  const handleRemoveFile = useCallback((index: number) => {
    setAttachedFiles(prev => prev.filter((_, i) => i !== index));
  }, []);

  /**
   * Handle message submission
   * Feature: frontend-backend-integration
   * Requirements: 6.1, 6.5, 6.6
   */
  const handleSubmit = useCallback(async () => {
    const content = inputValue.trim();
    if (!content && attachedFiles.length === 0) return;
    if (busy) return;

    if (ragMode) {
      setInputValue('');
      setAttachedFiles([]);
      setChatTaskStatus('sending', content, null);
      await doRagSend(content, messages);
      return;
    }

    // Build message content with file info
    let messageContent = content;
    if (attachedFiles.length > 0) {
      // Encode file info into message content: [FILE:name:type]
      const fileMarkers = attachedFiles
        .map(file => `[FILE:${file.name}:${file.type}]`)
        .join('');
      messageContent = fileMarkers + (content ? '\n' + content : '');
    }

    // Clear input immediately
    setInputValue('');

    // Prepare files for API
    let chatFiles: ChatFile[] | null = null;
    if (attachedFiles.length > 0) {
      chatFiles = await Promise.all(
        attachedFiles.map(async (file) => ({
          name: file.name,
          type: file.type,
          content: await fileToBase64(file),
        }))
      );
      setAttachedFiles([]);
    }

    // Save task state before starting (for recovery on page switch)
    setChatTaskStatus('sending', messageContent, chatFiles);

    // Use the extracted send function, pass current messages directly
    await doSend(messageContent, chatFiles, messages);
  }, [inputValue, attachedFiles, messages, busy, ragMode, doRagSend, doSend, setChatTaskStatus]);

  /**
   * Handle keyboard events
   */
  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  }, [handleSubmit]);

  /**
   * Handle intent action button click
   */
  const handleIntentAction = useCallback((action: string) => {
    if (onNavigate) {
      onNavigate(action);
    }
  }, [onNavigate]);

  /**
   * Handle quick action click
   */
  const handleQuickAction = useCallback((action: string) => {
    const prompts: Record<string, string> = {
      upload: '我想上传贷款资料',
      application: currentCustomerId ? '帮我生成一份企业贷款申请表' : '帮我生成贷款申请表',
      matching: currentCustomerId ? '帮我匹配贷款方案' : '帮我匹配贷款方案',
    };
    setInputValue(prompts[action] || '');
    textareaRef.current?.focus();
  }, [currentCustomerId]);

  /**
   * Handle switch customer: clear backend cache and reset conversation
   */
  const handleSwitchCustomer = useCallback(async () => {
    try {
      await clearCustomerCache();
    } catch (err) {
      console.error('Failed to clear customer cache:', err);
    }
    // Reset conversation
    setMessages([]);
    setInputValue('');
    setAttachedFiles([]);
    setLastIntent(null);
    setCurrentCustomer(null, null);
  }, [setCurrentCustomer]);

  const handleCustomerChange = useCallback((customerId: string) => {
    if (!customerId) {
      setCurrentCustomer(null, null);
      return;
    }

    const target = customerOptions.find((item) => item.record_id === customerId);
    setCurrentCustomer(target?.name ?? null, customerId);
  }, [customerOptions, setCurrentCustomer]);

  const handleOpenJob = useCallback(async (job: ChatJobSummaryResponse) => {
    const pendingJob = readPendingChatJob();
    const requestMessages =
      pendingJob?.jobId === job.jobId
        ? pendingJob.requestMessages
        : messages;

    if (job.customerId) {
      const matchedCustomer = customerOptions.find((item) => item.record_id === job.customerId);
      setCurrentCustomer(matchedCustomer?.name ?? null, job.customerId);
    }

    if (job.status === 'success') {
      const status = await getChatJobStatus(job.jobId, getSignal());
      const resolvedCustomerName = status.customerName || job.customerName || (job.customerId ? (customerOptions.find((item) => item.record_id === job.customerId)?.name ?? null) : null);
      consumeCompletedJobResult(status, requestMessages, job.customerId || null, resolvedCustomerName);
      return;
    }

    setCurrentJob(job);
    setChatJobResult(null);
    await pollChatJob(job.jobId, requestMessages, {
      startedAt: new Date(job.startedAt || job.createdAt || new Date().toISOString()).getTime(),
      restored: true,
      customerId: job.customerId || null,
      customerName: job.customerName || (job.customerId ? (customerOptions.find((item) => item.record_id === job.customerId)?.name ?? null) : null),
    });
  }, [readPendingChatJob, messages, customerOptions, setCurrentCustomer, getSignal, consumeCompletedJobResult, pollChatJob]);

  const handleJumpToProfile = useCallback(() => {
    if (!latestCompletedChatJob?.customerId) {
      return;
    }
    setCurrentCustomer(latestCompletedChatJob.customerName ?? null, latestCompletedChatJob.customerId);
    if (latestCompletedChatJob.targetPage) {
      onNavigate?.(latestCompletedChatJob.targetPage);
    }
  }, [latestCompletedChatJob, onNavigate, setCurrentCustomer]);

  const displayedChatJobs = useMemo(() => {
    if (jobFilterMode === 'current' && currentCustomerId) {
      return recentChatJobs.filter((job) => job.customerId === currentCustomerId);
    }
    return recentChatJobs;
  }, [jobFilterMode, currentCustomerId, recentChatJobs]);

  const groupedDisplayedChatJobs = useMemo(() => {
    const sortJobsForSidebar = (jobs: ChatJobSummaryResponse[]) => {
      const sorted = [...jobs].sort((a, b) => {
        const aIsCurrent = currentJob?.jobId === a.jobId ? 1 : 0;
        const bIsCurrent = currentJob?.jobId === b.jobId ? 1 : 0;
        if (aIsCurrent !== bIsCurrent) {
          return bIsCurrent - aIsCurrent;
        }

        const aTime = new Date(a.finishedAt || a.startedAt || a.createdAt || 0).getTime();
        const bTime = new Date(b.finishedAt || b.startedAt || b.createdAt || 0).getTime();
        return bTime - aTime;
      });
      return sorted;
    };

    const running = sortJobsForSidebar(displayedChatJobs.filter((job) => job.status === 'pending' || job.status === 'running'));
    const failed = sortJobsForSidebar(displayedChatJobs.filter((job) => job.status === 'failed'));
    const success = sortJobsForSidebar(displayedChatJobs.filter((job) => job.status === 'success'));

    return [
      {
        key: 'running',
        label: '处理中',
        jobs: running,
        icon: <Loader2 className="h-3.5 w-3.5" />,
        tone: 'border-blue-200 bg-blue-50 text-blue-700',
        countTone: 'bg-blue-100 text-blue-700',
      },
      {
        key: 'failed',
        label: '已失败',
        jobs: failed,
        icon: <AlertCircle className="h-3.5 w-3.5" />,
        tone: 'border-rose-200 bg-rose-50 text-rose-700',
        countTone: 'bg-rose-100 text-rose-700',
      },
      {
        key: 'success',
        label: '已完成',
        jobs: success,
        icon: <CheckCircle2 className="h-3.5 w-3.5" />,
        tone: 'border-emerald-200 bg-emerald-50 text-emerald-700',
        countTone: 'bg-emerald-100 text-emerald-700',
      },
    ].filter((group) => group.jobs.length > 0);
  }, [currentJob?.jobId, displayedChatJobs]);

  const recoveredResultView = useMemo(() => {
    if (!currentJob || !chatJobResult) {
      return null;
    }

    if (currentJob.jobType === 'chat_extract') {
      const response = chatJobResult as unknown as ChatResponse;
      return (
        <MessageBubble
          message={{
            role: 'assistant',
            content: response.message || '资料提取已完成。',
            reasoning: response.reasoning,
            intent: response.intent,
            data: response.data,
          }}
          onNavigate={onNavigate}
        />
      );
    }

    if (currentJob.jobType === 'risk_report') {
      const response = chatJobResult as unknown as CustomerRiskReportResponse;
      return (
        <StructuredDataCard
          data={{
            ...(response.report_json as unknown as Record<string, unknown>),
            generated_at: response.generated_at,
            profile_version: response.profile_version,
            profile_updated_at: response.profile_updated_at,
            previous_report: response.previous_report,
          }}
          onNavigate={onNavigate}
        />
      );
    }

    if (currentJob.jobType === 'application_generate') {
      const response = chatJobResult as unknown as import('../services/types').ApplicationResponse;
      return (
        <ApplicationResultCard
          data={{
            customerFound: response.customerFound,
            customerName: currentJob.customerName || undefined,
            applicationData: response.applicationData,
            applicationContent: response.applicationContent,
            warnings: response.warnings,
            metadata: response.metadata,
          }}
          onNavigate={onNavigate}
        />
      );
    }

    if (currentJob.jobType === 'scheme_match') {
      const response = chatJobResult as unknown as import('../services/types').SchemeMatchResponse & {
        creditType?: string;
        matchingData?: MatchingResultCardProps['data']['matchingData'];
        needsInput?: boolean;
        requiredFields?: string[];
      };
      return (
        <MatchingResultCard
          data={{
            customerName: currentJob.customerName || undefined,
            creditType: response.creditType,
            matchingData: response.matchingData,
            matchResult: response.matchResult,
            needsInput: response.needsInput,
            requiredFields: response.requiredFields,
          }}
          onNavigate={onNavigate}
        />
      );
    }

    return null;
  }, [chatJobResult, currentJob, onNavigate]);

  const toggleJobGroup = useCallback((groupKey: string) => {
    setCollapsedJobGroups((prev) => ({
      ...prev,
      [groupKey]: !prev[groupKey],
    }));
  }, []);

  const quickActions = [
    { icon: '📤', label: '上传贷款资料', action: 'upload' },
    { icon: '📝', label: '生成申请表', action: 'application' },
    { icon: '🎯', label: '匹配贷款方案', action: 'matching' },
  ];

  /**
   * Handle merge decision: user chose to merge into existing customer or create new
   * targetCustomerId = existing customer_id to merge into, or null to create new
   */
  const handleMergeDecision = useCallback(async (targetCustomerId: string | null) => {
    if (!mergeModal) return;
    const { customerName, pendingMessage, pendingFiles, pendingMessages } = mergeModal;
    setMergeModal(null);

    const mergeDecisions: Record<string, string> | undefined = targetCustomerId
      ? { [customerName]: targetCustomerId }
      : undefined;

    await doSend(pendingMessage, pendingFiles, pendingMessages, mergeDecisions);
  }, [mergeModal, doSend]);

  const recentJobsPanel = (
    <div className="rounded-2xl border border-slate-200 bg-[linear-gradient(180deg,#ffffff_0%,#f8fafc_100%)] p-4 shadow-sm">
      <div className="flex flex-col gap-2">
        <div>
          <div className="flex items-center gap-2">
            <span className="inline-flex rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
              任务侧栏
            </span>
            <div className="text-sm font-semibold text-slate-800">最近处理任务</div>
          </div>
          <div className="text-xs leading-5 text-slate-500">
            左侧查看资料提取、风险报告、方案匹配和申请表生成任务，右侧集中查看提取内容、反馈状态与结构化结果。
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => setShowRecentJobs((prev) => !prev)}
            className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-600 transition-colors hover:bg-slate-100"
          >
            {showRecentJobs ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            {showRecentJobs ? '隐藏任务' : '展开任务'}
          </button>
          <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-1 text-xs">
            <button
              type="button"
              onClick={() => setJobFilterMode('current')}
              className={`rounded-md px-3 py-1 transition-colors ${
                jobFilterMode === 'current'
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-slate-500 hover:bg-slate-100'
              }`}
            >
              当前客户
            </button>
            <button
              type="button"
              onClick={() => setJobFilterMode('all')}
              className={`rounded-md px-3 py-1 transition-colors ${
                jobFilterMode === 'all'
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-slate-500 hover:bg-slate-100'
              }`}
            >
              全部任务
            </button>
          </div>
          <button
            type="button"
            onClick={() => void loadRecentChatJobs()}
            className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-600 transition-colors hover:bg-slate-100"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${jobsLoading ? 'animate-spin' : ''}`} />
            刷新任务
          </button>
        </div>
      </div>

      {showRecentJobs ? (
        <div className="mt-4 space-y-3">
          {displayedChatJobs.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/80 px-4 py-4 text-xs text-slate-500">
              {jobsLoading ? '正在加载最近任务...' : jobFilterMode === 'current' && currentCustomerId ? '当前客户还没有最近处理任务。' : '当前还没有处理任务记录。'}
            </div>
          ) : (
            groupedDisplayedChatJobs.map((group) => (
              <div key={group.key} className="space-y-2">
                <div className="flex items-center justify-between px-1">
                  <button
                    type="button"
                    onClick={() => toggleJobGroup(group.key)}
                    className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors hover:opacity-90 ${group.tone}`}
                  >
                    <span className={group.key === 'running' ? 'animate-spin' : ''}>
                      {group.icon}
                    </span>
                    {collapsedJobGroups[group.key] ? (
                      <ChevronRight className="h-3.5 w-3.5" />
                    ) : (
                      <ChevronDown className="h-3.5 w-3.5" />
                    )}
                    <span>{group.label}</span>
                  </button>
                  <div className={`rounded-full px-2 py-0.5 text-[11px] ${group.countTone}`}>
                    {group.jobs.length} 条
                  </div>
                </div>
                {collapsedJobGroups[group.key] ? (
                  <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/70 px-4 py-3 text-xs text-slate-500">
                    当前已折叠“{group.label}”分组，点击标题可展开查看。
                  </div>
                ) : (
                  group.jobs.map((job) => {
                    const isLatestCompleted =
                      job.status === 'success' &&
                      currentJob?.jobId === job.jobId;
                    return (
                      <AsyncJobCard
                        key={job.jobId}
                        job={job}
                        isLatestCompleted={isLatestCompleted}
                        className={isLatestCompleted ? 'ring-2 ring-emerald-100' : ''}
                        onAction={(selectedJob) => void handleOpenJob(selectedJob as ChatJobSummaryResponse)}
                      />
                    );
                  })
                )}
              </div>
            ))
          )}
        </div>
      ) : (
        <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-slate-50/80 px-4 py-3 text-xs text-slate-500">
          当前已隐藏最近处理任务。{displayedChatJobs.length > 0 ? `当前筛选下共 ${displayedChatJobs.length} 条任务。` : '可随时展开查看任务进度和结果。'}
        </div>
      )}
    </div>
  );

  return (
    <div 
      className={`flex flex-col h-full bg-[linear-gradient(180deg,#ffffff_0%,#f8fafc_100%)] relative ${isDragging ? 'ring-2 ring-blue-500 ring-inset' : ''}`}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* Merge Decision Modal */}
      {mergeModal && (
        <div className="absolute inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md mx-4 overflow-hidden">
            <div className="px-5 py-4 bg-gradient-to-r from-amber-50 to-orange-50 border-b border-amber-100">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-amber-100 text-amber-600 flex items-center justify-center">
                  <AlertCircle className="w-5 h-5" />
                </div>
                <div>
                  <div className="font-semibold text-gray-800 text-sm">发现相似客户</div>
                  <div className="text-xs text-gray-500 mt-0.5">
                    「{mergeModal.customerName}」与以下客户名称相似
                  </div>
                </div>
              </div>
            </div>
            <div className="p-5 space-y-3">
              <p className="text-sm text-gray-600">请选择将此次上传的资料归属到哪个客户：</p>
              {mergeModal.candidates.map(c => (
                <button
                  key={c.customer_id}
                  onClick={() => handleMergeDecision(c.customer_id)}
                  className="w-full text-left px-4 py-3 rounded-xl border border-blue-200 bg-blue-50 hover:bg-blue-100 transition-colors"
                >
                  <div className="font-medium text-blue-800 text-sm">{c.name}</div>
                  <div className="text-xs text-blue-500 mt-0.5">
                    共同关键词：{c.shared_keywords.join('、')}
                  </div>
                </button>
              ))}
              <button
                onClick={() => handleMergeDecision(null)}
                className="w-full text-left px-4 py-3 rounded-xl border border-gray-200 bg-gray-50 hover:bg-gray-100 transition-colors"
              >
                <div className="font-medium text-gray-700 text-sm">新建客户「{mergeModal.customerName}」</div>
                <div className="text-xs text-gray-400 mt-0.5">作为独立客户保存</div>
              </button>
            </div>
          </div>
        </div>
      )}
      {/* Drag Overlay */}
      {isDragging && (
        <div className="absolute inset-0 bg-blue-50 bg-opacity-90 z-50 flex items-center justify-center pointer-events-none">
          <div className="text-center">
            <Upload className="w-16 h-16 text-blue-500 mx-auto mb-4" />
            <p className="text-blue-600 text-lg font-medium">拖放文件到此处上传</p>
            <p className="text-blue-400 text-sm mt-1">支持 PDF、Excel、Word、图片格式</p>
          </div>
        </div>
      )}
      {/* Chat Header - Feature: frontend-ui-optimization, Requirements: 5.1, 5.4 */}
      <div 
        className="border-b border-slate-200 bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.14),_transparent_28%),linear-gradient(135deg,#ffffff_0%,#f8fafc_100%)] px-6 py-4 flex-shrink-0"
        data-testid="chat-header"
      >
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div 
              className="flex items-center justify-center flex-shrink-0"
              style={{
                width: AI_AVATAR_STYLE.size,
                height: AI_AVATAR_STYLE.size,
                backgroundColor: AI_AVATAR_STYLE.bgColor,
                borderRadius: '50%',
              }}
              data-testid="chat-header-avatar"
            >
              <BotIcon className="w-5 h-5 text-white" />
            </div>
            <div className="flex flex-col gap-0.5">
              <span className="text-gray-800 text-base font-semibold" data-testid="chat-header-name">
                智能助手
              </span>
              <div className="flex items-center gap-2">
                <div 
                  className="w-2 h-2 bg-green-500 rounded-full"
                  data-testid="chat-header-status-dot"
                />
                <span 
                  className="text-green-500 text-xs font-medium"
                  data-testid="chat-header-status-text"
                >
                  在线
                </span>
                <span className="rounded-full border border-blue-200 bg-blue-50 px-2.5 py-0.5 text-[11px] font-medium text-blue-700">
                  当前服务链路已统一客户上下文
                </span>
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 lg:justify-end">
            <div className="hidden md:block">
              <label className="sr-only" htmlFor="chat-customer-select">选择客户</label>
              <select
                id="chat-customer-select"
                value={currentCustomerId ?? ''}
                onChange={(e) => handleCustomerChange(e.target.value)}
                className="min-w-[240px] rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-700 outline-none shadow-sm transition-colors focus:border-blue-300 focus:bg-white"
              >
                <option value="">
                  {customersLoading ? '加载客户中...' : '请选择客户'}
                </option>
                {customerOptions.map((customer) => (
                  <option key={customer.record_id} value={customer.record_id}>
                    {formatCustomerContextLabel(customer.record_id, customer.name)}
                  </option>
                ))}
              </select>
            </div>
            <button
              onClick={() => setRagMode((prev) => !prev)}
              className={`flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs transition-colors ${
                ragMode
                  ? 'border-indigo-200 bg-indigo-50 text-indigo-700'
                  : 'border-gray-200 bg-gray-50 text-gray-500 hover:bg-gray-100 hover:text-gray-700'
              }`}
              title="切换资料问答模式"
            >
              <FileCheck className="w-3.5 h-3.5" />
              {ragMode ? '资料问答已开启' : '开启资料问答'}
            </button>
            <button
              onClick={handleGenerateRiskReport}
              disabled={busy}
              className="flex items-center gap-1.5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs text-amber-700 transition-colors hover:bg-amber-100 disabled:opacity-50 disabled:cursor-not-allowed"
              title="生成当前客户风险评估报告"
            >
              <AlertCircle className="w-3.5 h-3.5" />
              生成风险报告
            </button>
            <button
              onClick={handleSwitchCustomer}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-500 bg-gray-50 border border-gray-200 rounded-lg hover:bg-gray-100 hover:text-gray-700 transition-colors"
              title="清空当前客户和对话"
              data-testid="switch-customer-button"
            >
              <RefreshCw className="w-3.5 h-3.5" />
              清空当前客户
            </button>
          </div>
        </div>
      </div>

      {currentCustomerId ? (
        <div className="border-b border-slate-100 bg-slate-50/80 px-6 py-2">
          <div className="mx-auto max-w-[50rem] text-xs text-slate-600">
            当前已选择客户：
            <span className="ml-1 font-medium text-slate-800">
              {formatCustomerContextLabel(currentCustomerId, currentCustomerName)}
            </span>
            <span className="ml-2 text-slate-500">
              生成申请表、匹配方案、资料问答和风险报告都会默认基于该客户处理
            </span>
          </div>
        </div>
      ) : (
        <div className="border-b border-slate-100 bg-amber-50/80 px-6 py-2">
          <div className="mx-auto max-w-[50rem] text-xs text-amber-700">
            当前还没有选定客户。建议先在顶部选择客户，再继续申请表生成、方案匹配、资料问答或风险报告。
          </div>
        </div>
      )}

      <div className="flex-1 overflow-y-auto px-6 py-6">
        <div className="mx-auto max-w-[82rem]">
          <div className="grid gap-6 xl:grid-cols-[22rem_minmax(0,1fr)]">
            <aside className="xl:sticky xl:top-6 xl:self-start">
              <div className="xl:max-h-[calc(100vh-12rem)] xl:overflow-y-auto xl:pr-1">
                {recentJobsPanel}
              </div>
            </aside>

            <section className="min-w-0 space-y-4">
              <div ref={resultTopRef} />
              {chatJobFeedback ? (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <ProcessFeedbackCard
                    tone={chatJobFeedback.tone}
                    title={chatJobFeedback.title}
                    description={chatJobFeedback.description}
                    persistenceHint={chatJobFeedback.persistenceHint}
                    nextStep={chatJobFeedback.nextStep}
                  />
                  {chatJobFeedback.tone === 'success' && latestCompletedChatJob?.customerId && latestCompletedChatJob.targetPage ? (
                    <div className="mt-3 flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={handleJumpToProfile}
                        className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-medium text-blue-700 transition-colors hover:bg-blue-100"
                      >
                        {latestCompletedChatJob.actionLabel}
                      </button>
                    </div>
                  ) : null}
                </div>
              ) : null}

              {riskFeedback ? (
                <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <ProcessFeedbackCard
                    tone={riskFeedback.tone}
                    title={riskFeedback.title}
                    description={riskFeedback.description}
                    persistenceHint={riskFeedback.persistenceHint}
                    nextStep={riskFeedback.nextStep}
                  />
                </div>
              ) : null}

              <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                <div className="mb-4 flex flex-col gap-1 border-b border-slate-100 pb-3">
                  <div className="flex items-center gap-2">
                    <span className="inline-flex rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                      结果查看区
                    </span>
                    <div className="text-sm font-semibold text-slate-800">
                      当前查看结果
                    </div>
                  </div>
                  <div className="text-xs text-slate-500">
                    右侧集中展示资料提取内容、任务反馈、风险报告和结构化结果，方便边看任务边核对内容。
                  </div>
                  <div className="flex flex-wrap gap-2 pt-1">
                    <span className="inline-flex rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-[11px] font-medium text-blue-700">
                      当前客户：{formatCustomerContextLabel(
                        currentJob?.customerId ?? latestCompletedChatJob?.customerId ?? currentCustomerId ?? null,
                        currentJob?.customerName ?? latestCompletedChatJob?.customerName ?? currentCustomerName ?? null
                      )}
                    </span>
                    <span className="inline-flex rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700">
                      当前任务：{currentJob ? getJobTypeLabel(currentJob.jobType) : latestCompletedChatJob ? getJobTypeLabel(latestCompletedChatJob.jobType) : '聊天结果'}
                    </span>
                  </div>
                </div>
                {recoveredResultView ? (
                  recoveredResultView
                ) : messages.length === 0 ? (
                  <>
                    {displayedChatJobs.length > 0 ? (
                      <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50/80 px-6 py-8 text-center">
                        <div className="text-sm font-semibold text-slate-700">当前还没有结果内容</div>
                        <div className="mt-2 text-xs leading-6 text-slate-500">
                          你可以先从左侧选择一条最近任务继续查看，或重新提交资料提取、风险报告、方案匹配、申请表生成任务。
                        </div>
                        <div className="mt-4 flex flex-wrap justify-center gap-3">
                          <button
                            type="button"
                            onClick={() => setShowRecentJobs(true)}
                            className="rounded-lg border border-blue-200 bg-blue-50 px-4 py-2 text-xs font-medium text-blue-700 transition-colors hover:bg-blue-100"
                          >
                            回到左侧查看任务
                          </button>
                          <button
                            type="button"
                            onClick={() => {
                              setInputValue('我想上传贷款资料');
                              textareaRef.current?.focus();
                            }}
                            className="rounded-lg border border-slate-200 bg-white px-4 py-2 text-xs font-medium text-slate-700 transition-colors hover:bg-slate-50"
                          >
                            重新发起资料提取
                          </button>
                        </div>
                      </div>
                    ) : (
                      <>
                        <WelcomeMessage />
                        <div className="text-center py-4">
                          <div className="text-gray-500 text-xs mb-3">快速开始</div>
                          <div className="flex gap-3 justify-center flex-wrap">
                            {quickActions.map((action) => (
                              <button
                                key={action.action}
                                onClick={() => handleQuickAction(action.action)}
                                className="px-5 py-3 bg-gray-100 border border-gray-200 rounded-lg text-gray-800 text-sm hover:bg-gray-50 transition-colors"
                              >
                                {action.icon} {action.label}
                              </button>
                            ))}
                            <button
                              onClick={() => {
                                setRagMode(true);
                                setInputValue('请基于当前客户资料回答问题');
                                textareaRef.current?.focus();
                              }}
                              className="px-5 py-3 bg-indigo-50 border border-indigo-200 rounded-lg text-indigo-700 text-sm hover:bg-indigo-100 transition-colors"
                            >
                              开始资料问答
                            </button>
                            <button
                              onClick={() => {
                                void handleGenerateRiskReport();
                              }}
                              className="px-5 py-3 bg-amber-50 border border-amber-200 rounded-lg text-amber-700 text-sm hover:bg-amber-100 transition-colors"
                            >
                              开始生成风险报告
                            </button>
                          </div>
                        </div>
                      </>
                    )}
                  </>
                ) : (
                  <>
                    {messages.map((msg, index) => (
                      <React.Fragment key={index}>
                        <MessageBubble message={msg} onNavigate={onNavigate} />
                        {msg.role === 'assistant' && index === messages.length - 1 && lastIntent && !msg.data && (
                          <div className="ml-12 mb-4">
                            <IntentActions intent={lastIntent} onAction={handleIntentAction} />
                          </div>
                        )}
                      </React.Fragment>
                    ))}
                  </>
                )}

                {busy && <TypingIndicator />}

                {activeError && (
                  <div className="flex justify-center mb-4">
                    <div className="bg-red-50 text-red-600 px-4 py-2 rounded-lg text-sm">
                      本次发送未成功：{activeError.message}
                      <button
                        onClick={handleSubmit}
                        className="ml-2 underline hover:no-underline"
                      >
                        再试一次
                      </button>
                    </div>
                  </div>
                )}

                <div ref={messagesEndRef} />
              </div>
            </section>
          </div>
        </div>
      </div>

      {/* File Preview */}
      <FilePreview files={attachedFiles} onRemove={handleRemoveFile} />

      {/* Chat Input - Feature: frontend-ui-optimization, Requirements: 5.5, 5.6 */}
      <div className="bg-white border-t border-gray-200 px-6 py-4 flex-shrink-0" data-testid="chat-input-bar">
        <div className="max-w-[50rem] mx-auto flex gap-3 items-end">
          {/* Hidden File Input */}
          <input
            ref={fileInputRef}
            type="file"
            multiple
            onChange={handleFileSelect}
            className="hidden"
            accept=".pdf,.xlsx,.xls,.doc,.docx,.png,.jpg,.jpeg"
            data-testid="file-input"
          />
          
          {/* Attachment Button - rounded square, gray background */}
          <button
            onClick={() => fileInputRef.current?.click()}
            className="w-10 h-10 bg-gray-100 rounded-lg flex items-center justify-center hover:bg-gray-200 transition-colors flex-shrink-0"
            title="添加附件"
            data-testid="attachment-button"
          >
            <Paperclip className="w-5 h-5 text-gray-500" />
          </button>

          {/* Input - pill-shaped with gray border */}
          <div 
            className="flex-1 bg-gray-50 border border-gray-300 rounded-full px-4 py-2 flex items-center"
            data-testid="input-container"
          >
            <textarea
              ref={textareaRef}
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={ragMode ? '请输入资料问答问题，Shift+Enter 换行' : '请输入要处理的内容，Shift+Enter 换行'}
              className="flex-1 bg-transparent border-none outline-none resize-none text-sm text-gray-800 placeholder-gray-400 min-h-[24px] max-h-[150px]"
              rows={1}
              disabled={busy}
              data-testid="message-input"
            />
          </div>

          {/* Send Button - circular, blue background */}
          <button
            onClick={handleSubmit}
            disabled={busy || (!inputValue.trim() && attachedFiles.length === 0)}
            className="w-10 h-10 bg-blue-500 rounded-full flex items-center justify-center hover:bg-blue-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex-shrink-0"
            data-testid="send-button"
          >
            {busy ? (
              <Loader2 className="w-5 h-5 text-white animate-spin" />
            ) : (
              <Send className="w-5 h-5 text-white" />
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ChatPage;

// Export design tokens and components for testing
export { 
  AI_AVATAR_STYLE, 
  USER_MESSAGE_STYLE, 
  AI_MESSAGE_STYLE,
  MessageBubble,
  ReasoningCollapse,
  ExtractionResultCard,
  ApplicationGuideCard,
  MatchingGuideCard,
  ApplicationResultCard,
  MatchingResultCard,
  StructuredDataCard,
};
export type { ChatMessageWithReasoning, ExtractionFileResult };


