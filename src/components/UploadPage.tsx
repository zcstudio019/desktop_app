/**
 * UploadPage Component - File Upload and Processing
 *
 * Handles file upload, document processing, and auto-save to local storage.
 *
 * Feature: frontend-backend-integration
 * Task 5.1: Connect UploadPage to file processing API
 * Task 5.2: Implement auto-save to local storage
 * 
 * Feature: frontend-ui-optimization
 * Task 6.1: Update file type display area with grid layout
 * Task 6.2: Update drag-and-drop zone styling
 * Task 6.3: Update file list and progress display
 */

import React, { useState, useCallback, useRef, useMemo, useEffect } from 'react';
import { 
  FileText, Upload, Check, X, Loader2, AlertCircle,
  Building2, User, Landmark, Wallet, BarChart3, Home, FileSearch, Receipt
} from 'lucide-react';
import { listCustomers, processFile, saveToStorage } from '../services/api';
import { useLoading } from '../hooks/useLoading';
import { useAbortController } from '../hooks/useAbortController';
import { useApp, type ExtractionResult, type UploadQueueItem } from '../context/AppContext';
import { ApiError, classifyError, ErrorType, type CustomerListItem } from '../services/types';
import ProcessFeedbackCard from './common/ProcessFeedbackCard';

// ============================================
// Type Definitions
// ============================================

interface FileTypeConfig {
  id: string;
  name: string;
  formats: string;
  color: string;
  bgColor: string;
  icon: React.ComponentType<{ className?: string }>;
  acceptedExtensions: string[];
}

interface QueueItem {
  id: string;
  file: File;
  documentType: string;
  status: 'pending' | 'processing' | 'success' | 'error';
  progress: number;
  error?: string;
  result?: ExtractionResult;
}

interface UploadedFile {
  id: string;
  name: string;
  size: string;
  time: string;
  type: string;
  color: string;
  documentType: string;
  result: ExtractionResult;
}

// ============================================
// Constants - File Type Configuration
// ============================================

const FILE_TYPES: FileTypeConfig[] = [
  { 
    id: 'enterprise_credit', 
    name: '企业征信', 
    formats: 'PDF / 图片', 
    color: 'text-blue-600',
    bgColor: 'bg-blue-50',
    icon: Building2,
    acceptedExtensions: ['.pdf', '.jpg', '.jpeg', '.png'] 
  },
  { 
    id: 'personal_credit', 
    name: '个人征信', 
    formats: 'PDF / 图片', 
    color: 'text-green-600',
    bgColor: 'bg-green-50',
    icon: User,
    acceptedExtensions: ['.pdf', '.jpg', '.jpeg', '.png'] 
  },
  { 
    id: 'enterprise_flow', 
    name: '企业流水', 
    formats: 'PDF / XLSX', 
    color: 'text-purple-600',
    bgColor: 'bg-purple-50',
    icon: Landmark,
    acceptedExtensions: ['.xlsx', '.xls', '.pdf'] 
  },
  { 
    id: 'personal_flow', 
    name: '个人流水', 
    formats: 'PDF / XLSX', 
    color: 'text-amber-600',
    bgColor: 'bg-amber-50',
    icon: Wallet,
    acceptedExtensions: ['.xlsx', '.xls', '.pdf'] 
  },
  { 
    id: 'financial_data', 
    name: '财务数据', 
    formats: 'PDF / XLSX', 
    color: 'text-red-600',
    bgColor: 'bg-red-50',
    icon: BarChart3,
    acceptedExtensions: ['.xlsx', '.xls', '.pdf'] 
  },
  { 
    id: 'collateral', 
    name: '抵押物信息', 
    formats: 'PDF / 图片', 
    color: 'text-cyan-600',
    bgColor: 'bg-cyan-50',
    icon: Home,
    acceptedExtensions: ['.jpg', '.jpeg', '.png', '.pdf'] 
  },
  { 
    id: 'jellyfish_report', 
    name: '水母报告', 
    formats: 'PDF / 图片', 
    color: 'text-indigo-600',
    bgColor: 'bg-indigo-50',
    icon: FileSearch,
    acceptedExtensions: ['.pdf', '.jpg', '.jpeg', '.png'] 
  },
  { 
    id: 'personal_tax', 
    name: '个人纳税/公积金', 
    formats: 'PDF / XLSX', 
    color: 'text-teal-600',
    bgColor: 'bg-teal-50',
    icon: Receipt,
    acceptedExtensions: ['.xlsx', '.xls', '.pdf'] 
  },
  {
    id: 'contract',
    name: '合同',
    formats: 'PDF / DOCX',
    color: 'text-slate-700',
    bgColor: 'bg-slate-50',
    icon: FileText,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'id_card',
    name: '身份证',
    formats: 'PDF / DOCX',
    color: 'text-rose-600',
    bgColor: 'bg-rose-50',
    icon: User,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'marriage_cert',
    name: '结婚证',
    formats: 'PDF / DOCX',
    color: 'text-pink-600',
    bgColor: 'bg-pink-50',
    icon: User,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'hukou',
    name: '户口本',
    formats: 'PDF / DOCX',
    color: 'text-orange-600',
    bgColor: 'bg-orange-50',
    icon: Home,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'property_report',
    name: '产调',
    formats: 'PDF / DOCX',
    color: 'text-cyan-700',
    bgColor: 'bg-cyan-50',
    icon: Home,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'business_license',
    name: '营业执照正副本',
    formats: 'PDF / DOCX',
    color: 'text-sky-700',
    bgColor: 'bg-sky-50',
    icon: Building2,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'account_license',
    name: '开户许可证',
    formats: 'PDF / DOCX',
    color: 'text-violet-700',
    bgColor: 'bg-violet-50',
    icon: Landmark,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'special_license',
    name: '特别许可证',
    formats: 'PDF / DOCX',
    color: 'text-fuchsia-700',
    bgColor: 'bg-fuchsia-50',
    icon: FileSearch,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'company_articles',
    name: '公司章程',
    formats: 'PDF / DOCX',
    color: 'text-indigo-700',
    bgColor: 'bg-indigo-50',
    icon: FileText,
    acceptedExtensions: ['.pdf', '.docx'],
  },
  {
    id: 'bank_statement',
    name: '银行对账单',
    formats: 'PDF / XLSX',
    color: 'text-emerald-700',
    bgColor: 'bg-emerald-50',
    icon: Landmark,
    acceptedExtensions: ['.pdf', '.xlsx', '.xls'],
  },
  {
    id: 'bank_statement_detail',
    name: '银行对账明细',
    formats: 'PDF / XLSX',
    color: 'text-lime-700',
    bgColor: 'bg-lime-50',
    icon: Landmark,
    acceptedExtensions: ['.pdf', '.xlsx', '.xls'],
  },
];
const MAX_FILE_SIZE = 50 * 1024 * 1024;

// ============================================
// Utility Functions
// ============================================

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

function getFileExtension(filename: string): string {
  const lastDot = filename.lastIndexOf('.');
  return lastDot >= 0 ? filename.slice(lastDot).toLowerCase() : '';
}

function getFileTypeDisplay(filename: string): string {
  const ext = getFileExtension(filename);
  const typeMap: Record<string, string> = { 
    '.pdf': 'PDF', 
    '.docx': 'DOCX',
    '.xlsx': 'XLS', 
    '.xls': 'XLS', 
    '.jpg': 'JPG', 
    '.jpeg': 'JPG', 
    '.png': 'PNG' 
  };
  return typeMap[ext] || 'FILE';
}

function getFileTypeColor(filename: string): string {
  const ext = getFileExtension(filename);
  const colorMap: Record<string, string> = {
    '.pdf': 'bg-blue-100 text-blue-600',
    '.docx': 'bg-violet-100 text-violet-600',
    '.xlsx': 'bg-emerald-100 text-emerald-600',
    '.xls': 'bg-emerald-100 text-emerald-600',
    '.jpg': 'bg-amber-100 text-amber-600',
    '.jpeg': 'bg-amber-100 text-amber-600',
    '.png': 'bg-amber-100 text-amber-600',
  };
  return colorMap[ext] || 'bg-gray-100 text-gray-500';
}

function getFileTypeIcon(documentType: string): React.ComponentType<{ className?: string }> {
  const config = FILE_TYPES.find(t => t.id === documentType);
  return config?.icon || FileText;
}

function formatRelativeTime(date: Date): string {
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return '刚刚';
  if (diffMins < 60) return `${diffMins}分钟前`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}小时前`;
  return `${Math.floor(diffHours / 24)}天前`;
}

function generateId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function formatCustomerOptionLabel(customerId: string | null | undefined, customerName: string | null | undefined): string {
  if (customerName?.trim()) return customerName.trim();
  if (!customerId) return '未选择客户';
  return customerId.replace(/^(enterprise_|personal_)/, '');
}

function validateFile(file: File, acceptedExtensions: string[]): { valid: boolean; error?: string } {
  const ext = getFileExtension(file.name);
  if (!acceptedExtensions.includes(ext)) {
    return { valid: false, error: `当前资料类型不支持 ${ext} 文件` };
  }
  if (file.size > MAX_FILE_SIZE) {
    return { valid: false, error: `文件过大: ${formatFileSize(file.size)}。最大支持 50MB` };
  }
  return { valid: true };
}

// ============================================
// Sub-Components
// ============================================

/** File Type Card - displays supported file type info */
interface FileTypeCardProps {
  config: FileTypeConfig;
}

const FileTypeCard: React.FC<FileTypeCardProps> = ({ config }) => {
  const IconComponent = config.icon;
  return (
    <div 
      data-testid={`file-type-${config.id}`}
      className={`flex items-center gap-3 p-3 rounded-lg ${config.bgColor} transition-all hover:shadow-sm`}
    >
      <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${config.bgColor}`}>
        <IconComponent className={`w-5 h-5 ${config.color}`} />
      </div>
      <div className="flex flex-col">
        <span className={`text-sm font-medium ${config.color}`}>{config.name}</span>
        <span className="text-xs text-gray-500">{config.formats}</span>
      </div>
    </div>
  );
};

/** Queue Item - displays file in upload queue with status */
interface QueueItemDisplayProps {
  item: QueueItem;
}

/** Renders the file type icon for a queue item */
/* eslint-disable react-hooks/static-components -- Dynamic icon selection based on document type is intentional */
const QueueItemIcon: React.FC<{ documentType: string }> = ({ documentType }) => {
  const Icon = getFileTypeIcon(documentType);
  return <Icon className="w-5 h-5" />;
};
/* eslint-enable react-hooks/static-components */

const QueueItemDisplay: React.FC<QueueItemDisplayProps> = ({ item }) => {
  
  const renderStatus = () => {
    switch (item.status) {
      case 'pending':
        return (
          <span data-testid="status-pending" className="text-gray-500 text-sm flex items-center gap-1">
            等待中...
          </span>
        );
      case 'processing':
        return (
          <span data-testid="status-processing" className="text-blue-500 text-sm flex items-center gap-1">
            <Loader2 className="w-4 h-4 animate-spin" />
            处理中...
          </span>
        );
      case 'success':
        return (
          <span data-testid="status-success" className="text-green-600 text-sm flex items-center gap-1">
            <Check className="w-4 h-4" />
            已保存到本地
          </span>
        );
      case 'error':
        return (
          <span data-testid="status-error" className="text-red-500 text-sm flex items-center gap-1">
            <X className="w-4 h-4" />
            {item.error || '失败'}
          </span>
        );
      default:
        return null;
    }
  };

  return (
    <div 
      data-testid={`queue-item-${item.id}`}
      className="bg-gray-50 rounded-lg p-4"
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-3">
          <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${getFileTypeColor(item.file.name)}`}>
            <QueueItemIcon documentType={item.documentType} />
          </div>
          <div className="flex flex-col">
            <span data-testid="file-name" className="text-gray-800 text-sm font-medium">
              {item.file.name}
            </span>
            <span className="text-gray-400 text-xs">
              {formatFileSize(item.file.size)}
            </span>
          </div>
        </div>
        <div data-testid="file-status">
          {renderStatus()}
        </div>
      </div>
      
      {/* Progress bar - shown when processing */}
      {(item.status === 'pending' || item.status === 'processing') && (
        <div data-testid="progress-indicator" className="mt-2">
          <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
            <div 
              className={`h-full transition-all duration-300 ${
                item.status === 'processing' ? 'bg-blue-500' : 'bg-gray-300'
              }`}
              style={{ width: `${item.progress}%` }}
            />
          </div>
        </div>
      )}
      
      {/* Error message */}
      {item.status === 'error' && item.error && (
        <div className="flex items-center gap-1 mt-2 text-red-500 text-xs">
          <AlertCircle className="w-3 h-3" />
          {item.error}
        </div>
      )}
    </div>
  );
};

// ============================================
// Main Component
// ============================================

const UploadPage: React.FC = () => {
  const { addCustomerData, state, setApplicationResult, setCurrentCustomer, setSchemeResult, setUploadTaskStatus, recordSystemActivity } = useApp();
  const { error, execute, reset: resetLoading } = useLoading<void>();
  const { getSignal, abort } = useAbortController();

  const [uploadQueue, setUploadQueue] = useState<QueueItem[]>([]);
  const [uploadedFiles, setUploadedFiles] = useState<UploadedFile[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [selectedDocumentType, setSelectedDocumentType] = useState<string>('enterprise_credit');
  const [customerName, setCustomerName] = useState<string>('');
  const [customerOptions, setCustomerOptions] = useState<CustomerListItem[]>([]);
  const [customersLoading, setCustomersLoading] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const processingRef = useRef(false);
  // Ref to track if recovery is in progress
  const isRecoveringRef = useRef(false);

  const isProcessing = useMemo(
    () => uploadQueue.some((item) => item.status === 'pending' || item.status === 'processing'),
    [uploadQueue]
  );
  const uploadSummary = useMemo(() => {
    const successCount = uploadQueue.filter((item) => item.status === 'success').length;
    const errorCount = uploadQueue.filter((item) => item.status === 'error').length;

    if (isProcessing) {
      return {
        tone: 'processing' as const,
        title: '正在处理上传资料',
        description: '系统正在解析文件、保存客户资料，并自动刷新资料汇总与问答索引。',
        persistenceHint: '处理中，完成后会自动保存已识别资料。',
        nextStep: '请稍候，处理完成后可去资料汇总或 AI 对话查看最新结果。',
      };
    }

    if (uploadQueue.length === 0) {
      return {
        tone: 'idle' as const,
        title: '等待上传资料',
        description: '支持企业征信、个人征信、流水、财务数据、水母报告等资料。',
        persistenceHint: '尚未开始本轮上传。',
        nextStep: '先选择客户与资料类型，再上传文件。',
      };
    }

    if (successCount > 0 && errorCount > 0) {
      return {
        tone: 'partial' as const,
        title: '部分资料处理成功',
        description: `本轮已成功保存 ${successCount} 份资料，另有 ${errorCount} 份处理失败。`,
        persistenceHint: '成功部分已保存到当前客户，并已触发资料汇总更新。',
        nextStep: '可先继续使用已保存资料，再补传失败文件。',
      };
    }

    if (successCount > 0) {
      return {
        tone: 'success' as const,
        title: '资料上传已完成',
        description: `本轮 ${successCount} 份资料已全部保存，并已自动刷新资料汇总与问答索引。`,
        persistenceHint: '主流程已完成保存。',
        nextStep: '建议前往资料汇总核对内容，或去 AI 对话继续问答与生成报告。',
      };
    }

    return {
      tone: 'error' as const,
      title: '资料处理失败',
      description: error?.message || '本轮上传未成功，系统未保存新的资料内容。',
      persistenceHint: '主流程未保存成功。',
      nextStep: '请检查文件格式或网络状态后重新上传。',
    };
  }, [error?.message, isProcessing, uploadQueue]);

  const selectedFileTypeConfig = useMemo(
    () => FILE_TYPES.find((item) => item.id === selectedDocumentType) ?? FILE_TYPES[0],
    [selectedDocumentType]
  );

  // Update task status in context when queue changes
  useEffect(() => {
    if (isProcessing) {
      const queueItems: UploadQueueItem[] = uploadQueue.map(item => ({
        id: item.id,
        fileName: item.file.name,
        documentType: item.documentType,
        status: item.status,
      }));
      setUploadTaskStatus('processing', queueItems);
    } else if (uploadQueue.length > 0 && uploadQueue.every(item => item.status === 'success' || item.status === 'error')) {
      setUploadTaskStatus('done', []);
    }
  }, [uploadQueue, isProcessing, setUploadTaskStatus]);

  // Handle task recovery on mount - Note: Upload recovery is limited because we can't persist File objects
  // We only show a notification that there was an interrupted upload
  useEffect(() => {
    const taskState = state.tasks.upload;
    if (taskState.status === 'processing' && taskState.queue.length > 0 && !isRecoveringRef.current) {
      isRecoveringRef.current = true;
      // We can't recover the actual files, but we can notify the user
      console.warn('Upload task was interrupted. Please re-upload the files.');
      // Reset the task status since we can't recover
      setUploadTaskStatus('idle', []);
      isRecoveringRef.current = false;
    }
  }, [state.tasks.upload, setUploadTaskStatus]); // Re-run when task state changes for recovery

  useEffect(() => {
    let cancelled = false;
    const loadCustomers = async () => {
      setCustomersLoading(true);
      try {
        const items = await listCustomers(undefined, getSignal());
        if (!cancelled) {
          setCustomerOptions(items);
        }
      } catch (loadError) {
        if (!cancelled) {
          console.error('Failed to load customers for upload selector:', loadError);
          setCustomerOptions([]);
        }
      } finally {
        if (!cancelled) {
          setCustomersLoading(false);
        }
      }
    };
    void loadCustomers();
    return () => {
      cancelled = true;
    };
  }, [getSignal]);

  useEffect(() => {
    if (state.extraction.currentCustomer) {
      setCustomerName(state.extraction.currentCustomer);
    }
  }, [state.extraction.currentCustomer]);

  // Note: customerNameOverride is passed explicitly to avoid stale closure issues.
  const processQueueItem = useCallback(async (item: QueueItem, customerNameOverride: string): Promise<void> => {
    const signal = getSignal();
    setUploadQueue((prev) => prev.map((q) => 
      q.id === item.id ? { ...q, status: 'processing' as const, progress: 30 } : q
    ));

    try {
      setUploadQueue((prev) => prev.map((q) => 
        q.id === item.id ? { ...q, progress: 60 } : q
      ));
      const processResult = await processFile(item.file, item.documentType || undefined, signal);

      setUploadQueue((prev) => prev.map((q) => 
        q.id === item.id ? { ...q, progress: 80 } : q
      ));
      const activeCustomerId = state.extraction.currentCustomerId ?? null;
      const activeCustomerName = activeCustomerId ? state.extraction.currentCustomer ?? '' : '';
      // Prefer the selected customer context first, then manual input, then OCR result.
      const finalCustomerName =
        activeCustomerName.trim() ||
        customerNameOverride.trim() ||
        processResult.customerName ||
        '';
      if (!finalCustomerName) {
        throw new Error('未识别到客户名称，请先补充客户名称后再保存。');
      }
      const storageResult = await saveToStorage({
        documentType: processResult.documentType,
        customerName: finalCustomerName,
        customerId: activeCustomerId,
        content: processResult.content,
        fileName: item.file.name,
      }, signal);

      const extractionResult: ExtractionResult = {
        documentType: processResult.documentType,
        content: processResult.content,
        customerName: processResult.customerName,
        savedToFeishu: storageResult.success,
        recordId: storageResult.recordId,
        customerId: storageResult.customerId ?? null,
      };

      // Use addCustomerData to group by customer name
      addCustomerData(activeCustomerName || finalCustomerName, extractionResult);
      setCurrentCustomer(activeCustomerName || finalCustomerName, storageResult.customerId ?? activeCustomerId);
      setUploadQueue((prev) => prev.map((q) => 
        q.id === item.id ? { ...q, status: 'success' as const, progress: 100, result: extractionResult } : q
      ));

      const uploadedFile: UploadedFile = {
        id: item.id,
        name: item.file.name,
        size: formatFileSize(item.file.size),
        time: formatRelativeTime(new Date()),
        type: getFileTypeDisplay(item.file.name),
        color: getFileTypeColor(item.file.name),
        documentType: processResult.documentType,
        result: extractionResult,
      };
      setUploadedFiles((prev) => [uploadedFile, ...prev]);
      if (
        activeCustomerId &&
        state.application.result &&
        state.application.result.metadata?.customer_id === activeCustomerId
      ) {
        setApplicationResult(
          {
            ...state.application.result,
            metadata: {
              ...state.application.result.metadata,
              stale: true,
              stale_reason: `${item.file.name} 已上传并覆盖同类旧资料，请重新生成申请表以使用最新内容。`,
              stale_at: new Date().toISOString(),
            },
          },
          activeCustomerName || finalCustomerName,
        );
      }
      recordSystemActivity({
        type: 'upload',
        title: '客户资料上传完成',
        description: `${item.file.name} 已保存，并已自动更新资料汇总与问答索引。`,
        customerName: activeCustomerName || finalCustomerName,
        customerId: storageResult.customerId ?? activeCustomerId,
        status: 'success',
      });
      if (
        activeCustomerId &&
        state.application.result &&
        state.application.result.metadata?.customer_id === activeCustomerId
      ) {
        recordSystemActivity({
          type: 'application',
          title: '申请表需重新生成',
          description: `${item.file.name} 已覆盖同类旧资料，原申请表已标记为需重生成。`,
          customerName: activeCustomerName || finalCustomerName,
          customerId: storageResult.customerId ?? activeCustomerId,
          status: 'warning',
        });
      }
      if (
        activeCustomerId &&
        state.scheme.result &&
        state.scheme.result.customerId === activeCustomerId &&
        state.scheme.result.result
      ) {
        setSchemeResult({
          ...state.scheme.result,
          stale: true,
          staleReason: `${item.file.name} 已上传并覆盖同类旧资料，请重新匹配方案以使用最新内容。`,
          staleAt: new Date().toISOString(),
        });
        recordSystemActivity({
          type: 'matching',
          title: '方案匹配需重新执行',
          description: `${item.file.name} 已覆盖同类旧资料，原方案匹配结果已标记为需重新匹配。`,
          customerName: activeCustomerName || finalCustomerName,
          customerId: storageResult.customerId ?? activeCustomerId,
          status: 'warning',
        });
      }
    } catch (err) {
      let errorMessage = '处理失败';
      if (err instanceof DOMException && err.name === 'AbortError') {
        errorMessage = '已取消';
      } else if (err instanceof ApiError) {
        errorMessage = err.message;
      } else if (err instanceof Error) {
        errorMessage = err.message;
      }
      const errorType = classifyError(err);
      if (errorType === ErrorType.CANCELLED) {
        errorMessage = '已取消';
      }
      setUploadQueue((prev) => prev.map((q) => 
        q.id === item.id ? { ...q, status: 'error' as const, error: errorMessage } : q
      ));
    }
  }, [addCustomerData, getSignal, recordSystemActivity, setApplicationResult, setCurrentCustomer, setSchemeResult, state.application.result, state.extraction.currentCustomer, state.extraction.currentCustomerId, state.scheme.result]);

  // Note: customerNameOverride is passed explicitly to avoid stale closure issues.
  const processQueue = useCallback(async (itemsToProcess?: QueueItem[], customerNameOverride?: string) => {
    if (processingRef.current) return;
    processingRef.current = true;
    try {
      await execute(async () => {
        // Use provided items or filter from current queue
        const pendingItems = itemsToProcess || uploadQueue.filter((item) => item.status === 'pending');
        for (const item of pendingItems) {
          await processQueueItem(item, customerNameOverride || '');
        }
      });
    } finally {
      processingRef.current = false;
    }
  }, [uploadQueue, execute, processQueueItem]);

  const addFilesToQueue = useCallback((files: FileList | File[]) => {
    const fileArray = Array.from(files);
    const newItems: QueueItem[] = [];
    for (const file of fileArray) {
      const validation = validateFile(file, selectedFileTypeConfig.acceptedExtensions);
      if (!validation.valid) {
        newItems.push({
          id: generateId(),
          file,
          documentType: '',
          status: 'error',
          progress: 0,
          error: validation.error,
        });
      } else {
        newItems.push({
          id: generateId(),
          file,
          documentType: selectedDocumentType,
          status: 'pending',
          progress: 0,
        });
      }
    }
    setUploadQueue((prev) => [...prev, ...newItems]);
    // Pass the new items directly to avoid stale closure issue
    // Pass customerName together to avoid stale closure issues.
    const pendingNewItems = newItems.filter(item => item.status === 'pending');
    if (pendingNewItems.length > 0) {
      const currentCustomerName = customerName; // Capture the latest value before scheduling.
      setTimeout(() => processQueue(pendingNewItems, currentCustomerName), 100);
    }
  }, [selectedDocumentType, selectedFileTypeConfig.acceptedExtensions, processQueue, customerName]);

  const handleDrop = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
    if (e.dataTransfer.files.length > 0) {
      addFilesToQueue(e.dataTransfer.files);
    }
  }, [addFilesToQueue]);

  const handleDragOver = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  }, []);

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      addFilesToQueue(e.target.files);
    }
    e.target.value = '';
  }, [addFilesToQueue]);

  const handleUploadClick = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleCustomerSelect = useCallback((customerId: string) => {
    if (!customerId) {
      setCurrentCustomer(null, null);
      setCustomerName('');
      return;
    }
    const target = customerOptions.find((item) => item.record_id === customerId);
    const nextName = target?.name ?? '';
    setCurrentCustomer(nextName || null, customerId);
    setCustomerName(nextName);
  }, [customerOptions, setCurrentCustomer]);

  const handleCancelUpload = useCallback(() => {
    abort();
    setUploadQueue((prev) => prev.map((item) => 
      (item.status === 'pending' || item.status === 'processing')
        ? { ...item, status: 'error' as const, error: '已取消' }
        : item
    ));
  }, [abort]);

  const clearCompletedQueue = useCallback(() => {
    setUploadQueue((prev) => prev.filter((item) => 
      item.status !== 'success' && item.status !== 'error'
    ));
  }, []);

  const removeUploadedFile = useCallback((id: string) => {
    setUploadedFiles((prev) => prev.filter((file) => file.id !== id));
  }, []);

  const viewResult = useCallback((result: ExtractionResult) => {
    void result;
    alert('资料已提取完成。可前往“资料汇总”或“客户管理”查看整理后的内容。');
  }, []);

  return (
    <div className="min-h-screen bg-slate-50 p-6">
      {/* Page Header */}
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-800 mb-1">上传资料</h1>
          <p className="text-gray-500 text-sm">请上传客户的征信、流水、财务数据等资料</p>
        </div>
        <div className="flex gap-3">
          <select
            value={state.extraction.currentCustomerId || ''}
            onChange={(e) => handleCustomerSelect(e.target.value)}
            className="px-4 py-2 bg-white border border-gray-200 rounded-lg text-gray-800 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 min-w-[220px]"
          >
            <option value="">{customersLoading ? '加载客户中...' : '请选择客户'}</option>
            {customerOptions.map((customer) => (
              <option key={customer.record_id} value={customer.record_id}>
                {formatCustomerOptionLabel(customer.record_id, customer.name)}
              </option>
            ))}
          </select>
          <select
            value={selectedDocumentType}
            onChange={(e) => setSelectedDocumentType(e.target.value)}
            className="px-4 py-2 bg-white border border-gray-200 rounded-lg text-gray-800 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="" disabled className="text-gray-400">自动识别类型（暂不可用）</option>
            {FILE_TYPES.map((config) => (
              <option key={config.id} value={config.id}>{config.name}</option>
            ))}
          </select>
          <input
            type="text"
            value={customerName}
            onChange={(e) => setCustomerName(e.target.value)}
            placeholder="输入客户名称（用于智能合并）"
            className="px-4 py-2 bg-white border border-gray-200 rounded-lg text-gray-800 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-48"
          />
          {state.extraction.currentCustomerId && (
            <button
              type="button"
              onClick={() => handleCustomerSelect('')}
              className="px-4 py-2 bg-slate-100 text-slate-700 rounded-lg text-sm font-medium hover:bg-slate-200 transition-colors"
            >
              清空选择
            </button>
          )}
          <button
            onClick={handleUploadClick}
            disabled={isProcessing}
            className="px-4 py-2 bg-blue-500 text-white rounded-lg text-sm font-medium hover:bg-blue-600 transition-colors shadow-lg shadow-blue-500/20 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isProcessing ? '资料处理中...' : '选择文件并开始处理'}
          </button>
        </div>
      </div>

      <div className="mb-6 grid gap-4 md:grid-cols-3">
        <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="text-xs text-slate-500">当前客户上下文</div>
          <div className="mt-2 text-base font-semibold text-slate-800">
            {state.extraction.currentCustomer || '未选择客户'}
          </div>
          <div className="mt-1 text-xs text-slate-400">
            新上传资料会优先并入当前客户，并自动刷新资料汇总与问答索引
          </div>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="text-xs text-slate-500">处理状态</div>
          <div className="mt-2 text-base font-semibold text-slate-800">
            {isProcessing ? '系统正在处理资料' : uploadQueue.length > 0 ? '本轮上传已处理完成' : '等待上传资料'}
          </div>
          <div className="mt-1 text-xs text-slate-400">
            {isProcessing ? '上传成功后将自动触发资料汇总更新和索引重建' : '支持企业征信、个人征信、流水、财务数据、水母报告等资料'}
          </div>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
          <div className="text-xs text-slate-500">本轮结果</div>
          <div className="mt-2 text-base font-semibold text-slate-800">
            成功 {uploadQueue.filter((item) => item.status === 'success').length} 份 / 失败 {uploadQueue.filter((item) => item.status === 'error').length} 份
          </div>
          <div className="mt-1 text-xs text-slate-400">
            已上传文件 {uploadedFiles.length} 份，可继续补传并自动并入同一客户资料
          </div>
        </div>
      </div>

      <ProcessFeedbackCard
        tone={uploadSummary.tone}
        title={uploadSummary.title}
        description={uploadSummary.description}
        persistenceHint={uploadSummary.persistenceHint}
        nextStep={uploadSummary.nextStep}
        className="mb-6"
      />

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept={selectedFileTypeConfig.acceptedExtensions.join(',')}
        onChange={handleFileSelect}
        className="hidden"
      />

      {/* Task 6.1: File Types Grid */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 mb-5 shadow-sm">
        <div className="flex items-center gap-2 mb-4 text-gray-800 text-sm font-semibold">
          <FileText className="w-4 h-4 text-blue-500" />
          支持的文件类型
        </div>
        <div data-testid="file-types-grid" className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-4">
          {FILE_TYPES.map((config) => (
            <FileTypeCard key={config.id} config={config} />
          ))}
        </div>
      </div>

      {/* Task 6.2: Drag and Drop Zone */}
      <div
        data-testid="drop-zone"
        onClick={handleUploadClick}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        className={`
          bg-white rounded-xl border-2 border-dashed p-12 mb-5 text-center cursor-pointer transition-all
          ${isDragOver 
            ? 'border-blue-500 bg-blue-50' 
            : 'border-gray-300 hover:border-blue-400 hover:bg-blue-50/30'
          }
        `}
      >
        <Upload className={`w-16 h-16 mx-auto mb-4 ${isDragOver ? 'text-blue-600' : 'text-gray-400'}`} />
        <div className={`text-base font-medium mb-2 ${isDragOver ? 'text-blue-600' : 'text-gray-700'}`}>
          {isDragOver ? '松开鼠标上传文件' : '拖拽文件到此处，或点击上传'}
        </div>
        <div className="text-gray-400 text-sm">
          当前支持 {selectedFileTypeConfig.formats}，单个文件最大 50MB
        </div>
      </div>

      {/* Task 6.3: Upload Queue with Progress */}
      {uploadQueue.length > 0 && (
        <div data-testid="upload-queue" className="bg-white rounded-xl border border-gray-200 p-5 mb-5 shadow-sm">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2 text-gray-800 text-sm font-semibold">
              <Upload className="w-4 h-4 text-gray-500" />
              上传进度 ({uploadQueue.length})
            </div>
            <div className="flex gap-2">
              {isProcessing && (
                <button
                  onClick={handleCancelUpload}
                  className="px-3 py-1.5 bg-white border border-red-500 rounded-lg text-red-500 text-xs hover:bg-red-50 transition-colors"
                >
                  取消上传
                </button>
              )}
              {uploadQueue.some((item) => item.status === 'success' || item.status === 'error') && (
                <button
                  onClick={clearCompletedQueue}
                  className="px-3 py-1.5 bg-white border border-gray-200 rounded-lg text-gray-500 text-xs hover:bg-gray-50 transition-colors"
                >
                  清除已完成
                </button>
              )}
            </div>
          </div>
          <div className="space-y-3">
            {uploadQueue.map((item) => (
              <QueueItemDisplay key={item.id} item={item} />
            ))}
          </div>
        </div>
      )}

      {/* Uploaded Files List */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
        <div className="flex items-center gap-2 mb-4 text-gray-800 text-sm font-semibold">
          <FileText className="w-4 h-4 text-gray-500" />
          已上传文件 ({uploadedFiles.length})
        </div>
        {uploadedFiles.length === 0 ? (
          <div className="text-center py-8 text-gray-400">
            <FileText className="w-12 h-12 mx-auto mb-2 opacity-50" />
            <p>本轮还没有已上传资料</p>
          </div>
        ) : (
          <div className="space-y-3">
            {uploadedFiles.map((file) => {
              const IconComponent = getFileTypeIcon(file.documentType);
              return (
                <div key={file.id} className="flex justify-between items-center p-4 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${file.color}`}>
                      <IconComponent className="w-5 h-5" />
                    </div>
                    <div className="flex flex-col">
                      <span className="text-gray-800 text-sm font-medium">{file.name}</span>
                      <span className="text-gray-400 text-xs">
                        {file.size} · {file.time}
                        {file.result.savedToFeishu && (
                          <span className="text-green-600 ml-2 inline-flex items-center gap-1">
                            <Check className="w-3 h-3" />
                            已保存到本地
                          </span>
                        )}
                      </span>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => viewResult(file.result)}
                      className="px-3 py-1.5 bg-white border border-gray-200 rounded-lg text-gray-500 text-xs hover:bg-gray-50 transition-colors"
                    >
                      查看整理结果
                    </button>
                    <button
                      onClick={() => removeUploadedFile(file.id)}
                      className="px-3 py-1.5 bg-white border border-red-500 rounded-lg text-red-500 text-xs hover:bg-red-50 transition-colors"
                    >
                      删除
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Error Toast */}
      {error && (
        <div className="fixed bottom-4 right-4 bg-red-50 border border-red-200 rounded-lg p-4 shadow-lg max-w-md">
          <div className="flex items-start gap-2">
            <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-red-800 font-medium">本次处理未完成</p>
              <p className="text-red-600 text-sm">{error.message}</p>
            </div>
            <button onClick={resetLoading} className="text-red-400 hover:text-red-600 ml-2">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadPage;


