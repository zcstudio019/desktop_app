import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  AlertCircle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ClipboardList,
  Download,
  Edit3,
  Save,
} from 'lucide-react';
import { getApplication, saveApplication } from '../services/api';
import ApplicationDiffCatalogPanel, {
  type ApplicationDiffCatalogFilterMode,
  type ApplicationDiffTargetGroup,
} from './ApplicationDiffCatalogPanel';
import ApplicationSectionCard from './ApplicationSectionCard';
import {
  collectDiffTargets,
  countDiffStats,
  hasVisibleFieldsInSection,
  type ApplicationDiffFilterMode,
  type ApplicationDiffTarget,
} from './applicationDiffUtils';
import { APPLICATION_RESULT_COPY } from './applicationDiffCopy';
import { useApp } from '../context/AppContext';

export interface ApplicationResultCardData {
  customerFound?: boolean;
  customerName?: string;
  loanType?: string;
  applicationData?: Record<string, Record<string, unknown>>;
  applicationContent?: string;
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
    saved_application_id?: string;
    previous_application_id?: string;
    saved_application_version_group_id?: string;
    saved_application_version_no?: number;
  };
  needsInput?: boolean;
  requiredFields?: string[];
}

export interface ApplicationGuideCardProps {
  data: {
    action?: string;
    requiredFields?: string[];
  };
  onNavigate?: (page: string) => void;
}

export interface ApplicationResultCardProps {
  relatedJobId?: string | null;
  onPersistMessageData?: (relatedJobId: string, nextData: ApplicationResultCardData) => void;
  data: ApplicationResultCardData;
  onNavigate?: (page: string) => void;
  previousApplicationCache?: Record<string, Record<string, Record<string, unknown>>>;
  onCachePreviousApplication?: (
    applicationId: string,
    applicationData: Record<string, Record<string, unknown>>,
  ) => void;
}

const APPLICATION_DIFF_FILTER_STORAGE_PREFIX = 'loan-assistant:application-diff-filter:';
const APPLICATION_HISTORY_DIFF_STORAGE_PREFIX = 'loan-assistant:application-history-diff:';
const APPLICATION_DIFF_CATALOG_STORAGE_PREFIX = 'loan-assistant:application-diff-catalog:';

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

function buildApplicationFormHtml(
  customerName: string,
  loanType: string,
  applicationData: Record<string, Record<string, unknown>>,
): string {
  const safeCustomerName = customerName.trim() || APPLICATION_RESULT_COPY.unnamedCustomer;
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
              <tbody>${rows}</tbody>
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
    <title>${escapeHtml(APPLICATION_RESULT_COPY.downloadFilePrefix)} - ${escapeHtml(safeCustomerName)}</title>
    <style>
      body { margin: 0; padding: 32px; background: #f8fafc; color: #0f172a; font-family: "PingFang SC", "Microsoft YaHei", sans-serif; }
      .page { max-width: 960px; margin: 0 auto; }
      .hero { background: linear-gradient(135deg, #fff7ed 0%, #fffbeb 100%); border: 1px solid #fed7aa; border-radius: 24px; padding: 28px; margin-bottom: 24px; }
      .hero h1 { margin: 0; font-size: 28px; line-height: 1.2; }
      .hero p { margin: 12px 0 0; color: #64748b; font-size: 15px; }
      .hero-meta { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 18px; }
      .chip { display: inline-flex; align-items: center; gap: 6px; padding: 8px 12px; border-radius: 999px; background: rgba(255,255,255,0.92); border: 1px solid #fde68a; color: #92400e; font-size: 13px; }
      .section-stack { display: grid; gap: 16px; }
      .section-card { background: white; border: 1px solid #e2e8f0; border-radius: 20px; overflow: hidden; }
      .section-header { display: flex; justify-content: space-between; align-items: center; padding: 18px 20px; background: linear-gradient(135deg, #f8fafc 0%, #ffffff 100%); border-bottom: 1px solid #e2e8f0; }
      .section-title { font-size: 18px; font-weight: 700; }
      .section-count { color: #64748b; font-size: 13px; }
      .table-shell { padding: 0 20px 20px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { border-bottom: 1px solid #edf2f7; padding: 14px 12px; vertical-align: top; text-align: left; }
      th { width: 32%; color: #64748b; font-weight: 600; background: #fcfdff; }
      tr:last-child th, tr:last-child td { border-bottom: none; }
      .footer { margin-top: 24px; color: #64748b; font-size: 12px; text-align: center; }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <h1>${escapeHtml(safeCustomerName)} · ${escapeHtml(loanTypeLabel)}申请表</h1>
        <p>导出时间：${escapeHtml(exportedAt)}。当前文件为结构化申请表导出版，便于补充、核对与归档。</p>
        <div class="hero-meta">
          <span class="chip">客户：${escapeHtml(safeCustomerName)}</span>
          <span class="chip">融资类型：${escapeHtml(loanTypeLabel)}</span>
          <span class="chip">导出方式：网页表单</span>
        </div>
      </section>
      <section class="section-stack">${sectionsHtml}</section>
      <div class="footer">Loan Assistant 结构化申请表导出</div>
    </main>
  </body>
</html>`;
}

function cloneChatApplicationData(
  source: Record<string, Record<string, unknown>> | null | undefined,
): Record<string, Record<string, unknown>> {
  return Object.fromEntries(
    Object.entries(source || {}).map(([sectionName, sectionData]) => [
      sectionName,
      { ...(sectionData || {}) },
    ]),
  );
}

function buildChatApplicationFieldSource(
  fieldName: string,
  value: unknown,
  metadata?: ApplicationResultCardData['metadata'],
) {
  const text = String(value ?? '').trim();
  const profileVersion = metadata?.profile_version ? `资料汇总 V${metadata.profile_version}` : '当前资料汇总';
  const profileTime = metadata?.profile_updated_at
    ? `，最近更新于 ${formatLocalDateTime(metadata.profile_updated_at)}`
    : '';

  if (!text || text === '-' || /待补充|暂无/.test(text)) {
    return {
      label: '待补字段',
      detail: `当前客户资料中未找到可直接引用的内容，建议补充材料后重新生成。本次判断基于${profileVersion}${profileTime}。`,
    };
  }

  if (/经营地址|注册地址|经营状态|统一社会信用代码|行业类型|成立时间/.test(fieldName)) {
    return {
      label: '企业征信 / 资料汇总',
      detail: `主要来自企业征信报告基础信息和资料汇总，本次生成基于${profileVersion}${profileTime}。`,
    };
  }

  if (/纳税|开票|营收|利润|财务|流水|收入|回款/.test(fieldName)) {
    return {
      label: '财务 / 纳税 / 流水资料',
      detail: `主要来自财务数据、纳税资料、银行流水和经营类报告，本次生成基于${profileVersion}${profileTime}。`,
    };
  }

  if (/征信|负债|逾期|担保|诉讼|信用卡|隐形负债/.test(fieldName)) {
    return {
      label: '征信 / 负债资料',
      detail: `主要来自企业征信、个人征信、公共记录和负债资料；如字段可计算，系统会自动综合推导。本次生成基于${profileVersion}${profileTime}。`,
    };
  }

  if (/抵押|资产|存货|固定资产|净资产/.test(fieldName)) {
    return {
      label: '资产 / 抵押材料',
      detail: `主要来自抵押物资料、资产清单和补充材料，本次生成基于${profileVersion}${profileTime}。`,
    };
  }

  return {
    label: '资料汇总 / 结构化提取',
    detail: `系统综合读取资料汇总与结构化提取结果生成该字段，本次依据为${profileVersion}${profileTime}。`,
  };
}

function getLoanTypeLabel(loanType?: string): string {
  return loanType === 'personal' ? '个人贷款' : '企业贷款';
}

function buildStorageScope(parts: Array<string | null | undefined>): string {
  return parts.filter(Boolean).join(':');
}

export const ApplicationGuideCard: React.FC<ApplicationGuideCardProps> = ({ data, onNavigate }) => {
  const fieldLabels: Record<string, string> = {
    customer_name: '客户名称',
    loan_type: '贷款类型',
    loan_amount: '贷款金额',
    loan_term: '贷款期限',
    repayment_source: '还款来源',
    collateral: '抵押物信息',
    business_data: '经营数据',
    financial_data: '财务资料',
  };

  const fields = (data.requiredFields || []).map((field) => fieldLabels[field] || field);

  return (
    <div className="mt-3 overflow-hidden rounded-xl border border-amber-200 bg-white shadow-sm" data-testid="application-guide-card">
      <div className="border-b border-amber-100 bg-gradient-to-r from-amber-50 to-orange-50 px-4 py-3">
        <div className="flex items-start gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-amber-100 text-amber-600">
            <ClipboardList className="h-5 w-5" />
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-sm font-semibold text-amber-900">{APPLICATION_RESULT_COPY.cardTitleBlank}</div>
            <div className="mt-1 text-xs leading-5 text-amber-800/80">
              还缺少生成申请表所需的关键信息，请先补齐资料后再继续生成。
            </div>
          </div>
          {onNavigate ? (
            <button
              type="button"
              onClick={() => onNavigate('application')}
              className="rounded-lg bg-white px-3 py-1.5 text-xs font-medium text-amber-700 shadow-sm ring-1 ring-amber-200 transition hover:bg-amber-50"
            >
              去申请表页补充
            </button>
          ) : null}
        </div>
      </div>
      <div className="px-4 py-4">
        <div className="text-xs font-medium uppercase tracking-wide text-slate-500">建议补充字段</div>
        <div className="mt-3 flex flex-wrap gap-2">
          {fields.length > 0 ? (
            fields.map((field) => (
              <span
                key={field}
                className="inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700"
              >
                {field}
              </span>
            ))
          ) : (
            <span className="text-sm text-slate-500">请完善申请表基本信息后再继续。</span>
          )}
        </div>
      </div>
    </div>
  );
};

export const ApplicationResultCard: React.FC<ApplicationResultCardProps> = ({
  data,
  relatedJobId,
  onPersistMessageData,
  onNavigate,
  previousApplicationCache,
  onCachePreviousApplication,
}) => {
  const { state, setApplicationResult, updateChatMessagesByJob } = useApp();

  const hasStructuredData = Boolean(data.applicationData && Object.keys(data.applicationData).length > 0);
  const initialApplicationData = useMemo(
    () => cloneChatApplicationData(data.applicationData),
    [data.applicationData],
  );

  const [isExpanded, setIsExpanded] = useState(true);
  const [editMode, setEditMode] = useState(false);
  const [editedData, setEditedData] = useState<Record<string, Record<string, unknown>>>(initialApplicationData);
  const [savedData, setSavedData] = useState<Record<string, Record<string, unknown>>>(initialApplicationData);
  const [previousSavedData, setPreviousSavedData] = useState<Record<string, Record<string, unknown>> | null>(null);
  const [savedApplicationId, setSavedApplicationId] = useState<string | null>(data.metadata?.saved_application_id || null);
  const [savedPreviousApplicationId, setSavedPreviousApplicationId] = useState<string | null>(
    data.metadata?.previous_application_id || null,
  );
  const [savedVersionGroupId, setSavedVersionGroupId] = useState<string | null>(
    data.metadata?.saved_application_version_group_id || null,
  );
  const [savingEdit, setSavingEdit] = useState(false);
  const [saveEditError, setSaveEditError] = useState<string | null>(null);
  const [saveEditNotice, setSaveEditNotice] = useState<string | null>(null);
  const [diffFilter, setDiffFilter] = useState<ApplicationDiffFilterMode>('all');
  const [historyDiffBulkAction, setHistoryDiffBulkAction] = useState<{ mode: 'expand' | 'collapse'; token: number }>({
    mode: 'collapse',
    token: 0,
  });
  const [sectionBulkAction, setSectionBulkAction] = useState<{ mode: 'expand' | 'collapse'; token: number }>({
    mode: 'expand',
    token: 0,
  });
  const [activeDiffRowKey, setActiveDiffRowKey] = useState<string | null>(null);
  const [pendingScrollRowKey, setPendingScrollRowKey] = useState<string | null>(null);
  const [diffCatalogFilter, setDiffCatalogFilter] = useState<ApplicationDiffCatalogFilterMode>('all');
  const [isDiffCatalogOpen, setIsDiffCatalogOpen] = useState(false);

  const catalogOpenInitializedRef = useRef(false);

  const stableCustomerId = data.metadata?.customer_id || state.extraction.currentCustomerId || null;
  const stableCustomerName =
    data.customerName ||
    state.application.lastCustomer ||
    state.extraction.currentCustomer ||
    APPLICATION_RESULT_COPY.unnamedCustomer;
  const loanTypeLabel = getLoanTypeLabel(data.loanType);

  const diffScopeKey = useMemo(
    () =>
      buildStorageScope([
        savedVersionGroupId,
        savedApplicationId,
        stableCustomerId,
        stableCustomerName,
      ]),
    [savedApplicationId, savedVersionGroupId, stableCustomerId, stableCustomerName],
  );

  const diffFilterStorageKey = `${APPLICATION_DIFF_FILTER_STORAGE_PREFIX}${diffScopeKey}`;
  const historyDiffStorageKeyBase = `${APPLICATION_HISTORY_DIFF_STORAGE_PREFIX}${diffScopeKey}`;
  const diffCatalogStorageKey = `${APPLICATION_DIFF_CATALOG_STORAGE_PREFIX}${diffScopeKey}`;

  const currentSavedData = useMemo(
    () => (Object.keys(savedData).length > 0 ? savedData : initialApplicationData),
    [initialApplicationData, savedData],
  );
  const displayData = editMode ? editedData : currentSavedData;

  const diffStats = useMemo(
    () => countDiffStats(displayData, currentSavedData, previousSavedData),
    [currentSavedData, displayData, previousSavedData],
  );
  const hasUnsavedChanges = diffStats.current > 0;
  const hasHistoryDiffs = diffStats.history > 0;

  const hasVisibleFieldsForCurrentFilter = useMemo(() => {
    if (!editMode || diffFilter === 'all') {
      return true;
    }
    return Object.entries(displayData).some(([sectionName, sectionData]) =>
      hasVisibleFieldsInSection(
        sectionData,
        currentSavedData[sectionName] || {},
        previousSavedData?.[sectionName] || null,
        diffFilter,
      ),
    );
  }, [currentSavedData, diffFilter, displayData, editMode, previousSavedData]);

  const diffTargets = useMemo(() => {
    if (!editMode) {
      return [] as ApplicationDiffTarget[];
    }
    return Object.entries(displayData).flatMap(([sectionName, sectionData]) =>
      collectDiffTargets(
        sectionName,
        sectionData,
        currentSavedData[sectionName] || {},
        previousSavedData?.[sectionName] || null,
        diffFilter,
      ),
    );
  }, [currentSavedData, diffFilter, displayData, editMode, previousSavedData]);

  const groupedDiffTargets = useMemo<ApplicationDiffTargetGroup[]>(() => {
    const grouped = new Map<string, ApplicationDiffTarget[]>();
    diffTargets.forEach((target) => {
      const existing = grouped.get(target.groupKey) || [];
      existing.push(target);
      grouped.set(target.groupKey, existing);
    });

    return Array.from(grouped.entries()).map(([groupKey, items]) => ({
      groupKey,
      items,
      count: items.length,
      currentCount: items.filter((item) => item.kind === 'current').length,
      historyCount: items.filter((item) => item.kind === 'history').length,
      bothCount: items.filter((item) => item.kind === 'both').length,
    }));
  }, [diffTargets]);

  const filteredGroupedDiffTargets = useMemo<ApplicationDiffTargetGroup[]>(() => {
    const matchesCatalog = (target: ApplicationDiffTarget) => {
      if (diffCatalogFilter === 'all') return true;
      if (diffCatalogFilter === 'both') return target.kind === 'both';
      return target.kind === diffCatalogFilter || target.kind === 'both';
    };

    return groupedDiffTargets
      .map((group) => {
        const items = group.items.filter(matchesCatalog);
        return {
          ...group,
          items,
          count: items.length,
          currentCount: items.filter((item) => item.kind === 'current').length,
          historyCount: items.filter((item) => item.kind === 'history').length,
          bothCount: items.filter((item) => item.kind === 'both').length,
        };
      })
      .filter((group) => group.items.length > 0);
  }, [diffCatalogFilter, groupedDiffTargets]);

  const activeDiffTarget = useMemo(
    () => diffTargets.find((target) => target.rowKey === activeDiffRowKey) || null,
    [activeDiffRowKey, diffTargets],
  );

  const hasNavigableDiffs = diffTargets.length > 0;
  const hasCustomizedReviewState =
    diffFilter !== 'all' || diffCatalogFilter !== 'all' || !isDiffCatalogOpen || activeDiffRowKey !== null;

  const generatedAtLabel = formatLocalDateTime(data.metadata?.generated_at);
  const profileVersionLabel = data.metadata?.profile_version ? `V${data.metadata.profile_version}` : '未记录';
  const profileUpdatedAtLabel = formatLocalDateTime(data.metadata?.profile_updated_at);
  const staleAtLabel = formatLocalDateTime(data.metadata?.stale_at);
  const sameCustomerStale = Boolean(data.metadata?.stale);
  const applicationStatusBadge = editMode
    ? { label: APPLICATION_RESULT_COPY.editModeBadge, className: 'border-blue-200 bg-blue-50 text-blue-700' }
    : sameCustomerStale
      ? { label: '需重新生成', className: 'border-amber-200 bg-amber-50 text-amber-700' }
      : { label: '最新可用', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' };

  useEffect(() => {
    setSavedApplicationId(data.metadata?.saved_application_id || null);
    setSavedPreviousApplicationId(data.metadata?.previous_application_id || null);
    setSavedVersionGroupId(data.metadata?.saved_application_version_group_id || null);
  }, [
    data.metadata?.previous_application_id,
    data.metadata?.saved_application_id,
    data.metadata?.saved_application_version_group_id,
  ]);

  useEffect(() => {
    if (editMode) return;
    setSavedData(initialApplicationData);
    setEditedData(initialApplicationData);
  }, [editMode, initialApplicationData]);

  useEffect(() => {
    if (typeof window === 'undefined' || !diffScopeKey) return;
    const stored = window.localStorage.getItem(diffFilterStorageKey);
    if (stored === 'all' || stored === 'current' || stored === 'history') {
      setDiffFilter(stored);
    }
  }, [diffFilterStorageKey, diffScopeKey]);

  useEffect(() => {
    if (typeof window === 'undefined' || !diffScopeKey) return;
    window.localStorage.setItem(diffFilterStorageKey, diffFilter);
  }, [diffFilter, diffFilterStorageKey, diffScopeKey]);

  useEffect(() => {
    if (!editMode) return;
    if (diffTargets.length === 0) {
      setIsDiffCatalogOpen(false);
      return;
    }
    if (typeof window === 'undefined' || !diffScopeKey) return;

    const stored = window.localStorage.getItem(diffCatalogStorageKey);
    if (stored === 'open' || stored === 'closed') {
      setIsDiffCatalogOpen(stored === 'open');
      catalogOpenInitializedRef.current = true;
      return;
    }

    if (!catalogOpenInitializedRef.current) {
      setIsDiffCatalogOpen(window.matchMedia('(min-width: 1400px)').matches);
      catalogOpenInitializedRef.current = true;
    }
  }, [diffCatalogStorageKey, diffScopeKey, diffTargets.length, editMode]);

  useEffect(() => {
    if (typeof window === 'undefined' || !diffScopeKey || !editMode) return;
    window.localStorage.setItem(diffCatalogStorageKey, isDiffCatalogOpen ? 'open' : 'closed');
  }, [diffCatalogStorageKey, diffScopeKey, editMode, isDiffCatalogOpen]);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const handler = (event: BeforeUnloadEvent) => {
      if (editMode && hasUnsavedChanges) {
        event.preventDefault();
        event.returnValue = '';
      }
    };

    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [editMode, hasUnsavedChanges]);

  useEffect(() => {
    if (!saveEditNotice || editMode) {
      return;
    }

    const timer = window.setTimeout(() => {
      setSaveEditNotice(null);
    }, 4000);

    return () => window.clearTimeout(timer);
  }, [editMode, saveEditNotice]);

  useEffect(() => {
    if (!pendingScrollRowKey) return;

    const nodes = Array.from(document.querySelectorAll('[data-diff-row-key]'));
    const target = nodes.find(
      (node) => node.getAttribute('data-diff-row-key') === pendingScrollRowKey,
    ) as HTMLElement | undefined;

    if (target) {
      target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setActiveDiffRowKey(pendingScrollRowKey);
      setPendingScrollRowKey(null);
    }
  }, [pendingScrollRowKey]);

  useEffect(() => {
    setActiveDiffRowKey(null);
  }, [diffFilter, editMode, savedApplicationId]);

  useEffect(() => {
    setDiffCatalogFilter('all');
  }, [diffFilter, savedApplicationId]);

  useEffect(() => {
    let cancelled = false;

    if (!savedPreviousApplicationId) {
      setPreviousSavedData(null);
      return;
    }

    const cached = previousApplicationCache?.[savedPreviousApplicationId];
    if (cached) {
      setPreviousSavedData(cloneChatApplicationData(cached));
      return;
    }

    void getApplication(savedPreviousApplicationId)
      .then((response) => {
        if (cancelled) return;
        const previousApplicationData = cloneChatApplicationData(
          response.applicationData as Record<string, Record<string, unknown>>,
        );
        setPreviousSavedData(previousApplicationData);
        onCachePreviousApplication?.(savedPreviousApplicationId, previousApplicationData);
      })
      .catch(() => {
        if (!cancelled) {
          setPreviousSavedData(null);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [onCachePreviousApplication, previousApplicationCache, savedPreviousApplicationId]);

  const handleFieldChange = useCallback((sectionTitle: string, fieldName: string, value: string) => {
    setEditedData((prev) => ({
      ...prev,
      [sectionTitle]: {
        ...(prev[sectionTitle] || {}),
        [fieldName]: value,
      },
    }));
    setSaveEditNotice(null);
    setSaveEditError(null);
  }, []);

  const navigateDiffField = useCallback(
    (direction: 'prev' | 'next') => {
      if (diffTargets.length === 0) return;

      const currentIndex = diffTargets.findIndex((item) => item.rowKey === activeDiffRowKey);
      const nextIndex =
        direction === 'next'
          ? (currentIndex + 1 + diffTargets.length) % diffTargets.length
          : (currentIndex - 1 + diffTargets.length) % diffTargets.length;
      const target = diffTargets[nextIndex];
      setSectionBulkAction((prev) => ({ mode: 'expand', token: prev.token + 1 }));
      setPendingScrollRowKey(target.rowKey);
    },
    [activeDiffRowKey, diffTargets],
  );

  const resetReviewView = useCallback(() => {
    setDiffFilter('all');
    setDiffCatalogFilter('all');
    setIsDiffCatalogOpen(diffTargets.length > 0);
    setActiveDiffRowKey(null);
    setPendingScrollRowKey(null);
    setHistoryDiffBulkAction((prev) => ({ mode: 'collapse', token: prev.token + 1 }));
    setSectionBulkAction((prev) => ({ mode: 'expand', token: prev.token + 1 }));
  }, [diffTargets.length]);

  const toggleEditMode = useCallback(() => {
    setSaveEditNotice(null);
    setSaveEditError(null);

    if (!editMode) {
      setEditedData(cloneChatApplicationData(currentSavedData));
      setEditMode(true);
      return;
    }

    if (hasUnsavedChanges && !window.confirm(APPLICATION_RESULT_COPY.discardChangesConfirm)) {
      return;
    }

    setEditedData(cloneChatApplicationData(currentSavedData));
    setEditMode(false);
  }, [currentSavedData, editMode, hasUnsavedChanges]);

  const saveEditedData = useCallback(async () => {
    if (!hasUnsavedChanges) return;

    setSavingEdit(true);
    setSaveEditError(null);

    try {
      const currentSnapshot = cloneChatApplicationData(currentSavedData);
      const nextSavedSnapshot = cloneChatApplicationData(editedData);

      const response = await saveApplication({
        customerName: stableCustomerName,
        customerId: stableCustomerId,
        loanType: data.loanType || 'enterprise',
        applicationData: nextSavedSnapshot,
        baseApplicationId: savedApplicationId || undefined,
        versionGroupId: savedVersionGroupId || undefined,
      });

      if (savedApplicationId) {
        onCachePreviousApplication?.(savedApplicationId, currentSnapshot);
      }

      setPreviousSavedData(currentSnapshot);
      setSavedData(nextSavedSnapshot);
      setEditedData(nextSavedSnapshot);
      setSavedApplicationId(response.id);
      setSavedPreviousApplicationId(response.previousApplicationId || null);
      setSavedVersionGroupId(response.versionGroupId || null);

      const nextMetadata: NonNullable<ApplicationResultCardData['metadata']> = {
        ...(data.metadata || {}),
        saved_application_id: response.id,
        previous_application_id: response.previousApplicationId || undefined,
        saved_application_version_group_id: response.versionGroupId || undefined,
        saved_application_version_no: response.versionNo,
      };

      const nextCardData: ApplicationResultCardData = {
        ...data,
        customerName: response.customerName || data.customerName,
        loanType: response.loanType || data.loanType,
        applicationData: nextSavedSnapshot,
        metadata: nextMetadata,
      };

      setApplicationResult(
        {
          content: nextCardData.applicationContent || '',
          customerFound: nextCardData.customerFound ?? true,
          warnings: nextCardData.warnings || [],
          applicationData: nextSavedSnapshot as Record<string, Record<string, string>>,
          metadata: nextMetadata,
        },
        nextCardData.customerName,
      );

      if (relatedJobId) {
        if (onPersistMessageData) {
          onPersistMessageData(relatedJobId, nextCardData);
        } else {
          updateChatMessagesByJob(relatedJobId, {
            data: nextCardData as unknown as Record<string, unknown>,
          });
        }
      }

      const versionLabel = response.versionNo
        ? APPLICATION_RESULT_COPY.saveVersionWithNo(response.versionNo)
        : APPLICATION_RESULT_COPY.saveVersionGeneric;
      setSaveEditNotice(
        `${versionLabel} ${
          response.previousApplicationId
            ? APPLICATION_RESULT_COPY.saveHistoryReady
            : APPLICATION_RESULT_COPY.saveHistoryEmpty
        }`,
      );
      setEditMode(false);
    } catch (error) {
      setSaveEditError(
        error instanceof Error && error.message ? error.message : APPLICATION_RESULT_COPY.saveFailedFallback,
      );
    } finally {
      setSavingEdit(false);
    }
  }, [
    currentSavedData,
    data,
    editedData,
    hasUnsavedChanges,
    onCachePreviousApplication,
    onPersistMessageData,
    relatedJobId,
    savedApplicationId,
    savedVersionGroupId,
    setApplicationResult,
    stableCustomerId,
    stableCustomerName,
    updateChatMessagesByJob,
  ]);

  useEffect(() => {
    if (!editMode || typeof window === 'undefined') {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const isTypingTarget =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target?.isContentEditable;

      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
        event.preventDefault();
        if (!savingEdit && hasUnsavedChanges) {
          void saveEditedData();
        }
        return;
      }

      if (event.key === 'Escape' && !savingEdit) {
        if (isTypingTarget) {
          const input = target as HTMLInputElement | HTMLTextAreaElement;
          if (typeof input.value === 'string' && input.value.length > 0) {
            return;
          }
        }
        event.preventDefault();
        toggleEditMode();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [editMode, hasUnsavedChanges, saveEditedData, savingEdit, toggleEditMode]);

  const downloadFormHtml = useCallback(() => {
    const exportData = editMode ? editedData : currentSavedData;
    const html = buildApplicationFormHtml(stableCustomerName, data.loanType || 'enterprise', exportData);
    createDownloadLink(
      new Blob([html], { type: 'text/html;charset=utf-8' }),
      `${APPLICATION_RESULT_COPY.downloadFilePrefix}-${stableCustomerName || APPLICATION_RESULT_COPY.unnamedCustomer}.html`,
    );
  }, [currentSavedData, data.loanType, editMode, editedData, stableCustomerName]);

  if (data.needsInput || (!hasStructuredData && !data.applicationContent)) {
    return <ApplicationGuideCard data={data} onNavigate={onNavigate} />;
  }

  return (
    <div className="mt-3 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm" data-testid="application-result-card">
      <div className="border-b border-gray-100 bg-gradient-to-r from-amber-50 to-orange-50 px-4 py-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="flex min-w-0 items-start gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-100 text-emerald-600">
              <CheckCircle2 className="h-5 w-5" />
            </div>
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-gray-800">
                {data.customerFound ? APPLICATION_RESULT_COPY.cardTitleGenerated : APPLICATION_RESULT_COPY.cardTitleBlank}
                <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${applicationStatusBadge.className}`}>
                  {applicationStatusBadge.label}
                </span>
              </div>
              <div className="mt-0.5 break-words text-xs text-gray-500">
                {`${APPLICATION_RESULT_COPY.customerLabel}${stableCustomerName} · ${loanTypeLabel}`}
              </div>
              <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-slate-500">
                <span>{APPLICATION_RESULT_COPY.generatedAtLabel}{generatedAtLabel}</span>
                <span>{APPLICATION_RESULT_COPY.profileVersionLabel}{profileVersionLabel}</span>
                <span>{APPLICATION_RESULT_COPY.profileUpdatedAtLabel}{profileUpdatedAtLabel}</span>
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-end gap-2">
            {hasStructuredData ? (
              editMode ? (
                <>
                  <button
                    onClick={saveEditedData}
                    disabled={savingEdit || !hasUnsavedChanges}
                    title={APPLICATION_RESULT_COPY.saveButtonShortcutHint}
                    className="flex items-center gap-1 rounded-lg bg-green-500 px-2.5 py-1.5 text-xs font-medium text-white transition-colors hover:bg-green-600 disabled:cursor-not-allowed disabled:bg-slate-300 disabled:hover:bg-slate-300"
                    data-testid="save-button"
                  >
                    <Save className="h-3.5 w-3.5" />
                    {savingEdit
                      ? APPLICATION_RESULT_COPY.saveButtonSaving
                      : hasUnsavedChanges
                        ? APPLICATION_RESULT_COPY.saveButtonIdle
                        : APPLICATION_RESULT_COPY.saveButtonDisabled}
                  </button>
                  <button
                    onClick={toggleEditMode}
                    disabled={savingEdit}
                    title={APPLICATION_RESULT_COPY.cancelEditShortcutHint}
                    className="rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
                    data-testid="cancel-edit-button"
                  >
                    {hasUnsavedChanges
                      ? APPLICATION_RESULT_COPY.cancelEditWithChanges
                      : APPLICATION_RESULT_COPY.cancelEditWithoutChanges}
                  </button>
                </>
              ) : (
                <button
                  onClick={toggleEditMode}
                  className="flex items-center gap-1 rounded-lg bg-gray-100 px-2.5 py-1.5 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-200"
                  data-testid="edit-button"
                >
                  <Edit3 className="h-3.5 w-3.5" />
                  {APPLICATION_RESULT_COPY.editButton}
                </button>
              )
            ) : null}
            {hasStructuredData ? (
              <button
                onClick={downloadFormHtml}
                disabled={savingEdit}
                title={editMode ? APPLICATION_RESULT_COPY.downloadDraftButton : APPLICATION_RESULT_COPY.downloadButton}
                className="flex items-center gap-1 rounded-lg bg-purple-500 px-2.5 py-1.5 text-xs font-medium text-white transition-colors hover:bg-purple-600 disabled:cursor-not-allowed disabled:opacity-50"
                data-testid="download-form-button"
              >
                <Download className="h-3.5 w-3.5" />
                {editMode
                  ? APPLICATION_RESULT_COPY.downloadDraftButton
                  : APPLICATION_RESULT_COPY.downloadButton}
              </button>
            ) : null}
            <button
              onClick={() => setIsExpanded((prev) => !prev)}
              type="button"
              aria-label={isExpanded ? APPLICATION_RESULT_COPY.collapseCardButton : APPLICATION_RESULT_COPY.expandCardButton}
              title={isExpanded ? APPLICATION_RESULT_COPY.collapseCardButton : APPLICATION_RESULT_COPY.expandCardButton}
              className="rounded-lg p-1.5 transition-colors hover:bg-amber-100"
            >
              {isExpanded ? <ChevronDown className="h-4 w-4 text-gray-500" /> : <ChevronRight className="h-4 w-4 text-gray-500" />}
            </button>
          </div>
        </div>
      </div>

      {data.warnings?.length ? (
        <div className="border-b border-yellow-100 bg-yellow-50 px-4 py-2">
          {data.warnings.map((warning, index) => (
            <div key={`${warning}-${index}`} className="flex items-center gap-2 text-xs text-yellow-700">
              <AlertCircle className="h-3.5 w-3.5" />
              <span>{warning}</span>
            </div>
          ))}
        </div>
      ) : null}

      {saveEditError ? (
        <div className="border-b border-rose-100 bg-rose-50/80 px-4 py-3 text-sm text-rose-700">{saveEditError}</div>
      ) : null}

      {saveEditNotice ? (
        <div className="border-b border-emerald-100 bg-emerald-50/80 px-4 py-3 text-sm text-emerald-700">{saveEditNotice}</div>
      ) : null}

      {editMode ? (
        <div className="border-b border-slate-100 bg-white px-4 py-3">
          <div className="space-y-3">
            <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
              <div className="flex flex-col gap-1">
                <div className="flex flex-wrap items-center gap-2 text-sm text-slate-600">
                  <span className="font-medium text-slate-700">{APPLICATION_RESULT_COPY.editSummaryPrefix}</span>
                  <span className="text-xs text-slate-500">
                    {APPLICATION_RESULT_COPY.editSummaryStats(diffStats.current, diffStats.history, diffStats.total)}
                  </span>
                </div>
                <div className="text-[11px] text-slate-400">
                  {APPLICATION_RESULT_COPY.editSummaryShortcutHint}
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                {hasUnsavedChanges ? (
                  <span className="inline-flex items-center rounded-full bg-amber-100 px-2.5 py-1 text-xs font-medium text-amber-700">
                    {APPLICATION_RESULT_COPY.unsavedChangesBadge}
                  </span>
                ) : null}
                {diffTargets.length > 0 ? (
                  <button
                    type="button"
                    onClick={() => setIsDiffCatalogOpen((prev) => !prev)}
                    className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                  >
                    {isDiffCatalogOpen ? APPLICATION_RESULT_COPY.toggleCatalogClose : APPLICATION_RESULT_COPY.toggleCatalogOpen}
                  </button>
                ) : null}
                {hasCustomizedReviewState ? (
                  <button
                    type="button"
                    onClick={resetReviewView}
                    className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                  >
                    {APPLICATION_RESULT_COPY.resetReviewViewButton}
                  </button>
                ) : null}
              </div>
            </div>

            <div className="grid gap-3 xl:grid-cols-2 2xl:grid-cols-3">
              <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                <div className="text-[11px] font-semibold tracking-wide text-slate-500">{APPLICATION_RESULT_COPY.filterGroupTitle}</div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {[
                    { value: 'all' as const, label: APPLICATION_RESULT_COPY.filterAllFields },
                    { value: 'current' as const, label: APPLICATION_RESULT_COPY.filterCurrentDiff(diffStats.current) },
                    { value: 'history' as const, label: APPLICATION_RESULT_COPY.filterHistoryDiff(diffStats.history) },
                  ].map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => setDiffFilter(option.value)}
                      className={`rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                        diffFilter === option.value
                          ? 'border-blue-500 bg-blue-50 text-blue-700'
                          : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="rounded-xl border border-dashed border-slate-200 bg-white p-3">
                <div className="text-[11px] font-semibold tracking-wide text-slate-400">{APPLICATION_RESULT_COPY.bulkActionsTitle}</div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {hasHistoryDiffs ? (
                    <>
                      <button
                        type="button"
                        onClick={() => setHistoryDiffBulkAction((prev) => ({ mode: 'expand', token: prev.token + 1 }))}
                        className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                      >
                        <span className="hidden 2xl:inline">{APPLICATION_RESULT_COPY.expandAllHistory}</span>
                        <span className="2xl:hidden">展开历史差异</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => setHistoryDiffBulkAction((prev) => ({ mode: 'collapse', token: prev.token + 1 }))}
                        className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                      >
                        <span className="hidden 2xl:inline">{APPLICATION_RESULT_COPY.collapseAllHistory}</span>
                        <span className="2xl:hidden">收起历史差异</span>
                      </button>
                    </>
                  ) : (
                    <span className="inline-flex items-center rounded-full border border-dashed border-slate-300 bg-white px-3 py-1.5 text-xs text-slate-500">
                      {APPLICATION_RESULT_COPY.bulkActionsEmptyHistory}
                    </span>
                  )}
                  <button
                    type="button"
                    onClick={() => setSectionBulkAction((prev) => ({ mode: 'expand', token: prev.token + 1 }))}
                    className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                  >
                    <span className="hidden 2xl:inline">{APPLICATION_RESULT_COPY.expandAllSections}</span>
                    <span className="2xl:hidden">展开分组</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setSectionBulkAction((prev) => ({ mode: 'collapse', token: prev.token + 1 }))}
                    className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                  >
                    <span className="hidden 2xl:inline">{APPLICATION_RESULT_COPY.collapseAllSections}</span>
                    <span className="2xl:hidden">收起分组</span>
                  </button>
                </div>
              </div>

              <div className="rounded-xl border border-slate-200 bg-slate-50 p-3 xl:col-span-2 2xl:col-span-1">
                <div className="text-[11px] font-semibold tracking-wide text-slate-500">{APPLICATION_RESULT_COPY.navigationTitle}</div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {hasNavigableDiffs ? (
                    <>
                      <button
                        type="button"
                        onClick={() => navigateDiffField('prev')}
                        className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                      >
                        <span className="hidden xl:inline">{APPLICATION_RESULT_COPY.previousDiffField}</span>
                        <span className="xl:hidden">上一个</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => navigateDiffField('next')}
                        className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                      >
                        <span className="hidden xl:inline">{APPLICATION_RESULT_COPY.nextDiffField}</span>
                        <span className="xl:hidden">下一个</span>
                      </button>
                    </>
                  ) : (
                    <span className="inline-flex items-center rounded-full border border-dashed border-slate-300 bg-white px-3 py-1.5 text-xs text-slate-500">
                      {APPLICATION_RESULT_COPY.navigationEmptyState}
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {sameCustomerStale ? (
        <div className="border-b border-amber-100 bg-amber-50/80 px-4 py-3 text-sm text-amber-800">
          <div className="font-semibold text-amber-900">{APPLICATION_RESULT_COPY.staleTitle}</div>
          <div className="mt-1 text-xs leading-5 text-amber-800/80">
            {data.metadata?.stale_reason || (staleAtLabel ? `失效时间：${staleAtLabel}` : APPLICATION_RESULT_COPY.staleFallbackTime)}
          </div>
          {onNavigate ? (
            <button
              type="button"
              onClick={() => onNavigate('application')}
              className="mt-2 rounded-lg bg-white px-3 py-1.5 text-xs font-medium text-amber-700 shadow-sm ring-1 ring-amber-200 transition hover:bg-amber-50"
            >
              {APPLICATION_RESULT_COPY.regenerateButton}
            </button>
          ) : null}
        </div>
      ) : null}

      {isExpanded ? (
        <div className="p-4">
          {hasStructuredData ? (
            <div className={`grid gap-4 ${editMode && diffTargets.length > 0 && isDiffCatalogOpen ? '2xl:grid-cols-[minmax(0,1fr)_20rem] 3xl:grid-cols-[minmax(0,1fr)_22rem]' : ''}`}>
              <div className="space-y-4">
                {Object.entries(displayData).map(([sectionName, sectionData]) => (
                  <ApplicationSectionCard
                    key={sectionName}
                    title={sectionName}
                    sectionPath={sectionName}
                    data={sectionData}
                    editMode={editMode}
                    diffFilter={diffFilter}
                    historyDiffBulkAction={historyDiffBulkAction}
                    sectionBulkAction={sectionBulkAction}
                    activeDiffRowKey={activeDiffRowKey}
                    onFieldChange={handleFieldChange}
                    metadata={data.metadata}
                    currentSavedData={currentSavedData[sectionName] || {}}
                    previousSavedData={previousSavedData?.[sectionName] || null}
                    historyDiffStorageKeyBase={historyDiffStorageKeyBase}
                    buildFieldSourceInfo={buildChatApplicationFieldSource}
                  />
                ))}
                {editMode && !hasVisibleFieldsForCurrentFilter ? (
                  <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
                    {APPLICATION_RESULT_COPY.emptyFilteredState}
                  </div>
                ) : null}
              </div>

              {editMode && diffTargets.length > 0 && isDiffCatalogOpen ? (
                <ApplicationDiffCatalogPanel
                  diffTargets={diffTargets}
                  filteredGroupedDiffTargets={filteredGroupedDiffTargets}
                  activeDiffTarget={activeDiffTarget}
                  activeDiffRowKey={activeDiffRowKey}
                  diffCatalogFilter={diffCatalogFilter}
                  onChangeCatalogFilter={setDiffCatalogFilter}
                  onJumpToField={(rowKey) => {
                    setSectionBulkAction((prev) => ({ mode: 'expand', token: prev.token + 1 }));
                    setPendingScrollRowKey(rowKey);
                  }}
                />
              ) : null}
            </div>
          ) : (
            <div
              className="prose prose-sm max-w-none overflow-x-auto text-gray-700 prose-table:border-collapse prose-th:border prose-th:border-gray-300 prose-th:bg-gray-100 prose-th:px-3 prose-th:py-2 prose-td:border prose-td:border-gray-300 prose-td:px-3 prose-td:py-2"
              data-testid="application-markdown-content"
            >
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {data.applicationContent || ''}
              </ReactMarkdown>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
};
